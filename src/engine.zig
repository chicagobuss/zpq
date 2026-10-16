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
const ResultProgram = @import("core/result_program.zig").ResultProgram;
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
const filter_selection = @import("core/filter/selection.zig");
const filter_parser = @import("core/filter/parser.zig");
const filter_prune = @import("core/filter/prune.zig");
const statistics = @import("core/parquet/statistics.zig");
const membership = @import("core/filter/membership.zig");
const sbbf = @import("core/parquet/bloom.zig");
const streaming = @import("core/writer/streaming.zig");
const fastpath = @import("core/writer/fastpath.zig");

const s3 = @import("io/s3.zig");
const s3_window = @import("io/s3_window.zig");
const tls = @import("io/tls.zig");
const http = @import("io/http.zig");
const sigv4 = @import("io/sigv4.zig");
const multipart_sink = @import("io/multipart_sink.zig");
const pool_mod = @import("io/pool.zig");
const coalescer = @import("io/coalescer.zig");
const work_cursor = @import("io/work_cursor.zig");
const spawn_util = @import("core/spawn.zig");
const AtomicWorkCursor = work_cursor.AtomicWorkCursor;
const meta_cache_mod = @import("io/meta_cache.zig");

/// Local coalesce gap. There is no first-byte latency to trade for extra bytes, so only the few bytes of metadata
/// between adjacent column chunks are merged. S3 uses `coalescer.s3CoalesceGap` instead.
const COALESCE_GAP: u64 = 64 * 1024;

const S3Fetcher = s3_window.Fetcher(s3.Pool(POOL_SIZE));

const FetchWindow = struct {
    limit: u64,
    always: bool = false,
    /// Explain fetches the trailer and exact footer only, and never starts a data fetcher.
    footer_only: bool = false,
};

// Large Lambda scans should overlap fetching and decoding instead of retaining a serial decode tail.
const lambda_window_cap: u64 = 128 << 20;

/// A query's S3 fetch window: `ZPQ_S3_WINDOW_KIB` when set, else budget-derived and capped on Lambda.
fn s3FetchWindow(env: ?std.process.Environ, max_memory: usize) u64 {
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

/// S3 pool permits. Data fetches may use all of them; footer fetches stay at `READ_CONCURRENCY`.
pub const POOL_SIZE: usize = s3.S3_IN_FLIGHT_MAX;

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
const ByteAccount = struct {
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

    fn add(self: *ByteAccount, n: u64) void {
        if (n == 0) return;
        const now = self.used.fetchAdd(n, .monotonic) + n;
        self.note(now);
    }

    fn sub(self: *ByteAccount, n: u64) void {
        if (n == 0) return;
        _ = self.used.fetchSub(n, .monotonic);
    }

    fn tryAdd(self: *ByteAccount, n: u64) bool {
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

/// Permits for an S3 output's part uploads, apart from the input reads' so neither waits for the other, over the same
/// pool so uploads reuse the connections reads leave idle. As many as uploads had while they shared the read permits.
pub const OUTPUT_POOL_SIZE: usize = s3.MAX_PARTS;
pub const OutputLane = pool_mod.Lane(s3.Pool(POOL_SIZE), OUTPUT_POOL_SIZE);

/// Zig's 16 MiB default makes short-lived worker creation expensive, but this is deliberately not tuned to the minimum:
/// a real fetch descends into BoringSSL and libc `getaddrinfo`/NSS, whose stack use nothing here bounds.
const READ_WORKER_STACK_SIZE: usize = 4 << 20;

/// Footer and metadata worker loops. One worker holds one permit. A worker sleeping in retry backoff still holds
/// its slot, so live concurrency can dip below this while a host throttles. Data fetches pass `s3.inFlight`;
/// `s3.DataWave` holds the first of those at `s3.firstWaveCap` (`ZPQ_S3_FIRST_WAVE`, default `S3_FIRST_WAVE`) until
/// a data-range response header arrives.
pub const READ_CONCURRENCY: usize = s3.S3_FIRST_WAVE;

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
    /// Force eligible GROUP BY queries through shared partitions.
    partagg: bool = false,
    inputs: []const []const u8, // raw paths/URLs (pre-glob — caller globs)
    output: ?[]const u8 = null,
    /// CLI-only: an already-open sink and an empty scalar result cut.
    output_fd: ?local_fs.fd_t = null,
    /// CLI-only: the answer is only written to Parquet, so a GROUP BY may build it as typed columns
    /// (`scan.MultiAggArgs.typed_answer`), never read back as rows.
    typed_answer: bool = false,
    empty_scalar: bool = false,
    quiet_diagnostics: bool = false,
    filter: ?[]const u8 = null,
    aggregate: ?[]const u8 = null,
    group_by: ?[]const u8 = null,
    columns: ?[]const []const u8 = null,
    select: ?[]const u8 = null,
    /// GROUP BY output columns as a select list writes them (SQL).
    select_cols: ?[]const expr_ast.SelectColumn = null,
    /// ORDER BY / LIMIT / OFFSET over the aggregate result rows (SQL); keys index `select_cols`.
    result_order: expr_ast.ResultOrder = .{},
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
    /// When set, receives the high-water of footer bytes charged to the resident budget.
    footer_resident_peak: ?*u64 = null,
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
        // The write streams row groups as they come; an ORDER BY over the rows goes through `runPrintTo`.
        if (args.result_order.rows.len > 0) return error.OrderByNotSupported;
        return .{ .write = try runWrite(ctx, args, out_path) };
    }
    return error.MissingOutputOrAggregate;
}

// ============================================================
// Aggregate path
// ============================================================

pub const AggregateExplanation = struct {
    aggregate: scan.AggregateExplanation,
    io_mode: enum { mmap, pread, uring, s3_buffered, s3_windowed },
    encoded: bool,
    max_memory: usize,
    sketch: ?scan.SketchTopK,
    result: ResultExplanation = .{},
};

/// The result program's part of a plan: HAVING, hidden ORDER BY/HAVING columns, and whether the strategies may cut by
/// the LIMIT (`masked` under HAVING, `none` without one).
pub const ResultExplanation = struct {
    having: bool = false,
    hidden: usize = 0,
    order_keys: usize = 0,
    cut_limit: enum { none, kept, masked } = .none,

    fn of(order: expr_ast.ResultOrder) ResultExplanation {
        return .{
            .having = order.having != null,
            .hidden = order.hidden.len,
            .order_keys = order.keys.len,
            .cut_limit = if (order.limit == null) .none else if (ResultProgram.cutOrder(order).limit == null) .masked else .kept,
        };
    }
};

/// Planning shares the execution path, with caller-owned arena storage and metadata-only input opening.
pub fn explainAggregate(ctx: Context, arena: std.mem.Allocator, args: QueryArgs) !AggregateExplanation {
    if (args.inputs.len == 0) return error.NoInputs;
    if (args.aggregate == null and args.group_by == null) return error.MissingOutputOrAggregate;
    if (args.select != null or args.columns != null) return error.AggregateMutuallyExclusive;
    return executeAggregate(ctx, arena, args, args.aggregate orelse "", true);
}

fn runAggregate(ctx: Context, args: QueryArgs, agg_str: []const u8) !AggResult {
    var arena_state = std.heap.ArenaAllocator.init(ctx.gpa);
    defer arena_state.deinit();
    return executeAggregate(ctx, arena_state.allocator(), args, agg_str, false);
}

fn executeAggregate(
    ctx: Context,
    arena: std.mem.Allocator,
    args: QueryArgs,
    agg_str: []const u8,
    comptime explain: bool,
) !if (explain) AggregateExplanation else AggResult {
    var changed = false;
    return executeAggregateOver(ctx, arena, args, agg_str, explain, &changed) catch |err|
        if (changed) error.InputChanged else err;
}

/// `changed`: set on an error when a local input changed under the query (`OpenedInputs.whole`).
fn executeAggregateOver(
    ctx: Context,
    arena: std.mem.Allocator,
    args: QueryArgs,
    agg_str: []const u8,
    comptime explain: bool,
    changed: *bool,
) !if (explain) AggregateExplanation else AggResult {
    const empty_result = args.empty_scalar or args.result_order.limit == 0;
    var t: Timings = .{};

    // 1. Open all inputs — local mmap or s3 parallel range-fetch.
    //    This step already does column pruning for s3 (parses the
    //    aggregate to know which leaves to fetch), so by the time we
    //    return, every Input.bytes contains exactly what the scan
    //    needs.
    const prof = profile.threadOf(args.profiler, .main);
    const t_open = nowMonoNs();
    var opened = try openInputs(ctx, arena, args, .{
        .limit = s3FetchWindow(ctx.env, args.max_memory),
        .footer_only = explain or empty_result,
    });
    defer opened.deinit();
    errdefer changed.* = !opened.whole();
    const t_open_end = nowMonoNs();
    t.read_ns = @intCast(t_open_end - t_open);
    profile.record(prof, .open_inputs, t_open, t_open_end, profile.args(.{args.inputs.len}));

    // 2. Run the orchestrator. (Yes, this re-parses the aggregate —
    //    cheap, microseconds. Easier than threading the parsed AST
    //    through the open step + the scan step.) Footers parsed here are probed here; the scan probes the ones it
    //    parses.
    //    Local files may be read explicitly rather than faulted in (`io/local_reader.zig`); their footers are then
    //    parsed here from the mapping, since the scan's copy of the bytes holds only what the reader plans.
    var io_policy = local_reader.Policy.fromEnv(ctx.env);
    // A GROUP BY cut by a plain COUNT or a count(DISTINCT) first and descending (sketch, census), or by a LIMIT with
    // no ORDER BY (first keys), reads the input twice: the mapping serves both passes (the first warms the page cache for the second),
    // where the explicit reader reads each row group once and so rules those cuts out. Even cold, the two cheap
    // passes beat one full grouping; when the cut then declines, the mapping costs little. Only `auto` is steered:
    // an explicit pread/io_uring keeps its exact fallback. What the strategies may cut by is `ResultProgram.cutOrder`
    // (under HAVING, no limit), and the shape is decided from the query bound to the footer, as the scan binds it.
    var cut_args = args;
    cut_args.result_order = ResultProgram.cutOrder(args.result_order);
    // The footers, parsed once here when the cut shape needs one, serve the scan below as well.
    var steer_metas: ?[]const schema.FileMetaData = null;
    if (io_policy.profile == .auto and args.group_by != null and cut_args.result_order.limit != null and
        !empty_result and allLocal(opened))
        if (sketchFromEnv(ctx.env)) |cfg| steer: {
            // A footer that does not parse is reported where the scan parses it; it steers nothing here.
            const m = materializeMetas(arena, opened, null, prof) catch break :steer;
            steer_metas = m;
            const meta0 = m[0];
            var probe_args: scan.MultiAggArgs = .{
                .inputs = &.{},
                .aggregate = agg_str,
                .group_by = args.group_by,
                .select_cols = args.select_cols,
                .result_order = cut_args.result_order,
            };
            probe_args.column_order = args.column_order;
            switch (scan.cutShape(arena, &meta0, probe_args)) {
                .count => io_policy.profile = .mmap,
                .count_distinct, .first_keys => if (cfg.census) {
                    io_policy.profile = .mmap;
                },
                .none => {},
            }
        };
    // Off Linux everything below is comptime-dead, so the reader (io_uring, futexes) is never compiled in there.
    const on_linux = @import("builtin").os.tag == .linux;
    const all_local = on_linux and allLocal(opened);
    // Files mapped when descriptors ran short gave theirs back: read through the mapping unless a mode is asked for.
    const choose_io = all_local and (io_policy.profile != .auto or opened.descriptorsHeld());
    const resident: f64 = if (choose_io and io_policy.profile == .auto) localResidency(ctx.gpa, opened) else 0;
    // Anything but the plain mapping needs the footers and the plan here: the explicit modes to read by them, and
    // `auto` over uncached files to weigh how much of them the query reads.
    const plan_io = choose_io and (io_policy.profile == .pread or io_policy.profile == .uring or
        (io_policy.profile == .auto and resident < 0.8));
    var probed = opened.membership;
    const metas: ?[]const schema.FileMetaData = if (explain or empty_result or opened.has_preparsed_meta or plan_io or
        steer_metas != null)
    blk: {
        const m = steer_metas orelse try materializeMetas(arena, opened, args.diag, prof);
        const filter: ?filter_ast.Filter = if (args.filter) |fs|
            (if (fs.len > 0) try filter_parser.parse(arena, fs, &m[0], args.diag) else null)
        else
            null;
        if (!explain and !empty_result) probed.add(try loadLocalMembership(arena, opened, m, filter, args.scan_all));
        break :blk m;
    } else null;
    const local_plan: ?LocalPlan = if (plan_io) try planLocalReads(arena, args, opened, metas.?) else null;
    const coverage: f64 = if (local_plan) |lp| lp.coverage() else 0;
    const io_mode: local_reader.Mode = if (choose_io) io_policy.choose(resident, coverage) else .mmap;
    profile.record(prof, .io_choice, nowMonoNs(), nowMonoNs(), profile.args(.{
        io_policy.profile,
        io_mode,
        @as(u64, @intFromFloat(resident * 1000)),
        @as(u64, if (local_plan != null) @intFromFloat(coverage * 1000) else std.math.maxInt(u32)),
    }));
    var scan_args: scan.MultiAggArgs = .{
        .inputs = opened.inputs,
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
        .result_order = cut_args.result_order,
        .typed_answer = args.typed_answer and args.output != null,
        .column_order = args.column_order,
        .max_memory = args.max_memory,
        // Half the budget for decoded row groups: the S3 fetch window takes a quarter, GROUP BY tables the rest.
        .decode_limit = args.max_memory / 2,
        .diag = args.diag,
        .profiler = args.profiler,
        .sketch_topk = sketchFromEnv(ctx.env),
        .partagg = args.partagg or envFlag(ctx.env, "ZPQ_PARTAGG"),
        .spill = !envDisables(ctx.env, "ZPQ_SPILL"),
        // Tests: a small threshold makes a small input move to the runs.
        .spill_groups = if (ctx.env.getPosix("ZPQ_SPILL_GROUPS")) |v|
            std.fmt.parseInt(usize, v, 10) catch 1 << 18
        else
            1 << 18,
        .distinct_pairs = if (envFlag(ctx.env, "ZPQ_DISTINCT_PAIRS"))
            .on
        else if (envDisables(ctx.env, "ZPQ_DISTINCT_PAIRS")) .off else .auto,
        .ndv_union = !envDisables(ctx.env, "ZPQ_NDV_UNION"),
        .dense = !envDisables(ctx.env, "ZPQ_DENSE"),
    };
    if (args.quiet_diagnostics) if (scan_args.sketch_topk) |*cfg| {
        cfg.trace = false;
    };
    if (explain) {
        if (io_mode != .mmap) {
            // As `planLocalReader` hands the scan its inputs: gated, with the mapped file beside them.
            const gated = try arena.dupe(scan.Input, opened.inputs);
            for (gated, opened.mmaps) |*in, m| {
                in.gate = metadataGate();
                if (m != null) in.plan_bytes = in.bytes;
            }
            scan_args.inputs = gated;
        }
        return .{
            .aggregate = try scan.explainAggregate(ctx.gpa, arena, scan_args),
            .io_mode = if (allLocal(opened)) switch (io_mode) {
                .mmap => .mmap,
                .pread => .pread,
                .uring => .uring,
            } else if (hasGatedInputs(opened.inputs)) .s3_windowed else .s3_buffered,
            .encoded = scan_args.encoded,
            .max_memory = args.max_memory,
            .sketch = scan_args.sketch_topk,
            .result = .of(args.result_order),
        };
    }
    // Declared after `opened`, so it is torn down (its threads joined) before the mappings go.
    var reader: ?*local_reader.Reader = null;
    defer if (reader) |rd| rd.destroy();
    const scan_inputs = if (io_mode == .mmap or empty_result) opened.inputs else try planLocalReader(
        ctx,
        arena,
        opened,
        local_plan.?,
        // The reader's buffers count against the budget as the S3 fetch window does: a fixed 1 GiB held reads far
        // past --max-memory (and N GiB under N libzpq queries).
        .{
            .mode = io_mode,
            .threads = io_policy.threads,
            .window = io_policy.window orelse s3_window.windowFor(args.max_memory),
        },
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
    var starter: ReadStarter = undefined;
    if (reader) |rd| {
        starter = .{ .rd = rd };
        scan_args.start_reads = .{ .ctx = &starter, .start = ReadStarter.start };
    }
    scan_args.inputs = scan_inputs;
    if (empty_result) {
        const empty_metas = try arena.dupe(schema.FileMetaData, metas.?);
        for (empty_metas) |*m| {
            m.num_rows = 0;
            m.row_groups = .empty;
        }
        scan_args.metas = empty_metas;
    }
    var r = try scan.runMultiAggregate(ctx.gpa, arena, scan_args);
    // Known once the reader has started, which the scan does once it has planned.
    if (reader) |rd| {
        const b = rd.backends();
        profile.record(prof, .local_backend, t_reader, t_reader, profile.args(.{
            b.uring_threads,
            b.pread_threads,
            b.direct_files,
            b.buffered_files,
        }));
    }
    t.parse_ns = r.timings.parse_ns;
    t.core = r.timings.core;
    t.decode_wall_ns = r.timings.decode_wall_ns;
    probed.add(r.membership);
    if (args.footer_resident_peak) |p| p.* = opened.footer_resident_peak;

    defer r.state.deinit(ctx.gpa);
    errdefer r.output.deinit(ctx.gpa);
    // ORDER BY keys index the SELECT list then the hidden terms, which GROUP BY rows follow column for column.
    if (r.output.group_cols) |cols| for (args.result_order.keys) |key| std.debug.assert(key.column < cols.len);
    const program: ResultProgram = .{ .order = args.result_order, .visible = r.visible_cols };
    try program.finish(ctx.gpa, &r.output, &r.state.group_col_types);
    // Without GROUP BY, the aggregates shown: the hidden HAVING terms follow them.
    const n_aggs = if (r.output.group_rows == null) r.visible_cols orelse r.state.agg_calls.len else r.state.agg_calls.len;

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
        bytes_out = if (r.output.typed) |typed|
            try writeTypedParquet(arena, out_path, r.output.group_cols.?, st.group_col_types.?, typed, codec, zstd, args.output_fd)
        else if (r.output.group_rows) |rows|
            try writeGroupedParquet(arena, out_path, r.output.group_cols.?, st.group_col_types.?, rows, codec, zstd, args.output_fd)
        else
            try writeOneRowParquet(arena, out_path, st.agg_calls[0..n_aggs], st.accumulators[0..n_aggs], st.agg_sources[0..n_aggs], codec, zstd, args.output_fd, args.empty_scalar or r.output.no_row);
        const t_footer_end = nowMonoNs();
        t.footer_ns += @intCast(t_footer_end - t_footer);
        profile.record(prof, .footer_write, t_footer, t_footer_end, profile.args(.{bytes_out}));
    }

    try opened.intact();
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
    /// The input's fetch gate (`scan.Input.gate`), and this row group's index in its file.
    gate: ?scan.RowGroupGate = null,
    file_rg: usize = 0,

    /// Wait until the row group's bytes are in, when they are still being fetched.
    fn arrive(self: RGJob) !void {
        if (self.gate) |g| try g.wait(g.ctx, self.file_rg);
    }

    /// The row group's bytes are no longer needed: the fetch window may have them back.
    fn leave(self: RGJob) void {
        if (self.gate) |g| g.done(g.ctx, self.file_rg, 1);
    }
};

/// Row group `rg` of `in` is skipped: give its fetch window share back as if it had been read.
fn skipRowGroup(in: scan.Input, rg: usize) void {
    if (in.gate) |g| g.done(g.ctx, rg, 1);
}

const EncodedRG = struct {
    bytes: []u8 = &.{}, // encoded RG bytes (gpa-owned; writer frees after sink.write)
    rg: ?schema.RowGroup = null, // footer meta, offsets relative to 0 (meta_arena-owned)
    /// Too big for a producer to decode at once: the writer re-encodes it a slice at a time straight into the sink in
    /// its turn (`encodeRGSliced`), and releases its input after.
    deferred: bool = false,
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
    diag: ?*Diag,
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
            rg_enc.column(&agg, i, col_arena.allocator()) catch |err| {
                if (err == error.DecimalValueExceedsPrecision) {
                    Diag.setColumn(diag, agg.schema_elems[i].name);
                }
                return err;
            };
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
    window: *std.Io.Semaphore, // the in-flight output budget, in `win_unit` permits (`jobUnits`)
    win_unit: u64,
    win_units: usize,
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
    /// What one producer's decoded rows may take; a row group past it is written in slices (`encodeRGSliced`).
    decode_share: usize = std.math.maxInt(usize),
    diag: ?*Diag = null,
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
    const enc = produceRG(w, i, ma, regions, prof) catch |e| {
        w.jobs[i].leave();
        w.slots[i].enc = .{ .err = e };
        w.slots[i].done.store(true, .release);
        w.completions.post(w.io);
        return;
    };
    // A deferred row group's input is the writer's to release.
    if (!enc.deferred) w.jobs[i].leave();
    profile.gauge(prof, .buffered_bytes, @intCast(enc.bytes.len));
    const now = w.inflight.fetchAdd(enc.bytes.len, .acq_rel) + enc.bytes.len;
    var p = w.peak_bytes.load(.monotonic);
    while (now > p) p = w.peak_bytes.cmpxchgWeak(p, now, .monotonic, .monotonic) orelse break;
    w.slots[i].enc = enc;
    w.slots[i].done.store(true, .release);
    w.completions.post(w.io);
}

/// Job `i`'s output: a byte copy, the row group sliced when it is too big to decode at once, or re-encoded whole.
fn produceRG(w: *WinCtx, i: usize, ma: std.mem.Allocator, regions: *WorkRegions, prof: ?*profile.Thread) !EncodedRG {
    const job = w.jobs[i];
    try job.arrive();
    if (job.passthrough) return copyRGToBuffer(w.gpa, ma, job, w.kept_set, prof, w.bloom_leaves);
    const bloom: consumer.BloomWrite = .{ .fpp = w.bloom_outputs };
    if (try sliceRG(job, w.fetch_arr, w.decode_share)) return .{ .deferred = true };
    return encodeRGToBuffer(
        w.gpa,
        ma,
        regions,
        job,
        w.filter,
        w.fetch_arr,
        w.output_specs,
        w.codec,
        w.zstd,
        w.decode_options,
        prof,
        bloom,
        w.diag,
    );
}

/// Whether `job`'s row group is re-encoded a slice of rows at a time (`encodeRGSliced`): its decoded columns would take
/// more than `share`. One that would, with a column that has no sliced form, is refused before anything decodes.
fn sliceRG(job: RGJob, fetch_arr: []const bool, share: usize) error{RowGroupTooLarge}!bool {
    const rg = job.rg;
    if (rg.num_rows <= 0) return false;
    // Decoded, then buffered for the encoder, then encoded.
    const est = consumer.decodeEstimate(.{
        .rg = rg,
        .meta = job.meta,
        .src = .{ .bytes = job.bytes, .byte_origin = 0 },
        .filter = null,
        .read = fetch_arr,
        .consumed = fetch_arr,
    }, fetch_arr, &.{}, false) * 3;
    if (est <= share) return false;
    for (fetch_arr, 0..) |needed, ci| {
        if (!needed or ci >= rg.columns.items.len) continue;
        if (!consumer.SliceReader.supports(rg, job.meta, ci)) return error.RowGroupTooLarge;
    }
    return true;
}

/// Re-encode `job`'s row group a slice of rows at a time, one output row group per slice, straight into the writer's
/// sink: each slice decodes (`consumer.SliceReader`), filters and encodes on memory of its own, so neither the decoded
/// rows nor the output are ever held whole. Runs on the writer, in the row group's turn; returns the rows written.
fn encodeRGSliced(w: *WinCtx, job: RGJob, prof: ?*profile.Thread) !i64 {
    const gpa = w.gpa;
    const rg = job.rg;
    const num_rows: usize = @intCast(@max(0, rg.num_rows));
    const src: consumer.RGSrc = .{ .bytes = job.bytes, .byte_origin = 0 };
    const est = consumer.decodeEstimate(.{
        .rg = rg,
        .meta = job.meta,
        .src = src,
        .filter = null,
        .read = w.fetch_arr,
        .consumed = w.fetch_arr,
    }, w.fetch_arr, &.{}, false) * 3;

    var pages = std.heap.ArenaAllocator.init(gpa);
    defer pages.deinit();
    var readers: std.ArrayList(consumer.SliceReader) = .empty;
    defer for (readers.items) |*r| r.deinit();
    const lookup = try pages.allocator().alloc(?usize, w.fetch_arr.len);
    @memset(lookup, null);
    for (w.fetch_arr, 0..) |needed, ci| {
        if (!needed or ci >= rg.columns.items.len) continue;
        const r = try consumer.SliceReader.init(pages.allocator(), gpa, rg, job.meta, src, ci, w.decode_options) orelse
            return error.UnsupportedColumnType; // `sliceRG` checked every column
        lookup[ci] = readers.items.len;
        try readers.append(pages.allocator(), r);
    }
    const per_row = @max(1, est / num_rows);
    const slice_rows = std.math.clamp(w.decode_share / per_row, 1, num_rows);

    var timings: consumer.Timings = .{ .prof = prof };
    defer {
        w.core.decode_ns += timings.decode_ns;
        w.core.eval_ns += timings.eval_ns;
        w.core.encode_ns += timings.encode_ns;
    }
    var surviving: i64 = 0;
    var done: usize = 0;
    while (done < num_rows) {
        const n = @min(slice_rows, num_rows - done);
        done += n;
        var slice = std.heap.ArenaAllocator.init(gpa);
        defer slice.deinit();
        const sa = slice.allocator();
        const cols = try sa.alloc(filter_eval.Batch.Column, readers.items.len);
        for (readers.items, cols) |*r, *c| c.* = try r.next(sa, n);
        const batch: filter_eval.Batch = .{ .cols = cols, .num_rows = n };
        var sel = try filter_selection.SelectionVector.init(sa, n);
        if (w.filter) |f| try filter_eval.evaluate(f, &batch, &sel, lookup, sa);
        var agg = try consumer.initOutputAggregator(sa, job.meta, w.output_specs);
        _ = try consumer.appendSliceRows(&agg, &batch, lookup, &sel, w.output_specs, sa);
        if (agg.num_rows == 0) continue;
        var rg_enc: consumer.RGEncoder = .{
            .out_arena = w.arena,
            .sink = w.sink,
            .out_offset = &w.out_offset,
            .codec = w.codec,
            .zstd = w.zstd,
            .bloom_write = .{ .fpp = w.bloom_outputs },
            .timings = &timings,
        };
        for (0..w.output_specs.len) |i| {
            rg_enc.column(&agg, i, sa) catch |err| {
                if (err == error.DecimalValueExceedsPrecision) {
                    Diag.setColumn(w.diag, agg.schema_elems[i].name);
                }
                return err;
            };
        }
        const out = rg_enc.finish(agg.num_rows);
        try w.new_row_groups.append(w.arena, out.rg.?);
        surviving += out.surviving_rows;
    }
    return surviving;
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
        } else if (enc.deferred) {
            defer w.jobs[next].leave();
            if (w.first_err == null) {
                const before = w.new_row_groups.items.len;
                const kept = encodeRGSliced(w, w.jobs[next], prof) catch |e| blk: {
                    w.first_err = e;
                    break :blk 0;
                };
                w.rows_kept += kept;
                w.rgs_kept += w.new_row_groups.items.len - before;
            }
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
        for (0..windowPermits(WinCtx, w, next)) |_| w.window.post(w.io);
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
        // One dispatching thread takes every permit, so no two producers each hold part of what both need.
        for (0..windowPermits(Ctx, w, i)) |_| w.window.waitUncancelable(w.io);
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
    /// The window's permit size and count (`jobUnits`).
    win_unit: u64,
    win_units: usize,
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
    /// The recompressed output one producer may gather; a row group that could write more is deferred to the writer.
    share: u64 = std.math.maxInt(u64),
};

const RecompressSlot = struct {
    /// Private to the producer until `done`, then to the writer, which frees it once the row group is written.
    arena: std.heap.ArenaAllocator,
    chunks: []?consumer.RecompressedChunk = &.{},
    /// Too big to gather (`recompressedSize`): the writer recompresses it straight into the sink.
    deferred: bool = false,
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
    // The input is released by the writer once it has placed the row group: it still copies the chunks that keep
    // their codec, and carried Bloom filters, from it.
    if (job.arrive()) |_| {
        // Output that could pass this producer's share (a codec that expands, as to UNCOMPRESSED) is not gathered
        // here: the writer recompresses it page by page straight into the sink in its turn.
        if (recompressedSize(job.rg, w.kept, w.codec) > w.share) {
            slot.deferred = true;
        } else if (consumer.recompressRowGroup(
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
    } else |e| slot.err = e;
    slot.encode_ns = timings.encode_ns;
    slot.done.store(true, .release);
    w.completions.post(w.io);
}

/// The most a recompressing copy of `rg` can write: a chunk that changes codec at its uncompressed size (the bound of
/// an UNCOMPRESSED rewrite, and above what a compressing codec writes), any other as it is.
fn recompressedSize(rg: *const schema.RowGroup, kept: ?[]const bool, codec: schema.CompressionCodec) u64 {
    var total: u64 = 0;
    for (rg.columns.items, 0..) |chunk, ci| {
        if (kept) |k| if (ci >= k.len or !k[ci]) continue;
        const m = chunk.meta_data orelse continue;
        const size = if (m.codec == codec)
            m.total_compressed_size
        else
            @max(m.total_compressed_size, m.total_uncompressed_size);
        total += @intCast(@max(0, size));
    }
    return total;
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
                .{
                    .codec = w.codec,
                    .zstd = w.zstd,
                    .scratch = w.gpa,
                    .done = if (slot.deferred) null else slot.chunks,
                },
                .{ .fpp = w.bloom_leaves, .meta = job.meta, .scratch = w.gpa },
            )) |out| {
                if (out.rg) |rg| w.new_row_groups.append(w.arena, rg) catch |e| {
                    w.first_err = e;
                };
                w.rows_kept += out.surviving_rows;
                if (out.surviving_rows > 0) w.rgs_kept += 1;
            } else |e| w.first_err = e;
        }
        job.leave();
        slot.arena.deinit();
        slot.arena = std.heap.ArenaAllocator.init(w.gpa);
        for (0..jobUnits(job, w.win_unit, w.win_units)) |_| w.window.post(w.io);
    }
}

const WriteWindow = struct {
    workers: usize,
    window: usize,
    /// The byte budget for buffered output in units of `unit` bytes: a row group in flight holds `jobUnits` of them.
    unit: u64,
    units: usize,
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
    // Never more than a quarter of the query's budget: the S3 fetch window takes another (`s3FetchWindow`), and the
    // rest is for the producers' decode and encode working sets.
    const inflight_budget: u64 = @min(@as(u64, args.max_memory) / 4, if (system.onLambda(ctx.env))
        system.discoverAvailableMemory(ctx.env) / 2
    else
        @min(@as(u64, 256) * 1024 * 1024, system.discoverAvailableMemory(ctx.env) / 8));
    const window = system.windowCapacity(inflight_budget, avg_rg, jobs.len);
    const unit = @max(@as(u64, 1), inflight_budget / window_units);
    return .{
        .workers = @min(async_budget, window),
        .window = window,
        .unit = unit,
        .units = @intCast(@max(@as(u64, 1), inflight_budget / unit)),
        .budget = async_budget,
    };
}

/// Permits the write window's byte budget is cut into.
const window_units = 1024;

/// The window's permits a row group in flight holds: its own size, not the average, so a few large row groups among
/// many small ones cannot all be buffered at once (they held several times the budget behind a slow sink). Never
/// more than the whole window, so a row group larger than the budget still runs, alone.
fn jobUnits(job: RGJob, unit: u64, units: usize) usize {
    const bytes: u64 = @intCast(@max(@as(i64, 0), job.rg.total_byte_size));
    return @intCast(std.math.clamp(std.math.divCeil(u64, bytes, unit) catch 1, 1, units));
}

/// `jobUnits` for job `i` of a write fan-out context; one permit for a context that counts row groups instead.
fn windowPermits(comptime Ctx: type, w: *const Ctx, i: usize) usize {
    if (!@hasField(Ctx, "win_unit")) return 1;
    return jobUnits(w.jobs[i], w.win_unit, w.win_units);
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

/// The sketched count cut (`scan.SketchTopK`) runs wherever it is eligible; `ZPQ_SKETCH_TOPK=0` turns it off,
/// `ZPQ_SKETCH_ADMIT` and `ZPQ_SKETCH_BITS` tune it, and `ZPQ_SKETCH_TRACE=1` reports its decisions on stderr.
fn sketchFromEnv(env: std.process.Environ) ?scan.SketchTopK {
    if (envDisables(env, "ZPQ_SKETCH_TOPK")) return null;
    var cfg: scan.SketchTopK = .{};
    if (env.getPosix("ZPQ_SKETCH_ADMIT")) |v| cfg.admit_per_row = std.fmt.parseInt(usize, v, 10) catch cfg.admit_per_row;
    if (env.getPosix("ZPQ_SKETCH_BITS")) |v| cfg.cell_bits = std.math.clamp(std.fmt.parseInt(u6, v, 10) catch cfg.cell_bits, 10, 30);
    cfg.trace = env.getPosix("ZPQ_SKETCH_TRACE") != null;
    cfg.census = !envDisables(env, "ZPQ_SKETCH_CENSUS");
    return cfg;
}

/// Whether `name` is set to "0" in `env`.
fn envFlag(env: std.process.Environ, name: []const u8) bool {
    const value = env.getPosix(name) orelse return false;
    return std.mem.eql(u8, value, "1");
}

fn envDisables(env: std.process.Environ, name: []const u8) bool {
    const v = env.getPosix(name) orelse return false;
    return std.mem.eql(u8, v, "0");
}

/// S3 credentials from the environment, telling a missing region apart from missing keys.
fn credentialsFromEnv(env: std.process.Environ) error{ NoCredentials, NoRegion }!s3.Credentials {
    var creds = s3.Credentials.fromEnv(env) catch |err| {
        return if (err == error.NoRegion) error.NoRegion else error.NoCredentials;
    };
    // The Lambda binary pre-connects the regional endpoint before it knows the bucket. Path-style uses that
    // same host, so those sockets are the ones the query acquires. The CLI is not on Lambda and stays virtual-hosted.
    if (system.onLambda(env)) creds.path_style = true;
    return creds;
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
    /// Leaves to decode: those the output references plus the filter's and the row sort keys'.
    fetch: []const bool,
    labels: Labels,
    /// SQL ORDER BY over plain rows (`ResultOrder.rows`), bound to input leaves.
    row_keys: []const RowSortKey = &.{},

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
    const row_keys = try arena.alloc(RowSortKey, args.result_order.rows.len);
    for (args.result_order.rows, row_keys, 0..) |rk, *key, k| {
        const e = try expr_parser.parseExprOnly(arena, rk.expr, meta0, args.diag);
        e.collectColumns(fetch);
        if (e == .col_ref) {
            const ci = e.col_ref.col_idx;
            const path = (try metadata.leafPathSegments(arena, meta0, ci)) orelse return error.UnknownColumn;
            if (meta0.getColumnLevels(path).max_rep > 0) {
                Diag.setColumn(args.diag, rk.expr);
                return error.NestedRowOutputNotSupported;
            }
            const elem = metadata.leafSchemaElement(meta0, ci) orelse return error.UnknownColumn;
            key.* = .{ .spec = .{ .passthrough = ci }, .descending = rk.descending, .unsigned = schema.isUnsignedInt(elem) };
        } else {
            // Computed as a written column would be; its lane is signed (arithmetic refuses UINT64 operands).
            const alias = try std.fmt.allocPrint(arena, "__order_{d}", .{k});
            key.* = .{ .spec = .{ .computed = .{ .expr = e, .alias = alias } }, .descending = rk.descending, .unsigned = false };
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
        .row_keys = row_keys,
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
    var changed = false;
    return runWriteOver(ctx, args, out_path, &changed) catch |err| if (changed) error.InputChanged else err;
}

/// As `executeAggregateOver`.
fn runWriteOver(ctx: Context, args: QueryArgs, out_path: []const u8, changed: *bool) !WriteResult {
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
    var opened = try openInputs(ctx, arena, args, .{ .limit = s3FetchWindow(ctx.env, args.max_memory) });
    defer opened.deinit();
    errdefer changed.* = !opened.whole();
    const t_open_end = nowMonoNs();
    t.read_ns = @intCast(t_open_end - t_open);
    profile.record(prof, .open_inputs, t_open, t_open_end, profile.args(.{args.inputs.len}));

    // 2. Parse meta of every file (cheap; serial), validate schemas
    //    against meta[0].
    const t_parse_start = nowMonoNs();
    const metas = try materializeMetas(arena, opened, args.diag, prof);
    const meta0 = &metas[0];
    // Same name is not enough: a byte-copy concatenation (and a shared footer) is only valid when the columns are
    // structurally identical, or the footer lies about the copied bytes.
    for (metas[1..]) |*m| {
        if (metadata.schemaMismatch(meta0.schema.items, m.schema.items, .exact) != null) return error.SchemaMismatch;
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
            .{
                .profiler = args.profiler,
                .diag = args.diag,
                // Parts uploading at once come out of the query's budget too: an eighth of it, at least two parts.
                .max_bytes_in_flight = @max(
                    2 * multipart_sink.TARGET_PART_SIZE,
                    @min(multipart_sink.DEFAULT_MAX_BYTES_IN_FLIGHT, args.max_memory / 8),
                ),
            },
        );
        sink = .{
            .ctx = @ptrCast(&mp_sink.?),
            .write_fn = multipart_sink.sinkWriteFn,
        };
    } else {
        fd_sink.fd = args.output_fd orelse try createFile(out_path);
        remove_on_error = args.output_fd == null;
        sink_owns_fd = args.output_fd == null;
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
            // From the schema, never a row group: the first input may have none, and the guards with it.
            .passthrough => |ci| {
                if (ci >= num_leaves) continue;
                const leaf = tree0.leaves[ci];
                if (leaf.type == .INT96)
                    return error.INT96ReencodeNotSupported;
                if (leaf.max_rep > 0)
                    return error.NestedReencodeNotSupported;
                if (metadata.leafSchemaElement(meta0, ci)) |elem| {
                    if (decimal_mod.kindFromSchema(&elem)) |k| {
                        if (k.physical == .BYTE_ARRAY and k.precision > 38) {
                            Diag.setColumn(args.diag, elem.name);
                            return error.UnsupportedDecimalPrecision;
                        }
                    }
                }
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

    if (args.diag) |diag| diag.execution_started = true;
    const kept_opt: ?[]const bool = if (kept_set != null) kept_arr else null;
    if (!need_encoder and args.codec != null and anyChunkNeedsRecompress(metas, kept_opt, args.codec.?)) {
        // Byte copy with a codec change: recompression is CPU-bound, so row groups fan out across the CPU budget
        // behind a byte window, as the re-encode path below does.
        var jobs: std.ArrayListUnmanaged(RGJob) = .empty;
        for (opened.inputs, metas, 0..) |in, *meta_p, ii| {
            bytes_in += in.bytes.len;
            for (meta_p.row_groups.items, 0..) |*src_rg, ri| {
                rgs_in += 1;
                rows_in += src_rg.num_rows;
                try jobs.append(arena, .{
                    .bytes = in.bytes,
                    .meta = meta_p,
                    .rg = src_rg,
                    .file_index = ii,
                    .rg_index = rgs_in - 1,
                    .gate = in.gate,
                    .file_rg = ri,
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
            var window_sem: std.Io.Semaphore = .{ .permits = sizing.units };
            var completions_sem: std.Io.Semaphore = .{ .permits = 0 };
            var rw = RecompressCtx{
                .win_unit = sizing.unit,
                .win_units = sizing.units,
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
            // The output in flight is a quarter of the budget, shared by the row groups recompressing at once.
            rw.share = args.max_memory / 4 / @max(1, effective);
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
            for (meta_const.row_groups.items, 0..) |*src_rg, ri| {
                rgs_in += 1;
                rows_in += src_rg.num_rows;
                const rg_span = profile.Span.begin(prof);
                defer rg_span.end(.copy_rg, profile.args(.{src_rg.total_byte_size}));
                if (in.gate) |g| try g.wait(g.ctx, ri);
                defer skipRowGroup(in, ri);
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
                if (!kept_arr[ci]) continue;
                const e = metadata.leafSchemaElement(meta0, ci) orelse continue;
                if (decimal_mod.kindFromSchema(&e) != null or schema.isFloat16(e)) {
                    has_widened_output = true;
                    break;
                }
            }
        }
        const bytecopy_ok = select_items == null and filter_opt != null and !has_widened_output;

        // 1. Collect surviving row groups (serial, cheap stats-only pruning).
        var jobs: std.ArrayListUnmanaged(RGJob) = .empty;
        for (opened.inputs, 0..) |in, ii| {
            bytes_in += in.bytes.len;
            const meta_p = &metas[ii];
            for (meta_p.row_groups.items, 0..) |*src_rg, ri| {
                rgs_in += 1;
                rows_in += src_rg.num_rows;
                var passthrough = false;
                if (filter_opt) |f| if (!args.scan_all) {
                    const d = filter_prune.pruneRowGroup(src_rg, f, meta_p);
                    if (d == .skip) {
                        skipRowGroup(in, ri);
                        continue;
                    }
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
                    .gate = in.gate,
                    .file_rg = ri,
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
            var window_sem: std.Io.Semaphore = .{ .permits = sizing.units };
            var completions_sem: std.Io.Semaphore = .{ .permits = 0 };
            var w = WinCtx{
                .win_unit = sizing.unit,
                .win_units = sizing.units,
                .gpa = ctx.gpa,
                .jobs = jobs.items,
                .filter = filter_opt,
                .fetch_arr = fetch_arr,
                .output_specs = output_specs,
                .kept_set = kept_set,
                .codec = args.codec orelse .SNAPPY,
                .zstd = zstd,
                .decode_options = .{ .fast_levels = args.fast_levels, .expand_limit = args.max_memory },
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
                .diag = args.diag,
                // Half the budget for the producers' decoded rows (the fetch window and the encoded output in flight
                // take a quarter each), shared among as many as -j allows. Not among those that run at once, which
                // the host's memory sizes: the share sets a sliced row group's output layout, so it depends only on
                // --max-memory and -j.
                .decode_share = args.max_memory / 2 / @max(1, sizing.budget),
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
            @backingInt(priority), pages[0], pages[1], pages[2], pages[3], zstd_counts.rechecked.load(.monotonic),
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

    // When re-encoding, BYTE_ARRAY-backed decimals are emitted as FIXED_LEN_BYTE_ARRAY
    // of the minimal width for their precision. Align the footer schema leaves so the
    // schema agrees with the on-disk column chunks. Byte-copy passthrough
    // (need_encoder=false) preserves the original BYTE_ARRAY encoding untouched.
    if (need_encoder) {
        coerceByteArrayDecimalLeavesToFlba(&new_schema_items);
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

    // Publish only after the sink has reported completion, and only what was read of inputs that stayed whole.
    try opened.intact();
    if (mp_sink) |*ms| {
        try ms.close();
        const mp_stats = ms.snapshotStats();
        t.mp_complete_ns = mp_stats.complete_ns;
        t.mp_await_ns = mp_stats.await_ns;
    }

    if (sink_owns_fd) {
        sink_owns_fd = false;
        try local_fs.closeChecked(fd_sink.fd);
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
/// Walk a flat thrift schema list and rewrite every BYTE_ARRAY DECIMAL leaf to
/// FIXED_LEN_BYTE_ARRAY with minimal byte width for its precision. Used after
/// the re-encode path, where BYTE_ARRAY decimals are emitted as FLBA by the encoder.
/// The file's footer must agree.
fn coerceByteArrayDecimalLeavesToFlba(items: *std.ArrayListUnmanaged(schema.SchemaElement)) void {
    for (items.items) |*elem| {
        // Only leaves (no children) can be DECIMAL.
        const nc = elem.num_children orelse 0;
        if (nc != 0) continue;
        const k = decimal_mod.kindFromSchema(elem) orelse continue;
        // Past 38 digits no FLBA width holds the value; the writers refuse such a column before it gets here.
        if (k.physical == .BYTE_ARRAY and k.precision >= 1 and k.precision <= 38) {
            elem.type = .FIXED_LEN_BYTE_ARRAY;
            elem.type_length = decimal_mod.minBytesForPrecision(k.precision);
        }
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
    fn descriptorsHeld(self: *const OpenedInputs) bool {
        for (self.mmaps) |m_opt| if (m_opt) |m| if (m.fd == null) return false;
        return true;
    }

    /// For a query's error path: zeros read past a cut parse as a corrupt page, which is the cut, not the file.
    fn whole(self: *const OpenedInputs) bool {
        self.intact() catch return false;
        return true;
    }

    pub fn deinit(self: *OpenedInputs) void {
        if (!self.leave_mappings) for (self.mmaps) |m_opt| if (m_opt) |m| m.unmap();
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

fn openInputs(
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
    // Windowed fetches: the mapped file buffers, and the fetcher filling them (`io/s3_window.zig`).
    var s3_mapped: std.ArrayList([]align(s3_window.page) u8) = .empty;
    errdefer for (s3_mapped.items) |b| s3_window.unmapBuffer(b);
    var s3_fetcher: ?*S3Fetcher = null;
    var windowed = false;
    var membership_stats: membership.Stats = .{};
    var footer_resident_peak: u64 = 0;
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
        for (s3_urls, 0..) |u, k| specs[k] = .{
            .url = u,
            .file_index = s3_indices.items[k],
            .footer_only = fetch_window.footer_only,
        };
        // Retained suffixes and in-flight footer bodies share one budget: the caller's max, and never the fetch
        // window's 64 MiB floor when that floor is higher. A suffix that does not fit is dropped and those ranges
        // are fetched later. The streaming window (including a test's ZPQ_S3_WINDOW_KIB) stays the admission limit;
        // a suffix the budget can hold is still reused.
        var account: ByteAccount = .{ .limit = @min(s3_window.windowFor(args.max_memory), @as(u64, args.max_memory)) };
        const batch_span = profile.Span.begin(prof);
        fetchMetaBatch(read_ctx, arena, creds, pool, specs, args.profiler, &account) catch |err| {
            for (specs) |*sp| {
                sp.releaseTail(ctx.gpa);
                if (sp.file_buf.len > 0) ctx.gpa.free(sp.file_buf);
                if (sp.footer_region.len > 0) ctx.gpa.free(sp.footer_region);
            }
            return err;
        };
        batch_span.end(.footer_batch, profile.args(.{specs.len}));
        footer_resident_peak = account.peak.load(.monotonic);
        var specs_returned = false;
        errdefer if (!specs_returned) {
            for (specs) |*sp| {
                sp.releaseTail(ctx.gpa);
                // A windowed buffer is mapped, and unmapped with `s3_mapped`.
                if (sp.file_buf.len > 0 and !windowed) ctx.gpa.free(sp.file_buf);
                if (sp.footer_region.len > 0) ctx.gpa.free(sp.footer_region);
            }
            for (s3_buffers.items) |b| ctx.gpa.free(b);
        };

        const metas_for_plan = try arena.alloc(schema.FileMetaData, specs.len);
        for (specs, metas_for_plan) |sp, *m| m.* = sp.meta;

        // Bloom filters first: what they prove lands on the footers' chunks, so the plan below and the scan workers
        // after it prune the same row groups.
        if (!fetch_window.footer_only and !args.scan_all) if (args.filter) |fs| if (fs.len > 0) {
            const f = try filter_parser.parse(arena, fs, &metas_for_plan[0], args.diag);
            // Bloom holds its own copies of what it probes. Cap them at the bytes the query can keep, not at the
            // fetch window's 64 MiB floor when that floor is above `--max-memory`.
            var src: S3Membership = .init(read_ctx, creds, pool, specs, @min(fetch_window.limit, account.limit));
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
        // Bloom has already copied what it needed. Keep only suffix bytes a planned range still overlaps.
        for (specs) |*sp| trimRetainedSuffix(ctx.gpa, sp, plan, &account);

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
        const Saved = struct {
            ranges: []const coalescer.Range,
            rg_first: []usize,
            gap_cap: u64,
            selected: u64,
        };
        const saved = try arena.alloc(Saved, specs.len);
        var all_pieces: std.ArrayList(coalescer.Piece) = .empty;
        var selected_all: u64 = 0;
        for (specs, saved, 0..) |*sp, *sv, fi| {
            var ranges: std.ArrayList(coalescer.Range) = .empty;
            const rg_first = try arena.alloc(usize, sp.meta.row_groups.items.len + 1);
            for (sp.meta.row_groups.items, 0..) |*rg, ri| {
                rg_first[ri] = ranges.items.len;
                const cols = plan.rowGroupColumns(rg, &sp.meta) orelse continue;
                try appendRgFetchRanges(arena, &ranges, rg, cols, plan.carriedFilters(rg, &sp.meta), sp.total_size);
            }
            rg_first[sp.meta.row_groups.items.len] = ranges.items.len;
            const gap_cap = coalescer.s3CoalesceGap(sp.rtt_ns, s3.S3_BYTES_PER_S);
            var selected: u64 = 0;
            var g: usize = 0;
            for (ranges.items, 0..) |r, ri| {
                while (g + 1 < rg_first.len and rg_first[g + 1] <= ri) g += 1;
                selected +|= r.len();
                const group: u32 = @intCast(g);
                try all_pieces.append(arena, .{
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
            arena,
            all_pieces.items,
            s3.inFlight(ctx.env),
            ampCap(selected_all, fetch_window.limit),
            null,
        );
        if (s3_window.supported) {
            var need: u64 = 0;
            for (upfront) |s| need += s.len();
            windowed = need > fetch_window.limit or (fetch_window.always and need > 0);
        }
        if (fetch_window.footer_only) {
            for (specs, 0..) |*sp, k| {
                const idx = s3_indices.items[k];
                inputs[idx] = .{
                    .name = paths[idx],
                    .bytes = &.{},
                    .logical_size = sp.total_size,
                    .gate = if (windowed) metadataGate() else null,
                };
                // Copy borrowed footer strings into the arena, then release both responses.
                const copy = try arena.dupe(u8, sp.footer_region[0 .. sp.footer_region.len - 8]);
                metas[idx] = try metadata.openFooter(arena, copy);
                meta_ready[idx] = true;
                releaseChargedTail(sp, ctx.gpa, &account);
                ctx.gpa.free(sp.footer_region);
                sp.footer_region = &.{};
            }
            specs_returned = true;
            return .{
                .gpa = ctx.gpa,
                .inputs = inputs,
                .metas = metas,
                .meta_ready = meta_ready,
                .has_preparsed_meta = true,
                .mmaps = mmaps,
                .owned_pools = owned_pools.items,
                .s3_buffers = &.{},
                .leave_mappings = ctx.exit_after,
                .footer_resident_peak = footer_resident_peak,
            };
        }
        var win_order: std.ArrayList(*s3_window.RowGroup) = .empty;
        const win_files = try arena.alloc(s3_window.File, specs.len);

        var fetch_jobs: std.ArrayList(s3.FetchJob) = .empty;
        var span_at: usize = 0;
        for (specs, win_files, saved, 0..) |*sp, *wf, sv, fi| {
            const obj: u32 = @intCast(fi);
            const span_lo = span_at;
            while (span_at < upfront.len and upfront[span_at].object == obj) span_at += 1;
            const file_spans = upfront[span_lo..span_at];
            // The tail serves the footer parse and the copies below. A windowed copy is applied when the row group
            // is admitted, so that tail stays until the fetcher stops; every other way out frees it here.
            var keep_tail = false;
            defer if (!keep_tail) releaseChargedTail(sp, ctx.gpa, &account);
            const meta = &sp.meta;
            sp.rg_columns = try arena.alloc(?[]const bool, meta.row_groups.items.len);
            wf.* = .{ .rgs = try arena.alloc(?*s3_window.RowGroup, meta.row_groups.items.len) };
            @memset(wf.rgs, null);

            // Each row group's ranges, `ranges[rg_first[rg]..rg_first[rg + 1]]`.
            var ranges: std.ArrayList(coalescer.Range) = .empty;
            var filter_ranges: std.ArrayList(coalescer.Range) = .empty;
            const rg_first = try arena.alloc(usize, meta.row_groups.items.len + 1);
            for (meta.row_groups.items, sp.rg_columns, 0..) |*rg, *rg_cols, ri| {
                rg_first[ri] = ranges.items.len;
                rg_cols.* = plan.rowGroupColumns(rg, meta);
                n_rgs += 1;
                const cols = rg_cols.* orelse {
                    n_rgs_pruned += 1;
                    continue;
                };
                // EVERY needed chunk goes into the range list — including ones inside the prefetched tail — so the
                // compact buffer covers it and offset rebasing can find it. Bytes at or past tail_start are
                // satisfied by copying from the tail below (no re-fetch); only the sub-tail_start portion becomes
                // a fetch job.
                try appendRgFetchRanges(arena, &ranges, rg, cols, plan.carriedFilters(rg, meta), sp.total_size);
                for (cols, 0..) |needed, ci| {
                    if (!needed or ci >= rg.columns.items.len) continue;
                    if (!plan.isFilterCol(ci)) continue;
                    const cm = rg.columns.items[ci].meta_data orelse continue;
                    const start: u64 = if (cm.dictionary_page_offset) |dp| @intCast(dp) else @intCast(cm.data_page_offset);
                    const len: u64 = @intCast(cm.total_compressed_size);
                    try filter_ranges.append(arena, .{ .start = start, .end = start + len });
                }
            }
            rg_first[meta.row_groups.items.len] = ranges.items.len;

            if (ranges.items.len == 0) {
                sp.file_buf = &.{};
                continue;
            }
            // Up front, `upfront` is the one plan for every file: the wave target counted all of them, and each
            // span stays inside its file. A windowed file is planned on its own under the window cap, one row group
            // to a buffer. That charge is what admission adds. Nothing merges again at dispatch.
            var compact: std.ArrayList(CompactRange) = .empty;
            var compact_len: usize = 0;
            const page = std.heap.pageSize();
            // Per row group, `compact.items[rg_compact[rg]..rg_compact[rg + 1]]` (windowed only).
            const rg_compact = try arena.alloc(usize, meta.row_groups.items.len + 1);
            var window_plan: []const coalescer.PlannedSpan = &.{};
            if (windowed) {
                var pieces: std.ArrayList(coalescer.Piece) = .empty;
                var g: usize = 0;
                for (sv.ranges, 0..) |r, ri| {
                    while (g + 1 < sv.rg_first.len and sv.rg_first[g + 1] <= ri) g += 1;
                    try pieces.append(arena, .{
                        .object = 0,
                        .group = @intCast(g),
                        .start = r.start,
                        .end = r.end,
                        .gap_cap = sv.gap_cap,
                    });
                }
                window_plan = try coalescer.coalesceDispatch(
                    arena,
                    pieces.items,
                    s3.inFlight(ctx.env),
                    sv.selected *| 2,
                    fetch_window.limit,
                );
                n_ranges += sv.ranges.len;
                n_merged += window_plan.len;
            } else {
                n_ranges += sv.ranges.len;
                n_merged += file_spans.len;
                for (file_spans) |s| {
                    if (s.end < s.start or s.end > sp.total_size) return error.BadResponse;
                    try compact.append(arena, .{ .start = s.start, .end = s.end, .dst_start = compact_len });
                    compact_len += @intCast(s.len());
                }
            }
            if (windowed) {
                const nrg = meta.row_groups.items.len;
                const bucket_n = try arena.alloc(usize, nrg);
                @memset(bucket_n, 0);
                for (window_plan) |s| bucket_n[s.group] += 1;
                const start_ix = try arena.alloc(usize, nrg);
                var acc: usize = 0;
                for (bucket_n, start_ix) |bn, *slot| {
                    slot.* = acc;
                    acc += bn;
                }
                const cursor = try arena.alloc(usize, nrg);
                @memcpy(cursor, start_ix);
                const order = try arena.alloc(usize, window_plan.len);
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
                        try compact.append(arena, .{ .start = s.start, .end = s.end, .dst_start = compact_len });
                        compact_len += @intCast(s.len());
                    }
                }
            }
            sp.compact_ranges = compact.items;
            if (windowed) {
                const buf = try s3_window.mapBuffer(compact_len);
                errdefer s3_window.unmapBuffer(buf);
                try s3_mapped.append(arena, buf);
                sp.file_buf = buf[0..compact_len];
            } else sp.file_buf = try ctx.gpa.alloc(u8, compact_len);
            try rebaseFetchedOffsets(
                &sp.meta,
                plan.columns(),
                sp.compact_ranges,
                sp.rg_columns,
                if (windowed) rg_compact else null,
            );

            const file_jobs_from = fetch_jobs.items.len;
            for (0..meta.row_groups.items.len) |ri| {
                if (!windowed and ri > 0) break;
                const span = if (windowed) sp.compact_ranges[rg_compact[ri]..rg_compact[ri + 1]] else sp.compact_ranges;
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
                        const list = if (windowed) &rg_jobs else &fetch_jobs;
                        const before = list.items.len;
                        try appendSplitFetch(list, arena, sp.url.bucket, sp.url.key, r.start, fetch_end, dst[0..@intCast(fetch_end - r.start)]);
                        for (list.items[before..]) |j| job_bytes += j.target.len;
                    }

                    if (r.end > sp.tail_start) {
                        const tail_copy_start = @max(r.start, sp.tail_start);
                        const dst_off: usize = @intCast(tail_copy_start - r.start);
                        const tail_off: usize = @intCast(tail_copy_start - sp.tail_start);
                        const copy_len: usize = @intCast(r.end - tail_copy_start);
                        if (tail_off + copy_len > sp.tail.len) return error.BadResponse;
                        const src = sp.tail[tail_off .. tail_off + copy_len];
                        if (windowed) {
                            try copies.append(arena, .{ .dst = dst[dst_off .. dst_off + copy_len], .src = src });
                            copy_bytes += copy_len;
                        } else @memcpy(dst[dst_off .. dst_off + copy_len], src);
                    }
                }
                if (copies.items.len > 0) keep_tail = true;
                if (windowed) try orderFilterFirst(arena, rg_jobs.items, filter_ranges.items);
                const rg_bytes = job_bytes + copy_bytes;
                if (!windowed or rg_bytes == 0) continue;
                // The row group's pages: from its page-aligned start to the next row group's.
                const seg_start = span[0].dst_start;
                const last = span[span.len - 1];
                const seg_end = @min(
                    std.mem.alignForward(usize, last.dst_start + @as(usize, @intCast(last.end - last.start)), page),
                    s3_mapped.items[s3_mapped.items.len - 1].len,
                );
                const wrg = try arena.create(s3_window.RowGroup);
                wrg.* = .{
                    .jobs = rg_jobs.items,
                    .copied = copies.items,
                    .bytes = rg_bytes,
                    .gap = coalescer.s3CoalesceGap(sp.rtt_ns, s3.S3_BYTES_PER_S),
                    .seg = @alignCast(s3_mapped.items[s3_mapped.items.len - 1][seg_start..seg_end]),
                };
                wf.rgs[ri] = wrg;
                try win_order.append(arena, wrg);
            }
            if (keep_tail) {
                const owned: []u8 = @constCast(sp.tail);
                s3_buffers.append(arena, owned) catch |err| {
                    keep_tail = false;
                    return err;
                };
                sp.tail = &.{};
            }
            if (!windowed) try orderFilterFirst(arena, fetch_jobs.items[file_jobs_from..], filter_ranges.items);
        }
        if (windowed) {
            const f = try arena.create(S3Fetcher);
            f.* = .{
                .window = .{
                    .gpa = ctx.gpa,
                    .order = win_order.items,
                    .limit = fetch_window.limit,
                    .batch = @max(1, fetch_window.limit / 4),
                },
                .pool = pool,
                .creds = creds,
                .workers = s3.inFlight(ctx.env),
                .profiler = args.profiler,
                .executor = readExecutor,
                .wave = .{ .cap = .init(s3.firstWaveCap(ctx.env)), .full = s3.inFlight(ctx.env) },
            };
            s3_fetcher = f;
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
            const fetch_span = profile.Span.begin(prof);
            // This call is the query's data wave. Footer and Bloom fetches do not share it.
            var data_wave: s3.DataWave = .{ .cap = .init(s3.firstWaveCap(ctx.env)), .full = s3.inFlight(ctx.env) };
            const fetched = try s3.fetchJobs(
                read_ctx.io,
                pool,
                ctx.gpa,
                arena,
                creds,
                fetch_jobs.items,
                s3.inFlight(ctx.env),
                args.profiler,
                &data_wave,
            );
            fetch_span.end(.fetch, profile.args(.{ fetch_jobs.items.len, fetched }));
            for (fetch_jobs.items) |j| try ensureHeadMagic(j.range.start, j.target);
        }

        // Build scan.Inputs. bytes is the compact per-file buffer with
        // all needed chunks materialized and metadata already rebased.
        for (s3_indices.items, 0..) |orig_idx, k| {
            inputs[orig_idx] = .{
                .name = paths[orig_idx],
                .bytes = specs[k].file_buf,
                .logical_size = specs[k].total_size,
                .gate = if (s3_fetcher != null) win_files[k].gate() else null,
            };
            // Ownership of these gpa buffers moves into `s3_buffers`. Clear the spec so the error path, which frees
            // both lists, cannot free one twice.
            if (@import("builtin").is_test) if (test_arm_s3_buffer_fail) |fa| {
                if (s3_buffers.items.len > 0 and s3_buffers.items.len == s3_buffers.capacity)
                    fa.fail_index = fa.alloc_index;
            };
            if (specs[k].file_buf.len > 0 and s3_fetcher == null) {
                try s3_buffers.append(arena, specs[k].file_buf);
                specs[k].file_buf = &.{};
            }
            if (specs[k].footer_region.len > 0) {
                try s3_buffers.append(arena, specs[k].footer_region);
                specs[k].footer_region = &.{};
            }
            metas[orig_idx] = specs[k].meta;
            meta_ready[orig_idx] = true;
        }
        specs_returned = true;
        if (s3_fetcher) |f| try f.start();
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
        .s3_fetcher = s3_fetcher,
        .s3_mapped = try s3_mapped.toOwnedSlice(arena),
        .footer_resident_peak = footer_resident_peak,
    };
}

fn hasGatedInputs(inputs: []const scan.Input) bool {
    for (inputs) |in| if (in.gate != null) return true;
    return false;
}

fn metadataGate() scan.RowGroupGate {
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
                planned +|= @intCast(cm.total_compressed_size);
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
fn planLocalReader(
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
    }
    // Started by the scan once it has planned (`scan.ReadStart`).
    return inputs;
}

/// `scan.ReadStart` for the local reader.
const ReadStarter = struct {
    rd: *local_reader.Reader,
    started: bool = false,

    fn start(ctx: *anyopaque) anyerror!void {
        const self: *ReadStarter = @ptrCast(@alignCast(ctx));
        if (self.started) return;
        self.started = true;
        try self.rd.start();
    }
};

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

    fn isFilterCol(self: *const RangePlan, ci: usize) bool {
        const mask = switch (self.*) {
            .aggregate => |*p| p.filter_cols,
            .projection => |p| p.filter_cols,
        };
        return ci < mask.len and mask[ci];
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
fn appendSplitFetch(
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
fn orderFilterFirst(arena: std.mem.Allocator, jobs: []s3.FetchJob, filters: []const coalescer.Range) !void {
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
    /// The query's fetch window. Bloom gaps are charged against it.
    window: u64 = 0,
    /// Requests kept in flight (`s3.inFlight`).
    in_flight: usize = s3.S3_IN_FLIGHT,

    /// Until measured: a GET's first byte after ~25 ms at ~80 MB/s a connection, as on Lambda against S3 in-region.
    const default_rtt_ns: u64 = 25 * std.time.ns_per_ms;
    const default_bytes_per_s: f64 = 80e6;

    fn init(
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

    fn source(self: *S3Membership) membership.Source {
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

fn releaseChargedTail(sp: *FetchSpec, gpa: std.mem.Allocator, account: *ByteAccount) void {
    if (sp.tail.len > 0 and sp.tail.ptr != sp.footer_region.ptr) account.sub(sp.tail.len);
    sp.releaseTail(gpa);
}

/// Drop a retained suffix down to the span planned ranges still overlap. No overlap frees it: a count over many
/// files keeps the footer, not a probe per file.
fn trimRetainedSuffix(gpa: std.mem.Allocator, sp: *FetchSpec, plan: RangePlan, account: *ByteAccount) void {
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
fn appendRgFetchRanges(
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
fn ampCap(selected: u64, budget: u64) u64 {
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

fn fetchMetaBatch(
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
            const cond = try s3.getViaPoolWithOpts(t.ctx.io, t.pool, scratch, t.creds, sp.url, .{
                .range = s3.Range.suffix(t.probe),
                .if_none_match = entry.etag,
                .prof = t.prof,
            });
            t.account.add(cond.body.len);
            defer t.account.sub(cond.body.len);
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

    // Cold path: one suffix. A footer that does not fit costs one more GET, inside populateFromTailResponse.
    const tail_resp = try s3.getViaPoolWithOpts(t.ctx.io, t.pool, scratch, t.creds, sp.url, .{
        .range = s3.Range.suffix(t.probe),
        .prof = t.prof,
    });
    t.account.add(tail_resp.body.len);
    defer t.account.sub(tail_resp.body.len);
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
    if (!std.mem.eql(u8, tail[tail.len - 4 ..], "PAR1")) return error.NotParquet;
    const footer_len: u64 = std.mem.readInt(u32, tail[tail.len - 8 ..][0..4], .little);
    if (footer_len + 8 > sp.total_size) return error.NotParquet;
    const footer_actual_start = sp.total_size - 8 - footer_len;
    // The suffix included offset 0, so the leading magic is in hand.
    if (sp.tail_start == 0) try ensureHeadMagic(0, tail);

    const footer_region_len: usize = @intCast(footer_len + 8);
    const footer_region = try t.ctx.gpa.alloc(u8, footer_region_len);
    sp.footer_region = footer_region;
    sp.footer_offset = footer_actual_start;

    // One more GET, and only this one, when the footer did not fit in the suffix.
    if (footer_actual_start < sp.tail_start) {
        const need = try s3.getViaPoolWithOpts(t.ctx.io, t.pool, scratch, t.creds, sp.url, .{
            .range = s3.Range.span(footer_actual_start, sp.tail_start - 1),
            .prof = t.prof,
        });
        t.account.add(need.body.len);
        defer t.account.sub(need.body.len);
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
fn ensureHeadMagic(start: u64, bytes: []const u8) !void {
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

/// Offset given to a chunk deliberately left unfetched. Far past any compact buffer, so a consumer that reads the chunk
/// anyway fails its bounds check (MissingChunkBytes) instead of decoding whatever bytes a stale offset lands on.
const UNFETCHED_CHUNK_OFFSET: i64 = std.math.maxInt(i64) / 4;

/// Point `meta`'s chunk offsets into the compact buffer `ranges` describe. `fetched` is every column some row group
/// fetched (`RangePlan.columns`) and `rg_columns` what each row group fetched (null where statistics pruned it).
/// `rg_compact`, when the buffer is windowed, gives each row group its own ranges,
/// `ranges[rg_compact[rg]..rg_compact[rg + 1]]`: its offsets rebase only onto those. Another row group's range may
/// cover the same source bytes (a range coalesced across a gap to a carried Bloom filter), but its segment is released
/// when that row group is done, not when this one is.
fn rebaseFetchedOffsets(
    meta: *schema.FileMetaData,
    fetched: []const bool,
    all_ranges: []const CompactRange,
    rg_columns: []const ?[]const bool,
    rg_compact: ?[]const usize,
) !void {
    std.debug.assert(rg_columns.len == meta.row_groups.items.len);
    if (rg_compact) |rc| std.debug.assert(rc.len == meta.row_groups.items.len + 1);
    // Page-index blocks are never fetched, so their offsets have nothing to rebase onto — yet consumers bounds-check
    // them against the compact buffer, not the file. In a noncanonical file a stale offset can land inside that buffer,
    // where unrelated bytes may decode as a ColumnIndex whose foreign min/max prune pages on read and get copied into
    // output on write. Pruned row groups are cleared too: the metadata no longer describes the source layout.
    for (meta.row_groups.items, 0..) |*rg, ri| {
        const ranges = if (rg_compact) |rc| all_ranges[rc[ri]..rc[ri + 1]] else all_ranges;
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

    for (meta.row_groups.items, rg_columns, 0..) |*rg, rg_cols_opt, ri| {
        const ranges = if (rg_compact) |rc| all_ranges[rc[ri]..rc[ri + 1]] else all_ranges;
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
/// Rightmost range with `start <= src`, when `ranges` is sorted by start and does not overlap.
fn rangeAt(src: u64, ranges: []const CompactRange) ?CompactRange {
    var lo: usize = 0;
    var hi = ranges.len;
    while (lo < hi) {
        const mid = lo + (hi - lo) / 2;
        if (ranges[mid].start <= src) lo = mid + 1 else hi = mid;
    }
    if (lo == 0) return null;
    return ranges[lo - 1];
}

fn rebaseContained(off_opt: ?i64, len_opt: ?i32, ranges: []const CompactRange) ?i64 {
    const off = std.math.cast(u64, off_opt orelse return null) orelse return null;
    const len = std.math.cast(u64, len_opt orelse return null) orelse return null;
    const r = rangeAt(off, ranges) orelse return null;
    if (off >= r.start and off + len <= r.end) return @intCast(@as(u64, @intCast(r.dst_start)) + (off - r.start));
    return null;
}

fn rebaseOneOffset(old: i64, ranges: []const CompactRange) !i64 {
    if (old < 0) return error.BadResponse;
    const src: u64 = @intCast(old);
    const r = rangeAt(src, ranges) orelse return error.BadResponse;
    if (src >= r.start and src < r.end) {
        const dst: u64 = @as(u64, @intCast(r.dst_start)) + (src - r.start);
        return @intCast(dst);
    }
    return error.BadResponse;
}

/// Where a footer suffix of `body_len` bytes starts in an object of `total` bytes. The total is the server's word
/// (Content-Range); one smaller than the suffix it came with is a bad response.
fn suffixStart(total: u64, body_len: usize) error{BadResponse}!u64 {
    if (body_len > total) return error.BadResponse;
    return total - body_len;
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
    output_fd: ?local_fs.fd_t,
    empty: bool,
) !u64 {
    const fd = output_fd orelse try createFile(out_path);
    var fd_open = output_fd == null;
    defer if (fd_open) local_fs.close(fd);
    errdefer if (output_fd == null) local_fs.removeFile(out_path);
    var fd_sink: FdSink = .{ .fd = fd };
    const sink: streaming.Sink = .{ .ctx = @ptrCast(&fd_sink), .write_fn = FdSink.writeFn };

    var all_outs: std.ArrayList(expr_agg.OutputCol) = .empty;
    for (agg_calls, 0..) |call, i| {
        const cols = try expr_agg.finalize(arena, call, accumulators[i]);
        for (cols) |c| try all_outs.append(arena, if (sources[i]) |src| try retypeOutputCol(arena, c, src) else c);
    }
    if (empty) for (all_outs.items) |*out| switch (out.col) {
        inline else => |*col| {
            col.values = col.values[0..0];
            if (col.def_levels) |levels| col.def_levels = levels[0..0];
        },
    };
    const written = try consumer.writeAggregateRows(arena, sink, all_outs.items, if (empty) 0 else 1, codec, zstd);
    if (fd_open) {
        fd_open = false;
        try local_fs.closeChecked(fd);
    }
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
    output_fd: ?local_fs.fd_t,
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
        .dec => {
            // An INT96 source reads as exact nanoseconds on this lane and is written back as a timestamp.
            const col = try groupedDecColumn(arena, name, rows, ci, ty.dec.?);
            try outs.append(arena, if (ty.source) |src| try retypeOutputCol(arena, col, src) else col);
        },
        .avg_dec => {
            const sum_name = try std.fmt.allocPrint(arena, "{s}__sum", .{name});
            const count_name = try std.fmt.allocPrint(arena, "{s}__count", .{name});
            try outs.append(arena, try groupedDecColumn(arena, sum_name, rows, ci, ty.dec.?.sum()));
            try outs.append(arena, try groupedColumn(i64, arena, count_name, false, rows, ci, .INT64, false));
        },
    };

    const fd = output_fd orelse try createFile(out_path);
    var fd_open = output_fd == null;
    defer if (fd_open) local_fs.close(fd);
    var fd_sink: FdSink = .{ .fd = fd };
    const sink: streaming.Sink = .{ .ctx = @ptrCast(&fd_sink), .write_fn = FdSink.writeFn };
    const written = try consumer.writeAggregateRows(arena, sink, outs.items, @intCast(rows.len), codec, zstd);
    if (fd_open) {
        fd_open = false;
        try local_fs.closeChecked(fd);
    }
    return written;
}

/// `writeGroupedParquet` for an answer built as typed columns (`scan.TypedAnswer`): the same columns, names, types and
/// file, with no row of values in between.
fn writeTypedParquet(
    arena: std.mem.Allocator,
    out_path: []const u8,
    cols: []const []const u8,
    types: []const scan.GroupColType,
    answer: scan.TypedAnswer,
    codec: schema.CompressionCodec,
    zstd: compression.ZstdPlan,
    output_fd: ?local_fs.fd_t,
) !u64 {
    var outs: std.ArrayList(expr_agg.OutputCol) = .empty;
    for (cols, types, answer.columns) |name, ty, col| switch (col) {
        inline .i64, .f64, .string => |l, tag| {
            const phys: schema.Type = switch (tag) {
                .i64 => .INT64,
                .f64 => .DOUBLE,
                .string => .BYTE_ARRAY,
                else => unreachable,
            };
            const out = typedColumn(@FieldType(@TypeOf(l), "values"), name, ty.nullable, l, phys);
            try outs.append(arena, if (ty.source) |src| try retypeOutputCol(arena, out, src) else out);
        },
        .avg => |a| {
            try outs.append(arena, typedColumn([]f64, try std.fmt.allocPrint(arena, "{s}__sum", .{name}), true, a.sum, .DOUBLE));
            try outs.append(arena, countColumn(try std.fmt.allocPrint(arena, "{s}__count", .{name}), a.count));
        },
        .dec => |l| {
            const out = decColumn(name, l, ty.dec.?);
            try outs.append(arena, if (ty.source) |src| try retypeOutputCol(arena, out, src) else out);
        },
        .avg_dec => |a| {
            try outs.append(arena, decColumn(try std.fmt.allocPrint(arena, "{s}__sum", .{name}), a.sum, ty.dec.?.sum()));
            try outs.append(arena, countColumn(try std.fmt.allocPrint(arena, "{s}__count", .{name}), a.count));
        },
    };
    const fd = output_fd orelse try createFile(out_path);
    var fd_open = output_fd == null;
    defer if (fd_open) local_fs.close(fd);
    var fd_sink: FdSink = .{ .fd = fd };
    const sink: streaming.Sink = .{ .ctx = @ptrCast(&fd_sink), .write_fn = FdSink.writeFn };
    const written = try consumer.writeAggregateRows(arena, sink, outs.items, @intCast(answer.rows), codec, zstd);
    if (fd_open) {
        fd_open = false;
        try local_fs.closeChecked(fd);
    }
    return written;
}

fn typedColumn(comptime S: type, name: []const u8, nullable: bool, l: anytype, parquet_type: schema.Type) expr_agg.OutputCol {
    const T = @typeInfo(S).pointer.child;
    const col: filter_eval.ColumnT(T) = .{
        .values = l.values,
        .def_levels = if (l.nulls) l.defs else null,
        .max_def = @intFromBool(nullable),
        .has_nulls = l.nulls,
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

fn countColumn(name: []const u8, counts: []const i64) expr_agg.OutputCol {
    return .{ .name = name, .col = .{ .i64 = .{ .values = counts, .max_def = 0 } }, .parquet_type = .INT64, .nullable = false };
}

fn decColumn(name: []const u8, l: scan.TypedAnswer.Lane(i128), spec: expr_ast.Dec) expr_agg.OutputCol {
    return .{
        .name = name,
        .col = .{ .i128 = .{
            .values = l.values,
            .def_levels = if (l.nulls) l.defs else null,
            .max_def = 1,
            .has_nulls = l.nulls,
        } },
        .parquet_type = spec.physical(),
        .nullable = true,
        .dec = spec,
    };
}

/// Give an aggregate output column the parquet type of the source column its values pass through from unchanged (a
/// bare GROUP BY key, min/max of a column), rather than its evaluation lane's plain INT64 / DOUBLE / BYTE_ARRAY: a
/// DATE comes back a DATE, a string a STRING, an INT8 an INT8. DECIMAL and FLOAT16 stay DOUBLE, the f64 values the
/// lane holds; INT96 becomes INT64 TIMESTAMP(NANOS), the value zpq decodes it to; fixed-length bytes stay plain
/// BYTE_ARRAY.
fn retypeOutputCol(arena: std.mem.Allocator, out: expr_agg.OutputCol, src: schema.SchemaElement) !expr_agg.OutputCol {
    if (src.type == .INT96) if (out.col == .i128) return int96OutputCol(arena, out);
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
/// An INT96 key or min/max, read as exact nanoseconds, written as INT64 TIMESTAMP(NANOS) as before; one past what
/// 64 bits of nanoseconds hold has no such column, and is refused rather than wrapped.
fn int96OutputCol(arena: std.mem.Allocator, out: expr_agg.OutputCol) !expr_agg.OutputCol {
    const c = out.col.i128;
    const vals = try arena.alloc(i64, c.values.len);
    for (c.values, vals, 0..) |v, *o, i| {
        const present = if (c.def_levels) |dl| dl[i] >= c.max_def else true;
        o.* = if (present) std.math.cast(i64, v) orelse return error.Int96OutOfRange else 0;
    }
    var res = out;
    res.col = .{ .i64 = sameNulls(i64, c, vals) };
    res.parquet_type = .INT64;
    res.dec = null;
    res.logical_type = .{ .TIMESTAMP = .{ .isAdjustedToUTC = false, .unit = .{ .NANOS = .{} } } };
    res.converted_type = null;
    return res;
}

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
            .avg_dec => |p| if (T == i64) p.count else return error.TypeMismatch,
            .dec => return error.TypeMismatch,
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

/// Column `ci` of every grouped row as an exact DECIMAL(`spec`) column: a `dec` value, or an avg's exact sum.
fn groupedDecColumn(
    arena: std.mem.Allocator,
    name: []const u8,
    rows: []const []const scan.AggValue,
    ci: usize,
    spec: expr_ast.Dec,
) !expr_agg.OutputCol {
    const values = try arena.alloc(i128, rows.len);
    const defs = try arena.alloc(u32, rows.len);
    var has_nulls = false;
    for (rows, values, defs) |row, *v, *d| {
        const cell: ?i128 = switch (row[ci]) {
            .null_val => null,
            .dec => |x| x.v,
            // The `__sum` column is DECIMAL(38, s): a wider exact sum cannot be stored, and is not rounded to fit.
            .avg_dec => |p| blk: {
                const x = p.sum orelse break :blk null;
                if (@abs(x) >= @as(u192, @intCast(spec.bound()))) return error.DecimalOverflow;
                break :blk @as(i128, @intCast(x));
            },
            else => return error.TypeMismatch,
        };
        v.* = cell orelse 0;
        d.* = @intFromBool(cell != null);
        if (cell == null) has_nulls = true;
    }
    return .{
        .name = name,
        .col = .{ .i128 = .{
            .values = values,
            .def_levels = if (has_nulls) defs else null,
            .max_def = 1,
            .has_nulls = has_nulls,
        } },
        .parquet_type = spec.physical(),
        .nullable = true,
        .dec = spec,
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

pub const PrintFormat = enum { csv, jsonl, json, parquet };

/// Shared text serialization keeps SQL ABI responses byte-identical to CLI rows.
pub fn writeAggregateText(writer: anytype, answer: scan.AggOutput, rows: usize, format: PrintFormat) !void {
    if (format == .json) try writer.writeByte('[');
    if (answer.group_rows) |groups| {
        for (groups, 0..) |values, ri| {
            if (format == .json and ri > 0) try writer.writeByte(',');
            try writer.writeByte('{');
            for (answer.group_cols.?, values, 0..) |name, value, ci| {
                if (ci > 0) try writer.writeByte(',');
                try writer.writeByte('"');
                try writeJsonString(writer, name);
                try writer.writeAll("\":");
                try writeAggValueJson(writer, value);
            }
            try writer.writeAll(if (format == .json) "}" else "}\n");
        }
    } else if (rows > 0) {
        try writer.writeByte('{');
        for (answer.aggs, 0..) |item, ci| {
            if (ci > 0) try writer.writeByte(',');
            try writer.writeByte('"');
            try writeJsonString(writer, item.alias);
            try writer.writeAll("\":");
            try writeAggValueJson(writer, item.value);
        }
        try writer.writeAll(if (format == .json) "}" else "}\n");
    }
    if (format == .json) try writer.writeAll("]\n");
}

pub const StdoutWriter = struct {
    fd: local_fs.fd_t = 1,
    sink: ?*std.Io.Writer = null,
    buf: [4096]u8 = undefined,
    pos: usize = 0,
    output_ns: u64 = 0,
    bytes_written: u64 = 0,
    json_array: bool = false,
    skip_remaining: usize = 0,
    parquet: ?*ParquetPrint = null,

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
        if (self.sink) |sink| try sink.writeAll(pending) else try local_fs.writeAll(self.fd, pending);
        self.bytes_written += pending.len;
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

/// `unscaled` (any signed integer width, an avg's 192-bit sum included) as decimal text with `scale` digits after the
/// point, every one printed: `-0.05`, `300.00`.
fn writeDecimal(writer: anytype, unscaled: anytype, scale: i32) !void {
    if (scale <= 0) {
        try writer.print("{d}", .{unscaled});
        return;
    }
    var buf: [64]u8 = undefined;
    const digits = try std.fmt.bufPrint(&buf, "{d}", .{@abs(unscaled)});
    const s: usize = @intCast(scale);
    if (unscaled < 0) try writer.writeByte('-');
    if (digits.len > s) {
        try writer.print("{s}.{s}", .{ digits[0 .. digits.len - s], digits[digits.len - s ..] });
    } else {
        try writer.writeAll("0.");
        for (0..s - digits.len) |_| try writer.writeByte('0');
        try writer.writeAll(digits);
    }
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

/// The same single-object JSON convention as query results. Passes describe permitted work, not a runtime trace.
pub fn writeAggregateExplanationJson(writer: anytype, explanation: AggregateExplanation) !void {
    const a = explanation.aggregate;
    const d = a.plan.dispatch;
    const merge = if (d.strategy == .partitioned_hash) "shared_partitions" else if (d.use_blocks) "ordered_blocks" else if (d.strategy == .scalar) "scalar" else "partitions";
    try writer.print("{{\"ok\":true,\"explain\":true,\"strategy\":\"{s}\",\"merge\":\"{s}\",\"io\":\"{s}\",\"workers\":{d},\"tables\":{d},\"work_items\":{d},\"max_memory\":{d},\"encoded\":{},\"reasons\":[", .{
        @tagName(d.strategy), merge,                  @tagName(explanation.io_mode), a.workers, d.tables,
        a.work_items,         explanation.max_memory, explanation.encoded,
    });
    var sep: []const u8 = "";
    var reasons = a.plan.reasons.iterator();
    while (reasons.next()) |reason| {
        try writer.print("{s}\"{s}\"", .{ sep, @tagName(reason) });
        sep = ",";
    }
    try writer.writeAll("],\"ndv\":");
    if (a.plan.ndv_stat) |stat| {
        try writer.print("{{\"source\":\"{s}\",\"lo\":{d},\"hi\":{d},\"rows\":{d},\"covered_rows\":{d},\"encoding_stats_rows\":{d},\"confidence\":\"{s}\",\"dictionary_bytes\":{d},\"column_bytes\":{d},\"key_bytes_estimate\":{d},\"union_attempted\":{},\"union_complete\":{}", .{
            @tagName(stat.source),
            stat.lo,
            stat.hi,
            stat.rows,
            stat.covered_rows,
            stat.encoding_stats_rows,
            @tagName(stat.confidence),
            stat.dictionary_bytes,
            stat.column_bytes,
            stat.key_bytes,
            stat.union_attempted,
            stat.union_complete,
        });
        // Separate a cost estimate from a proven upper bound, especially for composite keys.
        try writer.writeAll(",\"estimate\":");
        if (stat.estimate) |n| try writer.print("{d}", .{n}) else try writer.writeAll("null");
        try writer.writeAll("}");
    } else try writer.writeAll("null");
    try writer.writeAll(",\"plain_bytes_estimate\":");
    if (a.plan.plain_bytes) |n| try writer.print("{d}", .{n}) else try writer.writeAll("null");
    try writer.print(",\"shared_tables\":{d},\"count_agg\":", .{
        @as(usize, if (d.strategy == .partitioned_hash) 32 else 0),
    });
    if (d.count_agg) |idx| try writer.print("{d}", .{idx}) else try writer.writeAll("null");
    try writer.writeAll(",\"sketch\":");
    if (explanation.sketch) |cfg| {
        try writer.print("{{\"cell_bits\":{d},\"admit_per_row\":{d},\"certify\":{},\"census_allowed\":{},\"census_fused\":{},\"census_cell_repeat_gate\":{},\"census_fp_bits\":{d},\"census_check_rows\":{d}}}", .{
            cfg.cell_bits,             cfg.admit_per_row,  cfg.certify,           d.census_allowed, d.census_fused,
            d.census_cell_repeat_gate, cfg.census_fp_bits, cfg.census_check_rows,
        });
    } else try writer.writeAll("null");
    try writer.writeAll(",\"passes\":[");
    switch (d.strategy) {
        .scalar, .plain_hash, .partitioned_hash => try writeExplainPass(writer, "aggregate", null, a.column_names, a.columns),
        .first_keys => {
            try writeExplainPass(writer, "discover_prefix", null, a.column_names, a.columns);
            try writer.writeAll(",");
            try writeExplainPass(writer, "admit", "discovery_did_not_read_all", a.column_names, a.columns);
        },
        .sketch => if (d.distinct_census) {
            // The census rides the count pass; its admit pass reads the keys it admits, unless it gives up first.
            try writeExplainPass(writer, "census_count", null, a.column_names, a.count_columns.?);
            try writer.writeAll(",");
            try writeExplainPass(writer, "admit", "census_admits_few_rows", a.column_names, a.columns);
            try writer.writeAll(",");
            try writeExplainPass(
                writer,
                "aggregate",
                "census_abandoned_or_certificate_failed",
                a.column_names,
                a.columns,
            );
        } else {
            try writeExplainPass(writer, "count", null, a.column_names, a.count_columns.?);
            if (d.census_allowed) {
                try writer.writeAll(",");
                try writeExplainPass(writer, "census_count", if (d.census_fused)
                    "sketch_bails_and_fused_census_dropped"
                else if (d.census_cell_repeat_gate)
                    "sketch_bails_and_cells_favor_census"
                else
                    "sketch_bails", a.column_names, a.count_columns.?);
            }
            try writer.writeAll(",");
            try writeExplainPass(writer, "admit", "sketch_or_census_cut_viable", a.column_names, a.columns);
            try writer.writeAll(",");
            try writeExplainPass(
                writer,
                "aggregate",
                "cut_not_viable_or_certificate_failed",
                a.column_names,
                a.columns,
            );
        },
    }
    // The stage every strategy feeds (`ResultProgram`): what it keeps, ranks and drops after the groups are complete.
    const r = explanation.result;
    try writer.print("],\"result_program\":{{\"having\":{},\"hidden_columns\":{d},\"order_keys\":{d},\"cut_limit\":\"{s}\"}}", .{
        r.having, r.hidden, r.order_keys, @tagName(r.cut_limit),
    });
    try writer.writeAll(",\"runtime\":{\"batch_encoding\":true,\"slicing\":true,\"bloom_probing\":\"execution_only\"}}\n");
}

fn writeExplainPass(
    writer: anytype,
    kind: []const u8,
    condition: ?[]const u8,
    names: []const []const u8,
    columns: agg_plan.Columns,
) !void {
    try writer.print("{{\"kind\":\"{s}\",\"condition\":", .{kind});
    if (condition) |c| try writeJsonQuoted(writer, c) else try writer.writeAll("null");
    try writer.writeAll(",\"columns\":");
    try writeExplainColumns(writer, names, columns.read);
    try writer.writeAll(",\"full_match_columns\":");
    try writeExplainColumns(writer, names, columns.consumed);
    try writer.writeAll("}");
}

fn writeExplainColumns(writer: anytype, names: []const []const u8, mask: []const bool) !void {
    try writer.writeAll("[");
    var sep: []const u8 = "";
    for (names, mask) |name, reads| if (reads) {
        try writer.writeAll(sep);
        try writeJsonQuoted(writer, name);
        sep = ",";
    };
    try writer.writeAll("]");
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
        "\"membership\":{{\"bloom_pruned\":{d},\"dict_pruned\":{d},\"filters\":{d},\"dictionaries\":{d},\"requests\":{d},\"bytes\":{d},\"suffix_bytes\":{d},\"bloom_stopped\":{},\"dict_stopped\":{},\"bloom_skipped_oversize\":{d},\"request_kb\":{d},\"us\":{d}}}",
        .{
            s.row_groups_bloom, s.row_groups_dict,        s.filters,              s.dictionaries,
            s.requests,         s.bytes,                  s.suffix_bytes,         s.bloom_stopped,
            s.dict_stopped,     s.bloom_skipped_oversize, s.request_bytes / 1024, s.ns / std.time.ns_per_us,
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
        // Exact text: a JSON number with every digit of the scale, so a reader parsing numbers as decimals gets the
        // value bit for bit (one parsing them as doubles gets the nearest double, as with any JSON number).
        .dec => |d| try writeDecimal(writer, d.v, d.spec.scale),
        .avg_dec => |p| {
            try writer.writeAll("{\"sum\":");
            if (p.sum) |sum| try writeDecimal(writer, sum, p.spec.scale) else try writer.writeAll("null");
            try writer.print(",\"count\":{d},\"avg\":", .{p.count});
            if (p.mean) |m| try writeDecimal(writer, m, p.spec.scale) else try writer.writeAll("null");
            try writer.writeByte('}');
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
    // The scale as resolved: a leaf may carry only the logical type, whose scale the legacy field then lacks.
    const scale = if (decimal_mod.kindFromSchema(&elem)) |k| k.scale else 0;
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
        .i128 => |tb| {
            if (tb.max_def > 0 and tb.def_levels.items[row_idx] < tb.max_def) {
                try writer.writeAll(if (is_jsonl) "null" else "");
            } else {
                try writeDecimal(writer, tb.values.items[row_idx], scale);
            }
        },
    }
}

/// A plain-row ORDER BY key bound to its input column (`expr_ast.RowKey`).
const RowSortKey = struct {
    /// The key as an output column of the row: a passthrough leaf, or an expression computed like a written column.
    spec: consumer.OutputCol,
    descending: bool,
    /// An unsigned integer column: its lane holds raw bits, compared as unsigned.
    unsigned: bool,
};

/// One sort key value of a row. NULL sorts last in either direction.
const SortCell = union(enum) {
    null,
    int: i128,
    float: f64,
    str: []const u8,

    fn order(a: SortCell, b: SortCell) std.math.Order {
        if (a == .null or b == .null) return if (a == .null and b == .null) .eq else if (a == .null) .gt else .lt;
        return switch (a) {
            .int => |x| std.math.order(x, b.int),
            .float => |x| floatOrder(x, b.float),
            .str => |x| std.mem.order(u8, x, b.str),
            .null => unreachable,
        };
    }

    fn floatOrder(a: f64, b: f64) std.math.Order {
        if (std.math.isNan(a) or std.math.isNan(b)) {
            return if (std.math.isNan(a) == std.math.isNan(b)) .eq else if (std.math.isNan(a)) .gt else .lt;
        }
        return std.math.order(a, b);
    }
};

/// Key `k`'s value in row `row` of a buffer holding the key columns, borrowed from it.
fn sortCell(buf: consumer.OutputAggregator.ColumnBuf, row: usize, unsigned: bool) SortCell {
    switch (buf) {
        inline else => |tb, tag| {
            if (tb.def_levels.items.len > 0 and tb.def_levels.items[row] < tb.max_def) return .null;
            const v = tb.values.items[row];
            return switch (tag) {
                .i32 => .{ .int = if (unsigned) @as(u32, @bitCast(v)) else v },
                .i64 => .{ .int = if (unsigned) @as(u64, @bitCast(v)) else v },
                .i128 => .{ .int = v },
                .boolean => .{ .int = @intFromBool(v) },
                .f32, .f64 => .{ .float = v },
                .string => .{ .str = v },
            };
        },
    }
}

/// Where a row stands in the input, for ties: file, row group, and its place among the row group's filtered rows.
const RowPos = struct {
    file: u32,
    rg: u32,
    ord: u32 = 0,

    fn order(a: RowPos, b: RowPos) std.math.Order {
        if (a.file != b.file) return std.math.order(a.file, b.file);
        if (a.rg != b.rg) return std.math.order(a.rg, b.rg);
        return std.math.order(a.ord, b.ord);
    }
};

/// Keeps the first `h` (OFFSET + LIMIT) plain rows by a SQL ORDER BY across row groups, ties by input position, as a
/// stable sort of every row would. A row group's filter and key columns decode first; only a row group holding a row
/// that enters the heap decodes its output columns, and those rows are copied out, owned, before it is released. The
/// heap's top is the last row kept, so a row that sorts before it replaces it; one that ties it comes later in the
/// input and does not.
const TopRows = struct {
    gpa: std.mem.Allocator,
    meta: *const schema.FileMetaData,
    specs: []const consumer.OutputCol,
    keys: []const RowSortKey,
    key_specs: []consumer.OutputCol,
    h: usize,
    /// Bytes the kept rows and their heap may take; past it the query fails cleanly (no spilling).
    budget: usize,
    /// The rows admitted so far, their output columns copied; a heap entry indexes one. Rows evicted stay until
    /// `compact` drops them.
    store_arena: *std.heap.ArenaAllocator,
    store: consumer.OutputAggregator,
    heap: std.PriorityQueue(Entry, *const TopRows, later),

    const pending = std.math.maxInt(usize);

    const Entry = struct {
        cells: []SortCell,
        pos: RowPos,
        /// Its row in `store`, or `pending` until its row group's output columns are copied.
        row: usize,
    };

    fn init(
        gpa: std.mem.Allocator,
        meta: *const schema.FileMetaData,
        specs: []const consumer.OutputCol,
        keys: []const RowSortKey,
        h: usize,
        budget: usize,
    ) !TopRows {
        const key_specs = try gpa.alloc(consumer.OutputCol, keys.len);
        errdefer gpa.free(key_specs);
        for (keys, key_specs) |k, *spec| spec.* = k.spec;
        const store_arena = try gpa.create(std.heap.ArenaAllocator);
        store_arena.* = .init(gpa);
        errdefer {
            store_arena.deinit();
            gpa.destroy(store_arena);
        }
        return .{
            .gpa = gpa,
            .meta = meta,
            .specs = specs,
            .keys = keys,
            .key_specs = key_specs,
            .h = h,
            .budget = budget,
            .store_arena = store_arena,
            .store = try consumer.initOutputAggregator(store_arena.allocator(), meta, specs),
            .heap = .empty,
        };
    }

    fn deinit(self: *TopRows) void {
        self.heap.deinit(self.gpa);
        self.store_arena.deinit();
        self.gpa.destroy(self.store_arena);
        self.gpa.free(self.key_specs);
    }

    /// Whether row (`a_cells`, `a_pos`) sorts before row (`b_cells`, `b_pos`). NULL is last in either direction.
    fn before(keys: []const RowSortKey, a_cells: []const SortCell, a_pos: RowPos, b_cells: []const SortCell, b_pos: RowPos) bool {
        for (keys, a_cells, b_cells) |k, a, b| {
            var ord = a.order(b);
            if (k.descending and a != .null and b != .null) ord = ord.invert();
            if (ord != .eq) return ord == .lt;
        }
        return a_pos.order(b_pos) == .lt;
    }

    fn later(self: *const TopRows, a: Entry, b: Entry) std.math.Order {
        if (before(self.keys, b.cells, b.pos, a.cells, a.pos)) return .lt;
        if (before(self.keys, a.cells, a.pos, b.cells, b.pos)) return .gt;
        return .eq;
    }

    /// Offer one row group's filtered rows.
    fn offer(
        self: *TopRows,
        rg: *const schema.RowGroup,
        meta: *const schema.FileMetaData,
        src: consumer.RGSrc,
        filter: ?filter_ast.Filter,
        fetch: []const bool,
        at: RowPos,
        timings: *consumer.Timings,
        options: consumer.DecodeOptions,
    ) !void {
        if (self.h == 0) return;
        var rg_arena = std.heap.ArenaAllocator.init(self.gpa);
        defer rg_arena.deinit();
        const ra = rg_arena.allocator();
        var keyed = try consumer.initOutputAggregator(ra, meta, self.key_specs);
        const n = try consumer.appendProjectedRGWithOptions(&keyed, self.gpa, rg, meta, src, filter, fetch, self.key_specs, timings, options);
        if (n == 0) return;

        self.heap.context = self;
        const scratch = try ra.alloc(SortCell, self.keys.len);
        var admitted = false;
        for (0..n) |i| {
            for (self.keys, keyed.cols, scratch) |k, buf, *cell| cell.* = sortCell(buf, i, k.unsigned);
            const pos: RowPos = .{ .file = at.file, .rg = at.rg, .ord = @intCast(i) };
            if (self.heap.count() == self.h) {
                const top = self.heap.peek().?;
                if (!before(self.keys, scratch, pos, top.cells, top.pos)) continue;
                _ = self.heap.pop();
            }
            const cells = try self.store_arena.allocator().alloc(SortCell, self.keys.len);
            for (scratch, cells) |c, *d| d.* = try ownCell(self.store_arena.allocator(), c);
            try self.heap.push(self.gpa, .{ .cells = cells, .pos = pos, .row = pending });
            admitted = true;
        }
        if (!admitted) return;

        // The rows that entered and are still kept: copy their output columns, decoded once for the row group.
        var outputs = try consumer.initOutputAggregator(ra, meta, self.specs);
        const m = try consumer.appendProjectedRGWithOptions(&outputs, self.gpa, rg, meta, src, filter, fetch, self.specs, timings, options);
        if (m != n) return error.ShortDecode;
        for (self.heap.items) |*e| if (e.row == pending) {
            try copyRow(&self.store, &outputs, e.pos.ord);
            e.row = self.store.num_rows - 1;
        };
        if (self.store.num_rows > 2 *| self.h +| 1024) try self.compact();
        const held = self.store_arena.queryCapacity() + self.heap.cap * @sizeOf(Entry);
        if (held > self.budget) return error.ExceededMemoryBudget;
    }

    /// Whether a row group whose rows' first key is at best `best` holds no row the heap would keep: the heap is full
    /// and `best` sorts strictly after its last row's first key, so every row of the group sorts after that row
    /// whatever its later keys and position. A tie keeps the group.
    fn cutsOff(self: *const TopRows, best: SortCell) bool {
        if (self.h == 0 or self.heap.count() < self.h) return false;
        return sortsAfter(self.keys[0], best, self.heap.peek().?.cells[0]);
    }

    fn ownCell(arena: std.mem.Allocator, c: SortCell) !SortCell {
        return if (c == .str) .{ .str = try arena.dupe(u8, c.str) } else c;
    }

    /// Rebuild `store` with only the rows the heap still keeps.
    fn compact(self: *TopRows) !void {
        const fresh = try self.gpa.create(std.heap.ArenaAllocator);
        fresh.* = .init(self.gpa);
        errdefer {
            fresh.deinit();
            self.gpa.destroy(fresh);
        }
        const fa = fresh.allocator();
        var store = try consumer.initOutputAggregator(fa, self.meta, self.specs);
        const cells = try fa.alloc([]SortCell, self.heap.items.len);
        for (self.heap.items, cells) |e, *dst| {
            try copyRow(&store, &self.store, e.row);
            dst.* = try fa.alloc(SortCell, e.cells.len);
            for (e.cells, dst.*) |c, *d| d.* = try ownCell(fa, c);
        }
        // Nothing can fail from here: swap the rows in.
        for (self.heap.items, cells, 0..) |*e, c, i| {
            e.cells = c;
            e.row = i;
        }
        self.store_arena.deinit();
        self.gpa.destroy(self.store_arena);
        self.store_arena = fresh;
        self.store = store;
    }

    /// The kept rows in their order, on `arena`.
    fn ranked(self: *TopRows, arena: std.mem.Allocator) !consumer.OutputAggregator {
        const entries = try arena.dupe(Entry, self.heap.items);
        std.mem.sortUnstable(Entry, entries, self.keys, struct {
            fn lt(keys: []const RowSortKey, a: Entry, b: Entry) bool {
                return before(keys, a.cells, a.pos, b.cells, b.pos);
            }
        }.lt);
        var out = try consumer.initOutputAggregator(arena, self.meta, self.specs);
        for (entries) |e| try copyRow(&out, &self.store, e.row);
        return out;
    }
};

/// The best first-key value any row of a row group can have, from the column chunk's statistics: its min ascending, its
/// max descending, NULL when every row is NULL. Null when the statistics prove nothing: a computed key, bounds missing,
/// in an order zpq does not read, or of a type whose cell is not plainly the bound. Floats descending prove nothing
/// either: a NaN sorts first there, and writers leave NaN out of the bounds.
fn rowGroupBest(rg: *const schema.RowGroup, meta: *const schema.FileMetaData, key: RowSortKey) ?SortCell {
    const ci = switch (key.spec) {
        .passthrough => |ci| ci,
        .computed => return null,
    };
    if (ci >= rg.columns.items.len) return null;
    const cm = rg.columns.items[ci].meta_data orelse return null;
    const b = statistics.chunkBounds(rg, ci, meta) orelse {
        const stats = cm.statistics orelse return null;
        const nulls = stats.null_count orelse return null;
        return if (rg.num_rows > 0 and nulls == rg.num_rows) .null else null;
    };
    const bytes = if (key.descending) b.max else b.min;
    const elem = meta.getColumnSchema(cm.path_in_schema.items) orelse return null;
    if (elem.type == null or elem.type.? != cm.type) return null;
    switch (cm.type) {
        .INT32, .INT64 => {
            // DECIMAL, among others, is neither: its cell is not read from the bound here.
            if (!statistics.signedIntOrder(elem) and !schema.isUnsignedInt(elem)) return null;
            if (cm.type == .INT32) {
                if (bytes.len != 4) return null;
                const v = std.mem.readInt(i32, bytes[0..4], .little);
                return .{ .int = if (key.unsigned) @as(u32, @bitCast(v)) else v };
            }
            if (bytes.len != 8) return null;
            const v = std.mem.readInt(i64, bytes[0..8], .little);
            return .{ .int = if (key.unsigned) @as(u64, @bitCast(v)) else v };
        },
        .FLOAT, .DOUBLE => {
            if (key.descending or elem.logical_type != null or elem.converted_type != null) return null;
            const v: f64 = if (cm.type == .FLOAT) blk: {
                if (bytes.len != 4) return null;
                break :blk @as(f32, @bitCast(std.mem.readInt(u32, bytes[0..4], .little)));
            } else blk: {
                if (bytes.len != 8) return null;
                break :blk @as(f64, @bitCast(std.mem.readInt(u64, bytes[0..8], .little)));
            };
            return if (std.math.isNan(v)) null else .{ .float = v };
        },
        .BYTE_ARRAY => return if (statistics.bytewiseOrder(elem)) .{ .str = bytes } else null,
        else => return null,
    }
}

/// Whether every output column and sort key is a bare input column, which no row can fail to produce.
fn allPassthrough(specs: []const consumer.OutputCol, keys: []const RowSortKey) bool {
    for (specs) |spec| if (spec == .computed) return false;
    for (keys) |key| if (key.spec == .computed) return false;
    return true;
}

/// A row group of the plain-row Top-N scan, and the best first key its statistics allow (`rowGroupBest`).
const RankedRG = struct {
    file: u32,
    rg: u32,
    best: ?SortCell,

    /// Most promising first, so the heap fills with good rows early and its cutoff rules more row groups out. Row groups
    /// without a bound go first, in input order: they are read whatever the cutoff.
    fn sooner(key: RowSortKey, a: RankedRG, b: RankedRG) bool {
        const x = a.best orelse return b.best != null;
        const y = b.best orelse return false;
        var ord = x.order(y);
        if (key.descending and x != .null and y != .null) ord = ord.invert();
        return ord == .lt;
    }
};

/// Every row group of every input, with its first-key bound. Ordered most promising first when every input's bytes
/// are all in; a gated input is fetched in input order, so there the order stays and only the bounds are used.
fn rankRowGroups(
    arena: std.mem.Allocator,
    inputs: []const scan.Input,
    metas: []const schema.FileMetaData,
    key: RowSortKey,
    bounded: bool,
) ![]RankedRG {
    var n: usize = 0;
    for (metas) |*m| n += m.row_groups.items.len;
    const out = try arena.alloc(RankedRG, n);
    var at: usize = 0;
    for (metas, 0..) |*m, fi| for (m.row_groups.items, 0..) |*rg, ri| {
        out[at] = .{ .file = @intCast(fi), .rg = @intCast(ri), .best = if (bounded) rowGroupBest(rg, m, key) else null };
        at += 1;
    };
    if (!bounded) return out;
    for (inputs) |in| if (in.gate != null) return out;
    std.mem.sort(RankedRG, out, key, RankedRG.sooner);
    return out;
}

/// Append row `row` of `src` to `dst`, both built for the same output columns, its strings copied onto `dst`'s arena.
/// A column whose rows so far carried no definition levels gains them, all present, once one row does.
fn copyRow(dst: *consumer.OutputAggregator, src: *const consumer.OutputAggregator, row: usize) !void {
    for (dst.cols, src.cols) |*d, s| switch (d.*) {
        inline else => |*dt, tag| {
            const st = @field(s, @tagName(tag));
            std.debug.assert(!st.coded);
            var v = st.values.items[row];
            if (@TypeOf(v) == []const u8) v = try dst.arena.dupe(u8, v);
            try dt.values.append(dst.arena, v);
            if (st.def_levels.items.len > 0 or dt.def_levels.items.len > 0) {
                dt.max_def = @max(dt.max_def, st.max_def);
                while (dt.def_levels.items.len + 1 < dt.values.items.len) try dt.def_levels.append(dst.arena, dt.max_def);
                const level = if (st.def_levels.items.len > 0) st.def_levels.items[row] else dt.max_def;
                try dt.def_levels.append(dst.arena, level);
            }
        },
    };
    dst.num_rows += 1;
}

/// Whether first key `a` sorts strictly after `b` under `key` (NULL last in either direction).
fn sortsAfter(key: RowSortKey, a: SortCell, b: SortCell) bool {
    var ord = a.order(b);
    if (key.descending and a != .null and b != .null) ord = ord.invert();
    return ord == .gt;
}

/// The most rows the parallel plain-row Top-N keeps (OFFSET + LIMIT); past it, or without a LIMIT, `TopRows` runs.
/// Every worker holds a heap of that many keys, so it bounds the heaps as well as the gather.
const max_gather_rows = 1 << 16;

/// Bytes the parallel plain-row Top-N may hold at once: its heaps and their key copies, the merge, and the winners'
/// gathered columns. Each is reserved before it is allocated and given back when freed, so a refusal comes before the
/// memory does; it sends the query to `TopRows`, which answers the same. Row group decoding is not counted here, as it
/// is not for `TopRows`.
const Reservation = struct {
    limit: usize,
    used: std.atomic.Value(usize) = .init(0),

    fn take(self: *Reservation, n: usize) error{ExceededMemoryBudget}!void {
        var cur = self.used.load(.monotonic);
        while (true) {
            const next = std.math.add(usize, cur, n) catch return error.ExceededMemoryBudget;
            if (next > self.limit) return error.ExceededMemoryBudget;
            cur = self.used.cmpxchgWeak(cur, next, .monotonic, .monotonic) orelse return;
        }
    }

    fn give(self: *Reservation, n: usize) void {
        _ = self.used.fetchSub(n, .monotonic);
    }
};

/// Exact blocks on `gpa`, each reserved before it is allocated: no arena, whose next block grows with the last, so
/// what is held is what was reserved. One thread's at a time.
const ReservedMem = struct {
    gpa: std.mem.Allocator,
    res: *Reservation,
    blocks: std.ArrayList([]align(16) u8) = .empty,

    fn alloc(m: *ReservedMem, comptime T: type, n: usize) ![]T {
        comptime std.debug.assert(@alignOf(T) <= 16);
        const size = std.math.mul(usize, n, @sizeOf(T)) catch return error.ExceededMemoryBudget;
        try m.res.take(size);
        errdefer m.res.give(size);
        try m.blocks.ensureUnusedCapacity(m.gpa, 1);
        const b = try m.gpa.alignedAlloc(u8, .@"16", size);
        m.blocks.appendAssumeCapacity(b);
        return @alignCast(std.mem.bytesAsSlice(T, b));
    }

    fn deinit(m: *ReservedMem) void {
        for (m.blocks.items) |b| {
            m.res.give(b.len);
            m.gpa.free(b);
        }
        m.blocks.deinit(m.gpa);
    }
};

/// The plain-row Top-N over inputs whose bytes are all in, in two passes. Workers rank row groups by their filter and
/// key columns alone, each into a heap of its best `h` (keys, physical row position): a row outside a worker's best h
/// has h rows ahead of it overall, so the heaps together hold the answer. They share the best cutoff any full heap has
/// reached, and pass over a row group or row whose first key sorts strictly after it; a stale cutoff is only weaker.
/// Once the heaps merge to the final h rows, the output columns are read for those rows alone (`GatherTask`).
const RankShared = struct {
    gpa: std.mem.Allocator,
    io: std.Io,
    inputs: []const scan.Input,
    metas: []const schema.FileMetaData,
    filter: ?filter_ast.Filter,
    fetch: []const bool,
    keys: []const RowSortKey,
    key_specs: []const consumer.OutputCol,
    h: usize,
    prune: bool,
    /// The first key is a bare string column (`dictionaryBest`), and whether the filter reads that column alone.
    dict_key: ?DictKey,
    options: consumer.DecodeOptions,
    profiler: ?*profile.Profiler,
    cursor: work_cursor.AtomicWorkCursor(RankedRG),
    res: *Reservation,
    /// A worker failed: the others stop taking row groups.
    failed: std.atomic.Value(bool) = .init(false),
    /// Bumped with every new cutoff, so a worker copies it only when it changed.
    cut_version: std.atomic.Value(u64) = .init(0),
    cut_mutex: std.Io.Mutex = .init,
    /// The first key of some full heap's last row, its string on `gpa`. Guarded by `cut_mutex`.
    cut: ?SortCell = null,

    fn deinit(self: *RankShared) void {
        if (self.cut) |c| if (c == .str) {
            self.res.give(c.str.len);
            self.gpa.free(c.str);
        };
    }
};

const DictKey = struct { ci: usize, filtered: bool };

const RankEntry = struct {
    cells: []SortCell,
    /// `ord` is the row's physical place in its row group, before the filter.
    pos: RowPos,
    /// `cells` and their strings: one block on `gpa`, reserved, freed when the entry leaves the heap.
    block: []align(@alignOf(SortCell)) u8,

    fn later(keys: []const RowSortKey, a: RankEntry, b: RankEntry) std.math.Order {
        if (TopRows.before(keys, b.cells, b.pos, a.cells, a.pos)) return .lt;
        if (TopRows.before(keys, a.cells, a.pos, b.cells, b.pos)) return .gt;
        return .eq;
    }

    fn lessThan(keys: []const RowSortKey, a: RankEntry, b: RankEntry) bool {
        return TopRows.before(keys, a.cells, a.pos, b.cells, b.pos);
    }
};

const RankWorker = struct {
    shared: *RankShared,
    index: usize,
    /// Its capacity, `h`, reserved and allocated up front: a push never grows it.
    heap: std.PriorityQueue(RankEntry, []const RowSortKey, RankEntry.later),
    /// This worker's copy of the shared cutoff, as of `cut_seen`; its string on `cut_buf`.
    cut: ?SortCell = null,
    cut_buf: std.ArrayList(u8) = .empty,
    cut_seen: u64 = 0,
    timings: consumer.Timings = .{},
    err: ?anyerror = null,
    /// The item that failed, for a deterministic choice among failing workers.
    err_at: usize = std.math.maxInt(usize),
    /// Row groups whose rows this worker offered, for its profile span.
    ranked: usize = 0,

    fn init(shared: *RankShared, index: usize) !RankWorker {
        const size = std.math.mul(usize, shared.h, @sizeOf(RankEntry)) catch return error.ExceededMemoryBudget;
        try shared.res.take(size);
        errdefer shared.res.give(size);
        var heap: std.PriorityQueue(RankEntry, []const RowSortKey, RankEntry.later) = .initContext(shared.keys);
        try heap.ensureTotalCapacityPrecise(shared.gpa, shared.h);
        return .{ .shared = shared, .index = index, .heap = heap };
    }

    fn deinit(w: *RankWorker) void {
        const s = w.shared;
        for (w.heap.items) |e| w.free(e);
        s.res.give(w.heap.capacity() * @sizeOf(RankEntry));
        w.heap.deinit(s.gpa);
        s.res.give(w.cut_buf.capacity);
        w.cut_buf.deinit(s.gpa);
    }

    fn run(w: *RankWorker) void {
        const s = w.shared;
        const prof = profile.threadOf(s.profiler, .scan);
        const span = profile.Span.begin(prof);
        w.timings.prof = prof;
        defer span.end(.scan_worker, profile.args(.{ w.index, w.ranked }));
        while (!s.failed.load(.monotonic)) {
            const item = s.cursor.next() orelse break;
            w.step(item) catch |e| {
                w.err = e;
                w.err_at = item - s.cursor.items.ptr;
                s.failed.store(true, .monotonic);
                break;
            };
        }
    }

    fn step(w: *RankWorker, item: *const RankedRG) !void {
        const s = w.shared;
        const meta = &s.metas[item.file];
        const rg = &meta.row_groups.items[item.rg];
        if (s.prune) if (s.filter) |f| if (filter_prune.pruneRowGroup(rg, f, meta) == .skip) return;
        try w.refreshCut();
        if (item.best) |best| if (w.cut) |c| if (sortsAfter(s.keys[0], best, c)) return;
        if (s.dict_key != null) if (w.cut) |c| {
            var dict_mem = std.heap.ArenaAllocator.init(s.gpa);
            defer dict_mem.deinit();
            if (try w.dictionaryBest(dict_mem.allocator(), rg, meta, item.*)) |best| {
                if (sortsAfter(s.keys[0], best, c)) return;
            }
        };
        try w.rank(rg, meta, item.*);
        try w.publishCut();
    }

    /// Offer one row group's filtered rows to the heap, by their keys alone.
    fn rank(w: *RankWorker, rg: *const schema.RowGroup, meta: *const schema.FileMetaData, item: RankedRG) !void {
        const s = w.shared;
        var rg_arena = std.heap.ArenaAllocator.init(s.gpa);
        defer rg_arena.deinit();
        const ra = rg_arena.allocator();
        var keyed = try consumer.initOutputAggregator(ra, meta, s.key_specs);
        const src: consumer.RGSrc = .{ .bytes = s.inputs[item.file].bytes, .byte_origin = 0 };
        var p: consumer.ProjectedRG = undefined;
        try p.init(&keyed, s.gpa, rg, meta, src, s.filter, s.fetch, s.key_specs, &w.timings, s.options);
        defer p.deinit();
        w.ranked += 1;
        if (p.surviving == 0) return;
        for (0..s.key_specs.len) |i| try p.projectColumn(&keyed, i, s.gpa, &w.timings);
        keyed.num_rows += p.surviving;

        const scratch = try ra.alloc(SortCell, s.keys.len);
        var i: usize = 0;
        for (p.sel.mask, 0..) |word, wi| {
            var bits = word;
            while (bits != 0) : (bits &= bits - 1) {
                const at = i;
                i += 1;
                for (s.keys, keyed.cols, scratch) |k, buf, *cell| cell.* = sortCell(buf, at, k.unsigned);
                if (w.cut) |c| if (sortsAfter(s.keys[0], scratch[0], c)) continue;
                const pos: RowPos = .{ .file = item.file, .rg = item.rg, .ord = @intCast(wi * 64 + @ctz(bits)) };
                if (w.heap.count() == s.h) {
                    const top = w.heap.peek().?;
                    if (!TopRows.before(s.keys, scratch, pos, top.cells, top.pos)) continue;
                    w.free(w.heap.pop().?);
                }
                try w.admit(scratch, pos);
            }
        }
    }

    /// Push a row into the heap (which has room), its cells and strings copied into one reserved block.
    fn admit(w: *RankWorker, cells_in: []const SortCell, pos: RowPos) !void {
        const s = w.shared;
        const head = cells_in.len * @sizeOf(SortCell);
        var size = head;
        for (cells_in) |c| if (c == .str) {
            size += c.str.len;
        };
        try s.res.take(size);
        errdefer s.res.give(size);
        const block = try s.gpa.alignedAlloc(u8, .of(SortCell), size);
        const cells: []SortCell = @alignCast(std.mem.bytesAsSlice(SortCell, block[0..head]));
        var tail = block[head..];
        for (cells_in, cells) |c, *d| {
            d.* = c;
            if (c == .str) {
                @memcpy(tail[0..c.str.len], c.str);
                d.* = .{ .str = tail[0..c.str.len] };
                tail = tail[c.str.len..];
            }
        }
        std.debug.assert(w.heap.count() < w.heap.capacity());
        w.heap.push(s.gpa, .{ .cells = cells, .pos = pos, .block = block }) catch unreachable;
    }

    fn free(w: *RankWorker, e: RankEntry) void {
        w.shared.res.give(e.block.len);
        w.shared.gpa.free(e.block);
    }

    /// The best first key any row of a row group can have, from its string key chunk's dictionary: the smallest entry
    /// ascending, the largest descending, among those the filter keeps when it reads that column alone. NULL when no
    /// entry qualifies (the group's rows then have a NULL key or none qualifies). A safe bound whether or not a row
    /// uses the entry; null when the chunk is not dictionary-encoded throughout, so a value may not be an entry.
    fn dictionaryBest(
        w: *RankWorker,
        ra: std.mem.Allocator,
        rg: *const schema.RowGroup,
        meta: *const schema.FileMetaData,
        item: RankedRG,
    ) !?SortCell {
        const s = w.shared;
        const dk = s.dict_key.?;
        const src: consumer.RGSrc = .{ .bytes = s.inputs[item.file].bytes, .byte_origin = 0 };
        const dict = (try consumer.chunkDictionary(ra, rg, meta, src, dk.ci)) orelse return null;
        var kept: ?filter_selection.SelectionVector = null;
        if (dk.filtered) if (s.filter) |f| {
            const cols = [_]filter_eval.Batch.Column{.{ .string = .{ .values = dict } }};
            const lookup = try ra.alloc(?usize, meta.schema.items.len);
            @memset(lookup, null);
            lookup[dk.ci] = 0;
            var sel = try filter_selection.SelectionVector.init(ra, dict.len);
            try filter_eval.evaluate(f, &.{ .cols = &cols, .num_rows = dict.len }, &sel, lookup, ra);
            kept = sel;
        };
        var best: ?[]const u8 = null;
        const want: std.math.Order = if (s.keys[0].descending) .gt else .lt;
        for (dict, 0..) |v, k| {
            if (kept) |sel| if (!sel.isActive(k)) continue;
            if (best == null or std.mem.order(u8, v, best.?) == want) best = v;
        }
        return if (best) |b| .{ .str = b } else .null;
    }

    /// Copy the shared cutoff when it changed since this worker last looked.
    fn refreshCut(w: *RankWorker) !void {
        const s = w.shared;
        if (s.cut_version.load(.acquire) == w.cut_seen) return;
        s.cut_mutex.lockUncancelable(s.io);
        defer s.cut_mutex.unlock(s.io);
        const c = s.cut orelse return;
        if (c == .str and c.str.len > w.cut_buf.capacity) {
            const grow = c.str.len - w.cut_buf.capacity;
            try s.res.take(grow);
            w.cut_buf.ensureTotalCapacityPrecise(s.gpa, c.str.len) catch |e| {
                s.res.give(grow);
                return e;
            };
        }
        w.cut_buf.clearRetainingCapacity();
        w.cut = if (c == .str) .{ .str = blk: {
            w.cut_buf.appendSliceAssumeCapacity(c.str);
            break :blk w.cut_buf.items;
        } } else c;
        w.cut_seen = s.cut_version.load(.monotonic);
    }

    /// Share this worker's cutoff when its heap is full and its last row's first key beats the shared one.
    fn publishCut(w: *RankWorker) !void {
        const s = w.shared;
        if (w.heap.count() < s.h) return;
        const mine = w.heap.peek().?.cells[0];
        if (w.cut) |c| if (!sortsAfter(s.keys[0], c, mine)) return;
        s.cut_mutex.lockUncancelable(s.io);
        defer s.cut_mutex.unlock(s.io);
        if (s.cut) |c| if (!sortsAfter(s.keys[0], c, mine)) return;
        const owned: SortCell = if (mine == .str) blk: {
            try s.res.take(mine.str.len);
            break :blk .{ .str = s.gpa.dupe(u8, mine.str) catch |e| {
                s.res.give(mine.str.len);
                return e;
            } };
        } else mine;
        if (s.cut) |c| if (c == .str) {
            s.res.give(c.str.len);
            s.gpa.free(c.str);
        };
        s.cut = owned;
        _ = s.cut_version.fetchAdd(1, .release);
    }
};

/// The winners of one row group, by their physical rows, ascending.
const GatherGroup = struct {
    file: u32,
    rg: u32,
    rows: []const u32,
};

/// One output column of one row group holding winners: its winners' values, read a run of rows at a time with the
/// pages between them passed over (`consumer.SliceReader.skip`). Its buffers are sized once, for its rows, and every
/// byte is reserved first.
const GatherTask = struct {
    inputs: []const scan.Input,
    metas: []const schema.FileMetaData,
    group: *const GatherGroup,
    ci: usize,
    options: consumer.DecodeOptions,
    /// In the output column's lane; a string's values point into `bytes` once `read` returns.
    buf: consumer.OutputAggregator.ColumnBuf,
    mem: ReservedMem,
    bytes: std.ArrayList(u8) = .empty,
    err: ?anyerror = null,

    fn deinit(t: *GatherTask) void {
        t.mem.res.give(t.bytes.capacity);
        t.bytes.deinit(t.mem.gpa);
        t.mem.deinit();
    }

    fn run(t: *GatherTask) void {
        t.read() catch |e| {
            t.err = e;
        };
    }

    fn read(t: *GatherTask) !void {
        const g = t.group;
        const n = g.rows.len;
        const meta = &t.metas[g.file];
        const rg = &meta.row_groups.items[g.rg];
        const src: consumer.RGSrc = .{ .bytes = t.inputs[g.file].bytes, .byte_origin = 0 };
        const cm = rg.columns.items[t.ci].meta_data orelse return error.ColumnMetaMissing;
        const max_def = meta.getColumnLevels(cm.path_in_schema.items).max_def;
        // A string's byte offsets into `bytes`, one past each value.
        var ends: []usize = &.{};
        switch (t.buf) {
            inline else => |*tb| {
                const T = @typeInfo(@TypeOf(tb.values.items)).pointer.child;
                tb.values = .initBuffer(try t.mem.alloc(T, n));
                if (max_def > 0) tb.def_levels = .initBuffer(try t.mem.alloc(u32, n));
                if (T == []const u8) ends = try t.mem.alloc(usize, n);
            },
        }
        var held = std.heap.ArenaAllocator.init(t.mem.gpa);
        defer held.deinit();
        var r = (try consumer.SliceReader.init(held.allocator(), t.mem.gpa, rg, meta, src, t.ci, t.options)) orelse
            return error.NestedRowOutputNotSupported;
        defer r.deinit();
        var at: usize = 0;
        var i: usize = 0;
        while (i < n) {
            var j = i + 1;
            while (j < n and g.rows[j] == g.rows[j - 1] + 1) j += 1;
            var slice = std.heap.ArenaAllocator.init(t.mem.gpa);
            defer slice.deinit();
            const sa = slice.allocator();
            try r.skip(sa, g.rows[i] - at);
            const col = try consumer.outputLane(t.buf, try r.next(sa, j - i), sa);
            switch (col) {
                inline else => |c, tag| {
                    if (!std.mem.eql(u8, @tagName(std.meta.activeTag(t.buf)), @tagName(tag))) return error.TypeMismatch;
                    const tb = &@field(t.buf, @tagName(tag));
                    if (@TypeOf(c.values) == []const []const u8) {
                        var run_bytes: usize = 0;
                        for (c.values) |v| run_bytes += v.len;
                        try t.growBytes(run_bytes);
                        for (c.values, ends[i..j]) |v, *e| {
                            t.bytes.appendSliceAssumeCapacity(v);
                            e.* = t.bytes.items.len;
                        }
                        tb.values.items.len += c.values.len;
                    } else tb.values.appendSliceAssumeCapacity(c.values);
                    if (max_def > 0) {
                        if (c.def_levels) |dl| tb.def_levels.appendSliceAssumeCapacity(dl) else {
                            tb.def_levels.appendNTimesAssumeCapacity(c.max_def, j - i);
                        }
                        tb.max_def = c.max_def;
                    }
                },
            }
            at = g.rows[j - 1] + 1;
            i = j;
        }
        if (t.buf == .string) for (t.buf.string.values.items, ends, 0..) |*v, end, k| {
            v.* = t.bytes.items[if (k == 0) 0 else ends[k - 1]..end];
        };
    }

    /// Room for `more` string bytes, the grown buffer reserved before it is allocated.
    fn growBytes(t: *GatherTask, more: usize) !void {
        const need = t.bytes.items.len + more;
        if (need <= t.bytes.capacity) return;
        const old = t.bytes.capacity;
        const cap = @max(need, old *| 2);
        // Both blocks while the bytes move.
        try t.mem.res.take(cap);
        t.bytes.ensureTotalCapacityPrecise(t.mem.gpa, cap) catch |e| {
            t.mem.res.give(cap);
            return e;
        };
        t.mem.res.give(old);
    }
};

const GatherRunner = struct {
    cursor: *work_cursor.AtomicWorkCursor(GatherTask),

    fn run(g: *GatherRunner) void {
        while (g.cursor.next()) |t| t.run();
    }
};

/// Run `ctxs` on threads of their own, or on this one when there is one context or no thread starts.
fn runAll(comptime Ctx: type, ctxs: []Ctx, comptime func: fn (*Ctx) void) void {
    if (ctxs.len > 1 and spawn_util.runTree(Ctx, ctxs, .{ .stack_size = scan.WORKER_STACK_SIZE }, func)) return;
    func(&ctxs[0]);
}

/// Leaf `ci` when the first sort key is a bare string column, whose dictionary bounds a row group's keys
/// (`RankWorker.dictionaryBest`): BYTE_ARRAY, not DECIMAL (whose lane is numeric), compared as raw bytes as its
/// `SortCell` is. `filtered` when the filter reads that column alone, so it can judge each dictionary entry.
fn dictionaryKey(meta: *const schema.FileMetaData, key: RowSortKey, filter: ?filter_ast.Filter) ?DictKey {
    const ci = switch (key.spec) {
        .passthrough => |ci| ci,
        .computed => return null,
    };
    var leaves: schema.LeafIterator = .init(meta.schema.items);
    const leaf = while (leaves.next()) |l| {
        if (l.index == ci) break l;
    } else return null;
    if (leaf.element.type != .BYTE_ARRAY or leaf.max_rep > 0 or decimal_mod.kindFromSchema(leaf.element) != null) return null;
    const f = filter orelse return .{ .ci = ci, .filtered = false };
    var buf: [64]usize = undefined;
    var fba = std.heap.FixedBufferAllocator.init(std.mem.sliceAsBytes(&buf));
    var cols: std.ArrayList(usize) = .empty;
    f.collectColumns(&cols, fba.allocator()) catch return .{ .ci = ci, .filtered = false };
    for (cols.items) |c| if (c != ci) return .{ .ci = ci, .filtered = false };
    return .{ .ci = ci, .filtered = true };
}

/// Whether the plain-row Top-N can rank in parallel and gather its winners (`RankShared`): a LIMIT keeping at most
/// `max_gather_rows`, bare output columns and keys (a computed one can fail on a row, and passing over its row group
/// would answer where reading it raises the error), flat outputs (read a slice at a time), and every input's bytes
/// already in: a gated input releases a row group's bytes once read, and the gather reads winners' groups again.
fn gatherEligible(
    env: std.process.Environ,
    inputs: []const scan.Input,
    meta: *const schema.FileMetaData,
    limit: ?usize,
    keep: usize,
    specs: []const consumer.OutputCol,
    keys: []const RowSortKey,
) bool {
    if (envDisables(env, "ZPQ_TOPN_GATHER")) return false;
    const l = limit orelse return false;
    if (l == 0 or keep > max_gather_rows) return false;
    if (!allPassthrough(specs, keys)) return false;
    for (inputs) |in| if (in.gate != null) return false;
    var leaves: schema.LeafIterator = .init(meta.schema.items);
    while (leaves.next()) |leaf| for (specs) |spec| {
        if (spec.passthrough == leaf.index and leaf.max_rep > 0) return false;
    };
    return true;
}

/// The parallel plain-row Top-N's answer: the first `h` rows in order (`TopRows.ranked`'s), held in reserved memory
/// on `gpa` until `deinit`.
const Gathered = struct {
    out: consumer.OutputAggregator,
    /// The answer's columns and the merge's arrays.
    mem: ReservedMem,
    /// The winners' columns per row group; the answer's strings point into them.
    tasks: []GatherTask = &.{},

    fn deinit(g: *Gathered) void {
        for (g.tasks) |*t| t.deinit();
        g.mem.deinit();
    }
};

/// The plain-row Top-N ranked in parallel and gathered (see `RankShared`). Null when its memory would pass the
/// budget, refused before it is allocated and with everything given back; the caller then runs `TopRows`.
fn rankAndGather(
    ctx: Context,
    arena: std.mem.Allocator,
    args: QueryArgs,
    inputs: []const scan.Input,
    metas: []const schema.FileMetaData,
    filter: ?filter_ast.Filter,
    fetch: []const bool,
    specs: []const consumer.OutputCol,
    keys: []const RowSortKey,
    h: usize,
    items: []RankedRG,
    t: *Timings,
) !?Gathered {
    const res = try arena.create(Reservation);
    res.* = .{ .limit = args.max_memory / 2 };
    var g: Gathered = .{
        .out = try consumer.initOutputAggregator(arena, &metas[0], specs),
        .mem = .{ .gpa = ctx.gpa, .res = res },
    };
    gatherInto(&g, ctx, arena, args, inputs, metas, filter, fetch, specs, keys, h, items, t) catch |e| {
        g.deinit();
        return if (e == error.ExceededMemoryBudget) null else e;
    };
    return g;
}

fn gatherInto(
    g: *Gathered,
    ctx: Context,
    arena: std.mem.Allocator,
    args: QueryArgs,
    inputs: []const scan.Input,
    metas: []const schema.FileMetaData,
    filter: ?filter_ast.Filter,
    fetch: []const bool,
    specs: []const consumer.OutputCol,
    keys: []const RowSortKey,
    h: usize,
    items: []RankedRG,
    t: *Timings,
) !void {
    const gpa = ctx.gpa;
    const res = g.mem.res;
    const prof = profile.threadOf(args.profiler, .main);
    if (items.len == 0) return;
    const key_specs = try arena.alloc(consumer.OutputCol, keys.len);
    for (keys, key_specs) |k, *spec| spec.* = k.spec;
    const threads = if (args.parallelism > 0) args.parallelism else system.discoverAvailableParallelism(ctx.env);
    // Every worker holds a heap of h: fewer workers rather than heaps taking more than a quarter of the budget.
    var n_workers = std.math.clamp(threads, 1, items.len);
    while (n_workers > 1 and n_workers *| h *| @sizeOf(RankEntry) > res.limit / 4) n_workers -= 1;

    const t_rank = nowMonoNs();
    var shared: RankShared = .{
        .gpa = gpa,
        .io = ctx.io,
        .inputs = inputs,
        .metas = metas,
        .filter = filter,
        .fetch = fetch,
        .keys = keys,
        .key_specs = key_specs,
        .h = h,
        .prune = !args.scan_all,
        .dict_key = dictionaryKey(&metas[0], keys[0], filter),
        .options = .{ .fast_levels = args.fast_levels, .expand_limit = args.max_memory },
        .profiler = args.profiler,
        .cursor = .{ .items = items },
        .res = res,
    };
    defer shared.deinit();
    const workers = try g.mem.alloc(RankWorker, n_workers);
    var live: usize = 0;
    defer for (workers[0..live]) |*w| w.deinit();
    for (workers) |*w| {
        w.* = try .init(&shared, live);
        live += 1;
    }
    runAll(RankWorker, workers, RankWorker.run);
    const t_merge = nowMonoNs();
    profile.record(prof, .scan, t_rank, t_merge, profile.args(.{n_workers}));

    var failed: ?*const RankWorker = null;
    for (workers) |*w| {
        t.core.decode_ns += w.timings.decode_ns;
        t.core.eval_ns += w.timings.eval_ns;
        if (w.err != null and (failed == null or w.err_at < failed.?.err_at)) failed = w;
    }
    if (failed) |w| return w.err.?;

    var n_entries: usize = 0;
    for (workers) |w| n_entries += w.heap.count();
    const entries = try g.mem.alloc(RankEntry, n_entries);
    var at: usize = 0;
    for (workers) |w| {
        @memcpy(entries[at..][0..w.heap.count()], w.heap.items);
        at += w.heap.count();
    }
    std.mem.sortUnstable(RankEntry, entries, keys, RankEntry.lessThan);
    const winners = entries[0..@min(h, entries.len)];
    profile.record(prof, .merge, t_merge, nowMonoNs(), profile.args(.{ n_entries, winners.len }));
    if (winners.len == 0) return;

    // Winners by place in the input, grouped by row group; `slots[k]` is winner k's group and row within it.
    const t_gather = nowMonoNs();
    const by_pos = try g.mem.alloc(u32, winners.len);
    for (by_pos, 0..) |*b, k| b.* = @intCast(k);
    std.mem.sortUnstable(u32, by_pos, winners, struct {
        fn lt(ws: []const RankEntry, a: u32, b: u32) bool {
            return ws[a].pos.order(ws[b].pos) == .lt;
        }
    }.lt);
    const WinnerSlot = struct { group: u32, row: u32 };
    const slots = try g.mem.alloc(WinnerSlot, winners.len);
    const rows = try g.mem.alloc(u32, winners.len);
    var n_groups: usize = 0;
    for (by_pos, 0..) |k, n| {
        if (n == 0 or winners[k].pos.file != winners[by_pos[n - 1]].pos.file or
            winners[k].pos.rg != winners[by_pos[n - 1]].pos.rg) n_groups += 1;
    }
    const groups = try g.mem.alloc(GatherGroup, n_groups);
    var gi: usize = 0;
    var start: usize = 0;
    for (by_pos, 0..) |k, n| {
        const p = winners[k].pos;
        rows[n] = p.ord;
        const last = n + 1 == by_pos.len or winners[by_pos[n + 1]].pos.file != p.file or
            winners[by_pos[n + 1]].pos.rg != p.rg;
        slots[k] = .{ .group = @intCast(gi), .row = @intCast(n - start) };
        if (last) {
            groups[gi] = .{ .file = p.file, .rg = p.rg, .rows = rows[start .. n + 1] };
            gi += 1;
            start = n + 1;
        }
    }
    // Only positions are needed from here: the heaps go before the winners' columns are read.
    for (workers[0..live]) |*w| w.deinit();
    live = 0;

    const tasks = try g.mem.alloc(GatherTask, groups.len * specs.len);
    for (groups, 0..) |*grp, k| for (specs, g.out.cols, 0..) |spec, col, ci| {
        tasks[k * specs.len + ci] = .{
            .inputs = inputs,
            .metas = metas,
            .group = grp,
            .ci = spec.passthrough,
            .options = .{ .fast_levels = args.fast_levels, .expand_limit = args.max_memory },
            .buf = switch (col) {
                inline else => |_, tag| @unionInit(consumer.OutputAggregator.ColumnBuf, @tagName(tag), .{}),
            },
            .mem = .{ .gpa = gpa, .res = res },
        };
    };
    g.tasks = tasks;
    var cursor: work_cursor.AtomicWorkCursor(GatherTask) = .{ .items = tasks };
    const runners = try arena.alloc(GatherRunner, std.math.clamp(threads, 1, tasks.len));
    for (runners) |*r| r.* = .{ .cursor = &cursor };
    runAll(GatherRunner, runners, GatherRunner.run);
    // The first failure in task order, so the error does not depend on which thread met it first.
    for (tasks) |*task| if (task.err) |e| return e;

    // The answer's columns, in rank order, borrowing the tasks' strings.
    for (g.out.cols, 0..) |*col, ci| switch (col.*) {
        inline else => |*ob, tag| {
            const T = @typeInfo(@TypeOf(ob.values.items)).pointer.child;
            const first = &@field(tasks[ci].buf, @tagName(tag));
            const vals = try g.mem.alloc(T, winners.len);
            const defs: ?[]u32 = if (first.def_levels.capacity > 0) try g.mem.alloc(u32, winners.len) else null;
            for (slots, 0..) |s, k| {
                const src = &@field(tasks[s.group * specs.len + ci].buf, @tagName(tag));
                vals[k] = src.values.items[s.row];
                if (defs) |d| d[k] = src.def_levels.items[s.row];
            }
            ob.values = .initBuffer(vals);
            ob.values.items.len = vals.len;
            if (defs) |d| {
                ob.def_levels = .initBuffer(d);
                ob.def_levels.items.len = d.len;
            }
            ob.max_def = first.max_def;
        },
    };
    g.out.num_rows = winners.len;
    profile.record(prof, .project, t_gather, nowMonoNs(), profile.args(.{ winners.len, specs.len }));
}

const SlicedPrint = struct {
    names: []const []const u8,
    writer: *StdoutWriter,
    is_jsonl: bool,
    limit: ?usize,
    total_printed: *usize,
    /// What the row group's decoded columns may take; past it, it prints in slices.
    budget: usize,
    decode_options: consumer.DecodeOptions,
    /// Slice even a row group that looks to fit: one whose values expanded past the budget decoded whole.
    force: bool = false,
};

/// Print `rg` a slice of rows at a time when its decoded columns would take more than `o.budget`, or a limit is set:
/// each slice's fetched columns decode (`consumer.SliceReader`), filter and print on memory of their own, and printing
/// stops at the limit without decoding further. False, having printed nothing, when the row group fits (or has a
/// column with no sliced form but fits); the caller then prints it whole. One that fits neither way is refused before
/// anything decodes.
fn printRowGroupSliced(
    gpa: std.mem.Allocator,
    agg: *consumer.OutputAggregator,
    rg: *const schema.RowGroup,
    meta: *const schema.FileMetaData,
    src: consumer.RGSrc,
    filter: ?filter_ast.Filter,
    fetch_arr: []const bool,
    output_specs: []const consumer.OutputCol,
    o: SlicedPrint,
) !bool {
    const num_rows: usize = @intCast(@max(0, rg.num_rows));
    const est = consumer.decodeEstimate(.{
        .rg = rg,
        .meta = meta,
        .src = src,
        .filter = null,
        .read = fetch_arr,
        .consumed = fetch_arr,
    }, fetch_arr, &.{}, false) * 2; // decoded, then buffered for printing
    if (num_rows == 0) return false;
    // A limit also slices, so a print that wants a few rows decodes about that many.
    const wanted: ?usize = if (o.limit) |lim| lim -| o.total_printed.* else null;
    if (est <= o.budget and wanted == null and !o.force) return false;
    for (fetch_arr, 0..) |needed, ci| {
        if (!needed or ci >= rg.columns.items.len) continue;
        if (consumer.SliceReader.supports(rg, meta, ci)) continue;
        if (est > o.budget) return error.RowGroupTooLarge;
        return false;
    }

    var pages = std.heap.ArenaAllocator.init(gpa);
    defer pages.deinit();
    var readers: std.ArrayList(consumer.SliceReader) = .empty;
    defer for (readers.items) |*r| r.deinit();
    const lookup = try pages.allocator().alloc(?usize, fetch_arr.len);
    @memset(lookup, null);
    for (fetch_arr, 0..) |needed, ci| {
        if (!needed or ci >= rg.columns.items.len) continue;
        const r = try consumer.SliceReader.init(pages.allocator(), gpa, rg, meta, src, ci, o.decode_options) orelse
            unreachable; // every column was checked above
        lookup[ci] = readers.items.len;
        try readers.append(pages.allocator(), r);
    }
    // A slice takes a quarter of the budget. Under a limit the first slice is about twice the rows still wanted, and
    // each one after (a filter dropped rows) twice the last.
    const per_row = @max(1, est / num_rows);
    const max_rows = std.math.clamp(o.budget / 4 / per_row, 1, num_rows);
    var slice_rows = if (wanted) |w| std.math.clamp(w *| 2, @min(64, max_rows), max_rows) else max_rows;

    var done: usize = 0;
    while (done < num_rows) {
        if (o.limit) |lim| if (o.total_printed.* >= lim) break;
        const n = @min(slice_rows, num_rows - done);
        var slice = std.heap.ArenaAllocator.init(gpa);
        defer slice.deinit();
        const sa = slice.allocator();
        const cols = try sa.alloc(filter_eval.Batch.Column, readers.items.len);
        for (readers.items, cols) |*r, *c| c.* = try r.next(sa, n);
        const batch: filter_eval.Batch = .{ .cols = cols, .num_rows = n };
        var sel = try filter_selection.SelectionVector.init(sa, n);
        if (filter) |f| try filter_eval.evaluate(f, &batch, &sel, lookup, sa);
        // The aggregator's buffers live on the slice's memory and go with it.
        agg.arena = sa;
        for (agg.cols) |*c| c.* = switch (c.*) {
            inline else => |_, tag| @unionInit(consumer.OutputAggregator.ColumnBuf, @tagName(tag), .{}),
        };
        agg.num_rows = 0;
        _ = try consumer.appendSliceRows(agg, &batch, lookup, &sel, output_specs, sa);
        try flushAggregator(agg, o.names, o.writer, o.is_jsonl, o.limit, o.total_printed);
        for (agg.cols) |*c| c.* = switch (c.*) {
            inline else => |_, tag| @unionInit(consumer.OutputAggregator.ColumnBuf, @tagName(tag), .{}),
        };
        agg.num_rows = 0;
        done += n;
        slice_rows = @min(max_rows, slice_rows *| 2);
    }
    return true;
}

fn flushAggregator(
    agg: *consumer.OutputAggregator,
    names: []const []const u8,
    writer: anytype,
    is_jsonl: bool,
    limit: ?usize,
    total_printed: *usize,
) !void {
    const output_start = nowMonoNs();
    defer writer.output_ns += @intCast(nowMonoNs() - output_start);
    if (writer.parquet) |pq| {
        try pq.append(agg, limit, total_printed, &writer.skip_remaining);
        return;
    }
    if (agg.num_rows == 0) return;

    var row_idx: usize = 0;
    while (row_idx < agg.num_rows) : (row_idx += 1) {
        if (limit) |lim| {
            if (total_printed.* >= lim) break;
        }

        if (writer.skip_remaining > 0) {
            writer.skip_remaining -= 1;
            continue;
        }
        if (is_jsonl) {
            if (writer.json_array and total_printed.* > 0) try writer.writeByte(',');
            try writer.writeAll("{");
            for (agg.cols, 0..) |col, col_idx| {
                if (col_idx > 0) try writer.writeAll(",");
                try writeJsonQuoted(writer, names[col_idx]);
                try writer.writeByte(':');
                try printCell(writer, col, row_idx, agg.schema_elems[col_idx], true);
            }
            try writer.writeAll(if (writer.json_array) "}" else "}\n");
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

// The limited row sink shares decode/encode with printing, but keeps only footer metadata between slices.
const ParquetPrint = struct {
    arena: std.mem.Allocator,
    gpa: std.mem.Allocator,
    fd_sink: FdSink,
    codec: schema.CompressionCodec,
    zstd: compression.ZstdPlan,
    schema_items: std.ArrayListUnmanaged(schema.SchemaElement),
    rgs: std.ArrayListUnmanaged(schema.RowGroup) = .empty,
    out_offset: u64 = 0,

    fn sink(self: *ParquetPrint) streaming.Sink {
        return .{ .ctx = @ptrCast(&self.fd_sink), .write_fn = FdSink.writeFn };
    }

    fn start(self: *ParquetPrint) !void {
        try self.sink().write("PAR1");
        self.out_offset = 4;
    }

    fn append(self: *ParquetPrint, agg: *consumer.OutputAggregator, limit: ?usize, total: *usize, skip: *usize) !void {
        const start_row = @min(skip.*, agg.num_rows);
        skip.* -= start_row;
        const n = @min(agg.num_rows - start_row, if (limit) |lim| lim -| total.* else agg.num_rows);
        if (n == 0) return;
        const saved = try self.gpa.dupe(consumer.OutputAggregator.ColumnBuf, agg.cols);
        defer self.gpa.free(saved);
        const old_rows = agg.num_rows;
        defer {
            @memcpy(agg.cols, saved);
            agg.num_rows = old_rows;
        }
        for (agg.cols) |*col| switch (col.*) {
            inline else => |*buf| {
                buf.values.items = buf.values.items[start_row..][0..n];
                if (buf.def_levels.items.len > 0) buf.def_levels.items = buf.def_levels.items[start_row..][0..n];
            },
        };
        agg.num_rows = n;
        var timings: consumer.Timings = .{};
        const out = try consumer.encodeAggregator(agg, self.arena, self.gpa, self.sink(), &self.out_offset, self.codec, self.zstd, &timings, 1, true, .{});
        if (out.rg) |rg| try self.rgs.append(self.arena, rg);
        total.* += n;
    }

    fn finish(self: *ParquetPrint, rows: usize) !void {
        const meta: schema.FileMetaData = .{
            .version = 2,
            .schema = self.schema_items,
            .num_rows = @intCast(rows),
            .row_groups = self.rgs,
            .created_by = "zpq",
            .column_orders = try schema.FileMetaData.outputColumnOrders(self.arena, invariant.footerLeafCount(self.schema_items.items), null, &.{}),
        };
        var writer: thrift.Writer = .init(self.arena);
        defer writer.deinit();
        try meta.write(&writer);
        const footer = writer.bytes();
        try self.sink().write(footer);
        var len: [4]u8 = undefined;
        std.mem.writeInt(u32, &len, @intCast(footer.len), .little);
        try self.sink().write(&len);
        try self.sink().write("PAR1");
        self.out_offset += footer.len + 8;
    }
};

pub const PrintResult = struct {
    output_ns: u64 = 0,
    rows_in: usize = 0,
    rows: usize = 0,
    bytes_out: u64 = 0,
    membership: membership.Stats = .{},
    timings: Timings = .{},
};

pub fn runPrint(ctx: Context, args: QueryArgs, format: PrintFormat, limit: ?usize) !void {
    _ = try runPrintTo(ctx, args, format, limit, 1, 0);
}

pub fn runPrintTo(
    ctx: Context,
    args: QueryArgs,
    format: PrintFormat,
    limit: ?usize,
    fd: local_fs.fd_t,
    offset: usize,
) !PrintResult {
    return runPrintSink(ctx, args, format, limit, fd, offset, null);
}

/// Text output may target memory in an embedding process; no stdout redirection or temporary files.
pub fn runPrintSink(
    ctx: Context,
    args: QueryArgs,
    format: PrintFormat,
    limit: ?usize,
    fd: local_fs.fd_t,
    offset: usize,
    sink: ?*std.Io.Writer,
) !PrintResult {
    var changed = false;
    return runPrintSinkOver(ctx, args, format, limit, fd, offset, sink, &changed) catch |err|
        if (changed) error.InputChanged else err;
}

/// As `executeAggregateOver`.
fn runPrintSinkOver(
    ctx: Context,
    args: QueryArgs,
    format: PrintFormat,
    limit: ?usize,
    fd: local_fs.fd_t,
    offset: usize,
    sink: ?*std.Io.Writer,
    changed: *bool,
) !PrintResult {
    const t_start = nowMonoNs();
    const prof = profile.threadOf(args.profiler, .main);
    const query_span = profile.Span.begin(prof);
    defer query_span.end(.query, profile.args(.{}));
    var arena_state = std.heap.ArenaAllocator.init(ctx.gpa);
    defer arena_state.deinit();
    const arena = arena_state.allocator();

    if (args.select != null and args.columns != null) return error.AggregateMutexWithSelect;

    const open_span = profile.Span.begin(prof);
    // With a limit, fetch as the rows are printed, so a query that stops early stops fetching too.
    var opened = try openInputs(ctx, arena, args, .{
        .limit = s3FetchWindow(ctx.env, args.max_memory),
        .always = limit != null,
        .footer_only = limit == 0,
    });
    defer opened.deinit();
    errdefer changed.* = !opened.whole();
    open_span.end(.open_inputs, profile.args(.{args.inputs.len}));

    const parse_span = profile.Span.begin(prof);
    const metas = try materializeMetas(arena, opened, args.diag, prof);
    if (metas.len == 0) return .{};
    const meta0 = &metas[0];

    for (metas[1..]) |*m| {
        if (metadata.schemaMismatch(meta0.schema.items, m.schema.items, .exact) != null) return error.SchemaMismatch;
    }

    const tree0 = try schema_tree.SchemaTree.build(arena, meta0.schema.items);

    var proj = try planProjection(arena, args, meta0);
    const filter_opt = proj.filter;
    const fetch_arr = proj.fetch;
    const output_specs = proj.output_specs;
    if (limit != 0) _ = try loadLocalMembership(arena, opened, metas, filter_opt, args.scan_all);
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

    // The row sink has a flat schema and no repetition-level buffer. Keep unsupported
    // leaves out rather than publishing a footer that misdescribes their encoded pages.
    if (format == .parquet) for (output_specs) |spec| switch (spec) {
        .passthrough => |ci| {
            const leaf = tree0.leaves[ci];
            if (leaf.type == .INT96) return error.INT96ReencodeNotSupported;
            if (leaf.path.len > 1 or leaf.max_rep > 0) return error.NestedReencodeNotSupported;
            // As the full write refuses it: no output width holds a BYTE_ARRAY decimal past 38 digits.
            if (metadata.leafSchemaElement(meta0, ci)) |elem| {
                if (decimal_mod.kindFromSchema(&elem)) |k| if (k.precision > 38) {
                    Diag.setColumn(args.diag, elem.name);
                    return error.UnsupportedDecimalPrecision;
                };
            }
        },
        .computed => {},
    };

    var agg = try consumer.initOutputAggregator(arena, meta0, output_specs);

    var parquet: ParquetPrint = .{
        .arena = arena,
        .gpa = ctx.gpa,
        .fd_sink = .{ .fd = fd },
        .codec = args.codec orelse .SNAPPY,
        .zstd = .{ .lambda = args.compress_priority.lambda() },
        .schema_items = try buildSelectSchema(arena, meta0, output_specs),
    };
    if (format == .parquet) {
        coerceByteArrayDecimalLeavesToFlba(&parquet.schema_items);
        coerceFloat16LeavesToDouble(&parquet.schema_items);
        try parquet.start();
    }
    var stdout_writer = StdoutWriter{
        .fd = fd,
        .sink = sink,
        .json_array = format == .json,
        .skip_remaining = offset,
        .parquet = if (format == .parquet) &parquet else null,
    };
    defer stdout_writer.flush() catch {};

    const names = try proj.names(arena);
    const is_jsonl = format != .csv;
    if (format == .json) try stdout_writer.writeByte('[');
    if (!is_jsonl) {
        for (names, 0..) |name, col_idx| {
            if (col_idx > 0) try stdout_writer.writeAll(",");
            try writeCsvString(&stdout_writer, name);
        }
        try stdout_writer.writeAll("\n");
    }

    if (args.diag) |diag| diag.execution_started = true;
    var total_printed: usize = 0;
    var rows_in: usize = 0;

    var t: Timings = .{};
    t.core.prof = prof;

    if (proj.row_keys.len > 0) {
        // Without a LIMIT every row is kept and sorted, within half the memory budget (as decoded rows are).
        const keep = if (limit) |l| offset +| l else std.math.maxInt(usize);
        // With a LIMIT, row groups go most promising first, and one whose statistics cannot beat the heap's last row
        // is passed over. Ties keep their input positions (`RowPos`), so the order read changes no answer. Only over
        // bare columns: a computed output or key can fail on a row (`100 / id`), and passing over its group would
        // answer where reading in order raises the error. Filter leaves compare a column with a literal and never fail.
        const bounded = limit != null and !args.scan_all and allPassthrough(output_specs, proj.row_keys);
        const items = if (limit != 0) try rankRowGroups(arena, opened.inputs, metas, proj.row_keys[0], bounded) else try arena.alloc(RankedRG, 0);
        for (items) |item| rows_in += @intCast(metas[item.file].row_groups.items[item.rg].num_rows);
        var gathered = if (gatherEligible(ctx.env, opened.inputs, meta0, limit, keep, output_specs, proj.row_keys))
            try rankAndGather(ctx, arena, args, opened.inputs, metas, filter_opt, fetch_arr, output_specs, proj.row_keys, keep, items, &t)
        else
            null;
        defer if (gathered) |*g| g.deinit();
        var ranked = if (gathered) |g| g.out else ranked: {
            var top: TopRows = try .init(ctx.gpa, meta0, output_specs, proj.row_keys, keep, args.max_memory / 2);
            defer top.deinit();
            for (items) |item| {
                const in = opened.inputs[item.file];
                const meta_i = &metas[item.file];
                const rg_i: usize = item.rg;
                const src_rg = &meta_i.row_groups.items[rg_i];
                if (filter_opt) |f| if (!args.scan_all) {
                    if (filter_prune.pruneRowGroup(src_rg, f, meta_i) == .skip) {
                        skipRowGroup(in, rg_i);
                        continue;
                    }
                };
                if (item.best) |best| if (top.cutsOff(best)) {
                    skipRowGroup(in, rg_i);
                    continue;
                };
                if (in.gate) |g| try g.wait(g.ctx, rg_i);
                defer skipRowGroup(in, rg_i);
                try top.offer(src_rg, meta_i, .{ .bytes = in.bytes, .byte_origin = 0 }, filter_opt, fetch_arr, .{
                    .file = item.file,
                    .rg = item.rg,
                }, &t.core, .{ .fast_levels = args.fast_levels, .expand_limit = args.max_memory });
            }
            break :ranked try top.ranked(arena);
        };
        const print_span = profile.Span.begin(prof);
        try flushAggregator(&ranked, names, &stdout_writer, is_jsonl, limit, &total_printed);
        print_span.end(.sink_write, profile.args(.{ranked.num_rows}));
    } else for (opened.inputs, metas, 0..) |in, meta_i, file_i| {
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
                    skipRowGroup(in, rg_i);
                    continue;
                }
            };
            if (in.gate) |g| try g.wait(g.ctx, rg_i);
            defer skipRowGroup(in, rg_i);

            // A row group too big to hold decoded prints a slice of rows at a time.
            const sliced: SlicedPrint = .{
                .names = names,
                .writer = &stdout_writer,
                .is_jsonl = is_jsonl,
                .limit = limit,
                .total_printed = &total_printed,
                .budget = args.max_memory / 2,
                .decode_options = .{ .fast_levels = args.fast_levels, .expand_limit = args.max_memory },
            };
            const printed = try printRowGroupSliced(
                ctx.gpa,
                &agg,
                src_rg,
                &meta_const,
                rg_src,
                filter_opt,
                fetch_arr,
                output_specs,
                sliced,
            );
            if (printed) continue;

            _ = consumer.appendProjectedRGWithOptions(
                &agg,
                ctx.gpa,
                src_rg,
                &meta_const,
                rg_src,
                filter_opt,
                fetch_arr,
                output_specs,
                &t.core,
                .{ .fast_levels = args.fast_levels, .expand_limit = args.max_memory },
            ) catch |err| {
                // Values that expand past the budget held whole may still print a slice of rows at a time.
                if (err != error.DecodeExceededBudget) return err;
                consumer.resetAggregator(&agg);
                var streamed = sliced;
                streamed.force = true;
                const ok = try printRowGroupSliced(
                    ctx.gpa,
                    &agg,
                    src_rg,
                    &meta_const,
                    rg_src,
                    filter_opt,
                    fetch_arr,
                    output_specs,
                    streamed,
                );
                if (!ok) return err;
                continue;
            };

            const print_span = profile.Span.begin(prof);
            try flushAggregator(&agg, names, &stdout_writer, is_jsonl, limit, &total_printed);
            print_span.end(.sink_write, profile.args(.{agg.num_rows}));
            consumer.resetAggregator(&agg);
        }
    }

    const finish_start = nowMonoNs();
    try opened.intact();
    if (format == .json) try stdout_writer.writeAll("]\n");
    if (format == .parquet) try parquet.finish(total_printed);
    try stdout_writer.flush();
    stdout_writer.output_ns += @intCast(nowMonoNs() - finish_start);
    _ = t_start;
    return .{ .rows_in = rows_in, .rows = total_printed, .bytes_out = if (format == .parquet) parquet.out_offset else stdout_writer.bytes_written, .output_ns = stdout_writer.output_ns, .membership = opened.membership, .timings = t };
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

    try rebaseFetchedOffsets(&meta, &fetch_arr, &ranges, &rg_columns, null);

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

    try rebaseFetchedOffsets(&meta, &fetch_arr, &ranges, &rg_columns, null);

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

    try rebaseFetchedOffsets(&meta, &fetch_arr, &ranges, &rg_columns, null);

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

test "regression: printCell scales a DECIMAL whose leaf carries only the logical type" {
    const testing = std.testing;
    var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_state.deinit();
    const a = arena_state.allocator();

    var c: consumer.OutputAggregator.TypedBuf(i128) = .{};
    try c.values.append(a, 125);
    try c.values.append(a, -350);
    const elem: schema.SchemaElement = .{
        .type = .INT32,
        .type_length = null,
        .repetition_type = .REQUIRED,
        .name = "d",
        .num_children = null,
        .scale = null,
        .precision = null,
        .field_id = null,
        .logical_type = .{ .DECIMAL = .{ .scale = 2, .precision = 5 } },
    };
    var buf: [64]u8 = undefined;
    var w: std.Io.Writer = .fixed(&buf);
    try printCell(&w, .{ .i128 = c }, 0, elem, true);
    try w.writeByte(' ');
    try printCell(&w, .{ .i128 = c }, 1, elem, true);
    try testing.expectEqualStrings("1.25 -3.50", w.buffered());
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

test "regression: a row group in flight holds the write window's bytes for its own size, at most the whole window" {
    var rgs: [4]schema.RowGroup = undefined;
    for (&rgs, [_]i64{ 0, 1, 33 << 20, 4 << 30 }) |*rg, size| {
        rg.* = .{ .columns = .empty, .total_byte_size = size, .num_rows = 1 };
    }
    const job = struct {
        fn of(rg: *const schema.RowGroup) RGJob {
            return .{ .bytes = &.{}, .meta = undefined, .rg = rg };
        }
    }.of;
    const unit: u64 = (256 << 20) / window_units;
    try std.testing.expectEqual(@as(usize, 1), jobUnits(job(&rgs[0]), unit, window_units));
    try std.testing.expectEqual(@as(usize, 1), jobUnits(job(&rgs[1]), unit, window_units));
    try std.testing.expectEqual(@as(usize, 132), jobUnits(job(&rgs[2]), unit, window_units));
    // Larger than the whole budget: admitted alone, never waiting for permits that do not exist.
    try std.testing.expectEqual(@as(usize, window_units), jobUnits(job(&rgs[3]), unit, window_units));
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

test "a footer suffix whose Content-Range total is smaller than its body is a bad response, not an underflow" {
    try std.testing.expectError(error.BadResponse, suffixStart(50, 100));
    try std.testing.expectEqual(@as(u64, 0), try suffixStart(100, 100));
    try std.testing.expectEqual(@as(u64, 900), try suffixStart(1000, 100));
}

test "a recompressing copy drops page-index offsets that are not its chunk's, instead of overflowing on them" {
    const fixture = "ci/fixtures/parquet/null_pages.parquet";
    std.Io.Dir.cwd().access(std.testing.io, fixture, .{}) catch return error.SkipZigTest;
    const gpa = std.testing.allocator;
    const ctx: Context = .{ .gpa = gpa, .env = .empty, .io = std.testing.io };
    var arena_state = std.heap.ArenaAllocator.init(gpa);
    defer arena_state.deinit();
    const a = arena_state.allocator();

    // The fixture, with every chunk's index_page_offset and its first OffsetIndex location at minInt(i64): both are
    // the file's word, which footer open does not check. The copy subtracted the chunk's start from them.
    const src = try metadata.readFileSlice(fixture, gpa);
    defer gpa.free(src);
    var meta = try metadata.open(a, src);
    const footer_len = std.mem.readInt(u32, src[src.len - 8 ..][0..4], .little);
    var file: std.ArrayList(u8) = .empty;
    try file.appendSlice(a, src[0 .. src.len - 8 - footer_len]);
    var oi_patched = false;
    for (meta.row_groups.items) |*rg| for (rg.columns.items) |*cc| {
        cc.meta_data.?.index_page_offset = std.math.minInt(i64);
        if (oi_patched) continue;
        const off: usize = @intCast(cc.offset_index_offset orelse continue);
        const len: usize = @intCast(cc.offset_index_length.?);
        var r = thrift.Reader.init(src[off..][0..len]);
        var oi = try schema.OffsetIndex.read(a, &r);
        oi.page_locations.items[0].offset = std.math.minInt(i64);
        var w = thrift.Writer.init(a);
        try oi.write(&w);
        cc.offset_index_offset = @intCast(file.items.len);
        cc.offset_index_length = @intCast(w.bytes().len);
        try file.appendSlice(a, w.bytes());
        oi_patched = true;
    };
    try std.testing.expect(oi_patched);
    var fw = thrift.Writer.init(a);
    try meta.write(&fw);
    try file.appendSlice(a, fw.bytes());
    var len_le: [4]u8 = undefined;
    std.mem.writeInt(u32, &len_le, @intCast(fw.bytes().len), .little);
    try file.appendSlice(a, &len_le);
    try file.appendSlice(a, "PAR1");

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var in_buf: [96]u8 = undefined;
    const in = try std.fmt.bufPrint(&in_buf, ".zig-cache/tmp/{s}/in.parquet", .{tmp.sub_path});
    try std.Io.Dir.cwd().writeFile(std.testing.io, .{ .sub_path = in, .data = file.items });
    var out_buf: [96]u8 = undefined;
    const out = try std.fmt.bufPrint(&out_buf, ".zig-cache/tmp/{s}/out.parquet", .{tmp.sub_path});

    var r = try runQuery(ctx, .{ .inputs = &.{in}, .output = out, .codec = .ZSTD });
    r.deinit(gpa);
    const agg = "count(*) AS n";
    var want = try runQuery(ctx, .{ .inputs = &.{fixture}, .aggregate = agg });
    defer want.aggregate.deinit(gpa);
    var got = try runQuery(ctx, .{ .inputs = &.{out}, .aggregate = agg });
    defer got.aggregate.deinit(gpa);
    try std.testing.expectEqualDeep(want.aggregate.output.aggs[0].value, got.aggregate.output.aggs[0].value);
}

test "an overflow or a zero divisor fails a query only on a row whose value it consumes" {
    // Row 500 holds 9e18 in p (plain) and a (dictionary-coded), whose doubles overflow, and 0 in the divisor d
    // (tools/gen_overflow_rows_fixture.py). Every query that leaves row 500 out answers as if it were absent: a WHERE
    // that keeps the selection dense, a later conjunct, an aggregate's own FILTER, the group-key path, the coded path.
    // Every one that takes it in still fails.
    const fixture = "ci/fixtures/parquet/overflow_rows.parquet";
    std.Io.Dir.cwd().access(std.testing.io, fixture, .{}) catch return error.SkipZigTest;
    const gpa = std.testing.allocator;
    const ctx: Context = .{ .gpa = gpa, .env = .empty, .io = std.testing.io };
    const Case = struct {
        aggregate: []const u8,
        filter: ?[]const u8 = null,
        group_by: ?[]const u8 = null,
        /// The first aggregate's value, or with a GROUP BY the number of groups.
        want: i128 = 0,
        err: ?anyerror = null,
    };
    const cases = [_]Case{
        .{ .aggregate = "sum(p * 2) AS s", .filter = "p < 1000", .want = 99_000 },
        .{ .aggregate = "sum(p * 2) AS s", .filter = "p != 9000000000000000000", .want = 99_000 },
        .{ .aggregate = "sum(a * 2) AS s", .filter = "id != 500", .want = 99_000 },
        .{ .aggregate = "sum(10 / d) AS s", .filter = "id != 500", .want = 3_429 },
        .{ .aggregate = "sum(p * 2) AS s", .filter = "id >= 0 AND id <= 999 AND id != 500", .want = 99_000 },
        .{ .aggregate = "sum(p * 2) FILTER (WHERE id != 500) AS s, count(*) AS n", .want = 99_000 },
        .{ .aggregate = "count(*) AS n", .group_by = "p * 2 AS k", .filter = "id != 500", .want = 100 },
        .{ .aggregate = "count(*) AS n", .group_by = "a * 2 AS k", .filter = "id != 500", .want = 100 },
        .{ .aggregate = "count(*) AS n", .group_by = "10 / d AS k", .filter = "id != 500", .want = 5 },
        // Consumed: still an error.
        .{ .aggregate = "sum(p * 2) AS s", .err = error.IntegerOverflowMul },
        .{ .aggregate = "sum(a * 2) AS s", .filter = "id >= 0", .err = error.IntegerOverflowMul },
        .{ .aggregate = "sum(10 / d) AS s", .err = error.DivisionByZero },
        .{ .aggregate = "sum(p * 2) AS s", .filter = "id >= 0 AND id <= 999", .err = error.IntegerOverflowMul },
        .{ .aggregate = "sum(p * 2) FILTER (WHERE id >= 0) AS s", .err = error.IntegerOverflowMul },
        .{ .aggregate = "count(*) AS n", .group_by = "p * 2 AS k", .err = error.IntegerOverflowMul },
        .{ .aggregate = "count(*) AS n", .group_by = "10 / d AS k", .filter = "id >= 0", .err = error.DivisionByZero },
    };
    for (cases) |c| {
        const args: QueryArgs = .{
            .inputs = &.{fixture},
            .aggregate = c.aggregate,
            .filter = c.filter,
            .group_by = c.group_by,
        };
        if (c.err) |want_err| {
            try std.testing.expectError(want_err, runQuery(ctx, args));
            continue;
        }
        var r = try runQuery(ctx, args);
        defer r.deinit(gpa);
        const got: i128 = if (c.group_by != null)
            @intCast(r.aggregate.output.group_rows.?.len)
        else
            r.aggregate.output.aggs[0].value.i;
        std.testing.expectEqual(c.want, got) catch |err| {
            std.debug.print("case: {s} where {?s} by {?s}\n", .{ c.aggregate, c.filter, c.group_by });
            return err;
        };
    }
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
    try rebaseFetchedOffsets(&meta, &fetch_arr, &ranges, &rg_columns, null);
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

test "regression: an input with no row groups first keeps the re-encode guards of the inputs after it" {
    const dir = "ci/fixtures/parquet/";
    std.Io.Dir.cwd().access(std.testing.io, dir ++ "empty_first.parquet", .{}) catch return error.SkipZigTest;
    const gpa = std.testing.allocator;
    const ctx: Context = .{ .gpa = gpa, .env = .empty, .io = std.testing.io };
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var path_buf: [96]u8 = undefined;
    const out = try std.fmt.bufPrint(&path_buf, ".zig-cache/tmp/{s}/out.parquet", .{tmp.sub_path});

    // A LIST column re-encoded drops its repetition levels: refused, as it is for the second input alone.
    try std.testing.expectError(error.NestedReencodeNotSupported, runQuery(ctx, .{
        .inputs = &.{ dir ++ "empty_first.parquet", dir ++ "empty_first_list.parquet" },
        .output = out,
        .filter = "id > 2",
    }));
    // A FLOAT16 column's footer widens to DOUBLE, so a fully matching row group is re-encoded, never copied as FLBA.
    var w = try runQuery(ctx, .{
        .inputs = &.{ dir ++ "empty_first_f16.parquet", dir ++ "float16_rows.parquet" },
        .output = out,
        .filter = "id > 2",
    });
    defer w.deinit(gpa);
    var want = try runQuery(ctx, .{
        .inputs = &.{dir ++ "float16_rows.parquet"},
        .aggregate = "sum(h) AS s",
        .filter = "id > 2",
    });
    defer want.deinit(gpa);
    var got = try runQuery(ctx, .{ .inputs = &.{out}, .aggregate = "sum(h) AS s" });
    defer got.deinit(gpa);
    try std.testing.expectEqual(want.aggregate.output.aggs[0].value.f, got.aggregate.output.aggs[0].value.f);
}

test "regression: inputs whose legacy DECIMAL scales differ are refused on every path" {
    const dir = "ci/fixtures/parquet/";
    std.Io.Dir.cwd().access(std.testing.io, dir ++ "legacy_decimal_s2.parquet", .{}) catch return error.SkipZigTest;
    const gpa = std.testing.allocator;
    const ctx: Context = .{ .gpa = gpa, .env = .empty, .io = std.testing.io };
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var path_buf: [96]u8 = undefined;
    const out = try std.fmt.bufPrint(&path_buf, ".zig-cache/tmp/{s}/out.parquet", .{tmp.sub_path});
    const inputs: []const []const u8 = &.{ dir ++ "legacy_decimal_s2.parquet", dir ++ "legacy_decimal_s4.parquet" };

    const refused = [_]QueryArgs{
        .{ .inputs = inputs, .aggregate = "sum(value) AS s" },
        .{ .inputs = inputs, .output = out },
        .{ .inputs = inputs, .output = out, .filter = "value > 1" },
    };
    for (refused) |args| try std.testing.expectError(error.SchemaMismatch, runQuery(ctx, args));
    // The same file twice still reads as one schema.
    var r = try runQuery(ctx, .{ .inputs = &.{ inputs[0], inputs[0] }, .aggregate = "count(*) AS n" });
    defer r.deinit(gpa);
    try std.testing.expectEqual(@as(i128, 48), r.aggregate.output.aggs[0].value.i);
}

/// The answer's group rows as `v,v,...;` lines, sorted, so a test reads the same text whatever the group order.
fn sortedGroupText(gpa: std.mem.Allocator, ctx: Context, args: QueryArgs) ![]u8 {
    var r = try runQuery(ctx, args);
    defer r.deinit(gpa);
    var lines: std.ArrayList([]u8) = .empty;
    defer {
        for (lines.items) |l| gpa.free(l);
        lines.deinit(gpa);
    }
    for (r.aggregate.output.group_rows orelse &.{}) |row| {
        var aw: std.Io.Writer.Allocating = .init(gpa);
        errdefer aw.deinit();
        for (row) |v| {
            try writeAggValueJson(&aw.writer, v);
            try aw.writer.writeByte(',');
        }
        try lines.append(gpa, try aw.toOwnedSlice());
    }
    std.mem.sort([]u8, lines.items, {}, struct {
        fn lt(_: void, a: []u8, b: []u8) bool {
            return std.mem.lessThan(u8, a, b);
        }
    }.lt);
    var out: std.ArrayList(u8) = .empty;
    for (lines.items) |l| {
        try out.appendSlice(gpa, l);
        try out.append(gpa, ';');
    }
    return out.toOwnedSlice(gpa);
}

test "regression: grouped aggregates skip a computed argument's NULLs, coded or not" {
    const fixture = "ci/fixtures/parquet/nullable_mix.parquet";
    std.Io.Dir.cwd().access(std.testing.io, fixture, .{}) catch return error.SkipZigTest;
    const gpa = std.testing.allocator;
    // a = [1, -, -, 4, -, 6], b = [10, 20, -, -, -, 60], g = 1 1 2 2 3 3; the strings s and t have NULLs likewise.
    for ([_]bool{ true, false }) |encoded| {
        const ctx: Context = .{ .gpa = gpa, .env = .empty, .io = std.testing.io };
        const got = try sortedGroupText(gpa, ctx, .{
            .inputs = &.{fixture},
            .aggregate = "count(coalesce(a, b)) AS n, min(coalesce(a, b)) AS m, count(coalesce(s, t)) AS c, " ++
                "min(coalesce(s, t)) AS ms, count(strlen(s)) AS l",
            .group_by = "g",
            .encoded = encoded,
        });
        defer gpa.free(got);
        try std.testing.expectEqualStrings("1,2,1,2,\"q\",1,;2,1,4,1,\"y\",1,;3,1,6,1,\"z\",1,;", got);
    }
}

test "regression: arithmetic refuses only the NULLs it would read" {
    const fixture = "ci/fixtures/parquet/nullable_mix.parquet";
    std.Io.Dir.cwd().access(std.testing.io, fixture, .{}) catch return error.SkipZigTest;
    const gpa = std.testing.allocator;
    const ctx: Context = .{ .gpa = gpa, .env = .empty, .io = std.testing.io };
    // NULLs a WHERE or FILTER leaves out are no operand: arithmetic over the rest is well defined.
    var r = try runQuery(ctx, .{
        .inputs = &.{fixture},
        .aggregate = "sum(i32n + 5) AS s, sum(i32n * 2) FILTER (WHERE i32n IS NOT NULL) AS f",
        .filter = "i32n IS NOT NULL",
    });
    defer r.deinit(gpa);
    try std.testing.expectEqual(@as(i128, 35), r.aggregate.output.aggs[0].value.i);
    try std.testing.expectEqual(@as(i128, 30), r.aggregate.output.aggs[1].value.i);
    var cw = try runQuery(ctx, .{
        .inputs = &.{fixture},
        .aggregate = "sum(CASE WHEN i32n IS NOT NULL THEN i32n + 1 ELSE 0 END) AS s",
    });
    defer cw.deinit(gpa);
    try std.testing.expectEqual(@as(i128, 19), cw.aggregate.output.aggs[0].value.i);
    // A NULL that arithmetic would read is refused, a column's or a computed one's.
    try std.testing.expectError(error.NullableNotSupported, runQuery(ctx, .{
        .inputs = &.{fixture},
        .aggregate = "sum(i32n * 2) AS s",
    }));
    try std.testing.expectError(error.NullableNotSupported, runQuery(ctx, .{
        .inputs = &.{fixture},
        .aggregate = "sum(coalesce(a, b) + 1) AS s",
    }));
}

test "regression: an INT96 past what 64-bit nanoseconds hold reads, compares and aggregates exactly" {
    const dir = "ci/fixtures/parquet/";
    std.Io.Dir.cwd().access(std.testing.io, dir ++ "int96_year1.parquet", .{}) catch return error.SkipZigTest;
    const gpa = std.testing.allocator;
    const ctx: Context = .{ .gpa = gpa, .env = .empty, .io = std.testing.io };
    const Want = struct { file: []const u8, aggregate: []const u8, filter: ?[]const u8 = null, values: []const i128 };
    for ([_]Want{
        // `ts` holds 0001-01-01 00:00:00.764, which wrapped to 1754-08-30, then was refused.
        .{ .file = "int96_year1.parquet", .aggregate = "min(ts) AS m, max(ts) AS x", .values = &.{
            -62135596036000000000, 9223372036000000000,
        } },
        .{ .file = "int96_year1.parquet", .aggregate = "count(*) AS n", .filter = "ts < '1700-01-01'", .values = &.{
            1,
        } },
        .{
            .file = "int96_year1.parquet",
            .aggregate = "count(*) AS n",
            .filter = "ts = -62135596036000000000",
            .values = &.{1},
        },
        // Spark's: 9999-12-31 03:00 among 2024 dates, and a day count past any calendar.
        .{ .file = "int96_from_spark.parquet", .aggregate = "min(a) AS m, max(a) AS x, count(a) AS n", .values = &.{
            1704070800000000000, 361727810693890448384000, 5,
        } },
        .{ .file = "int96_from_spark.parquet", .aggregate = "count(*) AS n", .filter = "a > '2262-12-31'", .values = &.{
            2,
        } },
        // `dt3` holds `dt2`'s seconds as nanoseconds, back to 0000-01-01: 1678 of 1971 rows wrapped.
        .{ .file = "int96_bug4903.parquet", .aggregate = "sum(dt3) AS s, min(dt3) AS m", .values = &.{
            -61265237128709000000000, -62167402800000000000,
        } },
        .{ .file = "int96_bug4903.parquet", .aggregate = "sum(dt2) AS s, min(dt2) AS m", .values = &.{
            -61265237128709, -62167402800,
        } },
    }) |w| {
        var buf: [96]u8 = undefined;
        const path = try std.fmt.bufPrint(&buf, dir ++ "{s}", .{w.file});
        var r = try runQuery(ctx, .{ .inputs = &.{path}, .aggregate = w.aggregate, .filter = w.filter });
        defer r.deinit(gpa);
        for (w.values, r.aggregate.output.aggs[0..w.values.len]) |want, got| {
            const v: i128 = switch (got.value) {
                .i => |x| x,
                .dec => |d| d.v,
                else => return error.TestUnexpectedResult,
            };
            try std.testing.expectEqual(want, v);
        }
    }
    // Written out, an INT96 key stays an INT64 TIMESTAMP(NANOS) where 64 bits hold it, and is refused where not.
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var obuf: [96]u8 = undefined;
    const out = try std.fmt.bufPrint(&obuf, ".zig-cache/tmp/{s}/keys.parquet", .{tmp.sub_path});
    const y1: []const []const u8 = &.{dir ++ "int96_year1.parquet"};
    try std.testing.expectError(error.Int96OutOfRange, runQuery(ctx, .{
        .inputs = y1,
        .group_by = "ts",
        .aggregate = "count(*) AS n",
        .output = out,
    }));
    var written = try runQuery(ctx, .{
        .inputs = y1,
        .group_by = "ts",
        .aggregate = "count(*) AS n",
        .filter = "ts >= '1900-01-01' AND ts < '2262-01-01'",
        .output = out,
    });
    written.deinit(gpa);
    const bytes = try metadata.readFileSlice(out, gpa);
    defer gpa.free(bytes);
    var meta = try metadata.open(gpa, bytes);
    defer meta.deinit(gpa);
    const key = meta.schema.items[1];
    try std.testing.expectEqual(schema.Type.INT64, key.type.?);
    try std.testing.expect(key.logical_type.? == .TIMESTAMP);
}

test "regression: a definition level past its column's maximum fails the queries reading it" {
    const dir = "ci/fixtures/parquet/";
    std.Io.Dir.cwd().access(std.testing.io, dir ++ "def_level_past_max.parquet", .{}) catch return error.SkipZigTest;
    const gpa = std.testing.allocator;
    const ctx: Context = .{ .gpa = gpa, .env = .empty, .io = std.testing.io };
    // `st.a` (max_def 2) holds definition level 3: its rows read as present zeros, `count(st.a)` 8.
    const def3: []const []const u8 = &.{dir ++ "def_level_past_max.parquet"};
    for ([_][]const u8{ "count(st.a) AS n", "sum(st.a) AS n" }) |agg| {
        try std.testing.expectError(error.LevelOutOfRange, runQuery(ctx, .{ .inputs = def3, .aggregate = agg }));
    }
}

test "regression: a dictionary-encoded INT32 or INT64 DECIMAL groups on the coded path as on the plain one" {
    const fixture = "ci/fixtures/parquet/decimal_int_dictionary.parquet";
    std.Io.Dir.cwd().access(std.testing.io, fixture, .{}) catch return error.SkipZigTest;
    const gpa = std.testing.allocator;
    const ctx: Context = .{ .gpa = gpa, .env = .empty, .io = std.testing.io };
    for ([_][]const u8{ "p4s4", "p9s2", "p18s4" }) |key| {
        for ([_]?[]const u8{ null, "id > 100" }) |filter| {
            var texts: [2][]u8 = undefined;
            for ([_]bool{ true, false }, &texts) |encoded, *t| t.* = try sortedGroupText(gpa, ctx, .{
                .inputs = &.{fixture},
                .aggregate = "count(*) AS n, sum(p9s2) AS s",
                .group_by = key,
                .filter = filter,
                .encoded = encoded,
            });
            defer for (texts) |t| gpa.free(t);
            try std.testing.expectEqualStrings(texts[1], texts[0]);
        }
    }
}

test "regression: a BYTE_ARRAY DECIMAL past 38 digits prints, or is refused for Parquet, and precision 0 is malformed" {
    const dir = "ci/fixtures/parquet/";
    std.Io.Dir.cwd().access(std.testing.io, dir ++ "decimal_precision40.parquet", .{}) catch return error.SkipZigTest;
    const gpa = std.testing.allocator;
    const ctx: Context = .{ .gpa = gpa, .env = .empty, .io = std.testing.io };
    const p40: []const []const u8 = &.{dir ++ "decimal_precision40.parquet"};
    // Text output reads the column as a double (`initOutputAggregator`); coercing its schema to FLBA panicked.
    var out: std.Io.Writer.Allocating = .init(gpa);
    defer out.deinit();
    const printed = try runPrintSink(ctx, .{ .inputs = p40 }, .jsonl, 3, -1, 0, &out.writer);
    try std.testing.expectEqual(@as(usize, 3), printed.rows);
    var bin: std.Io.Writer.Allocating = .init(gpa);
    defer bin.deinit();
    var diag: Diag = .{};
    try std.testing.expectError(
        error.UnsupportedDecimalPrecision,
        runPrintSink(ctx, .{ .inputs = p40, .diag = &diag }, .parquet, 3, -1, 0, &bin.writer),
    );
    try std.testing.expectEqualStrings("value", diag.column.get());
    try std.testing.expectError(
        error.NotParquet,
        runQuery(ctx, .{ .inputs = &.{dir ++ "decimal_precision0.parquet"}, .aggregate = "count(*) AS n" }),
    );
}

test "regression: a V2 page whose header miscounts its nulls reads by its levels, as other readers read it" {
    const path = "ci/fixtures/parquet/v2_num_nulls_lies.parquet";
    std.Io.Dir.cwd().access(std.testing.io, path, .{}) catch return error.SkipZigTest;
    const gpa = std.testing.allocator;
    const ctx: Context = .{ .gpa = gpa, .env = .empty, .io = std.testing.io };
    // `i64`'s first page header says 8 nulls; its levels hold 9. Writers have miscounted the field, and it was
    // refused; the levels are the answer.
    for ([_]bool{ false, true }) |fast| {
        var r = try runQuery(ctx, .{ .inputs = &.{path}, .aggregate = "count(i64) AS n", .fast_levels = fast });
        defer r.deinit(gpa);
        try std.testing.expectEqual(@as(i128, 271), r.aggregate.output.aggs[0].value.i);
    }
}

test "regression: a def-level run claiming bytes past its stream is refused, not read as extra NULLs" {
    const ctx: Context = .{ .gpa = std.testing.allocator, .env = .empty, .io = std.testing.io };
    // One byte of `i64`'s def stream made a bit-packed run longer than the stream, in a V2 page (42 of 271 values
    // read as NULL) and a V1 one (7), which has no null count to check.
    for ([_][]const u8{ "v2_def_run_corrupt", "v1_def_run_corrupt" }) |name| {
        var buf: [128]u8 = undefined;
        const path = try std.fmt.bufPrint(&buf, "ci/fixtures/parquet/{s}.parquet", .{name});
        std.Io.Dir.cwd().access(std.testing.io, path, .{}) catch return error.SkipZigTest;
        for ([_]bool{ false, true }) |fast| try std.testing.expectError(
            error.UnexpectedPage,
            runQuery(ctx, .{ .inputs = &.{path}, .aggregate = "count(i64) AS n", .fast_levels = fast }),
        );
    }
}

test "regression: a DELTA_BYTE_ARRAY value of another length than its FIXED_LEN_BYTE_ARRAY column is refused" {
    const path = "ci/fixtures/parquet/dba_flba_len6.parquet";
    std.Io.Dir.cwd().access(std.testing.io, path, .{}) catch return error.SkipZigTest;
    const ctx: Context = .{ .gpa = std.testing.allocator, .env = .empty, .io = std.testing.io };
    // One byte of `fb`'s suffix lengths makes a value 6 bytes long in an FLBA(4) column.
    try std.testing.expectError(
        error.UnexpectedPage,
        runQuery(ctx, .{ .inputs = &.{path}, .aggregate = "count(*) AS n", .filter = "fb != 'abcd'" }),
    );
}

test "regression: the local reader reads the file mapped, even once another is renamed over its path" {
    if (@import("builtin").os.tag != .linux) return error.SkipZigTest;
    const gpa = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var buf: [256]u8 = undefined;
    const path = try std.fmt.bufPrintSentinel(&buf, ".zig-cache/tmp/{s}/f.parquet", .{tmp.sub_path}, 0);
    var obuf: [256]u8 = undefined;
    const other = try std.fmt.bufPrintSentinel(&obuf, ".zig-cache/tmp/{s}/g.parquet", .{tmp.sub_path}, 0);
    const first = "PAR1 the file mapped PAR1";
    for ([_]local_reader.Mode{ .pread, .uring }) |mode| {
        for ([_][:0]const u8{ path, other }, [_][]const u8{ first, "PAR1 its replacement PAR1" }) |p, text| {
            const fd = try local_fs.createFile(p);
            defer local_fs.close(fd);
            try local_fs.writeAll(fd, text);
        }
        const m = try local_fs.mapFile(path);
        defer m.unmap();
        // Between the open that read the footer and the reads, as a tool replaces a file. The reader opened the path
        // again and read the replacement's bytes against the first file's footer.
        try std.testing.expectEqual(std.posix.E.SUCCESS, std.posix.errno(std.posix.system.rename(other, path)));
        var arena_state = std.heap.ArenaAllocator.init(gpa);
        defer arena_state.deinit();
        const arena = arena_state.allocator();
        const inputs = [_]scan.Input{.{ .name = path, .bytes = m.bytes, .logical_size = m.bytes.len }};
        const mmaps = [_]?local_fs.Mapped{m};
        const opened: OpenedInputs = .{
            .gpa = gpa,
            .inputs = &inputs,
            .metas = &.{},
            .meta_ready = &.{},
            .mmaps = &mmaps,
            .owned_pools = &.{},
            .s3_buffers = &.{},
        };
        var whole = [_]local_reader.Range{.{ .start = 0, .end = m.bytes.len }};
        const per_rg = [_]?[]local_reader.Range{&whole};
        const plan: LocalPlan = .{ .files = &.{&per_rg}, .planned = m.bytes.len, .total = m.bytes.len };
        var reader: ?*local_reader.Reader = null;
        defer if (reader) |rd| rd.destroy();
        const ctx: Context = .{ .gpa = gpa, .env = .empty, .io = std.testing.io };
        const read = try planLocalReader(ctx, arena, opened, plan, .{ .mode = mode, .threads = 1 }, &reader);
        try reader.?.start();
        const gate = read[0].gate.?;
        try gate.wait(gate.ctx, 0);
        try std.testing.expectEqualStrings(first, read[0].bytes);
        gate.done(gate.ctx, 0, 1);
        try opened.intact();
    }
}

test "regression: more local inputs than the open-file limit allows still answer" {
    const posix = std.posix;
    if (posix.rlimit_resource == void) return error.SkipZigTest;
    const fixture = "ci/fixtures/parquet/nullable_mix.parquet";
    std.Io.Dir.cwd().access(std.testing.io, fixture, .{}) catch return error.SkipZigTest;
    const gpa = std.testing.allocator;
    const bytes = try metadata.readFileSlice(fixture, gpa);
    defer gpa.free(bytes);
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var arena_state = std.heap.ArenaAllocator.init(gpa);
    defer arena_state.deinit();
    const arena = arena_state.allocator();
    const n = 300;
    const paths = try arena.alloc([]const u8, n);
    for (paths, 0..) |*p, i| {
        p.* = try std.fmt.allocPrint(arena, ".zig-cache/tmp/{s}/f{d}.parquet", .{ tmp.sub_path, i });
        const fd = try local_fs.createFile(p.*);
        defer local_fs.close(fd);
        try local_fs.writeAll(fd, bytes);
    }
    // Each mapping held its descriptor to the query's end: past the limit the opens failed, where every file was
    // read before mappings held them. A host's limit can be this low; libzpq does not raise it.
    const before = try posix.getrlimit(.NOFILE);
    defer posix.setrlimit(.NOFILE, before) catch {};
    try posix.setrlimit(.NOFILE, .{ .cur = 64, .max = before.max });
    var one = try runQuery(.{ .gpa = gpa, .env = .empty, .io = std.testing.io }, .{
        .inputs = paths[0..1],
        .aggregate = "count(*) AS n",
    });
    const per_file = one.aggregate.output.aggs[0].value.i;
    one.deinit(gpa);
    for ([_][*:0]const u8{ "ZPQ_IO_PROFILE=auto", "ZPQ_IO_PROFILE=mmap" }) |line| {
        const lines = [_:null]?[*:0]const u8{line};
        const env: std.process.Environ = .{ .block = .{ .slice = &lines } };
        var res = try runQuery(.{ .gpa = gpa, .env = env, .io = std.testing.io }, .{
            .inputs = paths,
            .aggregate = "count(*) AS n",
        });
        defer res.deinit(gpa);
        try std.testing.expectEqual(per_file * n, res.aggregate.output.aggs[0].value.i);
    }
}

test "regression: a UINT_8 value past 8 bits is refused, not zero-extended" {
    const path = "ci/fixtures/parquet/uint8_out_of_range.parquet";
    std.Io.Dir.cwd().access(std.testing.io, path, .{}) catch return error.SkipZigTest;
    const ctx: Context = .{ .gpa = std.testing.allocator, .env = .empty, .io = std.testing.io };
    // One `u8` value holds 52314 in its INT32: it read as that, where its 8 bits say 90.
    for ([_]?[]const u8{ null, "u8 > 0" }) |filter| try std.testing.expectError(
        error.IntOutOfRange,
        runQuery(ctx, .{ .inputs = &.{path}, .aggregate = "max(u8) AS m", .filter = filter }),
    );
    // The other columns of the file still read.
    var ok = try runQuery(ctx, .{ .inputs = &.{path}, .aggregate = "count(*) AS n, max(i32) AS m" });
    defer ok.deinit(std.testing.allocator);
}

test "regression: a FIXED_LEN_BYTE_ARRAY of length 0 is malformed, not a column of empty strings" {
    const path = "ci/fixtures/parquet/flba_length0.parquet";
    std.Io.Dir.cwd().access(std.testing.io, path, .{}) catch return error.SkipZigTest;
    const ctx: Context = .{ .gpa = std.testing.allocator, .env = .empty, .io = std.testing.io };
    // Seven FLBA(4) values whose footer says type_length 0; they read as seven "".
    try std.testing.expectError(
        error.NotParquet,
        runQuery(ctx, .{ .inputs = &.{path}, .aggregate = "count(*) AS n", .filter = "fb = ''" }),
    );
}

test "regression: DELTA_BYTE_ARRAY values are charged where they decode, and a row group past it streams" {
    const path = "ci/fixtures/parquet/dba_pages.parquet";
    std.Io.Dir.cwd().access(std.testing.io, path, .{}) catch return error.SkipZigTest;
    const gpa = std.testing.allocator;
    const ctx: Context = .{ .gpa = gpa, .env = .empty, .io = std.testing.io };
    // 6.6 KiB: 1500 values of 100 kB over 30 pages of 5 MB each, in a chunk whose footer says 3 MB.
    for ([_]usize{ 1, 3 }) |j| {
        // A page past the whole budget is refused wherever its values decode, and is no GROUP BY's.
        try std.testing.expectError(error.DecodeExceededBudget, runQuery(ctx, .{
            .inputs = &.{path},
            .aggregate = "count(*) AS n",
            .filter = "s = 'x'",
            .parallelism = j,
            .max_memory = 2 << 20,
        }));
        for ([_]usize{ 2 << 20, 8 << 20 }) |budget| {
            // count(s) reads levels alone and never builds a value: it was refused for them.
            var counted = try runQuery(ctx, .{
                .inputs = &.{path},
                .aggregate = "count(s) AS n",
                .parallelism = j,
                .max_memory = budget,
            });
            defer counted.deinit(gpa);
            try std.testing.expectEqual(@as(i128, 1500), counted.aggregate.output.aggs[0].value.i);
        }
        // Read whole, the chunk's pages together pass the budget; a slice at a time they do not.
        var res = try runQuery(ctx, .{
            .inputs = &.{path},
            .aggregate = "count(*) AS n",
            .filter = "s = 'x'",
            .parallelism = j,
            .max_memory = 8 << 20,
        });
        defer res.deinit(gpa);
        try std.testing.expectEqual(@as(i128, 0), res.aggregate.output.aggs[0].value.i);
    }
    // Printed rows decode the same values, and took no budget at all.
    for ([_]usize{ 2 << 20, 64 << 20 }) |budget| {
        var out: std.Io.Writer.Allocating = .init(gpa);
        defer out.deinit();
        const print_args: QueryArgs = .{ .inputs = &.{path}, .filter = "s = 'x'", .max_memory = budget };
        const printed = runPrintSink(ctx, print_args, .jsonl, null, -1, 0, &out.writer);
        if (budget == 2 << 20) try std.testing.expectError(error.DecodeExceededBudget, printed) else {
            try std.testing.expectEqual(@as(usize, 0), (try printed).rows);
        }
    }
}

test "regression: a write recompressing a page past its chunk refuses it, as a read does" {
    const path = "ci/fixtures/parquet/page_bomb.parquet";
    std.Io.Dir.cwd().access(std.testing.io, path, .{}) catch return error.SkipZigTest;
    const gpa = std.testing.allocator;
    const ctx: Context = .{ .gpa = gpa, .env = .empty, .io = std.testing.io };
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var buf: [256]u8 = undefined;
    const out = try std.fmt.bufPrint(&buf, ".zig-cache/tmp/{s}/out.parquet", .{tmp.sub_path});
    // Its one page claims 1 GiB in a chunk the footer gives 32 KiB: the byte copy decompressed it whole to recompress
    // it, at any budget, serially or a page per worker.
    for ([_]schema.CompressionCodec{ .SNAPPY, .UNCOMPRESSED }) |codec| for ([_]usize{ 1, 3 }) |j| {
        try std.testing.expectError(error.PageLargerThanChunk, runQuery(ctx, .{
            .inputs = &.{path},
            .output = out,
            .codec = codec,
            .parallelism = j,
            .max_memory = 16 << 20,
        }));
    };
}

test "regression: a page inflating past its chunk, or values expanding past the budget, are refused" {
    const dir = "ci/fixtures/parquet/";
    std.Io.Dir.cwd().access(std.testing.io, dir ++ "page_bomb.parquet", .{}) catch return error.SkipZigTest;
    const gpa = std.testing.allocator;
    const ctx: Context = .{ .gpa = gpa, .env = .empty, .io = std.testing.io };
    // A 32 KiB file whose footer says one row and 32 KiB; its one page inflates to 1 GiB.
    var diag: Diag = .{};
    try std.testing.expectError(error.PageLargerThanChunk, runQuery(ctx, .{
        .inputs = &.{dir ++ "page_bomb.parquet"},
        .aggregate = "count(*) AS n",
        .filter = "x = 1",
        .max_memory = 16 << 20,
        .diag = &diag,
    }));
    // 442 bytes of DELTA_BYTE_ARRAY: 60 values of 100 kB, each its predecessor as a prefix.
    const dba: []const []const u8 = &.{dir ++ "dba_expand.parquet"};
    try std.testing.expectError(error.DecodeExceededBudget, runQuery(ctx, .{
        .inputs = dba,
        .aggregate = "count(*) AS n",
        .filter = "s = 'x'",
        .max_memory = 1 << 20,
    }));
    var r = try runQuery(ctx, .{ .inputs = dba, .aggregate = "count(*) AS n", .filter = "s = 'x'" });
    defer r.deinit(gpa);
    try std.testing.expectEqual(@as(i128, 0), r.aggregate.output.aggs[0].value.i);
}

test "every write path records a row group's uncompressed bytes as its total_byte_size" {
    const fixture = "ci/fixtures/parquet/bloom_keys.parquet";
    std.Io.Dir.cwd().access(std.testing.io, fixture, .{}) catch return error.SkipZigTest;
    const gpa = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var path_buf: [96]u8 = undefined;
    const out = try std.fmt.bufPrint(&path_buf, ".zig-cache/tmp/{s}/out.parquet", .{tmp.sub_path});
    const Case = struct {
        filter: ?[]const u8 = null,
        columns: ?[]const []const u8 = null,
        codec: ?schema.CompressionCodec = null,
    };
    // A filter re-encodes row group 0 and copies the rest whole; --columns copies chunks one by one; --codec
    // recompresses pages. The snappy fixture compresses, so the uncompressed and compressed sums differ everywhere.
    const cases = [_]Case{
        .{ .filter = "id >= 2" },
        .{ .columns = &.{ "id", "x" } },
        .{ .codec = .ZSTD },
        .{ .filter = "id >= 2", .columns = &.{ "x", "cat" } },
    };
    for (cases) |c| {
        var r = try runQuery(.{ .gpa = gpa, .env = .empty, .io = std.testing.io }, .{
            .inputs = &.{fixture},
            .output = out,
            .filter = c.filter,
            .columns = c.columns,
            .codec = c.codec,
        });
        defer r.deinit(gpa);
        var arena_state = std.heap.ArenaAllocator.init(gpa);
        defer arena_state.deinit();
        const bytes = try metadata.readFileSlice(out, arena_state.allocator());
        const meta = try metadata.open(arena_state.allocator(), bytes);
        try std.testing.expect(meta.row_groups.items.len > 0);
        for (meta.row_groups.items) |rg| {
            var compressed: i64 = 0;
            for (rg.columns.items) |col| compressed += col.meta_data.?.total_compressed_size;
            try std.testing.expectEqual(fastpath.uncompressedBytes(rg.columns.items), rg.total_byte_size);
            try std.testing.expect(rg.total_byte_size != compressed);
        }
    }
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

/// Armed by the footer-ownership test so the next `s3_buffers` growth returns `error.OutOfMemory`.
var test_arm_s3_buffer_fail: ?*std.testing.FailingAllocator = null;

test "moving a footer buffer into the scan has one owner when the next append fails" {
    const gpa = std.testing.allocator;
    const bytes = metadata.readFileSlice("ci/fixtures/parquet/group_order.parquet", gpa) catch return error.SkipZigTest;
    defer gpa.free(bytes);

    var fake: fake_s3.FakeS3 = undefined;
    try fake.start(gpa);
    defer fake.deinit();
    const nfiles = 16;
    var keys: [nfiles][]const u8 = undefined;
    for (&keys, 0..) |*k, i| {
        k.* = try std.fmt.allocPrint(gpa, "t/f{d}.parquet", .{i});
        errdefer gpa.free(k.*);
        try fake.serveBytes(k.*, bytes);
    }
    defer for (keys) |k| gpa.free(k);

    var backing = std.heap.ArenaAllocator.init(gpa);
    defer backing.deinit();
    // Remap would grow the buffer list without an alloc. Refuse it so the growth is an alloc the test can fail.
    var fa = std.testing.FailingAllocator.init(backing.allocator(), .{ .resize_fail_index = 0 });
    test_arm_s3_buffer_fail = &fa;
    defer test_arm_s3_buffer_fail = null;

    var arena_state = std.heap.ArenaAllocator.init(gpa);
    defer arena_state.deinit();
    const a = arena_state.allocator();
    const endpoint = try std.fmt.allocPrintSentinel(a, "S3_ENDPOINT_URL=http://127.0.0.1:{d}", .{fake.port}, 0);
    const env = [_:null]?[*:0]const u8{ endpoint, "AWS_REGION=us-east-1", "AWS_ACCESS_KEY_ID=ak", "AWS_SECRET_ACCESS_KEY=sk" };
    const urls = try a.alloc([]const u8, nfiles);
    for (keys, urls) |k, *u| u.* = try std.fmt.allocPrint(a, "s3://bkt/{s}", .{k});
    const q: QueryArgs = .{ .inputs = urls, .aggregate = "count(*) AS n", .max_memory = 16 << 20 };
    const ctx: Context = .{ .gpa = gpa, .env = .{ .block = .{ .slice = &env } }, .io = std.testing.io };
    try std.testing.expectError(error.OutOfMemory, openInputs(ctx, fa.allocator(), q, .{ .limit = 64 << 20 }));
    try std.testing.expect(fa.has_induced_failure);
}

test "an S3 footer is one suffix GET, and a bad leading magic fails only when those bytes were fetched" {
    const gpa = std.testing.allocator;
    const bytes = metadata.readFileSlice("ci/fixtures/parquet/group_order.parquet", gpa) catch return error.SkipZigTest;
    defer gpa.free(bytes);
    // 16 KiB covers this fixture's footer and not offset 0. 64 bytes does not cover the footer.
    try std.testing.expect(bytes.len > 16 * 1024);

    var corrupt = try gpa.dupe(u8, bytes);
    defer gpa.free(corrupt);
    corrupt[0] = 'X';

    var fake: fake_s3.FakeS3 = undefined;
    try fake.start(gpa);
    defer fake.deinit();
    try fake.serveBytes("t/ok.parquet", bytes);
    try fake.serveBytes("t/bad.parquet", corrupt);
    const q: QueryArgs = .{ .inputs = &.{}, .aggregate = "count(*) AS n" };

    const one = try fakeS3AggregateJsonEnv(gpa, &fake, &.{"t/ok.parquet"}, q, null, &.{"ZPQ_S3_FOOTER_PROBE=16384"});
    defer gpa.free(one);
    try std.testing.expectEqual(@as(u32, 1), fake.gets);

    const two = try fakeS3AggregateJsonEnv(gpa, &fake, &.{"t/ok.parquet"}, q, null, &.{"ZPQ_S3_FOOTER_PROBE=64"});
    defer gpa.free(two);
    try std.testing.expectEqualStrings(one, two);
    try std.testing.expectEqual(@as(u32, 3), fake.gets);

    // Offset 0 was not fetched, so a bad leading magic does not fail a statistics answer.
    const bad = try fakeS3AggregateJsonEnv(gpa, &fake, &.{"t/bad.parquet"}, q, null, &.{"ZPQ_S3_FOOTER_PROBE=16384"});
    defer gpa.free(bad);
    try std.testing.expectEqualStrings(one, bad);

    // The default probe is the whole fixture, so the leading magic is in that response.
    try std.testing.expectError(error.NotParquet, fakeS3AggregateJsonEnv(gpa, &fake, &.{"t/bad.parquet"}, q, null, &.{}));
}

test "a footer prefix that starts at offset 0 is checked for the leading magic" {
    const gpa = std.testing.allocator;
    const bytes = metadata.readFileSlice("ci/fixtures/parquet/group_order.parquet", gpa) catch return error.SkipZigTest;
    defer gpa.free(bytes);
    // The serialized footer and its trailer, and nothing in front of them. Not a Parquet file: no leading PAR1, no pages.
    const footer_len: usize = std.mem.readInt(u32, bytes[bytes.len - 8 ..][0..4], .little);
    const footer_only = bytes[bytes.len - 8 - footer_len ..];
    try std.testing.expect(!metadata.hasLeadingMagic(footer_only));

    var fake: fake_s3.FakeS3 = undefined;
    try fake.start(gpa);
    defer fake.deinit();
    try fake.serveBytes("t/footer.parquet", footer_only);
    const q: QueryArgs = .{ .inputs = &.{}, .aggregate = "count(*) AS n" };
    // An 8-byte probe is only the trailer, so the rest of this object comes back as a GET that starts at offset 0.
    try std.testing.expectError(error.NotParquet, fakeS3AggregateJsonEnv(gpa, &fake, &.{"t/footer.parquet"}, q, null, &.{"ZPQ_S3_FOOTER_PROBE=8"}));
}

test "many files keep a count under the memory budget" {
    const gpa = std.testing.allocator;
    const bytes = metadata.readFileSlice("ci/fixtures/parquet/group_order.parquet", gpa) catch return error.SkipZigTest;
    defer gpa.free(bytes);
    const footer_len: usize = std.mem.readInt(u32, bytes[bytes.len - 8 ..][0..4], .little);
    const split = bytes.len - 8 - footer_len;
    // Padding between the pages and the footer makes the default 1 MiB suffix real. The pages, and the row count, stay put.
    const pad: usize = 1 << 20;
    const padded = try gpa.alloc(u8, bytes.len + pad);
    defer gpa.free(padded);
    @memcpy(padded[0..split], bytes[0..split]);
    @memset(padded[split .. split + pad], 0);
    @memcpy(padded[split + pad ..], bytes[split..]);

    var meta_arena = std.heap.ArenaAllocator.init(gpa);
    defer meta_arena.deinit();
    const meta = try metadata.open(meta_arena.allocator(), padded);
    const n_files: usize = 48;
    const budget: usize = 16 << 20;

    var fake: fake_s3.FakeS3 = undefined;
    try fake.start(gpa);
    defer fake.deinit();
    var key_arena = std.heap.ArenaAllocator.init(gpa);
    defer key_arena.deinit();
    const ka = key_arena.allocator();
    const keys = try ka.alloc([]const u8, n_files);
    for (keys, 0..) |*k, i| {
        k.* = try std.fmt.allocPrint(ka, "t/p{d}.parquet", .{i});
        try fake.serveBytes(k.*, padded);
    }

    var accounted: u64 = 0;
    const q: QueryArgs = .{
        .inputs = &.{},
        .aggregate = "count(*) AS n",
        .parallelism = 2,
        .max_memory = budget,
        .footer_resident_peak = &accounted,
    };
    const got = try fakeS3AggregateJsonEnv(gpa, &fake, keys, q, null, &.{});
    defer gpa.free(got);
    var want_buf: [64]u8 = undefined;
    const want = try std.fmt.bufPrint(&want_buf, "n={d};", .{meta.num_rows * @as(i64, @intCast(n_files))});
    try std.testing.expectEqualStrings(want, got);
    // The budget charges retained suffixes and in-flight responses. In-flight charges are not refused, so the high
    // water can sit above the budget by one wave of probes. A size-band allocator high-water is not this number:
    // arena nodes and the debug allocator's cache move it around, which is what failed a budget+20MiB check.
    const wave = @as(u64, READ_CONCURRENCY) * FOOTER_PROBE_DEFAULT;
    try std.testing.expect(accounted <= @as(u64, budget) + wave);
    // RSS is not asserted here. VmHWM is the whole test process's lifetime high-water, so earlier tests fail it
    // even when this query stays inside the allowance. The loose bound runs in its own process; see the integration test.
}

// ── Windowed S3 fetch ──

const fake_s3 = @import("io/fake_s3.zig");

test "Lambda default fetch window streams a large projection and fetches a smaller one up front" {
    if (!s3_window.supported) return error.SkipZigTest;
    const gpa = std.testing.allocator;
    var fixture_arena = std.heap.ArenaAllocator.init(gpa);
    defer fixture_arena.deinit();
    const a = fixture_arena.allocator();
    // Two real uncompressed chunks let the same file cross the cap by selecting one column or both. Expressions
    // force page reads rather than a statistics answer; no fetch needs to be stalled or timed to observe the path.
    const values = try a.alloc(f64, (65 << 20) / @sizeOf(f64));
    for (values, 0..) |*v, i| v.* = @floatFromInt(i);
    const leaf: schema.SchemaElement = .{
        .type = .DOUBLE,
        .type_length = null,
        .repetition_type = .REQUIRED,
        .name = "x",
        .num_children = null,
        .scale = null,
        .precision = null,
        .field_id = null,
    };
    const encoder = @import("core/writer/encoder.zig");
    const encoded = try encoder.encodeColumn(a, .{
        .values = .{ .f64 = .{ .values = values } },
        .schema_elem = &leaf,
        .path_in_schema = &.{"x"},
        .codec = .UNCOMPRESSED,
    });
    try std.testing.expect(encoded.bytes.len > 64 << 20);
    var meta: schema.FileMetaData = .{
        .version = 1,
        .schema = .empty,
        .num_rows = @intCast(values.len),
        .row_groups = .empty,
        .created_by = null,
    };
    var root = leaf;
    root.type = null;
    root.repetition_type = null;
    root.name = "root";
    root.num_children = 2;
    try meta.schema.append(a, root);
    var rg: schema.RowGroup = .{ .columns = .empty, .total_byte_size = 0, .num_rows = meta.num_rows };
    for ([_][]const u8{ "x", "y" }, 0..) |name, i| {
        var elem = leaf;
        elem.name = name;
        try meta.schema.append(a, elem);
        var cm = encoded.meta;
        cm.path_in_schema = .empty;
        try cm.path_in_schema.append(a, name);
        cm.data_page_offset += @intCast(4 + i * encoded.bytes.len);
        try rg.columns.append(a, .{ .file_path = null, .file_offset = cm.data_page_offset, .meta_data = cm });
        rg.total_byte_size += cm.total_uncompressed_size;
    }
    try meta.row_groups.append(a, rg);
    var footer: thrift.Writer = .init(a);
    try meta.write(&footer);
    const bytes = try a.alloc(u8, 4 + 2 * encoded.bytes.len + footer.bytes().len + 8);
    @memcpy(bytes[0..4], "PAR1");
    for (0..2) |i| @memcpy(bytes[4 + i * encoded.bytes.len ..][0..encoded.bytes.len], encoded.bytes);
    @memcpy(bytes[4 + 2 * encoded.bytes.len ..][0..footer.bytes().len], footer.bytes());
    std.mem.writeInt(u32, bytes[bytes.len - 8 ..][0..4], @intCast(footer.bytes().len), .little);
    @memcpy(bytes[bytes.len - 4 ..], "PAR1");

    var fake: fake_s3.FakeS3 = undefined;
    try fake.start(gpa);
    defer fake.deinit();
    try fake.serveBytes("t/window.parquet", bytes);
    const endpoint = try std.fmt.allocPrintSentinel(a, "S3_ENDPOINT_URL=http://127.0.0.1:{d}", .{fake.port}, 0);
    const env_lines = [_:null]?[*:0]const u8{
        endpoint,
        "AWS_REGION=us-east-1",
        "AWS_ACCESS_KEY_ID=ak",
        "AWS_SECRET_ACCESS_KEY=sk",
        "AWS_LAMBDA_FUNCTION_MEMORY_SIZE=3008",
    };
    const env: std.process.Environ = .{ .block = .{ .slice = &env_lines } };
    const ctx: Context = .{ .gpa = gpa, .env = env, .io = std.testing.io };
    for ([_][]const u8{ "sum(x * x) AS s", "sum(x * x) AS s, sum(y * y) AS t" }, 0..) |aggregate, i| {
        var query_arena = std.heap.ArenaAllocator.init(gpa);
        defer query_arena.deinit();
        const args: QueryArgs = .{
            .inputs = &.{"s3://bkt/t/window.parquet"},
            .aggregate = aggregate,
            .max_memory = 1 << 30,
        };
        const window: FetchWindow = .{ .limit = s3FetchWindow(env, args.max_memory) };
        var opened = try openInputs(ctx, query_arena.allocator(), args, window);
        defer opened.deinit();
        try std.testing.expectEqual(i == 1, opened.s3_fetcher != null);
        try std.testing.expectEqual(i == 1, opened.inputs[0].gate != null);
        try std.testing.expectEqual(i == 1, opened.inputs[0].bytes.len > lambda_window_cap);
    }
}

/// An aggregate over `keys` served by `fake`, fetched within `window_kib` (null: the window from the memory budget,
/// here larger than the files, so fetched up front). Returns the answer as JSON text, for comparing across runs.
fn fakeS3AggregateJson(
    gpa: std.mem.Allocator,
    fake: *fake_s3.FakeS3,
    keys: []const []const u8,
    args: QueryArgs,
    window_kib: ?u32,
) ![]u8 {
    return fakeS3AggregateJsonEnv(gpa, fake, keys, args, window_kib, &.{});
}

/// `fakeS3AggregateJson` with `extra` lines added to the query's environment.
fn fakeS3AggregateJsonEnv(
    gpa: std.mem.Allocator,
    fake: *fake_s3.FakeS3,
    keys: []const []const u8,
    args: QueryArgs,
    window_kib: ?u32,
    extra: []const [*:0]const u8,
) ![]u8 {
    var arena_state = std.heap.ArenaAllocator.init(gpa);
    defer arena_state.deinit();
    const a = arena_state.allocator();
    var lines: std.ArrayList(?[*:0]const u8) = .empty;
    try lines.append(a, try std.fmt.allocPrintSentinel(a, "S3_ENDPOINT_URL=http://127.0.0.1:{d}", .{fake.port}, 0));
    try lines.appendSlice(a, &.{ "AWS_REGION=us-east-1", "AWS_ACCESS_KEY_ID=ak", "AWS_SECRET_ACCESS_KEY=sk" });
    for (extra) |e| try lines.append(a, e);
    if (window_kib) |k| try lines.append(a, try std.fmt.allocPrintSentinel(a, "ZPQ_S3_WINDOW_KIB={d}", .{k}, 0));
    const block = try a.allocSentinel(?[*:0]const u8, lines.items.len, null);
    @memcpy(block, lines.items);
    const urls = try a.alloc([]const u8, keys.len);
    for (keys, urls) |k, *u| u.* = try std.fmt.allocPrint(a, "s3://bkt/{s}", .{k});
    var q = args;
    q.inputs = urls;
    var res = try runQuery(.{ .gpa = gpa, .env = .{ .block = .{ .slice = block } }, .io = std.testing.io }, q);
    defer res.deinit(gpa);
    var aw: std.Io.Writer.Allocating = .init(gpa);
    errdefer aw.deinit();
    const out = res.aggregate.output;
    for (out.aggs) |item| {
        try aw.writer.print("{s}=", .{item.alias});
        try writeAggValueJson(&aw.writer, item.value);
        try aw.writer.writeByte(';');
    }
    for (out.group_rows orelse &.{}) |row| {
        for (row) |v| {
            try writeAggValueJson(&aw.writer, v);
            try aw.writer.writeByte(',');
        }
        try aw.writer.writeByte(';');
    }
    return aw.toOwnedSlice();
}

test "a windowed S3 aggregate answers exactly as the up-front fetch does, whatever the window" {
    if (@import("builtin").os.tag != .linux) return error.SkipZigTest;
    const gpa = std.testing.allocator;
    // 24 row groups of about 5 KiB. The probe stays at 64 KiB, under this fixture and over its footer, so the first
    // dozen row groups fall outside the suffix and are really fetched. The default 1 MiB probe would hold the file.
    const fixture = "ci/fixtures/parquet/group_order.parquet";
    const bytes = metadata.readFileSlice(fixture, gpa) catch return error.SkipZigTest;
    defer gpa.free(bytes);
    var fake: fake_s3.FakeS3 = undefined;
    try fake.start(gpa);
    defer fake.deinit();
    try fake.serveBytes("t/a.parquet", bytes);
    try fake.serveBytes("t/b.parquet", bytes);
    const keys = [_][]const u8{ "t/a.parquet", "t/b.parquet" };
    const queries = [_]QueryArgs{
        .{ .inputs = &.{}, .aggregate = "count(*) AS n, sum(i) AS s, min(k) AS m, avg(x) AS a, max(x) AS x" },
        .{
            .inputs = &.{},
            .aggregate = "sum(i * 2) AS p, count(*) AS n",
            .filter = "i >= 300 AND i < 400",
            .parallelism = 3,
        },
        .{ .inputs = &.{}, .aggregate = "sum(x) AS s, count(i) AS c", .group_by = "k", .parallelism = 3 },
        // A filter that prunes row groups: a pruned row group is never fetched, and its gate never waits.
        .{ .inputs = &.{}, .aggregate = "count(*) AS n, sum(i) AS s", .filter = "i > 450" },
    };
    const probe = [_][*:0]const u8{"ZPQ_S3_FOOTER_PROBE=65536"};
    for (queries) |q| {
        const whole = try fakeS3AggregateJsonEnv(gpa, &fake, &keys, q, null, &probe);
        defer gpa.free(whole);
        // 1 KiB is smaller than any row group, so each goes alone; 12 KiB takes a few at a time.
        for ([_]u32{ 1, 12 }) |kib| {
            const gets_before = fake.gets;
            const windowed = try fakeS3AggregateJsonEnv(gpa, &fake, &keys, q, kib, &probe);
            defer gpa.free(windowed);
            // Past the two suffixes: row groups were fetched through the window, not served from the suffix.
            if (q.filter == null) try std.testing.expect(fake.gets - gets_before > keys.len);
            std.testing.expectEqualStrings(whole, windowed) catch |err| {
                std.debug.print("window {d} KiB, aggregate {s}\n", .{ kib, q.aggregate.? });
                return err;
            };
        }
    }
}

test "a suffix that covers the file serves a windowed aggregate from one GET per file" {
    if (@import("builtin").os.tag != .linux) return error.SkipZigTest;
    const gpa = std.testing.allocator;
    const fixture = "ci/fixtures/parquet/group_order.parquet";
    const bytes = metadata.readFileSlice(fixture, gpa) catch return error.SkipZigTest;
    defer gpa.free(bytes);
    // Under the default 1 MiB probe, so the suffix is the whole object and no column range is fetched.
    try std.testing.expect(bytes.len < FOOTER_PROBE_DEFAULT);

    var fake: fake_s3.FakeS3 = undefined;
    try fake.start(gpa);
    defer fake.deinit();
    try fake.serveBytes("t/a.parquet", bytes);
    try fake.serveBytes("t/b.parquet", bytes);
    const keys = [_][]const u8{ "t/a.parquet", "t/b.parquet" };
    const q: QueryArgs = .{
        .inputs = &.{},
        .aggregate = "count(*) AS n, sum(i) AS s, min(k) AS m, avg(x) AS a, max(x) AS x",
    };

    const before_whole = fake.gets;
    const whole = try fakeS3AggregateJson(gpa, &fake, &keys, q, null);
    defer gpa.free(whole);
    try std.testing.expectEqual(@as(u32, keys.len), fake.gets - before_whole);

    // 1 KiB is smaller than a row group, so a window that missed the suffix would issue a GET per row group.
    for ([_]u32{ 1, 12 }) |kib| {
        const before = fake.gets;
        const windowed = try fakeS3AggregateJson(gpa, &fake, &keys, q, kib);
        defer gpa.free(windowed);
        try std.testing.expectEqual(@as(u32, keys.len), fake.gets - before);
        try std.testing.expectEqualStrings(whole, windowed);
    }
}

const TestDigest = struct { rows: i64, cols: [8]u64 };

/// Every value of every column of the parquet file `bytes`, decoded page by page and hashed per column in row order:
/// equal for two files holding the same rows however they are cut into row groups. Checks on the way that every page
/// decodes, every chunk holds the values its metadata counts, and every Bloom filter admits each value of its chunk.
fn testContentDigest(gpa: std.mem.Allocator, bytes: []const u8) !TestDigest {
    const column_mod = @import("core/parquet/column.zig");
    var arena_state = std.heap.ArenaAllocator.init(gpa);
    defer arena_state.deinit();
    const a = arena_state.allocator();
    const meta = try metadata.open(a, bytes);
    var hashers: [8]std.hash.Wyhash = @splat(.init(0));
    var rows: i64 = 0;
    for (meta.row_groups.items) |rg| {
        rows += rg.num_rows;
        if (rg.columns.items.len > hashers.len) return error.TooManyColumns;
        for (rg.columns.items, hashers[0..rg.columns.items.len]) |chunk, *h| {
            const cm = chunk.meta_data orelse return error.ColumnMetaMissing;
            const start: usize = @intCast(cm.dictionary_page_offset orelse cm.data_page_offset);
            const len: usize = @intCast(cm.total_compressed_size);
            if (start + len > bytes.len) return error.MissingChunkBytes;
            const n: usize = @intCast(cm.num_values);
            if (n != rg.num_rows) return error.ShortDecode;
            const levels = meta.getColumnLevels(cm.path_in_schema.items);
            const bits: ?[]const u8 = if (cm.bloom_filter_offset) |off| blk: {
                const end = @as(usize, @intCast(off)) + @as(usize, @intCast(cm.bloom_filter_length orelse 0));
                if (off < 0 or end > bytes.len) return error.BadBloomFilter;
                break :blk sbbf.bitset(bytes[@intCast(off)..end]) orelse return error.BadBloomFilter;
            } else null;
            switch (cm.type) {
                inline else => |t| {
                    const T = switch (t) {
                        .BOOLEAN => bool,
                        .INT32 => i32,
                        .INT64 => i64,
                        .FLOAT => f32,
                        .DOUBLE => f64,
                        .INT96, .BYTE_ARRAY, .FIXED_LEN_BYTE_ARRAY => []const u8,
                    };
                    var r = column_mod.ColumnChunkReader(T).init(bytes[start..][0..len], cm.codec, levels, a);
                    if (t == .INT96) r.type_length = 12;
                    if (t == .FIXED_LEN_BYTE_ARRAY)
                        r.type_length = consumer.flbaWidth(meta.getColumnSchema(cm.path_in_schema.items));
                    const values = try a.alloc(T, n);
                    const def = try a.alloc(u32, n);
                    @memset(def, @intCast(levels.max_def));
                    var got: usize = 0;
                    while (got < n) {
                        const k = if (levels.max_def > 0)
                            try r.decodeWithLevels(values[got..], def[got..])
                        else
                            try r.decode(values[got..]);
                        if (k == 0) break;
                        got += k;
                    }
                    if (got != n) return error.ShortDecode;
                    for (values, def) |v, d| {
                        if (d < levels.max_def) {
                            h.update("\x00");
                            continue;
                        }
                        const plain: []const u8 = if (T == []const u8) v else &std.mem.toBytes(std.mem.nativeToLittle(
                            @Int(.unsigned, @bitSizeOf(T)),
                            @bitCast(v),
                        ));
                        h.update("\x01");
                        h.update(&std.mem.toBytes(@as(u64, plain.len)));
                        h.update(plain);
                        if (bits) |b| if (T != bool and !sbbf.Probe.init(sbbf.hashBytes(plain)).mightContain(b))
                            return error.BloomFilterMissesValue;
                    }
                },
            }
        }
    }
    var out: [8]u64 = undefined;
    for (&hashers, &out) |*h, *o| o.* = h.final();
    return .{ .rows = rows, .cols = out };
}

test "every write reads back whole, holding what an unbounded run writes, through any window and budget" {
    // Each output is decoded page by page, its Bloom filters probed with every value, and its rows compared with a
    // local run's on the default budget: local and S3 inputs, every write path (byte copy, recompress, projection,
    // filter, computed select, Bloom-building), row groups far smaller and far larger than the window and the decode
    // budget, with and without Bloom filters to carry. A run that differs only in its S3 window writes the same bytes.
    if (@import("builtin").os.tag != .linux) return error.SkipZigTest;
    const gpa = std.testing.allocator;
    const tiny = metadata.readFileSlice("ci/fixtures/parquet/many_tiny.parquet", gpa) catch return error.SkipZigTest;
    defer gpa.free(tiny);
    const mixed = metadata.readFileSlice("ci/fixtures/parquet/mixed_codec.parquet", gpa) catch return error.SkipZigTest;
    defer gpa.free(mixed);

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var arena_state = std.heap.ArenaAllocator.init(gpa);
    defer arena_state.deinit();
    const a = arena_state.allocator();
    const dir = try std.fmt.allocPrint(a, ".zig-cache/tmp/{s}", .{tmp.sub_path});
    const out = try std.fmt.allocPrint(a, "{s}/out.parquet", .{dir});

    // The tiny row groups again, with Bloom filters on two columns written after the last row group.
    const tiny_path = try std.fmt.allocPrint(a, "{s}/tiny.parquet", .{dir});
    const bloom_path = try std.fmt.allocPrint(a, "{s}/tiny_bloom.parquet", .{dir});
    const mixed_path = try std.fmt.allocPrint(a, "{s}/mixed.parquet", .{dir});
    try std.Io.Dir.cwd().writeFile(std.testing.io, .{ .sub_path = tiny_path, .data = tiny });
    try std.Io.Dir.cwd().writeFile(std.testing.io, .{ .sub_path = mixed_path, .data = mixed });
    {
        var r = try runQuery(.{ .gpa = gpa, .env = .empty, .io = std.testing.io }, .{
            .inputs = &.{tiny_path},
            .output = bloom_path,
            .bloom_filters = &.{ .{ .column = "id" }, .{ .column = "s" } },
        });
        r.deinit(gpa);
    }
    const tiny_bloom = try metadata.readFileSlice(bloom_path, gpa);
    defer gpa.free(tiny_bloom);
    try std.testing.expect((try metadata.open(a, tiny_bloom)).row_groups.items[0].columns.items[0].meta_data.?
        .bloom_filter_offset != null);

    const Fixture = struct {
        path: []const u8,
        key: []const u8,
        bytes: []const u8,
        filter: []const u8,
        columns: []const []const u8,
        select: []const u8,
        bloom: []const u8,
    };
    const fixtures = [_]Fixture{
        .{
            .path = tiny_path,
            .key = "tiny",
            .bytes = tiny,
            .filter = "id >= 100",
            .columns = &.{ "id", "d" },
            .select = "id * 2 AS x, d, s",
            .bloom = "g",
        },
        .{
            .path = bloom_path,
            .key = "tiny_bloom",
            .bytes = tiny_bloom,
            .filter = "id >= 100",
            .columns = &.{ "id", "s" },
            .select = "id * 2 AS x, d, s",
            .bloom = "g",
        },
        .{
            .path = mixed_path,
            .key = "mixed",
            .bytes = mixed,
            .filter = "a >= 5",
            .columns = &.{ "a", "b" },
            .select = "a * 2 AS x, b, c",
            .bloom = "b",
        },
    };
    const S3 = enum { local, up_front, window };
    const Variant = struct { label: []const u8, s3: S3, j: usize, max_memory: ?usize = null };
    const variants = [_]Variant{
        .{ .label = "local -j1", .s3 = .local, .j = 1 },
        .{ .label = "local -j3 64K", .s3 = .local, .j = 3, .max_memory = 64 << 10 },
        .{ .label = "s3 up front -j1", .s3 = .up_front, .j = 1 },
        .{ .label = "s3 window -j1", .s3 = .window, .j = 1 },
        .{ .label = "s3 up front -j3", .s3 = .up_front, .j = 3 },
        .{ .label = "s3 window -j3", .s3 = .window, .j = 3 },
        .{ .label = "s3 window -j1 64K", .s3 = .window, .j = 1, .max_memory = 64 << 10 },
        .{ .label = "s3 window -j2 64K", .s3 = .window, .j = 2, .max_memory = 64 << 10 },
    };
    for (fixtures) |f| {
        const local_in = [_][]const u8{ f.path, f.path };
        const s3_in = [_][]const u8{
            try std.fmt.allocPrint(a, "s3://bkt/t/{s}_1.parquet", .{f.key}),
            try std.fmt.allocPrint(a, "s3://bkt/t/{s}_2.parquet", .{f.key}),
        };
        const Mode = struct { label: []const u8, args: QueryArgs };
        const modes = [_]Mode{
            .{ .label = "copy", .args = .{ .inputs = &.{} } },
            .{ .label = "recompress", .args = .{ .inputs = &.{}, .codec = .GZIP } },
            .{ .label = "expanding recompress", .args = .{ .inputs = &.{}, .codec = .UNCOMPRESSED } },
            .{ .label = "projection", .args = .{ .inputs = &.{}, .columns = f.columns } },
            .{ .label = "filter", .args = .{ .inputs = &.{}, .filter = f.filter } },
            .{ .label = "filter projection", .args = .{ .inputs = &.{}, .filter = f.filter, .columns = f.columns } },
            .{ .label = "select", .args = .{ .inputs = &.{}, .select = f.select } },
            .{
                .label = "filter + Bloom",
                .args = .{ .inputs = &.{}, .filter = f.filter, .bloom_filters = &.{.{ .column = f.bloom }} },
            },
        };
        for (modes) |m| {
            var want: ?TestDigest = null;
            // The bytes each window-less S3 run wrote, by -j, which the windowed run at that -j must match.
            var up_front: [4]?[]u8 = .{ null, null, null, null };
            defer for (up_front) |b| if (b) |x| gpa.free(x);
            for (variants) |v| {
                var q = m.args;
                q.inputs = if (v.s3 == .local) &local_in else &s3_in;
                q.output = out;
                q.parallelism = v.j;
                if (v.max_memory) |mm| q.max_memory = mm;
                // A server per run: one serves a bounded number of connections.
                var fake: fake_s3.FakeS3 = undefined;
                try fake.start(gpa);
                defer fake.deinit();
                for (s3_in, [_][]const u8{ "1", "2" }) |_, copy| {
                    try fake.serveBytes(try std.fmt.allocPrint(a, "t/{s}_{s}.parquet", .{ f.key, copy }), f.bytes);
                }
                var lines: std.ArrayList(?[*:0]const u8) = .empty;
                try lines.appendSlice(a, &.{
                    try std.fmt.allocPrintSentinel(a, "S3_ENDPOINT_URL=http://127.0.0.1:{d}", .{fake.port}, 0),
                    "AWS_REGION=us-east-1",
                    "AWS_ACCESS_KEY_ID=ak",
                    "AWS_SECRET_ACCESS_KEY=sk",
                });
                if (v.s3 == .window) try lines.append(a, "ZPQ_S3_WINDOW_KIB=1");
                const block = try a.allocSentinel(?[*:0]const u8, lines.items.len, null);
                @memcpy(block, lines.items);
                const env: std.process.Environ = if (v.s3 == .local) .empty else .{ .block = .{ .slice = block } };
                var r = runQuery(.{ .gpa = gpa, .env = env, .io = std.testing.io }, q) catch |err| {
                    std.debug.print("{s} / {s} / {s}: {s}\n", .{ f.key, m.label, v.label, @errorName(err) });
                    return err;
                };
                r.deinit(gpa);
                const written = try metadata.readFileSlice(out, gpa);
                var keep = false;
                defer if (!keep) gpa.free(written);
                const got = testContentDigest(gpa, written) catch |err| {
                    std.debug.print("{s} / {s} / {s}: output does not read back: {s}\n", .{
                        f.key, m.label, v.label, @errorName(err),
                    });
                    return err;
                };
                if (want) |w| std.testing.expectEqualDeep(w, got) catch |err| {
                    std.debug.print("{s} / {s} / {s}: rows differ from the unbounded run's\n", .{
                        f.key, m.label, v.label,
                    });
                    return err;
                } else want = got;
                if (v.s3 == .local or v.max_memory != null) continue;
                if (v.s3 == .up_front) {
                    up_front[v.j] = written;
                    keep = true;
                } else std.testing.expectEqualSlices(u8, up_front[v.j].?, written) catch |err| {
                    std.debug.print("{s} / {s} / {s}: bytes differ from the up-front fetch's\n", .{
                        f.key, m.label, v.label,
                    });
                    return err;
                };
            }
        }
    }
}

test "windowed S3 rebasing uses the owning row group's overlapping source range" {
    var arena_state = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena_state.deinit();
    const arena = arena_state.allocator();
    var meta: schema.FileMetaData = .{
        .version = 1,
        .schema = .empty,
        .num_rows = 2,
        .created_by = null,
        .row_groups = .empty,
    };
    for ([_]i64{ 1_000, 1_100 }, [_]i64{ 50_000, 60_000 }) |dpo, bfo| {
        var rg: schema.RowGroup = .{ .columns = .empty, .total_byte_size = 64, .num_rows = 1 };
        var chunk = pageIndexTestChunk(dpo);
        chunk.meta_data.?.bloom_filter_offset = bfo;
        chunk.meta_data.?.bloom_filter_length = 16;
        try rg.columns.append(arena, chunk);
        try meta.row_groups.append(arena, rg);
    }
    // Coalescing to carried Bloom filters copies the same source bytes into two row groups' segments, each released
    // when its own row group is done.
    const ranges = [_]CompactRange{
        .{ .start = 1_000, .end = 50_016, .dst_start = 0 },
        .{ .start = 1_100, .end = 60_016, .dst_start = 65_536 },
    };
    const fetched = [_]bool{true};
    const rg_columns = [_]?[]const bool{ &fetched, &fetched };
    const rg_compact = [_]usize{ 0, 1, 2 };
    try rebaseFetchedOffsets(&meta, &fetched, &ranges, &rg_columns, &rg_compact);
    // Row group 1's page and filter point into its own segment, not row group 0's copy of the same source bytes.
    const cm = meta.row_groups.items[1].columns.items[0].meta_data.?;
    try std.testing.expectEqual(@as(i64, 65_536), cm.data_page_offset);
    try std.testing.expectEqual(@as(?i64, 65_536 + 60_000 - 1_100), cm.bloom_filter_offset);
}

/// Deadlines short enough for tests, through the query's environment (not process state).
const stall_env = [_][*:0]const u8{
    "ZPQ_S3_CONNECT_TIMEOUT_MS=300",
    "ZPQ_S3_HANDSHAKE_TIMEOUT_MS=300",
    "ZPQ_S3_FIRST_BYTE_TIMEOUT_MS=200",
    "ZPQ_S3_IDLE_TIMEOUT_MS=200",
};

test "an S3 read that stalls is retried on a new connection and the query answers exactly, windowed or not" {
    if (@import("builtin").os.tag != .linux) return error.SkipZigTest;
    const gpa = std.testing.allocator;
    const fixture = "ci/fixtures/parquet/group_order.parquet";
    const bytes = metadata.readFileSlice(fixture, gpa) catch return error.SkipZigTest;
    defer gpa.free(bytes);
    const keys = [_][]const u8{ "t/a.parquet", "t/b.parquet" };
    const q: QueryArgs = .{ .inputs = &.{}, .aggregate = "count(*) AS n, sum(i) AS s, max(k) AS m", .parallelism = 2 };
    var want: ?[]u8 = null;
    defer if (want) |w| gpa.free(w);
    for ([_]?u32{ null, 1 }) |window| {
        for ([_]fake_s3.FakeS3.StallAt{ .before_headers, .mid_headers, .mid_body }) |at| {
            var fake: fake_s3.FakeS3 = undefined;
            try fake.start(gpa);
            defer fake.deinit();
            try fake.serveBytes("t/a.parquet", bytes);
            try fake.serveBytes("t/b.parquet", bytes);
            if (want == null) want = try fakeS3AggregateJsonEnv(gpa, &fake, &keys, q, window, &stall_env);
            fake.stall = "GET /bkt/t/b.parquet";
            fake.stall_at = at;
            const t0 = nowMonoNs();
            const got = try fakeS3AggregateJsonEnv(gpa, &fake, &keys, q, window, &stall_env);
            defer gpa.free(got);
            const ms = @divTrunc(nowMonoNs() - t0, std.time.ns_per_ms);
            std.testing.expectEqualStrings(want.?, got) catch |err| {
                std.debug.print("window {?d}, stall {t}\n", .{ window, at });
                return err;
            };
            try std.testing.expectEqual(@as(u32, 1), fake.stalled);
            // Recovered within the query's 200 ms timeouts, not a 10 s default.
            std.testing.expect(ms < 3_000) catch |err| {
                std.debug.print("window {?d}, stall {t}: {d} ms\n", .{ window, at, ms });
                return err;
            };
        }
    }
}

test "the parallel range fetches take the query's first-byte and idle timeouts, windowed or not" {
    // A stalled column-chunk range fetch (the fake stalls only an explicit range of 1 KiB or more, never a suffix)
    // recovers within the query's short timeout of the stalled kind, the other set long: were either ignored,
    // recovery would wait out a default. The probe is smaller than the fixture so those ranges are still requested.
    if (@import("builtin").os.tag != .linux) return error.SkipZigTest;
    const gpa = std.testing.allocator;
    const bytes = metadata.readFileSlice("ci/fixtures/parquet/mixed_codec.parquet", gpa) catch return error.SkipZigTest;
    defer gpa.free(bytes);
    const keys = [_][]const u8{"t/m.parquet"};
    const q: QueryArgs = .{ .inputs = &.{}, .aggregate = "count(*) AS n, sum(a) AS s, max(b) AS m", .parallelism = 2 };
    const Case = struct { at: fake_s3.FakeS3.StallAt, env: []const [*:0]const u8 };
    const cases = [_]Case{
        .{ .at = .before_headers, .env = &.{ "ZPQ_S3_FIRST_BYTE_TIMEOUT_MS=200", "ZPQ_S3_IDLE_TIMEOUT_MS=60000", "ZPQ_S3_FOOTER_PROBE=16384" } },
        .{ .at = .mid_body, .env = &.{ "ZPQ_S3_FIRST_BYTE_TIMEOUT_MS=60000", "ZPQ_S3_IDLE_TIMEOUT_MS=200", "ZPQ_S3_FOOTER_PROBE=16384" } },
    };
    var want: ?[]u8 = null;
    defer if (want) |w| gpa.free(w);
    for ([_]?u32{ null, 1 }) |window| for (cases) |c| {
        var fake: fake_s3.FakeS3 = undefined;
        try fake.start(gpa);
        defer fake.deinit();
        try fake.serveBytes("t/m.parquet", bytes);
        if (want == null) want = try fakeS3AggregateJson(gpa, &fake, &keys, q, window);
        fake.stall = "GET /bkt/t/m.parquet";
        fake.stall_at = c.at;
        fake.stall_min_len = 1024;
        const t0 = nowMonoNs();
        const got = try fakeS3AggregateJsonEnv(gpa, &fake, &keys, q, window, c.env);
        defer gpa.free(got);
        const ms = @divTrunc(nowMonoNs() - t0, std.time.ns_per_ms);
        std.testing.expectEqualStrings(want.?, got) catch |err| {
            std.debug.print("window {?d}, stall {t}\n", .{ window, c.at });
            return err;
        };
        try std.testing.expectEqual(@as(u32, 1), fake.stalled);
        std.testing.expect(ms >= 150 and ms < 3_000) catch |err| {
            std.debug.print("window {?d}, stall {t}: {d} ms\n", .{ window, c.at, ms });
            return err;
        };
    };
}

test "a recompressing S3 copy whose ranged read stalls writes the same bytes as one that does not" {
    // The Overture rewrite that hung on Lambda: a byte copy to another codec, input through the fetch window.
    if (@import("builtin").os.tag != .linux) return error.SkipZigTest;
    const gpa = std.testing.allocator;
    const bytes = metadata.readFileSlice("ci/fixtures/parquet/mixed_codec.parquet", gpa) catch return error.SkipZigTest;
    defer gpa.free(bytes);
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var path_buf: [96]u8 = undefined;
    const out = try std.fmt.bufPrint(&path_buf, ".zig-cache/tmp/{s}/out.parquet", .{tmp.sub_path});
    var arena_state = std.heap.ArenaAllocator.init(gpa);
    defer arena_state.deinit();
    const a = arena_state.allocator();

    var first: ?[]u8 = null;
    defer if (first) |f| gpa.free(f);
    for ([_]?fake_s3.FakeS3.StallAt{ null, .mid_body, .before_headers }) |at| {
        var fake: fake_s3.FakeS3 = undefined;
        try fake.start(gpa);
        defer fake.deinit();
        try fake.serveBytes("t/m.parquet", bytes);
        if (at) |x| {
            fake.stall = "GET /bkt/t/m.parquet";
            fake.stall_at = x;
            fake.stalls_left = 2;
            fake.stall_min_len = 1024; // column-chunk range fetches through the window
        }
        var lines: std.ArrayList(?[*:0]const u8) = .empty;
        try lines.append(a, try std.fmt.allocPrintSentinel(a, "S3_ENDPOINT_URL=http://127.0.0.1:{d}", .{fake.port}, 0));
        try lines.appendSlice(a, &.{ "AWS_REGION=us-east-1", "AWS_ACCESS_KEY_ID=ak", "AWS_SECRET_ACCESS_KEY=sk" });
        try lines.append(a, "ZPQ_S3_WINDOW_KIB=1");
        // Smaller than the fixture, so the copy still range-fetches column chunks instead of taking them from the suffix.
        try lines.append(a, "ZPQ_S3_FOOTER_PROBE=16384");
        for (stall_env) |e| try lines.append(a, e);
        const block = try a.allocSentinel(?[*:0]const u8, lines.items.len, null);
        @memcpy(block, lines.items);
        const t0 = nowMonoNs();
        var r = try runQuery(.{ .gpa = gpa, .env = .{ .block = .{ .slice = block } }, .io = std.testing.io }, .{
            .inputs = &.{"s3://bkt/t/m.parquet"},
            .output = out,
            .codec = .ZSTD,
            .compress_priority = .speed,
            .parallelism = 2,
        });
        r.deinit(gpa);
        // Two stalls, each recovered within the query's 200 ms timeouts, not a 10 s default.
        try std.testing.expect(@divTrunc(nowMonoNs() - t0, std.time.ns_per_ms) < 5_000);
        const written = try metadata.readFileSlice(out, gpa);
        if (first) |f| {
            defer gpa.free(written);
            try std.testing.expectEqualSlices(u8, f, written);
            try std.testing.expectEqual(@as(u32, 2), fake.stalled);
        } else first = written;
    }
}

test "a windowed S3 fetch that fails mid-scan fails the query rather than hanging it" {
    if (@import("builtin").os.tag != .linux) return error.SkipZigTest;
    const gpa = std.testing.allocator;
    const fixture = "ci/fixtures/parquet/group_order.parquet";
    const bytes = metadata.readFileSlice(fixture, gpa) catch return error.SkipZigTest;
    defer gpa.free(bytes);
    var fake: fake_s3.FakeS3 = undefined;
    try fake.start(gpa);
    defer fake.deinit();
    try fake.serveBytes("t/a.parquet", bytes);
    try fake.serveBytes("t/b.parquet", bytes);
    // One suffix per file, then a few row-group ranges; the rest are refused. The probe stays under the fixture so
    // the suffix is not the whole object.
    fake.gets_allowed = 5;
    const keys = [_][]const u8{ "t/a.parquet", "t/b.parquet" };
    const q: QueryArgs = .{ .inputs = &.{}, .aggregate = "count(*) AS n, sum(i) AS s", .parallelism = 2 };
    try std.testing.expectError(error.RangeFetchFailed, fakeS3AggregateJsonEnv(gpa, &fake, &keys, q, 1, &.{"ZPQ_S3_FOOTER_PROBE=16384"}));
}

test "every S3 write path writes the same bytes through a window as with everything fetched up front" {
    if (@import("builtin").os.tag != .linux) return error.SkipZigTest;
    const gpa = std.testing.allocator;
    const fixture = "ci/fixtures/parquet/group_order.parquet";
    const bytes = metadata.readFileSlice(fixture, gpa) catch return error.SkipZigTest;
    defer gpa.free(bytes);
    var fake: fake_s3.FakeS3 = undefined;
    try fake.start(gpa);
    defer fake.deinit();
    try fake.serveBytes("t/a.parquet", bytes);
    try fake.serveBytes("t/b.parquet", bytes);

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var path_buf: [96]u8 = undefined;
    const out = try std.fmt.bufPrint(&path_buf, ".zig-cache/tmp/{s}/out.parquet", .{tmp.sub_path});

    var arena_state = std.heap.ArenaAllocator.init(gpa);
    defer arena_state.deinit();
    const a = arena_state.allocator();
    const endpoint = try std.fmt.allocPrintSentinel(a, "S3_ENDPOINT_URL=http://127.0.0.1:{d}", .{fake.port}, 0);
    const creds = [_]?[*:0]const u8{
        endpoint,
        "AWS_REGION=us-east-1",
        "AWS_ACCESS_KEY_ID=ak",
        "AWS_SECRET_ACCESS_KEY=sk",
    };
    const inputs = [_][]const u8{ "s3://bkt/t/a.parquet", "s3://bkt/t/b.parquet" };
    const Case = struct { label: []const u8, args: QueryArgs };
    const cases = [_]Case{
        .{ .label = "byte copy", .args = .{ .inputs = &inputs, .columns = &.{ "k", "i" } } },
        .{ .label = "recompressing copy", .args = .{ .inputs = &inputs, .codec = .ZSTD } },
        .{ .label = "re-encode", .args = .{ .inputs = &inputs, .filter = "i >= 300", .parallelism = 3 } },
        .{ .label = "select", .args = .{ .inputs = &inputs, .select = "i * 2 AS d, k", .parallelism = 2 } },
    };
    // A recompressing copy of chunks in several codecs: some are rewritten, the rest copied from the fetched input,
    // which must still be held when the writer places them.
    const mixed = metadata.readFileSlice("ci/fixtures/parquet/mixed_codec.parquet", gpa) catch return error.SkipZigTest;
    defer gpa.free(mixed);
    try fake.serveBytes("t/mixed.parquet", mixed);
    const mixed_in = [_][]const u8{"s3://bkt/t/mixed.parquet"};
    const all_cases = cases ++ [_]Case{
        .{ .label = "mixed-codec recompress", .args = .{ .inputs = &mixed_in, .codec = .ZSTD, .parallelism = 3 } },
        .{ .label = "mixed-codec to uncompressed", .args = .{ .inputs = &mixed_in, .codec = .UNCOMPRESSED } },
        // A budget too small to gather any row group's output: the writer recompresses each page by page in its turn.
        .{
            .label = "mixed-codec streamed",
            .args = .{ .inputs = &mixed_in, .codec = .UNCOMPRESSED, .parallelism = 2, .max_memory = 64 << 10 },
        },
    };
    // The gathered UNCOMPRESSED rewrite, which the streamed one must match byte for byte.
    var gathered: ?[]u8 = null;
    defer if (gathered) |g| gpa.free(g);
    for (all_cases) |c| {
        var first: ?[]u8 = null;
        defer if (first) |b| {
            if (std.mem.eql(u8, c.label, "mixed-codec to uncompressed")) gathered = b else gpa.free(b);
        };

        // Up front (no override: the files fit the default window), then a window smaller than any row group.
        for ([_]?[*:0]const u8{ null, "ZPQ_S3_WINDOW_KIB=1" }) |win| {
            var lines: std.ArrayList(?[*:0]const u8) = .empty;
            try lines.appendSlice(a, &creds);
            if (win) |w| try lines.append(a, w);
            const block = try a.allocSentinel(?[*:0]const u8, lines.items.len, null);
            @memcpy(block, lines.items);
            var q = c.args;
            q.output = out;
            var r = try runQuery(.{ .gpa = gpa, .env = .{ .block = .{ .slice = block } }, .io = std.testing.io }, q);
            r.deinit(gpa);
            const written = try metadata.readFileSlice(out, gpa);
            if (first) |b| {
                defer gpa.free(written);
                std.testing.expectEqualSlices(u8, b, written) catch |err| {
                    std.debug.print("{s}: windowed output differs\n", .{c.label});
                    return err;
                };
            } else first = written;
        }
        if (std.mem.eql(u8, c.label, "mixed-codec streamed")) {
            std.testing.expectEqualSlices(u8, gathered.?, first.?) catch |err| {
                std.debug.print("streamed rewrite differs from the gathered one\n", .{});
                return err;
            };
        }
    }
}

test "explainAggregate: S3 reads exactly the trailer and footer, never pages" {
    const gpa = std.testing.allocator;
    const bytes = try metadata.readFileSlice("ci/fixtures/parquet/topk_skew.parquet", gpa);
    defer gpa.free(bytes);
    const footer_len = std.mem.readInt(u32, bytes[bytes.len - 8 ..][0..4], .little);
    const footer_start = bytes.len - 8 - footer_len;
    var fake: fake_s3.FakeS3 = undefined;
    try fake.start(gpa);
    defer fake.deinit();
    try fake.serveBytes("t/explain.parquet", bytes);
    fake.gets_allowed = 2;
    var arena_state = std.heap.ArenaAllocator.init(gpa);
    defer arena_state.deinit();
    const a = arena_state.allocator();
    const endpoint = try std.fmt.allocPrintSentinel(a, "S3_ENDPOINT_URL=http://127.0.0.1:{d}", .{fake.port}, 0);
    const lines = [_:null]?[*:0]const u8{
        endpoint,              "AWS_REGION=us-east-1", "AWS_ACCESS_KEY_ID=ak", "AWS_SECRET_ACCESS_KEY=sk",
        "ZPQ_IO_PROFILE=mmap", "ZPQ_SKETCH_BITS=10",
    };
    const ctx: Context = .{ .gpa = gpa, .env = .{ .block = .{ .slice = &lines } }, .io = std.testing.io };
    const explained = try explainAggregate(ctx, a, .{
        .inputs = &.{"s3://bkt/t/explain.parquet"},
        .group_by = "k",
        .aggregate = "count(*) AS c",
        .filter = "n < 12",
        .result_order = .{ .keys = &.{.{ .column = 1, .descending = true }}, .limit = 3 },
    });
    try std.testing.expectEqual(@as(u32, 2), fake.gets);
    try std.testing.expectEqual(@as(u64, footer_len + 8), fake.get_bytes);
    try std.testing.expectEqual(footer_start, fake.lowest_get_offset.?);
    try std.testing.expectEqual(.plain_hash, explained.aggregate.plan.dispatch.strategy);
    var out: std.Io.Writer.Allocating = .init(gpa);
    defer out.deinit();
    try writeAggregateExplanationJson(&out.writer, explained);
    const parsed = try std.json.parseFromSlice(std.json.Value, gpa, out.written(), .{});
    defer parsed.deinit();
    const pass = parsed.value.object.get("passes").?.array.items[0].object;
    try std.testing.expectEqualStrings("aggregate", pass.get("kind").?.string);
    try std.testing.expectEqual(@as(usize, 2), pass.get("columns").?.array.items.len);
}

test "explainAggregate: local IO and environment knobs preserve dispatch eligibility" {
    const gpa = std.testing.allocator;
    const modes = [_][*:0]const u8{ "ZPQ_IO_PROFILE=mmap", "ZPQ_IO_PROFILE=pread", "ZPQ_IO_PROFILE=uring" };
    for (modes, 0..) |mode, i| for ([_]bool{ false, true }) |disabled| {
        var arena_state = std.heap.ArenaAllocator.init(gpa);
        defer arena_state.deinit();
        const lines = [_:null]?[*:0]const u8{
            mode, if (disabled) "ZPQ_SKETCH_TOPK=0" else "ZPQ_SKETCH_TOPK=1", "ZPQ_ENCODED=0", "ZPQ_SKETCH_BITS=10",
        };
        const ctx: Context = .{ .gpa = gpa, .env = .{ .block = .{ .slice = &lines } }, .io = std.testing.io };
        const explained = try explainAggregate(ctx, arena_state.allocator(), .{
            .inputs = &.{"ci/fixtures/parquet/topk_skew.parquet"},
            .group_by = "k",
            .aggregate = "count(*) AS c",
            .result_order = .{ .keys = &.{.{ .column = 1, .descending = true }}, .limit = 3 },
        });
        _ = i;
        try std.testing.expectEqual(.plain_hash, explained.aggregate.plan.dispatch.strategy);
        try std.testing.expect(explained.aggregate.plan.reasons.contains(.small_ndv));
        try std.testing.expect(!explained.encoded);
    };
}

test "explainAggregate: partitioned force agrees for API and environment" {
    for ([_]bool{ false, true }) |api| {
        var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
        defer arena.deinit();
        const lines = [_:null]?[*:0]const u8{
            "ZPQ_IO_PROFILE=mmap", if (api) "ZPQ_PARTAGG=0" else "ZPQ_PARTAGG=1",
        };
        const ctx: Context = .{
            .gpa = std.testing.allocator,
            .env = .{ .block = .{ .slice = &lines } },
            .io = std.testing.io,
        };
        const got = try explainAggregate(ctx, arena.allocator(), .{
            .inputs = &.{"ci/fixtures/parquet/topk_skew.parquet"},
            .group_by = "k",
            .aggregate = "count(*) AS c",
            .partagg = api,
        });
        try std.testing.expectEqual(.partitioned_hash, got.aggregate.plan.dispatch.strategy);
        var out: std.Io.Writer.Allocating = .init(std.testing.allocator);
        defer out.deinit();
        try writeAggregateExplanationJson(&out.writer, got);
        const json = try std.json.parseFromSlice(std.json.Value, std.testing.allocator, out.written(), .{});
        defer json.deinit();
        try std.testing.expectEqualStrings("shared_partitions", json.value.object.get("merge").?.string);
    }
}

test "limited Parquet row sink restores borrowed buffers on every allocation failure" {
    const Test = struct {
        fn run(gpa: std.mem.Allocator, fd: local_fs.fd_t) !void {
            var arena = std.heap.ArenaAllocator.init(gpa);
            defer arena.deinit();
            var values = [_]i64{ 11, 22, 33 };
            var borrowed: std.ArrayList(i64) = .empty;
            borrowed.items = &values;
            borrowed.capacity = values.len;
            var columns = [_]consumer.OutputAggregator.ColumnBuf{
                .{ .i64 = .{ .values = borrowed } },
            };
            var elems = [_]schema.SchemaElement{
                .{ .name = "v", .type = .INT64, .repetition_type = .REQUIRED, .type_length = null, .num_children = null, .scale = null, .precision = null, .field_id = null },
            };
            const name = [_][]const u8{"v"};
            var paths = [_][]const []const u8{&name};
            var agg: consumer.OutputAggregator = .{
                .arena = arena.allocator(),
                .cols = &columns,
                .schema_elems = &elems,
                .paths = &paths,
                .num_rows = values.len,
                .captured = true,
            };
            var sink: ParquetPrint = .{
                .arena = arena.allocator(),
                .gpa = gpa,
                .fd_sink = .{ .fd = fd },
                .codec = .UNCOMPRESSED,
                .zstd = .{ .lambda = CompressPriority.size.lambda() },
                .schema_items = .empty,
            };
            var total: usize = 0;
            var skip: usize = 1;
            defer {
                std.debug.assert(agg.num_rows == values.len);
                std.debug.assert(agg.cols[0].i64.values.items.ptr == &values);
                std.debug.assert(agg.cols[0].i64.values.items.len == values.len);
            }
            try sink.append(&agg, 1, &total, &skip);
            try std.testing.expectEqual(@as(usize, 1), total);
        }
    };
    const fd = try std.posix.openat(std.posix.AT.FDCWD, "/dev/null", .{ .ACCMODE = .WRONLY }, 0);
    defer local_fs.close(fd);
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Test.run, .{fd});
}

test "rowGroupBest reads the first key a row group can reach, and only what its statistics prove" {
    const testing = std.testing;
    const fixtures = @import("core/parquet/test_fixtures.zig");
    var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_state.deinit();
    const a = arena_state.allocator();
    const asc: RowSortKey = .{ .spec = .{ .passthrough = 0 }, .descending = false, .unsigned = false };
    const desc: RowSortKey = .{ .spec = .{ .passthrough = 0 }, .descending = true, .unsigned = false };
    var lo: [8]u8 = undefined;
    var hi: [8]u8 = undefined;
    std.mem.writeInt(i64, &lo, -7, .little);
    std.mem.writeInt(i64, &hi, 40, .little);

    var c = try fixtures.StatsColumn.init(a, fixtures.statsLeaf(.INT64), .{ .min_value = &lo, .max_value = &hi });
    try testing.expectEqual(@as(i128, -7), rowGroupBest(&c.rg, &c.fm, asc).?.int);
    try testing.expectEqual(@as(i128, 40), rowGroupBest(&c.rg, &c.fm, desc).?.int);
    const computed: RowSortKey = .{ .spec = .{ .computed = .{ .expr = undefined, .alias = "e" } }, .descending = false, .unsigned = false };
    try testing.expect(rowGroupBest(&c.rg, &c.fm, computed) == null);

    // Unsigned: the max's bits are a value above every signed one.
    var uleaf = fixtures.statsLeaf(.INT64);
    uleaf.logical_type = .{ .INTEGER = .{ .bitWidth = 64, .isSigned = false } };
    const all_ones: [8]u8 = @splat(0xff);
    c = try fixtures.StatsColumn.init(a, uleaf, .{ .min_value = &lo, .max_value = &all_ones });
    const udesc: RowSortKey = .{ .spec = .{ .passthrough = 0 }, .descending = true, .unsigned = true };
    try testing.expectEqual(@as(i128, std.math.maxInt(u64)), rowGroupBest(&c.rg, &c.fm, udesc).?.int);

    // DECIMAL is not read; a double proves its min ascending, nothing descending, and a NaN bound nothing.
    var dec = fixtures.statsLeaf(.INT64);
    dec.logical_type = .{ .DECIMAL = .{ .scale = 2, .precision = 18 } };
    c = try fixtures.StatsColumn.init(a, dec, .{ .min_value = &lo, .max_value = &hi });
    try testing.expect(rowGroupBest(&c.rg, &c.fm, asc) == null);
    var dlo: [8]u8 = undefined;
    std.mem.writeInt(u64, &dlo, @bitCast(@as(f64, -2.5)), .little);
    c = try fixtures.StatsColumn.init(a, fixtures.statsLeaf(.DOUBLE), .{ .min_value = &dlo, .max_value = &dlo });
    try testing.expectEqual(@as(f64, -2.5), rowGroupBest(&c.rg, &c.fm, asc).?.float);
    try testing.expect(rowGroupBest(&c.rg, &c.fm, desc) == null);
    std.mem.writeInt(u64, &dlo, @bitCast(std.math.nan(f64)), .little);
    c = try fixtures.StatsColumn.init(a, fixtures.statsLeaf(.DOUBLE), .{ .min_value = &dlo, .max_value = &dlo });
    try testing.expect(rowGroupBest(&c.rg, &c.fm, asc) == null);

    // Strings in bytewise order; a group of NULLs only is NULL at best; no statistics prove nothing.
    var sleaf = fixtures.statsLeaf(.BYTE_ARRAY);
    sleaf.logical_type = .{ .STRING = .{} };
    c = try fixtures.StatsColumn.init(a, sleaf, .{ .min_value = "apple", .max_value = "pear" });
    try testing.expectEqualStrings("pear", rowGroupBest(&c.rg, &c.fm, desc).?.str);
    c = try fixtures.StatsColumn.init(a, fixtures.statsLeaf(.INT64), .{ .null_count = 100 });
    try testing.expect(rowGroupBest(&c.rg, &c.fm, asc).? == .null);
    c = try fixtures.StatsColumn.init(a, fixtures.statsLeaf(.INT64), .{ .null_count = 3 });
    try testing.expect(rowGroupBest(&c.rg, &c.fm, asc) == null);
}

test "TopRows.cutsOff passes over a row group only when its best first key sorts strictly after the last row kept" {
    const testing = std.testing;
    inline for (.{ false, true }) |descending| {
        const keys = [_]RowSortKey{.{ .spec = .{ .passthrough = 0 }, .descending = descending, .unsigned = false }};
        var top: TopRows = undefined;
        top.keys = &keys;
        top.h = 2;
        top.heap = .empty;
        top.heap.context = &top;
        defer top.heap.deinit(testing.allocator);
        var c5 = [_]SortCell{.{ .int = 5 }};
        var c9 = [_]SortCell{.{ .int = 9 }};
        try top.heap.push(testing.allocator, .{ .cells = &c5, .pos = .{ .file = 0, .rg = 3 }, .row = 0 });
        // Not full yet: nothing is ruled out.
        try testing.expect(!top.cutsOff(.{ .int = 1000 }));
        try top.heap.push(testing.allocator, .{ .cells = &c9, .pos = .{ .file = 0, .rg = 1 }, .row = 1 });
        // The last row kept is 9 ascending, 5 descending. A tie is kept: the group's rows may sit earlier.
        const last: i128 = if (descending) 5 else 9;
        const worse: i128 = if (descending) 4 else 10;
        const better: i128 = if (descending) 6 else 8;
        try testing.expect(top.cutsOff(.{ .int = worse }));
        try testing.expect(!top.cutsOff(.{ .int = last }));
        try testing.expect(!top.cutsOff(.{ .int = better }));
        // NULL sorts last either way.
        try testing.expect(top.cutsOff(.null));
    }
    // Once the last row kept is NULL, no group sorts strictly after it.
    const keys = [_]RowSortKey{.{ .spec = .{ .passthrough = 0 }, .descending = false, .unsigned = false }};
    var top: TopRows = undefined;
    top.keys = &keys;
    top.h = 1;
    top.heap = .empty;
    top.heap.context = &top;
    defer top.heap.deinit(testing.allocator);
    var cn = [_]SortCell{.null};
    try top.heap.push(testing.allocator, .{ .cells = &cn, .pos = .{ .file = 0, .rg = 0 }, .row = 0 });
    try testing.expect(!top.cutsOff(.null));
    try testing.expect(!top.cutsOff(.{ .int = 1 }));
}

test "RankedRG.sooner reads unbounded row groups first in input order, then the most promising, NULL last" {
    const testing = std.testing;
    inline for (.{ false, true }) |descending| {
        const key: RowSortKey = .{ .spec = .{ .passthrough = 0 }, .descending = descending, .unsigned = false };
        var rgs = [_]RankedRG{
            .{ .file = 0, .rg = 0, .best = .{ .int = 3 } },
            .{ .file = 0, .rg = 1, .best = null },
            .{ .file = 0, .rg = 2, .best = .null },
            .{ .file = 1, .rg = 0, .best = .{ .int = 7 } },
            .{ .file = 1, .rg = 1, .best = null },
            .{ .file = 1, .rg = 2, .best = .{ .int = 3 } },
        };
        std.mem.sort(RankedRG, &rgs, key, RankedRG.sooner);
        const want: [6][2]u32 = if (descending)
            .{ .{ 0, 1 }, .{ 1, 1 }, .{ 1, 0 }, .{ 0, 0 }, .{ 1, 2 }, .{ 0, 2 } }
        else
            .{ .{ 0, 1 }, .{ 1, 1 }, .{ 0, 0 }, .{ 1, 2 }, .{ 1, 0 }, .{ 0, 2 } };
        for (rgs, want) |r, w| {
            try testing.expectEqual(w[0], r.file);
            try testing.expectEqual(w[1], r.rg);
        }
    }
}

test "dictionaryKey takes a bare string first key, judged by the filter only when it reads that column alone" {
    std.Io.Dir.cwd().access(std.testing.io, topn_gather_inputs[0], .{}) catch return error.SkipZigTest;
    const gpa = std.testing.allocator;
    var arena_state = std.heap.ArenaAllocator.init(gpa);
    defer arena_state.deinit();
    const a = arena_state.allocator();
    const meta = try metadata.open(a, try metadata.readFileSlice(topn_gather_inputs[0], a));
    const key = struct {
        fn of(ci: usize) RowSortKey {
            return .{ .spec = .{ .passthrough = ci }, .descending = false, .unsigned = false };
        }
    }.of;
    const on_c = try filter_parser.parse(a, "c != ''", &meta, null);
    const on_k = try filter_parser.parse(a, "c != '' AND k > 3", &meta, null);
    try std.testing.expectEqual(DictKey{ .ci = 7, .filtered = true }, dictionaryKey(&meta, key(7), on_c).?);
    try std.testing.expectEqual(DictKey{ .ci = 7, .filtered = false }, dictionaryKey(&meta, key(7), on_k).?);
    try std.testing.expectEqual(DictKey{ .ci = 2, .filtered = false }, dictionaryKey(&meta, key(2), null).?);
    // Not strings: an integer, and a DECIMAL whose lane is numeric.
    try std.testing.expect(dictionaryKey(&meta, key(0), null) == null);
    try std.testing.expect(dictionaryKey(&meta, key(5), null) == null);
}

test "allPassthrough keeps a computed output or key off the bound-first Top-N" {
    const testing = std.testing;
    const bare = [_]consumer.OutputCol{ .{ .passthrough = 0 }, .{ .passthrough = 3 } };
    const key: RowSortKey = .{ .spec = .{ .passthrough = 1 }, .descending = false, .unsigned = false };
    try testing.expect(allPassthrough(&bare, &.{key}));
    const computed: consumer.OutputCol = .{ .computed = .{ .expr = undefined, .alias = "z" } };
    try testing.expect(!allPassthrough(&.{ bare[0], computed }, &.{key}));
    try testing.expect(!allPassthrough(&bare, &.{ key, .{ .spec = computed, .descending = true, .unsigned = false } }));
}

const topn_gather_inputs = [_][]const u8{
    "ci/fixtures/parquet/topn_gather_a.parquet",
    "ci/fixtures/parquet/topn_gather_b.parquet",
};

const TopnCase = struct {
    inputs: []const []const u8 = &topn_gather_inputs,
    max_memory: ?usize = null,
    select: ?[]const u8,
    filter: ?[]const u8 = null,
    keys: []const expr_ast.RowKey,
    limit: usize,
    offset: usize = 0,
};

/// Run a plain-row Top-N over the gather fixtures and return what it printed, on `gpa`.
fn topnPrint(gpa: std.mem.Allocator, env: std.process.Environ, c: TopnCase, j: usize, path: []const u8) ![]u8 {
    const fd = try local_fs.createFile(path);
    {
        defer local_fs.close(fd);
        var args: QueryArgs = .{
            .inputs = c.inputs,
            .select = c.select,
            .filter = c.filter,
            .result_order = .{ .rows = c.keys, .limit = c.limit, .offset = c.offset },
            .parallelism = j,
        };
        if (c.max_memory) |m| args.max_memory = m;
        _ = try runPrintTo(.{ .gpa = gpa, .env = env, .io = std.testing.io }, args, .jsonl, c.limit, fd, c.offset);
    }
    return metadata.readFileSlice(path, gpa);
}

test "the parallel plain-row Top-N prints what reading every row group in order prints, at every -j" {
    std.Io.Dir.cwd().access(std.testing.io, topn_gather_inputs[1], .{}) catch return error.SkipZigTest;
    const gpa = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var path_buf: [96]u8 = undefined;
    const path = try std.fmt.bufPrint(&path_buf, ".zig-cache/tmp/{s}/out.jsonl", .{tmp.sub_path});
    const serial_block = try gpa.allocSentinel(?[*:0]const u8, 1, null);
    defer gpa.free(serial_block);
    serial_block[0] = "ZPQ_TOPN_GATHER=0";
    const serial: std.process.Environ = .{ .block = .{ .slice = serial_block } };

    const cases = [_]TopnCase{
        // Ties within and across row groups and files; NULL keys last either way.
        .{ .select = "id, k, s", .keys = &.{.{ .expr = "k", .descending = false }}, .limit = 7 },
        .{ .select = "id, k", .keys = &.{.{ .expr = "k", .descending = true }}, .limit = 25, .offset = 40 },
        // A dictionary turning PLAIN mid-chunk, under a filter leaving gaps between survivors.
        .{ .select = "id, s", .filter = "k > 3", .keys = &.{.{ .expr = "s", .descending = true }}, .limit = 9, .offset = 4 },
        .{ .select = null, .filter = "s LIKE 's01%'", .keys = &.{
            .{ .expr = "k", .descending = true },
            .{ .expr = "s", .descending = false },
        }, .limit = 20 },
        // NaN and NULL floats, unsigned and DECIMAL order, a timestamp.
        .{ .select = "id, f", .keys = &.{.{ .expr = "f", .descending = false }}, .limit = 12 },
        .{ .select = "id, f", .keys = &.{.{ .expr = "f", .descending = true }}, .limit = 12 },
        .{ .select = "id, u", .keys = &.{.{ .expr = "u", .descending = true }}, .limit = 5 },
        .{ .select = "u, id", .keys = &.{.{ .expr = "u", .descending = false }}, .limit = 5 },
        .{ .select = "dec, id, ts", .keys = &.{
            .{ .expr = "dec", .descending = true },
            .{ .expr = "id", .descending = false },
        }, .limit = 8, .offset = 3 },
        // More rows than a row group holds, and an OFFSET reaching the last rows.
        .{ .select = "id, ts, s", .keys = &.{.{ .expr = "ts", .descending = false }}, .limit = 1500 },
        .{ .select = "id", .keys = &.{.{ .expr = "k", .descending = false }}, .limit = 5, .offset = 5990 },
        // A string key coded by its dictionary throughout, whose footer minimum ('') proves nothing: the dictionary
        // bounds each row group, judged by a filter on that column alone, by one on others, or by none.
        .{ .select = "id, c", .filter = "c != ''", .keys = &.{.{ .expr = "c", .descending = false }}, .limit = 6 },
        .{ .select = "id, c", .filter = "c != ''", .keys = &.{.{ .expr = "c", .descending = true }}, .limit = 6, .offset = 2 },
        .{ .select = "c, k", .filter = "k > 30", .keys = &.{
            .{ .expr = "c", .descending = false },
            .{ .expr = "k", .descending = true },
        }, .limit = 9 },
        .{ .select = "id, c", .keys = &.{.{ .expr = "c", .descending = false }}, .limit = 4 },
        .{ .select = "id, c", .filter = "c IS NULL OR c > 'c55'", .keys = &.{.{ .expr = "c", .descending = false }}, .limit = 30 },
        // No row survives the filter.
        .{ .select = "id", .filter = "id < 0", .keys = &.{.{ .expr = "id", .descending = false }}, .limit = 3 },
    };
    for (cases) |c| {
        const want = try topnPrint(gpa, serial, c, 1, path);
        defer gpa.free(want);
        for ([_]usize{ 1, 3, 16 }) |j| {
            const got = try topnPrint(gpa, .empty, c, j, path);
            defer gpa.free(got);
            std.testing.expectEqualStrings(want, got) catch |err| {
                std.debug.print("case: select {?s} where {?s} limit {d} offset {d} -j{d}\n", .{ c.select, c.filter, c.limit, c.offset, j });
                return err;
            };
        }
    }
}

test "the parallel plain-row Top-N fails an allocation cleanly wherever it happens" {
    std.Io.Dir.cwd().access(std.testing.io, topn_gather_inputs[1], .{}) catch return error.SkipZigTest;
    const gpa = std.testing.allocator;
    var setup = std.heap.ArenaAllocator.init(gpa);
    defer setup.deinit();
    const sa = setup.allocator();
    // Footer opening has failures of its own to report (an allocation there reads as bad metadata); open first.
    var inputs: [topn_gather_inputs.len]scan.Input = undefined;
    var metas: [topn_gather_inputs.len]schema.FileMetaData = undefined;
    for (topn_gather_inputs, &inputs, &metas) |path, *in, *m| {
        const bytes = try metadata.readFileSlice(path, sa);
        in.* = .{ .name = path, .bytes = bytes };
        m.* = try metadata.open(sa, bytes);
    }
    const args: QueryArgs = .{
        .inputs = &topn_gather_inputs,
        .select = "id, s, dec",
        .filter = "k > 40",
        .result_order = .{ .rows = &.{
            .{ .expr = "s", .descending = true },
            .{ .expr = "id", .descending = false },
        }, .limit = 4 },
        .parallelism = 1,
    };
    const proj = try planProjection(sa, args, &metas[0]);
    const items = try rankRowGroups(sa, &inputs, &metas, proj.row_keys[0], true);
    try std.testing.checkAllAllocationFailures(gpa, struct {
        fn run(a: std.mem.Allocator, x: QueryArgs, ins: []const scan.Input, ms: []const schema.FileMetaData, p: ProjectionPlan, its: []RankedRG) !void {
            // An arena that may grow a block in place allocates a varying number of times, by where the heap left
            // room; refusing every resize keeps the count fixed from run to run.
            var fixed: NoResizeAllocator = .{ .child = a };
            const fa = fixed.allocator();
            var arena = std.heap.ArenaAllocator.init(fa);
            defer arena.deinit();
            var t: Timings = .{};
            const ctx: Context = .{ .gpa = fa, .env = .empty, .io = std.testing.io };
            var g = (try rankAndGather(ctx, arena.allocator(), x, ins, ms, p.filter, p.fetch, p.output_specs, p.row_keys, 4, its, &t)).?;
            defer g.deinit();
            try std.testing.expectEqual(@as(usize, 4), g.out.num_rows);
        }
    }.run, .{ args, @as([]const scan.Input, &inputs), @as([]const schema.FileMetaData, &metas), proj, items });
}

/// Passes allocations through and refuses to grow or move one in place.
const NoResizeAllocator = struct {
    child: std.mem.Allocator,

    fn allocator(self: *NoResizeAllocator) std.mem.Allocator {
        return .{ .ptr = self, .vtable = &.{
            .alloc = alloc,
            .resize = std.mem.Allocator.noResize,
            .remap = std.mem.Allocator.noRemap,
            .free = free,
        } };
    }

    fn alloc(ctx: *anyopaque, n: usize, a: std.mem.Alignment, ra: usize) ?[*]u8 {
        const self: *NoResizeAllocator = @ptrCast(@alignCast(ctx));
        return self.child.rawAlloc(n, a, ra);
    }

    fn free(ctx: *anyopaque, m: []u8, a: std.mem.Alignment, ra: usize) void {
        const self: *NoResizeAllocator = @ptrCast(@alignCast(ctx));
        self.child.rawFree(m, a, ra);
    }
};

/// Counts the bytes held through it and the most held at once.
const PeakAllocator = struct {
    child: std.mem.Allocator,
    held: usize = 0,
    peak: usize = 0,

    fn allocator(self: *PeakAllocator) std.mem.Allocator {
        return .{ .ptr = self, .vtable = &.{ .alloc = alloc, .resize = resize, .remap = remap, .free = free } };
    }

    fn note(self: *PeakAllocator, old: usize, new: usize) void {
        self.held = self.held - old + new;
        self.peak = @max(self.peak, self.held);
    }

    fn alloc(ctx: *anyopaque, n: usize, a: std.mem.Alignment, ra: usize) ?[*]u8 {
        const self: *PeakAllocator = @ptrCast(@alignCast(ctx));
        const p = self.child.rawAlloc(n, a, ra) orelse return null;
        self.note(0, n);
        return p;
    }

    fn resize(ctx: *anyopaque, m: []u8, a: std.mem.Alignment, n: usize, ra: usize) bool {
        const self: *PeakAllocator = @ptrCast(@alignCast(ctx));
        if (!self.child.rawResize(m, a, n, ra)) return false;
        self.note(m.len, n);
        return true;
    }

    fn remap(ctx: *anyopaque, m: []u8, a: std.mem.Alignment, n: usize, ra: usize) ?[*]u8 {
        const self: *PeakAllocator = @ptrCast(@alignCast(ctx));
        const p = self.child.rawRemap(m, a, n, ra) orelse return null;
        self.note(m.len, n);
        return p;
    }

    fn free(ctx: *anyopaque, m: []u8, a: std.mem.Alignment, ra: usize) void {
        const self: *PeakAllocator = @ptrCast(@alignCast(ctx));
        self.child.rawFree(m, a, ra);
        self.note(m.len, 0);
    }
};

test "the parallel plain-row Top-N answers within the memory budget wherever reading in order does" {
    // Review repros: winners scattered through one dictionary-coded row group, so the gather reads many short runs.
    const small = "ci/fixtures/parquet/topn_budget_80.parquet";
    const wide = "ci/fixtures/parquet/topn_budget_12k.parquet";
    std.Io.Dir.cwd().access(std.testing.io, wide, .{}) catch return error.SkipZigTest;
    const gpa = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var path_buf: [96]u8 = undefined;
    const path = try std.fmt.bufPrint(&path_buf, ".zig-cache/tmp/{s}/out.jsonl", .{tmp.sub_path});
    const serial_block = try gpa.allocSentinel(?[*:0]const u8, 1, null);
    defer gpa.free(serial_block);
    serial_block[0] = "ZPQ_TOPN_GATHER=0";
    const serial: std.process.Environ = .{ .block = .{ .slice = serial_block } };
    const by_s: []const expr_ast.RowKey = &.{.{ .expr = "s", .descending = false }};

    for ([_]usize{ 10, 20, 40, 80 }) |limit| for ([_]usize{ 8192, 16384, 32768, 65536, 131072 }) |budget| {
        const c: TopnCase = .{ .inputs = &.{small}, .max_memory = budget, .select = null, .keys = by_s, .limit = limit };
        const want = topnPrint(gpa, serial, c, 1, path) catch |err| switch (err) {
            error.ExceededMemoryBudget => continue,
            else => return err,
        };
        defer gpa.free(want);
        for ([_]usize{ 1, 3, 16 }) |j| {
            const got = try topnPrint(gpa, .empty, c, j, path);
            defer gpa.free(got);
            try std.testing.expectEqualStrings(want, got);
        }
    };

    for ([_]usize{ 100, 500, 1000, 2000 }) |limit| {
        const c: TopnCase = .{ .inputs = &.{wide}, .max_memory = 8 << 20, .select = null, .keys = by_s, .limit = limit };
        const want = try topnPrint(gpa, serial, c, 1, path);
        defer gpa.free(want);
        var peak: PeakAllocator = .{ .child = gpa };
        const got = try topnPrint(peak.allocator(), .empty, c, 1, path);
        defer gpa.free(got);
        try std.testing.expectEqualStrings(want, got);
        // Its own reservations stay within half the budget; decoding the row group and the printed answer are the rest.
        try std.testing.expect(peak.peak < 8 << 20);
    }
}

test "in-process text sink flushes across buffer boundaries and allocation failures" {
    const Check = struct {
        fn write(gpa: std.mem.Allocator) !void {
            var out: std.Io.Writer.Allocating = .init(gpa);
            defer out.deinit();
            var w: StdoutWriter = .{ .sink = &out.writer };
            const bytes: [5000]u8 = @splat('x');
            w.writeAll(&bytes) catch return error.OutOfMemory;
            w.flush() catch return error.OutOfMemory;
            try std.testing.expectEqualStrings(&bytes, out.written());
            try std.testing.expectEqual(@as(u64, bytes.len), w.bytes_written);
        }
    };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Check.write, .{});
}
