const std = @import("std");
const s3 = @import("../io/s3.zig");
const pool_mod = @import("../io/pool.zig");
const profile = @import("../profile.zig");
const local_fs = @import("../local_fs.zig");
const meta_cache_mod = @import("../io/meta_cache.zig");
const local_reader = @import("../io/local_reader.zig");
const schema = @import("../core/schema.zig");
const core_memory = @import("../core/memory.zig");
const compression = @import("../core/parquet/compression.zig");
const consumer = @import("../core/consumer.zig");
const scan = @import("../core/scan.zig");
const expr_ast = @import("../core/expr/ast.zig");
const expr_agg = @import("../core/expr/agg.zig");
const filter_ast = @import("../core/filter/ast.zig");
const membership = @import("../core/filter/membership.zig");
const ResultProgram = @import("../core/result_program.zig").ResultProgram;
const Diag = @import("../core/diag.zig").Diag;
const text_format = @import("text_types.zig");
pub const bound_plan = @import("bound_plan.zig");
const TextKind = text_format.TextKind;
const AnswerType = text_format.AnswerType;
const DecimalColumns = text_format.DecimalColumns;

/// S3 pool permits. Data fetches may use all of them; footer fetches stay at `READ_CONCURRENCY`.
pub const POOL_SIZE: usize = s3.S3_IN_FLIGHT_MAX;

/// Permits for an S3 output's part uploads, apart from the input reads' so neither waits for the other, over the same
/// pool so uploads reuse the connections reads leave idle. As many as uploads had while they shared the read permits.
pub const OUTPUT_POOL_SIZE: usize = s3.MAX_PARTS;
pub const OutputLane = pool_mod.Lane(s3.Pool(POOL_SIZE), OUTPUT_POOL_SIZE);

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
    /// A `row_groups` range past its input's row groups, or not one range (or null) per input.
    RowGroupOutOfRange,
} || std.mem.Allocator.Error;

/// Typed synchronous handoff. Columns borrow the query; a sink must own anything it retains.
pub const ColumnarSink = struct {
    ctx: *anyopaque,
    schema_fn: *const fn (*anyopaque, []const expr_agg.OutputCol) anyerror!void,
    batch_fn: *const fn (*anyopaque, []const expr_agg.OutputCol, usize, usize) anyerror!void,
};

pub const QueryArgs = struct {
    /// Immutable frontend syntax. Every engine invocation owns its own metadata-bound cache.
    plan_source: ?bound_plan.Source = null,
    /// Internal synchronous handoff; callers supply plan_source, never a cache retained across invocations.
    plan_cache: ?*bound_plan.Cache = null,
    /// Force eligible GROUP BY queries through shared partitions.
    partagg: bool = false,
    inputs: []const []const u8, // raw paths/URLs (pre-glob — caller globs)
    output: ?[]const u8 = null,
    /// Sync local output data before atomic publication (CLI and libzpq).
    fsync: bool = @import("../output_publication.zig").default_fsync,
    /// An already-open sink and an empty scalar result cut.
    output_fd: ?local_fs.fd_t = null,
    /// CLI-only: the answer is only written to Parquet, so a GROUP BY may build it as typed columns
    /// (`scan.MultiAggArgs.typed_answer`), never read back as rows.
    typed_answer: bool = false,
    /// Consumed synchronously while query inputs, bound output types and the ledger are alive.
    text_output: ?struct { format: PrintFormat, fd: local_fs.fd_t = 1, sink: ?*std.Io.Writer = null } = null,
    /// Envelope consumers also drain inside the query lifetime; the borrowed answer must not escape the callback.
    aggregate_sink: ?struct {
        ctx: *anyopaque,
        write_fn: *const fn (*anyopaque, AggResult) anyerror!void,
    } = null,
    columnar_sink: ?ColumnarSink = null,
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
    /// Receives, for a plain-row print, the names of its DECIMAL output columns (duplicated on its `gpa`), so a
    /// reader of the text can parse just those exactly. An aggregate says so in `AggResult.answer_types`.
    decimal_columns: ?*DecimalColumns = null,
    /// Receives what a failure was about; see `core/diag.zig`.
    diag: ?*Diag = null,
    /// Requested output codec. Null when none was asked for: re-encoded
    /// output is then SNAPPY and byte copies keep each chunk's own codec.
    codec: ?schema.CompressionCodec = null,
    parquet_version: schema.ParquetVersion = .v2,
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
    /// `--cache` and libzpq `"cache"`: read local files through the page cache, never `O_DIRECT`, so the next
    /// query on them runs warm. Off, a cold planned read that fits the reader uses `O_DIRECT` io_uring.
    cache: bool = false,
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
    /// The `--max-memory` the caller set, when it set one: a write's row groups shrink to fit it
    /// (`RowGroupTarget.under`). A budget the engine chose itself never changes what is written.
    max_memory_explicit: ?usize = null,
    /// Where a re-encoding write cuts its output row groups (`--row-group-size`, `--row-group-bytes`).
    row_group: RowGroupTarget = .{},
    /// Records the query's spans when set (`--profile`, the Lambda's `"profile"`); see `profile.zig`.
    profiler: ?*profile.Profiler = null,
    /// Output columns to write split-block Bloom filters for (`--bloom-filter`). A copied chunk keeps the filter it
    /// has; one without is decoded to build one.
    bloom_filters: []const BloomSpec = &.{},
    bloom_filters_enabled: bool = true,
    /// Per input, parallel to `inputs`: read only row groups `start` up to but not including `end` of its footer; null
    /// (or a null slice) reads every row group. A coordinator splits one file across workers with it.
    row_groups: ?[]const ?[2]u32 = null,

    pub fn kind(self: QueryArgs) bound_plan.Kind {
        if (self.plan_source) |source| return source.kind;
        return if (self.group_by != null) .grouped else if (self.aggregate != null) .scalar else .rows;
    }

    pub inline fn withPlanCache(self: QueryArgs, cache: *bound_plan.Cache) QueryArgs {
        var args = self;
        if (args.plan_source != null) args.plan_cache = cache;
        return args;
    }

    pub fn bound(self: QueryArgs, arena: std.mem.Allocator, meta: *const schema.FileMetaData) !?*const bound_plan.Query {
        const source = self.plan_source orelse return null;
        const cache = self.plan_cache orelse return error.InvalidBoundPlan;
        return try cache.get(source, arena, meta, self.diag, .{
            .trust_stats = self.trust_stats,
            .nested_output = self.output != null,
        });
    }

    pub fn boundAggregate(self: QueryArgs, arena: std.mem.Allocator, meta: *const schema.FileMetaData) !?*const bound_plan.Aggregate {
        const query = (try self.bound(arena, meta)) orelse return null;
        if (query.* != .aggregate) return error.InvalidBoundPlan;
        return &query.aggregate;
    }
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
    /// Optional context-owned worker runtime. Its threads and codec TLS survive this query.
    worker_io: ?std.Io = null,
    /// Optional long-lived metadata cache. Lambda uses this to skip
    /// repeated footer fetches on warm-container repeat invocations
    /// against the same object (validated via If-None-Match
    /// conditional GET on every hit). CLI passes null.
    meta_cache: ?*meta_cache_mod.MetaCache = null,
    /// The process exits once this query's answer is out (the CLI). Input mappings and scan workers' regions are then
    /// left to the exit: unmapping a large populated mapping first holds the answer back by milliseconds, and the
    /// kernel tears it down at exit regardless. The CLI drops the input mappings' pages in parallel once the answer is
    /// out (`local_fs.releaseLeft`). Never for a process that runs another query (the Lambda).
    exit_after: bool = false,

    /// The context one query runs in, for every entry point and query kind alike: only a `--max-memory` the caller
    /// set pays for promptly released buffer mappings (`memory.queryAllocator`).
    pub fn forQuery(self: Context, args: *const QueryArgs) Context {
        var ctx = self;
        ctx.gpa = core_memory.queryAllocator(self.gpa, args.max_memory_explicit != null);
        return ctx;
    }
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
    /// Rows in every row group of the inputs; of those, the rows in row groups read (not pruned), and the rows the
    /// filter kept, which are the rows written.
    rows_in: i64,
    rows_scanned: i64,
    rows_matched: i64,
    bytes_in: u64,
    bytes_out: u64,
    row_groups_in: usize,
    row_groups_kept: usize,
    /// Row groups statistics (or membership, or a NaN probe) ruled out before any page was read.
    row_groups_pruned: usize = 0,
    /// Bloom filter probing: what it read and the row groups it ruled out beyond statistics.
    membership: membership.Stats = .{},
    timings: Timings,
};

pub const AggResult = struct {
    files_in: usize,
    /// As `scan.MultiAggResult`'s: every input row, the rows in row groups read, and the rows the filter kept.
    rows_in: i64,
    rows_scanned: i64,
    rows_matched: i64,
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
    /// How each answer column's byte values are printed (`writeAggregateText`), owned in `Context.gpa`.
    text_kinds: []const TextKind = &.{},
    /// What each answer column needs to print as CSV (`writeAggregateText`), owned in `Context.gpa`.
    answer_types: []const AnswerType = &.{},

    pub fn deinit(self: *AggResult, gpa: std.mem.Allocator) void {
        self.output.deinit(gpa);
        gpa.free(self.text_kinds);
        gpa.free(self.answer_types);
    }
};

pub const Timings = struct {
    /// Monotonic start of output work, including gaps between writes. The CLI ends it after publication.
    output_started_ns: i64 = 0,
    read_ns: u64 = 0,
    parse_ns: u64 = 0,
    core: consumer.Timings = .{},
    /// Wall-clock of the scan: an aggregate's parallel region (see scan.Timings.decode_wall_ns); a write's or a row
    /// output's, from its first row group to its footer.
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

pub const AggregateExplanation = struct {
    aggregate: scan.AggregateExplanation,
    io_mode: enum { mmap, pread, uring, s3_buffered, s3_windowed },
    /// Why `io_mode` was chosen. Remote plans use `remote`.
    io_reason: local_reader.Reason = .remote,
    encoded: bool,
    max_memory: usize,
    sketch: ?scan.SketchTopK,
    result: ResultExplanation = .{},
    /// The top-level AND's children in the order they run (`filter_ast.rankConjuncts`); one for a lone predicate.
    conjuncts: []const Conjunct = &.{},
};

pub const Conjunct = struct { column: []const u8, cost: filter_ast.Cost };

/// The result program's part of a plan: HAVING, hidden ORDER BY/HAVING columns, and whether the strategies may cut by
/// the LIMIT (`masked` under HAVING, `none` without one).
pub const ResultExplanation = struct {
    having: bool = false,
    hidden: usize = 0,
    order_keys: usize = 0,
    cut_limit: enum { none, kept, masked } = .none,

    pub fn of(order: expr_ast.ResultOrder) ResultExplanation {
        return .{
            .having = order.having != null,
            .hidden = order.hidden.len,
            .order_keys = order.keys.len,
            .cut_limit = if (order.limit == null) .none else if (ResultProgram.cutOrder(order).limit == null) .masked else .kept,
        };
    }
};

/// What output row groups are cut at: a row group is closed once it holds `rows` rows or its rows' PLAIN values take
/// `bytes` bytes (`consumer.plainBits`), whichever comes first. Both come from the query's options alone, so the
/// layout depends only on the rows written: never on `-j`, the machine's memory, timing, or the input's row groups.
pub const RowGroupTarget = struct {
    rows: usize = default_row_group_rows,
    bytes: u64 = default_row_group_bytes,

    /// The target under an explicit `--max-memory`: row groups no bigger than a sixteenth of it, so the rows held
    /// for one and its encoding fit beside the decoding the rest of the budget is for.
    pub fn under(self: RowGroupTarget, max_memory: ?usize) RowGroupTarget {
        var t = self;
        if (max_memory) |m| t.bytes = @max(1, @min(t.bytes, @as(u64, m) / 16));
        return t;
    }
};

/// 2^17 rows (DuckDB writes 122,880, Polars about 2^18) and 64 MiB: readers that split a file by row group read a
/// 2^20-row group of a 1M-row output 2-5x slower than 2^17-row ones (DuckDB, Polars), for 0.2% of file size and no
/// write time. The byte cap keeps a wide row group to what one writer buffers decoded and encodes at once.
pub const default_row_group_rows: usize = consumer.answer_rg_rows;
pub const default_row_group_bytes: u64 = 64 << 20;

pub const PrintFormat = enum { csv, jsonl, json, parquet };

test "BloomSpec.parse: a column, optionally with a probability" {
    try std.testing.expectEqualDeep(BloomSpec{ .column = "id" }, try BloomSpec.parse("id"));
    try std.testing.expectEqualDeep(BloomSpec{ .column = "id", .fpp = 0.05 }, try BloomSpec.parse(" id:0.05 "));
    // A suffix that is no number belongs to the name.
    try std.testing.expectEqualDeep(BloomSpec{ .column = "a:b" }, try BloomSpec.parse("a:b"));
    for ([_][]const u8{ "", "id:0", "id:1", "id:-0.1", "id:1.5", ":0.1" }) |bad| {
        try std.testing.expectError(error.BadBloomFilter, BloomSpec.parse(bad));
    }
}

/// A plain-row ORDER BY key bound to its input column (`expr_ast.RowKey`).
pub const RowSortKey = bound_plan.RowSortKey;
