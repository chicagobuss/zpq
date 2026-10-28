//! zpq's one query entry point.
//!
//! Both binaries (the CLI and the Lambda) are thin shells over
//! `runQuery(ctx, args) -> QueryResult`. Their job is purely:
//!   - CLI: argv → QueryArgs, format JSON to stdout, exit
//!   - Lambda: Runtime API JSON → QueryArgs, format JSON response, loop
//! Everything in between — input dispatch, parallel fetch, decode,
//! scan, output — lives here.
//!
//! The architecture follows the comparable engines: a per-file `scan.Input`
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

const engine_text = @import("engine/text.zig");
const engine_types = @import("engine/types.zig");
const engine_s3_rebase = @import("engine/s3_rebase.zig");
const engine_s3_fetch = @import("engine/s3_fetch.zig");
const engine_rank = @import("engine/rank.zig");
const engine_write_window = @import("engine/write_window.zig");
const engine_projection = @import("engine/projection.zig");
const engine_input = @import("engine/input.zig");
const engine_write = @import("engine/write.zig");
const engine_env = @import("engine/env.zig");
const engine_print = @import("engine/print.zig");
const engine_aggregate = @import("engine/aggregate.zig");
pub const fetchS3Schema = engine_input.fetchS3Schema;
const std = @import("std");
const nowMonoNs = @import("clock.zig").monoNs;
const profile = @import("profile.zig");
const local_fs = @import("local_fs.zig");
const local_reader = @import("io/local_reader.zig");

const schema = @import("core/schema.zig");
const compression = @import("core/parquet/compression.zig");
const decimal_mod = @import("core/parquet/decimal.zig");
const huge_pages = @import("core/huge_pages.zig");
const core_memory = @import("core/memory.zig");
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
const nanprobe = @import("core/filter/nanprobe.zig");
const sbbf = @import("core/parquet/bloom.zig");
const streaming = @import("core/writer/streaming.zig");
const fastpath = @import("core/writer/fastpath.zig");
const encoder = @import("core/writer/encoder.zig");
const page_index = @import("core/writer/page_index.zig");

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

const S3Fetcher = engine_s3_fetch.S3Fetcher;
const FetchWindow = engine_s3_fetch.FetchWindow;
pub const READ_CONCURRENCY = engine_s3_fetch.READ_CONCURRENCY;
const PAR1 = engine_s3_fetch.PAR1;
const planRanges = engine_s3_fetch.planRanges;
const appendSplitFetch = engine_s3_fetch.appendSplitFetch;
const orderFilterFirst = engine_s3_fetch.orderFilterFirst;
const FetchSpec = engine_s3_fetch.FetchSpec;
const S3Membership = engine_s3_fetch.S3Membership;
const releaseChargedTail = engine_s3_fetch.releaseChargedTail;
const trimRetainedSuffix = engine_s3_fetch.trimRetainedSuffix;
const appendRgFetchRanges = engine_s3_fetch.appendRgFetchRanges;
const ampCap = engine_s3_fetch.ampCap;
const fetchMetaBatch = engine_s3_fetch.fetchMetaBatch;
const ensureHeadMagic = engine_s3_fetch.ensureHeadMagic;

const lambda_window_cap = engine_s3_fetch.lambda_window_cap;
const s3FetchWindow = engine_s3_fetch.s3FetchWindow;

pub const POOL_SIZE = engine_types.POOL_SIZE;

pub const FOOTER_PROBE_DEFAULT = engine_s3_fetch.FOOTER_PROBE_DEFAULT;
pub const footerProbeBytes = engine_s3_fetch.footerProbeBytes;
pub const footerProbeFor = engine_s3_fetch.footerProbeFor;

const ByteAccount = engine_s3_fetch.ByteAccount;

pub const OUTPUT_POOL_SIZE = engine_types.OUTPUT_POOL_SIZE;
pub const OutputLane = engine_types.OutputLane;

pub const Error = engine_types.Error;
pub const ColumnarSink = engine_types.ColumnarSink;

pub const QueryArgs = engine_types.QueryArgs;
pub const CompressPriority = engine_types.CompressPriority;
pub const BloomSpec = engine_types.BloomSpec;
pub const Context = engine_types.Context;
pub const PoolRegistry = engine_types.PoolRegistry;
pub const QueryResult = engine_types.QueryResult;
pub const WriteResult = engine_types.WriteResult;
pub const AggResult = engine_types.AggResult;
pub const Timings = engine_types.Timings;

pub const runQuery = engine_aggregate.runQuery;
pub const explainAggregate = engine_aggregate.explainAggregate;

// ============================================================
// Aggregate path
// ============================================================

pub const AggregateExplanation = engine_types.AggregateExplanation;
pub const Conjunct = engine_types.Conjunct;

pub const ResultExplanation = engine_types.ResultExplanation;

// ============================================================
// Write path — projection / select / filter, output to local or s3
// ============================================================

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
// probe_io_concurrency / probe_backpressure spikes at e76a1e3 for why async + async_limit,
// not concurrent, and why a sliding window rather than byte credits.)

/// A Sink that appends into an in-memory buffer (offsets relative to 0).
pub const RowGroupTarget = engine_types.RowGroupTarget;
pub const default_row_group_rows = engine_types.default_row_group_rows;
pub const default_row_group_bytes = engine_types.default_row_group_bytes;

const ENCODE_REGION_RESERVE = engine_write_window.ENCODE_REGION_RESERVE;
const WorkRegions = engine_write_window.WorkRegions;
const RGJob = engine_write_window.RGJob;
const skipRowGroup = engine_write_window.skipRowGroup;
const Piece = engine_write_window.Piece;
const Slot = engine_write_window.Slot;
const WinCtx = engine_write_window.WinCtx;
const runWindowedReencode = engine_write_window.runWindowedReencode;
const runWindowed = engine_write_window.runWindowed;
const RecompressCtx = engine_write_window.RecompressCtx;
const RecompressSlot = engine_write_window.RecompressSlot;
const writeWindow = engine_write_window.writeWindow;
const anyChunkNeedsRecompress = engine_write_window.anyChunkNeedsRecompress;
const keptColumnsUseCodec = engine_write_window.keptColumnsUseCodec;

const sketchFromEnv = engine_env.sketchFromEnv;
const envFlag = engine_env.envFlag;
const envDisables = engine_env.envDisables;

const ProjectionPlan = engine_projection.ProjectionPlan;
const planProjection = engine_projection.planProjection;

const runWrite = engine_write.runWrite;
const coerceByteArrayDecimalLeavesToFlba = engine_write.coerceByteArrayDecimalLeavesToFlba;
const coerceFloat16LeavesToDouble = engine_write.coerceFloat16LeavesToDouble;
const buildSelectSchema = engine_write.buildSelectSchema;
const FdSink = engine_write.FdSink;
const createFile = engine_write.createFile;

const credentialsFromEnv = engine_input.credentialsFromEnv;
const OpenedInputs = engine_input.OpenedInputs;
const loadLocalMembership = engine_input.loadLocalMembership;
const materializeMetas = engine_input.materializeMetas;
const openInputs = engine_input.openInputs;
const hasGatedInputs = engine_input.hasGatedInputs;
const metadataGate = engine_input.metadataGate;
const allLocal = engine_input.allLocal;
const plannedResidency = engine_input.plannedResidency;
const LocalPlan = engine_input.LocalPlan;
const planLocalReads = engine_input.planLocalReads;
const planLocalReader = engine_input.planLocalReader;
const DictProbeSink = engine_input.DictProbeSink;
const ReadStarter = engine_input.ReadStarter;

const bindFilter = engine_projection.bindFilter;

const CompactRange = engine_s3_rebase.CompactRange;

const rebaseFetchedOffsets = engine_s3_rebase.rebaseFetchedOffsets;

const suffixStart = engine_s3_fetch.suffixStart;
const parseTotalFromContentRange = engine_s3_fetch.parseTotalFromContentRange;

const writeOneRowParquet = engine_write.writeOneRowParquet;
const groupedColumns = engine_write.groupedColumns;
const writeGroupedParquet = engine_write.writeGroupedParquet;
const typedColumns = engine_write.typedColumns;
const writeTypedParquet = engine_write.writeTypedParquet;
const writeIntegerStream = engine_write.writeIntegerStream;
const writeByteStream = engine_write.writeByteStream;
const retypeOutputCol = engine_write.retypeOutputCol;

// ============================================================
// Print path - CSV / JSONL streaming row output
// ============================================================

const releaseStreamAnswer = engine_text.releaseStreamAnswer;
const answerTypes = engine_text.answerTypes;
const answerTextKinds = engine_text.answerTextKinds;
pub const writeAggregateText = engine_text.writeAggregateText;
pub const writeAggregateJsonValue = engine_text.writeAggregateJsonValue;
pub const writeAggregateExplanationJson = engine_text.writeAggregateExplanationJson;

pub const PrintFormat = engine_types.PrintFormat;

const writeAggValueCsv = engine_text.writeAggValueCsv;

const writeCsvString = engine_text.writeCsvString;
const writeCsvParts = engine_text.writeCsvParts;

pub const writeJsonFloat = engine_text.writeJsonFloat;
pub const writeMembershipJson = engine_text.writeMembershipJson;
pub const kindAt = engine_text.kindAt;
pub const writeTypedAggValueJson = engine_text.writeTypedAggValueJson;
pub const writeAggValueJson = engine_text.writeAggValueJson;
pub const writeJsonQuoted = engine_text.writeJsonQuoted;
pub const writeJsonString = engine_text.writeJsonString;
pub const writeJsonText = engine_text.writeJsonText;
pub const TextKind = engine_text.TextKind;
pub const Temporal = engine_text.Temporal;
pub const writeBytesCsv = engine_text.writeBytesCsv;
pub const AnswerType = engine_text.AnswerType;
pub const DecimalColumns = engine_text.DecimalColumns;
pub const writeBytesJson = engine_text.writeBytesJson;
const printCell = engine_text.printCell;

const RowSortKey = engine_rank.RowSortKey;
const SortCell = engine_rank.SortCell;
const sortCell = engine_rank.sortCell;
const RowPos = engine_rank.RowPos;
const TopRows = engine_rank.TopRows;
const allPassthrough = engine_rank.allPassthrough;
const RankedRG = engine_rank.RankedRG;
const rankRowGroups = engine_rank.rankRowGroups;
const sortsAfter = engine_rank.sortsAfter;

const gatherEligible = engine_rank.gatherEligible;
const topnLimit = engine_rank.topnLimit;
const rankAndGather = engine_rank.rankAndGather;
const topn_gather_inputs = engine_rank.topn_gather_inputs;

pub const StdoutWriter = engine_print.StdoutWriter;
pub const PrintResult = engine_print.PrintResult;
pub const runPrint = engine_print.runPrint;
pub const runPrintTo = engine_print.runPrintTo;
pub const runPrintSink = engine_print.runPrintSink;

// ============================================================
// Compile-time API check.
// ============================================================
const pageIndexTestChunk = engine_s3_rebase.pageIndexTestChunk;

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
    engine_input.test_arm_s3_buffer_fail = &fa;
    defer engine_input.test_arm_s3_buffer_fail = null;

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
    if (@import("builtin").os.tag != .linux) return error.SkipZigTest; // where the window runs (`s3_window.supported`)
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
                if (v.max_memory) |mm| {
                    q.max_memory = mm;
                    q.max_memory_explicit = mm;
                }
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

test "explainAggregate: local IO and environment knobs preserve dispatch, and every read mode bounds dictionaries" {
    const gpa = std.testing.allocator;
    const modes = [_][*:0]const u8{ "ZPQ_IO_PROFILE=mmap", "ZPQ_IO_PROFILE=pread", "ZPQ_IO_PROFILE=uring" };
    for (modes, 0..) |mode, i| for ([_]bool{ false, true }) |disabled|
        for ([_]bool{ false, true }) |dense| {
            var arena_state = std.heap.ArenaAllocator.init(gpa);
            defer arena_state.deinit();
            const lines = [_:null]?[*:0]const u8{
                mode,
                if (disabled) "ZPQ_SKETCH_TOPK=0" else "ZPQ_SKETCH_TOPK=1",
                if (dense) "ZPQ_DENSE=1" else "ZPQ_DENSE=0",
                "ZPQ_ENCODED=0",
                "ZPQ_SKETCH_BITS=10",
            };
            const ctx: Context = .{ .gpa = gpa, .env = .{ .block = .{ .slice = &lines } }, .io = std.testing.io };
            const explained = try explainAggregate(ctx, arena_state.allocator(), .{
                .inputs = &.{"ci/fixtures/parquet/topk_skew.parquet"},
                .group_by = "k",
                .aggregate = "count(*) AS c",
                .result_order = .{ .keys = &.{.{ .column = 1, .descending = true }}, .limit = 3 },
            });
            try std.testing.expectEqual(.plain_hash, explained.aggregate.plan.dispatch.strategy);
            // Selected dictionary COUNT domains need the exact union beside readers too. With dense disabled,
            // headers suffice. Off Linux every mode uses the mapping, which always collects the exact union.
            const mapped = i == 0 or @import("builtin").os.tag != .linux;
            const reasons = explained.aggregate.plan.reasons;
            try std.testing.expect(reasons.contains(.small_ndv));
            try std.testing.expectEqual(mapped or dense, reasons.contains(.dictionary_union));
            // EXPLAIN reports the facts actually collected, including the reader's domain probe.
            const planning = explained.aggregate.plan.facts.planning.?;
            if (mapped or dense) {
                try std.testing.expectEqual(.dictionary_union, planning.source);
                const why: @TypeOf(planning.why) = if (mapped) .mapped else .reader_probe;
                try std.testing.expectEqual(why, planning.why);
            } else {
                try std.testing.expectEqual(.dictionary_headers, planning.source);
                try std.testing.expectEqual(.reader_no_choice, planning.why);
            }
            try std.testing.expect(!explained.encoded);
        };
}

test "a row group past half the query's budget is read through the mapping, whatever read mode is asked for" {
    const gpa = std.testing.allocator;
    for ([_][*:0]const u8{ "ZPQ_IO_PROFILE=pread", "ZPQ_IO_PROFILE=uring" }) |mode| for ([_]usize{ 1 << 30, 4096 }) |mm| {
        var arena_state = std.heap.ArenaAllocator.init(gpa);
        defer arena_state.deinit();
        const lines = [_:null]?[*:0]const u8{mode};
        const ctx: Context = .{ .gpa = gpa, .env = .{ .block = .{ .slice = &lines } }, .io = std.testing.io };
        const explained = try explainAggregate(ctx, arena_state.allocator(), .{
            .inputs = &.{"ci/fixtures/parquet/topk_skew.parquet"},
            .group_by = "k",
            .aggregate = "count(*) AS c",
            .max_memory = mm,
        });
        // The reader would hold a row group whole, beside everything the ledger holds; the mapping holds none of it.
        // Off Linux there is no reader: the mapping, whatever the budget.
        try std.testing.expectEqual(mm == 4096 or @import("builtin").os.tag != .linux, explained.io_mode == .mmap);
    };
}

test "explainAggregate: the facts a plan was made from, the admissible strategies and the winner" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const lines = [_:null]?[*:0]const u8{"ZPQ_IO_PROFILE=mmap"};
    const env: std.process.Environ = .{ .block = .{ .slice = &lines } };
    const ctx: Context = .{ .gpa = std.testing.allocator, .env = env, .io = std.testing.io };
    const got = try explainAggregate(ctx, arena.allocator(), .{
        .inputs = &.{"ci/fixtures/parquet/topk_skew.parquet"},
        .group_by = "k",
        .aggregate = "count(*) AS c",
        .filter = "n < 0",
        .result_order = .{ .keys = &.{.{ .column = 1, .descending = true }}, .limit = 3 },
    });
    var out: std.Io.Writer.Allocating = .init(std.testing.allocator);
    defer out.deinit();
    try writeAggregateExplanationJson(&out.writer, got);
    const json = try std.json.parseFromSlice(std.json.Value, std.testing.allocator, out.written(), .{});
    defer json.deinit();
    const facts = json.value.object.get("facts").?.object;
    try std.testing.expectEqualStrings("mmap", json.value.object.get("io").?.string);
    try std.testing.expectEqualStrings("profile asked for this", json.value.object.get("io_reason").?.string);
    try std.testing.expect(json.value.object.get("io_fallback") == null);
    try std.testing.expectEqual(@as(i64, 20000), facts.get("rows").?.object.get("value").?.integer);
    try std.testing.expectEqualStrings("exact", facts.get("row_groups").?.object.get("provenance").?.string);
    try std.testing.expectEqualStrings("bound", facts.get("groups").?.object.get("provenance").?.string);
    try std.testing.expectEqualStrings("dictionary_union", facts.get("planning").?.object.get("source").?.string);
    try std.testing.expectEqualStrings("mapped", facts.get("planning").?.object.get("why").?.string);
    try std.testing.expectEqualStrings("estimate", facts.get("filter_pass").?.object.get("provenance").?.string);
    var plain = false;
    for (json.value.object.get("candidates").?.array.items) |c| {
        if (std.mem.eql(u8, c.object.get("strategy").?.string, "plain_hash")) plain = c.object.get("admissible").?.bool;
    }
    try std.testing.expect(plain);
    try std.testing.expectEqualStrings(
        json.value.object.get("strategy").?.string,
        json.value.object.get("winner").?.string,
    );
    // The deciding rule is one of the plan's reasons: here the filter no row passes drops the count cut.
    const rule = json.value.object.get("rule").?.string;
    try std.testing.expectEqualStrings("selective_filter", rule);
    var listed = false;
    for (json.value.object.get("reasons").?.array.items) |r| listed = listed or std.mem.eql(u8, r.string, rule);
    try std.testing.expect(listed);
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

test "row output and writes report the row groups pruning skipped, as aggregates do" {
    const fixture = "ci/fixtures/parquet/full_match.parquet";
    std.Io.Dir.cwd().access(std.testing.io, fixture, .{}) catch return error.SkipZigTest;
    const gpa = std.testing.allocator;
    const ctx: Context = .{ .gpa = gpa, .env = .empty, .io = std.testing.io };
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var path_buf: [96]u8 = undefined;
    const out = try std.fmt.bufPrint(&path_buf, ".zig-cache/tmp/{s}/out.parquet", .{tmp.sub_path});
    var text_buf: [96]u8 = undefined;
    const text = try std.fmt.bufPrint(&text_buf, ".zig-cache/tmp/{s}/out.jsonl", .{tmp.sub_path});
    // Eight row groups, `b` 2i..2i+1 in row group i: only row group 6 can match.
    const filter = "b >= 12 AND b <= 13";

    var agg = try runQuery(ctx, .{ .inputs = &.{fixture}, .filter = filter, .aggregate = "count(*) AS n" });
    defer agg.aggregate.deinit(gpa);
    try std.testing.expectEqual(@as(usize, 7), agg.aggregate.row_groups_pruned);

    var w = try runQuery(ctx, .{ .inputs = &.{fixture}, .output = out, .filter = filter });
    defer w.deinit(gpa);
    try std.testing.expectEqual(@as(usize, 8), w.write.row_groups_in);
    try std.testing.expectEqual(@as(usize, 7), w.write.row_groups_pruned);
    try std.testing.expectEqual(@as(usize, 1), w.write.row_groups_kept);

    // Printed rows, in file order and ordered (a top-N over every row group), with and without a LIMIT.
    const orders = [_]expr_ast.ResultOrder{ .{}, .{ .rows = &.{.{ .expr = "b", .descending = true }} } };
    for (orders) |order| for ([_]?usize{ null, 3 }) |limit| {
        const fd = try local_fs.createFile(text);
        defer local_fs.close(fd);
        const p = try runPrintTo(ctx, .{ .inputs = &.{fixture}, .filter = filter, .columns = &.{"b"}, .result_order = order }, .jsonl, limit, fd, 0);
        // In file order a LIMIT the one matching row group fills stops before the last row group, as `rows_in` does.
        const stops = order.rows.len == 0 and limit != null;
        try std.testing.expectEqual(@as(usize, if (stops) 7 else 8), p.row_groups_in);
        try std.testing.expectEqual(@as(usize, if (stops) 6 else 7), p.row_groups_pruned);
    };
    // --scan-all prunes nothing.
    {
        const fd = try local_fs.createFile(text);
        defer local_fs.close(fd);
        const p = try runPrintTo(ctx, .{ .inputs = &.{fixture}, .filter = filter, .columns = &.{"b"}, .scan_all = true }, .jsonl, null, fd, 0);
        try std.testing.expectEqual(@as(usize, 0), p.row_groups_pruned);
    }
}

test "summary row counters: every input row, the rows read, and the rows the filter kept, on every query kind" {
    const fixture = "ci/fixtures/parquet/full_match.parquet";
    std.Io.Dir.cwd().access(std.testing.io, fixture, .{}) catch return error.SkipZigTest;
    const gpa = std.testing.allocator;
    const ctx: Context = .{ .gpa = gpa, .env = .empty, .io = std.testing.io };
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var path_buf: [96]u8 = undefined;
    const out = try std.fmt.bufPrint(&path_buf, ".zig-cache/tmp/{s}/out.parquet", .{tmp.sub_path});
    var text_buf: [96]u8 = undefined;
    const text = try std.fmt.bufPrint(&text_buf, ".zig-cache/tmp/{s}/out.jsonl", .{tmp.sub_path});
    // 8 row groups of 200 rows, `b` 2i and 2i+1 100 times each in row group i: the filter rules out row groups 0
    // and 7 on statistics, reads 1..6 (1200 rows) and keeps b = 3..12 (1000 rows). Without it every row counts.
    const Want = struct { filter: ?[]const u8, scanned: i64, matched: i64 };
    for ([_]Want{
        .{ .filter = "b >= 3 AND b <= 12", .scanned = 1200, .matched = 1000 },
        .{ .filter = null, .scanned = 1600, .matched = 1600 },
    }) |want| {
        for ([_]usize{ 1, 3 }) |j| for ([_]?[]const u8{ null, "b" }) |group_by| {
            var agg = try runQuery(ctx, .{
                .inputs = &.{fixture},
                .filter = want.filter,
                .aggregate = "count(*) AS n",
                .group_by = group_by,
                .parallelism = j,
            });
            defer agg.aggregate.deinit(gpa);
            const a = agg.aggregate;
            try std.testing.expectEqual(@as(i64, 1600), a.rows_in);
            try std.testing.expectEqual(want.scanned, a.rows_scanned);
            try std.testing.expectEqual(want.matched, a.rows_matched);
            // The aggregates fold exactly the matched rows.
            var n: i128 = 0;
            if (a.output.group_rows) |rows| {
                const at = for (a.output.group_cols.?, 0..) |name, ci| {
                    if (std.mem.eql(u8, name, "n")) break ci;
                } else unreachable;
                for (rows) |row| n += row[at].i;
            } else n = a.output.aggs[0].value.i;
            try std.testing.expectEqual(@as(i128, want.matched), n);
        };

        var w = try runQuery(ctx, .{ .inputs = &.{fixture}, .output = out, .filter = want.filter });
        defer w.deinit(gpa);
        try std.testing.expectEqual(@as(i64, 1600), w.write.rows_in);
        try std.testing.expectEqual(want.scanned, w.write.rows_scanned);
        try std.testing.expectEqual(want.matched, w.write.rows_matched);

        // Rows in file order and ordered (a top-N, every row kept without a LIMIT).
        const orders = [_]expr_ast.ResultOrder{ .{}, .{ .rows = &.{.{ .expr = "b", .descending = true }} } };
        for (orders) |order| for ([_]?usize{ null, 3 }) |limit| {
            const fd = try local_fs.createFile(text);
            defer local_fs.close(fd);
            const p = try runPrintTo(ctx, .{
                .inputs = &.{fixture},
                .filter = want.filter,
                .columns = &.{"b"},
                .result_order = order,
            }, .jsonl, limit, fd, 0);
            try std.testing.expectEqual(@as(usize, 1600), p.rows_in);
            if (limit == null) {
                try std.testing.expectEqual(@as(usize, @intCast(want.scanned)), p.rows_scanned);
                try std.testing.expectEqual(@as(usize, @intCast(want.matched)), p.rows_matched);
                try std.testing.expectEqual(p.rows_matched, p.rows);
            } else {
                // In file order a LIMIT stops reading once it is met; a top-N's workers may each read a row group
                // before a cut passes over the rest. Never fewer kept than printed.
                try std.testing.expectEqual(@as(usize, 3), p.rows);
                try std.testing.expect(p.rows <= p.rows_matched and p.rows_matched <= p.rows_scanned);
                if (order.rows.len == 0)
                    try std.testing.expect(p.rows_scanned < @as(usize, @intCast(want.scanned)))
                else
                    try std.testing.expect(p.rows_scanned <= @as(usize, @intCast(want.scanned)));
            }
        };
    }
}

test "a page that fails to decode is named by file, row group, column and page, on every query kind" {
    // The crafted case dict_index_oob_rle_500: an RLE_DICTIONARY index of 500 into a 12-entry dictionary, in data page
    // 1 (page 0 the dictionary) of `z`; its sibling is valid, so the failure is in the second input.
    const bad = "ci/fixtures/parquet/dict_index_oob.parquet";
    const good = "ci/fixtures/parquet/dict_index_ok.parquet";
    std.Io.Dir.cwd().access(std.testing.io, bad, .{}) catch return error.SkipZigTest;
    const gpa = std.testing.allocator;
    const ctx: Context = .{ .gpa = gpa, .env = .empty, .io = std.testing.io };
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var path_buf: [96]u8 = undefined;
    const out = try std.fmt.bufPrint(&path_buf, ".zig-cache/tmp/{s}/out.parquet", .{tmp.sub_path});
    var text_buf: [96]u8 = undefined;
    const text = try std.fmt.bufPrint(&text_buf, ".zig-cache/tmp/{s}/out.jsonl", .{tmp.sub_path});
    const inputs = [_][]const u8{ good, bad };
    const Check = struct {
        fn site(diag: *const Diag) !void {
            try std.testing.expect(diag.detail == .decode);
            const at = diag.detail.decode;
            try std.testing.expectEqualStrings(bad, at.input);
            try std.testing.expectEqual(@as(usize, 0), at.row_group);
            try std.testing.expectEqualStrings("z", at.column.get());
            try std.testing.expectEqual(@as(?u32, 1), at.page);
        }
    };
    for ([_]usize{ 1, 3 }) |j| for ([_]?[]const u8{ null, "z" }) |group_by| {
        var diag: Diag = .{};
        try std.testing.expectError(error.UnexpectedPage, runQuery(ctx, .{
            .inputs = &inputs,
            // sum reads the values; count(z) alone would read only the levels, which are sound.
            .aggregate = "sum(z) AS s",
            .group_by = group_by,
            .parallelism = j,
            .diag = &diag,
        }));
        try Check.site(&diag);
    };
    {
        var diag: Diag = .{};
        try std.testing.expectError(error.UnexpectedPage, runQuery(ctx, .{
            .inputs = &inputs,
            .output = out,
            .select = "z + 1 AS y",
            .diag = &diag,
        }));
        try Check.site(&diag);
    }
    const orders = [_]expr_ast.ResultOrder{ .{}, .{ .rows = &.{.{ .expr = "z", .descending = true }} } };
    for (orders) |order| {
        const fd = try local_fs.createFile(text);
        defer local_fs.close(fd);
        var diag: Diag = .{};
        try std.testing.expectError(error.UnexpectedPage, runPrintTo(ctx, .{
            .inputs = &inputs,
            .columns = &.{"z"},
            .result_order = order,
            .diag = &diag,
        }, .jsonl, null, fd, 0));
        try Check.site(&diag);
    }
}

test "written files name zpq as their writer once zpq encodes their data, and keep their source's otherwise" {
    const fixture = "ci/fixtures/parquet/full_match.parquet";
    std.Io.Dir.cwd().access(std.testing.io, fixture, .{}) catch return error.SkipZigTest;
    const gpa = std.testing.allocator;
    const ctx: Context = .{ .gpa = gpa, .env = .empty, .io = std.testing.io };
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var path_buf: [96]u8 = undefined;
    const out = try std.fmt.bufPrint(&path_buf, ".zig-cache/tmp/{s}/out.parquet", .{tmp.sub_path});
    const source = "parquet-cpp-arrow version 25.0.1";
    const Case = struct { args: QueryArgs, writer: []const u8 };
    const cases = [_]Case{
        // Re-encoded: a filter, a projection by --select, and a filter whose every row group fully matches.
        .{ .args = .{ .inputs = &.{fixture}, .output = out, .filter = "b >= 3" }, .writer = schema.ZPQ_CREATED_BY },
        .{ .args = .{ .inputs = &.{fixture}, .output = out, .select = "b, s" }, .writer = schema.ZPQ_CREATED_BY },
        // A byte copy, and a recompression that keeps every value and statistic: the source's.
        .{ .args = .{ .inputs = &.{fixture}, .output = out }, .writer = source },
        .{ .args = .{ .inputs = &.{fixture}, .output = out, .codec = .ZSTD }, .writer = source },
    };
    for (cases) |c| {
        var r = try runQuery(ctx, c.args);
        defer r.deinit(gpa);
        const bytes = try metadata.readFileSlice(out, gpa);
        defer gpa.free(bytes);
        var arena = std.heap.ArenaAllocator.init(gpa);
        defer arena.deinit();
        const meta = try metadata.open(arena.allocator(), bytes);
        try std.testing.expectEqualStrings(c.writer, meta.created_by.?);
    }
}

test "a borrowed worker runtime survives repeated writes and a sink failure" {
    const fixture = "ci/fixtures/parquet/full_match.parquet";
    std.Io.Dir.cwd().access(std.testing.io, fixture, .{}) catch return error.SkipZigTest;
    var runtime = std.Io.Threaded.init(std.testing.allocator, .{});
    defer runtime.deinit();
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var path_buf: [96]u8 = undefined;
    const path = try std.fmt.bufPrint(&path_buf, ".zig-cache/tmp/{s}/borrowed.parquet", .{tmp.sub_path});
    var expected: ?[]u8 = null;
    defer if (expected) |bytes| std.testing.allocator.free(bytes);
    for (0..3) |i| {
        const ctx: Context = .{
            .gpa = std.testing.allocator,
            .env = .empty,
            .io = std.testing.io,
            .worker_io = runtime.io(),
        };
        const args: QueryArgs = .{
            .inputs = &.{fixture},
            .output = path,
            .filter = "x > 0",
            .parallelism = 3,
            .max_memory = 256 << 20,
        };
        if (i == 1 and @import("builtin").os.tag == .linux) {
            const full = try std.posix.openat(std.posix.AT.FDCWD, "/dev/full", .{ .ACCMODE = .WRONLY }, 0);
            defer local_fs.close(full);
            var bad = args;
            bad.output_fd = full;
            try std.testing.expectError(error.NoSpaceLeft, runQuery(ctx, bad));
        }
        var answer = try runQuery(ctx, args);
        answer.deinit(std.testing.allocator);
        const bytes = try local_fs.readFile(std.testing.allocator, path);
        defer std.testing.allocator.free(bytes);
        if (expected) |want| try std.testing.expectEqualSlices(u8, want, bytes) else expected = try std.testing.allocator.dupe(u8, bytes);
        try std.testing.expect(runtime.worker_threads.load(.acquire) != null);
    }
}

test "ALP metadata opens and only a decoded ALP column refuses" {
    const path = "ci/fixtures/parquet/alp_extended.zstd.parquet";
    const gpa = std.testing.allocator;
    const ctx: Context = .{ .gpa = gpa, .env = .empty, .io = std.testing.io };
    var plain = try runQuery(ctx, .{
        .inputs = &.{path},
        .aggregate = "count(float_plain) AS n",
    });
    defer plain.deinit(gpa);
    try std.testing.expect(plain.aggregate.output.aggs[0].value.i > 0);
    var diag: Diag = .{};
    try std.testing.expectError(error.UnsupportedEncoding, runQuery(ctx, .{
        .inputs = &.{path},
        .aggregate = "sum(float_alp_1024) AS s",
        .diag = &diag,
        .scan_all = true,
    }));
    try std.testing.expect(diag.detail == .decode);
    try std.testing.expectEqual(schema.Encoding.ALP, diag.detail.decode.encoding.?);
}

test "M1 DISTINCT writes identical canonical Parquet with sixteen actual producers and both selected readers" {
    const fixture = "ci/fixtures/parquet/packed_count_indexed.parquet";
    const gpa = std.testing.allocator;
    const bytes = try metadata.readFileSlice(fixture, gpa);
    defer gpa.free(bytes);
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var path_buf: [96]u8 = undefined;
    const path = try std.fmt.bufPrint(&path_buf, ".zig-cache/tmp/{s}/m1.parquet", .{tmp.sub_path});
    const Case = struct { grouped: bool, filter: ?[]const u8 = null, top: bool = false };
    for ([_]Case{
        .{ .grouped = true },
        .{ .grouped = true, .top = true },
        .{ .grouped = true, .filter = "k IS NULL" },
        .{ .grouped = false, .filter = "f < -1000" },
    }) |c| {
        var expected: ?[]u8 = null;
        defer if (expected) |b| gpa.free(b);
        for ([_]bool{ false, true }) |owner| for ([_]usize{ 1, 16 }) |workers|
            for ([_]usize{ 0, 64 }) |window| {
                var arena = std.heap.ArenaAllocator.init(gpa);
                defer arena.deinit();
                const a = arena.allocator();
                const meta = try metadata.open(a, bytes);
                const args: scan.MultiAggArgs = .{
                    .inputs = &.{.{ .name = fixture, .bytes = bytes }},
                    .metas = &.{meta},
                    .aggregate = if (c.grouped) "count(DISTINCT k) AS d,count(*) AS c" else "count(DISTINCT b) AS d",
                    .group_by = if (c.grouped) "b" else null,
                    .filter = c.filter,
                    .parallelism = workers,
                    .min_bytes_per_worker = 1,
                    .decode_batch_rows = window,
                    .integer_records = owner,
                    .integer_records_min_rows = 0,
                    .byte_records = false,
                    .msd_records = false,
                    .dense = false,
                    .plain_rules = false,
                    .max_memory = 512 << 20,
                    .distinct_pairs = if (owner) .on else .off,
                    .result_order = if (c.top) .{
                        .keys = &.{.{ .column = 1, .descending = true }},
                        .limit = 3,
                    } else .{},
                };
                const explain = try scan.explainAggregate(gpa, a, args);
                try std.testing.expectEqual(workers, explain.workers);
                var result = try scan.runMultiAggregate(gpa, a, args);
                defer result.deinit(gpa);
                // runMultiAggregate owns its replay markers. These string-key/scalar cases cannot use the old
                // fixed-key pair fold, so this result flag proves the selected pair owner actually completed.
                try std.testing.expectEqual(owner, result.distinct_pairs);
                try result.output.applyResultOrder(gpa, args.result_order);
                if (result.output.group_rows) |rows| {
                    _ = try writeGroupedParquet(
                        a,
                        path,
                        result.output.group_cols.?,
                        result.state.group_col_types.?,
                        rows,
                        .SNAPPY,
                        .{},
                        .{},
                        null,
                    );
                } else {
                    _ = try writeOneRowParquet(
                        a,
                        path,
                        result.state.agg_calls,
                        result.state.accumulators,
                        result.state.agg_sources,
                        .SNAPPY,
                        .{},
                        null,
                        false,
                    );
                }
                const encoded = try metadata.readFileSlice(path, gpa);
                if (expected) |want| {
                    defer gpa.free(encoded);
                    try std.testing.expectEqualSlices(u8, want, encoded);
                } else expected = encoded;
            };
    }
}

test "M1 first-H finishes materialize only the original first keys at j1 and sixteen actual producers" {
    const fixture = "ci/fixtures/parquet/packed_count_indexed.parquet";
    const a = std.testing.allocator;
    const bytes = try metadata.readFileSlice(fixture, a);
    defer a.free(bytes);
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var path_buf: [96]u8 = undefined;
    const path = try std.fmt.bufPrint(&path_buf, ".zig-cache/tmp/{s}/first.parquet", .{tmp.sub_path});
    var expected: ?[]u8 = null;
    defer if (expected) |b| a.free(b);
    for ([_]usize{ 1, 16 }) |workers| for ([_]usize{ 0, 64 }) |window| {
        var arena = std.heap.ArenaAllocator.init(a);
        defer arena.deinit();
        const temp = arena.allocator();
        const meta = try metadata.open(temp, bytes);
        const args: scan.MultiAggArgs = .{
            .inputs = &.{.{ .name = fixture, .bytes = bytes }},
            .metas = &.{meta},
            .aggregate = "count(*) AS c,sum(f) AS s",
            .group_by = "k,b,f",
            .parallelism = workers,
            .min_bytes_per_worker = 1,
            .decode_batch_rows = window,
            .integer_records = false,
            .byte_records = false,
            .msd_records = false,
            .dense = false,
            .plain_rules = false,
            .spill = false,
            .sketch_topk = null,
            .ndv_policy = false,
            .max_memory = 512 << 20,
            .result_order = .{ .limit = 3, .offset = 1 },
        };
        const explanation = try scan.explainAggregate(a, temp, args);
        try std.testing.expectEqual(workers, explanation.workers);
        try std.testing.expectEqual(.plain_hash, explanation.plan.dispatch.strategy);
        try std.testing.expect(explanation.record_path == null);
        var result = try scan.runMultiAggregate(a, temp, args);
        defer result.deinit(a);
        try std.testing.expectEqual(@as(usize, 4), result.output.group_rows.?.len);
        try result.output.applyResultOrder(a, args.result_order);
        _ = try writeGroupedParquet(
            temp,
            path,
            result.output.group_cols.?,
            result.state.group_col_types.?,
            result.output.group_rows.?,
            .SNAPPY,
            .{},
            .{},
            null,
        );
        const encoded = try metadata.readFileSlice(path, a);
        if (expected) |want| {
            defer a.free(encoded);
            try std.testing.expectEqualSlices(u8, want, encoded);
        } else expected = encoded;
    };
}

// Opt-in: ZPQ_OOM_SWEEP=1 zig build test-core -Doptimize=Debug -Dtest-filter="oom sweep".
// The ordinary tiers run these declarations without the expensive allocation sweep.
const OomSweep = struct {
    const Case = enum { filtered, jsonl, csv, group1, group4, distinct, topk, decimal, parquet, strings, explain };
    const input = "ci/fixtures/parquet/column_order.parquet";
    const decimal_input = "ci/fixtures/parquet/decimal_exact.parquet";
    const env_lines = [_:null]?[*:0]const u8{ "ZPQ_IO_PROFILE=mmap", "ZPQ_DENSE=0" };
    const env: std.process.Environ = .{ .block = .{ .slice = &env_lines } };

    const Point = struct { caller: usize, worker: bool };

    fn recordPoint(points: *std.ArrayList(Point), point: Point) !void {
        for (points.items) |p| if (std.meta.eql(p, point)) return;
        try points.append(std.testing.allocator, point);
    }

    // FailingAllocator's counters are not atomic; the engine may call the supplied allocator on worker threads.
    // Refusing resize/remap also keeps arena growth from depending on the backing allocator's placement.
    const Locked = struct {
        child: std.mem.Allocator,
        lock: std.Io.Mutex = .init,
        coordinator: std.Thread.Id = undefined,
        points: ?*std.ArrayList(Point) = null,
        threads: ?*std.ArrayList(std.Thread.Id) = null,
        target: ?Point = null,
        failing: ?*std.testing.FailingAllocator = null,
        fn allocator(s: *Locked) std.mem.Allocator {
            s.coordinator = std.Thread.getCurrentId();
            return .{ .ptr = s, .vtable = &.{
                .alloc = alloc,
                .resize = std.mem.Allocator.noResize,
                .remap = std.mem.Allocator.noRemap,
                .free = free,
            } };
        }
        fn alloc(ctx: *anyopaque, n: usize, alignment: std.mem.Alignment, ra: usize) ?[*]u8 {
            const s: *Locked = @ptrCast(@alignCast(ctx));
            s.lock.lockUncancelable(std.testing.io);
            defer s.lock.unlock(std.testing.io);
            const thread = std.Thread.getCurrentId();
            const point: Point = .{ .caller = ra, .worker = thread != s.coordinator };
            if (s.points) |points| recordPoint(points, point) catch return null;
            if (s.threads) |threads| if (point.worker) {
                const known = for (threads.items) |id| {
                    if (id == thread) break true;
                } else false;
                if (!known) threads.append(std.testing.allocator, thread) catch return null;
            };
            if (s.target) |target| if (std.meta.eql(target, point)) {
                // Fail from this observed site onward, including allocations on peer workers during unwind.
                s.failing.?.fail_index = s.failing.?.alloc_index;
                s.target = null;
            };
            return s.child.rawAlloc(n, alignment, ra);
        }
        fn free(ctx: *anyopaque, bytes: []u8, alignment: std.mem.Alignment, ra: usize) void {
            const s: *Locked = @ptrCast(@alignCast(ctx));
            s.lock.lockUncancelable(std.testing.io);
            defer s.lock.unlock(std.testing.io);
            s.child.rawFree(bytes, alignment, ra);
        }
    };

    fn execute(a: std.mem.Allocator, case: Case, output: *std.Io.Writer, path: []const u8) !void {
        const ctx: Context = .{ .gpa = a, .env = env, .io = std.testing.io };
        var args: QueryArgs = .{ .inputs = &.{input}, .parallelism = 1, .cache = true };
        switch (case) {
            .filtered => {
                args.aggregate = "count(*) AS n";
                args.filter = "i > 2";
            },
            .jsonl, .csv => {
                args.select = "s, i";
                args.filter = "i > 2";
                _ = try runPrintSink(ctx, args, if (case == .jsonl) .jsonl else .csv, null, 1, 0, output);
                return;
            },
            .group1 => {
                args.group_by = "s";
                args.aggregate = "count(*) AS n, sum(i) AS total";
            },
            .group4 => {
                const wide = "ci/fixtures/parquet/census_widefilter.parquet";
                args.inputs = &.{ wide, wide, wide, wide };
                args.group_by = "k";
                args.aggregate = "sum(strlen(p)) AS total";
                args.parallelism = 4;
            },
            .distinct => args.aggregate = "count(DISTINCT s) AS n",
            .topk => {
                args.group_by = "s";
                args.aggregate = "sum(i) AS total";
                args.result_order = .{ .keys = &.{.{ .column = 1, .descending = true }}, .limit = 3 };
            },
            .decimal => {
                args.inputs = &.{decimal_input};
                args.aggregate = "sum(d9) AS total, avg(d9) AS mean";
            },
            .parquet => {
                args.output = path;
                args.filter = "i > 2";
                args.fsync = false;
            },
            .strings => {
                args.inputs = &.{"ci/fixtures/parquet/full_match.parquet"};
                args.group_by = "s";
                args.aggregate = "count(*) AS n";
            },
            .explain => {
                args.group_by = "s";
                args.aggregate = "count(*) AS n";
                var arena = std.heap.ArenaAllocator.init(a);
                defer arena.deinit();
                const explanation = try explainAggregate(ctx, arena.allocator(), args);
                try writeAggregateExplanationJson(output, explanation);
                return;
            },
        }
        if (case != .parquet) args.text_output = .{ .format = .json, .sink = output };
        var result = try runQuery(ctx, args);
        defer result.deinit(a);
        if (case == .parquet) {
            const bytes = try local_fs.readFile(std.testing.allocator, path);
            defer std.testing.allocator.free(bytes);
            try output.writeAll(bytes);
        }
    }

    fn checkBaseline(case: Case, bytes: []const u8) !void {
        const expected: ?[]const u8 = switch (case) {
            .filtered => "[{\"n\":4}]\n",
            .jsonl => "{\"s\":\"c\",\"i\":3}\n{\"s\":\"x\",\"i\":101}\n{\"s\":\"Y\",\"i\":102}\n{\"s\":\"z\",\"i\":103}\n",
            .csv => "s,i\nc,3\nx,101\nY,102\nz,103\n",
            .distinct => "[{\"n\":6}]\n",
            .topk => "[{\"s\":\"z\",\"total\":103},{\"s\":\"Y\",\"total\":102},{\"s\":\"x\",\"total\":101}]\n",
            .decimal => "[{\"total\":-151.50,\"mean\":-0.505000}]\n",
            else => null,
        };
        if (expected) |want| return std.testing.expectEqualStrings(want, bytes);
        switch (case) {
            .group1, .group4, .strings => {
                const parsed = try std.json.parseFromSlice(std.json.Value, std.testing.allocator, bytes, .{});
                defer parsed.deinit();
                const rows: usize = switch (case) {
                    .group1 => 6,
                    .group4 => 12000,
                    .strings => 1600,
                    else => unreachable,
                };
                try std.testing.expectEqual(rows, parsed.value.array.items.len);
            },
            .parquet => {
                try std.testing.expect(bytes.len >= 8);
                try std.testing.expectEqualStrings("PAR1", bytes[0..4]);
                try std.testing.expectEqualStrings("PAR1", bytes[bytes.len - 4 ..]);
            },
            .explain => {
                const parsed = try std.json.parseFromSlice(std.json.Value, std.testing.allocator, bytes, .{});
                defer parsed.deinit();
                try std.testing.expectEqualStrings("mmap", parsed.value.object.get("io").?.string);
            },
            else => unreachable,
        }
    }

    fn sweep(case: Case) !void {
        if (std.c.getenv("ZPQ_OOM_SWEEP") == null) return;
        var tmp = std.testing.tmpDir(.{});
        defer tmp.cleanup();
        var path_buf: [256]u8 = undefined;
        const path = try std.fmt.bufPrint(&path_buf, ".zig-cache/tmp/{s}/answer.parquet", .{tmp.sub_path});
        if (case == .group4) {
            const wide = "ci/fixtures/parquet/census_widefilter.parquet";
            var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
            defer arena.deinit();
            const explanation = try explainAggregate(.{ .gpa = std.testing.allocator, .env = env, .io = std.testing.io }, arena.allocator(), .{
                .inputs = &.{ wide, wide, wide, wide },
                .parallelism = 4,
                .cache = true,
                .group_by = "k",
                .aggregate = "sum(strlen(p)) AS total",
            });
            try std.testing.expectEqual(@as(usize, 4), explanation.aggregate.workers);
        }
        var baseline: std.Io.Writer.Allocating = .init(std.testing.allocator);
        defer baseline.deinit();
        var measuring = std.testing.FailingAllocator.init(std.testing.allocator, .{});
        var locked: Locked = .{ .child = measuring.allocator() };
        var points: std.ArrayList(Point) = .empty;
        defer points.deinit(std.testing.allocator);
        var threads: std.ArrayList(std.Thread.Id) = .empty;
        defer threads.deinit(std.testing.allocator);
        if (case == .group4) {
            locked.points = &points;
            locked.threads = &threads;
        }
        try execute(locked.allocator(), case, &baseline.writer, path);
        local_fs.removeFile(path);
        try checkBaseline(case, baseline.written());
        try std.testing.expectEqual(measuring.allocated_bytes, measuring.freed_bytes);
        if (case == .group4) try std.testing.expect(threads.items.len >= 4);
        const total = measuring.alloc_index;
        std.debug.print("oom sweep {s}: {d} allocations\n", .{ @tagName(case), total });
        var unreached: usize = 0;
        for (0..total) |i| {
            var failing = std.testing.FailingAllocator.init(std.testing.allocator, .{ .fail_index = i });
            var guarded: Locked = .{ .child = failing.allocator() };
            var out: std.Io.Writer.Allocating = .init(std.testing.allocator);
            defer out.deinit();
            std.debug.print("oom sweep {s}: fail {d}/{d}\n", .{ @tagName(case), i, total });
            const result = execute(guarded.allocator(), case, &out.writer, path);
            try checkAttempt(case, baseline.written(), out.written(), path, &failing, result);
            if (!failing.has_induced_failure) {
                // A different work distribution may need fewer arena blocks. This ordinal did not exist in this
                // execution: it must have succeeded identically, and cannot have allocated past the target.
                try std.testing.expect(failing.alloc_index <= i);
                unreached += 1;
            }
        }
        std.debug.print("oom sweep {s}: {d} ordinal attempts, {d} absent in their execution\n", .{
            @tagName(case), total, unreached,
        });
        if (case != .group4) {
            if (unreached != 0) return error.NondeterministicMemoryUsage;
            return;
        }
        // Keep the ordinal attempts above, but also fail every observed allocation site on coordinator and worker
        // paths. A site is independent of which worker wins a work item or how many arena blocks it needs.
        var worker_points: usize = 0;
        for (points.items, 0..) |point, i| {
            var failing = std.testing.FailingAllocator.init(std.testing.allocator, .{});
            var guarded: Locked = .{ .child = failing.allocator(), .target = point, .failing = &failing };
            var out: std.Io.Writer.Allocating = .init(std.testing.allocator);
            defer out.deinit();
            std.debug.print("oom sweep group4: site {d}/{d}, worker={any}\n", .{ i, points.items.len, point.worker });
            const result = execute(guarded.allocator(), case, &out.writer, path);
            try checkAttempt(case, baseline.written(), out.written(), path, &failing, result);
            // Unlike a global ordinal, each measured site must actually have been failed: no vacuous coverage.
            try std.testing.expect(failing.has_induced_failure);
            if (point.worker) worker_points += 1;
        }
        try std.testing.expect(worker_points > 0);
        std.debug.print("oom sweep group4: {d}/{d} sites injected ({d} worker), {d} allocating worker threads\n", .{
            points.items.len, points.items.len, worker_points, threads.items.len,
        });
    }

    fn checkAttempt(
        case: Case,
        baseline: []const u8,
        out: []const u8,
        path: []const u8,
        failing: *const std.testing.FailingAllocator,
        result: anyerror!void,
    ) !void {
        defer local_fs.removeFile(path);
        if (result) |_| {
            try std.testing.expectEqualStrings(baseline, out);
            if (failing.has_induced_failure) return error.SwallowedOutOfMemoryError;
        } else |_| {
            if (!failing.has_induced_failure) return error.UnexpectedFailure;
            if (case == .parquet) {
                if (std.Io.Dir.cwd().access(std.testing.io, path, .{})) |_| {
                    return error.PartialParquetOutput;
                } else |err| switch (err) {
                    error.FileNotFound => {},
                    else => return err,
                }
            } else if (!std.mem.startsWith(u8, baseline, out)) {
                return error.IncorrectPartialOutput;
            }
        }
        if (failing.allocated_bytes != failing.freed_bytes) {
            std.debug.print("oom sweep {s}: allocated {d}, freed {d}\n", .{
                @tagName(case), failing.allocated_bytes, failing.freed_bytes,
            });
            return error.MemoryLeakDetected;
        }
    }
};

test "oom sweep filtered read" {
    try OomSweep.sweep(.filtered);
}
test "oom sweep projection NDJSON" {
    try OomSweep.sweep(.jsonl);
}
test "oom sweep projection CSV" {
    try OomSweep.sweep(.csv);
}
test "oom sweep group one worker" {
    try OomSweep.sweep(.group1);
}
test "oom sweep group four workers" {
    try OomSweep.sweep(.group4);
}
test "oom sweep count distinct" {
    try OomSweep.sweep(.distinct);
}
test "oom sweep top k" {
    try OomSweep.sweep(.topk);
}
test "oom sweep decimal sum and average" {
    try OomSweep.sweep(.decimal);
}
test "oom sweep parquet write" {
    try OomSweep.sweep(.parquet);
}
test "oom sweep string groups" {
    try OomSweep.sweep(.strings);
}
test "oom sweep explain" {
    try OomSweep.sweep(.explain);
}
