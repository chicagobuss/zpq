const engine_text = @import("text.zig");
const engine_types = @import("types.zig");
const engine_s3_rebase = @import("s3_rebase.zig");
const engine_s3_fetch = @import("s3_fetch.zig");
const engine_rank = @import("rank.zig");
const engine_write_window = @import("write_window.zig");
const engine_projection = @import("projection.zig");
const engine_input = @import("input.zig");
const engine_write = @import("write.zig");
const engine_env = @import("env.zig");
const engine_print = @import("print.zig");
const engine_aggregate = @import("aggregate.zig");
const fetchS3Schema = engine_input.fetchS3Schema;
const std = @import("std");
const nowMonoNs = @import("../clock.zig").monoNs;
const profile = @import("../profile.zig");
const local_fs = @import("../local_fs.zig");
const local_reader = @import("../io/local_reader.zig");
const schema = @import("../core/schema.zig");
const compression = @import("../core/parquet/compression.zig");
const huge_pages = @import("../core/huge_pages.zig");
const consumer = @import("../core/consumer.zig");
const invariant = @import("../core/invariant.zig");
const scan = @import("../core/scan.zig");
const ResultProgram = @import("../core/result_program.zig").ResultProgram;
const Diag = @import("../core/diag.zig").Diag;
const agg_plan = @import("../core/agg_plan.zig");
const system = @import("../core/system.zig");
const metadata = @import("../core/parquet/metadata.zig");
const schema_tree = @import("../core/parquet/schema_tree.zig");
const thrift = @import("../core/thrift.zig");
const statistics = @import("../core/parquet/statistics.zig");
const membership = @import("../core/filter/membership.zig");
const nanprobe = @import("../core/filter/nanprobe.zig");
const sbbf = @import("../core/parquet/bloom.zig");
const streaming = @import("../core/writer/streaming.zig");
const fastpath = @import("../core/writer/fastpath.zig");
const encoder = @import("../core/writer/encoder.zig");
const page_index = @import("../core/writer/page_index.zig");
const s3 = @import("../io/s3.zig");
const s3_window = @import("../io/s3_window.zig");
const tls = @import("../io/tls.zig");
const http = @import("../io/http.zig");
const sigv4 = @import("../io/sigv4.zig");
const multipart_sink = @import("../io/multipart_sink.zig");
const coalescer = @import("../io/coalescer.zig");
const work_cursor = @import("../io/work_cursor.zig");
const S3Fetcher = engine_s3_fetch.S3Fetcher;
const FetchWindow = engine_s3_fetch.FetchWindow;
const READ_CONCURRENCY = engine_s3_fetch.READ_CONCURRENCY;
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
const POOL_SIZE = engine_types.POOL_SIZE;
const FOOTER_PROBE_DEFAULT = engine_s3_fetch.FOOTER_PROBE_DEFAULT;
const footerProbeBytes = engine_s3_fetch.footerProbeBytes;
const footerProbeFor = engine_s3_fetch.footerProbeFor;
const ByteAccount = engine_s3_fetch.ByteAccount;
const OUTPUT_POOL_SIZE = engine_types.OUTPUT_POOL_SIZE;
const OutputLane = engine_types.OutputLane;
const Error = engine_types.Error;
const ColumnarSink = engine_types.ColumnarSink;
const QueryArgs = engine_types.QueryArgs;
const CompressPriority = engine_types.CompressPriority;
const BloomSpec = engine_types.BloomSpec;
const Context = engine_types.Context;
const PoolRegistry = engine_types.PoolRegistry;
const QueryResult = engine_types.QueryResult;
const WriteResult = engine_types.WriteResult;
const AggResult = engine_types.AggResult;
const Timings = engine_types.Timings;
const runQuery = engine_aggregate.runQuery;
const explainAggregate = engine_aggregate.explainAggregate;
const AggregateExplanation = engine_types.AggregateExplanation;
const Conjunct = engine_types.Conjunct;
const ResultExplanation = engine_types.ResultExplanation;
const RowGroupTarget = engine_types.RowGroupTarget;
const default_row_group_rows = engine_types.default_row_group_rows;
const default_row_group_bytes = engine_types.default_row_group_bytes;
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
const releaseStreamAnswer = engine_text.releaseStreamAnswer;
const answerTypes = engine_text.answerTypes;
const answerTextKinds = engine_text.answerTextKinds;
const writeAggregateText = engine_text.writeAggregateText;
const writeAggregateJsonValue = engine_text.writeAggregateJsonValue;
const writeAggregateExplanationJson = engine_text.writeAggregateExplanationJson;
const PrintFormat = engine_types.PrintFormat;
const writeAggValueCsv = engine_text.writeAggValueCsv;
const writeCsvString = engine_text.writeCsvString;
const writeCsvParts = engine_text.writeCsvParts;
const writeJsonFloat = engine_text.writeJsonFloat;
const writeMembershipJson = engine_text.writeMembershipJson;
const kindAt = engine_text.kindAt;
const writeTypedAggValueJson = engine_text.writeTypedAggValueJson;
const writeAggValueJson = engine_text.writeAggValueJson;
const writeJsonQuoted = engine_text.writeJsonQuoted;
const writeJsonString = engine_text.writeJsonString;
const writeJsonText = engine_text.writeJsonText;
const TextKind = engine_text.TextKind;
const Temporal = engine_text.Temporal;
const writeBytesCsv = engine_text.writeBytesCsv;
const AnswerType = engine_text.AnswerType;
const DecimalColumns = engine_text.DecimalColumns;
const writeBytesJson = engine_text.writeBytesJson;
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
const StdoutWriter = engine_print.StdoutWriter;
const PrintResult = engine_print.PrintResult;
const runPrint = engine_print.runPrint;
const runPrintTo = engine_print.runPrintTo;
const runPrintSink = engine_print.runPrintSink;
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

const fake_s3 = @import("../io/fake_s3.zig");

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
    const column_mod = @import("../core/parquet/column.zig");
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

test "a windowed row print waits for a row group left to its writer, and fails with its fetch, at every parallelism" {
    // One row group too big for a producer's share beside 1-row ones: producers leave it to the writer before its bytes
    // are in. Its ranges come slowly, or are refused; the writer waits on its gate as -j1 does, and a refused fetch
    // fails the print with that error rather than decoding a buffer never filled.
    if (@import("builtin").os.tag != .linux) return error.SkipZigTest;
    const gpa = std.testing.allocator;
    const full = "ci/fixtures/parquet/full_match.parquet";
    std.Io.Dir.cwd().access(std.testing.io, full, .{}) catch return error.SkipZigTest;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var big_buf: [96]u8 = undefined;
    var small_buf: [96]u8 = undefined;
    const big_path = try std.fmt.bufPrint(&big_buf, ".zig-cache/tmp/{s}/big.parquet", .{tmp.sub_path});
    const small_path = try std.fmt.bufPrint(&small_buf, ".zig-cache/tmp/{s}/small.parquet", .{tmp.sub_path});
    const local: Context = .{ .gpa = gpa, .env = .empty, .io = std.testing.io };
    // A select re-encodes the big one into one row group, uncompressed so its column chunks are ranges of their own.
    {
        var r = try runQuery(local, .{
            .inputs = &.{full},
            .output = big_path,
            .select = "ts, b, s, x, n, u",
            .codec = .UNCOMPRESSED,
            .row_group = .{ .rows = 1600 },
        });
        r.deinit(gpa);
        const one: engine_types.RowGroupTarget = .{ .rows = 1 };
        var s = try runQuery(local, .{
            .inputs = &.{full},
            .output = small_path,
            .filter = "ts < 12",
            .row_group = one,
        });
        s.deinit(gpa);
    }
    const big = try metadata.readFileSlice(big_path, gpa);
    defer gpa.free(big);
    const small = try metadata.readFileSlice(small_path, gpa);
    defer gpa.free(small);
    // The footer probe takes each small file whole and leaves the big one's column chunks to ranged GETs.
    try std.testing.expect(small.len * 2 < big.len);

    var arena_state = std.heap.ArenaAllocator.init(gpa);
    defer arena_state.deinit();
    const a = arena_state.allocator();
    const urls = [_][]const u8{ "s3://bkt/t/a.parquet", "s3://bkt/t/big.parquet", "s3://bkt/t/b.parquet" };
    for ([_]bool{ false, true }) |refuse| {
        var first: ?[]u8 = null;
        defer if (first) |f| gpa.free(f);
        var first_err: ?anyerror = null;
        for ([_]usize{ 1, 2, 3, 16 }) |j| {
            var fake: fake_s3.FakeS3 = undefined;
            try fake.start(gpa);
            defer fake.deinit();
            try fake.serveBytes("t/a.parquet", small);
            try fake.serveBytes("t/big.parquet", big);
            try fake.serveBytes("t/b.parquet", small);
            if (refuse) {
                // The three footer reads and no more: the big row group's ranges are refused.
                fake.gets_allowed = 3;
            } else {
                fake.stall = "GET /bkt/t/big.parquet";
                fake.stall_at = .trickle;
                fake.stall_min_len = 1024; // a column chunk, not the footer's suffix
            }
            var lines: std.ArrayList(?[*:0]const u8) = .empty;
            const endpoint = try std.fmt.allocPrintSentinel(a, "S3_ENDPOINT_URL=http://127.0.0.1:{d}", .{fake.port}, 0);
            try lines.append(a, endpoint);
            try lines.appendSlice(a, &.{ "AWS_REGION=us-east-1", "AWS_ACCESS_KEY_ID=ak", "AWS_SECRET_ACCESS_KEY=sk" });
            try lines.append(a, "ZPQ_S3_WINDOW_KIB=1");
            try lines.append(a, try std.fmt.allocPrintSentinel(a, "ZPQ_S3_FOOTER_PROBE={d}", .{small.len}, 0));
            const block = try a.allocSentinel(?[*:0]const u8, lines.items.len, null);
            @memcpy(block, lines.items);
            const ctx: Context = .{ .gpa = gpa, .env = .{ .block = .{ .slice = block } }, .io = std.testing.io };
            var out: std.Io.Writer.Allocating = .init(gpa);
            defer out.deinit();
            const args: QueryArgs = .{ .inputs = &urls, .parallelism = j, .max_memory = 256 << 10 };
            const err: ?anyerror = if (runPrintSink(ctx, args, .jsonl, null, 1, 0, &out.writer)) |_| null else |e| e;
            if (!refuse) try std.testing.expectEqual(@as(u32, 1), fake.stalled);
            if (first) |f| {
                std.testing.expectEqual(first_err, err) catch |e| {
                    std.debug.print("refuse {} at -j{d}\n", .{ refuse, j });
                    return e;
                };
                try std.testing.expectEqualStrings(f, out.written());
            } else {
                first = try gpa.dupe(u8, out.written());
                first_err = err;
            }
        }
        if (refuse) {
            try std.testing.expectEqual(@as(?anyerror, error.RangeFetchFailed), first_err);
        } else {
            try std.testing.expectEqual(@as(?anyerror, null), first_err);
            try std.testing.expectEqual(@as(usize, 1624), std.mem.count(u8, first.?, "\n"));
        }
    }
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
