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
const expr_ast = @import("../core/expr/ast.zig");
const statistics = @import("../core/parquet/statistics.zig");
const membership = @import("../core/filter/membership.zig");
const nanprobe = @import("../core/filter/nanprobe.zig");
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
