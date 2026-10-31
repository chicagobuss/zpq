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
const fake_s3 = @import("../io/fake_s3.zig");

test "regression: a BOOLEAN answer column (key, min, max) prints true/false as its rows do, not its 0/1 lane" {
    var elem: schema.SchemaElement = .{
        .type = .BOOLEAN,
        .type_length = null,
        .repetition_type = .OPTIONAL,
        .name = "b",
        .num_children = null,
        .scale = null,
        .precision = null,
        .field_id = null,
    };
    try std.testing.expectEqual(TextKind.boolean, TextKind.ofSource(elem));
    elem.type = .INT32;
    try std.testing.expectEqual(TextKind.bytes, TextKind.ofSource(elem));

    const cols = [_][]const u8{ "b", "n" };
    const r0 = [_]scan.AggValue{ .{ .i = 1 }, .{ .i = 2 } };
    const r1 = [_]scan.AggValue{ .{ .i = 0 }, .{ .i = 1 } };
    const r2 = [_]scan.AggValue{ .null_val, .{ .i = 0 } };
    const rows = [_][]const scan.AggValue{ &r0, &r1, &r2 };
    const kinds = [_]TextKind{ .boolean, .utf8 };
    var buf: [256]u8 = undefined;
    var w: std.Io.Writer = .fixed(&buf);
    try writeAggregateText(&w, .{ .group_cols = &cols, .group_rows = &rows }, &kinds, &.{}, rows.len, .jsonl);
    try std.testing.expectEqualStrings(
        \\{"b":true,"n":2}
        \\{"b":false,"n":1}
        \\{"b":null,"n":0}
        \\
    , w.buffered());
    // An integer column keeps its integers.
    w = .fixed(&buf);
    try writeTypedAggValueJson(&w, .{ .i = 1 }, .bytes);
    try std.testing.expectEqualStrings("1", w.buffered());
    // No BOOLEAN value is bytes.
    w = .fixed(&buf);
    try std.testing.expectError(error.InvalidBinaryValue, writeBytesJson(&w, "x", .boolean));
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

test "a row query prints the same bytes at every parallelism, in every text format, failing where -j1 fails" {
    const full = "ci/fixtures/parquet/full_match.parquet"; // 8 row groups
    const tiny = "ci/fixtures/parquet/many_tiny.parquet"; // 512 row groups, ZSTD, a FLBA decimal
    const mix = "ci/fixtures/parquet/nullable_mix.parquet"; // 2 row groups, NULLs in most columns
    const escape = "ci/fixtures/parquet/json_escape.parquet"; // 1 row group, uncompressed
    for ([_][]const u8{ full, tiny, mix, escape }) |f| std.Io.Dir.cwd().access(std.testing.io, f, .{}) catch return error.SkipZigTest;
    const gpa = std.testing.allocator;
    const ctx: Context = .{ .gpa = gpa, .env = .empty, .io = std.testing.io };

    // One 1600-row row group beside 1-row ones: a budget that runs producers over the small ones leaves the big one to
    // the writer, which prints it a slice at a time, as -j1 does.
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var big_buf: [96]u8 = undefined;
    var small_buf: [96]u8 = undefined;
    const big = try std.fmt.bufPrint(&big_buf, ".zig-cache/tmp/{s}/big.parquet", .{tmp.sub_path});
    const small = try std.fmt.bufPrint(&small_buf, ".zig-cache/tmp/{s}/small.parquet", .{tmp.sub_path});
    for ([_]struct { []const u8, ?[]const u8, usize }{ .{ big, null, 1600 }, .{ small, "ts < 12", 1 } }) |w| {
        var r = try runQuery(ctx, .{ .inputs = &.{full}, .output = w[0], .filter = w[1], .row_group = .{ .rows = w[2] } });
        r.deinit(gpa);
    }
    // A STRING value that is not UTF-8 fails as it prints, after the bytes before it: the first `next` is a value
    // of the uncompressed column `s`, its first byte made one no UTF-8 has.
    var bad_buf: [96]u8 = undefined;
    const bad = try std.fmt.bufPrint(&bad_buf, ".zig-cache/tmp/{s}/bad_utf8.parquet", .{tmp.sub_path});
    {
        const bytes = try metadata.readFileSlice(escape, gpa);
        defer gpa.free(bytes);
        bytes[std.mem.indexOf(u8, bytes, "next").?] = 0xff;
        try std.Io.Dir.cwd().writeFile(std.testing.io, .{ .sub_path = bad, .data = bytes });
    }

    const Case = struct {
        inputs: []const []const u8,
        select: ?[]const u8 = null,
        filter: ?[]const u8 = null,
        offset: usize = 0,
        max_memory: usize = 512 << 20,
        err: ?anyerror = null,
    };
    const skewed: []const []const u8 = &.{ small, big, small };
    const cases = [_]Case{
        .{ .inputs = &.{full}, .filter = "s LIKE '%1%'" },
        .{ .inputs = &.{ full, full }, .select = "ts * 2 AS t2, s, x", .filter = "x > 0" },
        .{ .inputs = &.{full}, .offset = 333 },
        .{ .inputs = &.{tiny}, .filter = "g != 1" },
        .{ .inputs = &.{ mix, mix, mix } },
        // Row groups the producers' half of a small budget cannot all hold at once.
        .{ .inputs = &.{ full, full }, .filter = "b = 0", .max_memory = 96 << 10 },
        .{ .inputs = skewed, .max_memory = 256 << 10 },
        .{ .inputs = skewed, .filter = "x > 0", .max_memory = 512 << 10 },
        .{ .inputs = skewed, .max_memory = 1 << 20 },
        // Row 1000 (row group 5) divides by zero: the rows before its row group print, then its error.
        .{ .inputs = &.{full}, .select = "ts, 100 / (ts - 1000) AS q", .err = error.DivisionByZero },
        .{ .inputs = &.{ escape, escape, bad, escape }, .err = error.InvalidUtf8 },
    };
    for (cases) |c| for ([_]PrintFormat{ .jsonl, .json, .csv }) |format| {
        var first: ?[]u8 = null;
        defer if (first) |b| gpa.free(b);
        var first_rows: usize = 0;
        var first_column: Diag = .{};
        for ([_]usize{ 1, 2, 3, 16 }) |j| {
            var out: std.Io.Writer.Allocating = .init(gpa);
            defer out.deinit();
            var diag: Diag = .{};
            const args: QueryArgs = .{
                .inputs = c.inputs,
                .select = c.select,
                .filter = c.filter,
                .parallelism = j,
                .max_memory = c.max_memory,
                .diag = &diag,
            };
            const rows = if (runPrintSink(ctx, args, format, null, 1, c.offset, &out.writer)) |p| rows: {
                try std.testing.expect(c.err == null);
                break :rows p.rows;
            } else |e| rows: {
                const want: ?anyerror = e;
                if (c.err != want) std.debug.print("case of {d} inputs {t} at -j{d}: {t}\n", .{ c.inputs.len, format, j, e });
                try std.testing.expectEqual(c.err, want);
                break :rows 0;
            };
            if (first) |b| {
                std.testing.expectEqualStrings(b, out.written()) catch |e| {
                    std.debug.print("case of {d} inputs {t} at -j{d}\n", .{ c.inputs.len, format, j });
                    return e;
                };
                try std.testing.expectEqual(first_rows, rows);
                try std.testing.expectEqualStrings(first_column.column.get(), diag.column.get());
            } else {
                first = try gpa.dupe(u8, out.written());
                first_rows = rows;
                first_column = diag;
            }
        }
        try std.testing.expect(first.?.len > 0);
    };
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
    // (tools/zpq-gen overflow_rows). Every query that leaves row 500 out answers as if it were absent: a WHERE
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
    const sql_parser = if (@import("build_options").enable_sql) @import("../core/expr/sql_parser.zig") else return;
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

const profile_summary = @import("../profile_summary.zig");

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

test "regression: arithmetic over a NULL is NULL, which aggregates skip" {
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
    // A column's NULL or a computed one's makes the row's result NULL, which sum and count skip (it was refused).
    var nulls = try runQuery(ctx, .{
        .inputs = &.{fixture},
        .aggregate = "sum(i32n * 2) AS s, count(i32n * 2) AS n, sum(coalesce(a, b) + 1) AS c, count(coalesce(a, b) + 1) AS m",
    });
    defer nulls.deinit(gpa);
    const want = [_]i128{ 30, 4, 35, 4 };
    for (want, nulls.aggregate.output.aggs) |w, got| try std.testing.expectEqual(w, got.value.i);
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
            -9357363680509551616000, 253402225200000000000, 5,
        } },
        .{ .file = "int96_from_spark.parquet", .aggregate = "count(*) AS n", .filter = "a > '2262-12-31'", .values = &.{
            1,
        } },
        .{
            .file = "int96_from_spark.parquet",
            .aggregate = "count(*) AS n",
            .filter = "a = -9357363680509551616000",
            .values = &.{1},
        },
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

test "regression: inputs whose schema roots alone are named apart read together" {
    const dir = "ci/fixtures/parquet/";
    std.Io.Dir.cwd().access(std.testing.io, dir ++ "int96_pyarrow_root.parquet", .{}) catch return error.SkipZigTest;
    const gpa = std.testing.allocator;
    const ctx: Context = .{ .gpa = gpa, .env = .empty, .io = std.testing.io };
    // The same column `a`, under Spark's root `spark_schema` and pyarrow's `schema`: refused as a schema mismatch.
    const both: []const []const u8 = &.{ dir ++ "int96_from_spark.parquet", dir ++ "int96_pyarrow_root.parquet" };
    var r = try runQuery(ctx, .{ .inputs = both, .aggregate = "count(*) AS n, count(a) AS m" });
    defer r.deinit(gpa);
    try std.testing.expectEqual(@as(i128, 8), r.aggregate.output.aggs[0].value.i);
    try std.testing.expectEqual(@as(i128, 6), r.aggregate.output.aggs[1].value.i);
    var out: std.Io.Writer.Allocating = .init(gpa);
    defer out.deinit();
    const printed = try runPrintSink(ctx, .{ .inputs = both }, .jsonl, null, -1, 0, &out.writer);
    try std.testing.expectEqual(@as(usize, 8), printed.rows);
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
        // Read whole, the chunk's pages together pass the budget; a slice at a time, slices cut by the bytes their rows
        // took, they hold a 5 MB page and a slice (it held 160 MB at 8 MiB before slices were cut by bytes): not
        // within 4 MiB, where it refuses, and within 8 MiB, charged for the pieces its decode arena handed out rather
        // than the nodes it reserved.
        const refused = runQuery(ctx, .{
            .inputs = &.{path},
            .aggregate = "count(*) AS n",
            .filter = "s = 'x'",
            .parallelism = j,
            .max_memory = 4 << 20,
        });
        if (refused) |r| {
            var rr = r;
            rr.deinit(gpa);
            return error.TestUnexpectedResult;
        } else |e| try std.testing.expect(e == error.ExceededMemoryBudget or e == error.DecodeExceededBudget);
        for ([_]usize{ 8 << 20, 32 << 20 }) |budget| {
            var res = try runQuery(ctx, .{
                .inputs = &.{path},
                .aggregate = "count(*) AS n",
                .filter = "s = 'x'",
                .parallelism = j,
                .max_memory = budget,
            });
            defer res.deinit(gpa);
            try std.testing.expectEqual(@as(i128, 0), res.aggregate.output.aggs[0].value.i);
        }
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
