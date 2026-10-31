const engine_text = @import("text.zig");
const engine_types = @import("types.zig");
const engine_s3_rebase = @import("s3_rebase.zig");
const engine_s3_fetch = @import("s3_fetch.zig");
const engine_rank = @import("rank.zig");
const engine_write_window = @import("write_window.zig");
const print_window = @import("print_window.zig");
const engine_projection = @import("projection.zig");
const engine_input = @import("input.zig");
const engine_write = @import("write.zig");
const engine_env = @import("env.zig");
const std = @import("std");
const nowMonoNs = @import("../clock.zig").monoNs;
const profile = @import("../profile.zig");
const local_fs = @import("../local_fs.zig");
const local_reader = @import("../io/local_reader.zig");
const schema = @import("../core/schema.zig");
const compression = @import("../core/parquet/compression.zig");
const decimal_mod = @import("../core/parquet/decimal.zig");
const huge_pages = @import("../core/huge_pages.zig");
const core_memory = @import("../core/memory.zig");
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
const expr_agg = @import("../core/expr/agg.zig");
const filter_eval = @import("../core/filter/eval.zig");
const filter_ast = @import("../core/filter/ast.zig");
const filter_selection = @import("../core/filter/selection.zig");
const filter_prune = @import("../core/filter/prune.zig");
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
const pageIndexTestChunk = engine_s3_rebase.pageIndexTestChunk;
const profile_summary = @import("../profile_summary.zig");
const fake_s3 = @import("../io/fake_s3.zig");

test "Arrow columnar handoff applies the same row cut as text output" {
    const Capture = struct {
        schemas: usize = 0,
        rows: usize = 0,
        fn schemaFn(ptr: *anyopaque, cols: []const expr_agg.OutputCol) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.schemas += 1;
            try std.testing.expectEqual(@as(usize, 1), cols.len);
            try std.testing.expectEqualStrings("id", cols[0].name);
        }
        fn batchFn(ptr: *anyopaque, cols: []const expr_agg.OutputCol, lo: usize, hi: usize) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            try std.testing.expectEqual(@as(usize, 1), cols.len);
            try std.testing.expect(hi > lo);
            self.rows += hi - lo;
        }
    };
    const ctx: Context = .{ .gpa = std.testing.allocator, .env = .empty, .io = std.testing.io };
    var capture: Capture = .{};
    var args: QueryArgs = .{ .inputs = &.{"ci/fixtures/parquet/nullable_mix.parquet"}, .columns = &.{"id"}, .parallelism = 1 };
    var text: std.Io.Writer.Allocating = .init(std.testing.allocator);
    defer text.deinit();
    const want = try runPrintSink(ctx, args, .jsonl, 3, -1, 1, &text.writer);
    args.columnar_sink = .{ .ctx = &capture, .schema_fn = Capture.schemaFn, .batch_fn = Capture.batchFn };
    const got = try runPrintSink(ctx, args, .jsonl, 3, -1, 1, null);
    try std.testing.expectEqual(@as(usize, 1), capture.schemas);
    try std.testing.expectEqual(want.rows, capture.rows);
    try std.testing.expectEqual(want.rows, got.rows);
}

pub const StdoutWriter = struct {
    fd: local_fs.fd_t = 1,
    sink: ?*std.Io.Writer = null,
    buf: [4096]u8 = undefined,
    pos: usize = 0,
    output_ns: u64 = 0,
    output_started_ns: i64 = 0,
    bytes_written: u64 = 0,
    json_array: bool = false,
    skip_remaining: usize = 0,
    parquet: ?*ParquetPrint = null,
    columnar: ?ColumnarSink = null,
    /// Names the column of a value that cannot be printed (`TextKind`).
    diag: ?*Diag = null,

    pub fn writeAll(self: *StdoutWriter, bytes: []const u8) !void {
        if (bytes.len > 0 and self.output_started_ns == 0) self.output_started_ns = nowMonoNs();
        // A block no smaller than the buffer goes out whole, after what the buffer holds: a row group's printed text
        // is one write, not one per buffer's worth.
        if (bytes.len >= self.buf.len) {
            try self.flush();
            if (self.sink) |sink| try sink.writeAll(bytes) else try local_fs.writeAll(self.fd, bytes);
            self.bytes_written += bytes.len;
            return;
        }
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
        try printThrough(self, fmt, args);
    }

    pub fn flush(self: *StdoutWriter) !void {
        if (self.pos == 0) return;
        const pending = self.buf[0..self.pos];
        self.pos = 0;
        if (self.sink) |sink| try sink.writeAll(pending) else try local_fs.writeAll(self.fd, pending);
        self.bytes_written += pending.len;
    }
};

const printThrough = engine_text.printThrough;

test "StdoutWriter.print writes a line longer than its stack buffer" {
    var sink: std.Io.Writer.Allocating = .init(std.testing.allocator);
    defer sink.deinit();
    var w: StdoutWriter = .{ .sink = &sink.writer };
    const long: [2000]u8 = @splat('x');
    try w.print("min={s} ({s})\n", .{ &long, "exact" });
    try w.flush();
    const got = sink.written();
    try std.testing.expectEqual(long.len + "min= (exact)\n".len, got.len);
    try std.testing.expect(std.mem.startsWith(u8, got, "min=xxx") and std.mem.endsWith(u8, got, "x (exact)\n"));
}

/// Marks the leaves row output cannot print: under a LIST or MAP, or repeated without one (a legacy REPEATED field or
/// group), whose values are a list per row, not one each.
fn checkNestedListMap(node: schema_tree.Node, is_nested: bool, list_map_cols: []bool) void {
    switch (node) {
        .primitive => |p| {
            if (is_nested or p.max_rep > 0) {
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

pub const SlicedPrint = struct {
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
    /// Where the rows each slice read and kept are counted: only slices read count, as printing stops at the limit.
    counts: *consumer.Timings,
    /// Retained across the print's row groups when set. Null keeps the scratch for this row group only.
    scratch: ?*?*consumer.SliceScratch = null,
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

    var local_slot: ?*consumer.SliceScratch = null;
    const slot = o.scratch orelse &local_slot;
    defer if (o.scratch == null) consumer.releaseSlicePool(slot);
    const pool = try consumer.poolOn(slot, gpa);
    // Readers release their references first. An eighth of the query allowance stays for the next group.
    defer pool.trimAbove(core_memory.retainedCarry(o.decode_options.expand_limit));
    var n_cols: usize = 0;
    for (fetch_arr, 0..) |needed, ci| {
        if (needed and ci < rg.columns.items.len) n_cols += 1;
    }
    pool.rewind();
    try pool.reserve(n_cols);
    var pages = std.heap.ArenaAllocator.init(gpa);
    defer pages.deinit();
    var readers: std.ArrayList(consumer.SliceReader) = .empty;
    defer for (readers.items) |*r| r.deinit();
    const lookup = try pages.allocator().alloc(?usize, fetch_arr.len);
    @memset(lookup, null);
    for (fetch_arr, 0..) |needed, ci| {
        if (!needed or ci >= rg.columns.items.len) continue;
        const r = try consumer.SliceReader.initPooled(
            pages.allocator(),
            gpa,
            rg,
            meta,
            src,
            ci,
            o.decode_options,
            pool,
        ) orelse unreachable; // every column was checked above
        lookup[ci] = readers.items.len;
        try readers.append(pages.allocator(), r);
    }
    // A slice takes a quarter of the budget. Under a limit the first slice is about twice the rows still wanted, and
    // each one after (a filter dropped rows) twice the last.
    const per_row = @max(1, est / num_rows);
    const max_rows = std.math.clamp(o.budget / 4 / per_row, 1, num_rows);
    var slice_rows = if (wanted) |w| std.math.clamp(w *| 2, @min(64, max_rows), max_rows) else max_rows;

    var done: usize = 0;
    const slice = pool.valueArena();
    defer _ = slice.reset(.retain_capacity);
    while (done < num_rows) {
        if (o.limit) |lim| if (o.total_printed.* >= lim) break;
        const n = @min(slice_rows, num_rows - done);
        _ = slice.reset(.retain_capacity);
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
        o.counts.rows_scanned += n;
        o.counts.rows_matched += try consumer.appendSliceRows(agg, &batch, lookup, &sel, output_specs, sa);
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

/// Print `rg` whole, or a slice of rows at a time when it would not fit (`printRowGroupSliced`): the serial loop's step,
/// and the parallel writer's for a row group no producer took (`print_window`).
pub fn printRowGroup(
    gpa: std.mem.Allocator,
    agg: *consumer.OutputAggregator,
    rg: *const schema.RowGroup,
    meta: *const schema.FileMetaData,
    src: consumer.RGSrc,
    filter: ?filter_ast.Filter,
    fetch_arr: []const bool,
    output_specs: []const consumer.OutputCol,
    sliced: SlicedPrint,
) !void {
    if (try printRowGroupSliced(gpa, agg, rg, meta, src, filter, fetch_arr, output_specs, sliced)) return;
    const matched = consumer.appendProjectedRGWithOptions(
        agg,
        gpa,
        rg,
        meta,
        src,
        filter,
        fetch_arr,
        output_specs,
        sliced.counts,
        sliced.decode_options,
    ) catch |err| {
        // Values that expand past the budget held whole may still print a slice of rows at a time.
        if (err != error.DecodeExceededBudget) return err;
        consumer.resetAggregator(agg);
        var streamed = sliced;
        streamed.force = true;
        if (!try printRowGroupSliced(gpa, agg, rg, meta, src, filter, fetch_arr, output_specs, streamed)) return err;
        return;
    };
    sliced.counts.rows_scanned += @intCast(rg.num_rows);
    sliced.counts.rows_matched += matched;

    const print_span = profile.Span.begin(sliced.counts.prof);
    try flushAggregator(agg, sliced.names, sliced.writer, sliced.is_jsonl, sliced.limit, sliced.total_printed);
    print_span.end(.sink_write, profile.args(.{agg.num_rows}));
    consumer.resetAggregator(agg);
}

fn aggregatorColumns(arena: std.mem.Allocator, agg: *consumer.OutputAggregator, names: []const []const u8) ![]expr_agg.OutputCol {
    const cols = try arena.alloc(expr_agg.OutputCol, names.len);
    for (cols, names, agg.cols, agg.schema_elems) |*out, name, buf, elem| {
        out.* = .{
            .name = name,
            .col = consumer.bufRows(buf, 0, agg.num_rows),
            .parquet_type = elem.type.?,
            .type_length = elem.type_length,
            .nullable = elem.repetition_type == .OPTIONAL,
            .logical_type = elem.logical_type,
            .converted_type = elem.converted_type,
            .dec = if (decimal_mod.kindFromSchema(&elem)) |d| .{ .precision = @intCast(d.precision), .scale = @intCast(d.scale) } else null,
        };
    }
    return cols;
}

pub fn flushAggregator(
    agg: *consumer.OutputAggregator,
    names: []const []const u8,
    writer: anytype,
    is_jsonl: bool,
    limit: ?usize,
    total_printed: *usize,
) !void {
    if (agg.num_rows > 0 and writer.output_started_ns == 0) writer.output_started_ns = nowMonoNs();
    if (writer.parquet) |pq| {
        try pq.append(agg, limit, total_printed, &writer.skip_remaining);
        return;
    }
    if (writer.columnar) |cs| {
        const lo = @min(writer.skip_remaining, agg.num_rows);
        writer.skip_remaining -= lo;
        const hi = lo + @min(agg.num_rows - lo, if (limit) |n| n -| total_printed.* else agg.num_rows);
        var scratch = std.heap.ArenaAllocator.init(std.heap.c_allocator);
        defer scratch.deinit();
        if (hi > lo) try cs.batch_fn(cs.ctx, try aggregatorColumns(scratch.allocator(), agg, names), lo, hi);
        total_printed.* += hi - lo;
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
                printCell(writer, col, row_idx, agg.schema_elems[col_idx], true) catch |err| {
                    if (err == error.InvalidUtf8 or err == error.InvalidBinaryValue) {
                        Diag.setColumn(writer.diag, names[col_idx]);
                    }
                    return err;
                };
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
    parquet_version: schema.ParquetVersion = .v2,
    arena: std.mem.Allocator,
    gpa: std.mem.Allocator,
    fd_sink: FdSink,
    codec: schema.CompressionCodec,
    zstd: compression.ZstdPlan,
    schema_items: std.ArrayListUnmanaged(schema.SchemaElement),
    rgs: std.ArrayListUnmanaged(schema.RowGroup) = .empty,
    out_offset: u64 = 0,
    /// Where row groups are cut (`RowGroupTarget`), as on every write.
    target: RowGroupTarget = .{},
    /// Rows waiting for their row group: copied in as batches come, written once one closes.
    pending: ?consumer.OutputAggregator = null,
    pending_arena: ?std.heap.ArenaAllocator = null,
    pending_bits: u64 = 0,
    dictionary_bloom: bool = true,

    fn sink(self: *ParquetPrint) streaming.Sink {
        return self.fd_sink.sink();
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
        var scratch = std.heap.ArenaAllocator.init(self.gpa);
        defer scratch.deinit();
        const target_bits = self.target.bytes *| 8;
        var lo = start_row;
        const end = start_row + n;
        while (lo < end) {
            if (self.pending == null) {
                self.pending_arena = .init(self.gpa);
                const pa = self.pending_arena.?.allocator();
                const cols = try pa.alloc(consumer.OutputAggregator.ColumnBuf, agg.cols.len);
                for (cols, agg.cols) |*c, src| c.* = switch (src) {
                    inline else => |_, tag| @unionInit(consumer.OutputAggregator.ColumnBuf, @tagName(tag), .{}),
                };
                self.pending = .{
                    .arena = pa,
                    .cols = cols,
                    .schema_elems = agg.schema_elems,
                    .paths = agg.paths,
                    .num_rows = 0,
                    .captured = true,
                };
            }
            const p = &self.pending.?;
            var hi = @min(end, lo + (self.target.rows - p.num_rows));
            var bits = plainBitsOf(agg, lo, hi);
            const room = target_bits -| self.pending_bits;
            if (bits >= room) {
                // The row that reaches the byte target closes the row group.
                hi = lo;
                bits = 0;
                while (hi < end) {
                    bits += plainBitsOf(agg, hi, hi + 1);
                    hi += 1;
                    if (bits >= room) break;
                }
            }
            _ = scratch.reset(.retain_capacity);
            const sa = scratch.allocator();
            var all = try filter_selection.SelectionVector.init(sa, hi - lo);
            for (p.cols, agg.cols) |*dst, src| {
                try consumer.appendRows(dst, consumer.bufRows(src, lo, hi), &all, hi - lo, p.arena, sa);
            }
            p.num_rows += hi - lo;
            self.pending_bits += bits;
            total.* += hi - lo;
            lo = hi;
            if (p.num_rows >= self.target.rows or self.pending_bits >= target_bits) try self.flushPending();
        }
    }

    fn plainBitsOf(agg: *const consumer.OutputAggregator, lo: usize, hi: usize) u64 {
        var bits: u64 = 0;
        for (agg.cols, agg.schema_elems) |*c, *e| bits += consumer.plainBits(c, e, lo, hi);
        return bits;
    }

    /// Write the rows waiting as one row group, its page index after each chunk.
    fn flushPending(self: *ParquetPrint) !void {
        const p = &(self.pending orelse return);
        defer {
            self.pending_arena.?.deinit();
            self.pending_arena = null;
            self.pending = null;
            self.pending_bits = 0;
        }
        if (p.num_rows == 0) return;
        var timings: consumer.Timings = .{};
        const out = try consumer.encodeAggregator(
            p,
            self.arena,
            self.gpa,
            self.sink(),
            &self.out_offset,
            self.codec,
            self.zstd,
            &timings,
            1,
            true,
            .{ .dictionary_bloom = self.dictionary_bloom, .parquet_version = self.parquet_version },
        );
        if (out.rg) |rg| try self.rgs.append(self.arena, rg);
    }

    fn deinit(self: *ParquetPrint) void {
        if (self.pending_arena) |*a| a.deinit();
        self.pending_arena = null;
        self.pending = null;
    }

    fn finish(self: *ParquetPrint, rows: usize) !void {
        try self.flushPending();
        var out: streaming.SinkOut = .{ .sink = self.sink(), .offset = @intCast(self.out_offset) };
        try fastpath.placeFilters(&out, self.rgs.items);
        self.out_offset = out.offset;
        const meta: schema.FileMetaData = .{
            .version = self.parquet_version.footer(),
            .schema = self.schema_items,
            .num_rows = @intCast(rows),
            .row_groups = self.rgs,
            .created_by = schema.ZPQ_CREATED_BY,
            .column_orders = try schema.FileMetaData.outputColumnOrders(self.arena, invariant.footerLeafCount(self.schema_items.items), null, &.{}),
        };
        var writer: thrift.Writer = .init(self.arena);
        defer writer.deinit();
        try meta.write(&writer);
        const footer = writer.bytes();
        try self.sink().write(footer);
        var len: [4]u8 = undefined;
        std.mem.writeInt(u32, &len, std.math.cast(u32, footer.len) orelse return error.TooLarge, .little);
        try self.sink().write(&len);
        try self.sink().write("PAR1");
        self.out_offset += footer.len + 8;
    }
};

pub const PrintResult = struct {
    output_ns: u64 = 0,
    /// Rows in every row group of the inputs, as an aggregate or a write counts them; of those, the rows read (row
    /// groups not pruned, not passed over by a top-N, not left unread once the limit was met) and the rows the filter
    /// kept of them.
    rows_in: usize = 0,
    rows_scanned: usize = 0,
    rows_matched: usize = 0,
    /// Row groups read or passed over, and of those the ones statistics (or membership, or a NaN probe) ruled out.
    row_groups_in: usize = 0,
    row_groups_pruned: usize = 0,
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
    caller_ctx: Context,
    initial_args: QueryArgs,
    format: PrintFormat,
    limit: ?usize,
    fd: local_fs.fd_t,
    offset: usize,
    sink: ?*std.Io.Writer,
) !PrintResult {
    const ctx = caller_ctx.forQuery(&initial_args);
    var cache: engine_types.bound_plan.Cache = .{};
    const args = initial_args.withPlanCache(&cache);
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
    consumer.setDecodeFailure(null);
    errdefer consumer.resolveDecodeFailure(args.diag, metas, opened.inputs);
    if (metas.len == 0) return .{};
    const meta0 = &metas[0];

    for (metas[1..]) |*m| {
        if (metadata.schemaMismatch(meta0.schema.items, m.schema.items, .exact) != null) return error.SchemaMismatch;
    }

    const tree0 = try schema_tree.SchemaTree.build(arena, meta0.schema.items);

    var proj = try planProjection(arena, args, meta0);
    const filter_opt = proj.filter;
    const output_specs = proj.output_specs;
    if (limit != 0) _ = try loadLocalMembership(arena, opened, metas, filter_opt, args.scan_all, true);
    parse_span.end(.parse_query, profile.args(.{}));
    if (args.profiler) |p| p.setColumnsFromMeta(meta0);

    // Check nested columns
    const list_map_cols = try arena.alloc(bool, tree0.leaves.len);
    @memset(list_map_cols, false);
    checkNestedListMap(.{ .group = tree0.root }, false, list_map_cols);

    for (output_specs) |spec| {
        switch (spec) {
            .passthrough => |pt| {
                const ci = pt.ci;
                if (ci < list_map_cols.len and list_map_cols[ci]) {
                    Diag.setColumn(args.diag, try std.mem.join(arena, ".", tree0.leaves[ci].path));
                    return error.NestedRowOutputNotSupported;
                }
            },
            .computed => |comp| {
                const comp_cols = try arena.alloc(bool, tree0.leaves.len);
                @memset(comp_cols, false);
                comp.expr.collectColumns(comp_cols);
                for (comp_cols, 0..) |referenced, ci| {
                    if (referenced and ci < list_map_cols.len and list_map_cols[ci]) {
                        Diag.setColumn(args.diag, try std.mem.join(arena, ".", tree0.leaves[ci].path));
                        return error.NestedRowOutputNotSupported;
                    }
                }
            },
        }
    }

    // The row sink has a flat schema and no repetition-level buffer. Keep unsupported
    // leaves out rather than publishing a footer that misdescribes their encoded pages.
    if (format == .parquet) for (output_specs) |spec| switch (spec) {
        .passthrough => |pt| {
            const ci = pt.ci;
            const leaf = tree0.leaves[ci];
            if (leaf.type == .INT96) return error.INT96ReencodeNotSupported;
            // A named leaf is written flat under its name; an unnamed nested one would lose its parents.
            if ((leaf.path.len > 1 and pt.name == null) or leaf.max_rep > 0) return error.NestedReencodeNotSupported;
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

    if (args.columnar_sink != null) for (output_specs) |spec| switch (spec) {
        .passthrough => |pt| if (metadata.leafSchemaElement(meta0, pt.ci)) |elem| {
            if (decimal_mod.kindFromSchema(&elem)) |d| if (d.precision > 38) return error.UnsupportedDecimalPrecision;
        },
        .computed => {},
    };

    var agg = try consumer.initOutputAggregator(arena, meta0, output_specs);

    var parquet: ParquetPrint = .{
        .parquet_version = args.parquet_version,
        .arena = arena,
        .gpa = ctx.gpa,
        .fd_sink = .{ .fd = fd },
        .dictionary_bloom = args.bloom_filters_enabled,
        .codec = args.codec orelse .SNAPPY,
        .zstd = .{ .lambda = args.compress_priority.lambda() },
        .schema_items = try buildSelectSchema(arena, meta0, output_specs),
        .target = args.row_group.under(args.max_memory_explicit),
    };
    defer parquet.deinit();
    var output_started_ns: i64 = 0;
    if (format == .parquet) {
        coerceByteArrayDecimalLeavesToFlba(&parquet.schema_items);
        coerceFloat16LeavesToDouble(&parquet.schema_items);
        output_started_ns = nowMonoNs();
        try parquet.start();
    }
    var stdout_writer = StdoutWriter{
        .fd = fd,
        .sink = sink,
        .json_array = format == .json,
        .columnar = args.columnar_sink,
        .skip_remaining = offset,
        .parquet = if (format == .parquet) &parquet else null,
        .output_started_ns = output_started_ns,
        .diag = args.diag,
    };
    defer stdout_writer.flush() catch {};

    const names = try proj.names(arena);
    if (args.columnar_sink) |cs| try cs.schema_fn(cs.ctx, try aggregatorColumns(arena, &agg, names));
    if (args.decimal_columns) |dc| for (output_specs, names) |spec, name| {
        const decimal = switch (spec) {
            .passthrough => |pt| if (metadata.leafSchemaElement(meta0, pt.ci)) |elem|
                decimal_mod.kindFromSchema(&elem) != null and elem.type != .INT96
            else
                false,
            .computed => |c| c.expr.typeOf() == .dec,
        };
        if (decimal) try dc.names.append(dc.gpa, try dc.gpa.dupe(u8, name));
    };
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
    const t_scan = nowMonoNs();
    var t: Timings = .{};
    t.core.prof = prof;

    const counts = try printRows(ctx, arena, args, opened, metas, proj, &agg, names, &stdout_writer, format, limit, offset, &t);
    const total_printed = counts.rows;
    const rgs_in = counts.groups;
    const rgs_pruned = counts.pruned;

    t.decode_wall_ns = @intCast(nowMonoNs() - t_scan);
    const finish_start = nowMonoNs();
    if (stdout_writer.output_started_ns == 0) stdout_writer.output_started_ns = finish_start;
    try opened.intact();
    if (format == .json) try stdout_writer.writeAll("]\n");
    if (format == .parquet) try parquet.finish(total_printed);
    try stdout_writer.flush();
    t.output_started_ns = stdout_writer.output_started_ns;
    stdout_writer.output_ns = @intCast(nowMonoNs() - t.output_started_ns);
    _ = t_start;
    var all_rows: usize = 0;
    for (metas) |m| for (m.row_groups.items) |rg| {
        all_rows += @intCast(rg.num_rows);
    };
    return .{ .rows_in = all_rows, .rows_scanned = t.core.rows_scanned, .rows_matched = t.core.rows_matched, .row_groups_in = rgs_in, .row_groups_pruned = rgs_pruned, .rows = total_printed, .bytes_out = if (format == .parquet) parquet.out_offset else stdout_writer.bytes_written, .output_ns = stdout_writer.output_ns, .membership = opened.membership, .timings = t };
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
            defer sink.deinit();
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

test "CSV: temporal text, bytes under the JSON rules, quoting, NULL and the empty string apart" {
    var buf: [512]u8 = undefined;
    var w: std.Io.Writer = .fixed(&buf);
    const date: Temporal = .{ .kind = .date };
    const T = struct { t: Temporal, v: i128, want: []const u8 };
    for ([_]T{
        .{ .t = date, .v = 0, .want = "1970-01-01" },
        .{ .t = date, .v = -1, .want = "1969-12-31" },
        .{ .t = date, .v = 18_263, .want = "2020-01-02" },
        .{ .t = date, .v = 11_016, .want = "2000-02-29" },
        .{ .t = date, .v = -719_528, .want = "0000-01-01" },
        .{ .t = .{ .kind = .timestamp, .unit = .us }, .v = 1_577_934_245_123_456, .want = "2020-01-02 03:04:05.123456" },
        .{ .t = .{ .kind = .timestamp, .unit = .ms, .utc = true }, .v = -1, .want = "1969-12-31 23:59:59.999Z" },
        .{ .t = .{ .kind = .timestamp, .unit = .ns }, .v = 1, .want = "1970-01-01 00:00:00.000000001" },
        .{ .t = .{ .kind = .time, .unit = .ms }, .v = 3_723_500, .want = "01:02:03.500" },
        .{ .t = .{ .kind = .time, .unit = .ns }, .v = 86_399_999_999_999, .want = "23:59:59.999999999" },
    }) |c| {
        w = .fixed(&buf);
        try c.t.write(&w, c.v);
        try std.testing.expectEqualStrings(c.want, w.buffered());
    }
    var elem: schema.SchemaElement = .{
        .type = .INT32,
        .type_length = null,
        .repetition_type = .OPTIONAL,
        .name = "d",
        .num_children = null,
        .scale = null,
        .precision = null,
        .field_id = null,
        .converted_type = .DATE,
    };
    try std.testing.expect(Temporal.of(elem).?.kind == .date);
    elem.converted_type = .TIMESTAMP_MILLIS;
    elem.type = .INT64;
    try std.testing.expect(Temporal.of(elem).?.utc);
    elem.converted_type = .INT_64;
    try std.testing.expect(Temporal.of(elem) == null);

    const B = struct { v: []const u8, kind: TextKind, want: []const u8 };
    for ([_]B{
        .{ .v = "plain", .kind = .utf8, .want = "plain" },
        .{ .v = "", .kind = .utf8, .want = "\"\"" },
        .{ .v = "a,b \"q\"\nz", .kind = .utf8, .want = "\"a,b \"\"q\"\"\nz\"" },
        .{ .v = "text", .kind = .bytes, .want = "text" },
        .{ .v = "\xff\x00", .kind = .bytes, .want = "\"{\"\"base64\"\":\"\"/wA=\"\"}\"" },
        .{ .v = "ok", .kind = .base64, .want = "\"{\"\"base64\"\":\"\"b2s=\"\"}\"" },
        .{ .v = &.{ 0xb6, 0xd5, 0xf6, 0xa0, 0, 0, 0x40, 0, 0x80, 0, 0, 0, 0, 0, 0, 1 }, .kind = .uuid, .want = "b6d5f6a0-0000-4000-8000-000000000001" },
        .{ .v = &.{ 1, 0, 0, 0, 2, 0, 0, 0, 3, 0, 0, 0 }, .kind = .interval, .want = "\"{\"\"months\"\":1,\"\"days\"\":2,\"\"millis\"\":3}\"" },
    }) |c| {
        w = .fixed(&buf);
        try writeBytesCsv(&w, c.v, c.kind);
        try std.testing.expectEqualStrings(c.want, w.buffered());
    }
    w = .fixed(&buf);
    try std.testing.expectError(error.InvalidUtf8, writeBytesCsv(&w, "\xff", .utf8));

    // An answer: header (an avg_state as two columns), BOOLEAN, DATE, NULL and the empty string apart.
    const cols = [_][]const u8{ "b", "d", "s", "a" };
    const r0 = [_]scan.AggValue{ .{ .i = 1 }, .{ .i = 18_263 }, .{ .s = "" }, .{ .avg = .{ .sum = 3.5, .count = 2 } } };
    const r1 = [_]scan.AggValue{ .null_val, .null_val, .null_val, .{ .avg = .{ .sum = null, .count = 0 } } };
    const rows = [_][]const scan.AggValue{ &r0, &r1 };
    const kinds = [_]TextKind{ .boolean, .bytes, .utf8, .utf8 };
    const types = [_]AnswerType{ .{}, .{ .temporal = date }, .{}, .{ .avg = true } };
    w = .fixed(&buf);
    try writeAggregateText(&w, .{ .group_cols = &cols, .group_rows = &rows }, &kinds, &types, rows.len, .csv);
    try std.testing.expectEqualStrings(
        \\b,d,s,a__sum,a__count
        \\true,2020-01-02,"",3.5,2
        \\,,,,0
        \\
    , w.buffered());
}

const PrintCounts = struct { rows: usize, groups: usize, pruned: usize };

fn printRows(
    ctx: Context,
    arena: std.mem.Allocator,
    args: QueryArgs,
    opened: OpenedInputs,
    metas: []const schema.FileMetaData,
    proj: engine_projection.ProjectionPlan,
    agg: *consumer.OutputAggregator,
    names: []const []const u8,
    stdout_writer: *StdoutWriter,
    format: PrintFormat,
    limit: ?usize,
    offset: usize,
    t: *Timings,
) !PrintCounts {
    const prof = profile.threadOf(args.profiler, .main);
    const output_specs = proj.output_specs;
    const filter_opt = proj.filter;
    const fetch_arr = proj.fetch;
    const meta0 = &metas[0];
    const is_jsonl = format != .csv;
    var total_printed: usize = 0;
    var rgs_in: usize = 0;
    var rgs_pruned: usize = 0;
    if (proj.row_keys.len > 0) {
        // Without a LIMIT every row is kept and sorted, within half the memory budget (as decoded rows are).
        const keep = if (limit) |l| offset +| l else std.math.maxInt(usize);
        // With a LIMIT, row groups go most promising first, and one whose statistics cannot beat the heap's last row
        // is passed over. Ties keep their input positions (`RowPos`), so the order read changes no answer. Only over
        // bare columns: a computed output or key can fail on a row (`100 / id`), and passing over its group would
        // answer where reading in order raises the error. Filter leaves compare a column with a literal and never fail.
        const bounded = limit != null and !args.scan_all and allPassthrough(output_specs, proj.row_keys);
        const items = if (limit != 0) try rankRowGroups(arena, opened.inputs, metas, proj.row_keys[0], bounded) else try arena.alloc(RankedRG, 0);
        for (items) |item| {
            const meta_i = &metas[item.file];
            const rg = &meta_i.row_groups.items[item.rg];
            // Every ranked row group is visited, by the gather workers or the loop below, and pruned by the same
            // footer-only decision, so they are counted here once.
            rgs_in += 1;
            if (filter_opt) |f| if (!args.scan_all) {
                if (filter_prune.pruneRowGroup(rg, f, meta_i) == .skip) rgs_pruned += 1;
            };
        }
        var gathered = if (gatherEligible(ctx.env, opened.inputs, meta0, limit, keep, output_specs, proj.row_keys))
            try rankAndGather(ctx, arena, args, opened.inputs, metas, filter_opt, fetch_arr, output_specs, proj.row_keys, keep, items, t)
        else
            null;
        defer if (gathered) |*g| g.deinit();
        var ranked = if (gathered) |g| g.out else ranked: {
            const top_limit = topnLimit(ctx, args, opened.inputs);
            var top: TopRows = try .init(ctx.gpa, meta0, output_specs, proj.row_keys, keep, top_limit);
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
        try flushAggregator(&ranked, names, stdout_writer, is_jsonl, limit, &total_printed);
        print_span.end(.sink_write, profile.args(.{ranked.num_rows}));
    } else {
        var print_scratch: ?*consumer.SliceScratch = null;
        defer consumer.releaseSlicePool(&print_scratch);
        // A row group too big to hold decoded prints a slice of rows at a time.
        const sliced: SlicedPrint = .{
            .names = names,
            .writer = stdout_writer,
            .is_jsonl = is_jsonl,
            .limit = limit,
            .total_printed = &total_printed,
            .budget = args.max_memory / 2,
            .decode_options = .{ .fast_levels = args.fast_levels, .expand_limit = args.max_memory },
            .counts = &t.core,
            .scratch = &print_scratch,
        };
        if (try print_window.plan(ctx, arena, args, opened, metas, filter_opt, limit, format == .parquet)) |p| {
            try print_window.run(ctx, arena, args, p, agg, filter_opt, fetch_arr, output_specs, sliced, &t.core);
            return .{ .rows = total_printed, .groups = p.counts.groups, .pruned = p.counts.pruned };
        }
        for (opened.inputs, metas, 0..) |in, meta_i, file_i| {
            const rg_src: consumer.RGSrc = .{ .bytes = in.bytes, .byte_origin = 0 };
            const meta_const = meta_i;
            for (meta_const.row_groups.items, 0..) |*src_rg, rg_i| {
                if (limit) |lim| {
                    if (total_printed >= lim) break;
                }

                rgs_in += 1;

                const rg_span = profile.Span.begin(prof);
                var skipped = false;
                defer {
                    const decision: u8 = if (skipped) 0 else 2;
                    rg_span.end(.row_group, profile.args(.{ file_i, rg_i, decision, src_rg.num_rows }));
                }
                if (filter_opt) |f| if (!args.scan_all) {
                    if (filter_prune.pruneRowGroup(src_rg, f, &meta_const) == .skip) {
                        skipped = true;
                        rgs_pruned += 1;
                        skipRowGroup(in, rg_i);
                        continue;
                    }
                };
                if (in.gate) |g| try g.wait(g.ctx, rg_i);
                defer skipRowGroup(in, rg_i);
                try printRowGroup(ctx.gpa, agg, src_rg, &meta_const, rg_src, filter_opt, fetch_arr, output_specs, sliced);
            }
        }
    }

    return .{ .rows = total_printed, .groups = rgs_in, .pruned = rgs_pruned };
}
