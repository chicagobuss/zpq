const expr_ast = @import("../core/expr/ast.zig");
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
const std = @import("std");
const nowMonoNs = @import("../clock.zig").monoNs;
const profile = @import("../profile.zig");
const local_fs = @import("../local_fs.zig");
const local_reader = @import("../io/local_reader.zig");
const schema = @import("../core/schema.zig");
const compression = @import("../core/parquet/compression.zig");
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
const filter_ast = @import("../core/filter/ast.zig");
const filter_parser = @import("../core/filter/parser.zig");
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
const StdoutWriter = engine_print.StdoutWriter;
const PrintResult = engine_print.PrintResult;
const runPrint = engine_print.runPrint;
const runPrintTo = engine_print.runPrintTo;
const runPrintSink = engine_print.runPrintSink;
const pageIndexTestChunk = engine_s3_rebase.pageIndexTestChunk;
const profile_summary = @import("../profile_summary.zig");
const fake_s3 = @import("../io/fake_s3.zig");

pub fn runQuery(caller_ctx: Context, args: QueryArgs) !QueryResult {
    const ctx = caller_ctx.forQuery(&args);
    const span = profile.Span.begin(profile.threadOf(args.profiler, .main));
    defer span.end(.query, profile.args(.{}));
    if (args.inputs.len == 0) return error.NoInputs;
    if (args.kind() != .rows and (args.select != null or args.columns != null)) {
        return error.AggregateMutexWithSelect;
    }
    if (args.kind() != .rows) {
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

/// `filter`'s conjuncts as the plan runs them, each with the column it tests (a composite: its first leaf's).
fn explainConjuncts(arena: std.mem.Allocator, filter: ?filter_ast.Filter, meta: *const schema.FileMetaData) ![]const Conjunct {
    const f = filter orelse return &.{};
    const children: []const filter_ast.Filter = switch (f) {
        .and_filter => |c| if (c.in_set == null and !c.null_member) c.children else &.{f},
        else => &.{f},
    };
    const out = try arena.alloc(Conjunct, children.len);
    for (children, out) |child, *o| {
        var cols: std.ArrayList(usize) = .empty;
        try child.collectColumns(&cols, arena);
        const name = if (cols.items.len == 0) "" else blk: {
            const path = (try metadata.leafPathSegments(arena, meta, cols.items[0])) orelse break :blk "";
            break :blk try std.mem.join(arena, ".", path);
        };
        o.* = .{ .column = name, .cost = filter_ast.cost(child) };
    }
    return out;
}

/// Planning shares the execution path, with caller-owned arena storage and metadata-only input opening.
pub fn explainAggregate(caller_ctx: Context, arena: std.mem.Allocator, args: QueryArgs) !AggregateExplanation {
    const ctx = caller_ctx.forQuery(&args);
    if (args.inputs.len == 0) return error.NoInputs;
    if (args.kind() == .rows) return error.MissingOutputOrAggregate;
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
    initial_args: QueryArgs,
    agg_str: []const u8,
    comptime explain: bool,
) !if (explain) AggregateExplanation else AggResult {
    var cache: engine_types.bound_plan.Cache = .{};
    const args = initial_args.withPlanCache(&cache);
    if (args.text_output != null and args.aggregate_sink != null) return error.ConflictingAggregateSinks;
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
    // The query's ledger: what the scan's tables, decode memory, merge and answer hold together stays under it.
    var ledger: core_memory.SharedBudget = .{ .limit = args.max_memory };
    defer if (args.profiler) |p| p.noteLedger(ledger.peak.load(.monotonic), args.max_memory);

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
    io_policy.cache = args.cache;
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
    if (io_policy.profile == .auto and args.kind() == .grouped and cut_args.result_order.limit != null and
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
                .bound_aggregate = try args.boundAggregate(arena, &meta0),
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
    // Anything but the plain mapping needs the footers and the plan here: the explicit modes to read by them, and
    // `auto` to weigh the bytes this query will read (a warm slice of a cold file still maps).
    const plan_io = choose_io and io_policy.profile != .mmap;
    var probed = opened.membership;
    // A `row_groups` range lives in the footers parsed here; the scan's own parse would read every row group.
    const metas: ?[]const schema.FileMetaData = if (explain or empty_result or opened.has_preparsed_meta or plan_io or
        steer_metas != null or args.row_groups != null or args.plan_source != null)
    blk: {
        const m = steer_metas orelse try materializeMetas(arena, opened, args.diag, prof);
        const filter: ?filter_ast.Filter = if (try args.boundAggregate(arena, &m[0])) |bound| bound.filter else if (args.filter) |fs|
            (if (fs.len > 0) try bindFilter(arena, fs, &m[0], args) else null)
        else
            null;
        if (!explain and !empty_result) probed.add(try loadLocalMembership(arena, opened, m, filter, args.scan_all, false));
        break :blk m;
    } else null;
    const local_plan: ?LocalPlan = if (plan_io) try planLocalReads(arena, args, opened, metas.?) else null;
    const resident: f64 = if (choose_io and io_policy.profile == .auto)
        plannedResidency(ctx.gpa, opened, local_plan)
    else
        0;
    const coverage: f64 = if (local_plan) |lp| lp.coverage() else 0;
    // A row group the reader would hold past half the query's ledger is read through the mapping, whose pages are
    // the page cache's, not the query's.
    const fits_reader = if (local_plan) |lp| lp.largest <= args.max_memory / 2 else true;
    var choice: local_reader.Choice = if (!choose_io)
        .{ .mode = .mmap, .reason = if (all_local) .mapping else .remote }
    else
        io_policy.choose(resident, coverage);
    choice = choice.withinReader(fits_reader);
    const io_mode = choice.mode;
    profile.record(prof, .io_choice, nowMonoNs(), nowMonoNs(), profile.args(.{
        io_policy.profile,
        io_mode,
        @as(u64, @intFromFloat(resident * 1000)),
        @as(u64, if (local_plan != null) @intFromFloat(coverage * 1000) else std.math.maxInt(u32)),
        choice.reason,
    }));
    var scan_args = aggregateScanArgs(ctx, args, agg_str, opened.inputs, metas, cut_args.result_order, &ledger);
    if (args.plan_source != null) scan_args.bound_aggregate = try args.boundAggregate(arena, &metas.?[0]);
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
            .io_reason = choice.reason,
            .encoded = scan_args.encoded,
            .max_memory = args.max_memory,
            .sketch = scan_args.sketch_topk,
            .result = .of(args.result_order),
            .conjuncts = blk: {
                const ms = metas orelse break :blk &.{};
                if (ms.len == 0) break :blk &.{};
                const filter = if (scan_args.bound_aggregate) |bound| bound.filter else if (args.filter) |text|
                    try filter_parser.parse(arena, text, &ms[0], null)
                else
                    null;
                break :blk try explainConjuncts(arena, filter, &ms[0]);
            },
        };
    }
    // Declared after `opened`, so it is torn down (its threads joined) before the mappings go.
    var reader: ?*local_reader.Reader = null;
    // Off Linux the reader never exists (`on_linux`), and its teardown's futex and close must not be compiled in.
    defer if (on_linux) if (reader) |rd| rd.destroy();
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
            .ledger = &ledger,
            .direct = choice.directRead(),
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
        scan_args.start_reads = .{
            .ctx = &starter,
            .start = ReadStarter.start,
            .abandon = ReadStarter.abandon,
            .planned = ReadStarter.planned,
        };
    }
    scan_args.inputs = scan_inputs;
    var probe_sink: DictProbeSink = .{ .inputs = scan_inputs, .mmaps = opened.mmaps };
    if (args.kind() == .grouped and allLocal(opened)) {
        probe_sink.cold = try arena.alloc(DictProbeSink.Cold, scan_inputs.len);
        @memset(probe_sink.cold, @fromBackingInt(@intCast(0)));
        scan_args.dict_probe = .{ .ctx = &probe_sink, .add = DictProbeSink.add };
    }
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
    const text_kinds = try answerTextKinds(ctx.gpa, r.output, r.state, args.output == null, args.diag);
    errdefer ctx.gpa.free(text_kinds);
    const answer_types = try answerTypes(ctx.gpa, r.output, r.state);
    errdefer ctx.gpa.free(answer_types);
    // Without GROUP BY, the aggregates shown: the hidden HAVING terms follow them.
    const n_aggs = if (r.output.group_rows == null) r.visible_cols orelse r.state.agg_calls.len else r.state.agg_calls.len;

    // 3. Optional 1-row parquet output. Aggregate output is local-only;
    //    S3 writes use the row-group streaming path.
    var bytes_out = try writeAggregateParquet(ctx, arena, args, &r, n_aggs, &ledger, &t);

    if (args.columnar_sink) |cs| {
        try exportAggregateColumns(ctx.gpa, arena, cs, r, n_aggs, args.empty_scalar);
        releaseStreamAnswer(&r.output);
    }

    // Text used to print only after runQuery returned: preserve its input/error check before the first byte.
    try opened.intact();
    if (args.output == null) if (args.text_output) |text| {
        t.output_started_ns = nowMonoNs();
        var writer: StdoutWriter = .{ .fd = text.fd, .sink = text.sink };
        const rows = if (args.empty_scalar) 0 else r.output.rowCount();
        try writeAggregateText(&writer, r.output, text_kinds, answer_types, rows, text.format);
        try writer.flush();
        bytes_out = writer.bytes_written;
        // Both stream states hold the query's ledger, and their bound outputs borrow this arena.
        releaseStreamAnswer(&r.output);
    };
    var answer: AggResult = .{
        .files_in = r.files_in,
        .rows_in = r.rows_in,
        .rows_scanned = r.rows_scanned,
        .rows_matched = r.rows_matched,
        .bytes_in = r.bytes_in,
        .bytes_out = bytes_out,
        .row_groups_in = r.row_groups_in,
        .row_groups_pruned = r.row_groups_pruned,
        .row_groups_full_match = r.row_groups_full_match,
        .cols_stat_pruned = r.cols_stat_pruned,
        .membership = probed,
        .timings = t,
        .output = r.output,
        .text_kinds = text_kinds,
        .answer_types = answer_types,
    };
    if (args.output == null) if (args.aggregate_sink) |sink| {
        try sink.write_fn(sink.ctx, answer);
        releaseStreamAnswer(&answer.output);
    };
    // The returned arenas live with AggResult; this invocation's ledger is stack-local.
    answer.output.detachArenaLedger();
    return answer;
}

// Keep this setup in its caller, as before extraction, so the scan's options need no outlined argument handoff.
inline fn aggregateScanArgs(
    ctx: Context,
    args: QueryArgs,
    agg_str: []const u8,
    inputs: []const scan.Input,
    metas: ?[]const schema.FileMetaData,
    order: expr_ast.ResultOrder,
    ledger: *core_memory.SharedBudget,
) scan.MultiAggArgs {
    const decode_rows_env = ctx.env.getPosix("ZPQ_DECODE_BATCH_ROWS");
    var scan_args: scan.MultiAggArgs = .{
        .inputs = inputs,
        .metas = metas,
        .filter = args.filter,
        .aggregate = agg_str,
        .group_by = args.group_by,
        .parallelism = args.parallelism,
        .scan_all = args.scan_all,
        .trust_stats = args.trust_stats,
        // Aggregate consumers need presence, not an expanded level array when the encoded run proves every row
        // present. The forced legacy path is for cross-checks; dictionary consumers already make this proof.
        .fast_levels = args.fast_levels or !envDisables(ctx.env, "ZPQ_LEVEL_RUNS"),
        .level_runs = blk: {
            const v = ctx.env.getPosix("ZPQ_LEVEL_RUNS") orelse break :blk null;
            if (std.mem.eql(u8, v, "0")) break :blk false;
            break :blk if (std.mem.eql(u8, v, "1")) true else null;
        },
        .decode_batch_rows = if (decode_rows_env) |v|
            std.fmt.parseInt(usize, v, 10) catch 0
        else
            0,
        .auto_decode_batches = decode_rows_env == null,
        .dependent_keys = !envDisables(ctx.env, "ZPQ_GROUP_KEY_FD"),
        .expr_batch_rows = if (ctx.env.getPosix("ZPQ_EXPR_BATCH_ROWS")) |v|
            std.fmt.parseInt(usize, v, 10) catch null
        else
            null,
        .encoded = args.encoded and !envDisables(ctx.env, "ZPQ_ENCODED"),
        .leave_regions = ctx.exit_after,
        .unordered = args.unordered,
        .select_cols = args.select_cols,
        .result_order = order,
        .typed_answer = args.typed_answer and args.output != null,
        .record_answer = (args.text_output != null or args.aggregate_sink != null) and args.output == null,
        .integer_records = !envDisables(ctx.env, "ZPQ_INTEGER_RECORDS"),
        .sorted_count_stream = !envDisables(ctx.env, "ZPQ_SORTED_COUNT_STREAM"),
        .msd_records = !envDisables(ctx.env, "ZPQ_MSD_RECORDS"),
        .msd_cache_rows = if (ctx.env.getPosix("ZPQ_MSD_CACHE_ROWS")) |v|
            std.fmt.parseInt(usize, v, 10) catch null
        else
            null,
        .byte_records = !envDisables(ctx.env, "ZPQ_BYTE_RECORDS"),
        .byte_top_partitions = if (ctx.env.getPosix("ZPQ_BYTE_TOP_PARTITIONS")) |v|
            std.fmt.parseInt(usize, v, 10) catch null
        else
            null,
        .byte_top_batch_rows = if (ctx.env.getPosix("ZPQ_BYTE_TOP_BATCH_ROWS")) |v|
            std.fmt.parseInt(usize, v, 10) catch null
        else
            null,
        .column_order = args.column_order,
        .max_memory = args.max_memory,
        .ledger = ledger,
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
    return scan_args;
}

fn writeAggregateParquet(
    ctx: Context,
    arena: std.mem.Allocator,
    args: QueryArgs,
    r: *scan.MultiAggResult,
    n_aggs: usize,
    ledger: *core_memory.SharedBudget,
    t: *Timings,
) !u64 {
    const prof = profile.threadOf(args.profiler, .main);
    var bytes_out: u64 = 0;
    if (args.output) |out_path| {
        if (std.mem.startsWith(u8, out_path, "s3://")) {
            return error.BadOutputUrl;
        }
        const t_footer = nowMonoNs();
        t.output_started_ns = t_footer;
        const st = r.state;
        const codec = args.codec orelse .SNAPPY;
        const zstd: compression.ZstdPlan = .{ .lambda = args.compress_priority.lambda() };
        // A big answer's columns encode on the query's workers, with a quarter of its budget for those in flight, as a
        // re-encode's output in flight takes, drawn from the ledger beside the answer, within the slack a GROUP BY's
        // merge and answer have past its limit.
        const encode: consumer.AggEncode = .{
            .parquet_version = args.parquet_version,
            .workers = if (args.parallelism > 0) args.parallelism else system.discoverAvailableParallelism(ctx.env),
            .budget = args.max_memory / 4,
            .ledger = ledger,
            .dictionary_bloom = args.bloom_filters_enabled,
        };
        ledger.limit += core_memory.finalize_allowance;
        defer ledger.limit -= core_memory.finalize_allowance;
        const names = r.output.group_cols;
        const fd = args.output_fd;
        bytes_out = if (r.output.integer_stream) |stream|
            try writeIntegerStream(
                arena,
                out_path,
                names.?,
                st.group_col_types.?,
                stream,
                codec,
                zstd,
                encode,
                fd,
                args.profiler,
            )
        else if (r.output.byte_stream) |stream|
            try writeByteStream(arena, out_path, st.group_col_types.?, stream, codec, zstd, encode, fd, args.profiler)
        else if (r.output.typed) |typed|
            try writeTypedParquet(arena, out_path, names.?, st.group_col_types.?, typed, codec, zstd, encode, fd)
        else if (r.output.group_rows) |rows|
            try writeGroupedParquet(arena, out_path, names.?, st.group_col_types.?, rows, codec, zstd, encode, fd)
        else
            try writeOneRowParquet(arena, out_path, st.agg_calls[0..n_aggs], st.accumulators[0..n_aggs], st.agg_sources[0..n_aggs], codec, zstd, encode, args.output_fd, args.empty_scalar or r.output.no_row);
        releaseStreamAnswer(&r.output);
        const t_footer_end = nowMonoNs();
        t.footer_ns += @intCast(t_footer_end - t_footer);
        profile.record(prof, .footer_write, t_footer, t_footer_end, profile.args(.{bytes_out}));
    }
    return bytes_out;
}

fn exportAggregateColumns(gpa: std.mem.Allocator, arena: std.mem.Allocator, cs: ColumnarSink, r: anytype, n_aggs: usize, empty: bool) !void {
    const out = r.output;
    if (out.integer_stream) |stream| {
        var lo: usize = 0;
        while (true) {
            var window = std.heap.ArenaAllocator.init(gpa);
            defer window.deinit();
            const hi = @min(stream.state.group_count, lo + consumer.answer_rg_rows);
            const a = try stream.window(window.allocator(), r.state.group_col_types.?, lo, hi, 1);
            const cols = try typedColumns(window.allocator(), out.group_cols.?, r.state.group_col_types.?, a);
            if (lo == 0) try cs.schema_fn(cs.ctx, cols.items);
            if (hi > lo) try cs.batch_fn(cs.ctx, cols.items, 0, hi - lo);
            lo = hi;
            if (lo == stream.state.group_count) return;
        }
    }
    if (out.byte_stream) |stream| {
        var lo: usize = 0;
        while (true) {
            var window = std.heap.ArenaAllocator.init(gpa);
            defer window.deinit();
            const hi = @min(stream.state.group_count, lo + consumer.answer_rg_rows);
            const a = try stream.window(window.allocator(), lo, hi, 1);
            for (r.state.group_col_types.?, a.columns) |ty, *col| if (ty.source) |src| {
                col.* = try retypeOutputCol(window.allocator(), col.*, src);
            };
            if (lo == 0) try cs.schema_fn(cs.ctx, a.columns);
            if (hi > lo) try cs.batch_fn(cs.ctx, a.columns, 0, hi - lo);
            lo = hi;
            if (lo == stream.state.group_count) return;
        }
    }
    var cols: std.ArrayList(expr_agg.OutputCol) = .empty;
    var rows: usize = 0;
    if (out.typed) |typed| {
        cols = try typedColumns(arena, out.group_cols.?, r.state.group_col_types.?, typed);
        rows = typed.rows;
    } else if (out.group_rows) |group_rows| {
        cols = try groupedColumns(arena, out.group_cols.?, r.state.group_col_types.?, group_rows);
        rows = group_rows.len;
    } else {
        for (r.state.agg_calls[0..n_aggs], 0..) |call, i| {
            for (try expr_agg.finalize(arena, call, r.state.accumulators[i])) |col| {
                try cols.append(arena, if (r.state.agg_sources[i]) |src| try retypeOutputCol(arena, col, src) else col);
            }
        }
        rows = if (empty or out.no_row) 0 else 1;
    }
    try cs.schema_fn(cs.ctx, cols.items);
    var lo: usize = 0;
    while (lo < rows) {
        const hi = @min(rows, lo + consumer.answer_rg_rows);
        try cs.batch_fn(cs.ctx, cols.items, lo, hi);
        lo = hi;
    }
}

test "aggregate consumers conflict before input opening or output in every build mode" {
    const a = std.testing.allocator;
    const ctx: Context = .{ .gpa = a, .env = .empty, .io = std.testing.io };
    var buf: [8]u8 = undefined;
    var writer: std.Io.Writer = .fixed(&buf);
    var called = false;
    const Callback = struct {
        fn write(raw: *anyopaque, _: AggResult) !void {
            const flag: *bool = @ptrCast(@alignCast(raw));
            flag.* = true;
        }
    };
    const args: QueryArgs = .{
        .inputs = &.{"missing-must-not-open.parquet"},
        .aggregate = "count(*) AS c",
        .text_output = .{ .format = .json, .sink = &writer },
        .aggregate_sink = .{ .ctx = &called, .write_fn = Callback.write },
    };
    try std.testing.expectError(error.ConflictingAggregateSinks, runQuery(ctx, args));
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    try std.testing.expectError(error.ConflictingAggregateSinks, explainAggregate(ctx, arena.allocator(), args));
    try std.testing.expect(!called);
    try std.testing.expectEqual(@as(usize, 0), writer.buffered().len);
}

test "aggregate drains retain input-change precedence over a later write error" {
    const a = std.testing.allocator;
    const bytes = try metadata.readFileSlice("ci/fixtures/parquet/full_match.parquet", a);
    defer a.free(bytes);
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var path_buf: [128]u8 = undefined;
    const path = try std.fmt.bufPrint(&path_buf, ".zig-cache/tmp/{s}/changed.parquet", .{tmp.sub_path});
    const Callback = struct {
        path: []const u8,
        failure: anyerror,
        called: bool = false,
        fn write(raw: *anyopaque, _: AggResult) !void {
            const c: *@This() = @ptrCast(@alignCast(raw));
            const fd = try local_fs.createFile(c.path);
            local_fs.close(fd);
            c.called = true;
            return c.failure;
        }
    };
    const ctx: Context = .{ .gpa = a, .env = .empty, .io = std.testing.io };
    for ([_]anyerror{ error.BrokenPipe, error.NoSpaceLeft, error.WriteFailed }) |failure| {
        const fd = try local_fs.createFile(path);
        try local_fs.writeAll(fd, bytes);
        local_fs.close(fd);
        var callback: Callback = .{ .path = path, .failure = failure };
        try std.testing.expectError(error.InputChanged, runQuery(ctx, .{
            .inputs = &.{path},
            .aggregate = "count(*) AS c",
            .aggregate_sink = .{ .ctx = &callback, .write_fn = Callback.write },
        }));
        try std.testing.expect(callback.called);
    }
}

test "released group metadata never emits a scalar phantom row" {
    const answer: scan.AggOutput = .{
        .group_cols = &.{ "k", "c", "s" },
        .typed = .{ .rows = 5, .columns = &.{} },
    };
    try std.testing.expectEqual(@as(usize, 5), answer.rowCount());
    const formats = [_]PrintFormat{ .json, .jsonl, .csv };
    const expected = [_][]const u8{ "[]\n", "", "k,c,s\n" };
    for (formats, expected) |format, want| {
        var buf: [64]u8 = undefined;
        var writer: std.Io.Writer = .fixed(&buf);
        try writeAggregateText(&writer, answer, &.{}, &.{}, answer.rowCount(), format);
        try std.testing.expectEqualStrings(want, writer.buffered());
    }
}

// ============================================================
// Compile-time API check.
// ============================================================
test "engine: API is well-typed" {
    _ = runQuery;
    _ = runPrint;
}
