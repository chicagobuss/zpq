const scan = @import("../core/scan.zig");
const expr_agg = @import("../core/expr/agg.zig");
const thrift = @import("../core/thrift.zig");
const huge_pages = @import("../core/huge_pages.zig");
const decimal_mod = @import("../core/parquet/decimal.zig");
const invariant = @import("../core/invariant.zig");
const std = @import("std");
const nowMonoNs = @import("../clock.zig").monoNs;
const profile = @import("../profile.zig");
const local_fs = @import("../local_fs.zig");
const schema = @import("../core/schema.zig");
const schema_tree = @import("../core/parquet/schema_tree.zig");
const metadata = @import("../core/parquet/metadata.zig");
const compression = @import("../core/parquet/compression.zig");
const consumer = @import("../core/consumer.zig");
const core_memory = @import("../core/memory.zig");
const filter_ast = @import("../core/filter/ast.zig");
const filter_eval = @import("../core/filter/eval.zig");
const filter_prune = @import("../core/filter/prune.zig");
const expr_ast = @import("../core/expr/ast.zig");
const membership = @import("../core/filter/membership.zig");
const nanprobe = @import("../core/filter/nanprobe.zig");
const encoder = @import("../core/writer/encoder.zig");
const streaming = @import("../core/writer/streaming.zig");
const fastpath = @import("../core/writer/fastpath.zig");
const page_index = @import("../core/writer/page_index.zig");
const s3 = @import("../io/s3.zig");
const tls = @import("../io/tls.zig");
const multipart_sink = @import("../io/multipart_sink.zig");
const Diag = @import("../core/diag.zig").Diag;
const engine_types = @import("types.zig");
const Context = engine_types.Context;
const QueryArgs = engine_types.QueryArgs;
const WriteResult = engine_types.WriteResult;
const Timings = engine_types.Timings;
const POOL_SIZE = engine_types.POOL_SIZE;
const OutputLane = engine_types.OutputLane;
const projection = @import("projection.zig");
const ProjectionPlan = projection.ProjectionPlan;
const planProjection = projection.planProjection;
const input = @import("input.zig");
const openInputs = input.openInputs;
const materializeMetas = input.materializeMetas;
const loadLocalMembership = input.loadLocalMembership;
const credentialsFromEnv = input.credentialsFromEnv;
const s3_fetch = @import("s3_fetch.zig");
const s3FetchWindow = s3_fetch.s3FetchWindow;
const PAR1 = s3_fetch.PAR1;
const window_mod = @import("write_window.zig");
const ENCODE_REGION_RESERVE = window_mod.ENCODE_REGION_RESERVE;
const WorkRegions = window_mod.WorkRegions;
const RGJob = window_mod.RGJob;
const skipRowGroup = window_mod.skipRowGroup;
const Slot = window_mod.Slot;
const WinCtx = window_mod.WinCtx;
const runWindowedReencode = window_mod.runWindowedReencode;
const runWindowed = window_mod.runWindowed;
const RecompressCtx = window_mod.RecompressCtx;
const RecompressSlot = window_mod.RecompressSlot;
const writeWindow = window_mod.writeWindow;
const anyChunkNeedsRecompress = window_mod.anyChunkNeedsRecompress;
const keptColumnsUseCodec = window_mod.keptColumnsUseCodec;

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
            .passthrough => |pt| {
                const ci = pt.ci;
                var walk: schema.LeafIterator = undefined;
                const leaf = meta0.leafAt(ci, &walk) orelse return error.UnknownColumn;
                const elem = leaf.element.*;
                const flba_decimal = elem.type == .FIXED_LEN_BYTE_ARRAY and decimal_mod.kindFromSchema(&elem) != null;
                if (elem.type == .BOOLEAN or elem.type == .INT96 or leaf.max_rep > 0 or flba_decimal) {
                    Diag.setColumn(args.diag, spec.column);
                    return error.BloomFilterUnsupportedColumn;
                }
                leaves[ci] = spec.fpp;
            },
            .computed => |c| if (c.expr.isBoolean()) {
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

pub fn runWrite(ctx: Context, args: QueryArgs, out_path: []const u8) !WriteResult {
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
    consumer.setDecodeFailure(null);
    errdefer consumer.resolveDecodeFailure(args.diag, metas, opened.inputs);
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
    probed.add(try loadLocalMembership(arena, opened, metas, filter_opt, args.scan_all, true));
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
        sink = fd_sink.sink();
    }

    // 5. Stream output. PAR1 magic, then per-file × per-RG
    //    encodeRG/copyRG, then footer.
    var out_offset: u64 = 0;
    t.output_started_ns = nowMonoNs();
    try sink.write(&PAR1);
    out_offset += PAR1.len;

    var rows_in: i64 = 0;
    var rows_pruned: i64 = 0;
    var rows_kept: i64 = 0;
    var rgs_in: usize = 0;
    var rgs_pruned: usize = 0;
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
            .passthrough => |pt| {
                const ci = pt.ci;
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
    var page_indexes: page_index.Deferred = .{};

    if (args.diag) |diag| diag.execution_started = true;
    const t_scan = nowMonoNs();
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
                .worker_io = ctx.worker_io,
                .win_unit = sizing.unit,
                .win_units = sizing.units,
                .gpa = ctx.gpa,
                .jobs = jobs.items,
                .kept = kept_opt,
                .codec = args.codec.?,
                .zstd = zstd,
                .bloom_leaves = blooms.leaves,
                .dictionary_bloom = args.bloom_filters_enabled,
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
                const bloom_write: consumer.BloomWrite = .{
                    .fpp = blooms.leaves,
                    .meta = &meta_i,
                    .scratch = ctx.gpa,
                    .dictionary_bloom = args.bloom_filters_enabled,
                };
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
                        rgs_pruned += 1;
                        rows_pruned += src_rg.num_rows;
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

            // The write's ledger: the producers' decode and encode memory, the row groups waiting for the writer and
            // its slices hold no more than the budget together, past each producer's first huge page.
            var ledger: core_memory.SharedBudget = .{ .limit = args.max_memory };
            defer if (args.profiler) |p| p.noteLedger(ledger.peak.load(.monotonic), args.max_memory);
            var memory: core_memory.Charged = .{ .child = ctx.gpa, .budget = &ledger };
            var slicing: core_memory.Charged = .{ .child = std.heap.page_allocator, .budget = &ledger };
            var lean_ledger: core_memory.SharedBudget = .{ .limit = core_memory.lean_decode };
            var lean: core_memory.Charged = .{ .child = std.heap.page_allocator, .budget = &lean_ledger };
            const regions = try arena.alloc(WorkRegions, @max(effective_async_budget, 1) + sizing.budget + 1);
            for (regions) |*r| {
                r.* = .init(ctx.gpa);
                r.rows.budget = &ledger;
                r.work.budget = &ledger;
                r.rows.allowance = huge_pages.huge_page_size;
            }
            defer for (regions) |*r| r.deinit();
            var window_sem: std.Io.Semaphore = .{ .permits = sizing.units };
            var completions_sem: std.Io.Semaphore = .{ .permits = 0 };
            const early_arr = try arena.alloc(bool, fetch_arr.len);
            @memset(early_arr, false);
            if (filter_opt) |f| filter_ast.markColumns(f, early_arr);
            for (output_specs) |spec| if (spec == .computed) spec.computed.expr.collectColumns(early_arr);
            // Every piece's aggregator has these leaves; the row groups cut from them are written under them.
            const out_agg = try consumer.initOutputAggregator(arena, meta0, output_specs);
            var w = WinCtx{
                .worker_io = ctx.worker_io,
                .rg_target = args.row_group.under(args.max_memory_explicit),
                .early_arr = early_arr,
                .out_elems = out_agg.schema_elems,
                .out_paths = out_agg.paths,
                .page_indexes = &page_indexes,
                .memory = &memory,
                .slicing = &slicing,
                .lean = &lean,
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
                .parquet_version = args.parquet_version,
                .decode_options = .{ .fast_levels = args.fast_levels, .expand_limit = args.max_memory },
                .bloom_outputs = blooms.outputs,
                .bloom_leaves = blooms.leaves,
                .dictionary_bloom = args.bloom_filters_enabled,
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
                // Leave room for the executor beside whole producer pieces; wide input need not take the serial
                // slice path merely because few input groups are available. Output boundaries remain unchanged.
                .decode_share = args.max_memory / 2 / @max(1, effective_async_budget),
            };
            // Set once, before any producer: the pieces' arenas hold these addresses until they are freed.
            w.cpu_budget = sizing.budget;
            w.piece_pool = .{ .gpa = ctx.gpa, .rows_alloc = w.pieceAlloc(), .ledger = &ledger };
            defer w.piece_pool.deinit();
            defer consumer.releaseSlicePool(&w.slice_pool);
            w.piece_pages.child = w.outAlloc();
            w.slicing_pages.child = slicing.allocator();
            w.lean_pages.child = lean.allocator();
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

    // 6. Page indexes of the re-encoded chunks and Bloom filters after the row groups, then the footer.
    const t_footer = nowMonoNs();
    t.decode_wall_ns = @intCast(t_footer - t_scan);
    const footer_span = profile.Span.begin(prof);
    {
        var so: streaming.SinkOut = .{ .sink = sink, .offset = @intCast(out_offset) };
        try page_indexes.write(arena, &so, new_row_groups.items);
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
    // Encoded column data makes the file zpq's (row groups copied beside it keep only bounds no writer quirk
    // reads differently); a pure byte copy or recompression stays its source's.
    if (need_encoder) for (new_row_groups.items) |*rg| for (rg.columns.items) |*c| schema.dropForeignDeprecatedBounds(c);
    const new_meta: schema.FileMetaData = .{
        .version = args.parquet_version.footer(),
        .schema = new_schema_items,
        .num_rows = rows_kept,
        .created_by = if (need_encoder) schema.ZPQ_CREATED_BY else meta0.created_by,
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
    std.mem.writeInt(u32, &len_bytes, std.math.cast(u32, footer_bytes.len) orelse return error.TooLarge, .little);
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
        .rows_scanned = rows_in - rows_pruned,
        .rows_matched = rows_kept,
        .bytes_in = bytes_in,
        .bytes_out = out_offset,
        .row_groups_in = rgs_in,
        .row_groups_kept = rg_kept,
        .row_groups_pruned = rgs_pruned,
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
pub fn coerceByteArrayDecimalLeavesToFlba(items: *std.ArrayListUnmanaged(schema.SchemaElement)) void {
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
pub fn coerceFloat16LeavesToDouble(items: *std.ArrayListUnmanaged(schema.SchemaElement)) void {
    for (items.items) |*elem| {
        const nc = elem.num_children orelse 0;
        if (nc != 0 or !schema.isFloat16(elem.*)) continue;
        elem.type = .DOUBLE;
        elem.type_length = null;
        elem.converted_type = null;
        elem.logical_type = null;
        elem.logical_type_raw = &.{};
        elem.scale = null;
        elem.precision = null;
    }
}

pub fn buildSelectSchema(
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
            .passthrough => |pt| {
                const elem = metadata.leafSchemaElement(meta, pt.ci) orelse return error.SchemaMismatch;
                var copy = elem;
                copy.num_children = 0;
                if (pt.name) |name| {
                    const path = (try metadata.leafPathSegments(arena, meta, pt.ci)) orelse return error.SchemaMismatch;
                    consumer.renameLeaf(&copy, name, meta.getColumnLevels(path).max_def);
                }
                try out.append(arena, copy);
            },
            .computed => |c| try out.append(arena, consumer.computedSchemaElem(c)),
        }
    }

    return out;
}


pub const FdSink = struct {
    fd: local_fs.fd_t,
    written: u64 = 0,
    writeback_to: u64 = 0,

    pub fn sink(self: *FdSink) streaming.Sink {
        return .{ .ctx = self, .write_fn = writeFn, .write_segments_fn = writeSegmentsFn };
    }

    pub fn writeFn(ctx: *anyopaque, bytes: []const u8) anyerror!void {
        const self: *FdSink = @ptrCast(@alignCast(ctx));
        try local_fs.writeAll(self.fd, bytes);
        try self.advance(bytes.len);
    }

    pub fn writeSegmentsFn(ctx: *anyopaque, segments: []const []const u8) anyerror!void {
        const self: *FdSink = @ptrCast(@alignCast(ctx));
        var item: usize = 0;
        var offset: usize = 0;
        while (item < segments.len) {
            var batch: [64][]const u8 = undefined;
            var count: usize = 0;
            var bytes: usize = 0;
            const room = (64 << 20) - (self.written - self.writeback_to);
            while (item < segments.len and count < batch.len and bytes < room) {
                const segment = segments[item][offset..];
                const take = @min(segment.len, room - bytes);
                if (take != 0) {
                    batch[count] = segment[0..take];
                    count += 1;
                    bytes += take;
                }
                if (take == segment.len) {
                    item += 1;
                    offset = 0;
                } else offset += take;
            }
            try local_fs.writeSegments(self.fd, batch[0..count]);
            try self.advance(bytes);
        }
    }

    fn advance(self: *FdSink, bytes: usize) !void {
        self.written += bytes;
        if (self.written - self.writeback_to >= 64 << 20) {
            try local_fs.startWriteback(self.fd, self.writeback_to, self.written - self.writeback_to);
            self.writeback_to = self.written;
        }
    }
};

/// Keeps the engine's `OpenFailed` / `PathTooLong` contract over `local_fs.createFile`'s detailed errors.
pub fn createFile(path: []const u8) !local_fs.fd_t {
    return local_fs.createFile(path) catch |err| switch (err) {
        error.NameTooLong => error.PathTooLong,
        else => error.OpenFailed,
    };
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


test "FdSink segments keep one contiguous writeback range across page and vector boundaries" {
    if (@import("builtin").os.tag != .linux) return error.SkipZigTest;
    const fd = try std.posix.openat(std.posix.AT.FDCWD, "/dev/null", .{ .ACCMODE = .WRONLY }, 0);
    defer local_fs.close(fd);
    var buffer: [1 << 20]u8 = @splat(0x5a);
    var segments: [130][]const u8 = @splat(&buffer);
    segments[0] = "";
    segments[64] = "";
    var sink: FdSink = .{ .fd = fd };
    try FdSink.writeFn(&sink, "header");
    try FdSink.writeSegmentsFn(&sink, &segments);
    try std.testing.expectEqual(@as(u64, (128 << 20) + 6), sink.written);
    try std.testing.expectEqual(@as(u64, 128 << 20), sink.writeback_to);
    try FdSink.writeFn(&sink, "footer");
    try std.testing.expectEqual(@as(u64, (128 << 20) + 12), sink.written);
    try std.testing.expectEqual(@as(u64, 128 << 20), sink.writeback_to);
}


// ============================================================
// 1-row aggregate output (local files)
// ============================================================

/// Write a 1-row aggregate parquet to a local path. Kept in the engine so
/// both binaries can call it.

pub fn writeOneRowParquet(
    arena: std.mem.Allocator,
    out_path: []const u8,
    agg_calls: []const expr_agg.AggCall,
    accumulators: []const expr_agg.Accumulator,
    sources: []const ?schema.SchemaElement,
    codec: schema.CompressionCodec,
    zstd: compression.ZstdPlan,
    encode: consumer.AggEncode,
    output_fd: ?local_fs.fd_t,
    empty: bool,
) !u64 {
    const fd = output_fd orelse try createFile(out_path);
    var fd_open = output_fd == null;
    defer if (fd_open) local_fs.close(fd);
    errdefer if (output_fd == null) local_fs.removeFile(out_path);
    var fd_sink: FdSink = .{ .fd = fd };
    const sink: streaming.Sink = fd_sink.sink();

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
    const written = try consumer.writeAggregateRows(arena, sink, all_outs.items, if (empty) 0 else 1, codec, zstd, encode);
    if (fd_open) {
        fd_open = false;
        try local_fs.closeChecked(fd);
    }
    return written;
}

/// Write GROUP BY results as parquet: one row per group, columns in `cols` order, values exactly as the JSON output
/// reports them. avg_state splits into `<name>__sum` / `<name>__count` as in the 1-row output.
pub fn groupedColumns(arena: std.mem.Allocator, cols: []const []const u8, types: []const scan.GroupColType, rows: []const []const scan.AggValue) !std.ArrayList(expr_agg.OutputCol) {
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

    return outs;
}

pub fn writeGroupedParquet(
    arena: std.mem.Allocator,
    out_path: []const u8,
    cols: []const []const u8,
    types: []const scan.GroupColType,
    rows: []const []const scan.AggValue,
    codec: schema.CompressionCodec,
    zstd: compression.ZstdPlan,
    encode: consumer.AggEncode,
    output_fd: ?local_fs.fd_t,
) !u64 {
    const outs = try groupedColumns(arena, cols, types, rows);
    const fd = output_fd orelse try createFile(out_path);
    var fd_open = output_fd == null;
    defer if (fd_open) local_fs.close(fd);
    var fd_sink: FdSink = .{ .fd = fd };
    const sink: streaming.Sink = fd_sink.sink();
    const written = try consumer.writeAggregateRows(arena, sink, outs.items, @intCast(rows.len), codec, zstd, encode);
    if (fd_open) {
        fd_open = false;
        try local_fs.closeChecked(fd);
    }
    return written;
}

/// `writeGroupedParquet` for an answer built as typed columns (`scan.TypedAnswer`): the same columns, names, types and
/// file, with no row of values in between.
pub fn typedColumns(arena: std.mem.Allocator, cols: []const []const u8, types: []const scan.GroupColType, answer: scan.TypedAnswer) !std.ArrayList(expr_agg.OutputCol) {
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
    return outs;
}

pub fn writeTypedParquet(
    arena: std.mem.Allocator,
    out_path: []const u8,
    cols: []const []const u8,
    types: []const scan.GroupColType,
    answer: scan.TypedAnswer,
    codec: schema.CompressionCodec,
    zstd: compression.ZstdPlan,
    encode: consumer.AggEncode,
    output_fd: ?local_fs.fd_t,
) !u64 {
    const outs = try typedColumns(arena, cols, types, answer);
    const fd = output_fd orelse try createFile(out_path);
    var fd_open = output_fd == null;
    defer if (fd_open) local_fs.close(fd);
    var fd_sink: FdSink = .{ .fd = fd };
    const sink: streaming.Sink = fd_sink.sink();
    const rows: i64 = @intCast(answer.rows);
    const written = try consumer.writeAggregateRows(arena, sink, outs.items, rows, codec, zstd, encode);
    if (fd_open) {
        fd_open = false;
        try local_fs.closeChecked(fd);
    }
    return written;
}

pub fn writeIntegerStream(
    arena: std.mem.Allocator,
    out_path: []const u8,
    names: []const []const u8,
    types: []const scan.GroupColType,
    stream: scan.IntegerStream,
    codec: schema.CompressionCodec,
    zstd: compression.ZstdPlan,
    encode: consumer.AggEncode,
    output_fd: ?local_fs.fd_t,
    profiler: ?*profile.Profiler,
) !u64 {
    const fd = output_fd orelse try createFile(out_path);
    var fd_open = output_fd == null;
    defer if (fd_open) local_fs.close(fd);
    var fd_sink: FdSink = .{ .fd = fd };
    var file = try consumer.AggregateFile.init(arena, fd_sink.sink());
    defer file.deinit();
    try file.reserveIntegerWorkspace(encode);
    var window = std.heap.ArenaAllocator.init(std.heap.page_allocator);
    defer window.deinit();
    const cols = try arena.alloc(expr_agg.OutputCol, names.len);
    const total = stream.state.group_count;
    var lo: usize = 0;
    while (lo < total or (lo == 0 and total == 0)) {
        const hi = @min(total, lo + 4 * consumer.answer_rg_rows);
        const a = window.allocator();
        const columns_span = profile.Span.begin(profile.threadOf(profiler, .main));
        const answer = try stream.window(a, types, lo, hi, encode.workers);
        columns_span.end(.integer_columns, profile.args(.{}));
        for (names, types, answer.columns, cols) |name, ty, col, *out| {
            const plain = typedColumn([]i64, name, ty.nullable, col.i64, .INT64);
            out.* = if (ty.source) |src| try retypeOutputCol(arena, plain, src) else plain;
            out.distinct = ty.source != null;
        }
        try file.append(cols, hi - lo, codec, zstd, encode);
        lo = hi;
        if (lo == total) {
            const written = try file.finish(cols);
            if (fd_open) {
                fd_open = false;
                try local_fs.closeChecked(fd);
            }
            return written;
        }
        _ = window.reset(.retain_capacity);
    }
    unreachable;
}

pub fn writeByteStream(
    arena: std.mem.Allocator,
    out_path: []const u8,
    types: []const scan.GroupColType,
    stream: scan.ByteStream,
    codec: schema.CompressionCodec,
    zstd: compression.ZstdPlan,
    encode: consumer.AggEncode,
    output_fd: ?local_fs.fd_t,
    profiler: ?*profile.Profiler,
) !u64 {
    const fd = output_fd orelse try createFile(out_path);
    var fd_open = output_fd == null;
    defer if (fd_open) local_fs.close(fd);
    var fd_sink: FdSink = .{ .fd = fd };
    var file = try consumer.AggregateFile.init(arena, fd_sink.sink());
    defer file.deinit();
    try file.reserveByteWorkspace(encode);
    var window = std.heap.ArenaAllocator.init(std.heap.page_allocator);
    defer window.deinit();
    const total = stream.state.group_count;
    var lo: usize = 0;
    while (lo < total or (lo == 0 and total == 0)) {
        const hi = @min(total, lo + 4 * consumer.answer_rg_rows);
        const columns_span = profile.Span.begin(profile.threadOf(profiler, .main));
        const answer = try stream.window(window.allocator(), lo, hi, encode.workers);
        columns_span.end(.byte_columns, profile.args(.{}));
        for (types, answer.columns) |ty, *col| {
            if (ty.source) |src| col.* = try retypeOutputCol(arena, col.*, src);
        }
        try file.append(answer.columns, hi - lo, codec, zstd, encode);
        lo = hi;
        if (lo == total) {
            const written = try file.finish(answer.columns);
            if (fd_open) {
                fd_open = false;
                try local_fs.closeChecked(fd);
            }
            return written;
        }
        _ = window.reset(.retain_capacity);
    }
    unreachable;
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
/// lane holds; INT96 becomes INT64 TIMESTAMP(NANOS), the value zpq decodes it to; fixed-length bytes retain their
/// width and logical annotation.
pub fn retypeOutputCol(arena: std.mem.Allocator, out: expr_agg.OutputCol, src: schema.SchemaElement) !expr_agg.OutputCol {
    if (src.type == .INT96) if (out.col == .i128) return int96OutputCol(arena, out);
    if (decimal_mod.kindFromSchema(&src) != null or schema.isFloat16(src)) return out;
    var res = out;
    res.logical_type = src.logical_type;
    res.logical_type_raw = src.logical_type_raw;
    res.converted_type = src.converted_type;
    switch (src.type orelse return out) {
        .INT64, .DOUBLE, .BYTE_ARRAY => {},
        .INT96 => {
            res.logical_type_raw = &.{};
            res.logical_type = .{ .TIMESTAMP = .{ .isAdjustedToUTC = false, .unit = .{ .NANOS = .{} } } };
            res.converted_type = null;
        },
        .FIXED_LEN_BYTE_ARRAY => {
            res.parquet_type = .FIXED_LEN_BYTE_ARRAY;
            res.type_length = src.type_length;
        },
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

/// Column `ci` of every grouped row as a `T` lane. For an avg_state column, `T == f64` takes the pair's sum and
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
                expr_agg.u64Lane(x)
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

/// Column `ci` of every grouped row as an exact DECIMAL(`spec`) column: a `dec` value, or an avg_state's exact sum.
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

