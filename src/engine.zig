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

const schema = @import("core/schema.zig");
const decimal_mod = @import("core/parquet/decimal.zig");
const consumer = @import("core/consumer.zig");
const invariant = @import("core/invariant.zig");
const scan = @import("core/scan.zig");
const metadata = @import("core/parquet/metadata.zig");
const schema_tree = @import("core/parquet/schema_tree.zig");
const thrift = @import("core/thrift.zig");
const expr_ast = @import("core/expr/ast.zig");
const expr_parser = @import("core/expr/parser.zig");
const expr_agg = @import("core/expr/agg.zig");
const filter_ast = @import("core/filter/ast.zig");
const filter_parser = @import("core/filter/parser.zig");
const filter_prune = @import("core/filter/prune.zig");
const streaming = @import("core/writer/streaming.zig");

const s3 = @import("io/s3.zig");
const tls = @import("io/tls.zig");
const http = @import("io/http.zig");
const sigv4 = @import("io/sigv4.zig");
const multipart_sink = @import("io/multipart_sink.zig");
const coalescer = @import("io/coalescer.zig");
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

pub const Error = error{
    NoInputs,
    EmptyAggregate,
    AggregateMutexWithSelect,
    MissingOutputOrAggregate,
    SchemaMismatch,
    NestedReencodeNotSupported,
    FooterSchemaChunkMismatch,
    CrossBucketNotSupported,
    BadInputUrl,
    BadOutputUrl,
    NoCredentials,
    BadResponse,
    TailTooSmall,
    NotParquet,
    AggSumOverflow,
    OpenFailed,
    EmptyFile,
    PathTooLong,
} || std.mem.Allocator.Error;

pub const QueryArgs = struct {
    inputs: []const []const u8, // raw paths/URLs (pre-glob — caller globs)
    output: ?[]const u8 = null,
    filter: ?[]const u8 = null,
    aggregate: ?[]const u8 = null,
    columns: ?[]const []const u8 = null,
    select: ?[]const u8 = null,
    codec: schema.CompressionCodec = .SNAPPY,
    parallelism: usize = 0,
    /// `--scan-all`: disable every stats shortcut (row-group pruning,
    /// stats-driven column drop, aggregate stat short-circuit) and decode
    /// every page/byte. Slower but thorough — the paranoid path, and the
    /// correctness valve for files whose writer emitted untrustworthy stats.
    scan_all: bool = false,
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
};

pub const QueryResult = union(enum) {
    aggregate: AggResult,
    write: WriteResult,
};

pub const WriteResult = struct {
    files_in: usize,
    rows_in: i64,
    rows_kept: i64,
    bytes_in: u64,
    bytes_out: u64,
    row_groups_in: usize,
    row_groups_kept: usize,
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
    cols_stat_pruned: usize,
    aggs: []scan.AggOutputItem,
    timings: Timings,
};

pub const Timings = struct {
    read_ns: u64 = 0,
    parse_ns: u64 = 0,
    core: consumer.Timings = .{},
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
    if (args.inputs.len == 0) return error.NoInputs;
    if (args.aggregate != null and (args.select != null or args.columns != null)) {
        return error.AggregateMutexWithSelect;
    }
    if (args.aggregate) |agg_str| {
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
    const t_open = nowMonoNs();
    var opened = try openInputs(ctx, arena, args);
    defer opened.deinit();
    t.read_ns = @intCast(nowMonoNs() - t_open);

    // 2. Run the orchestrator. (Yes, this re-parses the aggregate —
    //    cheap, microseconds. Easier than threading the parsed AST
    //    through the open step + the scan step.)
    const r = try scan.runMultiAggregate(ctx.gpa, arena, .{
        .inputs = opened.inputs,
        .filter = args.filter,
        .aggregate = agg_str,
        .parallelism = args.parallelism,
        .scan_all = args.scan_all,
    });
    t.parse_ns = r.timings.parse_ns;
    t.core = r.timings.core;

    // 3. Optional 1-row parquet output. Aggregate output is local-only;
    //    S3 writes use the row-group streaming path.
    var bytes_out: u64 = 0;
    if (args.output) |out_path| {
        if (std.mem.startsWith(u8, out_path, "s3://")) {
            return error.BadOutputUrl;
        }
        const t_footer = nowMonoNs();
        bytes_out = try writeOneRowParquet(arena, out_path, r.agg_calls, r.accumulators, args.codec);
        t.footer_ns += @intCast(nowMonoNs() - t_footer);
    }

    return .{
        .files_in = r.files_in,
        .rows_in = r.rows_in,
        .rows_kept = r.rows_kept,
        .bytes_in = r.bytes_in,
        .bytes_out = bytes_out,
        .row_groups_in = r.row_groups_in,
        .row_groups_pruned = r.row_groups_pruned,
        .cols_stat_pruned = r.cols_stat_pruned,
        .aggs = r.aggs,
        .timings = t,
    };
}

// ============================================================
// Write path — projection / select / filter, output to local or s3
// ============================================================

const PAR1: [4]u8 = .{ 'P', 'A', 'R', '1' };

fn runWrite(ctx: Context, args: QueryArgs, out_path: []const u8) !WriteResult {
    var arena_state = std.heap.ArenaAllocator.init(ctx.gpa);
    defer arena_state.deinit();
    const arena = arena_state.allocator();

    var t: Timings = .{};

    if (args.select != null and args.columns != null) return error.AggregateMutexWithSelect;

    // 1. Open all inputs (mmap or parallel s3 range-fetch w/ column
    //    pruning — same path runAggregate uses).
    const t_open = nowMonoNs();
    var opened = try openInputs(ctx, arena, args);
    defer opened.deinit();
    t.read_ns = @intCast(nowMonoNs() - t_open);

    // 2. Parse meta of every file (cheap; serial), validate schemas
    //    against meta[0].
    const t_parse_start = nowMonoNs();
    var metas = try arena.alloc(schema.FileMetaData, opened.inputs.len);
    for (opened.inputs, 0..) |in, i| metas[i] = try metadata.open(arena, in.bytes);
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
            if (a_e.type != b_e.type) return error.SchemaMismatch;
            if (a_e.repetition_type != b_e.repetition_type) return error.SchemaMismatch;
            if (a_e.converted_type != b_e.converted_type) return error.SchemaMismatch;
            if (!std.meta.eql(a_e.logical_type, b_e.logical_type)) return error.SchemaMismatch;
            if (a_e.type_length != b_e.type_length) return error.SchemaMismatch;
        }
        _ = i;
    }
    const tree0 = try schema_tree.SchemaTree.build(arena, meta0.schema.items);

    // 3. Resolve projection columns (--columns) to leaf indices.
    var kept_set: ?[]bool = null;
    if (args.columns) |cols_list| {
        const set = try arena.alloc(bool, tree0.leaves.len);
        @memset(set, false);
        for (cols_list) |name| {
            const indices = try tree0.resolveTopLevel(arena, name);
            for (indices) |idx| set[idx] = true;
        }
        kept_set = set;
    }

    // 4. Parse --select if given. Mutually exclusive with --columns
    //    (already checked).
    const select_items: ?[]expr_ast.SelectItem = if (args.select) |s|
        try expr_parser.parseSelect(arena, s, meta0)
    else
        null;

    // 5. Parse --filter against meta0.
    var filter_opt: ?filter_ast.Filter = null;
    if (args.filter) |fs| if (fs.len > 0) {
        filter_opt = try filter_parser.parse(arena, fs, meta0);
    };
    t.parse_ns = @intCast(nowMonoNs() - t_parse_start);

    // 6. Build kept_arr + fetch_arr + output_specs (same shape as
    //    cli/query.zig:run did — lifted).
    const num_leaves = meta0.row_groups.items[0].columns.items.len;
    const kept_arr = try arena.alloc(bool, num_leaves);
    if (kept_set) |s| @memcpy(kept_arr, s) else @memset(kept_arr, true);
    const fetch_arr = try arena.alloc(bool, num_leaves);
    @memset(fetch_arr, false);

    var output_specs: std.ArrayList(consumer.OutputCol) = .empty;
    var any_computed = false;
    if (select_items) |items| {
        for (items) |item| {
            switch (item.expr) {
                .col_ref => |c| {
                    if (item.alias) |alias| {
                        try output_specs.append(arena, .{ .computed = .{ .expr = item.expr, .alias = alias } });
                        any_computed = true;
                    } else {
                        try output_specs.append(arena, .{ .passthrough = c.col_idx });
                    }
                    if (c.col_idx < num_leaves) fetch_arr[c.col_idx] = true;
                },
                else => {
                    const alias = item.alias orelse return error.MissingOutputOrAggregate; // alias required
                    try output_specs.append(arena, .{ .computed = .{ .expr = item.expr, .alias = alias } });
                    any_computed = true;
                    item.expr.collectColumns(fetch_arr);
                },
            }
        }
    } else {
        for (kept_arr, 0..) |b, i| if (b) {
            try output_specs.append(arena, .{ .passthrough = i });
            fetch_arr[i] = true;
        };
    }
    if (filter_opt) |f| {
        var filter_cols: std.ArrayList(usize) = .empty;
        try f.collectColumns(&filter_cols, arena);
        for (filter_cols.items) |ci| {
            if (ci < num_leaves) fetch_arr[ci] = true;
        }
    }

    var kept_in_order: std.ArrayList(usize) = .empty;
    for (kept_arr, 0..) |b, i| if (b) try kept_in_order.append(arena, i);

    // 7. Open output sink: local fd or s3 multipart.
    const out_is_s3 = std.mem.startsWith(u8, out_path, "s3://");
    var fd_sink: FdSink = .{ .fd = 0 };
    var mp_sink: ?multipart_sink.MultipartSink = null;
    var sink: streaming.Sink = undefined;
    var sink_owns_fd = false;
    defer if (sink_owns_fd) {
        _ = std.os.linux.close(fd_sink.fd);
    };
    var out_owned_pool: ?*s3.Pool(POOL_SIZE) = null;
    defer if (out_owned_pool) |p| p.deinit();
    defer if (mp_sink) |*ms| {
        if (!ms.isClosed()) ms.abort();
        ms.deinit();
    };

    if (out_is_s3) {
        const out_url = s3.Url.parse(out_path) catch return error.BadOutputUrl;
        const creds = s3.Credentials.fromEnv(ctx.env) catch return error.NoCredentials;
        const out_criteria = try s3.poolCriteria(arena, creds, out_url.bucket);
        const out_pool: *s3.Pool(POOL_SIZE) = if (ctx.pool_registry) |reg|
            try reg.ensure_for_bucket(reg.ctx, ctx.gpa, creds, out_url.bucket)
        else blk: {
            const p = try arena.create(s3.Pool(POOL_SIZE));
            try p.init(ctx.gpa);
            out_owned_pool = p;
            break :blk p;
        };
        mp_sink = multipart_sink.MultipartSink.init(
            ctx.io,
            ctx.gpa,
            arena,
            creds,
            out_url,
            out_pool,
            out_criteria,
            .{},
        );
        sink = .{
            .ctx = @ptrCast(&mp_sink.?),
            .write_fn = multipart_sink.sinkWriteFn,
        };
    } else {
        fd_sink.fd = try createFile(out_path);
        sink_owns_fd = true;
        sink = .{
            .ctx = @ptrCast(&fd_sink),
            .write_fn = FdSink.writeFn,
        };
    }

    // 8. Stream output. PAR1 magic, then per-file × per-RG
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
    // `--select` always re-encodes: the byte-copy fastpath copies every column
    // chunk in leaf order with `kept_set == null`, but a select builds the
    // footer schema from only the selected/reordered columns — so a pure
    // passthrough `--select "a,b"` would emit a footer that disagrees with the
    // copied row-group data (N chunks, M<N columns). Routing it through the
    // encoder makes the footer match what's written. (`--columns` keeps the
    // fast lossless subset path via `kept_set`.)
    const need_encoder = filter_opt != null or any_computed or select_items != null;

    // Re-encoding a nested (repeated) column would drop its rep-levels — the
    // OutputAggregator carries only values/def-levels — yielding pages whose
    // decoded level counts disagree with the header while the footer still
    // describes nested data. Reject loudly instead of writing corrupt output.
    // (Pure SELECT * / `--columns` byte-copy nested columns intact via the
    // fastpath, which never decodes.)
    if (need_encoder) {
        for (output_specs.items) |spec| switch (spec) {
            .passthrough => |ci| {
                if (ci >= num_leaves) continue;
                const cm = meta0.row_groups.items[0].columns.items[ci].meta_data orelse continue;
                if (meta0.getColumnLevels(cm.path_in_schema.items).max_rep > 0)
                    return error.NestedReencodeNotSupported;
            },
            .computed => {},
        };
    }

    // Threshold for flushing the output aggregator mid-stream. Larger
    // → better snappy compression on DOUBLE columns (per-page hash
    // table sees more redundancy). 250K rows × 10 cols × ~15 B avg
    // ≈ 37 MB peak buffer, well within a 2GB Lambda. Above this we
    // flush and start a new output RG.
    const TARGET_ROWS_PER_OUTPUT_RG: usize = 250_000;

    var agg_opt: ?consumer.OutputAggregator = null;
    defer if (need_encoder and agg_opt != null) {
        // Buffers live on `arena`; arena cleanup handles them.
    };
    if (need_encoder and metas.len > 0) {
        agg_opt = try consumer.initOutputAggregator(arena, &metas[0], output_specs.items);
    }

    for (opened.inputs, metas) |in, meta_i| {
        bytes_in += in.bytes.len;
        const rg_src: consumer.RGSrc = .{ .bytes = in.bytes, .byte_origin = 0 };
        const meta_const = meta_i;
        for (meta_const.row_groups.items) |*src_rg| {
            rgs_in += 1;
            rows_in += src_rg.num_rows;
            if (filter_opt) |f| if (!args.scan_all) {
                if ((try filter_prune.pruneRowGroup(src_rg, f, arena, &meta_const)) == .skip) continue;
            };
            if (need_encoder) {
                _ = try consumer.appendProjectedRG(
                    &agg_opt.?,
                    ctx.gpa,
                    src_rg,
                    &meta_const,
                    rg_src,
                    filter_opt,
                    fetch_arr,
                    output_specs.items,
                    &t.core,
                );
                if (agg_opt.?.num_rows >= TARGET_ROWS_PER_OUTPUT_RG) {
                    const out = try consumer.encodeAggregator(
                        &agg_opt.?,
                        arena,
                        ctx.gpa,
                        sink,
                        &out_offset,
                        args.codec,
                        &t.core,
                    );
                    if (out.rg) |new_rg| try new_row_groups.append(arena, new_rg);
                    rows_kept += out.surviving_rows;
                    if (out.surviving_rows > 0) rg_kept += 1;
                    consumer.resetAggregator(&agg_opt.?);
                }
            } else {
                const out = if (kept_set != null) try consumer.copyRG(
                    arena,
                    src_rg,
                    rg_src,
                    kept_arr,
                    sink,
                    &out_offset,
                    &t.core,
                ) else try consumer.copyRG(
                    arena,
                    src_rg,
                    rg_src,
                    null,
                    sink,
                    &out_offset,
                    &t.core,
                );
                if (out.rg) |new_rg| try new_row_groups.append(arena, new_rg);
                rows_kept += out.surviving_rows;
                if (out.surviving_rows > 0) rg_kept += 1;
            }
        }
    }

    // Final flush: any remaining buffered rows go into one trailing RG.
    if (need_encoder and agg_opt != null and agg_opt.?.num_rows > 0) {
        const out = try consumer.encodeAggregator(
            &agg_opt.?,
            arena,
            ctx.gpa,
            sink,
            &out_offset,
            args.codec,
            &t.core,
        );
        if (out.rg) |new_rg| try new_row_groups.append(arena, new_rg);
        rows_kept += out.surviving_rows;
        if (out.surviving_rows > 0) rg_kept += 1;
    }

    // 9. Build footer.
    const t_footer = nowMonoNs();
    var new_schema_items = meta0.schema;
    if (select_items != null) {
        // Build a flat schema for --select. Each output_spec produces
        // one leaf in the output. (Lifted from cli/query.zig.)
        new_schema_items = try buildSelectSchema(arena, meta0, output_specs.items);
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
    }

    const new_meta: schema.FileMetaData = .{
        .version = meta0.version,
        .schema = new_schema_items,
        .num_rows = rows_kept,
        .created_by = meta0.created_by,
        .row_groups = new_row_groups,
    };

    // Output structural invariant — ALWAYS ON, even under ReleaseFast.
    // Every row group must carry exactly one column chunk per footer
    // leaf. If the footer leaf count and emitted chunks disagree, strict
    // readers reject the file. This guard must survive ReleaseFast, so it
    // is an error instead of a debug assert.
    const footer_leaves = invariant.footerLeafCount(new_schema_items.items);
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

    // 10. Close s3 multipart upload (commits the parts). Local fd
    //     closes via defer.
    if (mp_sink) |*ms| {
        try ms.close();
        const mp_stats = ms.snapshotStats();
        t.mp_complete_ns = mp_stats.complete_ns;
        t.mp_await_ns = mp_stats.await_ns;
    }

    return .{
        .files_in = opened.inputs.len,
        .rows_in = rows_in,
        .rows_kept = rows_kept,
        .bytes_in = bytes_in,
        .bytes_out = out_offset,
        .row_groups_in = rgs_in,
        .row_groups_kept = rg_kept,
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
        if (decimal_mod.kindFromSchema(elem) == null) continue;

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
                const cm = meta.row_groups.items[0].columns.items[ci].meta_data orelse return error.SchemaMismatch;
                const elem = meta.getColumnSchema(cm.path_in_schema.items) orelse return error.SchemaMismatch;
                var copy = elem;
                copy.num_children = 0;
                try out.append(arena, copy);
            },
            .computed => |c| {
                const expr_type = c.expr.typeOf();
                try out.append(arena, .{
                    .type = expr_type.toParquet(),
                    .type_length = null,
                    .repetition_type = .REQUIRED,
                    .name = c.alias,
                    .num_children = 0,
                    .converted_type = if (expr_type == .str) .UTF8 else null,
                    .logical_type = if (expr_type == .str) .{ .STRING = .{} } else null,
                    .scale = null,
                    .precision = null,
                    .field_id = null,
                });
            },
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
    /// Per-input mmap state — set for local files so we can munmap on
    /// deinit. Null for s3 inputs (their bytes live in the arena).
    mmaps: []const ?MmapHandle,
    /// One s3 pool per bucket touched. Owned here when the caller's
    /// `Context.pool_registry` is null; borrowed otherwise.
    owned_pools: []*s3.Pool(POOL_SIZE),
    /// S3 sparse file buffers are gpa-owned because they must outlive the
    /// per-task scratch arenas used during parallel metadata fetch.
    s3_buffers: []const []u8,

    pub fn deinit(self: *OpenedInputs) void {
        for (self.mmaps) |m_opt| if (m_opt) |m| munmapFile(m);
        for (self.s3_buffers) |buf| self.gpa.free(buf);
        for (self.owned_pools) |p| {
            p.deinit();
        }
    }
};

const MmapHandle = struct {
    addr: [*]const u8,
    len: usize,
};

fn openInputs(
    ctx: Context,
    arena: std.mem.Allocator,
    args: QueryArgs,
) !OpenedInputs {
    const paths = args.inputs;
    var inputs = try arena.alloc(scan.Input, paths.len);
    var mmaps = try arena.alloc(?MmapHandle, paths.len);
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
        const m = try mmapFile(paths[i]);
        mmaps[i] = m;
        inputs[i] = .{ .name = paths[i], .bytes = m.addr[0..m.len] };
    }

    // S3: gather per-bucket batches, one pool per bucket, fetch each
    // batch in parallel via Io.Group.
    var owned_pools: std.ArrayList(*s3.Pool(POOL_SIZE)) = .empty;
    var s3_buffers: std.ArrayList([]u8) = .empty;
    if (s3_indices.items.len > 0) {
        const creds = s3.Credentials.fromEnv(ctx.env) catch return error.NoCredentials;

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
            try owned_pools.append(arena, p);
            break :blk p;
        };

        // Fetch metadata (tail + head + footer) for every s3 file in
        // parallel. After this the per-file meta is parsed and we know
        // total file size + tail_start.
        var specs = try arena.alloc(FetchSpec, s3_urls.len);
        for (s3_urls, 0..) |u, k| specs[k] = .{ .url = u };
        fetchMetaBatch(ctx, arena, creds, pool, specs) catch |err| {
            for (specs) |sp| if (sp.file_buf.len > 0) ctx.gpa.free(sp.file_buf);
            return err;
        };
        var specs_returned = false;
        errdefer if (!specs_returned) {
            for (specs) |sp| if (sp.file_buf.len > 0) ctx.gpa.free(sp.file_buf);
        };

        // Compute fetch_set from whatever args are set: aggregate,
        // filter, columns, select. Same path for both aggregate and
        // write modes. (Yes, this re-parses arg strings that scan
        // and runWrite will parse again — microseconds, not worth
        // plumbing the parsed AST through.)
        //
        // Default: write-mode passthrough (no aggregate, no projection)
        // outputs every column, so every column needs to be fetched.
        // Aggregate / --columns / --select narrow this. Filter columns
        // are always added on top.
        const meta0 = &specs[0].meta;
        const num_leaves = meta0.row_groups.items[0].columns.items.len;
        const fetch_arr = try arena.alloc(bool, num_leaves);
        const is_aggregate = args.aggregate != null;
        const has_projection = args.columns != null or args.select != null;
        if (!is_aggregate and !has_projection) {
            @memset(fetch_arr, true);
        } else {
            @memset(fetch_arr, false);
        }
        const filter_for_plan: ?filter_ast.Filter = if (args.filter) |fs|
            (if (fs.len > 0) try filter_parser.parse(arena, fs, meta0) else null)
        else
            null;
        if (filter_for_plan) |f| {
            var cols: std.ArrayList(usize) = .empty;
            try f.collectColumns(&cols, arena);
            for (cols.items) |ci| {
                if (ci < num_leaves) fetch_arr[ci] = true;
            }
        }
        const agg_calls_for_plan: ?[]const expr_agg.AggCall = if (args.aggregate) |agg_str|
            try expr_parser.parseAggList(arena, agg_str, meta0)
        else
            null;
        if (agg_calls_for_plan) |calls| {
            for (calls) |call| {
                if (call.arg) |arg_expr| arg_expr.collectColumns(fetch_arr);
                if (call.where) |w_expr| {
                    var cols: std.ArrayList(usize) = .empty;
                    try w_expr.collectColumns(&cols, arena);
                    for (cols.items) |ci| {
                        if (ci < num_leaves) fetch_arr[ci] = true;
                    }
                }
            }
        }
        if (filter_for_plan == null and !args.scan_all) {
            if (agg_calls_for_plan) |calls| {
                const metas_for_stats = try arena.alloc(schema.FileMetaData, specs.len);
                for (specs, 0..) |sp, i| metas_for_stats[i] = sp.meta;

                var per_agg_cols = try arena.alloc(bool, num_leaves);
                for (fetch_arr, 0..) |needed, ci| {
                    if (!needed) continue;

                    var prunable = true;
                    for (calls) |call| {
                        @memset(per_agg_cols, false);
                        if (call.arg) |arg_expr| arg_expr.collectColumns(per_agg_cols);
                        if (call.where) |w_expr| {
                            var cols: std.ArrayList(usize) = .empty;
                            try w_expr.collectColumns(&cols, arena);
                            for (cols.items) |wci| {
                                if (wci < num_leaves) per_agg_cols[wci] = true;
                            }
                        }
                        if (!per_agg_cols[ci]) continue;
                        if (!expr_agg.statsCoverageComplete(call, metas_for_stats, ci)) {
                            prunable = false;
                            break;
                        }
                    }
                    if (prunable) fetch_arr[ci] = false;
                }
            }
        }
        if (args.select) |sel| {
            const items = try expr_parser.parseSelect(arena, sel, meta0);
            for (items) |item| item.expr.collectColumns(fetch_arr);
        }
        if (args.columns) |cols_list| {
            const tree = try schema_tree.SchemaTree.build(arena, meta0.schema.items);
            for (cols_list) |name| {
                const indices = try tree.resolveTopLevel(arena, name);
                for (indices) |idx| {
                    if (idx < num_leaves) fetch_arr[idx] = true;
                }
            }
        }

        // Per file: stat-prune RGs, collect raw byte ranges for every
        // (surviving RG) × (needed col), coalesce within the file, and
        // emit one FetchJob per coalesced span. Coalescing matters
        // because column chunks in a parquet RG are written back-to-
        // back; fetching them as separate GETs spends most of the wall
        // in HTTP round-trip overhead even though the bytes are
        // physically adjacent. Merged spans land directly in the file
        // buffer at their absolute offsets — gap bytes get fetched but
        // the decoder ignores them.
        var fetch_jobs: std.ArrayList(s3.FetchJob) = .empty;
        for (specs) |*sp| {
            const meta = &sp.meta;
            sp.survivors = try arena.alloc(bool, meta.row_groups.items.len);

            var ranges: std.ArrayList(coalescer.Range) = .empty;
            for (meta.row_groups.items, 0..) |rg, rg_i| {
                if (filter_for_plan) |f| if (!args.scan_all) {
                    if ((try filter_prune.pruneRowGroup(&rg, f, arena, meta)) == .skip) {
                        sp.survivors[rg_i] = false;
                        continue;
                    }
                };
                sp.survivors[rg_i] = true;
                for (fetch_arr, 0..) |needed, ci| {
                    if (!needed) continue;
                    if (ci >= rg.columns.items.len) continue;
                    const cm = rg.columns.items[ci].meta_data orelse continue;
                    const start: u64 = if (cm.dictionary_page_offset) |dp| @intCast(dp) else @intCast(cm.data_page_offset);
                    const len: u64 = @intCast(cm.total_compressed_size);
                    // Skip ranges already covered by the prefetched tail
                    // (head + footer). Anything below tail_start is new
                    // bytes we need.
                    if (start >= sp.tail_start) continue;
                    try ranges.append(arena, .{ .start = start, .end = start + len });
                }
            }

            if (ranges.items.len == 0) continue;
            const merged = try coalescer.Coalescer.coalesce(arena, ranges.items, COALESCE_GAP);
            for (merged) |r| {
                // If a writer crammed a chunk right against the footer,
                // clamp here — those bytes already arrived in the tail
                // fetch.
                const end = @min(r.end, sp.tail_start);
                if (r.start >= end) continue;
                try fetch_jobs.append(arena, .{
                    .bucket = sp.url.bucket,
                    .key = sp.url.key,
                    .range = .{ .start = r.start, .end = end },
                    .target = sp.file_buf[r.start..end],
                });
            }
        }

        // Parallel fetch all the column-chunk ranges across all files.
        // s3.fetchJobs handles the Io.Group fan-out + per-job pool
        // acquire/release.
        if (fetch_jobs.items.len > 0) {
            _ = try s3.fetchJobs(ctx.io, pool, ctx.gpa, arena, creds, fetch_jobs.items);
        }

        // Build scan.Inputs. bytes is the per-file buffer with all the
        // needed chunks materialized at their absolute offsets.
        for (s3_indices.items, 0..) |orig_idx, k| {
            inputs[orig_idx] = .{
                .name = paths[orig_idx],
                .bytes = specs[k].file_buf,
            };
            try s3_buffers.append(arena, specs[k].file_buf);
        }
        specs_returned = true;
    }

    return .{
        .gpa = ctx.gpa,
        .inputs = inputs,
        .mmaps = mmaps,
        .owned_pools = try owned_pools.toOwnedSlice(arena),
        .s3_buffers = try s3_buffers.toOwnedSlice(arena),
    };
}

/// Per-s3-file state during the meta-fetch + range-fetch dance.
/// `file_buf` is sized to total file size; we sparse-fill the regions
/// the query touches. Parquet's absolute byte offsets in metadata
/// (data_page_offset, dictionary_page_offset) index into this.
const FetchSpec = struct {
    url: s3.Url,
    meta: schema.FileMetaData = undefined,
    file_buf: []u8 = &.{},
    total_size: u64 = 0,
    tail_start: u64 = 0,
    survivors: []bool = &.{},
    err: ?anyerror = null,
};

fn fetchMetaBatch(
    ctx: Context,
    arena: std.mem.Allocator,
    creds: s3.Credentials,
    pool: *s3.Pool(POOL_SIZE),
    specs: []FetchSpec,
) !void {
    const TaskCtx = struct {
        ctx: Context,
        creds: s3.Credentials,
        pool: *s3.Pool(POOL_SIZE),
        sp: *FetchSpec,
    };
    const Task = struct {
        fn run(t: *TaskCtx) std.Io.Cancelable!void {
            var scratch_state = std.heap.ArenaAllocator.init(t.ctx.gpa);
            defer scratch_state.deinit();
            doFetchMeta(t.*, scratch_state.allocator()) catch |err| {
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
    };
    {
        var group: std.Io.Group = .init;
        defer group.cancel(ctx.io);
        for (task_ctxs) |*tc| try group.concurrent(ctx.io, Task.run, .{tc});
        try group.await(ctx.io);
    }
    for (specs) |sp| if (sp.err) |err| return err;
    for (specs) |*sp| sp.meta = try metadata.open(arena, sp.file_buf);
}

fn doFetchMeta(t: anytype, scratch: std.mem.Allocator) !void {
    const sp = t.sp;

    // Cache fast path: if we have a previously-cached entry, issue the
    // tail GET with `If-None-Match`. A 304 means the object is
    // unchanged → reconstruct the sparse file_buf from the cached
    // footer bytes + a stamped leading PAR1 magic. Skips the head GET
    // and the footer-prefix GET entirely.
    //
    // On 200/206 the object has changed — the cached entry is stale,
    // we proceed exactly as the cold path. The fresh entry is
    // installed at the end of the cold path.
    if (t.ctx.meta_cache) |cache| {
        if (cache.get(t.ctx.io, sp.url.bucket, sp.url.key)) |entry| {
            const cond = try s3.getViaPoolWithOpts(t.ctx.io, t.pool, scratch, t.creds, sp.url, .{
                .range = s3.Range.suffix(TAIL_SIZE),
                .if_none_match = entry.etag,
            });
            if (cond.status == 304) {
                // Reconstruct the file_buf so downstream metadata.open
                // sees a valid (sparse) view.
                sp.total_size = entry.total_size;
                sp.file_buf = try t.ctx.gpa.alloc(u8, sp.total_size);
                errdefer {
                    t.ctx.gpa.free(sp.file_buf);
                    sp.file_buf = &.{};
                }
                @memcpy(sp.file_buf[0..PAR1.len], &PAR1);
                @memcpy(sp.file_buf[entry.footer_offset..sp.total_size], entry.footer_bytes);
                sp.tail_start = entry.footer_offset; // not strictly used by callers
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
                try populateFromTailResponse(t, scratch, cond);
                try maybeCacheMeta(t.ctx, sp, cond.header("ETag"));
                return;
            }
            return error.BadResponse;
        }
    }

    // Cold path: normal tail/head/footer-prefix fetches.
    const tail_resp = try s3.getViaPool(t.ctx.io, t.pool, scratch, t.creds, sp.url, s3.Range.suffix(TAIL_SIZE));
    if (tail_resp.status != 206 and tail_resp.status != 200) return error.BadResponse;
    try populateFromTailResponse(t, scratch, tail_resp);
    try maybeCacheMeta(t.ctx, sp, tail_resp.header("ETag"));
}

/// Shared "we have the tail response, finish the metadata fetch" path
/// used by both the cold and (cache-stale → 200) paths. On entry,
/// `sp` is empty; on success, `sp.file_buf`/`total_size`/`tail_start`
/// are populated and the file_buf has its head + footer regions
/// filled in.
fn populateFromTailResponse(t: anytype, scratch: std.mem.Allocator, tail_resp: http.Response) !void {
    const sp = t.sp;
    sp.total_size = try parseTotalFromContentRange(tail_resp.header("Content-Range"));
    sp.file_buf = try t.ctx.gpa.alloc(u8, sp.total_size);
    errdefer {
        t.ctx.gpa.free(sp.file_buf);
        sp.file_buf = &.{};
    }
    sp.tail_start = sp.total_size - tail_resp.body.len;
    @memcpy(sp.file_buf[sp.tail_start..], tail_resp.body);

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
    const head = try s3.getViaPool(t.ctx.io, t.pool, scratch, t.creds, sp.url, s3.Range.span(0, 7));
    if (head.status != 206) return error.BadResponse;
    if (head.body.len < PAR1.len) return error.NotParquet;
    if (!std.mem.eql(u8, head.body[0..PAR1.len], &PAR1)) return error.NotParquet;
    @memcpy(sp.file_buf[0..head.body.len], head.body);

    // Footer: if it doesn't all fit in the tail we just fetched, fetch
    // the part we're missing.
    if (footer_actual_start < sp.tail_start) {
        const need = try s3.getViaPool(t.ctx.io, t.pool, scratch, t.creds, sp.url, s3.Range.span(footer_actual_start, sp.tail_start - 1));
        if (need.status != 206) return error.BadResponse;
        @memcpy(sp.file_buf[footer_actual_start..sp.tail_start], need.body);
    }
}

/// Insert the just-fetched object into the cache (if cache is wired
/// and we got an ETag back from S3). Bytes captured cover
/// `[footer_offset, total_size)` — the thrift footer plus the trailer
/// (4-byte length + PAR1) — which is what the cache hit path
/// rebuilds from.
fn maybeCacheMeta(ctx: Context, sp: *FetchSpec, etag_opt: ?[]const u8) !void {
    const cache = ctx.meta_cache orelse return;
    const etag_raw = etag_opt orelse return;

    // S3 ETags arrive double-quoted in the response header
    // (`ETag: "abc123"`). Strip them so cache keys are stable.
    var etag = etag_raw;
    if (etag.len >= 2 and etag[0] == '"' and etag[etag.len - 1] == '"') {
        etag = etag[1 .. etag.len - 1];
    }

    if (sp.file_buf.len < 8) return; // can't compute footer offset
    const footer_len: u64 = std.mem.readInt(
        u32,
        sp.file_buf[sp.total_size - 8 ..][0..4],
        .little,
    );
    const footer_offset = sp.total_size - 8 - footer_len;
    const cached_bytes = sp.file_buf[footer_offset..sp.total_size];

    cache.put(
        ctx.io,
        sp.url.bucket,
        sp.url.key,
        etag,
        sp.total_size,
        footer_offset,
        cached_bytes,
    ) catch {
        // Cache insert failures are non-fatal — proceed without
        // caching this entry.
    };
}

fn parseTotalFromContentRange(hdr: ?[]const u8) !u64 {
    const v = hdr orelse return error.BadResponse;
    // Format: "bytes start-end/total"
    const slash = std.mem.indexOfScalar(u8, v, '/') orelse return error.BadResponse;
    return std.fmt.parseInt(u64, std.mem.trim(u8, v[slash + 1 ..], " \t"), 10) catch error.BadResponse;
}

// ============================================================
// Local mmap (Linux-only — same as cli/query.zig had)
// ============================================================

fn mmapFile(path: []const u8) !MmapHandle {
    const linux = std.os.linux;
    var path_z: [4096]u8 = undefined;
    if (path.len + 1 > path_z.len) return error.PathTooLong;
    @memcpy(path_z[0..path.len], path);
    path_z[path.len] = 0;

    const r_open = linux.openat(linux.AT.FDCWD, @ptrCast(&path_z[0]), .{ .ACCMODE = .RDONLY, .CLOEXEC = true }, 0);
    const r_open_signed: isize = @bitCast(r_open);
    if (r_open_signed < 0) return error.OpenFailed;
    const fd: linux.fd_t = @intCast(r_open_signed);
    defer _ = linux.close(fd);

    const SEEK_END: usize = 2;
    const end_pos = linux.lseek(fd, 0, SEEK_END);
    const size: usize = @intCast(end_pos);
    if (size == 0) return error.EmptyFile;

    const r_map = linux.mmap(
        null,
        size,
        .{ .READ = true },
        .{ .TYPE = .PRIVATE },
        @intCast(fd),
        0,
    );
    const r_signed: isize = @bitCast(r_map);
    if (r_signed < 0) return error.OpenFailed;

    const ptr: [*]const u8 = @ptrFromInt(r_map);
    return .{ .addr = ptr, .len = size };
}

fn munmapFile(m: MmapHandle) void {
    _ = std.os.linux.munmap(@ptrCast(@constCast(m.addr)), m.len);
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
    codec: schema.CompressionCodec,
) !u64 {
    const fd = try createFile(out_path);
    defer _ = std.os.linux.close(fd);
    var fd_sink: FdSink = .{ .fd = fd };
    const sink: streaming.Sink = .{ .ctx = @ptrCast(&fd_sink), .write_fn = FdSink.writeFn };

    var all_outs: std.ArrayList(expr_agg.OutputCol) = .empty;
    for (agg_calls, 0..) |call, i| {
        const cols = try expr_agg.finalize(arena, call, accumulators[i]);
        for (cols) |c| try all_outs.append(arena, c);
    }
    const written = try consumer.writeOneRowAggregate(arena, sink, all_outs.items, codec);
    return written;
}

const FdSink = struct {
    fd: std.os.linux.fd_t,
    written: u64 = 0,

    pub fn writeFn(ctx: *anyopaque, bytes: []const u8) anyerror!void {
        const self: *FdSink = @ptrCast(@alignCast(ctx));
        var i: usize = 0;
        while (i < bytes.len) {
            const r = std.os.linux.write(self.fd, bytes.ptr + i, bytes.len - i);
            const n: isize = @bitCast(r);
            if (n <= 0) return error.WriteFailed;
            i += @intCast(n);
        }
        self.written += bytes.len;
    }
};

fn createFile(path: []const u8) !std.os.linux.fd_t {
    const linux = std.os.linux;
    var path_z: [4096]u8 = undefined;
    if (path.len + 1 > path_z.len) return error.PathTooLong;
    @memcpy(path_z[0..path.len], path);
    path_z[path.len] = 0;
    const r = linux.openat(
        linux.AT.FDCWD,
        @ptrCast(&path_z[0]),
        .{ .ACCMODE = .WRONLY, .CREAT = true, .TRUNC = true, .CLOEXEC = true },
        0o644,
    );
    const r_signed: isize = @bitCast(r);
    if (r_signed < 0) return error.OpenFailed;
    return @intCast(r_signed);
}

fn nowMonoNs() i64 {
    var ts: std.os.linux.timespec = .{ .sec = 0, .nsec = 0 };
    _ = std.os.linux.clock_gettime(.MONOTONIC, &ts);
    return @as(i64, ts.sec) * std.time.ns_per_s + @as(i64, ts.nsec);
}

// ============================================================
// Compile-time API check.
// ============================================================
test "engine: API is well-typed" {
    _ = runQuery;
}
