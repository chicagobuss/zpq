//! Per-row-group scan→consumer protocol.
//!
//! Lambda's parallel-fetcher orchestrator and the CLI's sequential
//! local-file driver both feed the same shape of work to the same
//! two consumers: *encode* (decode → filter → re-encode kept cols)
//! and *copy* (byte-copy kept chunks, shift offsets).
//!
//! The only thing they differ on is *where the bytes come from*:
//! lambda hands in a `RowGroupResult.raw_bytes` window covering the
//! kept-column span, CLI hands in the entire mmap'd file. Both shapes
//! collapse to a single `RGSrc { bytes, byte_origin }` — the per-
//! column offset into the source equals `col.dpo - byte_origin`
//! either way.
//!
//! Output is always a `streaming.Sink` (multipart-S3 in lambda; an
//! fd-backed sink in the CLI). Per-phase timings flow into a shared
//! `Timings` accumulator the caller owns.
//!
//! Layering: this module lives in `core/`, so it must NOT depend on
//! any I/O. The `Sink` is opaque; the source is a borrowed slice.

const std = @import("std");
const schema = @import("schema.zig");
const filter_ast = @import("filter/ast.zig");
const filter_eval = @import("filter/eval.zig");
const filter_selection = @import("filter/selection.zig");
const expr_ast = @import("expr/ast.zig");
const expr_eval = @import("expr/eval.zig");
const encoder = @import("writer/encoder.zig");
const streaming = @import("writer/streaming.zig");
const column_mod = @import("parquet/column.zig");

/// Error set the consumer functions can return. Inferred from the
/// underlying decoders/encoders/sinks; unioned here for documentation.
/// (The actual function signatures use `anyerror` because `sink.write`
/// is opaque and may surface I/O errors we don't enumerate at this
/// layer.)
pub const Error = error{
    ColumnMetaMissing,
    SchemaLookupFailed,
    MissingChunkBytes,
    MissingDecodedColumn,
    UnsupportedColumnType,
    ShortDecode,
    InvalidColumnOffsets,
    BadColumnIndex,
};

/// Whole-RG byte window. The consumer slices per-column from this:
///
///     col_off = col.data_page_offset - byte_origin
///     chunk   = bytes[col_off .. col_off + total_compressed_size]
///
/// `byte_origin` is the absolute offset (in the source file) where
/// `bytes[0]` lives. CLI passes `byte_origin = 0` and `bytes = file_bytes`;
/// lambda passes `byte_origin = rg_byte_start` and `bytes = rg_result.raw_bytes`.
pub const RGSrc = struct {
    bytes: []const u8,
    byte_origin: u64,
};

/// Per-phase wall-clock counters in nanoseconds. Both consumers
/// accumulate into the caller-supplied struct so multi-RG totals
/// add up naturally.
pub const Timings = struct {
    decode_ns: u64 = 0,
    eval_ns: u64 = 0,
    encode_ns: u64 = 0,
    sink_ns: u64 = 0,
};

/// Per-RG result. `surviving_rows == 0` means the filter dropped
/// every row — in that case `rg` is null and the caller MUST NOT
/// append it to the output's row-group list.
pub const RGOut = struct {
    surviving_rows: i64,
    rg: ?schema.RowGroup,
};

/// One output column. `passthrough` re-encodes a kept input column;
/// `computed` evaluates an expression against the (post-filter) decoded
/// batch and encodes the result as a new flat leaf.
pub const OutputCol = union(enum) {
    passthrough: usize,
    computed: Computed,

    pub const Computed = struct {
        expr: expr_ast.Expr,
        /// Output column name. The caller's footer-build step uses
        /// the same string as the leaf SchemaElement's name.
        alias: []const u8,
    };
};

/// Decode → filter → re-encode kept columns → write to `sink`.
///
/// `out_arena` lifetime: holds the new `RGOut.rg` metadata + encoded
/// chunk bytes after the function returns. Typically the per-invocation
/// arena that also owns the output footer.
///
/// `gpa` lifetime: backs the per-RG decode/eval scratch arena. Freed
/// before this function returns.
pub fn encodeRG(
    out_arena: std.mem.Allocator,
    gpa: std.mem.Allocator,
    rg: *const schema.RowGroup,
    meta: *const schema.FileMetaData,
    src: RGSrc,
    /// Optional filter. When null, every row in the RG survives — used
    /// by the projection-only and `--select`-with-computed-only paths.
    filter: ?filter_ast.Filter,
    fetch_set: []const bool,
    output_specs: []const OutputCol,
    sink: streaming.Sink,
    out_offset: *u64,
    output_codec: schema.CompressionCodec,
    timings: *Timings,
) !RGOut {
    const num_rows: usize = @intCast(rg.num_rows);
    const num_leaves = rg.columns.items.len;

    var rg_arena_state = std.heap.ArenaAllocator.init(gpa);
    defer rg_arena_state.deinit();
    const ra = rg_arena_state.allocator();

    var batch_cols: std.ArrayList(filter_eval.Batch.Column) = .empty;
    var lookup = try ra.alloc(?usize, meta.schema.items.len);
    @memset(lookup, null);

    var batch_pos_for_col = try ra.alloc(?usize, num_leaves);
    @memset(batch_pos_for_col, null);

    const t_decode_start = nowMonoNs();
    for (fetch_set, 0..) |needed, ci| {
        if (!needed) continue;
        const col = &rg.columns.items[ci];
        const cm = col.meta_data orelse return error.ColumnMetaMissing;
        const start: usize = if (cm.dictionary_page_offset) |dp| @intCast(dp) else @intCast(cm.data_page_offset);
        const len: usize = @intCast(cm.total_compressed_size);
        const buf_off = start - @as(usize, @intCast(src.byte_origin));
        if (buf_off + len > src.bytes.len) return error.MissingChunkBytes;
        const chunk = src.bytes[buf_off .. buf_off + len];

        const levels = meta.getColumnLevels(cm.path_in_schema.items);
        const n_leaves: usize = @intCast(cm.num_values);

        const decoded: filter_eval.Batch.Column = switch (cm.type) {
            .INT32 => .{ .i32 = try decodeColumnT(i32, ra, chunk, cm.codec, levels, n_leaves) },
            .INT64 => .{ .i64 = try decodeColumnT(i64, ra, chunk, cm.codec, levels, n_leaves) },
            .FLOAT => .{ .f32 = try decodeColumnT(f32, ra, chunk, cm.codec, levels, n_leaves) },
            .DOUBLE => .{ .f64 = try decodeColumnT(f64, ra, chunk, cm.codec, levels, n_leaves) },
            .BYTE_ARRAY => .{ .string = try decodeColumnT([]const u8, ra, chunk, cm.codec, levels, n_leaves) },
            .BOOLEAN => .{ .boolean = try decodeColumnT(bool, ra, chunk, cm.codec, levels, n_leaves) },
            else => return error.UnsupportedColumnType,
        };
        batch_pos_for_col[ci] = batch_cols.items.len;
        lookup[ci] = batch_cols.items.len;
        try batch_cols.append(ra, decoded);
    }
    const t_decode_end = nowMonoNs();
    timings.decode_ns += @intCast(t_decode_end - t_decode_start);

    const batch: filter_eval.Batch = .{ .cols = batch_cols.items, .num_rows = num_rows };
    var sel = try filter_selection.SelectionVector.init(ra, num_rows);
    if (filter) |f| try filter_eval.evaluate(f, &batch, &sel, lookup, ra);
    const t_eval_end = nowMonoNs();
    timings.eval_ns += @intCast(t_eval_end - t_decode_end);

    const surviving = sel.count();
    if (surviving == 0) return .{ .surviving_rows = 0, .rg = null };

    var rg_columns: std.ArrayListUnmanaged(schema.ColumnChunk) = .empty;
    try rg_columns.ensureTotalCapacity(out_arena, output_specs.len);
    var rg_total: i64 = 0;

    // Encode every output column in parallel into per-task arenas, then
    // walk the results sequentially to push bytes to the sink and
    // record offsets in deterministic order. Lambda has 2 vCPUs so we
    // cap workers at min(N, cpus); the encode step on balanced /
    // broad workloads is ~360-700ms single-threaded and dominates the
    // remaining gap to polars.
    const n_specs = output_specs.len;
    const cpus = std.Thread.getCpuCount() catch 2;
    const num_workers: usize = @min(n_specs, @max(@as(usize, 2), cpus));

    const results = try ra.alloc(?encoder.EncodedColumn, n_specs);
    @memset(results, null);

    // Per-task arenas are declared here so their deinit fires AFTER
    // the write phase below — `enc.meta`'s inner slices live in these
    // arenas and are deep-cloned into out_arena during the write
    // phase. Allocating only when num_workers > 1 keeps the
    // single-thread fast path free of arena init overhead.
    var task_arenas: ?[]std.heap.ArenaAllocator = null;
    defer if (task_arenas) |arenas| {
        for (arenas) |*ar| ar.deinit();
    };

    const t_enc_start = nowMonoNs();
    if (num_workers <= 1) {
        // Trivial path: single-threaded fallback. Avoids the spawn
        // cost when there's nothing to parallelize.
        for (output_specs, 0..) |spec, i| {
            results[i] = try encodeOneSpec(ra, spec, rg, meta, &batch, &sel, lookup, batch_pos_for_col, output_codec);
        }
    } else {
        const arenas = try ra.alloc(std.heap.ArenaAllocator, num_workers);
        for (arenas) |*ar| ar.* = std.heap.ArenaAllocator.init(gpa);
        task_arenas = arenas;

        const ctxs = try ra.alloc(EncodeWorkerCtx, num_workers);
        const threads = try ra.alloc(std.Thread, num_workers);
        for (0..num_workers) |w| {
            ctxs[w] = .{
                .arena = arenas[w].allocator(),
                .start = w,
                .stride = num_workers,
                .output_specs = output_specs,
                .rg = rg,
                .meta = meta,
                .batch = &batch,
                .sel = &sel,
                .lookup = lookup,
                .batch_pos_for_col = batch_pos_for_col,
                .output_codec = output_codec,
                .results = results,
                .err = null,
            };
            threads[w] = try std.Thread.spawn(.{}, encodeWorker, .{&ctxs[w]});
        }
        for (threads) |t| t.join();
        for (ctxs) |c| if (c.err) |err| return err;
    }
    timings.encode_ns += @intCast(nowMonoNs() - t_enc_start);

    // Sequential write phase — bytes go to sink in deterministic
    // output order, even though parallel workers produced them
    // out-of-order. `sink.write` copies bytes into the sink's own
    // buffer so per-task arenas can be deinited at function return.
    // ColumnMetaData inner slices live in per-task arenas; deep-clone
    // into out_arena so they outlive the function (footer references
    // them).
    for (results, 0..) |maybe_enc, i| {
        _ = i;
        const enc = maybe_enc orelse return error.MissingEncodeResult;
        const col_start_in_file: i64 = @intCast(out_offset.*);
        var em = try cloneColumnMeta(out_arena, enc.meta);
        em.data_page_offset += col_start_in_file;
        if (em.dictionary_page_offset) |dpo| em.dictionary_page_offset = dpo + col_start_in_file;
        const t_sink_start = nowMonoNs();
        try sink.write(enc.bytes);
        timings.sink_ns += @intCast(nowMonoNs() - t_sink_start);
        out_offset.* += enc.bytes.len;
        rg_total += @intCast(enc.bytes.len);

        try rg_columns.append(out_arena, .{
            .file_path = null,
            .file_offset = col_start_in_file,
            .meta_data = em,
        });
    }

    return .{
        .surviving_rows = @intCast(surviving),
        .rg = .{
            .columns = rg_columns,
            .total_byte_size = rg_total,
            .num_rows = @intCast(surviving),
        },
    };
}

const EncodeWorkerCtx = struct {
    arena: std.mem.Allocator,
    /// First spec index this worker handles. Worker iterates
    /// `start, start + stride, start + 2*stride, ...` until end.
    start: usize,
    stride: usize,
    output_specs: []const OutputCol,
    rg: *const schema.RowGroup,
    meta: *const schema.FileMetaData,
    /// Read-only view of the decoded batch (shared across workers).
    batch: *const filter_eval.Batch,
    sel: *const filter_selection.SelectionVector,
    lookup: []const ?usize,
    batch_pos_for_col: []const ?usize,
    output_codec: schema.CompressionCodec,
    /// Output slot per spec index. Workers write only their own
    /// indices; no synchronization needed across slots.
    results: []?encoder.EncodedColumn,
    err: ?anyerror,
};

fn encodeWorker(ctx: *EncodeWorkerCtx) void {
    var i = ctx.start;
    while (i < ctx.output_specs.len) : (i += ctx.stride) {
        ctx.results[i] = encodeOneSpec(
            ctx.arena,
            ctx.output_specs[i],
            ctx.rg,
            ctx.meta,
            ctx.batch,
            ctx.sel,
            ctx.lookup,
            ctx.batch_pos_for_col,
            ctx.output_codec,
        ) catch |err| {
            ctx.err = err;
            return;
        };
    }
}

/// Encode one output spec into `arena`. Identical work to the old
/// inline body — extracted so both the single-threaded fallback and
/// the parallel worker can share it.
fn encodeOneSpec(
    arena: std.mem.Allocator,
    spec: OutputCol,
    rg: *const schema.RowGroup,
    meta: *const schema.FileMetaData,
    batch: *const filter_eval.Batch,
    sel: *const filter_selection.SelectionVector,
    lookup: []const ?usize,
    batch_pos_for_col: []const ?usize,
    output_codec: schema.CompressionCodec,
) !encoder.EncodedColumn {
    var path_in_schema: []const []const u8 = undefined;
    var leaf_elem: schema.SchemaElement = undefined;
    var filtered: filter_eval.Batch.Column = undefined;

    switch (spec) {
        .passthrough => |kept_ci| {
            const batch_pos = batch_pos_for_col[kept_ci] orelse return error.MissingDecodedColumn;
            filtered = try encoder.applySelection(arena, batch.cols[batch_pos], sel);
            const cm = rg.columns.items[kept_ci].meta_data orelse return error.ColumnMetaMissing;
            leaf_elem = meta.getColumnSchema(cm.path_in_schema.items) orelse return error.SchemaLookupFailed;
            path_in_schema = cm.path_in_schema.items;
        },
        .computed => |c| {
            const result = try expr_eval.evalExpr(arena, batch, lookup, c.expr);
            filtered = try encoder.applySelection(arena, result, sel);
            const path_buf = try arena.alloc([]const u8, 1);
            path_buf[0] = c.alias;
            path_in_schema = path_buf;
            const expr_type = c.expr.typeOf();
            leaf_elem = .{
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
            };
        },
    }

    return try encoder.encodeColumn(arena, .{
        .values = filtered,
        .schema_elem = &leaf_elem,
        .path_in_schema = path_in_schema,
        .codec = output_codec,
    });
}

/// Deep-clone a ColumnMetaData's inner slices into `arena`. Used by
/// the parallel-encode write phase to migrate metadata out of per-task
/// arenas (which get freed at end of RG processing) into the longer-
/// lived `out_arena` (which survives until footer write).
///
/// What gets duped:
///   - `encodings.items` and `path_in_schema.items` containers (POD
///     element arrays — safe to copy header-by-header). The strings
///     inside path_in_schema already live in long-lived storage
///     (source-meta arena for passthrough, parser arena for computed).
///   - `statistics.{min,max,min_value,max_value}` byte slices: for
///     BYTE_ARRAY columns these point into per-task arena, so we
///     `arena.dupe`. For numeric stats they're page_allocator-owned
///     and could be borrowed, but uniform dupe keeps the code simple.
fn cloneColumnMeta(
    arena: std.mem.Allocator,
    src: schema.ColumnMetaData,
) !schema.ColumnMetaData {
    var encodings: schema.EncodingList = .empty;
    try encodings.appendSlice(arena, src.encodings.items);

    var path_list: schema.StringList = .empty;
    try path_list.appendSlice(arena, src.path_in_schema.items);

    var stats: ?schema.Statistics = null;
    if (src.statistics) |st| {
        stats = .{
            .max = if (st.max) |v| try arena.dupe(u8, v) else null,
            .min = if (st.min) |v| try arena.dupe(u8, v) else null,
            .null_count = st.null_count,
            .distinct_count = st.distinct_count,
            .max_value = if (st.max_value) |v| try arena.dupe(u8, v) else null,
            .min_value = if (st.min_value) |v| try arena.dupe(u8, v) else null,
        };
    }

    return .{
        .type = src.type,
        .encodings = encodings,
        .path_in_schema = path_list,
        .codec = src.codec,
        .num_values = src.num_values,
        .total_uncompressed_size = src.total_uncompressed_size,
        .total_compressed_size = src.total_compressed_size,
        .data_page_offset = src.data_page_offset,
        .index_page_offset = src.index_page_offset,
        .dictionary_page_offset = src.dictionary_page_offset,
        .statistics = stats,
    };
}

/// Byte-copy the kept chunks (or the whole RG span) into `sink` and
/// emit a cloned `schema.RowGroup` with offsets shifted to the new
/// stream position.
///
/// `kept_set = null`: no projection — push `src.bytes` (which the
/// caller has arranged to be the whole-RG bounding-box span) and
/// shift every chunk by the same delta.
///
/// `kept_set != null`: per-column copy. Only chunks where
/// `kept_set[ci] == true` are written.
pub fn copyRG(
    out_arena: std.mem.Allocator,
    rg: *const schema.RowGroup,
    src: RGSrc,
    kept_set: ?[]const bool,
    sink: streaming.Sink,
    out_offset: *u64,
    timings: *Timings,
) !RGOut {
    if (kept_set) |kept| {
        var new_cols: std.ArrayListUnmanaged(schema.ColumnChunk) = .empty;
        try new_cols.ensureTotalCapacity(out_arena, rg.columns.items.len);
        var rg_total: i64 = 0;

        for (kept, 0..) |b, ci| {
            if (!b) continue;
            if (ci >= rg.columns.items.len) return error.BadColumnIndex;
            const src_chunk = rg.columns.items[ci];
            const m = src_chunk.meta_data orelse return error.InvalidColumnOffsets;
            const src_start: usize = if (m.dictionary_page_offset) |d| @intCast(d) else @intCast(m.data_page_offset);
            const src_len: usize = @intCast(m.total_compressed_size);
            const buf_off = src_start - @as(usize, @intCast(src.byte_origin));
            if (buf_off + src_len > src.bytes.len) return error.MissingChunkBytes;
            const slice = src.bytes[buf_off .. buf_off + src_len];

            const new_col_start = out_offset.*;
            const t_sink_start = nowMonoNs();
            try sink.write(slice);
            timings.sink_ns += @intCast(nowMonoNs() - t_sink_start);
            out_offset.* += src_len;

            const delta: i64 = @as(i64, @intCast(new_col_start)) - @as(i64, @intCast(src_start));
            try new_cols.append(out_arena, shiftChunk(src_chunk, delta));
            rg_total += @intCast(src_len);
        }

        return .{
            .surviving_rows = rg.num_rows,
            .rg = .{
                .columns = new_cols,
                .total_byte_size = rg_total,
                .num_rows = rg.num_rows,
            },
        };
    }

    // No projection: caller arranged for `src.bytes` to be the whole
    // RG bounding-box [min_col_start, max_col_end). One sink.write,
    // shift every chunk by a single delta.
    const new_start = out_offset.*;
    const t_sink_start = nowMonoNs();
    try sink.write(src.bytes);
    timings.sink_ns += @intCast(nowMonoNs() - t_sink_start);
    out_offset.* += src.bytes.len;
    const delta: i64 = @as(i64, @intCast(new_start)) - @as(i64, @intCast(src.byte_origin));

    var cols: std.ArrayListUnmanaged(schema.ColumnChunk) = .empty;
    try cols.ensureTotalCapacity(out_arena, rg.columns.items.len);
    for (rg.columns.items) |chunk| {
        try cols.append(out_arena, shiftChunk(chunk, delta));
    }

    return .{
        .surviving_rows = rg.num_rows,
        .rg = .{
            .columns = cols,
            .total_byte_size = rg.total_byte_size,
            .num_rows = rg.num_rows,
        },
    };
}

/// Clone a ColumnChunk with all in-stream offsets shifted by `delta`,
/// dropping the offset/column-index pointers (we don't currently
/// re-emit page indexes — caller's footer omits them).
fn shiftChunk(src: schema.ColumnChunk, delta: i64) schema.ColumnChunk {
    var out = src;
    out.offset_index_offset = null;
    out.offset_index_length = null;
    out.column_index_offset = null;
    out.column_index_length = null;
    if (out.meta_data) |*m| {
        m.data_page_offset += delta;
        if (m.dictionary_page_offset) |d| m.dictionary_page_offset = d + delta;
        if (m.index_page_offset) |d| m.index_page_offset = d + delta;
    }
    if (out.meta_data) |m| out.file_offset = m.data_page_offset;
    return out;
}

/// Decode an entire column chunk into a `ColumnT(T)` view: values
/// plus def_levels (OPTIONAL) and optionally rep_levels (LIST/MAP).
/// `num_leaves` is the column chunk's `num_values` from metadata
/// (counts LEAVES, not logical rows — for nested cols this can be
/// larger than the RG's row count).
pub fn decodeColumnT(
    comptime T: type,
    arena: std.mem.Allocator,
    chunk: []const u8,
    codec: schema.CompressionCodec,
    levels: schema.Levels,
    num_leaves: usize,
) !filter_eval.ColumnT(T) {
    const values = try arena.alloc(T, num_leaves);
    var reader = column_mod.ColumnChunkReader(T).init(chunk, codec, levels, arena);
    if (levels.max_rep > 0) {
        const def_levels = try arena.alloc(u32, num_leaves);
        const rep_levels = try arena.alloc(u32, num_leaves);
        var written: usize = 0;
        while (written < num_leaves) {
            const n = reader.decodeWithRepLevels(values[written..], def_levels[written..], rep_levels[written..]) catch return error.ShortDecode;
            if (n == 0) break;
            written += n;
        }
        if (written != num_leaves) return error.ShortDecode;
        return .{
            .values = values,
            .def_levels = def_levels,
            .max_def = @intCast(levels.max_def),
            .rep_levels = rep_levels,
            .max_rep = @intCast(levels.max_rep),
        };
    }
    if (levels.max_def > 0) {
        const def_levels = try arena.alloc(u32, num_leaves);
        var written: usize = 0;
        while (written < num_leaves) {
            const n = reader.decodeWithLevels(values[written..], def_levels[written..]) catch return error.ShortDecode;
            if (n == 0) break;
            written += n;
        }
        if (written != num_leaves) return error.ShortDecode;
        return .{ .values = values, .def_levels = def_levels, .max_def = @intCast(levels.max_def) };
    }
    var written: usize = 0;
    while (written < num_leaves) {
        const n = reader.decode(values[written..]) catch return error.ShortDecode;
        if (n == 0) break;
        written += n;
    }
    if (written != num_leaves) return error.ShortDecode;
    return .{ .values = values };
}

fn nowMonoNs() i64 {
    var ts: std.os.linux.timespec = .{ .sec = 0, .nsec = 0 };
    _ = std.os.linux.clock_gettime(.MONOTONIC, &ts);
    return @as(i64, ts.sec) * std.time.ns_per_s + @as(i64, ts.nsec);
}

// ============================================================
// Compile-time API check. End-to-end behavior is exercised through
// the lambda integration tests + CLI golden tests.
// ============================================================
test "consumer: API is well-typed" {
    _ = encodeRG;
    _ = copyRG;
    _ = decodeColumnT;
    _ = shiftChunk;
}
