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
const filter_prune = @import("filter/prune.zig");
const expr_ast = @import("expr/ast.zig");
const expr_eval = @import("expr/eval.zig");
const expr_agg = @import("expr/agg.zig");
const encoder = @import("writer/encoder.zig");
const streaming = @import("writer/streaming.zig");
const fastpath = @import("writer/fastpath.zig");
const thrift = @import("thrift.zig");
const column_mod = @import("parquet/column.zig");
const decimal_mod = @import("parquet/decimal.zig");
const invariant = @import("invariant.zig");
const int96_mod = @import("parquet/int96.zig");

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

/// Shift every absolute file offset in a row group's column metadata by
/// `base`. Used by the row-group-parallel write path: each RG is encoded
/// into its own buffer with offsets relative to 0, then placed at its
/// final file position `base` and rebased here. Mirrors exactly the
/// `+= out_offset.*` arithmetic `encodeAggregator` does inline when it
/// writes straight to the sink.
pub fn rebaseRowGroupOffsets(rg: *schema.RowGroup, base: i64) void {
    for (rg.columns.items) |*col| {
        col.file_offset += base;
        if (col.column_index_offset) |v| col.column_index_offset = v + base;
        if (col.offset_index_offset) |v| col.offset_index_offset = v + base;
        if (col.meta_data) |*md| {
            md.data_page_offset += base;
            if (md.dictionary_page_offset) |dp| md.dictionary_page_offset = dp + base;
        }
    }
}

/// Accumulates filtered+projected output columns across multiple input
/// row groups so the encoder can emit one large output RG.
///
/// Why: snappy compresses DOUBLE PLAIN pages ~50% on 120K-row pages
/// but only ~75% on 25K-row pages. Same data, smaller blocks lose
/// hash-table context across page boundaries. Per-RG output (one
/// page-per-chunk) bloats output significantly on wide-DOUBLE tables;
/// aggregating into a single ~250K-row output RG closes the gap.
///
/// Memory cost: O(num_rows × sum(col_value_size + 4_bytes_def_level)).
/// For 250K rows × 10 cols × ~15 bytes avg, ~37 MB peak.
pub const OutputAggregator = struct {
    arena: std.mem.Allocator,
    cols: []ColumnBuf,
    /// One per output spec. Captured on first append:
    /// - passthrough → source RG's leaf schema element (borrowed from
    ///   meta, which outlives the aggregator).
    /// - computed   → synthesized schema element, alias arena-dupe'd.
    schema_elems: []schema.SchemaElement,
    paths: [][]const []const u8,
    num_rows: usize,
    captured: bool,

    pub const ColumnBuf = union(enum) {
        i32: TypedBuf(i32),
        i64: TypedBuf(i64),
        f32: TypedBuf(f32),
        f64: TypedBuf(f64),
        string: TypedBuf([]const u8),
        boolean: TypedBuf(bool),
        /// Lossless DECIMAL output lane: unscaled integers (widened to
        /// i128). Only INT32/INT64-backed decimals use it (D1); the buf
        /// is converted back to an i32/i64 column at encode time, with
        /// the source DECIMAL schema element preserved.
        decimal: TypedBuf(i128),
    };

    pub fn TypedBuf(comptime T: type) type {
        return struct {
            values: std.ArrayList(T) = .empty,
            def_levels: std.ArrayList(u32) = .empty,
            max_def: u32 = 0,
        };
    }
};

/// Build an empty aggregator sized to `output_specs`. Column types
/// are pre-built from the source meta (passthrough) or expression
/// type (computed) so per-RG appends just push into typed buffers.
pub fn initOutputAggregator(
    arena: std.mem.Allocator,
    meta: *const schema.FileMetaData,
    output_specs: []const OutputCol,
) !OutputAggregator {
    const cols = try arena.alloc(OutputAggregator.ColumnBuf, output_specs.len);
    const schemas = try arena.alloc(schema.SchemaElement, output_specs.len);
    const paths = try arena.alloc([]const []const u8, output_specs.len);

    for (output_specs, 0..) |spec, i| {
        const phys_type: schema.Type = switch (spec) {
            .passthrough => |kept_ci| blk: {
                // Find which leaf-column slot has this path. row_groups[*].columns[kept_ci]
                // gives us the path; meta.getColumnSchema returns the SchemaElement.
                // We use the first RG's metadata to look up the leaf — every RG of
                // the same file shares the same schema.
                if (meta.row_groups.items.len == 0) return error.ColumnMetaMissing;
                const cm = meta.row_groups.items[0].columns.items[kept_ci].meta_data orelse
                    return error.ColumnMetaMissing;
                const src_elem = meta.getColumnSchema(cm.path_in_schema.items) orelse
                    return error.SchemaLookupFailed;
                paths[i] = cm.path_in_schema.items;

                // DECIMAL source columns. INT32/INT64/FLBA-backed take the
                // lossless integer lane: decode to unscaled i128, keep the
                // source DECIMAL schema element, re-encode at the same
                // physical type. Only BYTE_ARRAY-backed DECIMAL (rare)
                // still routes through f64 → DOUBLE. Byte-copy
                // projection (copyRG / fastpath) preserves any DECIMAL.
                if (decimal_mod.kindFromSchema(&src_elem)) |k| {
                    if (k.physical == .INT32 or k.physical == .INT64 or
                        k.physical == .FIXED_LEN_BYTE_ARRAY)
                    {
                        schemas[i] = src_elem; // keep DECIMAL annotation
                        cols[i] = .{ .decimal = .{} };
                        continue;
                    }
                    schemas[i] = .{
                        .type = .DOUBLE,
                        .type_length = null,
                        .repetition_type = src_elem.repetition_type,
                        .name = src_elem.name,
                        .num_children = 0,
                        .converted_type = null,
                        .logical_type = null,
                        .scale = null,
                        .precision = null,
                        .field_id = null,
                    };
                    break :blk schema.Type.DOUBLE;
                }

                schemas[i] = src_elem;
                break :blk cm.type;
            },
            .computed => |c| blk: {
                const expr_type = c.expr.typeOf();
                schemas[i] = .{
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
                const path_buf = try arena.alloc([]const u8, 1);
                path_buf[0] = c.alias;
                paths[i] = path_buf;
                break :blk expr_type.toParquet();
            },
        };

        cols[i] = switch (phys_type) {
            .INT32 => .{ .i32 = .{} },
            .INT64 => .{ .i64 = .{} },
            .INT96 => .{ .i64 = .{} },
            .FLOAT => .{ .f32 = .{} },
            .DOUBLE => .{ .f64 = .{} },
            .BYTE_ARRAY => .{ .string = .{} },
            .BOOLEAN => .{ .boolean = .{} },
            // Non-decimal FLBA decodes to raw bytes → the string lane. (Decode→
            // re-encode emits BYTE_ARRAY; byte-copy passthrough preserves FLBA.)
            .FIXED_LEN_BYTE_ARRAY => .{ .string = .{} },
        };
    }

    return .{
        .arena = arena,
        .cols = cols,
        .schema_elems = schemas,
        .paths = paths,
        .num_rows = 0,
        .captured = true,
    };
}

/// Reset the aggregator's row buffers (keep schema/paths/cols-typing).
/// Used when flushing a partial output RG (row threshold) and continuing.
pub fn resetAggregator(agg: *OutputAggregator) void {
    for (agg.cols) |*cbuf| {
        switch (cbuf.*) {
            inline else => |*tb| {
                tb.values.clearRetainingCapacity();
                tb.def_levels.clearRetainingCapacity();
            },
        }
    }
    agg.num_rows = 0;
}

/// Decode + filter + eval one input RG; project survivors into the
/// aggregator. Uses a per-RG scratch arena that's freed at function
/// return — BYTE_ARRAY slice content is dup'd into `agg.arena` so
/// it outlives the source RG.
///
/// Returns the number of surviving rows added.
pub fn appendProjectedRG(
    agg: *OutputAggregator,
    gpa: std.mem.Allocator,
    rg: *const schema.RowGroup,
    meta: *const schema.FileMetaData,
    src: RGSrc,
    filter: ?filter_ast.Filter,
    fetch_set: []const bool,
    output_specs: []const OutputCol,
    timings: *Timings,
) !usize {
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
        // Programmer invariant: the chunk's absolute start is at or past
        // this byte window's origin, so the subtraction below can't
        // underflow. A malformed file is caught by the bounds error on
        // the next line; this guards our own RGSrc/byte_origin math.
        invariant.assert(start >= @as(usize, @intCast(src.byte_origin)));
        const buf_off = start - @as(usize, @intCast(src.byte_origin));
        if (buf_off + len > src.bytes.len) return error.MissingChunkBytes;
        const chunk = src.bytes[buf_off .. buf_off + len];

        const levels = meta.getColumnLevels(cm.path_in_schema.items);
        const n_leaves: usize = @intCast(cm.num_values);

        // DECIMAL columns: regardless of physical backing (INT32, INT64,
        // FLBA), decode to f64 with scale applied. Reuses the existing
        // f64 column lane through filter / agg / expression.
        const schema_elem = meta.getColumnSchema(cm.path_in_schema.items);
        const dec_kind: ?decimal_mod.Kind = if (schema_elem) |se| decimal_mod.kindFromSchema(&se) else null;

        const decoded: filter_eval.Batch.Column = if (dec_kind) |k| .{
            .f64 = try decimal_mod.decodeColumnAsF64(ra, chunk, cm.codec, levels, n_leaves, k),
        } else if (schema_elem != null and schema.isFloat16(schema_elem.?)) .{
            // FLOAT16 (FLBA(2), IEEE half) → f64 lane for numeric agg/filter.
            .f64 = try decodeFloat16ColumnAsF64(ra, chunk, cm.codec, levels, n_leaves),
        } else switch (cm.type) {
            .INT32 => .{ .i32 = try decodeColumnT(i32, ra, chunk, cm.codec, levels, n_leaves) },
            .INT64 => .{ .i64 = try decodeColumnT(i64, ra, chunk, cm.codec, levels, n_leaves) },
            .FLOAT => .{ .f32 = try decodeColumnT(f32, ra, chunk, cm.codec, levels, n_leaves) },
            .DOUBLE => .{ .f64 = try decodeColumnT(f64, ra, chunk, cm.codec, levels, n_leaves) },
            .BYTE_ARRAY => .{ .string = try decodeColumnT([]const u8, ra, chunk, cm.codec, levels, n_leaves) },
            .BOOLEAN => .{ .boolean = try decodeColumnT(bool, ra, chunk, cm.codec, levels, n_leaves) },
            // INT96 (legacy Spark/Impala timestamp) → i64 epoch-nanoseconds.
            .INT96 => .{ .i64 = try int96_mod.decodeColumnAsI64Nanos(ra, chunk, cm.codec, levels, n_leaves) },
            // FIXED_LEN_BYTE_ARRAY (non-decimal — decimal handled above): raw
            // fixed-width bytes (UUID / Float16 / fixed binary).
            .FIXED_LEN_BYTE_ARRAY => .{ .string = try decodeFlbaColumn(ra, chunk, cm.codec, levels, n_leaves, flbaWidth(schema_elem)) },
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
    if (surviving == 0) return 0;

    // For each output spec: produce a post-selection Batch.Column and
    // copy values into the aggregator. BYTE_ARRAY content is duped into
    // agg.arena (slice bytes live on the per-RG arena which is freed
    // when this function returns).
    for (output_specs, 0..) |spec, i| {
        // Lossless DECIMAL lane: the buf is `.decimal` only for an
        // INT32/INT64-backed passthrough decimal. Decode the source
        // chunk to unscaled i128 (a second, cheap decode beside the f64
        // batch copy — see the decode-once follow-up in plan 02), apply
        // the selection, and append the integers untouched.
        if (agg.cols[i] == .decimal) {
            const kept_ci = switch (spec) {
                .passthrough => |ci| ci,
                .computed => return error.MissingDecodedColumn,
            };
            const dcol = try decodeDecimalI128(ra, rg, meta, src, kept_ci);
            const filtered_dec = try encoder.applySelectionI128(ra, dcol, &sel);
            try appendTyped(i128, &agg.cols[i].decimal, filtered_dec.values, filtered_dec.def_levels, filtered_dec.max_def, agg.arena);
            continue;
        }

        var filtered: filter_eval.Batch.Column = undefined;
        switch (spec) {
            .passthrough => |kept_ci| {
                const batch_pos = batch_pos_for_col[kept_ci] orelse return error.MissingDecodedColumn;
                filtered = try encoder.applySelection(ra, batch.cols[batch_pos], &sel);
            },
            .computed => |c| {
                const result = try expr_eval.evalExpr(ra, &batch, lookup, c.expr);
                filtered = try encoder.applySelection(ra, result, &sel);
            },
        }

        try appendIntoBuf(&agg.cols[i], filtered, agg.arena);
    }

    agg.num_rows += surviving;
    return surviving;
}

/// Decode one DECIMAL column's chunk to unscaled i128 integers (the
/// lossless output lane). Re-derives the chunk window the same way the
/// batch-decode loop does. Caller guarantees the column is an
/// INT32/INT64-backed decimal.
fn decodeDecimalI128(
    ra: std.mem.Allocator,
    rg: *const schema.RowGroup,
    meta: *const schema.FileMetaData,
    src: RGSrc,
    kept_ci: usize,
) !filter_eval.ColumnT(i128) {
    const cm = rg.columns.items[kept_ci].meta_data orelse return error.ColumnMetaMissing;
    const start: usize = if (cm.dictionary_page_offset) |dp| @intCast(dp) else @intCast(cm.data_page_offset);
    const len: usize = @intCast(cm.total_compressed_size);
    invariant.assert(start >= @as(usize, @intCast(src.byte_origin)));
    const buf_off = start - @as(usize, @intCast(src.byte_origin));
    if (buf_off + len > src.bytes.len) return error.MissingChunkBytes;
    const chunk = src.bytes[buf_off .. buf_off + len];

    const levels = meta.getColumnLevels(cm.path_in_schema.items);
    const n_leaves: usize = @intCast(cm.num_values);
    const se = meta.getColumnSchema(cm.path_in_schema.items) orelse return error.SchemaLookupFailed;
    const kind = decimal_mod.kindFromSchema(&se) orelse return error.SchemaLookupFailed;
    return decimal_mod.decodeColumnAsI128(ra, chunk, cm.codec, levels, n_leaves, kind);
}

/// Synthesize and write a one-page OffsetIndex + ColumnIndex for a
/// freshly-encoded chunk (the encoder emits a single data page per
/// chunk), derived from `meta`'s stats. `abs_data_offset` is the data
/// page's absolute file offset. Returns the new pointers; emits no
/// ColumnIndex when stats lack usable min/max (readers fall back).
fn synthPageIndexToSink(
    out_arena: std.mem.Allocator,
    sink: streaming.Sink,
    out_offset: *u64,
    meta: *const schema.ColumnMetaData,
    abs_data_offset: i64,
) !fastpath.PageIndexPtrs {
    var ptrs = fastpath.PageIndexPtrs{};
    const stats = meta.statistics orelse return ptrs;
    // Data-page byte size = total chunk size minus the dictionary page
    // (the relative data_page_offset is the dict-page span, 0 if none).
    const data_size: i32 = @intCast(meta.total_compressed_size - meta.data_page_offset);
    const null_count = stats.null_count orelse 0;
    const all_null = meta.num_values > 0 and null_count == meta.num_values;

    // ColumnIndex (one page) — needs min/max bytes unless the page is
    // all-null (in which case both are empty per the spec).
    if (all_null or (stats.min_value != null and stats.max_value != null)) {
        var ci = schema.ColumnIndex{ .boundary_order = .UNORDERED };
        try ci.null_pages.append(out_arena, all_null);
        try ci.min_values.append(out_arena, if (all_null) "" else stats.min_value.?);
        try ci.max_values.append(out_arena, if (all_null) "" else stats.max_value.?);
        var ncs: std.ArrayListUnmanaged(i64) = .empty;
        try ncs.append(out_arena, null_count);
        ci.null_counts = ncs;

        var w = thrift.Writer.init(out_arena);
        try ci.write(&w);
        const bytes = w.bytes();
        ptrs.column_index_offset = @intCast(out_offset.*);
        try sink.write(bytes);
        out_offset.* += bytes.len;
        ptrs.column_index_length = @intCast(bytes.len);
    }

    // OffsetIndex (one data page).
    {
        var oi = schema.OffsetIndex{};
        try oi.page_locations.append(out_arena, .{
            .offset = abs_data_offset,
            .compressed_page_size = data_size,
            .first_row_index = 0,
        });
        var w = thrift.Writer.init(out_arena);
        try oi.write(&w);
        const bytes = w.bytes();
        ptrs.offset_index_offset = @intCast(out_offset.*);
        try sink.write(bytes);
        out_offset.* += bytes.len;
        ptrs.offset_index_length = @intCast(bytes.len);
    }
    return ptrs;
}

fn appendIntoBuf(
    buf: *OutputAggregator.ColumnBuf,
    src: filter_eval.Batch.Column,
    dupe_arena: std.mem.Allocator,
) !void {
    switch (src) {
        .i32 => |c| try appendTyped(i32, &buf.i32, c.values, c.def_levels, c.max_def, dupe_arena),
        .i64 => |c| try appendTyped(i64, &buf.i64, c.values, c.def_levels, c.max_def, dupe_arena),
        .f32 => |c| try appendTyped(f32, &buf.f32, c.values, c.def_levels, c.max_def, dupe_arena),
        .f64 => |c| try appendTyped(f64, &buf.f64, c.values, c.def_levels, c.max_def, dupe_arena),
        .boolean => |c| try appendTyped(bool, &buf.boolean, c.values, c.def_levels, c.max_def, dupe_arena),
        .string => |c| {
            // Dupe slice bytes into agg arena so they outlive the source RG.
            const duped = try dupe_arena.alloc([]const u8, c.values.len);
            for (c.values, 0..) |v, i| duped[i] = try dupe_arena.dupe(u8, v);
            try appendTyped([]const u8, &buf.string, duped, c.def_levels, c.max_def, dupe_arena);
        },
    }
}

fn appendTyped(
    comptime T: type,
    tb: anytype,
    values: []const T,
    def_levels: ?[]const u32,
    max_def: u32,
    arena: std.mem.Allocator,
) !void {
    try tb.values.appendSlice(arena, values);
    if (max_def > 0) {
        tb.max_def = max_def;
        const dl = def_levels orelse {
            // Source was REQUIRED-as-dense but agg's max_def says optional.
            // Emit max_def for every row (all present).
            const n = values.len;
            const ones = try arena.alloc(u32, n);
            @memset(ones, max_def);
            try tb.def_levels.appendSlice(arena, ones);
            return;
        };
        try tb.def_levels.appendSlice(arena, dl);
    }
}

/// Encode the aggregator's accumulated rows as ONE output row group,
/// write to sink, and return RGOut. Caller should `resetAggregator()`
/// afterward if continuing. Returns null RG when num_rows == 0.
pub fn encodeAggregator(
    agg: *OutputAggregator,
    out_arena: std.mem.Allocator,
    gpa: std.mem.Allocator,
    sink: streaming.Sink,
    out_offset: *u64,
    output_codec: schema.CompressionCodec,
    timings: *Timings,
    // Column-encode parallelism: 0 = auto (min(n_cols, cpus)). Pass 1 to
    // force serial column encode when the CALLER is already parallel across
    // row groups, so we don't oversubscribe (N_rg_workers × N_col_workers).
    encode_workers: usize,
) !RGOut {
    if (agg.num_rows == 0) return .{ .surviving_rows = 0, .rg = null };

    var rg_arena_state = std.heap.ArenaAllocator.init(gpa);
    defer rg_arena_state.deinit();
    const ra = rg_arena_state.allocator();

    const n_specs = agg.cols.len;

    // Synthesize a Batch.Column per output col from the aggregator's
    // typed buffers. Lifetimes: these point into agg.arena.
    const batch_cols = try ra.alloc(filter_eval.Batch.Column, n_specs);
    for (agg.cols, 0..) |cbuf, i| {
        batch_cols[i] = switch (cbuf) {
            .i32 => |b| .{ .i32 = .{
                .values = b.values.items,
                .def_levels = if (b.max_def > 0) b.def_levels.items else null,
                .max_def = b.max_def,
            } },
            .i64 => |b| .{ .i64 = .{
                .values = b.values.items,
                .def_levels = if (b.max_def > 0) b.def_levels.items else null,
                .max_def = b.max_def,
            } },
            .f32 => |b| .{ .f32 = .{
                .values = b.values.items,
                .def_levels = if (b.max_def > 0) b.def_levels.items else null,
                .max_def = b.max_def,
            } },
            .f64 => |b| .{ .f64 = .{
                .values = b.values.items,
                .def_levels = if (b.max_def > 0) b.def_levels.items else null,
                .max_def = b.max_def,
            } },
            .string => |b| .{ .string = .{
                .values = b.values.items,
                .def_levels = if (b.max_def > 0) b.def_levels.items else null,
                .max_def = b.max_def,
            } },
            .boolean => |b| .{ .boolean = .{
                .values = b.values.items,
                .def_levels = if (b.max_def > 0) b.def_levels.items else null,
                .max_def = b.max_def,
            } },
            // Lossless DECIMAL: narrow the unscaled i128 back to its
            // source physical width (exact — the values were decoded
            // from i32/i64 and widened) and feed the existing integer
            // encoder. The kept DECIMAL schema element makes the footer
            // (and `meta.type`) carry the logical type through.
            .decimal => |b| blk: {
                const phys = agg.schema_elems[i].type orelse return error.UnsupportedColumnType;
                const dl = if (b.max_def > 0) b.def_levels.items else null;
                switch (phys) {
                    .INT32 => {
                        const vals = try ra.alloc(i32, b.values.items.len);
                        for (b.values.items, 0..) |v, j| vals[j] = @intCast(v);
                        break :blk .{ .i32 = .{ .values = vals, .def_levels = dl, .max_def = b.max_def } };
                    },
                    .INT64 => {
                        const vals = try ra.alloc(i64, b.values.items.len);
                        for (b.values.items, 0..) |v, j| vals[j] = @intCast(v);
                        break :blk .{ .i64 = .{ .values = vals, .def_levels = dl, .max_def = b.max_def } };
                    },
                    // FLBA decimals can't ride an integer lane — placeholder
                    // here; the pre-pass below encodes them via encodeDecimalFlba.
                    .FIXED_LEN_BYTE_ARRAY => break :blk .{ .i64 = .{ .values = &.{} } },
                    else => return error.UnsupportedColumnType,
                }
            },
        };
    }

    // Parallel encode of all output columns. Same shape as the per-RG
    // path: spawn workers up to min(n_specs, cpus), each pulls a slice
    // of indices via stride. Results indexed by spec position so the
    // sequential write phase below preserves output order.
    const cpus = std.Thread.getCpuCount() catch 2;
    const auto_workers: usize = @min(n_specs, @max(@as(usize, 2), cpus));
    const num_workers: usize = if (encode_workers > 0) @min(n_specs, encode_workers) else auto_workers;

    const results = try ra.alloc(?encoder.EncodedColumn, n_specs);
    @memset(results, null);

    var task_arenas: ?[]std.heap.ArenaAllocator = null;
    defer if (task_arenas) |arenas| {
        for (arenas) |*ar| ar.deinit();
    };

    // Pre-pass: FLBA-backed decimals don't fit a Batch.Column lane, so
    // encode them directly (a small subset; serial is fine). The encode
    // loops below skip any index already filled here.
    for (agg.cols, 0..) |cbuf, i| {
        if (cbuf == .decimal and agg.schema_elems[i].type == .FIXED_LEN_BYTE_ARRAY) {
            const b = cbuf.decimal;
            results[i] = try encoder.encodeDecimalFlba(ra, .{
                .values = b.values.items,
                .def_levels = if (b.max_def > 0) b.def_levels.items else null,
                .max_def = b.max_def,
            }, &agg.schema_elems[i], agg.paths[i], output_codec);
        }
    }

    const t_enc_start = nowMonoNs();
    if (num_workers <= 1) {
        for (0..n_specs) |i| {
            if (results[i] != null) continue; // FLBA decimal done in pre-pass
            results[i] = try encoder.encodeColumn(ra, .{
                .values = batch_cols[i],
                .schema_elem = &agg.schema_elems[i],
                .path_in_schema = agg.paths[i],
                .codec = output_codec,
            });
        }
    } else {
        const arenas = try ra.alloc(std.heap.ArenaAllocator, num_workers);
        for (arenas) |*ar| ar.* = std.heap.ArenaAllocator.init(gpa);
        task_arenas = arenas;

        const ctxs = try ra.alloc(AggEncodeCtx, num_workers);
        const threads = try ra.alloc(std.Thread, num_workers);
        for (0..num_workers) |w| {
            ctxs[w] = .{
                .arena = arenas[w].allocator(),
                .start = w,
                .stride = num_workers,
                .batch_cols = batch_cols,
                .schema_elems = agg.schema_elems,
                .paths = agg.paths,
                .codec = output_codec,
                .results = results,
                .err = null,
            };
            threads[w] = try std.Thread.spawn(.{}, aggEncodeWorker, .{&ctxs[w]});
        }
        for (threads) |t| t.join();
        for (ctxs) |c| if (c.err) |err| return err;
    }
    timings.encode_ns += @intCast(nowMonoNs() - t_enc_start);

    // Sequential write phase — same ordering guarantees as the per-RG
    // path. Deep-clone column metadata into out_arena (which outlives
    // the task arenas).
    var rg_columns: std.ArrayListUnmanaged(schema.ColumnChunk) = .empty;
    try rg_columns.ensureTotalCapacity(out_arena, n_specs);
    var rg_total: i64 = 0;

    for (results) |maybe_enc| {
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

        // Synthesize the page index for this freshly-encoded chunk (one
        // data page → one OffsetIndex + ColumnIndex entry, built from the
        // stats the encoder already computed), written right after the
        // chunk data. Mirrors the byte-copy carry-forward so a filtered
        // re-encode keeps page-level pruning too. (2b)
        const idx = try synthPageIndexToSink(out_arena, sink, out_offset, &enc.meta, em.data_page_offset);

        try rg_columns.append(out_arena, .{
            .file_path = null,
            .file_offset = col_start_in_file,
            .meta_data = em,
            .offset_index_offset = idx.offset_index_offset,
            .offset_index_length = idx.offset_index_length,
            .column_index_offset = idx.column_index_offset,
            .column_index_length = idx.column_index_length,
        });
    }

    return .{
        .surviving_rows = @intCast(agg.num_rows),
        .rg = .{
            .columns = rg_columns,
            .total_byte_size = rg_total,
            .num_rows = @intCast(agg.num_rows),
        },
    };
}

const AggEncodeCtx = struct {
    arena: std.mem.Allocator,
    start: usize,
    stride: usize,
    batch_cols: []const filter_eval.Batch.Column,
    schema_elems: []const schema.SchemaElement,
    paths: []const []const []const u8,
    codec: schema.CompressionCodec,
    results: []?encoder.EncodedColumn,
    err: ?anyerror,
};

fn aggEncodeWorker(ctx: *AggEncodeCtx) void {
    var i = ctx.start;
    while (i < ctx.batch_cols.len) : (i += ctx.stride) {
        if (ctx.results[i] != null) continue; // FLBA decimal done in pre-pass
        ctx.results[i] = encoder.encodeColumn(ctx.arena, .{
            .values = ctx.batch_cols[i],
            .schema_elem = &ctx.schema_elems[i],
            .path_in_schema = ctx.paths[i],
            .codec = ctx.codec,
        }) catch |err| {
            ctx.err = err;
            return;
        };
    }
}


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
/// Write a 1-row parquet file containing one column-chunk per
/// `OutputCol` (avg-style aggs already split into two cols upstream).
/// Used by both CLI and Lambda aggregate paths — the output of an
/// aggregate workload is a tiny parquet that downstream consumers
/// (Iceberg/Athena/Polars/another zpq invocation) read like any
/// other file.
///
/// Returns total bytes written. The sink may be an FdSink (CLI) or
/// MultipartSink (Lambda) — same protocol either way.
pub fn writeOneRowAggregate(
    arena: std.mem.Allocator,
    sink: streaming.Sink,
    output_cols: []const expr_agg.OutputCol,
    codec: schema.CompressionCodec,
) !u64 {
    const MAGIC: [4]u8 = .{ 'P', 'A', 'R', '1' };
    var off: u64 = 0;
    try sink.write(&MAGIC);
    off += MAGIC.len;

    var rg_columns: std.ArrayListUnmanaged(schema.ColumnChunk) = .empty;
    var rg_total: i64 = 0;
    for (output_cols) |out_col| {
        const path_buf = try arena.alloc([]const u8, 1);
        path_buf[0] = out_col.name;
        const leaf_elem: schema.SchemaElement = .{
            .type = out_col.parquet_type,
            .type_length = null,
            .repetition_type = .REQUIRED,
            .name = out_col.name,
            .num_children = 0,
            .converted_type = null,
            .logical_type = null,
            .scale = null,
            .precision = null,
            .field_id = null,
        };
        const enc = try encoder.encodeColumn(arena, .{
            .values = out_col.col,
            .schema_elem = &leaf_elem,
            .path_in_schema = path_buf,
            .codec = codec,
        });
        const col_start: i64 = @intCast(off);
        var em = enc.meta;
        em.data_page_offset += col_start;
        if (em.dictionary_page_offset) |dpo| em.dictionary_page_offset = dpo + col_start;
        try sink.write(enc.bytes);
        off += enc.bytes.len;
        rg_total += @intCast(enc.bytes.len);
        try rg_columns.append(arena, .{
            .file_path = null,
            .file_offset = col_start,
            .meta_data = em,
        });
    }

    var new_schema: std.ArrayListUnmanaged(schema.SchemaElement) = .empty;
    try new_schema.append(arena, .{
        .type = null,
        .type_length = null,
        .repetition_type = null,
        .name = "schema",
        .num_children = @intCast(output_cols.len),
        .converted_type = null,
        .logical_type = null,
        .scale = null,
        .precision = null,
        .field_id = null,
    });
    for (output_cols) |out_col| {
        try new_schema.append(arena, .{
            .type = out_col.parquet_type,
            .type_length = null,
            .repetition_type = .REQUIRED,
            .name = out_col.name,
            .num_children = 0,
            .converted_type = null,
            .logical_type = null,
            .scale = null,
            .precision = null,
            .field_id = null,
        });
    }

    var new_row_groups: std.ArrayListUnmanaged(schema.RowGroup) = .empty;
    try new_row_groups.append(arena, .{
        .columns = rg_columns,
        .total_byte_size = rg_total,
        .num_rows = 1,
    });

    const new_meta: schema.FileMetaData = .{
        .version = 1,
        .schema = new_schema,
        .num_rows = 1,
        .created_by = null,
        .row_groups = new_row_groups,
    };

    var w: thrift.Writer = .init(arena);
    defer w.deinit();
    try new_meta.write(&w);
    const footer_bytes = w.bytes();
    try sink.write(footer_bytes);
    off += footer_bytes.len;

    var len_bytes: [4]u8 = undefined;
    std.mem.writeInt(u32, &len_bytes, @intCast(footer_bytes.len), .little);
    try sink.write(&len_bytes);
    off += len_bytes.len;
    try sink.write(&MAGIC);
    off += MAGIC.len;

    return off;
}

const ConjunctiveLeaf = struct {
    filter: filter_ast.Filter,
    col_idx: usize,
};

fn collectConjunctiveLeaves(f: filter_ast.Filter, list: *std.ArrayList(ConjunctiveLeaf), allocator: std.mem.Allocator) !void {
    switch (f) {
        .and_filter => |c| {
            try collectConjunctiveLeaves(c.left.*, list, allocator);
            try collectConjunctiveLeaves(c.right.*, list, allocator);
        },
        .or_filter => {},
        else => {
            const col_idx = switch (f) {
                .int32 => |l| l.col_idx,
                .int64 => |l| l.col_idx,
                .float => |l| l.col_idx,
                .double => |l| l.col_idx,
                .string => |l| l.col_idx,
                .boolean => |l| l.col_idx,
                .null_check => |l| l.col_idx,
                .like => |l| l.col_idx,
                else => unreachable,
            };
            try list.append(allocator, .{ .filter = f, .col_idx = col_idx });
        },
    }
}

/// Per-RG aggregator update. Decodes the columns referenced by the
/// outer filter + each aggregate's args/predicates, applies the outer
/// filter, then folds active rows into each accumulator (with each
/// agg's own `FILTER (WHERE ...)` predicate composed in).
///
/// State (`accumulators`) is owned by the caller and persists across
/// RGs of the same query. After all RGs are scanned, the caller calls
/// `expr_agg.finalize(...)` per accumulator and writes the resulting
/// 1-row parquet via the existing streaming sink + encoder.
///
/// No sink writes, no offset advancement, no RGOut. Aggregates produce
/// nothing per-RG — only at end-of-scan.
pub fn scanRGForAgg(
    gpa: std.mem.Allocator,
    rg: *const schema.RowGroup,
    meta: *const schema.FileMetaData,
    src: RGSrc,
    filter: ?filter_ast.Filter,
    /// When true, never answer aggregates from row-group statistics —
    /// force every agg down the decode path (the `--scan-all` escape
    /// hatch: paranoid / untrusted-stats mode).
    scan_all: bool,
    /// When false (the default), min/max/sum are answered by decoding rather
    /// than trusting file statistics, which can be inaccurate. `count(*)` is
    /// always answered from RowGroup.num_rows regardless. `--trust-stats`
    /// sets this true to restore the stats fast-path for trusted writers.
    trust_stats: bool,
    fetch_set: []const bool,
    agg_calls: []const expr_agg.AggCall,
    accumulators: []expr_agg.Accumulator,
    group_by_keys: ?[]const expr_ast.Expr,
    group_table: ?*expr_agg.GroupTable,
    timings: *Timings,
) !void {
    const is_grouped = group_table != null;
    if (is_grouped) {
        std.debug.assert(accumulators.len == 0);
    } else {
        std.debug.assert(agg_calls.len == accumulators.len);
    }
    const num_rows: usize = @intCast(rg.num_rows);
    const num_leaves = rg.columns.items.len;
    const has_outer_filter = filter != null;

    // 1. Stats short-circuit pass. For every agg eligible (count /
    //    min / max with no outer filter and no per-agg WHERE), try
    //    to answer from RG metadata only. Mark those as "handled"
    //    so the decode pass below skips them. Aggs whose stat
    //    short-circuit fails (stats absent) fall through to decode.
    const handled_via_stats = try gpa.alloc(bool, agg_calls.len);
    defer gpa.free(handled_via_stats);
    @memset(handled_via_stats, false);

    var any_decode_required = false;
    if (is_grouped) {
        any_decode_required = true;
    } else {
        const t_stats_start = nowMonoNs();
        for (agg_calls, 0..) |call, i| {
            if (scan_all or !expr_agg.canStatShortCircuit(call, has_outer_filter, trust_stats)) {
                any_decode_required = true;
                continue;
            }
            const ok = try expr_agg.updateOneFromStats(&accumulators[i], call, rg, meta);
            if (ok) {
                handled_via_stats[i] = true;
            } else {
                any_decode_required = true;
            }
        }
        timings.eval_ns += @intCast(nowMonoNs() - t_stats_start);
    }

    // Fast path: every agg answered from stats, no decode needed at
    // all. This is the headline win — `max(cost)`, `count(*)`, etc.
    // complete in microseconds because we never read any data pages.
    if (!any_decode_required) return;

    // 2. Decode path for the aggs that couldn't be stat-handled.
    var rg_arena_state = std.heap.ArenaAllocator.init(gpa);
    defer rg_arena_state.deinit();
    const ra = rg_arena_state.allocator();

    var batch_cols: std.ArrayList(filter_eval.Batch.Column) = .empty;
    var lookup = try ra.alloc(?usize, meta.schema.items.len);
    @memset(lookup, null);

    const PageIndex = struct {
        col_index: schema.ColumnIndex,
        offset_index: schema.OffsetIndex,
    };

    const col_indexes = try ra.alloc(?PageIndex, num_leaves);
    @memset(col_indexes, null);

    if (!scan_all) {
        for (fetch_set, 0..) |needed, ci| {
            if (!needed) continue;
            if (ci >= num_leaves) continue;
            const chunk_meta = rg.columns.items[ci];

            const co = chunk_meta.column_index_offset orelse continue;
            const cl = chunk_meta.column_index_length orelse continue;
            const oo = chunk_meta.offset_index_offset orelse continue;
            const ol = chunk_meta.offset_index_length orelse continue;

            const col_bytes = originSlice(src, co, cl) orelse continue;
            const off_bytes = originSlice(src, oo, ol) orelse continue;

            var col_reader = thrift.Reader.init(col_bytes);
            const col_index = schema.ColumnIndex.read(ra, &col_reader) catch continue;

            var off_reader = thrift.Reader.init(off_bytes);
            const offset_index = schema.OffsetIndex.read(ra, &off_reader) catch continue;

            col_indexes[ci] = PageIndex{
                .col_index = col_index,
                .offset_index = offset_index,
            };
        }
    }

    var sel = try filter_selection.SelectionVector.init(ra, num_rows);

    const col_page_is_skipped = try ra.alloc(?[]bool, num_leaves);
    @memset(col_page_is_skipped, null);
    const col_page_is_always_match = try ra.alloc(?[]bool, num_leaves);
    @memset(col_page_is_always_match, null);

    var conj_leaves: std.ArrayList(ConjunctiveLeaf) = .empty;
    if (filter) |f| {
        try collectConjunctiveLeaves(f, &conj_leaves, ra);
    }

    for (conj_leaves.items) |leaf| {
        const ci = leaf.col_idx;
        if (ci >= num_leaves) continue;
        if (col_indexes[ci]) |pi| {
            const locs = pi.offset_index.page_locations.items;
            const skipped = col_page_is_skipped[ci] orelse blk: {
                const s = try ra.alloc(bool, locs.len);
                @memset(s, false);
                break :blk s;
            };
            const always = col_page_is_always_match[ci] orelse blk: {
                const a = try ra.alloc(bool, locs.len);
                @memset(a, false);
                break :blk a;
            };
            const is_first = col_page_is_skipped[ci] == null;

            var ci_list = try ra.alloc(?schema.ColumnIndex, num_leaves);
            @memset(ci_list, null);
            for (col_indexes, 0..) |p_idx, idx| {
                if (p_idx) |p| ci_list[idx] = p.col_index;
            }

            for (locs, 0..) |loc, page_idx| {
                const dec = try filter_prune.prunePage(rg, page_idx, leaf.filter, ci_list, ra, meta, trust_stats);
                const start: usize = @intCast(loc.first_row_index);
                const end: usize = if (page_idx + 1 < locs.len) @intCast(locs[page_idx + 1].first_row_index) else num_rows;

                if (dec == .skip) {
                    skipped[page_idx] = true;
                    var r = start;
                    while (r < end) : (r += 1) {
                        sel.set(r, false);
                    }
                }

                const is_always = dec == .always_match;
                if (is_first) {
                    always[page_idx] = is_always;
                } else {
                    always[page_idx] = always[page_idx] and is_always;
                }
            }

            col_page_is_skipped[ci] = skipped;
            col_page_is_always_match[ci] = always;
        }
    }

    const t_decode_start = nowMonoNs();
    for (fetch_set, 0..) |needed, ci| {
        if (!needed) continue;
        if (ci >= num_leaves) continue;
        const cm = rg.columns.items[ci].meta_data orelse return error.ColumnMetaMissing;
        const start: usize = if (cm.dictionary_page_offset) |dp| @intCast(dp) else @intCast(cm.data_page_offset);
        const len: usize = @intCast(cm.total_compressed_size);
        invariant.assert(start >= @as(usize, @intCast(src.byte_origin)));
        const buf_off = start - @as(usize, @intCast(src.byte_origin));
        if (buf_off + len > src.bytes.len) return error.MissingChunkBytes;
        const chunk = src.bytes[buf_off .. buf_off + len];

        const levels = meta.getColumnLevels(cm.path_in_schema.items);
        const n_leaves: usize = @intCast(cm.num_values);

        var prune_info: ?PruningInfo = null;
        if (col_indexes[ci]) |pi| {
            const locs = pi.offset_index.page_locations.items;
            var skipped = col_page_is_skipped[ci];
            var always = col_page_is_always_match[ci];

            if (skipped == null) {
                const s = try ra.alloc(bool, locs.len);
                @memset(s, false);
                const a = try ra.alloc(bool, locs.len);
                @memset(a, false);

                for (locs, 0..) |loc, page_idx| {
                    const start_idx: usize = @intCast(loc.first_row_index);
                    const end_idx: usize = if (page_idx + 1 < locs.len) @intCast(locs[page_idx + 1].first_row_index) else num_rows;

                    var has_active = false;
                    var r = start_idx;
                    while (r < end_idx) : (r += 1) {
                        if (sel.isActive(r)) {
                            has_active = true;
                            break;
                        }
                    }
                    if (!has_active) {
                        s[page_idx] = true;
                    }
                }
                skipped = s;
                always = a;
            }

            prune_info = PruningInfo{
                .locations = locs,
                .page_is_skipped = skipped.?,
                .page_is_always_match = always.?,
                .dictionary_page_offset = cm.dictionary_page_offset,
                .chunk_file_offset = @intCast(start),
                .col_index = pi.col_index,
            };
        }

        const schema_elem = meta.getColumnSchema(cm.path_in_schema.items);
        const dec_kind: ?decimal_mod.Kind = if (schema_elem) |se| decimal_mod.kindFromSchema(&se) else null;

        const decoded: filter_eval.Batch.Column = if (dec_kind) |k| .{
            .f64 = try decimal_mod.decodeColumnAsF64(ra, chunk, cm.codec, levels, n_leaves, k),
        } else if (schema_elem != null and schema.isFloat16(schema_elem.?)) .{
            .f64 = try decodeFloat16ColumnAsF64Pruned(ra, chunk, cm.codec, levels, n_leaves, prune_info),
        } else switch (cm.type) {
            .INT32 => if (schema_elem != null and schema.isUnsignedIntTo32(schema_elem.?))
                .{ .i64 = try decodeU32ColumnAsI64Pruned(ra, chunk, cm.codec, levels, n_leaves, prune_info) }
            else
                .{ .i32 = try decodeColumnTPruned(i32, ra, chunk, cm.codec, levels, n_leaves, prune_info) },
            .INT64 => .{ .i64 = try decodeColumnTPruned(i64, ra, chunk, cm.codec, levels, n_leaves, prune_info) },
            .FLOAT => .{ .f32 = try decodeColumnTPruned(f32, ra, chunk, cm.codec, levels, n_leaves, prune_info) },
            .DOUBLE => .{ .f64 = try decodeColumnTPruned(f64, ra, chunk, cm.codec, levels, n_leaves, prune_info) },
            .BYTE_ARRAY => .{ .string = try decodeColumnTPruned([]const u8, ra, chunk, cm.codec, levels, n_leaves, prune_info) },
            .BOOLEAN => .{ .boolean = try decodeColumnTPruned(bool, ra, chunk, cm.codec, levels, n_leaves, prune_info) },
            .INT96 => .{ .i64 = try int96_mod.decodeColumnAsI64Nanos(ra, chunk, cm.codec, levels, n_leaves) },
            .FIXED_LEN_BYTE_ARRAY => .{ .string = try decodeFlbaColumnPruned(ra, chunk, cm.codec, levels, n_leaves, flbaWidth(schema_elem), prune_info) },
        };
        lookup[ci] = batch_cols.items.len;
        try batch_cols.append(ra, decoded);
    }
    const t_decode_end = nowMonoNs();
    timings.decode_ns += @intCast(t_decode_end - t_decode_start);

    const batch: filter_eval.Batch = .{ .cols = batch_cols.items, .num_rows = num_rows };
    if (filter) |f| try filter_eval.evaluate(f, &batch, &sel, lookup, ra);
    const t_eval_end = nowMonoNs();
    timings.eval_ns += @intCast(t_eval_end - t_decode_end);

    if (is_grouped) {
        const gb_keys = group_by_keys.?;
        var key_cols = try ra.alloc(filter_eval.Batch.Column, gb_keys.len);
        for (gb_keys, 0..) |key_expr, idx| {
            key_cols[idx] = try expr_eval.evalGroupKeyExpr(ra, &batch, lookup, key_expr);
        }

        var group_id_arr = try ra.alloc(u32, num_rows);
        @memset(group_id_arr, 0);

        var key_scratch: std.ArrayList(u8) = .empty;
        defer key_scratch.deinit(ra);

        var r: usize = 0;
        while (r < num_rows) : (r += 1) {
            if (sel.isActive(r)) {
                try expr_agg.serializeRowKey(&key_scratch, ra, key_cols, r);
                const gid = try group_table.?.getOrInsert(key_scratch.items, agg_calls);
                group_id_arr[r] = gid;
            }
        }

        for (agg_calls, 0..) |call, i| {
            try expr_agg.updateOneGrouped(
                ra,
                gpa,
                group_table.?.accumulators.items,
                group_id_arr,
                i,
                agg_calls.len,
                call,
                &batch,
                lookup,
                &sel,
            );
        }
    } else {
        // Update only the aggs that weren't handled via stats. The agg's
        // own per-WHERE predicate (if any) is composed inside updateOne.
        for (agg_calls, 0..) |call, i| {
            if (handled_via_stats[i]) continue;
            // `ra` is per-RG scratch; `gpa` is the persist allocator for owned
            // results (string min/max winner) that outlive both `ra` and the
            // cross-worker merge.
            try expr_agg.updateOne(ra, gpa, &accumulators[i], call, &batch, lookup, &sel);
        }
    }
    timings.encode_ns += @intCast(nowMonoNs() - t_eval_end);
}

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
            // Carry the page index forward, written to the sink right
            // after this chunk's data (offsets rebased by `delta`).
            const idx = try carryIndexToSink(out_arena, sink, out_offset, src, src_chunk, delta);
            try new_cols.append(out_arena, shiftChunkWithIndex(src_chunk, delta, idx));
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

    // No projection: compute this RG's bounding box
    // [min_col_start, max_col_end) from the column metadata, copy
    // just that slice, then shift every chunk's offset by a single
    // delta.
    //
    // We compute the bounding box internally rather than trusting
    // the caller because the contract is easy to get wrong — passing
    // the whole-file bytes here would cause us to write every RG's
    // worth of bytes once per RG (an N-row-group file produces N×
    // output bloat). Production callers pass the whole-file bytes;
    // this branch makes that safe.
    var rg_min: u64 = std.math.maxInt(u64);
    var rg_max: u64 = 0;
    for (rg.columns.items) |chunk| {
        const m = chunk.meta_data orelse return error.InvalidColumnOffsets;
        const start: u64 = if (m.dictionary_page_offset) |d| @intCast(d) else @intCast(m.data_page_offset);
        const end: u64 = start + @as(u64, @intCast(m.total_compressed_size));
        if (start < rg_min) rg_min = start;
        if (end > rg_max) rg_max = end;
    }
    if (rg_min == std.math.maxInt(u64)) return error.InvalidColumnOffsets;

    const buf_off: usize = @intCast(rg_min - @as(u64, @intCast(src.byte_origin)));
    const slice_len: usize = @intCast(rg_max - rg_min);
    if (buf_off + slice_len > src.bytes.len) return error.MissingChunkBytes;
    const rg_slice = src.bytes[buf_off .. buf_off + slice_len];

    const new_start = out_offset.*;
    const t_sink_start = nowMonoNs();
    try sink.write(rg_slice);
    timings.sink_ns += @intCast(nowMonoNs() - t_sink_start);
    out_offset.* += slice_len;
    const delta: i64 = @as(i64, @intCast(new_start)) - @as(i64, @intCast(rg_min));

    var cols: std.ArrayListUnmanaged(schema.ColumnChunk) = .empty;
    try cols.ensureTotalCapacity(out_arena, rg.columns.items.len);
    for (rg.columns.items) |chunk| {
        // Whole-RG copy: one delta for the group. Index bytes for each
        // chunk are appended after the RG data block, in column order.
        const idx = try carryIndexToSink(out_arena, sink, out_offset, src, chunk, delta);
        try cols.append(out_arena, shiftChunkWithIndex(chunk, delta, idx));
    }

    return .{
        .surviving_rows = rg.num_rows,
        .rg = .{
            .columns = cols,
            .total_byte_size = @intCast(slice_len),
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

/// `shiftChunk` with the page-index pointers set to the carried-forward
/// locations instead of dropped.
fn shiftChunkWithIndex(src: schema.ColumnChunk, delta: i64, idx: fastpath.PageIndexPtrs) schema.ColumnChunk {
    var out = shiftChunk(src, delta);
    out.offset_index_offset = idx.offset_index_offset;
    out.offset_index_length = idx.offset_index_length;
    out.column_index_offset = idx.column_index_offset;
    out.column_index_length = idx.column_index_length;
    return out;
}

/// Slice the source page-index bytes for a chunk out of `src.bytes`,
/// accounting for `byte_origin` (the buffer may start partway into the
/// file). Null when the index isn't present in the fetched bytes.
fn originSlice(src: RGSrc, file_off: i64, len: i32) ?[]const u8 {
    if (file_off < 0 or len < 0) return null;
    const fo: u64 = @intCast(file_off);
    if (fo < src.byte_origin) return null;
    const buf_off: usize = @intCast(fo - src.byte_origin);
    const l: usize = @intCast(len);
    if (buf_off + l > src.bytes.len) return null;
    return src.bytes[buf_off .. buf_off + l];
}

/// Re-serialize the source chunk's page index to the sink (ColumnIndex
/// verbatim, OffsetIndex offsets rebased by `delta`), returning the new
/// output-relative pointers. Absent/unfetched/unparseable → null
/// pointers (page index dropped; readers fall back to RG-level pruning).
fn carryIndexToSink(
    out_arena: std.mem.Allocator,
    sink: streaming.Sink,
    out_offset: *u64,
    src: RGSrc,
    src_chunk: schema.ColumnChunk,
    delta: i64,
) !fastpath.PageIndexPtrs {
    var ptrs = fastpath.PageIndexPtrs{};

    if (src_chunk.column_index_offset) |co| if (src_chunk.column_index_length) |cl| {
        if (originSlice(src, co, cl)) |bytes| {
            if (fastpath.reserializeColumnIndex(out_arena, bytes)) |ser| {
                ptrs.column_index_offset = @intCast(out_offset.*);
                try sink.write(ser);
                out_offset.* += ser.len;
                ptrs.column_index_length = @intCast(ser.len);
            }
        }
    };

    if (src_chunk.offset_index_offset) |oo| if (src_chunk.offset_index_length) |ol| {
        if (originSlice(src, oo, ol)) |bytes| {
            if (fastpath.reserializeOffsetIndexShifted(out_arena, bytes, delta)) |ser| {
                ptrs.offset_index_offset = @intCast(out_offset.*);
                try sink.write(ser);
                out_offset.* += ser.len;
                ptrs.offset_index_length = @intCast(ser.len);
            }
        }
    };

    return ptrs;
}

/// Decode an entire column chunk into a `ColumnT(T)` view: values
/// plus def_levels (OPTIONAL) and optionally rep_levels (LIST/MAP).
/// `num_leaves` is the column chunk's `num_values` from metadata
/// (counts LEAVES, not logical rows — for nested cols this can be
/// larger than the RG's row count).
pub const PruningInfo = struct {
    locations: []const schema.PageLocation,
    page_is_skipped: []const bool,
    page_is_always_match: []const bool,
    dictionary_page_offset: ?i64,
    chunk_file_offset: i64,
    col_index: schema.ColumnIndex,
};

fn defaultVal(comptime T: type) T {
    return switch (T) {
        i32, i64 => 0,
        f32, f64 => 0.0,
        bool => false,
        []const u8 => "",
        else => unreachable,
    };
}

fn decodeWithReaderPruned(
    comptime T: type,
    arena: std.mem.Allocator,
    reader: *column_mod.ColumnChunkReader(T),
    levels: schema.Levels,
    num_leaves: usize,
    prune: PruningInfo,
) !filter_eval.ColumnT(T) {
    const values = try arena.alloc(T, num_leaves);
    @memset(values, defaultVal(T));

    const max_def = levels.max_def;
    const max_rep = levels.max_rep;

    var def_levels: ?[]u32 = null;
    var rep_levels: ?[]u32 = null;

    if (max_rep > 0) {
        def_levels = try arena.alloc(u32, num_leaves);
        @memset(def_levels.?, 0);
        rep_levels = try arena.alloc(u32, num_leaves);
        @memset(rep_levels.?, 0);
    } else if (max_def > 0) {
        def_levels = try arena.alloc(u32, num_leaves);
        @memset(def_levels.?, 0);
    }

    if (prune.dictionary_page_offset) |dict_off| {
        try reader.pages.seekToPage(dict_off, prune.chunk_file_offset);
        _ = try reader.advancePage();
    }

    // 2. Loop over pages and decode or skip
    for (prune.locations, 0..) |loc, pi| {
        const start: usize = @intCast(loc.first_row_index);
        const end: usize = if (pi + 1 < prune.locations.len) @intCast(prune.locations[pi + 1].first_row_index) else num_leaves;

        const is_skipped = prune.page_is_skipped[pi];
        const is_always_match = prune.page_is_always_match[pi];

        if (is_skipped or is_always_match) {
            if (is_always_match) {
                const min_bytes = prune.col_index.min_values.items[pi];
                const min_v = if (T == []const u8 or T == bool)
                    defaultVal(T)
                else blk: {
                    const encoded_mod = @import("filter/encoded.zig");
                    break :blk encoded_mod.readFixedLE(T, min_bytes) orelse defaultVal(T);
                };
                @memset(values[start..end], min_v);

                if (def_levels) |dl| {
                    @memset(dl[start..end], @intCast(max_def));
                }
            }

            // Reposition the PageReader past this page
            if (pi + 1 < prune.locations.len) {
                try reader.pages.seekToPage(prune.locations[pi + 1].offset, prune.chunk_file_offset);
            } else {
                reader.pages.pos = reader.pages.chunk.len;
            }
            continue;
        }

        _ = try reader.seekAndInstallPage(loc.offset, prune.chunk_file_offset);

        const want = end - start;
        if (max_rep > 0) {
            const n = try reader.decodeWithRepLevels(values[start..end], def_levels.?[start..end], rep_levels.?[start..end]);
            if (n != want) return error.ShortDecode;
        } else if (max_def > 0) {
            const n = try reader.decodeWithLevels(values[start..end], def_levels.?[start..end]);
            if (n != want) return error.ShortDecode;
        } else {
            const n = try reader.decode(values[start..end]);
            if (n != want) return error.ShortDecode;
        }
    }

    return .{
        .values = values,
        .def_levels = def_levels,
        .max_def = @intCast(max_def),
        .rep_levels = rep_levels,
        .max_rep = @intCast(max_rep),
        .has_nulls = reader.has_nulls,
    };
}

pub fn decodeColumnTPruned(
    comptime T: type,
    arena: std.mem.Allocator,
    chunk: []const u8,
    codec: schema.CompressionCodec,
    levels: schema.Levels,
    num_leaves: usize,
    prune: ?PruningInfo,
) !filter_eval.ColumnT(T) {
    var reader = column_mod.ColumnChunkReader(T).init(chunk, codec, levels, arena);
    if (prune) |p| {
        return decodeWithReaderPruned(T, arena, &reader, levels, num_leaves, p);
    } else {
        return decodeWithReader(T, arena, &reader, levels, num_leaves);
    }
}

fn decodeFloat16ColumnAsF64Pruned(
    arena: std.mem.Allocator,
    chunk: []const u8,
    codec: schema.CompressionCodec,
    levels: schema.Levels,
    num_leaves: usize,
    prune: ?PruningInfo,
) !filter_eval.ColumnT(f64) {
    const cb = try decodeFlbaColumnPruned(arena, chunk, codec, levels, num_leaves, 2, prune);
    const out = try arena.alloc(f64, cb.values.len);
    @memset(out, 0);
    for (cb.values, 0..) |bytes, i| {
        if (bytes.len >= 2) {
            const bits = std.mem.readInt(u16, bytes[0..2], .little);
            out[i] = @floatCast(@as(f16, @bitCast(bits)));
        }
    }
    return .{
        .values = out,
        .def_levels = cb.def_levels,
        .max_def = cb.max_def,
        .rep_levels = cb.rep_levels,
        .has_nulls = cb.has_nulls,
    };
}

fn decodeU32ColumnAsI64Pruned(
    arena: std.mem.Allocator,
    chunk: []const u8,
    codec: schema.CompressionCodec,
    levels: schema.Levels,
    num_leaves: usize,
    prune: ?PruningInfo,
) !filter_eval.ColumnT(i64) {
    const c32 = try decodeColumnTPruned(i32, arena, chunk, codec, levels, num_leaves, prune);
    const out = try arena.alloc(i64, c32.values.len);
    @memset(out, 0);
    for (c32.values, 0..) |v, i| out[i] = @as(i64, @as(u32, @bitCast(v)));
    return .{
        .values = out,
        .def_levels = c32.def_levels,
        .max_def = c32.max_def,
        .rep_levels = c32.rep_levels,
        .has_nulls = c32.has_nulls,
    };
}

pub fn decodeFlbaColumnPruned(
    arena: std.mem.Allocator,
    chunk: []const u8,
    codec: schema.CompressionCodec,
    levels: schema.Levels,
    num_leaves: usize,
    type_length: usize,
    prune: ?PruningInfo,
) !filter_eval.ColumnT([]const u8) {
    var reader = column_mod.ColumnChunkReader([]const u8).init(chunk, codec, levels, arena);
    reader.type_length = type_length;
    if (prune) |p| {
        return decodeWithReaderPruned([]const u8, arena, &reader, levels, num_leaves, p);
    } else {
        return decodeWithReader([]const u8, arena, &reader, levels, num_leaves);
    }
}

pub fn decodeColumnT(
    comptime T: type,
    arena: std.mem.Allocator,
    chunk: []const u8,
    codec: schema.CompressionCodec,
    levels: schema.Levels,
    num_leaves: usize,
) !filter_eval.ColumnT(T) {
    var reader = column_mod.ColumnChunkReader(T).init(chunk, codec, levels, arena);
    return decodeWithReader(T, arena, &reader, levels, num_leaves);
}

/// Fixed byte width of a FIXED_LEN_BYTE_ARRAY column from its SchemaElement
/// (0 if absent). Used to drive `decodeFlbaColumn`.
pub fn flbaWidth(se: ?schema.SchemaElement) usize {
    if (se) |s| if (s.type_length) |t| return @intCast(t);
    return 0;
}

/// Decode a FLOAT16 column (FIXED_LEN_BYTE_ARRAY(2), IEEE half) into the f64
/// lane: each 2-byte little-endian half → `f16` → `f64`. Mirrors the decimal
/// path so float16 flows through numeric agg/filter/expr.
fn decodeFloat16ColumnAsF64(
    arena: std.mem.Allocator,
    chunk: []const u8,
    codec: schema.CompressionCodec,
    levels: schema.Levels,
    num_leaves: usize,
) !filter_eval.ColumnT(f64) {
    const cb = try decodeFlbaColumn(arena, chunk, codec, levels, num_leaves, 2);
    const out = try arena.alloc(f64, cb.values.len);
    for (cb.values, 0..) |bytes, i| {
        if (bytes.len >= 2) {
            const bits = std.mem.readInt(u16, bytes[0..2], .little);
            out[i] = @floatCast(@as(f16, @bitCast(bits)));
        } else {
            out[i] = 0;
        }
    }
    return .{
        .values = out,
        .def_levels = cb.def_levels,
        .max_def = cb.max_def,
        .rep_levels = cb.rep_levels,
        .has_nulls = cb.has_nulls,
    };
}

/// Decode an INT32 column whose values are unsigned, zero-extending each into
/// an `i64` lane (`u32` → `i64`). See `schema.isUnsignedIntTo32`.
fn decodeU32ColumnAsI64(
    arena: std.mem.Allocator,
    chunk: []const u8,
    codec: schema.CompressionCodec,
    levels: schema.Levels,
    num_leaves: usize,
) !filter_eval.ColumnT(i64) {
    const c32 = try decodeColumnT(i32, arena, chunk, codec, levels, num_leaves);
    const out = try arena.alloc(i64, c32.values.len);
    for (c32.values, 0..) |v, i| out[i] = @as(i64, @as(u32, @bitCast(v)));
    return .{
        .values = out,
        .def_levels = c32.def_levels,
        .max_def = c32.max_def,
        .rep_levels = c32.rep_levels,
        .has_nulls = c32.has_nulls,
    };
}

/// FIXED_LEN_BYTE_ARRAY columns decode to raw fixed-width `[]const u8` slices
/// (no per-value length prefix). `type_length` is the column's fixed byte width
/// from its SchemaElement. Decimal-FLBA has its own f64 path (decimal.zig); this
/// is the raw-bytes path — UUID / Float16 / fixed binary.
pub fn decodeFlbaColumn(
    arena: std.mem.Allocator,
    chunk: []const u8,
    codec: schema.CompressionCodec,
    levels: schema.Levels,
    num_leaves: usize,
    type_length: usize,
) !filter_eval.ColumnT([]const u8) {
    var reader = column_mod.ColumnChunkReader([]const u8).init(chunk, codec, levels, arena);
    reader.type_length = type_length;
    return decodeWithReader([]const u8, arena, &reader, levels, num_leaves);
}

fn decodeWithReader(
    comptime T: type,
    arena: std.mem.Allocator,
    reader: *column_mod.ColumnChunkReader(T),
    levels: schema.Levels,
    num_leaves: usize,
) !filter_eval.ColumnT(T) {
    const values = try arena.alloc(T, num_leaves);
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
            .has_nulls = reader.has_nulls,
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
        return .{
            .values = values,
            .def_levels = def_levels,
            .max_def = @intCast(levels.max_def),
            .has_nulls = reader.has_nulls,
        };
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
    _ = appendProjectedRG;
    _ = encodeAggregator;
    _ = copyRG;
    _ = decodeColumnT;
    _ = shiftChunk;
}

// Regression: copyRG(no_projection) used to write whatever `src.bytes`
// the caller passed, even when those bytes covered more than just this
// RG. The engine passes the *whole file*, so a file with N row groups
// produced an N×-bloated output. Lock the fix: with a hand-built 2-RG
// layout in a 1000-byte buffer, copying RG0 must write only RG0's
// bounding box (60 bytes), not the whole buffer.
test "copyRG no-projection: writes only this RG's bounding box, not src.bytes" {
    const testing = std.testing;
    var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_state.deinit();
    const a = arena_state.allocator();

    // Fake file layout: 1000 bytes total. RG0's columns span
    // [100, 160), RG1's span [500, 580). The two RGs are far apart in
    // the source file (typical for real parquet — pages + dict pages
    // + column metadata between them).
    const total_bytes: usize = 1000;
    var buf = try a.alloc(u8, total_bytes);
    @memset(buf, 0); // start with zeros so we can identify what got copied

    // Stamp recognizable patterns into RG0's region so we can assert
    // the right bytes ended up in the output.
    for (100..160) |i| buf[i] = @intCast((i - 100) + 1); // 1, 2, 3, ...

    var path: schema.StringList = .empty;
    try path.append(a, "x");
    var encs: schema.EncodingList = .empty;
    try encs.append(a, .PLAIN);

    // RG0: two columns at offsets 100 and 140 (each 20 bytes
    // compressed). Bounding box: [100, 160).
    var rg0_cols: std.ArrayListUnmanaged(schema.ColumnChunk) = .empty;
    try rg0_cols.append(a, .{
        .file_path = null,
        .file_offset = 0,
        .meta_data = .{
            .type = .INT64,
            .encodings = encs,
            .path_in_schema = path,
            .codec = .UNCOMPRESSED,
            .num_values = 10,
            .total_uncompressed_size = 20,
            .total_compressed_size = 20,
            .data_page_offset = 100,
            .index_page_offset = null,
            .dictionary_page_offset = null,
        },
    });
    try rg0_cols.append(a, .{
        .file_path = null,
        .file_offset = 0,
        .meta_data = .{
            .type = .INT64,
            .encodings = encs,
            .path_in_schema = path,
            .codec = .UNCOMPRESSED,
            .num_values = 10,
            .total_uncompressed_size = 20,
            .total_compressed_size = 20,
            .data_page_offset = 140,
            .index_page_offset = null,
            .dictionary_page_offset = null,
        },
    });
    // ALSO add a column from far away in the file to make sure copyRG
    // doesn't include it (it shouldn't — it's not in RG0). Sit it at
    // offset 800 — well past RG0's bounding box. (In a real file this
    // wouldn't happen; here it's a guardrail against "did we accidentally
    // re-fall-through to whole-file copy?".)
    // Note: copyRG operates on rg.columns.items only, so adding bytes
    // outside that doesn't affect it. RG0's bounding box is determined
    // entirely from RG0's columns.

    const rg0: schema.RowGroup = .{
        .columns = rg0_cols,
        .total_byte_size = 60,
        .num_rows = 10,
    };

    // Capture sink — records writes into an ArrayList.
    const Capture = struct {
        bytes: std.ArrayList(u8) = .empty,
        alloc: std.mem.Allocator,
        fn write(ctx: *anyopaque, b: []const u8) anyerror!void {
            const self: *@This() = @ptrCast(@alignCast(ctx));
            try self.bytes.appendSlice(self.alloc, b);
        }
    };
    var cap: Capture = .{ .alloc = a };
    const sink: streaming.Sink = .{
        .ctx = &cap,
        .write_fn = Capture.write,
    };

    var out_offset: u64 = 0;
    var timings: Timings = .{};
    const out = try copyRG(
        a,
        &rg0,
        .{ .bytes = buf, .byte_origin = 0 },
        null, // no projection — exercises the fixed branch
        sink,
        &out_offset,
        &timings,
    );

    // Bounding box for RG0 is [100, 160) = 60 bytes. Anything more
    // means we're back to the old bug.
    try testing.expectEqual(@as(usize, 60), cap.bytes.items.len);
    try testing.expectEqual(@as(u64, 60), out_offset);
    // Content should match buf[100..160] exactly (the stamped pattern).
    try testing.expectEqualSlices(u8, buf[100..160], cap.bytes.items);
    // Output RG carries the new bounding-box size.
    try testing.expect(out.rg != null);
    try testing.expectEqual(@as(i64, 60), out.rg.?.total_byte_size);
    try testing.expectEqual(@as(i64, 10), out.rg.?.num_rows);
    // Column offsets must be shifted to point into the output stream.
    // Column 0 was at source offset 100; output bounding box starts at
    // output offset 0; so new offset = 0 (i.e. new_start - rg_min = 0).
    try testing.expectEqual(@as(i64, 0), out.rg.?.columns.items[0].meta_data.?.data_page_offset);
    // Column 1 was at source offset 140 → output offset 40 (140 - 100).
    try testing.expectEqual(@as(i64, 40), out.rg.?.columns.items[1].meta_data.?.data_page_offset);
}

// --- DECIMAL decode: fixture-free encode→decode round-trip ---
// The decimal *decode* tests in decimal.zig read parquet-testing fixtures
// and skip in CI without the corpus (as do all of column.zig's decode
// tests). This exercises the full INT32/INT64-backed decimal decode path
// — page parse → ColumnChunkReader → scale → f64 — with no fixture, by
// encoding a known int column and decoding it back as a DECIMAL.
test "decimal: INT32/INT64 backings decode round-trip through the encoder" {
    const testing = std.testing;
    var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_state.deinit();
    const arena = arena_state.allocator();

    { // INT32-backed, scale 2 → raw / 100
        const raw = [_]i32{ 12345, -6789, 100, 0, 999999 };
        const elem = schema.SchemaElement{
            .type = .INT32,       .type_length = null, .repetition_type = .REQUIRED,
            .name = "value",      .num_children = 0,   .converted_type = .DECIMAL,
            .logical_type = null, .scale = 2,          .precision = 9,
            .field_id = null,
        };
        const enc = try encoder.encodeColumn(arena, .{
            .values = .{ .i32 = .{ .values = &raw } },
            .schema_elem = &elem,
            .path_in_schema = &[_][]const u8{"value"},
            .codec = .UNCOMPRESSED,
        });
        const kind = decimal_mod.kindFromSchema(&elem).?;
        const col = try decimal_mod.decodeColumnAsF64(arena, enc.bytes, .UNCOMPRESSED, .{ .max_def = 0, .max_rep = 0 }, raw.len, kind);
        const want = [_]f64{ 123.45, -67.89, 1.00, 0.0, 9999.99 };
        try testing.expectEqual(want.len, col.values.len);
        for (want, col.values) |w, g| try testing.expectApproxEqAbs(w, g, 1e-9);
    }

    { // INT64-backed, scale 3
        const raw = [_]i64{ 1000, -250, 0, 123456789 };
        const elem = schema.SchemaElement{
            .type = .INT64,       .type_length = null, .repetition_type = .REQUIRED,
            .name = "value",      .num_children = 0,   .converted_type = .DECIMAL,
            .logical_type = null, .scale = 3,          .precision = 18,
            .field_id = null,
        };
        const enc = try encoder.encodeColumn(arena, .{
            .values = .{ .i64 = .{ .values = &raw } },
            .schema_elem = &elem,
            .path_in_schema = &[_][]const u8{"value"},
            .codec = .UNCOMPRESSED,
        });
        const kind = decimal_mod.kindFromSchema(&elem).?;
        const col = try decimal_mod.decodeColumnAsF64(arena, enc.bytes, .UNCOMPRESSED, .{ .max_def = 0, .max_rep = 0 }, raw.len, kind);
        const want = [_]f64{ 1.0, -0.25, 0.0, 123456.789 };
        try testing.expectEqual(want.len, col.values.len);
        for (want, col.values) |w, g| try testing.expectApproxEqAbs(w, g, 1e-6);
    }
}
