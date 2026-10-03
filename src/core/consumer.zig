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
const nowMonoNs = @import("../clock.zig").monoNs;

/// Zig's 16 MiB default made thread setup/teardown dominate short queries. Duplicated rather than imported from
/// scan.zig, which imports this module.
const WORKER_STACK_SIZE: usize = 1 << 20;
const spawn_util = @import("spawn.zig");
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
const compression = @import("parquet/compression.zig");
const invariant = @import("invariant.zig");
const int96_mod = @import("parquet/int96.zig");
const metadata = @import("parquet/metadata.zig");

pub const DecodeOptions = column_mod.DecodeOptions;
pub const DecodeScratch = column_mod.DecodeScratch;
pub const RowGroupArena = @import("row_group_arena.zig").RowGroupArena;

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
            if (md.index_page_offset) |ip| md.index_page_offset = ip + base;
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
    /// - passthrough → source RG's leaf schema element, except logical
    ///   types widened by decode (currently FLOAT16 → DOUBLE).
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
                // The leaf's path comes from the schema, which every chunk's path_in_schema matches (footer open
                // enforces it), so a file with no row groups still builds its output schema.
                const path = try metadata.leafPathSegments(arena, meta, kept_ci) orelse return error.ColumnMetaMissing;
                const src_elem = meta.getColumnSchema(path) orelse
                    return error.SchemaLookupFailed;
                paths[i] = path;

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

                // FLOAT16 is physically FLBA(2), but decode widens it to
                // the f64 lane. Keep the buffer and output schema aligned:
                // row printing consumes f64 directly, while re-encoding
                // writes a valid DOUBLE column instead of FLBA metadata over
                // eight-byte values.
                if (schema.isFloat16(src_elem)) {
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
                        .field_id = src_elem.field_id,
                    };
                    cols[i] = .{ .f64 = .{} };
                    continue;
                }

                schemas[i] = src_elem;
                break :blk src_elem.type orelse return error.SchemaLookupFailed;
            },
            .computed => |c| blk: {
                const expr_type = c.expr.typeOf();
                schemas[i] = computedSchemaElem(c);
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
    return appendProjectedRGWithOptions(
        agg,
        gpa,
        rg,
        meta,
        src,
        filter,
        fetch_set,
        output_specs,
        timings,
        .{},
    );
}

pub fn appendProjectedRGWithOptions(
    agg: *OutputAggregator,
    gpa: std.mem.Allocator,
    rg: *const schema.RowGroup,
    meta: *const schema.FileMetaData,
    src: RGSrc,
    filter: ?filter_ast.Filter,
    fetch_set: []const bool,
    output_specs: []const OutputCol,
    timings: *Timings,
    decode_options: DecodeOptions,
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
            .f64 = try decimal_mod.decodeColumnAsF64WithOptions(
                ra,
                chunk,
                cm.codec,
                levels,
                n_leaves,
                k,
                decode_options,
                null,
            ),
        } else if (schema_elem != null and schema.isFloat16(schema_elem.?)) .{
            // FLOAT16 (FLBA(2), IEEE half) → f64 lane for numeric agg/filter.
            .f64 = try decodeFloat16ColumnAsF64(ra, chunk, cm.codec, levels, n_leaves, decode_options),
        } else switch (cm.type) {
            .INT32 => .{
                .i32 = try decodeColumnTWithOptions(i32, ra, chunk, cm.codec, levels, n_leaves, decode_options),
            },
            .INT64 => .{
                .i64 = try decodeColumnTWithOptions(i64, ra, chunk, cm.codec, levels, n_leaves, decode_options),
            },
            .FLOAT => .{
                .f32 = try decodeColumnTWithOptions(f32, ra, chunk, cm.codec, levels, n_leaves, decode_options),
            },
            .DOUBLE => .{
                .f64 = try decodeColumnTWithOptions(f64, ra, chunk, cm.codec, levels, n_leaves, decode_options),
            },
            .BYTE_ARRAY => .{
                .string = try decodeColumnTWithOptions(
                    []const u8,
                    ra,
                    chunk,
                    cm.codec,
                    levels,
                    n_leaves,
                    decode_options,
                ),
            },
            .BOOLEAN => .{
                .boolean = try decodeColumnTWithOptions(bool, ra, chunk, cm.codec, levels, n_leaves, decode_options),
            },
            // INT96 (legacy Spark/Impala timestamp) → i64 epoch-nanoseconds.
            .INT96 => .{ .i64 = try int96_mod.decodeColumnAsI64Nanos(ra, chunk, cm.codec, levels, n_leaves) },
            // FIXED_LEN_BYTE_ARRAY (non-decimal — decimal handled above): raw
            // fixed-width bytes (UUID / Float16 / fixed binary).
            .FIXED_LEN_BYTE_ARRAY => .{ .string = try decodeFlbaColumnWithOptions(
                ra,
                chunk,
                cm.codec,
                levels,
                n_leaves,
                flbaWidth(schema_elem),
                decode_options,
            ) },
        };
        try checkRowShape(decoded, num_rows);
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
            const dcol = try decodeDecimalI128(ra, rg, meta, src, kept_ci, decode_options);
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
    decode_options: DecodeOptions,
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
    return decimal_mod.decodeColumnAsI128WithOptions(
        ra,
        chunk,
        cm.codec,
        levels,
        n_leaves,
        kind,
        decode_options,
    );
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
    // Whether to synthesize + write the page index (ColumnIndex/OffsetIndex)
    // inline after each chunk. MUST be false when writing to a relative-offset
    // buffer that is later placed at a nonzero file offset: the OffsetIndex's
    // serialized PageLocation.offset would stay buffer-relative and mislead
    // readers. Direct-to-file callers (absolute out_offset) pass true.
    emit_page_index: bool,
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
        // Initialize every context before spawning: a partial spawn failure runs the rest inline, so none may be
        // half-built.
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
        }

        var spawned: usize = 0;
        for (0..num_workers) |w| {
            // Not `try`: an early return would unwind while earlier workers are still encoding into `arenas`, which
            // the caller releases.
            threads[w] = spawn_util.spawn(.{ .stack_size = WORKER_STACK_SIZE }, aggEncodeWorker, .{&ctxs[w]}) catch break;
            spawned += 1;
        }
        // Every context that missed its thread must run here, or its columns go unencoded and the write phase fails
        // with MissingEncodeResult.
        for (spawned..num_workers) |w| aggEncodeWorker(&ctxs[w]);
        for (threads[0..spawned]) |t| t.join();
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
        const idx = if (emit_page_index)
            try synthPageIndexToSink(out_arena, sink, out_offset, &enc.meta, em.data_page_offset)
        else
            fastpath.PageIndexPtrs{};

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

/// The output leaf for a computed column. Shared by the row buffers (whose printing reads it) and the footer.
pub fn computedSchemaElem(c: OutputCol.Computed) schema.SchemaElement {
    const expr_type = c.expr.typeOf();
    // An aliased UINT64 reference carries the column's raw bits through the i64 lane; keep the annotation so they
    // are printed and written as the unsigned values they are.
    const unsigned_64 = c.expr == .col_ref and c.expr.col_ref.unsigned_64;
    return .{
        .type = expr_type.toParquet(),
        .type_length = null,
        .repetition_type = .REQUIRED,
        .name = c.alias,
        .num_children = 0,
        .converted_type = if (expr_type == .str) .UTF8 else if (unsigned_64) .UINT_64 else null,
        .logical_type = if (expr_type == .str)
            .{ .STRING = .{} }
        else if (unsigned_64)
            .{ .INTEGER = .{ .bitWidth = 64, .isSigned = false } }
        else
            null,
        .scale = null,
        .precision = null,
        .field_id = null,
    };
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

/// Write an aggregate result — one row, or one per GROUP BY group — as a
/// single-row-group parquet file with one column-chunk per `OutputCol`
/// (avg-style aggs already split into two cols upstream), each holding
/// `num_rows` values. The output of an aggregate workload is a tiny parquet
/// that downstream consumers (Iceberg/Athena/Polars/another zpq invocation)
/// read like any other file.
///
/// Returns total bytes written. The sink may be an FdSink (CLI) or
/// MultipartSink (Lambda) — same protocol either way.
pub fn writeAggregateRows(
    arena: std.mem.Allocator,
    sink: streaming.Sink,
    output_cols: []const expr_agg.OutputCol,
    num_rows: i64,
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
            .repetition_type = if (out_col.nullable) .OPTIONAL else .REQUIRED,
            .name = out_col.name,
            .num_children = 0,
            .converted_type = out_col.converted_type,
            .logical_type = out_col.logical_type,
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
        // The encoder orders min/max signed; an unsigned column's type-defined order is not, so drop the bounds.
        if (schema.isUnsignedInt(leaf_elem)) if (em.statistics) |*st| {
            st.min = null;
            st.max = null;
            st.min_value = null;
            st.max_value = null;
        };
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
            .repetition_type = if (out_col.nullable) .OPTIONAL else .REQUIRED,
            .name = out_col.name,
            .num_children = 0,
            .converted_type = out_col.converted_type,
            .logical_type = out_col.logical_type,
            .scale = null,
            .precision = null,
            .field_id = null,
        });
    }

    var new_row_groups: std.ArrayListUnmanaged(schema.RowGroup) = .empty;
    try new_row_groups.append(arena, .{
        .columns = rg_columns,
        .total_byte_size = rg_total,
        .num_rows = num_rows,
    });

    const new_meta: schema.FileMetaData = .{
        .version = 1,
        .schema = new_schema,
        .num_rows = num_rows,
        .created_by = null,
        .row_groups = new_row_groups,
        .column_orders = try schema.FileMetaData.outputColumnOrders(arena, rg_columns.items.len, null, &.{}),
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
                .uint64 => |l| l.col_idx,
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
    decode_options: DecodeOptions,
    /// The calling worker's, reused across row groups like `rg_arena`.
    scratch: *DecodeScratch,
    fetch_set: []const bool,
    agg_calls: []const expr_agg.AggCall,
    accumulators: []expr_agg.Accumulator,
    group_by_keys: ?[]const expr_ast.Expr,
    group_table: ?*expr_agg.GroupTable,
    /// Caller-owned decode arena, reused across row groups: this call starts a new row group in it.
    rg_arena: *RowGroupArena,
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
    // Rewound, not rebuilt: a fresh arena per row group hands its pages back to the OS and re-faults them every time.
    // The price is that each worker's arena holds its high-water mark (one row group) for the whole scan.
    rg_arena.nextRowGroup();
    const ra = rg_arena.allocator();

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
            // Like an unparseable index, an inconsistent one is ignored and the row group is scanned whole.
            if (!pageIndexUsable(col_index, offset_index, num_rows)) continue;

            const cm = chunk_meta.meta_data orelse continue;
            const levels = meta.getColumnLevels(cm.path_in_schema.items);
            if (!pageIndexIsPlausible(&col_index, &offset_index, cm, levels, rg.num_rows)) continue;

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
                const dec = filter_prune.prunePage(rg, page_idx, leaf.filter, ci_list, meta, trust_stats);
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

    // Columns read by an aggregate argument, a per-agg FILTER, or a GROUP BY
    // key. The always-match shortcut in `decodeWithReaderPruned` fills such a
    // page with the ColumnIndex minimum instead of decoding it, which is only
    // sound when the filter is the sole consumer of the values.
    const values_consumed = try ra.alloc(bool, num_leaves);
    @memset(values_consumed, false);
    for (agg_calls) |call| {
        if (call.arg) |arg_expr| arg_expr.collectColumns(values_consumed);
        if (call.where) |w_expr| {
            var cols: std.ArrayList(usize) = .empty;
            try w_expr.collectColumns(&cols, ra);
            for (cols.items) |wci| if (wci < num_leaves) {
                values_consumed[wci] = true;
            };
        }
    }
    if (group_by_keys) |keys| {
        for (keys) |key_expr| key_expr.collectColumns(values_consumed);
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
            if (values_consumed[ci]) {
                const a = try ra.alloc(bool, locs.len);
                @memset(a, false);
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
            .f64 = try decimal_mod.decodeColumnAsF64WithOptions(
                ra,
                chunk,
                cm.codec,
                levels,
                n_leaves,
                k,
                decode_options,
                scratch,
            ),
        } else if (schema_elem != null and schema.isFloat16(schema_elem.?)) .{
            .f64 = try decodeFloat16ColumnAsF64Pruned(
                ra,
                chunk,
                cm.codec,
                levels,
                n_leaves,
                prune_info,
                decode_options,
                scratch,
            ),
        } else switch (cm.type) {
            .INT32 => if (schema_elem != null and schema.isUnsignedIntTo32(schema_elem.?))
                .{ .i64 = try decodeU32ColumnAsI64Pruned(
                    ra,
                    chunk,
                    cm.codec,
                    levels,
                    n_leaves,
                    prune_info,
                    decode_options,
                    scratch,
                ) }
            else
                .{
                    .i32 = try decodeColumnTPruned(
                        i32,
                        ra,
                        chunk,
                        cm.codec,
                        levels,
                        n_leaves,
                        prune_info,
                        decode_options,
                        scratch,
                    ),
                },
            .INT64 => .{
                .i64 = try decodeColumnTPruned(
                    i64,
                    ra,
                    chunk,
                    cm.codec,
                    levels,
                    n_leaves,
                    prune_info,
                    decode_options,
                    scratch,
                ),
            },
            .FLOAT => .{
                .f32 = try decodeColumnTPruned(
                    f32,
                    ra,
                    chunk,
                    cm.codec,
                    levels,
                    n_leaves,
                    prune_info,
                    decode_options,
                    scratch,
                ),
            },
            .DOUBLE => .{
                .f64 = try decodeColumnTPruned(
                    f64,
                    ra,
                    chunk,
                    cm.codec,
                    levels,
                    n_leaves,
                    prune_info,
                    decode_options,
                    scratch,
                ),
            },
            .BYTE_ARRAY => .{
                .string = try decodeColumnTPruned(
                    []const u8,
                    ra,
                    chunk,
                    cm.codec,
                    levels,
                    n_leaves,
                    prune_info,
                    decode_options,
                    scratch,
                ),
            },
            .BOOLEAN => .{
                .boolean = try decodeColumnTPruned(
                    bool,
                    ra,
                    chunk,
                    cm.codec,
                    levels,
                    n_leaves,
                    prune_info,
                    decode_options,
                    scratch,
                ),
            },
            .INT96 => .{ .i64 = try int96_mod.decodeColumnAsI64Nanos(ra, chunk, cm.codec, levels, n_leaves) },
            .FIXED_LEN_BYTE_ARRAY => .{ .string = try decodeFlbaColumnPruned(
                ra,
                chunk,
                cm.codec,
                levels,
                n_leaves,
                flbaWidth(schema_elem),
                prune_info,
                decode_options,
                scratch,
            ) },
        };
        lookup[ci] = batch_cols.items.len;
        try checkRowShape(decoded, num_rows);
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

        // Never cached across row groups: the resolver's memo holds pointers into `ra`.
        var key_resolver = expr_agg.GroupKeyResolver.init(key_cols);
        defer key_resolver.deinit(ra);

        var r: usize = 0;
        while (r < num_rows) : (r += 1) {
            if (sel.isActive(r)) {
                group_id_arr[r] = try key_resolver.resolve(ra, &group_table.?.*, key_cols, r, agg_calls);
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

/// Byte-copy one row group into `sink` and return a clone of it whose
/// chunk offsets (and carried page indexes) point at the new stream
/// position.
///
/// `kept_set = null`: no projection. The group's bounding span, from its
/// first chunk start to its last chunk end, is copied from `src` in one
/// write and every chunk shifts by the same delta.
///
/// `kept_set != null`: per-column copy. Only chunks where
/// `kept_set[ci] == true` are written. A `recompress` that changes any
/// chunk's codec takes this path too, with every column kept.
pub fn copyRG(
    out_arena: std.mem.Allocator,
    rg: *const schema.RowGroup,
    src: RGSrc,
    kept_set: ?[]const bool,
    sink: streaming.Sink,
    out_offset: *u64,
    timings: *Timings,
    // See encodeAggregator: false when writing to a relative-offset buffer that
    // is later placed at a nonzero file offset (the carried OffsetIndex's
    // PageLocation.offset would stay buffer-relative). Direct-to-file: true.
    emit_page_index: bool,
    /// A requested output codec. Chunks already in it are copied verbatim;
    /// the rest have their pages recompressed. Null keeps every chunk's codec.
    recompress: ?Recompress,
) !RGOut {
    // A chunk that must change codec changes size, so the whole-group copy
    // below cannot place it; take the per-chunk path for every column.
    var all_kept: ?[]bool = null;
    if (kept_set == null) if (recompress) |rc| {
        for (rg.columns.items) |chunk| {
            const m = chunk.meta_data orelse return error.InvalidColumnOffsets;
            if (m.codec == rc.codec) continue;
            all_kept = try out_arena.alloc(bool, rg.columns.items.len);
            @memset(all_kept.?, true);
            break;
        }
    };
    const kept_eff: ?[]const bool = kept_set orelse all_kept;

    if (kept_eff) |kept| {
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
            if (recompress) |rc| if (m.codec != rc.codec) {
                const written = try copyChunkRecompressed(rc, src, src_chunk, slice, src_start, sink, out_offset, timings, emit_page_index);
                try new_cols.append(out_arena, written);
                rg_total += written.meta_data.?.total_compressed_size;
                continue;
            };
            const t_sink_start = nowMonoNs();
            try sink.write(slice);
            timings.sink_ns += @intCast(nowMonoNs() - t_sink_start);
            out_offset.* += src_len;

            const delta: i64 = @as(i64, @intCast(new_col_start)) - @as(i64, @intCast(src_start));
            // Carry the page index forward, written to the sink right
            // after this chunk's data (offsets rebased by `delta`).
            const idx = if (emit_page_index)
                try carryIndexToSink(out_arena, sink, out_offset, src, src_chunk, delta)
            else
                fastpath.PageIndexPtrs{};
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
        const idx = if (emit_page_index)
            try carryIndexToSink(out_arena, sink, out_offset, src, chunk, delta)
        else
            fastpath.PageIndexPtrs{};
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

/// A codec change requested of a byte copy. `scratch` backs the per-chunk
/// working memory, released once the chunk is written.
pub const Recompress = struct {
    codec: schema.CompressionCodec,
    scratch: std.mem.Allocator,
};

/// One page of a recompressed chunk: where it started in the source chunk,
/// and where it starts and how long it is (header included) in the output.
const PageMove = struct { src_off: usize, dst_off: usize, dst_len: usize };

const RecompressedChunk = struct {
    bytes: []const u8,
    pages: []const PageMove,
    /// Output size change of the page headers, which `total_uncompressed_size` counts.
    header_delta: i64,
};

/// Rewrite every page of a column chunk from codec `from` to codec `to`.
/// Only compressed payloads change: v1 data and dictionary pages are
/// recompressed whole, a v2 page's values section (its levels are never
/// compressed), and a v2 page stored uncompressed is kept as is. Each
/// header is re-emitted field by field with the new compressed size and
/// without its CRC, which covered the old bytes; everything else in it,
/// page statistics included, is carried over unchanged.
fn recompressChunk(
    arena: std.mem.Allocator,
    chunk: []const u8,
    from: schema.CompressionCodec,
    to: schema.CompressionCodec,
) !RecompressedChunk {
    var out: std.ArrayList(u8) = .empty;
    var pages: std.ArrayList(PageMove) = .empty;
    var header_delta: i64 = 0;
    var pos: usize = 0;
    while (pos < chunk.len) {
        var r = thrift.Reader.init(chunk[pos..]);
        const h = try schema.PageHeader.read(&r);
        const hdr_len = r.pos;
        if (h.compressed_page_size < 0 or h.uncompressed_page_size < 0) return error.BadPageHeader;
        const body_len: usize = @intCast(h.compressed_page_size);
        if (body_len > chunk.len - pos - hdr_len) return error.TruncatedPage;
        const body = chunk[pos + hdr_len ..][0..body_len];
        const raw_len: usize = @intCast(h.uncompressed_page_size);

        var new_body: []const u8 = body;
        switch (h.type) {
            .DATA_PAGE, .DICTIONARY_PAGE => new_body = try recode(arena, body, from, to, raw_len),
            .DATA_PAGE_V2 => {
                const v2 = h.data_page_header_v2 orelse return error.BadPageHeader;
                if (v2.is_compressed) {
                    if (v2.definition_levels_byte_length < 0 or v2.repetition_levels_byte_length < 0) return error.BadPageHeader;
                    const levels: usize = @as(usize, @intCast(v2.definition_levels_byte_length)) +
                        @as(usize, @intCast(v2.repetition_levels_byte_length));
                    if (levels > body.len or levels > raw_len) return error.BadPageHeader;
                    const values = try recode(arena, body[levels..], from, to, raw_len - levels);
                    const joined = try arena.alloc(u8, levels + values.len);
                    @memcpy(joined[0..levels], body[0..levels]);
                    @memcpy(joined[levels..], values);
                    new_body = joined;
                }
            },
            // Index pages carry no compressed payload.
            .INDEX_PAGE => {},
        }

        const new_hdr = try patchPageHeader(arena, chunk[pos..][0..hdr_len], @intCast(new_body.len));
        try pages.append(arena, .{ .src_off = pos, .dst_off = out.items.len, .dst_len = new_hdr.len + new_body.len });
        try out.appendSlice(arena, new_hdr);
        try out.appendSlice(arena, new_body);
        header_delta += @as(i64, @intCast(new_hdr.len)) - @as(i64, @intCast(hdr_len));
        pos += hdr_len + body_len;
    }
    return .{ .bytes = out.items, .pages = pages.items, .header_delta = header_delta };
}

fn recode(
    arena: std.mem.Allocator,
    body: []const u8,
    from: schema.CompressionCodec,
    to: schema.CompressionCodec,
    raw_len: usize,
) ![]const u8 {
    const raw = try compression.decompress(arena, body, from, raw_len);
    return compression.compress(arena, raw, to);
}

/// Re-emit a serialized PageHeader with `compressed_page_size` replaced and
/// `crc` dropped, copying every other top-level field's bytes verbatim.
fn patchPageHeader(arena: std.mem.Allocator, header: []const u8, compressed_size: i32) ![]const u8 {
    var r = thrift.Reader.init(header);
    var w = thrift.Writer.init(arena);
    r.readStructBegin();
    w.writeStructBegin();
    while (true) {
        const f = try r.readFieldBegin();
        if (f.type == .Stop) break;
        const start = r.pos;
        try r.skip(f.type);
        switch (f.id) {
            3 => try w.writeFieldI32(3, compressed_size),
            4 => {},
            else => {
                try w.writeFieldBegin(f.type, f.id);
                try w.writeBytes(header[start..r.pos]);
            },
        }
    }
    try w.writeStructEnd();
    return w.bytes();
}

/// The output offset of the page that started `src_rel` bytes into the source chunk.
fn movedPage(pages: []const PageMove, src_rel: i64) ?PageMove {
    if (src_rel < 0) return null;
    const want: usize = @intCast(src_rel);
    // Pages are recorded in chunk order, so `src_off` ascends.
    var lo: usize = 0;
    var hi: usize = pages.len;
    while (lo < hi) {
        const mid = lo + (hi - lo) / 2;
        if (pages[mid].src_off < want) lo = mid + 1 else hi = mid;
    }
    return if (lo < pages.len and pages[lo].src_off == want) pages[lo] else null;
}

/// Where an offset `src_rel` bytes into the source chunk lands in the
/// recompressed one: the same page when it names a page start, else the next
/// page start (or the end). Writers do record offsets that name no page, such
/// as a `data_page_offset` of 0 on a chunk holding only a dictionary page.
fn relocated(re: RecompressedChunk, src_rel: i64) usize {
    if (movedPage(re.pages, src_rel)) |p| return p.dst_off;
    for (re.pages) |p| if (@as(i64, @intCast(p.src_off)) >= src_rel) return p.dst_off;
    return re.bytes.len;
}

/// Byte-copy path for a chunk whose codec differs from the requested one:
/// recompress its pages, write them at `out_offset`, and return the chunk's
/// metadata pointing at them. The page index follows the pages (ColumnIndex
/// unchanged, OffsetIndex locations remapped); if a location does not land on
/// a page start, both are dropped rather than written wrong.
fn copyChunkRecompressed(
    rc: Recompress,
    src: RGSrc,
    src_chunk: schema.ColumnChunk,
    slice: []const u8,
    src_start: usize,
    sink: streaming.Sink,
    out_offset: *u64,
    timings: *Timings,
    emit_page_index: bool,
) !schema.ColumnChunk {
    var scratch_state = std.heap.ArenaAllocator.init(rc.scratch);
    defer scratch_state.deinit();
    const scratch = scratch_state.allocator();
    const m = src_chunk.meta_data.?;

    const t_encode = nowMonoNs();
    const re = recompressChunk(scratch, slice, m.codec, rc.codec) catch |err| {
        if (err == error.UnsupportedCodec) {
            std.debug.print("zpq: cannot recompress column `{s}` to {s}: its pages use {s}, which zpq cannot decompress\n", .{
                if (m.path_in_schema.items.len > 0) m.path_in_schema.items[m.path_in_schema.items.len - 1] else "?",
                @tagName(rc.codec),
                @tagName(m.codec),
            });
        }
        return err;
    };
    timings.encode_ns += @intCast(nowMonoNs() - t_encode);

    const new_start: i64 = @intCast(out_offset.*);
    const t_sink_start = nowMonoNs();
    try sink.write(re.bytes);
    timings.sink_ns += @intCast(nowMonoNs() - t_sink_start);
    out_offset.* += re.bytes.len;

    const src_base: i64 = @intCast(src_start);
    var out = src_chunk;
    var meta = m;
    meta.codec = rc.codec;
    meta.total_compressed_size = @intCast(re.bytes.len);
    meta.total_uncompressed_size += re.header_delta;
    meta.data_page_offset = new_start + @as(i64, @intCast(relocated(re, m.data_page_offset - src_base)));
    if (m.dictionary_page_offset) |d| {
        meta.dictionary_page_offset = new_start + @as(i64, @intCast(relocated(re, d - src_base)));
    }
    meta.index_page_offset = if (m.index_page_offset) |ip|
        if (movedPage(re.pages, ip - src_base)) |p| new_start + @as(i64, @intCast(p.dst_off)) else null
    else
        null;
    out.meta_data = meta;
    out.file_offset = meta.data_page_offset;
    out.offset_index_offset = null;
    out.offset_index_length = null;
    out.column_index_offset = null;
    out.column_index_length = null;
    if (!emit_page_index) return out;

    // The OffsetIndex is carried when every location remaps onto a page
    // start; the ColumnIndex only beside it, since its pages are found
    // through those locations.
    const oo = src_chunk.offset_index_offset orelse return out;
    const ol = src_chunk.offset_index_length orelse return out;
    const oi_bytes = originSlice(src, oo, ol) orelse return out;
    var oi_reader = thrift.Reader.init(oi_bytes);
    var oi = schema.OffsetIndex.read(scratch, &oi_reader) catch return out;
    for (oi.page_locations.items) |*loc| {
        const p = movedPage(re.pages, loc.offset - src_base) orelse return out;
        loc.offset = new_start + @as(i64, @intCast(p.dst_off));
        loc.compressed_page_size = @intCast(p.dst_len);
    }
    var oi_w = thrift.Writer.init(scratch);
    oi.write(&oi_w) catch return out;

    if (src_chunk.column_index_offset) |co| if (src_chunk.column_index_length) |cl| {
        if (originSlice(src, co, cl)) |ci_bytes| if (fastpath.reserializeColumnIndex(scratch, ci_bytes)) |ci_ser| {
            out.column_index_offset = @intCast(out_offset.*);
            out.column_index_length = @intCast(ci_ser.len);
            try sink.write(ci_ser);
            out_offset.* += ci_ser.len;
        };
    };
    out.offset_index_offset = @intCast(out_offset.*);
    out.offset_index_length = @intCast(oi_w.bytes().len);
    try sink.write(oi_w.bytes());
    out_offset.* += oi_w.bytes().len;
    return out;
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

/// Whether a chunk's ColumnIndex is fit to prune pages with. Page pruning and the always-match fill act on these
/// claims without decoding, so one the footer or the OffsetIndex can contradict for free is dropped whole and the
/// chunk reads as if it had no page index: a writer that got one claim wrong cannot be trusted on its neighbours.
///
/// The contradictions checked:
///   - per-page lists whose lengths disagree with the OffsetIndex page count;
///   - a negative null count, which no page can have;
///   - a page flagged all-null on a leaf with no optional or repeated ancestor (max definition level 0), which
///     cannot hold a null at all;
///   - for a non-repeated leaf, whose entries are its rows, a page flagged all-null whose null count is not its row
///     count, or null pages covering more rows than the chunk's own statistics say are null.
///
/// Writers that collected no page statistics are known to flag every page null and record -1 null counts, while
/// the pages hold ordinary values; trusting them drops every row.
pub fn pageIndexIsPlausible(
    ci: *const schema.ColumnIndex,
    oi: *const schema.OffsetIndex,
    cm: schema.ColumnMetaData,
    levels: schema.Levels,
    rg_rows: i64,
) bool {
    const locs = oi.page_locations.items;
    const n = locs.len;
    if (ci.null_pages.items.len != n or ci.min_values.items.len != n or ci.max_values.items.len != n) return false;
    if (ci.null_counts) |nc| {
        if (nc.items.len != n) return false;
        for (nc.items) |c| if (c < 0) return false;
    }

    var null_page_rows: i64 = 0;
    for (ci.null_pages.items, 0..) |is_null, i| {
        if (!is_null) continue;
        if (levels.max_def == 0) return false;
        // A repeated leaf's counts are entries, not rows; only the flag itself is checked there.
        if (levels.max_rep > 0) continue;
        const end = if (i + 1 < n) locs[i + 1].first_row_index else rg_rows;
        const rows = end - locs[i].first_row_index;
        if (ci.null_counts) |nc| if (nc.items[i] != rows) return false;
        null_page_rows += rows;
    }
    if (levels.max_rep == 0) {
        if (cm.statistics) |s| if (s.null_count) |chunk_nulls| {
            if (null_page_rows > chunk_nulls) return false;
        };
    }
    return true;
}

/// Whether a column's page index can drive page pruning. Its arrays are indexed by page number together, and each
/// page's `first_row_index` slices the row selection, so the arrays must agree in length and the row starts must be
/// in range and non-decreasing. Page offsets and sizes are checked again where the pages are read.
fn pageIndexUsable(ci: schema.ColumnIndex, oi: schema.OffsetIndex, num_rows: usize) bool {
    const locs = oi.page_locations.items;
    if (ci.null_pages.items.len != locs.len) return false;
    if (ci.min_values.items.len != locs.len or ci.max_values.items.len != locs.len) return false;
    if (ci.null_counts) |nc| if (nc.items.len != locs.len) return false;
    var prev: i64 = 0;
    for (locs) |loc| {
        if (loc.offset < 0 or loc.compressed_page_size < 0) return false;
        if (loc.first_row_index < prev) return false;
        if (loc.first_row_index > num_rows) return false;
        prev = loc.first_row_index;
    }
    return true;
}

test "pageIndexUsable rejects page indexes whose arrays or row starts disagree" {
    const testing = std.testing;
    var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_state.deinit();
    const a = arena_state.allocator();

    var ci: schema.ColumnIndex = .{};
    var oi: schema.OffsetIndex = .{};
    for ([_]i64{ 0, 10, 20 }) |first| {
        try ci.null_pages.append(a, false);
        try ci.min_values.append(a, "");
        try ci.max_values.append(a, "");
        try oi.page_locations.append(a, .{ .offset = 4 + first, .compressed_page_size = 8, .first_row_index = first });
    }
    try testing.expect(pageIndexUsable(ci, oi, 30));
    try testing.expect(!pageIndexUsable(ci, oi, 19)); // a page starts past the row group

    oi.page_locations.items[2].first_row_index = 5; // goes backwards
    try testing.expect(!pageIndexUsable(ci, oi, 30));
    oi.page_locations.items[2].first_row_index = 20;

    oi.page_locations.items[0].first_row_index = -1;
    try testing.expect(!pageIndexUsable(ci, oi, 30));
    oi.page_locations.items[0].first_row_index = 0;

    _ = ci.min_values.pop(); // ColumnIndex shorter than OffsetIndex
    try testing.expect(!pageIndexUsable(ci, oi, 30));
}

/// A decoded column must describe exactly its row group's rows: a flat column one value per row, a repeated one
/// `num_rows` record starts (repetition level 0), the first entry among them. Filters, the selection vector and the
/// output aggregator all walk every column against `num_rows`, so a chunk whose counts or levels disagree is corrupt.
/// O(1) for flat columns; a pass over the levels for repeated ones.
pub fn checkRowShape(col: filter_eval.Batch.Column, num_rows: usize) error{ColumnRowCountMismatch}!void {
    switch (col) {
        inline else => |c| {
            if (c.rep_levels) |rl| {
                if (rl.len > 0 and rl[0] != 0) return error.ColumnRowCountMismatch;
                var starts: usize = 0;
                for (rl) |r| starts += @intFromBool(r == 0);
                if (starts != num_rows) return error.ColumnRowCountMismatch;
            } else if (c.values.len != num_rows) return error.ColumnRowCountMismatch;
            if (c.def_levels) |dl| if (dl.len != c.values.len) return error.ColumnRowCountMismatch;
        },
    }
}

/// One uncompressed page: header + body.
fn pageForTest(arena: std.mem.Allocator, header: schema.PageHeader, body: []const u8) ![]u8 {
    var w = thrift.Writer.init(arena);
    try header.write(&w);
    var out: std.ArrayList(u8) = .empty;
    try out.appendSlice(arena, w.bytes());
    try out.appendSlice(arena, body);
    return out.items;
}

test "a page claiming more values than its chunk is rejected before sizing buffers from the claim" {
    // A few bytes can claim any count: one RLE def-level run of N present values. The level buffer used to be
    // sized from that claim, so a one-row chunk could make the reader allocate gigabytes.
    const testing = std.testing;
    var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_state.deinit();
    const a = arena_state.allocator();

    const claimed: u32 = 1 << 24;
    var body: std.ArrayList(u8) = .empty;
    var run: [8]u8 = undefined; // ULEB128 run header (claimed << 1), then the 1-byte value
    var n: u32 = claimed << 1;
    var len: usize = 0;
    while (true) : (len += 1) {
        run[len] = @truncate(n & 0x7f);
        n >>= 7;
        if (n == 0) break;
        run[len] |= 0x80;
    }
    len += 1;
    run[len] = 1;
    len += 1;
    var prefix: [4]u8 = undefined;
    std.mem.writeInt(u32, &prefix, @intCast(len), .little);
    try body.appendSlice(a, &prefix);
    try body.appendSlice(a, run[0..len]);
    try body.appendSlice(a, &[_]u8{ 42, 0, 0, 0 });
    const page = try pageForTest(a, .{
        .type = .DATA_PAGE,
        .uncompressed_page_size = @intCast(body.items.len),
        .compressed_page_size = @intCast(body.items.len),
        .crc = null,
        .data_page_header = .{ .num_values = claimed, .encoding = .PLAIN, .definition_level_encoding = .RLE, .repetition_level_encoding = .RLE },
        .dictionary_page_header = null,
        .data_page_header_v2 = null,
    }, body.items);
    const optional: schema.Levels = .{ .max_def = 1, .max_rep = 0 };
    try testing.expectError(error.PageValueCountExceedsChunk, decodeColumnT(i32, a, page, .UNCOMPRESSED, optional, 1));

    // A dictionary page claiming far more entries than its bytes can hold.
    const dict_body = [_]u8{ 1, 0, 0, 0, 'a' };
    const dict_page = try pageForTest(a, .{
        .type = .DICTIONARY_PAGE,
        .uncompressed_page_size = dict_body.len,
        .compressed_page_size = dict_body.len,
        .crc = null,
        .data_page_header = null,
        .dictionary_page_header = .{ .num_values = 1 << 20, .encoding = .PLAIN, .is_sorted = null },
        .data_page_header_v2 = null,
    }, &dict_body);
    const required: schema.Levels = .{ .max_def = 0, .max_rep = 0 };
    try testing.expectError(error.DictionaryLargerThanPage, decodeColumnT([]const u8, a, dict_page, .UNCOMPRESSED, required, 1));
}

test "checkRowShape ties flat and repeated columns to the row count" {
    const testing = std.testing;
    try checkRowShape(.{ .i32 = .{ .values = &.{ 1, 2, 3 } } }, 3);
    try testing.expectError(error.ColumnRowCountMismatch, checkRowShape(.{ .i32 = .{ .values = &.{ 1, 2 } } }, 3));
    // Repeated: three records, the second holding two entries.
    const rep = filter_eval.Batch.Column{ .i32 = .{
        .values = &.{ 1, 2, 3, 4 },
        .def_levels = &.{ 1, 1, 1, 1 },
        .max_def = 1,
        .rep_levels = &.{ 0, 0, 1, 0 },
        .max_rep = 1,
    } };
    try checkRowShape(rep, 3);
    try testing.expectError(error.ColumnRowCountMismatch, checkRowShape(rep, 4));
    const no_start = filter_eval.Batch.Column{ .i32 = .{ .values = &.{1}, .rep_levels = &.{1}, .max_rep = 1 } };
    try testing.expectError(error.ColumnRowCountMismatch, checkRowShape(no_start, 1));
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

/// A chunk's page index resolved against the filter: which pages to skip,
/// which the filter matches in full, and where each page lies.
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
    reader.value_budget = num_leaves;
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
        // Always materialised on the pruned path, even under
        // `--fast-levels`. The zero-fill is load-bearing here: a page
        // this scan skips keeps def level 0, which is what makes
        // downstream treat its untouched default values as null rather
        // than as real data. Dropping the array would turn skipped
        // pages into present zeros. Pages that ARE decoded still take
        // the cheap all-present path inside `decodePageSlice`.
        def_levels = try arena.alloc(u32, num_leaves);
        @memset(def_levels.?, 0);
    }

    // `dictionary_page_offset` is optional. When it is absent, writers may
    // place a dictionary at `data_page_offset` or put the first data page
    // there. The OffsetIndex distinguishes the observed shapes: a first data
    // page after the chunk start leaves a leading page worth probing, while an
    // equal offset proves there is nothing to install. Do not use the
    // deprecated `ColumnChunk.file_offset`, which is inconsistent across
    // writers. A file that hides a dictionary before an unfetched
    // `data_page_offset` still fails cleanly with DictionaryMissing.
    const possible_dict_offset = prune.dictionary_page_offset orelse blk: {
        if (prune.locations.len > 0 and
            prune.locations[0].offset > prune.chunk_file_offset)
        {
            break :blk prune.chunk_file_offset;
        }
        break :blk null;
    };
    if (possible_dict_offset) |dict_off| {
        _ = try reader.seekAndInstallDictionaryPage(dict_off, prune.chunk_file_offset);
    }

    // Page row starts slice `values`, which is sized from the chunk's value count, not the row group's row count
    // the index was checked against.
    for (prune.locations) |loc| if (loc.first_row_index > num_leaves) return error.ShortDecode;

    // 2. Loop over pages and decode or skip
    for (prune.locations, 0..) |loc, pi| {
        const start: usize = @intCast(loc.first_row_index);
        const end: usize = if (pi + 1 < prune.locations.len) @intCast(prune.locations[pi + 1].first_row_index) else num_leaves;

        const is_skipped = prune.page_is_skipped[pi];
        // An always-match page stands in for its rows with values that satisfy what the filter proved. On a page
        // the ColumnIndex flags all-null that is `IS NULL`, so its rows must read as absent (definition level 0),
        // not as present copies of the page minimum. A leaf without levels cannot express that: decode instead.
        const null_page = pi < prune.col_index.null_pages.items.len and prune.col_index.null_pages.items[pi];
        const is_always_match = prune.page_is_always_match[pi] and (!null_page or def_levels != null);

        if (is_skipped or is_always_match) {
            if (is_always_match and null_page) {
                @memset(values[start..end], defaultVal(T));
                @memset(def_levels.?[start..end], 0);
            } else if (is_always_match) {
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

/// Decode an entire column chunk into a `ColumnT(T)` view: values
/// plus def_levels (OPTIONAL) and optionally rep_levels (LIST/MAP).
/// `num_leaves` is the column chunk's `num_values` from metadata
/// (counts LEAVES, not logical rows — for nested cols this can be
/// larger than the RG's row count).
pub fn decodeColumnTPruned(
    comptime T: type,
    arena: std.mem.Allocator,
    chunk: []const u8,
    codec: schema.CompressionCodec,
    levels: schema.Levels,
    num_leaves: usize,
    prune: ?PruningInfo,
    decode_options: DecodeOptions,
    scratch: ?*DecodeScratch,
) !filter_eval.ColumnT(T) {
    var reader = column_mod.ColumnChunkReader(T).initWithOptions(chunk, codec, levels, arena, decode_options, scratch);
    if (prune) |p| {
        return decodeWithReaderPruned(T, arena, &reader, levels, num_leaves, p);
    } else {
        return decodeWithReader(T, &reader, num_leaves);
    }
}

fn decodeFloat16ColumnAsF64Pruned(
    arena: std.mem.Allocator,
    chunk: []const u8,
    codec: schema.CompressionCodec,
    levels: schema.Levels,
    num_leaves: usize,
    prune: ?PruningInfo,
    decode_options: DecodeOptions,
    scratch: ?*DecodeScratch,
) !filter_eval.ColumnT(f64) {
    const cb = try decodeFlbaColumnPruned(arena, chunk, codec, levels, num_leaves, 2, prune, decode_options, scratch);
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
    decode_options: DecodeOptions,
    scratch: ?*DecodeScratch,
) !filter_eval.ColumnT(i64) {
    const c32 = try decodeColumnTPruned(i32, arena, chunk, codec, levels, num_leaves, prune, decode_options, scratch);
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
    decode_options: DecodeOptions,
    scratch: ?*DecodeScratch,
) !filter_eval.ColumnT([]const u8) {
    var reader = column_mod.ColumnChunkReader([]const u8).initWithOptions(
        chunk,
        codec,
        levels,
        arena,
        decode_options,
        scratch,
    );
    reader.type_length = type_length;
    if (prune) |p| {
        return decodeWithReaderPruned([]const u8, arena, &reader, levels, num_leaves, p);
    } else {
        return decodeWithReader([]const u8, &reader, num_leaves);
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
    return decodeColumnTWithOptions(T, arena, chunk, codec, levels, num_leaves, .{});
}

pub fn decodeColumnTWithOptions(
    comptime T: type,
    arena: std.mem.Allocator,
    chunk: []const u8,
    codec: schema.CompressionCodec,
    levels: schema.Levels,
    num_leaves: usize,
    decode_options: DecodeOptions,
) !filter_eval.ColumnT(T) {
    var reader = column_mod.ColumnChunkReader(T).initWithOptions(chunk, codec, levels, arena, decode_options, null);
    return decodeWithReader(T, &reader, num_leaves);
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
    decode_options: DecodeOptions,
) !filter_eval.ColumnT(f64) {
    const cb = try decodeFlbaColumnWithOptions(arena, chunk, codec, levels, num_leaves, 2, decode_options);
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
    decode_options: DecodeOptions,
) !filter_eval.ColumnT(i64) {
    const c32 = try decodeColumnTWithOptions(i32, arena, chunk, codec, levels, num_leaves, decode_options);
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
    return decodeFlbaColumnWithOptions(arena, chunk, codec, levels, num_leaves, type_length, .{});
}

pub fn decodeFlbaColumnWithOptions(
    arena: std.mem.Allocator,
    chunk: []const u8,
    codec: schema.CompressionCodec,
    levels: schema.Levels,
    num_leaves: usize,
    type_length: usize,
    decode_options: DecodeOptions,
) !filter_eval.ColumnT([]const u8) {
    var reader = column_mod.ColumnChunkReader([]const u8).initWithOptions(
        chunk,
        codec,
        levels,
        arena,
        decode_options,
        null,
    );
    reader.type_length = type_length;
    return decodeWithReader([]const u8, &reader, num_leaves);
}

fn decodeWithReader(
    comptime T: type,
    reader: *column_mod.ColumnChunkReader(T),
    num_leaves: usize,
) !filter_eval.ColumnT(T) {
    const leaves = try reader.readAll(num_leaves);
    return .{
        .values = leaves.values,
        .def_levels = leaves.def_levels,
        .max_def = @intCast(reader.levels.max_def),
        .rep_levels = leaves.rep_levels,
        .max_rep = @intCast(reader.levels.max_rep),
        .has_nulls = leaves.has_nulls,
    };
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

test "initOutputAggregator widens FLOAT16 passthrough to DOUBLE" {
    const testing = std.testing;
    var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_state.deinit();
    const arena = arena_state.allocator();

    var file_meta = schema.FileMetaData{
        .version = 2,
        .schema = .empty,
        .num_rows = 1,
        .created_by = null,
        .row_groups = .empty,
    };
    try file_meta.schema.append(arena, .{
        .type = null,
        .type_length = null,
        .repetition_type = .REQUIRED,
        .name = "schema",
        .num_children = 1,
        .scale = null,
        .precision = null,
        .field_id = null,
    });
    try file_meta.schema.append(arena, .{
        .type = .FIXED_LEN_BYTE_ARRAY,
        .type_length = 2,
        .repetition_type = .OPTIONAL,
        .name = "half",
        .num_children = 0,
        .logical_type = .{ .FLOAT16 = .{} },
        .scale = null,
        .precision = null,
        .field_id = 7,
    });

    var path: schema.StringList = .empty;
    try path.append(arena, "half");
    var columns: std.ArrayListUnmanaged(schema.ColumnChunk) = .empty;
    try columns.append(arena, .{
        .file_path = null,
        .file_offset = 0,
        .meta_data = .{
            .type = .FIXED_LEN_BYTE_ARRAY,
            .encodings = .empty,
            .path_in_schema = path,
            .codec = .UNCOMPRESSED,
            .num_values = 1,
            .total_uncompressed_size = 0,
            .total_compressed_size = 0,
            .data_page_offset = 0,
            .index_page_offset = null,
            .dictionary_page_offset = null,
        },
    });
    try file_meta.row_groups.append(arena, .{
        .columns = columns,
        .total_byte_size = 0,
        .num_rows = 1,
    });

    const specs = [_]OutputCol{.{ .passthrough = 0 }};
    const agg = try initOutputAggregator(arena, &file_meta, &specs);
    try testing.expect(agg.cols[0] == .f64);
    try testing.expectEqual(schema.Type.DOUBLE, agg.schema_elems[0].type.?);
    try testing.expectEqual(schema.FieldRepetitionType.OPTIONAL, agg.schema_elems[0].repetition_type.?);
    try testing.expectEqual(@as(?i32, 7), agg.schema_elems[0].field_id);
    try testing.expect(agg.schema_elems[0].logical_type == null);
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
        true, // direct-to-file: emit page index
        null,
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
            .type = .INT32,
            .type_length = null,
            .repetition_type = .REQUIRED,
            .name = "value",
            .num_children = 0,
            .converted_type = .DECIMAL,
            .logical_type = null,
            .scale = 2,
            .precision = 9,
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
            .type = .INT64,
            .type_length = null,
            .repetition_type = .REQUIRED,
            .name = "value",
            .num_children = 0,
            .converted_type = .DECIMAL,
            .logical_type = null,
            .scale = 3,
            .precision = 18,
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

// --- --fast-levels: all-present definition-level detection ---
//
// The flag lets a decode skip materialising definition levels for any
// page whose level stream proves every value present. These build the
// awkward shapes by hand — an all-present page, a page with real nulls,
// and a chunk that changes from one to the other mid-stream — because
// the mid-stream transition is the only place the optimisation has to
// hand work back to the ordinary path, and getting the hand-off wrong
// would silently shift every value after it.
//
// Every case asserts the SAME values with the flag on and off. A faster
// wrong answer is the failure mode worth guarding, not a slower one.

const fl_test_elem = schema.SchemaElement{
    .type = .INT32,
    .type_length = null,
    .repetition_type = .OPTIONAL,
    .name = "v",
    .num_children = 0,
    .converted_type = null,
    .logical_type = null,
    .scale = null,
    .precision = null,
    .field_id = null,
};

/// Encode one OPTIONAL INT32 page. `def` of null means "no level array
/// supplied" — the encoder then writes an all-present stream itself.
fn flEncodePage(
    arena: std.mem.Allocator,
    values: []const i32,
    def: ?[]const u32,
) ![]u8 {
    const enc = try encoder.encodeColumn(arena, .{
        .values = .{ .i32 = .{ .values = values, .def_levels = def, .max_def = 1 } },
        .schema_elem = &fl_test_elem,
        .path_in_schema = &[_][]const u8{"v"},
        .codec = .UNCOMPRESSED,
    });
    return enc.bytes;
}

const fl_levels = schema.Levels{ .max_def = 1, .max_rep = 0 };

test "fast-levels: all-present OPTIONAL column decodes identically with the flag on" {
    const testing = std.testing;
    var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_state.deinit();
    const arena = arena_state.allocator();

    // High-cardinality so the encoder stays PLAIN rather than dict.
    var vals: [512]i32 = undefined;
    for (&vals, 0..) |*v, i| v.* = @intCast(i * 7919);
    const chunk = try flEncodePage(arena, &vals, null);

    const off = try decodeColumnT(i32, arena, chunk, .UNCOMPRESSED, fl_levels, vals.len);
    const on = try decodeColumnTWithOptions(
        i32,
        arena,
        chunk,
        .UNCOMPRESSED,
        fl_levels,
        vals.len,
        .{ .fast_levels = true },
    );

    try testing.expectEqualSlices(i32, &vals, off.values);
    try testing.expectEqualSlices(i32, &vals, on.values);

    // The whole point: with the flag on the level array is never built.
    // `def_levels == null` alongside `max_def == 1` is how the column
    // reports "no nulls", which is exactly what the off path spells out
    // one u32 at a time.
    try testing.expect(off.def_levels != null);
    for (off.def_levels.?) |d| try testing.expectEqual(@as(u32, 1), d);
    try testing.expect(on.def_levels == null);
    try testing.expectEqual(@as(u32, 1), on.max_def);
    try testing.expect(!on.has_nulls);
}

test "fast-levels: a page with real nulls still decodes through the ordinary path" {
    const testing = std.testing;
    var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_state.deinit();
    const arena = arena_state.allocator();

    // 300 slots, every 5th one null. Values sit at their logical index
    // so a misplaced scatter is visible, not just a wrong count.
    const n = 300;
    var vals: [n]i32 = undefined;
    var def: [n]u32 = undefined;
    for (0..n) |i| {
        const is_null = (i % 5) == 0;
        def[i] = if (is_null) 0 else 1;
        vals[i] = if (is_null) 0 else @intCast(i * 31 + 1);
    }
    const chunk = try flEncodePage(arena, &vals, &def);

    const off = try decodeColumnT(i32, arena, chunk, .UNCOMPRESSED, fl_levels, n);
    const on = try decodeColumnTWithOptions(
        i32,
        arena,
        chunk,
        .UNCOMPRESSED,
        fl_levels,
        n,
        .{ .fast_levels = true },
    );

    try testing.expectEqualSlices(i32, &vals, off.values);
    try testing.expectEqualSlices(i32, &vals, on.values);
    // Nulls are present, so the levels must be materialised either way.
    try testing.expect(on.def_levels != null);
    try testing.expectEqualSlices(u32, &def, on.def_levels.?);
    try testing.expect(on.has_nulls);
}

test "fast-levels: chunk that turns null part-way hands back to the level path" {
    const testing = std.testing;
    var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_state.deinit();
    const arena = arena_state.allocator();

    // Page 1: 256 values, all present — taken by the fast path.
    // Page 2: 256 values, half null — must stop the fast path, allocate
    //         the level array, back-fill page 1 as all-present, and
    //         resume without dropping or shifting a value.
    const per_page = 256;
    var p1: [per_page]i32 = undefined;
    for (&p1, 0..) |*v, i| v.* = @intCast(i * 7919 + 3);

    var p2: [per_page]i32 = undefined;
    var p2_def: [per_page]u32 = undefined;
    for (0..per_page) |i| {
        const is_null = (i % 2) == 1;
        p2_def[i] = if (is_null) 0 else 1;
        p2[i] = if (is_null) 0 else @intCast(1_000_000 + i * 13);
    }

    // `EncodedColumn.bytes` is documented as header-plus-data ready to
    // concatenate, which is what makes a two-page chunk buildable here.
    const page1 = try flEncodePage(arena, &p1, null);
    const page2 = try flEncodePage(arena, &p2, &p2_def);
    const chunk = try arena.alloc(u8, page1.len + page2.len);
    @memcpy(chunk[0..page1.len], page1);
    @memcpy(chunk[page1.len..], page2);

    const total = per_page * 2;
    var want_vals: [total]i32 = undefined;
    var want_def: [total]u32 = undefined;
    @memcpy(want_vals[0..per_page], &p1);
    @memcpy(want_vals[per_page..], &p2);
    @memset(want_def[0..per_page], 1);
    @memcpy(want_def[per_page..], &p2_def);

    const off = try decodeColumnT(i32, arena, chunk, .UNCOMPRESSED, fl_levels, total);
    const on = try decodeColumnTWithOptions(
        i32,
        arena,
        chunk,
        .UNCOMPRESSED,
        fl_levels,
        total,
        .{ .fast_levels = true },
    );

    try testing.expectEqualSlices(i32, &want_vals, off.values);
    try testing.expectEqualSlices(i32, &want_vals, on.values);
    try testing.expect(on.def_levels != null);
    // Page 1's levels were never decoded from the wire — they are the
    // back-fill. If the hand-off is off by a page these are 0, not 1.
    try testing.expectEqualSlices(u32, &want_def, on.def_levels.?);
    try testing.expect(on.has_nulls);
}

test "fast-levels: all-present pages on both sides of a null page" {
    const testing = std.testing;
    var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_state.deinit();
    const arena = arena_state.allocator();

    // The fast path only runs while it has never seen a null. Once it
    // hands off it must not resume, or the third page's levels would go
    // unwritten and read back as the zeroes nobody wrote.
    const per_page = 128;
    var present: [per_page]i32 = undefined;
    for (&present, 0..) |*v, i| v.* = @intCast(i * 104729 + 5);

    var mid: [per_page]i32 = undefined;
    var mid_def: [per_page]u32 = undefined;
    for (0..per_page) |i| {
        const is_null = i < 4;
        mid_def[i] = if (is_null) 0 else 1;
        mid[i] = if (is_null) 0 else @intCast(500 + i);
    }

    const pages = [_][]u8{
        try flEncodePage(arena, &present, null),
        try flEncodePage(arena, &mid, &mid_def),
        try flEncodePage(arena, &present, null),
    };
    var total_len: usize = 0;
    for (pages) |p| total_len += p.len;
    const chunk = try arena.alloc(u8, total_len);
    var at: usize = 0;
    for (pages) |p| {
        @memcpy(chunk[at .. at + p.len], p);
        at += p.len;
    }

    const total = per_page * 3;
    const off = try decodeColumnT(i32, arena, chunk, .UNCOMPRESSED, fl_levels, total);
    const on = try decodeColumnTWithOptions(
        i32,
        arena,
        chunk,
        .UNCOMPRESSED,
        fl_levels,
        total,
        .{ .fast_levels = true },
    );

    try testing.expectEqualSlices(i32, off.values, on.values);
    try testing.expectEqualSlices(u32, off.def_levels.?, on.def_levels.?);
    try testing.expectEqualSlices(i32, &present, on.values[per_page * 2 ..]);
    for (on.def_levels.?[per_page * 2 ..]) |d| try testing.expectEqual(@as(u32, 1), d);
}

test "computedSchemaElem keeps the unsigned annotation of an aliased UINT64 reference" {
    const ref: expr_ast.Expr = .{ .col_ref = .{
        .col_idx = 0,
        .physical_type = .INT64,
        .expr_type = .i64,
        .unsigned_64 = true,
    } };
    const elem = computedSchemaElem(.{ .expr = ref, .alias = "b" });
    try std.testing.expect(schema.isUnsignedInt64(elem));
    try std.testing.expectEqualStrings("b", elem.name);

    var signed = ref;
    signed.col_ref.unsigned_64 = false;
    try std.testing.expect(!schema.isUnsignedInt(computedSchemaElem(.{ .expr = signed, .alias = "b" })));
}

test "pageIndexIsPlausible: corpus page indexes are kept except the ones their footers contradict" {
    const testing = std.testing;
    // Every parquet-testing file that carries a page index. The datapage_v1 trio flags every page of two REQUIRED
    // columns null (with -1 null counts) while the pages hold values; the rest, int32_with_null_pages's genuine null
    // pages included, must keep their index so page pruning still applies to them.
    const Case = struct { path: []const u8, plausible: bool };
    const cases = [_]Case{
        .{ .path = "alltypes_tiny_pages.parquet", .plausible = true },
        .{ .path = "alltypes_tiny_pages_plain.parquet", .plausible = true },
        .{ .path = "binary_truncated_min_max.parquet", .plausible = true },
        .{ .path = "data_index_bloom_encoding_stats.parquet", .plausible = true },
        .{ .path = "data_index_bloom_encoding_with_length.parquet", .plausible = true },
        .{ .path = "datapage_v2_empty_datapage.snappy.parquet", .plausible = true },
        .{ .path = "delta_encoding_required_column.parquet", .plausible = true },
        .{ .path = "fixed_length_byte_array.parquet", .plausible = true },
        .{ .path = "floating_orders_nan_count.parquet", .plausible = true },
        .{ .path = "int32_with_null_pages.parquet", .plausible = true },
        .{ .path = "old_list_structure.parquet", .plausible = true },
        .{ .path = "plain-dict-uncompressed-checksum.parquet", .plausible = true },
        .{ .path = "repeated_primitive_no_list.parquet", .plausible = true },
        .{ .path = "rle-dict-snappy-checksum.parquet", .plausible = true },
        .{ .path = "datapage_v1-corrupt-checksum.parquet", .plausible = false },
        .{ .path = "datapage_v1-snappy-compressed-checksum.parquet", .plausible = false },
        .{ .path = "datapage_v1-uncompressed-checksum.parquet", .plausible = false },
    };
    for (cases) |case| {
        var path_buf: [256]u8 = undefined;
        const path = try std.fmt.bufPrint(&path_buf, "data/parquet-testing/data/{s}", .{case.path});
        const bytes = metadata.readFileSlice(path, testing.allocator) catch |err| {
            if (err == error.FileNotFound) {
                std.debug.print("skipping: {s} not present\n", .{path});
                return error.SkipZigTest;
            }
            return err;
        };
        defer testing.allocator.free(bytes);
        var meta = try metadata.open(testing.allocator, bytes);
        defer meta.deinit(testing.allocator);

        var arena = std.heap.ArenaAllocator.init(testing.allocator);
        defer arena.deinit();
        var indexed: usize = 0;
        var rejected: usize = 0;
        for (meta.row_groups.items) |rg| for (rg.columns.items) |chunk| {
            const cm = chunk.meta_data orelse continue;
            const co: usize = @intCast(chunk.column_index_offset orelse continue);
            const oo: usize = @intCast(chunk.offset_index_offset orelse continue);
            var cr = thrift.Reader.init(bytes[co..][0..@intCast(chunk.column_index_length.?)]);
            const col_index = try schema.ColumnIndex.read(arena.allocator(), &cr);
            var orr = thrift.Reader.init(bytes[oo..][0..@intCast(chunk.offset_index_length.?)]);
            const offset_index = try schema.OffsetIndex.read(arena.allocator(), &orr);
            indexed += 1;
            const levels = meta.getColumnLevels(cm.path_in_schema.items);
            if (!pageIndexIsPlausible(&col_index, &offset_index, cm, levels, rg.num_rows)) rejected += 1;
        };
        testing.expect(indexed > 0) catch |err| {
            std.debug.print("{s}: no page index found\n", .{case.path});
            return err;
        };
        const want_rejected: usize = if (case.plausible) 0 else indexed;
        testing.expectEqual(want_rejected, rejected) catch |err| {
            std.debug.print("{s}: rejected {d} of {d} page indexes\n", .{ case.path, rejected, indexed });
            return err;
        };
    }
}

test "pageIndexIsPlausible: each contradiction drops the index on its own" {
    const testing = std.testing;
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    // Two pages of 10 and 6 rows; page 1 is all null.
    var oi: schema.OffsetIndex = .{};
    try oi.page_locations.appendSlice(a, &.{
        .{ .offset = 4, .compressed_page_size = 10, .first_row_index = 0 },
        .{ .offset = 14, .compressed_page_size = 10, .first_row_index = 10 },
    });
    const Fixture = struct {
        fn index(alloc: std.mem.Allocator, null_pages: []const bool, null_counts: ?[]const i64) !schema.ColumnIndex {
            var ci: schema.ColumnIndex = .{};
            try ci.null_pages.appendSlice(alloc, null_pages);
            for (null_pages) |np| {
                const bound: []const u8 = if (np) "" else "\x01\x00\x00\x00";
                try ci.min_values.append(alloc, bound);
                try ci.max_values.append(alloc, bound);
            }
            if (null_counts) |nc| {
                var list: std.ArrayListUnmanaged(i64) = .empty;
                try list.appendSlice(alloc, nc);
                ci.null_counts = list;
            }
            return ci;
        }
    };
    const cm: schema.ColumnMetaData = .{
        .type = .INT32,
        .encodings = .empty,
        .path_in_schema = .empty,
        .codec = .UNCOMPRESSED,
        .num_values = 16,
        .total_uncompressed_size = 0,
        .total_compressed_size = 0,
        .data_page_offset = 4,
        .index_page_offset = null,
        .dictionary_page_offset = null,
    };
    const optional: schema.Levels = .{ .max_def = 1, .max_rep = 0 };
    const required: schema.Levels = .{ .max_def = 0, .max_rep = 0 };
    const repeated: schema.Levels = .{ .max_def = 2, .max_rep = 1 };

    const good = try Fixture.index(a, &.{ false, true }, &.{ 2, 6 });
    try testing.expect(pageIndexIsPlausible(&good, &oi, cm, optional, 16));
    // Without null counts the flag alone is taken on an optional leaf.
    const no_counts = try Fixture.index(a, &.{ false, true }, null);
    try testing.expect(pageIndexIsPlausible(&no_counts, &oi, cm, optional, 16));
    // A null page on a leaf that cannot be null.
    try testing.expect(!pageIndexIsPlausible(&no_counts, &oi, cm, required, 16));
    // A required leaf with no null page is fine.
    const all_values = try Fixture.index(a, &.{ false, false }, &.{ 0, 0 });
    try testing.expect(pageIndexIsPlausible(&all_values, &oi, cm, required, 16));
    // Negative null counts.
    const negative = try Fixture.index(a, &.{ false, false }, &.{ -1, 0 });
    try testing.expect(!pageIndexIsPlausible(&negative, &oi, cm, optional, 16));
    // A null page whose null count is not its row count.
    const short = try Fixture.index(a, &.{ false, true }, &.{ 0, 5 });
    try testing.expect(!pageIndexIsPlausible(&short, &oi, cm, optional, 16));
    // ...is only checked where entries are rows.
    try testing.expect(pageIndexIsPlausible(&short, &oi, cm, repeated, 16));
    // Null pages covering more rows than the chunk statistics count as null.
    var with_stats = cm;
    with_stats.statistics = .{ .null_count = 3 };
    try testing.expect(!pageIndexIsPlausible(&no_counts, &oi, with_stats, optional, 16));
    with_stats.statistics = .{ .null_count = 6 };
    try testing.expect(pageIndexIsPlausible(&no_counts, &oi, with_stats, optional, 16));
    // Per-page lists that disagree with the OffsetIndex page count.
    const one_page = try Fixture.index(a, &.{false}, null);
    try testing.expect(!pageIndexIsPlausible(&one_page, &oi, cm, optional, 16));
}

test "recompressChunk: a codec round trip gives back the chunk byte for byte" {
    // tiny_pages: uncompressed v1 pages, dictionaries, no CRCs, so UNCOMPRESSED -> ZSTD -> UNCOMPRESSED must reproduce
    // every header field and payload exactly. datapage_v2: v2 pages, whose levels stay outside the compressed values.
    const testing = std.testing;
    const Case = struct { path: []const u8, via: schema.CompressionCodec };
    const cases = [_]Case{
        .{ .path = "data/parquet-testing/data/alltypes_tiny_pages.parquet", .via = .ZSTD },
        .{ .path = "data/parquet-testing/data/datapage_v2.snappy.parquet", .via = .GZIP },
    };
    for (cases) |case| {
        const bytes = metadata.readFileSlice(case.path, testing.allocator) catch |err| {
            if (err == error.FileNotFound) {
                std.debug.print("skipping: {s} not present\n", .{case.path});
                return error.SkipZigTest;
            }
            return err;
        };
        defer testing.allocator.free(bytes);
        var meta = try metadata.open(testing.allocator, bytes);
        defer meta.deinit(testing.allocator);

        var arena = std.heap.ArenaAllocator.init(testing.allocator);
        defer arena.deinit();
        const a = arena.allocator();
        var pages: usize = 0;
        for (meta.row_groups.items) |rg| for (rg.columns.items) |chunk| {
            const cm = chunk.meta_data.?;
            const start: usize = @intCast(cm.dictionary_page_offset orelse cm.data_page_offset);
            const src = bytes[start..][0..@intCast(cm.total_compressed_size)];
            // Normalise to UNCOMPRESSED first, so the comparison does not depend on one compressor's output.
            const plain = try recompressChunk(a, src, cm.codec, .UNCOMPRESSED);
            const there = try recompressChunk(a, plain.bytes, .UNCOMPRESSED, case.via);
            const back = try recompressChunk(a, there.bytes, case.via, .UNCOMPRESSED);
            try testing.expectEqualSlices(u8, plain.bytes, back.bytes);
            if (cm.codec == .UNCOMPRESSED) try testing.expectEqualSlices(u8, src, plain.bytes);
            try testing.expectEqual(plain.pages.len, there.pages.len);
            // `there` was cut from `plain`, so each of its pages starts where `plain` put that page.
            for (there.pages, plain.pages) |t, p| try testing.expectEqual(p.dst_off, t.src_off);
            pages += there.pages.len;
        };
        try testing.expect(pages > 0);
    }
}
