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
const profile = @import("../profile.zig");

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
const agg_plan = @import("agg_plan.zig");
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
const agg_coded = @import("expr/agg_coded.zig");
const partagg = @import("partagg.zig");

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
    /// The accumulating thread's profile buffer; spans reuse the timestamps these counters are taken from. A Timings
    /// accumulates on one thread at a time, which is what makes handing it the buffer safe.
    prof: ?*profile.Thread = null,
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
    /// Keep a dictionary-encoded source column's codes and dictionary instead of its values, for the encoder to
    /// re-dictionary without hashing a value. Only for an aggregator holding one row group (the per-row-group
    /// re-encode): codes index one source dictionary.
    keep_codes: bool = false,

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
        i128: TypedBuf(i128),
    };

    pub fn TypedBuf(comptime T: type) type {
        return struct {
            values: std.ArrayList(T) = .empty,
            def_levels: std.ArrayList(u32) = .empty,
            max_def: u32 = 0,
            /// With `coded`, the rows are `codes` into `dict` (0 at null rows) and `values` is empty.
            coded: bool = false,
            codes: std.ArrayList(u32) = .empty,
            dict: []const T = &.{},
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
                // The leaf and its path come from the schema, which every chunk's path_in_schema matches (footer
                // open enforces it), so a file with no row groups still builds its output schema.
                var leaves: schema.LeafIterator = .init(meta.schema.items);
                const leaf = while (leaves.next()) |l| {
                    if (l.index == kept_ci) break l;
                } else return error.ColumnMetaMissing;
                const src_elem = leaf.element.*;
                paths[i] = try arena.dupe([]const u8, leaf.path);

                // DECIMAL source columns. INT32/INT64/FLBA-backed take the
                // lossless integer lane: decode to unscaled i128, keep the
                // source DECIMAL schema element, re-encode at the same
                // physical type. BYTE_ARRAY-backed decimals re-encode as
                // FIXED_LEN_BYTE_ARRAY of the minimal width for their
                // precision. Byte-copy projection (copyRG / fastpath)
                // preserves any DECIMAL untouched.
                if (decimal_mod.kindFromSchema(&src_elem)) |k| {
                    if (k.physical == .INT32 or k.physical == .INT64 or
                        k.physical == .FIXED_LEN_BYTE_ARRAY)
                    {
                        schemas[i] = src_elem; // keep DECIMAL annotation
                        cols[i] = .{ .i128 = .{} };
                        continue;
                    } else if (k.physical == .BYTE_ARRAY) {
                        if (k.precision <= 38) {
                            schemas[i] = src_elem;
                            schemas[i].type = .FIXED_LEN_BYTE_ARRAY;
                            schemas[i].type_length = decimal_mod.minBytesForPrecision(k.precision);
                            cols[i] = .{ .i128 = .{} };
                        } else {
                            // Legacy BYTE_ARRAY decimals with precision > 38 fall back to the f64 lane
                            // (read/print path; re-encode writes are refused with the column named).
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
                        }
                        continue;
                    }
                    return error.UnsupportedDecimalPhysicalType;
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
                // An exact DECIMAL result buffers its unscaled values, encoded at the width its schema leaf names.
                if (expr_type == .dec) {
                    cols[i] = .{ .i128 = .{} };
                    continue;
                }
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
                tb.codes.clearRetainingCapacity();
                tb.coded = false;
                tb.dict = &.{};
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
    var p: ProjectedRG = undefined;
    try p.init(agg, gpa, rg, meta, src, filter, fetch_set, output_specs, timings, decode_options);
    defer p.deinit();
    if (p.surviving == 0) return 0;
    const project_span = profile.Span.begin(timings.prof);
    defer project_span.end(.project, profile.args(.{ p.surviving, output_specs.len }));
    for (0..output_specs.len) |i| try p.projectColumn(agg, i, gpa, timings);
    agg.num_rows += p.surviving;
    return p.surviving;
}

/// A row group with its filter evaluated, its output columns still to project one at a time (`projectColumn`). Late
/// materialisation: only what the filter and computed outputs read is decoded here; every other output column is
/// decoded only when projected, on an arena released right after. Holds its arena by address: never moved once
/// `init` has run.
pub const ProjectedRG = struct {
    arena_state: std.heap.ArenaAllocator,
    rg: *const schema.RowGroup,
    meta: *const schema.FileMetaData,
    src: RGSrc,
    fetch_set: []const bool,
    output_specs: []const OutputCol,
    decode_options: DecodeOptions,
    batch: filter_eval.Batch,
    lookup: []?usize,
    batch_pos_for_col: []?usize,
    /// Per leaf: a DECIMAL column both filtered on and written as DECIMAL, decoded once as unscaled integers; the
    /// filter reads the f64 column made from them, the output these.
    unscaled: []?filter_eval.ColumnT(i128),
    sel: filter_selection.SelectionVector,
    surviving: usize,

    pub fn init(
        self: *ProjectedRG,
        agg: *const OutputAggregator,
        gpa: std.mem.Allocator,
        rg: *const schema.RowGroup,
        meta: *const schema.FileMetaData,
        src: RGSrc,
        filter: ?filter_ast.Filter,
        fetch_set: []const bool,
        output_specs: []const OutputCol,
        timings: *Timings,
        decode_options: DecodeOptions,
    ) !void {
        self.arena_state = std.heap.ArenaAllocator.init(gpa);
        errdefer self.arena_state.deinit();
        const ra = self.arena_state.allocator();
        const num_rows: usize = @intCast(rg.num_rows);
        const num_leaves = rg.columns.items.len;

        var batch_cols: std.ArrayList(filter_eval.Batch.Column) = .empty;
        const lookup = try ra.alloc(?usize, meta.schema.items.len);
        @memset(lookup, null);
        const batch_pos_for_col = try ra.alloc(?usize, num_leaves);
        @memset(batch_pos_for_col, null);

        const early = try ra.alloc(bool, num_leaves);
        @memset(early, false);
        if (filter) |f| {
            var cols: std.ArrayList(usize) = .empty;
            try f.collectColumns(&cols, ra);
            for (cols.items) |ci| if (ci < num_leaves) {
                early[ci] = true;
            };
        }
        // Columns a computed output reads stay values; any other may decode to dictionary codes (`keep_codes`).
        const computed_refs = try ra.alloc(bool, num_leaves);
        @memset(computed_refs, false);
        for (output_specs) |spec| switch (spec) {
            .computed => |c| c.expr.collectColumns(computed_refs),
            .passthrough => {},
        };
        for (early, computed_refs) |*e, c| e.* = e.* or c;
        // Leaves an output writes through the lossless DECIMAL lane (INT32/INT64-backed passthrough decimals).
        const unscaled = try ra.alloc(?filter_eval.ColumnT(i128), num_leaves);
        @memset(unscaled, null);
        const lossless = try ra.alloc(bool, num_leaves);
        @memset(lossless, false);
        for (output_specs, agg.cols) |spec, cbuf| switch (spec) {
            .passthrough => |ci| if (cbuf == .i128 and ci < num_leaves) {
                lossless[ci] = true;
            },
            .computed => {},
        };

        const t_decode_start = nowMonoNs();
        for (fetch_set, 0..) |needed, ci| {
            if (!needed or !early[ci]) continue;
            const kind = if (lossless[ci]) decimalKind(rg, meta, ci) else null;
            const int_backed = if (kind) |k| k.physical == .INT32 or k.physical == .INT64 else false;
            const decoded: filter_eval.Batch.Column = if (int_backed) blk: {
                const dcol = try decodeDecimalI128(ra, rg, meta, src, ci, decode_options);
                unscaled[ci] = dcol;
                var view = try decimal_mod.scaledF64FromI128(ra, dcol, kind.?.scale);
                view.unscaled = .{ .i128 = dcol.values };
                break :blk .{ .f64 = view };
            } else if (agg.keep_codes and !computed_refs[ci])
                try decodeWriteColumnMaybeCoded(ra, rg, meta, src, ci, decode_options, timings.prof)
            else
                try decodeWriteColumn(ra, rg, meta, src, ci, decode_options, timings.prof);
            batch_pos_for_col[ci] = batch_cols.items.len;
            lookup[ci] = batch_cols.items.len;
            try batch_cols.append(ra, decoded);
        }
        const t_decode_end = nowMonoNs();
        timings.decode_ns += @intCast(t_decode_end - t_decode_start);
        profile.record(timings.prof, .decode, t_decode_start, t_decode_end, profile.args(.{ 0, 0, batch_cols.items.len }));

        const batch: filter_eval.Batch = .{ .cols = batch_cols.items, .num_rows = num_rows };
        var sel = try filter_selection.SelectionVector.init(ra, num_rows);
        if (filter) |f| try filter_eval.evaluate(f, &batch, &sel, lookup, ra);
        const t_eval_end = nowMonoNs();
        timings.eval_ns += @intCast(t_eval_end - t_decode_end);
        const surviving = sel.count();
        profile.record(timings.prof, .filter, t_decode_end, t_eval_end, profile.args(.{ num_rows, surviving }));

        self.* = .{
            .arena_state = self.arena_state,
            .rg = rg,
            .meta = meta,
            .src = src,
            .fetch_set = fetch_set,
            .output_specs = output_specs,
            .decode_options = decode_options,
            .batch = batch,
            .lookup = lookup,
            .batch_pos_for_col = batch_pos_for_col,
            .unscaled = unscaled,
            .sel = sel,
            .surviving = surviving,
        };
    }

    pub fn deinit(self: *ProjectedRG) void {
        self.arena_state.deinit();
    }

    /// Append output column `i`'s surviving rows to `agg.cols[i]` (on `agg.arena`). A column the filter didn't need
    /// decodes on an arena over `gpa`, freed before returning. The caller adds `surviving` to `agg.num_rows`.
    pub fn projectColumn(
        self: *ProjectedRG,
        agg: *OutputAggregator,
        i: usize,
        gpa: std.mem.Allocator,
        timings: *Timings,
    ) !void {
        const ra = self.arena_state.allocator();
        const rg = self.rg;
        const meta = self.meta;
        const src = self.src;
        const decode_options = self.decode_options;
        const spec = self.output_specs[i];
        // Lossless DECIMAL lane: a passthrough decimal's buf is `.i128`. Decode the source chunk to unscaled i128,
        // apply the selection, and append the integers untouched. A computed DECIMAL evaluates to `.i128` below.
        if (agg.cols[i] == .i128 and spec == .passthrough) {
            const kept_ci = spec.passthrough;
            var late = std.heap.ArenaAllocator.init(gpa);
            defer late.deinit();
            const dcol = self.unscaled[kept_ci] orelse
                try decodeDecimalI128(late.allocator(), rg, meta, src, kept_ci, decode_options);
            const filtered_dec = try encoder.applySelectionI128(late.allocator(), dcol, &self.sel);
            const f = filtered_dec;
            try appendTyped(i128, &agg.cols[i].i128, f.values, f.def_levels, f.max_def, agg.arena);
            return;
        }

        var late = std.heap.ArenaAllocator.init(gpa);
        defer late.deinit();
        const col: filter_eval.Batch.Column = switch (spec) {
            .passthrough => |kept_ci| if (self.batch_pos_for_col[kept_ci]) |pos|
                self.batch.cols[pos]
            else if (kept_ci < self.fetch_set.len and self.fetch_set[kept_ci]) blk: {
                const t0 = nowMonoNs();
                const la = late.allocator();
                const decoded = if (agg.keep_codes)
                    try decodeWriteColumnMaybeCoded(la, rg, meta, src, kept_ci, decode_options, timings.prof)
                else
                    try decodeWriteColumn(la, rg, meta, src, kept_ci, decode_options, timings.prof);
                const t1 = nowMonoNs();
                timings.decode_ns += @intCast(t1 - t0);
                profile.record(timings.prof, .decode, t0, t1, profile.args(.{ 0, 0, 1 }));
                break :blk decoded;
            } else return error.MissingDecodedColumn,
            .computed => |c| try expr_eval.evalExpr(ra, &self.batch.restrictedTo(&self.sel), self.lookup, c.expr),
        };
        if (col.nullInfo().max_rep == 0) {
            try appendSelected(&agg.cols[i], col, &self.sel, self.surviving, agg.arena, agg.keep_codes, late.allocator());
            return;
        }
        try appendIntoBuf(&agg.cols[i], try encoder.applySelection(late.allocator(), col, &self.sel), agg.arena);
    }
};

/// Decode column `ci` of a row group into the batch lane the write path keeps it in.
fn decodeWriteColumn(
    ra: std.mem.Allocator,
    rg: *const schema.RowGroup,
    meta: *const schema.FileMetaData,
    src: RGSrc,
    ci: usize,
    decode_options: DecodeOptions,
    prof: ?*profile.Thread,
) !filter_eval.Batch.Column {
    const num_rows: usize = @intCast(rg.num_rows);
    const chunk_span = profile.Span.begin(prof);
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

    // DECIMAL columns, whatever their backing: the f64 view filters compare, with the exact unscaled values beside it
    // for expressions and aggregates (`decodeColumnExact`).
    const schema_elem = meta.getColumnSchema(cm.path_in_schema.items);
    const dec_kind: ?decimal_mod.Kind = if (schema_elem) |se| decimal_mod.kindFromSchema(&se) else null;

    const decoded: filter_eval.Batch.Column = if (dec_kind) |k| .{
        .f64 = try decimal_mod.decodeColumnExact(ra, chunk, cm.codec, levels, n_leaves, k, decode_options, null),
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
    if (chunk_span.t != null) chunk_span.end(.column_chunk, chunkArgs(0, 0, ci, cm));
    return decoded;
}

/// Append a slice's surviving rows to `agg` (on `agg.arena`): `batch` holds every fetched column of the slice
/// (`lookup` by leaf), `sel` its filtered rows. The sliced counterpart of `ProjectedRG.projectColumn` over every
/// output.
pub fn appendSliceRows(
    agg: *OutputAggregator,
    batch: *const filter_eval.Batch,
    lookup: []const ?usize,
    sel: *const filter_selection.SelectionVector,
    output_specs: []const OutputCol,
    scratch: std.mem.Allocator,
) !usize {
    const surviving = sel.count();
    if (surviving == 0) return 0;
    for (output_specs, 0..) |spec, i| {
        var col: filter_eval.Batch.Column = switch (spec) {
            .passthrough => |ci| batch.cols[lookup[ci] orelse return error.MissingDecodedColumn],
            .computed => |c| try expr_eval.evalExpr(scratch, &batch.restrictedTo(sel), lookup, c.expr),
        };
        // A DECIMAL written losslessly buffers its exact integers.
        if (agg.cols[i] == .i128 and col == .f64) {
            // Into a temporary first: assigned straight into `col`, the union's new tag lands before `col` is read.
            const exact = try expr_eval.unscaledView(scratch, col);
            col = .{ .i128 = exact };
        }
        try appendSelected(&agg.cols[i], col, sel, surviving, agg.arena, false, scratch);
    }
    agg.num_rows += surviving;
    return surviving;
}

/// One flat column chunk decoded a slice of rows at a time, into the lane `decodeWriteColumn` gives the whole chunk:
/// for a row group too big to hold decoded at once. Each reader keeps its own page buffers, so the memory held is a
/// slice of values plus a page per column (and a byte-array column's dictionary): fixed-width readers recycle theirs
/// (`scratch`); byte-array readers decode a page at a time onto `pages`, copy the slice's bytes out and free the page
/// once it is drained.
pub const SliceReader = struct {
    /// In place for the reader's life: a reader holds pointers into itself once it has decoded.
    raw: *Raw,
    scratch: *DecodeScratch,
    /// A byte-array reader's page allocations, reset whenever a page is drained.
    pages: *std.heap.ArenaAllocator,
    /// Holds a byte-array dictionary for the chunk, copied off `pages` once installed.
    held: std.mem.Allocator,
    dict_held: bool = false,
    levels: schema.Levels,
    dec: ?decimal_mod.Kind,
    float16: bool,
    int96: bool,
    left: usize,

    const Raw = union(enum) {
        i32: column_mod.ColumnChunkReader(i32),
        i64: column_mod.ColumnChunkReader(i64),
        f32: column_mod.ColumnChunkReader(f32),
        f64: column_mod.ColumnChunkReader(f64),
        boolean: column_mod.ColumnChunkReader(bool),
        bytes: column_mod.ColumnChunkReader([]const u8),
    };

    /// A reader of leaf `ci`, or null when the column has no sliced form (nested); the caller then decodes the row
    /// group whole. `a` holds the slices' bookkeeping and must live until the last slice is used.
    pub fn init(
        a: std.mem.Allocator,
        gpa: std.mem.Allocator,
        rg: *const schema.RowGroup,
        meta: *const schema.FileMetaData,
        src: RGSrc,
        ci: usize,
        decode_options: DecodeOptions,
    ) !?SliceReader {
        if (!supports(rg, meta, ci)) return null;
        const cm = rg.columns.items[ci].meta_data orelse return error.ColumnMetaMissing;
        const levels = meta.getColumnLevels(cm.path_in_schema.items);
        const se = meta.getColumnSchema(cm.path_in_schema.items);
        const dec: ?decimal_mod.Kind = if (se) |e| decimal_mod.kindFromSchema(&e) else null;
        if (dec) |k| {
            if (cm.type == .FIXED_LEN_BYTE_ARRAY and
                (k.byte_width == 0 or k.byte_width > decimal_mod.MAX_FLBA_BYTE_WIDTH))
                return error.DecimalByteWidthTooLarge;
        }
        const start: usize = if (cm.dictionary_page_offset) |dp| @intCast(dp) else @intCast(cm.data_page_offset);
        const len: usize = @intCast(cm.total_compressed_size);
        invariant.assert(start >= @as(usize, @intCast(src.byte_origin)));
        const buf_off = start - @as(usize, @intCast(src.byte_origin));
        if (buf_off + len > src.bytes.len) return error.MissingChunkBytes;
        const chunk = src.bytes[buf_off .. buf_off + len];
        const scratch = try a.create(DecodeScratch);
        scratch.* = .init(gpa);
        errdefer scratch.deinit();
        const pages = try a.create(std.heap.ArenaAllocator);
        pages.* = .init(gpa);
        const raw = try a.create(Raw);
        raw.* = switch (cm.type) {
            inline .INT32, .INT64, .FLOAT, .DOUBLE, .BOOLEAN => |t| blk: {
                const T, const tag = switch (t) {
                    .INT32 => .{ i32, "i32" },
                    .INT64 => .{ i64, "i64" },
                    .FLOAT => .{ f32, "f32" },
                    .DOUBLE => .{ f64, "f64" },
                    .BOOLEAN => .{ bool, "boolean" },
                    else => comptime unreachable,
                };
                const Reader = column_mod.ColumnChunkReader(T);
                const r = Reader.initWithOptions(chunk, cm.codec, levels, a, decode_options, scratch);
                break :blk @unionInit(Raw, tag, r);
            },
            .BYTE_ARRAY, .FIXED_LEN_BYTE_ARRAY, .INT96 => blk: {
                const Reader = column_mod.ColumnChunkReader([]const u8);
                var r = Reader.initWithOptions(chunk, cm.codec, levels, pages.allocator(), decode_options, null);
                r.type_length = switch (cm.type) {
                    .FIXED_LEN_BYTE_ARRAY => flbaWidth(se),
                    .INT96 => int96_mod.INT96_WIDTH,
                    else => 0,
                };
                break :blk .{ .bytes = r };
            },
        };
        return .{
            .raw = raw,
            .scratch = scratch,
            .pages = pages,
            .held = a,
            .levels = levels,
            .dec = dec,
            .float16 = if (se) |e| schema.isFloat16(e) else false,
            .int96 = cm.type == .INT96,
            .left = @intCast(cm.num_values),
        };
    }

    pub fn deinit(self: *SliceReader) void {
        self.scratch.deinit();
        self.pages.deinit();
    }

    /// Whether leaf `ci` has a sliced form (what `init` returns non-null for).
    pub fn supports(rg: *const schema.RowGroup, meta: *const schema.FileMetaData, ci: usize) bool {
        const cm = rg.columns.items[ci].meta_data orelse return false;
        return meta.getColumnLevels(cm.path_in_schema.items).max_rep == 0;
    }

    /// The next `n` rows (fewer only at the chunk's end), on `a`.
    pub fn next(self: *SliceReader, a: std.mem.Allocator, n_want: usize) !filter_eval.Batch.Column {
        const n = @min(n_want, self.left);
        self.left -= n;
        switch (self.raw.*) {
            inline else => |*r, tag| {
                const T = @TypeOf(r.*).Value;
                const values = try a.alloc(T, n);
                const def: ?[]u32 = if (self.levels.max_def > 0) try a.alloc(u32, n) else null;
                const got = if (T == []const u8)
                    try self.decodeBytes(r, a, values, def)
                else
                    try decodeSome(T, r, values, def);
                if (got != n) return error.ShortDecode;
                const max_def: u32 = @intCast(self.levels.max_def);
                var has_nulls = false;
                if (def) |d| for (d) |v| {
                    if (v < max_def) {
                        has_nulls = true;
                        break;
                    }
                };
                const col: filter_eval.ColumnT(T) = .{
                    .values = values,
                    .def_levels = def,
                    .max_def = max_def,
                    .has_nulls = has_nulls,
                };
                if (self.dec) |k| return .{ .f64 = try exactSlice(T, a, col, k) };
                if (T == []const u8 and self.int96) {
                    const out = try a.alloc(i64, n);
                    for (values, out) |b, *o| o.* = if (b.len == int96_mod.INT96_WIDTH)
                        int96_mod.int96ToEpochNanos(b)
                    else
                        0;
                    const nanos: filter_eval.ColumnT(i64) = .{
                        .values = out,
                        .def_levels = def,
                        .max_def = max_def,
                        .has_nulls = has_nulls,
                    };
                    return .{ .i64 = nanos };
                }
                if (self.float16 and T == []const u8) {
                    const out = try a.alloc(f64, n);
                    for (values, out) |b, *o| o.* = if (b.len >= 2)
                        @floatCast(@as(f16, @bitCast(std.mem.readInt(u16, b[0..2], .little))))
                    else
                        0;
                    const half: filter_eval.ColumnT(f64) = .{
                        .values = out,
                        .def_levels = def,
                        .max_def = max_def,
                        .has_nulls = has_nulls,
                    };
                    return .{ .f64 = half };
                }
                return switch (tag) {
                    .bytes => .{ .string = col },
                    else => @unionInit(filter_eval.Batch.Column, @tagName(tag), col),
                };
            },
        }
    }

    fn decodeSome(comptime T: type, r: *column_mod.ColumnChunkReader(T), values: []T, def: ?[]u32) !usize {
        var got: usize = 0;
        while (got < values.len) {
            const k = if (def) |d|
                try r.decodeWithLevels(values[got..], d[got..])
            else
                try r.decode(values[got..]);
            if (k == 0) break;
            got += k;
        }
        return got;
    }

    /// Byte arrays a page at a time: each page's values are copied onto `a` before the next page is read, and a
    /// drained page's bytes are freed, so the reader holds one page however many the slice spans.
    fn decodeBytes(
        self: *SliceReader,
        r: *column_mod.ColumnChunkReader([]const u8),
        a: std.mem.Allocator,
        values: [][]const u8,
        def: ?[]u32,
    ) !usize {
        var got: usize = 0;
        while (got < values.len) {
            if (r.pageValuesLeft() == 0) {
                _ = self.pages.reset(.retain_capacity);
                if (!try r.nextPage()) break;
                if (!self.dict_held) {
                    // The dictionary page comes first, so it is in by now; it serves every page that follows.
                    if (r.dictionary) |d| r.dictionary = try copyBytes(self.held, try self.held.dupe([]const u8, d));
                    self.dict_held = true;
                }
                continue;
            }
            const want = @min(values.len - got, r.pageValuesLeft());
            const dst = values[got..][0..want];
            const k = if (def) |d| try r.decodeWithLevels(dst, d[got..][0..want]) else try r.decode(dst);
            if (k != want) return error.ShortDecode;
            _ = try copyBytes(a, dst);
            got += k;
        }
        return got;
    }

    /// Re-point `vals` at one block of `a` holding their bytes.
    fn copyBytes(a: std.mem.Allocator, vals: [][]const u8) ![][]const u8 {
        var total: usize = 0;
        for (vals) |v| total += v.len;
        const buf = try a.alloc(u8, total);
        var off: usize = 0;
        for (vals) |*v| {
            @memcpy(buf[off..][0..v.len], v.*);
            v.* = buf[off..][0..v.len];
            off += v.len;
        }
        return vals;
    }

    /// A DECIMAL slice as `decimal_mod.decodeColumnExact` gives a chunk: the f64 view with the exact integers beside.
    fn exactSlice(
        comptime T: type,
        a: std.mem.Allocator,
        col: filter_eval.ColumnT(T),
        k: decimal_mod.Kind,
    ) !filter_eval.ColumnT(f64) {
        const out = try a.alloc(f64, col.values.len);
        var view: filter_eval.ColumnT(f64) = .{
            .values = out,
            .def_levels = col.def_levels,
            .max_def = col.max_def,
            .has_nulls = col.has_nulls,
        };
        switch (T) {
            i32, i64 => {
                decimal_mod.applyScaleSimd(T, col.values, k.scale, out);
                view.unscaled = @unionInit(filter_eval.Exact, @typeName(T), col.values);
            },
            []const u8 => {
                const u = try a.alloc(i128, col.values.len);
                for (col.values, out, u, 0..) |b, *o, *x, i| {
                    const present = if (col.def_levels) |d| d[i] >= col.max_def else true;
                    if (!present) {
                        o.* = 0;
                        x.* = 0;
                        continue;
                    }
                    if (b.len > decimal_mod.MAX_FLBA_BYTE_WIDTH) return error.DecimalByteWidthTooLarge;
                    x.* = decimal_mod.flbaToI128(b);
                    o.* = decimal_mod.applyScaleI128(x.*, k.scale);
                }
                view.unscaled = .{ .i128 = u };
            },
            else => return error.UnsupportedDecimalPhysicalType,
        }
        return view;
    }
};

/// Decode one DECIMAL column's chunk to unscaled i128 integers (the
/// lossless output lane). Re-derives the chunk window the same way the
/// batch-decode loop does. Caller guarantees the column is an
/// INT32/INT64-backed decimal.
/// Leaf `ci`'s DECIMAL kind, or null when it is no decimal.
fn decimalKind(rg: *const schema.RowGroup, meta: *const schema.FileMetaData, ci: usize) ?decimal_mod.Kind {
    const cm = rg.columns.items[ci].meta_data orelse return null;
    const se = meta.getColumnSchema(cm.path_in_schema.items) orelse return null;
    return decimal_mod.kindFromSchema(&se);
}

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

/// Append the rows `sel` keeps of a flat column straight into the aggregator's buffer: one copy, no intermediate
/// filtered column. String bytes are copied into one block of `arena`, since the decoded pages they point into die
/// with the row group.
///
/// A coded column (`codes` + `dict`) keeps its codes when `keep_codes` lets the buffer take them: the buffer is still
/// empty, so every code indexes this one dictionary, which is copied into `arena`. Otherwise its values are gathered
/// first, on `scratch`.
fn appendSelected(
    buf: *OutputAggregator.ColumnBuf,
    src_in: filter_eval.Batch.Column,
    sel: *const filter_selection.SelectionVector,
    surviving: usize,
    arena: std.mem.Allocator,
    keep_codes: bool,
    scratch: std.mem.Allocator,
) !void {
    var src = src_in;
    if (src.codes() != null) {
        const fresh = switch (buf.*) {
            inline else => |*tb| tb.values.items.len == 0 and !tb.coded,
        };
        if (keep_codes and fresh) return appendSelectedCodes(buf, src, sel, surviving, arena);
        src = try agg_coded.materialize(scratch, src);
    }
    switch (buf.*) {
        inline else => |*tb| if (tb.coded) try materializeBuf(tb, arena),
    }
    switch (src) {
        inline else => |c, tag| {
            const T = @TypeOf(c.values[0]);
            const tb = &@field(buf.*, @tagName(tag));
            // Exact sizing: the aggregator usually holds one row group, and ArrayList growth would leave half again
            // as much unused (and, being arena memory, never reclaimed).
            try tb.values.ensureTotalCapacityPrecise(arena, tb.values.items.len + surviving);
            const out = tb.values.addManyAsSliceAssumeCapacity(surviving);
            encoder.compactByMask(T, c.values, sel.mask, out);
            if (T == []const u8) {
                var total: usize = 0;
                for (out) |v| total += v.len;
                const block = try arena.alloc(u8, total);
                var pos: usize = 0;
                for (out) |*v| {
                    @memcpy(block[pos..][0..v.len], v.*);
                    v.* = block[pos..][0..v.len];
                    pos += v.len;
                }
            }
            if (c.max_def == 0) return;
            tb.max_def = c.max_def;
            try tb.def_levels.ensureTotalCapacityPrecise(arena, tb.def_levels.items.len + surviving);
            const dl_out = tb.def_levels.addManyAsSliceAssumeCapacity(surviving);
            if (c.def_levels) |dl| {
                encoder.compactByMask(u32, dl, sel.mask, dl_out);
            } else @memset(dl_out, c.max_def);
        },
    }
}

fn appendSelectedCodes(
    buf: *OutputAggregator.ColumnBuf,
    src: filter_eval.Batch.Column,
    sel: *const filter_selection.SelectionVector,
    surviving: usize,
    arena: std.mem.Allocator,
) !void {
    switch (src) {
        inline else => |c, tag| {
            const T = @TypeOf(c.dict[0]);
            const tb = &@field(buf.*, @tagName(tag));
            try tb.codes.ensureTotalCapacityPrecise(arena, surviving);
            encoder.compactByMask(u32, c.codes.?, sel.mask, tb.codes.addManyAsSliceAssumeCapacity(surviving));
            if (T == []const u8) {
                // The entries point into the source's decoded dictionary page, which dies with the row group.
                var total: usize = 0;
                for (c.dict) |v| total += v.len;
                const block = try arena.alloc(u8, total);
                const dict = try arena.alloc([]const u8, c.dict.len);
                var pos: usize = 0;
                for (dict, c.dict) |*d, v| {
                    @memcpy(block[pos..][0..v.len], v);
                    d.* = block[pos..][0..v.len];
                    pos += v.len;
                }
                tb.dict = dict;
            } else tb.dict = try arena.dupe(T, c.dict);
            tb.coded = true;
            if (c.max_def == 0) return;
            tb.max_def = c.max_def;
            try tb.def_levels.ensureTotalCapacityPrecise(arena, surviving);
            const dl_out = tb.def_levels.addManyAsSliceAssumeCapacity(surviving);
            if (c.def_levels) |dl| {
                encoder.compactByMask(u32, dl, sel.mask, dl_out);
            } else @memset(dl_out, c.max_def);
        },
    }
}

/// Turn a coded buffer back into values, for an append it cannot keep codes for.
fn materializeBuf(tb: anytype, arena: std.mem.Allocator) !void {
    const T = @TypeOf(tb.dict[0]);
    try tb.values.ensureTotalCapacityPrecise(arena, tb.codes.items.len);
    const vals = tb.values.addManyAsSliceAssumeCapacity(tb.codes.items.len);
    for (vals, tb.codes.items, 0..) |*v, k, r| {
        const present = tb.max_def == 0 or tb.def_levels.items[r] == tb.max_def;
        v.* = if (present) tb.dict[k] else std.mem.zeroes(T);
    }
    tb.codes.clearRetainingCapacity();
    tb.coded = false;
    tb.dict = &.{};
}

/// `decodeWriteColumn`, but a dictionary-encoded chunk of a type the write path keeps unconverted decodes to codes
/// (see `ColumnChunkReader.readAllCodes`); anything else decodes as values.
fn decodeWriteColumnMaybeCoded(
    ra: std.mem.Allocator,
    rg: *const schema.RowGroup,
    meta: *const schema.FileMetaData,
    src: RGSrc,
    ci: usize,
    decode_options: DecodeOptions,
    prof: ?*profile.Thread,
) !filter_eval.Batch.Column {
    const cm = rg.columns.items[ci].meta_data orelse return error.ColumnMetaMissing;
    const levels = meta.getColumnLevels(cm.path_in_schema.items);
    const se = meta.getColumnSchema(cm.path_in_schema.items);
    const converted = if (se) |e| decimal_mod.kindFromSchema(&e) != null or schema.isFloat16(e) else false;
    if (!decode_options.encoded or converted or levels.max_rep > 0)
        return decodeWriteColumn(ra, rg, meta, src, ci, decode_options, prof);
    const start: usize = if (cm.dictionary_page_offset) |dp| @intCast(dp) else @intCast(cm.data_page_offset);
    const chunk = try sourceBytes(src, .{ .start = start, .len = @intCast(cm.total_compressed_size) });
    const n_leaves: usize = @intCast(cm.num_values);
    const chunk_span = profile.Span.begin(prof);
    const coded: ?filter_eval.Batch.Column = switch (cm.type) {
        .INT32 => try codedOrValues(i32, .i32, ra, chunk, cm, levels, n_leaves, decode_options, null),
        .INT64 => try codedOrValues(i64, .i64, ra, chunk, cm, levels, n_leaves, decode_options, null),
        .FLOAT => try codedOrValues(f32, .f32, ra, chunk, cm, levels, n_leaves, decode_options, null),
        .DOUBLE => try codedOrValues(f64, .f64, ra, chunk, cm, levels, n_leaves, decode_options, null),
        .BYTE_ARRAY => try codedOrValues([]const u8, .string, ra, chunk, cm, levels, n_leaves, decode_options, null),
        else => null,
    };
    const col = coded orelse return decodeWriteColumn(ra, rg, meta, src, ci, decode_options, prof);
    try checkRowShape(col, @intCast(rg.num_rows));
    if (chunk_span.t != null) chunk_span.end(.column_chunk, chunkArgs(0, 0, ci, cm));
    return col;
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
        .i128 => |c| try appendTyped(i128, &buf.i128, c.values, c.def_levels, c.max_def, dupe_arena),
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
    zstd: compression.ZstdPlan,
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
    bloom_write: BloomWrite,
) !RGOut {
    if (agg.num_rows == 0) return .{ .surviving_rows = 0, .rg = null };

    var rg_arena_state = std.heap.ArenaAllocator.init(gpa);
    defer rg_arena_state.deinit();
    const ra = rg_arena_state.allocator();

    const n_specs = agg.cols.len;

    // A Batch.Column per output col over the aggregator's typed buffers (pointing into agg.arena).
    const batch_cols = try ra.alloc(filter_eval.Batch.Column, n_specs);
    for (batch_cols, 0..) |*bc, i| bc.* = try aggBatchColumn(agg, i, ra);

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
        if (cbuf == .i128 and agg.schema_elems[i].type == .FIXED_LEN_BYTE_ARRAY) {
            const b = cbuf.i128;
            results[i] = try encoder.encodeDecimalFlba(ra, .{
                .values = b.values.items,
                .def_levels = if (b.max_def > 0) b.def_levels.items else null,
                .max_def = b.max_def,
            }, &agg.schema_elems[i], agg.paths[i], output_codec, zstd);
        }
    }

    var rg_columns: std.ArrayListUnmanaged(schema.ColumnChunk) = .empty;
    try rg_columns.ensureTotalCapacity(out_arena, n_specs);
    var rg_total: i64 = 0;
    const placer: ChunkPlacer = .{
        .out_arena = out_arena,
        .sink = sink,
        .out_offset = out_offset,
        .timings = timings,
        .emit_page_index = emit_page_index,
        .columns = &rg_columns,
        .total = &rg_total,
    };

    const t_enc_start = nowMonoNs();
    if (num_workers <= 1) {
        // Serial: write each chunk as soon as it is encoded and drop its working memory (hash tables, index and
        // payload buffers, the compressor's output), so a row group holds one column's encoding at a time.
        for (0..n_specs) |i| {
            var col_arena = std.heap.ArenaAllocator.init(gpa);
            defer col_arena.deinit();
            const enc = results[i] orelse
                try encodeAggColumn(agg, i, col_arena.allocator(), output_codec, zstd, bloom_write, timings);
            try placer.place(enc);
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
                .zstd = zstd,
                .bloom_write = bloom_write,
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
        const t_enc_end = nowMonoNs();
        timings.encode_ns += @intCast(t_enc_end - t_enc_start);
        profile.record(timings.prof, .encode, t_enc_start, t_enc_end, profile.args(.{ n_specs, agg.num_rows }));
        // Sequential write phase, in output order.
        for (results) |maybe_enc| try placer.place(maybe_enc orelse return error.MissingEncodeResult);
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

/// Output column `i` of `agg` as a Batch.Column over its typed buffer. DECIMAL lanes narrow the unscaled i128 back to
/// their source physical width (exact: decoded from i32/i64 and widened) for the integer encoder, the kept DECIMAL
/// schema element carrying the logical type; an FLBA decimal gets an empty placeholder (`encodeAggColumn` encodes it).
fn aggBatchColumn(agg: *const OutputAggregator, i: usize, ra: std.mem.Allocator) !filter_eval.Batch.Column {
    const cbuf = agg.cols[i];
    return switch (cbuf) {
        .i32 => |b| .{ .i32 = .{
            .values = b.values.items,
            .def_levels = if (b.max_def > 0) b.def_levels.items else null,
            .max_def = b.max_def,
            .codes = if (b.coded) b.codes.items else null,
            .dict = b.dict,
        } },
        .i64 => |b| .{ .i64 = .{
            .values = b.values.items,
            .def_levels = if (b.max_def > 0) b.def_levels.items else null,
            .max_def = b.max_def,
            .codes = if (b.coded) b.codes.items else null,
            .dict = b.dict,
        } },
        .f32 => |b| .{ .f32 = .{
            .values = b.values.items,
            .def_levels = if (b.max_def > 0) b.def_levels.items else null,
            .max_def = b.max_def,
            .codes = if (b.coded) b.codes.items else null,
            .dict = b.dict,
        } },
        .f64 => |b| .{ .f64 = .{
            .values = b.values.items,
            .def_levels = if (b.max_def > 0) b.def_levels.items else null,
            .max_def = b.max_def,
            .codes = if (b.coded) b.codes.items else null,
            .dict = b.dict,
        } },
        .string => |b| .{ .string = .{
            .values = b.values.items,
            .def_levels = if (b.max_def > 0) b.def_levels.items else null,
            .max_def = b.max_def,
            .codes = if (b.coded) b.codes.items else null,
            .dict = b.dict,
        } },
        .boolean => |b| .{ .boolean = .{
            .values = b.values.items,
            .def_levels = if (b.max_def > 0) b.def_levels.items else null,
            .max_def = b.max_def,
            .codes = if (b.coded) b.codes.items else null,
            .dict = b.dict,
        } },
        // Lossless DECIMAL: narrow the unscaled i128 back to its
        // source physical width (exact — the values were decoded
        // from i32/i64 and widened) and feed the existing integer
        // encoder. The kept DECIMAL schema element makes the footer
        // (and `meta.type`) carry the logical type through.
        .i128 => |b| blk: {
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

/// Encode output column `i` of `agg` on `arena`.
fn encodeAggColumn(
    agg: *const OutputAggregator,
    i: usize,
    arena: std.mem.Allocator,
    output_codec: schema.CompressionCodec,
    zstd: compression.ZstdPlan,
    bloom_write: BloomWrite,
    timings: *Timings,
) !encoder.EncodedColumn {
    const t0 = nowMonoNs();
    const span = profile.Span.begin(timings.prof);
    const e = if (agg.cols[i] == .i128 and agg.schema_elems[i].type == .FIXED_LEN_BYTE_ARRAY) blk: {
        const b = agg.cols[i].i128;
        break :blk try encoder.encodeDecimalFlba(arena, .{
            .values = b.values.items,
            .def_levels = if (b.max_def > 0) b.def_levels.items else null,
            .max_def = b.max_def,
        }, &agg.schema_elems[i], agg.paths[i], output_codec, zstd);
    } else try encoder.encodeColumn(arena, .{
        .values = try aggBatchColumn(agg, i, arena),
        .schema_elem = &agg.schema_elems[i],
        .path_in_schema = agg.paths[i],
        .codec = output_codec,
        .zstd = zstd,
        .bloom_fpp = bloom_write.at(i),
    });
    if (span.t != null) {
        const em = e.meta;
        span.end(.encode_chunk, profile.args(.{
            i, output_codec, em.total_uncompressed_size, em.total_compressed_size, em.num_values,
        }));
    }
    const t1 = nowMonoNs();
    timings.encode_ns += @intCast(t1 - t0);
    profile.record(timings.prof, .encode, t0, t1, profile.args(.{ 1, agg.num_rows }));
    return e;
}

/// Encodes and places a row group's output columns one at a time, in output order, so a caller can project, encode
/// and drop each column before the next (the per-row-group re-encode's working set is then one column, not a row
/// group). Offsets are relative to `out_offset`; no page index (see `encodeAggregator`'s `emit_page_index`).
pub const RGEncoder = struct {
    columns: std.ArrayListUnmanaged(schema.ColumnChunk) = .empty,
    total: i64 = 0,
    out_arena: std.mem.Allocator,
    sink: streaming.Sink,
    out_offset: *u64,
    codec: schema.CompressionCodec,
    zstd: compression.ZstdPlan = .{},
    bloom_write: BloomWrite,
    timings: *Timings,

    /// Encode `agg`'s column `i` (its rows complete) on `arena` and write it; `arena` may be released once this
    /// returns, the chunk's metadata being cloned into `out_arena`.
    pub fn column(self: *RGEncoder, agg: *const OutputAggregator, i: usize, arena: std.mem.Allocator) !void {
        if (self.columns.capacity == 0) try self.columns.ensureTotalCapacity(self.out_arena, agg.cols.len);
        const enc = try encodeAggColumn(agg, i, arena, self.codec, self.zstd, self.bloom_write, self.timings);
        const placer: ChunkPlacer = .{
            .out_arena = self.out_arena,
            .sink = self.sink,
            .out_offset = self.out_offset,
            .timings = self.timings,
            .emit_page_index = false,
            .columns = &self.columns,
            .total = &self.total,
        };
        try placer.place(enc);
    }

    pub fn finish(self: *RGEncoder, num_rows: usize) RGOut {
        if (num_rows == 0) return .{ .surviving_rows = 0, .rg = null };
        return .{
            .surviving_rows = @intCast(num_rows),
            .rg = .{ .columns = self.columns, .total_byte_size = self.total, .num_rows = @intCast(num_rows) },
        };
    }
};

/// Writes encoded chunks to the sink in output order: metadata deep-cloned into `out_arena` (which outlives the
/// encoders' arenas) with offsets made absolute, and, direct to a file, a synthesised page index after each chunk.
const ChunkPlacer = struct {
    out_arena: std.mem.Allocator,
    sink: streaming.Sink,
    out_offset: *u64,
    timings: *Timings,
    emit_page_index: bool,
    columns: *std.ArrayListUnmanaged(schema.ColumnChunk),
    total: *i64,

    fn place(self: ChunkPlacer, enc: encoder.EncodedColumn) !void {
        const col_start_in_file: i64 = @intCast(self.out_offset.*);
        var em = try cloneColumnMeta(self.out_arena, enc.meta);
        em.data_page_offset += col_start_in_file;
        if (em.dictionary_page_offset) |dpo| em.dictionary_page_offset = dpo + col_start_in_file;
        const t_sink_start = nowMonoNs();
        try self.sink.write(enc.bytes);
        const t_sink_end = nowMonoNs();
        self.timings.sink_ns += @intCast(t_sink_end - t_sink_start);
        profile.record(self.timings.prof, .sink_write, t_sink_start, t_sink_end, profile.args(.{enc.bytes.len}));
        self.out_offset.* += enc.bytes.len;
        self.total.* += @intCast(enc.bytes.len);

        // One data page per freshly encoded chunk → one OffsetIndex + ColumnIndex entry, from the stats the
        // encoder already computed, so a filtered re-encode keeps page-level pruning too.
        const idx = if (self.emit_page_index)
            try synthPageIndexToSink(self.out_arena, self.sink, self.out_offset, &enc.meta, em.data_page_offset)
        else
            fastpath.PageIndexPtrs{};

        try self.columns.append(self.out_arena, .{
            .file_path = null,
            .file_offset = col_start_in_file,
            .meta_data = em,
            .offset_index_offset = idx.offset_index_offset,
            .offset_index_length = idx.offset_index_length,
            .column_index_offset = idx.column_index_offset,
            .column_index_length = idx.column_index_length,
            // Placed after the last row group, so it must outlive this row group's arena and the encoder's.
            .bloom_out = if (enc.bloom.len > 0) try self.out_arena.dupe(u8, enc.bloom) else &.{},
        });
    }
};

const AggEncodeCtx = struct {
    arena: std.mem.Allocator,
    start: usize,
    stride: usize,
    batch_cols: []const filter_eval.Batch.Column,
    schema_elems: []const schema.SchemaElement,
    paths: []const []const []const u8,
    codec: schema.CompressionCodec,
    zstd: compression.ZstdPlan,
    bloom_write: BloomWrite,
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
            .zstd = ctx.zstd,
            .bloom_fpp = ctx.bloom_write.at(i),
        }) catch |err| {
            ctx.err = err;
            return;
        };
    }
}

/// The output leaf for a computed column. Shared by the row buffers (whose printing reads it) and the footer.
pub fn computedSchemaElem(c: OutputCol.Computed) schema.SchemaElement {
    const expr_type = c.expr.typeOf();
    if (expr_type == .dec) {
        var elem: schema.SchemaElement = .{
            .type = null,
            .type_length = null,
            .repetition_type = .REQUIRED,
            .name = c.alias,
            .num_children = 0,
            .converted_type = null,
            .logical_type = null,
            .scale = null,
            .precision = null,
            .field_id = null,
        };
        decimalElem(&elem, c.expr.decSpec().?);
        return elem;
    }
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
        .encoding_stats = if (src.encoding_stats) |es| try es.clone(arena) else null,
    };
}

/// The schema leaf an aggregate output column is written under. An exact DECIMAL is annotated DECIMAL(p, s), both
/// ways (logical and converted type), at the physical width `Dec.physical` gives its precision.
fn aggLeafElem(out_col: expr_agg.OutputCol) schema.SchemaElement {
    var elem: schema.SchemaElement = .{
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
    if (out_col.dec) |d| decimalElem(&elem, d);
    return elem;
}

/// Type `elem` as DECIMAL(`d`) at the physical width its precision takes.
pub fn decimalElem(elem: *schema.SchemaElement, d: expr_ast.Dec) void {
    elem.type = d.physical();
    elem.type_length = if (d.physical() == .FIXED_LEN_BYTE_ARRAY) 16 else null;
    elem.converted_type = .DECIMAL;
    elem.logical_type = .{ .DECIMAL = .{ .scale = d.scale, .precision = d.precision } };
    elem.scale = d.scale;
    elem.precision = d.precision;
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
    zstd: compression.ZstdPlan,
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
        const leaf_elem = aggLeafElem(out_col);
        const enc = try encoder.encodeColumn(arena, .{
            .values = out_col.col,
            .schema_elem = &leaf_elem,
            .path_in_schema = path_buf,
            .codec = codec,
            .zstd = zstd,
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
    for (output_cols) |out_col| try new_schema.append(arena, aggLeafElem(out_col));

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
        .and_filter => |c| for (c.children) |child| try collectConjunctiveLeaves(child, list, allocator),
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

/// One row group as `scanRGForAgg` reads it. Everything is borrowed.
pub const AggRowGroup = struct {
    rg: *const schema.RowGroup,
    /// Input file and row-group index, for profile spans only.
    file_index: usize = 0,
    rg_index: usize = 0,
    meta: *const schema.FileMetaData,
    src: RGSrc,
    /// The outer filter; null when there is none or statistics prove every row passes it.
    filter: ?filter_ast.Filter,
    /// Columns to decode (`agg_plan.Columns.forRowGroup`).
    read: []const bool,
    /// Columns whose values an aggregate argument, a per-aggregate FILTER or a GROUP BY key consumes
    /// (`agg_plan.Columns.consumed`); any other column in `read` only the outer filter reads.
    consumed: []const bool,
    /// Columns that must decode their stored values (`agg_plan.Columns.preserve`). Null preserves every column.
    preserve: ?[]const bool = null,
};

/// What `scanRGForAgg` folds a row group's surviving rows into. The caller owns the state, which persists across
/// row groups of one query.
pub const AggTarget = union(enum) {
    /// One accumulator per call, in call order.
    scalar: struct { calls: []const expr_agg.AggCall, accumulators: []expr_agg.Accumulator },
    /// Rows go to the group their key values select; the table holds `calls.len` accumulators per group.
    grouped: struct {
        calls: []const expr_agg.AggCall,
        keys: []const expr_ast.Expr,
        table: *expr_agg.GroupTable,
        /// `expr_agg.firstPosition` of the row group's row 0.
        pos_base: u64,
        /// Set together: fold through shared partitions instead of leaving every group in `table`.
        part: ?*partagg.PartAgg = null,
        local: ?*partagg.Local = null,
    },

    fn calls(self: AggTarget) []const expr_agg.AggCall {
        return switch (self) {
            inline else => |t| t.calls,
        };
    }

    pub fn planCalls(self: AggTarget) []const expr_agg.AggCall {
        return if (self == .grouped and self.grouped.table.countOnly()) &.{} else self.calls();
    }
};

/// A scan worker's state, reused from one row group to the next.
pub const AggWorkspace = struct {
    scratch: *DecodeScratch,
    /// Rewound at each row group that decodes: everything decoded lives here until the next one.
    rg_arena: *RowGroupArena,
    timings: *Timings,
    /// Shared by the query's scan workers; null decodes without a bound (tests, callers that budget elsewhere).
    budget: ?*DecodeBudget = null,
};

/// The memory a query's scan workers' decoded row groups may take together (`scan.runMultiAggregate` gives it half of
/// `max_memory`). A worker reserves a row group's estimate (`decodeEstimate`) before decoding it and gives it back
/// after, so a row group that does not fit beside the others waits for them. One bigger than a worker's share decodes
/// staged — the filter and keys, then one aggregate's columns at a time on memory handed back after each — and one
/// that does not fit the whole budget even so is refused before anything is allocated, rather than killed by the OS.
pub const DecodeBudget = struct {
    limit: usize,
    workers: usize = 1,
    used: std.atomic.Value(usize) = .init(0),

    /// What one worker may hold without staging: its even share.
    pub fn share(self: *const DecodeBudget) usize {
        return self.limit / @max(1, self.workers);
    }

    pub fn acquire(self: *DecodeBudget, bytes: usize) error{RowGroupTooLarge}!void {
        if (bytes > self.limit) return error.RowGroupTooLarge;
        var waits: u32 = 0;
        while (true) {
            const cur = self.used.load(.monotonic);
            if (cur + bytes <= self.limit) {
                if (self.used.cmpxchgWeak(cur, cur + bytes, .acquire, .monotonic) == null) return;
                continue;
            }
            // Others hold the budget for a whole row group's decode: milliseconds at least. Yield a few times, then
            // sleep rather than spin on a core a decoding worker needs (Lambda has two).
            waits += 1;
            if (waits < 8) std.Thread.yield() catch {} else {
                const ts: std.c.timespec = .{ .sec = 0, .nsec = 200_000 };
                _ = std.c.nanosleep(&ts, null);
            }
        }
    }

    pub fn release(self: *DecodeBudget, bytes: usize) void {
        _ = self.used.fetchSub(bytes, .release);
    }
};

/// Bytes decoding `cols` of `in` takes, from its metadata: each leaf's values in its batch lane (a string as a slice
/// plus its page bytes, which the values point into), its levels, and a computed argument's intermediate columns.
/// An estimate, kept on the high side; it decides waiting and staging, not correctness. Saturating: each footer count
/// is bounded (`metadata.max_row_count`), but a wide schema sums many of them.
pub fn decodeEstimate(in: AggRowGroup, cols: []const bool, calls: []const expr_agg.AggCall, grouped: bool) usize {
    const rows: usize = @intCast(@max(0, in.rg.num_rows));
    var total: usize = rows / 8 + 64; // selection
    for (cols, 0..) |needed, ci| {
        if (!needed or ci >= in.rg.columns.items.len) continue;
        const cm = in.rg.columns.items[ci].meta_data orelse continue;
        const n: usize = @intCast(@max(0, cm.num_values));
        const levels = in.meta.getColumnLevels(cm.path_in_schema.items);
        const se = in.meta.getColumnSchema(cm.path_in_schema.items);
        const decimal = if (se) |e| decimal_mod.kindFromSchema(&e) != null else false;
        const lane: usize = switch (cm.type) {
            .BOOLEAN => 1,
            .INT32 => if (decimal) 12 else 8, // a decimal's f64 view beside its integers; unsigned widens to 8
            .FLOAT, .DOUBLE, .INT96 => 8,
            .INT64 => if (decimal) 16 else 8,
            .BYTE_ARRAY, .FIXED_LEN_BYTE_ARRAY => 24, // slice, or a decimal's f64 view and i128
        };
        total +|= n *| lane;
        if (cm.type == .BYTE_ARRAY or cm.type == .FIXED_LEN_BYTE_ARRAY) {
            total +|= @intCast(@max(0, cm.total_uncompressed_size));
        }
        if (levels.max_def > 0) total +|= n *| 4;
        if (levels.max_rep > 0) total +|= n *| 4;
    }
    // A computed argument materializes a 16-byte column per operator; a grouped scan a group id per row.
    for (calls) |c| if (c.arg) |a| if (a != .col_ref) {
        total +|= rows *| 16 *| a.depth();
    };
    return total +| if (grouped) rows *| 4 else 0;
}

/// A window of `sel`, rows `[lo, lo + n)`, as its own selection. Bits past `n` stay clear.
fn selWindow(
    a: std.mem.Allocator,
    sel: *const filter_selection.SelectionVector,
    lo: usize,
    n: usize,
) !filter_selection.SelectionVector {
    var w = try filter_selection.SelectionVector.init(a, n);
    var copied: usize = 0;
    while (copied < n) {
        const src_bit = lo + copied;
        const src_word = sel.mask[src_bit / 64];
        const sshift: u6 = @intCast(src_bit % 64);
        const dshift: u6 = @intCast(copied % 64);
        var take = @min(n - copied, @as(usize, 64) - sshift);
        take = @min(take, @as(usize, 64) - dshift);
        const di = copied / 64;
        if (take == 64) {
            w.mask[di] = src_word;
        } else {
            const mask = (@as(u64, 1) << @as(u6, @intCast(take))) - 1;
            const bits = (src_word >> sshift) & mask;
            w.mask[di] = (w.mask[di] & ~(mask << dshift)) | (bits << dshift);
        }
        copied += take;
    }
    return w;
}

fn sliceColumn(col: filter_eval.Batch.Column, lo: usize, n: usize) filter_eval.Batch.Column {
    return switch (col) {
        inline else => |c, tag| blk: {
            var o = c;
            if (c.codes) |codes| if (codes.len >= lo + n) {
                o.codes = codes[lo..][0..n];
            };
            if (c.values.len >= lo + n) o.values = c.values[lo..][0..n];
            if (c.def_levels) |dl| if (dl.len >= lo + n) {
                o.def_levels = dl[lo..][0..n];
            };
            if (c.rep_levels) |rl| if (rl.len >= lo + n) {
                o.rep_levels = rl[lo..][0..n];
            };
            if (c.unscaled) |u| o.unscaled = switch (u) {
                .i32 => |v| if (v.len >= lo + n) .{ .i32 = v[lo..][0..n] } else u,
                .i64 => |v| if (v.len >= lo + n) .{ .i64 = v[lo..][0..n] } else u,
                .i128 => |v| if (v.len >= lo + n) .{ .i128 = v[lo..][0..n] } else u,
            };
            if (o.levels_only_rows != 0) o.levels_only_rows = n;
            break :blk @unionInit(filter_eval.Batch.Column, @tagName(tag), o);
        },
    };
}

/// Hash of the key `row` would be stored under, matching `GroupTable.routeHash` after insert.
fn routeRowHash(
    ra: std.mem.Allocator,
    scratch: *std.ArrayList(u8),
    table: *const expr_agg.GroupTable,
    key_cols: []const filter_eval.Batch.Column,
    row: usize,
) !u64 {
    if (table.fixed_stride != 0) {
        var image: [32]u8 = undefined;
        if (expr_agg.packFixedKey(key_cols, row, &image)) |nb| if (nb == table.fixed_stride) {
            return table.hashRoute(image[0..nb]);
        };
    }
    try expr_agg.serializeRowKey(scratch, ra, key_cols, row);
    return table.hashRoute(scratch.items);
}

fn insertRow(
    ra: std.mem.Allocator,
    scratch: *std.ArrayList(u8),
    table: *expr_agg.GroupTable,
    key_cols: []const filter_eval.Batch.Column,
    row: usize,
    calls: []const expr_agg.AggCall,
    pos: u64,
) !u32 {
    if (table.fixed_stride != 0) {
        var image: [32]u8 = undefined;
        if (expr_agg.packFixedKey(key_cols, row, &image)) |nb| if (nb == table.fixed_stride) {
            return table.getOrInsertFixed(image[0..nb], calls, pos + row);
        };
    }
    try expr_agg.serializeRowKey(scratch, ra, key_cols, row);
    return table.getOrInsert(scratch.items, calls, pos + row);
}

/// Abandoned pre-aggregation: one hash per row into the shared partition, under that partition's lock.
fn scatterGrouped(
    ra: std.mem.Allocator,
    gpa: std.mem.Allocator,
    g: @FieldType(AggTarget, "grouped"),
    part: *partagg.PartAgg,
    cols: []filter_eval.Batch.Column,
    lookup: []const ?usize,
    win_sel: *const filter_selection.SelectionVector,
    consumed: []const bool,
    prepared_values: *bool,
    pos: u64,
    off: usize,
    n: usize,
) !void {
    if (!prepared_values.*) {
        for (lookup, 0..) |at, ci| if (at) |p| if (ci < consumed.len and consumed[ci]) {
            cols[p] = try agg_coded.materialize(ra, cols[p]);
        };
        prepared_values.* = true;
    }
    const win_cols = try ra.alloc(filter_eval.Batch.Column, cols.len);
    for (cols, win_cols) |c, *o| o.* = sliceColumn(c, off, n);
    var win: filter_eval.Batch = .{ .cols = win_cols, .num_rows = n };
    const key_cols = try ra.alloc(filter_eval.Batch.Column, g.keys.len);
    const keyed = win.restrictedTo(win_sel);
    for (g.keys, key_cols) |k, *kc| kc.* = try expr_eval.evalGroupKeyExpr(ra, &keyed, lookup, k);

    const which = try ra.alloc(u8, n);
    var counts: [partagg.n_parts]u32 = @splat(0);
    var scratch: std.ArrayList(u8) = .empty;
    defer scratch.deinit(ra);
    for (0..n) |r| {
        if (!win_sel.isActive(r)) continue;
        const h = try routeRowHash(ra, &scratch, g.table, key_cols, r);
        const p: u8 = @intCast(part.indexOf(h));
        which[r] = p;
        counts[p] += 1;
    }
    var start: [partagg.n_parts + 1]u32 = undefined;
    start[0] = 0;
    for (0..partagg.n_parts) |p| start[p + 1] = start[p] + counts[p];
    const order = try ra.alloc(u32, start[partagg.n_parts]);
    var at = start;
    for (0..n) |r| {
        if (!win_sel.isActive(r)) continue;
        const p = which[r];
        order[at[p]] = @intCast(r);
        at[p] += 1;
    }
    const ids = try ra.alloc(u32, n);
    var preps = try expr_agg.prepareGrouped(ra, g.calls, &win, lookup, win_sel);
    defer preps.deinit();
    for (part.parts, 0..) |*pt, p| {
        const lo = start[p];
        const hi = start[p + 1];
        if (lo == hi) continue;
        partagg.PartAgg.lock(pt);
        defer partagg.PartAgg.unlock(pt);
        for (order[lo..hi]) |r| {
            ids[r] = try insertRow(ra, &scratch, &pt.table, key_cols, r, g.calls, pos);
        }
        try preps.fold(gpa, pt.table.accumulators.items, ids, order[lo..hi], g.calls);
    }
}

/// Fold `cols` through the worker's local table in windows, flushing into `g.part` when the local table fills or
/// stops reducing. One window is what `groupedCoded` or the row path already know how to fold.
fn foldGroupedWindows(
    ra: std.mem.Allocator,
    gpa: std.mem.Allocator,
    target: AggTarget,
    cols: []filter_eval.Batch.Column,
    lookup: []const ?usize,
    sel: *const filter_selection.SelectionVector,
    consumed: []const bool,
    encoded: bool,
) !void {
    const g = switch (target) {
        .grouped => |grouped| grouped,
        .scalar => return,
    };
    const part = g.part orelse return;
    const local = g.local orelse return;
    var prepared_values = false;
    var off: usize = 0;
    while (off < sel.len) {
        if (part.needsFlush(local, g.table)) try part.pump(local, g.table, 0);
        const n = part.nextSpan(local, g.table, sel.len - off);
        if (n == 0) break;
        var win_sel = try selWindow(ra, sel, off, n);
        defer win_sel.deinit();
        const pos = g.pos_base + @as(u64, @intCast(off));
        // A local table that is not reducing the rows is pure overhead: hash each key once, straight into its partition.
        if (local.abandoned) {
            try scatterGrouped(ra, gpa, g, part, cols, lookup, &win_sel, consumed, &prepared_values, pos, off, n);
            off += n;
            continue;
        }
        if (!prepared_values and encoded) {
            const win_cols = try ra.alloc(filter_eval.Batch.Column, cols.len);
            for (cols, win_cols) |c, *o| o.* = sliceColumn(c, off, n);
            const win: filter_eval.Batch = .{ .cols = win_cols, .num_rows = n };
            if (try agg_coded.groupedCoded(ra, gpa, g.calls, g.keys, g.table, pos, &win, lookup, &win_sel)) {
                try part.pump(local, g.table, @intCast(win_sel.count()));
                off += n;
                continue;
            }
        }
        if (!prepared_values) {
            for (lookup, 0..) |at, ci| if (at) |p| if (ci < consumed.len and consumed[ci]) {
                cols[p] = try agg_coded.materialize(ra, cols[p]);
            };
            prepared_values = true;
        }
        const win_cols = try ra.alloc(filter_eval.Batch.Column, cols.len);
        for (cols, win_cols) |c, *o| o.* = sliceColumn(c, off, n);
        var win: filter_eval.Batch = .{ .cols = win_cols, .num_rows = n };
        const key_cols = try ra.alloc(filter_eval.Batch.Column, g.keys.len);
        const keyed = win.restrictedTo(&win_sel);
        for (g.keys, key_cols) |k, *kc| kc.* = try expr_eval.evalGroupKeyExpr(ra, &keyed, lookup, k);
        const ids = try ra.alloc(u32, n);
        @memset(ids, 0);
        var resolver = expr_agg.GroupKeyResolver.init(key_cols);
        defer resolver.deinit(ra);
        for (0..n) |r| if (win_sel.isActive(r)) {
            ids[r] = try resolver.resolve(ra, g.table, key_cols, r, g.calls, pos);
        };
        try expr_agg.sinkSketchRows(g.table, ids, &win_sel);
        if (!g.table.countOnly()) for (g.calls, 0..) |call, i| {
            try expr_agg.updateOneGrouped(ra, gpa, g.table.accumulators.items, ids, i, g.calls.len, call, &win, lookup, &win_sel);
        };
        try part.pump(local, g.table, @intCast(win_sel.count()));
        off += n;
    }
}

/// Per-RG aggregator update. Answers what statistics can, then decodes
/// the columns the rest read, applies the outer filter, and folds the
/// surviving rows into `target` (each aggregate's own
/// `FILTER (WHERE ...)` composed in).
///
/// No sink writes, no offset advancement, no RGOut. Aggregates produce
/// nothing per-RG — only at end-of-scan, when the caller finalizes
/// `target`'s accumulators.
///
/// `gpa` owns what outlives the row group: string min/max winners and
/// group-table entries.
pub fn scanRGForAgg(
    gpa: std.mem.Allocator,
    in: AggRowGroup,
    stats: agg_plan.StatsUse,
    decode_options: DecodeOptions,
    target: AggTarget,
    ws: AggWorkspace,
) !void {
    const calls = target.planCalls();
    const num_rows: usize = @intCast(in.rg.num_rows);

    // 1. Stats short-circuit pass. Aggregates answered from the row
    //    group's metadata alone are marked handled so the decode pass
    //    below skips them; the rest fall through to decode.
    const handled_via_stats = try gpa.alloc(bool, calls.len);
    defer gpa.free(handled_via_stats);
    @memset(handled_via_stats, false);
    const any_decode_required = switch (target) {
        // A chunk statistic describes every group at once, so it answers none of them.
        .grouped => true,
        .scalar => |s| try foldFromStats(s.calls, s.accumulators, in, stats, handled_via_stats, ws.timings),
    };

    // Fast path: every agg answered from stats, no decode needed at
    // all. This is the headline win — `max(cost)`, `count(*)`, etc.
    // complete in microseconds because we never read any data pages.
    if (!any_decode_required) return;

    // 2. Decode path for the aggs that couldn't be stat-handled.
    // Rewound, not rebuilt: a fresh arena per row group hands its pages back to the OS and re-faults them every time.
    // The price is that each worker's arena holds its high-water mark (one row group) for the whole scan, which the
    // decode budget bounds: a row group past a worker's share decodes staged, on memory handed back when it is done.
    ws.rg_arena.nextRowGroup();
    const prof = ws.timings.prof;

    var staged = false;
    var reserved: usize = 0;
    var plan_arena = std.heap.ArenaAllocator.init(gpa);
    defer plan_arena.deinit();
    const stage_plan: ?StagePlan = if (ws.budget) |b| plan: {
        const full = decodeEstimate(in, in.read, calls, target == .grouped);
        if (target == .grouped and target.grouped.table.countOnly()) {
            const keys_only = decodeEstimate(in, in.read, &.{}, true);
            std.debug.assert(full == keys_only);
        }
        reserved = full;
        // Too big for this worker's share: a slice of rows at a time when every column has a sliced form, else one
        // aggregate's columns at a time.
        if (full > b.share() and try scanRGSliced(gpa, in, decode_options, target, ws, b, handled_via_stats, full)) {
            return;
        }
        if (full > b.share()) {
            const sp = try StagePlan.init(plan_arena.allocator(), in, target, handled_via_stats);
            if (sp.estimate < full) {
                staged = true;
                reserved = sp.estimate;
                break :plan sp;
            }
        }
        break :plan null;
    } else null;
    if (ws.budget) |b| try b.acquire(reserved);
    defer if (ws.budget) |b| b.release(reserved);
    // Staged, everything decoded lives on memory of its own, given back with the row group; otherwise on the
    // worker's arena.
    var staged_arena = std.heap.ArenaAllocator.init(gpa);
    defer staged_arena.deinit();
    const ra = if (staged) staged_arena.allocator() else ws.rg_arena.allocator();

    const prune_span = profile.Span.begin(prof);
    const page_indexes = try loadPageIndexes(ra, in, stats);
    var sel = try filter_selection.SelectionVector.init(ra, num_rows);
    const filter_pages = try pruneFilterPages(ra, in, page_indexes, stats, &sel);

    const t_decode_start = nowMonoNs();
    var dec: ChunkDecoder = .{
        .in = in,
        .uses = try planCodeUses(ra, in, target),
        .page_indexes = page_indexes,
        .filter_pages = filter_pages,
        .num_rows = num_rows,
        .decode_options = decode_options,
        .ws = ws,
        .target = target,
    };
    var batch_cols: std.ArrayList(filter_eval.Batch.Column) = .empty;
    var lookup = try ra.alloc(?usize, in.meta.schema.items.len);
    @memset(lookup, null);
    // Pages are pruned by the rows the filter's page statistics leave, as when every column decodes up front.
    const sel_pre = if (staged) try sel.cloneAlloc(ra) else sel;
    for (in.read, 0..) |needed, ci| {
        if (!needed or ci >= in.rg.columns.items.len) continue;
        if (stage_plan) |sp| if (!sp.base[ci]) continue;
        lookup[ci] = batch_cols.items.len;
        try batch_cols.append(ra, try dec.column(ra, ci, &sel_pre));
    }
    const t_decode_end = nowMonoNs();
    ws.timings.decode_ns += @intCast(t_decode_end - t_decode_start);
    if (prof) |t| {
        t.record(.prune_pages, prune_span.start, t_decode_start, profile.args(.{ dec.pages_total, dec.pages_skipped }));
        const decode_args = profile.args(.{ in.file_index, in.rg_index, batch_cols.items.len });
        t.record(.decode, t_decode_start, t_decode_end, decode_args);
    }

    if (target == .grouped and !staged and decode_options.encoded)
        try agg_coded.codePlainKeys(ra, target.grouped.keys, batch_cols.items, lookup);
    const batch: filter_eval.Batch = .{ .cols = batch_cols.items, .num_rows = num_rows };
    if (in.filter) |f| try filter_eval.evaluate(f, &batch, &sel, lookup, ra);
    const t_eval_end = nowMonoNs();
    ws.timings.eval_ns += @intCast(t_eval_end - t_decode_end);
    const selected: usize = if (prof != null) sel.count() else 0;
    profile.record(prof, .filter, t_decode_end, t_eval_end, profile.args(.{ num_rows, selected }));

    switch (target) {
        .grouped => |g| {
            if (g.part != null and !staged) {
                try foldGroupedWindows(ra, gpa, target, batch_cols.items, lookup, &sel, in.consumed, decode_options.encoded);
                const t_agg_end = nowMonoNs();
                ws.timings.encode_ns += @intCast(t_agg_end - t_eval_end);
                profile.record(prof, .aggregate, t_eval_end, t_agg_end, profile.args(.{ selected, calls.len }));
                return;
            }
            if (!staged and decode_options.encoded and
                try agg_coded.groupedCoded(ra, gpa, g.calls, g.keys, g.table, g.pos_base, &batch, lookup, &sel))
            {
                const t_agg_end = nowMonoNs();
                ws.timings.encode_ns += @intCast(t_agg_end - t_eval_end);
                profile.record(prof, .aggregate, t_eval_end, t_agg_end, profile.args(.{ selected, calls.len }));
                return;
            }
            // The row path reads values: gather any column decoded as codes only, but not one only the outer filter
            // read, which it has evaluated already.
            for (lookup, 0..) |at, ci| if (at) |pos| if (ci < in.consumed.len and in.consumed[ci]) {
                batch_cols.items[pos] = try agg_coded.materialize(ra, batch_cols.items[pos]);
            };
            var key_cols = try ra.alloc(filter_eval.Batch.Column, g.keys.len);
            for (g.keys, 0..) |key_expr, idx| {
                key_cols[idx] = try expr_eval.evalGroupKeyExpr(ra, &batch.restrictedTo(&sel), lookup, key_expr);
            }

            var group_id_arr = try ra.alloc(u32, num_rows);
            @memset(group_id_arr, 0);

            // Never cached across row groups: the resolver's memo holds pointers into `ra`.
            var key_resolver = expr_agg.GroupKeyResolver.init(key_cols);
            defer key_resolver.deinit(ra);

            var r: usize = 0;
            while (r < num_rows) : (r += 1) {
                if (sel.isActive(r)) {
                    group_id_arr[r] = try key_resolver.resolve(ra, g.table, key_cols, r, g.calls, g.pos_base);
                }
            }
            try expr_agg.sinkSketchRows(g.table, group_id_arr, &sel);

            // A sketch's count pass has read no aggregate's columns, and has no group to fold into.
            if (!g.table.countOnly()) for (g.calls, 0..) |call, i| {
                var call_arena = std.heap.ArenaAllocator.init(gpa);
                defer call_arena.deinit();
                const ca = if (staged) call_arena.allocator() else ra;
                const cb = if (stage_plan) |sp|
                    try dec.withCallColumns(ca, sp, i, &batch, lookup, &sel_pre, true)
                else
                    CallBatch{ .batch = batch, .lookup = lookup };
                try expr_agg.updateOneGrouped(
                    ca,
                    gpa,
                    g.table.accumulators.items,
                    group_id_arr,
                    i,
                    g.calls.len,
                    call,
                    &cb.batch,
                    cb.lookup,
                    &sel,
                );
            };
            // The windowed path returned above. A staged row group folded whole still has to enter the partitions
            // before the next row group, or one worker's local table keeps every key it has seen.
            if (g.part) |p| if (g.local) |local| try p.pump(local, g.table, @intCast(sel.count()));
        },
        .scalar => |s| {
            // Update only the aggs that weren't handled via stats. The agg's
            // own per-WHERE predicate (if any) is composed inside updateOne.
            for (s.calls, s.accumulators, handled_via_stats, 0..) |call, *acc, handled, i| {
                if (handled) continue;
                var call_arena = std.heap.ArenaAllocator.init(gpa);
                defer call_arena.deinit();
                const ca = if (staged) call_arena.allocator() else ra;
                const cb = if (stage_plan) |sp|
                    try dec.withCallColumns(ca, sp, i, &batch, lookup, &sel_pre, false)
                else
                    CallBatch{ .batch = batch, .lookup = lookup };
                // `ca` is per-RG (or per-call) scratch; `gpa` is the persist allocator for owned results (string
                // min/max winner) that outlive both and the cross-worker merge.
                try expr_agg.updateOne(ca, gpa, acc, call, &cb.batch, cb.lookup, &sel);
            }
        },
    }
    const t_agg_end = nowMonoNs();
    ws.timings.encode_ns += @intCast(t_agg_end - t_eval_end);
    profile.record(prof, .aggregate, t_eval_end, t_agg_end, profile.args(.{ selected, calls.len }));
}

/// `scanRGForAgg`'s decode and fold, a slice of rows at a time (`SliceReader`), each slice on memory of its own:
/// for a row group too big for a worker's share of `budget`. False, having done nothing, when some column has no
/// sliced form.
fn scanRGSliced(
    gpa: std.mem.Allocator,
    in: AggRowGroup,
    decode_options: DecodeOptions,
    target: AggTarget,
    ws: AggWorkspace,
    budget: *DecodeBudget,
    handled: []const bool,
    full: usize,
) !bool {
    const num_rows: usize = @intCast(@max(0, in.rg.num_rows));
    if (num_rows == 0) return false;
    var pages = std.heap.ArenaAllocator.init(gpa);
    defer pages.deinit();
    var readers: std.ArrayList(SliceReader) = .empty;
    defer for (readers.items) |*r| r.deinit();
    const lookup = try pages.allocator().alloc(?usize, in.meta.schema.items.len);
    @memset(lookup, null);
    for (in.read, 0..) |needed, ci| {
        if (!needed or ci >= in.rg.columns.items.len) continue;
        const r = try SliceReader.init(pages.allocator(), gpa, in.rg, in.meta, in.src, ci, decode_options) orelse
            return false;
        lookup[ci] = readers.items.len;
        try readers.append(pages.allocator(), r);
    }
    const per_row = @max(1, full / num_rows);
    const slice_rows = std.math.clamp(budget.share() / per_row, 1, num_rows);
    const reserved = @min(budget.limit, slice_rows * per_row);
    try budget.acquire(reserved);
    defer budget.release(reserved);

    const prof = ws.timings.prof;
    const t0 = nowMonoNs();
    var done: usize = 0;
    while (done < num_rows) {
        const n = @min(slice_rows, num_rows - done);
        defer done += n;
        var slice = std.heap.ArenaAllocator.init(gpa);
        defer slice.deinit();
        const sa = slice.allocator();
        const cols = try sa.alloc(filter_eval.Batch.Column, readers.items.len);
        for (readers.items, cols) |*r, *c| c.* = try r.next(sa, n);
        const batch: filter_eval.Batch = .{ .cols = cols, .num_rows = n };
        var sel = try filter_selection.SelectionVector.init(sa, n);
        if (in.filter) |f| try filter_eval.evaluate(f, &batch, &sel, lookup, sa);
        switch (target) {
            .scalar => |s| for (s.calls, s.accumulators, handled) |call, *acc, h| {
                if (h) continue;
                try expr_agg.updateOne(sa, gpa, acc, call, &batch, lookup, &sel);
            },
            .grouped => |g| {
                if (g.part != null) {
                    var shifted = target;
                    shifted.grouped.pos_base = g.pos_base + @as(u64, @intCast(done));
                    try foldGroupedWindows(sa, gpa, shifted, cols, lookup, &sel, in.consumed, decode_options.encoded);
                    continue;
                }
                const key_cols = try sa.alloc(filter_eval.Batch.Column, g.keys.len);
                const keyed = batch.restrictedTo(&sel);
                for (g.keys, key_cols) |k, *kc| kc.* = try expr_eval.evalGroupKeyExpr(sa, &keyed, lookup, k);
                const ids = try sa.alloc(u32, n);
                @memset(ids, 0);
                var resolver = expr_agg.GroupKeyResolver.init(key_cols);
                defer resolver.deinit(sa);
                // First appearance counts rows from the row group's start, as when it decodes whole.
                for (0..n) |r| if (sel.isActive(r)) {
                    ids[r] = try resolver.resolve(sa, g.table, key_cols, r, g.calls, g.pos_base + done);
                };
                try expr_agg.sinkSketchRows(g.table, ids, &sel);
                if (!g.table.countOnly()) for (g.calls, 0..) |call, i| {
                    const accs = g.table.accumulators.items;
                    try expr_agg.updateOneGrouped(sa, gpa, accs, ids, i, g.calls.len, call, &batch, lookup, &sel);
                };
            },
        }
    }
    const t1 = nowMonoNs();
    ws.timings.decode_ns += @intCast(t1 - t0);
    profile.record(prof, .decode, t0, t1, profile.args(.{ in.file_index, in.rg_index, readers.items.len }));
    return true;
}

/// A staged row group's decode: the columns the outer filter and GROUP BY keys read (`base`), decoded first; then
/// each aggregate's own (`calls[i]`, less `base`), decoded for it alone and dropped after.
const StagePlan = struct {
    base: []bool,
    calls: [][]bool,
    /// The most the plan holds at once: `base` and the largest call's columns.
    estimate: usize,

    fn init(a: std.mem.Allocator, in: AggRowGroup, target: AggTarget, handled: []const bool) !StagePlan {
        const n = in.read.len;
        const base = try a.alloc(bool, n);
        @memset(base, false);
        if (in.filter) |f| {
            var cols: std.ArrayList(usize) = .empty;
            try f.collectColumns(&cols, a);
            for (cols.items) |ci| if (ci < n) {
                base[ci] = true;
            };
        }
        switch (target) {
            .grouped => |g| for (g.keys) |k| k.collectColumns(base),
            .scalar => {},
        }
        for (base, in.read) |*b, r| b.* = b.* and r;
        const calls = target.planCalls();
        if (target == .grouped and target.grouped.table.countOnly()) {
            std.debug.assert(calls.len == 0);
        }
        const per_call = try a.alloc([]bool, calls.len);
        var widest: usize = 0;
        for (calls, per_call, 0..) |call, *cols, i| {
            cols.* = try a.alloc(bool, n);
            @memset(cols.*, false);
            if (i < handled.len and handled[i]) continue;
            if (call.arg) |arg| arg.collectColumns(cols.*);
            if (call.where) |w| {
                var wc: std.ArrayList(usize) = .empty;
                try w.collectColumns(&wc, a);
                for (wc.items) |ci| if (ci < n) {
                    cols.*[ci] = true;
                };
            }
            for (cols.*, in.read, base) |*c, r, b| c.* = c.* and r and !b;
            widest = @max(widest, decodeEstimate(in, cols.*, calls[i .. i + 1], false));
        }
        const base_bytes = decodeEstimate(in, base, &.{}, target == .grouped);
        return .{ .base = base, .calls = per_call, .estimate = base_bytes + widest };
    }
};

const CallBatch = struct { batch: filter_eval.Batch, lookup: []?usize };

/// Decodes one row group's column chunks into batch lanes, the way every column of `in.read` is decoded.
const ChunkDecoder = struct {
    in: AggRowGroup,
    uses: CodeUses,
    page_indexes: []const ?PageIndex,
    filter_pages: FilterPages,
    num_rows: usize,
    decode_options: DecodeOptions,
    ws: AggWorkspace,
    target: AggTarget,
    pages_total: usize = 0,
    pages_skipped: usize = 0,

    fn column(
        self: *ChunkDecoder,
        a: std.mem.Allocator,
        ci: usize,
        sel: *const filter_selection.SelectionVector,
    ) !filter_eval.Batch.Column {
        const in = self.in;
        const prof = self.ws.timings.prof;
        const cm = in.rg.columns.items[ci].meta_data orelse return error.ColumnMetaMissing;
        const start: usize = if (cm.dictionary_page_offset) |dp| @intCast(dp) else @intCast(cm.data_page_offset);
        const len: usize = @intCast(cm.total_compressed_size);
        invariant.assert(start >= @as(usize, @intCast(in.src.byte_origin)));
        const buf_off = start - @as(usize, @intCast(in.src.byte_origin));
        if (buf_off + len > in.src.bytes.len) return error.MissingChunkBytes;
        const chunk = in.src.bytes[buf_off .. buf_off + len];

        const chunk_span = profile.Span.begin(prof);
        const prune_info: ?PruningInfo = if (self.page_indexes[ci]) |pi|
            try pagePruning(a, pi, self.filter_pages, ci, if (in.preserve) |p| p[ci] else true, sel, self.num_rows, cm, start)
        else
            null;
        // Encoded execution, on chunks read whole (page pruning decodes values page by page): a column only counted
        // reads its levels; one only bare aggregates read, over every row, folds straight into a histogram of its
        // dictionary codes; one filters, keys or aggregates read decodes to codes they evaluate once per entry.
        const uses = self.uses;
        const decode_options = self.decode_options;
        const scratch = self.ws.scratch;
        const encoded = decode_options.encoded and prune_info == null and ci < uses.code.len;
        const levels_only = encoded and uses.levels[ci] and !uses.code[ci] and !uses.value[ci] and
            self.target == .scalar;
        const hist_col: ?filter_eval.Batch.Column = if (encoded and !levels_only and uses.code[ci] and
            uses.hist_ok[ci] and !uses.value[ci])
            try decodeAggColumnHist(a, chunk, cm, in.meta, decode_options, scratch, self.num_rows)
        else
            null;
        var decoded = if (hist_col) |hc|
            hc
        else if (levels_only)
            try decodeLevelsOnly(a, chunk, cm, in.meta, decode_options, scratch, self.num_rows)
        else if (encoded and (uses.code[ci] or uses.levels[ci]))
            (try decodeAggColumnCoded(a, chunk, cm, in.meta, decode_options, scratch)) orelse
                try decodeAggColumn(a, chunk, cm, in.meta, prune_info, decode_options, scratch)
        else
            try decodeAggColumn(a, chunk, cm, in.meta, prune_info, decode_options, scratch);
        if (self.target == .grouped and self.target.grouped.table.countOnly() and !in.consumed[ci] and decoded.codes() != null) {
            std.debug.assert(!uses.value[ci]);
        }
        if (ci < uses.value.len and uses.value[ci]) decoded = try agg_coded.materialize(a, decoded);
        try checkRowShape(decoded, self.num_rows);
        if (chunk_span.t != null) {
            chunk_span.end(.column_chunk, chunkArgs(in.file_index, in.rg_index, ci, cm));
            if (prune_info) |pi| {
                self.pages_total += pi.page_is_skipped.len;
                self.pages_skipped += std.mem.count(bool, pi.page_is_skipped, &.{true});
            }
        }
        return decoded;
    }

    /// `batch` with call `i`'s own columns (`plan.calls[i]`) decoded beside it, on `a`.
    fn withCallColumns(
        self: *ChunkDecoder,
        a: std.mem.Allocator,
        plan: StagePlan,
        i: usize,
        batch: *const filter_eval.Batch,
        lookup: []const ?usize,
        sel: *const filter_selection.SelectionVector,
        values: bool,
    ) !CallBatch {
        var cols: std.ArrayList(filter_eval.Batch.Column) = .empty;
        try cols.appendSlice(a, batch.cols);
        const lk = try a.dupe(?usize, lookup);
        for (plan.calls[i], 0..) |needed, ci| {
            if (!needed) continue;
            var col = try self.column(a, ci, sel);
            // The grouped row fold reads values, not codes.
            if (values) col = try agg_coded.materialize(a, col);
            lk[ci] = cols.items.len;
            try cols.append(a, col);
        }
        return .{ .batch = .{ .cols = cols.items, .num_rows = batch.num_rows }, .lookup = lk };
    }
};

/// `column_chunk` span arguments for chunk `ci` of a row group.
fn chunkArgs(file: usize, rg: usize, ci: usize, cm: schema.ColumnMetaData) profile.Args {
    return profile.args(.{
        file,
        rg,
        ci,
        profile.chunkTypes(cm.codec, profile.chunkEncoding(cm.encodings.items), cm.type),
        cm.total_compressed_size,
        cm.total_uncompressed_size,
    });
}

/// Which columns a coded decode serves (`code`) and which still need materialized values (`value`).
const CodeUses = struct { code: []bool, value: []bool, levels: []bool, hist_ok: []bool };

fn planCodeUses(ra: std.mem.Allocator, in: AggRowGroup, target: AggTarget) !CodeUses {
    const n = in.read.len;
    const u: CodeUses = .{
        .code = try ra.alloc(bool, n),
        .value = try ra.alloc(bool, n),
        .levels = try ra.alloc(bool, n),
        .hist_ok = try ra.alloc(bool, n),
    };
    @memset(u.code, false);
    @memset(u.value, false);
    @memset(u.levels, false);
    // A column folds straight into a histogram when every row is selected and only unfiltered scalar aggregates
    // read it as a bare argument.
    @memset(u.hist_ok, in.filter == null and target == .scalar);
    if (target == .scalar) for (target.calls()) |call| {
        if (call.where) |w| markFilterSet(u.hist_ok, w, false);
        if (call.arg) |arg| {
            if (call.where != null) markExprSet(u.hist_ok, arg, false);
            if (arg != .col_ref) markExprSet(u.hist_ok, arg, false);
        }
    };
    if (in.filter) |f| markFilter(u, f);
    for (target.planCalls()) |call| {
        if (call.where) |w| markFilter(u, w);
        if (call.arg) |arg| switch (arg) {
            .col_ref => |c| mark(if (call.func == .count and target == .scalar) u.levels else u.code, c.col_idx),
            // A scalar fold evaluates a computed argument itself, gathering coded columns (`agg_coded.gatherSelected`).
            else => if (target == .scalar) arg.collectColumns(u.code) else markExprValues(u, arg),
        };
    }
    switch (target) {
        .grouped => |g| for (g.keys) |k| switch (k) {
            .col_ref => |c| mark(u.code, c.col_idx),
            else => markExprValues(u, k),
        },
        .scalar => {},
    }
    return u;
}

fn markFilterSet(set: []bool, f: filter_ast.Filter, v: bool) void {
    switch (f) {
        .and_filter, .or_filter => |c| for (c.children) |child| markFilterSet(set, child, v),
        inline else => |l| if (l.col_idx < set.len) {
            set[l.col_idx] = v;
        },
    }
}

fn markExprSet(set: []bool, e: expr_ast.Expr, v: bool) void {
    switch (e) {
        .literal => {},
        .col_ref => |c| if (c.col_idx < set.len) {
            set[c.col_idx] = v;
        },
        .binop => |b| {
            markExprSet(set, b.left.*, v);
            markExprSet(set, b.right.*, v);
        },
        .call => |c| for (c.args) |a| markExprSet(set, a.*, v),
    }
}

fn mark(set: []bool, ci: usize) void {
    if (ci < set.len) set[ci] = true;
}

fn markFilter(u: CodeUses, f: filter_ast.Filter) void {
    switch (f) {
        .and_filter, .or_filter => |c| for (c.children) |child| markFilter(u, child),
        .int32 => |l| mark(u.code, l.col_idx),
        .int64 => |l| mark(u.code, l.col_idx),
        .uint64 => |l| mark(u.code, l.col_idx),
        .float => |l| mark(u.code, l.col_idx),
        .double => |l| mark(u.code, l.col_idx),
        .string => |l| mark(u.code, l.col_idx),
        .boolean => |l| mark(u.value, l.col_idx),
        .null_check => |l| mark(u.levels, l.col_idx),
        .like => |l| mark(u.code, l.col_idx),
    }
}

fn markExprValues(u: CodeUses, e: expr_ast.Expr) void {
    switch (e) {
        .literal => {},
        .col_ref => |c| mark(u.value, c.col_idx),
        .binop => |b| {
            markExprValues(u, b.left.*);
            markExprValues(u, b.right.*);
        },
        .call => |c| for (c.args) |a| markExprValues(u, a.*),
    }
}

/// `decodeAggColumn` as dictionary codes, or null when the chunk's type or encoding has no coded form here.
fn decodeAggColumnCoded(
    ra: std.mem.Allocator,
    chunk: []const u8,
    cm: schema.ColumnMetaData,
    meta: *const schema.FileMetaData,
    decode_options: DecodeOptions,
    scratch: *DecodeScratch,
) !?filter_eval.Batch.Column {
    const levels = meta.getColumnLevels(cm.path_in_schema.items);
    if (levels.max_rep > 0) return null;
    const n_leaves: usize = @intCast(cm.num_values);
    const schema_elem = meta.getColumnSchema(cm.path_in_schema.items);
    const dec_kind: ?decimal_mod.Kind = if (schema_elem) |se| decimal_mod.kindFromSchema(&se) else null;
    if (dec_kind) |k| {
        return switch (k.physical) {
            inline .INT32, .INT64 => |p| blk: {
                const T = if (p == .INT32) i32 else i64;
                const r = (try decodeCodes(T, ra, chunk, cm.codec, levels, n_leaves, decode_options, scratch)) orelse
                    break :blk null;
                switch (r) {
                    .codes => |c| {
                        const d = try ra.alloc(f64, c.dict.len);
                        decimal_mod.applyScaleSimd(T, c.dict, k.scale, d);
                        var col = codedCol(f64, c, d, levels);
                        col.dict_unscaled = @unionInit(filter_eval.Exact, @typeName(T), c.dict);
                        break :blk .{ .f64 = col };
                    },
                    .values => |v| {
                        const d = try ra.alloc(f64, v.values.len);
                        decimal_mod.applyScaleSimd(T, v.values, k.scale, d);
                        var col = scaledCol(f64, d, v, levels);
                        col.unscaled = @unionInit(filter_eval.Exact, @typeName(T), v.values);
                        break :blk .{ .f64 = col };
                    },
                }
            },
            else => null,
        };
    }
    if (schema_elem != null and schema.isFloat16(schema_elem.?)) return null;
    return switch (cm.type) {
        .INT32 => blk: {
            if (schema_elem != null and schema.isUnsignedIntTo32(schema_elem.?)) {
                const r = (try decodeCodes(i32, ra, chunk, cm.codec, levels, n_leaves, decode_options, scratch)) orelse
                    break :blk null;
                switch (r) {
                    .codes => |c| {
                        const d = try ra.alloc(i64, c.dict.len);
                        for (d, c.dict) |*o, v| o.* = @as(u32, @bitCast(v));
                        break :blk .{ .i64 = codedCol(i64, c, d, levels) };
                    },
                    // Rare (unsigned INT32 whose dictionary overflowed): decode again the ordinary way.
                    .values => break :blk null,
                }
            }
            break :blk try codedOrValues(i32, .i32, ra, chunk, cm, levels, n_leaves, decode_options, scratch);
        },
        .INT64 => try codedOrValues(i64, .i64, ra, chunk, cm, levels, n_leaves, decode_options, scratch),
        .FLOAT => try codedOrValues(f32, .f32, ra, chunk, cm, levels, n_leaves, decode_options, scratch),
        .DOUBLE => try codedOrValues(f64, .f64, ra, chunk, cm, levels, n_leaves, decode_options, scratch),
        .BYTE_ARRAY => try codedOrValues([]const u8, .string, ra, chunk, cm, levels, n_leaves, decode_options, scratch),
        else => null,
    };
}

fn codedOrValues(
    comptime T: type,
    comptime tag: std.meta.Tag(filter_eval.Batch.Column),
    ra: std.mem.Allocator,
    chunk: []const u8,
    cm: schema.ColumnMetaData,
    levels: schema.Levels,
    n_leaves: usize,
    decode_options: DecodeOptions,
    scratch: ?*DecodeScratch,
) !?filter_eval.Batch.Column {
    const r = (try decodeCodes(T, ra, chunk, cm.codec, levels, n_leaves, decode_options, scratch)) orelse return null;
    return switch (r) {
        .codes => |c| @unionInit(filter_eval.Batch.Column, @tagName(tag), codedCol(T, c, c.dict, levels)),
        .values => |v| @unionInit(filter_eval.Batch.Column, @tagName(tag), valuesCol(T, v, levels)),
    };
}

/// `decodeAggColumnCoded` folded straight into a code histogram, or null when the chunk doesn't qualify (the caller
/// then decodes it afresh). Decimal and unsigned-INT32 dictionaries convert once per entry.
fn decodeAggColumnHist(
    ra: std.mem.Allocator,
    chunk: []const u8,
    cm: schema.ColumnMetaData,
    meta: *const schema.FileMetaData,
    decode_options: DecodeOptions,
    scratch: *DecodeScratch,
    num_rows: usize,
) !?filter_eval.Batch.Column {
    const levels = meta.getColumnLevels(cm.path_in_schema.items);
    if (levels.max_rep > 0) return null;
    const n_leaves: usize = @intCast(cm.num_values);
    const schema_elem = meta.getColumnSchema(cm.path_in_schema.items);
    const dec_kind: ?decimal_mod.Kind = if (schema_elem) |se| decimal_mod.kindFromSchema(&se) else null;
    if (dec_kind) |k| {
        return switch (k.physical) {
            inline .INT32, .INT64 => |p| blk: {
                const T = if (p == .INT32) i32 else i64;
                const r = (try histOf(T, ra, chunk, cm.codec, levels, n_leaves, decode_options, scratch)) orelse
                    break :blk null;
                switch (r) {
                    .hist => |h| {
                        const d = try ra.alloc(f64, h.dict.len);
                        decimal_mod.applyScaleSimd(T, h.dict, k.scale, d);
                        var col = histCol(f64, h, d, num_rows);
                        col.dict_unscaled = @unionInit(filter_eval.Exact, @typeName(T), h.dict);
                        break :blk .{ .f64 = col };
                    },
                    .values => |v| {
                        const d = try ra.alloc(f64, v.values.len);
                        decimal_mod.applyScaleSimd(T, v.values, k.scale, d);
                        var col = scaledCol(f64, d, v, levels);
                        col.unscaled = @unionInit(filter_eval.Exact, @typeName(T), v.values);
                        break :blk .{ .f64 = col };
                    },
                }
            },
            else => null,
        };
    }
    if (schema_elem != null and schema.isFloat16(schema_elem.?)) return null;
    return switch (cm.type) {
        .INT32 => blk: {
            const r = (try histOf(i32, ra, chunk, cm.codec, levels, n_leaves, decode_options, scratch)) orelse
                break :blk null;
            const unsigned = schema_elem != null and schema.isUnsignedIntTo32(schema_elem.?);
            switch (r) {
                .hist => |h| {
                    if (unsigned) {
                        const d = try ra.alloc(i64, h.dict.len);
                        for (d, h.dict) |*o, v| o.* = @as(u32, @bitCast(v));
                        break :blk .{ .i64 = histCol(i64, h, d, num_rows) };
                    }
                    break :blk .{ .i32 = histCol(i32, h, h.dict, num_rows) };
                },
                .values => |v| {
                    if (unsigned) {
                        const d = try ra.alloc(i64, v.values.len);
                        for (d, v.values) |*o, x| o.* = @as(u32, @bitCast(x));
                        break :blk .{ .i64 = scaledCol(i64, d, v, levels) };
                    }
                    break :blk .{ .i32 = valuesCol(i32, v, levels) };
                },
            }
        },
        inline .INT64, .FLOAT, .DOUBLE, .BYTE_ARRAY => |t| blk: {
            const T = switch (t) {
                .INT64 => i64,
                .FLOAT => f32,
                .DOUBLE => f64,
                else => []const u8,
            };
            const r = (try histOf(T, ra, chunk, cm.codec, levels, n_leaves, decode_options, scratch)) orelse
                break :blk null;
            const tag = switch (t) {
                .INT64 => "i64",
                .FLOAT => "f32",
                .DOUBLE => "f64",
                else => "string",
            };
            break :blk switch (r) {
                .hist => |h| @unionInit(filter_eval.Batch.Column, tag, histCol(T, h, h.dict, num_rows)),
                .values => |v| @unionInit(filter_eval.Batch.Column, tag, valuesCol(T, v, levels)),
            };
        },
        else => null,
    };
}

fn histCol(comptime T: type, h: anytype, dict: []const T, num_rows: usize) filter_eval.ColumnT(T) {
    return .{
        .values = &.{},
        .dict = dict,
        .hist = h.hist,
        .levels_only_rows = num_rows,
        .has_nulls = h.num_present != num_rows,
    };
}

fn histOf(
    comptime T: type,
    ra: std.mem.Allocator,
    chunk: []const u8,
    codec: schema.CompressionCodec,
    levels: schema.Levels,
    n_leaves: usize,
    decode_options: DecodeOptions,
    scratch: *DecodeScratch,
) !?column_mod.ColumnChunkReader(T).HistOrValues {
    var reader = column_mod.ColumnChunkReader(T).initWithOptions(chunk, codec, levels, ra, decode_options, scratch);
    return reader.readAllHistogram(n_leaves);
}

/// A column of definition levels only, typed as `decodeAggColumn` would type it so lookups and type checks agree.
fn decodeLevelsOnly(
    ra: std.mem.Allocator,
    chunk: []const u8,
    cm: schema.ColumnMetaData,
    meta: *const schema.FileMetaData,
    decode_options: DecodeOptions,
    scratch: *DecodeScratch,
    num_rows: usize,
) !filter_eval.Batch.Column {
    _ = num_rows;
    const levels = meta.getColumnLevels(cm.path_in_schema.items);
    const n_leaves: usize = @intCast(cm.num_values);
    var dl: ?[]u32 = null;
    if (levels.max_def > 0) {
        // The reader of the column's own physical type, though only levels are read: pages still install their value
        // decoders, and a dictionary its entries, which another type's reader would mis-size or refuse (a FLOAT16
        // dictionary's 2-byte entries, a DELTA_BYTE_ARRAY page).
        const r = switch (cm.type) {
            inline else => |t| blk: {
                const T = switch (t) {
                    .BOOLEAN => bool,
                    .INT32 => i32,
                    .INT64 => i64,
                    .FLOAT => f32,
                    .DOUBLE => f64,
                    .INT96, .BYTE_ARRAY, .FIXED_LEN_BYTE_ARRAY => []const u8,
                };
                var reader = column_mod.ColumnChunkReader(T).initWithOptions(
                    chunk,
                    cm.codec,
                    levels,
                    ra,
                    decode_options,
                    scratch,
                );
                reader.type_length = switch (t) {
                    .FIXED_LEN_BYTE_ARRAY => flbaWidth(meta.getColumnSchema(cm.path_in_schema.items)),
                    .INT96 => int96_mod.INT96_WIDTH,
                    else => 0,
                };
                break :blk try reader.readAllLevels(n_leaves);
            },
        } orelse return decodeAggColumn(ra, chunk, cm, meta, null, decode_options, scratch);
        dl = r.def_levels;
    }
    var has_nulls = false;
    if (dl) |d| for (d) |x| {
        if (x < levels.max_def) {
            has_nulls = true;
            break;
        }
    };
    // The lane tag is irrelevant to count(col) and IS [NOT] NULL; i64 keeps every reader of nullInfo happy.
    return .{ .i64 = .{
        .values = &.{},
        .def_levels = dl,
        .max_def = @intCast(levels.max_def),
        .has_nulls = has_nulls,
        .levels_only_rows = if (dl) |d| d.len else n_leaves,
    } };
}

/// A values column built from converted values `d` (decimal scaling, unsigned widening) with `v`'s levels.
fn scaledCol(comptime T: type, d: []const T, v: anytype, levels: schema.Levels) filter_eval.ColumnT(T) {
    return .{ .values = d, .def_levels = v.def_levels, .max_def = @intCast(levels.max_def), .has_nulls = v.has_nulls };
}

fn codedCol(comptime T: type, c: anytype, dict: []const T, levels: schema.Levels) filter_eval.ColumnT(T) {
    return .{
        .values = &.{},
        .codes = c.codes,
        .dict = dict,
        .def_levels = c.def_levels,
        .max_def = @intCast(levels.max_def),
        .has_nulls = c.has_nulls,
    };
}

fn valuesCol(comptime T: type, v: anytype, levels: schema.Levels) filter_eval.ColumnT(T) {
    return .{
        .values = v.values,
        .def_levels = v.def_levels,
        .max_def = @intCast(levels.max_def),
        .has_nulls = v.has_nulls,
    };
}

fn decodeCodes(
    comptime T: type,
    ra: std.mem.Allocator,
    chunk: []const u8,
    codec: schema.CompressionCodec,
    levels: schema.Levels,
    n_leaves: usize,
    decode_options: DecodeOptions,
    scratch: ?*DecodeScratch,
) !?column_mod.ColumnChunkReader(T).CodedOrValues {
    var reader = column_mod.ColumnChunkReader(T).initWithOptions(chunk, codec, levels, ra, decode_options, scratch);
    return reader.readAllCodes(n_leaves);
}

/// Fold every call statistics can answer for this row group into its accumulator and mark it in `handled`. Returns
/// whether any call still needs the row group decoded.
fn foldFromStats(
    calls: []const expr_agg.AggCall,
    accumulators: []expr_agg.Accumulator,
    in: AggRowGroup,
    stats: agg_plan.StatsUse,
    handled: []bool,
    timings: *Timings,
) !bool {
    std.debug.assert(calls.len == accumulators.len);
    const t_stats_start = nowMonoNs();
    var answered: usize = 0;
    defer {
        const t_stats_end = nowMonoNs();
        timings.eval_ns += @intCast(t_stats_end - t_stats_start);
        profile.record(timings.prof, .stats_fold, t_stats_start, t_stats_end, profile.args(.{answered}));
    }
    var any_decode_required = false;
    for (calls, accumulators, handled) |call, *acc, *h| {
        if (stats == .ignore or !expr_agg.canStatShortCircuit(call, in.filter != null, stats == .trust)) {
            any_decode_required = true;
            continue;
        }
        if (try expr_agg.updateOneFromStats(acc, call, in.rg, in.meta)) {
            h.* = true;
            answered += 1;
        } else {
            any_decode_required = true;
        }
    }
    return any_decode_required;
}

const PageIndex = struct {
    col_index: schema.ColumnIndex,
    offset_index: schema.OffsetIndex,
};

/// The page indexes of the columns to decode, where present and consistent with the footer; none under
/// `--scan-all`. An unparseable or inconsistent index is ignored and its column decoded whole.
fn loadPageIndexes(ra: std.mem.Allocator, in: AggRowGroup, stats: agg_plan.StatsUse) ![]?PageIndex {
    const num_leaves = in.rg.columns.items.len;
    const num_rows: usize = @intCast(in.rg.num_rows);
    const indexes = try ra.alloc(?PageIndex, num_leaves);
    @memset(indexes, null);
    if (stats == .ignore) return indexes;

    for (in.read, 0..) |needed, ci| {
        if (!needed) continue;
        if (ci >= num_leaves) continue;
        const chunk_meta = in.rg.columns.items[ci];

        const co = chunk_meta.column_index_offset orelse continue;
        const cl = chunk_meta.column_index_length orelse continue;
        const oo = chunk_meta.offset_index_offset orelse continue;
        const ol = chunk_meta.offset_index_length orelse continue;

        const col_bytes = originSlice(in.src, co, cl) orelse continue;
        const off_bytes = originSlice(in.src, oo, ol) orelse continue;

        var col_reader = thrift.Reader.init(col_bytes);
        const col_index = schema.ColumnIndex.read(ra, &col_reader) catch continue;

        var off_reader = thrift.Reader.init(off_bytes);
        const offset_index = schema.OffsetIndex.read(ra, &off_reader) catch continue;
        if (!pageIndexUsable(col_index, offset_index, num_rows)) continue;

        const cm = chunk_meta.meta_data orelse continue;
        const levels = in.meta.getColumnLevels(cm.path_in_schema.items);
        if (!pageIndexIsPlausible(&col_index, &offset_index, cm, levels, in.rg.num_rows)) continue;

        indexes[ci] = .{ .col_index = col_index, .offset_index = offset_index };
    }
    return indexes;
}

/// Per-page verdicts of the outer filter's conjunctive leaves, for the columns they test. Null for a column no
/// leaf tests (or with no page index).
const FilterPages = struct {
    skipped: []?[]bool,
    always_match: []?[]bool,
};

/// Prune pages with the outer filter's conjunctive leaves and deselect the rows of every page a leaf skips.
fn pruneFilterPages(
    ra: std.mem.Allocator,
    in: AggRowGroup,
    page_indexes: []const ?PageIndex,
    stats: agg_plan.StatsUse,
    sel: *filter_selection.SelectionVector,
) !FilterPages {
    const num_leaves = in.rg.columns.items.len;
    const num_rows: usize = @intCast(in.rg.num_rows);
    const pages: FilterPages = .{
        .skipped = try ra.alloc(?[]bool, num_leaves),
        .always_match = try ra.alloc(?[]bool, num_leaves),
    };
    @memset(pages.skipped, null);
    @memset(pages.always_match, null);

    var conj_leaves: std.ArrayList(ConjunctiveLeaf) = .empty;
    if (in.filter) |f| {
        try collectConjunctiveLeaves(f, &conj_leaves, ra);
    }

    for (conj_leaves.items) |leaf| {
        const ci = leaf.col_idx;
        if (ci >= num_leaves) continue;
        if (page_indexes[ci]) |pi| {
            const locs = pi.offset_index.page_locations.items;
            const skipped = pages.skipped[ci] orelse blk: {
                const s = try ra.alloc(bool, locs.len);
                @memset(s, false);
                break :blk s;
            };
            const always = pages.always_match[ci] orelse blk: {
                const a = try ra.alloc(bool, locs.len);
                @memset(a, false);
                break :blk a;
            };
            const is_first = pages.skipped[ci] == null;

            var ci_list = try ra.alloc(?schema.ColumnIndex, num_leaves);
            @memset(ci_list, null);
            for (page_indexes, 0..) |p_idx, idx| {
                if (p_idx) |p| ci_list[idx] = p.col_index;
            }

            for (locs, 0..) |loc, page_idx| {
                const dec = filter_prune.prunePage(in.rg, page_idx, leaf.filter, ci_list, in.meta, stats == .trust);
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

            pages.skipped[ci] = skipped;
            pages.always_match[ci] = always;
        }
    }
    return pages;
}

/// How to decode column `ci` page by page. A column no filter leaf tests skips just the pages left with no active
/// row. The always-match shortcut in `decodeWithReaderPruned` fills a page with the ColumnIndex minimum instead of
/// decoding it, which is only sound when the filter's mandatory conjuncts are the sole readers of the values: never
/// for a `preserve`d column. In `c >= 0 AND (c = 2 OR b = 1)` a page of c=[1,2] proves `c >= 0`, and filling it with 1
/// would fail the row c=2, b=0 that passes.
fn pagePruning(
    ra: std.mem.Allocator,
    pi: PageIndex,
    filter_pages: FilterPages,
    ci: usize,
    preserve: bool,
    sel: *const filter_selection.SelectionVector,
    num_rows: usize,
    cm: schema.ColumnMetaData,
    chunk_start: usize,
) !PruningInfo {
    const locs = pi.offset_index.page_locations.items;
    var skipped = filter_pages.skipped[ci];
    var always = filter_pages.always_match[ci];

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
    if (preserve) {
        const a = try ra.alloc(bool, locs.len);
        @memset(a, false);
        always = a;
    }

    return .{
        .locations = locs,
        .page_is_skipped = skipped.?,
        .page_is_always_match = always.?,
        .dictionary_page_offset = cm.dictionary_page_offset,
        .chunk_file_offset = @intCast(chunk_start),
        .col_index = pi.col_index,
    };
}

/// Decode one column chunk into the batch lane its physical and logical type select.
fn decodeAggColumn(
    ra: std.mem.Allocator,
    chunk: []const u8,
    cm: schema.ColumnMetaData,
    meta: *const schema.FileMetaData,
    prune_info: ?PruningInfo,
    decode_options: DecodeOptions,
    scratch: *DecodeScratch,
) !filter_eval.Batch.Column {
    const levels = meta.getColumnLevels(cm.path_in_schema.items);
    const n_leaves: usize = @intCast(cm.num_values);
    const schema_elem = meta.getColumnSchema(cm.path_in_schema.items);
    const dec_kind: ?decimal_mod.Kind = if (schema_elem) |se| decimal_mod.kindFromSchema(&se) else null;

    if (dec_kind) |k| return .{
        .f64 = try decimal_mod.decodeColumnExact(ra, chunk, cm.codec, levels, n_leaves, k, decode_options, scratch),
    };
    if (schema_elem != null and schema.isFloat16(schema_elem.?)) return .{
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
    };
    return switch (cm.type) {
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
            .{ .i32 = try decodeColumnTPruned(
                i32,
                ra,
                chunk,
                cm.codec,
                levels,
                n_leaves,
                prune_info,
                decode_options,
                scratch,
            ) },
        .INT64 => .{ .i64 = try decodeColumnTPruned(
            i64,
            ra,
            chunk,
            cm.codec,
            levels,
            n_leaves,
            prune_info,
            decode_options,
            scratch,
        ) },
        .FLOAT => .{ .f32 = try decodeColumnTPruned(
            f32,
            ra,
            chunk,
            cm.codec,
            levels,
            n_leaves,
            prune_info,
            decode_options,
            scratch,
        ) },
        .DOUBLE => .{ .f64 = try decodeColumnTPruned(
            f64,
            ra,
            chunk,
            cm.codec,
            levels,
            n_leaves,
            prune_info,
            decode_options,
            scratch,
        ) },
        .BYTE_ARRAY => .{ .string = try decodeColumnTPruned(
            []const u8,
            ra,
            chunk,
            cm.codec,
            levels,
            n_leaves,
            prune_info,
            decode_options,
            scratch,
        ) },
        .BOOLEAN => .{ .boolean = try decodeColumnTPruned(
            bool,
            ra,
            chunk,
            cm.codec,
            levels,
            n_leaves,
            prune_info,
            decode_options,
            scratch,
        ) },
        .INT96 => .{ .i64 = try int96_mod.decodeColumnAsI64Nanos(
            ra,
            chunk,
            cm.codec,
            levels,
            n_leaves,
        ) },
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
    /// Must hold the file from offset 0: page indexes are found by their file offsets.
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
    bloom_write: BloomWrite,
) !RGOut {
    invariant.assert(src.byte_origin == 0);
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

    var out: streaming.SinkOut = .{ .sink = sink, .offset = @intCast(out_offset.*) };
    defer out_offset.* = out.offset;

    if (kept_eff) |kept| {
        var new_cols: std.ArrayListUnmanaged(schema.ColumnChunk) = .empty;
        try new_cols.ensureTotalCapacity(out_arena, rg.columns.items.len);
        var rg_total: i64 = 0;

        for (kept, 0..) |b, ci| {
            if (!b) continue;
            if (ci >= rg.columns.items.len) return error.BadColumnIndex;
            const src_chunk = &rg.columns.items[ci];
            const m = src_chunk.meta_data orelse return error.InvalidColumnOffsets;
            const range = fastpath.chunkRange(&m, std.math.maxInt(usize)) orelse return error.InvalidColumnOffsets;
            const slice = try sourceBytes(src, range);

            if (recompress) |rc| if (m.codec != rc.codec) {
                var written = try copyChunkRecompressed(
                    rc,
                    src,
                    src_chunk.*,
                    slice,
                    range.start,
                    &out,
                    timings,
                    emit_page_index,
                    if (rc.done) |d| d[ci] else null,
                );
                try requestedFilter(out_arena, &written, rg, ci, src, bloom_write);
                try new_cols.append(out_arena, written);
                rg_total += written.meta_data.?.total_compressed_size;
                continue;
            };
            const delta = offsetDelta(out.pos(), range.start);
            try timedWrite(&out, slice, timings);
            // Carry the page index forward, written right after this chunk's data.
            var copied = try carryChunk(out_arena, &out, src, src_chunk, delta, emit_page_index);
            try requestedFilter(out_arena, &copied, rg, ci, src, bloom_write);
            try new_cols.append(out_arena, copied);
            rg_total += @intCast(range.len);
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

    // No projection: copy this RG's bounding box [min_col_start,
    // max_col_end), then shift every chunk's offset by a single delta.
    //
    // We compute the bounding box internally rather than trusting
    // the caller because the contract is easy to get wrong — passing
    // the whole-file bytes here would cause us to write every RG's
    // worth of bytes once per RG (an N-row-group file produces N×
    // output bloat). Production callers pass the whole-file bytes;
    // this branch makes that safe.
    for (rg.columns.items) |chunk| if (chunk.meta_data == null) return error.InvalidColumnOffsets;
    const range = try fastpath.rowGroupByteRange(rg, std.math.maxInt(usize)) orelse return error.InvalidColumnOffsets;
    const rg_slice = try sourceBytes(src, range);

    const delta = offsetDelta(out.pos(), range.start);
    try timedWrite(&out, rg_slice, timings);

    var cols: std.ArrayListUnmanaged(schema.ColumnChunk) = .empty;
    try cols.ensureTotalCapacity(out_arena, rg.columns.items.len);
    // Whole-RG copy: one delta for the group. Index bytes for each
    // chunk are appended after the RG data block, in column order.
    for (rg.columns.items, 0..) |*chunk, ci| {
        var copied = try carryChunk(out_arena, &out, src, chunk, delta, emit_page_index);
        try requestedFilter(out_arena, &copied, rg, ci, src, bloom_write);
        try cols.append(out_arena, copied);
    }

    return .{
        .surviving_rows = rg.num_rows,
        .rg = .{
            .columns = cols,
            .total_byte_size = @intCast(range.len),
            .num_rows = rg.num_rows,
        },
    };
}

/// Give a copied chunk the Bloom filter the write asks of column `ci`, when it carries none from its source: decode
/// the source chunk and build one from its values. A filter it carries is copied onto `out_arena` too: the filters
/// are written after the last row group, and a windowed input has released this one's bytes by then.
fn requestedFilter(
    out_arena: std.mem.Allocator,
    copied: *schema.ColumnChunk,
    rg: *const schema.RowGroup,
    ci: usize,
    src: RGSrc,
    bloom_write: BloomWrite,
) !void {
    if (copied.bloom_out.len > 0) {
        copied.bloom_out = try out_arena.dupe(u8, copied.bloom_out);
        return;
    }
    const fpp = bloom_write.at(ci) orelse return;
    const meta = bloom_write.meta orelse return;
    var scratch = std.heap.ArenaAllocator.init(bloom_write.scratch orelse return);
    defer scratch.deinit();
    const a = scratch.allocator();
    const chunk = &rg.columns.items[ci];
    const cm = chunk.meta_data orelse return;
    const range = fastpath.chunkRange(&cm, std.math.maxInt(usize)) orelse return error.InvalidColumnOffsets;
    const bytes = try sourceBytes(src, range);
    const levels = meta.getColumnLevels(cm.path_in_schema.items);
    const n: usize = @intCast(cm.num_values);
    const col: filter_eval.Batch.Column = switch (cm.type) {
        .INT32 => .{ .i32 = try decodeColumnT(i32, a, bytes, cm.codec, levels, n) },
        .INT64 => .{ .i64 = try decodeColumnT(i64, a, bytes, cm.codec, levels, n) },
        .FLOAT => .{ .f32 = try decodeColumnT(f32, a, bytes, cm.codec, levels, n) },
        .DOUBLE => .{ .f64 = try decodeColumnT(f64, a, bytes, cm.codec, levels, n) },
        .BYTE_ARRAY => .{ .string = try decodeColumnT([]const u8, a, bytes, cm.codec, levels, n) },
        .FIXED_LEN_BYTE_ARRAY => .{ .string = try decodeFlbaColumn(
            a,
            bytes,
            cm.codec,
            levels,
            n,
            flbaWidth(meta.getColumnSchema(cm.path_in_schema.items)),
        ) },
        else => return,
    };
    const filter = try encoder.bloomFilter(a, col, cm.type, fpp) orelse return;
    copied.bloom_out = try out_arena.dupe(u8, filter);
}

/// The bytes of `range`, a span of the source file, in `src`. An S3 input holds only the chunks its plan fetched, so
/// one outside them is MissingChunkBytes.
fn sourceBytes(src: RGSrc, range: fastpath.ByteRange) ![]const u8 {
    const origin: usize = @intCast(src.byte_origin);
    if (range.start < origin) return error.MissingChunkBytes;
    const off = range.start - origin;
    if (off > src.bytes.len or range.len > src.bytes.len - off) return error.MissingChunkBytes;
    return src.bytes[off..][0..range.len];
}

fn offsetDelta(new_start: usize, src_start: usize) i64 {
    return @as(i64, @intCast(new_start)) - @as(i64, @intCast(src_start));
}

fn timedWrite(out: *streaming.SinkOut, bytes: []const u8, timings: *Timings) !void {
    const t_sink_start = nowMonoNs();
    try out.write(bytes);
    const t_sink_end = nowMonoNs();
    timings.sink_ns += @intCast(t_sink_end - t_sink_start);
    profile.record(timings.prof, .sink_write, t_sink_start, t_sink_end, profile.args(.{bytes.len}));
}

/// A copied chunk's metadata, offsets moved by `delta`, with its page index carried into `out` when the output
/// keeps one.
fn carryChunk(
    out_arena: std.mem.Allocator,
    out: *streaming.SinkOut,
    src: RGSrc,
    chunk: *const schema.ColumnChunk,
    delta: i64,
    emit_page_index: bool,
) !schema.ColumnChunk {
    return if (emit_page_index)
        fastpath.rebaseChunk(out_arena, out, src.bytes, chunk, delta)
    else
        fastpath.copiedChunk(src.bytes, chunk, delta);
}

/// The Bloom filters a write builds: the false-positive probability of each column's filter, null for none. Indexed
/// by output column for `encodeAggregator`, by source leaf for `copyRG`, where a copied chunk that carries no filter
/// of its own is decoded (through `meta`, on `scratch`) to build one.
pub const BloomWrite = struct {
    fpp: []const ?f64 = &.{},
    meta: ?*const schema.FileMetaData = null,
    scratch: ?std.mem.Allocator = null,

    fn at(self: BloomWrite, i: usize) ?f64 {
        return if (i < self.fpp.len) self.fpp[i] else null;
    }
};

/// A codec change requested of a byte copy. `scratch` backs the per-chunk
/// working memory, released once the chunk is written.
pub const Recompress = struct {
    codec: schema.CompressionCodec,
    zstd: compression.ZstdPlan = .{},
    scratch: std.mem.Allocator,
    /// The row group's chunks already recompressed by `recompressRowGroup`, indexed by column (null where the chunk
    /// keeps its codec), so row groups can be recompressed in parallel and still be placed in order by `copyRG`.
    done: ?[]const ?RecompressedChunk = null,
};

/// One page of a recompressed chunk: where it started in the source chunk,
/// and where it starts and how long it is (header included) in the output.
const PageMove = struct { src_off: usize, dst_off: usize, dst_len: usize };

pub const RecompressedChunk = struct {
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
///
/// The result lives on `arena`; `scratch` holds the decompressed page, one
/// buffer reused across pages and freed before returning.
fn recompressChunk(
    arena: std.mem.Allocator,
    scratch: std.mem.Allocator,
    chunk: []const u8,
    from: schema.CompressionCodec,
    to: schema.CompressionCodec,
    zstd: compression.ZstdPlan,
) !RecompressedChunk {
    var out: std.ArrayList(u8) = .empty;
    var pages: std.ArrayList(PageMove) = .empty;
    var raw: std.ArrayList(u8) = .empty;
    defer raw.deinit(scratch);
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
        const new_body = try recodePage(arena, scratch, &raw, &h, body, from, to, zstd);
        const new_hdr = try patchPageHeader(arena, chunk[pos..][0..hdr_len], @intCast(new_body.len));
        try pages.append(arena, .{ .src_off = pos, .dst_off = out.items.len, .dst_len = new_hdr.len + new_body.len });
        try out.appendSlice(arena, new_hdr);
        try out.appendSlice(arena, new_body);
        header_delta += @as(i64, @intCast(new_hdr.len)) - @as(i64, @intCast(hdr_len));
        pos += hdr_len + body_len;
    }
    return .{ .bytes = out.items, .pages = pages.items, .header_delta = header_delta };
}

/// `recompressChunk`, each page written to `out` as soon as it is recoded rather than gathered: the result's `bytes`
/// is empty, its `pages` place what was written. `arena` holds the page moves; each page's own memory is released
/// before the next.
fn recompressChunkTo(
    arena: std.mem.Allocator,
    scratch: std.mem.Allocator,
    chunk: []const u8,
    from: schema.CompressionCodec,
    to: schema.CompressionCodec,
    zstd: compression.ZstdPlan,
    out: *streaming.SinkOut,
    timings: *Timings,
) !RecompressedChunk {
    var pages: std.ArrayList(PageMove) = .empty;
    var raw: std.ArrayList(u8) = .empty;
    defer raw.deinit(scratch);
    var header_delta: i64 = 0;
    var pos: usize = 0;
    var dst: usize = 0;
    while (pos < chunk.len) {
        var page_arena = std.heap.ArenaAllocator.init(scratch);
        defer page_arena.deinit();
        const pa = page_arena.allocator();
        var r = thrift.Reader.init(chunk[pos..]);
        const h = try schema.PageHeader.read(&r);
        const hdr_len = r.pos;
        if (h.compressed_page_size < 0 or h.uncompressed_page_size < 0) return error.BadPageHeader;
        const body_len: usize = @intCast(h.compressed_page_size);
        if (body_len > chunk.len - pos - hdr_len) return error.TruncatedPage;
        const body = chunk[pos + hdr_len ..][0..body_len];
        const new_body = try recodePage(pa, scratch, &raw, &h, body, from, to, zstd);
        const new_hdr = try patchPageHeader(pa, chunk[pos..][0..hdr_len], @intCast(new_body.len));
        try pages.append(arena, .{ .src_off = pos, .dst_off = dst, .dst_len = new_hdr.len + new_body.len });
        try timedWrite(out, new_hdr, timings);
        try timedWrite(out, new_body, timings);
        dst += new_hdr.len + new_body.len;
        header_delta += @as(i64, @intCast(new_hdr.len)) - @as(i64, @intCast(hdr_len));
        pos += hdr_len + body_len;
    }
    return .{ .bytes = &.{}, .pages = pages.items, .header_delta = header_delta };
}

/// A page's body in codec `to`: a v1 data or dictionary page recompressed whole, a compressed v2 page's values section
/// (its levels are never compressed), anything else as it is.
fn recodePage(
    arena: std.mem.Allocator,
    scratch: std.mem.Allocator,
    raw: *std.ArrayList(u8),
    h: *const schema.PageHeader,
    body: []const u8,
    from: schema.CompressionCodec,
    to: schema.CompressionCodec,
    zstd: compression.ZstdPlan,
) ![]const u8 {
    const raw_len: usize = @intCast(h.uncompressed_page_size);
    switch (h.type) {
        .DATA_PAGE, .DICTIONARY_PAGE => return recode(arena, scratch, raw, body, from, to, zstd, raw_len),
        .DATA_PAGE_V2 => {
            const v2 = h.data_page_header_v2 orelse return error.BadPageHeader;
            if (!v2.is_compressed) return body;
            if (v2.definition_levels_byte_length < 0 or v2.repetition_levels_byte_length < 0) return error.BadPageHeader;
            const levels: usize = @as(usize, @intCast(v2.definition_levels_byte_length)) +
                @as(usize, @intCast(v2.repetition_levels_byte_length));
            if (levels > body.len or levels > raw_len) return error.BadPageHeader;
            const values = try recode(arena, scratch, raw, body[levels..], from, to, zstd, raw_len - levels);
            const joined = try arena.alloc(u8, levels + values.len);
            @memcpy(joined[0..levels], body[0..levels]);
            @memcpy(joined[levels..], values);
            return joined;
        },
        // Index pages carry no compressed payload.
        .INDEX_PAGE => return body,
    }
}

fn recode(
    arena: std.mem.Allocator,
    scratch: std.mem.Allocator,
    raw: *std.ArrayList(u8),
    body: []const u8,
    from: schema.CompressionCodec,
    to: schema.CompressionCodec,
    zstd: compression.ZstdPlan,
    raw_len: usize,
) ![]const u8 {
    // UNCOMPRESSED decompresses to `body` itself: no copy into the buffer.
    if (from == .UNCOMPRESSED or raw_len == 0)
        return compression.compressPage(arena, try compression.decompress(arena, body, from, raw_len), to, zstd);
    try raw.resize(scratch, raw_len);
    try compression.decompressInto(arena, body, from, raw.items);
    return compression.compressPage(arena, raw.items, to, zstd);
}

/// Recompress every chunk of `rg` that `kept` selects (null: all) and is not already in `codec`, one entry per
/// column, for `copyRG` to place through `Recompress.done`. Results live on `arena`; `scratch` is per-chunk working
/// memory, and must be safe to use from several threads when `workers` > 1. Safe to run for several row groups at
/// once: it only reads `src`. With `workers` > 1 the row group's pages are recompressed by that many threads and
/// placed in order: the same bytes as one thread writes.
pub fn recompressRowGroup(
    arena: std.mem.Allocator,
    scratch: std.mem.Allocator,
    rg: *const schema.RowGroup,
    src: RGSrc,
    kept: ?[]const bool,
    codec: schema.CompressionCodec,
    zstd: compression.ZstdPlan,
    timings: *Timings,
    workers: usize,
) ![]?RecompressedChunk {
    if (workers > 1) return recompressRowGroupPages(arena, scratch, rg, src, kept, codec, zstd, timings, workers);
    const done = try arena.alloc(?RecompressedChunk, rg.columns.items.len);
    @memset(done, null);
    for (rg.columns.items, done, 0..) |*chunk, *slot, ci| {
        if (kept) |k| if (ci >= k.len or !k[ci]) continue;
        const m = chunk.meta_data orelse return error.InvalidColumnOffsets;
        if (m.codec == codec) continue;
        const range = fastpath.chunkRange(&m, std.math.maxInt(usize)) orelse return error.InvalidColumnOffsets;
        const t0 = nowMonoNs();
        slot.* = recompressChunk(arena, scratch, try sourceBytes(src, range), m.codec, codec, zstd) catch |err|
            return reportRecompressError(err, m, codec);
        const t1 = nowMonoNs();
        timings.encode_ns += @intCast(t1 - t0);
        profile.record(timings.prof, .encode, t0, t1, profile.args(.{ 1, m.num_values }));
    }
    return done;
}

/// One page of a row group being recompressed: its header, where it sits in its chunk, and its new body once a
/// worker has recoded it.
const PageTask = struct {
    column: usize,
    header: schema.PageHeader,
    src_off: usize,
    hdr: []const u8,
    body: []const u8,
    from: schema.CompressionCodec,
    out: []const u8 = &.{},
};

const RecodeShared = struct {
    tasks: []PageTask,
    next: std.atomic.Value(usize) = .init(0),
    to: schema.CompressionCodec,
    zstd: compression.ZstdPlan,
    scratch: std.mem.Allocator,
    /// The lowest index of a task that failed, and its error: the failure one thread working in order meets first.
    failed: std.atomic.Value(usize) = .init(std.math.maxInt(usize)),
    errs: []?anyerror,
};

/// A worker of `recompressRowGroupPages`: recodes tasks until none are left, its bodies on its own arena.
const RecodeWorker = struct {
    shared: *RecodeShared,
    arena: std.heap.ArenaAllocator,
    busy_ns: u64 = 0,

    fn run(self: *RecodeWorker) void {
        const t0 = nowMonoNs();
        defer self.busy_ns = @intCast(nowMonoNs() - t0);
        const sh = self.shared;
        var raw: std.ArrayList(u8) = .empty;
        defer raw.deinit(sh.scratch);
        while (true) {
            const i = sh.next.fetchAdd(1, .monotonic);
            if (i >= sh.tasks.len) return;
            // Tasks past a failure are not needed: the failure is reported, nothing written.
            if (i > sh.failed.load(.monotonic)) continue;
            const t = &sh.tasks[i];
            t.out = recodePage(self.arena.allocator(), sh.scratch, &raw, &t.header, t.body, t.from, sh.to, sh.zstd) catch |err| {
                sh.errs[i] = err;
                _ = sh.failed.fetchMin(i, .monotonic);
                continue;
            };
        }
    }
};

/// `recompressRowGroup` over `workers` threads: every page of the row group's chunks is a task, recoded by whichever
/// worker takes it, then each chunk is assembled in page order as `recompressChunk` assembles it.
fn recompressRowGroupPages(
    arena: std.mem.Allocator,
    scratch: std.mem.Allocator,
    rg: *const schema.RowGroup,
    src: RGSrc,
    kept: ?[]const bool,
    codec: schema.CompressionCodec,
    zstd: compression.ZstdPlan,
    timings: *Timings,
    workers: usize,
) ![]?RecompressedChunk {
    const t0 = nowMonoNs();
    const done = try arena.alloc(?RecompressedChunk, rg.columns.items.len);
    @memset(done, null);
    var tasks: std.ArrayList(PageTask) = .empty;
    defer tasks.deinit(scratch);
    // A chunk that cannot be located or read, or a page header that does not parse, is reported after any failure in
    // the pages before it, as one thread meets them. Errors locating a chunk are returned as they are, as the serial
    // loop returns them; a page header's go through `reportRecompressError`, as `recompressChunk`'s do.
    var parse_err: ?struct { column: usize, err: anyerror, report: bool = true } = null;
    var values: i64 = 0;
    collect: for (rg.columns.items, 0..) |*chunk, ci| {
        if (kept) |k| if (ci >= k.len or !k[ci]) continue;
        const m = chunk.meta_data orelse {
            parse_err = .{ .column = ci, .err = error.InvalidColumnOffsets, .report = false };
            break :collect;
        };
        if (m.codec == codec) continue;
        const range = fastpath.chunkRange(&m, std.math.maxInt(usize)) orelse {
            parse_err = .{ .column = ci, .err = error.InvalidColumnOffsets, .report = false };
            break :collect;
        };
        const bytes = sourceBytes(src, range) catch |err| {
            parse_err = .{ .column = ci, .err = err, .report = false };
            break :collect;
        };
        values += m.num_values;
        var pos: usize = 0;
        while (pos < bytes.len) {
            var r = thrift.Reader.init(bytes[pos..]);
            const h = schema.PageHeader.read(&r) catch |err| {
                parse_err = .{ .column = ci, .err = err };
                break :collect;
            };
            const hdr_len = r.pos;
            if (h.compressed_page_size < 0 or h.uncompressed_page_size < 0) {
                parse_err = .{ .column = ci, .err = error.BadPageHeader };
                break :collect;
            }
            const body_len: usize = @intCast(h.compressed_page_size);
            if (body_len > bytes.len - pos - hdr_len) {
                parse_err = .{ .column = ci, .err = error.TruncatedPage };
                break :collect;
            }
            try tasks.append(scratch, .{
                .column = ci,
                .header = h,
                .src_off = pos,
                .hdr = bytes[pos..][0..hdr_len],
                .body = bytes[pos + hdr_len ..][0..body_len],
                .from = m.codec,
            });
            pos += hdr_len + body_len;
        }
        // An empty chunk still gets its (empty) entry, as `recompressChunk` gives it.
        done[ci] = .{ .bytes = &.{}, .pages = &.{}, .header_delta = 0 };
    }

    const errs = try scratch.alloc(?anyerror, tasks.items.len);
    defer scratch.free(errs);
    @memset(errs, null);
    var shared: RecodeShared = .{ .tasks = tasks.items, .to = codec, .zstd = zstd, .scratch = scratch, .errs = errs };
    const n_workers = @max(1, @min(workers, tasks.items.len));
    const pool = try scratch.alloc(RecodeWorker, n_workers);
    defer scratch.free(pool);
    for (pool) |*w| w.* = .{ .shared = &shared, .arena = .init(scratch) };
    defer for (pool) |*w| w.arena.deinit();
    const threads = try scratch.alloc(?std.Thread, n_workers);
    defer scratch.free(threads);
    @memset(threads, null);
    // The calling thread is worker 0; a worker that cannot start leaves its share to the others.
    for (pool[1..], threads[1..]) |*w, *t| t.* = std.Thread.spawn(.{}, RecodeWorker.run, .{w}) catch null;
    pool[0].run();
    for (threads[1..]) |t| if (t) |th| th.join();

    var busy: u64 = 0;
    for (pool) |w| busy += w.busy_ns;
    timings.encode_ns += busy;
    profile.record(timings.prof, .encode, t0, nowMonoNs(), profile.args(.{ done.len, values }));

    const failed = shared.failed.load(.monotonic);
    if (failed < tasks.items.len) {
        const m = rg.columns.items[tasks.items[failed].column].meta_data.?;
        return reportRecompressError(errs[failed].?, m, codec);
    }
    if (parse_err) |pe| {
        if (!pe.report) return pe.err;
        return reportRecompressError(pe.err, rg.columns.items[pe.column].meta_data.?, codec);
    }

    // Assemble each chunk in page order, as `recompressChunk` does.
    var i: usize = 0;
    while (i < tasks.items.len) {
        const ci = tasks.items[i].column;
        var out: std.ArrayList(u8) = .empty;
        var pages: std.ArrayList(PageMove) = .empty;
        var header_delta: i64 = 0;
        while (i < tasks.items.len and tasks.items[i].column == ci) : (i += 1) {
            const t = tasks.items[i];
            const new_hdr = try patchPageHeader(arena, t.hdr, @intCast(t.out.len));
            try pages.append(arena, .{ .src_off = t.src_off, .dst_off = out.items.len, .dst_len = new_hdr.len + t.out.len });
            try out.appendSlice(arena, new_hdr);
            try out.appendSlice(arena, t.out);
            header_delta += @as(i64, @intCast(new_hdr.len)) - @as(i64, @intCast(t.hdr.len));
        }
        done[ci] = .{ .bytes = out.items, .pages = pages.items, .header_delta = header_delta };
    }
    return done;
}

fn reportRecompressError(err: anyerror, m: schema.ColumnMetaData, to: schema.CompressionCodec) anyerror {
    if (err == error.UnsupportedCodec) {
        const msg = "zpq: cannot recompress column `{s}` to {s}: its pages use {s}, which zpq cannot decompress\n";
        std.debug.print(msg, .{
            if (m.path_in_schema.items.len > 0) m.path_in_schema.items[m.path_in_schema.items.len - 1] else "?",
            @tagName(to),
            @tagName(m.codec),
        });
    }
    return err;
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
    // The end: past the last page (`bytes` is empty when the pages went straight to the sink).
    if (re.pages.len == 0) return re.bytes.len;
    const last = re.pages[re.pages.len - 1];
    return last.dst_off + last.dst_len;
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
    out: *streaming.SinkOut,
    timings: *Timings,
    emit_page_index: bool,
    /// This chunk recompressed ahead of time (`Recompress.done`), else null to recompress it here.
    precomputed: ?RecompressedChunk,
) !schema.ColumnChunk {
    var scratch_state = std.heap.ArenaAllocator.init(rc.scratch);
    defer scratch_state.deinit();
    const scratch = scratch_state.allocator();
    const m = src_chunk.meta_data.?;

    const new_start: i64 = @intCast(out.pos());
    const re = if (precomputed) |p| blk: {
        try timedWrite(out, p.bytes, timings);
        break :blk p;
    } else blk: {
        // Page by page into the sink: a codec that expands (to UNCOMPRESSED, say) never holds more than a page.
        const t_encode = nowMonoNs();
        const r = recompressChunkTo(scratch, rc.scratch, slice, m.codec, rc.codec, rc.zstd, out, timings) catch |err|
            return reportRecompressError(err, m, rc.codec);
        const t_encode_end = nowMonoNs();
        timings.encode_ns += @intCast(t_encode_end - t_encode);
        profile.record(timings.prof, .encode, t_encode, t_encode_end, profile.args(.{ 1, m.num_values }));
        break :blk r;
    };
    const written: usize = @intCast(@as(i64, @intCast(out.pos())) - new_start);

    const src_base: i64 = @intCast(src_start);
    var chunk = src_chunk;
    chunk.absent = &.{};
    // Recompressing changes the bytes, not the values: the filter still holds.
    chunk.bloom_out = fastpath.carriedFilter(src.bytes, &src_chunk);
    var meta = m;
    meta.bloom_filter_offset = null;
    meta.bloom_filter_length = null;
    meta.codec = rc.codec;
    meta.total_compressed_size = @intCast(written);
    meta.total_uncompressed_size += re.header_delta;
    meta.data_page_offset = new_start + @as(i64, @intCast(relocated(re, m.data_page_offset - src_base)));
    if (m.dictionary_page_offset) |d| {
        meta.dictionary_page_offset = new_start + @as(i64, @intCast(relocated(re, d - src_base)));
    }
    // index_page_offset and the OffsetIndex locations are the file's word, unvalidated at footer open: one that is
    // not a page of this chunk (or not even an offset from it) drops what it would locate, as an unusable index is.
    meta.index_page_offset = if (m.index_page_offset) |ip| blk: {
        const rel = std.math.sub(i64, ip, src_base) catch break :blk null;
        break :blk if (movedPage(re.pages, rel)) |p| new_start + @as(i64, @intCast(p.dst_off)) else null;
    } else null;
    chunk.meta_data = meta;
    chunk.file_offset = meta.data_page_offset;
    chunk.offset_index_offset = null;
    chunk.offset_index_length = null;
    chunk.column_index_offset = null;
    chunk.column_index_length = null;
    if (!emit_page_index) return chunk;

    // The OffsetIndex is carried when every location remaps onto a page
    // start; the ColumnIndex only beside it, since its pages are found
    // through those locations.
    const oo = src_chunk.offset_index_offset orelse return chunk;
    const ol = src_chunk.offset_index_length orelse return chunk;
    const oi_bytes = originSlice(src, oo, ol) orelse return chunk;
    var oi_reader = thrift.Reader.init(oi_bytes);
    var oi = schema.OffsetIndex.read(scratch, &oi_reader) catch return chunk;
    for (oi.page_locations.items) |*loc| {
        const rel = std.math.sub(i64, loc.offset, src_base) catch return chunk;
        const p = movedPage(re.pages, rel) orelse return chunk;
        loc.offset = new_start + @as(i64, @intCast(p.dst_off));
        loc.compressed_page_size = @intCast(p.dst_len);
    }
    var oi_w = thrift.Writer.init(scratch);
    oi.write(&oi_w) catch return chunk;

    if (src_chunk.column_index_offset) |co| if (src_chunk.column_index_length) |cl| {
        if (originSlice(src, co, cl)) |ci_bytes| if (fastpath.reserializeColumnIndex(scratch, ci_bytes)) |ci_ser| {
            chunk.column_index_offset = @intCast(out.pos());
            chunk.column_index_length = @intCast(ci_ser.len);
            try out.write(ci_ser);
        };
    };
    chunk.offset_index_offset = @intCast(out.pos());
    chunk.offset_index_length = @intCast(oi_w.bytes().len);
    try out.write(oi_w.bytes());
    return chunk;
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
            } else if (c.rowCount() != num_rows) return error.ColumnRowCountMismatch;
            if (c.def_levels) |dl| if (dl.len != c.rowCount()) return error.ColumnRowCountMismatch;
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
        .{},
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
            const plain = try recompressChunk(a, testing.allocator, src, cm.codec, .UNCOMPRESSED, .{});
            const there = try recompressChunk(a, testing.allocator, plain.bytes, .UNCOMPRESSED, case.via, .{});
            const back = try recompressChunk(a, testing.allocator, there.bytes, case.via, .UNCOMPRESSED, .{});
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

test "recompressRowGroup over several threads writes the bytes one thread writes, and fails where it fails" {
    const testing = std.testing;
    const paths = [_][]const u8{
        "data/parquet-testing/data/alltypes_tiny_pages.parquet",
        "data/parquet-testing/data/datapage_v2.snappy.parquet",
    };
    for (paths) |path| {
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
        const a = arena.allocator();
        var timings: Timings = .{};
        for (meta.row_groups.items) |*rg| for ([_]schema.CompressionCodec{ .ZSTD, .GZIP, .UNCOMPRESSED }) |codec| {
            // Every column, and every other one.
            const alternate = try a.alloc(bool, rg.columns.items.len);
            for (alternate, 0..) |*k, i| k.* = i % 2 == 0;
            for ([_]?[]const bool{ null, alternate }) |kept| {
                const src: RGSrc = .{ .bytes = bytes, .byte_origin = 0 };
                const one = try recompressRowGroup(a, testing.allocator, rg, src, kept, codec, .{}, &timings, 1);
                for ([_]usize{ 2, 5 }) |workers| {
                    const many =
                        try recompressRowGroup(a, testing.allocator, rg, src, kept, codec, .{}, &timings, workers);
                    try testing.expectEqual(one.len, many.len);
                    for (one, many) |x, y| {
                        try testing.expectEqual(x == null, y == null);
                        const p = x orelse continue;
                        const q = y.?;
                        try testing.expectEqualSlices(u8, p.bytes, q.bytes);
                        try testing.expectEqualSlices(PageMove, p.pages, q.pages);
                        try testing.expectEqual(p.header_delta, q.header_delta);
                    }
                }
            }
        };

        // A page body that does not decompress, in the first chunk, and a page header that does not parse, in the
        // last: one thread fails at the body first, and so must several.
        const rg = &meta.row_groups.items[0];
        if (rg.columns.items.len < 2 or rg.columns.items[0].meta_data.?.codec == .UNCOMPRESSED) continue;
        const broken = try a.dupe(u8, bytes);
        const first = rg.columns.items[0].meta_data.?;
        const first_start: usize = @intCast(first.dictionary_page_offset orelse first.data_page_offset);
        var r = thrift.Reader.init(broken[first_start..]);
        const h = try schema.PageHeader.read(&r);
        @memset(broken[first_start + r.pos ..][0..@intCast(@min(h.compressed_page_size, 16))], 0xFF);
        const last = rg.columns.items[rg.columns.items.len - 1].meta_data.?;
        const last_start: usize = @intCast(last.dictionary_page_offset orelse last.data_page_offset);
        @memset(broken[last_start..][0..4], 0xFF);
        const src: RGSrc = .{ .bytes = broken, .byte_origin = 0 };
        const want = if (recompressRowGroup(a, testing.allocator, rg, src, null, .ZSTD, .{}, &timings, 1)) |_|
            return error.TestExpectedError
        else |err|
            err;
        try testing.expectError(want, recompressRowGroup(a, testing.allocator, rg, src, null, .ZSTD, .{}, &timings, 4));
    }
}

test "recompressRowGroup over several threads reports a bad page body ahead of a later chunk with no metadata" {
    const testing = std.testing;
    const path = "data/parquet-testing/data/datapage_v2.snappy.parquet";
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
    var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_state.deinit();
    const a = arena_state.allocator();
    const rg = &meta.row_groups.items[0];
    // The first chunk's first page body does not decompress; the last chunk has no metadata.
    const broken = try a.dupe(u8, bytes);
    const m = rg.columns.items[0].meta_data.?;
    const start: usize = @intCast(m.dictionary_page_offset orelse m.data_page_offset);
    var r = thrift.Reader.init(broken[start..]);
    const h = try schema.PageHeader.read(&r);
    @memset(broken[start + r.pos ..][0..@intCast(@min(h.compressed_page_size, 16))], 0xFF);
    const last = &rg.columns.items[rg.columns.items.len - 1];
    const saved = last.meta_data;
    defer last.meta_data = saved;
    last.meta_data = null;
    var timings: Timings = .{};
    const src: RGSrc = .{ .bytes = broken, .byte_origin = 0 };
    const want = if (recompressRowGroup(a, testing.allocator, rg, src, null, .ZSTD, .{}, &timings, 1)) |_|
        return error.TestExpectedError
    else |err|
        err;
    try testing.expectError(want, recompressRowGroup(a, testing.allocator, rg, src, null, .ZSTD, .{}, &timings, 4));
}

test "decimal: nullable BYTE_ARRAY with filter dropping rows preserves definition levels and exactness" {
    const testing = std.testing;
    var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_state.deinit();
    const arena = arena_state.allocator();

    const elem = schema.SchemaElement{
        .type = .BYTE_ARRAY,
        .type_length = null,
        .repetition_type = .OPTIONAL,
        .name = "balance",
        .num_children = 0,
        .converted_type = .DECIMAL,
        .logical_type = .{ .DECIMAL = .{ .scale = 2, .precision = 9 } },
        .scale = 2,
        .precision = 9,
        .field_id = null,
    };

    const raw_strs = [_][]const u8{
        "",
        &[_]u8{ 0x30, 0x39 },
        &[_]u8{ 0xEC, 0x78 },
        "",
        &[_]u8{ 0x0F, 0x42, 0x3F },
        "",
        &[_]u8{0x05},
        "",
    };
    const def_levels = [_]u32{ 0, 1, 1, 0, 1, 0, 1, 0 };
    const enc = try encoder.encodeColumn(arena, .{
        .values = .{ .string = .{
            .values = &raw_strs,
            .def_levels = &def_levels,
            .max_def = 1,
        } },
        .schema_elem = &elem,
        .path_in_schema = &[_][]const u8{"balance"},
        .codec = .UNCOMPRESSED,
    });

    const kind = decimal_mod.kindFromSchema(&elem).?;
    const levels = schema.Levels{ .max_def = 1, .max_rep = 0 };

    const decoded = try decimal_mod.decodeColumnAsI128(arena, enc.bytes, .UNCOMPRESSED, levels, 8, kind);
    try testing.expectEqual(@as(usize, 8), decoded.values.len);
    try testing.expect(decoded.def_levels != null);
    try testing.expectEqualSlices(u32, &def_levels, decoded.def_levels.?);
    try testing.expectEqual(@as(i128, 0), decoded.values[0]);
    try testing.expectEqual(@as(i128, 12345), decoded.values[1]);
    try testing.expectEqual(@as(i128, -5000), decoded.values[2]);
    try testing.expectEqual(@as(i128, 0), decoded.values[3]);
    try testing.expectEqual(@as(i128, 999999), decoded.values[4]);
    try testing.expectEqual(@as(i128, 0), decoded.values[5]);
    try testing.expectEqual(@as(i128, 5), decoded.values[6]);
    try testing.expectEqual(@as(i128, 0), decoded.values[7]);

    var sel = try filter_selection.SelectionVector.init(arena, 8);
    sel.mask[0] = 0b00111100;
    const filtered = try encoder.applySelectionI128(arena, decoded, &sel);
    try testing.expectEqual(@as(usize, 4), filtered.values.len);

    var out_elem = elem;
    out_elem.type = .FIXED_LEN_BYTE_ARRAY;
    out_elem.type_length = decimal_mod.minBytesForPrecision(kind.precision);
    const re_enc = try encoder.encodeDecimalFlba(
        arena,
        filtered,
        &out_elem,
        &[_][]const u8{"balance"},
        .UNCOMPRESSED,
        .{},
    );

    const flba_kind = decimal_mod.kindFromSchema(&out_elem).?;
    const back = try decimal_mod.decodeColumnAsI128(arena, re_enc.bytes, .UNCOMPRESSED, levels, 4, flba_kind);
    try testing.expectEqual(@as(usize, 4), back.values.len);
    try testing.expect(back.def_levels != null);
    const want_defs = [_]u32{ 1, 0, 1, 0 };
    try testing.expectEqualSlices(u32, &want_defs, back.def_levels.?);
    try testing.expectEqual(@as(i128, -5000), back.values[0]);
    try testing.expectEqual(@as(i128, 0), back.values[1]);
    try testing.expectEqual(@as(i128, 999999), back.values[2]);
    try testing.expectEqual(@as(i128, 0), back.values[3]);
}

test "initOutputAggregator rewrites BYTE_ARRAY decimal to FIXED_LEN_BYTE_ARRAY with minBytesForPrecision" {
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
        .type = .BYTE_ARRAY,
        .type_length = null,
        .repetition_type = .OPTIONAL,
        .name = "price",
        .num_children = 0,
        .logical_type = .{ .DECIMAL = .{ .scale = 2, .precision = 10 } },
        .scale = null,
        .precision = null,
        .field_id = 1,
    });

    var path: schema.StringList = .empty;
    try path.append(arena, "price");
    var columns: std.ArrayListUnmanaged(schema.ColumnChunk) = .empty;
    try columns.append(arena, .{
        .file_path = null,
        .file_offset = 0,
        .meta_data = .{
            .type = .BYTE_ARRAY,
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
    try testing.expect(agg.cols[0] == .i128);
    try testing.expectEqual(schema.Type.FIXED_LEN_BYTE_ARRAY, agg.schema_elems[0].type.?);
    try testing.expectEqual(@as(?i32, 5), agg.schema_elems[0].type_length);
    try testing.expectEqual(schema.FieldRepetitionType.OPTIONAL, agg.schema_elems[0].repetition_type.?);
}

test "initOutputAggregator maps precision > 38 BYTE_ARRAY decimal to f64" {
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
        .type = .BYTE_ARRAY,
        .type_length = null,
        .repetition_type = .OPTIONAL,
        .name = "big_dec",
        .num_children = 0,
        .logical_type = .{ .DECIMAL = .{ .scale = 2, .precision = 40 } },
        .scale = null,
        .precision = null,
        .field_id = 1,
    });

    var path: schema.StringList = .empty;
    try path.append(arena, "big_dec");
    var columns: std.ArrayListUnmanaged(schema.ColumnChunk) = .empty;
    try columns.append(arena, .{
        .file_path = null,
        .file_offset = 0,
        .meta_data = .{
            .type = .BYTE_ARRAY,
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
}

test "StagePlan.init in countOnly mode plans zero calls and charges only base columns" {
    var arena_state = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena_state.deinit();
    const arena = arena_state.allocator();

    var file_meta: schema.FileMetaData = undefined;
    file_meta.schema = .empty;
    file_meta.row_groups = .empty;
    try file_meta.schema.append(arena, .{
        .name = "schema",
        .num_children = 2,
        .type = null,
        .type_length = null,
        .repetition_type = .REQUIRED,
        .scale = null,
        .precision = null,
        .field_id = null,
    });
    try file_meta.schema.append(arena, .{
        .name = "k",
        .type = .INT64,
        .num_children = 0,
        .repetition_type = .REQUIRED,
        .type_length = null,
        .scale = null,
        .precision = null,
        .field_id = 0,
    });
    try file_meta.schema.append(arena, .{
        .name = "v",
        .type = .INT64,
        .num_children = 0,
        .repetition_type = .REQUIRED,
        .type_length = null,
        .scale = null,
        .precision = null,
        .field_id = 1,
    });

    var path_k: schema.StringList = .empty;
    try path_k.append(arena, "k");
    var path_v: schema.StringList = .empty;
    try path_v.append(arena, "v");

    var columns: std.ArrayListUnmanaged(schema.ColumnChunk) = .empty;
    try columns.append(arena, .{
        .file_path = null,
        .file_offset = 0,
        .meta_data = .{
            .type = .INT64,
            .encodings = .empty,
            .path_in_schema = path_k,
            .codec = .UNCOMPRESSED,
            .num_values = 100,
            .total_uncompressed_size = 800,
            .total_compressed_size = 800,
            .data_page_offset = 0,
            .index_page_offset = null,
            .dictionary_page_offset = null,
        },
    });
    try columns.append(arena, .{
        .file_path = null,
        .file_offset = 0,
        .meta_data = .{
            .type = .INT64,
            .encodings = .empty,
            .path_in_schema = path_v,
            .codec = .UNCOMPRESSED,
            .num_values = 100,
            .total_uncompressed_size = 800,
            .total_compressed_size = 800,
            .data_page_offset = 0,
            .index_page_offset = null,
            .dictionary_page_offset = null,
        },
    });
    try file_meta.row_groups.append(arena, .{
        .columns = columns,
        .total_byte_size = 1600,
        .num_rows = 100,
    });

    const in: AggRowGroup = .{
        .rg = &file_meta.row_groups.items[0],
        .file_index = 0,
        .rg_index = 0,
        .meta = &file_meta,
        .src = .{ .bytes = &.{}, .byte_origin = 0 },
        .filter = null,
        .read = &.{ true, true },
        .consumed = &.{ true, false },
    };

    var table: expr_agg.GroupTable = .init(std.testing.allocator, 1 << 20);
    defer table.deinit();
    var sketch: expr_agg.CountSketch = .{ .cells = try arena.alloc(u32, 16), .shift = 60 };
    @memset(sketch.cells, 0);
    table.sketch = &sketch;
    try std.testing.expect(table.countOnly());

    const left_expr = try arena.create(expr_ast.Expr);
    left_expr.* = .{ .col_ref = .{ .col_idx = 1, .physical_type = .INT64, .expr_type = .i64 } };
    const right_expr = try arena.create(expr_ast.Expr);
    right_expr.* = .{ .col_ref = .{ .col_idx = 1, .physical_type = .INT64, .expr_type = .i64 } };
    const arg_expr: expr_ast.Expr = .{
        .binop = .{
            .op = .add,
            .left = left_expr,
            .right = right_expr,
            .result_type = .i64,
            .depth = 2,
        },
    };

    const calls = [_]expr_agg.AggCall{.{
        .func = .sum,
        .alias = "s",
        .result = .i64,
        .arg = arg_expr,
        .where = null,
    }};
    const keys = [_]expr_ast.Expr{.{ .col_ref = .{ .col_idx = 0, .physical_type = .INT64, .expr_type = .i64 } }};
    const target: AggTarget = .{ .grouped = .{
        .calls = &calls,
        .keys = &keys,
        .table = &table,
        .pos_base = 0,
    } };

    const sp = try StagePlan.init(arena, in, target, &.{false});
    try std.testing.expectEqual(@as(usize, 0), sp.calls.len);
    try std.testing.expectEqual(decodeEstimate(in, sp.base, &.{}, true), sp.estimate);
}

test "StagePlan.init in countOnly mode: allocation failures cleanly unwind without leak" {
    try std.testing.checkAllAllocationFailures(std.testing.allocator, struct {
        fn run(gpa: std.mem.Allocator) !void {
            var arena_state = std.heap.ArenaAllocator.init(gpa);
            defer arena_state.deinit();
            const arena = arena_state.allocator();

            var file_meta: schema.FileMetaData = undefined;
            file_meta.schema = .empty;
            file_meta.row_groups = .empty;
            try file_meta.schema.append(arena, .{
                .name = "schema",
                .num_children = 2,
                .type = null,
                .type_length = null,
                .repetition_type = .REQUIRED,
                .scale = null,
                .precision = null,
                .field_id = null,
            });
            try file_meta.schema.append(arena, .{
                .name = "k",
                .type = .INT64,
                .num_children = 0,
                .repetition_type = .REQUIRED,
                .type_length = null,
                .scale = null,
                .precision = null,
                .field_id = 0,
            });
            try file_meta.schema.append(arena, .{
                .name = "v",
                .type = .INT64,
                .num_children = 0,
                .repetition_type = .REQUIRED,
                .type_length = null,
                .scale = null,
                .precision = null,
                .field_id = 1,
            });

            var path_k: schema.StringList = .empty;
            try path_k.append(arena, "k");
            var path_v: schema.StringList = .empty;
            try path_v.append(arena, "v");

            var columns: std.ArrayListUnmanaged(schema.ColumnChunk) = .empty;
            try columns.append(arena, .{
                .file_path = null,
                .file_offset = 0,
                .meta_data = .{
                    .type = .INT64,
                    .encodings = .empty,
                    .path_in_schema = path_k,
                    .codec = .UNCOMPRESSED,
                    .num_values = 100,
                    .total_uncompressed_size = 800,
                    .total_compressed_size = 800,
                    .data_page_offset = 0,
                    .index_page_offset = null,
                    .dictionary_page_offset = null,
                },
            });
            try columns.append(arena, .{
                .file_path = null,
                .file_offset = 0,
                .meta_data = .{
                    .type = .INT64,
                    .encodings = .empty,
                    .path_in_schema = path_v,
                    .codec = .UNCOMPRESSED,
                    .num_values = 100,
                    .total_uncompressed_size = 800,
                    .total_compressed_size = 800,
                    .data_page_offset = 0,
                    .index_page_offset = null,
                    .dictionary_page_offset = null,
                },
            });
            try file_meta.row_groups.append(arena, .{
                .columns = columns,
                .total_byte_size = 1600,
                .num_rows = 100,
            });

            const in: AggRowGroup = .{
                .rg = &file_meta.row_groups.items[0],
                .file_index = 0,
                .rg_index = 0,
                .meta = &file_meta,
                .src = .{ .bytes = &.{}, .byte_origin = 0 },
                .filter = null,
                .read = &.{ true, true },
                .consumed = &.{ true, false },
            };

            var table: expr_agg.GroupTable = .init(arena, 1 << 20);
            var sketch: expr_agg.CountSketch = .{ .cells = try arena.alloc(u32, 16), .shift = 60 };
            @memset(sketch.cells, 0);
            table.sketch = &sketch;

            const left_expr = try arena.create(expr_ast.Expr);
            left_expr.* = .{ .col_ref = .{ .col_idx = 1, .physical_type = .INT64, .expr_type = .i64 } };
            const right_expr = try arena.create(expr_ast.Expr);
            right_expr.* = .{ .col_ref = .{ .col_idx = 1, .physical_type = .INT64, .expr_type = .i64 } };
            const arg_expr: expr_ast.Expr = .{
                .binop = .{
                    .op = .add,
                    .left = left_expr,
                    .right = right_expr,
                    .result_type = .i64,
                    .depth = 2,
                },
            };

            const calls = [_]expr_agg.AggCall{.{
                .func = .sum,
                .alias = "s",
                .result = .i64,
                .arg = arg_expr,
                .where = null,
            }};
            const keys = [_]expr_ast.Expr{.{ .col_ref = .{ .col_idx = 0, .physical_type = .INT64, .expr_type = .i64 } }};
            const target: AggTarget = .{ .grouped = .{
                .calls = &calls,
                .keys = &keys,
                .table = &table,
                .pos_base = 0,
            } };

            const sp = try StagePlan.init(arena, in, target, &.{false});
            try std.testing.expectEqual(@as(usize, 0), sp.calls.len);
        }
    }.run, .{});
}
