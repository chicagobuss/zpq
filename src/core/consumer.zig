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
const builtin = @import("builtin");
const owner_groups = @import("owner_groups.zig");
const msd_groups = @import("msd_groups.zig");
const retained_pages = @import("retained_pages.zig");
const nowMonoNs = @import("../clock.zig").monoNs;
const profile = @import("../profile.zig");

/// Zig's 16 MiB default made thread setup/teardown dominate short queries. Duplicated rather than imported from
/// scan.zig, which imports this module.
const WORKER_STACK_SIZE: usize = 1 << 20;
const spawn_util = @import("spawn.zig");
const blocking = @import("blocking.zig");
const schema = @import("schema.zig");
const filter_ast = @import("filter/ast.zig");
const filter_eval = @import("filter/eval.zig");
const filter_selection = @import("filter/selection.zig");
const filter_prune = @import("filter/prune.zig");
const expr_ast = @import("expr/ast.zig");
const expr_eval = @import("expr/eval.zig");
const expr_agg = @import("expr/agg.zig");
const core_memory = @import("memory.zig");
const agg_plan = @import("agg_plan.zig");
const encoder = @import("writer/encoder.zig");
const streaming = @import("writer/streaming.zig");
const fastpath = @import("writer/fastpath.zig");
const page_index = @import("writer/page_index.zig");
const thrift = @import("thrift.zig");
const column_mod = @import("parquet/column.zig");
const page_mod = @import("parquet/page.zig");
const Diag = @import("diag.zig").Diag;
const Text = @import("diag.zig").Text;
const decimal_mod = @import("parquet/decimal.zig");
const compression = @import("parquet/compression.zig");
const invariant = @import("invariant.zig");
const int96_mod = @import("parquet/int96.zig");
const metadata = @import("parquet/metadata.zig");
const statistics = @import("parquet/statistics.zig");
const header_writer = @import("writer/page_header.zig");
const WriterStatsPolicy = @import("parquet/writer_statistics.zig").Policy;
const agg_coded = @import("expr/agg_coded.zig");
const agg_dense = @import("expr/agg_dense.zig");
const partagg = @import("partagg.zig");
const huge_pages = @import("huge_pages.zig");

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
    /// A row output's rows read (in row groups not pruned or passed over) and kept by its filter, for the summary.
    /// Counted by the drivers, not by `ProjectedRG`, which a top-N runs twice over a row group it keeps rows of.
    rows_scanned: u64 = 0,
    rows_matched: u64 = 0,
    /// The accumulating thread's profile buffer; spans reuse the timestamps these counters are taken from. A Timings
    /// accumulates on one thread at a time, which is what makes handing it the buffer safe.
    prof: ?*profile.Thread = null,
};

/// The column chunk this thread last began to decode. A marker, not a scope: set as each chunk (or slice) starts, and
/// read only once a decode error has ended the query, so it names where that error was raised. A worker hands its own
/// to the thread that reports the error (`decodeFailure`, `setDecodeFailure`); `resolveDecodeFailure` turns it into
/// the file, row group, column path and page of the message.
const decode_order = @import("decode_order.zig");
pub const DecodeSite = decode_order.Site;
threadlocal var decode_site: ?DecodeSite = null;

/// Mark `column` of `rg` as this thread's chunk. Its page is unknown until a page begins, so a slice reader that
/// re-marks its column per slice, interleaved with other columns' readers, never reports another column's page.
pub fn enterChunk(rg: *const schema.RowGroup, column: usize) void {
    decode_site = .{ .rg = rg, .column = column };
    page_mod.last_page = null;
    page_mod.last_encoding = null;
}

/// This thread's decode site, with the page it last began, for a worker to hand over with its error.
pub fn decodeFailure() ?DecodeSite {
    var site = decode_site orelse return null;
    site.page = page_mod.last_page;
    site.encoding = page_mod.last_encoding;
    return site;
}

/// Adopt a worker's failure site as this thread's; null clears it (a query's start).
pub fn setDecodeFailure(site: ?DecodeSite) void {
    decode_site = site;
    page_mod.last_page = if (site) |s| s.page else null;
    page_mod.last_encoding = if (site) |s| s.encoding else null;
}

/// After a failure ended a query over `metas`, whose `inputs` each have a `name`: this thread's decode site as the
/// file, row group, leaf path and page of `diag`'s `decode` detail. Nothing over another detail, or for a site outside
/// these inputs (one left from an earlier query on the thread: compared by address only, never followed).
pub fn resolveDecodeFailure(diag: ?*Diag, metas: []const schema.FileMetaData, inputs: anytype) void {
    const d = diag orelse return;
    if (d.detail != .none) return;
    const site = decodeFailure() orelse return;
    // By the row group's address in a file's row-group list, which a decoder's copy of the footer shares.
    const at = @intFromPtr(site.rg);
    var file: usize = 0;
    const rg = while (file < metas.len) : (file += 1) {
        const rgs = metas[file].row_groups.items;
        const lo = @intFromPtr(rgs.ptr);
        if (at >= lo and at < lo + rgs.len * @sizeOf(schema.RowGroup)) break (at - lo) / @sizeOf(schema.RowGroup);
    } else return;
    var path: Text = .{};
    var leaves: schema.LeafIterator = .init(metas[file].schema.items);
    while (leaves.next()) |leaf| if (leaf.index == site.column) {
        for (leaf.path, 0..) |seg, i| {
            if (i > 0) path.append(".");
            path.append(seg);
        }
        break;
    };
    d.detail = .{ .decode = .{
        .input = inputs[file].name,
        .row_group = rg,
        .column = path,
        .page = site.page,
        .encoding = site.encoding,
    } };
}

/// Errors raised by reading a column chunk's pages, whose message names where: the corrupt or unsupported encodings
/// a page can hold, not the query's own failures (overflow, memory) that a decode site would mislabel.
pub fn isDecodeError(err: anyerror) bool {
    const names = [_][]const u8{
        "UnexpectedPage",       "DictionaryMissing",          "UnexpectedDictionaryPage", "ShortDecode",
        "DefLevelsMismatch",    "BadPageHeader",              "BadPageCount",             "DecompressionFailed",
        "SizeMismatch",         "NegativeSize",               "MissingChunkBytes",        "UnexpectedEndOfChunk",
        "LevelOutOfRange",      "IntOutOfRange",              "PageLargerThanChunk",      "UnsupportedEncoding",
        "UnsupportedPageType",  "PageValueCountExceedsChunk", "DictionaryLargerThanPage", "UnsupportedCodec",
        "InvalidColumnOffsets", "ColumnMetaMissing",
    };
    for (names) |n| if (std.mem.eql(u8, n, @errorName(err))) return true;
    return false;
}

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
            .passthrough => |pt| blk: {
                const kept_ci = pt.ci;
                // The leaf and its path come from the schema, which every chunk's path_in_schema matches (footer
                // open enforces it), so a file with no row groups still builds its output schema.
                var walk: schema.LeafIterator = undefined;
                const leaf = meta.leafAt(kept_ci, &walk) orelse return error.ColumnMetaMissing;
                const src_elem = leaf.element.*;
                paths[i] = try arena.dupe([]const u8, leaf.path);

                // INT96: exact epoch nanoseconds (`decimal.exactKind`) on the integer lane. The schema stays
                // INT96, whose re-encoding is refused before rows are written.
                if (src_elem.type == .INT96) {
                    schemas[i] = src_elem;
                    cols[i] = .{ .i128 = .{} };
                    continue;
                }

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
                break :blk schemas[i].type.?;
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
    // A named passthrough is its leaf renamed and flat (`OutputCol.Passthrough`).
    for (output_specs, schemas, paths) |spec, *elem, *path| if (spec == .passthrough) if (spec.passthrough.name) |name| {
        renameLeaf(elem, name, meta.getColumnLevels(path.*).max_def);
        const one = try arena.alloc([]const u8, 1);
        one[0] = name;
        path.* = one;
    };

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
    /// Per leaf: read by some computed output.
    computed_refs: []const bool,
    /// What computed outputs evaluate over, made by the first one projected (`survivors`).
    survived: ?Survivors = null,

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
            .passthrough => |pt| if (cbuf == .i128 and pt.ci < num_leaves) {
                lossless[pt.ci] = true;
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
            } else if (agg.keep_codes)
                // Codes even for a column computed outputs read: the filter runs on the dictionary, and only the
                // kept rows' values are looked up (`Survivors`).
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
            .computed_refs = computed_refs,
        };
    }

    fn survivors(self: *ProjectedRG) !*const Survivors {
        if (self.survived == null) self.survived = try Survivors.init(
            self.arena_state.allocator(),
            &self.batch,
            self.lookup,
            self.computed_refs,
            &self.sel,
            self.surviving,
        );
        return &self.survived.?;
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
            const kept_ci = spec.passthrough.ci;
            var late = std.heap.ArenaAllocator.init(gpa);
            defer late.deinit();
            const dcol = self.unscaled[kept_ci] orelse
                try decodeDecimalI128(late.allocator(), rg, meta, src, kept_ci, decode_options);
            const filtered_dec = try encoder.applySelectionI128(late.allocator(), dcol, &self.sel);
            const f = if (spec.passthrough.name != null) try flatLevels(i128, late.allocator(), filtered_dec) else filtered_dec;
            try appendTyped(i128, &agg.cols[i].i128, f.values, f.def_levels, f.max_def, agg.arena);
            return;
        }

        var late = std.heap.ArenaAllocator.init(gpa);
        defer late.deinit();
        const decoded: filter_eval.Batch.Column = switch (spec) {
            .passthrough => |pt| if (self.batch_pos_for_col[pt.ci]) |pos|
                self.batch.cols[pos]
            else if (pt.ci < self.fetch_set.len and self.fetch_set[pt.ci]) blk: {
                const t0 = nowMonoNs();
                const la = late.allocator();
                const decoded = if (agg.keep_codes)
                    try decodeWriteColumnMaybeCoded(la, rg, meta, src, pt.ci, decode_options, timings.prof)
                else
                    try decodeWriteColumn(la, rg, meta, src, pt.ci, decode_options, timings.prof);
                const t1 = nowMonoNs();
                timings.decode_ns += @intCast(t1 - t0);
                profile.record(timings.prof, .decode, t0, t1, profile.args(.{ 0, 0, 1 }));
                break :blk decoded;
            } else return error.MissingDecodedColumn,
            .computed => |c| {
                const s = try self.survivors();
                const col = try booleanLane(ra, agg.cols[i], try expr_eval.evalExpr(ra, &s.batch, self.lookup, c.expr));
                try appendSelected(&agg.cols[i], col, s.sel, self.surviving, agg.arena, false, late.allocator());
                return;
            },
        };
        const col = if (spec == .passthrough) try namedLevels(late.allocator(), spec.passthrough, decoded) else decoded;
        if (col.nullInfo().max_rep == 0) {
            try appendSelected(&agg.cols[i], col, &self.sel, self.surviving, agg.arena, agg.keep_codes, late.allocator());
            return;
        }
        try appendIntoBuf(&agg.cols[i], try encoder.applySelection(late.allocator(), col, &self.sel), agg.arena);
    }
};

/// The rows a filter kept, as computed outputs evaluate over them: the columns they read gathered down to those rows,
/// so no expression is computed for a row that is not written (a `value * 1.19` over 1% of a row group costs 1% of
/// one over all of it). `sel` then covers the gathered rows. A column that cannot be gathered row for row (nested)
/// leaves the whole batch, restricted to the kept rows (`Batch.sel`) as before: values computed for every row, faults
/// raised only for kept ones.
pub const Survivors = struct {
    batch: filter_eval.Batch,
    sel: *const filter_selection.SelectionVector,

    /// `refs` marks, by leaf, the columns computed outputs read (through `lookup`); others are left as they are.
    pub fn init(
        arena: std.mem.Allocator,
        batch: *const filter_eval.Batch,
        lookup: []const ?usize,
        refs: []const bool,
        sel: *const filter_selection.SelectionVector,
        surviving: usize,
    ) !Survivors {
        const cols = try arena.dupe(filter_eval.Batch.Column, batch.cols);
        // A column decoded as dictionary codes gets values here, for the rows kept only.
        if (surviving == batch.num_rows) {
            for (refs, 0..) |r, ci| {
                if (!r or ci >= lookup.len) continue;
                const pos = lookup[ci] orelse continue;
                cols[pos] = try agg_coded.materialize(arena, cols[pos]);
            }
            return .{ .batch = .{ .cols = cols, .num_rows = batch.num_rows }, .sel = sel };
        }
        for (refs, 0..) |r, ci| {
            if (!r or ci >= lookup.len) continue;
            const pos = lookup[ci] orelse continue;
            const gathered = try gatherRows(arena, batch.cols[pos], sel, surviving) orelse {
                if (batch.cols[pos].codes() != null) return error.MissingDecodedColumn;
                return .{ .batch = batch.restrictedTo(sel), .sel = sel };
            };
            cols[pos] = try agg_coded.materialize(arena, gathered);
        }
        const all = try arena.create(filter_selection.SelectionVector);
        all.* = try filter_selection.SelectionVector.init(arena, surviving);
        return .{ .batch = .{ .cols = cols, .num_rows = surviving }, .sel = all };
    }
};

/// `col`'s rows that `sel` keeps (`surviving` of them), with everything kept row for row moved alongside: levels,
/// dictionary codes and a DECIMAL's exact integers. Null for a column whose values are not one per row.
fn gatherRows(
    arena: std.mem.Allocator,
    col: filter_eval.Batch.Column,
    sel: *const filter_selection.SelectionVector,
    surviving: usize,
) !?filter_eval.Batch.Column {
    switch (col) {
        inline else => |c, tag| {
            if (c.max_rep > 0 or c.levels_only_rows != 0 or c.hist != null) return null;
            const T = @typeInfo(@TypeOf(c.values)).pointer.child;
            var out = c;
            if (!c.codesOnly()) {
                const vals = try arena.alloc(T, surviving);
                encoder.compactByMask(T, c.values, sel.mask, vals);
                out.values = vals;
            }
            if (c.codes) |k| {
                const codes = try arena.alloc(u32, surviving);
                encoder.compactByMask(u32, k, sel.mask, codes);
                out.codes = codes;
            }
            if (c.def_levels) |dl| {
                const levels = try arena.alloc(u32, surviving);
                encoder.compactByMask(u32, dl, sel.mask, levels);
                out.def_levels = levels;
                out.has_nulls = for (levels) |d| {
                    if (d < c.max_def) break true;
                } else false;
            }
            if (c.unscaled) |u| out.unscaled = switch (u) {
                inline else => |v, utag| blk: {
                    const U = @typeInfo(@TypeOf(v)).pointer.child;
                    const exact = try arena.alloc(U, surviving);
                    encoder.compactByMask(U, v, sel.mask, exact);
                    break :blk @unionInit(filter_eval.Exact, @tagName(utag), exact);
                },
            };
            return @unionInit(filter_eval.Batch.Column, @tagName(tag), out);
        },
    }
}

/// Decode column `ci` of a row group into the batch lane the write path keeps it in.
fn decodeWriteColumn(
    ra: std.mem.Allocator,
    rg: *const schema.RowGroup,
    meta: *const schema.FileMetaData,
    src: RGSrc,
    ci: usize,
    decode_options_in: DecodeOptions,
    prof: ?*profile.Thread,
) !filter_eval.Batch.Column {
    enterChunk(rg, ci);
    const num_rows: usize = @intCast(rg.num_rows);
    const chunk_span = profile.Span.begin(prof);
    const col = &rg.columns.items[ci];
    const cm = col.meta_data orelse return error.ColumnMetaMissing;
    const decode_options = column_mod.chunkOptions(decode_options_in, meta, cm);
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

    // DECIMAL columns, whatever their backing: the f64 view, with the exact unscaled values beside it that filters,
    // expressions and aggregates read (`decodeColumnExact`).
    const schema_elem = meta.getColumnSchema(cm.path_in_schema.items);
    const dec_kind: ?decimal_mod.Kind = if (schema_elem) |se| decimal_mod.exactKind(&se) else null;

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
        .INT96 => .{ .i64 = try int96_mod.decodeColumnAsI64Nanos(
            ra,
            chunk,
            cm.codec,
            levels,
            n_leaves,
            decode_options.page_bytes_limit,
        ) },
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
    var survived: ?Survivors = null;
    for (output_specs, 0..) |spec, i| {
        var rows = sel;
        var col: filter_eval.Batch.Column = switch (spec) {
            .passthrough => |pt| try namedLevels(scratch, pt, batch.cols[lookup[pt.ci] orelse return error.MissingDecodedColumn]),
            .computed => |c| blk: {
                if (survived == null) {
                    const refs = try scratch.alloc(bool, lookup.len);
                    @memset(refs, false);
                    for (output_specs) |o| if (o == .computed) o.computed.expr.collectColumns(refs);
                    survived = try Survivors.init(scratch, batch, lookup, refs, sel, surviving);
                }
                rows = survived.?.sel;
                break :blk try booleanLane(scratch, agg.cols[i], try expr_eval.evalExpr(scratch, &survived.?.batch, lookup, c.expr));
            },
        };
        // A DECIMAL written losslessly buffers its exact integers.
        if (agg.cols[i] == .i128 and col == .f64) {
            // Into a temporary first: assigned straight into `col`, the union's new tag lands before `col` is read.
            const exact = try expr_eval.unscaledView(scratch, col);
            col = .{ .i128 = exact };
        }
        try appendSelected(&agg.cols[i], col, rows, surviving, agg.arena, false, scratch);
    }
    agg.num_rows += surviving;
    return surviving;
}

/// A named passthrough's leaf as the flat column it is written as: present (`def == max_def`) or NULL, at one level.
/// A nested leaf counts its parents' levels too, which a flat leaf has none of. Others pass through as they are.
fn namedLevels(scratch: std.mem.Allocator, pt: OutputCol.Passthrough, col: filter_eval.Batch.Column) !filter_eval.Batch.Column {
    if (pt.name == null) return col;
    return switch (col) {
        inline else => |c, tag| @unionInit(filter_eval.Batch.Column, @tagName(tag), try flatLevels(@TypeOf(c.values[0]), scratch, c)),
    };
}

fn flatLevels(comptime T: type, scratch: std.mem.Allocator, c: filter_eval.ColumnT(T)) !filter_eval.ColumnT(T) {
    if (c.max_def <= 1 or c.max_rep > 0) return c;
    var out = c;
    out.max_def = 1;
    if (c.def_levels) |dl| {
        const flat = try scratch.alloc(u32, dl.len);
        for (dl, flat) |d, *f| f.* = @intFromBool(d == c.max_def);
        out.def_levels = flat;
    }
    return out;
}

/// `elem`, a kept leaf's schema element, as the flat leaf `name` that a renamed passthrough writes: the same type and
/// annotation, OPTIONAL exactly when the leaf can be NULL at its depth (`max_def`), whatever its own repetition says.
pub fn renameLeaf(elem: *schema.SchemaElement, name: []const u8, max_def: i32) void {
    elem.name = name;
    elem.num_children = 0;
    elem.repetition_type = if (max_def > 0) .OPTIONAL else .REQUIRED;
}

/// `col`, a computed output's values, in the BOOLEAN lane when its buffer `buf` is one: expressions evaluate a
/// BOOLEAN as 0/1 in the integer lane.
fn booleanLane(scratch: std.mem.Allocator, buf: OutputAggregator.ColumnBuf, col: filter_eval.Batch.Column) !filter_eval.Batch.Column {
    if (buf != .boolean or col != .i64) return col;
    const c = col.i64;
    const values = try scratch.alloc(bool, c.values.len);
    for (c.values, values) |v, *o| o.* = v != 0;
    return .{ .boolean = .{ .values = values, .def_levels = c.def_levels, .max_def = c.max_def, .has_nulls = c.has_nulls } };
}

/// `col`, a passthrough output's rows as `SliceReader.next` reads them, in the lane of the output buffer `buf`, as
/// `appendSliceRows` would write it: a DECIMAL written losslessly as its exact integers.
pub fn outputLane(
    buf: OutputAggregator.ColumnBuf,
    col: filter_eval.Batch.Column,
    scratch: std.mem.Allocator,
) !filter_eval.Batch.Column {
    if (buf == .i128 and col == .f64) return .{ .i128 = try expr_eval.unscaledView(scratch, col) };
    return col;
}

/// The dictionary of flat BYTE_ARRAY chunk `ci`, on `a`, when every one of its data pages is dictionary-encoded, so
/// that each of its values is an entry; null otherwise. Reads no data page's body.
pub fn chunkDictionary(
    a: std.mem.Allocator,
    rg: *const schema.RowGroup,
    meta: *const schema.FileMetaData,
    src: RGSrc,
    ci: usize,
) !?[]const []const u8 {
    const cm = rg.columns.items[ci].meta_data orelse return error.ColumnMetaMissing;
    if (cm.type != .BYTE_ARRAY) return null;
    const levels = meta.getColumnLevels(cm.path_in_schema.items);
    if (levels.max_rep > 0) return null;
    const start: usize = if (cm.dictionary_page_offset) |dp| @intCast(dp) else @intCast(cm.data_page_offset);
    const len: usize = @intCast(cm.total_compressed_size);
    invariant.assert(start >= @as(usize, @intCast(src.byte_origin)));
    const buf_off = start - @as(usize, @intCast(src.byte_origin));
    if (buf_off + len > src.bytes.len) return error.MissingChunkBytes;
    var r = column_mod.ColumnChunkReader([]const u8).initWithOptions(
        src.bytes[buf_off..][0..len],
        cm.codec,
        levels,
        a,
        column_mod.chunkOptions(.{}, meta, cm),
        null,
    );
    return r.dictionaryOfEveryPage();
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
    /// Set by `chargePages`: what the pages draw from, and what the pages on `held` drew.
    page_budget: ?*core_memory.SharedBudget = null,
    held_drawn: usize = 0,
    levels: schema.Levels,
    dec: ?decimal_mod.Kind,
    exact_only: bool = false,
    float16: bool,
    int96: bool,
    left: usize,
    /// Data pages `skip` passed over without decompressing them.
    pages_passed: usize = 0,
    /// Only the levels are read (see `levelsOnly`): a count(col)'s column, read as its whole-chunk path reads it.
    levels_only: bool = false,
    /// The chunk it reads, re-marked as each slice starts (`enterChunk`).
    site: DecodeSite,

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
        decode_options_in: DecodeOptions,
    ) !?SliceReader {
        if (!supports(rg, meta, ci)) return null;
        enterChunk(rg, ci);
        const cm = rg.columns.items[ci].meta_data orelse return error.ColumnMetaMissing;
        const decode_options = column_mod.chunkOptions(decode_options_in, meta, cm);
        const levels = meta.getColumnLevels(cm.path_in_schema.items);
        const se = meta.getColumnSchema(cm.path_in_schema.items);
        const dec: ?decimal_mod.Kind = if (se) |e| decimal_mod.exactKind(&e) else null;
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
                var r = Reader.initWithOptions(chunk, cm.codec, levels, a, decode_options, scratch);
                r.value_budget = @intCast(cm.num_values);
                break :blk @unionInit(Raw, tag, r);
            },
            .BYTE_ARRAY, .FIXED_LEN_BYTE_ARRAY, .INT96 => blk: {
                const Reader = column_mod.ColumnChunkReader([]const u8);
                // On `a` until the dictionary is in (`holdDictionary`), which then lives there without a copy.
                var r = Reader.initWithOptions(chunk, cm.codec, levels, a, decode_options, null);
                r.value_budget = @intCast(cm.num_values);
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
            .exact_only = decode_options.decimal_exact_only,
            .float16 = if (se) |e| schema.isFloat16(e) else false,
            .int96 = cm.type == .INT96,
            .left = @intCast(cm.num_values),
            .site = .{ .rg = rg, .column = ci },
        };
    }

    /// Payloads remain owned by the admitted aggregate after this reader's scratch is reset.
    pub fn retainPayloads(self: *SliceReader, owner: *retained_pages.Chunk) void {
        switch (self.raw.*) {
            .bytes => |*r| r.pages.retained = owner,
            else => unreachable,
        }
    }

    pub fn deinit(self: *SliceReader) void {
        self.scratch.deinit();
        self.pages.deinit();
        if (self.page_budget) |b| {
            b.release(self.held_drawn);
            switch (self.raw.*) {
                .bytes => |*r| b.release(r.pages.drawn),
                else => {},
            }
        }
    }

    /// Whether leaf `ci` has a sliced form (what `init` returns non-null for).
    pub fn supports(rg: *const schema.RowGroup, meta: *const schema.FileMetaData, ci: usize) bool {
        const cm = rg.columns.items[ci].meta_data orelse return false;
        return meta.getColumnLevels(cm.path_in_schema.items).max_rep == 0;
    }

    /// The next `n` rows (fewer only at the chunk's end), on `a`.
    /// Read only definition levels from here on: a column that only count(col) reads, which the whole-chunk path
    /// counts from its levels without decoding a value (`decodeLevelsOnly`). Sliced the same way, a value that would
    /// not decode fails the query, or does not, whatever the budget.
    pub fn levelsOnly(self: *SliceReader) void {
        // Nested or REQUIRED, the whole-chunk path decodes the values after all (`readAllLevels` declines).
        if (self.levels.max_def > 0 and self.levels.max_rep == 0) self.levels_only = true;
    }

    /// Fresh error probe: separate the retained byte dictionary from page storage before any page is installed.
    pub fn beginProbe(self: *SliceReader) void {
        switch (self.raw.*) {
            .bytes => |*r| {
                r.dictionary_arena = self.held;
                r.pages.dictionary_arena = self.held;
                r.arena = self.pages.allocator();
                r.pages.arena = self.pages.allocator();
                self.dict_held = true;
            },
            else => {},
        }
    }

    /// Between probe units no value refers to the drained page. Keep only its decoded dictionary.
    pub fn releaseProbePage(self: *SliceReader) void {
        invariant.assert(self.pageLeft() == 0);
        _ = self.pages.reset(.free_all);
        const s = self.scratch;
        s.gpa.free(s.page);
        s.gpa.free(s.def_levels);
        s.gpa.free(s.rep_levels);
        s.page = &.{};
        s.def_levels = &.{};
        s.rep_levels = &.{};
    }

    pub fn pageLeft(self: *const SliceReader) usize {
        return switch (self.raw.*) {
            inline else => |*r| r.pageValuesLeft(),
        };
    }

    pub fn pageOrdinal(self: *const SliceReader) ?u32 {
        return switch (self.raw.*) {
            inline else => |*r| if (r.pages.ordinal == 0) null else r.pages.ordinal - 1,
        };
    }

    pub fn next(self: *SliceReader, a: std.mem.Allocator, n_want: usize) !filter_eval.Batch.Column {
        enterChunk(self.site.rg, self.site.column);
        const n = @min(n_want, self.left);
        self.left -= n;
        if (n != 0 and !self.levels_only and self.dec == null and !self.float16 and !self.int96) {
            switch (self.raw.*) {
                inline .i32, .i64, .bytes => |*r| if (r.options.any_dictionary and r.options.encoded)
                    return self.nextCoded(@TypeOf(r.*).Value, r, a, n),
                else => {},
            }
        }
        if (self.levels_only) {
            const max_def: u32 = @intCast(self.levels.max_def);
            const def = try a.alloc(u32, n);
            var got: usize = 0;
            switch (self.raw.*) {
                inline else => |*r| while (got < n) {
                    // As `decodeBytes`: a byte-array reader's drained page goes back before the next comes in, or a
                    // row group's pages would pile up on `pages`.
                    if (r.pageValuesLeft() == 0) {
                        if (@TypeOf(r.*).Value == []const u8) {
                            self.resetPages();
                            if (!try r.nextPage()) break;
                            try self.holdDictionary(r);
                        } else if (!try r.nextPage()) break;
                        continue;
                    }
                    got += try r.readLevels(def[got..][0..@min(n - got, r.pageValuesLeft())]);
                },
            }
            if (got != n) return error.ShortDecode;
            return .{ .i64 = .{
                .values = &.{},
                .def_levels = def,
                .max_def = max_def,
                .has_nulls = std.mem.indexOfNone(u32, def, &.{max_def}) != null,
                .levels_only_rows = n,
            } };
        }
        switch (self.raw.*) {
            inline else => |*r, tag| {
                const T = @TypeOf(r.*).Value;
                const values = try a.alloc(T, n);
                // A proven all-present numeric page needs no per-slice levels either. Byte-array values keep
                // decodeBytes' page/dictionary lifetime handling when a window crosses page boundaries.
                const present = if (T != []const u8 and self.levels.max_def == 1)
                    try r.decodeAllPresent(values)
                else
                    0;
                const def: ?[]u32 = if (self.levels.max_def > 0 and present != n) try a.alloc(u32, n) else null;
                if (def) |d| @memset(d[0..present], @as(u32, @intCast(self.levels.max_def)));
                const got = present + if (T == []const u8)
                    try self.decodeBytes(r, a, values, def)
                else
                    try decodeSome(T, r, values[present..], if (def) |d| d[present..] else null);
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
                if (self.dec) |k| {
                    if (self.exact_only and (T == i32 or T == i64)) {
                        var exact = col;
                        exact.unscaled = @unionInit(filter_eval.Exact, @typeName(T), col.values);
                        return @unionInit(filter_eval.Batch.Column, @typeName(T), exact);
                    }
                    return .{ .f64 = try exactSlice(T, a, col, k) };
                }
                if (T == []const u8 and self.int96) {
                    const out = try a.alloc(i64, n);
                    for (values, out) |b, *o| o.* = if (b.len == int96_mod.INT96_WIDTH)
                        try int96_mod.int96ToEpochNanos(b)
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

    fn nextCoded(
        self: *SliceReader,
        comptime T: type,
        r: *column_mod.ColumnChunkReader(T),
        a: std.mem.Allocator,
        n: usize,
    ) !filter_eval.Batch.Column {
        r.options.fast_levels = true;
        const codes = try a.alloc(u32, n);
        const max_def: u32 = @intCast(self.levels.max_def);
        const def: ?[]u32 = if (max_def > 0) try a.alloc(u32, n) else null;
        var got: usize = 0;
        while (got < n) {
            if (r.pageValuesLeft() == 0) {
                if (T == []const u8) {
                    self.resetPages();
                    r.releaseExpansion();
                }
                if (!try r.nextPage()) return error.ShortDecode;
                if (T == []const u8) try self.holdDictionary(r);
                continue;
            }
            const want = @min(n - got, r.pageValuesLeft());
            if (!try r.readPageCodes(codes[got..][0..want], if (def) |d| d[got..][0..want] else null)) {
                const values = try a.alloc(T, n);
                const dict = r.dictionary;
                for (0..got) |i| values[i] = if (def != null and def.?[i] < max_def)
                    if (T == []const u8) "" else 0
                else
                    dict.?[codes[i]];
                r.options.fast_levels = false;
                const read = if (T == []const u8)
                    try self.decodeBytes(r, a, values[got..], if (def) |d| d[got..] else null)
                else
                    try decodeSome(T, r, values[got..], if (def) |d| d[got..] else null);
                if (read != n - got) return error.ShortDecode;
                const col = valuesCol(T, .{
                    .values = values,
                    .def_levels = def,
                    .has_nulls = r.has_nulls,
                }, self.levels);
                return @unionInit(filter_eval.Batch.Column, if (T == []const u8) "string" else @typeName(T), col);
            }
            got += want;
        }
        const dict = r.dictionary orelse return error.DictionaryMissing;
        const col = codedCol(T, .{ .codes = codes, .def_levels = def, .has_nulls = r.has_nulls }, dict, self.levels);
        return @unionInit(filter_eval.Batch.Column, if (T == []const u8) "string" else @typeName(T), col);
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
                self.resetPages();
                r.releaseExpansion();
                if (!try r.nextPage()) break;
                try self.holdDictionary(r);
                continue;
            }
            const want = @min(values.len - got, r.pageValuesLeft());
            const dst = values[got..][0..want];
            const k = if (def) |d| try r.decodeWithLevels(dst, d[got..][0..want]) else try r.decode(dst);
            if (k != want) return error.ShortDecode;
            if (r.pages.retained) |owner| {
                for (dst) |*value| if (!owner.contains(value.*)) {
                    // Expanded strings have a different capacity bound. Refuse the retained layout rather than
                    // misreporting its ownership limit as corrupt input; the rescannable scan retries normally.
                    return error.RetainedEncodingUnsupported;
                };
            } else _ = try copyBytes(a, dst);
            got += k;
        }
        return got;
    }

    /// A byte-array chunk reads on `held` until its dictionary is in, which so lives as long as the reader without a
    /// copy (a copy off `pages` held it twice while it was made); its pages after that read on `pages`. The
    /// dictionary page comes first, so once a data page is in there is none to come.
    fn holdDictionary(self: *SliceReader, r: *column_mod.ColumnChunkReader([]const u8)) !void {
        if (self.dict_held) return;
        self.dict_held = r.dictionary != null or r.data_page_seen;
        if (!self.dict_held) return;
        r.arena = self.pages.allocator();
        r.pages.arena = self.pages.allocator();
        // Under `chargePages`, what is on `held` stays drawn until `deinit`: its pages, and the dictionary's slices
        // (drawn as it was installed).
        if (r.pages.budget != null) {
            self.held_drawn = r.pages.drawn;
            r.pages.drawn = 0;
        }
    }

    /// A byte-array chunk's pages on `raw`, each drawn from `budget` before it is decompressed (its inflated size)
    /// and given back as `pages` is reset, rather than on an arena that draws a node half again past each one. Right
    /// after `init`, before any page is read.
    pub fn chargePages(self: *SliceReader, raw: std.mem.Allocator, budget: *core_memory.SharedBudget) void {
        switch (self.raw.*) {
            .bytes => |*r| {
                self.pages.* = .init(raw);
                r.pages.budget = budget;
                self.page_budget = budget;
            },
            else => {},
        }
    }

    /// Reset `pages`, giving back what its pages drew.
    fn resetPages(self: *SliceReader) void {
        _ = self.pages.reset(.retain_capacity);
        if (self.page_budget) |b| switch (self.raw.*) {
            .bytes => |*r| {
                b.release(r.pages.drawn);
                r.pages.drawn = 0;
            },
            else => {},
        };
    }

    /// Pass over the next `n` rows. A data page whose rows all fall in the skip is passed over by its header, never
    /// decompressed; within a page the rows decode onto `a` and are dropped. Lets a few rows of a row group be read
    /// without decoding the pages between them.
    pub fn skip(self: *SliceReader, a: std.mem.Allocator, n_want: usize) !void {
        const n = @min(n_want, self.left);
        self.left -= n;
        switch (self.raw.*) {
            inline else => |*r| {
                const T = @TypeOf(r.*).Value;
                var left = n;
                var discard: []T = &.{};
                var discard_def: []u32 = &.{};
                while (left > 0) {
                    if (r.pageValuesLeft() == 0) {
                        if (T == []const u8) {
                            self.resetPages();
                            r.releaseExpansion();
                        }
                        const passed = try r.passPageWithin(left);
                        if (T == []const u8) try self.holdDictionary(r);
                        if (passed) |rows| {
                            left -= rows;
                            self.pages_passed += 1;
                            continue;
                        }
                        if (!try r.nextPage()) return error.ShortDecode;
                        if (T == []const u8) try self.holdDictionary(r);
                        continue;
                    }
                    const k = @min(left, r.pageValuesLeft(), 4096);
                    if (discard.len < k) {
                        discard = try a.alloc(T, k);
                        if (self.levels.max_def > 0) discard_def = try a.alloc(u32, k);
                    }
                    const got = if (self.levels.max_def > 0)
                        try r.decodeWithLevels(discard[0..k], discard_def[0..k])
                    else
                        try r.decode(discard[0..k]);
                    if (got != k) return error.ShortDecode;
                    left -= k;
                }
            },
        }
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
                    if (k.physical == .INT96) {
                        if (b.len != int96_mod.INT96_WIDTH) return error.UnexpectedPage;
                        x.* = int96_mod.int96ToNanos(b);
                        o.* = @floatFromInt(x.*);
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
    return decimal_mod.exactKind(&se);
}

fn decodeDecimalI128(
    ra: std.mem.Allocator,
    rg: *const schema.RowGroup,
    meta: *const schema.FileMetaData,
    src: RGSrc,
    kept_ci: usize,
    decode_options_in: DecodeOptions,
) !filter_eval.ColumnT(i128) {
    enterChunk(rg, kept_ci);
    const cm = rg.columns.items[kept_ci].meta_data orelse return error.ColumnMetaMissing;
    const decode_options = column_mod.chunkOptions(decode_options_in, meta, cm);
    const start: usize = if (cm.dictionary_page_offset) |dp| @intCast(dp) else @intCast(cm.data_page_offset);
    const len: usize = @intCast(cm.total_compressed_size);
    invariant.assert(start >= @as(usize, @intCast(src.byte_origin)));
    const buf_off = start - @as(usize, @intCast(src.byte_origin));
    if (buf_off + len > src.bytes.len) return error.MissingChunkBytes;
    const chunk = src.bytes[buf_off .. buf_off + len];

    const levels = meta.getColumnLevels(cm.path_in_schema.items);
    const n_leaves: usize = @intCast(cm.num_values);
    const se = meta.getColumnSchema(cm.path_in_schema.items) orelse return error.SchemaLookupFailed;
    const kind = decimal_mod.exactKind(&se) orelse return error.SchemaLookupFailed;
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
        // Codes only when the dictionary, copied with them, is no bigger than the rows: a selective filter over a
        // chunk of mostly distinct values would otherwise copy its whole dictionary for a few rows.
        const dict_len = switch (src) {
            inline else => |c| c.dict.len,
        };
        if (keep_codes and fresh and dict_len <= surviving) return appendSelectedCodes(buf, src, sel, surviving, arena);
        src = try agg_coded.materialize(scratch, src);
    }
    switch (buf.*) {
        inline else => |*tb| if (tb.coded) try materializeBuf(tb, arena),
    }
    switch (src) {
        inline else => |c, tag| {
            const T = @TypeOf(c.values[0]);
            const tb = &@field(buf.*, @tagName(tag));
            const before = tb.values.items.len;
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
            // Levels stay one per row however the batches come: a computed column is NULL-free in one row group (no
            // levels, the vector kernels' result) and NULL in the next, so a batch without levels appends present
            // rows to a buffer that has them, and a buffer gaining levels first gives its earlier rows theirs.
            const max_def = @max(c.max_def, tb.max_def);
            if (max_def == 0) return;
            if (tb.max_def == 0) {
                try tb.def_levels.ensureTotalCapacityPrecise(arena, before);
                @memset(tb.def_levels.addManyAsSliceAssumeCapacity(before), max_def);
            } else if (tb.max_def != max_def) for (tb.def_levels.items) |*d| {
                if (d.* == tb.max_def) d.* = max_def;
            };
            tb.max_def = max_def;
            try tb.def_levels.ensureTotalCapacityPrecise(arena, tb.def_levels.items.len + surviving);
            const dl_out = tb.def_levels.addManyAsSliceAssumeCapacity(surviving);
            if (c.def_levels) |dl| {
                encoder.compactByMask(u32, dl, sel.mask, dl_out);
                // A flat column is present at its own max; carry it to the buffer's.
                if (c.max_def != max_def) for (dl_out) |*d| {
                    if (d.* == c.max_def) d.* = max_def;
                };
            } else @memset(dl_out, max_def);
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
    decode_options_in: DecodeOptions,
    prof: ?*profile.Thread,
) !filter_eval.Batch.Column {
    enterChunk(rg, ci);
    const cm = rg.columns.items[ci].meta_data orelse return error.ColumnMetaMissing;
    const decode_options = column_mod.chunkOptions(decode_options_in, meta, cm);
    const levels = meta.getColumnLevels(cm.path_in_schema.items);
    const se = meta.getColumnSchema(cm.path_in_schema.items);
    const converted = if (se) |e| decimal_mod.exactKind(&e) != null or schema.isFloat16(e) else false;
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
            try placer.place(enc, &agg.schema_elems[i]);
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
        for (results, agg.schema_elems) |maybe_enc, *elem| {
            try placer.place(maybe_enc orelse return error.MissingEncodeResult, elem);
        }
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
    } else try encoder.encodeColumnSegments(arena, .{
        .values = try aggBatchColumn(agg, i, arena),
        .schema_elem = &agg.schema_elems[i],
        .path_in_schema = agg.paths[i],
        .codec = output_codec,
        .zstd = zstd,
        .bloom_fpp = bloom_write.at(i),
        .dictionary_bloom = bloom_write.dictionary_bloom,
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
        try placer.place(enc, &agg.schema_elems[i]);
    }

    pub fn finish(self: *RGEncoder, num_rows: usize) RGOut {
        if (num_rows == 0) return .{ .surviving_rows = 0, .rg = null };
        return .{
            .surviving_rows = @intCast(num_rows),
            .rg = .{ .columns = self.columns, .total_byte_size = self.total, .num_rows = @intCast(num_rows) },
        };
    }
};

/// Rows `lo..hi` of an output buffer as the column the encoder takes: its values, or its codes into the dictionary
/// they index, and its levels; an exact DECIMAL as its unscaled integers (the encoder narrows them to the schema's
/// width).
pub fn bufRows(buf: OutputAggregator.ColumnBuf, lo: usize, hi: usize) filter_eval.Batch.Column {
    return switch (buf) {
        inline else => |b, tag| @unionInit(filter_eval.Batch.Column, @tagName(tag), .{
            .values = if (b.coded) &.{} else b.values.items[lo..hi],
            .def_levels = if (b.max_def > 0) b.def_levels.items[lo..hi] else null,
            .max_def = b.max_def,
            .has_nulls = b.max_def > 0,
            .codes = if (b.coded) b.codes.items[lo..hi] else null,
            .dict = b.dict,
        }),
    };
}

/// Append the rows `sel` keeps of `src` to `buf`, copying them onto `arena` (`appendSelected`, never keeping codes).
pub fn appendRows(
    buf: *OutputAggregator.ColumnBuf,
    src: filter_eval.Batch.Column,
    sel: *const filter_selection.SelectionVector,
    surviving: usize,
    arena: std.mem.Allocator,
    scratch: std.mem.Allocator,
) !void {
    return appendSelected(buf, src, sel, surviving, arena, false, scratch);
}

/// Rows `lo..hi` of one output buffer.
pub const BufRange = struct {
    buf: *const OutputAggregator.ColumnBuf,
    lo: usize,
    hi: usize,
};

/// The rows of `ranges`, in order, as one column (`bufRows` of each, joined on `arena`). Ranges of buffers coded by
/// one dictionary stay codes; otherwise values, a coded range's taken from its dictionary. Levels run at the deepest
/// range's `max_def`, a range without levels present throughout. Which of these a column comes out as never changes
/// its encoding: coded and plain columns, with or without all-present levels, encode to the same bytes.
pub fn joinRows(arena: std.mem.Allocator, ranges: []const BufRange) !filter_eval.Batch.Column {
    std.debug.assert(ranges.len > 0);
    if (ranges.len == 1) return bufRows(ranges[0].buf.*, ranges[0].lo, ranges[0].hi);
    switch (ranges[0].buf.*) {
        inline else => |first, tag| {
            const TB = @TypeOf(first);
            const T = @typeInfo(@TypeOf(first.dict)).pointer.child;
            var total: usize = 0;
            var max_def: u32 = 0;
            var one_dict = first.coded;
            for (ranges) |r| {
                const b = @field(r.buf.*, @tagName(tag));
                total += r.hi - r.lo;
                max_def = @max(max_def, b.max_def);
                one_dict = one_dict and b.coded and b.dict.ptr == first.dict.ptr and b.dict.len == first.dict.len;
            }
            var out: filter_eval.ColumnT(T) = .{ .values = &.{}, .max_def = max_def, .has_nulls = max_def > 0 };
            if (one_dict) {
                const codes = try arena.alloc(u32, total);
                var pos: usize = 0;
                for (ranges) |r| {
                    const b: *const TB = &@field(r.buf.*, @tagName(tag));
                    @memcpy(codes[pos..][0 .. r.hi - r.lo], b.codes.items[r.lo..r.hi]);
                    pos += r.hi - r.lo;
                }
                out.codes = codes;
                out.dict = first.dict;
            } else {
                const values = try arena.alloc(T, total);
                var pos: usize = 0;
                for (ranges) |r| {
                    const b: *const TB = &@field(r.buf.*, @tagName(tag));
                    const dst = values[pos..][0 .. r.hi - r.lo];
                    if (b.coded) {
                        for (dst, b.codes.items[r.lo..r.hi], r.lo..) |*v, k, row| {
                            const present = b.max_def == 0 or b.def_levels.items[row] == b.max_def;
                            v.* = if (present) b.dict[k] else std.mem.zeroes(T);
                        }
                    } else @memcpy(dst, b.values.items[r.lo..r.hi]);
                    pos += r.hi - r.lo;
                }
                out.values = values;
            }
            if (max_def > 0) {
                const levels = try arena.alloc(u32, total);
                var pos: usize = 0;
                for (ranges) |r| {
                    const b: *const TB = &@field(r.buf.*, @tagName(tag));
                    const dst = levels[pos..][0 .. r.hi - r.lo];
                    if (b.max_def == 0) {
                        @memset(dst, max_def);
                    } else {
                        @memcpy(dst, b.def_levels.items[r.lo..r.hi]);
                        if (b.max_def != max_def) for (dst) |*d| {
                            if (d.* == b.max_def) d.* = max_def;
                        };
                    }
                    pos += r.hi - r.lo;
                }
                out.def_levels = levels;
            }
            return @unionInit(filter_eval.Batch.Column, @tagName(tag), out);
        },
    }
}

/// The PLAIN size, in bits, of the present values in rows `lo..hi` of an output buffer written as `elem`: its physical
/// width (a BOOLEAN one bit, a FIXED_LEN_BYTE_ARRAY its length), a BYTE_ARRAY 4 bytes plus its length. What output row
/// groups are cut by: a property of the rows, however they were decoded or batched.
pub fn plainBits(buf: *const OutputAggregator.ColumnBuf, elem: *const schema.SchemaElement, lo: usize, hi: usize) u64 {
    const fixed: ?u64 = switch (elem.type orelse .BYTE_ARRAY) {
        .BOOLEAN => 1,
        .INT32, .FLOAT => 32,
        .INT64, .DOUBLE => 64,
        .INT96 => 96,
        .FIXED_LEN_BYTE_ARRAY => @as(u64, @intCast(@max(0, elem.type_length orelse 0))) * 8,
        .BYTE_ARRAY => null,
    };
    switch (buf.*) {
        inline else => |b| {
            const levels: ?[]const u32 = if (b.max_def > 0) b.def_levels.items[lo..hi] else null;
            if (fixed) |w| return w * encoder.countPresentRows(hi - lo, levels, b.max_def);
            if (comptime @TypeOf(b.dict) != []const []const u8) {
                return 0;
            } else {
                var bits: u64 = 0;
                for (lo..hi) |r| {
                    if (levels) |l| if (l[r - lo] != b.max_def) continue;
                    const v = if (b.coded) b.dict[b.codes.items[r]] else b.values.items[r];
                    bits += (4 + @as(u64, v.len)) * 8;
                }
                return bits;
            }
        },
    }
}

/// Per row, the PLAIN bits of every row before it (`plainBits` over all of `agg`'s columns, summed): `n + 1` running
/// totals, so the bits of rows `lo..hi` are `prefix[hi] - prefix[lo]` and the row a byte target falls in is found by
/// search. The sums are `plainBits`'s exactly.
pub fn plainBitsPrefix(arena: std.mem.Allocator, agg: *const OutputAggregator) ![]u64 {
    const n = agg.num_rows;
    const prefix = try arena.alloc(u64, n + 1);
    @memset(prefix, 0);
    // Fixed-width columns without NULLs take the same bits every row: added once per row in the running sum.
    var per_row: u64 = 0;
    for (agg.cols, agg.schema_elems) |*buf, *elem| {
        const fixed: ?u64 = switch (elem.type orelse .BYTE_ARRAY) {
            .BOOLEAN => 1,
            .INT32, .FLOAT => 32,
            .INT64, .DOUBLE => 64,
            .INT96 => 96,
            .FIXED_LEN_BYTE_ARRAY => @as(u64, @intCast(@max(0, elem.type_length orelse 0))) * 8,
            .BYTE_ARRAY => null,
        };
        switch (buf.*) {
            inline else => |b| {
                if (fixed) |w| if (b.max_def == 0) {
                    per_row += w;
                    continue;
                };
                for (0..n) |r| {
                    if (b.max_def > 0 and b.def_levels.items[r] != b.max_def) continue;
                    prefix[r + 1] += fixed orelse if (comptime @TypeOf(b.dict) != []const []const u8) 0 else blk: {
                        const v = if (b.coded) b.dict[b.codes.items[r]] else b.values.items[r];
                        break :blk (4 + @as(u64, v.len)) * 8;
                    };
                }
            },
        }
    }
    for (1..n + 1) |r| prefix[r] += prefix[r - 1] + per_row;
    return prefix;
}

/// An encoded chunk placed at the sink's offset `out_offset.*`: its metadata copied into `out_arena` with offsets made
/// absolute, and its pages (offsets still relative to the chunk) copied there too, for its page index.
pub fn placeChunk(
    out_arena: std.mem.Allocator,
    sink: streaming.Sink,
    out_offset: *u64,
    enc: encoder.EncodedColumn,
) !struct { chunk: schema.ColumnChunk, pages: []const encoder.PageInfo } {
    const start: i64 = @intCast(out_offset.*);
    var em = try cloneColumnMeta(out_arena, enc.meta);
    em.data_page_offset += start;
    if (em.dictionary_page_offset) |dpo| em.dictionary_page_offset = dpo + start;
    const pages = try out_arena.dupe(encoder.PageInfo, enc.pages);
    for (pages) |*p| {
        if (p.min) |v| p.min = try out_arena.dupe(u8, v);
        if (p.max) |v| p.max = try out_arena.dupe(u8, v);
    }
    try enc.writeTo(sink);
    out_offset.* += enc.byteLen();
    return .{
        .chunk = .{
            .file_path = null,
            .file_offset = start,
            .meta_data = em,
            .bloom_out = if (enc.bloom.len > 0) try out_arena.dupe(u8, enc.bloom) else &.{},
        },
        .pages = pages,
    };
}

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

    fn place(self: ChunkPlacer, enc: encoder.EncodedColumn, elem: *const schema.SchemaElement) !void {
        const col_start_in_file: i64 = @intCast(self.out_offset.*);
        var em = try cloneColumnMeta(self.out_arena, enc.meta);
        em.data_page_offset += col_start_in_file;
        if (em.dictionary_page_offset) |dpo| em.dictionary_page_offset = dpo + col_start_in_file;
        const t_sink_start = nowMonoNs();
        try enc.writeTo(self.sink);
        const t_sink_end = nowMonoNs();
        self.timings.sink_ns += @intCast(t_sink_end - t_sink_start);
        profile.record(self.timings.prof, .sink_write, t_sink_start, t_sink_end, profile.args(.{enc.byteLen()}));
        self.out_offset.* += enc.byteLen();
        self.total.* += enc.meta.total_uncompressed_size;

        var chunk: schema.ColumnChunk = .{
            .file_path = null,
            .file_offset = col_start_in_file,
            .meta_data = em,
            // Placed after the last row group, so it must outlive this row group's arena and the encoder's.
            .bloom_out = if (enc.bloom.len > 0) try self.out_arena.dupe(u8, enc.bloom) else &.{},
        };
        // Straight to a file the chunk's page index can follow it, where its pages' offsets are final.
        if (self.emit_page_index) {
            var so: streaming.SinkOut = .{ .sink = self.sink, .offset = @intCast(self.out_offset.*) };
            try page_index.writeInline(self.out_arena, &so, &chunk, enc.pages, elem);
            self.out_offset.* = so.offset;
        }
        try self.columns.append(self.out_arena, chunk);
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
        ctx.results[i] = encoder.encodeColumnSegments(ctx.arena, .{
            .values = ctx.batch_cols[i],
            .schema_elem = &ctx.schema_elems[i],
            .path_in_schema = ctx.paths[i],
            .codec = ctx.codec,
            .zstd = ctx.zstd,
            .bloom_fpp = ctx.bloom_write.at(i),
            .dictionary_bloom = ctx.bloom_write.dictionary_bloom,
        }) catch |err| {
            ctx.err = err;
            return;
        };
    }
}

/// The output leaf for a computed column. Shared by the row buffers (whose printing reads it) and the footer.
pub fn computedSchemaElem(c: OutputCol.Computed) schema.SchemaElement {
    const expr_type = c.expr.typeOf();
    // A computed column that can be NULL (`strlen` of a nullable column) must say so, or its NULLs misdescribe it.
    const repetition: schema.FieldRepetitionType = if (c.expr.mayBeNull() and c.expr != .col_ref) .OPTIONAL else .REQUIRED;
    if (expr_type == .dec) {
        var elem: schema.SchemaElement = .{
            .type = null,
            .type_length = null,
            .repetition_type = repetition,
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
    if (c.source) |src| if (c.expr == .col_ref and !schema.isFloat16(src) and (src.type == .BYTE_ARRAY or
        src.type == .FIXED_LEN_BYTE_ARRAY or src.type == .INT64 or src.type == .DOUBLE or src.type == .BOOLEAN))
    {
        var renamed = src;
        renamed.name = c.alias;
        renamed.num_children = 0;
        return renamed;
    };
    // An aliased UINT64 reference carries the column's raw bits through the i64 lane; keep the annotation so they
    // are printed and written as the unsigned values they are.
    const unsigned_64 = c.expr == .col_ref and c.expr.col_ref.unsigned_64;
    return .{
        // A BOOLEAN expression's 0/1 lane is written and printed as the BOOLEAN SQL types it.
        .type = if (c.expr.isBoolean()) .BOOLEAN else expr_type.toParquet(),
        .type_length = null,
        .repetition_type = repetition,
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
    passthrough: Passthrough,
    computed: Computed,

    /// A kept input leaf, written as it is. With `name` (a SELECT alias, or a nested leaf's dotted label) it is the
    /// same leaf renamed and flat: its type, annotation and NULLs unchanged, its definition levels mapped to 0/1.
    pub const Passthrough = struct {
        ci: usize,
        name: ?[]const u8 = null,
    };

    pub const Computed = struct {
        expr: expr_ast.Expr,
        /// Output column name. The caller's footer-build step uses
        /// the same string as the leaf SchemaElement's name.
        alias: []const u8,
        /// A renamed reference retains its source annotation when the value lane is unchanged.
        source: ?schema.SchemaElement = null,
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
            .is_max_value_exact = st.is_max_value_exact,
            .is_min_value_exact = st.is_min_value_exact,
            .nan_count = st.nan_count,
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
        .type_length = out_col.type_length,
        .repetition_type = if (out_col.nullable) .OPTIONAL else .REQUIRED,
        .name = out_col.name,
        .num_children = 0,
        .converted_type = out_col.converted_type,
        .logical_type = out_col.logical_type,
        .logical_type_raw = out_col.logical_type_raw,
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
    elem.logical_type_raw = &.{};
    elem.logical_type = .{ .DECIMAL = .{ .scale = d.scale, .precision = d.precision } };
    elem.scale = d.scale;
    elem.precision = d.precision;
}

/// Rows from which `writeAggregateRows` encodes columns on several threads: fewer encode faster than threads start.
const parallel_encode_rows = 1 << 16;

/// Rows per row group of a written answer. A bigger answer is cut at these fixed boundaries, never by `-j` or memory,
/// so its row groups' columns encode on every worker and its readers can split it the same way; a smaller one is the
/// single row group it always was.
pub const answer_rg_rows: usize = 1 << 17;

/// Rows `lo..hi` of an answer column: its values and levels, nothing else (an answer column is never coded,
/// repeated or carrying a decimal's exact view).
fn sliceOutputCol(col: expr_agg.OutputCol, lo: usize, hi: usize) expr_agg.OutputCol {
    var out = col;
    switch (out.col) {
        inline else => |*c| {
            std.debug.assert(c.codes == null and c.rep_levels == null and c.unscaled == null);
            c.values = c.values[lo..hi];
            if (c.def_levels) |d| {
                c.def_levels = d[lo..hi];
                c.has_nulls = std.mem.indexOfScalar(u32, d[lo..hi], 0) != null;
            }
        },
    }
    return out;
}

fn encodeAggOutputCol(
    arena: std.mem.Allocator,
    out_col: expr_agg.OutputCol,
    leaf_elem: *const schema.SchemaElement,
    codec: schema.CompressionCodec,
    zstd: compression.ZstdPlan,
    dictionary_bloom: bool,
) !encoder.EncodedColumn {
    const path_buf = try arena.alloc([]const u8, 1);
    path_buf[0] = out_col.name;
    return encoder.encodeColumnSegments(arena, .{
        .values = out_col.col,
        .schema_elem = leaf_elem,
        .path_in_schema = path_buf,
        .codec = codec,
        .zstd = zstd,
        .distinct = out_col.distinct,
        .dictionary_bloom = dictionary_bloom,
    });
}

/// How `writeAggregateRows` may encode a big answer's columns together: on up to `workers` threads (the query's), with
/// the columns in flight at once charged at most `budget` bytes (`aggEncodeCharge`). The default encodes them one at
/// a time, as every column of a small answer is.
pub const AggEncode = struct {
    dictionary_bloom: bool = true,
    workers: usize = 1,
    budget: u64 = 0,
    /// The query's ledger: what the columns in flight hold is drawn from it before they encode, a wave narrowing to
    /// what it takes, and a column it cannot take alone refused. Null: the waves' `budget` alone, and a column alone
    /// uncharged.
    ledger: ?*expr_agg.SharedBudget = null,
};

/// `meta` with everything it points to copied into `arena`, every other field as it is: it outlives the wave arena
/// its encoding was made in. Unlike `cloneColumnMeta`, which builds a copy field by field, a field the encoder sets in
/// the future cannot be lost here.
fn ownColumnMeta(arena: std.mem.Allocator, meta: schema.ColumnMetaData) !schema.ColumnMetaData {
    var out = meta;
    out.encodings = .empty;
    try out.encodings.appendSlice(arena, meta.encodings.items);
    out.path_in_schema = .empty;
    try out.path_in_schema.appendSlice(arena, meta.path_in_schema.items);
    if (meta.statistics) |st| {
        var copy = st;
        inline for (.{ "max", "min", "max_value", "min_value" }) |f| {
            if (@field(st, f)) |v| @field(copy, f) = try arena.dupe(u8, v);
        }
        out.statistics = copy;
    }
    if (meta.encoding_stats) |es| out.encoding_stats = try es.clone(arena);
    return out;
}

/// What encoding `col` may hold at its peak: three times its values and levels, the estimate the re-encode path
/// slices row groups by (a dictionary attempt's table and indices, the page, its compressed copy).
fn aggEncodeCharge(col: expr_agg.OutputCol) u64 {
    const bytes: u64 = switch (col.col) {
        .string => |c| blk: {
            var n: u64 = c.values.len * @sizeOf([]const u8);
            for (c.values) |v| n += v.len;
            break :blk n + if (c.def_levels) |d| d.len * @sizeOf(u32) else 0;
        },
        inline else => |c| c.values.len * @sizeOf(@TypeOf(c.values[0])) +
            if (c.def_levels) |d| d.len * @sizeOf(u32) else 0,
    };
    return bytes *| 3;
}

/// A run of an answer's columns encoded together, one thread a column, each into an arena of its own that holds the
/// encoding until `reset`.
const AggColumnWave = struct {
    arenas: []std.heap.ArenaAllocator,
    results: []anyerror!encoder.EncodedColumn,
    tasks: []Task,

    const Task = struct {
        cols: []const expr_agg.OutputCol = &.{},
        arenas: []std.heap.ArenaAllocator,
        results: []anyerror!encoder.EncodedColumn,
        codec: schema.CompressionCodec,
        zstd: compression.ZstdPlan,
        cursor: *std.atomic.Value(usize),
        dictionary_bloom: bool,

        /// Claim columns until none remain: one a failed spawn left unclaimed is taken by a thread that did start.
        fn run(t: *Task) void {
            while (true) {
                const i = t.cursor.fetchAdd(1, .monotonic);
                if (i >= t.cols.len) return;
                const elem = aggLeafElem(t.cols[i]);
                t.results[i] = encodeAggOutputCol(
                    t.arenas[i].allocator(),
                    t.cols[i],
                    &elem,
                    t.codec,
                    t.zstd,
                    t.dictionary_bloom,
                );
            }
        }
    };

    /// Room for waves of up to `width` columns, taken from `arena` before anything is written, so a wave allocates
    /// nothing there.
    fn init(arena: std.mem.Allocator, width: usize) !AggColumnWave {
        const arenas = try arena.alloc(std.heap.ArenaAllocator, width);
        for (arenas) |*a| a.* = .init(std.heap.page_allocator);
        return .{
            .arenas = arenas,
            .results = try arena.alloc(anyerror!encoder.EncodedColumn, width),
            .tasks = try arena.alloc(Task, width),
        };
    }

    /// Encode `cols`, at most `arenas.len` of them, one thread a column.
    fn run(
        w: *AggColumnWave,
        cols: []const expr_agg.OutputCol,
        codec: schema.CompressionCodec,
        zstd: compression.ZstdPlan,
        dictionary_bloom: bool,
    ) void {
        std.debug.assert(cols.len <= w.arenas.len);
        var cursor = std.atomic.Value(usize).init(0);
        const tasks = w.tasks[0..cols.len];
        for (tasks) |*t| t.* = .{
            .cols = cols,
            .arenas = w.arenas,
            .results = w.results,
            .codec = codec,
            .zstd = zstd,
            .cursor = &cursor,
            .dictionary_bloom = dictionary_bloom,
        };
        if (!spawn_util.runTree(Task, tasks, .{ .stack_size = WORKER_STACK_SIZE }, Task.run)) Task.run(&tasks[0]);
    }

    /// Drop what the last wave's columns hold, keeping the pages for the next wave: what is kept is at most what one
    /// wave held, which the budget admitted.
    fn reset(w: *AggColumnWave) void {
        for (w.arenas) |*a| _ = a.reset(.retain_capacity);
    }

    fn deinit(w: *AggColumnWave) void {
        for (w.arenas) |*a| a.deinit();
    }
};

/// Write an aggregate result — one row, or one per GROUP BY group — as a
/// parquet file with one column-chunk per `OutputCol` per row group
/// (avg-style aggs already split into two cols upstream): one row group of
/// `num_rows` values, or row groups of `answer_rg_rows` for a bigger answer,
/// their chunks encoded together. The output of an aggregate workload is a parquet
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
    encode: AggEncode,
) !u64 {
    const together = (output_cols.len > 1 or num_rows > answer_rg_rows) and num_rows >= parallel_encode_rows;
    return writeAggregateRowsEncoded(
        arena,
        sink,
        output_cols,
        num_rows,
        answer_rg_rows,
        codec,
        zstd,
        if (together) encode else .{ .ledger = encode.ledger, .dictionary_bloom = encode.dictionary_bloom },
    );
}

/// `writeAggregateRows` under `encode`, whatever the answer's size, in row groups of `rg_rows` (`answer_rg_rows`); the
/// bytes written are the same for every `encode`.
fn writeAggregateRowsEncoded(
    arena: std.mem.Allocator,
    sink: streaming.Sink,
    output_cols: []const expr_agg.OutputCol,
    num_rows: i64,
    rg_rows: usize,
    codec: schema.CompressionCodec,
    zstd: compression.ZstdPlan,
    encode: AggEncode,
) !u64 {
    var file = try AggregateFile.init(arena, sink);
    defer file.deinit();
    try appendAggregateRows(&file, output_cols, num_rows, rg_rows, codec, zstd, encode);
    return file.finish(output_cols);
}

/// One deterministic aggregate file, fed complete fixed row groups in bounded windows. The caller keeps a window
/// alive through `append` and may then free it; only owned footer metadata survives. The last window may be short.
pub const AggregateFile = struct {
    arena: std.mem.Allocator,
    sink: streaming.Sink,
    off: u64 = 4,
    rows: i64 = 0,
    groups: std.ArrayListUnmanaged(schema.RowGroup) = .empty,
    cached_wave: ?AggColumnWave = null,
    cached_width: usize = 0,
    cached_alone: std.heap.ArenaAllocator = .init(std.heap.page_allocator),
    cached_ledger: ?*expr_agg.SharedBudget = null,
    cached_charge: usize = 0,
    cached_unit_charge: usize = 0,

    pub fn init(arena: std.mem.Allocator, sink: streaming.Sink) !AggregateFile {
        try sink.write("PAR1");
        return .{ .arena = arena, .sink = sink };
    }

    pub fn deinit(f: *AggregateFile) void {
        f.releaseWorkspace();
    }

    fn releaseWorkspace(f: *AggregateFile) void {
        if (f.cached_wave) |*w| w.deinit();
        f.cached_wave = null;
        f.cached_alone.deinit();
        f.cached_alone = .init(std.heap.page_allocator);
        if (f.cached_ledger) |l| l.release(f.cached_charge);
        f.cached_ledger = null;
        f.cached_charge = 0;
        f.cached_width = 0;
        f.cached_unit_charge = 0;
    }

    /// Reuse one bounded wave for four row groups of three integer columns. Its full scratch upper bound stays
    /// charged until deinit, including retained pages between windows. A refused reservation keeps the usual waves.
    pub fn reserveIntegerWorkspace(f: *AggregateFile, encode: AggEncode) !void {
        return f.reserveWorkspace(encode, answer_rg_rows * 12 * 3);
    }

    /// A short-key window owns at most 16 payload bytes, one slice and one definition level per key row.
    pub fn reserveByteWorkspace(f: *AggregateFile, encode: AggEncode) !void {
        return f.reserveWorkspace(encode, answer_rg_rows * 36 * 3);
    }

    fn reserveWorkspace(f: *AggregateFile, encode: AggEncode, unit_charge: usize) !void {
        std.debug.assert(f.cached_width == 0 and f.cached_charge == 0);
        const admitted = encode.budget / unit_charge;
        if (admitted == 0) return;
        var width = @max(1, @min(@min(encode.workers, 12), admitted));
        // A trailing single unit uses its own arena beside every retained wave arena.
        if (width > 1 and width == admitted) width -= 1;
        const charge = (width + @intFromBool(width > 1)) * unit_charge;
        if (encode.ledger) |l| _ = l.drawExact(charge) orelse return;
        f.cached_ledger = encode.ledger;
        f.cached_charge = charge;
        f.cached_unit_charge = unit_charge;
        if (width > 1) f.cached_wave = try AggColumnWave.init(f.arena, width);
        f.cached_width = width;
    }

    pub fn append(
        f: *AggregateFile,
        cols: []const expr_agg.OutputCol,
        rows: usize,
        codec: schema.CompressionCodec,
        zstd: compression.ZstdPlan,
        encode: AggEncode,
    ) !void {
        try appendAggregateRows(f, cols, @intCast(rows), answer_rg_rows, codec, zstd, encode);
    }

    /// `cols` supplies names and types only, and may have empty value slices.
    pub fn finish(f: *AggregateFile, cols: []const expr_agg.OutputCol) !u64 {
        return aggregateFooter(f.arena, f.sink, cols, f.rows, f.groups, f.off);
    }
};

fn appendAggregateRows(
    file: *AggregateFile,
    output_cols: []const expr_agg.OutputCol,
    num_rows: i64,
    rg_rows: usize,
    codec: schema.CompressionCodec,
    zstd: compression.ZstdPlan,
    options: AggEncode,
) !void {
    const arena = file.arena;
    const sink = file.sink;
    var off = file.off;
    var encode = options;

    // Each row group's columns in order, row group after row group: the units encoded and written below. One row
    // group is the columns themselves.
    const total_rows: usize = @intCast(num_rows);
    const n_rg = @max(1, std.math.divCeil(usize, total_rows, rg_rows) catch unreachable);
    const units: []const expr_agg.OutputCol = if (n_rg == 1) output_cols else blk: {
        const all = try arena.alloc(expr_agg.OutputCol, n_rg * output_cols.len);
        for (0..n_rg) |g| {
            const lo = g * rg_rows;
            const hi = @min(total_rows, lo + rg_rows);
            for (output_cols, 0..) |c, i| all[g * output_cols.len + i] = sliceOutputCol(c, lo, hi);
        }
        break :blk all;
    };
    var new_row_groups: std.ArrayListUnmanaged(schema.RowGroup) = .empty;
    try new_row_groups.ensureTotalCapacity(arena, n_rg);

    var rg_columns: std.ArrayListUnmanaged(schema.ColumnChunk) = .empty;
    var rg_total: i64 = 0;
    // Columns are encoded in waves, in order: as many at once as the workers and the budget admit, each on a thread. A
    // column the budget cannot hold beside the one before it starts a wave alone, which encodes as the serial loop
    // does. Each wave's columns are then written, and any error returned, in column order, so the file and the error
    // are the serial loop's; their metadata is copied out and the wave's memory freed before the next. A column alone
    // encodes on memory given back once it is written, not on `arena`, which kept every column's until the end.
    if (file.cached_width != 0) {
        for (units) |unit| {
            // New shapes must not silently outgrow a prepaid unit in release builds. Return the reservation and
            // retained arenas first, then use the ordinary per-wave ledger draws for this and later windows.
            if (aggEncodeCharge(unit) > file.cached_unit_charge) {
                file.releaseWorkspace();
                break;
            }
        }
    }
    if (file.cached_width != 0) {
        encode.ledger = null;
        encode.workers = @min(encode.workers, file.cached_width);
    }
    const width = @min(encode.workers, units.len);
    var wave: ?AggColumnWave = file.cached_wave orelse
        if (width > 1) try AggColumnWave.init(arena, width) else @as(?AggColumnWave, null);
    defer if (file.cached_width == 0) if (wave) |*w| w.deinit();
    var own_alone = std.heap.ArenaAllocator.init(std.heap.page_allocator);
    defer own_alone.deinit();
    const alone = if (file.cached_width != 0) &file.cached_alone else &own_alone;
    var drawn: u64 = 0;
    defer if (encode.ledger) |l| l.release(drawn);
    var wave_lo: usize = 0;
    var wave_hi: usize = 0;
    for (units, 0..) |out_col, i| {
        if (i == wave_hi) {
            if (encode.ledger) |l| l.release(drawn);
            drawn = 0;
            wave_lo = i;
            wave_hi = i + 1;
            if (wave) |*w| {
                w.reset();
                var charged: u64 = aggEncodeCharge(out_col);
                while (wave_hi < units.len and wave_hi - i < width) : (wave_hi += 1) {
                    const c = aggEncodeCharge(units[wave_hi]);
                    if (charged +| c > encode.budget) break;
                    charged += c;
                }
                // Drawn whole, or narrowed a column at a time to what the ledger takes.
                if (encode.ledger) |l| while (wave_hi - i > 1) : (wave_hi -= 1) {
                    if (l.drawExact(charged) != null) {
                        drawn = charged;
                        break;
                    }
                    charged -= aggEncodeCharge(units[wave_hi - 1]);
                };
                if (wave_hi - i > 1) w.run(units[i..wave_hi], codec, zstd, encode.dictionary_bloom);
            }
            if (wave_hi - i == 1) if (encode.ledger) |l| {
                drawn = l.drawExact(aggEncodeCharge(out_col)) orelse return error.ExceededMemoryBudget;
            };
        }
        const leaf_elem = aggLeafElem(out_col);
        const in_wave = wave != null and wave_hi - wave_lo > 1;
        defer if (!in_wave) {
            _ = alone.reset(if (file.cached_width != 0) .retain_capacity else .free_all);
        };
        const enc = if (in_wave)
            try wave.?.results[i - wave_lo]
        else
            try encodeAggOutputCol(alone.allocator(), out_col, &leaf_elem, codec, zstd, encode.dictionary_bloom);
        const col_start: i64 = @intCast(off);
        var em = try ownColumnMeta(arena, enc.meta);
        em.data_page_offset += col_start;
        if (em.dictionary_page_offset) |dpo| em.dictionary_page_offset = dpo + col_start;
        // The encoder orders min/max signed; an unsigned column's type-defined order is not, so drop the bounds.
        if (schema.isUnsignedInt(leaf_elem)) if (em.statistics) |*st| {
            st.min = null;
            st.max = null;
            st.min_value = null;
            st.max_value = null;
        };
        try enc.writeTo(sink);
        off += enc.byteLen();
        rg_total += em.total_uncompressed_size;
        try rg_columns.append(arena, .{
            .file_path = null,
            .file_offset = col_start,
            .meta_data = em,
            .bloom_out = try arena.dupe(u8, enc.bloom),
        });
        if (rg_columns.items.len == output_cols.len) {
            const g = new_row_groups.items.len;
            const g_rows = if (n_rg == 1) num_rows else @as(i64, @intCast(@min(total_rows - g * rg_rows, rg_rows)));
            new_row_groups.appendAssumeCapacity(.{
                .columns = rg_columns,
                .total_byte_size = rg_total,
                .num_rows = g_rows,
            });
            rg_columns = .empty;
            rg_total = 0;
        }
    }

    // No columns, no units: the one row group is still written, empty.
    if (new_row_groups.items.len == 0)
        new_row_groups.appendAssumeCapacity(.{
            .columns = rg_columns,
            .total_byte_size = rg_total,
            .num_rows = num_rows,
        });
    try file.groups.appendSlice(arena, new_row_groups.items);
    file.rows += num_rows;
    file.off = off;
}

fn aggregateFooter(
    arena: std.mem.Allocator,
    sink: streaming.Sink,
    output_cols: []const expr_agg.OutputCol,
    num_rows: i64,
    new_row_groups: std.ArrayListUnmanaged(schema.RowGroup),
    start: u64,
) !u64 {
    var out: streaming.SinkOut = .{ .sink = sink, .offset = @intCast(start) };
    try fastpath.placeFilters(&out, new_row_groups.items);
    var off: u64 = out.offset;
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

    const new_meta: schema.FileMetaData = .{
        .version = 1,
        .schema = new_schema,
        .num_rows = num_rows,
        .created_by = schema.ZPQ_CREATED_BY,
        .row_groups = new_row_groups,
        .column_orders = try schema.FileMetaData.outputColumnOrders(arena, output_cols.len, null, &.{}),
    };

    var w: thrift.Writer = .init(arena);
    defer w.deinit();
    try new_meta.write(&w);
    const footer_bytes = w.bytes();
    try sink.write(footer_bytes);
    off += footer_bytes.len;

    var len_bytes: [4]u8 = undefined;
    std.mem.writeInt(u32, &len_bytes, std.math.cast(u32, footer_bytes.len) orelse return error.TooLarge, .little);
    try sink.write(&len_bytes);
    off += len_bytes.len;
    try sink.write("PAR1");
    off += 4;

    return off;
}

const ConjunctiveLeaf = struct {
    filter: filter_ast.Filter,
    col_idx: usize,
};

fn collectConjunctiveLeaves(f: filter_ast.Filter, list: *std.ArrayList(ConjunctiveLeaf), allocator: std.mem.Allocator) !void {
    switch (f) {
        // A NULL-member AND passes no row; what its leaves prove of pages says nothing of that.
        .and_filter => |c| if (!c.null_member) for (c.children) |child| try collectConjunctiveLeaves(child, list, allocator),
        .or_filter => {},
        else => {
            const col_idx = switch (f) {
                .int32 => |l| l.col_idx,
                .int64 => |l| l.col_idx,
                .uint64 => |l| l.col_idx,
                .float => |l| l.col_idx,
                .double => |l| l.col_idx,
                .decimal => |l| l.col_idx,
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
    /// Projected input-column ranks for decode-error ties; null keeps read-mask order.
    error_order: ?[]const usize = null,
    error_ranks: ?struct {
        ctx: *const anyopaque,
        make: *const fn (*const anyopaque, std.mem.Allocator, usize) anyerror![]const usize,
    } = null,
    /// The full query projection when workers split a row group by aggregate calls.
    error_read: ?[]const bool = null,
    error_calls: ?[]const expr_agg.AggCall = null,
    /// Columns to decode (`agg_plan.Columns.forRowGroup`).
    read: []const bool,
    /// Columns whose values an aggregate argument, a per-aggregate FILTER or a GROUP BY key consumes
    /// (`agg_plan.Columns.consumed`); any other column in `read` only the outer filter reads.
    consumed: []const bool,
    /// Columns that must decode their stored values (`agg_plan.Columns.preserve`). Null preserves every column.
    preserve: ?[]const bool = null,
};

pub const PackedCount = struct {
    pub const Source = struct { file: usize, rg: usize };

    domain: *const agg_dense.Domain,
    keys: []const expr_ast.Expr,
    sources: []const Source,
    bindings: []agg_dense.CountAxis,
    refused: *std.atomic.Value(bool),
};

pub const IntegerCountSink = struct {
    local: *owner_groups.IntCountTop.Local,
    packing: ?*PackedCount = null,

    fn batch(
        s: IntegerCountSink,
        view: filter_eval.Batch,
        lookup: []const ?usize,
        offset: usize,
        selected: ?*const filter_selection.SelectionVector,
    ) !void {
        const p = s.packing orelse return s.local.batchSelected(view, lookup, offset, selected);
        if (p.refused.load(.monotonic)) return;
        const source = p.sources[s.local.run_item];
        if (!try p.domain.bind(p.keys, source.file, source.rg, &view, lookup, p.bindings)) {
            p.refused.store(true, .monotonic);
            return;
        }
        if (selected) |sel| {
            var it = sel.iterator();
            while (it.next()) |row| try s.packedRow(p, offset, row);
        } else for (0..view.num_rows) |r| try s.packedRow(p, offset, r);
    }

    inline fn packedRow(s: IntegerCountSink, p: *const PackedCount, offset: usize, r: usize) !void {
        const id = try agg_dense.productId(p.bindings, r, true);
        const ordinal = s.local.ordinal_base + @as(u32, @intCast(offset + r));
        try s.local.append(@bitCast(@as(u64, @intCast(id))), {}, ordinal);
    }
};

/// The owned-record layouts share worker startup and decoded-batch routing.
pub const RecordSink = union(enum) {
    integer: *owner_groups.IntFull.Local,
    integer_top: *owner_groups.IntSumTop.Local,
    integer_count_top: IntegerCountSink,
    bytes: *owner_groups.Full.Local,
    byte_top: *owner_groups.SumTop.Local,
    byte_count_top: *owner_groups.CountTop.Local,
    msd: *msd_groups.Local,

    pub fn start(s: RecordSink, ordinal_base: u32, item: usize) void {
        switch (s) {
            .msd => |local| local.start(ordinal_base, item),
            .integer_count_top => |s0| s0.local.start(ordinal_base, item),
            inline else => |local| local.start(ordinal_base, item),
        }
    }

    fn retention(s: RecordSink, column: usize) ?*retained_pages.Chunk {
        return switch (s) {
            .msd => |local| if (column == local.state.key_col) local.owner else null,
            else => null,
        };
    }

    fn rewindRetained(s: RecordSink) void {
        switch (s) {
            .msd => |local| local.owner.?.used = 0,
            else => {},
        }
    }

    fn decodeReservation(s: RecordSink, ordinary: usize) usize {
        return switch (s) {
            // Retained payload capacity is already funded outside the decoder. Reserving it again would force
            // slices, changing which column's decode error wins. Leave the quote's margin for arena rounding.
            .msd => |local| local.state.decode_budget.limit / local.state.locals.len - (4 << 20),
            else => ordinary,
        };
    }

    fn batch(
        s: RecordSink,
        view: filter_eval.Batch,
        lookup: []const ?usize,
        offset: usize,
        sel: *const filter_selection.SelectionVector,
    ) !void {
        const count = sel.count();
        if (count == 0) return;
        const selected: ?*const filter_selection.SelectionVector = if (count == view.num_rows) null else sel;
        switch (s) {
            .integer_count_top => |s0| try s0.batch(view, lookup, offset, selected),
            inline else => |local| try local.batchSelected(view, lookup, offset, selected),
        }
    }
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
        /// A retained key domain's lanes: a row group whose dictionary the domain translated folds there, by global
        /// id, instead of into `table`.
        dense: ?*agg_dense.Lanes = null,
        records: ?*const RecordSink = null,
    },

    fn exactInputs(self: AggTarget) bool {
        if (self != .grouped or self.grouped.records != null) return false;
        // Decimal keys and mixed float consumers retain their existing scaled views. This request concerns only
        // exact arithmetic operands, not their logical types or the decoder used for unsupported expressions.
        for (self.grouped.keys) |key| if (key.typeOf() == .dec) return false;
        for (self.grouped.calls) |call| {
            if (call.where != null or call.func == .count_distinct) return false;
            if (call.arg) |arg| {
                if (arg.typeOf() != .dec or !exactTree(arg)) return false;
            } else if (call.func != .count) return false;
        }
        return true;
    }

    fn exactTree(e: expr_ast.Expr) bool {
        return switch (e) {
            .literal => |l| l == .dec or l == .i64,
            .col_ref => |c| c.expr_type == .dec or c.expr_type == .i64,
            .binop => |b| b.result_type == .dec and exactTree(b.left.*) and exactTree(b.right.*),
            else => false,
        };
    }

    fn codedKey(self: AggTarget, ci: usize) bool {
        if (self == .grouped) if (self.grouped.records) |r| {
            if (r.* == .integer_count_top and r.integer_count_top.packing != null)
                for (r.integer_count_top.packing.?.keys) |key| {
                    if (key.col_ref.col_idx == ci) return true;
                };
        };
        // The ordinary multi-aggregate consumer can fold dictionary products too. Keeping the original codes
        // avoids expanding strings only to intern them again; the row fallback materializes only if it needs to.
        if (self == .grouped and (self.grouped.dense != null or self.grouped.records == null)) {
            // Float extrema keep their established representation: histogram order can choose another NaN payload.
            if (self.grouped.dense == null) for (self.grouped.calls) |call| {
                if (call.result == .f64 and (call.func == .min or call.func == .max)) return false;
            };
            for (self.grouped.keys) |key| if (key == .col_ref and key.col_ref.col_idx == ci) return true;
        }
        return false;
    }

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
    /// The scan coordinator revalidates after all workers and their arenas have unwound.
    defer_decode_errors: bool = false,
    decode_batch_rows: usize = 0,
    auto_decode_batches: bool = false,
    cache_hints: ?*CacheHints = null,
    scan_workers: usize = 1,
    expr_batch_rows: ?usize = null,
    scratch: *DecodeScratch,
    /// Rewound at each row group that decodes: everything decoded lives here until the next one.
    rg_arena: *RowGroupArena,
    timings: *Timings,
    /// Shared by the query's scan workers; null decodes without a bound (tests, callers that budget elsewhere).
    budget: ?*DecodeBudget = null,
    /// The worker's region under `scratch` and `rg_arena`, which keeps an earlier pass's pages: trimmed before a row
    /// group decodes sliced or staged on memory of its own, so those pages are not held beside its reservation.
    region: ?*huge_pages.Region = null,
    /// Where a row group decodes staged or sliced, beside the region: drawn from the query's ledger, as the region is.
    /// Null allocates from the scan's `gpa`.
    charged: ?std.mem.Allocator = null,
    /// Optional cost hints use their own refusal flags, while charging the same ledger.
    planning_charged: ?std.mem.Allocator = null,
    /// The ledger `charged` draws from, which a sliced row group sizes its slices to fit.
    ledger: ?*const expr_agg.SharedBudget = null,
};

/// Whether a row group's whole decode of `bytes` may go on the worker's region: the query's ledger has room now for
/// what the region does not hold already (`Region.fitsRowGroup`). Without a region (or a ledger) only the decode budget
/// decides.
fn admitWhole(ws: AggWorkspace, bytes: usize) bool {
    const r = ws.region orelse return true;
    return r.fitsRowGroup(bytes);
}

/// Give back everything the worker's region holds, the row-group arena's nodes and the decode scratch, with what
/// they drew from the ledger: a row group the ledger refused there then decodes in slices on memory of its own, not
/// beside the pages it was refused for. Nothing allocated on either may be live.
fn dropRegion(ws: AggWorkspace) void {
    const r = ws.region orelse return;
    if (ws.rg_arena.region == null) {
        const prof = ws.scratch.prof;
        ws.scratch.deinit();
        ws.scratch.* = .init(r.allocator());
        ws.scratch.prof = prof;
        ws.rg_arena.deinit();
        ws.rg_arena.* = .init(r.allocator());
    }
    r.rewind();
    r.trim();
}

/// `err` was the region's or `ws.charged`'s refusal of a draw, not the host's out-of-memory: the row group may still
/// decode in slices, on less.
fn refusedDraw(ws: AggWorkspace, err: anyerror) bool {
    if (err != error.OutOfMemory) return false;
    if (ws.region) |r| if (r.refused) {
        r.refused = false;
        return true;
    };
    return false;
}

/// The memory a query's scan workers' decoded row groups may take together (`scan.runMultiAggregate` gives it half of
/// `max_memory`). A worker reserves a row group's estimate (`decodeEstimate`) before decoding it and gives it back
/// after, so a row group that does not fit beside the others waits for them. One bigger than a worker's share decodes
/// staged — the filter and keys, then one aggregate's columns at a time on memory handed back after each — and one
/// that does not fit the whole budget even so is refused before anything is allocated, rather than killed by the OS.
pub const DecodeBudget = struct {
    limit: usize,
    workers: usize = 1,
    used: std.atomic.Value(usize) = .init(0),
    /// Bumped by every release; a worker whose row group does not fit sleeps on it until one does.
    released: std.atomic.Value(u32) = .init(0),
    /// Workers asleep in `acquire`: a release with none wakes nobody, and costs no system call.
    waiters: std.atomic.Value(u32) = .init(0),

    /// What one worker may hold without staging: its even share.
    pub fn share(self: *const DecodeBudget) usize {
        return self.limit / @max(1, self.workers);
    }

    /// Others hold the budget for a whole row group's decode, milliseconds at least, so a worker that does not fit
    /// sleeps until a release rather than spin or poll on a core a decoding worker needs (Lambda has two).
    pub fn acquire(self: *DecodeBudget, bytes: usize) error{RowGroupTooLarge}!void {
        if (bytes > self.limit) return error.RowGroupTooLarge;
        if (self.tryAcquire(bytes)) return;
        _ = self.waiters.fetchAdd(1, .seq_cst);
        defer _ = self.waiters.fetchSub(1, .seq_cst);
        while (true) {
            // Read `released` before trying: a release between the two moves it, and the wait returns at once.
            const seen = self.released.load(.seq_cst);
            if (self.tryAcquire(bytes)) return;
            blocking.io.futexWaitUncancelable(u32, &self.released.raw, seen);
        }
    }

    pub fn release(self: *DecodeBudget, bytes: usize) void {
        _ = self.used.fetchSub(bytes, .release);
        _ = self.released.fetchAdd(1, .seq_cst);
        if (self.waiters.load(.seq_cst) > 0) blocking.io.futexWake(u32, &self.released.raw, std.math.maxInt(u32));
    }

    /// `acquire` without waiting: false, holding nothing, when the bytes do not fit now. For optional scratch whose
    /// caller has a path that needs none.
    pub fn tryAcquire(self: *DecodeBudget, bytes: usize) bool {
        while (true) {
            const cur = self.used.load(.monotonic);
            if (cur + bytes > self.limit) return false;
            if (self.used.cmpxchgWeak(cur, cur + bytes, .acquire, .monotonic) == null) return true;
        }
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
        const decimal = if (se) |e| decimal_mod.exactKind(&e) != null else false;
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

/// Abandoned pre-aggregation: each row appended to this worker's run for its key's partition.
fn scatterGrouped(
    ra: std.mem.Allocator,
    values: std.mem.Allocator,
    gpa: std.mem.Allocator,
    g: @FieldType(AggTarget, "grouped"),
    part: *partagg.PartAgg,
    local: *partagg.Local,
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
            cols[p] = try agg_coded.materialize(values, cols[p]);
        };
        prepared_values.* = true;
    }
    const win_cols = try ra.alloc(filter_eval.Batch.Column, cols.len);
    for (cols, win_cols) |c, *o| o.* = sliceColumn(c, off, n);
    var win: filter_eval.Batch = .{ .cols = win_cols, .num_rows = n };
    const key_cols = try ra.alloc(filter_eval.Batch.Column, g.keys.len);
    const keyed = win.restrictedTo(win_sel);
    for (g.keys, key_cols) |k, *kc| kc.* = try expr_eval.evalGroupKeyExpr(ra, &keyed, lookup, k);

    const which = try ra.alloc(u16, n);
    var counts: [partagg.n_parts]u32 = @splat(0);
    var scratch: std.ArrayList(u8) = .empty;
    defer scratch.deinit(ra);
    for (0..n) |r| {
        if (!win_sel.isActive(r)) continue;
        const h = try routeRowHash(ra, &scratch, g.table, key_cols, r);
        const p: u16 = @intCast(part.indexOf(h));
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
    if (!part.usesRuns(local)) {
        for (part.parts, 0..) |*pt, p| {
            const lo = start[p];
            const hi = start[p + 1];
            if (lo == hi) continue;
            partagg.PartAgg.lock(pt);
            defer partagg.PartAgg.unlock(pt);
            for (order[lo..hi]) |r| ids[r] = try insertRow(ra, &scratch, &pt.table, key_cols, r, g.calls, pos);
            try preps.fold(gpa, pt.table.accumulators.items, ids, order[lo..hi], g.calls, if (pt.table.compact.stride != 0) &pt.table.compact else null);
        }
        return;
    }
    const runs = local.runs.?;
    for (runs, 0..) |*run, p| {
        const lo = start[p];
        const hi = start[p + 1];
        if (lo == hi) continue;
        for (order[lo..hi]) |r| {
            var image: [32]u8 = undefined;
            const key = try storedRowKey(ra, &scratch, g.table, key_cols, r, &image);
            ids[r] = @intCast(try part.append(run, key, pos + r));
        }
        try preps.fold(gpa, run.accs.items, ids, order[lo..hi], g.calls, if (run.compact.stride != 0) &run.compact else null);
    }
}

/// The key `row` would be stored under in `table`: its fixed image, or its serialized bytes in `scratch`.
fn storedRowKey(
    ra: std.mem.Allocator,
    scratch: *std.ArrayList(u8),
    table: *const expr_agg.GroupTable,
    key_cols: []const filter_eval.Batch.Column,
    row: usize,
    image: *[32]u8,
) ![]const u8 {
    if (table.fixed_stride != 0) {
        if (expr_agg.packFixedKey(key_cols, row, image)) |nb| if (nb == table.fixed_stride) return image[0..nb];
    }
    try expr_agg.serializeRowKey(scratch, ra, key_cols, row);
    if (table.fixed_stride == 0) return scratch.items;
    return expr_agg.fixedImage(table.fixed_stride, scratch.items, image) orelse error.BadGroupKey;
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
    expr_batch_rows: ?usize,
    /// Charged for a coded composite space past the free one, as the whole-row-group fold is.
    budget: ?*DecodeBudget,
    /// The region `ra` allocates from, if it does.
    region: ?*huge_pages.Region,
) !void {
    const g = switch (target) {
        .grouped => |grouped| grouped,
        .scalar => return,
    };
    const part = g.part orelse return;
    const local = g.local orelse return;
    // An armed worker folds a batch the coded path takes whole, as the plain fold does: in windows, each would
    // resolve again nearly every code of a large dictionary.
    if (local.armed and encoded and sel.len > 0) {
        const whole: filter_eval.Batch = .{ .cols = cols, .num_rows = sel.len, .expr_batch_rows = expr_batch_rows };
        if (try agg_coded.groupedCoded(ra, gpa, g.calls, g.keys, g.table, g.pos_base, &whole, lookup, sel, budget, region)) {
            try part.pump(local, g.table, @intCast(sel.count()));
            return;
        }
    }
    // Group tables/runs own their keys and states. Only decoded columns survive a window: otherwise routing,
    // selection and expression scratch accumulates across the row group beside the prepaid run ceiling.
    var window = RowGroupArena.init(ra);
    defer window.deinit();
    var prepared_values = false;
    var off: usize = 0;
    while (off < sel.len) {
        window.nextRowGroup();
        const wa = window.allocator();
        if (part.needsFlush(local, g.table)) try part.pump(local, g.table, 0);
        const n = part.nextSpan(local, g.table, sel.len - off);
        if (n == 0) break;
        var win_sel = try selWindow(wa, sel, off, n);
        defer win_sel.deinit();
        const pos = g.pos_base + @as(u64, @intCast(off));
        // A local table that is not reducing the rows is pure overhead: hash each key once, straight into its partition.
        if (local.abandoned) {
            try scatterGrouped(wa, ra, gpa, g, part, local, cols, lookup, &win_sel, consumed, &prepared_values, pos, off, n);
            off += n;
            continue;
        }
        if (!prepared_values and encoded) {
            const win_cols = try wa.alloc(filter_eval.Batch.Column, cols.len);
            for (cols, win_cols) |c, *o| o.* = sliceColumn(c, off, n);
            const win: filter_eval.Batch = .{ .cols = win_cols, .num_rows = n, .expr_batch_rows = expr_batch_rows };
            if (try agg_coded.groupedCoded(wa, gpa, g.calls, g.keys, g.table, pos, &win, lookup, &win_sel, budget, region)) {
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
        const win_cols = try wa.alloc(filter_eval.Batch.Column, cols.len);
        for (cols, win_cols) |c, *o| o.* = sliceColumn(c, off, n);
        var win: filter_eval.Batch = .{ .cols = win_cols, .num_rows = n };
        const key_cols = try wa.alloc(filter_eval.Batch.Column, g.keys.len);
        const keyed = win.restrictedTo(&win_sel);
        for (g.keys, key_cols) |k, *kc| kc.* = try expr_eval.evalGroupKeyExpr(wa, &keyed, lookup, k);
        const ids = try wa.alloc(u32, n);
        @memset(ids, 0);
        var resolver = expr_agg.GroupKeyResolver.init(key_cols);
        defer resolver.deinit(wa);
        try resolver.resolveBatch(wa, g.table, key_cols, g.calls, pos, &win_sel, ids);
        try expr_agg.sinkSketchRows(g.table, ids, &win_sel);
        if (!g.table.countOnly()) for (g.calls, 0..) |_, i| {
            try expr_agg.updateTableGrouped(wa, gpa, g.table, ids, i, g.calls, &win, lookup, &win_sel);
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
/// Returns the rows the outer filter kept (every row without one), for the summary's `rows_matched`.
pub fn scanRGForAgg(
    gpa: std.mem.Allocator,
    in: AggRowGroup,
    stats: agg_plan.StatsUse,
    decode_options: DecodeOptions,
    target: AggTarget,
    ws: AggWorkspace,
) !u64 {
    var execution = ws;
    if (ws.auto_decode_batches and ws.decode_batch_rows == 0 and ws.expr_batch_rows != 0 and
        (target != .grouped or (decode_options.encoded and target.grouped.part == null and
            target.grouped.records == null)))
        execution.decode_batch_rows = automaticCacheRowsWithHints(ws.planning_charged orelse ws.charged orelse gpa, in, target, ws.scan_workers, ws.cache_hints) catch 0;
    if (ws.defer_decode_errors) return scanRGForAggImpl(gpa, in, stats, decode_options, target, execution);
    return scanRGForAggImpl(gpa, in, stats, decode_options, target, execution) catch |err| {
        if (!isDecodeError(err)) return err;
        const site = decodeFailure() orelse return err;
        const failure = canonicalDecodeFailure(
            ws.charged orelse gpa,
            in,
            decode_options,
            target.calls(),
            target == .scalar,
            .{ .err = err, .site = site },
        );
        setDecodeFailure(failure.site);
        return failure.err;
    };
}

/// Error-only work, after releasing the failed scan's arenas. All allocations use the caller's bounded, charged
/// allocator. Resource failure ends the probe with its best confirmed decode error; it never becomes the diagnostic.
pub fn canonicalDecodeFailure(
    a: std.mem.Allocator,
    in: AggRowGroup,
    options: DecodeOptions,
    calls: []const expr_agg.AggCall,
    scalar: bool,
    original: decode_order.Failure,
) decode_order.Failure {
    return (probeDecodeFailure(a, in, options, calls, scalar) catch null) orelse original;
}

/// Probe one full-projection row group. Valid groups contribute no failure; refusal contributes no evidence.
pub fn probeDecodeFailure(
    a: std.mem.Allocator,
    in: AggRowGroup,
    options: DecodeOptions,
    calls: []const expr_agg.AggCall,
    scalar: bool,
) !?decode_order.Failure {
    var planning = std.heap.ArenaAllocator.init(a);
    defer planning.deinit();
    const pa = planning.allocator();
    var check_in = in;
    check_in.read = in.error_read orelse in.read;
    if (in.error_ranks) |rank| check_in.error_order = try rank.make(rank.ctx, pa, check_in.read.len);
    const only = try pa.alloc(bool, check_in.read.len);
    @memset(only, false);
    if (scalar) {
        const target: AggTarget = .{ .scalar = .{ .calls = calls, .accumulators = &.{} } };
        const uses = try planCodeUses(pa, check_in, target, false);
        for (only, 0..) |*v, ci| v.* = uses.levels[ci] and !uses.code[ci] and !uses.value[ci];
    }
    return decode_order.probe(SliceReader, isDecodeError, a, check_in, options, only);
}

// Checked arithmetic keeps its expression-major error order. A forced cache window may change only when
// fault-free expressions are evaluated; memory-driven slicing otherwise retains its existing admission.
fn cacheExpressionsSafe(target: AggTarget) bool {
    for (target.calls()) |call| if (call.arg) |arg| {
        if (arg == .col_ref and (arg.col_ref.unsigned_64 or arg.col_ref.expr_type == .dec)) return false;
        if (arg != .col_ref and !floatTreeSafe(arg)) return false;
    };
    if (target == .grouped) for (target.grouped.keys) |key| {
        if (key == .col_ref and (key.col_ref.unsigned_64 or key.col_ref.expr_type == .dec)) return false;
        if (key != .col_ref and !floatTreeSafe(key)) return false;
    };
    return true;
}

fn floatTreeSafe(e: expr_ast.Expr) bool {
    return switch (e) {
        .literal => |l| l != .str,
        .col_ref => |c| c.expr_type == .f64 and (c.physical_type == .FLOAT or c.physical_type == .DOUBLE),
        .binop => |b| b.result_type == .f64 and b.op != .concat and
            floatTreeSafe(b.left.*) and floatTreeSafe(b.right.*),
        .call, .case_ => false,
    };
}

const CacheDictionary = struct { entries: usize, bytes: usize };

/// Query-local header facts, shared across workers/passes. No page or source-buffer pointers are retained.
/// Optional allocation refusal leaves the cost hint uncached; it never affects decode diagnostics.
pub const CacheHints = struct {
    const Key = struct { rg: *const schema.RowGroup, column: usize };
    const Shard = struct {
        lock: std.Io.Mutex = .init,
        facts: std.AutoHashMapUnmanaged(Key, ?CacheDictionary) = .empty,
    };
    inline_shard: Shard = .{},
    shards: []Shard = &.{},
    allocator: std.mem.Allocator,

    /// Initialized before workers start. Refusal retains the inline memo, without changing decoder flags.
    pub fn initShards(h: *CacheHints, workers: usize) !void {
        if (workers <= 1) return;
        const shards = try h.allocator.alloc(Shard, workers);
        @memset(shards, .{});
        h.shards = shards;
    }

    pub fn deinit(h: *CacheHints) void {
        h.inline_shard.facts.deinit(h.allocator);
        h.inline_shard.facts = .empty;
        for (h.shards) |*shard| shard.facts.deinit(h.allocator);
        if (h.shards.len != 0) h.allocator.free(h.shards);
        h.shards = &.{};
    }

    fn count(h: *const CacheHints) usize {
        var n = h.inline_shard.facts.count();
        for (h.shards) |shard| n += shard.facts.count();
        return n;
    }

    fn get(h: *CacheHints, in: AggRowGroup, ci: usize) ?CacheDictionary {
        const key: Key = .{ .rg = in.rg, .column = ci };
        const hash = (std.hash_map.AutoContext(Key){}).hash(key);
        const shard = if (h.shards.len == 0) &h.inline_shard else &h.shards[hash % h.shards.len];
        shard.lock.lockUncancelable(@import("blocking.zig").io);
        defer shard.lock.unlock(@import("blocking.zig").io);
        if (shard.facts.getPtr(key)) |fact| return fact.*;
        const fact = cacheDictionary(in, ci);
        shard.facts.put(h.allocator, key, fact) catch {};
        return fact;
    }
};

pub fn cacheHintsWanted(calls: []const expr_agg.AggCall) bool {
    for (calls) |call| if (call.arg) |arg| if (arg == .binop and arg.typeOf() == .f64) return true;
    return false;
}

/// Header-only proof of a column's finite dictionary domain. It is a cost hint: malformed/incomplete bytes decline
/// admission and leave all errors to ordinary execution. No statistics or decompression result authorizes a path.
fn cacheDictionary(in: AggRowGroup, ci: usize) ?CacheDictionary {
    if (ci >= in.rg.columns.items.len) return null;
    const cm = in.rg.columns.items[ci].meta_data orelse return null;
    if (cm.total_compressed_size < 0) return null;
    const offset = cm.dictionary_page_offset orelse return null;
    const absolute = std.math.cast(usize, offset) orelse return null;
    const origin = std.math.cast(usize, in.src.byte_origin) orelse return null;
    if (absolute < origin) return null;
    const start = absolute - origin;
    const len: usize = @intCast(cm.total_compressed_size);
    if (start > in.src.bytes.len or len > in.src.bytes.len - start) return null;
    var pages = page_mod.PageReader.init(in.src.bytes[start..][0..len], cm.codec, std.heap.page_allocator);
    var dict: ?CacheDictionary = null;
    var data = false;
    while (pages.peek() catch return null) |p| {
        switch (p.header.type) {
            .DICTIONARY_PAGE => {
                if (dict != null or data) return null;
                const h = p.header.dictionary_page_header orelse return null;
                if (h.num_values <= 0) return null;
                dict = .{ .entries = @intCast(h.num_values), .bytes = @intCast(p.header.uncompressed_page_size) };
            },
            .DATA_PAGE, .DATA_PAGE_V2 => {
                if (dict == null) return null;
                const encoding = if (p.header.data_page_header) |h|
                    h.encoding
                else if (p.header.data_page_header_v2) |h| h.encoding else return null;
                if (encoding != .RLE_DICTIONARY and encoding != .PLAIN_DICTIONARY) return null;
                data = true;
            },
            .INDEX_PAGE => {},
        }
        pages.pass(p);
    }
    return if (data) dict else null;
}

fn cacheFilterNodes(f: filter_ast.Filter) usize {
    return switch (f) {
        .and_filter, .or_filter => |c| blk: {
            var n: usize = 1;
            for (c.children) |child| n +|= cacheFilterNodes(child);
            break :blk n;
        },
        else => 1,
    };
}

fn cacheExpandedTreeBytes(e: expr_ast.Expr) usize {
    return switch (e) {
        .binop => |b| 12 +| cacheExpandedTreeBytes(b.left.*) +| cacheExpandedTreeBytes(b.right.*),
        .literal => 8,
        else => 16,
    };
}

fn wholeFloatTreeBytes(e: expr_ast.Expr) usize {
    return switch (e) {
        .binop => |b| 12 +| wholeFloatTreeBytes(b.left.*) +| wholeFloatTreeBytes(b.right.*),
        .literal => 8,
        .col_ref => |c| if (c.physical_type == .FLOAT) 8 else 0,
        else => 0,
    };
}

/// Admit only computed fault-free float trees. Decoded inputs, all DAG results, key codes, selection and group
/// IDs share the stable L2 worker budget; pages/dictionaries use the decoder's separate bounded buffers.
fn automaticCacheRows(gpa: std.mem.Allocator, in: AggRowGroup, target: AggTarget, workers: usize) !usize {
    return automaticCacheRowsWithHints(gpa, in, target, workers, null);
}

fn automaticCacheRowsWithHints(gpa: std.mem.Allocator, in: AggRowGroup, target: AggTarget, workers: usize, hints: ?*CacheHints) !usize {
    var computed = false;
    for (target.calls()) |call| {
        if (call.where != null or call.func == .count_distinct or call.result == .bytes) return 0;
        if (call.arg) |arg| {
            computed = computed or arg == .binop;
            if (arg == .col_ref and (arg.col_ref.unsigned_64 or arg.col_ref.expr_type == .dec)) return 0;
        }
    }
    if (!computed or !cacheExpressionsSafe(target)) return 0;
    if (target == .grouped) for (target.grouped.keys) |key| {
        if (key != .col_ref or key.col_ref.unsigned_64 or key.col_ref.expr_type == .dec) return 0;
    };
    if (@import("hw.zig").l2BytesPerWorker() == 0) return 0;
    var planning = std.heap.ArenaAllocator.init(gpa);
    defer planning.deinit();
    const a = planning.allocator();
    const lookup = try a.alloc(?usize, in.meta.schema.items.len);
    @memset(lookup, null);
    var cols: std.ArrayList(filter_eval.Batch.Column) = .empty;
    var copied_bytes: usize = 0;
    var fixed_keys: usize = 0;
    var whole_values: usize = 0;
    var largest_chunk: usize = 0;
    for (in.read, 0..) |needed, ci| {
        if (!needed) continue;
        if (!SliceReader.supports(in.rg, in.meta, ci)) return 0;
        const cm = in.rg.columns.items[ci].meta_data orelse return 0;
        const n: usize = @intCast(@max(0, cm.num_values));
        const dictionary = if (hints) |h| h.get(in, ci) else cacheDictionary(in, ci);
        const lane: usize = if (dictionary != null) 4 else switch (cm.type) {
            .INT32, .FLOAT => 4,
            .BOOLEAN => 1,
            else => 8,
        };
        whole_values +|= n *| lane;
        if (in.meta.getColumnLevels(cm.path_in_schema.items).max_def > 0)
            whole_values +|= if (dictionary != null) n / 8 +| 8 else n *| 4;
        largest_chunk = @max(largest_chunk, @as(usize, @intCast(@max(0, cm.total_uncompressed_size))));
        if (in.meta.getColumnSchema(cm.path_in_schema.items)) |se|
            if (decimal_mod.exactKind(&se) != null) return 0;
        switch (cm.type) {
            .BYTE_ARRAY => {
                const d = dictionary orelse return 0;
                // decodeBytes can copy the same dictionary string on every row. Its entire encoded dictionary
                // bounds one string's width, even when compression makes a footer's per-row estimate tiny.
                copied_bytes +|= d.bytes;
                fixed_keys +|= d.bytes +| d.entries *| @sizeOf([]const u8);
            },
            .FIXED_LEN_BYTE_ARRAY, .INT96 => return 0,
            else => {},
        }
        lookup[ci] = cols.items.len;
        // FloatPlan inspects lane types and layouts before decoding; other input lanes are counted below.
        const col: filter_eval.Batch.Column = switch (cm.type) {
            .FLOAT => .{ .f32 = .{ .values = &.{} } },
            .DOUBLE => .{ .f64 = .{ .values = &.{} } },
            else => .{ .i64 = .{ .values = &.{} } },
        };
        try cols.append(a, col);
    }
    const batch: filter_eval.Batch = .{ .cols = cols.items, .num_rows = 0 };
    var plan: expr_eval.FloatPlan = .{};
    for (target.calls()) |call| if (call.arg) |arg| if (arg != .col_ref) {
        _ = (try plan.add(a, &batch, lookup, arg)) orelse return 0;
    };
    const rows: usize = @intCast(@max(0, in.rg.num_rows));
    if (rows == 0) return 0;
    const grouped = target == .grouped;
    const whole_dictionaries = fixed_keys;
    var space: usize = 1;
    if (grouped) for (target.grouped.keys) |key| {
        const d = (if (hints) |h| h.get(in, key.col_ref.col_idx) else cacheDictionary(in, key.col_ref.col_idx)) orelse return 0;
        fixed_keys +|= agg_coded.plainKeyScratchBytes(d.entries) orelse return 0;
        space *|= d.entries +| 1;
    };
    const inputs = std.math.divCeil(usize, decodeEstimate(in, in.read, &.{}, grouped), rows) catch return 0;
    const key_bytes = if (grouped) 4 * target.grouped.keys.len else 0;
    // Counting the DAG's borrowed input lanes again is conservative and reserves room for widening and key scratch.
    var widened: usize = 0;
    for (target.calls()) |call| if (call.arg) |arg| if (arg == .col_ref and
        (arg.col_ref.physical_type == .INT32 or arg.col_ref.physical_type == .FLOAT))
    {
        widened +|= 8;
    };
    const masks = target.calls().len +| if (in.filter) |f| cacheFilterNodes(f) else @as(usize, 0);
    // A window with only NULL keys can decline recoding. Its general fold must fit too, even without CSE.
    var expanded: usize = 8;
    if (grouped) for (target.calls()) |call| if (call.arg) |arg| {
        if (arg != .col_ref) expanded +|= cacheExpandedTreeBytes(arg);
    };
    const live = inputs +| @max(plan.liveBytesPerRow(), expanded) +| key_bytes +| copied_bytes +| widened +| masks;
    const group_state = if (grouped) space *| (64 +| target.grouped.keys.len *| 32 +|
        target.calls().len *| (2 * @sizeOf(expr_agg.Accumulator) + 32) +| copied_bytes) else 0;
    const fixed = fixed_keys +| group_state +|
        (plan.nodes.items.len + cols.items.len) * @sizeOf(filter_eval.Batch.Column) +|
        in.meta.schema.items.len *| @sizeOf(?usize) *| (3 + if (grouped) target.grouped.keys.len else @as(usize, 0)) +|
        target.calls().len * @sizeOf(expr_agg.Accumulator);
    const cap = expr_eval.floatBatchRows(live, fixed, null);
    // Whole vectors are often cheaper while their hot state fits in L3. This is a cost estimate, independent of
    // the conservative L2 bound above: dictionary codes/presence, values/levels, one page and aggregate scratch.
    const l3 = @import("hw.zig").l3BytesPerActiveWorker(workers);
    if (l3 != 0) {
        var whole = whole_values +| largest_chunk +| rows / 8 +| whole_dictionaries +| group_state;
        if (grouped) {
            whole +|= rows *| 4 +| plan.liveBytesPerRow() *|
                expr_eval.floatBatchRows(plan.liveBytesPerRow(), group_state, null);
        } else for (target.calls()) |call| if (call.arg) |arg| {
            if (arg != .col_ref) whole +|= rows *| wholeFloatTreeBytes(arg);
        };
        if (whole <= l3) return 0;
    }
    return if (cap != 0 and cap < rows) cap else 0;
}

/// Share computed scalar inputs too, then use the ordinary typed folds over their prepared values. Synthesizing a
/// borrowed DOUBLE column keeps NULL masks and canonical 64-row reductions in updateOne; no partial sum is merged.
fn foldScalarCache(
    a: std.mem.Allocator,
    persist: std.mem.Allocator,
    calls: []const expr_agg.AggCall,
    accumulators: []expr_agg.Accumulator,
    handled: []const bool,
    batch: *const filter_eval.Batch,
    lookup: []const ?usize,
    sel: *const filter_selection.SelectionVector,
) !bool {
    var plan: expr_eval.FloatPlan = .{};
    const roots = try a.alloc(?usize, calls.len);
    @memset(roots, null);
    for (calls, handled, roots) |call, h, *root| {
        if (h) continue;
        if (call.where != null or call.func == .count_distinct or call.result == .bytes) return false;
        if (call.arg) |arg| if (arg != .col_ref) {
            root.* = (try plan.add(a, batch, lookup, arg)) orelse return false;
        };
    }
    if (plan.nodes.items.len == 0) return false;
    const restricted = batch.restrictedTo(sel);
    const values = try plan.evaluate(a, &restricted, lookup);
    for (calls, accumulators, handled, roots) |call, *acc, h, root| {
        if (h) continue;
        if (root) |r| {
            var prepared = call;
            prepared.arg = .{ .col_ref = .{ .col_idx = 0, .physical_type = .DOUBLE, .expr_type = .f64 } };
            const input: filter_eval.Batch = .{ .cols = &.{values[r]}, .num_rows = batch.num_rows };
            try expr_agg.updateOne(a, persist, acc, prepared, &input, &.{0}, sel);
        } else try expr_agg.updateOne(a, persist, acc, call, batch, lookup, sel);
    }
    return true;
}

fn scanRGForAggImpl(
    gpa: std.mem.Allocator,
    in: AggRowGroup,
    stats: agg_plan.StatsUse,
    decode_options: DecodeOptions,
    target: AggTarget,
    ws: AggWorkspace,
) !u64 {
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
    // An outer filter disables every stats answer, so here none is pending; with one it decodes to count its rows.
    if (!any_decode_required and in.filter == null) return num_rows;

    // 2. Decode path for the aggs that couldn't be stat-handled.
    // Rewound, not rebuilt: a fresh arena per row group hands its pages back to the OS and re-faults them every time.
    // The price is that each worker's arena holds its high-water mark (one row group) for the whole scan, which the
    // decode budget bounds: a row group past a worker's share decodes staged, on memory handed back when it is done.
    ws.rg_arena.nextRowGroup();
    if (ws.region) |r| r.settle();
    const prof = ws.timings.prof;

    var staged = false;
    var reserved: usize = 0;
    // Staged and sliced decodes live beside the region, on memory drawn from the query's ledger as the region's is.
    const side = ws.charged orelse gpa;
    var plan_arena = std.heap.ArenaAllocator.init(side);
    defer plan_arena.deinit();
    var full_estimate: usize = 0;
    const stage_plan: ?StagePlan = if (ws.budget) |b| plan: {
        const ordinary = decodeEstimate(in, in.read, calls, target == .grouped);
        const full = if (target == .grouped and target.grouped.records != null)
            target.grouped.records.?.decodeReservation(ordinary)
        else
            ordinary;
        full_estimate = full;
        if (target == .grouped and target.grouped.table.countOnly()) {
            const keys_only = decodeEstimate(in, in.read, &.{}, true);
            std.debug.assert(full == keys_only);
        }
        reserved = full;
        // Too big for this worker's share, or for what the query's ledger has left: a slice of rows at a time when
        // every column has a sliced form, else one aggregate's columns at a time.
        const fits_share = full <= b.share();
        const discovering = target == .grouped and target.grouped.table.discovery != null;
        const force_cache = ws.decode_batch_rows != 0 and (discovering or cacheExpressionsSafe(target));
        const whole = !force_cache and fits_share and admitWhole(ws, full);
        if (!whole) if (ws.region) |r| if (fits_share) dropRegion(ws) else r.trim();
        if (!whole) if (try scanRGSliced(gpa, in, decode_options, target, ws, b, handled_via_stats, full)) |kept|
            return kept;
        if (!whole) {
            if (target == .grouped and target.grouped.records != null) return error.ExceededMemoryBudget;
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
    var held = reserved;
    defer if (ws.budget) |b| b.release(held);
    // Staged, everything decoded lives on memory of its own, given back with the row group; otherwise on the
    // worker's arena.
    var staged_arena = std.heap.ArenaAllocator.init(side);
    defer staged_arena.deinit();
    const ra = if (staged) staged_arena.allocator() else ws.rg_arena.allocator();

    const prune_span = profile.Span.begin(prof);
    const page_indexes = try loadPageIndexes(ra, in, stats);
    var sel = try filter_selection.SelectionVector.init(ra, num_rows);
    const filter_pages = try pruneFilterPages(ra, in, page_indexes, stats, &sel);
    // Page pruning left few rows: decode just those, and let them stand for the row group from here on.
    const keep = if (staged) null else try compactRows(ra, in, page_indexes, &sel, target);
    const batch_rows = if (keep != null) sel.count() else num_rows;

    const t_decode_start = nowMonoNs();
    var dec: ChunkDecoder = .{
        .in = in,
        .uses = try planCodeUses(ra, in, target, staged),
        .page_indexes = page_indexes,
        .filter_pages = filter_pages,
        .num_rows = num_rows,
        .decode_options = decode_options,
        .ws = ws,
        .target = target,
        .keep = keep,
        .rows_out = if (keep != null) batch_rows else null,
    };
    var batch_cols: std.ArrayList(filter_eval.Batch.Column) = .empty;
    var lookup = try ra.alloc(?usize, in.meta.schema.items.len);
    @memset(lookup, null);
    // Pages are pruned by the rows the filter's page statistics leave, as when every column decodes up front.
    const sel_pre = if (staged) try sel.cloneAlloc(ra) else sel;
    // Compact columns: every kept row is selected so far.
    if (keep != null) sel = try filter_selection.SelectionVector.init(ra, batch_rows);
    // Payload columns decode after the filter, gated by its survivors; a staged plan decodes them with its calls.
    dec.payload = try payloadColumns(ra, in, target);
    var filtered = false;
    for ([_]bool{ false, true }) |late| for (in.read, 0..) |needed, ci| {
        if (late and dec.payload == null) break;
        if (!needed or ci >= in.rg.columns.items.len) continue;
        if (stage_plan) |sp| if (!sp.base[ci]) continue;
        if (dec.payload) |p| if (p[ci] != late) continue;
        if (late and !filtered) {
            const first: filter_eval.Batch = .{ .cols = batch_cols.items, .num_rows = batch_rows, .expr_batch_rows = ws.expr_batch_rows };
            try filter_eval.evaluate(in.filter.?, &first, &sel, lookup, ra);
            filtered = true;
            dec.gate = &sel;
        }
        lookup[ci] = batch_cols.items.len;
        const col = dec.column(ra, ci, &sel_pre) catch |err| {
            // Values that expand past the budget read whole (a footer can understate them by any factor) may still
            // stream a slice at a time. Nothing is aggregated before every column decodes, so the row group starts
            // over; its reservation and what it decoded go back first.
            const b = ws.budget orelse return err;
            const refused = refusedDraw(ws, err);
            if ((err != error.DecodeExceededBudget and !refused) or staged) return err;
            b.release(held);
            held = 0;
            if (refused) dropRegion(ws) else {
                ws.rg_arena.nextRowGroup();
                if (ws.region) |r| r.trim();
            }
            // No record has been emitted: release the failed whole-decode payload before rereading it in slices.
            if (target == .grouped) if (target.grouped.records) |records| records.rewindRetained();
            if (try scanRGSliced(gpa, in, decode_options, target, ws, b, handled_via_stats, full_estimate)) |kept|
                return kept;
            return err;
        };
        try batch_cols.append(ra, col);
    };
    const t_decode_end = nowMonoNs();
    ws.timings.decode_ns += @intCast(t_decode_end - t_decode_start);
    if (prof) |t| {
        t.record(.prune_pages, prune_span.start, t_decode_start, profile.args(.{ dec.pages_total, dec.pages_skipped }));
        const decode_args = profile.args(.{ in.file_index, in.rg_index, batch_cols.items.len });
        t.record(.decode, t_decode_start, t_decode_end, decode_args);
    }

    if (target == .grouped and target.grouped.records == null and !staged and decode_options.encoded)
        try agg_coded.codePlainKeys(ra, target.grouped.keys, batch_cols.items, lookup);
    const batch: filter_eval.Batch = .{
        .cols = batch_cols.items,
        .num_rows = batch_rows,
        .expr_batch_rows = ws.expr_batch_rows,
    };
    if (in.filter) |f| if (!filtered) try filter_eval.evaluate(f, &batch, &sel, lookup, ra);
    // A staged plan's call columns decode below, after the filter: payload among them is gated the same way.
    if (dec.payload != null) dec.gate = &sel;
    const t_eval_end = nowMonoNs();
    ws.timings.eval_ns += @intCast(t_eval_end - t_decode_end);
    const selected: usize = if (in.filter != null) sel.count() else num_rows;
    profile.record(prof, .filter, t_decode_end, t_eval_end, profile.args(.{ batch_rows, selected }));

    switch (target) {
        .grouped => |g| {
            if (g.records) |records| {
                try records.batch(batch, lookup, 0, &sel);
                const end = nowMonoNs();
                ws.timings.encode_ns += @intCast(end - t_eval_end);
                profile.record(prof, .aggregate, t_eval_end, end, profile.args(.{ selected, calls.len }));
                return selected;
            }
            if (g.part != null and !staged) {
                try foldGroupedWindows(
                    ra,
                    gpa,
                    target,
                    batch_cols.items,
                    lookup,
                    &sel,
                    in.consumed,
                    decode_options.encoded,
                    ws.expr_batch_rows,
                    ws.budget,
                    ws.region,
                );
                const t_agg_end = nowMonoNs();
                ws.timings.encode_ns += @intCast(t_agg_end - t_eval_end);
                profile.record(prof, .aggregate, t_eval_end, t_agg_end, profile.args(.{ selected, calls.len }));
                return selected;
            }
            const dense = if (g.dense) |d| !staged and decode_options.encoded and
                try d.foldKeys(
                    ra,
                    gpa,
                    g.keys,
                    in.file_index,
                    in.rg_index,
                    g.pos_base,
                    &batch,
                    lookup,
                    &sel,
                ) else false;
            if (dense or (!staged and decode_options.encoded and
                try agg_coded.groupedCoded(ra, gpa, g.calls, g.keys, g.table, g.pos_base, &batch, lookup, &sel, ws.budget, ws.region)))
            {
                const t_agg_end = nowMonoNs();
                ws.timings.encode_ns += @intCast(t_agg_end - t_eval_end);
                profile.record(prof, .aggregate, t_eval_end, t_agg_end, profile.args(.{ selected, calls.len }));
                return selected;
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

            const group_id_arr = try ra.alloc(u32, batch_rows);
            @memset(group_id_arr, 0);

            // Never cached across row groups: the resolver's memo holds pointers into `ra`.
            var key_resolver = expr_agg.GroupKeyResolver.init(key_cols);
            defer key_resolver.deinit(ra);

            try key_resolver.resolveBatch(ra, g.table, key_cols, g.calls, g.pos_base, &sel, group_id_arr);
            try expr_agg.sinkSketchRows(g.table, group_id_arr, &sel);

            // A sketch's count pass has read no aggregate's columns, and has no group to fold into.
            if (!g.table.countOnly()) for (g.calls, 0..) |_, i| {
                var call_arena = std.heap.ArenaAllocator.init(side);
                defer call_arena.deinit();
                const ca = if (staged) call_arena.allocator() else ra;
                const cb = if (stage_plan) |sp|
                    try dec.withCallColumns(ca, sp, i, &batch, lookup, &sel_pre, true)
                else
                    CallBatch{ .batch = batch, .lookup = lookup };
                try expr_agg.updateTableGrouped(
                    ca,
                    gpa,
                    g.table,
                    group_id_arr,
                    i,
                    g.calls,
                    &cb.batch,
                    cb.lookup,
                    &sel,
                );
            };
            // The windowed path returned above. A staged row group folded whole still has to enter the partitions
            // before the next row group, or one worker's local table keeps every key it has seen.
            if (g.part) |p| if (g.local) |local| try p.pump(local, g.table, @intCast(sel.count()));
        },
        .scalar => |s| scalar: {
            // Update only the aggs that weren't handled via stats. The agg's
            // own per-WHERE predicate (if any) is composed inside updateOne.
            if (!staged and try expr_agg.updateSharedInteger(
                ra,
                s.calls,
                s.accumulators,
                handled_via_stats,
                &batch,
                lookup,
                &sel,
            )) break :scalar;
            for (s.calls, s.accumulators, handled_via_stats, 0..) |call, *acc, handled, i| {
                if (handled) continue;
                var call_arena = std.heap.ArenaAllocator.init(side);
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
    return selected;
}

/// `scanRGForAgg`'s decode and fold, a slice of rows at a time (`SliceReader`), each slice on memory of its own:
/// for a row group too big for a worker's share of `budget`. The rows the outer filter kept; null, having done
/// nothing, when some column has no sliced form.
/// The rows of a row group its filter keeps, reading only the columns before the payload a slice at a time.
fn filterSurvivors(
    a: std.mem.Allocator,
    side: std.mem.Allocator,
    in: AggRowGroup,
    decode_options: DecodeOptions,
    payload: []const bool,
    num_rows: usize,
    ws: AggWorkspace,
) !?filter_selection.SelectionVector {
    var survivors = try filter_selection.SelectionVector.init(a, num_rows);
    var readers: std.ArrayList(SliceReader) = .empty;
    defer for (readers.items) |*r| r.deinit();
    const lookup = try a.alloc(?usize, in.meta.schema.items.len);
    @memset(lookup, null);
    for (in.read, payload, 0..) |needed, late, ci| {
        if (!needed or late or ci >= in.rg.columns.items.len) continue;
        const r = try SliceReader.init(a, side, in.rg, in.meta, in.src, ci, decode_options) orelse return null;
        lookup[ci] = readers.items.len;
        try readers.append(a, r);
    }
    var slice = std.heap.ArenaAllocator.init(side);
    defer slice.deinit();
    // A memory granule over the filter's own columns; it never changes which rows survive.
    const step: usize = 1 << 16;
    var done: usize = 0;
    while (done < num_rows) : (done += step) {
        _ = slice.reset(.retain_capacity);
        const sa = slice.allocator();
        const n = @min(step, num_rows - done);
        const cols = try sa.alloc(filter_eval.Batch.Column, readers.items.len);
        for (readers.items, cols) |*r, *c| c.* = try r.next(sa, n);
        const batch: filter_eval.Batch = .{ .cols = cols, .num_rows = n, .expr_batch_rows = ws.expr_batch_rows };
        var sel = try filter_selection.SelectionVector.init(sa, n);
        try filter_eval.evaluate(in.filter.?, &batch, &sel, lookup, sa);
        for (0..n) |i| if (!sel.isActive(i)) survivors.set(done + i, false);
    }
    return survivors;
}

fn scanRGSliced(
    gpa: std.mem.Allocator,
    in: AggRowGroup,
    decode_options: DecodeOptions,
    target: AggTarget,
    ws: AggWorkspace,
    budget: *DecodeBudget,
    handled: []const bool,
    full: usize,
) !?u64 {
    const num_rows: usize = @intCast(@max(0, in.rg.num_rows));
    if (num_rows == 0) return null;
    const side = ws.charged orelse gpa;
    var pages = std.heap.ArenaAllocator.init(side);
    defer pages.deinit();
    var readers: std.ArrayList(SliceReader) = .empty;
    defer for (readers.items) |*r| r.deinit();
    const lookup = try pages.allocator().alloc(?usize, in.meta.schema.items.len);
    @memset(lookup, null);
    // The columns the whole-chunk path reads as levels only (`decodeLevelsOnly`), read so here too.
    const uses = try planCodeUses(pages.allocator(), in, target, false);
    const levels_only = try pages.allocator().alloc(bool, in.read.len);
    for (levels_only, 0..) |*l, ci| l.* = decode_options.encoded and target == .scalar and ci < uses.levels.len and
        uses.levels[ci] and !uses.code[ci] and !uses.value[ci];
    // Payload columns are gated by the filter's survivors over the whole row group, as when it decodes whole.
    const payload = try payloadColumns(pages.allocator(), in, target);
    var survivors: filter_selection.SelectionVector = undefined;
    if (payload) |p| survivors = (try filterSurvivors(pages.allocator(), side, in, decode_options, p, num_rows, ws)) orelse
        return null;
    for (in.read, 0..) |needed, ci| {
        if (!needed or ci >= in.rg.columns.items.len) continue;
        // The first pass already validated and evaluated filter-only columns. Keep only the values the consumer
        // reads; a column shared by the filter and an aggregate still needs its value lane here.
        if (payload != null and ci < in.consumed.len and !in.consumed[ci]) continue;
        var options = decode_options;
        options.any_dictionary = target.codedKey(ci);
        options.decimal_exact_only = target.exactInputs();
        if (payload) |p| if (p[ci]) {
            options.row_gate = &survivors;
        };
        var r = try SliceReader.init(pages.allocator(), side, in.rg, in.meta, in.src, ci, options) orelse
            return null;
        if (levels_only[ci]) r.levelsOnly();
        if (target == .grouped) if (target.grouped.records) |records| if (records.retention(ci)) |owner| {
            r.retainPayloads(owner);
            r.raw.bytes.pages.prof = ws.timings.prof;
        };
        lookup[ci] = readers.items.len;
        try readers.append(pages.allocator(), r);
    }
    const per_row = @max(1, full / num_rows);
    // Half of what the ledger has left, so another worker's slice still fits beside this one; slicing changes no
    // answer, only how many rows decode at once.
    const room = if (ws.ledger) |l| l.sliceAvailable() / 2 else std.math.maxInt(usize);
    const discovering = target == .grouped and target.grouped.table.discovery != null;
    const forced = if (ws.decode_batch_rows != 0 and (discovering or cacheExpressionsSafe(target)))
        ws.decode_batch_rows
    else
        std.math.maxInt(usize);
    const slice_rows = sliceRows(@min(@min(budget.share(), room) / per_row, forced), num_rows);
    const reserved = @min(budget.limit, slice_rows * per_row);
    try budget.acquire(reserved);
    defer budget.release(reserved);

    const prof = ws.timings.prof;
    const t0 = nowMonoNs();
    var done: usize = 0;
    // Under a ledger, slices are cut by what rows took, not the footer's estimate, which can understate values by any
    // factor (DELTA_BYTE_ARRAY copies a prefix into every one): the first slice is the least, each next is what the
    // reservation, or the ledger's room, holds at the bytes a row of the last one took.
    var rows_now = if (ws.ledger != null) @min(slice_rows, 64) else slice_rows;
    // One arena for every slice, its pages kept from one to the next rather than faulted in afresh; what a row took is
    // measured on the first, when the arena holds nothing else. The worker's own row-group arena when it is a region
    // (nothing is on it by now: the row group starts over here), whose blocks are what the slice asked for; an
    // `ArenaAllocator` reserves past that, and its reservation was charged as if used.
    const on_region = ws.region != null and ws.rg_arena.region != null;
    var slice = std.heap.ArenaAllocator.init(side);
    defer slice.deinit();
    var took: ?usize = null;
    var matched: u64 = 0;
    while (done < num_rows) {
        const n = @min(rows_now, num_rows - done);
        defer done += n;
        if (on_region) ws.rg_arena.nextRowGroup() else _ = slice.reset(.retain_capacity);
        defer if (ws.ledger) |l| {
            const kept = if (on_region) ws.region.?.end else slice.queryCapacity();
            if (took == null) took = @max(1, kept / n);
            rows_now = sliceRows(@min(@min(reserved, l.sliceAvailable() / 2 + kept) / took.?, forced), num_rows);
        };
        const sa = if (on_region) ws.rg_arena.allocator() else slice.allocator();
        const cols = try sa.alloc(filter_eval.Batch.Column, readers.items.len);
        for (readers.items, cols) |*r, *c| c.* = r.next(sa, n) catch |err| {
            // A record slice may read pages that the ordinary indexed plan skips. Replay establishes its error.
            if (in.filter != null and target == .grouped and target.grouped.records != null and isDecodeError(err))
                return error.RetainedEncodingUnsupported;
            return err;
        };
        const batch: filter_eval.Batch = .{ .cols = cols, .num_rows = n, .expr_batch_rows = ws.expr_batch_rows };
        var sel = if (payload != null)
            try survivors.alignedWindow(done, n)
        else
            try filter_selection.SelectionVector.init(sa, n);
        if (payload == null) if (in.filter) |f| try filter_eval.evaluate(f, &batch, &sel, lookup, sa);
        matched += if (in.filter != null) sel.count() else n;
        switch (target) {
            .scalar => |s| {
                if (ws.decode_batch_rows != 0 and cacheExpressionsSafe(target) and
                    try foldScalarCache(sa, gpa, s.calls, s.accumulators, handled, &batch, lookup, &sel)) continue;
                if (try expr_agg.updateSharedInteger(sa, s.calls, s.accumulators, handled, &batch, lookup, &sel))
                    continue;
                for (s.calls, s.accumulators, handled) |call, *acc, h| {
                    if (h) continue;
                    try expr_agg.updateOne(sa, gpa, acc, call, &batch, lookup, &sel);
                }
            },
            .grouped => |g| {
                if (g.records) |records| {
                    const emit_start = nowMonoNs();
                    try records.batch(batch, lookup, done, &sel);
                    if (records.* == .msd) profile.record(prof, .msd_emit, emit_start, nowMonoNs(), profile.args(.{}));
                    continue;
                }
                if (g.part != null) {
                    var shifted = target;
                    shifted.grouped.pos_base = g.pos_base + @as(u64, @intCast(done));
                    try foldGroupedWindows(
                        sa,
                        gpa,
                        shifted,
                        cols,
                        lookup,
                        &sel,
                        in.consumed,
                        decode_options.encoded,
                        ws.expr_batch_rows,
                        ws.budget,
                        null,
                    );
                    continue;
                }
                if (!discovering and decode_options.encoded) if (g.dense) |d| {
                    if (try d.foldKeys(
                        sa,
                        gpa,
                        g.keys,
                        in.file_index,
                        in.rg_index,
                        g.pos_base + done,
                        &batch,
                        lookup,
                        &sel,
                    )) continue;
                };
                if (!discovering and decode_options.encoded and ws.decode_batch_rows != 0 and
                    cacheExpressionsSafe(target))
                {
                    try agg_coded.codePlainKeys(sa, g.keys, cols, lookup);
                    if (try agg_coded.groupedCoded(
                        sa,
                        gpa,
                        g.calls,
                        g.keys,
                        g.table,
                        g.pos_base + done,
                        &batch,
                        lookup,
                        &sel,
                        ws.budget,
                        null,
                    )) continue;
                }
                // A refused dictionary product may leave one key coded and another PLAIN. The ordinary key
                // resolver reads value lanes, so materialize before crossing that fallback boundary.
                for (cols) |*col| col.* = try agg_coded.materialize(sa, col.*);
                const key_cols = try sa.alloc(filter_eval.Batch.Column, g.keys.len);
                const keyed = batch.restrictedTo(&sel);
                for (g.keys, key_cols) |k, *kc| kc.* = try expr_eval.evalGroupKeyExpr(sa, &keyed, lookup, k);
                const ids = try sa.alloc(u32, n);
                @memset(ids, 0);
                var resolver = expr_agg.GroupKeyResolver.init(key_cols);
                defer resolver.deinit(sa);
                // First appearance counts rows from the row group's start, as when it decodes whole.
                try resolver.resolveBatch(sa, g.table, key_cols, g.calls, g.pos_base + done, &sel, ids);
                try expr_agg.sinkSketchRows(g.table, ids, &sel);
                if (!g.table.countOnly()) for (g.calls, 0..) |_, i| {
                    try expr_agg.updateTableGrouped(sa, gpa, g.table, ids, i, g.calls, &batch, lookup, &sel);
                };
            },
        }
        if (discovering and target.grouped.table.discoveryDone(target.grouped.pos_base + done + n)) break;
    }
    const t1 = nowMonoNs();
    ws.timings.decode_ns += @intCast(t1 - t0);
    profile.record(prof, .decode, t0, t1, profile.args(.{ in.file_index, in.rg_index, readers.items.len }));
    return matched;
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
        const base = try baseColumns(a, in, target);
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

/// The columns of `in.read` a row group needs before its filter has run: the filter's own, and the group keys.
fn baseColumns(a: std.mem.Allocator, in: AggRowGroup, target: AggTarget) ![]bool {
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
    return base;
}

/// Payload columns: what a filtered row group reads past `baseColumns`, flat. Every decode path (whole, compact,
/// staged, sliced) decodes them after the filter, gated by its survivors (`DecodeOptions.row_gate`), so which of their
/// pages decode depends on the input and the query alone. Null when there are none.
fn payloadColumns(a: std.mem.Allocator, in: AggRowGroup, target: AggTarget) !?[]const bool {
    if (in.filter == null) return null;
    if (target == .grouped and target.grouped.records != null) return null;
    const base = try baseColumns(a, in, target);
    var any = false;
    for (base, in.read, 0..) |*b, r, ci| {
        const flat = ci < in.rg.columns.items.len and if (in.rg.columns.items[ci].meta_data) |cm|
            in.meta.getColumnLevels(cm.path_in_schema.items).max_rep == 0
        else
            false;
        b.* = r and !b.* and flat;
        any = any or b.*;
    }
    return if (any) base else null;
}

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
    /// Decode just these rows (`compactRows`); `rows_out` is their count.
    keep: ?[]const RowRange = null,
    rows_out: ?usize = null,
    /// `payloadColumns`, gated by `gate` (the filter's survivors, in batch rows) once the filter has run.
    payload: ?[]const bool = null,
    gate: ?*const filter_selection.SelectionVector = null,
    pages_total: usize = 0,
    pages_skipped: usize = 0,

    fn column(
        self: *ChunkDecoder,
        a: std.mem.Allocator,
        ci: usize,
        sel: *const filter_selection.SelectionVector,
    ) !filter_eval.Batch.Column {
        const in = self.in;
        enterChunk(in.rg, ci);
        const prof = self.ws.timings.prof;
        const cm = in.rg.columns.items[ci].meta_data orelse return error.ColumnMetaMissing;
        const start: usize = if (cm.dictionary_page_offset) |dp| @intCast(dp) else @intCast(cm.data_page_offset);
        const len: usize = @intCast(cm.total_compressed_size);
        invariant.assert(start >= @as(usize, @intCast(in.src.byte_origin)));
        const buf_off = start - @as(usize, @intCast(in.src.byte_origin));
        if (buf_off + len > in.src.bytes.len) return error.MissingChunkBytes;
        const chunk = in.src.bytes[buf_off .. buf_off + len];

        const chunk_span = profile.Span.begin(prof);
        var prune_info: ?PruningInfo = if (self.page_indexes[ci]) |pi|
            try pagePruning(a, pi, self.filter_pages, ci, if (in.preserve) |p| p[ci] else true, sel, self.num_rows, cm, start)
        else
            null;
        if (prune_info) |*p| p.keep = self.keep;
        // Encoded execution on whole chunks: a column only counted
        // reads its levels; one only bare aggregates read, over every row, folds straight into a histogram of its
        // dictionary codes; one filters, keys or aggregates read decodes to codes they evaluate once per entry.
        const uses = self.uses;
        var decode_options = column_mod.chunkOptions(self.decode_options, in.meta, cm);
        if (self.payload) |p| if (p[ci]) {
            std.debug.assert(self.gate != null);
            decode_options.row_gate = self.gate;
        };
        decode_options.any_dictionary = self.target.codedKey(ci);
        decode_options.decimal_exact_only = self.target.exactInputs();
        const pruned_pages = if (prune_info) |p|
            std.mem.indexOfScalar(bool, p.page_is_skipped, true) != null
        else
            false;
        if (self.target == .grouped) if (self.target.grouped.records) |records| if (records.retention(ci)) |owner| {
            var reader = (try SliceReader.init(a, a, in.rg, in.meta, in.src, ci, decode_options)).?;
            defer reader.deinit();
            reader.retainPayloads(owner);
            reader.raw.bytes.pages.prof = prof;
            const decoded = reader.next(a, self.num_rows) catch |err| {
                // Retention reads complete pages. The ordinary pruned replay decides whether a corrupt page matters.
                if (pruned_pages and isDecodeError(err)) return error.RetainedEncodingUnsupported;
                return err;
            };
            try checkRowShape(decoded, self.num_rows);
            if (chunk_span.t != null) chunk_span.end(.column_chunk, chunkArgs(in.file_index, in.rg_index, ci, cm));
            return decoded;
        };
        const scratch = self.ws.scratch;
        const packed_codes = self.target == .grouped and if (self.target.grouped.records) |r|
            r.* == .integer_count_top and r.integer_count_top.packing != null
        else
            false;
        // Packed owners need the original dictionary. Read complete codes and apply the survivor mask at the sink;
        // pruned replay remains authoritative for failures on pages the query does not need.
        const keyed_whole = decode_options.any_dictionary and self.keep == null and
            (!pruned_pages or packed_codes);
        const encoded = decode_options.encoded and (prune_info == null or keyed_whole) and ci < uses.code.len and
            decode_options.row_gate == null;
        const levels_only = encoded and uses.levels[ci] and !uses.code[ci] and !uses.value[ci] and
            self.target == .scalar;
        const firsts = self.target == .grouped;
        const hist_col: ?filter_eval.Batch.Column = if (encoded and !levels_only and uses.code[ci] and
            uses.hist_ok[ci] and !uses.value[ci])
            try decodeAggColumnHist(a, chunk, cm, in.meta, decode_options, scratch, self.num_rows, firsts)
        else
            null;
        var read_whole_codes = false;
        var decoded = if (hist_col) |hc|
            hc
        else if (levels_only)
            try decodeLevelsOnly(a, chunk, cm, in.meta, decode_options, scratch, self.num_rows)
        else if (encoded and (uses.code[ci] or uses.levels[ci])) coded: {
            const col = decodeAggColumnCoded(a, chunk, cm, in.meta, decode_options, scratch) catch |err| {
                if (!packed_codes or !pruned_pages or !isDecodeError(err)) return err;
                enterChunk(in.rg, ci);
                break :coded try decodeAggColumn(a, chunk, cm, in.meta, prune_info, decode_options, scratch);
            };
            read_whole_codes = col != null;
            break :coded col orelse try decodeAggColumn(a, chunk, cm, in.meta, prune_info, decode_options, scratch);
        } else try decodeAggColumn(a, chunk, cm, in.meta, prune_info, decode_options, scratch);
        if (self.target == .grouped and self.target.grouped.table.countOnly() and !in.consumed[ci] and decoded.codes() != null) {
            std.debug.assert(!uses.value[ci]);
        }
        if (ci < uses.value.len and uses.value[ci]) decoded = try agg_coded.materialize(a, decoded);
        try checkRowShape(decoded, self.rows_out orelse self.num_rows);
        if (chunk_span.t != null) {
            chunk_span.end(.column_chunk, chunkArgs(in.file_index, in.rg_index, ci, cm));
            if (prune_info) |pi| {
                self.pages_total += pi.page_is_skipped.len;
                if (!read_whole_codes) self.pages_skipped += std.mem.count(bool, pi.page_is_skipped, &.{true});
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
/// Rows per slice of a row group read `fit` rows at a time: a multiple of 64, so every slice starts on a block of
/// the float sum order (`agg.sumBlocksF64`) and slicing changes no bit of a float sum or avg.
fn sliceRows(fit: usize, num_rows: usize) usize {
    return @min(num_rows, @max(64, fit / 64 * 64));
}

const CodeUses = struct { code: []bool, value: []bool, levels: []bool, hist_ok: []bool };

fn planCodeUses(ra: std.mem.Allocator, in: AggRowGroup, target: AggTarget, staged: bool) !CodeUses {
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
    // So does a lone GROUP BY key that only counts read, its groups resolved from the histogram
    // (`agg_coded.groupedCoded`). A staged, windowed, sketched or owned-record fold needs per-row identities.
    if (in.filter == null and !staged) switch (target) {
        .grouped => |g| if (g.records == null and g.part == null and g.table.sketch == null and
            g.table.discovery == null)
        {
            if (agg_coded.histGroupKey(g.calls, g.keys)) |key| if (key < n) {
                u.hist_ok[key] = true;
            };
        },
        .scalar => {},
    };
    if (target == .scalar) for (target.calls()) |call| {
        if (call.where) |w| markFilterSet(u.hist_ok, w, false);
        if (call.arg) |arg| {
            if (call.where != null or arg != .col_ref or agg_coded.floatSum(call)) markExprSet(u.hist_ok, arg, false);
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
        .case_ => |c| {
            for (c.whens) |w| {
                markFilterSet(set, w.cond, v);
                if (w.then) |t| markExprSet(set, t.*, v);
            }
            if (c.else_) |other| markExprSet(set, other.*, v);
        },
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
        .decimal => |l| mark(u.code, l.col_idx),
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
        // A condition reads values too: the CASE evaluates it over the row values it gives its branches.
        .case_ => |c| {
            for (c.whens) |w| {
                markFilterSet(u.value, w.cond, true);
                if (w.then) |t| markExprValues(u, t.*);
            }
            if (c.else_) |other| markExprValues(u, other.*);
        },
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
    const dec_kind: ?decimal_mod.Kind = if (schema_elem) |se| decimal_mod.exactKind(&se) else null;
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
/// then decodes it afresh). Decimal and unsigned-INT32 dictionaries convert once per entry. `firsts` (a GROUP BY key,
/// `agg_coded.histGroupKey`) also notes each entry's first row, for keys typed exactly as their codes decode.
fn decodeAggColumnHist(
    ra: std.mem.Allocator,
    chunk: []const u8,
    cm: schema.ColumnMetaData,
    meta: *const schema.FileMetaData,
    decode_options: DecodeOptions,
    scratch: *DecodeScratch,
    num_rows: usize,
    firsts: bool,
) !?filter_eval.Batch.Column {
    const levels = meta.getColumnLevels(cm.path_in_schema.items);
    if (levels.max_rep > 0) return null;
    const n_leaves: usize = @intCast(cm.num_values);
    const schema_elem = meta.getColumnSchema(cm.path_in_schema.items);
    const dec_kind: ?decimal_mod.Kind = if (schema_elem) |se| decimal_mod.exactKind(&se) else null;
    if (firsts) {
        if (dec_kind != null) return null;
        // The reader declines a key chunk at its first null row, after allocating and folding up to it, and the chunk
        // then decodes afresh; so only try a nullable column whose statistics count no nulls (a wrong count costs
        // that retry, never an answer).
        if (levels.max_def > 0) {
            const nulls = if (cm.statistics) |st| st.null_count else null;
            if (nulls == null or nulls.? != 0) return null;
        }
        switch (cm.type) {
            .INT64, .BYTE_ARRAY => {},
            .INT32 => if (schema_elem != null and schema.isUnsignedIntTo32(schema_elem.?)) return null,
            else => return null,
        }
    }
    if (dec_kind) |k| {
        return switch (k.physical) {
            inline .INT32, .INT64 => |p| blk: {
                const T = if (p == .INT32) i32 else i64;
                const r = (try histOf(T, ra, chunk, cm.codec, levels, n_leaves, decode_options, scratch, firsts)) orelse
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
            const r = (try histOf(i32, ra, chunk, cm.codec, levels, n_leaves, decode_options, scratch, firsts)) orelse
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
            const r = (try histOf(T, ra, chunk, cm.codec, levels, n_leaves, decode_options, scratch, firsts)) orelse
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
        .hist_first = h.first,
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
    firsts: bool,
) !?column_mod.ColumnChunkReader(T).HistOrValues {
    var reader = column_mod.ColumnChunkReader(T).initWithOptions(chunk, codec, levels, ra, decode_options, scratch);
    return reader.readAllHistogram(n_leaves, firsts);
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
        const cm = chunk_meta.meta_data orelse continue;
        const levels = in.meta.getColumnLevels(cm.path_in_schema.items);
        const elem = in.meta.getColumnSchema(cm.path_in_schema.items);
        if (!statistics.pageIndexWriterUsable(&col_index, cm, elem, in.meta.created_by)) continue;
        const trust: statistics.Trust = if (stats == .trust) .plausible else .corroborated;
        const rows = in.rg.num_rows;
        if (!statistics.pageIndexIsPlausible(&col_index, &offset_index, cm, elem, levels, rows, trust)) continue;

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
                    sel.clearRange(start, end);
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

            if (!sel.anyInRange(start_idx, end_idx)) s[page_idx] = true;
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
    const dec_kind: ?decimal_mod.Kind = if (schema_elem) |se| decimal_mod.exactKind(&se) else null;

    if (dec_kind) |k| return decimal_mod.decodeAggregateColumn(
        ra,
        chunk,
        cm.codec,
        levels,
        n_leaves,
        k,
        decode_options,
        scratch,
    );
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
            decode_options.page_bytes_limit,
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
/// chunk's codec, or stripping affected page-header statistics, takes this path too, with every column kept.
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
    // Changed codecs or untrusted page-header statistics can change chunk sizes.
    var all_kept: ?[]bool = null;
    if (kept_set == null) {
        for (rg.columns.items) |*chunk| {
            const m = chunk.meta_data orelse return error.InvalidColumnOffsets;
            const changes_codec = if (recompress) |rc| m.codec != rc.codec else false;
            if (!changes_codec and fastpath.writerPolicy(chunk, bloom_write.meta) == .trust) continue;
            all_kept = try out_arena.alloc(bool, rg.columns.items.len);
            @memset(all_kept.?, true);
            break;
        }
    }
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
            const safe = fastpath.writerSafeChunk(out_arena, src.bytes, &rg.columns.items[ci], bloom_write.meta);
            const src_chunk = &safe;
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
                rg_total += written.meta_data.?.total_uncompressed_size;
                continue;
            };
            if (m.writer_stats != .trust) {
                var timed: TimedCopyOut = .{ .out = &out, .timings = timings };
                var copied = try fastpath.copyPolicyChunk(out_arena, &timed, src.bytes, src_chunk, emit_page_index);
                try requestedFilter(out_arena, &copied, rg, ci, src, bloom_write);
                try new_cols.append(out_arena, copied);
                rg_total += copied.meta_data.?.total_uncompressed_size;
                continue;
            }
            const delta = offsetDelta(out.pos(), range.start);
            try timedWrite(&out, slice, timings);
            // Carry the page index forward, written right after this chunk's data.
            var copied = try carryChunk(out_arena, &out, src, src_chunk, delta, emit_page_index);
            try requestedFilter(out_arena, &copied, rg, ci, src, bloom_write);
            try new_cols.append(out_arena, copied);
            rg_total += m.total_uncompressed_size;
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
        const safe = fastpath.writerSafeChunk(out_arena, src.bytes, chunk, bloom_write.meta);
        var copied = try carryChunk(out_arena, &out, src, &safe, delta, emit_page_index);
        try requestedFilter(out_arena, &copied, rg, ci, src, bloom_write);
        try cols.append(out_arena, copied);
    }

    return .{
        .surviving_rows = rg.num_rows,
        .rg = .{
            .columns = cols,
            .total_byte_size = fastpath.uncompressedBytes(cols.items),
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
    const explicit = bloom_write.at(ci);
    if (explicit == null and !bloom_write.dictionary_bloom) return;
    const meta = bloom_write.meta orelse return;
    var scratch = std.heap.ArenaAllocator.init(bloom_write.scratch orelse return);
    defer scratch.deinit();
    const a = scratch.allocator();
    const chunk = &rg.columns.items[ci];
    const cm = chunk.meta_data orelse return;
    const range = fastpath.chunkRange(&cm, std.math.maxInt(usize)) orelse return error.InvalidColumnOffsets;
    const bytes = try sourceBytes(src, range);
    const levels = meta.getColumnLevels(cm.path_in_schema.items);
    if (explicit == null) {
        if (cm.dictionary_page_offset == null) return;
        const filter = try dictionaryBloom(a, bytes, cm, levels, meta) orelse return;
        copied.bloom_out = try out_arena.dupe(u8, filter);
        return;
    }
    const fpp = explicit.?;
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

/// Verify every data page uses the dictionary before filtering its entries; mixed chunks must never omit values.
fn dictionaryBloom(
    a: std.mem.Allocator,
    bytes: []const u8,
    cm: schema.ColumnMetaData,
    levels: schema.Levels,
    meta: *const schema.FileMetaData,
) !?[]u8 {
    inline for (.{ i32, i64, f32, f64, []const u8 }) |T| {
        const matches = switch (T) {
            i32 => cm.type == .INT32,
            i64 => cm.type == .INT64,
            f32 => cm.type == .FLOAT,
            f64 => cm.type == .DOUBLE,
            []const u8 => cm.type == .BYTE_ARRAY or cm.type == .FIXED_LEN_BYTE_ARRAY or cm.type == .INT96,
            else => unreachable,
        };
        if (matches) {
            var reader = column_mod.ColumnChunkReader(T).initWithOptions(
                bytes,
                cm.codec,
                levels,
                a,
                column_mod.chunkOptions(.{}, meta, cm),
                null,
            );
            if (T == []const u8) {
                if (cm.type == .INT96) reader.type_length = int96_mod.INT96_WIDTH else if (cm.type == .FIXED_LEN_BYTE_ARRAY)
                    reader.type_length = flbaWidth(meta.getColumnSchema(cm.path_in_schema.items));
            }
            const entries = try reader.dictionaryOfEveryPage() orelse return null;
            if (T == []const u8 and cm.type == .INT96) for (entries) |entry| {
                // Noncanonical times can equal another day's timestamp; a reader may hash that canonical form.
                const nanos = std.mem.readInt(i64, entry[0..8], .little);
                if (nanos < 0 or nanos >= std.time.ns_per_day) return null;
            };
            const tag = if (T == []const u8) "string" else @typeName(T);
            const col = @unionInit(filter_eval.Batch.Column, tag, .{ .values = entries });
            // INT96 PLAIN is 12 raw bytes, hashed as a fixed byte array, without converting its timestamp.
            const hash_type = if (cm.type == .INT96) .FIXED_LEN_BYTE_ARRAY else cm.type;
            return encoder.bloomFilter(a, col, hash_type, 0.01);
        }
    }
    return null;
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

const TimedCopyOut = struct {
    out: *streaming.SinkOut,
    timings: *Timings,

    pub fn pos(t: *const TimedCopyOut) usize {
        return t.out.pos();
    }

    pub fn write(t: *TimedCopyOut, bytes: []const u8) !void {
        try timedWrite(t.out, bytes, t.timings);
    }
};

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
    dictionary_bloom: bool = true,
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
/// valid page statistics included, is carried over unchanged. Known writer defects remove affected records.
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
    limit: usize,
    writer_policy: WriterStatsPolicy,
) !RecompressedChunk {
    var out: std.ArrayList(u8) = .empty;
    var pages: std.ArrayList(PageMove) = .empty;
    var raw: std.ArrayList(u8) = .empty;
    defer raw.deinit(scratch);
    var header_delta: i64 = 0;
    // The decoder's bound on the pages' claims (`column.chunkOptions`): a claim sizes the buffer it inflates into.
    var inflated: usize = 0;
    var pos: usize = 0;
    while (pos < chunk.len) {
        var r = thrift.Reader.init(chunk[pos..]);
        const h = try schema.PageHeader.read(&r);
        const hdr_len = r.pos;
        if (h.compressed_page_size < 0 or h.uncompressed_page_size < 0) return error.BadPageHeader;
        const body_len: usize = @intCast(h.compressed_page_size);
        if (body_len > chunk.len - pos - hdr_len) return error.TruncatedPage;
        const body = chunk[pos + hdr_len ..][0..body_len];
        inflated +|= @intCast(h.uncompressed_page_size);
        if (inflated > limit) return error.PageLargerThanChunk;
        const new_body = try recodePage(arena, scratch, &raw, &h, body, from, to, zstd);
        const raw_header = chunk[pos..][0..hdr_len];
        const new_hdr = try header_writer.rewrite(arena, raw_header, @intCast(new_body.len), writer_policy);
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
    limit: usize,
    writer_policy: WriterStatsPolicy,
) !RecompressedChunk {
    var pages: std.ArrayList(PageMove) = .empty;
    var raw: std.ArrayList(u8) = .empty;
    defer raw.deinit(scratch);
    var header_delta: i64 = 0;
    var inflated: usize = 0;
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
        inflated +|= @intCast(h.uncompressed_page_size);
        if (inflated > limit) return error.PageLargerThanChunk;
        const new_body = try recodePage(pa, scratch, &raw, &h, body, from, to, zstd);
        const new_hdr = try header_writer.rewrite(pa, chunk[pos..][0..hdr_len], @intCast(new_body.len), writer_policy);
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
    try compression.decompressInto(body, from, raw.items);
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
        const bytes = try sourceBytes(src, range);
        const limit = column_mod.inflateLimit(m);
        slot.* = recompressChunk(arena, scratch, bytes, m.codec, codec, zstd, limit, m.writer_stats) catch |err|
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
        const limit = column_mod.inflateLimit(m);
        var inflated: usize = 0;
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
            inflated +|= @intCast(h.uncompressed_page_size);
            if (inflated > limit) {
                parse_err = .{ .column = ci, .err = error.PageLargerThanChunk };
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
            const policy = rg.columns.items[ci].meta_data.?.writer_stats;
            const new_hdr = try header_writer.rewrite(arena, t.hdr, @intCast(t.out.len), policy);
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
        const limit = column_mod.inflateLimit(m);
        const r = recompressChunkTo(
            scratch,
            rc.scratch,
            slice,
            m.codec,
            rc.codec,
            rc.zstd,
            out,
            timings,
            limit,
            m.writer_stats,
        ) catch |err|
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
    /// Decode only these rows, in order, into a column of their total length (`compactRows`).
    keep: ?[]const RowRange = null,
};

/// Rows `lo..hi` of a row group.
pub const RowRange = struct { lo: usize, hi: usize };

/// Test-only: off compares compact decoding with whole columns; the count shows a test reached it.
pub var compact_rows_in_tests = true;
pub var compacted_row_groups: std.atomic.Value(usize) = .init(0);

/// The selected runs of a page-pruned row group, when decoding just them is worth it: a filter's page index left at
/// most half of the rows, and every column the row group decodes can be read page by page (a page index, one value per
/// row). Their rows, in order, then stand for the row group: page pruning already deselected the rest, and a compact
/// column keeps relative row order, so first appearances and ties are unchanged. Null decodes whole columns.
fn compactRows(
    ra: std.mem.Allocator,
    in: AggRowGroup,
    page_indexes: []const ?PageIndex,
    sel: *const filter_selection.SelectionVector,
    target: AggTarget,
) !?[]const RowRange {
    if (in.filter == null or (builtin.is_test and !compact_rows_in_tests)) return null;
    if (target == .grouped and target.grouped.records != null) return null;
    const kept = sel.count();
    if (kept * 2 > sel.len) return null;
    for (in.read, 0..) |needed, ci| {
        if (!needed or ci >= in.rg.columns.items.len) continue;
        if (page_indexes[ci] == null) return null;
        const cm = in.rg.columns.items[ci].meta_data orelse return null;
        if (in.meta.getColumnLevels(cm.path_in_schema.items).max_rep != 0) return null;
        // Exact DECIMAL and INT96 decoders read whole chunks, not kept rows (`decodeAggColumn`).
        if (cm.type == .INT96) return null;
        if (in.meta.getColumnSchema(cm.path_in_schema.items)) |se| if (decimal_mod.exactKind(&se) != null) return null;
    }
    var ranges: std.ArrayList(RowRange) = .empty;
    var at: usize = 0;
    while (at < sel.len) {
        while (at < sel.len and !sel.isActive(at)) at += 1;
        const lo = at;
        while (at < sel.len and sel.isActive(at)) at += 1;
        if (at > lo) try ranges.append(ra, .{ .lo = lo, .hi = at });
    }
    if (builtin.is_test) _ = compacted_row_groups.fetchAdd(1, .monotonic);
    return ranges.items;
}

fn defaultVal(comptime T: type) T {
    return switch (T) {
        i32, i64 => 0,
        f32, f64 => 0.0,
        bool => false,
        []const u8 => "",
        else => unreachable,
    };
}

/// `decodeWithReaderPruned` for just the rows of `keep` (sorted, disjoint, one value per row): pages with no kept row
/// are skipped unread, the others decode into a page-sized scratch and their kept rows are copied out in order. The
/// result has one entry per kept row and never zero-fills the rows it leaves out.
/// Whether a data page of rows `start..end` is passed unread by the reader's row gate (`DecodeOptions.row_gate`).
fn gatedOut(comptime T: type, reader: *const column_mod.ColumnChunkReader(T), start: usize, end: usize) bool {
    const gate = reader.gate orelse return false;
    return !gate.anyInRange(@min(start, gate.len), @min(end, gate.len));
}

fn decodeKeptRows(
    comptime T: type,
    arena: std.mem.Allocator,
    reader: *column_mod.ColumnChunkReader(T),
    levels: schema.Levels,
    num_leaves: usize,
    prune: PruningInfo,
    keep: []const RowRange,
) !filter_eval.ColumnT(T) {
    if (levels.max_rep != 0) return error.ShortDecode;
    // The row gate is in compact positions here: decided below per page, never by the reader in row-group rows.
    const gate_kept = reader.gate;
    reader.gate = null;
    var total: usize = 0;
    for (keep) |r| total += r.hi - r.lo;
    reader.value_budget = num_leaves;
    const max_def = levels.max_def;
    const values = try arena.alloc(T, total);
    const def_levels: ?[]u32 = if (max_def > 0) try arena.alloc(u32, total) else null;
    const possible_dict_offset = prune.dictionary_page_offset orelse blk: {
        if (prune.locations.len > 0 and prune.locations[0].offset > prune.chunk_file_offset)
            break :blk prune.chunk_file_offset;
        break :blk null;
    };
    if (possible_dict_offset) |dict_off| _ = try reader.seekAndInstallDictionaryPage(dict_off, prune.chunk_file_offset);
    for (prune.locations) |loc| if (loc.first_row_index > num_leaves) return error.ShortDecode;
    var page_values: []T = &.{};
    var page_def: []u32 = &.{};
    var k: usize = 0; // first range that may meet the current page
    var out: usize = 0; // where range `k`'s rows go
    for (prune.locations, 0..) |loc, pi| {
        const start: usize = @intCast(loc.first_row_index);
        const end: usize = if (pi + 1 < prune.locations.len) @intCast(prune.locations[pi + 1].first_row_index) else num_leaves;
        while (k < keep.len and keep[k].hi <= start) : (k += 1) out += keep[k].hi - keep[k].lo;
        var meets = k < keep.len and keep[k].lo < end;
        if (meets) if (gate_kept) |gate| {
            // Gated by the filter's survivors among the kept rows, in compact positions.
            var any = false;
            var j = k;
            var o = out;
            while (j < keep.len and keep[j].lo < end) : (j += 1) {
                const lo = @max(keep[j].lo, start);
                const hi = @min(keep[j].hi, end);
                const at = o + (lo - keep[j].lo);
                any = any or gate.anyInRange(at, at + (hi - lo));
                if (keep[j].hi > end) break;
                o += keep[j].hi - keep[j].lo;
            }
            if (!any) {
                // Reads as nulls; `values` and levels for its kept rows are filled below as for a null page.
                var j2 = k;
                var o2 = out;
                while (j2 < keep.len and keep[j2].lo < end) : (j2 += 1) {
                    const lo = @max(keep[j2].lo, start);
                    const hi = @min(keep[j2].hi, end);
                    const at = o2 + (lo - keep[j2].lo);
                    @memset(values[at..][0 .. hi - lo], defaultVal(T));
                    @memset(def_levels.?[at..][0 .. hi - lo], 0);
                    if (keep[j2].hi > end) break;
                    o2 += keep[j2].hi - keep[j2].lo;
                }
                meets = false;
            }
        };
        const null_page = pi < prune.col_index.null_pages.items.len and prune.col_index.null_pages.items[pi];
        const always = prune.page_is_always_match[pi] and (!null_page or def_levels != null);
        // A skipped page holds no kept row (pruning deselected its rows); one that did would decode below.
        if (!meets or always) {
            if (meets) {
                // An always-match page stands in for its rows as `decodeWithReaderPruned` fills them.
                const fill: T = if (null_page or T == []const u8 or T == bool) defaultVal(T) else blk: {
                    const encoded_mod = @import("filter/encoded.zig");
                    break :blk encoded_mod.readFixedLE(T, prune.col_index.min_values.items[pi]) orelse defaultVal(T);
                };
                var j = k;
                var o = out;
                while (j < keep.len and keep[j].lo < end) : (j += 1) {
                    const lo = @max(keep[j].lo, start);
                    const hi = @min(keep[j].hi, end);
                    const at = o + (lo - keep[j].lo);
                    @memset(values[at..][0 .. hi - lo], fill);
                    if (def_levels) |dl| @memset(dl[at..][0 .. hi - lo], if (null_page) 0 else @intCast(max_def));
                    if (keep[j].hi > end) break;
                    o += keep[j].hi - keep[j].lo;
                }
            }
            if (pi + 1 < prune.locations.len) {
                try reader.pages.seekToPage(prune.locations[pi + 1].offset, prune.chunk_file_offset);
            } else reader.pages.pos = reader.pages.chunk.len;
            continue;
        }
        // The row gate counts from this page's first row, which a seek skips to.
        reader.gate_row = start;
        _ = try reader.seekAndInstallPage(loc.offset, prune.chunk_file_offset);
        const want = end - start;
        if (page_values.len < want) {
            page_values = try arena.alloc(T, want);
            if (max_def > 0) page_def = try arena.alloc(u32, want);
        }
        // Null slots keep the default the decoder leaves them at.
        @memset(page_values[0..want], defaultVal(T));
        const n = if (max_def > 0)
            try reader.decodeWithLevels(page_values[0..want], page_def[0..want])
        else
            try reader.decode(page_values[0..want]);
        if (n != want) return error.ShortDecode;
        var j = k;
        var o = out;
        while (j < keep.len and keep[j].lo < end) : (j += 1) {
            const lo = @max(keep[j].lo, start);
            const hi = @min(keep[j].hi, end);
            const at = o + (lo - keep[j].lo);
            @memcpy(values[at..][0 .. hi - lo], page_values[lo - start .. hi - start]);
            if (def_levels) |dl| @memcpy(dl[at..][0 .. hi - lo], page_def[lo - start .. hi - start]);
            if (keep[j].hi > end) break;
            o += keep[j].hi - keep[j].lo;
        }
    }
    return .{
        .values = values,
        .def_levels = def_levels,
        .max_def = @intCast(max_def),
        .rep_levels = null,
        .max_rep = 0,
        .has_nulls = reader.has_nulls,
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
    if (prune.keep) |keep| return decodeKeptRows(T, arena, reader, levels, num_leaves, prune, keep);
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

        const is_skipped = prune.page_is_skipped[pi] or gatedOut(T, reader, start, end);
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

        // The row gate counts from this page's first row, which a seek skips to.
        reader.gate_row = start;
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
test "a decode budget's waiting worker sleeps until a release makes room, then holds its bytes" {
    var budget: DecodeBudget = .{ .limit = 100 };
    try budget.acquire(80);
    const Waiter = struct {
        fn run(b: *DecodeBudget, got: *std.atomic.Value(bool)) void {
            b.acquire(50) catch return;
            got.store(true, .release);
        }
    };
    var got: std.atomic.Value(bool) = .init(false);
    const t = try std.Thread.spawn(.{}, Waiter.run, .{ &budget, &got });
    for (0..10) |_| {
        try std.testing.io.sleep(.fromMilliseconds(1), .awake);
        try std.testing.expect(!got.load(.acquire));
    }
    budget.release(80);
    t.join();
    try std.testing.expect(got.load(.acquire));
    try std.testing.expectEqual(@as(usize, 50), budget.used.load(.monotonic));
    try std.testing.expectError(error.RowGroupTooLarge, budget.acquire(101));
    budget.release(50);
}

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

    const specs = [_]OutputCol{.{ .passthrough = .{ .ci = 0 } }};
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
            .total_uncompressed_size = 50,
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
            .total_uncompressed_size = 30,
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
    // The output RG's total_byte_size is its chunks' uncompressed bytes (50 + 30), not the 60 bytes copied.
    try testing.expect(out.rg != null);
    try testing.expectEqual(@as(i64, 80), out.rg.?.total_byte_size);
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

test "regression: a BOOLEAN expression is a BOOLEAN output leaf and buffer, its 0/1 lane stored as booleans" {
    const testing = std.testing;
    var b: expr_ast.Expr = .{ .col_ref = .{ .col_idx = 0, .physical_type = .BOOLEAN, .expr_type = .i64 } };
    var src: schema.SchemaElement = .{
        .type = .BOOLEAN,
        .type_length = null,
        .repetition_type = .OPTIONAL,
        .name = "b",
        .num_children = null,
        .scale = null,
        .precision = null,
        .field_id = null,
    };
    // An aliased reference keeps its column's leaf, nullable as the column is.
    const aliased = computedSchemaElem(.{ .expr = b, .alias = "k", .source = src });
    try testing.expectEqual(schema.Type.BOOLEAN, aliased.type.?);
    try testing.expectEqual(schema.FieldRepetitionType.OPTIONAL, aliased.repetition_type.?);
    try testing.expectEqualStrings("k", aliased.name);
    src.type = .INT32;
    const always: filter_ast.Filter = .{ .and_filter = .{ .children = &.{} } };
    const whens = [_]expr_ast.Case.When{.{ .cond = always, .then = &b }};
    const case: expr_ast.Expr = .{ .case_ = .{ .whens = &whens, .else_ = null, .result_type = .i64, .depth = 1 } };
    const computed = computedSchemaElem(.{ .expr = case, .alias = "k" });
    try testing.expectEqual(schema.Type.BOOLEAN, computed.type.?);
    try testing.expectEqual(schema.FieldRepetitionType.OPTIONAL, computed.repetition_type.?);

    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const vals = [_]i64{ 1, 0, 0 };
    const defs = [_]u32{ 1, 1, 0 };
    const col: filter_eval.Batch.Column = .{ .i64 = .{ .values = &vals, .def_levels = &defs, .max_def = 1, .has_nulls = true } };
    const lane = try booleanLane(arena.allocator(), .{ .boolean = .{} }, col);
    try testing.expectEqualSlices(bool, &.{ true, false, false }, lane.boolean.values);
    try testing.expectEqualSlices(u32, &defs, lane.boolean.def_levels.?);
    // Any other buffer takes the lane as it is.
    try testing.expect((try booleanLane(arena.allocator(), .{ .i64 = .{} }, col)) == .i64);
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
        const max = std.math.maxInt(usize);
        var pages: usize = 0;
        for (meta.row_groups.items) |rg| for (rg.columns.items) |chunk| {
            const cm = chunk.meta_data.?;
            const start: usize = @intCast(cm.dictionary_page_offset orelse cm.data_page_offset);
            const src = bytes[start..][0..@intCast(cm.total_compressed_size)];
            // Normalise to UNCOMPRESSED first, so the comparison does not depend on one compressor's output.
            const alloc = testing.allocator;
            const policy = cm.writer_stats;
            const plain = try recompressChunk(a, alloc, src, cm.codec, .UNCOMPRESSED, .{}, max, policy);
            const there = try recompressChunk(a, alloc, plain.bytes, .UNCOMPRESSED, case.via, .{}, max, policy);
            const back = try recompressChunk(a, alloc, there.bytes, case.via, .UNCOMPRESSED, .{}, max, policy);
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

    const specs = [_]OutputCol{.{ .passthrough = .{ .ci = 0 } }};
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

    const specs = [_]OutputCol{.{ .passthrough = .{ .ci = 0 } }};
    const agg = try initOutputAggregator(arena, &file_meta, &specs);
    try testing.expect(agg.cols[0] == .f64);
    try testing.expectEqual(schema.Type.DOUBLE, agg.schema_elems[0].type.?);
}

test "StagePlan.init in countOnly mode plans zero calls and charges only base columns" {
    var arena_state = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena_state.deinit();
    const arena = arena_state.allocator();

    var file_meta: schema.FileMetaData = .{
        .version = 1,
        .schema = .empty,
        .num_rows = 0,
        .created_by = null,
        .row_groups = .empty,
    };
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

            var file_meta: schema.FileMetaData = .{
                .version = 1,
                .schema = .empty,
                .num_rows = 0,
                .created_by = null,
                .row_groups = .empty,
            };
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

test "a key chunk whose statistics claim no nulls but holds one declines the first-row histogram and decodes as codes" {
    const testing = std.testing;
    const hybrid_rle = @import("parquet/encoding/hybrid_rle.zig");
    var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_state.deinit();
    const a = arena_state.allocator();

    var file_meta = schema.FileMetaData{
        .version = 2,
        .schema = .empty,
        .num_rows = 64,
        .created_by = null,
        .row_groups = .empty,
    };
    try file_meta.schema.append(a, .{
        .type = null,
        .type_length = null,
        .repetition_type = .REQUIRED,
        .name = "schema",
        .num_children = 1,
        .scale = null,
        .precision = null,
        .field_id = null,
    });
    try file_meta.schema.append(a, .{
        .type = .INT64,
        .type_length = null,
        .repetition_type = .OPTIONAL,
        .name = "k",
        .num_children = 0,
        .scale = null,
        .precision = null,
        .field_id = 1,
    });

    // A 4-entry dictionary; 64 leaves, the 40th null. The chunk statistics wrongly count no nulls.
    const dict = std.mem.toBytes([4]i64{ 400, 100, 300, 200 });
    var levels: [64]u32 = @splat(1);
    levels[39] = 0;
    var codes: [63]u32 = undefined;
    for (&codes, 0..) |*c, i| c.* = @intCast((i * 5 / 3) % 4);
    var body: std.ArrayList(u8) = .empty;
    const def_bytes = try hybrid_rle.encode(a, &levels, 1);
    try body.appendSlice(a, &std.mem.toBytes(@as(u32, @intCast(def_bytes.len))));
    try body.appendSlice(a, def_bytes);
    try body.append(a, 2);
    try body.appendSlice(a, try hybrid_rle.encode(a, &codes, 2));
    var chunk: std.ArrayList(u8) = .empty;
    try chunk.appendSlice(a, try pageForTest(a, .{
        .type = .DICTIONARY_PAGE,
        .uncompressed_page_size = dict.len,
        .compressed_page_size = dict.len,
        .crc = null,
        .data_page_header = null,
        .dictionary_page_header = .{ .num_values = 4, .encoding = .PLAIN, .is_sorted = null },
        .data_page_header_v2 = null,
    }, &dict));
    try chunk.appendSlice(a, try pageForTest(a, .{
        .type = .DATA_PAGE,
        .uncompressed_page_size = @intCast(body.items.len),
        .compressed_page_size = @intCast(body.items.len),
        .crc = null,
        .data_page_header = .{
            .num_values = 64,
            .encoding = .RLE_DICTIONARY,
            .definition_level_encoding = .RLE,
            .repetition_level_encoding = .RLE,
        },
        .dictionary_page_header = null,
        .data_page_header_v2 = null,
    }, body.items));

    var path: schema.StringList = .empty;
    try path.append(a, "k");
    const cm: schema.ColumnMetaData = .{
        .type = .INT64,
        .encodings = .empty,
        .path_in_schema = path,
        .codec = .UNCOMPRESSED,
        .num_values = 64,
        .total_uncompressed_size = @intCast(chunk.items.len),
        .total_compressed_size = @intCast(chunk.items.len),
        .data_page_offset = 0,
        .index_page_offset = null,
        .dictionary_page_offset = null,
        .statistics = .{ .null_count = 0 },
    };
    var scratch = DecodeScratch.init(a);
    const declined = try decodeAggColumnHist(a, chunk.items, cm, &file_meta, .{ .encoded = true }, &scratch, 64, true);
    try testing.expect(declined == null);
    // The retry the scan then makes: the right codes, and the null row marked.
    const col = (try decodeAggColumnCoded(a, chunk.items, cm, &file_meta, .{ .encoded = true }, &scratch)).?;
    const got = col.i64;
    try testing.expect(got.has_nulls);
    try testing.expectEqualSlices(u32, &levels, got.def_levels.?);
    var next: usize = 0;
    for (levels, 0..) |l, r| if (l == 1) {
        try testing.expectEqual(codes[next], got.codes.?[r]);
        next += 1;
    };
    try testing.expectEqualSlices(i64, &.{ 400, 100, 300, 200 }, got.dict);
}

test "SliceReader.skip reads the rows after it as decoding every row would, passing whole pages unread" {
    const testing = std.testing;
    // V1 and V2 pages of about 40 rows, a dictionary turning PLAIN part-way through `s`, NULLs, NaN, DECIMAL
    // (tools/zpq-gen topn_gather).
    for ([_][]const u8{ "ci/fixtures/parquet/topn_gather_a.parquet", "ci/fixtures/parquet/topn_gather_b.parquet" }) |path| {
        const bytes = metadata.readFileSlice(path, testing.allocator) catch |err| {
            if (err == error.FileNotFound) return error.SkipZigTest;
            return err;
        };
        defer testing.allocator.free(bytes);
        var meta = try metadata.open(testing.allocator, bytes);
        defer meta.deinit(testing.allocator);
        var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
        defer arena_state.deinit();
        const a = arena_state.allocator();
        const src: RGSrc = .{ .bytes = bytes, .byte_origin = 0 };
        const rg = &meta.row_groups.items[1];
        const picks = [_]usize{ 0, 1, 39, 40, 41, 300, 301, 302, 640, 998, 999 };
        for (0..rg.columns.items.len) |ci| {
            var whole = (try SliceReader.init(a, testing.allocator, rg, &meta, src, ci, .{})).?;
            defer whole.deinit();
            const all = try whole.next(a, @intCast(rg.num_rows));
            var sparse = (try SliceReader.init(a, testing.allocator, rg, &meta, src, ci, .{})).?;
            defer sparse.deinit();
            var at: usize = 0;
            for (picks) |p| {
                try sparse.skip(a, p - at);
                const one = try sparse.next(a, 1);
                at = p + 1;
                switch (all) {
                    inline else => |want, tag| {
                        const got = @field(one, @tagName(tag));
                        const null_want = if (want.def_levels) |d| d[p] < want.max_def else false;
                        const null_got = if (got.def_levels) |d| d[0] < got.max_def else false;
                        try testing.expectEqual(null_want, null_got);
                        if (!null_want) {
                            const T = @TypeOf(want.values[0]);
                            if (T == []const u8) {
                                try testing.expectEqualStrings(want.values[p], got.values[0]);
                            } else if (T == f64 and std.math.isNan(want.values[p])) {
                                try testing.expect(std.math.isNan(got.values[0]));
                            } else try testing.expectEqual(want.values[p], got.values[0]);
                        }
                    },
                }
            }
            try testing.expect(sparse.pages_passed > 0);
        }
    }
}

test "a sliced string reader under chargePages draws each page and its dictionary, and gives all back at deinit" {
    const testing = std.testing;
    // A dictionary-coded, snappy string column (`s`) of eight row groups.
    const path = "ci/fixtures/parquet/census_dictionary_review.parquet";
    const bytes = metadata.readFileSlice(path, testing.allocator) catch |err| {
        if (err == error.FileNotFound) return error.SkipZigTest;
        return err;
    };
    defer testing.allocator.free(bytes);
    var meta = try metadata.open(testing.allocator, bytes);
    defer meta.deinit(testing.allocator);
    var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_state.deinit();
    const a = arena_state.allocator();
    const src: RGSrc = .{ .bytes = bytes, .byte_origin = 0 };
    const rg = &meta.row_groups.items[0];
    const rows: usize = @intCast(rg.num_rows);
    var plain = (try SliceReader.init(a, testing.allocator, rg, &meta, src, 2, .{})).?;
    defer plain.deinit();
    const want = try plain.next(a, rows);
    var budget: core_memory.SharedBudget = .{ .limit = 1 << 30 };
    {
        var r = (try SliceReader.init(a, testing.allocator, rg, &meta, src, 2, .{})).?;
        defer r.deinit();
        r.chargePages(testing.allocator, &budget);
        var at: usize = 0;
        while (at < rows) {
            const n = @min(rows - at, 37);
            const got = try r.next(a, n);
            for (want.string.values[at..][0..n], got.string.values) |w, g| try testing.expectEqualStrings(w, g);
            at += n;
        }
        try testing.expect(budget.peak.load(.monotonic) > 0);
    }
    try testing.expectEqual(@as(usize, 0), budget.used.load(.monotonic));
    // A page the budget cannot take is refused before it is decompressed.
    var tiny: core_memory.SharedBudget = .{ .limit = 1 };
    var r = (try SliceReader.init(a, testing.allocator, rg, &meta, src, 2, .{})).?;
    defer r.deinit();
    r.chargePages(testing.allocator, &tiny);
    try testing.expectError(error.ExceededMemoryBudget, r.next(a, rows));
    try testing.expectEqual(@as(usize, 0), tiny.used.load(.monotonic));
}

test "chunkDictionary gives a string chunk's dictionary only when every data page is coded by it" {
    const testing = std.testing;
    for ([_][]const u8{ "ci/fixtures/parquet/topn_gather_a.parquet", "ci/fixtures/parquet/topn_gather_b.parquet" }) |path| {
        const bytes = metadata.readFileSlice(path, testing.allocator) catch |err| {
            if (err == error.FileNotFound) return error.SkipZigTest;
            return err;
        };
        defer testing.allocator.free(bytes);
        var meta = try metadata.open(testing.allocator, bytes);
        defer meta.deinit(testing.allocator);
        var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
        defer arena_state.deinit();
        const a = arena_state.allocator();
        const src: RGSrc = .{ .bytes = bytes, .byte_origin = 0 };
        for (meta.row_groups.items) |*rg| {
            // `s` outgrows its dictionary page and turns PLAIN; `c` stays coded; `id` is not a string.
            try testing.expect(try chunkDictionary(a, rg, &meta, src, 2) == null);
            try testing.expect(try chunkDictionary(a, rg, &meta, src, 0) == null);
            const dict = (try chunkDictionary(a, rg, &meta, src, 7)).?;
            // Eight values (those ending 0 or 5 fall on the rows holding '') and the empty string; NULL is no entry.
            try testing.expectEqual(@as(usize, 9), dict.len);
            var whole = (try SliceReader.init(a, testing.allocator, rg, &meta, src, 7, .{})).?;
            defer whole.deinit();
            const col = (try whole.next(a, @intCast(rg.num_rows))).string;
            for (col.values, 0..) |v, i| {
                if (col.def_levels.?[i] < col.max_def) continue;
                for (dict) |d| {
                    if (std.mem.eql(u8, d, v)) break;
                } else return error.TestValueNotInDictionary;
            }
        }
    }
}

/// Columns of each lane and encoding an answer writes: near-unique integers (DELTA), repeating ones (dictionary),
/// floats and strings with NULLs, and a DECIMAL whose value at `bad_row`, when set, its INT32 cannot hold.
fn aggAnswerCols(a: std.mem.Allocator, n: usize, bad_row: ?usize) ![]expr_agg.OutputCol {
    const uniq = try a.alloc(i64, n);
    const reps = try a.alloc(i64, n);
    const fl = try a.alloc(f64, n);
    const strs = try a.alloc([]const u8, n);
    const decs = try a.alloc(i128, n);
    const defs = try a.alloc(u32, n);
    for (0..n) |i| {
        uniq[i] = @bitCast(@as(u64, i) *% 0x9E3779B97F4A7C15);
        reps[i] = @intCast(i % 37);
        fl[i] = @as(f64, @floatFromInt(i % 1000)) * 0.25;
        strs[i] = try std.fmt.allocPrint(a, "v{d}", .{i % 500});
        decs[i] = @intCast(i % 9000);
        defs[i] = @intFromBool(i % 7 != 0);
    }
    if (bad_row) |r| decs[r] = 1 << 40;
    const cols = try a.alloc(expr_agg.OutputCol, 5);
    cols[0] = .{
        .name = "u",
        .col = .{ .i64 = .{ .values = uniq, .max_def = 0 } },
        .parquet_type = .INT64,
        .nullable = false,
    };
    cols[1] = .{
        .name = "r",
        .col = .{ .i64 = .{ .values = reps, .max_def = 0 } },
        .parquet_type = .INT64,
        .nullable = false,
    };
    cols[2] = .{
        .name = "d",
        .col = .{ .i128 = .{ .values = decs, .max_def = 0 } },
        .parquet_type = .INT32,
        .nullable = false,
        .dec = .{ .precision = 9, .scale = 2 },
    };
    cols[3] = .{
        .name = "f",
        .col = .{ .f64 = .{ .values = fl, .def_levels = defs, .max_def = 1, .has_nulls = true } },
        .parquet_type = .DOUBLE,
        .nullable = true,
    };
    cols[4] = .{
        .name = "s",
        .col = .{ .string = .{ .values = strs, .def_levels = defs, .max_def = 1, .has_nulls = true } },
        .parquet_type = .BYTE_ARRAY,
        .nullable = true,
    };
    return cols;
}

/// Keeps what is written; fails the write numbered `fail_at` (0 is the magic), if set.
const AnswerCapture = struct {
    a: std.mem.Allocator,
    bytes: std.ArrayList(u8) = .empty,
    writes: usize = 0,
    fail_at: ?usize = null,

    fn sink(c: *AnswerCapture) streaming.Sink {
        return .{ .ctx = @ptrCast(c), .write_fn = write };
    }

    fn write(ctx: *anyopaque, b: []const u8) anyerror!void {
        const c: *AnswerCapture = @ptrCast(@alignCast(ctx));
        defer c.writes += 1;
        if (c.fail_at == c.writes) return error.TestSinkFailed;
        try c.bytes.appendSlice(c.a, b);
    }
};

test "writeAggregateRows: columns encoded on threads write the serial bytes, whatever spawns fail" {
    var arena_state = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena_state.deinit();
    const a = arena_state.allocator();
    const n = 70_000;
    const cols = try aggAnswerCols(a, n, null);
    for ([_]schema.CompressionCodec{ .SNAPPY, .ZSTD, .UNCOMPRESSED }) |codec| {
        var serial: AnswerCapture = .{ .a = a };
        const want = try writeAggregateRowsEncoded(a, serial.sink(), cols, n, answer_rg_rows, codec, .{}, .{});
        try std.testing.expectEqual(@as(u64, serial.bytes.items.len), want);
        // Every column in one wave; two columns' charge a wave, so waves of two and lone columns; and spawns failing.
        const two = aggEncodeCharge(cols[0]) + aggEncodeCharge(cols[1]);
        const Run = struct { encode: AggEncode, fail_after: ?usize = null };
        for ([_]Run{
            .{ .encode = .{ .workers = 8, .budget = std.math.maxInt(u64) } },
            .{ .encode = .{ .workers = 3, .budget = two } },
            .{ .encode = .{ .workers = 8, .budget = std.math.maxInt(u64) }, .fail_after = 0 },
            .{ .encode = .{ .workers = 8, .budget = std.math.maxInt(u64) }, .fail_after = 2 },
        }) |run| {
            if (run.fail_after) |k| spawn_util.injectFailureAfter(k);
            defer spawn_util.resetFailure();
            var par: AnswerCapture = .{ .a = a };
            const got = try writeAggregateRowsEncoded(a, par.sink(), cols, n, answer_rg_rows, codec, .{}, run.encode);
            try std.testing.expectEqual(want, got);
            try std.testing.expectEqualSlices(u8, serial.bytes.items, par.bytes.items);
        }
    }
}

test "writeAggregateRows: on threads, the serial error and the bytes written before it" {
    var arena_state = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena_state.deinit();
    const a = arena_state.allocator();
    const n = 1000;
    // Column 2 cannot encode; write 1 (column 0) fails, or write 2 (column 1), or none does.
    const cols = try aggAnswerCols(a, n, 600);
    for ([_]?usize{ null, 1, 2 }) |fail_at| {
        var serial: AnswerCapture = .{ .a = a, .fail_at = fail_at };
        const want = writeAggregateRowsEncoded(a, serial.sink(), cols, n, answer_rg_rows, .SNAPPY, .{}, .{});
        var par: AnswerCapture = .{ .a = a, .fail_at = fail_at };
        const all: AggEncode = .{ .workers = 8, .budget = std.math.maxInt(u64) };
        const got = writeAggregateRowsEncoded(a, par.sink(), cols, n, answer_rg_rows, .SNAPPY, .{}, all);
        try std.testing.expectError(if (fail_at == null) error.UnsupportedType else error.TestSinkFailed, want);
        try std.testing.expectEqual(want, got);
        try std.testing.expectEqualSlices(u8, serial.bytes.items, par.bytes.items);
    }
}

fn writeAnswerOnThreads(a: std.mem.Allocator) !void {
    var arena_state = std.heap.ArenaAllocator.init(a);
    defer arena_state.deinit();
    const arena = arena_state.allocator();
    const cols = try aggAnswerCols(arena, 300, null);
    var out: AnswerCapture = .{ .a = arena };
    _ = try writeAggregateRowsEncoded(
        arena,
        out.sink(),
        cols,
        300,
        answer_rg_rows,
        .SNAPPY,
        .{},
        .{ .workers = 4, .budget = 1 << 40 },
    );
}

test "writeAggregateRows: one worker, or a budget no two columns fit, starts no thread" {
    var arena_state = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena_state.deinit();
    const a = arena_state.allocator();
    const n = 70_000;
    const cols = try aggAnswerCols(a, n, null);
    for ([_]AggEncode{ .{ .workers = 1, .budget = std.math.maxInt(u64) }, .{ .workers = 16, .budget = 0 } }) |encode| {
        // A spawn would fail and be counted.
        spawn_util.injectFailureAfter(0);
        defer spawn_util.resetFailure();
        var out: AnswerCapture = .{ .a = a };
        _ = try writeAggregateRows(a, out.sink(), cols, n, .SNAPPY, .{}, encode);
        try std.testing.expectEqual(@as(usize, 0), spawn_util.failuresInjected());
    }
}

test "writeAggregateRows: a column encoded alone still draws from the ledger" {
    var arena_state = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena_state.deinit();
    const a = arena_state.allocator();
    const cols = try aggAnswerCols(a, 1000, null);
    // One column, and few rows: no waves, but the column is the ledger's to refuse.
    var ledger: expr_agg.SharedBudget = .{ .limit = 1 };
    var out: AnswerCapture = .{ .a = a };
    const encode: AggEncode = .{ .workers = 16, .budget = std.math.maxInt(u64), .ledger = &ledger };
    const one = writeAggregateRows(a, out.sink(), cols[0..1], 1000, .SNAPPY, .{}, encode);
    try std.testing.expectError(error.ExceededMemoryBudget, one);
    ledger.limit = 1 << 30;
    _ = try writeAggregateRows(a, out.sink(), cols, 1000, .SNAPPY, .{}, encode);
    try std.testing.expectEqual(@as(usize, 0), ledger.used.load(.monotonic));
}

test "writeAggregateRows: on threads, an allocation failure frees everything" {
    try std.testing.checkAllAllocationFailures(std.testing.allocator, writeAnswerOnThreads, .{});
}

/// The bytes between a written answer's leading magic and its footer: its column chunks, in order.
fn answerChunks(a: std.mem.Allocator, file: []const u8) ![]const u8 {
    const meta = try answerFooter(a, file);
    const footer_len = std.mem.readInt(u32, file[file.len - 8 ..][0..4], .little);
    var end = file.len - 8 - footer_len;
    for (meta.row_groups.items) |rg| for (rg.columns.items) |c| {
        if (c.meta_data) |cm| {
            if (cm.bloom_filter_offset) |off| end = @min(end, @as(usize, @intCast(off)));
        }
    };
    return file[4..end];
}

fn answerFooter(a: std.mem.Allocator, file: []const u8) !schema.FileMetaData {
    const footer_len = std.mem.readInt(u32, file[file.len - 8 ..][0..4], .little);
    var reader = thrift.Reader.init(file[file.len - 8 - footer_len .. file.len - 8]);
    return schema.FileMetaData.read(a, &reader);
}

test "writeAggregateRows: big answers use fixed row groups with serial chunks at every worker count" {
    var arena_state = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena_state.deinit();
    const a = arena_state.allocator();
    const n = 1000;
    const rg = 300;
    const cols = try aggAnswerCols(a, n, null);
    var serial: AnswerCapture = .{ .a = a };
    _ = try writeAggregateRowsEncoded(a, serial.sink(), cols, n, rg, .SNAPPY, .{}, .{});
    const meta = try answerFooter(a, serial.bytes.items);
    try std.testing.expectEqual(@as(i64, n), meta.num_rows);
    try std.testing.expectEqual(@as(usize, 4), meta.row_groups.items.len);
    for (meta.row_groups.items, [_]i64{ 300, 300, 300, 100 }) |g, rows| {
        try std.testing.expectEqual(rows, g.num_rows);
        try std.testing.expectEqual(cols.len, g.columns.items.len);
    }
    // Row group g's chunks are what its rows encode to as an answer of their own, in order.
    var want: std.ArrayList(u8) = .empty;
    for (0..4) |g| {
        const lo = g * rg;
        const hi = @min(n, lo + rg);
        const part = try a.alloc(expr_agg.OutputCol, cols.len);
        for (cols, part) |c, *p| p.* = sliceOutputCol(c, lo, hi);
        var alone: AnswerCapture = .{ .a = a };
        _ = try writeAggregateRowsEncoded(a, alone.sink(), part, @intCast(hi - lo), rg, .SNAPPY, .{}, .{});
        try want.appendSlice(a, try answerChunks(a, alone.bytes.items));
    }
    try std.testing.expectEqualSlices(u8, want.items, try answerChunks(a, serial.bytes.items));
    // The same file whatever the workers, the budget's waves, or failing spawns.
    const two = aggEncodeCharge(sliceOutputCol(cols[0], 0, rg)) + aggEncodeCharge(sliceOutputCol(cols[1], 0, rg));
    const Run = struct { encode: AggEncode, fail_after: ?usize = null };
    for ([_]Run{
        .{ .encode = .{ .workers = 16, .budget = std.math.maxInt(u64) } },
        .{ .encode = .{ .workers = 3, .budget = two } },
        .{ .encode = .{ .workers = 7, .budget = std.math.maxInt(u64) }, .fail_after = 1 },
    }) |run| {
        if (run.fail_after) |k| spawn_util.injectFailureAfter(k);
        defer spawn_util.resetFailure();
        var par: AnswerCapture = .{ .a = a };
        _ = try writeAggregateRowsEncoded(a, par.sink(), cols, n, rg, .SNAPPY, .{}, run.encode);
        try std.testing.expectEqualSlices(u8, serial.bytes.items, par.bytes.items);
    }
}

test "writeAggregateRows: an answer of a row group's rows or fewer is one row group" {
    var arena_state = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena_state.deinit();
    const a = arena_state.allocator();
    const cols = try aggAnswerCols(a, 300, null);
    for ([_]usize{ 0, 1, 300 }) |n| {
        const part = try a.alloc(expr_agg.OutputCol, cols.len);
        for (cols, part) |c, *p| p.* = sliceOutputCol(c, 0, n);
        var out: AnswerCapture = .{ .a = a };
        _ = try writeAggregateRowsEncoded(a, out.sink(), part, @intCast(n), 300, .SNAPPY, .{}, .{});
        const meta = try answerFooter(a, out.bytes.items);
        try std.testing.expectEqual(@as(usize, 1), meta.row_groups.items.len);
        try std.testing.expectEqual(@as(i64, @intCast(n)), meta.row_groups.items[0].num_rows);
    }
}

fn writeRowGroupsOnThreads(a: std.mem.Allocator) !void {
    var arena_state = std.heap.ArenaAllocator.init(a);
    defer arena_state.deinit();
    const arena = arena_state.allocator();
    const cols = try aggAnswerCols(arena, 300, null);
    var out: AnswerCapture = .{ .a = arena };
    _ = try writeAggregateRowsEncoded(
        arena,
        out.sink(),
        cols,
        300,
        128,
        .SNAPPY,
        .{},
        .{ .workers = 4, .budget = 1 << 40 },
    );
}

test "writeAggregateRows: row groups on threads, an allocation failure frees everything" {
    try std.testing.checkAllAllocationFailures(std.testing.allocator, writeRowGroupsOnThreads, .{});
}

fn writeAnswerWindows(a: std.mem.Allocator) !void {
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const alloc = arena.allocator();
    const cols = try aggAnswerCols(alloc, 1000, null);
    var whole: AnswerCapture = .{ .a = alloc };
    _ = try writeAggregateRowsEncoded(alloc, whole.sink(), cols, 1000, 300, .SNAPPY, .{}, .{});
    var windows: AnswerCapture = .{ .a = alloc };
    var file = try AggregateFile.init(alloc, windows.sink());
    defer file.deinit();
    for ([_][2]usize{ .{ 0, 600 }, .{ 600, 1000 } }) |range| {
        const part = try alloc.alloc(expr_agg.OutputCol, cols.len);
        for (cols, part) |c, *p| p.* = sliceOutputCol(c, range[0], range[1]);
        try appendAggregateRows(
            &file,
            part,
            @intCast(range[1] - range[0]),
            300,
            .SNAPPY,
            .{},
            .{ .workers = 4, .budget = 1 << 30 },
        );
    }
    _ = try file.finish(cols);
    try std.testing.expectEqualSlices(u8, whole.bytes.items, windows.bytes.items);
}

test "writeAggregateRows: bounded windows write the whole answer's bytes and free every failed allocation" {
    try writeAnswerWindows(std.testing.allocator);
    try std.testing.checkAllAllocationFailures(std.testing.allocator, writeAnswerWindows, .{});
}

fn cachedIntegerWindows(a: std.mem.Allocator) !void {
    var budget: expr_agg.SharedBudget = .{ .limit = 1 << 30 };
    defer std.debug.assert(budget.used.load(.monotonic) == 0);
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const alloc = arena.allocator();
    const all = try aggAnswerCols(alloc, 1201, null);
    var cols = [_]expr_agg.OutputCol{ all[0], all[1], all[0] };
    cols[2].name = "s";
    var whole: AnswerCapture = .{ .a = alloc };
    _ = try writeAggregateRowsEncoded(alloc, whole.sink(), &cols, 1201, 300, .SNAPPY, .{}, .{});
    var windows: AnswerCapture = .{ .a = alloc };
    var file = try AggregateFile.init(alloc, windows.sink());
    defer file.deinit();
    const encode: AggEncode = .{ .workers = 11, .budget = 128 << 20, .ledger = &budget };
    try file.reserveIntegerWorkspace(encode);
    try std.testing.expectEqual(@as(usize, 12 * answer_rg_rows * 12 * 3), file.cached_charge);
    try std.testing.expectEqual(file.cached_charge, budget.used.load(.monotonic));
    for ([_][2]usize{ .{ 0, 1200 }, .{ 1200, 1201 } }) |range| {
        var part: [3]expr_agg.OutputCol = undefined;
        for (cols, &part) |c, *p| p.* = sliceOutputCol(c, range[0], range[1]);
        try appendAggregateRows(&file, &part, @intCast(range[1] - range[0]), 300, .SNAPPY, .{}, encode);
    }
    _ = try file.finish(&cols);
    try std.testing.expectEqualSlices(u8, whole.bytes.items, windows.bytes.items);
    // A later wider shape returns every retained arena before drawing its ordinary per-wave workspace.
    file.releaseWorkspace();
    var fallback: AnswerCapture = .{ .a = alloc };
    var wider = try AggregateFile.init(alloc, fallback.sink());
    defer wider.deinit();
    try wider.reserveIntegerWorkspace(encode);
    wider.cached_unit_charge = 1;
    try appendAggregateRows(&wider, &cols, 1201, 300, .SNAPPY, .{}, encode);
    _ = try wider.finish(&cols);
    try std.testing.expectEqualSlices(u8, whole.bytes.items, fallback.bytes.items);
    try std.testing.expectEqual(@as(usize, 0), wider.cached_charge);
    try std.testing.expectEqual(@as(usize, 0), budget.used.load(.monotonic));
}

test "writeAggregateRows: retained integer encode scratch stays charged through windows and unwinds on failure" {
    try cachedIntegerWindows(std.testing.allocator);
    try std.testing.checkAllAllocationFailures(std.testing.allocator, cachedIntegerWindows, .{});
}

fn checkPartitionWindowScratch(a: std.mem.Allocator, n: usize, coded: bool) !void {
    const strings = try a.alloc([]const u8, n);
    defer a.free(strings);
    const text = try a.alloc([96]u8, n);
    defer a.free(text);
    for (strings, text, 0..) |*s, *buf, i| {
        s.* = try std.fmt.bufPrint(buf, "a distinct long key whose bytes must outlive grouping window {d:0>8}", .{i});
    }
    const ints = try a.alloc(i64, n);
    defer a.free(ints);
    for (ints, 0..) |*v, i| v.* = @intCast(i);
    const codes = try a.alloc(u32, n);
    defer a.free(codes);
    for (codes, 0..) |*code, i| code.* = @intCast(i);
    var cols = [_]filter_eval.Batch.Column{
        .{ .string = .{
            .values = if (coded) &.{} else strings,
            .dict = if (coded) strings else &.{},
            .codes = if (coded) codes else null,
            .max_def = 0,
        } },
        .{ .i64 = .{ .values = ints, .max_def = 0 } },
    };
    const keys = [_]expr_ast.Expr{
        .{ .col_ref = .{ .col_idx = 0, .physical_type = .BYTE_ARRAY, .expr_type = .str } },
        .{ .col_ref = .{ .col_idx = 1, .physical_type = .INT64, .expr_type = .i64 } },
    };
    const calls = [_]expr_agg.AggCall{.{ .func = .count, .arg = null, .where = null, .alias = "c", .result = .i64 }};
    const Merge = struct {
        fn count(dst: *expr_agg.Accumulator, src: *expr_agg.Accumulator, _: std.mem.Allocator) !void {
            dst.count += src.count;
        }
    };
    var budget: expr_agg.SharedBudget = .{ .limit = 64 << 20 };
    defer std.debug.assert(budget.used.load(.monotonic) == 0);
    var part = try partagg.PartAgg.init(a, &budget, &calls, 0, Merge.count, 4, 4, 1, true);
    defer part.deinit();
    part.compact = true;
    try std.testing.expect(try part.admitPrivate(n, 100));
    var table = expr_agg.GroupTable.init(a, 0);
    table.shared = &budget;
    table.compact.configure(&calls);
    defer table.deinit();
    var local: partagg.Local = .{ .runs = &part.runs[0] };
    var sel = try filter_selection.SelectionVector.init(a, n);
    defer sel.deinit();
    var scratch_budget: expr_agg.SharedBudget = .{ .limit = 512 << 10 };
    var charged: @import("memory.zig").Charged = .{ .child = a, .budget = &scratch_budget };
    var arena = RowGroupArena.init(charged.allocator());
    defer arena.deinit();
    const target: AggTarget = .{ .grouped = .{
        .calls = &calls,
        .keys = &keys,
        .table = &table,
        .pos_base = 0,
        .part = &part,
        .local = &local,
    } };
    try foldGroupedWindows(
        arena.allocator(),
        a,
        target,
        &cols,
        &.{ 0, 1 },
        &sel,
        &.{ coded, false },
        coded,
        null,
        null,
        null,
    );
    try part.flush(&local, &table);
    try part.foldRuns(1);
    var groups: usize = 0;
    for (part.parts) |*p| {
        groups += p.table.groupCount();
        for (0..p.table.groupCount()) |g| {
            var tmp: [16]expr_agg.Accumulator = undefined;
            try std.testing.expectEqual(@as(u64, 1), p.table.readGroup(g, 1, &tmp)[0].count);
            const k = p.table.keyAt(g);
            const id = std.mem.readInt(i64, k[k.len - 8 ..][0..8], .little);
            try std.testing.expectEqual(@as(u64, @intCast(id)), p.table.first.items[g]);
            const len = std.mem.readInt(u32, k[1..5], .little);
            try std.testing.expectEqualStrings(strings[@intCast(id)], k[5..][0..len]);
        }
    }
    try std.testing.expectEqual(@as(usize, n), groups);
}

test "partition windows reuse charged scratch instead of retaining a row group's windows" {
    try checkPartitionWindowScratch(std.testing.allocator, 32_768, false);
}

test "partition window scratch keeps materialized dictionaries live across resets" {
    try checkPartitionWindowScratch(std.testing.allocator, 8192, true);
}

test "partition window scratch returns every charge on allocation failure" {
    try std.testing.checkAllAllocationFailures(std.testing.allocator, checkPartitionWindowScratch, .{ 32, false });
}

test "regression: a renamed passthrough is its leaf renamed and flat, NULL where it is NULL at any depth" {
    const testing = std.testing;
    var elem: schema.SchemaElement = .{
        .type = .INT32,
        .type_length = null,
        .repetition_type = .REQUIRED,
        .name = "a",
        .num_children = null,
        .scale = null,
        .precision = null,
        .field_id = null,
        .converted_type = .DATE,
        .logical_type = .{ .DATE = .{} },
    };
    // A REQUIRED field under an OPTIONAL struct can be NULL: flat, it is OPTIONAL.
    renameLeaf(&elem, "k", 1);
    try testing.expectEqualStrings("k", elem.name);
    try testing.expectEqual(schema.FieldRepetitionType.OPTIONAL, elem.repetition_type.?);
    try testing.expectEqual(schema.Type.INT32, elem.type.?);
    try testing.expect(elem.logical_type.? == .DATE);
    renameLeaf(&elem, "k", 0);
    try testing.expectEqual(schema.FieldRepetitionType.REQUIRED, elem.repetition_type.?);

    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    // Levels of an OPTIONAL leaf in an OPTIONAL struct: 0 struct NULL, 1 leaf NULL, 2 present.
    const vals = [_]i32{ 0, 0, 7, 9 };
    const defs = [_]u32{ 0, 1, 2, 2 };
    const col: filter_eval.Batch.Column = .{ .i32 = .{ .values = &vals, .def_levels = &defs, .max_def = 2, .has_nulls = true } };
    const named = try namedLevels(a, .{ .ci = 0, .name = "k" }, col);
    try testing.expectEqual(@as(u32, 1), named.i32.max_def);
    try testing.expectEqualSlices(u32, &.{ 0, 0, 1, 1 }, named.i32.def_levels.?);
    try testing.expectEqualSlices(i32, &vals, named.i32.values);
    // Unnamed, or already flat, the column is untouched.
    try testing.expectEqual(@as(u32, 2), (try namedLevels(a, .{ .ci = 0 }, col)).i32.max_def);
    const flat_defs = [_]u32{ 0, 1, 1, 1 };
    const flat: filter_eval.Batch.Column = .{ .i32 = .{ .values = &vals, .def_levels = &flat_defs, .max_def = 1, .has_nulls = true } };
    try testing.expectEqual(flat.i32.def_levels.?.ptr, (try namedLevels(a, .{ .ci = 0, .name = "k" }, flat)).i32.def_levels.?.ptr);
}

test "appendSelected keeps one level per row when a column's batches come with and without levels" {
    var arena_state = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena_state.deinit();
    const a = arena_state.allocator();
    const vals = [_]i64{ 7, 8 };
    const defs = [_]u32{ 1, 0 };
    // A computed column NULL in one row group (levels) and NULL-free in the next (none), in both orders.
    const with: filter_eval.Batch.Column = .{ .i64 = .{ .values = &vals, .def_levels = &defs, .max_def = 1, .has_nulls = true } };
    const without: filter_eval.Batch.Column = .{ .i64 = .{ .values = &vals } };
    var sel = try filter_selection.SelectionVector.init(a, 2);
    const Case = struct { first: filter_eval.Batch.Column, second: filter_eval.Batch.Column, want: [4]u32 };
    for ([_]Case{
        .{ .first = with, .second = without, .want = .{ 1, 0, 1, 1 } },
        .{ .first = without, .second = with, .want = .{ 1, 1, 1, 0 } },
    }) |c| {
        var buf: OutputAggregator.ColumnBuf = .{ .i64 = .{} };
        try appendSelected(&buf, c.first, &sel, 2, a, false, a);
        try appendSelected(&buf, c.second, &sel, 2, a, false, a);
        try std.testing.expectEqual(@as(usize, 4), buf.i64.values.items.len);
        try std.testing.expectEqualSlices(u32, &c.want, buf.i64.def_levels.items);
        try std.testing.expectEqual(@as(u32, 1), buf.i64.max_def);
    }
    // NULL-free batches alone carry no levels.
    var plain: OutputAggregator.ColumnBuf = .{ .i64 = .{} };
    try appendSelected(&plain, without, &sel, 2, a, false, a);
    try appendSelected(&plain, without, &sel, 2, a, false, a);
    try std.testing.expectEqual(@as(usize, 0), plain.i64.def_levels.items.len);
}

test "Survivors: computed outputs evaluate over the kept rows only, with their levels, codes and exact values" {
    const testing = std.testing;
    var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_state.deinit();
    const a = arena_state.allocator();
    const n = 200;
    const x = try a.alloc(f64, n);
    const x_def = try a.alloc(u32, n);
    const d_vals = try a.alloc(f64, n);
    const d_exact = try a.alloc(i64, n);
    const codes = try a.alloc(u32, n);
    var sel = try filter_selection.SelectionVector.init(a, n);
    for (0..n) |r| {
        x[r] = @floatFromInt(r);
        x_def[r] = @intFromBool(r % 7 != 3);
        d_exact[r] = @intCast(r * 25);
        d_vals[r] = @as(f64, @floatFromInt(r * 25)) / 100.0;
        codes[r] = @intCast(r % 3);
        sel.set(r, r % 5 == 1 or r % 7 == 3 or r == 64);
    }
    const surviving = sel.count();
    const dict = [_][]const u8{ "a", "bb", "ccc" };
    const cols = [_]filter_eval.Batch.Column{
        .{ .f64 = .{ .values = x, .def_levels = x_def, .max_def = 1, .has_nulls = true } },
        .{ .f64 = .{ .values = d_vals, .unscaled = .{ .i64 = d_exact } } },
        .{ .string = .{ .values = &.{}, .codes = codes, .dict = &dict } },
    };
    const batch: filter_eval.Batch = .{ .cols = &cols, .num_rows = n };
    const lookup = [_]?usize{ 0, 1, 2 };
    const s = try Survivors.init(a, &batch, &lookup, &.{ true, true, true }, &sel, surviving);
    try testing.expectEqual(surviving, s.batch.num_rows);
    try testing.expectEqual(surviving, s.sel.count());
    var k: usize = 0;
    for (0..n) |r| {
        if (!sel.isActive(r)) continue;
        try testing.expectEqual(x[r], s.batch.cols[0].f64.values[k]);
        try testing.expectEqual(x_def[r], s.batch.cols[0].f64.def_levels.?[k]);
        try testing.expectEqual(d_exact[r], s.batch.cols[1].f64.unscaled.?.i64[k]);
        try testing.expectEqual(codes[r], s.batch.cols[2].string.codes.?[k]);
        k += 1;
    }
    try testing.expect(s.batch.cols[0].f64.has_nulls);
    // A coded column comes back with values, looked up for the kept rows only.
    k = 0;
    for (0..n) |r| {
        if (!sel.isActive(r)) continue;
        try testing.expectEqualStrings(dict[codes[r]], s.batch.cols[2].string.values[k]);
        k += 1;
    }

    // Every row kept: nothing is gathered. Kept rows with no NULL among them: the gathered column says so.
    var all = try filter_selection.SelectionVector.init(a, n);
    const whole = try Survivors.init(a, &batch, &lookup, &.{ true, true, true }, &all, n);
    try testing.expectEqual(x.ptr, whole.batch.cols[0].f64.values.ptr);
    var present = try filter_selection.SelectionVector.init(a, n);
    for (0..n) |r| present.set(r, x_def[r] == 1 and r % 2 == 0);
    const no_nulls = try Survivors.init(a, &batch, &lookup, &.{ true, false, false }, &present, present.count());
    try testing.expect(!no_nulls.batch.cols[0].f64.has_nulls);
    // Columns no computed output reads stay as they are.
    try testing.expectEqual(d_vals.ptr, no_nulls.batch.cols[1].f64.values.ptr);
}

test "appendSliceRows: a computed column over the kept rows is the one computed over every row and then filtered" {
    const testing = std.testing;
    var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_state.deinit();
    const a = arena_state.allocator();
    const n = 300;
    const x = try a.alloc(f64, n);
    const x_def = try a.alloc(u32, n);
    var sel = try filter_selection.SelectionVector.init(a, n);
    for (0..n) |r| {
        x[r] = @as(f64, @floatFromInt(r)) * 1.5 - 7;
        x_def[r] = @intFromBool(r % 11 != 0);
        sel.set(r, r % 3 != 1);
    }
    const cols = [_]filter_eval.Batch.Column{.{ .f64 = .{ .values = x, .def_levels = x_def, .max_def = 1, .has_nulls = true } }};
    const batch: filter_eval.Batch = .{ .cols = &cols, .num_rows = n };
    const lookup = [_]?usize{0};
    var left: expr_ast.Expr = .{ .col_ref = .{ .col_idx = 0, .physical_type = .DOUBLE, .expr_type = .f64 } };
    var right: expr_ast.Expr = .{ .literal = .{ .f64 = 1.19 } };
    const e: expr_ast.Expr = .{ .binop = .{ .op = .mul, .left = &left, .right = &right, .result_type = .f64, .depth = 1 } };
    const specs = [_]OutputCol{.{ .computed = .{ .expr = e, .alias = "g" } }};
    var meta: schema.FileMetaData = .{ .version = 1, .schema = .empty, .num_rows = n, .created_by = null, .row_groups = .empty };
    var agg = try initOutputAggregator(a, &meta, &specs);
    try testing.expectEqual(sel.count(), try appendSliceRows(&agg, &batch, &lookup, &sel, &specs, a));
    const dense = try expr_eval.evalExpr(a, &batch.restrictedTo(&sel), &lookup, e);
    var want: OutputAggregator.ColumnBuf = .{ .f64 = .{} };
    try appendSelected(&want, dense, &sel, sel.count(), a, false, a);
    try testing.expectEqualSlices(u32, want.f64.def_levels.items, agg.cols[0].f64.def_levels.items);
    for (want.f64.values.items, agg.cols[0].f64.values.items, want.f64.def_levels.items) |w, g, d| {
        if (d == 1) try testing.expectEqual(w, g);
    }
}

test "cloneColumnMeta keeps every statistics field, the bounds' exactness among them" {
    var arena_state = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena_state.deinit();
    const a = arena_state.allocator();
    const st: schema.Statistics = .{
        .max = "b",
        .min = "a",
        .null_count = 3,
        .distinct_count = 7,
        .max_value = "y",
        .min_value = "x",
        .is_max_value_exact = false,
        .is_min_value_exact = true,
        .nan_count = 2,
    };
    const cm: schema.ColumnMetaData = .{
        .type = .BYTE_ARRAY,
        .encodings = .empty,
        .path_in_schema = .empty,
        .codec = .UNCOMPRESSED,
        .num_values = 10,
        .total_uncompressed_size = 0,
        .total_compressed_size = 0,
        .data_page_offset = 0,
        .index_page_offset = null,
        .dictionary_page_offset = null,
        .statistics = st,
    };
    const got = (try cloneColumnMeta(a, cm)).statistics.?;
    inline for (@typeInfo(schema.Statistics).@"struct".field_names) |name| {
        const want = @field(st, name);
        const have = @field(got, name);
        if (@TypeOf(want) == ?[]const u8) {
            try std.testing.expectEqualStrings(want.?, have.?);
        } else try std.testing.expectEqual(want, have);
    }
}

test "plainBitsPrefix: every range's bits are plainBits' over the same rows, whatever the lanes and NULLs" {
    const testing = std.testing;
    var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_state.deinit();
    const a = arena_state.allocator();
    const n = 257;
    var agg: OutputAggregator = .{
        .arena = a,
        .cols = try a.alloc(OutputAggregator.ColumnBuf, 4),
        .schema_elems = try a.alloc(schema.SchemaElement, 4),
        .paths = &.{},
        .num_rows = n,
        .captured = true,
    };
    const base: schema.SchemaElement = .{
        .type = .INT64,
        .type_length = null,
        .repetition_type = .OPTIONAL,
        .name = "c",
        .num_children = null,
        .scale = null,
        .precision = null,
        .field_id = null,
    };
    for (agg.schema_elems) |*e| e.* = base;
    agg.schema_elems[1].type = .BYTE_ARRAY;
    agg.schema_elems[2].type = .BYTE_ARRAY;
    agg.schema_elems[3].type = .BOOLEAN;
    agg.cols[0] = .{ .i64 = .{} };
    agg.cols[1] = .{ .string = .{} };
    agg.cols[2] = .{ .string = .{ .coded = true, .dict = &.{ "x", "yyyy", "" }, .max_def = 1 } };
    agg.cols[3] = .{ .boolean = .{ .max_def = 1 } };
    for (0..n) |r| {
        try agg.cols[0].i64.values.append(a, @intCast(r));
        try agg.cols[1].string.values.append(a, "abcdefghij"[0 .. r % 11]);
        try agg.cols[2].string.codes.append(a, @intCast(r % 3));
        try agg.cols[2].string.def_levels.append(a, @intFromBool(r % 4 != 0));
        try agg.cols[3].boolean.values.append(a, r % 2 == 0);
        try agg.cols[3].boolean.def_levels.append(a, @intFromBool(r % 5 != 0));
    }
    const prefix = try plainBitsPrefix(a, &agg);
    try testing.expectEqual(@as(usize, n + 1), prefix.len);
    for ([_][2]usize{ .{ 0, n }, .{ 0, 0 }, .{ 3, 4 }, .{ 17, 200 }, .{ 100, n } }) |lohi| {
        var want: u64 = 0;
        for (agg.cols, agg.schema_elems) |*c, *e| want += plainBits(c, e, lohi[0], lohi[1]);
        try testing.expectEqual(want, prefix[lohi[1]] - prefix[lohi[0]]);
    }
}

test "canonical decode failure checks every competing column and unwinds allocation failures" {
    var storage = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer storage.deinit();
    const a = storage.allocator();
    const header: schema.PageHeader = .{
        .type = .DATA_PAGE,
        .uncompressed_page_size = 4,
        .compressed_page_size = 4,
        .crc = null,
        .data_page_header = .{
            .num_values = 300,
            .encoding = .PLAIN,
            .definition_level_encoding = .RLE,
            .repetition_level_encoding = .RLE,
        },
        .dictionary_page_header = null,
        .data_page_header_v2 = null,
    };
    var writer = thrift.Writer.init(a);
    try header.write(&writer);
    const bytes = try std.mem.concat(a, u8, &.{ writer.bytes(), &.{ 255, 255, 255, 255 } });
    var elements = [_]schema.SchemaElement{ fl_test_elem, fl_test_elem, fl_test_elem };
    elements[0].name = "schema";
    elements[0].type = null;
    elements[0].repetition_type = .REQUIRED;
    elements[0].num_children = 2;
    elements[1].name = "a";
    elements[2].name = "b";
    var paths = [_][1][]const u8{ .{"a"}, .{"b"} };
    var columns: [2]schema.ColumnChunk = undefined;
    for (&columns, &paths) |*c, *path| c.* = .{
        .file_path = null,
        .file_offset = 0,
        .meta_data = .{
            .type = .INT32,
            .encodings = .empty,
            .path_in_schema = .{ .items = path, .capacity = 1, .pointer_stability = .{} },
            .codec = .UNCOMPRESSED,
            .num_values = 300,
            .total_uncompressed_size = @intCast(bytes.len),
            .total_compressed_size = @intCast(bytes.len),
            .data_page_offset = 0,
            .index_page_offset = null,
            .dictionary_page_offset = null,
        },
    };
    var groups = [_]schema.RowGroup{.{
        .columns = .{ .items = &columns, .capacity = 2, .pointer_stability = .{} },
        .total_byte_size = @intCast(bytes.len * 2),
        .num_rows = 300,
    }};
    const meta: schema.FileMetaData = .{
        .version = 1,
        .schema = .{ .items = &elements, .capacity = 3, .pointer_stability = .{} },
        .num_rows = 300,
        .row_groups = .{ .items = &groups, .capacity = 1, .pointer_stability = .{} },
        .created_by = null,
    };
    const in: AggRowGroup = .{
        .rg = &groups[0],
        .meta = &meta,
        .src = .{ .bytes = bytes, .byte_origin = 0 },
        .filter = null,
        .read = &.{ true, true },
        .consumed = &.{ true, true },
        .error_ranks = .{ .ctx = &meta, .make = struct {
            fn make(_: *const anyopaque, alloc: std.mem.Allocator, n: usize) anyerror![]const usize {
                const ranks = try alloc.alloc(usize, n);
                for (ranks, 0..) |*rank, i| rank.* = i;
                return ranks;
            }
        }.make },
    };
    // Probe OOM is intentionally absorbed: exhaust every allocation site and assert cleanup/diagnostic preservation.
    var count: usize = 0;
    while (count < 64) : (count += 1) {
        var failing = std.testing.FailingAllocator.init(std.testing.allocator, .{ .fail_index = count });
        const failure = canonicalDecodeFailure(
            failing.allocator(),
            in,
            .{},
            &.{},
            true,
            .{ .err = error.UnexpectedPage, .site = .{ .rg = in.rg, .column = 1, .page = 0 } },
        );
        try std.testing.expectEqual(error.UnexpectedPage, failure.err);
        try std.testing.expect(failure.site.column == 0 or failure.site.column == 1);
        try std.testing.expectEqual(failing.allocated_bytes, failing.freed_bytes);
        if (!failing.has_induced_failure) {
            try std.testing.expectEqual(@as(usize, 0), failure.site.column);
            break;
        }
    }
    try std.testing.expect(count < 64);
    var ledger: core_memory.SharedBudget = .{ .limit = 1024 };
    var charged: core_memory.Charged = .{ .child = std.testing.allocator, .budget = &ledger };
    const original: decode_order.Failure = .{
        .err = error.UnexpectedPage,
        .site = .{ .rg = in.rg, .column = 1, .page = 0 },
    };
    const bounded = decode_order.canonical(SliceReader, isDecodeError, charged.allocator(), in, .{}, &.{ false, false }, original);
    try std.testing.expectEqual(original.err, bounded.err);
    try std.testing.expectEqual(@as(usize, 0), ledger.used.load(.monotonic));
    try std.testing.expect(ledger.peak.load(.monotonic) <= ledger.limit);
    // A marker whose failure cannot be reproduced never outranks a real page, even at the same row.
    columns[0].meta_data.?.num_values = 0;
    const reproduced = decode_order.canonical(SliceReader, isDecodeError, std.testing.allocator, in, .{}, &.{ false, false }, .{ .err = error.UnexpectedPage, .site = .{ .rg = in.rg, .column = 0, .page = 0 } });
    try std.testing.expectEqual(@as(usize, 1), reproduced.site.column);
}

test "cache decode admission counts inputs and DAG, follows sharing, and releases failed planning allocations" {
    const hw = @import("hw.zig");
    defer hw.test_cpu = null;
    hw.test_cpu = .{ .l2 = .{ .bytes = 1 << 20, .max_sharing = 2 } };
    var elements = [_]schema.SchemaElement{ fl_test_elem, fl_test_elem };
    elements[0].name = "schema";
    elements[0].type = null;
    elements[0].repetition_type = .REQUIRED;
    elements[0].num_children = 1;
    elements[1].type = .DOUBLE;
    var path = [_][]const u8{"v"};
    var columns = [_]schema.ColumnChunk{.{ .file_path = null, .file_offset = 0, .meta_data = .{
        .type = .DOUBLE,
        .encodings = .empty,
        .path_in_schema = .{ .items = &path, .capacity = 1, .pointer_stability = .{} },
        .codec = .UNCOMPRESSED,
        .num_values = 65553,
        .total_uncompressed_size = 65553 * 8,
        .total_compressed_size = 65553 * 8,
        .data_page_offset = 0,
        .index_page_offset = null,
        .dictionary_page_offset = null,
    } }};
    var rg: schema.RowGroup = .{
        .columns = .{ .items = &columns, .capacity = 1, .pointer_stability = .{} },
        .total_byte_size = 65553 * 8,
        .num_rows = 65553,
    };
    const meta: schema.FileMetaData = .{
        .version = 1,
        .schema = .{ .items = &elements, .capacity = 2, .pointer_stability = .{} },
        .num_rows = 65553,
        .created_by = null,
        .row_groups = .empty,
    };
    const in: AggRowGroup = .{
        .rg = &rg,
        .meta = &meta,
        .src = .{ .bytes = &.{}, .byte_origin = 0 },
        .filter = null,
        .read = &.{true},
        .consumed = &.{true},
    };
    var x: expr_ast.Expr = .{ .col_ref = .{ .col_idx = 0, .physical_type = .DOUBLE, .expr_type = .f64 } };
    var one: expr_ast.Expr = .{ .literal = .{ .i64 = 1 } };
    const sum: expr_ast.Expr = .{ .binop = .{
        .op = .add,
        .left = &x,
        .right = &one,
        .result_type = .f64,
        .depth = 2,
    } };
    var call = [_]expr_agg.AggCall{.{ .func = .sum, .arg = sum, .result = .f64, .where = null, .alias = "s" }};
    const target: AggTarget = .{ .scalar = .{ .calls = &call, .accumulators = &.{} } };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, struct {
        fn run(a: std.mem.Allocator, input: AggRowGroup, t: AggTarget) !void {
            const n = try automaticCacheRows(a, input, t, 16);
            try std.testing.expect(n >= 64 and n < 65553 and n % 64 == 0);
        }
    }.run, .{ in, target });
    const small = try automaticCacheRows(std.testing.allocator, in, target, 16);
    hw.test_cpu.?.l2.bytes = 1280 << 10;
    const larger = try automaticCacheRows(std.testing.allocator, in, target, 16);
    try std.testing.expect(larger > small);
    hw.test_cpu.?.l3.bytes = 16 << 20;
    try std.testing.expectEqual(@as(usize, 0), try automaticCacheRows(std.testing.allocator, in, target, 1));
    try std.testing.expect(try automaticCacheRows(std.testing.allocator, in, target, 16) > 0);
    hw.test_cpu.?.l3.bytes = 0;
    hw.test_cpu.?.l2.bytes = 0;
    try std.testing.expectEqual(@as(usize, 0), try automaticCacheRows(std.testing.allocator, in, target, 16));
    hw.test_cpu.?.l2.bytes = 1 << 20;
    call[0].arg = x;
    try std.testing.expectEqual(@as(usize, 0), try automaticCacheRows(std.testing.allocator, in, target, 16));
}

test "cache decode admission bounds dictionary strings and composite domains from every page header" {
    const testing = std.testing;
    const hw = @import("hw.zig");
    defer hw.test_cpu = null;
    hw.test_cpu = .{ .l2 = .{ .bytes = 1 << 20, .max_sharing = 2 } };
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const Fixture = struct {
        fn bytes(alloc: std.mem.Allocator, entries: i32, width: i32, last: schema.Encoding) ![]const u8 {
            var w = thrift.Writer.init(alloc);
            var h: schema.PageHeader = .{
                .type = .DICTIONARY_PAGE,
                .uncompressed_page_size = width,
                .compressed_page_size = 1,
                .crc = null,
                .data_page_header = null,
                .data_page_header_v2 = null,
                .dictionary_page_header = .{ .num_values = entries, .encoding = .PLAIN, .is_sorted = null },
            };
            try h.write(&w);
            try w.buffer.append(alloc, 0);
            h.type = .DATA_PAGE;
            h.uncompressed_page_size = 1;
            h.dictionary_page_header = null;
            h.data_page_header = .{
                .num_values = 32768,
                .encoding = .RLE_DICTIONARY,
                .definition_level_encoding = .RLE,
                .repetition_level_encoding = .RLE,
            };
            try h.write(&w);
            try w.buffer.append(alloc, 0);
            h.type = .DATA_PAGE_V2;
            h.data_page_header = null;
            h.data_page_header_v2 = .{
                .num_values = 32785,
                .num_nulls = 0,
                .num_rows = 32785,
                .encoding = last,
                .definition_levels_byte_length = 0,
                .repetition_levels_byte_length = 0,
                .is_compressed = true,
            };
            try h.write(&w);
            try w.buffer.append(alloc, 0);
            return w.bytes(); // Bodies deliberately aren't decoded by this admission-only hint.
        }
    };
    var elements = [_]schema.SchemaElement{ fl_test_elem, fl_test_elem, fl_test_elem };
    elements[0].name = "schema";
    elements[0].type = null;
    elements[0].repetition_type = .REQUIRED;
    elements[0].num_children = 2;
    elements[1].type = .DOUBLE;
    elements[1].name = "x";
    elements[2].type = .BYTE_ARRAY;
    elements[2].name = "g";
    var paths = [_][1][]const u8{ .{"x"}, .{"g"} };
    var columns: [2]schema.ColumnChunk = undefined;
    for (&columns, &paths, 0..) |*c, *path, i| c.* = .{
        .file_path = null,
        .file_offset = 1000,
        .meta_data = .{
            .type = if (i == 0) .DOUBLE else .BYTE_ARRAY,
            .encodings = .empty,
            .path_in_schema = .{ .items = path, .capacity = 1, .pointer_stability = .{} },
            .codec = .ZSTD,
            .num_values = 65553,
            .total_uncompressed_size = 65553 * 8,
            .total_compressed_size = 0,
            .data_page_offset = 1000,
            .index_page_offset = null,
            .dictionary_page_offset = if (i == 1) 1000 else null,
        },
    };
    var rg: schema.RowGroup = .{
        .columns = .{ .items = &columns, .capacity = 2, .pointer_stability = .{} },
        .total_byte_size = 65553 * 8,
        .num_rows = 65553,
    };
    const meta: schema.FileMetaData = .{
        .version = 1,
        .schema = .{ .items = &elements, .capacity = 3, .pointer_stability = .{} },
        .num_rows = rg.num_rows,
        .created_by = null,
        .row_groups = .empty,
    };
    var in: AggRowGroup = .{
        .rg = &rg,
        .meta = &meta,
        .src = .{ .bytes = &.{}, .byte_origin = 1000 },
        .filter = null,
        .read = &.{ true, true },
        .consumed = &.{ true, true },
    };
    var x: expr_ast.Expr = .{ .col_ref = .{ .col_idx = 0, .physical_type = .DOUBLE, .expr_type = .f64 } };
    var one: expr_ast.Expr = .{ .literal = .{ .i64 = 1 } };
    const computed: expr_ast.Expr = .{ .binop = .{
        .op = .add,
        .left = &x,
        .right = &one,
        .result_type = .f64,
        .depth = 2,
    } };
    const calls = [_]expr_agg.AggCall{.{ .func = .sum, .arg = computed, .result = .f64, .where = null, .alias = "s" }};
    const key: expr_ast.Expr = .{ .col_ref = .{ .col_idx = 1, .physical_type = .BYTE_ARRAY, .expr_type = .str } };
    var target: AggTarget = .{ .grouped = .{ .calls = &calls, .keys = &.{key}, .table = undefined, .pos_base = 0 } };
    var small: usize = 0;
    for ([_]i32{ 24, 4000, 2 << 20 }) |width| {
        const bytes = try Fixture.bytes(a, 2, width, .RLE_DICTIONARY);
        in.src.bytes = bytes;
        columns[1].meta_data.?.total_compressed_size = @intCast(bytes.len);
        const proof = cacheDictionary(in, 1).?;
        var funding: core_memory.SharedBudget = .{ .limit = 64 << 10 };
        var charge: core_memory.Charged = .{ .child = testing.allocator, .budget = &funding };
        var memo: CacheHints = .{ .allocator = charge.allocator() };
        try memo.initShards(3);
        try testing.expectEqualDeep(proof, memo.get(in, 1).?);
        try testing.expectEqualDeep(proof, memo.get(in, 1).?);
        try testing.expectEqual(@as(usize, 1), memo.count());
        memo.deinit();
        try testing.expectEqual(@as(usize, 0), funding.used.load(.monotonic));
        var failing = testing.FailingAllocator.init(testing.allocator, .{ .fail_index = 0 });
        var uncached: CacheHints = .{ .allocator = failing.allocator() };
        try testing.expectError(error.OutOfMemory, uncached.initShards(3));
        try testing.expectEqual(@as(usize, 0), uncached.shards.len);
        try testing.expectEqualDeep(proof, uncached.get(in, 1).?);
        try testing.expectEqual(@as(usize, 0), uncached.count());
        uncached.deinit();

        try testing.expectEqual(@as(usize, @intCast(width)), proof.bytes);
        const rows = try automaticCacheRows(testing.allocator, in, target, 16);
        if (width == 24) {
            small = rows;
            try testing.expect(small >= 64 and small < 65553);
        } else try testing.expect(rows < small);
        if (width == 2 << 20) try testing.expectEqual(@as(usize, 0), rows);
    }
    // Every data page must qualify, including V2 after an eligible V1 page.
    in.src.bytes = try Fixture.bytes(a, 2, 24, .PLAIN);
    columns[1].meta_data.?.total_compressed_size = @intCast(in.src.bytes.len);
    try testing.expect(cacheDictionary(in, 1) == null);
    try testing.expectEqual(@as(usize, 0), try automaticCacheRows(testing.allocator, in, target, 16));
    in.src.bytes = try Fixture.bytes(a, 256, 4096, .RLE_DICTIONARY);
    columns[1].meta_data.?.total_compressed_size = @intCast(in.src.bytes.len);
    // Two individually bounded keys can still have a composite table much larger than L2.
    target.grouped.keys = &.{ key, key };
    try testing.expectEqual(@as(usize, 0), try automaticCacheRows(testing.allocator, in, target, 16));
    target.grouped.keys = &.{key};
    in.src.bytes = try Fixture.bytes(a, 257, 24, .RLE_DICTIONARY);
    columns[1].meta_data.?.total_compressed_size = @intCast(in.src.bytes.len);
    try testing.expectEqual(@as(usize, 0), try automaticCacheRows(testing.allocator, in, target, 16));
    columns[1].meta_data.?.dictionary_page_offset = -1;
    try testing.expect(cacheDictionary(in, 1) == null);
}

test "scalar cache DAG shares nullable inputs without changing canonical sum blocks, including allocation failure" {
    try std.testing.checkAllAllocationFailures(std.testing.allocator, testScalarCache, .{});
}

fn testScalarCache(backing: std.mem.Allocator) !void {
    const testing = std.testing;
    var arena = std.heap.ArenaAllocator.init(backing);
    defer arena.deinit();
    const a = arena.allocator();
    const n = 193;
    const values = try a.alloc(f64, n);
    const defs = try a.alloc(u32, n);
    for (values, defs, 0..) |*v, *d, i| {
        v.* = switch (i % 6) {
            0 => 1e16,
            1 => 1,
            2 => -1e16,
            3 => 0.1,
            4 => -0.0,
            else => std.math.nan(f64),
        };
        d.* = @intFromBool(i % 17 != 0);
    }
    var x: expr_ast.Expr = .{ .col_ref = .{ .col_idx = 0, .physical_type = .DOUBLE, .expr_type = .f64 } };
    var one: expr_ast.Expr = .{ .literal = .{ .i64 = 1 } };
    var base: expr_ast.Expr = .{ .binop = .{
        .op = .add,
        .left = &x,
        .right = &one,
        .result_type = .f64,
        .depth = 2,
    } };
    var repeated = base;
    const square: expr_ast.Expr = .{ .binop = .{
        .op = .mul,
        .left = &base,
        .right = &repeated,
        .result_type = .f64,
        .depth = 3,
    } };
    const calls = [_]expr_agg.AggCall{
        .{ .func = .sum, .arg = base, .result = .f64, .where = null, .alias = "s" },
        .{ .func = .sum, .arg = square, .result = .f64, .where = null, .alias = "ss" },
        .{ .func = .avg, .arg = base, .result = .avg_f64, .where = null, .alias = "a" },
        .{ .func = .count, .arg = base, .result = .i64, .where = null, .alias = "n" },
        .{ .func = .min, .arg = base, .result = .f64, .where = null, .alias = "lo" },
    };
    const cols = [_]filter_eval.Batch.Column{.{ .f64 = .{
        .values = values,
        .def_levels = defs,
        .max_def = 1,
        .has_nulls = true,
    } }};
    const batch: filter_eval.Batch = .{ .cols = &cols, .num_rows = n };
    for ([_]bool{ false, true }) |empty| {
        var sel = try filter_selection.SelectionVector.init(a, n);
        for (0..n) |i| sel.set(i, !empty and i % 3 != 1);
        var want: [calls.len]expr_agg.Accumulator = undefined;
        var got: [calls.len]expr_agg.Accumulator = undefined;
        for (calls, &want, &got) |call, *w, *g| {
            w.* = .init(call);
            g.* = .init(call);
            try expr_agg.updateOne(a, a, w, call, &batch, &.{0}, &sel);
        }
        var off: usize = 0;
        while (off < n) : (off += 64) {
            const count = @min(64, n - off);
            const col = try cols[0].sliceRows(off, count);
            const part: filter_eval.Batch = .{ .cols = &.{col}, .num_rows = count };
            const mask = try sel.alignedWindow(off, count);
            try testing.expect(try foldScalarCache(
                a,
                a,
                &calls,
                &got,
                &@as([calls.len]bool, @splat(false)),
                &part,
                &.{0},
                &mask,
            ));
        }
        for (want, got) |w, g| switch (w) {
            .sum_f, .min_f => |expected| {
                const actual = if (g == .sum_f) g.sum_f else g.min_f;
                try testing.expectEqual(expected != null, actual != null);
                if (expected) |v| try testing.expectEqual(@as(u64, @bitCast(v)), @as(u64, @bitCast(actual.?)));
            },
            .avg => |avg| {
                try testing.expectEqual(avg.count, g.avg.count);
                try testing.expectEqual(@as(u64, @bitCast(avg.sum)), @as(u64, @bitCast(g.avg.sum)));
            },
            .count => |count| try testing.expectEqual(count, g.count),
            else => unreachable,
        };
    }
}

test "writeAggregateRows preserves the automatic Bloom policy for small and parallel answers" {
    var arena_state = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena_state.deinit();
    const a = arena_state.allocator();
    for ([_]usize{ 1000, 70000 }) |n| {
        const cols = try aggAnswerCols(a, n, null);
        for ([_]bool{ false, true }) |enabled| for ([_]usize{ 1, 3 }) |workers| {
            var capture: AnswerCapture = .{ .a = a };
            _ = try writeAggregateRows(a, capture.sink(), cols, @intCast(n), .SNAPPY, .{}, .{
                .dictionary_bloom = enabled,
                .workers = workers,
                .budget = std.math.maxInt(u64),
            });
            const meta = try answerFooter(a, capture.bytes.items);
            const repeated = meta.row_groups.items[0].columns.items[1].meta_data.?;
            try std.testing.expect(repeated.dictionary_page_offset != null);
            try std.testing.expectEqual(enabled, repeated.bloom_filter_offset != null);
        };
    }
}

test "copied INT96 dictionary filters hash raw entries and decline noncanonical times" {
    var arena_state = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena_state.deinit();
    const a = arena_state.allocator();
    const bloom = @import("parquet/bloom.zig");
    var elem: schema.SchemaElement = .{
        .name = "t",
        .type = .FIXED_LEN_BYTE_ARRAY,
        .type_length = int96_mod.INT96_WIDTH,
        .repetition_type = .REQUIRED,
        .num_children = 0,
        .scale = null,
        .precision = null,
        .field_id = null,
    };
    var raw: [2][12]u8 = @splat(@splat(0));
    for (&raw) |*v| std.mem.writeInt(u32, v[8..12], 2440588, .little);
    var values: [256][]const u8 = undefined;
    for (&values, 0..) |*v, i| v.* = &raw[i % 2];
    for ([_]bool{ false, true }) |noncanonical| {
        std.mem.writeInt(i64, raw[1][0..8], if (noncanonical) std.time.ns_per_day else 2_000_000_000, .little);
        const encoded = try encoder.encodeColumn(a, .{
            .values = .{ .string = .{ .values = &values } },
            .schema_elem = &elem,
            .path_in_schema = &.{"t"},
            .dictionary_bloom = false,
        });
        var cm = encoded.meta;
        cm.type = .INT96;
        var root = elem;
        root.name = "root";
        root.type = null;
        root.type_length = null;
        root.num_children = 1;
        var meta: schema.FileMetaData = .{
            .version = 2,
            .schema = .empty,
            .num_rows = values.len,
            .created_by = schema.ZPQ_CREATED_BY,
            .row_groups = .empty,
        };
        try meta.schema.append(a, root);
        var leaf = elem;
        leaf.type = .INT96;
        leaf.type_length = null;
        try meta.schema.append(a, leaf);
        const filter = try dictionaryBloom(a, encoded.bytes, cm, .{ .max_def = 0, .max_rep = 0 }, &meta);
        if (noncanonical) {
            try std.testing.expect(filter == null);
        } else {
            const bits = bloom.bitset(filter.?).?;
            for (raw) |v| try std.testing.expect(bloom.Probe.init(bloom.hashBytes(&v)).mightContain(bits));
        }
    }
}

test "grouped key dictionaries keep later-key codes with and without dense lanes" {
    const testing = std.testing;
    var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_state.deinit();
    const a = arena_state.allocator();
    var ints: [256]i32 = undefined;
    var names: [256][]const u8 = undefined;
    for (&ints, &names, 0..) |*k, *v, i| {
        k.* = @intCast(i % 2);
        v.* = try std.fmt.allocPrint(a, "dictionary-value-{d}", .{i % 64});
    }
    const cols = [_]expr_agg.OutputCol{
        .{ .name = "a", .parquet_type = .INT32, .nullable = false, .col = .{ .i32 = .{ .values = &ints } } },
        .{ .name = "b", .parquet_type = .BYTE_ARRAY, .nullable = false, .col = .{ .string = .{ .values = &names } } },
    };
    var capture: AnswerCapture = .{ .a = a };
    _ = try writeAggregateRows(a, capture.sink(), &cols, ints.len, .SNAPPY, .{}, .{});
    const meta = try metadata.open(a, capture.bytes.items);
    const rg = &meta.row_groups.items[0];
    const src: RGSrc = .{ .bytes = capture.bytes.items, .byte_origin = 0 };
    var lanes: agg_dense.Lanes = undefined;
    var target: AggTarget = .{ .grouped = .{
        .calls = &.{},
        .keys = &.{
            .{ .col_ref = .{ .col_idx = 0, .physical_type = .INT32, .expr_type = .i64 } },
            .{ .col_ref = .{ .col_idx = 1, .physical_type = .BYTE_ARRAY, .expr_type = .str } },
        },
        .dense = &lanes,
        .table = undefined,
        .pos_base = 0,
    } };
    try testing.expect(target.codedKey(0));
    try testing.expect(target.codedKey(1));
    try testing.expect(!target.codedKey(2));
    target.grouped.dense = null;
    try testing.expect(target.codedKey(0));
    try testing.expect(target.codedKey(1));
    const cm = rg.columns.items[1].meta_data.?;
    const start: usize = @intCast(cm.dictionary_page_offset.?);
    const chunk = capture.bytes.items[start..][0..@intCast(cm.total_compressed_size)];
    for ([_]bool{ false, true }) |force| {
        const options: DecodeOptions = .{ .encoded = true, .any_dictionary = force and target.codedKey(1) };
        var scratch = DecodeScratch.init(testing.allocator);
        defer scratch.deinit();
        const col = (try decodeAggColumnCoded(a, chunk, cm, &meta, options, &scratch)).?;
        try testing.expectEqual(force, col.codes() != null);
        if (!force) continue;
        for ([_]usize{ 1, 17, 64, 256 }) |step| {
            var reader = (try SliceReader.init(a, testing.allocator, rg, &meta, src, 1, options)).?;
            defer reader.deinit();
            var slice = std.heap.ArenaAllocator.init(testing.allocator);
            defer slice.deinit();
            var done: usize = 0;
            while (done < names.len) {
                _ = slice.reset(.free_all);
                const got = try reader.next(slice.allocator(), step);
                const codes = got.codes().?;
                try testing.expect(got.string.dict.len > names.len / 8);
                for (codes, 0..) |code, i|
                    try testing.expectEqualStrings(names[done + i], got.string.dict[code]);
                done += codes.len;
            }
        }
    }
    // A slice spanning the dictionary/PLAIN boundary must gather its prefix without rereading pages.
    var mixed: std.ArrayList(u8) = .empty;
    var coded_body: std.ArrayList(u8) = .empty;
    try coded_body.append(a, 1);
    try coded_body.appendSlice(a, try @import("parquet/encoding/hybrid_rle.zig").encode(a, &.{ 0, 1, 0, 1 }, 1));
    const dict_values = [_]i32{ 10, 20 };
    const plain_values = [_]i32{ 30, 40, 50, 60 };
    const bodies = [_][]const u8{ std.mem.asBytes(&dict_values), coded_body.items, std.mem.asBytes(&plain_values) };
    for (bodies, 0..) |body, i| {
        var w = thrift.Writer.init(a);
        const h: schema.PageHeader = .{
            .type = if (i == 0) .DICTIONARY_PAGE else .DATA_PAGE,
            .uncompressed_page_size = @intCast(body.len),
            .compressed_page_size = @intCast(body.len),
            .crc = null,
            .data_page_header_v2 = null,
            .data_page_header = if (i == 0) null else .{
                .num_values = 4,
                .encoding = if (i == 1) .RLE_DICTIONARY else .PLAIN,
                .definition_level_encoding = .RLE,
                .repetition_level_encoding = .RLE,
            },
            .dictionary_page_header = if (i == 0) .{ .num_values = 2, .encoding = .PLAIN, .is_sorted = null } else null,
        };
        try h.write(&w);
        try mixed.appendSlice(a, w.bytes());
        try mixed.appendSlice(a, body);
    }
    var mixed_rg = rg.*;
    mixed_rg.columns.items = try a.dupe(schema.ColumnChunk, rg.columns.items);
    const mixed_cm = &mixed_rg.columns.items[0].meta_data.?;
    mixed_cm.codec = .UNCOMPRESSED;
    mixed_cm.dictionary_page_offset = 0;
    mixed_cm.data_page_offset = 0;
    mixed_cm.total_compressed_size = @intCast(mixed.items.len);
    mixed_cm.num_values = 8;
    const expected = [_]i32{ 10, 20, 10, 20, 30, 40, 50, 60 };
    for ([_]usize{ 1, 3, 5, 8 }) |step| {
        var r = (try SliceReader.init(
            a,
            testing.allocator,
            &mixed_rg,
            &meta,
            .{ .bytes = mixed.items, .byte_origin = 0 },
            0,
            .{ .encoded = true, .any_dictionary = true },
        )).?;
        defer r.deinit();
        var done: usize = 0;
        while (done < expected.len) {
            const c = (try r.next(a, step)).i32;
            const n = if (c.codes) |codes| codes.len else c.values.len;
            for (0..n) |i| try testing.expectEqual(
                expected[done + i],
                if (c.codes) |codes| c.dict[codes[i]] else c.values[i],
            );
            done += n;
        }
    }
    const Trial = struct {
        fn run(gpa: std.mem.Allocator, bytes: []const u8, m: *const schema.FileMetaData) !void {
            var arena = std.heap.ArenaAllocator.init(gpa);
            defer arena.deinit();
            const alloc = arena.allocator();
            var reader = (try SliceReader.init(
                alloc,
                gpa,
                &m.row_groups.items[0],
                m,
                .{ .bytes = bytes, .byte_origin = 0 },
                1,
                .{ .encoded = true, .any_dictionary = true },
            )).?;
            defer reader.deinit();
            var slice = std.heap.ArenaAllocator.init(gpa);
            defer slice.deinit();
            while (reader.left != 0) {
                _ = slice.reset(.free_all);
                const c = try reader.next(slice.allocator(), 17);
                try std.testing.expect(c.codes() != null);
            }
        }
    };
    try testing.checkAllAllocationFailures(testing.allocator, Trial.run, .{ capture.bytes.items, &meta });
}
