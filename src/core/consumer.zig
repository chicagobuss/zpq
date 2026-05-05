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

    for (output_specs) |spec| {
        // Build the (filtered_values, schema_elem, path_in_schema)
        // tuple per output column. Passthrough copies from the
        // decoded input + uses the input's schema element; computed
        // evaluates an expression against the post-decode batch and
        // synthesizes a flat leaf SchemaElement on the fly.
        var path_in_schema: []const []const u8 = undefined;
        var leaf_elem: schema.SchemaElement = undefined;
        var filtered: filter_eval.Batch.Column = undefined;

        switch (spec) {
            .passthrough => |kept_ci| {
                const batch_pos = batch_pos_for_col[kept_ci] orelse return error.MissingDecodedColumn;
                filtered = try encoder.applySelection(out_arena, batch_cols.items[batch_pos], &sel);
                const cm = rg.columns.items[kept_ci].meta_data orelse return error.ColumnMetaMissing;
                leaf_elem = meta.getColumnSchema(cm.path_in_schema.items) orelse return error.SchemaLookupFailed;
                path_in_schema = cm.path_in_schema.items;
            },
            .computed => |c| {
                const result = try expr_eval.evalExpr(ra, &batch, lookup, c.expr);
                filtered = try encoder.applySelection(out_arena, result, &sel);
                const path_buf = try out_arena.alloc([]const u8, 1);
                path_buf[0] = c.alias;
                path_in_schema = path_buf;
                const expr_type = c.expr.typeOf();
                // BYTE_ARRAY columns need a UTF8 / STRING annotation
                // for downstream readers (pyarrow, polars, etc) to
                // surface them as strings rather than raw binary. Our
                // string concat result is always UTF-8 because the
                // input string columns are UTF-8.
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

        const t_enc_start = nowMonoNs();
        const enc = try encoder.encodeColumn(out_arena, .{
            .values = filtered,
            .schema_elem = &leaf_elem,
            .path_in_schema = path_in_schema,
            .codec = output_codec,
        });
        const t_enc_end = nowMonoNs();
        timings.encode_ns += @intCast(t_enc_end - t_enc_start);

        const col_start_in_file: i64 = @intCast(out_offset.*);
        var em = enc.meta;
        em.data_page_offset += col_start_in_file;
        if (em.dictionary_page_offset) |dpo| em.dictionary_page_offset = dpo + col_start_in_file;
        try sink.write(enc.bytes);
        const t_sink_end = nowMonoNs();
        timings.sink_ns += @intCast(t_sink_end - t_enc_end);
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
