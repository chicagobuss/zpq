const expr_ast = @import("../expr/ast.zig");
const filter_ast = @import("../filter/ast.zig");
const profile = @import("../../profile.zig");
const nowMonoNs = @import("../../clock.zig").monoNs;
const DecodeOptions = @import("../parquet/column.zig").DecodeOptions;
const RGSrc = types.RGSrc;
const Timings = types.Timings;
const consumer_column = @import("column.zig");
const decimalKind = consumer_column.decimalKind;
const decodeDecimalI128 = consumer_column.decodeDecimalI128;
const decodeWriteColumn = consumer_column.decodeWriteColumn;
const decodeWriteColumnMaybeCoded = consumer_column.decodeWriteColumnMaybeCoded;
const std = @import("std");
const expr_eval = @import("../expr/eval.zig");
const filter_selection = @import("../filter/selection.zig");
const encoder = @import("../writer/encoder.zig");
const agg_coded = @import("../expr/agg_coded.zig");
const filter_eval = @import("../filter/eval.zig");
const schema = @import("../schema.zig");
const decimal_mod = @import("../parquet/decimal.zig");
const types = @import("types.zig");
const OutputCol = types.OutputCol;
const computedSchemaElem = types.computedSchemaElem;
const renameLeaf = types.renameLeaf;

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


/// Append the rows `sel` keeps of a flat column straight into the aggregator's buffer: one copy, no intermediate
/// filtered column. String bytes are copied into one block of `arena`, since the decoded pages they point into die
/// with the row group.
///
/// A coded column (`codes` + `dict`) keeps its codes when `keep_codes` lets the buffer take them: the buffer is still
/// empty, so every code indexes this one dictionary, which is copied into `arena`. Otherwise its values are gathered
/// first, on `scratch`.
pub fn appendSelected(
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

pub fn appendIntoBuf(
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

pub fn appendTyped(
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


/// A named passthrough's leaf as the flat column it is written as: present (`def == max_def`) or NULL, at one level.
/// A nested leaf counts its parents' levels too, which a flat leaf has none of. Others pass through as they are.
pub fn namedLevels(scratch: std.mem.Allocator, pt: OutputCol.Passthrough, col: filter_eval.Batch.Column) !filter_eval.Batch.Column {
    if (pt.name == null) return col;
    return switch (col) {
        inline else => |c, tag| @unionInit(filter_eval.Batch.Column, @tagName(tag), try flatLevels(@TypeOf(c.values[0]), scratch, c)),
    };
}

pub fn flatLevels(comptime T: type, scratch: std.mem.Allocator, c: filter_eval.ColumnT(T)) !filter_eval.ColumnT(T) {
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

/// `col`, a computed output's values, in the lane of its buffer `buf`: expressions evaluate a BOOLEAN as 0/1 and a
/// DATE as its days, both in the integer lane.
pub fn computedLane(scratch: std.mem.Allocator, buf: OutputAggregator.ColumnBuf, col: filter_eval.Batch.Column) !filter_eval.Batch.Column {
    if (col != .i64) return col;
    const c = col.i64;
    switch (buf) {
        .boolean => {
            const values = try scratch.alloc(bool, c.values.len);
            for (c.values, values) |v, *o| o.* = v != 0;
            return .{ .boolean = .{ .values = values, .def_levels = c.def_levels, .max_def = c.max_def, .has_nulls = c.has_nulls } };
        },
        .i32 => {
            // Days `make_date` range-checked to INTEGER; a NULL row's slot holds no value to check.
            const values = try scratch.alloc(i32, c.values.len);
            for (c.values, values) |v, *o| o.* = @truncate(v);
            return .{ .i32 = .{ .values = values, .def_levels = c.def_levels, .max_def = c.max_def, .has_nulls = c.has_nulls } };
        },
        else => return col,
    }
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
                const col = try computedLane(ra, agg.cols[i], try expr_eval.evalExpr(ra, &s.batch, self.lookup, c.expr));
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
pub fn gatherRows(
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
            // A length-only string column has no value slices. Compacting the empty buffer would read past it.
            if (!c.codesOnly() and (c.byte_lengths == null or c.values.len != 0)) {
                const vals = try arena.alloc(T, surviving);
                encoder.compactByMask(T, c.values, sel.mask, vals);
                out.values = vals;
            }
            if (c.byte_lengths) |lens| {
                const kept_lens = try arena.alloc(u32, surviving);
                encoder.compactByMask(u32, lens, sel.mask, kept_lens);
                out.byte_lengths = kept_lens;
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
                break :blk try computedLane(scratch, agg.cols[i], try expr_eval.evalExpr(scratch, &survived.?.batch, lookup, c.expr));
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

