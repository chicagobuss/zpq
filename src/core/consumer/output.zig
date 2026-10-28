const std = @import("std");
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

