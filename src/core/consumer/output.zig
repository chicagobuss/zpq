const std = @import("std");
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

