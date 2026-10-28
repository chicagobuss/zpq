const std = @import("std");
const schema = @import("../schema.zig");
const filter_eval = @import("../filter/eval.zig");
const decimal_mod = @import("../parquet/decimal.zig");
const int96_mod = @import("../parquet/int96.zig");
const column_mod = @import("../parquet/column.zig");
const DecodeOptions = column_mod.DecodeOptions;
const DecodeScratch = column_mod.DecodeScratch;
const consumer_column = @import("column.zig");
const PruningInfo = consumer_column.PruningInfo;
const flbaWidth = consumer_column.flbaWidth;
const decodeColumnTPruned = consumer_column.decodeColumnTPruned;
const decodeFlbaColumnPruned = consumer_column.decodeFlbaColumnPruned;
const decodeFloat16ColumnAsF64Pruned = consumer_column.decodeFloat16ColumnAsF64Pruned;
const decodeU32ColumnAsI64Pruned = consumer_column.decodeU32ColumnAsI64Pruned;

/// `decodeAggColumn` as dictionary codes, or null when the chunk's type or encoding has no coded form here.
pub fn decodeAggColumnCoded(
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

const codedOrValues = consumer_column.codedOrValues;

/// `decodeAggColumnCoded` folded straight into a code histogram, or null when the chunk doesn't qualify (the caller
/// then decodes it afresh). Decimal and unsigned-INT32 dictionaries convert once per entry. `firsts` (a GROUP BY key,
/// `agg_coded.histGroupKey`) also notes each entry's first row, for keys typed exactly as their codes decode.
pub fn decodeAggColumnHist(
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
pub fn decodeLevelsOnly(
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

const codedCol = consumer_column.codedCol;

const valuesCol = consumer_column.valuesCol;

const decodeCodes = consumer_column.decodeCodes;


/// Decode one column chunk into the batch lane its physical and logical type select.
pub fn decodeAggColumn(
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

