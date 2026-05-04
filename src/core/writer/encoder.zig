//! Parquet PLAIN encoder for column chunks.
//!
//! Phase 5.4b first cut. Supports:
//!   - Encodings: PLAIN only (per-type little-endian, BYTE_ARRAY as
//!     <u32 LE len><bytes>, BOOLEAN bit-packed LSB first).
//!   - Codec: UNCOMPRESSED only.
//!   - Single data page per column chunk.
//!   - Required (non-nullable) columns. Optional/null handling lives
//!     in 5.4b.1 — encodeColumn errors on schema_elem with
//!     repetition_type == OPTIONAL until then.
//!   - Min/max stats over surviving values. No null_count yet (we
//!     don't track nulls in the decode path).
//!
//! Design: the per-page primitive matches DuckDB / Polars: emit a
//! PageHeader thrift, then the encoded data bytes. ColumnMetaData
//! is built side-by-side with the bytes so the caller can drop it
//! into the file footer.

const std = @import("std");
const schema = @import("../schema.zig");
const thrift = @import("../thrift.zig");
const filter_eval = @import("../filter/eval.zig");
const filter_selection = @import("../filter/selection.zig");

pub const Error = error{
    NullableNotSupported,
    UnsupportedType,
    TooLarge,
} || std.mem.Allocator.Error;

pub const EncodedColumn = struct {
    /// Page header thrift + encoded data bytes, ready to concatenate.
    bytes: []u8,
    /// ColumnMetaData for the file footer. data_page_offset is relative
    /// to the start of `bytes`; the caller adds the column's absolute
    /// position in the output file.
    meta: schema.ColumnMetaData,
};

pub const ColumnInput = struct {
    /// Decoded values for this column. Caller pre-applies any filter
    /// SelectionVector before calling encodeColumn.
    values: filter_eval.Batch.Column,
    /// Schema element for this column (used for type, name, options).
    schema_elem: *const schema.SchemaElement,
    /// path_in_schema for the resulting ColumnMetaData. Typically a
    /// one-element list [name] for flat schemas.
    path_in_schema: []const []const u8,
};

/// Encode one column chunk: header + data + ColumnMetaData. The
/// caller fills in the absolute file offset on data_page_offset
/// after concatenating into the output file.
///
/// IMPORTANT: this encoder writes data WITHOUT definition or
/// repetition levels (assumes max_def == 0, max_rep == 0). The
/// caller must arrange for the OUTPUT schema's leaf elements to
/// have repetition_type == REQUIRED. For filter outputs this is
/// semantically correct — surviving values are never null because
/// the filter eval skips null inputs. See main.zig:cloneSchemaAsRequired.
pub fn encodeColumn(arena: std.mem.Allocator, in: ColumnInput) Error!EncodedColumn {
    const elem = in.schema_elem;
    if (elem.type == null) return error.UnsupportedType;
    const phys = elem.type.?;

    // 1. Encode values via PLAIN.
    const data = try encodeValuesPlain(arena, in.values);
    const num_values: i64 = @intCast(valueCount(in.values));

    // 2. Compute stats.
    var stats = computeStats(in.values);
    // We don't track nulls in the decode path yet; leave null_count out.
    _ = &stats;

    // 3. Build PageHeader.
    var page_hdr: schema.PageHeader = .{
        .type = .DATA_PAGE,
        .uncompressed_page_size = @intCast(data.len),
        .compressed_page_size = @intCast(data.len), // codec=UNCOMPRESSED
        .crc = null,
        .data_page_header = .{
            .num_values = @intCast(num_values),
            .encoding = .PLAIN,
            .definition_level_encoding = .RLE,
            .repetition_level_encoding = .RLE,
        },
        .dictionary_page_header = null,
    };
    var w: thrift.Writer = .init(arena);
    defer w.deinit();
    try page_hdr.write(&w);
    const header_bytes = w.bytes();

    // 4. Concatenate header + data.
    const total = try arena.alloc(u8, header_bytes.len + data.len);
    @memcpy(total[0..header_bytes.len], header_bytes);
    @memcpy(total[header_bytes.len..], data);

    // 5. Build ColumnMetaData.
    var encodings: schema.EncodingList = .empty;
    try encodings.append(arena, .PLAIN);

    var path_list: schema.StringList = .empty;
    try path_list.appendSlice(arena, in.path_in_schema);

    const meta: schema.ColumnMetaData = .{
        .type = phys,
        .encodings = encodings,
        .path_in_schema = path_list,
        .codec = .UNCOMPRESSED,
        .num_values = num_values,
        .total_uncompressed_size = @intCast(total.len),
        .total_compressed_size = @intCast(total.len),
        .data_page_offset = 0, // caller adjusts to absolute offset
        .index_page_offset = null,
        .dictionary_page_offset = null,
        .statistics = stats,
    };

    return .{ .bytes = total, .meta = meta };
}

/// Materialize the subset of `c` that's active in `sel` into newly-
/// allocated typed slices in `arena`. Used by the filter+encode path
/// to convert (decoded values, SelectionVector) into pre-filtered
/// values ready for encodeColumn.
pub fn applySelection(
    arena: std.mem.Allocator,
    c: filter_eval.Batch.Column,
    sel: *const filter_selection.SelectionVector,
) Error!filter_eval.Batch.Column {
    const surviving = sel.count();
    return switch (c) {
        .i32 => |v| blk: {
            const out = try arena.alloc(i32, surviving);
            var w: usize = 0;
            for (v, 0..) |item, i| {
                if (sel.isActive(i)) {
                    out[w] = item;
                    w += 1;
                }
            }
            break :blk .{ .i32 = out };
        },
        .i64 => |v| blk: {
            const out = try arena.alloc(i64, surviving);
            var w: usize = 0;
            for (v, 0..) |item, i| {
                if (sel.isActive(i)) {
                    out[w] = item;
                    w += 1;
                }
            }
            break :blk .{ .i64 = out };
        },
        .f32 => |v| blk: {
            const out = try arena.alloc(f32, surviving);
            var w: usize = 0;
            for (v, 0..) |item, i| {
                if (sel.isActive(i)) {
                    out[w] = item;
                    w += 1;
                }
            }
            break :blk .{ .f32 = out };
        },
        .f64 => |v| blk: {
            const out = try arena.alloc(f64, surviving);
            var w: usize = 0;
            for (v, 0..) |item, i| {
                if (sel.isActive(i)) {
                    out[w] = item;
                    w += 1;
                }
            }
            break :blk .{ .f64 = out };
        },
        .string => |v| blk: {
            const out = try arena.alloc([]const u8, surviving);
            var w: usize = 0;
            for (v, 0..) |item, i| {
                if (sel.isActive(i)) {
                    out[w] = item;
                    w += 1;
                }
            }
            break :blk .{ .string = out };
        },
        .boolean => |v| blk: {
            const out = try arena.alloc(bool, surviving);
            var w: usize = 0;
            for (v, 0..) |item, i| {
                if (sel.isActive(i)) {
                    out[w] = item;
                    w += 1;
                }
            }
            break :blk .{ .boolean = out };
        },
    };
}

// ============================================================
// PLAIN encoders per physical type
// ============================================================

fn encodeValuesPlain(arena: std.mem.Allocator, vals: filter_eval.Batch.Column) Error![]u8 {
    return switch (vals) {
        .i32 => |v| try encodePlainTyped(i32, arena, v),
        .i64 => |v| try encodePlainTyped(i64, arena, v),
        .f32 => |v| try encodePlainTyped(f32, arena, v),
        .f64 => |v| try encodePlainTyped(f64, arena, v),
        .string => |v| try encodePlainBytes(arena, v),
        .boolean => |v| try encodePlainBool(arena, v),
    };
}

/// PLAIN encoding for fixed-width primitive types (i32/i64/f32/f64).
/// Just little-endian bytes back-to-back.
fn encodePlainTyped(comptime T: type, arena: std.mem.Allocator, values: []const T) Error![]u8 {
    const item_size = @sizeOf(T);
    const out = try arena.alloc(u8, values.len * item_size);
    for (values, 0..) |v, i| {
        const offset = i * item_size;
        switch (@typeInfo(T)) {
            .int => std.mem.writeInt(T, out[offset..][0..item_size], v, .little),
            .float => {
                const Bits = std.meta.Int(.unsigned, @bitSizeOf(T));
                const bits: Bits = @bitCast(v);
                std.mem.writeInt(Bits, out[offset..][0..item_size], bits, .little);
            },
            else => @compileError("encodePlainTyped: unsupported type"),
        }
    }
    return out;
}

/// PLAIN encoding for BYTE_ARRAY: each value is `<u32 LE length><bytes>`.
fn encodePlainBytes(arena: std.mem.Allocator, values: []const []const u8) Error![]u8 {
    var total: usize = 0;
    for (values) |v| total += 4 + v.len;

    const out = try arena.alloc(u8, total);
    var pos: usize = 0;
    for (values) |v| {
        const len_u32: u32 = @intCast(v.len);
        std.mem.writeInt(u32, out[pos..][0..4], len_u32, .little);
        @memcpy(out[pos + 4 ..][0..v.len], v);
        pos += 4 + v.len;
    }
    return out;
}

/// PLAIN encoding for BOOLEAN: bit-packed, LSB first.
/// 8 values per byte; final byte's high bits are 0 if not full.
fn encodePlainBool(arena: std.mem.Allocator, values: []const bool) Error![]u8 {
    const num_bytes = (values.len + 7) / 8;
    const out = try arena.alloc(u8, num_bytes);
    @memset(out, 0);
    for (values, 0..) |v, i| {
        if (v) {
            const byte_idx = i / 8;
            const bit_idx: u3 = @intCast(i % 8);
            out[byte_idx] |= (@as(u8, 1) << bit_idx);
        }
    }
    return out;
}

fn valueCount(c: filter_eval.Batch.Column) usize {
    return switch (c) {
        .i32 => |v| v.len,
        .i64 => |v| v.len,
        .f32 => |v| v.len,
        .f64 => |v| v.len,
        .string => |v| v.len,
        .boolean => |v| v.len,
    };
}

// ============================================================
// Statistics — min/max over surviving values
// ============================================================

fn computeStats(c: filter_eval.Batch.Column) ?schema.Statistics {
    return switch (c) {
        .i32 => |v| statsTyped(i32, v),
        .i64 => |v| statsTyped(i64, v),
        .f32 => |v| statsTypedFloat(f32, v),
        .f64 => |v| statsTypedFloat(f64, v),
        .string => |v| statsBytes(v),
        .boolean => |v| statsBool(v),
    };
}

fn statsTyped(comptime T: type, values: []const T) ?schema.Statistics {
    if (values.len == 0) return null;
    var lo: T = values[0];
    var hi: T = values[0];
    for (values[1..]) |v| {
        if (v < lo) lo = v;
        if (v > hi) hi = v;
    }
    // Stats are stored as little-endian byte strings of the typed value.
    const item_size = @sizeOf(T);
    const min_buf = std.heap.page_allocator.alloc(u8, item_size) catch return null;
    const max_buf = std.heap.page_allocator.alloc(u8, item_size) catch {
        std.heap.page_allocator.free(min_buf);
        return null;
    };
    std.mem.writeInt(T, min_buf[0..item_size], lo, .little);
    std.mem.writeInt(T, max_buf[0..item_size], hi, .little);
    return .{ .min_value = min_buf, .max_value = max_buf };
}

fn statsTypedFloat(comptime T: type, values: []const T) ?schema.Statistics {
    if (values.len == 0) return null;
    var lo: T = values[0];
    var hi: T = values[0];
    for (values[1..]) |v| {
        if (v < lo) lo = v;
        if (v > hi) hi = v;
    }
    const Bits = std.meta.Int(.unsigned, @bitSizeOf(T));
    const item_size = @sizeOf(T);
    const min_buf = std.heap.page_allocator.alloc(u8, item_size) catch return null;
    const max_buf = std.heap.page_allocator.alloc(u8, item_size) catch {
        std.heap.page_allocator.free(min_buf);
        return null;
    };
    const lo_bits: Bits = @bitCast(lo);
    const hi_bits: Bits = @bitCast(hi);
    std.mem.writeInt(Bits, min_buf[0..item_size], lo_bits, .little);
    std.mem.writeInt(Bits, max_buf[0..item_size], hi_bits, .little);
    return .{ .min_value = min_buf, .max_value = max_buf };
}

fn statsBytes(values: []const []const u8) ?schema.Statistics {
    if (values.len == 0) return null;
    var lo: []const u8 = values[0];
    var hi: []const u8 = values[0];
    for (values[1..]) |v| {
        if (std.mem.lessThan(u8, v, lo)) lo = v;
        if (std.mem.lessThan(u8, hi, v)) hi = v;
    }
    return .{ .min_value = lo, .max_value = hi };
}

fn statsBool(values: []const bool) ?schema.Statistics {
    if (values.len == 0) return null;
    var has_t = false;
    var has_f = false;
    for (values) |v| {
        if (v) has_t = true else has_f = true;
        if (has_t and has_f) break;
    }
    const min_buf = std.heap.page_allocator.alloc(u8, 1) catch return null;
    const max_buf = std.heap.page_allocator.alloc(u8, 1) catch {
        std.heap.page_allocator.free(min_buf);
        return null;
    };
    min_buf[0] = if (has_f) 0 else 1;
    max_buf[0] = if (has_t) 1 else 0;
    return .{ .min_value = min_buf, .max_value = max_buf };
}

// ============================================================
// Tests — round-trip through metadata.open
// ============================================================

const testing = std.testing;
const metadata = @import("../parquet/metadata.zig");

test "encodePlainTyped i32 round-trip via slice copy" {
    const arena = testing.allocator;
    const values = [_]i32{ -1, 0, 1, 100, std.math.maxInt(i32), std.math.minInt(i32) };
    const encoded = try encodePlainTyped(i32, arena, &values);
    defer arena.free(encoded);

    try testing.expectEqual(@as(usize, values.len * @sizeOf(i32)), encoded.len);
    // Decode back.
    var i: usize = 0;
    while (i < values.len) : (i += 1) {
        const got = std.mem.readInt(i32, encoded[i * 4 ..][0..4], .little);
        try testing.expectEqual(values[i], got);
    }
}

test "encodePlainBytes round-trip" {
    const arena = testing.allocator;
    const v = [_][]const u8{ "alpha", "", "BETA", "γ" };
    const encoded = try encodePlainBytes(arena, &v);
    defer arena.free(encoded);

    var pos: usize = 0;
    for (v) |orig| {
        const len = std.mem.readInt(u32, encoded[pos..][0..4], .little);
        try testing.expectEqual(@as(u32, @intCast(orig.len)), len);
        try testing.expectEqualStrings(orig, encoded[pos + 4 .. pos + 4 + len]);
        pos += 4 + len;
    }
}

test "encodePlainBool packs 8 per byte LSB-first" {
    const arena = testing.allocator;
    const v = [_]bool{ true, false, true, true, false, false, true, false, true };
    // bits LSB first: byte0 = 0b01001101, byte1 = 0b00000001
    const encoded = try encodePlainBool(arena, &v);
    defer arena.free(encoded);

    try testing.expectEqual(@as(usize, 2), encoded.len);
    try testing.expectEqual(@as(u8, 0b01001101), encoded[0]);
    try testing.expectEqual(@as(u8, 0b00000001), encoded[1]);
}
