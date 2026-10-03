//! What a column's Parquet statistics say, and in which order they say it.
//!
//! Chunk statistics and the page index carry bounds as Parquet bytes, in an order fixed by the column's physical
//! type, its annotation and the file's declared column order. Deciding which bounds are readable, in which order, is
//! Parquet semantics rather than filter or aggregate policy: filter pruning (`filter/prune.zig`) uses the bounds to
//! rule row groups and pages out, and aggregate folding (`expr/agg.zig`) to answer min/max/sum without decoding.
//! Neither use asks more of the bounds than that they order every value; the stronger full-match proof (no nulls,
//! a flat column, exact endpoints) stays with the filter pruner that makes it.

const std = @import("std");
const schema = @import("../schema.zig");
const decimal_mod = @import("decimal.zig");

pub const Bounds = struct { min: []const u8, max: []const u8 };

/// A column chunk's bounds in the column's type-defined order, or null when the file gives none zpq can read in that
/// order. `min_value`/`max_value` are written in it. The deprecated `min`/`max` were written with a signed comparison
/// whatever the type, so they stand in only where that is the type-defined order too; for byte arrays (unsigned
/// bytewise), DECIMAL over FLBA or BYTE_ARRAY (numeric, but compared as signed bytes) and unsigned integers they
/// misorder values, so are never read. DECIMAL over INT32/INT64 compares as its signed unscaled integer, which is the
/// numeric order, so it reads them like any signed integer (parquet-mr: SIGNED). Without file metadata there is no
/// schema to check, and the deprecated pair is not read either.
pub fn chunkBounds(rg: *const schema.RowGroup, col_idx: usize, file_meta: ?*const schema.FileMetaData) ?Bounds {
    if (!boundsReadable(rg, col_idx, file_meta)) return null;
    const cm = rg.columns.items[col_idx].meta_data orelse return null;
    const stats = cm.statistics orelse return null;
    if (stats.min_value) |min| if (stats.max_value) |max| return .{ .min = min, .max = max };
    const min = stats.min orelse return null;
    const max = stats.max orelse return null;
    const elem = (file_meta orelse return null).getColumnSchema(cm.path_in_schema.items) orelse return null;
    if (elem.type == null or elem.type.? != cm.type) return null;
    if (!deprecatedOrderIsTypeOrder(elem)) return null;
    return .{ .min = min, .max = max };
}

/// Whether a column's bounds, chunk statistics or page index alike, are in an order zpq reads. The file may declare
/// an order other than TYPE_DEFINED_ORDER, one zpq does not implement, and the spec says to ignore min and max then;
/// an absent column_orders list is read as type-defined, as parquet-mr does. An annotation zpq does not recognise
/// (read as UNKNOWN) or INTERVAL has no order at all. Without file metadata nothing can be checked, so nothing is
/// refused: those callers already accept what the chunk's own min_value/max_value say.
pub fn boundsReadable(rg: *const schema.RowGroup, col_idx: usize, file_meta: ?*const schema.FileMetaData) bool {
    if (col_idx >= rg.columns.items.len) return false;
    const fm = file_meta orelse return true;
    if (fm.column_orders) |co| {
        if (col_idx >= co.items.len or co.items[col_idx] != schema.COLUMN_ORDER_TYPE_DEFINED) return false;
    }
    const cm = rg.columns.items[col_idx].meta_data orelse return false;
    const elem = fm.getColumnSchema(cm.path_in_schema.items) orelse return true;
    if (elem.logical_type) |lt| if (lt == .UNKNOWN) return false;
    if (elem.converted_type) |ct| if (ct == .INTERVAL) return false;
    return true;
}

/// The types whose type-defined order is the signed order the deprecated min/max were written in.
fn deprecatedOrderIsTypeOrder(elem: schema.SchemaElement) bool {
    return switch (elem.type orelse return false) {
        .INT32, .INT64 => signedIntOrder(elem) or intBackedDecimal(elem),
        .FLOAT, .DOUBLE => elem.logical_type == null and elem.converted_type == null,
        .BOOLEAN => true,
        .BYTE_ARRAY, .FIXED_LEN_BYTE_ARRAY, .INT96 => false,
    };
}

fn intBackedDecimal(elem: schema.SchemaElement) bool {
    const kind = decimal_mod.kindFromSchema(&elem) orelse return false;
    return kind.physical == .INT32 or kind.physical == .INT64;
}

/// INT32/INT64 whose type-defined order is signed: plain, signed INTEGER, DATE, TIME, TIMESTAMP.
pub fn signedIntOrder(elem: schema.SchemaElement) bool {
    const t = elem.type orelse return false;
    if (t != .INT32 and t != .INT64) return false;
    if (schema.isUnsignedInt(elem)) return false;
    if (elem.logical_type) |lt| switch (lt) {
        .INTEGER => |it| if (!it.isSigned) return false,
        .DATE, .TIME, .TIMESTAMP => {},
        else => return false,
    };
    if (elem.converted_type) |ct| switch (ct) {
        .INT_8, .INT_16, .INT_32, .INT_64 => {},
        .DATE, .TIME_MILLIS, .TIME_MICROS, .TIMESTAMP_MILLIS, .TIMESTAMP_MICROS => {},
        else => return false,
    };
    return true;
}

/// BYTE_ARRAY whose type-defined order is unsigned bytewise: plain, STRING, ENUM, JSON.
pub fn bytewiseOrder(elem: schema.SchemaElement) bool {
    const t = elem.type orelse return false;
    if (t != .BYTE_ARRAY) return false;
    if (elem.logical_type) |lt| switch (lt) {
        .STRING, .ENUM, .JSON => {},
        else => return false,
    };
    if (elem.converted_type) |ct| switch (ct) {
        .UTF8, .ENUM, .JSON => {},
        else => return false,
    };
    return true;
}

// ============================================================
// Tests
// ============================================================

const testing = std.testing;

/// One flat column `c` with the given stats, 100 rows, in a file whose schema declares `elem`.
pub const ColumnForTest = struct {
    rg: schema.RowGroup,
    fm: schema.FileMetaData,

    pub fn init(a: std.mem.Allocator, elem: schema.SchemaElement, stats: schema.Statistics) !ColumnForTest {
        var path: schema.StringList = .empty;
        try path.append(a, "c");
        var rg: schema.RowGroup = .{ .columns = .empty, .total_byte_size = 0, .num_rows = 100 };
        try rg.columns.append(a, .{ .file_path = null, .file_offset = 0, .meta_data = .{
            .type = elem.type.?,
            .encodings = .empty,
            .path_in_schema = path,
            .codec = .UNCOMPRESSED,
            .num_values = 100,
            .total_uncompressed_size = 0,
            .total_compressed_size = 0,
            .data_page_offset = 0,
            .index_page_offset = null,
            .dictionary_page_offset = null,
            .statistics = stats,
        } });
        var items: std.ArrayListUnmanaged(schema.SchemaElement) = .empty;
        try items.append(a, .{
            .type = null,
            .type_length = null,
            .repetition_type = null,
            .name = "schema",
            .num_children = 1,
            .scale = null,
            .precision = null,
            .field_id = null,
        });
        var leaf = elem;
        leaf.name = "c";
        leaf.num_children = 0;
        try items.append(a, leaf);
        return .{
            .rg = rg,
            .fm = .{ .version = 1, .schema = items, .num_rows = 100, .created_by = null, .row_groups = .empty },
        };
    }
};

/// An OPTIONAL leaf `c` of physical type `t` with no annotation.
pub fn leafForTest(t: schema.Type) schema.SchemaElement {
    return .{
        .type = t,
        .type_length = null,
        .repetition_type = .OPTIONAL,
        .name = "c",
        .num_children = 0,
        .scale = null,
        .precision = null,
        .field_id = null,
    };
}

test "chunkBounds reads the deprecated pair only where signed is the type-defined order" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const legacy: schema.Statistics = .{ .min = "lo", .max = "hi" };

    var date = leafForTest(.INT32);
    date.logical_type = .{ .DATE = .{} };
    var dec = leafForTest(.INT32);
    dec.logical_type = .{ .DECIMAL = .{ .scale = 2, .precision = 9 } };
    var flba_dec = leafForTest(.FIXED_LEN_BYTE_ARRAY);
    flba_dec.type_length = 4;
    flba_dec.logical_type = .{ .DECIMAL = .{ .scale = 2, .precision = 9 } };
    var bytes_dec = leafForTest(.BYTE_ARRAY);
    bytes_dec.converted_type = .DECIMAL;
    bytes_dec.scale = 2;
    bytes_dec.precision = 9;
    var u64_conv = leafForTest(.INT64);
    u64_conv.converted_type = .UINT_64;
    var str = leafForTest(.BYTE_ARRAY);
    str.logical_type = .{ .STRING = .{} };
    const cases = [_]struct { elem: schema.SchemaElement, usable: bool }{
        .{ .elem = leafForTest(.INT64), .usable = true },
        .{ .elem = date, .usable = true },
        .{ .elem = leafForTest(.DOUBLE), .usable = true },
        .{ .elem = leafForTest(.BOOLEAN), .usable = true },
        .{ .elem = dec, .usable = true },
        .{ .elem = flba_dec, .usable = false },
        .{ .elem = bytes_dec, .usable = false },
        .{ .elem = u64_conv, .usable = false },
        .{ .elem = str, .usable = false },
        .{ .elem = leafForTest(.BYTE_ARRAY), .usable = false },
        .{ .elem = leafForTest(.FIXED_LEN_BYTE_ARRAY), .usable = false },
        .{ .elem = leafForTest(.INT96), .usable = false },
    };
    for (cases) |c| {
        const fx = try ColumnForTest.init(a, c.elem, legacy);
        try testing.expectEqual(c.usable, chunkBounds(&fx.rg, 0, &fx.fm) != null);
        // No schema, no way to tell the order: never.
        try testing.expect(chunkBounds(&fx.rg, 0, null) == null);
    }

    // min_value/max_value are type-ordered for every type, and the two pairs are never mixed.
    const modern = try ColumnForTest.init(a, str, .{ .min_value = "a", .max_value = "z", .min = "x", .max = "y" });
    try testing.expectEqualStrings("a", chunkBounds(&modern.rg, 0, &modern.fm).?.min);
    const half = try ColumnForTest.init(a, leafForTest(.INT64), .{ .min_value = "a", .min = "lo", .max = "hi" });
    try testing.expectEqualStrings("lo", chunkBounds(&half.rg, 0, &half.fm).?.min);
}
