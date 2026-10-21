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

/// How far statistics, which are untrusted input, are taken at their word. Claims no conforming writer can make (min
/// above max in the column's order, a null count below zero or above the chunk's values, a flat chunk whose value
/// count is not its row count) make them unusable whatever is asked. Claims that only contradict each other or go
/// uncorroborated (an all-null count beside bounds, a zero count without the bounds a writer would have written) are
/// used only when the user vouches for the file: `--trust-stats` trusts plausible statistics, never impossible ones.
pub const Trust = enum { corroborated, plausible };

/// What a chunk's statistics can be used for; a null field is unknown and its question is answered by decoding.
pub const ChunkStats = struct {
    /// In the column's type-defined order (`writtenBounds`), min never above max.
    bounds: ?Bounds = null,
    null_count: ?i64 = null,
};

/// The one gate every reader of chunk statistics goes through: pruning, the full-match proof, stats-as-answer.
pub fn chunkStats(
    rg: *const schema.RowGroup,
    col_idx: usize,
    file_meta: ?*const schema.FileMetaData,
    trust: Trust,
) ChunkStats {
    if (col_idx >= rg.columns.items.len) return .{};
    const cm = rg.columns.items[col_idx].meta_data orelse return .{};
    const stats = cm.statistics orelse return .{};
    const elem = if (file_meta) |fm| fm.getColumnSchema(cm.path_in_schema.items) else null;
    if (!valuesAddUp(rg, cm, file_meta)) return .{};
    const bounds = writtenBounds(rg, col_idx, file_meta);
    if (bounds) |b| if (boundsInverted(cm.type, elem, b.min, b.max)) return .{};
    if (stats.null_count) |nc| if (nc < 0 or nc > cm.num_values) return .{};
    const out: ChunkStats = .{ .bounds = bounds, .null_count = stats.null_count };
    if (trust == .plausible) return out;

    const nc = stats.null_count orelse return out;
    // Any written pair counts, read or not: the writer saw a value.
    const has_bounds = (stats.min_value != null and stats.max_value != null) or (stats.min != null and stats.max != null);
    if (cm.num_values > 0 and nc == cm.num_values and has_bounds) return .{};
    if (cm.num_values > 0 and nc == 0 and !has_bounds and boundsExpected(rg, col_idx, cm, file_meta)) {
        return .{ .bounds = bounds };
    }
    return out;
}

/// Every row has at least one entry in each leaf, and a leaf that does not repeat has exactly one.
fn valuesAddUp(rg: *const schema.RowGroup, cm: schema.ColumnMetaData, file_meta: ?*const schema.FileMetaData) bool {
    if (cm.num_values < rg.num_rows) return false;
    const fm = file_meta orelse return true;
    // getColumnLevels reports {0, 0} for a path it cannot find; resolve the element first so that can't pass as flat.
    _ = fm.getColumnSchema(cm.path_in_schema.items) orelse return true;
    return fm.getColumnLevels(cm.path_in_schema.items).max_rep > 0 or cm.num_values == rg.num_rows;
}

/// Whether a writer that saw a non-null value would have recorded bounds for it. Not for an order zpq cannot read
/// (the writer had none to keep them in), INT96 (no defined order), or a float chunk counted as NaN throughout (NaN is
/// left out of bounds). Writers also drop bounds over a size limit; such a chunk decodes for its null checks.
fn boundsExpected(
    rg: *const schema.RowGroup,
    col_idx: usize,
    cm: schema.ColumnMetaData,
    file_meta: ?*const schema.FileMetaData,
) bool {
    if (cm.type == .INT96 or !boundsReadable(rg, col_idx, file_meta)) return false;
    if (cm.type == .FLOAT or cm.type == .DOUBLE) if (cm.statistics.?.nan_count) |n| if (n == cm.num_values) return false;
    return true;
}

/// Whether `min` sorts above `max` in the column's type-defined order, which no writer's bounds can. Bounds this
/// cannot read (a width the type does not have, a DECIMAL too wide, NaN) are not judged; every reader declines them.
/// Without a schema element INT32/INT64 are taken as signed, so an unsigned chunk spanning 2^31 loses its bounds
/// rather than being misread.
pub fn boundsInverted(physical: schema.Type, elem: ?schema.SchemaElement, min: []const u8, max: []const u8) bool {
    if (elem) |e| {
        if (decimal_mod.kindFromSchema(&e)) |k| {
            const lo = decimal_mod.statToI128(min, k) orelse return false;
            const hi = decimal_mod.statToI128(max, k) orelse return false;
            return lo > hi;
        }
        if (schema.isFloat16(e)) return fixedInverted(f16, min, max);
        if (schema.isUnsignedInt(e)) return switch (physical) {
            .INT32 => fixedInverted(u32, min, max),
            .INT64 => fixedInverted(u64, min, max),
            else => false,
        };
    }
    return switch (physical) {
        .BOOLEAN => min.len == 1 and max.len == 1 and min[0] != 0 and max[0] == 0,
        .INT32 => fixedInverted(i32, min, max),
        .INT64 => fixedInverted(i64, min, max),
        .FLOAT => fixedInverted(f32, min, max),
        .DOUBLE => fixedInverted(f64, min, max),
        .BYTE_ARRAY, .FIXED_LEN_BYTE_ARRAY => std.mem.order(u8, min, max) == .gt,
        .INT96 => false,
    };
}

fn fixedInverted(comptime T: type, min: []const u8, max: []const u8) bool {
    const n = @sizeOf(T);
    if (min.len != n or max.len != n) return false;
    const U = @Int(.unsigned, n * 8);
    const lo: T = @bitCast(std.mem.readInt(U, min[0..n], .little));
    const hi: T = @bitCast(std.mem.readInt(U, max[0..n], .little));
    return lo > hi; // false for NaN
}

/// A column chunk's bounds in the column's type-defined order, or null when the file gives none zpq can read in that
/// order. `min_value`/`max_value` are written in it. The deprecated `min`/`max` were written with a signed comparison
/// whatever the type, so they stand in only where that is the type-defined order too; for byte arrays (unsigned
/// bytewise), DECIMAL over FLBA or BYTE_ARRAY (numeric, but compared as signed bytes) and unsigned integers they
/// misorder values, so are never read. DECIMAL over INT32/INT64 compares as its signed unscaled integer, which is the
/// numeric order, so it reads them like any signed integer (parquet-mr: SIGNED). Without file metadata there is no
/// schema to check, and the deprecated pair is not read either.
/// Read as written: callers go through `chunkStats`.
fn writtenBounds(rg: *const schema.RowGroup, col_idx: usize, file_meta: ?*const schema.FileMetaData) ?Bounds {
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
/// IEEE_754_TOTAL_ORDER on a FLOAT/DOUBLE column is read too: its bounds are the least and greatest non-NaN values,
/// as type-defined float bounds are, except that a chunk or page of nothing but NaN has NaN bounds, which every
/// reader of float bounds already refuses to prune on; -0 below +0 changes no comparison zpq makes, which takes them
/// as equal.
pub fn boundsReadable(rg: *const schema.RowGroup, col_idx: usize, file_meta: ?*const schema.FileMetaData) bool {
    if (col_idx >= rg.columns.items.len) return false;
    const fm = file_meta orelse return true;
    const cm = rg.columns.items[col_idx].meta_data orelse return false;
    if (fm.column_orders) |co| {
        if (col_idx >= co.items.len) return false;
        const float = cm.type == .FLOAT or cm.type == .DOUBLE;
        const total = float and co.items[col_idx] == schema.COLUMN_ORDER_IEEE_754_TOTAL;
        if (co.items[col_idx] != schema.COLUMN_ORDER_TYPE_DEFINED and !total) return false;
    }
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

/// Whether a chunk's ColumnIndex is fit to prune pages with: the page-index side of `chunkStats`, under the same two
/// tiers. Page pruning and the always-match fill act on these claims without decoding, so an index the footer, the
/// OffsetIndex or its own entries contradict is dropped whole and the chunk reads as if it had none: a writer that got
/// one claim wrong cannot be trusted on its neighbours.
///
/// Impossible, refused under any `trust`:
///   - per-page lists whose lengths disagree with the OffsetIndex page count;
///   - a negative null count, which no page can have;
///   - a page flagged all-null on a leaf with no optional or repeated ancestor (max definition level 0), which
///     cannot hold a null at all;
///   - a value page whose min sorts above its max (`boundsInverted`);
///   - for a non-repeated leaf, whose entries are its rows, a null count above the page's rows, a page flagged
///     all-null whose null count is not its row count, or null pages covering more rows than the chunk's own
///     statistics say are null.
/// Contradictory, refused unless `.plausible`: a null page with bounds (a writer leaves them empty), and on a
/// non-repeated leaf a page of nothing but nulls not flagged as a null page.
///
/// Writers that collected no page statistics are known to flag every page null and record -1 null counts, while
/// the pages hold ordinary values; trusting them drops every row.
pub fn pageIndexIsPlausible(
    ci: *const schema.ColumnIndex,
    oi: *const schema.OffsetIndex,
    cm: schema.ColumnMetaData,
    elem: ?schema.SchemaElement,
    levels: schema.Levels,
    rg_rows: i64,
    trust: Trust,
) bool {
    const locs = oi.page_locations.items;
    const n = locs.len;
    if (ci.null_pages.items.len != n or ci.min_values.items.len != n or ci.max_values.items.len != n) return false;
    if (ci.null_counts) |nc| {
        if (nc.items.len != n) return false;
        for (nc.items) |c| if (c < 0) return false;
    }

    var null_page_rows: i64 = 0;
    for (ci.null_pages.items, ci.min_values.items, ci.max_values.items, 0..) |is_null, min, max, i| {
        if (!is_null and boundsInverted(cm.type, elem, min, max)) return false;
        if (is_null and levels.max_def == 0) return false;
        if (is_null and trust == .corroborated and (min.len > 0 or max.len > 0)) return false;
        // A repeated leaf's counts are entries, not rows; only the flags and bounds are checked there.
        if (levels.max_rep > 0) continue;
        const end = if (i + 1 < n) locs[i + 1].first_row_index else rg_rows;
        const rows = end - locs[i].first_row_index;
        if (ci.null_counts) |nc| {
            if (nc.items[i] > rows) return false;
            if (is_null and nc.items[i] != rows) return false;
            if (!is_null and trust == .corroborated and rows > 0 and nc.items[i] == rows) return false;
        }
        if (is_null) null_page_rows += rows;
    }
    if (levels.max_rep == 0) {
        if (cm.statistics) |s| if (s.null_count) |chunk_nulls| {
            if (null_page_rows > chunk_nulls) return false;
        };
    }
    return true;
}

// ============================================================
// Tests
// ============================================================

const testing = std.testing;
const test_fixtures = @import("test_fixtures.zig");
const leafForTest = test_fixtures.statsLeaf;
const metadata = @import("metadata.zig");
const thrift = @import("../thrift.zig");
const ColumnForTest = test_fixtures.StatsColumn;

test "writtenBounds reads the deprecated pair only where signed is the type-defined order" {
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
        try testing.expectEqual(c.usable, writtenBounds(&fx.rg, 0, &fx.fm) != null);
        // No schema, no way to tell the order: never.
        try testing.expect(writtenBounds(&fx.rg, 0, null) == null);
    }

    // min_value/max_value are type-ordered for every type, and the two pairs are never mixed.
    const modern = try ColumnForTest.init(a, str, .{ .min_value = "a", .max_value = "z", .min = "x", .max = "y" });
    try testing.expectEqualStrings("a", writtenBounds(&modern.rg, 0, &modern.fm).?.min);
    const half = try ColumnForTest.init(a, leafForTest(.INT64), .{ .min_value = "a", .min = "lo", .max = "hi" });
    try testing.expectEqualStrings("lo", writtenBounds(&half.rg, 0, &half.fm).?.min);
}

test "pageIndexIsPlausible: corpus page indexes are kept except the ones their footers contradict" {
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
            const elem = meta.getColumnSchema(cm.path_in_schema.items);
            if (!pageIndexIsPlausible(&col_index, &offset_index, cm, elem, levels, rg.num_rows, .corroborated)) rejected += 1;
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
    try testing.expect(pageIndexIsPlausible(&good, &oi, cm, null, optional, 16, .corroborated));
    // Without null counts the flag alone is taken on an optional leaf.
    const no_counts = try Fixture.index(a, &.{ false, true }, null);
    try testing.expect(pageIndexIsPlausible(&no_counts, &oi, cm, null, optional, 16, .corroborated));
    // A null page on a leaf that cannot be null.
    try testing.expect(!pageIndexIsPlausible(&no_counts, &oi, cm, null, required, 16, .corroborated));
    // A required leaf with no null page is fine.
    const all_values = try Fixture.index(a, &.{ false, false }, &.{ 0, 0 });
    try testing.expect(pageIndexIsPlausible(&all_values, &oi, cm, null, required, 16, .corroborated));
    // Negative null counts.
    const negative = try Fixture.index(a, &.{ false, false }, &.{ -1, 0 });
    try testing.expect(!pageIndexIsPlausible(&negative, &oi, cm, null, optional, 16, .corroborated));
    // A null page whose null count is not its row count.
    const short = try Fixture.index(a, &.{ false, true }, &.{ 0, 5 });
    try testing.expect(!pageIndexIsPlausible(&short, &oi, cm, null, optional, 16, .corroborated));
    // ...is only checked where entries are rows.
    try testing.expect(pageIndexIsPlausible(&short, &oi, cm, null, repeated, 16, .corroborated));
    // Null pages covering more rows than the chunk statistics count as null.
    var with_stats = cm;
    with_stats.statistics = .{ .null_count = 3 };
    try testing.expect(!pageIndexIsPlausible(&no_counts, &oi, with_stats, null, optional, 16, .corroborated));
    with_stats.statistics = .{ .null_count = 6 };
    try testing.expect(pageIndexIsPlausible(&no_counts, &oi, with_stats, null, optional, 16, .corroborated));
    // Per-page lists that disagree with the OffsetIndex page count.
    const one_page = try Fixture.index(a, &.{false}, null);
    try testing.expect(!pageIndexIsPlausible(&one_page, &oi, cm, null, optional, 16, .corroborated));
}
