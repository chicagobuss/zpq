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
    const has_bounds = (stats.min_value != null and stats.max_value != null) or
        (stats.min != null and stats.max != null);
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
    const float = cm.type == .FLOAT or cm.type == .DOUBLE;
    if (float) if (cm.statistics.?.nan_count) |n| if (n == cm.num_values) return false;
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
///   - page locations out of page order: each page lies inside the chunk's bytes, after the one before it, and the
///     row starts begin at 0 and increase strictly below the row group's rows. Skipping a page seeks to the next
///     location and slices the row selection by these starts, so a misplaced one decodes the wrong page's rows;
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
    // `ColumnIndex.read` already leaves a NaN count list of another length out.
    if (ci.nan_counts) |nc| if (nc.items.len != n) return false;
    if (!pagesInOrder(locs, cm, rg_rows)) return false;

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

fn pagesInOrder(locs: []const schema.PageLocation, cm: schema.ColumnMetaData, rg_rows: i64) bool {
    const start = if (cm.dictionary_page_offset) |d| @min(d, cm.data_page_offset) else cm.data_page_offset;
    if (start < 0 or cm.total_compressed_size < 0) return false;
    const end = std.math.add(i64, start, cm.total_compressed_size) catch return false;
    var bytes_from = start;
    for (locs, 0..) |loc, i| {
        if (loc.offset < bytes_from or loc.compressed_page_size <= 0) return false;
        bytes_from = std.math.add(i64, loc.offset, loc.compressed_page_size) catch return false;
        if (bytes_from > end) return false;
        if (loc.first_row_index >= rg_rows) return false;
        if (i == 0) {
            if (loc.first_row_index != 0) return false;
        } else if (loc.first_row_index <= locs[i - 1].first_row_index) return false;
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
            const trust: Trust = .corroborated;
            const plausible = pageIndexIsPlausible(&col_index, &offset_index, cm, elem, levels, rg.num_rows, trust);
            if (!plausible) rejected += 1;
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
        .total_compressed_size = 20,
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

fn le(comptime T: type, v: T) [@sizeOf(T)]u8 {
    var b: [@sizeOf(T)]u8 = undefined;
    std.mem.writeInt(@Int(.unsigned, @bitSizeOf(T)), &b, @bitCast(v), .little);
    return b;
}

test "chunkStats: impossible statistics are refused under any trust" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const lo = le(i64, 150);
    const hi = le(i64, 120);
    const f_lo = le(f64, 1.5);
    const f_hi = le(f64, -2);
    const u_max = le(u32, 0xffff_ffff);
    const u_one = le(u32, 1);
    const f_nan = le(f64, std.math.nan(f64));
    const f_one = le(f64, 1);
    var u32_elem = leafForTest(.INT32);
    u32_elem.converted_type = .UINT_32;
    var dec = leafForTest(.FIXED_LEN_BYTE_ARRAY);
    dec.type_length = 2;
    dec.logical_type = .{ .DECIMAL = .{ .scale = 0, .precision = 4 } };
    var str = leafForTest(.BYTE_ARRAY);
    str.logical_type = .{ .STRING = .{} };
    const Case = struct { elem: schema.SchemaElement, stats: schema.Statistics };
    const impossible = [_]Case{
        // min above max, in each order: signed, float, bytewise, unsigned, DECIMAL, boolean, the deprecated pair.
        .{ .elem = leafForTest(.INT64), .stats = .{ .min_value = &lo, .max_value = &hi, .null_count = 0 } },
        .{ .elem = leafForTest(.DOUBLE), .stats = .{ .min_value = &f_lo, .max_value = &f_hi, .null_count = 0 } },
        .{ .elem = str, .stats = .{ .min_value = "0150", .max_value = "0120", .null_count = 0 } },
        .{ .elem = u32_elem, .stats = .{ .min_value = &u_max, .max_value = &u_one, .null_count = 0 } },
        .{ .elem = dec, .stats = .{ .min_value = "\x00\x05", .max_value = "\xff\xfe", .null_count = 0 } },
        .{ .elem = leafForTest(.BOOLEAN), .stats = .{ .min_value = "\x01", .max_value = "\x00", .null_count = 0 } },
        .{ .elem = leafForTest(.INT64), .stats = .{ .min = &lo, .max = &hi, .null_count = 0 } },
        // A null count outside the chunk's 100 values, the 1000-on-100 variant of T2 among them.
        .{ .elem = leafForTest(.INT64), .stats = .{ .min_value = &hi, .max_value = &lo, .null_count = -1 } },
        .{ .elem = leafForTest(.INT64), .stats = .{ .min_value = &hi, .max_value = &lo, .null_count = 101 } },
        .{ .elem = leafForTest(.INT64), .stats = .{ .min_value = &hi, .max_value = &lo, .null_count = 1000 } },
        .{ .elem = leafForTest(.INT64), .stats = .{ .null_count = 1000 } },
    };
    for (impossible, 0..) |c, i| for ([_]Trust{ .corroborated, .plausible }) |trust| {
        const fx = try ColumnForTest.init(a, c.elem, c.stats);
        const got = chunkStats(&fx.rg, 0, &fx.fm, trust);
        testing.expect(got.bounds == null and got.null_count == null) catch |err| {
            std.debug.print("impossible case {d} used under {t}\n", .{ i, trust });
            return err;
        };
    };

    // Bounds in order for the column's own order stay, though another order would call them inverted.
    const unsigned_ok = try ColumnForTest.init(a, u32_elem, .{ .min_value = &u_one, .max_value = &u_max });
    try testing.expect(chunkStats(&unsigned_ok.rg, 0, &unsigned_ok.fm, .corroborated).bounds != null);
    const dec_ok = try ColumnForTest.init(a, dec, .{ .min_value = "\xff\xfe", .max_value = "\x00\x05" });
    try testing.expect(chunkStats(&dec_ok.rg, 0, &dec_ok.fm, .corroborated).bounds != null);
    // NaN bounds are not judged here; every reader of float bounds declines them.
    const nan = try ColumnForTest.init(a, leafForTest(.DOUBLE), .{ .min_value = &f_nan, .max_value = &f_one });
    try testing.expect(chunkStats(&nan.rg, 0, &nan.fm, .corroborated).bounds != null);

    // A flat chunk whose value count is not its row count, either way.
    for ([_]i64{ 99, 101 }) |num_values| for ([_]Trust{ .corroborated, .plausible }) |trust| {
        var fx = try ColumnForTest.init(a, leafForTest(.INT64), .{ .min_value = &hi, .max_value = &lo });
        fx.rg.columns.items[0].meta_data.?.num_values = num_values;
        const got = chunkStats(&fx.rg, 0, &fx.fm, trust);
        try testing.expect(got.bounds == null and got.null_count == null);
    };
}

test "chunkStats: contradictory and uncorroborated null counts are used only when plausible ones are trusted" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const lo = le(i64, 100);
    const hi = le(i64, 199);

    // Every row null, yet bounds (T2): both claims go by default.
    const i64_elem = leafForTest(.INT64);
    const all_null = try ColumnForTest.init(a, i64_elem, .{ .min_value = &lo, .max_value = &hi, .null_count = 100 });
    const strict = chunkStats(&all_null.rg, 0, &all_null.fm, .corroborated);
    try testing.expect(strict.bounds == null and strict.null_count == null);
    const trusted = chunkStats(&all_null.rg, 0, &all_null.fm, .plausible);
    try testing.expectEqual(@as(?i64, 100), trusted.null_count);
    try testing.expect(trusted.bounds != null);
    // ...the deprecated pair counts as bounds too.
    const legacy = try ColumnForTest.init(a, leafForTest(.INT64), .{ .min = &lo, .max = &hi, .null_count = 100 });
    try testing.expect(chunkStats(&legacy.rg, 0, &legacy.fm, .corroborated).null_count == null);
    // Every row null and no bounds is what a writer writes.
    const honest = try ColumnForTest.init(a, leafForTest(.INT64), .{ .null_count = 100 });
    try testing.expectEqual(@as(?i64, 100), chunkStats(&honest.rg, 0, &honest.fm, .corroborated).null_count);

    // No nulls and no bounds (T3): uncorroborated by default where a writer would have written bounds.
    const bare = try ColumnForTest.init(a, leafForTest(.INT64), .{ .null_count = 0 });
    try testing.expectEqual(@as(?i64, null), chunkStats(&bare.rg, 0, &bare.fm, .corroborated).null_count);
    try testing.expectEqual(@as(?i64, 0), chunkStats(&bare.rg, 0, &bare.fm, .plausible).null_count);
    const with_bounds = try ColumnForTest.init(a, i64_elem, .{ .min_value = &lo, .max_value = &hi, .null_count = 0 });
    try testing.expectEqual(@as(?i64, 0), chunkStats(&with_bounds.rg, 0, &with_bounds.fm, .corroborated).null_count);
    // Where a writer keeps no bounds: INT96, an order zpq cannot read, a float chunk of nothing but NaN.
    const int96 = try ColumnForTest.init(a, leafForTest(.INT96), .{ .null_count = 0 });
    try testing.expectEqual(@as(?i64, 0), chunkStats(&int96.rg, 0, &int96.fm, .corroborated).null_count);
    var interval = leafForTest(.FIXED_LEN_BYTE_ARRAY);
    interval.converted_type = .INTERVAL;
    const unordered = try ColumnForTest.init(a, interval, .{ .null_count = 0 });
    try testing.expectEqual(@as(?i64, 0), chunkStats(&unordered.rg, 0, &unordered.fm, .corroborated).null_count);
    const all_nan = try ColumnForTest.init(a, leafForTest(.DOUBLE), .{ .null_count = 0, .nan_count = 100 });
    try testing.expectEqual(@as(?i64, 0), chunkStats(&all_nan.rg, 0, &all_nan.fm, .corroborated).null_count);
    const some_nan = try ColumnForTest.init(a, leafForTest(.DOUBLE), .{ .null_count = 0, .nan_count = 3 });
    try testing.expectEqual(@as(?i64, null), chunkStats(&some_nan.rg, 0, &some_nan.fm, .corroborated).null_count);
}

test "pageIndexIsPlausible: page locations must lie in page order inside the chunk" {
    var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_state.deinit();
    const a = arena_state.allocator();

    // Three pages of 10 rows and 10 bytes from byte 4; the chunk spans bytes 4..34.
    var ci: schema.ColumnIndex = .{};
    var oi: schema.OffsetIndex = .{};
    for ([_]i64{ 0, 10, 20 }) |first| {
        try ci.null_pages.append(a, false);
        try ci.min_values.append(a, "");
        try ci.max_values.append(a, "");
        try oi.page_locations.append(a, .{ .offset = 4 + first, .compressed_page_size = 10, .first_row_index = first });
    }
    const cm: schema.ColumnMetaData = .{
        .type = .BYTE_ARRAY,
        .encodings = .empty,
        .path_in_schema = .empty,
        .codec = .UNCOMPRESSED,
        .num_values = 30,
        .total_uncompressed_size = 30,
        .total_compressed_size = 30,
        .data_page_offset = 4,
        .index_page_offset = null,
        .dictionary_page_offset = null,
    };
    const check = struct {
        fn f(c: *const schema.ColumnIndex, o: *const schema.OffsetIndex, m: schema.ColumnMetaData, rows: i64) bool {
            return pageIndexIsPlausible(c, o, m, null, .{ .max_def = 1, .max_rep = 0 }, rows, .plausible);
        }
    }.f;
    try testing.expect(check(&ci, &oi, cm, 30));
    try testing.expect(!check(&ci, &oi, cm, 20)); // a page starts at the row group's end

    const locs = oi.page_locations.items;
    const Edit = struct { page: usize, loc: schema.PageLocation };
    const edits = [_]Edit{
        .{ .page = 2, .loc = .{ .offset = 24, .compressed_page_size = 10, .first_row_index = 5 } }, // rows go back
        .{ .page = 2, .loc = .{ .offset = 24, .compressed_page_size = 10, .first_row_index = 10 } }, // rows repeat
        .{ .page = 0, .loc = .{ .offset = 4, .compressed_page_size = 10, .first_row_index = 3 } }, // rows start late
        .{ .page = 0, .loc = .{ .offset = 4, .compressed_page_size = 10, .first_row_index = -1 } },
        .{ .page = 1, .loc = .{ .offset = 10, .compressed_page_size = 10, .first_row_index = 10 } }, // overlaps page 0
        .{ .page = 0, .loc = .{ .offset = 0, .compressed_page_size = 4, .first_row_index = 0 } }, // before the chunk
        .{ .page = 2, .loc = .{ .offset = 30, .compressed_page_size = 10, .first_row_index = 20 } }, // past its end
        .{ .page = 1, .loc = .{ .offset = 14, .compressed_page_size = 0, .first_row_index = 10 } }, // no bytes
    };
    for (edits, 0..) |e, i| {
        const kept = locs[e.page];
        locs[e.page] = e.loc;
        defer locs[e.page] = kept;
        testing.expect(!check(&ci, &oi, cm, 30)) catch |err| {
            std.debug.print("page location edit {d} kept the index\n", .{i});
            return err;
        };
    }
    // T7: pages 1 and 2 swapped while their row starts stay in order.
    std.mem.swap(i64, &locs[1].offset, &locs[2].offset);
    try testing.expect(!check(&ci, &oi, cm, 30));
    std.mem.swap(i64, &locs[1].offset, &locs[2].offset);

    // A dictionary page first moves the chunk's start before the first data page.
    var with_dict = cm;
    with_dict.dictionary_page_offset = 0;
    with_dict.data_page_offset = 4;
    with_dict.total_compressed_size = 34;
    try testing.expect(check(&ci, &oi, with_dict, 30));

    _ = ci.min_values.pop(); // ColumnIndex shorter than OffsetIndex
    try testing.expect(!check(&ci, &oi, cm, 30));
}

test "pageIndexIsPlausible: inverted page bounds are refused under any trust, contradictions only by default" {
    var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_state.deinit();
    const a = arena_state.allocator();

    // Two pages of 10 rows each.
    var oi: schema.OffsetIndex = .{};
    try oi.page_locations.appendSlice(a, &.{
        .{ .offset = 4, .compressed_page_size = 10, .first_row_index = 0 },
        .{ .offset = 14, .compressed_page_size = 10, .first_row_index = 10 },
    });
    const Page = struct { null_page: bool = false, min: []const u8, max: []const u8, nulls: i64 = 0 };
    const Fx = struct {
        fn index(alloc: std.mem.Allocator, pages: []const Page) !schema.ColumnIndex {
            var ci: schema.ColumnIndex = .{};
            var nc: std.ArrayListUnmanaged(i64) = .empty;
            for (pages) |p| {
                try ci.null_pages.append(alloc, p.null_page);
                try ci.min_values.append(alloc, p.min);
                try ci.max_values.append(alloc, p.max);
                try nc.append(alloc, p.nulls);
            }
            ci.null_counts = nc;
            return ci;
        }
    };
    var cm: schema.ColumnMetaData = .{
        .type = .INT32,
        .encodings = .empty,
        .path_in_schema = .empty,
        .codec = .UNCOMPRESSED,
        .num_values = 20,
        .total_uncompressed_size = 20,
        .total_compressed_size = 20,
        .data_page_offset = 4,
        .index_page_offset = null,
        .dictionary_page_offset = null,
    };
    const optional: schema.Levels = .{ .max_def = 1, .max_rep = 0 };
    const one = le(i32, 1);
    const five = le(i32, 5);
    const max_u = le(u32, 0xffff_ffff);
    var u32_elem = leafForTest(.INT32);
    u32_elem.converted_type = .UINT_32;
    const Case = struct {
        type: schema.Type = .INT32,
        elem: ?schema.SchemaElement = null,
        pages: []const Page,
        /// Kept by default, and under --trust-stats.
        strict: bool,
        trusted: bool,
    };
    const ok: Page = .{ .min = &one, .max = &five };
    const wide: Page = .{ .min = &one, .max = &max_u }; // in order unsigned, inverted signed
    const null_with_bounds: Page = .{ .null_page = true, .min = &one, .max = &five, .nulls = 10 };
    const honest_null: Page = .{ .null_page = true, .min = "", .max = "", .nulls = 10 };
    const cases = [_]Case{
        .{ .pages = &.{ ok, .{ .min = &one, .max = &five, .nulls = 3 } }, .strict = true, .trusted = true },
        // T6: one value page with min above max.
        .{ .pages = &.{ .{ .min = &five, .max = &one }, ok }, .strict = false, .trusted = false },
        .{
            .type = .BYTE_ARRAY,
            .pages = &.{ .{ .min = "0149", .max = "0125" }, .{ .min = "a", .max = "b" } },
            .strict = false,
            .trusted = false,
        },
        .{ .elem = u32_elem, .pages = &.{ wide, ok }, .strict = true, .trusted = true },
        .{ .pages = &.{ wide, ok }, .strict = false, .trusted = false },
        // A null count above the page's rows.
        .{ .pages = &.{ .{ .min = &one, .max = &five, .nulls = 11 }, ok }, .strict = false, .trusted = false },
        // A null page with bounds, and a page of nothing but nulls not flagged as one.
        .{ .pages = &.{ null_with_bounds, ok }, .strict = false, .trusted = true },
        .{ .pages = &.{ .{ .min = &one, .max = &five, .nulls = 10 }, ok }, .strict = false, .trusted = true },
        .{ .pages = &.{ honest_null, ok }, .strict = true, .trusted = true },
    };
    for (cases, 0..) |c, i| {
        const ci = try Fx.index(a, c.pages);
        cm.type = c.type;
        cm.statistics = .{ .null_count = 20 };
        for ([_]Trust{ .corroborated, .plausible }, [_]bool{ c.strict, c.trusted }) |trust, want| {
            testing.expectEqual(want, pageIndexIsPlausible(&ci, &oi, cm, c.elem, optional, 20, trust)) catch |err| {
                std.debug.print("page bounds case {d} under {t}\n", .{ i, trust });
                return err;
            };
        }
    }
}
