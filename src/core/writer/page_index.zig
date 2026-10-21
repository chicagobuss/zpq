//! The page index of a freshly encoded column chunk: its ColumnIndex (per data page: null flag, bounds, null and NaN
//! counts, and whether the bounds are ordered) and OffsetIndex (per data page: where it starts, its size with header,
//! its first row), from what the encoder recorded of each page (`encoder.PageInfo`).
//!
//! Both are written after the row groups, every ColumnIndex column by column and then every OffsetIndex column by
//! column, as parquet-mr lays them out, so a reader that prunes on one column finds its indexes for every row group
//! in one range (`Deferred`).

const std = @import("std");
const schema = @import("../schema.zig");
const thrift = @import("../thrift.zig");
const encoder = @import("encoder.zig");
const streaming = @import("streaming.zig");
const decimal_mod = @import("../parquet/decimal.zig");

/// The ColumnIndex of a chunk of type `elem`, or null when it cannot be truthful: a page holding present values
/// without bounds (a float page of NaN only). Readers then fall back to the chunk's statistics.
pub fn columnIndex(
    arena: std.mem.Allocator,
    pages: []const encoder.PageInfo,
    elem: *const schema.SchemaElement,
) !?schema.ColumnIndex {
    if (pages.len == 0) return null;
    var ci: schema.ColumnIndex = .{};
    var nulls: std.ArrayListUnmanaged(i64) = .empty;
    var nans: std.ArrayListUnmanaged(i64) = .empty;
    const float = elem.type == .FLOAT or elem.type == .DOUBLE;
    for (pages) |p| {
        const null_page = p.null_count == @as(i64, @intCast(p.rows));
        if (!null_page and (p.min == null or p.max == null)) return null;
        try ci.null_pages.append(arena, null_page);
        try ci.min_values.append(arena, if (null_page) "" else p.min.?);
        try ci.max_values.append(arena, if (null_page) "" else p.max.?);
        try nulls.append(arena, p.null_count);
        if (float) try nans.append(arena, p.nan_count orelse 0);
    }
    ci.null_counts = nulls;
    if (float) ci.nan_counts = nans;
    ci.boundary_order = boundaryOrder(pages, elem);
    return ci;
}

/// ASCENDING when every page's min and max are at least the previous non-null page's, DESCENDING when at most, in the
/// column's type-defined order; UNORDERED otherwise (and for one page, which is trivially both).
fn boundaryOrder(pages: []const encoder.PageInfo, elem: *const schema.SchemaElement) schema.BoundaryOrder {
    var asc = true;
    var desc = true;
    var prev: ?encoder.PageInfo = null;
    var bounded: usize = 0;
    for (pages) |p| {
        if (p.min == null) continue;
        bounded += 1;
        if (prev) |q| {
            const lo = compare(elem, q.min.?, p.min.?);
            const hi = compare(elem, q.max.?, p.max.?);
            if (lo == .gt or hi == .gt) asc = false;
            if (lo == .lt or hi == .lt) desc = false;
        }
        prev = p;
    }
    if (bounded < 2) return .UNORDERED;
    if (asc) return .ASCENDING;
    if (desc) return .DESCENDING;
    return .UNORDERED;
}

/// Two bounds of a column of `elem`'s type, in its type-defined order (unsigned integers unsigned, byte arrays
/// bytewise unsigned). Bounds never hold NaN.
fn compare(elem: *const schema.SchemaElement, a: []const u8, b: []const u8) std.math.Order {
    const unsigned = schema.isUnsignedInt(elem.*);
    return switch (elem.type orelse return .eq) {
        .INT32 => if (unsigned) order(u32, a, b) else order(i32, a, b),
        .INT64 => if (unsigned) order(u64, a, b) else order(i64, a, b),
        .FLOAT => std.math.order(@as(f32, @bitCast(read(u32, a))), @as(f32, @bitCast(read(u32, b)))),
        .DOUBLE => std.math.order(@as(f64, @bitCast(read(u64, a))), @as(f64, @bitCast(read(u64, b)))),
        .BOOLEAN => std.math.order(a[0], b[0]),
        .BYTE_ARRAY, .FIXED_LEN_BYTE_ARRAY, .INT96 => if (decimal_mod.kindFromSchema(elem) != null)
            signedBigEndian(a, b)
        else
            std.mem.order(u8, a, b),
    };
}

/// Two's-complement big-endian integers of one width, a DECIMAL's bounds: by sign, then bytewise.
fn signedBigEndian(a: []const u8, b: []const u8) std.math.Order {
    if (a.len == 0 or b.len == 0) return std.math.order(a.len, b.len);
    const sa: i8 = @bitCast(a[0]);
    const sb: i8 = @bitCast(b[0]);
    if (sa != sb) return std.math.order(sa, sb);
    return std.mem.order(u8, a[1..], b[1..]);
}

fn read(comptime T: type, bytes: []const u8) T {
    return std.mem.readInt(T, bytes[0..@sizeOf(T)], .little);
}

fn order(comptime T: type, a: []const u8, b: []const u8) std.math.Order {
    return std.math.order(read(T, a), read(T, b));
}

/// The OffsetIndex of a chunk whose bytes start at `chunk_start` in the file.
pub fn offsetIndex(arena: std.mem.Allocator, pages: []const encoder.PageInfo, chunk_start: u64) !schema.OffsetIndex {
    var oi: schema.OffsetIndex = .{};
    try oi.page_locations.ensureTotalCapacityPrecise(arena, pages.len);
    for (pages) |p| oi.page_locations.appendAssumeCapacity(.{
        .offset = @intCast(chunk_start + p.offset),
        .compressed_page_size = @intCast(p.size),
        .first_row_index = @intCast(p.first_row),
    });
    return oi;
}

fn serialize(arena: std.mem.Allocator, value: anytype) ![]const u8 {
    var w: thrift.Writer = .init(arena);
    try value.write(&w);
    return w.bytes();
}

/// Write `chunk`'s ColumnIndex and OffsetIndex straight after it, for writers that place chunks at their final offset
/// one at a time; sets the chunk's pointers to them.
pub fn writeInline(
    arena: std.mem.Allocator,
    out: *streaming.SinkOut,
    chunk: *schema.ColumnChunk,
    pages: []const encoder.PageInfo,
    elem: *const schema.SchemaElement,
) !void {
    if (pages.len == 0) return;
    const start: u64 = @intCast(chunkStart(chunk));
    if (try columnIndex(arena, pages, elem)) |ci| {
        const bytes = try serialize(arena, &ci);
        chunk.column_index_offset = @intCast(out.pos());
        chunk.column_index_length = @intCast(bytes.len);
        try out.write(bytes);
    }
    const bytes = try serialize(arena, &(try offsetIndex(arena, pages, start)));
    chunk.offset_index_offset = @intCast(out.pos());
    chunk.offset_index_length = @intCast(bytes.len);
    try out.write(bytes);
}

/// Where a chunk's bytes start in the file: its dictionary page, else its first data page.
fn chunkStart(chunk: *const schema.ColumnChunk) i64 {
    const m = chunk.meta_data.?;
    return m.dictionary_page_offset orelse m.data_page_offset;
}

/// Page indexes of chunks already placed, written together once the row groups are (`write`).
pub const Deferred = struct {
    entries: std.ArrayListUnmanaged(Entry) = .empty,

    const Entry = struct {
        rg: usize,
        col: usize,
        pages: []const encoder.PageInfo,
        elem: *const schema.SchemaElement,
    };

    /// Chunk `col` of output row group `rg`, its pages (relative to the chunk's start) on memory that lives until
    /// `write`.
    pub fn add(
        self: *Deferred,
        arena: std.mem.Allocator,
        rg: usize,
        col: usize,
        pages: []const encoder.PageInfo,
        elem: *const schema.SchemaElement,
    ) !void {
        if (pages.len == 0) return;
        try self.entries.append(arena, .{ .rg = rg, .col = col, .pages = pages, .elem = elem });
    }

    /// Write every ColumnIndex, column by column and row group by row group within one, then every OffsetIndex in
    /// the same order, and point `rgs`' chunks at them.
    pub fn write(self: *Deferred, arena: std.mem.Allocator, out: *streaming.SinkOut, rgs: []schema.RowGroup) !void {
        const items = self.entries.items;
        std.mem.sort(Entry, items, {}, struct {
            fn lt(_: void, a: Entry, b: Entry) bool {
                return if (a.col != b.col) a.col < b.col else a.rg < b.rg;
            }
        }.lt);
        for (items) |e| {
            const chunk = &rgs[e.rg].columns.items[e.col];
            const ci = (try columnIndex(arena, e.pages, e.elem)) orelse continue;
            const bytes = try serialize(arena, &ci);
            chunk.column_index_offset = @intCast(out.pos());
            chunk.column_index_length = @intCast(bytes.len);
            try out.write(bytes);
        }
        for (items) |e| {
            const chunk = &rgs[e.rg].columns.items[e.col];
            const bytes = try serialize(arena, &(try offsetIndex(arena, e.pages, @intCast(chunkStart(chunk)))));
            chunk.offset_index_offset = @intCast(out.pos());
            chunk.offset_index_length = @intCast(bytes.len);
            try out.write(bytes);
        }
    }
};

const testing = std.testing;

fn elemOf(t: schema.Type) schema.SchemaElement {
    return .{
        .type = t,
        .type_length = null,
        .repetition_type = .OPTIONAL,
        .name = "c",
        .num_children = null,
        .scale = null,
        .precision = null,
        .field_id = null,
    };
}

test "boundary order follows the type's order, skips null pages, and a single page is unordered" {
    var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_state.deinit();
    const a = arena_state.allocator();
    const le = struct {
        fn i(a_: std.mem.Allocator, v: i32) []const u8 {
            const b = a_.alloc(u8, 4) catch unreachable;
            std.mem.writeInt(i32, b[0..4], v, .little);
            return b;
        }
    };
    const P = encoder.PageInfo;
    const asc = [_]P{
        .{ .offset = 0, .size = 1, .first_row = 0, .rows = 2, .null_count = 0, .min = le.i(a, -5), .max = le.i(a, 1) },
        .{ .offset = 1, .size = 1, .first_row = 2, .rows = 2, .null_count = 2 },
        .{ .offset = 2, .size = 1, .first_row = 4, .rows = 2, .null_count = 0, .min = le.i(a, 1), .max = le.i(a, 9) },
    };
    var e = elemOf(.INT32);
    const ci = (try columnIndex(a, &asc, &e)).?;
    try testing.expectEqual(schema.BoundaryOrder.ASCENDING, ci.boundary_order);
    try testing.expectEqualSlices(bool, &.{ false, true, false }, ci.null_pages.items);
    try testing.expectEqualSlices(i64, &.{ 0, 2, 0 }, ci.null_counts.?.items);
    // Unsigned: -5 is the largest value, so the same bounds are no longer ascending.
    e.converted_type = .UINT_32;
    try testing.expectEqual(schema.BoundaryOrder.UNORDERED, (try columnIndex(a, &asc, &e)).?.boundary_order);
    try testing.expectEqual(schema.BoundaryOrder.UNORDERED, (try columnIndex(a, asc[0..1], &e)).?.boundary_order);
    const desc = [_]P{ asc[2], asc[0] };
    e.converted_type = null;
    try testing.expectEqual(schema.BoundaryOrder.DESCENDING, (try columnIndex(a, &desc, &e)).?.boundary_order);
    // A page with present values but no bounds (NaN only) leaves no truthful column index.
    const nan_only = [_]P{.{ .offset = 0, .size = 1, .first_row = 0, .rows = 3, .null_count = 1, .nan_count = 2 }};
    var d = elemOf(.DOUBLE);
    try testing.expect((try columnIndex(a, &nan_only, &d)) == null);
}

test "decimal bounds order as signed integers: a negative page before a positive one is ascending" {
    var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_state.deinit();
    const a = arena_state.allocator();
    var e = elemOf(.FIXED_LEN_BYTE_ARRAY);
    e.type_length = 2;
    e.converted_type = .DECIMAL;
    e.scale = 1;
    e.precision = 4;
    const neg = [_]u8{ 0xff, 0x00 }; // -256
    const neg1 = [_]u8{ 0xff, 0xff }; // -1
    const pos = [_]u8{ 0x00, 0x05 }; // 5
    const P = encoder.PageInfo;
    const pages = [_]P{
        .{ .offset = 0, .size = 1, .first_row = 0, .rows = 1, .null_count = 0, .min = &neg, .max = &neg1 },
        .{ .offset = 1, .size = 1, .first_row = 1, .rows = 1, .null_count = 0, .min = &pos, .max = &pos },
    };
    try testing.expectEqual(schema.BoundaryOrder.ASCENDING, (try columnIndex(a, &pages, &e)).?.boundary_order);
}

test "offset index locations are the pages' absolute starts, sizes and first rows" {
    var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_state.deinit();
    const a = arena_state.allocator();
    const pages = [_]encoder.PageInfo{
        .{ .offset = 30, .size = 100, .first_row = 0, .rows = 10, .null_count = 0 },
        .{ .offset = 130, .size = 90, .first_row = 10, .rows = 7, .null_count = 0 },
    };
    const oi = try offsetIndex(a, &pages, 1000);
    try testing.expectEqual(@as(usize, 2), oi.page_locations.items.len);
    try testing.expectEqual(@as(i64, 1030), oi.page_locations.items[0].offset);
    try testing.expectEqual(@as(i64, 1130), oi.page_locations.items[1].offset);
    try testing.expectEqual(@as(i32, 90), oi.page_locations.items[1].compressed_page_size);
    try testing.expectEqual(@as(i64, 10), oi.page_locations.items[1].first_row_index);
}
