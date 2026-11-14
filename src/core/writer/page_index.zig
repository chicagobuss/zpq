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
    return columnIndexWithOptions(arena, pages, elem, .{});
}

pub const Options = struct { unordered: bool = false, omit_bounds: bool = false };

fn columnIndexWithOptions(arena: std.mem.Allocator, pages: []const encoder.PageInfo, elem: *const schema.SchemaElement, options: Options) !?schema.ColumnIndex {
    if (options.omit_bounds) return null;
    if (pages.len == 0) return null;
    var ci: schema.ColumnIndex = .{};
    var nulls: std.ArrayList(i64) = .empty;
    var nans: std.ArrayList(i64) = .empty;
    var known_nulls = true;
    var known_nans = true;
    const float = elem.type == .FLOAT or elem.type == .DOUBLE;
    for (pages) |p| {
        const null_page = p.null_count == @as(i64, @intCast(p.rows));
        if (!null_page and (p.min == null or p.max == null)) return null;
        if (!null_page and (compare(elem, p.min.?, p.max.?) orelse return null) == .gt) return null;
        try ci.null_pages.append(arena, null_page);
        try ci.min_values.append(arena, if (null_page) "" else p.min.?);
        try ci.max_values.append(arena, if (null_page) "" else p.max.?);
        try nulls.append(arena, p.null_count);
        known_nulls = known_nulls and p.null_count >= 0;
        known_nans = known_nans and p.nan_count != null;
        if (float) try nans.append(arena, p.nan_count orelse 0);
    }
    if (known_nulls) ci.null_counts = nulls;
    if (float and known_nans) ci.nan_counts = nans;
    ci.boundary_order = if (options.unordered) .UNORDERED else boundaryOrder(pages, elem);
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
            const lo = compare(elem, q.min.?, p.min.?) orelse return .UNORDERED;
            const hi = compare(elem, q.max.?, p.max.?) orelse return .UNORDERED;
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
pub fn compare(elem: *const schema.SchemaElement, a: []const u8, b: []const u8) ?std.math.Order {
    const width: usize = switch (elem.type orelse return null) {
        .INT32, .FLOAT => 4,
        .INT64, .DOUBLE => 8,
        .BOOLEAN => 1,
        .INT96 => 12,
        .FIXED_LEN_BYTE_ARRAY => std.math.cast(usize, elem.type_length orelse return null) orelse return null,
        .BYTE_ARRAY => if (decimal_mod.kindFromSchema(elem) != null) 1 else 0,
    };
    if (a.len < width or b.len < width) return null;
    const unsigned = schema.isUnsignedInt(elem.*);
    return switch (elem.type orelse return .eq) {
        .INT32 => if (unsigned) order(u32, a, b) else order(i32, a, b),
        .INT64 => if (unsigned) order(u64, a, b) else order(i64, a, b),
        .FLOAT => floatOrder(f32, u32, a, b),
        .DOUBLE => floatOrder(f64, u64, a, b),
        .BOOLEAN => std.math.order(a[0], b[0]),
        .BYTE_ARRAY, .FIXED_LEN_BYTE_ARRAY, .INT96 => if (decimal_mod.kindFromSchema(elem) != null)
            signedBigEndian(a, b)
        else
            std.mem.order(u8, a, b),
    };
}

/// BYTE_ARRAY DECIMAL can use different widths for the same signed integer.
fn signedBigEndian(left: []const u8, right: []const u8) std.math.Order {
    var a = left;
    var b = right;
    const negative = a[0] & 0x80 != 0;
    if (negative != (b[0] & 0x80 != 0)) return if (negative) .lt else .gt;
    while (a.len > 1 and ((a[0] == 0 and a[1] & 0x80 == 0) or (a[0] == 0xff and a[1] & 0x80 != 0))) a = a[1..];
    while (b.len > 1 and ((b[0] == 0 and b[1] & 0x80 == 0) or (b[0] == 0xff and b[1] & 0x80 != 0))) b = b[1..];
    if (a.len != b.len) return if (negative) std.math.order(b.len, a.len) else std.math.order(a.len, b.len);
    return std.mem.order(u8, a, b);
}

fn floatOrder(comptime F: type, comptime U: type, a: []const u8, b: []const u8) ?std.math.Order {
    const x: F = @bitCast(read(U, a));
    const y: F = @bitCast(read(U, b));
    if (std.math.isNan(x) or std.math.isNan(y)) return null;
    return std.math.order(x, y);
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
        .compressed_page_size = std.math.cast(i32, p.size) orelse return error.TooLarge,
        .first_row_index = @intCast(p.first_row),
    });
    return oi;
}

fn serialize(arena: std.mem.Allocator, value: anytype) ![]const u8 {
    var w: thrift.Writer = .init(arena);
    try value.write(&w);
    return w.bytes();
}

/// Hold `chunk`'s ColumnIndex and OffsetIndex, from its encoded `pages`, for `placeCarried`; the chunk must already be
/// at its final offset.
pub fn attach(
    arena: std.mem.Allocator,
    chunk: *schema.ColumnChunk,
    pages: []const encoder.PageInfo,
    elem: *const schema.SchemaElement,
) !void {
    return attachWithOptions(arena, chunk, pages, elem, .{});
}

pub fn attachWithOptions(arena: std.mem.Allocator, chunk: *schema.ColumnChunk, pages: []const encoder.PageInfo, elem: *const schema.SchemaElement, options: Options) !void {
    if (pages.len == 0) return;
    const start: u64 = @intCast(chunkStart(chunk));
    if (try columnIndexWithOptions(arena, pages, elem, options)) |ci| chunk.column_index_out = try serialize(arena, &ci);
    chunk.offset_index_out = try serialize(arena, &(try offsetIndex(arena, pages, start)));
}

/// Write every page index the chunks of `rgs` hold (`column_index_out`, `offset_index_out`) after the row groups:
/// every ColumnIndex, column by column and row group by row group within one, then every OffsetIndex in the same
/// order, as other writers lay them out. Some readers (parquet-go) read each kind as one range from the first
/// chunk's, so one placed beside its chunk breaks them for a file of more than one row group.
pub fn placeCarried(out: anytype, rgs: []schema.RowGroup) !void {
    var cols: usize = 0;
    for (rgs) |rg| cols = @max(cols, rg.columns.items.len);
    for (0..cols) |ci| for (rgs) |*rg| {
        if (ci >= rg.columns.items.len) continue;
        const c = &rg.columns.items[ci];
        if (c.column_index_out.len == 0) continue;
        c.column_index_offset = @intCast(out.pos());
        c.column_index_length = @intCast(c.column_index_out.len);
        try out.write(c.column_index_out);
    };
    for (0..cols) |ci| for (rgs) |*rg| {
        if (ci >= rg.columns.items.len) continue;
        const c = &rg.columns.items[ci];
        if (c.offset_index_out.len == 0) continue;
        c.offset_index_offset = @intCast(out.pos());
        c.offset_index_length = @intCast(c.offset_index_out.len);
        try out.write(c.offset_index_out);
    };
}

/// Where a chunk's bytes start in the file: its dictionary page, else its first data page.
fn chunkStart(chunk: *const schema.ColumnChunk) i64 {
    const m = chunk.meta_data.?;
    return m.dictionary_page_offset orelse m.data_page_offset;
}

/// Page indexes of chunks already placed, written together once the row groups are (`write`).
pub const Deferred = struct {
    entries: std.ArrayList(Entry) = .empty,

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

    /// Build each entry's page index onto its chunk, then write every chunk's of `rgs` (`placeCarried`): the copied
    /// row groups' among the encoded ones', in one order.
    pub fn write(self: *Deferred, arena: std.mem.Allocator, out: *streaming.SinkOut, rgs: []schema.RowGroup) !void {
        for (self.entries.items) |e| try attach(arena, &rgs[e.rg].columns.items[e.col], e.pages, e.elem);
        try placeCarried(out, rgs);
    }
};

const testing = std.testing;

test "review: variable-width signed decimal bounds include sign extension and length" {
    var e = elemOf(.BYTE_ARRAY);
    e.converted_type = .DECIMAL;
    e.scale = 0;
    e.precision = 9;
    try testing.expectEqual(std.math.Order.lt, compare(&e, "\xff\x00", "\xf6"));
    try testing.expectEqual(std.math.Order.lt, compare(&e, "\x7f", "\x01\x00"));
    try testing.expectEqual(std.math.Order.eq, compare(&e, "\x00\x7f", "\x7f"));
    try testing.expectEqual(std.math.Order.eq, compare(&e, "\xff\xff\x00", "\xff\x00"));
    const pages = [_]encoder.PageInfo{
        .{ .offset = 0, .size = 1, .first_row = 0, .rows = 1, .null_count = 0, .min = "\xf6", .max = "\xf6" },
        .{ .offset = 1, .size = 1, .first_row = 1, .rows = 1, .null_count = 0, .min = "\xff\x00", .max = "\xff\x00" },
        .{ .offset = 2, .size = 1, .first_row = 2, .rows = 1, .null_count = 0, .min = "\x64", .max = "\x64" },
    };
    try testing.expectEqual(schema.BoundaryOrder.UNORDERED, boundaryOrder(&pages, &e));
}

test "review: malformed and NaN page bounds produce no column index" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    for ([_]schema.Type{ .FLOAT, .DOUBLE, .INT32, .INT64, .BOOLEAN }) |t| {
        const e = elemOf(t);
        const pages = [_]encoder.PageInfo{
            .{ .offset = 0, .size = 1, .first_row = 0, .rows = 1, .null_count = 0, .min = "", .max = "" },
            .{ .offset = 1, .size = 1, .first_row = 1, .rows = 1, .null_count = 0, .min = "", .max = "" },
        };
        try testing.expect((try columnIndex(a, &pages, &e)) == null);
    }
    var nan: [8]u8 = @splat(0);
    for ([_]schema.Type{ .FLOAT, .DOUBLE }) |t| {
        const e = elemOf(t);
        const width: usize = if (t == .FLOAT) 4 else 8;
        if (t == .FLOAT) std.mem.writeInt(u32, nan[0..4], 0x7fc00000, .little) else std.mem.writeInt(u64, &nan, 0x7ff8000000000000, .little);
        const pages = [_]encoder.PageInfo{.{ .offset = 0, .size = 1, .first_row = 0, .rows = 1, .null_count = 0, .min = nan[0..width], .max = nan[0..width] }};
        try testing.expect((try columnIndex(a, &pages, &e)) == null);
    }
}

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
    // Unsigned: -5 is the largest value; valid page bounds in that order are not monotone.
    e.converted_type = .UINT_32;
    try testing.expect((try columnIndex(a, &asc, &e)) == null); // The first signed pair is inverted unsigned.
    var unsigned = [_]P{ asc[0], asc[2], asc[2] };
    unsigned[0].max = unsigned[0].min;
    unsigned[1].max = unsigned[1].min;
    unsigned[2].min = unsigned[2].max;
    try testing.expectEqual(schema.BoundaryOrder.UNORDERED, (try columnIndex(a, &unsigned, &e)).?.boundary_order);
    try testing.expectEqual(schema.BoundaryOrder.UNORDERED, (try columnIndex(a, unsigned[0..1], &e)).?.boundary_order);
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
