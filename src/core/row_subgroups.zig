//! Query-owned page locations and dictionaries for independently scheduled ranges of a flat row group.
const std = @import("std");
const schema = @import("schema.zig");
const column = @import("parquet/column.zig");
const page = @import("parquet/page.zig");

pub var test_ranges: std.atomic.Value(usize) = .init(0);

pub const Range = struct { lo: usize, hi: usize };
pub const Dictionary = union(enum) {
    i32: []const i32,
    i64: []const i64,
    f32: []const f32,
    f64: []const f64,
    boolean: []const bool,
    bytes: []const []const u8,

    pub fn get(self: Dictionary, comptime T: type) []const T {
        return @field(self, switch (T) {
            i32 => "i32",
            i64 => "i64",
            f32 => "f32",
            f64 => "f64",
            bool => "boolean",
            []const u8 => "bytes",
            else => @compileError("unsupported dictionary type"),
        });
    }
};

pub const Column = struct {
    locations: []const schema.PageLocation,
    chunk_offset: i64,
    dictionary: ?Dictionary = null,
    dictionary_offset: ?i64 = null,
};

pub const Group = struct {
    columns: []?Column,
    ranges: []const Range,
    unfinished: *std.atomic.Value(usize),

    /// A charged arena owns every result, including decompressed dictionary backing. It outlives all workers.
    /// Failure is an optional-plan refusal: the caller releases the arena and keeps the established decoder.
    pub fn init(
        a: std.mem.Allocator,
        bytes: []const u8,
        rg: *const schema.RowGroup,
        meta: *const schema.FileMetaData,
        read: []const bool,
        parts: usize,
    ) !?Group {
        if (parts < 2 or rg.num_rows <= 0) return null;
        const columns = try a.alloc(?Column, rg.columns.items.len);
        @memset(columns, null);
        var widest: usize = 0;
        var width: u64 = 0;
        for (read, 0..) |needed, ci| {
            if (!needed) continue;
            if (ci >= columns.len) return null;
            const cm = rg.columns.items[ci].meta_data orelse return null;
            // One leaf per row is insufficient for nested optional structs: keep their established semantics too.
            if (cm.path_in_schema.items.len != 1 or meta.getColumnLevels(cm.path_in_schema.items).max_rep != 0 or
                cm.num_values != rg.num_rows) return null;
            columns[ci] = try locate(a, bytes, cm, @intCast(rg.num_rows));
            const chunk_width: u64 = @intCast(cm.total_compressed_size);
            if (chunk_width > width) {
                width = chunk_width;
                widest = ci;
            }
        }
        const anchor = columns[widest] orelse return null;
        if (anchor.locations.len < 2) return null;
        const n = @min(parts, anchor.locations.len);
        var total: u64 = 0;
        for (anchor.locations) |loc| total += @intCast(loc.compressed_page_size);
        var ranges: std.ArrayList(Range) = .empty;
        var lo: usize = 0;
        var consumed: u64 = 0;
        for (anchor.locations, 0..) |loc, pi| {
            consumed += @intCast(loc.compressed_page_size);
            if (pi + 1 < anchor.locations.len and ranges.items.len + 1 < n and
                consumed >= total * (ranges.items.len + 1) / n)
            {
                const hi: usize = @intCast(anchor.locations[pi + 1].first_row_index);
                try ranges.append(a, .{ .lo = lo, .hi = hi });
                lo = hi;
            }
        }
        try ranges.append(a, .{ .lo = lo, .hi = @intCast(rg.num_rows) });
        if (ranges.items.len < 2) return null;
        // Admission and every page walk precede dictionary decompression. No range owns a private dictionary copy.
        for (columns, 0..) |*entry, ci| if (entry.*) |*col| {
            if (col.dictionary_offset == null) continue;
            const cm = rg.columns.items[ci].meta_data.?;
            const chunk = try chunkBytes(bytes, cm);
            const options = column.chunkOptions(.{ .page_bytes_limit = column.inflateLimit(cm) }, meta, cm);
            const levels = meta.getColumnLevels(cm.path_in_schema.items);
            switch (cm.type) {
                inline .INT32, .INT64, .FLOAT, .DOUBLE, .BOOLEAN, .BYTE_ARRAY, .FIXED_LEN_BYTE_ARRAY, .INT96 => |tag| {
                    const T, const name = switch (tag) {
                        .INT32 => .{ i32, "i32" },
                        .INT64 => .{ i64, "i64" },
                        .FLOAT => .{ f32, "f32" },
                        .DOUBLE => .{ f64, "f64" },
                        .BOOLEAN => .{ bool, "boolean" },
                        else => .{ []const u8, "bytes" },
                    };
                    var r = column.ColumnChunkReader(T).initWithOptions(chunk, cm.codec, levels, a, options, null);
                    if (T == []const u8) r.type_length = if (tag == .INT96) 12 else if (tag == .FIXED_LEN_BYTE_ARRAY)
                        columnWidth(meta.getColumnSchema(cm.path_in_schema.items))
                    else
                        0;
                    if (!try r.seekAndInstallDictionaryPage(col.dictionary_offset.?, col.chunk_offset)) return null;
                    col.dictionary = @unionInit(Dictionary, name, r.dictionary orelse return null);
                },
            }
        };
        const unfinished = try a.create(std.atomic.Value(usize));
        unfinished.* = .init(ranges.items.len);
        return .{ .columns = columns, .ranges = ranges.items, .unfinished = unfinished };
    }
};

fn columnWidth(se: ?schema.SchemaElement) usize {
    return if (se) |s| std.math.cast(usize, s.type_length orelse 0) orelse 0 else 0;
}

fn chunkBytes(bytes: []const u8, cm: schema.ColumnMetaData) ![]const u8 {
    const start = std.math.cast(usize, cm.dictionary_page_offset orelse cm.data_page_offset) orelse
        return error.InvalidColumnOffsets;
    const len = std.math.cast(usize, cm.total_compressed_size) orelse return error.InvalidColumnOffsets;
    if (start > bytes.len or len > bytes.len - start) return error.MissingChunkBytes;
    return bytes[start..][0..len];
}

fn locate(a: std.mem.Allocator, bytes: []const u8, cm: schema.ColumnMetaData, rows: usize) !Column {
    const chunk = try chunkBytes(bytes, cm);
    const start = cm.dictionary_page_offset orelse cm.data_page_offset;
    var reader = page.PageReader.init(chunk, cm.codec, a);
    var locations: std.ArrayList(schema.PageLocation) = .empty;
    var row: usize = 0;
    var dict: ?i64 = null;
    var inflated: usize = 0;
    // Derive locations from validated headers instead of trusting optional serialized indexes. No data page is
    // decompressed, including for old writers without an index. Invalid metadata declines the whole split.
    while (try reader.peek()) |p| {
        inflated = std.math.add(usize, inflated, @intCast(p.header.uncompressed_page_size)) catch return error.BadPageHeader;
        if (inflated > column.inflateLimit(cm)) return error.PageLargerThanChunk;
        const offset = std.math.add(i64, start, @intCast(reader.pos)) catch return error.InvalidColumnOffsets;
        switch (p.header.type) {
            .DICTIONARY_PAGE => {
                if (dict != null or row != 0 or locations.items.len != 0) return error.UnexpectedDictionaryPage;
                dict = offset;
            },
            .DATA_PAGE, .DATA_PAGE_V2 => {
                const count = if (p.header.type == .DATA_PAGE)
                    (p.header.data_page_header orelse return error.BadPageHeader).num_values
                else blk: {
                    const h = p.header.data_page_header_v2 orelse return error.BadPageHeader;
                    if (h.num_rows != h.num_values) return error.BadPageCount;
                    break :blk h.num_rows;
                };
                if (count <= 0) return error.BadPageCount;
                const n: usize = @intCast(count);
                if (row > rows or n > rows - row) return error.ShortDecode;
                try locations.append(a, .{ .offset = offset, .compressed_page_size = std.math.cast(i32, p.size) orelse
                    return error.BadPageHeader, .first_row_index = @intCast(row) });
                row += n;
            },
            .INDEX_PAGE => {},
        }
        reader.pass(p);
    }
    if (row != rows) return error.ShortDecode;
    return .{ .locations = locations.items, .chunk_offset = start, .dictionary_offset = dict };
}

pub const TestFixture = struct { bytes: []const u8, meta: schema.FileMetaData };

/// One group with independent page cuts, compressed dictionaries and NULLs; no serialized page index.
pub fn testFixture(a: std.mem.Allocator) !TestFixture {
    const encoder = @import("writer/encoder.zig");
    const n = 4096;
    var meta: schema.FileMetaData = .{
        .version = 1,
        .schema = .empty,
        .num_rows = n,
        .created_by = null,
        .row_groups = .empty,
    };
    const root: schema.SchemaElement = .{
        .name = "schema",
        .type = null,
        .type_length = null,
        .repetition_type = null,
        .num_children = 3,
        .scale = null,
        .precision = null,
        .field_id = null,
    };
    try meta.schema.append(a, root);
    var rg: schema.RowGroup = .{ .columns = .empty, .total_byte_size = 0, .num_rows = n };
    var bytes: std.ArrayList(u8) = .empty;
    for ([_][]const u8{ "k", "s", "v" }, 0..) |name, ci| {
        var se = root;
        se.name = name;
        se.num_children = null;
        se.type = if (ci == 1) .BYTE_ARRAY else .INT64;
        se.repetition_type = .OPTIONAL;
        try meta.schema.append(a, se);
        const defs = try a.alloc(u32, n);
        const ints = try a.alloc(i64, n);
        const strings = try a.alloc([]const u8, n);
        for (defs, ints, strings, 0..) |*d, *v, *s, i| {
            d.* = @intFromBool((i + ci) % 17 != 0);
            v.* = @intCast(if (ci == 0) i % 37 else i);
            s.* = try std.fmt.allocPrint(a, "dictionary-string-{d}", .{i % 23});
        }
        const encoded = try encoder.encodeColumn(a, .{
            .schema_elem = &se,
            .path_in_schema = &.{name},
            .page_rows = @as(usize, 53) + ci * 44,
            .dictionary_bloom = false,
            .values = if (ci == 1) .{ .string = .{ .values = strings, .def_levels = defs, .max_def = 1, .has_nulls = true } } else .{ .i64 = .{ .values = ints, .def_levels = defs, .max_def = 1, .has_nulls = true } },
        });
        var cm = encoded.meta;
        cm.data_page_offset += @intCast(bytes.items.len);
        if (cm.dictionary_page_offset) |off| cm.dictionary_page_offset = off + @as(i64, @intCast(bytes.items.len));
        try rg.columns.append(a, .{ .file_path = null, .file_offset = 0, .meta_data = cm });
        rg.total_byte_size += cm.total_uncompressed_size;
        try bytes.appendSlice(a, encoded.bytes);
    }
    try meta.row_groups.append(a, rg);
    return .{ .bytes = bytes.items, .meta = meta };
}

test "row subgroups walk misaligned pages and retain one compressed dictionary per column" {
    const t = std.testing;
    var arena = std.heap.ArenaAllocator.init(t.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const f = try testFixture(a);
    const rg = &f.meta.row_groups.items[0];
    const group = (try Group.init(a, f.bytes, rg, &f.meta, &.{ true, true, true }, 8)).?;
    try t.expect(group.ranges.len > 2);
    var end: usize = 0;
    for (group.ranges) |r| {
        try t.expectEqual(end, r.lo);
        try t.expect(r.hi > r.lo);
        end = r.hi;
    }
    try t.expectEqual(@as(usize, 4096), end);
    try t.expect(group.columns[0].?.locations[1].first_row_index != group.columns[1].?.locations[1].first_row_index);
    try t.expect(group.columns[1].?.dictionary != null);
    try t.expectEqual(@as(usize, 23), group.columns[1].?.dictionary.?.bytes.len);
    var nested = try a.dupe(schema.ColumnChunk, rg.columns.items);
    nested[0].meta_data.?.path_in_schema.items = try a.dupe([]const u8, &.{ "nested", "k" });
    var nested_rg = rg.*;
    nested_rg.columns.items = nested;
    try t.expectEqual(@as(?Group, null), try Group.init(a, f.bytes, &nested_rg, &f.meta, &.{ true, true, true }, 8));
}
