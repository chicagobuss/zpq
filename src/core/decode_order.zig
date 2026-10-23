//! Decode-error ordering, evaluated only after a decoder has failed. Pages are the failing units: their first
//! logical row wins, then the projected column's position. Each page keeps the decoder's established checks.
const std = @import("std");
const schema = @import("schema.zig");
const thrift = @import("thrift.zig");
const column = @import("parquet/column.zig");

pub const Site = struct {
    rg: *const schema.RowGroup,
    column: usize,
    page: ?u32 = null,
    first_row: usize = 0,
    projection: usize = 0,
};

pub const Failure = struct { err: anyerror, site: Site };

pub fn less(a: Site, b: Site) bool {
    if (a.first_row != b.first_row) return a.first_row < b.first_row;
    if (a.projection != b.projection) return a.projection < b.projection;
    return a.column < b.column;
}

/// Recheck the failing column first to recover its unit even when a resumed slice lost its diagnostic marker.
/// Then check competing columns only through units starting at or before that row. No state reaches the query.
/// Reader is the ordinary sliced decoder, so conversion and within-page validation remain the same as execution.
pub fn canonical(
    comptime Reader: type,
    comptime isDecodeError: fn (anyerror) bool,
    gpa: std.mem.Allocator,
    in: anytype,
    options: column.DecodeOptions,
    levels_only: []const bool,
    original: Failure,
) !Failure {
    var best = original;
    if (try checkColumn(
        Reader,
        isDecodeError,
        gpa,
        in,
        options,
        levels_only,
        original.site.column,
        std.math.maxInt(usize),
    )) |found| best = found;
    for (in.read, 0..) |needed, ci| {
        if (!needed or ci == original.site.column) continue;
        if (try checkColumn(Reader, isDecodeError, gpa, in, options, levels_only, ci, best.site.first_row)) |found| {
            if (less(found.site, best.site)) best = found;
        }
    }
    return best;
}

fn checkColumn(
    comptime Reader: type,
    comptime isDecodeError: fn (anyerror) bool,
    gpa: std.mem.Allocator,
    in: anytype,
    options: column.DecodeOptions,
    levels_only: []const bool,
    ci: usize,
    through_row: usize,
) !?Failure {
    var held = std.heap.ArenaAllocator.init(gpa);
    defer held.deinit();
    var reader = Reader.init(held.allocator(), gpa, in.rg, in.meta, in.src, ci, options) catch |err| {
        if (!isDecodeError(err)) return err;
        return Failure{ .err = err, .site = makeSite(in, ci, null) };
    } orelse return null; // Repeated aggregate columns are refused before scanning.
    defer reader.deinit();
    if (ci < levels_only.len and levels_only[ci]) reader.levelsOnly();
    var window = std.heap.ArenaAllocator.init(gpa);
    defer window.deinit();
    var done: usize = 0;
    while (reader.left != 0) {
        const page_left = reader.pageLeft();
        // Finish a page that started before the bound, including value faults beyond the bound within that page.
        if (page_left == 0 and done > through_row) break;
        const n = if (page_left == 0) 1 else @min(64, page_left);
        _ = window.reset(.retain_capacity);
        _ = reader.next(window.allocator(), n) catch |err| {
            if (!isDecodeError(err)) return err;
            return Failure{ .err = err, .site = makeSite(in, ci, reader.pageOrdinal()) };
        };
        done += n;
    }
    return null;
}

fn makeSite(in: anytype, ci: usize, page: ?u32) Site {
    return .{
        .rg = in.rg,
        .column = ci,
        .page = page,
        .first_row = firstRow(in.rg, in.src, ci, page),
        .projection = if (in.error_order) |order| order[ci] else ci,
    };
}

/// Page starts come from the preceding flat data-page counts, never from column statistics or cache batches.
/// A dictionary, header or missing-chunk failure before any data belongs to row zero.
fn firstRow(rg: *const schema.RowGroup, src: anytype, ci: usize, wanted: ?u32) usize {
    const target = wanted orelse return 0;
    const cm = rg.columns.items[ci].meta_data orelse return 0;
    const start: usize = @intCast(cm.dictionary_page_offset orelse cm.data_page_offset);
    const origin: usize = @intCast(src.byte_origin);
    if (start < origin) return 0;
    const offset = start - origin;
    const size: usize = @intCast(cm.total_compressed_size);
    if (offset > src.bytes.len or size > src.bytes.len - offset) return 0;
    const bytes = src.bytes[offset..][0..size];
    var pos: usize = 0;
    var ordinal: u32 = 0;
    var row: usize = 0;
    while (ordinal < target and pos < bytes.len) : (ordinal += 1) {
        var parser = thrift.Reader.init(bytes[pos..]);
        const h = schema.PageHeader.read(&parser) catch return row;
        if (h.compressed_page_size < 0) return row;
        const payload: usize = @intCast(h.compressed_page_size);
        if (parser.pos > bytes.len - pos or payload > bytes.len - pos - parser.pos) return row;
        pos += parser.pos + payload;
        const count = switch (h.type) {
            .DATA_PAGE => if (h.data_page_header) |d| d.num_values else 0,
            .DATA_PAGE_V2 => if (h.data_page_header_v2) |d| d.num_values else 0,
            else => 0,
        };
        row +|= @intCast(@max(0, count));
    }
    return row;
}

test "decode units order by first row then projection, independently of page ordinal" {
    const rg: schema.RowGroup = undefined;
    const a: Site = .{ .rg = &rg, .column = 7, .page = 3, .first_row = 100, .projection = 0 };
    var b = a;
    b.column = 0;
    b.page = 1;
    b.first_row = 101;
    try std.testing.expect(less(a, b));
    b.first_row = 100;
    b.projection = 1;
    try std.testing.expect(less(a, b));
    b.projection = 0;
    try std.testing.expect(less(b, a));
}
