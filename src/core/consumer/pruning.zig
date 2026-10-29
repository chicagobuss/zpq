const std = @import("std");
const schema = @import("../schema.zig");
const filter_ast = @import("../filter/ast.zig");
const filter_prune = @import("../filter/prune.zig");
const filter_selection = @import("../filter/selection.zig");
const agg_plan = @import("../agg_plan.zig");
const thrift = @import("../thrift.zig");
const statistics = @import("../parquet/statistics.zig");
const decimal_mod = @import("../parquet/decimal.zig");
const AggRowGroup = @import("types.zig").AggRowGroup;
const AggTarget = @import("target.zig").AggTarget;
const column = @import("column.zig");
const originSlice = column.originSlice;
const PruningInfo = column.PruningInfo;
const RowRange = column.RowRange;

const ConjunctiveLeaf = struct {
    filter: filter_ast.Filter,
    col_idx: usize,
};

fn collectConjunctiveLeaves(f: filter_ast.Filter, list: *std.ArrayList(ConjunctiveLeaf), allocator: std.mem.Allocator) !void {
    switch (f) {
        // A NULL-member AND passes no row; what its leaves prove of pages says nothing of that.
        .and_filter => |c| if (!c.null_member) for (c.children) |child| try collectConjunctiveLeaves(child, list, allocator),
        .or_filter => {},
        else => {
            const col_idx = switch (f) {
                .int32 => |l| l.col_idx,
                .int64 => |l| l.col_idx,
                .uint64 => |l| l.col_idx,
                .float => |l| l.col_idx,
                .double => |l| l.col_idx,
                .decimal => |l| l.col_idx,
                .string => |l| l.col_idx,
                .boolean => |l| l.col_idx,
                .null_check => |l| l.col_idx,
                .like => |l| l.col_idx,
                else => unreachable,
            };
            try list.append(allocator, .{ .filter = f, .col_idx = col_idx });
        },
    }
}

pub const PageIndex = struct {
    col_index: schema.ColumnIndex,
    offset_index: schema.OffsetIndex,
};

/// The page indexes of the columns to decode, where present and consistent with the footer; none under
/// `--scan-all`. An unparseable or inconsistent index is ignored and its column decoded whole.
pub fn loadPageIndexes(ra: std.mem.Allocator, in: AggRowGroup, stats: agg_plan.StatsUse) ![]?PageIndex {
    const num_leaves = in.rg.columns.items.len;
    const indexes = try ra.alloc(?PageIndex, num_leaves);
    @memset(indexes, null);
    if (stats == .ignore) return indexes;

    for (in.read, 0..) |needed, ci| {
        if (!needed) continue;
        if (ci >= num_leaves) continue;
        const chunk_meta = in.rg.columns.items[ci];

        const co = chunk_meta.column_index_offset orelse continue;
        const cl = chunk_meta.column_index_length orelse continue;
        const oo = chunk_meta.offset_index_offset orelse continue;
        const ol = chunk_meta.offset_index_length orelse continue;

        const col_bytes = originSlice(in.src, co, cl) orelse continue;
        const off_bytes = originSlice(in.src, oo, ol) orelse continue;

        var col_reader = thrift.Reader.init(col_bytes);
        const col_index = schema.ColumnIndex.read(ra, &col_reader) catch continue;

        var off_reader = thrift.Reader.init(off_bytes);
        const offset_index = schema.OffsetIndex.read(ra, &off_reader) catch continue;
        const cm = chunk_meta.meta_data orelse continue;
        const levels = in.meta.getColumnLevels(cm.path_in_schema.items);
        const elem = in.meta.getColumnSchema(cm.path_in_schema.items);
        if (!statistics.pageIndexWriterUsable(&col_index, cm, elem, in.meta.created_by)) continue;
        const trust: statistics.Trust = if (stats == .trust) .plausible else .corroborated;
        const rows = in.rg.num_rows;
        if (!statistics.pageIndexIsPlausible(&col_index, &offset_index, cm, elem, levels, rows, trust)) continue;

        indexes[ci] = .{ .col_index = col_index, .offset_index = offset_index };
    }
    return indexes;
}

/// Per-page verdicts of the outer filter's conjunctive leaves, for the columns they test. Null for a column no
/// leaf tests (or with no page index).
pub const FilterPages = struct {
    skipped: []?[]bool,
    always_match: []?[]bool,
};

/// Prune pages with the outer filter's conjunctive leaves and deselect the rows of every page a leaf skips.
pub fn pruneFilterPages(
    ra: std.mem.Allocator,
    in: AggRowGroup,
    page_indexes: []const ?PageIndex,
    stats: agg_plan.StatsUse,
    sel: *filter_selection.SelectionVector,
) !FilterPages {
    const num_leaves = in.rg.columns.items.len;
    const num_rows: usize = @intCast(in.rg.num_rows);
    const pages: FilterPages = .{
        .skipped = try ra.alloc(?[]bool, num_leaves),
        .always_match = try ra.alloc(?[]bool, num_leaves),
    };
    @memset(pages.skipped, null);
    @memset(pages.always_match, null);

    var conj_leaves: std.ArrayList(ConjunctiveLeaf) = .empty;
    if (in.filter) |f| {
        try collectConjunctiveLeaves(f, &conj_leaves, ra);
    }

    for (conj_leaves.items) |leaf| {
        const ci = leaf.col_idx;
        if (ci >= num_leaves) continue;
        if (page_indexes[ci]) |pi| {
            const locs = pi.offset_index.page_locations.items;
            const skipped = pages.skipped[ci] orelse blk: {
                const s = try ra.alloc(bool, locs.len);
                @memset(s, false);
                break :blk s;
            };
            const always = pages.always_match[ci] orelse blk: {
                const a = try ra.alloc(bool, locs.len);
                @memset(a, false);
                break :blk a;
            };
            const is_first = pages.skipped[ci] == null;

            var ci_list = try ra.alloc(?schema.ColumnIndex, num_leaves);
            @memset(ci_list, null);
            for (page_indexes, 0..) |p_idx, idx| {
                if (p_idx) |p| ci_list[idx] = p.col_index;
            }

            for (locs, 0..) |loc, page_idx| {
                const dec = filter_prune.prunePage(in.rg, page_idx, leaf.filter, ci_list, in.meta, stats == .trust);
                const start: usize = @intCast(loc.first_row_index);
                const end: usize = if (page_idx + 1 < locs.len) @intCast(locs[page_idx + 1].first_row_index) else num_rows;

                if (dec == .skip) {
                    skipped[page_idx] = true;
                    sel.clearRange(start, end);
                }

                const is_always = dec == .always_match;
                if (is_first) {
                    always[page_idx] = is_always;
                } else {
                    always[page_idx] = always[page_idx] and is_always;
                }
            }

            pages.skipped[ci] = skipped;
            pages.always_match[ci] = always;
        }
    }
    return pages;
}

/// How to decode column `ci` page by page. A column no filter leaf tests skips just the pages left with no active
/// row. The always-match shortcut in `decodeWithReaderPruned` fills a page with the ColumnIndex minimum instead of
/// decoding it, which is only sound when the filter's mandatory conjuncts are the sole readers of the values: never
/// for a `preserve`d column. In `c >= 0 AND (c = 2 OR b = 1)` a page of c=[1,2] proves `c >= 0`, and filling it with 1
/// would fail the row c=2, b=0 that passes.
pub fn pagePruning(
    ra: std.mem.Allocator,
    pi: PageIndex,
    filter_pages: FilterPages,
    ci: usize,
    preserve: bool,
    sel: *const filter_selection.SelectionVector,
    num_rows: usize,
    cm: schema.ColumnMetaData,
    chunk_start: usize,
) !PruningInfo {
    const locs = pi.offset_index.page_locations.items;
    var skipped = filter_pages.skipped[ci];
    var always = filter_pages.always_match[ci];

    if (skipped == null) {
        const s = try ra.alloc(bool, locs.len);
        @memset(s, false);
        const a = try ra.alloc(bool, locs.len);
        @memset(a, false);

        for (locs, 0..) |loc, page_idx| {
            const start_idx: usize = @intCast(loc.first_row_index);
            const end_idx: usize = if (page_idx + 1 < locs.len) @intCast(locs[page_idx + 1].first_row_index) else num_rows;

            if (!sel.anyInRange(start_idx, end_idx)) s[page_idx] = true;
        }
        skipped = s;
        always = a;
    }
    if (preserve) {
        const a = try ra.alloc(bool, locs.len);
        @memset(a, false);
        always = a;
    }

    return .{
        .locations = locs,
        .page_is_skipped = skipped.?,
        .page_is_always_match = always.?,
        .dictionary_page_offset = cm.dictionary_page_offset,
        .chunk_file_offset = @intCast(chunk_start),
        .col_index = pi.col_index,
    };
}


/// The selected runs of a page-pruned row group, when decoding just them is worth it: a filter's page index left at
/// most half of the rows, and every column the row group decodes can be read page by page (a page index, one value per
/// row). Their rows, in order, then stand for the row group: page pruning already deselected the rest, and a compact
/// column keeps relative row order, so first appearances and ties are unchanged. Null decodes whole columns.
pub fn compactRows(
    ra: std.mem.Allocator,
    in: AggRowGroup,
    page_indexes: []const ?PageIndex,
    sel: *const filter_selection.SelectionVector,
    target: AggTarget,
    control: ?*@import("workspace.zig").CompactionControl,
) !?[]const RowRange {
    if (in.filter == null or (control != null and !control.?.enabled)) return null;
    if (target == .grouped and target.grouped.records != null) return null;
    const kept = sel.count();
    if (kept * 2 > sel.len) return null;
    for (in.read, 0..) |needed, ci| {
        if (!needed or ci >= in.rg.columns.items.len) continue;
        if (page_indexes[ci] == null) return null;
        const cm = in.rg.columns.items[ci].meta_data orelse return null;
        if (in.meta.getColumnLevels(cm.path_in_schema.items).max_rep != 0) return null;
        // Exact DECIMAL and INT96 decoders read whole chunks, not kept rows (`decodeAggColumn`).
        if (cm.type == .INT96) return null;
        if (in.meta.getColumnSchema(cm.path_in_schema.items)) |se| if (decimal_mod.exactKind(&se) != null) return null;
    }
    var ranges: std.ArrayList(RowRange) = .empty;
    var at: usize = 0;
    while (at < sel.len) {
        while (at < sel.len and !sel.isActive(at)) at += 1;
        const lo = at;
        while (at < sel.len and sel.isActive(at)) at += 1;
        if (at > lo) try ranges.append(ra, .{ .lo = lo, .hi = at });
    }
    if (control) |c| _ = c.row_groups.fetchAdd(1, .monotonic);
    return ranges.items;
}

