const std = @import("std");
const schema = @import("../core/schema.zig");

pub const CompactRange = struct {
    start: u64,
    end: u64,
    dst_start: usize,
};


/// Offset given to a chunk deliberately left unfetched. Far past any compact buffer, so a consumer that reads the chunk
/// anyway fails its bounds check (MissingChunkBytes) instead of decoding whatever bytes a stale offset lands on.
const UNFETCHED_CHUNK_OFFSET: i64 = std.math.maxInt(i64) / 4;

/// Point `meta`'s chunk offsets into the compact buffer `ranges` describe. `fetched` is every column some row group
/// fetched (`RangePlan.columns`) and `rg_columns` what each row group fetched (null where statistics pruned it).
/// `rg_compact`, when the buffer is windowed, gives each row group its own ranges,
/// `ranges[rg_compact[rg]..rg_compact[rg + 1]]`: its offsets rebase only onto those. Another row group's range may
/// cover the same source bytes (a range coalesced across a gap to a carried Bloom filter), but its segment is released
/// when that row group is done, not when this one is.
pub fn rebaseFetchedOffsets(
    meta: *schema.FileMetaData,
    fetched: []const bool,
    all_ranges: []const CompactRange,
    rg_columns: []const ?[]const bool,
    rg_compact: ?[]const usize,
) !void {
    std.debug.assert(rg_columns.len == meta.row_groups.items.len);
    if (rg_compact) |rc| std.debug.assert(rc.len == meta.row_groups.items.len + 1);
    // Page-index blocks are never fetched, so their offsets have nothing to rebase onto — yet consumers bounds-check
    // them against the compact buffer, not the file. In a noncanonical file a stale offset can land inside that buffer,
    // where unrelated bytes may decode as a ColumnIndex whose foreign min/max prune pages on read and get copied into
    // output on write. Pruned row groups are cleared too: the metadata no longer describes the source layout.
    for (meta.row_groups.items, 0..) |*rg, ri| {
        const ranges = if (rg_compact) |rc| all_ranges[rc[ri]..rc[ri + 1]] else all_ranges;
        for (rg.columns.items) |*chunk| {
            chunk.column_index_offset = null;
            chunk.column_index_length = null;
            chunk.offset_index_offset = null;
            chunk.offset_index_length = null;
            // A Bloom filter fetched for a byte copy moves into the buffer; any other no longer describes it. (Probing
            // is over: it read the filters through their source offsets before this.)
            if (chunk.meta_data) |*cm| {
                cm.bloom_filter_offset = rebaseContained(cm.bloom_filter_offset, cm.bloom_filter_length, ranges);
                if (cm.bloom_filter_offset == null) cm.bloom_filter_length = null;
            }
        }
    }

    for (meta.row_groups.items, rg_columns, 0..) |*rg, rg_cols_opt, ri| {
        const ranges = if (rg_compact) |rc| all_ranges[rc[ri]..rc[ri + 1]] else all_ranges;
        // Pruned row groups contributed no fetch ranges, so their chunk
        // offsets can't be rebased — and never need to be: the scan
        // workers re-prune with the same stats predicate before touching
        // any offset in these groups.
        const rg_cols = rg_cols_opt orelse continue;
        for (rg.columns.items, 0..) |*chunk, ci| {
            if (ci >= fetched.len or !fetched[ci]) continue;
            // Fetched elsewhere but not in this row group: the filter-only chunk of one proven fully matching.
            if (!rg_cols[ci]) {
                if (chunk.meta_data) |*cm| {
                    cm.data_page_offset = UNFETCHED_CHUNK_OFFSET;
                    cm.dictionary_page_offset = null;
                    cm.index_page_offset = null;
                }
                chunk.file_offset = UNFETCHED_CHUNK_OFFSET;
                continue;
            }
            if (chunk.meta_data) |*cm| {
                cm.data_page_offset = try rebaseOneOffset(cm.data_page_offset, ranges);
                if (cm.dictionary_page_offset) |d| {
                    cm.dictionary_page_offset = try rebaseOneOffset(d, ranges);
                }
                if (cm.index_page_offset) |d| {
                    cm.index_page_offset = rebaseOneOffset(d, ranges) catch null;
                }
            }
            if (chunk.meta_data) |cm| chunk.file_offset = cm.data_page_offset;
        }
    }
}

/// Where `[off, off + len)` of the source lands in the compact buffer, when one range holds all of it.
/// Rightmost range with `start <= src`, when `ranges` is sorted by start and does not overlap.
fn rangeAt(src: u64, ranges: []const CompactRange) ?CompactRange {
    var lo: usize = 0;
    var hi = ranges.len;
    while (lo < hi) {
        const mid = lo + (hi - lo) / 2;
        if (ranges[mid].start <= src) lo = mid + 1 else hi = mid;
    }
    if (lo == 0) return null;
    return ranges[lo - 1];
}

fn rebaseContained(off_opt: ?i64, len_opt: ?i32, ranges: []const CompactRange) ?i64 {
    const off = std.math.cast(u64, off_opt orelse return null) orelse return null;
    const len = std.math.cast(u64, len_opt orelse return null) orelse return null;
    const r = rangeAt(off, ranges) orelse return null;
    if (off >= r.start and off + len <= r.end) return @intCast(@as(u64, @intCast(r.dst_start)) + (off - r.start));
    return null;
}

fn rebaseOneOffset(old: i64, ranges: []const CompactRange) !i64 {
    if (old < 0) return error.BadResponse;
    const src: u64 = @intCast(old);
    const r = rangeAt(src, ranges) orelse return error.BadResponse;
    if (src >= r.start and src < r.end) {
        const dst: u64 = @as(u64, @intCast(r.dst_start)) + (src - r.start);
        return @intCast(dst);
    }
    return error.BadResponse;
}


pub fn pageIndexTestChunk(data_page_offset: i64) schema.ColumnChunk {
    return .{
        .file_path = null,
        .file_offset = data_page_offset,
        .meta_data = .{
            .type = .INT32,
            .encodings = .empty,
            .path_in_schema = .empty,
            .codec = .UNCOMPRESSED,
            .num_values = 0,
            .total_uncompressed_size = 0,
            .total_compressed_size = 0,
            .data_page_offset = data_page_offset,
            .index_page_offset = null,
            .dictionary_page_offset = null,
            .statistics = null,
        },
        .column_index_offset = 32,
        .column_index_length = 16,
        .offset_index_offset = 96,
        .offset_index_length = 16,
    };
}

test "rebaseFetchedOffsets clears page-index pointers that would resolve inside the compact buffer" {
    const gpa = std.testing.allocator;
    var arena_state = std.heap.ArenaAllocator.init(gpa);
    defer arena_state.deinit();
    const arena = arena_state.allocator();

    // The index pointers deliberately fall *inside* the implied 128-byte compact buffer, modelling the noncanonical
    // file this clearing exists for; a conforming writer's offsets would fail bounds anyway.
    var meta: schema.FileMetaData = .{
        .version = 1,
        .schema = .empty,
        .num_rows = 0,
        .created_by = null,
        .row_groups = .empty,
    };
    for ([_]i64{ 1_000_000, 2_000_000 }) |dpo| {
        var rg: schema.RowGroup = .{
            .columns = .empty,
            .total_byte_size = 0,
            .num_rows = 0,
        };
        const chunk = pageIndexTestChunk(dpo);
        try rg.columns.append(arena, chunk);
        try meta.row_groups.append(arena, rg);
    }

    const ranges = [_]CompactRange{
        .{ .start = 1_000_000, .end = 1_000_064, .dst_start = 0 },
        .{ .start = 2_000_000, .end = 2_000_064, .dst_start = 64 },
    };
    const fetch_arr = [_]bool{true};
    const rg_columns = [_]?[]const bool{ &fetch_arr, &fetch_arr };

    try rebaseFetchedOffsets(&meta, &fetch_arr, &ranges, &rg_columns, null);

    for (meta.row_groups.items, 0..) |rg, i| {
        const chunk = rg.columns.items[0];
        try std.testing.expectEqual(
            @as(i64, @intCast(i * 64)),
            chunk.meta_data.?.data_page_offset,
        );
        try std.testing.expectEqual(@as(?i64, null), chunk.column_index_offset);
        try std.testing.expectEqual(@as(?i32, null), chunk.column_index_length);
        try std.testing.expectEqual(@as(?i64, null), chunk.offset_index_offset);
        try std.testing.expectEqual(@as(?i32, null), chunk.offset_index_length);
    }
}

test "rebaseFetchedOffsets clears page-index pointers in pruned row groups too" {
    const gpa = std.testing.allocator;
    var arena_state = std.heap.ArenaAllocator.init(gpa);
    defer arena_state.deinit();
    const arena = arena_state.allocator();

    // A pruned row group is skipped by the rebase loop, so clearing that rode along with rebasing would leave its
    // stale pointers.
    var meta: schema.FileMetaData = .{
        .version = 1,
        .schema = .empty,
        .num_rows = 0,
        .created_by = null,
        .row_groups = .empty,
    };
    var rg: schema.RowGroup = .{
        .columns = .empty,
        .total_byte_size = 0,
        .num_rows = 0,
    };
    var chunk = pageIndexTestChunk(9_000_000);
    chunk.column_index_offset = 128;
    chunk.column_index_length = 32;
    chunk.offset_index_offset = 160;
    try rg.columns.append(arena, chunk);
    try meta.row_groups.append(arena, rg);

    const ranges = [_]CompactRange{};
    const fetch_arr = [_]bool{true};
    const rg_columns = [_]?[]const bool{null};

    try rebaseFetchedOffsets(&meta, &fetch_arr, &ranges, &rg_columns, null);

    const out = meta.row_groups.items[0].columns.items[0];
    try std.testing.expectEqual(@as(?i64, null), out.column_index_offset);
    try std.testing.expectEqual(@as(?i64, null), out.offset_index_offset);
    try std.testing.expectEqual(@as(i64, 9_000_000), out.meta_data.?.data_page_offset);
}

test "rebaseFetchedOffsets poisons filter-only chunks it left unfetched in proven row groups" {
    const gpa = std.testing.allocator;
    var arena_state = std.heap.ArenaAllocator.init(gpa);
    defer arena_state.deinit();
    const arena = arena_state.allocator();

    // Two row groups x two columns (0 = filter-only, 1 = aggregate input). Only row group 1 is proven, so its column-0
    // chunk was never fetched: a rebase would fail on it, and a stale source offset could land inside the compact
    // buffer and decode unrelated bytes.
    var meta: schema.FileMetaData = .{
        .version = 1,
        .schema = .empty,
        .num_rows = 0,
        .created_by = null,
        .row_groups = .empty,
    };
    for ([_][2]i64{ .{ 1_000, 2_000 }, .{ 3_000, 4_000 } }) |offs| {
        var rg: schema.RowGroup = .{ .columns = .empty, .total_byte_size = 0, .num_rows = 0 };
        for (offs) |o| try rg.columns.append(arena, pageIndexTestChunk(o));
        try meta.row_groups.append(arena, rg);
    }
    const ranges = [_]CompactRange{
        .{ .start = 1_000, .end = 1_064, .dst_start = 0 },
        .{ .start = 2_000, .end = 2_064, .dst_start = 64 },
        .{ .start = 4_000, .end = 4_064, .dst_start = 128 },
    };
    const fetch_arr = [_]bool{ true, true };
    const consumed = [_]bool{ false, true };
    const rg_columns = [_]?[]const bool{ &fetch_arr, &consumed };

    try rebaseFetchedOffsets(&meta, &fetch_arr, &ranges, &rg_columns, null);

    const rg0 = meta.row_groups.items[0].columns.items;
    try std.testing.expectEqual(@as(i64, 0), rg0[0].meta_data.?.data_page_offset);
    try std.testing.expectEqual(@as(i64, 64), rg0[1].meta_data.?.data_page_offset);
    const rg1 = meta.row_groups.items[1].columns.items;
    try std.testing.expectEqual(UNFETCHED_CHUNK_OFFSET, rg1[0].meta_data.?.data_page_offset);
    try std.testing.expectEqual(UNFETCHED_CHUNK_OFFSET, rg1[0].file_offset);
    try std.testing.expectEqual(@as(i64, 128), rg1[1].meta_data.?.data_page_offset);
}



test "rebaseFetchedOffsets moves a fetched Bloom filter into the compact buffer and clears any other" {
    const gpa = std.testing.allocator;
    var arena_state = std.heap.ArenaAllocator.init(gpa);
    defer arena_state.deinit();
    const arena = arena_state.allocator();
    var meta: schema.FileMetaData = .{
        .version = 1,
        .schema = .empty,
        .num_rows = 0,
        .created_by = null,
        .row_groups = .empty,
    };
    // Row group 0's filter was fetched whole; row group 1's was not; row group 2's half was, and it is pruned.
    for ([_]i64{ 1_000_000, 2_000_000, 3_000_000 }, [_]i64{ 9_000_016, 9_500_000, 9_000_040 }) |dpo, bfo| {
        var rg: schema.RowGroup = .{ .columns = .empty, .total_byte_size = 0, .num_rows = 0 };
        var chunk = pageIndexTestChunk(dpo);
        chunk.meta_data.?.bloom_filter_offset = bfo;
        chunk.meta_data.?.bloom_filter_length = 16;
        try rg.columns.append(arena, chunk);
        try meta.row_groups.append(arena, rg);
    }
    const ranges = [_]CompactRange{
        .{ .start = 1_000_000, .end = 1_000_064, .dst_start = 0 },
        .{ .start = 2_000_000, .end = 2_000_064, .dst_start = 64 },
        .{ .start = 9_000_000, .end = 9_000_048, .dst_start = 128 },
    };
    const fetch_arr = [_]bool{true};
    const rg_columns = [_]?[]const bool{ &fetch_arr, &fetch_arr, null };
    try rebaseFetchedOffsets(&meta, &fetch_arr, &ranges, &rg_columns, null);
    const got = [_]?i64{ 144, null, null };
    for (meta.row_groups.items, got) |rg, want| {
        const cm = rg.columns.items[0].meta_data.?;
        try std.testing.expectEqual(want, cm.bloom_filter_offset);
        try std.testing.expectEqual(@as(?i32, if (want != null) 16 else null), cm.bloom_filter_length);
    }
}

