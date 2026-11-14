//! Whole-source-group compaction, parquet-format 2.14.0. Only dictionary index payloads are reencoded.
const std = @import("std");
const schema = @import("../schema.zig");
const thrift = @import("../thrift.zig");
const fast = @import("fastpath.zig");
const enc = @import("encoder.zig");
const index = @import("page_index.zig");
const compression = @import("../parquet/compression.zig");
const rle = @import("../parquet/encoding/hybrid_rle.zig");
const bloom = @import("../parquet/bloom.zig");
const patch = @import("thrift_patch.zig");
pub const Target = struct {
    rows: u64 = 131072,
    /// Encoded uncompressed pages plus headers; the reencoder counts PLAIN value bytes instead.
    encoded_uncompressed_bytes: u64 = 64 << 20,
    pub fn reached(t: Target, rows: u64, bytes: u64) bool {
        return rows >= t.rows or bytes >= t.encoded_uncompressed_bytes;
    }
};
pub const Source = struct {
    bytes: []const u8,
    meta: *const schema.FileMetaData,
    rg: *const schema.RowGroup,
    fd: ?std.posix.fd_t = null,
    file_offsets: bool = false,
    pub fn ownProjected(s: Source, a: std.mem.Allocator, kept: ?[]const bool) !Source {
        var start: usize = s.bytes.len;
        var end: usize = 0;
        for (s.rg.columns.items, 0..) |c, ci| {
            if (kept) |k| if (!k[ci]) continue;
            const range = fast.chunkRange(&(c.meta_data orelse return error.InvalidColumnOffsets), s.bytes.len) orelse return error.InvalidColumnOffsets;
            start = @min(start, range.start);
            end = @max(end, range.start + range.len);
        }
        if (end < start) {
            start = 0;
            end = 0;
        }
        const rg = try a.create(schema.RowGroup);
        rg.* = s.rg.*;
        rg.columns = .empty;
        for (s.rg.columns.items, 0..) |*c, ci| {
            if (kept) |k| if (!k[ci]) {
                try rg.columns.append(a, c.*);
                continue;
            };
            var moved = fast.rebaseChunk(a, s.bytes, c, -@as(i64, @intCast(start)));
            moved.bloom_out = try a.dupe(u8, moved.bloom_out);
            try rg.columns.append(a, moved);
        }
        return .{ .bytes = try a.dupe(u8, s.bytes[start..end]), .meta = s.meta, .rg = rg };
    }
};
pub const Options = struct {
    dictionary_limit: ?usize = null,
    filter_fpp: []const ?f64 = &.{},
};
const Page = struct {
    source: usize,
    offset: usize,
    size: usize,
    header_len: usize,
    header: schema.PageHeader,
    first_row: u64,
    rows: u64,
    stats: ?schema.Statistics,
};
const Column = struct {
    ci: usize,
    elem: schema.SchemaElement,
    maps: [][]const u32,
    pages: std.ArrayList(Page) = .empty,
    dictionary: std.ArrayList(u8) = .empty,
    entries: std.ArrayList([]const u8) = .empty,
    has_dictionary: bool = false,
    all_dictionary: bool = true,
    statistics: ?schema.Statistics = null,
    filter: []const u8 = &.{},
};
pub const Plan = struct {
    sources: []const Source,
    columns: []Column,
    rows: u64,
    /// Null ends a run early; all admission precedes output. Callers retain whole-group copy for a single source.
    pub fn init(a: std.mem.Allocator, sources: []const Source, kept: ?[]const usize, opts: Options) !?Plan {
        if (sources.len < 2) return null;
        const first = sources[0];
        const columns = try a.alloc(Column, if (kept) |k| k.len else first.rg.columns.items.len);
        var rows: u64 = 0;
        for (sources) |s| {
            if (s.rg.num_rows < 0 or s.rg.columns.items.len != first.rg.columns.items.len) return null;
            rows += @intCast(s.rg.num_rows);
        }
        for (columns, 0..) |*col, oi| {
            const ci = if (kept) |k| k[oi] else oi;
            const cm0 = first.rg.columns.items[ci].meta_data orelse return null;
            const elem = first.meta.getColumnSchema(cm0.path_in_schema.items) orelse return null;
            if (schema.isFloat16(elem)) return null;
            col.* = .{ .ci = ci, .elem = elem, .maps = try a.alloc([]const u32, sources.len) };
            @memset(col.maps, &.{});
            var intern: std.StringHashMap(u32) = .init(a);
            defer intern.deinit();
            // Page headers give exact upper bounds before decompression. Reserve once: growing and rehashing the
            // dictionary table dominated the sampled CPU time, while retired arena buffers stayed live as well.
            var entry_bound: usize = 0;
            var byte_bound: usize = 0;
            for (sources) |s| {
                const cm = s.rg.columns.items[ci].meta_data orelse return null;
                const range = fast.chunkRange(&cm, s.bytes.len) orelse return error.InvalidColumnOffsets;
                if (range.len == 0) return null;
                var r = thrift.Reader.init(s.bytes[range.start..][0..range.len]);
                const h = try schema.PageHeader.read(&r);
                if (h.type != .DICTIONARY_PAGE) continue;
                switch (cm.codec) {
                    .UNCOMPRESSED, .SNAPPY, .GZIP, .ZSTD, .LZ4_RAW => {},
                    else => return null,
                }
                const dh = h.dictionary_page_header orelse return error.BadPageHeader;
                if (dh.num_values < 0 or h.uncompressed_page_size < 0 or h.compressed_page_size < 0)
                    return error.BadPageHeader;
                if (@as(usize, @intCast(h.compressed_page_size)) > range.len - r.pos) return error.TruncatedPage;
                entry_bound = try std.math.add(usize, entry_bound, @intCast(dh.num_values));
                byte_bound = try std.math.add(usize, byte_bound, @intCast(h.uncompressed_page_size));
            }
            const numeric = cm0.type == .INT32 or cm0.type == .INT64 or cm0.type == .FLOAT or cm0.type == .DOUBLE or
                (cm0.type == .FIXED_LEN_BYTE_ARRAY and elem.type_length != null and elem.type_length.? > 0 and elem.type_length.? <= 8);
            var fixed: ?enc.Interner(u64) = null;
            var general: ?enc.MapInterner(u64) = null;
            if (numeric and entry_bound > 0) fixed = try enc.Interner(u64).init(a, entry_bound) else try intern.ensureTotalCapacity(std.math.cast(u32, entry_bound) orelse return null);
            try col.entries.ensureTotalCapacity(a, entry_bound);
            try col.dictionary.ensureTotalCapacity(a, byte_bound);
            var first_row: u64 = 0;
            var stats_complete = true;
            var filter_bits: ?[]u8 = null;
            var filters_complete = true;
            var has_filter = false;
            for (sources, 0..) |s, si| {
                const chunk = &s.rg.columns.items[ci];
                const cm = chunk.meta_data orelse return null;
                const se = s.meta.getColumnSchema(cm.path_in_schema.items) orelse return null;
                const levels = s.meta.getColumnLevels(cm.path_in_schema.items);
                if (chunk.encrypted or chunk.file_path != null or levels.max_rep != 0 or
                    cm.num_values != s.rg.num_rows or cm.type != cm0.type or cm.codec != cm0.codec or
                    fast.writerPolicy(chunk, s.meta) != .trust or !compatible(elem, se)) return null;
                if (cm.statistics) |st| {
                    if (si == 0) col.statistics = st else if (col.statistics) |*acc| mergeStats(acc, st, &elem);
                } else stats_complete = false;
                const filter = if (chunk.bloom_out.len > 0) chunk.bloom_out else fast.carriedFilter(s.bytes, chunk);
                if (bloom.bitset(filter)) |bits| {
                    has_filter = true;
                    if (filter_bits) |dst| {
                        if (dst.len == bits.len) {
                            for (dst, bits) |*d, b| d.* |= b;
                        } else filters_complete = false;
                    } else filter_bits = try a.dupe(u8, bits);
                } else filters_complete = false;
                var ix: ?schema.ColumnIndex = null;
                const raw_ix = if (chunk.column_index_out.len > 0) chunk.column_index_out else indexed(s.bytes, chunk.column_index_offset, chunk.column_index_length) orelse &.{};
                if (raw_ix.len > 0) {
                    var ir = thrift.Reader.init(raw_ix);
                    ix = schema.ColumnIndex.read(a, &ir) catch null;
                }
                const range = fast.chunkRange(&cm, s.bytes.len) orelse return error.InvalidColumnOffsets;
                var pos = range.start;
                var row: u64 = 0;
                var pi: usize = 0;
                var seen_dict = false;
                while (pos < range.start + range.len) {
                    var r = thrift.Reader.init(s.bytes[pos..][0 .. range.start + range.len - pos]);
                    const h = try schema.PageHeader.read(&r);
                    if (h.compressed_page_size < 0 or h.uncompressed_page_size < 0) return error.BadPageHeader;
                    const len = r.pos + @as(usize, @intCast(h.compressed_page_size));
                    if (len > range.start + range.len - pos) return error.TruncatedPage;
                    if (h.type == .DICTIONARY_PAGE) {
                        if (seen_dict or pos != range.start) return error.BadPageHeader;
                        seen_dict = true;
                        col.has_dictionary = true;
                        const dh = h.dictionary_page_header orelse return error.BadPageHeader;
                        if (dh.num_values < 0 or dh.encoding != .PLAIN or cm.type == .BOOLEAN) return null;
                        const plain = try compression.decompress(a, s.bytes[pos + r.pos ..][0 .. len - r.pos], cm.codec, @intCast(h.uncompressed_page_size));
                        const map = try a.alloc(u32, @intCast(dh.num_values));
                        var at: usize = 0;
                        for (map) |*mapped| {
                            const entry = try dictionaryEntry(plain, &at, &elem);
                            var id: u32 = undefined;
                            var fresh: bool = undefined;
                            if (fixed) |*lookup| {
                                const key = numericKey(entry);
                                const slot = if (general) |*g| g.find(key).? else blk: {
                                    if (lookup.find(key)) |slot| break :blk slot;
                                    // The encoder's bounded-probe interner has the same adversarial fallback here.
                                    general = try enc.MapInterner(u64).init(a, entry_bound);
                                    for (col.entries.items, 0..) |old, i| general.?.find(numericKey(old)).?.* = @intCast(i + 1);
                                    break :blk general.?.find(key).?;
                                };
                                fresh = slot.* == 0;
                                if (fresh) slot.* = std.math.cast(u32, col.entries.items.len + 1) orelse return null;
                                id = slot.* - 1;
                            } else {
                                const gop = intern.getOrPutAssumeCapacity(entry);
                                fresh = !gop.found_existing;
                                if (fresh) gop.value_ptr.* = std.math.cast(u32, col.entries.items.len) orelse return null;
                                id = gop.value_ptr.*;
                            }
                            if (fresh) {
                                col.entries.appendAssumeCapacity(entry);
                                col.dictionary.appendSliceAssumeCapacity(entry);
                            }
                            mapped.* = id;
                        }
                        if (at != plain.len) return error.BadPageHeader;
                        col.maps[si] = map;
                    } else if (h.type == .DATA_PAGE or h.type == .DATA_PAGE_V2) {
                        const nv = if (h.data_page_header) |d| d.num_values else (h.data_page_header_v2 orelse return error.BadPageHeader).num_values;
                        if (nv < 0) return error.BadPageHeader;
                        const count: u64 = @intCast(nv);
                        const encoding = if (h.data_page_header) |d| d.encoding else h.data_page_header_v2.?.encoding;
                        const coded = encoding == .RLE_DICTIONARY or encoding == .PLAIN_DICTIONARY;
                        if (coded and !seen_dict) return error.BadPageHeader;
                        if (coded and levels.max_def > 0) if (h.data_page_header) |dh| {
                            if (dh.definition_level_encoding != .RLE) return null;
                        };
                        col.all_dictionary = col.all_dictionary and coded;
                        var st: ?schema.Statistics = null;
                        const fields = try patch.fieldsOf(a, s.bytes[pos..][0..r.pos]);
                        if (patch.find(fields, if (h.type == .DATA_PAGE) 5 else 8)) |dh| {
                            const inner = try patch.fieldsOf(a, dh.raw);
                            if (patch.find(inner, if (h.type == .DATA_PAGE) 5 else 8)) |raw| {
                                var sr = thrift.Reader.init(raw.raw);
                                st = try schema.Statistics.read(&sr);
                            }
                        }
                        if (ix) |x| if (pi < x.null_pages.items.len and pi < x.min_values.items.len and pi < x.max_values.items.len) {
                            st = .{ .min_value = if (x.null_pages.items[pi]) null else x.min_values.items[pi], .max_value = if (x.null_pages.items[pi]) null else x.max_values.items[pi], .null_count = if (x.null_counts) |nc| if (pi < nc.items.len) nc.items[pi] else null else if (x.null_pages.items[pi]) @intCast(count) else null, .nan_count = if (x.nan_counts) |nc| if (pi < nc.items.len) nc.items[pi] else null else null };
                        };
                        try col.pages.append(a, .{ .source = si, .offset = pos, .size = len, .header_len = r.pos, .header = h, .first_row = first_row + row, .rows = count, .stats = st });
                        row += count;
                        pi += 1;
                    } else return null;
                    pos += len;
                }
                if (row != @as(u64, @intCast(s.rg.num_rows))) return error.BadPageHeader;
                first_row += row;
            }
            if (col.dictionary.items.len > (opts.dictionary_limit orelse enc.dictPageMaxBytes()) or col.entries.items.len > std.math.maxInt(i32)) return null;
            if (!stats_complete) col.statistics = null;
            if (col.statistics) |*st| {
                st.distinct_count = null;
                if (elem.type == .INT96 or elem.type == .FLOAT or elem.type == .DOUBLE) {
                    st.min = null;
                    st.max = null;
                    st.min_value = null;
                    st.max_value = null;
                }
            }
            if (has_filter and filters_complete) {
                var header: [bloom.max_header_bytes]u8 = undefined;
                col.filter = try std.mem.concat(a, u8, &.{ bloom.writeHeader(@intCast(filter_bits.?.len), &header), filter_bits.? });
            } else if (ci < opts.filter_fpp.len) {
                if (opts.filter_fpp[ci]) |fpp| {
                    if (col.has_dictionary and col.all_dictionary) {
                        var b = try bloom.Builder.init(a, col.entries.items.len, fpp);
                        defer b.deinit(a);
                        for (col.entries.items) |entry| b.insert(bloom.hashBytes(if (cm0.type == .BYTE_ARRAY) entry[4..] else entry));
                        col.filter = try b.serialize(a);
                    } else col.filter = try rebuildFilter(a, sources, ci, fpp);
                }
            }
        }
        return .{ .sources = sources, .columns = columns, .rows = rows };
    }
    pub fn write(p: Plan, a: std.mem.Allocator, out: anytype) !schema.RowGroup {
        var columns: std.ArrayList(schema.ColumnChunk) = .empty;
        var scratch = std.heap.ArenaAllocator.init(a);
        defer scratch.deinit();
        for (p.columns) |col| {
            const cm0 = p.sources[0].rg.columns.items[col.ci].meta_data.?;
            var chunk = fast.shiftChunk(&p.sources[0].rg.columns.items[col.ci], 0);
            const cm = &chunk.meta_data.?;
            cm.encodings = .empty;
            cm.encoding_stats = std.ArrayList(schema.PageEncodingStats).empty;
            cm.statistics = col.statistics;
            cm.num_values = @intCast(p.rows);
            cm.total_uncompressed_size = 0;
            cm.dictionary_page_offset = null;
            cm.index_page_offset = null;
            chunk.bloom_out = try a.dupe(u8, col.filter);
            for (p.sources) |s| for (s.rg.columns.items[col.ci].meta_data.?.encodings.items) |e| {
                if (std.mem.indexOfScalar(schema.Encoding, cm.encodings.items, e) == null) try cm.encodings.append(a, e);
            };
            const start = out.pos();
            if (col.has_dictionary) {
                _ = scratch.reset(.retain_capacity);
                const sa = scratch.allocator();
                const body = try compression.compress(sa, col.dictionary.items, cm0.codec);
                const h: schema.PageHeader = .{ .type = .DICTIONARY_PAGE, .compressed_page_size = std.math.cast(i32, body.len) orelse return error.TooLarge, .uncompressed_page_size = std.math.cast(i32, col.dictionary.items.len) orelse return error.TooLarge, .crc = null, .data_page_header = null, .data_page_header_v2 = null, .dictionary_page_header = .{ .num_values = @intCast(col.entries.items.len), .encoding = .PLAIN, .is_sorted = false } };
                cm.dictionary_page_offset = @intCast(out.pos());
                const hl = try writePage(sa, out, h, body);
                cm.total_uncompressed_size += @intCast(hl + col.dictionary.items.len);
                try countEncoding(a, cm, .DICTIONARY_PAGE, .PLAIN);
            }
            cm.data_page_offset = @intCast(out.pos());
            chunk.file_offset = cm.data_page_offset;
            var pages: std.ArrayList(enc.PageInfo) = .empty;
            for (col.pages.items) |pg| {
                _ = scratch.reset(.retain_capacity);
                const sa = scratch.allocator();
                const source = p.sources[pg.source];
                const original = source.bytes[pg.offset..][0..pg.size];
                var h = pg.header;
                const encoding = if (h.data_page_header) |d| d.encoding else h.data_page_header_v2.?.encoding;
                const coded = encoding == .RLE_DICTIONARY or encoding == .PLAIN_DICTIONARY;
                const at = out.pos();
                const map = col.maps[pg.source];
                const identity = for (map, 0..) |v, i| {
                    if (v != i) break false;
                } else true;
                if (!coded or identity) {
                    _ = try out.copyRange(source.fd, pg.offset, original);
                    cm.total_uncompressed_size += @intCast(pg.header_len + @as(usize, @intCast(h.uncompressed_page_size)));
                } else {
                    const rewritten = try remap(sa, source.meta, cm0, h, original[pg.header_len..], map, col.entries.items.len);
                    h.compressed_page_size = std.math.cast(i32, rewritten.body.len) orelse return error.TooLarge;
                    h.uncompressed_page_size = std.math.cast(i32, rewritten.uncompressed) orelse return error.TooLarge;
                    if (h.crc != null) h.crc = @bitCast(std.hash.Crc32.hash(rewritten.body));
                    var header = patch.Builder.init(sa);
                    try header.keep(try patch.fieldsOf(sa, original[0..pg.header_len]));
                    try header.putInt(2, i32, h.uncompressed_page_size);
                    try header.putInt(3, i32, h.compressed_page_size);
                    if (h.crc) |crc| try header.putInt(4, i32, crc);
                    const bytes = try header.encode();
                    try out.write(bytes);
                    try out.write(rewritten.body);
                    const hl = bytes.len;
                    cm.total_uncompressed_size += @intCast(hl + rewritten.uncompressed);
                }
                const st = pg.stats;
                try pages.append(a, .{ .offset = at - start, .size = std.math.cast(u32, out.pos() - at) orelse return error.TooLarge, .first_row = pg.first_row, .rows = pg.rows, .null_count = if (st) |s| s.null_count orelse -1 else -1, .min = if (st) |s| s.min_value orelse s.min else null, .max = if (st) |s| s.max_value orelse s.max else null });
                try countEncoding(a, cm, h.type, encoding);
            }
            cm.total_compressed_size = @intCast(out.pos() - start);
            try index.attachWithOptions(a, &chunk, pages.items, &col.elem, .{
                .unordered = col.elem.type == .FLOAT or col.elem.type == .DOUBLE,
                .omit_bounds = col.elem.type == .INT96,
            });
            try columns.append(a, chunk);
        }
        return .{ .columns = columns, .num_rows = @intCast(p.rows), .total_byte_size = fast.uncompressedBytes(columns.items) };
    }
};

/// A filter union is unsafe if a source is missing or uses another bitset size. Decode only for rebuilding this
/// optional filter; the original encoded value pages still go to the sink unchanged.
fn rebuildFilter(a: std.mem.Allocator, sources: []const Source, ci: usize, fpp: f64) ![]const u8 {
    var n: u64 = 0;
    for (sources) |s| n += @intCast(s.rg.num_rows);
    var builder = try bloom.Builder.init(a, n, fpp);
    defer builder.deinit(a);
    var scratch = std.heap.ArenaAllocator.init(a);
    defer scratch.deinit();
    for (sources) |s| {
        _ = scratch.reset(.retain_capacity);
        const sa = scratch.allocator();
        const cm = s.rg.columns.items[ci].meta_data.?;
        const range = fast.chunkRange(&cm, s.bytes.len) orelse return error.InvalidColumnOffsets;
        const levels = s.meta.getColumnLevels(cm.path_in_schema.items);
        switch (cm.type) {
            inline else => |tag| {
                const T = switch (tag) {
                    .INT32 => i32,
                    .INT64 => i64,
                    .FLOAT => f32,
                    .DOUBLE => f64,
                    .BOOLEAN => bool,
                    .BYTE_ARRAY, .FIXED_LEN_BYTE_ARRAY, .INT96 => []const u8,
                };
                var reader = @import("../parquet/column.zig").ColumnChunkReader(T).init(s.bytes[range.start..][0..range.len], cm.codec, levels, sa);
                if (T == []const u8) reader.type_length = if (tag == .INT96) 12 else if (tag == .FIXED_LEN_BYTE_ARRAY)
                    @intCast((s.meta.getColumnSchema(cm.path_in_schema.items).?).type_length.?)
                else
                    0;
                const leaves = try reader.readAll(@intCast(cm.num_values));
                for (leaves.values, 0..) |v, i| {
                    if (leaves.def_levels) |defs| if (defs[i] != @as(u32, @intCast(levels.max_def))) continue;
                    const hash = if (T == []const u8) bloom.hashBytes(v) else blk: {
                        var raw: [8]u8 = undefined;
                        if (T == bool) {
                            raw[0] = @intFromBool(v);
                            break :blk bloom.hashBytes(raw[0..1]);
                        }
                        const U = @Int(.unsigned, @bitSizeOf(T));
                        std.mem.writeInt(U, raw[0..@sizeOf(T)], @bitCast(v), .little);
                        break :blk bloom.hashBytes(raw[0..@sizeOf(T)]);
                    };
                    builder.insert(hash);
                }
            },
        }
    }
    return builder.serialize(a);
}
fn numericKey(raw: []const u8) u64 {
    if (raw.len == 4) return std.mem.readInt(u32, raw[0..4], .little);
    if (raw.len == 8) return std.mem.readInt(u64, raw[0..8], .little);
    // A column has one fixed width. Exact-width loads preserve raw bits without a per-entry memcpy.
    inline for (1..8) |n| {
        if (raw.len == n) return std.mem.readInt(@Int(.unsigned, n * 8), raw[0..n], .little);
    }
    unreachable;
}

fn compatible(a: schema.SchemaElement, b: schema.SchemaElement) bool {
    return a.type == b.type and a.type_length == b.type_length and a.scale == b.scale and a.precision == b.precision and
        a.repetition_type == b.repetition_type and a.converted_type == b.converted_type and
        std.mem.eql(u8, a.logical_type_raw, b.logical_type_raw);
}
fn indexed(bytes: []const u8, off: ?i64, len: ?i32) ?[]const u8 {
    const o = std.math.cast(usize, off orelse return null) orelse return null;
    const n = std.math.cast(usize, len orelse return null) orelse return null;
    if (o > bytes.len or n > bytes.len - o) return null;
    return bytes[o..][0..n];
}
fn dictionaryEntry(bytes: []const u8, at: *usize, elem: *const schema.SchemaElement) ![]const u8 {
    var n: usize = switch (elem.type.?) {
        .INT32, .FLOAT => 4,
        .INT64, .DOUBLE => 8,
        .INT96 => 12,
        .FIXED_LEN_BYTE_ARRAY => std.math.cast(usize, elem.type_length orelse 0) orelse return error.BadPageHeader,
        .BYTE_ARRAY => 4,
        else => return error.BadPageHeader,
    };
    if (at.* > bytes.len or n > bytes.len - at.*) return error.TruncatedPage;
    if (elem.type == .BYTE_ARRAY) n += std.mem.readInt(u32, bytes[at.*..][0..4], .little);
    if (n == 0 or n > bytes.len - at.*) return error.TruncatedPage;
    const entry = bytes[at.*..][0..n];
    at.* += n;
    return entry;
}
fn writePage(a: std.mem.Allocator, out: anytype, h: schema.PageHeader, body: []const u8) !usize {
    var w: thrift.Writer = .init(a);
    defer w.deinit();
    try h.write(&w);
    const n = w.bytes().len;
    try out.write(w.bytes());
    try out.write(body);
    return n;
}
fn countEncoding(a: std.mem.Allocator, cm: *schema.ColumnMetaData, t: schema.PageType, e: schema.Encoding) !void {
    if (std.mem.indexOfScalar(schema.Encoding, cm.encodings.items, e) == null) try cm.encodings.append(a, e);
    if (cm.encoding_stats) |*stats| {
        for (stats.items) |*s| if (s.page_type == @backingInt(t) and s.encoding == @backingInt(e)) {
            s.count += 1;
            return;
        };
        try stats.append(a, .{ .page_type = @backingInt(t), .encoding = @backingInt(e), .count = 1 });
    }
}
fn mergeStats(acc: *schema.Statistics, s: schema.Statistics, elem: *const schema.SchemaElement) void {
    acc.null_count = if (acc.null_count != null and s.null_count != null) acc.null_count.? + s.null_count.? else null;
    acc.nan_count = if (acc.nan_count != null and s.nan_count != null) acc.nan_count.? + s.nan_count.? else null;
    const omit_bounds = elem.type == .FLOAT or elem.type == .DOUBLE or elem.type == .INT96;
    const lo = if (omit_bounds) null else acc.min_value orelse acc.min;
    const hi = if (omit_bounds) null else acc.max_value orelse acc.max;
    const slo = s.min_value orelse s.min;
    const shi = s.max_value orelse s.max;
    acc.min = null;
    acc.max = null;
    acc.min_value = if (lo != null and slo != null) if (index.compare(elem, lo.?, slo.?)) |o| if (o == .gt) slo else lo else null else null;
    acc.max_value = if (hi != null and shi != null) if (index.compare(elem, hi.?, shi.?)) |o| if (o == .lt) shi else hi else null else null;
    acc.is_min_value_exact = false;
    acc.is_max_value_exact = false;
    acc.distinct_count = null;
}
const Rewritten = struct { body: []const u8, uncompressed: usize };
fn remap(a: std.mem.Allocator, meta: *const schema.FileMetaData, cm: schema.ColumnMetaData, h: schema.PageHeader, body: []const u8, map: []const u32, entries: usize) !Rewritten {
    var level_len: usize = 0;
    var present: usize = 0;
    var plain: []const u8 = undefined;
    var compress_values = true;
    if (h.data_page_header) |dh| {
        plain = try compression.decompress(a, body, cm.codec, @intCast(h.uncompressed_page_size));
        present = @intCast(dh.num_values);
        const max_def = meta.getColumnLevels(cm.path_in_schema.items).max_def;
        if (max_def > 0) {
            if (dh.definition_level_encoding != .RLE or plain.len < 4) return error.BadPageHeader;
            const len = std.mem.readInt(u32, plain[0..4], .little);
            if (len > plain.len - 4) return error.TruncatedPage;
            var d = rle.HybridRleDecoder.init(plain[4..][0..len], enc.bitWidthFor(@intCast(max_def)));
            present = try d.countPresentLevels(@intCast(max_def), present);
            level_len = 4 + len;
        }
    } else {
        const dh = h.data_page_header_v2.?;
        if (dh.num_nulls < 0 or dh.num_nulls > dh.num_values or dh.definition_levels_byte_length < 0 or
            dh.repetition_levels_byte_length != 0) return error.BadPageHeader;
        present = @intCast(dh.num_values - dh.num_nulls);
        level_len = @intCast(dh.definition_levels_byte_length);
        if (level_len > body.len or level_len > @as(usize, @intCast(h.uncompressed_page_size))) return error.TruncatedPage;
        compress_values = dh.is_compressed;
        const v = if (compress_values) try compression.decompress(a, body[level_len..], cm.codec, @as(usize, @intCast(h.uncompressed_page_size)) - level_len) else body[level_len..];
        plain = try std.mem.concat(a, u8, &.{ body[0..level_len], v });
    }
    const values = plain[level_len..];
    const ids = try a.alloc(u32, present);
    if (present > 0) {
        if (values.len == 0 or values[0] > 32) return error.BadPageHeader;
        var d = rle.HybridRleDecoder.init(values[1..], values[0]);
        if (try d.decode(ids) != present) return error.ShortDecode;
        for (ids) |*id| {
            if (id.* >= map.len) return error.BadPageHeader;
            id.* = map[id.*];
        }
    }
    const width = enc.bitWidthFor(@intCast(entries -| 1));
    const encoded = try rle.encode(a, ids, width);
    const codes = try std.mem.concat(a, u8, &.{ &.{width}, encoded });
    if (h.data_page_header != null) {
        const joined = try std.mem.concat(a, u8, &.{ plain[0..level_len], codes });
        return .{ .body = try compression.compress(a, joined, cm.codec), .uncompressed = joined.len };
    }
    const stored = if (compress_values) try compression.compress(a, codes, cm.codec) else codes;
    return .{ .body = try std.mem.concat(a, u8, &.{ plain[0..level_len], stored }), .uncompressed = level_len + codes.len };
}

test "coalesce target counts encoded headers and closes at the first whole-group boundary" {
    const t: Target = .{ .rows = 131072, .encoded_uncompressed_bytes = 64 << 20 };
    try std.testing.expect(!t.reached(65536, 1 << 20));
    try std.testing.expect(t.reached(131072, 2 << 20));
    try std.testing.expect(t.reached(10, 64 << 20));
}

fn testSource(a: std.mem.Allocator, dict: ?[]const i32, vals: []const u32, v2: bool, all_null: bool) !Source {
    const streaming = @import("streaming.zig");
    var sink: streaming.MemorySink = .{ .arena = a };
    var out: streaming.SinkOut = .{ .sink = sink.sink() };
    var raw: std.ArrayList(u8) = .empty;
    const elem: schema.SchemaElement = .{ .type = .INT32, .type_length = null, .repetition_type = if (all_null) .OPTIONAL else .REQUIRED, .name = "x", .num_children = null, .scale = null, .precision = null, .field_id = null };
    const meta = try a.create(schema.FileMetaData);
    meta.* = .{ .version = 1, .schema = .empty, .row_groups = .empty, .num_rows = @intCast(vals.len), .created_by = schema.ZPQ_CREATED_BY };
    var root = elem;
    root.type = null;
    root.name = "schema";
    root.num_children = 1;
    root.repetition_type = null;
    try meta.schema.appendSlice(a, &.{ root, elem });
    var h: schema.PageHeader = .{ .type = .DICTIONARY_PAGE, .compressed_page_size = 0, .uncompressed_page_size = 0, .crc = null, .dictionary_page_header = null, .data_page_header = null, .data_page_header_v2 = null };
    var uncompressed: usize = 0;
    if (dict) |d| {
        for (d) |v| {
            var b: [4]u8 = undefined;
            std.mem.writeInt(i32, &b, v, .little);
            try raw.appendSlice(a, &b);
        }
        h.compressed_page_size = @intCast(raw.items.len);
        h.uncompressed_page_size = h.compressed_page_size;
        h.dictionary_page_header = .{ .num_values = @intCast(d.len), .encoding = .PLAIN, .is_sorted = false };
        uncompressed += try writePage(a, &out, h, raw.items);
        uncompressed += raw.items.len;
    }
    const data_at = out.pos();
    raw = .empty;
    if (all_null) {
        const levels = try rle.encode(a, try a.alloc(u32, 0), 1);
        _ = levels;
        const defs = try a.alloc(u32, vals.len);
        @memset(defs, 0);
        const dl = try rle.encode(a, defs, 1);
        if (!v2) {
            var b: [4]u8 = undefined;
            std.mem.writeInt(u32, &b, @intCast(dl.len), .little);
            try raw.appendSlice(a, &b);
        }
        try raw.appendSlice(a, dl);
        if (dict != null) try raw.append(a, 0);
    } else if (dict != null) {
        const width = enc.bitWidthFor(@intCast(dict.?.len -| 1));
        try raw.append(a, width);
        try raw.appendSlice(a, try rle.encode(a, vals, width));
    } else for (vals) |v| {
        var b: [4]u8 = undefined;
        std.mem.writeInt(u32, &b, v, .little);
        try raw.appendSlice(a, &b);
    }
    h.type = if (v2) .DATA_PAGE_V2 else .DATA_PAGE;
    h.dictionary_page_header = null;
    h.compressed_page_size = @intCast(raw.items.len);
    h.uncompressed_page_size = h.compressed_page_size;
    h.crc = @bitCast(std.hash.Crc32.hash(raw.items));
    const encoding: schema.Encoding = if (dict != null) .RLE_DICTIONARY else .PLAIN;
    if (v2) h.data_page_header_v2 = .{ .num_values = @intCast(vals.len), .num_nulls = if (all_null) @intCast(vals.len) else 0, .num_rows = @intCast(vals.len), .encoding = encoding, .definition_levels_byte_length = if (all_null) @intCast(raw.items.len - @intFromBool(dict != null)) else 0, .repetition_levels_byte_length = 0, .is_compressed = false } else h.data_page_header = .{ .num_values = @intCast(vals.len), .encoding = encoding, .definition_level_encoding = .RLE, .repetition_level_encoding = .RLE };
    uncompressed += try writePage(a, &out, h, raw.items);
    uncompressed += raw.items.len;
    var path: schema.StringList = .empty;
    try path.append(a, "x");
    var encodings: schema.EncodingList = .empty;
    try encodings.appendSlice(a, &.{ .PLAIN, .RLE, encoding });
    var rg: schema.RowGroup = .{ .columns = .empty, .num_rows = @intCast(vals.len), .total_byte_size = @intCast(uncompressed) };
    try rg.columns.append(a, .{ .file_path = null, .file_offset = @intCast(data_at), .meta_data = .{ .type = .INT32, .codec = .UNCOMPRESSED, .num_values = @intCast(vals.len), .encodings = encodings, .path_in_schema = path, .data_page_offset = @intCast(data_at), .dictionary_page_offset = if (dict != null) 0 else null, .index_page_offset = null, .total_compressed_size = @intCast(out.pos()), .total_uncompressed_size = @intCast(uncompressed) } });
    try meta.row_groups.append(a, rg);
    return .{ .bytes = sink.buffer.items, .meta = meta, .rg = &meta.row_groups.items[0] };
}

test "coalesce dictionaries remap indices across V1 V2 and PLAIN pages; indexes and CRCs describe output" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const sources = [_]Source{
        try testSource(a, &.{ 10, 20 }, &.{ 0, 1, 0, 1 }, false, false),
        try testSource(a, &.{ 20, 30 }, &.{ 0, 1, 1, 0 }, true, false),
        try testSource(a, null, &.{ 40, 50 }, false, false),
    };
    const plan = (try Plan.init(a, &sources, null, .{})).?;
    const streaming = @import("streaming.zig");
    var sink: streaming.MemorySink = .{ .arena = a };
    var out: streaming.SinkOut = .{ .sink = sink.sink() };
    const rg = try plan.write(a, &out);
    var reader = @import("../parquet/column.zig").ColumnChunkReader(i32).init(sink.buffer.items, .UNCOMPRESSED, .{ .max_def = 0, .max_rep = 0 }, a);
    const values = try reader.readAll(10);
    try std.testing.expectEqualSlices(i32, &.{ 10, 20, 10, 20, 20, 30, 30, 20, 40, 50 }, values.values);
    var ir = thrift.Reader.init(rg.columns.items[0].offset_index_out);
    const oi = try schema.OffsetIndex.read(a, &ir);
    try std.testing.expectEqual(@as(usize, 3), oi.page_locations.items.len);
    for (oi.page_locations.items, [_]i64{ 0, 4, 8 }) |loc, row| {
        try std.testing.expectEqual(row, loc.first_row_index);
        var r = thrift.Reader.init(sink.buffer.items[@intCast(loc.offset)..]);
        const h = try schema.PageHeader.read(&r);
        try std.testing.expectEqual(loc.compressed_page_size, @as(i32, @intCast(r.pos)) + h.compressed_page_size);
        if (h.crc) |crc| try std.testing.expectEqual(@as(u32, @bitCast(crc)), std.hash.Crc32.hash(sink.buffer.items[@as(usize, @intCast(loc.offset)) + r.pos ..][0..@intCast(h.compressed_page_size)]));
    }
    try std.testing.expect((try Plan.init(a, sources[0..2], null, .{ .dictionary_limit = 8 })) == null);
}

test "coalesce empty dictionaries with all NULL chunks have no fabricated values" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const sources = [_]Source{ try testSource(a, &.{}, &.{ 0, 0, 0 }, false, true), try testSource(a, &.{}, &.{ 0, 0 }, true, true) };
    const plan = (try Plan.init(a, &sources, null, .{})).?;
    const streaming = @import("streaming.zig");
    var sink: streaming.MemorySink = .{ .arena = a };
    var out: streaming.SinkOut = .{ .sink = sink.sink() };
    const rg = try plan.write(a, &out);
    try std.testing.expectEqual(@as(i64, 5), rg.num_rows);
    var reader = @import("../parquet/column.zig").ColumnChunkReader(i32).init(sink.buffer.items, .UNCOMPRESSED, .{ .max_def = 1, .max_rep = 0 }, a);
    const values = try reader.readAll(5);
    try std.testing.expect(values.has_nulls);
    try std.testing.expectEqualSlices(u32, &.{ 0, 0, 0, 0, 0 }, values.def_levels.?);
}

test "coalesce Bloom unions and dictionary rebuilds cover every source value" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var sources = [_]Source{ try testSource(a, &.{ 10, 20 }, &.{ 0, 1 }, false, false), try testSource(a, &.{ 20, 30 }, &.{ 0, 1 }, true, false) };
    for (&sources, 0..) |s, i| {
        var b = try bloom.Builder.init(a, 2, 0.01);
        defer b.deinit(a);
        for ([_]i32{ if (i == 0) 10 else 20, if (i == 0) 20 else 30 }) |v| {
            var raw: [4]u8 = undefined;
            std.mem.writeInt(i32, &raw, v, .little);
            b.insert(bloom.hashBytes(&raw));
        }
        s.rg.columns.items[0].bloom_out = try b.serialize(a);
    }
    for (0..2) |mode| {
        if (mode == 1) sources[1].rg.columns.items[0].bloom_out = &.{};
        if (mode == 1) try std.testing.expectEqual(@as(usize, 0), (try Plan.init(a, &sources, null, .{})).?.columns[0].filter.len);
        const plan = (try Plan.init(a, &sources, null, .{ .filter_fpp = &.{0.01} })).?;
        const bits = bloom.bitset(plan.columns[0].filter).?;
        for ([_]i32{ 10, 20, 30 }) |v| {
            var raw: [4]u8 = undefined;
            std.mem.writeInt(i32, &raw, v, .little);
            try std.testing.expect(bloom.mightContainScalar(bits, bloom.hashBytes(&raw)));
        }
    }
    var different = try bloom.Builder.init(a, 1000, 0.01);
    defer different.deinit(a);
    for ([_]i32{ 20, 30 }) |v| {
        var raw: [4]u8 = undefined;
        std.mem.writeInt(i32, &raw, v, .little);
        different.insert(bloom.hashBytes(&raw));
    }
    sources[1].rg.columns.items[0].bloom_out = try different.serialize(a);
    try std.testing.expectEqual(@as(usize, 0), (try Plan.init(a, &sources, null, .{})).?.columns[0].filter.len);
    const rebuilt = (try Plan.init(a, &sources, null, .{ .filter_fpp = &.{0.01} })).?;
    try std.testing.expect(rebuilt.columns[0].filter.len > 0);
    sources[1] = try testSource(a, null, &.{ 30, 40 }, false, false);
    try std.testing.expectEqual(@as(usize, 0), (try Plan.init(a, &sources, null, .{})).?.columns[0].filter.len);
    const mixed = (try Plan.init(a, &sources, null, .{ .filter_fpp = &.{0.01} })).?;
    for ([_]i32{ 10, 20, 30, 40 }) |v| {
        var raw: [4]u8 = undefined;
        std.mem.writeInt(i32, &raw, v, .little);
        try std.testing.expect(bloom.mightContainScalar(bloom.bitset(mixed.columns[0].filter).?, bloom.hashBytes(&raw)));
    }
}

test "coalesce and whole copies create no unrequested Bloom filters" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const s = try testSource(a, &.{ 10, 20 }, &.{ 0, 1 }, false, false);
    const plan = (try Plan.init(a, &.{ s, s }, null, .{})).?;
    try std.testing.expectEqual(@as(usize, 0), plan.columns[0].filter.len);
    const streaming = @import("streaming.zig");
    var sink: streaming.MemorySink = .{ .arena = a };
    var offset: u64 = 0;
    var timing: @import("../consumer/types.zig").Timings = .{};
    const copied = try @import("../consumer/copy.zig").copyRG(a, s.rg, .{ .bytes = s.bytes, .byte_origin = 0 }, null, sink.sink(), &offset, &timing, true, null, .{ .meta = s.meta, .scratch = a });
    try std.testing.expectEqual(@as(usize, 0), copied.rg.?.columns.items[0].bloom_out.len);
}

test "coalesce owned window sources retain runtime filters and decode after the source is released" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const source = try testSource(a, &.{ 10, 20 }, &.{ 0, 1, 1, 0 }, false, false);
    var b = try bloom.Builder.init(a, 2, 0.01);
    defer b.deinit(a);
    var raw: [4]u8 = undefined;
    std.mem.writeInt(i32, &raw, 10, .little);
    b.insert(bloom.hashBytes(&raw));
    std.mem.writeInt(i32, &raw, 20, .little);
    b.insert(bloom.hashBytes(&raw));
    const filter = try b.serialize(a);
    source.rg.columns.items[0].bloom_out = filter;
    const owned = try source.ownProjected(a, null);
    try std.testing.expectEqualSlices(u8, filter, owned.rg.columns.items[0].bloom_out);
    const relocated = fast.rebaseChunk(a, owned.bytes, &owned.rg.columns.items[0], 17);
    try std.testing.expectEqualSlices(u8, filter, relocated.bloom_out);
    @memset(@constCast(source.bytes), 0);
    const pair = [_]Source{ owned, owned };
    const plan = (try Plan.init(a, &pair, null, .{})).?;
    const streaming = @import("streaming.zig");
    var sink: streaming.MemorySink = .{ .arena = a };
    var out: streaming.SinkOut = .{ .sink = sink.sink() };
    _ = try plan.write(a, &out);
    var reader = @import("../parquet/column.zig").ColumnChunkReader(i32).init(sink.buffer.items, .UNCOMPRESSED, .{ .max_def = 0, .max_rep = 0 }, a);
    try std.testing.expectEqualSlices(i32, &.{ 10, 20, 20, 10, 10, 20, 20, 10 }, (try reader.readAll(8)).values);
}

test "coalesce numeric dictionary collisions preserve IDs when promoting the encoder interner" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var keys: [20]i32 = undefined;
    var ids: [20]u32 = undefined;
    const bits = std.math.log2_int_ceil(usize, 2 * (2 * keys.len + 1));
    const shift: u6 = @intCast(64 - bits);
    var found: usize = 0;
    var candidate: u32 = 0;
    while (found < keys.len) : (candidate += 1) {
        if ((@as(u64, candidate) *% 0x9E3779B97F4A7C15) >> shift != 0) continue;
        keys[found] = @intCast(candidate);
        ids[found] = @intCast(found);
        found += 1;
    }
    const sources = [_]Source{ try testSource(a, &keys, &ids, false, false), try testSource(a, &keys, &ids, true, false) };
    const p = (try Plan.init(a, &sources, null, .{})).?;
    const streaming = @import("streaming.zig");
    var sink: streaming.MemorySink = .{ .arena = a };
    var out: streaming.SinkOut = .{ .sink = sink.sink() };
    _ = try p.write(a, &out);
    var r = @import("../parquet/column.zig").ColumnChunkReader(i32).init(sink.buffer.items, .UNCOMPRESSED, .{ .max_def = 0, .max_rep = 0 }, a);
    const got = try r.readAll(keys.len * 2);
    try std.testing.expectEqualSlices(i32, &keys, got.values[0..keys.len]);
    try std.testing.expectEqualSlices(i32, &keys, got.values[keys.len..]);
}

test "review: a staged fallback copies its serialized column index onto output ownership" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    var pending = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer pending.deinit();
    const a = arena.allocator();
    const s = try testSource(a, null, &.{42}, false, false);
    const bound = [_]u8{ 42, 0, 0, 0 };
    try index.attach(pending.allocator(), &s.rg.columns.items[0], &.{.{ .offset = 0, .size = @intCast(s.bytes.len), .first_row = 0, .rows = 1, .null_count = 0, .min = &bound, .max = &bound }}, &s.meta.schema.items[1]);
    const held = try @import("../consumer/copy.zig").prepareCoalesceSource(pending.allocator(), a, s, &.{0.01});
    const expected = try a.dupe(u8, held.rg.columns.items[0].column_index_out);
    const streaming = @import("streaming.zig");
    var sink: streaming.MemorySink = .{ .arena = a };
    var offset: u64 = 0;
    var timing: @import("../consumer/types.zig").Timings = .{};
    const copied = try @import("../consumer/copy.zig").copyRG(a, held.rg, .{ .bytes = held.bytes, .byte_origin = 0 }, null, sink.sink(), &offset, &timing, true, null, .{});
    @memset(@constCast(held.rg.columns.items[0].column_index_out), 0x55);
    try std.testing.expectEqualSlices(u8, expected, copied.rg.?.columns.items[0].column_index_out);
}

test "coalesce seven-byte fixed dictionary keys preserve raw entries and remap order" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const first = "\x00\x00\x00\x00\x00\x00\x01";
    const second = "\xff\xff\xff\xff\xff\xff\xff";
    var sources: [2]Source = undefined;
    for (&sources, 0..) |*s, i| {
        s.* = try testSource(a, null, &.{0}, false, false);
        const e = &s.meta.schema.items[1];
        e.type = .FIXED_LEN_BYTE_ARRAY;
        e.type_length = 7;
        e.converted_type = .DECIMAL;
        e.scale = 0;
        e.precision = 16;
        var values: [64][]const u8 = undefined;
        for (&values, 0..) |*v, j| v.* = if ((j + i) % 2 == 0) first else second;
        const encoded = try enc.encodeColumn(a, .{
            .values = .{ .string = .{ .values = &values } },
            .schema_elem = e,
            .path_in_schema = &.{"x"},
            .dictionary_bloom = false,
        });
        s.bytes = encoded.bytes;
        s.rg.columns.items[0].meta_data = encoded.meta;
        // This test supplies raw entries to the low-level byte encoder, not exact-decimal statistics.
        s.rg.columns.items[0].meta_data.?.statistics = null;
        s.meta.row_groups.items[0].num_rows = values.len;
        @constCast(s.meta).num_rows = values.len;
    }
    const plan = (try Plan.init(a, &sources, null, .{})).?;
    try std.testing.expectEqual(@as(usize, 2), plan.columns[0].entries.items.len);
    try std.testing.expectEqualSlices(u32, &.{ 0, 1 }, plan.columns[0].maps[0]);
    try std.testing.expectEqualSlices(u32, &.{ 1, 0 }, plan.columns[0].maps[1]);
    try std.testing.expectEqualSlices(u8, first, plan.columns[0].entries.items[0]);
    try std.testing.expectEqualSlices(u8, second, plan.columns[0].entries.items[1]);
}

test "review: untrusted staged column index cannot survive writer safety" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const s = try testSource(a, null, &.{1}, false, false);
    s.rg.columns.items[0].meta_data.?.writer_stats = .absent;
    s.rg.columns.items[0].column_index_out = "untrusted index";
    const safe = fast.writerSafeChunk(a, s.bytes, &s.rg.columns.items[0], null);
    try std.testing.expectEqual(@as(usize, 0), safe.column_index_out.len);
}

test "review: zero-byte chunks refuse merging and can be staged at EOF" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const s = try testSource(a, null, &.{}, false, false);
    const cm = &s.rg.columns.items[0].meta_data.?;
    cm.total_compressed_size = 0;
    cm.total_uncompressed_size = 0;
    cm.data_page_offset = @intCast(s.bytes.len);
    const owned = try s.ownProjected(a, null);
    try std.testing.expectEqual(@as(usize, 0), owned.bytes.len);
    try std.testing.expect((try Plan.init(a, &.{ s, s }, null, .{})) == null);
}

test "review: NaN chunk bounds are discarded without comparison" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const nan = [_]u8{ 0, 0, 0xc0, 0x7f };
    const sources = [_]Source{ try testSource(a, null, &.{0}, false, false), try testSource(a, null, &.{0}, false, false) };
    for (sources) |s| {
        s.meta.schema.items[1].type = .FLOAT;
        s.rg.columns.items[0].meta_data.?.type = .FLOAT;
        s.rg.columns.items[0].meta_data.?.statistics = .{ .min_value = &nan, .max_value = &nan, .null_count = 0 };
    }
    const plan = (try Plan.init(a, &sources, null, .{})).?;
    try std.testing.expect(plan.columns[0].statistics.?.min_value == null);
}
