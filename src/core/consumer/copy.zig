const decodeFlbaColumn = @import("column.zig").decodeFlbaColumn;
const decodeColumnT = @import("column.zig").decodeColumnT;
const originSlice = @import("column.zig").originSlice;
const invariant = @import("../invariant.zig");
const fastpath = @import("../writer/fastpath.zig");
const std = @import("std");
const schema = @import("../schema.zig");
const streaming = @import("../writer/streaming.zig");
const encoder = @import("../writer/encoder.zig");
const page_index = @import("../writer/page_index.zig");
const thrift = @import("../thrift.zig");
const column_mod = @import("../parquet/column.zig");
const page_mod = @import("../parquet/page.zig");
const compression = @import("../parquet/compression.zig");
const metadata = @import("../parquet/metadata.zig");
const int96_mod = @import("../parquet/int96.zig");
const header_writer = @import("../writer/page_header.zig");
const WriterStatsPolicy = @import("../parquet/writer_statistics.zig").Policy;
const filter_eval = @import("../filter/eval.zig");
const profile = @import("../../profile.zig");
const nowMonoNs = @import("../../clock.zig").monoNs;
const types = @import("types.zig");
const RGSrc = types.RGSrc;
const Timings = types.Timings;
const RGOut = types.RGOut;
const SliceReader = @import("slice_reader.zig").SliceReader;
const flbaWidth = @import("column.zig").flbaWidth;

/// Byte-copy one row group into `sink` and return a clone of it whose
/// chunk offsets (and carried page indexes) point at the new stream
/// position.
///
/// `kept_set = null`: no projection. The group's bounding span, from its
/// first chunk start to its last chunk end, is copied from `src` in one
/// write and every chunk shifts by the same delta.
///
/// `kept_set != null`: per-column copy. Only chunks where
/// `kept_set[ci] == true` are written. A `recompress` that changes any
/// chunk's codec, or stripping affected page-header statistics, takes this path too, with every column kept.
pub fn copyRG(
    out_arena: std.mem.Allocator,
    rg: *const schema.RowGroup,
    /// Must hold the file from offset 0: page indexes are found by their file offsets.
    src: RGSrc,
    kept_set: ?[]const bool,
    sink: streaming.Sink,
    out_offset: *u64,
    timings: *Timings,
    // See encodeAggregator: false when writing to a relative-offset buffer that
    // is later placed at a nonzero file offset (the carried OffsetIndex's
    // PageLocation.offset would stay buffer-relative). Direct-to-file: true.
    emit_page_index: bool,
    /// A requested output codec. Chunks already in it are copied verbatim;
    /// the rest have their pages recompressed. Null keeps every chunk's codec.
    recompress: ?Recompress,
    bloom_write: BloomWrite,
) !RGOut {
    invariant.assert(src.byte_origin == 0);
    // Changed codecs or untrusted page-header statistics can change chunk sizes.
    var all_kept: ?[]bool = null;
    if (kept_set == null) {
        for (rg.columns.items) |*chunk| {
            const m = chunk.meta_data orelse return error.InvalidColumnOffsets;
            const changes_codec = if (recompress) |rc| m.codec != rc.codec else false;
            if (!changes_codec and fastpath.writerPolicy(chunk, bloom_write.meta) == .trust) continue;
            all_kept = try out_arena.alloc(bool, rg.columns.items.len);
            @memset(all_kept.?, true);
            break;
        }
    }
    const kept_eff: ?[]const bool = kept_set orelse all_kept;

    var out: streaming.SinkOut = .{ .sink = sink, .offset = @intCast(out_offset.*) };
    defer out_offset.* = out.offset;

    if (kept_eff) |kept| {
        var new_cols: std.ArrayListUnmanaged(schema.ColumnChunk) = .empty;
        try new_cols.ensureTotalCapacity(out_arena, rg.columns.items.len);
        var rg_total: i64 = 0;

        for (kept, 0..) |b, ci| {
            if (!b) continue;
            if (ci >= rg.columns.items.len) return error.BadColumnIndex;
            const safe = fastpath.writerSafeChunk(out_arena, src.bytes, &rg.columns.items[ci], bloom_write.meta);
            const src_chunk = &safe;
            const m = src_chunk.meta_data orelse return error.InvalidColumnOffsets;
            const range = fastpath.chunkRange(&m, std.math.maxInt(usize)) orelse return error.InvalidColumnOffsets;
            const slice = try sourceBytes(src, range);

            if (recompress) |rc| if (m.codec != rc.codec) {
                var written = try copyChunkRecompressed(
                    rc,
                    src,
                    src_chunk.*,
                    slice,
                    range.start,
                    &out,
                    timings,
                    emit_page_index,
                    if (rc.done) |d| d[ci] else null,
                );
                try requestedFilter(out_arena, &written, rg, ci, src, bloom_write);
                try new_cols.append(out_arena, written);
                rg_total += written.meta_data.?.total_uncompressed_size;
                continue;
            };
            if (m.writer_stats != .trust) {
                var timed: TimedCopyOut = .{ .out = &out, .timings = timings };
                var copied = try fastpath.copyPolicyChunk(out_arena, &timed, src.bytes, src_chunk, emit_page_index);
                try requestedFilter(out_arena, &copied, rg, ci, src, bloom_write);
                try new_cols.append(out_arena, copied);
                rg_total += copied.meta_data.?.total_uncompressed_size;
                continue;
            }
            const delta = offsetDelta(out.pos(), range.start);
            try timedWrite(&out, slice, timings);
            // Carry the page index forward, written right after this chunk's data.
            var copied = try carryChunk(out_arena, &out, src, src_chunk, delta, emit_page_index);
            try requestedFilter(out_arena, &copied, rg, ci, src, bloom_write);
            try new_cols.append(out_arena, copied);
            rg_total += m.total_uncompressed_size;
        }

        return .{
            .surviving_rows = rg.num_rows,
            .rg = .{
                .columns = new_cols,
                .total_byte_size = rg_total,
                .num_rows = rg.num_rows,
            },
        };
    }

    // No projection: copy this RG's bounding box [min_col_start,
    // max_col_end), then shift every chunk's offset by a single delta.
    //
    // We compute the bounding box internally rather than trusting
    // the caller because the contract is easy to get wrong — passing
    // the whole-file bytes here would cause us to write every RG's
    // worth of bytes once per RG (an N-row-group file produces N×
    // output bloat). Production callers pass the whole-file bytes;
    // this branch makes that safe.
    for (rg.columns.items) |chunk| if (chunk.meta_data == null) return error.InvalidColumnOffsets;
    const range = try fastpath.rowGroupByteRange(rg, std.math.maxInt(usize)) orelse return error.InvalidColumnOffsets;
    const rg_slice = try sourceBytes(src, range);

    const delta = offsetDelta(out.pos(), range.start);
    try timedWrite(&out, rg_slice, timings);

    var cols: std.ArrayListUnmanaged(schema.ColumnChunk) = .empty;
    try cols.ensureTotalCapacity(out_arena, rg.columns.items.len);
    // Whole-RG copy: one delta for the group. Index bytes for each
    // chunk are appended after the RG data block, in column order.
    for (rg.columns.items, 0..) |*chunk, ci| {
        const safe = fastpath.writerSafeChunk(out_arena, src.bytes, chunk, bloom_write.meta);
        var copied = try carryChunk(out_arena, &out, src, &safe, delta, emit_page_index);
        try requestedFilter(out_arena, &copied, rg, ci, src, bloom_write);
        try cols.append(out_arena, copied);
    }

    return .{
        .surviving_rows = rg.num_rows,
        .rg = .{
            .columns = cols,
            .total_byte_size = fastpath.uncompressedBytes(cols.items),
            .num_rows = rg.num_rows,
        },
    };
}

/// Give a copied chunk the Bloom filter the write asks of column `ci`, when it carries none from its source: decode
/// the source chunk and build one from its values. A filter it carries is copied onto `out_arena` too: the filters
/// are written after the last row group, and a windowed input has released this one's bytes by then.
fn requestedFilter(
    out_arena: std.mem.Allocator,
    copied: *schema.ColumnChunk,
    rg: *const schema.RowGroup,
    ci: usize,
    src: RGSrc,
    bloom_write: BloomWrite,
) !void {
    if (copied.bloom_out.len > 0) {
        copied.bloom_out = try out_arena.dupe(u8, copied.bloom_out);
        return;
    }
    const explicit = bloom_write.at(ci);
    if (explicit == null and !bloom_write.dictionary_bloom) return;
    const meta = bloom_write.meta orelse return;
    var scratch = std.heap.ArenaAllocator.init(bloom_write.scratch orelse return);
    defer scratch.deinit();
    const a = scratch.allocator();
    const chunk = &rg.columns.items[ci];
    const cm = chunk.meta_data orelse return;
    const range = fastpath.chunkRange(&cm, std.math.maxInt(usize)) orelse return error.InvalidColumnOffsets;
    const bytes = try sourceBytes(src, range);
    const levels = meta.getColumnLevels(cm.path_in_schema.items);
    if (explicit == null) {
        if (cm.dictionary_page_offset == null) return;
        const filter = try dictionaryBloom(a, bytes, cm, levels, meta) orelse return;
        copied.bloom_out = try out_arena.dupe(u8, filter);
        return;
    }
    const fpp = explicit.?;
    const n: usize = @intCast(cm.num_values);
    const col: filter_eval.Batch.Column = switch (cm.type) {
        .INT32 => .{ .i32 = try decodeColumnT(i32, a, bytes, cm.codec, levels, n) },
        .INT64 => .{ .i64 = try decodeColumnT(i64, a, bytes, cm.codec, levels, n) },
        .FLOAT => .{ .f32 = try decodeColumnT(f32, a, bytes, cm.codec, levels, n) },
        .DOUBLE => .{ .f64 = try decodeColumnT(f64, a, bytes, cm.codec, levels, n) },
        .BYTE_ARRAY => .{ .string = try decodeColumnT([]const u8, a, bytes, cm.codec, levels, n) },
        .FIXED_LEN_BYTE_ARRAY => .{ .string = try decodeFlbaColumn(
            a,
            bytes,
            cm.codec,
            levels,
            n,
            flbaWidth(meta.getColumnSchema(cm.path_in_schema.items)),
        ) },
        else => return,
    };
    const filter = try encoder.bloomFilter(a, col, cm.type, fpp) orelse return;
    copied.bloom_out = try out_arena.dupe(u8, filter);
}

/// Verify every data page uses the dictionary before filtering its entries; mixed chunks must never omit values.
pub fn dictionaryBloom(
    a: std.mem.Allocator,
    bytes: []const u8,
    cm: schema.ColumnMetaData,
    levels: schema.Levels,
    meta: *const schema.FileMetaData,
) !?[]u8 {
    inline for (.{ i32, i64, f32, f64, []const u8 }) |T| {
        const matches = switch (T) {
            i32 => cm.type == .INT32,
            i64 => cm.type == .INT64,
            f32 => cm.type == .FLOAT,
            f64 => cm.type == .DOUBLE,
            []const u8 => cm.type == .BYTE_ARRAY or cm.type == .FIXED_LEN_BYTE_ARRAY or cm.type == .INT96,
            else => unreachable,
        };
        if (matches) {
            var reader = column_mod.ColumnChunkReader(T).initWithOptions(
                bytes,
                cm.codec,
                levels,
                a,
                column_mod.chunkOptions(.{}, meta, cm),
                null,
            );
            if (T == []const u8) {
                if (cm.type == .INT96) reader.type_length = int96_mod.INT96_WIDTH else if (cm.type == .FIXED_LEN_BYTE_ARRAY)
                    reader.type_length = flbaWidth(meta.getColumnSchema(cm.path_in_schema.items));
            }
            const entries = try reader.dictionaryOfEveryPage() orelse return null;
            if (T == []const u8 and cm.type == .INT96) for (entries) |entry| {
                // Noncanonical times can equal another day's timestamp; a reader may hash that canonical form.
                const nanos = std.mem.readInt(i64, entry[0..8], .little);
                if (nanos < 0 or nanos >= std.time.ns_per_day) return null;
            };
            const tag = if (T == []const u8) "string" else @typeName(T);
            const col = @unionInit(filter_eval.Batch.Column, tag, .{ .values = entries });
            // INT96 PLAIN is 12 raw bytes, hashed as a fixed byte array, without converting its timestamp.
            const hash_type = if (cm.type == .INT96) .FIXED_LEN_BYTE_ARRAY else cm.type;
            return encoder.bloomFilter(a, col, hash_type, 0.01);
        }
    }
    return null;
}

const sourceBytes = types.sourceBytes;

fn offsetDelta(new_start: usize, src_start: usize) i64 {
    return @as(i64, @intCast(new_start)) - @as(i64, @intCast(src_start));
}

fn timedWrite(out: *streaming.SinkOut, bytes: []const u8, timings: *Timings) !void {
    const t_sink_start = nowMonoNs();
    try out.write(bytes);
    const t_sink_end = nowMonoNs();
    timings.sink_ns += @intCast(t_sink_end - t_sink_start);
    profile.record(timings.prof, .sink_write, t_sink_start, t_sink_end, profile.args(.{bytes.len}));
}

const TimedCopyOut = struct {
    out: *streaming.SinkOut,
    timings: *Timings,

    pub fn pos(t: *const TimedCopyOut) usize {
        return t.out.pos();
    }

    pub fn write(t: *TimedCopyOut, bytes: []const u8) !void {
        try timedWrite(t.out, bytes, t.timings);
    }
};

/// A copied chunk's metadata, offsets moved by `delta`, with its page index carried into `out` when the output
/// keeps one.
fn carryChunk(
    out_arena: std.mem.Allocator,
    out: *streaming.SinkOut,
    src: RGSrc,
    chunk: *const schema.ColumnChunk,
    delta: i64,
    emit_page_index: bool,
) !schema.ColumnChunk {
    return if (emit_page_index)
        fastpath.rebaseChunk(out_arena, out, src.bytes, chunk, delta)
    else
        fastpath.copiedChunk(src.bytes, chunk, delta);
}

/// The Bloom filters a write builds: the false-positive probability of each column's filter, null for none. Indexed
/// by output column for `encodeAggregator`, by source leaf for `copyRG`, where a copied chunk that carries no filter
/// of its own is decoded (through `meta`, on `scratch`) to build one.
pub const BloomWrite = struct {
    parquet_version: schema.ParquetVersion = .v2,
    dictionary_bloom: bool = true,
    fpp: []const ?f64 = &.{},
    meta: ?*const schema.FileMetaData = null,
    scratch: ?std.mem.Allocator = null,

    pub fn at(self: BloomWrite, i: usize) ?f64 {
        return if (i < self.fpp.len) self.fpp[i] else null;
    }
};

/// A codec change requested of a byte copy. `scratch` backs the per-chunk
/// working memory, released once the chunk is written.
pub const Recompress = struct {
    codec: schema.CompressionCodec,
    zstd: compression.ZstdPlan = .{},
    scratch: std.mem.Allocator,
    /// The row group's chunks already recompressed by `recompressRowGroup`, indexed by column (null where the chunk
    /// keeps its codec), so row groups can be recompressed in parallel and still be placed in order by `copyRG`.
    done: ?[]const ?RecompressedChunk = null,
};

/// One page of a recompressed chunk: where it started in the source chunk,
/// and where it starts and how long it is (header included) in the output.
pub const PageMove = struct { src_off: usize, dst_off: usize, dst_len: usize };

pub const RecompressedChunk = struct {
    bytes: []const u8,
    pages: []const PageMove,
    /// Output size change of the page headers, which `total_uncompressed_size` counts.
    header_delta: i64,
};

/// Rewrite every page of a column chunk from codec `from` to codec `to`.
/// Only compressed payloads change: v1 data and dictionary pages are
/// recompressed whole, a v2 page's values section (its levels are never
/// compressed), and a v2 page stored uncompressed is kept as is. Each
/// header is re-emitted field by field with the new compressed size and
/// without its CRC, which covered the old bytes; everything else in it,
/// valid page statistics included, is carried over unchanged. Known writer defects remove affected records.
///
/// The result lives on `arena`; `scratch` holds the decompressed page, one
/// buffer reused across pages and freed before returning.
pub fn recompressChunk(
    arena: std.mem.Allocator,
    scratch: std.mem.Allocator,
    chunk: []const u8,
    from: schema.CompressionCodec,
    to: schema.CompressionCodec,
    zstd: compression.ZstdPlan,
    limit: usize,
    writer_policy: WriterStatsPolicy,
) !RecompressedChunk {
    var out: std.ArrayList(u8) = .empty;
    var pages: std.ArrayList(PageMove) = .empty;
    var raw: std.ArrayList(u8) = .empty;
    defer raw.deinit(scratch);
    var header_delta: i64 = 0;
    // The decoder's bound on the pages' claims (`column.chunkOptions`): a claim sizes the buffer it inflates into.
    var inflated: usize = 0;
    var pos: usize = 0;
    while (pos < chunk.len) {
        var r = thrift.Reader.init(chunk[pos..]);
        const h = try schema.PageHeader.read(&r);
        const hdr_len = r.pos;
        if (h.compressed_page_size < 0 or h.uncompressed_page_size < 0) return error.BadPageHeader;
        const body_len: usize = @intCast(h.compressed_page_size);
        if (body_len > chunk.len - pos - hdr_len) return error.TruncatedPage;
        const body = chunk[pos + hdr_len ..][0..body_len];
        inflated +|= @intCast(h.uncompressed_page_size);
        if (inflated > limit) return error.PageLargerThanChunk;
        const new_body = try recodePage(arena, scratch, &raw, &h, body, from, to, zstd);
        const raw_header = chunk[pos..][0..hdr_len];
        const new_hdr = try header_writer.rewrite(arena, raw_header, @intCast(new_body.len), writer_policy);
        try pages.append(arena, .{ .src_off = pos, .dst_off = out.items.len, .dst_len = new_hdr.len + new_body.len });
        try out.appendSlice(arena, new_hdr);
        try out.appendSlice(arena, new_body);
        header_delta += @as(i64, @intCast(new_hdr.len)) - @as(i64, @intCast(hdr_len));
        pos += hdr_len + body_len;
    }
    return .{ .bytes = out.items, .pages = pages.items, .header_delta = header_delta };
}

/// `recompressChunk`, each page written to `out` as soon as it is recoded rather than gathered: the result's `bytes`
/// is empty, its `pages` place what was written. `arena` holds the page moves; each page's own memory is released
/// before the next.
fn recompressChunkTo(
    arena: std.mem.Allocator,
    scratch: std.mem.Allocator,
    chunk: []const u8,
    from: schema.CompressionCodec,
    to: schema.CompressionCodec,
    zstd: compression.ZstdPlan,
    out: *streaming.SinkOut,
    timings: *Timings,
    limit: usize,
    writer_policy: WriterStatsPolicy,
) !RecompressedChunk {
    var pages: std.ArrayList(PageMove) = .empty;
    var raw: std.ArrayList(u8) = .empty;
    defer raw.deinit(scratch);
    var header_delta: i64 = 0;
    var inflated: usize = 0;
    var pos: usize = 0;
    var dst: usize = 0;
    while (pos < chunk.len) {
        var page_arena = std.heap.ArenaAllocator.init(scratch);
        defer page_arena.deinit();
        const pa = page_arena.allocator();
        var r = thrift.Reader.init(chunk[pos..]);
        const h = try schema.PageHeader.read(&r);
        const hdr_len = r.pos;
        if (h.compressed_page_size < 0 or h.uncompressed_page_size < 0) return error.BadPageHeader;
        const body_len: usize = @intCast(h.compressed_page_size);
        if (body_len > chunk.len - pos - hdr_len) return error.TruncatedPage;
        const body = chunk[pos + hdr_len ..][0..body_len];
        inflated +|= @intCast(h.uncompressed_page_size);
        if (inflated > limit) return error.PageLargerThanChunk;
        const new_body = try recodePage(pa, scratch, &raw, &h, body, from, to, zstd);
        const new_hdr = try header_writer.rewrite(pa, chunk[pos..][0..hdr_len], @intCast(new_body.len), writer_policy);
        try pages.append(arena, .{ .src_off = pos, .dst_off = dst, .dst_len = new_hdr.len + new_body.len });
        try timedWrite(out, new_hdr, timings);
        try timedWrite(out, new_body, timings);
        dst += new_hdr.len + new_body.len;
        header_delta += @as(i64, @intCast(new_hdr.len)) - @as(i64, @intCast(hdr_len));
        pos += hdr_len + body_len;
    }
    return .{ .bytes = &.{}, .pages = pages.items, .header_delta = header_delta };
}

/// A page's body in codec `to`: a v1 data or dictionary page recompressed whole, a compressed v2 page's values section
/// (its levels are never compressed), anything else as it is.
fn recodePage(
    arena: std.mem.Allocator,
    scratch: std.mem.Allocator,
    raw: *std.ArrayList(u8),
    h: *const schema.PageHeader,
    body: []const u8,
    from: schema.CompressionCodec,
    to: schema.CompressionCodec,
    zstd: compression.ZstdPlan,
) ![]const u8 {
    const raw_len: usize = @intCast(h.uncompressed_page_size);
    switch (h.type) {
        .DATA_PAGE, .DICTIONARY_PAGE => return recode(arena, scratch, raw, body, from, to, zstd, raw_len),
        .DATA_PAGE_V2 => {
            const v2 = h.data_page_header_v2 orelse return error.BadPageHeader;
            if (!v2.is_compressed) return body;
            if (v2.definition_levels_byte_length < 0 or v2.repetition_levels_byte_length < 0) return error.BadPageHeader;
            const levels: usize = @as(usize, @intCast(v2.definition_levels_byte_length)) +
                @as(usize, @intCast(v2.repetition_levels_byte_length));
            if (levels > body.len or levels > raw_len) return error.BadPageHeader;
            const values = try recode(arena, scratch, raw, body[levels..], from, to, zstd, raw_len - levels);
            const joined = try arena.alloc(u8, levels + values.len);
            @memcpy(joined[0..levels], body[0..levels]);
            @memcpy(joined[levels..], values);
            return joined;
        },
        // Index pages carry no compressed payload.
        .INDEX_PAGE => return body,
    }
}

fn recode(
    arena: std.mem.Allocator,
    scratch: std.mem.Allocator,
    raw: *std.ArrayList(u8),
    body: []const u8,
    from: schema.CompressionCodec,
    to: schema.CompressionCodec,
    zstd: compression.ZstdPlan,
    raw_len: usize,
) ![]const u8 {
    // UNCOMPRESSED decompresses to `body` itself: no copy into the buffer.
    if (from == .UNCOMPRESSED or raw_len == 0)
        return compression.compressPage(arena, try compression.decompress(arena, body, from, raw_len), to, zstd);
    try raw.resize(scratch, raw_len);
    try compression.decompressInto(body, from, raw.items);
    return compression.compressPage(arena, raw.items, to, zstd);
}

/// Recompress every chunk of `rg` that `kept` selects (null: all) and is not already in `codec`, one entry per
/// column, for `copyRG` to place through `Recompress.done`. Results live on `arena`; `scratch` is per-chunk working
/// memory, and must be safe to use from several threads when `workers` > 1. Safe to run for several row groups at
/// once: it only reads `src`. With `workers` > 1 the row group's pages are recompressed by that many threads and
/// placed in order: the same bytes as one thread writes.
pub fn recompressRowGroup(
    arena: std.mem.Allocator,
    scratch: std.mem.Allocator,
    rg: *const schema.RowGroup,
    src: RGSrc,
    kept: ?[]const bool,
    codec: schema.CompressionCodec,
    zstd: compression.ZstdPlan,
    timings: *Timings,
    workers: usize,
) ![]?RecompressedChunk {
    if (workers > 1) return recompressRowGroupPages(arena, scratch, rg, src, kept, codec, zstd, timings, workers);
    const done = try arena.alloc(?RecompressedChunk, rg.columns.items.len);
    @memset(done, null);
    for (rg.columns.items, done, 0..) |*chunk, *slot, ci| {
        if (kept) |k| if (ci >= k.len or !k[ci]) continue;
        const m = chunk.meta_data orelse return error.InvalidColumnOffsets;
        if (m.codec == codec) continue;
        const range = fastpath.chunkRange(&m, std.math.maxInt(usize)) orelse return error.InvalidColumnOffsets;
        const t0 = nowMonoNs();
        const bytes = try sourceBytes(src, range);
        const limit = column_mod.inflateLimit(m);
        slot.* = recompressChunk(arena, scratch, bytes, m.codec, codec, zstd, limit, m.writer_stats) catch |err|
            return reportRecompressError(err, m, codec);
        const t1 = nowMonoNs();
        timings.encode_ns += @intCast(t1 - t0);
        profile.record(timings.prof, .encode, t0, t1, profile.args(.{ 1, m.num_values }));
    }
    return done;
}

/// One page of a row group being recompressed: its header, where it sits in its chunk, and its new body once a
/// worker has recoded it.
const PageTask = struct {
    column: usize,
    header: schema.PageHeader,
    src_off: usize,
    hdr: []const u8,
    body: []const u8,
    from: schema.CompressionCodec,
    out: []const u8 = &.{},
};

const RecodeShared = struct {
    tasks: []PageTask,
    next: std.atomic.Value(usize) = .init(0),
    to: schema.CompressionCodec,
    zstd: compression.ZstdPlan,
    scratch: std.mem.Allocator,
    /// The lowest index of a task that failed, and its error: the failure one thread working in order meets first.
    failed: std.atomic.Value(usize) = .init(std.math.maxInt(usize)),
    errs: []?anyerror,
};

/// A worker of `recompressRowGroupPages`: recodes tasks until none are left, its bodies on its own arena.
const RecodeWorker = struct {
    shared: *RecodeShared,
    arena: std.heap.ArenaAllocator,
    busy_ns: u64 = 0,

    fn run(self: *RecodeWorker) void {
        const t0 = nowMonoNs();
        defer self.busy_ns = @intCast(nowMonoNs() - t0);
        const sh = self.shared;
        var raw: std.ArrayList(u8) = .empty;
        defer raw.deinit(sh.scratch);
        while (true) {
            const i = sh.next.fetchAdd(1, .monotonic);
            if (i >= sh.tasks.len) return;
            // Tasks past a failure are not needed: the failure is reported, nothing written.
            if (i > sh.failed.load(.monotonic)) continue;
            const t = &sh.tasks[i];
            t.out = recodePage(self.arena.allocator(), sh.scratch, &raw, &t.header, t.body, t.from, sh.to, sh.zstd) catch |err| {
                sh.errs[i] = err;
                _ = sh.failed.fetchMin(i, .monotonic);
                continue;
            };
        }
    }
};

/// `recompressRowGroup` over `workers` threads: every page of the row group's chunks is a task, recoded by whichever
/// worker takes it, then each chunk is assembled in page order as `recompressChunk` assembles it.
fn recompressRowGroupPages(
    arena: std.mem.Allocator,
    scratch: std.mem.Allocator,
    rg: *const schema.RowGroup,
    src: RGSrc,
    kept: ?[]const bool,
    codec: schema.CompressionCodec,
    zstd: compression.ZstdPlan,
    timings: *Timings,
    workers: usize,
) ![]?RecompressedChunk {
    const t0 = nowMonoNs();
    const done = try arena.alloc(?RecompressedChunk, rg.columns.items.len);
    @memset(done, null);
    var tasks: std.ArrayList(PageTask) = .empty;
    defer tasks.deinit(scratch);
    // A chunk that cannot be located or read, or a page header that does not parse, is reported after any failure in
    // the pages before it, as one thread meets them. Errors locating a chunk are returned as they are, as the serial
    // loop returns them; a page header's go through `reportRecompressError`, as `recompressChunk`'s do.
    var parse_err: ?struct { column: usize, err: anyerror, report: bool = true } = null;
    var values: i64 = 0;
    collect: for (rg.columns.items, 0..) |*chunk, ci| {
        if (kept) |k| if (ci >= k.len or !k[ci]) continue;
        const m = chunk.meta_data orelse {
            parse_err = .{ .column = ci, .err = error.InvalidColumnOffsets, .report = false };
            break :collect;
        };
        if (m.codec == codec) continue;
        const range = fastpath.chunkRange(&m, std.math.maxInt(usize)) orelse {
            parse_err = .{ .column = ci, .err = error.InvalidColumnOffsets, .report = false };
            break :collect;
        };
        const bytes = sourceBytes(src, range) catch |err| {
            parse_err = .{ .column = ci, .err = err, .report = false };
            break :collect;
        };
        values += m.num_values;
        const limit = column_mod.inflateLimit(m);
        var inflated: usize = 0;
        var pos: usize = 0;
        while (pos < bytes.len) {
            var r = thrift.Reader.init(bytes[pos..]);
            const h = schema.PageHeader.read(&r) catch |err| {
                parse_err = .{ .column = ci, .err = err };
                break :collect;
            };
            const hdr_len = r.pos;
            if (h.compressed_page_size < 0 or h.uncompressed_page_size < 0) {
                parse_err = .{ .column = ci, .err = error.BadPageHeader };
                break :collect;
            }
            const body_len: usize = @intCast(h.compressed_page_size);
            if (body_len > bytes.len - pos - hdr_len) {
                parse_err = .{ .column = ci, .err = error.TruncatedPage };
                break :collect;
            }
            inflated +|= @intCast(h.uncompressed_page_size);
            if (inflated > limit) {
                parse_err = .{ .column = ci, .err = error.PageLargerThanChunk };
                break :collect;
            }
            try tasks.append(scratch, .{
                .column = ci,
                .header = h,
                .src_off = pos,
                .hdr = bytes[pos..][0..hdr_len],
                .body = bytes[pos + hdr_len ..][0..body_len],
                .from = m.codec,
            });
            pos += hdr_len + body_len;
        }
        // An empty chunk still gets its (empty) entry, as `recompressChunk` gives it.
        done[ci] = .{ .bytes = &.{}, .pages = &.{}, .header_delta = 0 };
    }

    const errs = try scratch.alloc(?anyerror, tasks.items.len);
    defer scratch.free(errs);
    @memset(errs, null);
    var shared: RecodeShared = .{ .tasks = tasks.items, .to = codec, .zstd = zstd, .scratch = scratch, .errs = errs };
    const n_workers = @max(1, @min(workers, tasks.items.len));
    const pool = try scratch.alloc(RecodeWorker, n_workers);
    defer scratch.free(pool);
    for (pool) |*w| w.* = .{ .shared = &shared, .arena = .init(scratch) };
    defer for (pool) |*w| w.arena.deinit();
    const threads = try scratch.alloc(?std.Thread, n_workers);
    defer scratch.free(threads);
    @memset(threads, null);
    // The calling thread is worker 0; a worker that cannot start leaves its share to the others.
    for (pool[1..], threads[1..]) |*w, *t| t.* = std.Thread.spawn(.{}, RecodeWorker.run, .{w}) catch null;
    pool[0].run();
    for (threads[1..]) |t| if (t) |th| th.join();

    var busy: u64 = 0;
    for (pool) |w| busy += w.busy_ns;
    timings.encode_ns += busy;
    profile.record(timings.prof, .encode, t0, nowMonoNs(), profile.args(.{ done.len, values }));

    const failed = shared.failed.load(.monotonic);
    if (failed < tasks.items.len) {
        const m = rg.columns.items[tasks.items[failed].column].meta_data.?;
        return reportRecompressError(errs[failed].?, m, codec);
    }
    if (parse_err) |pe| {
        if (!pe.report) return pe.err;
        return reportRecompressError(pe.err, rg.columns.items[pe.column].meta_data.?, codec);
    }

    // Assemble each chunk in page order, as `recompressChunk` does.
    var i: usize = 0;
    while (i < tasks.items.len) {
        const ci = tasks.items[i].column;
        var out: std.ArrayList(u8) = .empty;
        var pages: std.ArrayList(PageMove) = .empty;
        var header_delta: i64 = 0;
        while (i < tasks.items.len and tasks.items[i].column == ci) : (i += 1) {
            const t = tasks.items[i];
            const policy = rg.columns.items[ci].meta_data.?.writer_stats;
            const new_hdr = try header_writer.rewrite(arena, t.hdr, @intCast(t.out.len), policy);
            try pages.append(arena, .{ .src_off = t.src_off, .dst_off = out.items.len, .dst_len = new_hdr.len + t.out.len });
            try out.appendSlice(arena, new_hdr);
            try out.appendSlice(arena, t.out);
            header_delta += @as(i64, @intCast(new_hdr.len)) - @as(i64, @intCast(t.hdr.len));
        }
        done[ci] = .{ .bytes = out.items, .pages = pages.items, .header_delta = header_delta };
    }
    return done;
}

fn reportRecompressError(err: anytype, m: schema.ColumnMetaData, to: schema.CompressionCodec) @TypeOf(err) {
    if (err == error.UnsupportedCodec) {
        const msg = "zpq: cannot recompress column `{s}` to {s}: its pages use {s}, which zpq cannot decompress\n";
        std.debug.print(msg, .{
            if (m.path_in_schema.items.len > 0) m.path_in_schema.items[m.path_in_schema.items.len - 1] else "?",
            @tagName(to),
            @tagName(m.codec),
        });
    }
    return err;
}

/// The output offset of the page that started `src_rel` bytes into the source chunk.
fn movedPage(pages: []const PageMove, src_rel: i64) ?PageMove {
    if (src_rel < 0) return null;
    const want: usize = @intCast(src_rel);
    // Pages are recorded in chunk order, so `src_off` ascends.
    var lo: usize = 0;
    var hi: usize = pages.len;
    while (lo < hi) {
        const mid = lo + (hi - lo) / 2;
        if (pages[mid].src_off < want) lo = mid + 1 else hi = mid;
    }
    return if (lo < pages.len and pages[lo].src_off == want) pages[lo] else null;
}

/// Where an offset `src_rel` bytes into the source chunk lands in the
/// recompressed one: the same page when it names a page start, else the next
/// page start (or the end). Writers do record offsets that name no page, such
/// as a `data_page_offset` of 0 on a chunk holding only a dictionary page.
fn relocated(re: RecompressedChunk, src_rel: i64) usize {
    if (movedPage(re.pages, src_rel)) |p| return p.dst_off;
    for (re.pages) |p| if (@as(i64, @intCast(p.src_off)) >= src_rel) return p.dst_off;
    // The end: past the last page (`bytes` is empty when the pages went straight to the sink).
    if (re.pages.len == 0) return re.bytes.len;
    const last = re.pages[re.pages.len - 1];
    return last.dst_off + last.dst_len;
}

/// Byte-copy path for a chunk whose codec differs from the requested one:
/// recompress its pages, write them at `out_offset`, and return the chunk's
/// metadata pointing at them. The page index follows the pages (ColumnIndex
/// unchanged, OffsetIndex locations remapped); if a location does not land on
/// a page start, both are dropped rather than written wrong.
fn copyChunkRecompressed(
    rc: Recompress,
    src: RGSrc,
    src_chunk: schema.ColumnChunk,
    slice: []const u8,
    src_start: usize,
    out: *streaming.SinkOut,
    timings: *Timings,
    emit_page_index: bool,
    /// This chunk recompressed ahead of time (`Recompress.done`), else null to recompress it here.
    precomputed: ?RecompressedChunk,
) !schema.ColumnChunk {
    var scratch_state = std.heap.ArenaAllocator.init(rc.scratch);
    defer scratch_state.deinit();
    const scratch = scratch_state.allocator();
    const m = src_chunk.meta_data.?;

    const new_start: i64 = @intCast(out.pos());
    const re = if (precomputed) |p| blk: {
        try timedWrite(out, p.bytes, timings);
        break :blk p;
    } else blk: {
        // Page by page into the sink: a codec that expands (to UNCOMPRESSED, say) never holds more than a page.
        const t_encode = nowMonoNs();
        const limit = column_mod.inflateLimit(m);
        const r = recompressChunkTo(
            scratch,
            rc.scratch,
            slice,
            m.codec,
            rc.codec,
            rc.zstd,
            out,
            timings,
            limit,
            m.writer_stats,
        ) catch |err|
            return reportRecompressError(err, m, rc.codec);
        const t_encode_end = nowMonoNs();
        timings.encode_ns += @intCast(t_encode_end - t_encode);
        profile.record(timings.prof, .encode, t_encode, t_encode_end, profile.args(.{ 1, m.num_values }));
        break :blk r;
    };
    const written: usize = @intCast(@as(i64, @intCast(out.pos())) - new_start);

    const src_base: i64 = @intCast(src_start);
    var chunk = src_chunk;
    chunk.absent = &.{};
    // Recompressing changes the bytes, not the values: the filter still holds.
    chunk.bloom_out = fastpath.carriedFilter(src.bytes, &src_chunk);
    var meta = m;
    meta.bloom_filter_offset = null;
    meta.bloom_filter_length = null;
    meta.codec = rc.codec;
    meta.total_compressed_size = @intCast(written);
    meta.total_uncompressed_size += re.header_delta;
    meta.data_page_offset = new_start + @as(i64, @intCast(relocated(re, m.data_page_offset - src_base)));
    if (m.dictionary_page_offset) |d| {
        meta.dictionary_page_offset = new_start + @as(i64, @intCast(relocated(re, d - src_base)));
    }
    // index_page_offset and the OffsetIndex locations are the file's word, unvalidated at footer open: one that is
    // not a page of this chunk (or not even an offset from it) drops what it would locate, as an unusable index is.
    meta.index_page_offset = if (m.index_page_offset) |ip| blk: {
        const rel = std.math.sub(i64, ip, src_base) catch break :blk null;
        break :blk if (movedPage(re.pages, rel)) |p| new_start + @as(i64, @intCast(p.dst_off)) else null;
    } else null;
    chunk.meta_data = meta;
    chunk.file_offset = meta.data_page_offset;
    chunk.offset_index_offset = null;
    chunk.offset_index_length = null;
    chunk.column_index_offset = null;
    chunk.column_index_length = null;
    if (!emit_page_index) return chunk;

    // The OffsetIndex is carried when every location remaps onto a page
    // start; the ColumnIndex only beside it, since its pages are found
    // through those locations.
    const oo = src_chunk.offset_index_offset orelse return chunk;
    const ol = src_chunk.offset_index_length orelse return chunk;
    const oi_bytes = originSlice(src, oo, ol) orelse return chunk;
    var oi_reader = thrift.Reader.init(oi_bytes);
    var oi = schema.OffsetIndex.read(scratch, &oi_reader) catch return chunk;
    for (oi.page_locations.items) |*loc| {
        const rel = std.math.sub(i64, loc.offset, src_base) catch return chunk;
        const p = movedPage(re.pages, rel) orelse return chunk;
        loc.offset = new_start + @as(i64, @intCast(p.dst_off));
        loc.compressed_page_size = @intCast(p.dst_len);
    }
    var oi_w = thrift.Writer.init(scratch);
    oi.write(&oi_w) catch return chunk;

    if (src_chunk.column_index_offset) |co| if (src_chunk.column_index_length) |cl| {
        if (originSlice(src, co, cl)) |ci_bytes| if (fastpath.reserializeColumnIndex(scratch, ci_bytes)) |ci_ser| {
            chunk.column_index_offset = @intCast(out.pos());
            chunk.column_index_length = @intCast(ci_ser.len);
            try out.write(ci_ser);
        };
    };
    chunk.offset_index_offset = @intCast(out.pos());
    chunk.offset_index_length = @intCast(oi_w.bytes().len);
    try out.write(oi_w.bytes());
    return chunk;
}
