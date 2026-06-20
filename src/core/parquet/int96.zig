//! INT96 (legacy Spark/Impala timestamp) decode → i64 nanoseconds since the
//! Unix epoch.
//!
//! INT96 is a deprecated 12-byte fixed-width physical type, in practice always
//! a timestamp: the low 8 bytes are nanoseconds-of-day (little-endian i64), the
//! high 4 bytes are the Julian day number (little-endian u32). The conversion
//! is lifted verbatim from DuckDB's parquet extension
//! (extension/parquet/parquet_timestamp.cpp, ImpalaTimestampToNanoseconds):
//!
//!   epoch_ns = (julian_day - 2440588) * 86_400_000_000_000 + nanos_of_day
//!
//! INT96 isn't wired through ColumnChunkReader (generic-on-T only handles the
//! standard widths), so the page-walk mirrors decimal.zig's FLBA path (PLAIN +
//! dictionary, DATA_PAGE V1/V2 with RLE-hybrid levels). We decode to an i64
//! column of epoch-nanos; the filter parser then treats an INT96 column as
//! TIMESTAMP(nanos) and aggregates widen it like any i64.

const std = @import("std");
const schema = @import("../schema.zig");
const filter_eval = @import("../filter/eval.zig");
const page_mod = @import("page.zig");
const hybrid_rle = @import("encoding/hybrid_rle.zig");

pub const Error = error{
    ShortDecode,
    UnexpectedPage,
    DictionaryMissing,
    UnsupportedEncoding,
    UnexpectedEndOfChunk,
} || page_mod.Error || std.mem.Allocator.Error;

const INT96_WIDTH: usize = 12;
const JULIAN_TO_UNIX_EPOCH_DAYS: i64 = 2440588;
const NANOSECONDS_PER_DAY: i64 = 86_400_000_000_000;

/// 12 little-endian bytes → nanoseconds since the Unix epoch.
/// Caller guarantees `bytes.len >= 12`. (DuckDB's ImpalaTimestamp math.)
pub fn int96ToEpochNanos(bytes: []const u8) i64 {
    const nanos_of_day = std.mem.readInt(i64, bytes[0..8], .little);
    const julian_day = std.mem.readInt(u32, bytes[8..12], .little);
    const days_since_epoch = @as(i64, julian_day) - JULIAN_TO_UNIX_EPOCH_DAYS;
    return days_since_epoch * NANOSECONDS_PER_DAY + nanos_of_day;
}

/// Decode an INT96 column chunk to a ColumnT(i64) of epoch-nanoseconds.
pub fn decodeColumnAsI64Nanos(
    arena: std.mem.Allocator,
    chunk: []const u8,
    codec: schema.CompressionCodec,
    levels: schema.Levels,
    num_leaves: usize,
) Error!filter_eval.ColumnT(i64) {
    const values = try arena.alloc(i64, num_leaves);

    var def_levels_buf: ?[]u32 = null;
    var rep_levels_buf: ?[]u32 = null;
    if (levels.max_def > 0) def_levels_buf = try arena.alloc(u32, num_leaves);
    if (levels.max_rep > 0) rep_levels_buf = try arena.alloc(u32, num_leaves);

    // Cached dict (INT96 values from the DICTIONARY_PAGE, converted to
    // epoch-nanos once so data-page index resolution is direct).
    var dict_i64: ?[]const i64 = null;

    var pr = page_mod.PageReader.init(chunk, codec, arena);
    var written: usize = 0;

    while (try pr.next()) |pg| {
        switch (pg.header.type) {
            .DICTIONARY_PAGE => {
                const dh = pg.header.dictionary_page_header orelse return error.UnexpectedPage;
                const dict_count: usize = @intCast(dh.num_values);
                const expected_bytes = dict_count * INT96_WIDTH;
                if (pg.bytes.len < expected_bytes) return error.ShortDecode;
                const dict = try arena.alloc(i64, dict_count);
                for (0..dict_count) |i| {
                    const off = i * INT96_WIDTH;
                    dict[i] = int96ToEpochNanos(pg.bytes[off .. off + INT96_WIDTH]);
                }
                dict_i64 = dict;
            },
            .DATA_PAGE, .DATA_PAGE_V2 => {
                try decodeDataPage(pg, dict_i64, levels, values, def_levels_buf, rep_levels_buf, &written);
            },
            .INDEX_PAGE => continue,
        }
    }

    if (written != num_leaves) return error.ShortDecode;

    return .{
        .values = values,
        .def_levels = def_levels_buf,
        .max_def = @intCast(levels.max_def),
        .rep_levels = rep_levels_buf,
        .max_rep = @intCast(levels.max_rep),
    };
}

fn decodeDataPage(
    pg: page_mod.Page,
    dict_i64: ?[]const i64,
    levels: schema.Levels,
    values_out: []i64,
    def_levels_buf: ?[]u32,
    rep_levels_buf: ?[]u32,
    written: *usize,
) Error!void {
    var page_num_values: usize = 0;
    var encoding: schema.Encoding = .PLAIN;
    if (pg.header.type == .DATA_PAGE) {
        const dph = pg.header.data_page_header orelse return error.UnexpectedPage;
        page_num_values = @intCast(dph.num_values);
        encoding = dph.encoding;
    } else {
        const dph = pg.header.data_page_header_v2 orelse return error.UnexpectedPage;
        page_num_values = @intCast(dph.num_values);
        encoding = dph.encoding;
    }

    // Strip rep/def level prefixes (V1: u32-len-prefixed RLE; V2: fixed
    // byte lengths from the page header), leaving the values payload.
    var values_bytes = pg.bytes;
    if (pg.header.type == .DATA_PAGE) {
        if (levels.max_rep > 0) {
            if (values_bytes.len < 4) return error.UnexpectedEndOfChunk;
            const rep_len = std.mem.readInt(u32, values_bytes[0..4], .little);
            if (4 + rep_len > values_bytes.len) return error.UnexpectedEndOfChunk;
            try decodeLevels(values_bytes[4 .. 4 + rep_len], @intCast(levels.max_rep), page_num_values, rep_levels_buf, written.*);
            values_bytes = values_bytes[4 + rep_len ..];
        }
        if (levels.max_def > 0) {
            if (values_bytes.len < 4) return error.UnexpectedEndOfChunk;
            const def_len = std.mem.readInt(u32, values_bytes[0..4], .little);
            if (4 + def_len > values_bytes.len) return error.UnexpectedEndOfChunk;
            try decodeLevels(values_bytes[4 .. 4 + def_len], @as(u32, @intCast(levels.max_def)), page_num_values, def_levels_buf, written.*);
            values_bytes = values_bytes[4 + def_len ..];
        }
    } else {
        const v2 = pg.header.data_page_header_v2 orelse return error.UnexpectedPage;
        const rep_len: usize = @intCast(v2.repetition_levels_byte_length);
        const def_len: usize = @intCast(v2.definition_levels_byte_length);
        if (rep_len + def_len > values_bytes.len) return error.UnexpectedEndOfChunk;
        if (levels.max_rep > 0 and rep_len > 0) {
            try decodeLevels(values_bytes[0..rep_len], @as(u32, @intCast(levels.max_rep)), page_num_values, rep_levels_buf, written.*);
        }
        if (levels.max_def > 0 and def_len > 0) {
            try decodeLevels(values_bytes[rep_len .. rep_len + def_len], @as(u32, @intCast(levels.max_def)), page_num_values, def_levels_buf, written.*);
        }
        values_bytes = values_bytes[rep_len + def_len ..];
    }

    const num_present: usize = if (def_levels_buf) |dl| blk: {
        var c: usize = 0;
        for (dl[written.* .. written.* + page_num_values]) |d| if (d == levels.max_def) {
            c += 1;
        };
        break :blk c;
    } else page_num_values;

    switch (encoding) {
        .PLAIN => {
            const expected = num_present * INT96_WIDTH;
            if (values_bytes.len < expected) return error.ShortDecode;
            if (def_levels_buf) |dl| {
                var raw_pos: usize = 0;
                for (0..page_num_values) |i| {
                    const idx = written.* + i;
                    if (dl[idx] == levels.max_def) {
                        const off = raw_pos * INT96_WIDTH;
                        values_out[idx] = int96ToEpochNanos(values_bytes[off .. off + INT96_WIDTH]);
                        raw_pos += 1;
                    } else {
                        values_out[idx] = 0;
                    }
                }
            } else {
                for (0..page_num_values) |i| {
                    const off = i * INT96_WIDTH;
                    values_out[written.* + i] = int96ToEpochNanos(values_bytes[off .. off + INT96_WIDTH]);
                }
            }
        },
        .PLAIN_DICTIONARY, .RLE_DICTIONARY => {
            const dict = dict_i64 orelse return error.DictionaryMissing;
            if (values_bytes.len == 0) return error.ShortDecode;
            const bit_width = values_bytes[0];
            if (bit_width > 32) return error.UnsupportedEncoding;
            var idx_dec = hybrid_rle.HybridRleDecoder.init(values_bytes[1..], bit_width);
            var idx_buf: [256]u32 = undefined;
            var present_decoded: usize = 0;
            var page_pos: usize = 0;
            while (present_decoded < num_present) {
                const want = @min(num_present - present_decoded, idx_buf.len);
                const n = idx_dec.decode(idx_buf[0..want]) catch return error.ShortDecode;
                if (n == 0) return error.ShortDecode;
                if (def_levels_buf) |dl| {
                    var k: usize = 0;
                    while (k < n) {
                        while (page_pos < page_num_values and dl[written.* + page_pos] != levels.max_def) {
                            values_out[written.* + page_pos] = 0;
                            page_pos += 1;
                        }
                        if (page_pos >= page_num_values) return error.ShortDecode;
                        if (idx_buf[k] >= dict.len) return error.UnexpectedPage;
                        values_out[written.* + page_pos] = dict[idx_buf[k]];
                        page_pos += 1;
                        k += 1;
                    }
                } else {
                    for (0..n) |k| {
                        if (idx_buf[k] >= dict.len) return error.UnexpectedPage;
                        values_out[written.* + present_decoded + k] = dict[idx_buf[k]];
                    }
                }
                present_decoded += n;
            }
            if (def_levels_buf) |dl| {
                while (page_pos < page_num_values) {
                    if (dl[written.* + page_pos] != levels.max_def) values_out[written.* + page_pos] = 0;
                    page_pos += 1;
                }
            }
        },
        else => return error.UnsupportedEncoding,
    }

    written.* += page_num_values;
}

fn decodeLevels(encoded: []const u8, max: u32, expected: usize, out_buf: ?[]u32, start: usize) Error!void {
    const out = out_buf orelse return;
    const bit_width = bitWidthFor(max);
    var dec = hybrid_rle.HybridRleDecoder.init(encoded, bit_width);
    const got = dec.decode(out[start .. start + expected]) catch return error.ShortDecode;
    if (got != expected) return error.ShortDecode;
}

fn bitWidthFor(max: u32) u8 {
    if (max == 0) return 0;
    return @intCast(32 - @clz(max));
}

const testing = std.testing;

test "int96ToEpochNanos matches duckdb's reference math" {
    // 1970-01-01 00:00:00 → julian 2440588, 0 nanos-of-day → 0.
    var b = [_]u8{0} ** 12;
    std.mem.writeInt(u32, b[8..12], 2440588, .little);
    try testing.expectEqual(@as(i64, 0), int96ToEpochNanos(&b));

    // One day later, +1 second into the day.
    std.mem.writeInt(i64, b[0..8], 1_000_000_000, .little); // 1s in nanos
    std.mem.writeInt(u32, b[8..12], 2440589, .little);
    try testing.expectEqual(@as(i64, 86_400_000_000_000 + 1_000_000_000), int96ToEpochNanos(&b));
}
