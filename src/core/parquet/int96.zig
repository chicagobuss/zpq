//! INT96 (legacy Spark/Impala timestamp) decode → exact nanoseconds since the Unix epoch.
//!
//! INT96 is a deprecated 12-byte fixed-width physical type, in practice always
//! a timestamp: the low 8 bytes are nanoseconds-of-day (little-endian i64), the
//! high 4 bytes are the Julian day number (little-endian i32). The conversion
//! follows Spark's signed day count and parquet-format 2.14's INT96_TIMESTAMP_ORDER:
//!
//!   epoch_ns = (julian_day - 2440588) * 86_400_000_000_000 + nanos_of_day
//!
//! INT96 isn't wired through ColumnChunkReader (generic-on-T only handles the
//! standard widths), so the page-walk mirrors decimal.zig's FLBA path (PLAIN +
//! dictionary, DATA_PAGE V1/V2 with RLE-hybrid levels). The i128 lane holds every
//! instant exactly; the i64 fast path refuses values it cannot hold.

const std = @import("std");
const schema = @import("../schema.zig");
const filter_eval = @import("../filter/eval.zig");
const page_mod = @import("page.zig");
const hybrid_rle = @import("encoding/hybrid_rle.zig");

pub const Error = error{
    ShortDecode,
    UnexpectedPage,
    /// A second dictionary page in a chunk, or one after a data page: the format allows one, first (as
    /// `column.Error.UnexpectedDictionaryPage`).
    UnexpectedDictionaryPage,
    DictionaryMissing,
    UnsupportedEncoding,
    UnexpectedEndOfChunk,
    /// Read into 64 bits (`int96ToEpochNanos`), or written as 64-bit nanoseconds, an instant they cannot hold (before
    /// 1677 or after 2262), or a time of day outside one day; wrapped, either read as another instant.
    Int96OutOfRange,
    /// A repetition or definition level past the schema's maximum (see `hybrid_rle.decodeLevels`).
    LevelOutOfRange,
} || page_mod.Error || std.mem.Allocator.Error;

pub const INT96_WIDTH: usize = 12;
const JULIAN_TO_UNIX_EPOCH_DAYS: i64 = 2440588;
const NANOSECONDS_PER_DAY: i64 = 86_400_000_000_000;

/// 12 little-endian bytes → nanoseconds since the Unix epoch.
/// Caller guarantees `bytes.len >= 12`. Narrow only after exact signed arithmetic.
pub fn int96ToEpochNanos(bytes: []const u8) error{Int96OutOfRange}!i64 {
    const nanos_of_day = std.mem.readInt(i64, bytes[0..8], .little);
    if (nanos_of_day < 0 or nanos_of_day >= NANOSECONDS_PER_DAY) return error.Int96OutOfRange;
    // Widen before multiplying: the first i64 instant has a midnight outside i64.
    return std.math.cast(i64, int96ToNanos(bytes)) orelse error.Int96OutOfRange;
}

/// 12 little-endian bytes → nanoseconds since the Unix epoch, exactly, whatever the year: Spark writes 0001-01-01 and
/// 9999-12-31 as sentinels, past what 64 bits of nanoseconds hold. A time of day outside a day is added as it stands,
/// as other readers add it; every 12 bytes are some instant, and i128 holds them all.
pub fn int96ToNanos(bytes: []const u8) i128 {
    const nanos_of_day = std.mem.readInt(i64, bytes[0..8], .little);
    const julian_day = std.mem.readInt(i32, bytes[8..12], .little);
    const days_since_epoch = @as(i128, julian_day) - JULIAN_TO_UNIX_EPOCH_DAYS;
    return days_since_epoch * NANOSECONDS_PER_DAY + nanos_of_day;
}

fn toNanos(comptime T: type, bytes: []const u8) error{Int96OutOfRange}!T {
    return if (T == i128) int96ToNanos(bytes) else int96ToEpochNanos(bytes);
}

/// Decode an INT96 column chunk to exact epoch-nanoseconds (`int96ToNanos`).
pub fn decodeColumnAsI128Nanos(
    arena: std.mem.Allocator,
    chunk: []const u8,
    codec: schema.CompressionCodec,
    levels: schema.Levels,
    num_leaves: usize,
    byte_limit: usize,
) Error!filter_eval.ColumnT(i128) {
    return decodeColumnNanos(i128, arena, chunk, codec, levels, num_leaves, byte_limit);
}

/// Decode an INT96 column chunk to a ColumnT(i64) of epoch-nanoseconds.
pub fn decodeColumnAsI64Nanos(
    arena: std.mem.Allocator,
    chunk: []const u8,
    codec: schema.CompressionCodec,
    levels: schema.Levels,
    num_leaves: usize,
    /// `DecodeOptions.page_bytes_limit`.
    byte_limit: usize,
) Error!filter_eval.ColumnT(i64) {
    return decodeColumnNanos(i64, arena, chunk, codec, levels, num_leaves, byte_limit);
}

fn decodeColumnNanos(
    comptime T: type,
    arena: std.mem.Allocator,
    chunk: []const u8,
    codec: schema.CompressionCodec,
    levels: schema.Levels,
    num_leaves: usize,
    /// `DecodeOptions.page_bytes_limit`.
    byte_limit: usize,
) Error!filter_eval.ColumnT(T) {
    const values = try arena.alloc(T, num_leaves);

    var def_levels_buf: ?[]u32 = null;
    var rep_levels_buf: ?[]u32 = null;
    if (levels.max_def > 0) def_levels_buf = try arena.alloc(u32, num_leaves);
    if (levels.max_rep > 0) rep_levels_buf = try arena.alloc(u32, num_leaves);

    // Cached dict (INT96 values from the DICTIONARY_PAGE, converted to
    // epoch-nanos once so data-page index resolution is direct).
    var dict_i64: ?[]const T = null;

    var pr = page_mod.PageReader.init(chunk, codec, arena);
    pr.byte_limit = byte_limit;
    var written: usize = 0;
    var has_nulls = false;
    var data_seen = false;

    while (try pr.next()) |pg| {
        switch (pg.header.type) {
            .DICTIONARY_PAGE => {
                if (dict_i64 != null or data_seen) return error.UnexpectedDictionaryPage;
                const dh = pg.header.dictionary_page_header orelse return error.UnexpectedPage;
                const dict_count: usize = @intCast(dh.num_values);
                const expected_bytes = dict_count * INT96_WIDTH;
                if (pg.bytes.len < expected_bytes) return error.ShortDecode;
                const dict = try arena.alloc(T, dict_count);
                for (0..dict_count) |i| {
                    const off = i * INT96_WIDTH;
                    dict[i] = try toNanos(T, pg.bytes[off .. off + INT96_WIDTH]);
                }
                dict_i64 = dict;
            },
            .DATA_PAGE, .DATA_PAGE_V2 => {
                data_seen = true;
                const dict = dict_i64;
                try decodeDataPage(T, pg, dict, levels, values, def_levels_buf, rep_levels_buf, &written, &has_nulls);
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
        .has_nulls = has_nulls,
    };
}

fn decodeDataPage(
    comptime T: type,
    pg: page_mod.Page,
    dict_i64: ?[]const T,
    levels: schema.Levels,
    values_out: []T,
    def_levels_buf: ?[]u32,
    rep_levels_buf: ?[]u32,
    written: *usize,
    has_nulls: *bool,
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
    // The output buffers are sized from the chunk's num_values; a page claiming more than is left would write past
    // them.
    if (page_num_values > values_out.len - written.*) return error.ShortDecode;

    // Strip rep/def level prefixes (V1: u32-len-prefixed RLE; V2: fixed
    // byte lengths from the page header), leaving the values payload.
    var values_bytes = pg.bytes;
    if (pg.header.type == .DATA_PAGE) {
        if (levels.max_rep > 0) {
            if (values_bytes.len < 4) return error.UnexpectedEndOfChunk;
            const rep_len: usize = std.mem.readInt(u32, values_bytes[0..4], .little);
            if (rep_len > values_bytes.len - 4) return error.UnexpectedEndOfChunk;
            try decodeLevels(values_bytes[4 .. 4 + rep_len], @intCast(levels.max_rep), page_num_values, rep_levels_buf, written.*);
            values_bytes = values_bytes[4 + rep_len ..];
        }
        if (levels.max_def > 0) {
            if (values_bytes.len < 4) return error.UnexpectedEndOfChunk;
            const def_len: usize = std.mem.readInt(u32, values_bytes[0..4], .little);
            if (def_len > values_bytes.len - 4) return error.UnexpectedEndOfChunk;
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
        // Without level bytes the def levels would be left unwritten; an empty page is the only legitimate case.
        if (levels.max_def > 0 and def_len == 0 and page_num_values > 0) return error.ShortDecode;
        if (levels.max_rep > 0 and rep_len == 0 and page_num_values > 0) return error.ShortDecode;
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

    if (num_present < page_num_values) {
        has_nulls.* = true;
    }

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
                        values_out[idx] = try toNanos(T, values_bytes[off .. off + INT96_WIDTH]);
                        raw_pos += 1;
                    } else {
                        values_out[idx] = 0;
                    }
                }
            } else {
                for (0..page_num_values) |i| {
                    const off = i * INT96_WIDTH;
                    values_out[written.* + i] = try toNanos(T, values_bytes[off .. off + INT96_WIDTH]);
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
    hybrid_rle.decodeLevels(encoded, max, out[start .. start + expected]) catch |err| return switch (err) {
        error.CorruptLevels, error.LevelCountMismatch => error.ShortDecode,
        error.LevelOutOfRange => error.LevelOutOfRange,
    };
}

const testing = std.testing;

test "int96ToEpochNanos matches duckdb's reference math" {
    // 1970-01-01 00:00:00 → julian 2440588, 0 nanos-of-day → 0.
    var b: [12]u8 = @splat(0);
    std.mem.writeInt(u32, b[8..12], 2440588, .little);
    try testing.expectEqual(@as(i64, 0), try int96ToEpochNanos(&b));

    // One day later, +1 second into the day.
    std.mem.writeInt(i64, b[0..8], 1_000_000_000, .little); // 1s in nanos
    std.mem.writeInt(u32, b[8..12], 2440589, .little);
    try testing.expectEqual(@as(i64, 86_400_000_000_000 + 1_000_000_000), try int96ToEpochNanos(&b));
}

test "regression: an INT96 instant past the nanosecond range, or a time of day outside one day, is refused" {
    var b: [12]u8 = @splat(0);
    // 0001-01-01 (julian 1721426) and 9999-12-31 (julian 5373484), Spark's sentinels: wrapped, they read as 1754
    // and as some other year.
    for ([_]u32{ 1721426, 5373484, 0, std.math.maxInt(u32) }) |day| {
        std.mem.writeInt(u32, b[8..12], day, .little);
        try testing.expectError(error.Int96OutOfRange, int96ToEpochNanos(&b));
    }
    std.mem.writeInt(u32, b[8..12], 2440588, .little);
    for ([_]i64{ -1, NANOSECONDS_PER_DAY }) |tod| {
        std.mem.writeInt(i64, b[0..8], tod, .little);
        try testing.expectError(error.Int96OutOfRange, int96ToEpochNanos(&b));
    }
    // The last instant i64 nanoseconds hold, 2262-04-11 23:47:16.854775807, still reads.
    const last_day: u32 = @intCast(@divFloor(std.math.maxInt(i64), NANOSECONDS_PER_DAY) + JULIAN_TO_UNIX_EPOCH_DAYS);
    std.mem.writeInt(u32, b[8..12], last_day, .little);
    std.mem.writeInt(i64, b[0..8], @mod(std.math.maxInt(i64), NANOSECONDS_PER_DAY), .little);
    try testing.expectEqual(@as(i64, std.math.maxInt(i64)), try int96ToEpochNanos(&b));
}

test "regression: an INT96 instant reads exactly in any year, as 64-bit nanoseconds do where they reach" {
    var b: [12]u8 = @splat(0);
    const Case = struct { day: u32, tod: i64 };
    // 0001-01-01, 1500-01-01, either side of the first and the last instant 64-bit nanoseconds hold
    // (1677-09-21 00:12:43.145224192, 2262-04-11 23:47:16.854775807), and 9999-12-31.
    for ([_]Case{
        .{ .day = 1721426, .tod = 0 },
        .{ .day = 2268924, .tod = 0 },
        .{ .day = 2333836, .tod = 763145224191 },
        .{ .day = 2333836, .tod = 763145224192 },
        .{ .day = 2547339, .tod = 85636854775807 },
        .{ .day = 2547339, .tod = 85636854775808 },
        .{ .day = 5373484, .tod = NANOSECONDS_PER_DAY - 1 },
        .{ .day = 2440588, .tod = -1 },
    }) |c| {
        std.mem.writeInt(i64, b[0..8], c.tod, .little);
        std.mem.writeInt(u32, b[8..12], c.day, .little);
        const want = (@as(i128, c.day) - JULIAN_TO_UNIX_EPOCH_DAYS) * NANOSECONDS_PER_DAY + c.tod;
        try testing.expectEqual(want, int96ToNanos(&b));
        // Where 64 bits answer, the two agree to the nanosecond, so in-range answers are what they were.
        if (int96ToEpochNanos(&b)) |w| try testing.expectEqual(want, w) else |_| {}
    }
    std.mem.writeInt(i64, b[0..8], 0, .little);
    std.mem.writeInt(u32, b[8..12], 1721426, .little);
    try testing.expectEqual(@as(i128, -62135596800) * 1_000_000_000, int96ToNanos(&b));
}

/// A chunk of uncompressed pages: `true` a PLAIN dictionary page of `n` entries, `false` a V1 data page of `n`
/// RLE_DICTIONARY indices; each with `body`.
fn chunkForTest(arena: std.mem.Allocator, pages: []const struct { bool, i32, []const u8 }) ![]const u8 {
    const thrift = @import("../thrift.zig");
    var chunk: std.ArrayList(u8) = .empty;
    for (pages) |pg| {
        const dictionary, const n, const body = pg;
        const header = schema.PageHeader{
            .type = if (dictionary) .DICTIONARY_PAGE else .DATA_PAGE,
            .uncompressed_page_size = @intCast(body.len),
            .compressed_page_size = @intCast(body.len),
            .crc = null,
            .data_page_header = if (dictionary) null else .{
                .num_values = n,
                .encoding = .RLE_DICTIONARY,
                .definition_level_encoding = .RLE,
                .repetition_level_encoding = .RLE,
            },
            .dictionary_page_header = if (!dictionary)
                null
            else
                .{ .num_values = n, .encoding = .PLAIN, .is_sorted = null },
            .data_page_header_v2 = null,
        };
        var w = thrift.Writer.init(arena);
        try header.write(&w);
        try chunk.appendSlice(arena, w.bytes());
        try chunk.appendSlice(arena, body);
    }
    return chunk.items;
}

test "a second INT96 dictionary page, after data or after the first, is a corrupt chunk" {
    var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_state.deinit();
    const a = arena_state.allocator();
    var dict: [24]u8 = @splat(0); // two INT96 entries: 1970-01-01, a day zpq can read
    for (0..2) |i| std.mem.writeInt(u32, dict[i * 12 + 8 ..][0..4], 2440588, .little);
    const data = [_]u8{ 1, 2 << 1, 0 }; // two indices, bit width 1, one RLE run of 0
    const levels: schema.Levels = .{ .max_def = 0, .max_rep = 0 };
    const dict_page = .{ true, 2, @as([]const u8, &dict) };
    const data_page = .{ false, 2, @as([]const u8, &data) };
    const back_to_back = try chunkForTest(a, &.{ dict_page, dict_page, data_page });
    const twice = decodeColumnAsI64Nanos(a, back_to_back, .UNCOMPRESSED, levels, 2, std.math.maxInt(usize));
    try testing.expectError(error.UnexpectedDictionaryPage, twice);
    const after_data = try chunkForTest(a, &.{ dict_page, data_page, dict_page, data_page });
    const late = decodeColumnAsI64Nanos(a, after_data, .UNCOMPRESSED, levels, 4, std.math.maxInt(usize));
    try testing.expectError(error.UnexpectedDictionaryPage, late);
    // One dictionary, first: decodes.
    const good = try chunkForTest(a, &.{ .{ true, 2, &dict }, .{ false, 2, &data } });
    _ = try decodeColumnAsI64Nanos(a, good, .UNCOMPRESSED, levels, 2, std.math.maxInt(usize));
}

test "INT96 signed Julian days stay exact and narrow only when representable" {
    var b: [12]u8 = @splat(0);
    for ([_]struct { day: i32, want: i128 }{
        .{ .day = -1, .want = -210866889600000000000 },
        .{ .day = std.math.minInt(i32), .want = -185753453990400000000000 },
        .{ .day = std.math.maxInt(i32), .want = 185331720297600000000000 },
    }) |c| {
        std.mem.writeInt(i32, b[8..12], c.day, .little);
        try testing.expectEqual(c.want, int96ToNanos(&b));
        try testing.expectError(error.Int96OutOfRange, int96ToEpochNanos(&b));
    }
    const first = std.math.minInt(i64);
    const first_day: i32 = @intCast(@divFloor(first, NANOSECONDS_PER_DAY) + JULIAN_TO_UNIX_EPOCH_DAYS);
    std.mem.writeInt(i32, b[8..12], first_day, .little);
    std.mem.writeInt(i64, b[0..8], @mod(first, NANOSECONDS_PER_DAY), .little);
    try testing.expectEqual(first, try int96ToEpochNanos(&b));
    std.mem.writeInt(i64, b[0..8], @mod(first, NANOSECONDS_PER_DAY) - 1, .little);
    try testing.expectError(error.Int96OutOfRange, int96ToEpochNanos(&b));
}
