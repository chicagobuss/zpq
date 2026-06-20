//! DECIMAL physical-type support.
//!
//! Parquet's Decimal logical type sits atop four physical encodings
//! (INT32 ≤ 9 digits, INT64 ≤ 18 digits, FIXED_LEN_BYTE_ARRAY ≤ 38
//! digits, BYTE_ARRAY rare). This module recognises the type from
//! schema and decodes the on-disk bytes to `f64` regardless of
//! backing.
//!
//! Why f64 and not a Decimal lane:
//! - Reuses the existing `f64` column variant through filter / agg /
//!   expression evaluators with zero new plumbing.
//! - Reuses every aggregate primitive we already have.
//! - Output side is trivial (DOUBLE PLAIN, already supported).
//!
//! The cost: f64 has ~15.95 decimal digits of precision; sums over
//! tens of millions of small-cent values may diverge from an exact
//! Decimal engine in the 7th-8th decimal. A true Decimal-typed lane
//! is a follow-up if a workload demonstrates the loss matters.
//!
//! Byte parsing: parquet stores Decimal values big-endian, two's-
//! complement. INT32 / INT64 backings still use parquet's standard
//! little-endian fixed-width int wire format and only the *logical*
//! interpretation differs (apply scale). FLBA backings use big-
//! endian raw bytes of `type_length` width.

const std = @import("std");
const schema = @import("../schema.zig");
const filter_eval = @import("../filter/eval.zig");
const page_mod = @import("page.zig");
const plain = @import("encoding/plain.zig");
const rle_dict = @import("encoding/rle_dict.zig");
const hybrid_rle = @import("encoding/hybrid_rle.zig");

pub const Error = error{
    UnsupportedDecimalEncoding,
    UnsupportedDecimalPhysicalType,
    DecimalByteWidthTooLarge,
    DictionaryMissing,
    ShortDecode,
    UnexpectedPage,
    UnsupportedEncoding,
} || page_mod.Error || std.mem.Allocator.Error;

/// Maximum FLBA byte width we accept. 16 bytes = i128, which covers
/// precision ≤ 38 (the parquet spec's max). Widths > 16 require i192+
/// bignum handling; punt with a clear error.
pub const MAX_FLBA_BYTE_WIDTH: usize = 16;

/// What we learn from inspecting a column's SchemaElement when the
/// column carries DECIMAL logical type.
pub const Kind = struct {
    scale: i32,
    precision: i32,
    physical: schema.Type,
    /// For FIXED_LEN_BYTE_ARRAY, the column's type_length. Zero for
    /// INT32 / INT64 / BYTE_ARRAY (not applicable).
    byte_width: u8,
};

/// Recognise DECIMAL on a SchemaElement. Returns null if the column
/// isn't a Decimal.
///
/// Two encodings to support:
/// 1. Modern (parquet-format ≥ 2.4): `logical_type = DECIMAL{scale,precision}`.
/// 2. Legacy (Spark, older writers): `converted_type = DECIMAL` with
///    `scale` / `precision` fields directly on the SchemaElement.
///
/// Real production data still arrives in legacy form regularly, so we
/// recognise both. When both are present we prefer the LogicalType
/// payload (newer writers set both for compat; the LogicalType wins).
pub fn kindFromSchema(elem: *const schema.SchemaElement) ?Kind {
    const phys = elem.type orelse return null;
    const tl: u8 = if (elem.type_length) |v| @intCast(v) else 0;

    if (elem.logical_type) |lt| switch (lt) {
        .DECIMAL => |d| return Kind{
            .scale = d.scale,
            .precision = d.precision,
            .physical = phys,
            .byte_width = tl,
        },
        else => {},
    };

    if (elem.converted_type) |ct| if (ct == .DECIMAL) {
        const scale = elem.scale orelse 0;
        const precision = elem.precision orelse return null;
        return Kind{
            .scale = scale,
            .precision = precision,
            .physical = phys,
            .byte_width = tl,
        };
    };

    return null;
}

/// Precomputed powers of ten as f64. Exact for 0..22; rounded
/// thereafter (f64's mantissa runs out at 10^22). Used to apply
/// Decimal scale: real_value = raw / pow10[scale].
const pow10_f64: [39]f64 = blk: {
    @setEvalBranchQuota(200);
    var arr: [39]f64 = undefined;
    var v: f64 = 1.0;
    var i: usize = 0;
    while (i < 39) : (i += 1) {
        arr[i] = v;
        v *= 10.0;
    }
    break :blk arr;
};

/// Big-endian, two's-complement, variable-width signed → i128.
/// Sign-extends the high byte before reading. Caller guarantees
/// `bytes.len <= 16`.
pub fn flbaToI128(bytes: []const u8) i128 {
    if (bytes.len == 0) return 0;
    if (bytes.len >= 16) {
        // Use exactly the last 16 bytes — anything beyond is just
        // sign-extension we'd discard anyway.
        return std.mem.readInt(i128, bytes[bytes.len - 16 ..][0..16], .big);
    }
    var buf: [16]u8 = undefined;
    const fill: u8 = if (bytes[0] & 0x80 != 0) 0xff else 0x00;
    @memset(buf[0 .. 16 - bytes.len], fill);
    @memcpy(buf[16 - bytes.len ..], bytes);
    return std.mem.readInt(i128, &buf, .big);
}

/// Apply Decimal scale to a raw value. Branch-free conversion.
pub inline fn applyScaleI128(raw: i128, scale: i32) f64 {
    const f: f64 = @floatFromInt(raw);
    if (scale <= 0) return f;
    const s: usize = @intCast(scale);
    if (s >= pow10_f64.len) return f / std.math.pow(f64, 10.0, @floatFromInt(s));
    return f / pow10_f64[s];
}

pub inline fn applyScaleInt(comptime T: type, raw: T, scale: i32) f64 {
    return applyScaleI128(@intCast(raw), scale);
}

/// Vectorized `applyScaleInt` over a whole buffer. For the common case
/// (0 < scale < pow10 table — i.e. every real-world DECIMAL) this is a
/// SIMD vector divide: `@floatFromInt` widens 8 raw ints to f64 lanes,
/// then one vector divide by the splatted 10^scale. IEEE divide is
/// per-lane independent, so each lane is bit-identical to the scalar
/// `f / pow10[scale]` — decimal results are unchanged, only throughput
/// differs (the scalar loop did one f64 divide per value; this does 8
/// per instruction on AVX). scale<=0 / out-of-table scales fall back to
/// the scalar path.
fn applyScaleSimd(comptime T: type, raw: []const T, scale: i32, out: []f64) void {
    if (scale <= 0 or scale >= pow10_f64.len) {
        for (raw, 0..) |v, i| out[i] = applyScaleInt(T, v, scale);
        return;
    }
    const LANES = 8;
    const divisor: @Vector(LANES, f64) = @splat(pow10_f64[@intCast(scale)]);
    var i: usize = 0;
    while (i + LANES <= raw.len) : (i += LANES) {
        const iv: @Vector(LANES, T) = raw[i..][0..LANES].*;
        const fv: @Vector(LANES, f64) = @floatFromInt(iv);
        out[i..][0..LANES].* = fv / divisor;
    }
    for (i..raw.len) |j| out[j] = applyScaleInt(T, raw[j], scale);
}

/// High-level entry point. Decode a DECIMAL column to a ColumnT(f64)
/// regardless of physical backing. Handles PLAIN + PLAIN_DICTIONARY
/// (the two encodings we see in practice).
///
/// `chunk` is the whole column-chunk bytes (dictionary page + data
/// pages back-to-back). `codec` is the column's compression codec.
/// `levels` carries max_def / max_rep for nullable / nested columns.
pub fn decodeColumnAsF64(
    arena: std.mem.Allocator,
    chunk: []const u8,
    codec: schema.CompressionCodec,
    levels: schema.Levels,
    num_leaves: usize,
    kind: Kind,
) Error!filter_eval.ColumnT(f64) {
    return switch (kind.physical) {
        .INT32 => try decodeIntBacked(i32, arena, chunk, codec, levels, num_leaves, kind.scale),
        .INT64 => try decodeIntBacked(i64, arena, chunk, codec, levels, num_leaves, kind.scale),
        .FIXED_LEN_BYTE_ARRAY => try decodeFlbaBacked(arena, chunk, codec, levels, num_leaves, kind),
        .BYTE_ARRAY => try decodeByteArrayBacked(arena, chunk, codec, levels, num_leaves, kind.scale),
        else => return error.UnsupportedDecimalPhysicalType,
    };
}

// ----- INT32 / INT64-backed -----
//
// The wire format is identical to a regular fixed-width int column.
// We lean on the existing column reader for that, then map values to
// f64 with scale.

const column_mod = @import("column.zig");

fn decodeIntBacked(
    comptime T: type,
    arena: std.mem.Allocator,
    chunk: []const u8,
    codec: schema.CompressionCodec,
    levels: schema.Levels,
    num_leaves: usize,
    scale: i32,
) Error!filter_eval.ColumnT(f64) {
    // Decode the raw ints first using the existing column reader
    // shape. This mirrors decodeColumnT's body in consumer.zig but
    // without the indirection — we need the raw values to apply scale
    // in a second pass.
    const raw_values = try arena.alloc(T, num_leaves);
    var reader = column_mod.ColumnChunkReader(T).init(chunk, codec, levels, arena);

    var def_levels_buf: ?[]u32 = null;
    var rep_levels_buf: ?[]u32 = null;

    if (levels.max_rep > 0) {
        const dl = try arena.alloc(u32, num_leaves);
        const rl = try arena.alloc(u32, num_leaves);
        var written: usize = 0;
        while (written < num_leaves) {
            const n = reader.decodeWithRepLevels(raw_values[written..], dl[written..], rl[written..]) catch return error.ShortDecode;
            if (n == 0) break;
            written += n;
        }
        if (written != num_leaves) return error.ShortDecode;
        def_levels_buf = dl;
        rep_levels_buf = rl;
    } else if (levels.max_def > 0) {
        const dl = try arena.alloc(u32, num_leaves);
        var written: usize = 0;
        while (written < num_leaves) {
            const n = reader.decodeWithLevels(raw_values[written..], dl[written..]) catch return error.ShortDecode;
            if (n == 0) break;
            written += n;
        }
        if (written != num_leaves) return error.ShortDecode;
        def_levels_buf = dl;
    } else {
        var written: usize = 0;
        while (written < num_leaves) {
            const n = reader.decode(raw_values[written..]) catch return error.ShortDecode;
            if (n == 0) break;
            written += n;
        }
        if (written != num_leaves) return error.ShortDecode;
    }

    // Apply scale into a new f64 buffer.
    const values = try arena.alloc(f64, num_leaves);
    applyScaleSimd(T, raw_values, scale, values);

    return .{
        .values = values,
        .def_levels = def_levels_buf,
        .max_def = @intCast(levels.max_def),
        .rep_levels = rep_levels_buf,
        .max_rep = @intCast(levels.max_rep),
    };
}

// ----- BYTE_ARRAY-backed -----
//
// Each value is a length-prefixed byte string of big-endian two's-
// complement. ColumnChunkReader([]const u8) already speaks BYTE_ARRAY
// (PLAIN length-prefix + PLAIN_DICTIONARY + DELTA_*), so we lean on
// it for the wire-format work and apply the scale conversion in a
// second pass. Variable-width per row means each slice can be 0..16
// bytes; we cap at MAX_FLBA_BYTE_WIDTH per the i128 ceiling.

fn decodeByteArrayBacked(
    arena: std.mem.Allocator,
    chunk: []const u8,
    codec: schema.CompressionCodec,
    levels: schema.Levels,
    num_leaves: usize,
    scale: i32,
) Error!filter_eval.ColumnT(f64) {
    const raw_slices = try arena.alloc([]const u8, num_leaves);
    var reader = column_mod.ColumnChunkReader([]const u8).init(chunk, codec, levels, arena);

    var def_levels_buf: ?[]u32 = null;
    var rep_levels_buf: ?[]u32 = null;

    if (levels.max_rep > 0) {
        const dl = try arena.alloc(u32, num_leaves);
        const rl = try arena.alloc(u32, num_leaves);
        var written: usize = 0;
        while (written < num_leaves) {
            const n = reader.decodeWithRepLevels(raw_slices[written..], dl[written..], rl[written..]) catch return error.ShortDecode;
            if (n == 0) break;
            written += n;
        }
        if (written != num_leaves) return error.ShortDecode;
        def_levels_buf = dl;
        rep_levels_buf = rl;
    } else if (levels.max_def > 0) {
        const dl = try arena.alloc(u32, num_leaves);
        var written: usize = 0;
        while (written < num_leaves) {
            const n = reader.decodeWithLevels(raw_slices[written..], dl[written..]) catch return error.ShortDecode;
            if (n == 0) break;
            written += n;
        }
        if (written != num_leaves) return error.ShortDecode;
        def_levels_buf = dl;
    } else {
        var written: usize = 0;
        while (written < num_leaves) {
            const n = reader.decode(raw_slices[written..]) catch return error.ShortDecode;
            if (n == 0) break;
            written += n;
        }
        if (written != num_leaves) return error.ShortDecode;
    }

    const max_def: u32 = @intCast(levels.max_def);
    const values = try arena.alloc(f64, num_leaves);
    for (raw_slices, 0..) |s, i| {
        // Null slot: leave 0.0; aggregators consult def_levels.
        if (def_levels_buf) |dl| {
            if (dl[i] < max_def) {
                values[i] = 0.0;
                continue;
            }
        }
        if (s.len > MAX_FLBA_BYTE_WIDTH) return error.DecimalByteWidthTooLarge;
        values[i] = applyScaleI128(flbaToI128(s), scale);
    }

    return .{
        .values = values,
        .def_levels = def_levels_buf,
        .max_def = @intCast(levels.max_def),
        .rep_levels = rep_levels_buf,
        .max_rep = @intCast(levels.max_rep),
    };
}

// ----- FIXED_LEN_BYTE_ARRAY-backed -----
//
// FLBA isn't wired through ColumnChunkReader (the generic-on-T
// reader only knows i32/i64/f32/f64/bool/[]const u8). Walk pages
// directly here; the surface is small.

fn decodeFlbaBacked(
    arena: std.mem.Allocator,
    chunk: []const u8,
    codec: schema.CompressionCodec,
    levels: schema.Levels,
    num_leaves: usize,
    kind: Kind,
) Error!filter_eval.ColumnT(f64) {
    if (kind.byte_width == 0 or kind.byte_width > MAX_FLBA_BYTE_WIDTH) {
        return error.DecimalByteWidthTooLarge;
    }
    const bw: usize = kind.byte_width;
    const scale = kind.scale;

    const values = try arena.alloc(f64, num_leaves);

    var def_levels_buf: ?[]u32 = null;
    var rep_levels_buf: ?[]u32 = null;
    if (levels.max_def > 0) def_levels_buf = try arena.alloc(u32, num_leaves);
    if (levels.max_rep > 0) rep_levels_buf = try arena.alloc(u32, num_leaves);

    // Cached dict (FLBA values from the DICTIONARY_PAGE, decoded as
    // f64 once so data-page index resolution is direct).
    var dict_f64: ?[]const f64 = null;

    var pr = page_mod.PageReader.init(chunk, codec, arena);
    var written: usize = 0;

    while (try pr.next()) |pg| {
        switch (pg.header.type) {
            .DICTIONARY_PAGE => {
                const dh = pg.header.dictionary_page_header orelse return error.UnexpectedPage;
                const dict_count: usize = @intCast(dh.num_values);
                const expected_bytes = dict_count * bw;
                if (pg.bytes.len < expected_bytes) return error.ShortDecode;
                const dict = try arena.alloc(f64, dict_count);
                for (0..dict_count) |i| {
                    const off = i * bw;
                    dict[i] = applyScaleI128(flbaToI128(pg.bytes[off .. off + bw]), scale);
                }
                dict_f64 = dict;
            },
            .DATA_PAGE, .DATA_PAGE_V2 => {
                try decodeFlbaDataPage(
                    pg,
                    bw,
                    scale,
                    dict_f64,
                    levels,
                    values,
                    def_levels_buf,
                    rep_levels_buf,
                    &written,
                );
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

fn decodeFlbaDataPage(
    pg: page_mod.Page,
    byte_width: usize,
    scale: i32,
    dict_f64: ?[]const f64,
    levels: schema.Levels,
    values_out: []f64,
    def_levels_buf: ?[]u32,
    rep_levels_buf: ?[]u32,
    written: *usize,
) Error!void {
    // Page-header-level metadata varies between V1 and V2.
    var page_num_values: usize = 0;
    var encoding: schema.Encoding = .PLAIN;
    if (pg.header.type == .DATA_PAGE) {
        const dph = pg.header.data_page_header orelse return error.UnexpectedPage;
        page_num_values = @intCast(dph.num_values);
        encoding = dph.encoding;
    } else { // DATA_PAGE_V2
        const dph = pg.header.data_page_header_v2 orelse return error.UnexpectedPage;
        page_num_values = @intCast(dph.num_values);
        encoding = dph.encoding;
    }

    // Extract rep/def level slices (if any) and the values payload.
    var values_bytes = pg.bytes;
    if (pg.header.type == .DATA_PAGE) {
        // V1: levels are RLE-prefixed at the start of the page bytes.
        if (levels.max_rep > 0) {
            if (values_bytes.len < 4) return error.UnexpectedEndOfChunk;
            const rep_len = std.mem.readInt(u32, values_bytes[0..4], .little);
            if (4 + rep_len > values_bytes.len) return error.UnexpectedEndOfChunk;
            const rep_bytes = values_bytes[4 .. 4 + rep_len];
            try decodeLevels(rep_bytes, @intCast(levels.max_rep), page_num_values, rep_levels_buf, written.*);
            values_bytes = values_bytes[4 + rep_len ..];
        }
        if (levels.max_def > 0) {
            if (values_bytes.len < 4) return error.UnexpectedEndOfChunk;
            const def_len = std.mem.readInt(u32, values_bytes[0..4], .little);
            if (4 + def_len > values_bytes.len) return error.UnexpectedEndOfChunk;
            const def_bytes = values_bytes[4 .. 4 + def_len];
            try decodeLevels(def_bytes, @as(u32, @intCast(levels.max_def)), page_num_values, def_levels_buf, written.*);
            values_bytes = values_bytes[4 + def_len ..];
        }
    } else {
        // V2: levels are at fixed offsets per the page header, never
        // RLE-prefixed by a u32. PageReader has already laid them out
        // contiguous with the values. Mirror the V1 framing for
        // simplicity: assume PageReader exposes the same `pg.bytes`
        // layout for V2 with explicit byte lengths from the v2 header.
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

    // num_present: how many non-null slots this page contributes. For
    // OPTIONAL/REPEATED columns that's def_levels filtered to max_def;
    // for REQUIRED it's just page_num_values.
    const num_present: usize = if (def_levels_buf) |dl| blk: {
        var c: usize = 0;
        for (dl[written.* .. written.* + page_num_values]) |d| if (d == levels.max_def) {
            c += 1;
        };
        break :blk c;
    } else page_num_values;

    switch (encoding) {
        .PLAIN => {
            const expected = num_present * byte_width;
            if (values_bytes.len < expected) return error.ShortDecode;
            if (def_levels_buf) |dl| {
                // Sparse path: emit zeros for null slots (the f64 zero
                // is harmless; the aggregator inspects def_levels).
                var raw_pos: usize = 0;
                for (0..page_num_values) |i| {
                    const idx = written.* + i;
                    if (dl[idx] == levels.max_def) {
                        const off = raw_pos * byte_width;
                        values_out[idx] = applyScaleI128(flbaToI128(values_bytes[off .. off + byte_width]), scale);
                        raw_pos += 1;
                    } else {
                        values_out[idx] = 0.0;
                    }
                }
            } else {
                for (0..page_num_values) |i| {
                    const off = i * byte_width;
                    values_out[written.* + i] = applyScaleI128(flbaToI128(values_bytes[off .. off + byte_width]), scale);
                }
            }
        },
        .PLAIN_DICTIONARY, .RLE_DICTIONARY => {
            const dict = dict_f64 orelse return error.DictionaryMissing;
            // Data page: [bit_width: u8][hybrid_rle indices]
            if (values_bytes.len == 0) return error.ShortDecode;
            const bit_width = values_bytes[0];
            if (bit_width > 32) return error.UnsupportedEncoding;
            var idx_dec = hybrid_rle.HybridRleDecoder.init(values_bytes[1..], bit_width);
            // Decode num_present indices into a scratch buffer, then
            // scatter into values_out respecting def_levels.
            var idx_buf: [256]u32 = undefined;
            var present_decoded: usize = 0;
            var page_pos: usize = 0; // position within this page's slots
            while (present_decoded < num_present) {
                const want = @min(num_present - present_decoded, idx_buf.len);
                const n = idx_dec.decode(idx_buf[0..want]) catch return error.ShortDecode;
                if (n == 0) return error.ShortDecode;
                if (def_levels_buf) |dl| {
                    // Map the next `n` present indices into the next
                    // page slots where def_level == max_def.
                    var k: usize = 0;
                    while (k < n) {
                        // Find the next present slot in this page
                        while (page_pos < page_num_values and dl[written.* + page_pos] != levels.max_def) {
                            values_out[written.* + page_pos] = 0.0;
                            page_pos += 1;
                        }
                        if (page_pos >= page_num_values) return error.ShortDecode;
                        if (idx_buf[k] >= dict.len) return error.UnexpectedPage;
                        values_out[written.* + page_pos] = dict[idx_buf[k]];
                        page_pos += 1;
                        k += 1;
                    }
                } else {
                    // Dense path: indices map 1:1 to output slots.
                    for (0..n) |k| {
                        if (idx_buf[k] >= dict.len) return error.UnexpectedPage;
                        values_out[written.* + present_decoded + k] = dict[idx_buf[k]];
                    }
                }
                present_decoded += n;
            }
            // Finish any trailing null slots after the last present index.
            if (def_levels_buf) |dl| {
                while (page_pos < page_num_values) {
                    if (dl[written.* + page_pos] != levels.max_def) {
                        values_out[written.* + page_pos] = 0.0;
                    }
                    page_pos += 1;
                }
            }
        },
        .BYTE_STREAM_SPLIT => {
            // The values region is `byte_width` contiguous byte-planes of N
            // bytes each (N = num_present); value i's byte j lives at
            // `values_bytes[j*N + i]`. Gather each value's bytes into a small
            // stack buffer (decimal is ≤16 bytes) — same big-endian order the
            // PLAIN branch hands to flbaToI128 — so no allocation needed.
            if (byte_width == 0 or byte_width > 32) return error.UnsupportedDecimalEncoding;
            const expected = num_present * byte_width;
            if (values_bytes.len < expected) return error.ShortDecode;
            var tmp: [32]u8 = undefined;
            if (def_levels_buf) |dl| {
                var raw_pos: usize = 0;
                for (0..page_num_values) |i| {
                    const idx = written.* + i;
                    if (dl[idx] == levels.max_def) {
                        for (0..byte_width) |j| tmp[j] = values_bytes[j * num_present + raw_pos];
                        values_out[idx] = applyScaleI128(flbaToI128(tmp[0..byte_width]), scale);
                        raw_pos += 1;
                    } else {
                        values_out[idx] = 0.0;
                    }
                }
            } else {
                for (0..page_num_values) |i| {
                    for (0..byte_width) |j| tmp[j] = values_bytes[j * num_present + i];
                    values_out[written.* + i] = applyScaleI128(flbaToI128(tmp[0..byte_width]), scale);
                }
            }
        },
        else => return error.UnsupportedDecimalEncoding,
    }

    written.* += page_num_values;
}

fn decodeLevels(
    encoded: []const u8,
    max: u32,
    expected: usize,
    out_buf: ?[]u32,
    start: usize,
) Error!void {
    const out = out_buf orelse return; // No buffer = caller doesn't care.
    const bit_width = bitWidthFor(max);
    var dec = hybrid_rle.HybridRleDecoder.init(encoded, bit_width);
    const got = dec.decode(out[start .. start + expected]) catch return error.ShortDecode;
    if (got != expected) return error.ShortDecode;
}

fn bitWidthFor(max: u32) u8 {
    if (max == 0) return 0;
    return @intCast(32 - @clz(max));
}

// ============================================================
// Tests
// ============================================================

const testing = std.testing;

test "pow10_f64 table is exact up to 10^15" {
    try testing.expectEqual(@as(f64, 1.0), pow10_f64[0]);
    try testing.expectEqual(@as(f64, 10.0), pow10_f64[1]);
    try testing.expectEqual(@as(f64, 100.0), pow10_f64[2]);
    try testing.expectEqual(@as(f64, 1e15), pow10_f64[15]);
}

test "flbaToI128: small positive values" {
    try testing.expectEqual(@as(i128, 0), flbaToI128(&[_]u8{0x00}));
    try testing.expectEqual(@as(i128, 1), flbaToI128(&[_]u8{0x01}));
    try testing.expectEqual(@as(i128, 127), flbaToI128(&[_]u8{0x7f}));
    try testing.expectEqual(@as(i128, 256), flbaToI128(&[_]u8{ 0x01, 0x00 }));
    try testing.expectEqual(@as(i128, 1_000_000), flbaToI128(&[_]u8{ 0x0f, 0x42, 0x40 }));
}

test "flbaToI128: negative values sign-extend correctly" {
    // -1 in any byte width is all 0xff.
    try testing.expectEqual(@as(i128, -1), flbaToI128(&[_]u8{0xff}));
    try testing.expectEqual(@as(i128, -1), flbaToI128(&[_]u8{ 0xff, 0xff }));
    try testing.expectEqual(@as(i128, -1), flbaToI128(&[_]u8{ 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff }));
    // Most-negative byte: 0x80 0x00... at varying widths.
    try testing.expectEqual(@as(i128, -128), flbaToI128(&[_]u8{0x80}));
    try testing.expectEqual(@as(i128, -32768), flbaToI128(&[_]u8{ 0x80, 0x00 }));
    // -1_000_000 = 0xfff_0bdc0 (24-bit two's-complement of 1_000_000)
    try testing.expectEqual(@as(i128, -1_000_000), flbaToI128(&[_]u8{ 0xf0, 0xbd, 0xc0 }));
}

test "flbaToI128: 16-byte width round-trip" {
    // i128 max = 2^127 - 1
    const big: i128 = std.math.maxInt(i128);
    var buf: [16]u8 = undefined;
    std.mem.writeInt(i128, &buf, big, .big);
    try testing.expectEqual(big, flbaToI128(&buf));

    const small_neg: i128 = std.math.minInt(i128);
    std.mem.writeInt(i128, &buf, small_neg, .big);
    try testing.expectEqual(small_neg, flbaToI128(&buf));
}

test "applyScaleInt converts known values" {
    // 12345 with scale=2 = 123.45
    try testing.expectApproxEqAbs(@as(f64, 123.45), applyScaleInt(i32, 12345, 2), 1e-9);
    // -98765 with scale=4 = -9.8765
    try testing.expectApproxEqAbs(@as(f64, -9.8765), applyScaleInt(i32, -98765, 4), 1e-9);
    // 1 with scale=8 = 1e-8
    try testing.expectApproxEqAbs(@as(f64, 1e-8), applyScaleInt(i64, 1, 8), 1e-12);
    // 0 always 0.
    try testing.expectEqual(@as(f64, 0.0), applyScaleInt(i64, 0, 18));
}

test "kindFromSchema recognises Decimal columns" {
    var elem = schema.SchemaElement{
        .type = .INT64,
        .type_length = null,
        .repetition_type = .OPTIONAL,
        .name = "amount",
        .num_children = 0,
        .converted_type = null,
        .logical_type = .{ .DECIMAL = .{ .scale = 4, .precision = 18 } },
        .scale = null,
        .precision = null,
        .field_id = null,
    };
    const k = kindFromSchema(&elem) orelse return error.TestUnexpectedResult;
    try testing.expectEqual(@as(i32, 4), k.scale);
    try testing.expectEqual(@as(i32, 18), k.precision);
    try testing.expectEqual(schema.Type.INT64, k.physical);
    try testing.expectEqual(@as(u8, 0), k.byte_width); // not FLBA

    // Non-decimal logical type returns null.
    elem.logical_type = .{ .STRING = .{} };
    try testing.expectEqual(@as(?Kind, null), kindFromSchema(&elem));

    // No logical type at all → null.
    elem.logical_type = null;
    try testing.expectEqual(@as(?Kind, null), kindFromSchema(&elem));
}

test "kindFromSchema picks up FLBA byte_width" {
    const elem = schema.SchemaElement{
        .type = .FIXED_LEN_BYTE_ARRAY,
        .type_length = 9,
        .repetition_type = .OPTIONAL,
        .name = "price",
        .num_children = 0,
        .converted_type = null,
        .logical_type = .{ .DECIMAL = .{ .scale = 2, .precision = 18 } },
        .scale = null,
        .precision = null,
        .field_id = null,
    };
    const k = kindFromSchema(&elem) orelse return error.TestUnexpectedResult;
    try testing.expectEqual(@as(u8, 9), k.byte_width);
}

test "kindFromSchema accepts legacy ConvertedType.DECIMAL" {
    // Spark / older writers express DECIMAL via converted_type=DECIMAL
    // plus top-level scale/precision rather than via LogicalType.
    const elem = schema.SchemaElement{
        .type = .INT32,
        .type_length = null,
        .repetition_type = .OPTIONAL,
        .name = "value",
        .num_children = 0,
        .converted_type = .DECIMAL,
        .logical_type = null,
        .scale = 2,
        .precision = 4,
        .field_id = null,
    };
    const k = kindFromSchema(&elem) orelse return error.TestUnexpectedResult;
    try testing.expectEqual(@as(i32, 2), k.scale);
    try testing.expectEqual(@as(i32, 4), k.precision);
    try testing.expectEqual(schema.Type.INT32, k.physical);
}

// ----- Integration: real parquet-testing decimal fixtures -----
//
// The apache/parquet-testing repo (vendored at data/parquet-testing/)
// ships canonical decimal fixtures written by Spark. Each holds 24
// values 1.00..24.00 (scale=2, sum=300.00). We decode them through
// the same decodeColumnAsF64 path the consumer uses and assert the
// values + sum match.

const metadata = @import("metadata.zig");

fn readFileSlice(path: []const u8, allocator: std.mem.Allocator) ![]u8 {
    const linux = std.os.linux;
    var path_z: [256]u8 = undefined;
    if (path.len + 1 > path_z.len) return error.PathTooLong;
    @memcpy(path_z[0..path.len], path);
    path_z[path.len] = 0;

    const r_open = linux.openat(linux.AT.FDCWD, @ptrCast(&path_z[0]), .{ .ACCMODE = .RDONLY, .CLOEXEC = true }, 0);
    if (@as(isize, @bitCast(r_open)) < 0) return error.FileNotFound;
    const fd: linux.fd_t = @intCast(@as(isize, @bitCast(r_open)));
    defer _ = linux.close(fd);

    const SEEK_END: usize = 2;
    const SEEK_SET: usize = 0;
    const end_pos = linux.lseek(fd, 0, SEEK_END);
    _ = linux.lseek(fd, 0, SEEK_SET);
    const size: usize = @intCast(end_pos);

    const buf = try allocator.alloc(u8, size);
    errdefer allocator.free(buf);
    var off: usize = 0;
    while (off < size) {
        const n = linux.read(fd, buf[off..].ptr, size - off);
        const bytes: usize = @intCast(@as(isize, @bitCast(n)));
        if (bytes == 0) break;
        off += bytes;
    }
    return buf;
}

fn checkDecimalFixture(
    fixture_path: []const u8,
    expected_physical: schema.Type,
    expected_precision: i32,
    expected_byte_width: u8,
) !void {
    const file_bytes = readFileSlice(fixture_path, testing.allocator) catch |err| {
        if (err == error.FileNotFound) {
            std.debug.print("skipping: {s} not present\n", .{fixture_path});
            return;
        }
        return err;
    };
    defer testing.allocator.free(file_bytes);

    var meta = try metadata.open(testing.allocator, file_bytes);
    defer meta.deinit(testing.allocator);

    try testing.expect(meta.row_groups.items.len >= 1);
    const rg0 = &meta.row_groups.items[0];
    try testing.expect(rg0.columns.items.len >= 1);
    const col = rg0.columns.items[0].meta_data.?;

    // Recognise the column as DECIMAL.
    const path: [1][]const u8 = .{"value"};
    const elem = meta.getColumnSchema(&path) orelse return error.SchemaLookupFailed;
    const kind = kindFromSchema(&elem) orelse return error.NotRecognisedAsDecimal;

    try testing.expectEqual(expected_physical, kind.physical);
    try testing.expectEqual(expected_precision, kind.precision);
    try testing.expectEqual(@as(i32, 2), kind.scale);
    try testing.expectEqual(expected_byte_width, kind.byte_width);

    // Decode.
    const chunk_start: usize = if (col.dictionary_page_offset) |dp| @intCast(dp) else @intCast(col.data_page_offset);
    const chunk_len: usize = @intCast(col.total_compressed_size);
    const chunk = file_bytes[chunk_start .. chunk_start + chunk_len];

    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();

    const levels = meta.getColumnLevels(&path);
    const n_leaves: usize = @intCast(col.num_values);
    const column = try decodeColumnAsF64(arena.allocator(), chunk, col.codec, levels, n_leaves, kind);

    try testing.expectEqual(@as(usize, 24), column.values.len);

    var sum: f64 = 0;
    var i: usize = 0;
    while (i < column.values.len) : (i += 1) {
        const present = if (column.def_levels) |dl|
            dl[i] == column.max_def
        else
            true;
        if (!present) continue;
        sum += column.values[i];
    }
    // Sum of 1..24 = 300.
    try testing.expectApproxEqAbs(@as(f64, 300.0), sum, 1e-9);

    // Spot-check that values 1.00..24.00 are all present (we don't
    // know the iteration order pre-emptively if reorderings could
    // exist — these fixtures are sequential in row order, so verify).
    i = 0;
    while (i < column.values.len) : (i += 1) {
        const expected = @as(f64, @floatFromInt(i + 1));
        try testing.expectApproxEqAbs(expected, column.values[i], 1e-9);
    }
}

test "decode int32-backed decimal fixture (precision=4, scale=2)" {
    try checkDecimalFixture(
        "data/parquet-testing/data/int32_decimal.parquet",
        .INT32,
        4,
        0,
    );
}

test "decode int64-backed decimal fixture (precision=10, scale=2)" {
    try checkDecimalFixture(
        "data/parquet-testing/data/int64_decimal.parquet",
        .INT64,
        10,
        0,
    );
}

test "decode fixed-length-byte-array-backed decimal fixture (precision=25, scale=2)" {
    try checkDecimalFixture(
        "data/parquet-testing/data/fixed_length_decimal.parquet",
        .FIXED_LEN_BYTE_ARRAY,
        25,
        11,
    );
}

test "decode legacy fixed-length-byte-array-backed decimal fixture (precision=13, scale=2)" {
    try checkDecimalFixture(
        "data/parquet-testing/data/fixed_length_decimal_legacy.parquet",
        .FIXED_LEN_BYTE_ARRAY,
        13,
        6,
    );
}

test "decode byte-array-backed decimal fixture (precision=4, scale=2)" {
    // BYTE_ARRAY-backed Decimal uses length-prefixed big-endian two's-
    // complement bytes (length per value varies but each ≤ 16 bytes
    // for precision ≤ 38). Same fixture pattern: 24 values 1..24.
    try checkDecimalFixture(
        "data/parquet-testing/data/byte_array_decimal.parquet",
        .BYTE_ARRAY,
        4,
        0, // type_length not set for BYTE_ARRAY
    );
}

test "applyScaleI128: scale 0 identity and signed scaling" {
    try testing.expectApproxEqAbs(@as(f64, 12345.0), applyScaleI128(12345, 0), 1e-9);
    try testing.expectApproxEqAbs(@as(f64, 123.45), applyScaleI128(12345, 2), 1e-9);
    try testing.expectApproxEqAbs(@as(f64, -123.45), applyScaleI128(-12345, 2), 1e-9);
}

test "applyScaleI128: out-of-table scale uses the pow fallback without trapping" {
    // pow10_f64 covers scale 0..38; scale >= len hits the std.math.pow path.
    // (Out of spec — precision/scale <= 38 — but must not index out of bounds.)
    const v = applyScaleI128(12345, 40);
    try testing.expect(v > 0 and v < 1e-30);
}
