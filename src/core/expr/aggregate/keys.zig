const std = @import("std");
const expr_ast = @import("../ast.zig");
const filter_eval = @import("../../filter/eval.zig");
const splitMix64 = @import("census.zig").splitMix64;

pub fn isRowNull(col: filter_eval.Batch.Column, r: usize) bool {
    return switch (col) {
        inline else => |c| if (c.def_levels) |dl| dl[r] < c.max_def else false,
    };
}

pub fn serializeRowKey(
    list: *std.ArrayList(u8),
    allocator: std.mem.Allocator,
    key_cols: []const filter_eval.Batch.Column,
    row: usize,
) !void {
    list.clearRetainingCapacity();

    for (key_cols) |col| {
        if (isRowNull(col, row)) {
            try list.append(allocator, 0); // 0 = null
        } else {
            try list.append(allocator, 1); // 1 = present
            switch (col) {
                .i32 => |c| {
                    var buf: [8]u8 = undefined;
                    std.mem.writeInt(i64, &buf, @intCast(c.values[row]), .little);
                    try list.appendSlice(allocator, &buf);
                },
                .i64 => |c| {
                    var buf: [8]u8 = undefined;
                    std.mem.writeInt(i64, &buf, c.values[row], .little);
                    try list.appendSlice(allocator, &buf);
                },
                .f32 => |c| {
                    var f: f64 = @floatCast(c.values[row]);
                    if (std.math.isNan(f)) {
                        f = std.math.nan(f64);
                    } else if (f == 0.0) {
                        f = 0.0;
                    }
                    var buf: [8]u8 = undefined;
                    std.mem.writeInt(u64, &buf, @bitCast(f), .little);
                    try list.appendSlice(allocator, &buf);
                },
                .f64 => |c| {
                    var f = c.values[row];
                    if (std.math.isNan(f)) {
                        f = std.math.nan(f64);
                    } else if (f == 0.0) {
                        f = 0.0;
                    }
                    var buf: [8]u8 = undefined;
                    std.mem.writeInt(u64, &buf, @bitCast(f), .little);
                    try list.appendSlice(allocator, &buf);
                },
                .string => |c| {
                    const s = c.values[row];
                    var len_buf: [4]u8 = undefined;
                    std.mem.writeInt(u32, &len_buf, @intCast(s.len), .little);
                    try list.appendSlice(allocator, &len_buf);
                    try list.appendSlice(allocator, s);
                },
                .boolean => |c| {
                    // Same framing as evalGroupKeyExpr: BOOLEAN widens to the
                    // i64 lane. deserializeKeyColumn only knows {i64,f64,string},
                    // so a 1-byte write here would disagree with the reader.
                    var buf: [8]u8 = undefined;
                    std.mem.writeInt(i64, &buf, if (c.values[row]) 1 else 0, .little);
                    try list.appendSlice(allocator, &buf);
                },
                // An exact DECIMAL key: its unscaled value. A key column has one scale, so equal values frame equally.
                .i128 => |c| {
                    var buf: [16]u8 = undefined;
                    std.mem.writeInt(i128, &buf, c.values[row], .little);
                    try list.appendSlice(allocator, &buf);
                },
            }
        }
    }
}

/// Byte width of one fixed-width group key, or null when a key is a string, a decimal, or wider than the inline
/// image. Each integer or float column is a presence byte plus 8 payload bytes, the same bytes `serializeRowKey`
/// writes for a present value. Null payload is stored as zeros so every group of a query has one width.
pub fn fixedGroupStride(keys: []const expr_ast.Expr) ?u8 {
    if (keys.len == 0 or keys.len > 3) return null;
    for (keys) |k| switch (k.typeOf()) {
        .str, .dec => return null,
        .i64, .f64 => {},
    };
    return @intCast(keys.len * 9);
}

/// `serializeRowKey`'s bytes for `row`, with a null column stored as a zero presence and 8 zero payload bytes.
/// Null when a column is not fixed-width. The width is `9 * cols.len` for the columns this accepts.
pub fn packFixedKey(cols: []const filter_eval.Batch.Column, row: usize, out: *[32]u8) ?u8 {
    var d: usize = 0;
    for (cols) |col| {
        if (d + 9 > out.len) return null;
        switch (col) {
            .string, .i128 => return null,
            .i32, .i64, .f32, .f64, .boolean => {},
        }
        if (isRowNull(col, row)) {
            @memset(out[d..][0..9], 0);
            d += 9;
            continue;
        }
        out[d] = 1;
        d += 1;
        switch (col) {
            .i32 => |c| std.mem.writeInt(i64, out[d..][0..8], c.values[row], .little),
            .i64 => |c| std.mem.writeInt(i64, out[d..][0..8], c.values[row], .little),
            .boolean => |c| std.mem.writeInt(i64, out[d..][0..8], if (c.values[row]) 1 else 0, .little),
            .f32 => |c| {
                var f: f64 = @floatCast(c.values[row]);
                if (std.math.isNan(f)) f = std.math.nan(f64) else if (f == 0.0) f = 0.0;
                std.mem.writeInt(u64, out[d..][0..8], @bitCast(f), .little);
            },
            .f64 => |c| {
                var f = c.values[row];
                if (std.math.isNan(f)) f = std.math.nan(f64) else if (f == 0.0) f = 0.0;
                std.mem.writeInt(u64, out[d..][0..8], @bitCast(f), .little);
            },
            .string, .i128 => unreachable,
        }
        d += 8;
    }
    return @intCast(d);
}

/// Variable-length framing (`serializeRowKey`) of a fixed image. Present columns already match; a null column drops
/// the zero payload so `deserializeKeyColumn` sees the bytes it always has.
pub fn canonicalKey(image: []const u8, buf: *[48]u8) []const u8 {
    // Only a fixed-width image (`fixedGroupStride`: at most three 9-byte columns) is reframed; a caller holding
    // variable-length keys must not ask.
    std.debug.assert(image.len % 9 == 0 and image.len <= 27);
    var i: usize = 0;
    while (i < image.len) : (i += 9) if (image[i] == 0) break;
    if (i == image.len) return image;
    var d: usize = 0;
    i = 0;
    while (i < image.len) : (i += 9) {
        buf[d] = image[i];
        d += 1;
        if (image[i] != 0) {
            @memcpy(buf[d..][0..8], image[i + 1 ..][0..8]);
            d += 8;
        }
    }
    return buf[0..d];
}

/// `key` as a fixed-width table of `stride` stores it: as is when already its image, else expanded into `out`.
pub fn fixedImage(stride: usize, key: []const u8, out: *[32]u8) ?[]const u8 {
    if (key.len == stride) return key;
    return expandFixed(stride, key, out);
}

pub fn expandFixed(stride: usize, variable: []const u8, out: *[32]u8) ?[]const u8 {
    if (stride > out.len or stride % 9 != 0) return null;
    @memset(out[0..stride], 0);
    var s: usize = 0;
    var d: usize = 0;
    while (d < stride) {
        if (s >= variable.len) return null;
        const pres = variable[s];
        s += 1;
        out[d] = pres;
        d += 1;
        if (pres != 0) {
            if (s + 8 > variable.len) return null;
            @memcpy(out[d..][0..8], variable[s..][0..8]);
            s += 8;
        }
        d += 8;
    }
    if (s != variable.len) return null;
    return out[0..stride];
}

/// SplitMix of each 8-byte chunk. Zero is kept: an empty slot is an id sentinel, not a hash sentinel. Stored on the
/// slot so growing the table replays it and does not mix the key again.
pub fn hashFixed(bytes: []const u8) u64 {
    var h: u64 = 0;
    var i: usize = 0;
    while (i + 8 <= bytes.len) : (i += 8) {
        h ^= splitMix64(std.mem.readInt(u64, bytes[i..][0..8], .little));
    }
    if (i < bytes.len) {
        var tail: [8]u8 = @splat(0);
        @memcpy(tail[0 .. bytes.len - i], bytes[i..]);
        h ^= splitMix64(std.mem.readInt(u64, &tail, .little));
    }
    return h;
}

