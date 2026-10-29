const expr_ast = @import("../ast.zig");
const filter_eval = @import("../../filter/eval.zig");
const Error = @import("types.zig").Error;
const splitMix64 = @import("census.zig").splitMix64;
const std = @import("std");

/// Float min/max order every path shares, as DuckDB, Postgres and Spark order floats: NaN sorts above every number,
/// and of two equal zeros -0 is the lesser. It is a total order, so a min or max is the same whatever order values and
/// partial results meet in: min skips NaN unless every value is NaN, max is NaN if any value is.
pub inline fn minTotal(a: f64, b: f64) f64 {
    if (std.math.isNan(a)) return b;
    if (std.math.isNan(b)) return a;
    if (a < b) return a;
    if (b < a) return b;
    return if (std.math.signbit(a)) a else b;
}

pub inline fn maxTotal(a: f64, b: f64) f64 {
    if (std.math.isNan(a)) return a;
    if (std.math.isNan(b)) return b;
    if (a > b) return a;
    if (b > a) return b;
    return if (std.math.signbit(a)) b else a;
}

/// Fold `v` into a float min (`is_min`) or max accumulator slot.
pub inline fn foldExtremeF(comptime is_min: bool, slot: *?f64, v: f64) void {
    slot.* = if (slot.*) |cur| (if (is_min) minTotal(cur, v) else maxTotal(cur, v)) else v;
}



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


/// Stable 32-bit references into append-only 64 KiB chunks. Records never straddle separately allocated blocks.
/// Reset reuses blocks; handing the arena to an answer preserves every borrowed key until that answer is dropped.
pub const KeyArena = struct {
    pub const chunk_size = 1 << 16;
    pub const Block = struct { bytes: []u8, start: u32 };
    blocks: std.ArrayList(Block) = .empty,
    chunks: std.ArrayList([*]u8) = .empty,
    data_bytes: usize = 0,
    block: usize = 0,
    used: usize = 0,
    /// A tiny query ceiling can bootstrap below one chunk without changing the logical 64 KiB offset geometry.
    bootstrap: usize = chunk_size,
    allocator: std.mem.Allocator = undefined,
    owner: ?*anyopaque = null,
    reserveFn: ?*const fn (*anyopaque, usize) Error!void = null,
    settleFn: ?*const fn (*anyopaque) void = null,

    pub fn append(self: *KeyArena, value: []const u8) !u32 {
        if (self.reserveFn) |reserve| {
            const growth = (try self.projected(value.len)) - self.held();
            try reserve(self.owner.?, std.math.add(usize, growth, try self.projectedOverlap(value.len)) catch
                return error.ExceededMemoryBudget);
        }
        defer if (self.settleFn) |settle| settle(self.owner.?);
        return self.put(self.allocator, value);
    }

    fn header(len: usize, out: *[5]u8) !usize {
        var n = std.math.cast(u32, len) orelse return error.ExceededMemoryBudget;
        var i: usize = 0;
        while (true) : (i += 1) {
            out[i] = @as(u8, @truncate(n)) & 0x7f;
            n >>= 7;
            if (n == 0) return i + 1;
            out[i] |= 0x80;
        }
    }

    const Place = struct { block: usize, used: usize, need: usize, capacity: usize };
    fn place(self: *const KeyArena, len: usize) !Place {
        var hdr: [5]u8 = undefined;
        const need = len + try header(len, &hdr);
        var b = self.block;
        var at = self.used;
        while (b < self.blocks.items.len) : (b += 1) {
            const cap = self.blocks.items[b].bytes.len;
            if (need <= chunk_size and at / chunk_size != (at + need - 1) / chunk_size)
                at = std.mem.alignForward(usize, at, chunk_size);
            if (at <= cap and need <= cap - at) return .{ .block = b, .used = at, .need = need, .capacity = 0 };
            at = 0;
        }
        // A small per-table credit starts below one physical chunk and grows geometrically. References still
        // use one logical 64 KiB chunk id per short block; large-query arenas stay at the ordinary chunk size.
        const cap = if (need <= chunk_size)
            @min(chunk_size, @max(
                if (self.blocks.items.len == 0) self.bootstrap else self.blocks.items[self.blocks.items.len - 1].bytes.len * 2,
                std.math.ceilPowerOfTwoAssert(usize, need),
            ))
        else
            std.mem.alignForward(usize, need, chunk_size);
        const logical = std.mem.alignForward(usize, cap, chunk_size);
        if (@as(u64, self.chunks.items.len) * chunk_size + logical > @as(u64, 1) << 32)
            return error.ExceededMemoryBudget;
        return .{ .block = b, .used = 0, .need = need, .capacity = cap };
    }

    pub fn held(self: *const KeyArena) usize {
        return self.data_bytes + self.blocks.capacity * @sizeOf(Block) + self.chunks.capacity * @sizeOf([*]u8);
    }

    pub fn projected(self: *const KeyArena, len: usize) !usize {
        const p = try self.place(len);
        if (p.capacity == 0) return self.held();
        const blocks = self.blocks.items.len + 1;
        const chunks = self.chunks.items.len + (p.capacity + chunk_size - 1) / chunk_size;
        return self.held() + p.capacity +
            (if (blocks > self.blocks.capacity)
                (@max(blocks, self.blocks.capacity * 2) - self.blocks.capacity) * @sizeOf(Block)
            else
                0) +
            (if (chunks > self.chunks.capacity)
                (@max(chunks, self.chunks.capacity * 2) - self.chunks.capacity) * @sizeOf([*]u8)
            else
                0);
    }

    pub fn projectedOverlap(self: *const KeyArena, len: usize) !usize {
        const p = try self.place(len);
        if (p.capacity == 0) return 0;
        const nc = (p.capacity + chunk_size - 1) / chunk_size;
        const blocks = if (self.blocks.items.len == self.blocks.capacity)
            self.blocks.capacity * @sizeOf(Block)
        else
            0;
        const chunks = if (self.chunks.items.len + nc > self.chunks.capacity)
            self.chunks.capacity * @sizeOf([*]u8)
        else
            0;
        return @max(blocks, chunks);
    }

    pub fn put(self: *KeyArena, a: std.mem.Allocator, value: []const u8) !u32 {
        return self.putParts(a, &.{}, value);
    }

    pub fn putParts(self: *KeyArena, a: std.mem.Allocator, prefix: []const u8, value: []const u8) !u32 {
        const len = std.math.add(usize, prefix.len, value.len) catch return error.ExceededMemoryBudget;
        const p = try self.place(len);
        if (p.capacity != 0) {
            if (self.blocks.items.len == self.blocks.capacity)
                try self.blocks.ensureTotalCapacityPrecise(a, @max(1, self.blocks.capacity * 2));
            const nc = (p.capacity + chunk_size - 1) / chunk_size;
            if (self.chunks.items.len + nc > self.chunks.capacity)
                try self.chunks.ensureTotalCapacityPrecise(a, @max(self.chunks.items.len + nc, self.chunks.capacity * 2));
            const bytes = try a.alloc(u8, p.capacity);
            self.data_bytes += bytes.len;
            self.blocks.appendAssumeCapacity(.{ .bytes = bytes, .start = @intCast(self.chunks.items.len * chunk_size) });
            for (0..nc) |i| self.chunks.appendAssumeCapacity(bytes.ptr + i * chunk_size);
        }
        const b = self.blocks.items[p.block];
        var hdr: [5]u8 = undefined;
        const hn = try header(len, &hdr);
        @memcpy(b.bytes[p.used..][0..hn], hdr[0..hn]);
        @memcpy(b.bytes[p.used + hn ..][0..prefix.len], prefix);
        @memcpy(b.bytes[p.used + hn + prefix.len ..][0..value.len], value);
        self.block = p.block;
        self.used = p.used + p.need;
        return @intCast(@as(usize, b.start) + p.used);
    }

    pub fn get(self: *const KeyArena, ref: u32) []const u8 {
        const p = self.chunks.items[ref >> 16] + (ref & (chunk_size - 1));
        var len: u32 = 0;
        var shift: u5 = 0;
        var i: usize = 0;
        while (true) : (i += 1) {
            const b = p[i];
            len |= @as(u32, b & 0x7f) << shift;
            if (b < 0x80) return p[i + 1 ..][0..len];
            shift += 7;
        }
    }

    pub fn reset(self: *KeyArena) void {
        self.block = 0;
        self.used = 0;
    }

    pub fn deinit(self: *KeyArena, a: std.mem.Allocator) void {
        for (self.blocks.items) |b| a.free(b.bytes);
        self.blocks.deinit(a);
        self.chunks.deinit(a);
        self.* = .{};
    }
};

