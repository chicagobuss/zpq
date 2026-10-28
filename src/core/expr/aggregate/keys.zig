const std = @import("std");
const GroupTable = @import("group_table.zig").GroupTable;
const AggCall = @import("types.zig").AggCall;
const filter_selection = @import("../../filter/selection.zig");
const Error = @import("types.zig").Error;
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


/// Stable 32-bit references into append-only 64 KiB chunks. Records never straddle separately allocated blocks.
/// Reset reuses blocks; handing the arena to an answer preserves every borrowed key until that answer is dropped.
pub const KeyArena = struct {
    pub const chunk_size = 1 << 16;
    const Block = struct { bytes: []u8, start: u32 };
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

    fn projectedOverlap(self: *const KeyArena, len: usize) !usize {
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

    fn putParts(self: *KeyArena, a: std.mem.Allocator, prefix: []const u8, value: []const u8) !u32 {
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

test "STRGROUP stable arena records cover empty embedded-zero and multi-chunk values" {
    const a = std.testing.allocator;
    var arena: KeyArena = .{};
    defer arena.deinit(a);
    var refs: [7]u32 = undefined;
    const sizes = [_]usize{ 0, 1, 127, 65530, 65536, 131073, 3 };
    for (sizes, &refs) |n, *ref| {
        const bytes = try a.alloc(u8, n);
        defer a.free(bytes);
        @memset(bytes, 0);
        if (n > 0) bytes[n - 1] = 17;
        ref.* = try arena.put(a, bytes);
        try std.testing.expectEqualStrings(bytes, arena.get(ref.*));
    }
    for (sizes, refs) |n, ref| {
        const bytes = arena.get(ref);
        try std.testing.expectEqual(n, bytes.len);
        if (n > 0) try std.testing.expectEqual(@as(u8, 17), bytes[n - 1]);
    }
    const blocks = arena.blocks.items.len;
    var data_bytes: usize = 0;
    for (arena.blocks.items) |block| data_bytes += block.bytes.len;
    try std.testing.expectEqual(data_bytes, arena.data_bytes);
    const held = arena.held();
    arena.reset();
    try std.testing.expectEqualStrings("reuse", arena.get(try arena.put(a, "reuse")));
    try std.testing.expectEqual(blocks, arena.blocks.items.len);
    try std.testing.expectEqual(held, arena.held());
}


/// Memo of (ptr, len) -> group id for the dominant GROUP BY shape, a single string key. Dictionary-encoded values are
/// slices into one cached buffer, so this collapses one key hash per *row* into one per *distinct dictionary value*.
/// Strictly an accelerator: it hits only on exact (ptr, len) identity, `getOrInsert` stays the sole creator of groups,
/// and null rows take the slow path that preserves null-vs-empty-string semantics.
///
/// LIFETIME: keys are raw pointers into the decode arena. One resolver per scan call, never across row groups — a
/// reset arena can reissue an address and alias a stale entry to the wrong group.
pub const GroupKeyResolver = struct {
    scratch: std.ArrayList(u8),
    memo: std.HashMapUnmanaged(SliceId, u32, SliceIdContext, std.hash_map.default_max_load_percentage),
    fast_path: bool,
    /// Rows looked up in `memo`, and how many it answered. Strings decoded plain sit at an address of their own
    /// even when equal, so the memo stops after `memo_trial` rows that it answers fewer than one in eight of.
    memo_rows: u32 = 0,
    memo_hits: u32 = 0,
    memo_on: bool = true,

    const memo_trial = 4096;

    const SliceId = struct { ptr: usize, len: usize };

    /// `AutoHashMap` would wyhash all 16 bytes of `SliceId` on this per-row path. Mixing the pointer alone suffices —
    /// dictionary entries sit at well-spread addresses — and `len` still participates in `eql`, so identity stays exact
    /// under any collision.
    const SliceIdContext = struct {
        pub fn hash(_: SliceIdContext, k: SliceId) u64 {
            const p: u64 = k.ptr;
            return (p ^ (p >> 32)) *% 0x9E3779B97F4A7C15;
        }
        pub fn eql(_: SliceIdContext, a: SliceId, b: SliceId) bool {
            return a.ptr == b.ptr and a.len == b.len;
        }
    };

    pub fn init(key_cols: []const filter_eval.Batch.Column) GroupKeyResolver {
        return .{
            .scratch = .empty,
            .memo = .empty,
            .fast_path = key_cols.len == 1 and key_cols[0] == .string,
        };
    }

    pub fn deinit(self: *GroupKeyResolver, allocator: std.mem.Allocator) void {
        self.scratch.deinit(allocator);
        self.memo.deinit(allocator);
    }

    pub fn resolveBatch(
        self: *GroupKeyResolver,
        allocator: std.mem.Allocator,
        table: *GroupTable,
        cols: []const filter_eval.Batch.Column,
        calls: []const AggCall,
        pos: u64,
        sel: *const filter_selection.SelectionVector,
        ids: []u32,
        /// Original row of each batch row, when the batch was gathered down to the selection. Null: row `r` is
        /// original row `r`, and `pos + r` is where it first appears. The group's first row stays that ordinal.
        ordinals: ?[]const u32,
    ) !void {
        const place = struct {
            fn at(base: u64, rows: ?[]const u32, index: usize) u64 {
                return if (rows) |o| base + o[index] else base + index;
            }
        }.at;
        if (ordinals) |o| std.debug.assert(o.len >= sel.len);
        if (table.fixed_stride == 0) {
            for (0..sel.len) |r| if (sel.isActive(r)) {
                ids[r] = try self.resolveAt(allocator, table, cols, r, calls, place(pos, ordinals, r));
            };
            return;
        }
        const batch_n = 64;
        var images: [batch_n][32]u8 = undefined;
        var hashes: [batch_n]u64 = undefined;
        var off: usize = 0;
        while (off < sel.len) : (off += batch_n) {
            const n = @min(batch_n, sel.len - off);
            var packed_mask: u64 = 0;
            for (0..n) |i| if (sel.isActive(off + i)) {
                if (packFixedKey(cols, off + i, &images[i])) |len| if (len == table.fixed_stride) {
                    hashes[i] = hashFixed(images[i][0..len]);
                    packed_mask |= @as(u64, 1) << @intCast(i);
                    if (table.fixedSlotCount() >= 1 << 15) {
                        const slot = @as(usize, @as(u32, @truncate(hashes[i]))) & (table.fixedSlotCount() - 1);
                        @prefetch(table.fixedSlot(slot), .{ .rw = .read, .locality = 2 });
                    }
                };
            };
            for (0..n) |i| if (sel.isActive(off + i)) {
                ids[off + i] = if (packed_mask & (@as(u64, 1) << @intCast(i)) != 0)
                    try table.getOrInsertFixedHashed(
                        images[i][0..table.fixed_stride],
                        calls,
                        place(pos, ordinals, off + i),
                        hashes[i],
                    )
                else
                    try self.resolveAt(allocator, table, cols, off + i, calls, place(pos, ordinals, off + i));
            };
        }
    }

    pub fn resolve(
        self: *GroupKeyResolver,
        allocator: std.mem.Allocator,
        table: *GroupTable,
        key_cols: []const filter_eval.Batch.Column,
        row: usize,
        agg_calls: []const AggCall,
        pos_base: u64,
    ) !u32 {
        return self.resolveAt(allocator, table, key_cols, row, agg_calls, pos_base + row);
    }

    fn resolveAt(
        self: *GroupKeyResolver,
        allocator: std.mem.Allocator,
        table: *GroupTable,
        key_cols: []const filter_eval.Batch.Column,
        row: usize,
        agg_calls: []const AggCall,
        at: u64,
    ) !u32 {
        // Fixed-width keys never touch the scratch list: the image is a stack buffer, and the table hashes that
        // instead of the serialized bytes.
        if (table.fixed_stride != 0) {
            var image: [32]u8 = undefined;
            if (packFixedKey(key_cols, row, &image)) |n| if (n == table.fixed_stride) {
                return table.getOrInsertFixed(image[0..n], agg_calls, at);
            };
        }
        if (self.fast_path and !isRowNull(key_cols[0], row)) {
            const s = key_cols[0].string.values[row];
            if (table.sketch != null) if (try table.sketchRaw(s)) |id| return id;
            if (!self.memo_on) {
                try serializeRowKey(&self.scratch, allocator, key_cols, row);
                return table.getOrInsert(self.scratch.items, agg_calls, at);
            }
            if (self.memo_rows < memo_trial) {
                self.memo_rows += 1;
            } else if (self.memo_rows == memo_trial) {
                self.memo_rows += 1;
                if (self.memo_hits * 8 < memo_trial) {
                    self.memo_on = false;
                    self.memo.clearAndFree(allocator);
                    try serializeRowKey(&self.scratch, allocator, key_cols, row);
                    return table.getOrInsert(self.scratch.items, agg_calls, at);
                }
            }
            const id: SliceId = .{ .ptr = @intFromPtr(s.ptr), .len = s.len };
            const gop = try self.memo.getOrPut(allocator, id);
            if (gop.found_existing) {
                self.memo_hits +|= 1;
                return gop.value_ptr.*;
            }
            // On error the entry just reserved would hold an undefined value.
            errdefer _ = self.memo.remove(id);
            try serializeRowKey(&self.scratch, allocator, key_cols, row);
            const gid = try table.getOrInsert(self.scratch.items, agg_calls, at);
            gop.value_ptr.* = gid;
            return gid;
        }

        try serializeRowKey(&self.scratch, allocator, key_cols, row);
        return table.getOrInsert(self.scratch.items, agg_calls, at);
    }
};

test "GroupKeyResolver: the string memo stops where it does not hit and keys resolve the same either way" {
    const a = std.testing.allocator;
    const calls = [_]AggCall{.{ .func = .count, .arg = null, .where = null, .alias = "c", .result = .i64 }};
    const n = 3 * GroupKeyResolver.memo_trial;
    // Dictionary-shaped: 50 distinct slices, each repeated, so the memo answers nearly every row. Plain-shaped: every
    // row its own copy of one of the same 50 strings, so the memo never answers.
    var dict: [50][8]u8 = undefined;
    for (&dict, 0..) |*d, i| _ = std.fmt.bufPrint(d, "key-{d:0>4}", .{i}) catch unreachable;
    const plain = try a.alloc([8]u8, n);
    defer a.free(plain);
    const shared_vals = try a.alloc([]const u8, n);
    defer a.free(shared_vals);
    const plain_vals = try a.alloc([]const u8, n);
    defer a.free(plain_vals);
    for (0..n) |r| {
        shared_vals[r] = &dict[r % 50];
        plain[r] = dict[r % 50];
        plain_vals[r] = &plain[r];
    }
    for ([_][]const []const u8{ shared_vals, plain_vals }, [_]bool{ true, false }) |vals, memo_stays| {
        var table = GroupTable.init(a, 1 << 30);
        defer table.deinit();
        const cols = [_]filter_eval.Batch.Column{.{ .string = .{ .values = vals } }};
        var resolver = GroupKeyResolver.init(&cols);
        defer resolver.deinit(a);
        for (0..n) |r| {
            const gid = try resolver.resolve(a, &table, &cols, r, &calls, 0);
            try std.testing.expectEqual(@as(u32, @intCast(r % 50)), gid);
        }
        try std.testing.expectEqual(memo_stays, resolver.memo_on);
        try std.testing.expectEqual(@as(usize, 50), table.groupCount());
    }
}

