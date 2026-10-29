const CountSketch = @import("census.zig").CountSketch;
const sinkSketchRows = @import("group_table.zig").sinkSketchRows;
const std = @import("std");
const GroupTable = @import("group_table.zig").GroupTable;
const AggCall = @import("types.zig").AggCall;
const filter_selection = @import("../../filter/selection.zig");
const Error = @import("types.zig").Error;
const expr_ast = @import("../ast.zig");
const filter_eval = @import("../../filter/eval.zig");
const contracts = @import("contracts.zig");

pub const isRowNull = contracts.isRowNull;
pub const serializeRowKey = contracts.serializeRowKey;
pub const fixedGroupStride = contracts.fixedGroupStride;
pub const packFixedKey = contracts.packFixedKey;
pub const canonicalKey = contracts.canonicalKey;
pub const fixedImage = contracts.fixedImage;
pub const expandFixed = contracts.expandFixed;
pub const hashFixed = contracts.hashFixed;
pub const KeyArena = contracts.KeyArena;

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


test "a sketched table counts every row in the count pass and groups only admitted cells in the admit pass" {
    const calls = [_]AggCall{.{ .func = .count, .arg = null, .where = null, .alias = "c", .result = .i64 }};
    // Long keys take the unframed path, short ones the framed one; a null key is framed too.
    const vals = [_][]const u8{
        "http://example.com/a-long-enough-path", "short",
        "http://example.com/a-long-enough-path", "http://example.com/another/long/path",
        "short",                                 "",
    };
    const defs = [_]u32{ 1, 1, 1, 1, 1, 0 };
    const cols = [_]filter_eval.Batch.Column{.{ .string = .{
        .values = &vals,
        .def_levels = &defs,
        .max_def = 1,
        .has_nulls = true,
    } }};
    const a = std.testing.allocator;
    for ([_]u8{ 0, 9 }) |fixed| {
        var cells: [1 << 10]u32 = @splat(0);
        var sketch: CountSketch = .{ .cells = &cells, .shift = 64 - 10 };
        var table = GroupTable.init(a, 1 << 20);
        defer table.deinit();
        table.sketch = &sketch;
        var ids: [vals.len]u32 = undefined;
        var sel = try filter_selection.SelectionVector.init(a, vals.len);
        defer sel.deinit();
        if (fixed == 0) {
            var resolver = GroupKeyResolver.init(&cols);
            defer resolver.deinit(a);
            for (&ids, 0..) |*id, r| id.* = try resolver.resolve(a, &table, &cols, r, &calls, r);
        } else {
            // A fixed-width table sketches by its own image hash.
            table.fixed_stride = fixed;
            for (&ids, 0..) |*id, r| {
                var image: [9]u8 = @splat(0);
                image[0] = @intFromBool(r != 5);
                image[1] = @intCast(vals[r].len);
                id.* = try table.getOrInsertFixed(&image, &calls, r);
            }
        }
        try sinkSketchRows(&table, &ids, &sel);
        try std.testing.expectEqual(@as(usize, 0), table.groupCount());
        try std.testing.expectEqual(@as(usize, 0), sel.count());
        var total: u64 = 0;
        for (cells) |c| total += c;
        try std.testing.expectEqual(@as(u64, vals.len), total);
        // The same key always lands in the same cell.
        try std.testing.expectEqual(ids[0], ids[2]);
        try std.testing.expectEqual(ids[1], ids[4]);

        // Admit only the first key's cell: its rows get one group, every other row is dropped uncounted.
        var admit: [(1 << 10) / 64]u64 = @splat(0);
        const cell = std.math.maxInt(u32) - ids[0];
        admit[cell / 64] |= @as(u64, 1) << @intCast(cell % 64);
        sketch.admit = &admit;
        @memset(&cells, 0);
        var sel2 = try filter_selection.SelectionVector.init(a, vals.len);
        defer sel2.deinit();
        if (fixed == 0) {
            var resolver = GroupKeyResolver.init(&cols);
            defer resolver.deinit(a);
            for (&ids, 0..) |*id, r| id.* = try resolver.resolve(a, &table, &cols, r, &calls, r);
        } else for (&ids, 0..) |*id, r| {
            var image: [9]u8 = @splat(0);
            image[0] = @intFromBool(r != 5);
            image[1] = @intCast(vals[r].len);
            id.* = try table.getOrInsertFixed(&image, &calls, r);
        }
        try sinkSketchRows(&table, &ids, &sel2);
        try std.testing.expect(table.groupCount() >= 1);
        try std.testing.expectEqual(@as(u32, 0), ids[0]);
        try std.testing.expectEqual(@as(u64, 0), table.first.items[0]);
        for (ids, 0..) |id, r| try std.testing.expectEqual(id < table.groupCount(), sel2.isActive(r));
        for (cells) |c| try std.testing.expectEqual(@as(u32, 0), c);
    }
}



test "batched fixed lookups preserve sparse selections nulls NaNs and signed zeros" {
    const n = 129;
    var values: [n]f64 = undefined;
    var defs: [n]u32 = undefined;
    for (0..n) |r| {
        values[r] = switch (r % 4) {
            0 => -0.0,
            1 => 0.0,
            2 => @bitCast(@as(u64, 0x7ff8000000000001)),
            else => 7.0,
        };
        defs[r] = if (r % 19 == 0) 0 else 1;
    }
    const cols = [_]filter_eval.Batch.Column{.{ .f64 = .{
        .values = &values,
        .def_levels = &defs,
        .max_def = 1,
        .has_nulls = true,
    } }};
    const calls = [_]AggCall{.{ .func = .count, .arg = null, .where = null, .alias = "c", .result = .i64 }};
    var sel = try filter_selection.SelectionVector.init(std.testing.allocator, n);
    defer sel.deinit();
    for (0..n) |r| if (r % 11 == 0) {
        sel.set(r, false);
    };
    var scalar = GroupTable.init(std.testing.allocator, 1 << 20);
    defer scalar.deinit();
    var batched = GroupTable.init(std.testing.allocator, 1 << 20);
    defer batched.deinit();
    scalar.fixed_stride = 9;
    batched.fixed_stride = 9;
    var resolver = GroupKeyResolver.init(&cols);
    defer resolver.deinit(std.testing.allocator);
    var ids: [n]u32 = @splat(0);
    try resolver.resolveBatch(std.testing.allocator, &batched, &cols, &calls, 1000, &sel, &ids, null);
    for (0..n) |r| if (sel.isActive(r)) {
        const id = try resolver.resolve(std.testing.allocator, &scalar, &cols, r, &calls, 1000);
        try std.testing.expectEqual(id, ids[r]);
    };
    try std.testing.expectEqual(scalar.groupCount(), batched.groupCount());
    try std.testing.expectEqualSlices(u64, scalar.first.items, batched.first.items);
}

test "resolveBatch on a gathered selection keeps the original first row" {
    const values = [_]i32{ 10, 30 };
    const cols = [_]filter_eval.Batch.Column{.{ .i32 = .{ .values = &values } }};
    const calls = [_]AggCall{.{ .func = .count, .arg = null, .where = null, .alias = "c", .result = .i64 }};
    var sel = try filter_selection.SelectionVector.init(std.testing.allocator, 2);
    defer sel.deinit();
    var table = GroupTable.init(std.testing.allocator, 1 << 20);
    defer table.deinit();
    var resolver = GroupKeyResolver.init(&cols);
    defer resolver.deinit(std.testing.allocator);
    var ids: [2]u32 = @splat(0);
    const ordinals = [_]u32{ 1, 3 };
    try resolver.resolveBatch(std.testing.allocator, &table, &cols, &calls, 100, &sel, &ids, &ordinals);
    try std.testing.expectEqual(@as(usize, 2), table.first.items.len);
    try std.testing.expectEqual(@as(u64, 101), table.first.items[0]);
    try std.testing.expectEqual(@as(u64, 103), table.first.items[1]);
}

