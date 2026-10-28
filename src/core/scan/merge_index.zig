const std = @import("std");
const testing = std.testing;
const expr_agg = @import("../expr/agg.zig");
const core_bytes = @import("../bytes.zig");
const keyEql = core_bytes.keyEql;
const KeyHashMap = core_bytes.KeyHashMap;

const HashKey = struct { bytes: []const u8, hash: u64 };

const HashKeyCtx = struct {
    pub fn hash(_: @This(), k: HashKey) u64 {
        return k.hash;
    }
    pub fn eql(_: @This(), k: HashKey, stored: []const u8) bool {
        return keyEql(k.bytes, stored);
    }
};

/// A merge's key -> merged group index, sized up front for every key it will see. Fixed-width keys (every table with
/// the same `fixed_stride`) may be copied into the slots of an open-addressed table, so a probe compares the slot's
/// own bytes instead of following a slice into a source table; other keys go through a map borrowing them (`Inline`).
pub const MergeIndex = union(enum) {
    bytes: KeyHashMap(u32),
    fixed9: FixedMergeIndex(9),
    fixed18: FixedMergeIndex(18),
    fixed27: FixedMergeIndex(27),

    const Entry = struct { found: bool, value: *u32 };

    /// Which fixed-width keys go inline. An inline slot of a 27-byte key is 32 bytes, against the map's 21 per slot.
    const Inline = enum {
        /// Every stride, at most 2/3 full: the in-place merge, which copies no group state, so its index is the most
        /// it holds per group (up to three times the map's bytes at 27-byte keys).
        any,
        /// 9-byte keys only, at the map's own capacity: 16-byte slots, so never more bytes than the map would take. The
        /// merges that copy state, where the index sits beside the state.
        within_map,
    };

    pub fn init(allocator: std.mem.Allocator, tables: []const expr_agg.GroupTable, n: usize, mode: Inline) !MergeIndex {
        const n32: u32 = std.math.cast(u32, n) orelse return error.OutOfMemory;
        var stride: u8 = if (tables.len > 0) tables[0].fixed_stride else 0;
        for (tables) |gt| if (gt.fixed_stride != stride) {
            stride = 0;
        };
        if (mode == .within_map and stride != 9) stride = 0;
        // `std.HashMap.capacityForSize` at its default 80% load, for `within_map`.
        const load = std.hash_map.default_max_load_percentage;
        const map_cap = std.math.ceilPowerOfTwo(u64, @as(u64, n32) * 100 / load + 1) catch return error.OutOfMemory;
        const cap: u64 = switch (mode) {
            .any => @as(u64, n32) + n32 / 2 + 1,
            .within_map => map_cap,
        };
        return switch (stride) {
            9 => .{ .fixed9 = try .init(allocator, cap) },
            18 => .{ .fixed18 = try .init(allocator, cap) },
            27 => .{ .fixed27 = try .init(allocator, cap) },
            else => blk: {
                var map = KeyHashMap(u32).init(allocator);
                errdefer map.deinit();
                try map.ensureTotalCapacity(n32);
                break :blk .{ .bytes = map };
            },
        };
    }

    pub fn deinit(self: *MergeIndex) void {
        switch (self.*) {
            .bytes => |*m| m.deinit(),
            inline else => |*f| f.allocator.free(f.slots),
        }
    }

    /// `hash` is `std.hash.Wyhash.hash(0, key)`, the map's own hash. A new entry's value is the caller's to set.
    pub inline fn getOrPut(self: *MergeIndex, key: []const u8, hash: u64) Entry {
        switch (self.*) {
            .bytes => |*m| {
                const got = m.getOrPutAssumeCapacityAdapted(HashKey{ .bytes = key, .hash = hash }, HashKeyCtx{});
                if (!got.found_existing) got.key_ptr.* = key;
                return .{ .found = got.found_existing, .value = got.value_ptr };
            },
            inline else => |*f| return f.getOrPut(key, hash),
        }
    }
};

fn FixedMergeIndex(comptime stride: usize) type {
    return struct {
        allocator: std.mem.Allocator,
        slots: []Slot,

        /// 16, 24 or 32 bytes: a stride-9 slot is a quarter of a cache line.
        const Slot = extern struct { key: [stride]u8, id: u32 };
        const empty = std.math.maxInt(u32);

        /// At least `min_cap` slots, more than the keys it will hold, so a probe always ends at an empty slot.
        fn init(allocator: std.mem.Allocator, min_cap: u64) !@This() {
            const want = std.math.cast(usize, @max(16, min_cap)) orelse return error.OutOfMemory;
            const cap = std.math.ceilPowerOfTwo(usize, want) catch return error.OutOfMemory;
            const slots = try allocator.alloc(Slot, cap);
            for (slots) |*s| s.id = empty;
            return .{ .allocator = allocator, .slots = slots };
        }

        /// Never more keys than `init` was given, so an empty slot is always found. Ids are below that count, so none
        /// is `empty`.
        inline fn getOrPut(self: *@This(), key: []const u8, hash: u64) MergeIndex.Entry {
            std.debug.assert(key.len == stride);
            const mask = self.slots.len - 1;
            var i: usize = @intCast(hash & mask);
            while (true) : (i = (i + 1) & mask) {
                const s = &self.slots[i];
                if (s.id == empty) {
                    s.key = key[0..stride].*;
                    return .{ .found = false, .value = &s.id };
                }
                if (keyEql(&s.key, key)) return .{ .found = true, .value = &s.id };
            }
        }
    };
}

test "MergeIndex: inline fixed-width keys find what the byte map finds, at every stride and either size" {
    var prng = std.Random.DefaultPrng.init(0x5eed);
    const rand = prng.random();
    inline for (.{ 9, 18, 27 }) |stride| {
        const pool = 1500;
        var images: [pool][stride]u8 = undefined;
        for (&images, 0..) |*img, i| {
            rand.bytes(img);
            // Pairs that differ only in the last byte, past the first 8-byte word the compare loads.
            if (i % 2 == 1) {
                img.* = images[i - 1];
                img[stride - 1] +%= 1;
            }
        }
        var t = expr_agg.GroupTable.init(testing.allocator, 1 << 20);
        t.fixed_stride = stride;
        const tables = [_]expr_agg.GroupTable{t};
        const n = 5000;
        for ([_]MergeIndex.Inline{ .any, .within_map }) |mode| {
            var fixed = try MergeIndex.init(testing.allocator, &tables, n, mode);
            defer fixed.deinit();
            var bytes: MergeIndex = .{ .bytes = KeyHashMap(u32).init(testing.allocator) };
            defer bytes.deinit();
            try bytes.bytes.ensureTotalCapacity(n);
            if (mode == .within_map) {
                // 9-byte keys only, and in no more bytes than the map's keys, values and metadata.
                try testing.expectEqual(stride == 9, fixed != .bytes);
                if (stride == 9) try testing.expect(fixed.fixed9.slots.len * 16 <= bytes.bytes.capacity() * 21);
            } else try testing.expect(fixed != .bytes);
            var next: u32 = 0;
            for (0..n) |_| {
                const key: []const u8 = &images[rand.uintLessThan(usize, pool)];
                const h = std.hash.Wyhash.hash(0, key);
                const f = fixed.getOrPut(key, h);
                const b = bytes.getOrPut(key, h);
                try testing.expectEqual(b.found, f.found);
                if (b.found) {
                    try testing.expectEqual(b.value.*, f.value.*);
                } else {
                    b.value.* = next;
                    f.value.* = next;
                    next += 1;
                }
            }
        }
    }
}

