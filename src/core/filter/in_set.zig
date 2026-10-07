//! `col IN (...)` / `col NOT IN (...)`, bound once per query.
//!
//! The parser spells an IN list as an OR of `=` leaves (NOT IN: an AND of `!=`), and evaluated leaf by leaf that costs
//! one pass over the rows per literal. `InSet` holds the literals in the structure that tests a row fastest for their
//! type and number, so evaluation is one pass, and no shape branches on the data:
//!   - up to `simd_max` numbers: one vector compare against all of them;
//!   - integers spanning at most `bitmap_max_bits` values: one bit per value of the range, one load per row;
//!   - other numbers: an open-addressing table built so every literal sits within `window` slots of its home slot,
//!     so a probe is one vector load and compare of that window;
//!   - strings: compared directly when few, else the same table over their hashes, comparing bytes only on a match;
//!   - literals chosen so the table can't seat them (colliding homes) without growing past `max_growth` per literal:
//!     a sorted array searched without branches, for strings an array of their sorted hashes.
//! Equality is the leaves' own: `==` on numbers (so -0 matches +0 and NaN matches nothing), bytes on strings; a null
//! row matches neither IN nor NOT IN.

const std = @import("std");

/// Literals compared all at once, as one vector.
pub const simd_max = 8;
/// Strings compared directly up to this many; past it one hash per row is cheaper than the byte compares.
const str_direct_max = 2;
/// Slots a probe compares at once; every literal lies within this many slots of where it hashes.
const window = 8;
/// 128 KiB of bitmap: within L2, and one load per row beats any probe.
const bitmap_max_bits = 1 << 20;
/// Slots per literal the window table may grow to before the sorted fallback; 4 is where it starts.
const max_growth = 64;

pub const InSet = struct {
    col_idx: usize,
    /// NOT IN: true for a present row whose value is none of the literals.
    negated: bool,
    probe: Probe,

    pub const Probe = union(enum) {
        i32: NumSet(i32),
        i64: NumSet(i64),
        /// An unsigned column, compared on the stored bits: a u32 zero-extended from an INT32 lane, or a u64.
        u64: NumSet(u64),
        f32: NumSet(f32),
        f64: NumSet(f64),
        string: StrSet,
    };

    /// The same literals, the other way round: what NOT does to the list.
    pub fn negate(self: *const InSet, arena: std.mem.Allocator) !*const InSet {
        const out = try arena.create(InSet);
        out.* = self.*;
        out.negated = !self.negated;
        return out;
    }
};

/// Fibonacci hashing: the top `bits` bits of a multiply.
inline fn home(x: u64, bits: u6) usize {
    return @intCast((x *% 0x9E3779B97F4A7C15) >> @intCast(@as(u7, 64) - bits));
}

/// Slots `[0, 2^bits + window)` with each key at most `window - 1` past `home(key, bits)`; the window never wraps.
/// Doubles the table until every key fits, which a load factor of 1/4 makes the first try almost always; null once it
/// would pass `max_growth` slots per key, as keys picked to share a home can force.
fn windowTable(comptime K: type, arena: std.mem.Allocator, keys: []const K, empty: K) !?struct { slots: []K, bits: u6 } {
    const start = std.math.log2_int_ceil(usize, @max(keys.len * 4, 16));
    const limit = std.math.log2_int_ceil(usize, @max(keys.len * max_growth, 16));
    var bits: u6 = @intCast(start);
    outer: while (bits <= limit) : (bits += 1) {
        const slots = try arena.alloc(K, (@as(usize, 1) << bits) + window);
        @memset(slots, empty);
        for (keys) |k| {
            const h = home(@as(u64, k), bits);
            for (slots[h..][0..window]) |*s| {
                if (s.* == empty) {
                    s.* = k;
                    break;
                }
            } else {
                arena.free(slots);
                continue :outer;
            }
        }
        return .{ .slots = slots, .bits = bits };
    }
    return null;
}

/// Whether `v` is in `s`, sorted ascending and not empty. The halving steps depend only on the length, so the search
/// takes no branch on the data.
inline fn sortedContains(comptime T: type, s: []const T, v: T) bool {
    var base: usize = 0;
    var len = s.len;
    while (len > 1) {
        const half = len / 2;
        base = if (s[base + half - 1] < v) base + half else base;
        len -= half;
    }
    return s[base] == v;
}

pub fn NumSet(comptime T: type) type {
    const is_int = @typeInfo(T) == .int;
    // Floats are tabled by their bits; +0.0 makes -0 the same bits as +0.
    const Bits = @Int(.unsigned, @bitSizeOf(T));
    return struct {
        const Self = @This();
        const V = @Vector(simd_max, T);
        const W = @Vector(window, Bits);

        how: union(enum) {
            /// Padded with copies of the first literal.
            few: V,
            /// Bit `v - min` set for each literal.
            bitmap: struct { min: T, max: T, bits: []const u64 },
            /// See `windowTable`; `empty` marks a free slot and is no literal's bits.
            hash: struct { slots: []const Bits, bits: u6, empty: Bits },
            /// The literals' bits, sorted, when `windowTable` can't seat them.
            sorted: []const Bits,
        },

        /// `values`: the literals, in any order, duplicates allowed. NaN never equals anything, so it is dropped.
        pub fn init(arena: std.mem.Allocator, values: []const T) !Self {
            var distinct = try arena.alloc(T, values.len);
            var n: usize = 0;
            for (values) |v| {
                if (comptime !is_int) {
                    if (std.math.isNan(v)) continue;
                }
                distinct[n] = if (is_int) v else v + 0.0;
                n += 1;
            }
            distinct = distinct[0..n];
            std.mem.sort(T, distinct, {}, std.sort.asc(T));
            n = 0;
            for (distinct) |v| {
                if (n > 0 and distinct[n - 1] == v) continue;
                distinct[n] = v;
                n += 1;
            }
            distinct = distinct[0..n];

            // Only NaN: nothing matches, and NaN lanes compare false everywhere.
            if (distinct.len == 0) {
                if (comptime is_int) unreachable;
                return .{ .how = .{ .few = @splat(std.math.nan(T)) } };
            }
            if (distinct.len <= simd_max) {
                var lanes: [simd_max]T = @splat(distinct[0]);
                @memcpy(lanes[0..distinct.len], distinct);
                return .{ .how = .{ .few = lanes } };
            }
            if (is_int) {
                const min = distinct[0];
                const wide: i129 = @as(i129, distinct[distinct.len - 1]) - min + 1;
                if (wide <= bitmap_max_bits) {
                    const span: u64 = @intCast(wide);
                    const bits = try arena.alloc(u64, (span + 63) / 64);
                    @memset(bits, 0);
                    for (distinct) |v| {
                        const off: u64 = @as(Bits, @bitCast(v -% min));
                        bits[off / 64] |= @as(u64, 1) << @intCast(off % 64);
                    }
                    return .{ .how = .{ .bitmap = .{ .min = min, .max = distinct[distinct.len - 1], .bits = bits } } };
                }
            }
            const keys = try arena.alloc(Bits, distinct.len);
            for (distinct, keys) |v, *k| k.* = @bitCast(v);
            std.mem.sort(Bits, keys, {}, std.sort.asc(Bits));
            // A free-slot marker no literal can be: the first unsigned value no key takes (fewer keys than values).
            var empty: Bits = 0;
            for (keys) |k| {
                if (k != empty) break;
                empty += 1;
            }
            const t = try windowTable(Bits, arena, keys, empty) orelse return .{ .how = .{ .sorted = keys } };
            return .{ .how = .{ .hash = .{ .slots = t.slots, .bits = t.bits, .empty = empty } } };
        }

        pub inline fn contains(self: *const Self, v: T) bool {
            switch (self.how) {
                .few => |lanes| return @reduce(.Or, lanes == @as(V, @splat(v))),
                .bitmap => |b| {
                    if (comptime !is_int) unreachable;
                    // Both bounds, not one unsigned compare of `v - min`: that wraps back into range for a value near the
                    // other end of T. Out of range reads word 0, so the load needs no branch either.
                    const in_range = @intFromBool(v >= b.min) & @intFromBool(v <= b.max) == 1;
                    const off: u64 = if (in_range) @as(Bits, @bitCast(v -% b.min)) else 0;
                    return in_range and (b.bits[off / 64] >> @intCast(off % 64)) & 1 == 1;
                },
                .hash => |h| {
                    const key: Bits = @bitCast(if (is_int) v else v + 0.0);
                    const at = home(@as(u64, key), h.bits);
                    const slots: W = h.slots[at..][0..window].*;
                    // A NaN row has NaN bits, which no literal has; the empty marker matches free slots, so exclude it.
                    return @reduce(.Or, slots == @as(W, @splat(key))) and key != h.empty;
                },
                .sorted => |keys| return sortedContains(Bits, keys, @bitCast(if (is_int) v else v + 0.0)),
            }
        }
    };
}

pub const StrSet = struct {
    /// Distinct literals.
    values: []const []const u8,
    /// Empty when `values` are few enough to compare directly; else `windowTable` slots of hashes (0 = free) with
    /// `index + 1` of the literal beside each, or, when the table can't seat them (`sorted`), the distinct hashes in
    /// ascending order with `index[i]` the first literal of `hashes[i]`.
    hashes: []const u64 = &.{},
    index: []const u32 = &.{},
    bits: u6 = 0,
    sorted: bool = false,

    pub fn init(arena: std.mem.Allocator, values: []const []const u8) !StrSet {
        var set: std.StringArrayHashMapUnmanaged(void) = .empty;
        for (values) |v| try set.put(arena, v, {});
        const distinct = set.keys();
        if (distinct.len <= str_direct_max) return .{ .values = distinct };
        const keys = try arena.alloc(u64, distinct.len);
        for (distinct, keys) |v, *k| k.* = hash(v);
        // Distinct strings may share a hash; the table holds each hash once, and a match compares every literal of it.
        std.mem.sort(u64, keys, {}, std.sort.asc(u64));
        var n: usize = 0;
        for (keys) |k| {
            if (n > 0 and keys[n - 1] == k) continue;
            keys[n] = k;
            n += 1;
        }
        // Literals ordered by hash, so the ones sharing a hash sit together from `index`.
        const by_hash = try arena.dupe([]const u8, distinct);
        std.mem.sort([]const u8, by_hash, {}, struct {
            fn lt(_: void, x: []const u8, y: []const u8) bool {
                return hash(x) < hash(y);
            }
        }.lt);
        const t = try windowTable(u64, arena, keys[0..n], 0) orelse {
            const first = try arena.alloc(u32, n);
            var i: usize = 0;
            for (keys[0..n], first) |h, *f| {
                while (hash(by_hash[i]) != h) i += 1;
                f.* = @intCast(i);
            }
            return .{ .values = by_hash, .hashes = keys[0..n], .index = first, .sorted = true };
        };
        const index = try arena.alloc(u32, t.slots.len);
        @memset(index, 0);
        var i: usize = 0;
        while (i < by_hash.len) {
            const h = hash(by_hash[i]);
            const at = home(h, t.bits);
            const slot = at + std.mem.indexOfScalar(u64, t.slots[at..][0..window], h).?;
            index[slot] = @intCast(i + 1);
            while (i < by_hash.len and hash(by_hash[i]) == h) i += 1;
        }
        return .{ .values = by_hash, .hashes = t.slots, .index = index, .bits = t.bits };
    }

    /// Never 0, which marks a free slot.
    fn hash(v: []const u8) u64 {
        return std.hash.Wyhash.hash(0, v) | 1;
    }

    pub inline fn contains(self: *const StrSet, v: []const u8) bool {
        if (self.hashes.len == 0) {
            for (self.values) |lit| if (std.mem.eql(u8, lit, v)) return true;
            return false;
        }
        const h = hash(v);
        var i: usize = undefined;
        if (self.sorted) {
            var base: usize = 0;
            var len = self.hashes.len;
            while (len > 1) {
                const half = len / 2;
                base = if (self.hashes[base + half - 1] < h) base + half else base;
                len -= half;
            }
            if (self.hashes[base] != h) return false;
            i = self.index[base];
        } else {
            const at = home(h, self.bits);
            const slots: @Vector(window, u64) = self.hashes[at..][0..window].*;
            const hit: u8 = @bitCast(slots == @as(@Vector(window, u64), @splat(h)));
            if (hit == 0) return false;
            i = self.index[at + @ctz(hit)] - 1;
        }
        while (i < self.values.len and hash(self.values[i]) == h) : (i += 1) {
            if (std.mem.eql(u8, self.values[i], v)) return true;
        }
        return false;
    }
};

/// `sel_word &= (row present) and (row's value in `set`) != negated` for the 64 rows `values[base..]` covers, a
/// selection word at a time. Every row of a word is tested, selected or not, so the loop carries no branch on the
/// selection.
pub fn filterWords(
    comptime T: type,
    set: anytype,
    negated: bool,
    values: []const T,
    def_levels: ?[]const u32,
    max_def: u32,
    mask: []u64,
) void {
    const n = values.len;
    for (mask, 0..) |*word, w| {
        if (word.* == 0) continue;
        const base = w * 64;
        const rows = values[base..@min(base + 64, n)];
        var bits: u64 = 0;
        for (rows, 0..) |v, j| bits |= @as(u64, @intFromBool(set.contains(v) != negated)) << @intCast(j);
        if (def_levels) |dl| {
            var present: u64 = 0;
            for (dl[base..][0..rows.len], 0..) |d, j| present |= @as(u64, @intFromBool(d == max_def)) << @intCast(j);
            bits &= present;
        }
        word.* &= bits;
    }
}

const testing = std.testing;

test "each numeric shape answers as == does" {
    var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_state.deinit();
    const a = arena_state.allocator();
    var prng = std.Random.DefaultPrng.init(7);
    const r = prng.random();
    // few, bitmap (dense), hash (sparse), and the extremes of the type.
    const cases = [_]struct { k: usize, spread: i64, shape: []const u8 }{
        .{ .k = 5, .spread = 1000, .shape = "few" },
        .{ .k = 300, .spread = 600, .shape = "bitmap" },
        .{ .k = 40, .spread = 1 << 40, .shape = "hash" },
        .{ .k = 3000, .spread = 1 << 40, .shape = "hash" },
    };
    for (cases) |c| {
        const lits = try a.alloc(i64, c.k);
        for (lits) |*l| l.* = r.intRangeAtMost(i64, -c.spread, c.spread);
        if (c.spread > 1 << 20) {
            lits[0] = std.math.minInt(i64);
            lits[1] = std.math.maxInt(i64);
        }
        const set = try NumSet(i64).init(a, lits);
        try testing.expectEqualStrings(c.shape, @tagName(set.how));
        for (lits) |l| try testing.expect(set.contains(l));
        for (0..20_000) |_| {
            const v = if (r.boolean()) r.intRangeAtMost(i64, -c.spread - 5, c.spread + 5) else r.int(i64);
            const want = std.mem.indexOfScalar(i64, lits, v) != null;
            try testing.expectEqual(want, set.contains(v));
        }
    }
}

test "floats: -0 matches +0, NaN matches nothing, in every shape" {
    var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_state.deinit();
    const a = arena_state.allocator();
    const nan = std.math.nan(f64);
    for ([_]usize{ 3, 50 }) |k| {
        const lits = try a.alloc(f64, k);
        for (lits, 0..) |*l, i| l.* = @as(f64, @floatFromInt(i)) * 1.5 + 1;
        lits[0] = -0.0;
        lits[1] = nan;
        const set = try NumSet(f64).init(a, lits);
        try testing.expect(set.contains(0.0) and set.contains(-0.0));
        try testing.expect(!set.contains(nan));
        try testing.expect(set.contains(lits[k - 1]) and !set.contains(lits[k - 1] + 0.25));
        try testing.expect(!set.contains(std.math.inf(f64)) and !set.contains(-std.math.inf(f64)));
    }
    const only_nan = try NumSet(f32).init(a, &.{std.math.nan(f32)});
    try testing.expect(!only_nan.contains(std.math.nan(f32)) and !only_nan.contains(0));
}

test "strings: direct and hashed, duplicates and the empty string" {
    var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_state.deinit();
    const a = arena_state.allocator();
    for ([_]usize{ 2, 4, 500 }) |k| {
        const lits = try a.alloc([]const u8, k);
        for (lits, 0..) |*l, i| l.* = try std.fmt.allocPrint(a, "v{d}", .{i * 3});
        lits[k - 1] = "";
        lits[k - 2] = lits[0];
        const set = try StrSet.init(a, lits);
        try testing.expectEqual(k > str_direct_max, set.hashes.len > 0);
        for (lits) |l| try testing.expect(set.contains(l));
        try testing.expect(!set.contains("v1") and !set.contains("v0 ") and !set.contains("V0"));
    }
}

test "filterWords: nulls, the incoming selection and NOT IN" {
    var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_state.deinit();
    const a = arena_state.allocator();
    const n = 200;
    var vals: [n]i32 = undefined;
    var defs: [n]u32 = undefined;
    for (&vals, &defs, 0..) |*v, *d, i| {
        v.* = @intCast(i % 17);
        d.* = @intFromBool(i % 7 != 0);
    }
    const set = try NumSet(i32).init(a, &.{ 1, 4, 9, 16, 100, 2, 3, 5, 6, 7 });
    for ([_]bool{ false, true }) |negated| {
        var mask: [(n + 63) / 64]u64 = @splat(0);
        for (0..n) |i| if (i % 3 != 0) {
            mask[i / 64] |= @as(u64, 1) << @intCast(i % 64);
        };
        filterWords(i32, &set, negated, &vals, &defs, 1, &mask);
        for (0..n) |i| {
            const listed = set.contains(vals[i]);
            const want = i % 3 != 0 and defs[i] == 1 and listed != negated;
            try testing.expectEqual(want, (mask[i / 64] >> @intCast(i % 64)) & 1 == 1);
        }
    }
}

/// The multiplicative inverse of `home`'s multiplier: `k * inverse` hashes home to `k * 2^64`, slot 0 at every size.
fn homeInverse() u64 {
    const m: u64 = 0x9E3779B97F4A7C15;
    var x: u64 = m;
    for (0..6) |_| x *%= 2 -% m *% x;
    return x;
}

test "literals chosen to share every home fall back to a sorted search, in bounded memory" {
    var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_state.deinit();
    const a = arena_state.allocator();
    const inv = homeInverse();
    try testing.expectEqual(@as(u64, 1), inv *% 0x9E3779B97F4A7C15);
    var keys: [window + 1]u64 = undefined;
    for (&keys, 0..) |*k, i| k.* = @as(u64, i) *% inv;
    std.mem.sort(u64, &keys, {}, std.sort.asc(u64));
    try testing.expect(try windowTable(u64, a, &keys, 1) == null);

    var lits: [window + 1]i64 = undefined;
    for (&lits, 0..) |*l, i| l.* = @bitCast(@as(u64, i) *% inv);
    const set = try NumSet(i64).init(a, &lits);
    try testing.expectEqualStrings("sorted", @tagName(set.how));
    for (lits) |l| try testing.expect(set.contains(l) and !set.contains(l +% 1));
}

test "a bitmap at one end of the type rejects values from the other end" {
    var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_state.deinit();
    const a = arena_state.allocator();
    inline for (.{ i32, i64, u64 }) |T| {
        var lits: [300]T = undefined;
        for (&lits, 0..) |*l, i| l.* = std.math.maxInt(T) - @as(T, @intCast(2 * i));
        const set = try NumSet(T).init(a, &lits);
        try testing.expectEqualStrings("bitmap", @tagName(set.how));
        var v: T = std.math.minInt(T);
        for (0..2000) |_| {
            try testing.expect(!set.contains(v));
            v += 1;
        }
        for (lits) |l| try testing.expect(set.contains(l) and !set.contains(l - 1));
    }
}

test "regression: nine adversarial integer literals bind within one MiB" {
    const backing = try testing.allocator.alloc(u8, 1 << 20);
    defer testing.allocator.free(backing);
    var fixed = std.heap.FixedBufferAllocator.init(backing);
    const multiplier: u64 = 0x9E3779B97F4A7C15;
    const inverse: u64 = 0xf1de83e19937733d;
    try testing.expectEqual(@as(u64, 1), inverse *% multiplier);
    var literals: [9]i64 = undefined;
    for (&literals, 0..) |*literal, i| literal.* = @bitCast(@as(u64, @intCast(i)) *% inverse);
    const set = try NumSet(i64).init(fixed.allocator(), &literals);
    for (literals) |literal| try testing.expect(set.contains(literal));
    try testing.expect(!set.contains(1));
}
