//! Exact grouping of short byte keys over owned records in complete owner partitions.
//!
//! One record layout, one scatter and one group state serve every admitted shape: a COUNT(*) + SUM full answer in
//! first-appearance order, and bounded top-H selections ranked by one signed INT64 SUM or by COUNT(*). A key of at
//! most 16 bytes (15 for top-H) is held inline, with exact length and NULL identity, so NULL, empty and
//! embedded-zero keys are distinct; a longer key declines the attempt. Every key is complete in one owner partition
//! before it is folded, ordered or selected. A scalar DISTINCT counts the complete non-NULL owners without
//! retaining groups or merging worker sets.
const std = @import("std");
const eval = @import("filter/eval.zig");
const Selection = @import("filter/selection.zig").SelectionVector;
const memory = @import("memory.zig");
const spawn = @import("spawn.zig");
const hw = @import("hw.zig");
const HugePageAdvisor = @import("huge_pages.zig").HugePageAdvisor;
const now = @import("../clock.zig").monoNs;
pub const Lane = @import("owner_lanes.zig").Lane;
pub const PackedLayout = @import("owner_lanes.zig").Layout;
pub const Packed = @import("owner_packed.zig").Owner;
pub const Pairs = @import("owner_pairs.zig").Owner;
pub const SelectedOwner = @import("owner_sink.zig").Packed;

test {
    _ = @import("owner_lanes.zig");
    _ = @import("owner_cache.zig");
    _ = @import("owner_records.zig");
    _ = @import("owner_fold.zig");
    _ = @import("owner_packed.zig");
    _ = @import("owner_pairs.zig");
    _ = @import("owner_sink.zig");
    _ = @import("owner_result.zig");
    _ = @import("owner_cost.zig");
}

const none = std.math.maxInt(u32);
const ordinal_bits = 27;
const ordinal_mask: u32 = (1 << ordinal_bits) - 1;
const present: u32 = 1 << 30;
const null_key: u32 = 1 << 31;
const stack_size = 1 << 20;
const cache_headroom = 64 << 10;
const producer_row_bytes = 128;
pub const max_workers = 64;
/// The full answer's fixed fanout: its fold writes groups contiguously per partition.
pub const full_parts = 2048;

pub const Cut = struct { k: usize, descending: bool };
pub const Kind = enum {
    /// COUNT(*) and SUM per key, every group, in first-appearance order.
    full,
    /// Top-H by SUM.
    sum_top,
    /// Top-H by COUNT(*); no value lane.
    count_top,
};

/// A key's inline identity: zero-padded bytes, exact length (high ordinal bits) and NULL (low flag bit).
/// `tagged` (top-H): at most 15 bytes, the last byte holding the length (255 for NULL), so one comparison decides
/// identity. Untagged (the full answer): up to 16 bytes, told apart by the length and NULL bit kept beside them.
/// Built in registers from two loads of the key (the padded bytes, little-endian): no buffer is stored and reread.
fn inlineKey(key: ?[]const u8, comptime tagged: bool) error{ExceededMemoryBudget}!u128 {
    const k = key orelse return if (tagged) @as(u128, 255) << 120 else 0;
    if (k.len > @as(usize, if (tagged) 15 else 16)) return error.ExceededMemoryBudget;
    var lo: u64 = 0;
    var hi: u64 = 0;
    if (k.len >= 8) {
        lo = std.mem.readInt(u64, k[0..8], .little);
        // The last eight bytes, less the ones already in `lo`.
        if (k.len > 8) hi = std.mem.readInt(u64, k[k.len - 8 ..][0..8], .little) >> @intCast(8 * (16 - k.len));
    } else if (k.len >= 4) {
        const tail: u64 = std.mem.readInt(u32, k[k.len - 4 ..][0..4], .little);
        lo = std.mem.readInt(u32, k[0..4], .little) | (if (k.len > 4) tail >> @intCast(8 * (8 - k.len)) else 0) << 32;
    } else for (k, 0..) |b, i| {
        lo |= @as(u64, b) << @intCast(8 * i);
    }
    if (tagged) hi |= @as(u64, k.len) << 56;
    return @as(u128, hi) << 64 | lo;
}

/// Partition and probe hash of an inline identity; never part of identity or order.
inline fn mixKey(bits: u128, len: u32) u64 {
    const m = @as(u128, @as(u64, @truncate(bits)) ^ 0x9E3779B97F4A7C15) *
        (@as(u64, @truncate(bits >> 64)) ^ 0xD6E8FEB86659FD93 ^ (@as(u64, len) << 32));
    return @as(u64, @truncate(m)) ^ @as(u64, @truncate(m >> 64));
}

/// A group's exact state over keys of `KeyBits` (u128 inline bytes, u64 integer bits).
pub fn GroupOf(comptime KeyBits: type) type {
    return struct {
        const integer = KeyBits == u64;
        const first_mask: u32 = if (integer) (1 << 31) - 1 else ordinal_mask;
        const sum_present: u32 = if (integer) 1 << 31 else present;

        // Eight-byte alignment keeps this state at 40 bytes without narrowing the exact sum.
        sum: i128 align(8) = 0,
        key: KeyBits align(8),
        /// Integer keys: 31 count bits and a SUM-present bit. Byte keys: 30 count bits, SUM-present and key-NULL.
        count: u32,
        /// Integer keys: 31 ordinal bits and a key-NULL bit. Byte keys: 27 ordinal bits and five length bits.
        first: u32,

        pub fn ordinal(g: @This()) u32 {
            return g.first & first_mask;
        }
        pub fn keyBytes(g: *const @This()) ?[]const u8 {
            return if (g.count & null_key != 0) null else std.mem.asBytes(&g.key)[0 .. g.first >> ordinal_bits];
        }
        pub fn countValue(g: @This()) i64 {
            return g.count & (sum_present - 1);
        }
        pub fn sumValue(g: @This()) ?i128 {
            return if (g.count & sum_present != 0) g.sum else null;
        }
        /// An integer key (`Codec.int64`); null for the NULL key.
        pub fn keyValue(g: @This()) ?i64 {
            return if (g.first & null_key != 0) null else @bitCast(@as(u64, @truncate(g.key)));
        }
    };
}
comptime {
    std.debug.assert(@sizeOf(GroupOf(u128)) == 40 and @sizeOf(GroupOf(u64)) == 32);
}

fn cacheBudget() usize {
    return hw.l2BytesPerWorkerOr(640 << 10);
}
/// Producer window for the top-H layouts: decoded views, lanes and an emitted record share the cache headroom.
pub fn batchRows(forced: ?usize) usize {
    if (forced) |n| return if (n == 0) 0 else powerOfTwo(n, 64, 16384);
    return powerOfTwo((cacheBudget() -| cache_headroom) / producer_row_bytes, 64, 16384);
}
fn powerOfTwo(n: usize, lo: usize, hi: usize) usize {
    return @as(usize, 1) << @intCast(std.math.log2_int(usize, std.math.clamp(n, lo, hi)));
}

/// How a key is held: up to 16 bytes inline, or a signed 64-bit integer.
pub const Codec = enum { bytes, int64 };

/// Exclusive row-count bound shared by admission, EXPLAIN and record construction. Byte keys reserve five
/// ordinal bits for their length; integer keys use one for NULL and keep the old 31-bit row/count capacity.
pub fn rowLimit(comptime codec: Codec) usize {
    return @as(usize, 1) << (if (codec == .int64) @as(u6, 31) else ordinal_bits);
}

pub fn admitsRows(comptime codec: Codec, rows: usize) bool {
    return rows < rowLimit(codec);
}

pub const Full = Owner(.full, .bytes);
pub const SumTop = Owner(.sum_top, .bytes);
pub const CountTop = Owner(.count_top, .bytes);
pub const IntFull = Owner(.full, .int64);
pub const IntSumTop = Owner(.sum_top, .int64);
pub const IntCountTop = Owner(.count_top, .int64);

pub fn Owner(comptime kind: Kind, comptime codec: Codec) type {
    const has_value = kind != .count_top;
    const Key = if (codec == .bytes) []const u8 else i64;
    const KeyBits = if (codec == .bytes) u128 else u64;
    // Byte top-H identities carry their length and NULL in the bits; integers and full answers compare a flag.
    const tagged = codec == .bytes and kind != .full;
    const cached_count = codec == .int64 and kind == .count_top;
    const CountEntry = struct { key: u64 = 0, first: u32 = 0, count: u32 = 0 };
    const CountPolicy = struct {
        pub const Entry = CountEntry;
        pub fn validate(r: Entry) error{BadPartial}!void {
            if (r.count == 0 or r.count >= null_key) return error.BadPartial;
        }
        pub fn active(r: Entry) bool {
            return r.count != 0;
        }
        pub fn same(a: Entry, b: Entry) bool {
            return (a.first & null_key) == (b.first & null_key) and a.key == b.key;
        }
        pub fn hash(r: Entry) u64 {
            return if (r.first & null_key != 0) 0x621985775bcbd930 else mixKey(r.key, 0);
        }
        pub fn clear(r: *Entry) void {
            r.count = 0;
        }
        pub fn coalesce(a: *Entry, b: Entry) error{}!bool {
            const count = std.math.add(u32, a.count, b.count) catch return false;
            a.count = count;
            a.first = @min(a.first, b.first);
            return true;
        }
    };
    const CountCache = @import("owner_cache.zig").Local(CountPolicy);
    // The integer full answer's exact local partials: COUNT, a checked i64 SUM and the first ordinal per key.
    const cached_full = codec == .int64 and kind == .full;
    const FullEntry = struct { key: u64 = 0, sum: i64 = 0, first: u32 = 0, count: u32 = 0 };
    // A partial's weight shares its record's flags with the key and value NULL bits, so it stays below 2^30.
    const full_count_mask: u32 = (1 << 30) - 1;
    const full_present: u32 = 1 << 31;
    const FullPolicy = struct {
        pub const Entry = FullEntry;
        pub fn validate(r: Entry) error{BadPartial}!void {
            if (r.count & full_count_mask == 0) return error.BadPartial;
        }
        pub fn active(r: Entry) bool {
            return r.count & full_count_mask != 0;
        }
        pub fn same(a: Entry, b: Entry) bool {
            return (a.first & null_key) == (b.first & null_key) and a.key == b.key;
        }
        pub fn hash(r: Entry) u64 {
            return CountPolicy.hash(.{ .key = r.key, .first = r.first });
        }
        pub fn clear(r: *Entry) void {
            r.count = 0;
        }
        /// All or nothing: a COUNT past the weight bits or an i64 SUM overflow leaves `a` unchanged for a flush.
        pub fn coalesce(a: *Entry, b: Entry) error{}!bool {
            const count = (a.count & full_count_mask) + (b.count & full_count_mask);
            if (count > full_count_mask) return false;
            var sum = a.sum;
            if (b.count & full_present != 0) {
                const add = @addWithOverflow(a.sum, b.sum);
                if (add[1] != 0) return false;
                sum = add[0];
            }
            a.count = count | ((a.count | b.count) & full_present);
            a.sum = sum;
            a.first = @min(a.first, b.first);
            return true;
        }
    };
    const FullCache = @import("owner_cache.zig").Local(FullPolicy);
    const CountLayout = PackedLayout(u128, &.{}, u32);
    const CountFold = @import("owner_fold.zig").Table(CountLayout);
    // Ordinals are below 2^31. A tagged COUNT partial stores its exact weight in flags, with NULL in bit zero.
    const partial: u32 = 1 << 31;

    return struct {
        const Self = @This();
        pub const Group = GroupOf(KeyBits);
        pub const Record = if (has_value) extern struct {
            key: KeyBits align(8),
            value: i64,
            /// Integer keys use the full ordinal; byte keys keep their exact length in the high five bits.
            ordinal: u32,
            /// Key NULL, value NULL, then 30 hash bits.
            flags: u32,
        } else extern struct {
            key: KeyBits align(8),
            ordinal: u32,
            flags: u32,
        };
        const Records = @import("owner_records.zig").Writer(Record, u32);
        const Block = Records.Block;
        /// Worst-case group scratch plus fewer than four u32 hash slots per input row after rounding.
        const table_row_bytes = @sizeOf(Group) + 16;

        fn countCacheEntries() usize {
            return if (cached_count) @import("owner_cache.zig").capacity(@sizeOf(CountEntry)) else 0;
        }
        fn fullCacheEntries() usize {
            return if (cached_full) @import("owner_cache.zig").tableCapacity(@sizeOf(FullEntry)) else 0;
        }

        /// The aggregate a top-H layout ranks by: COUNT, or SUM (NULL when no value was present).
        pub fn rank(g: Group) ?i128 {
            return if (kind == .count_top) g.countValue() else g.sumValue();
        }
        fn worse(descending: bool, a: Group, b: Group) std.math.Order {
            const av = rank(a);
            const bv = rank(b);
            if (av != null and bv == null) return .gt;
            if (av == null and bv != null) return .lt;
            if (av != null) {
                const order = if (descending) std.math.order(av.?, bv.?) else std.math.order(bv.?, av.?);
                if (order != .eq) return order;
            }
            return std.math.order(b.ordinal(), a.ordinal());
        }
        const Heap = std.PriorityQueue(Group, bool, worse);

        /// The top-H fanout: owner tables sized to the per-worker L2 share, bounded by producer frontiers in the
        /// shared L3 share. A conservative uniform estimate; skew changes performance, never key identity.
        pub fn partitionCount(rows: usize, workers: usize, producer_rows: usize, forced: ?usize) usize {
            if (forced) |n| return powerOfTwo(if (n == 0) 2048 else n, 64, 16384);
            const tables = rows *| table_row_bytes;
            const count = std.math.divCeil(usize, tables, @max(1, cacheBudget() -| cache_headroom)) catch 16384;
            const l2_parts = std.math.ceilPowerOfTwo(usize, std.math.clamp(count, 64, 16384)) catch 16384;
            const l3 = hw.l3BytesPerActiveWorker(workers);
            // Ordinary memory-driven decoding has no bounded cache window. Unknown L3 keeps the L2 policy too.
            if (l3 == 0 or producer_rows == 0) return l2_parts;
            const producer_fixed = producer_rows *| producer_row_bytes +| cache_headroom;
            const frontier_bytes = @sizeOf(Block) + 3 * @sizeOf(u32);
            const capacity = (l3 -| producer_fixed) / frontier_bytes;
            if (capacity < 64) return l2_parts;
            const parts = @min(l2_parts, powerOfTwo(capacity, 64, 16384));
            // Fewer write streams may trade L2-resident fold scratch for L3 residency. If neither phase can fit
            // together under these estimates, preserve the L2 fold target instead of making both spill.
            if (tables > (l3 -| cache_headroom) *| parts) return l2_parts;
            return parts;
        }

        pub const Local = struct {
            state: *State,
            ordinal_base: u32 = 0,
            records: Records.Frontier = .{},
            /// Key and value NULL bits seen (the full answer's column nullability).
            nulls: u32 = 0,
            run_item: usize = 0,
            run_stopped: bool = false,
            count_cache: CountCache = .{},
            full_cache: FullCache = .{},
            contributed_rows: usize = 0,
            cache_seen: u32 = 0,
            cache_hits: u32 = 0,
            cache_disabled: bool = false,

            fn emitCount(l: *Local, entry: CountEntry) error{ExceededMemoryBudget}!void {
                const kn = entry.first & null_key != 0;
                try l.appendRecord(
                    entry.key,
                    {},
                    entry.first & Group.first_mask,
                    kn,
                    0,
                    CountPolicy.hash(entry),
                    entry.count,
                );
            }

            fn flushCount(l: *Local, entry: *CountEntry) error{ExceededMemoryBudget}!void {
                try CountCache.flushEntry(entry, l, emitCount);
            }

            fn emitFull(l: *Local, entry: FullEntry) error{ExceededMemoryBudget}!void {
                try l.appendRecord(
                    entry.key,
                    if (entry.count & full_present != 0) entry.sum else null,
                    entry.first & Group.first_mask,
                    entry.first & null_key != 0,
                    0,
                    FullPolicy.hash(entry),
                    entry.count & full_count_mask,
                );
            }

            fn flushCounts(l: *Local) !void {
                if (cached_count) try l.count_cache.flush(l, emitCount);
                if (cached_full) try l.full_cache.flush(l, emitFull);
            }

            pub fn start(l: *Local, ordinal: u32, item: usize) void {
                l.ordinal_base = ordinal;
                l.run_item = item;
                l.run_stopped = false;
                l.cache_seen = 0;
                l.cache_hits = 0;
                l.cache_disabled = false;
                l.count_cache.last = null;
                l.full_cache.last = null;
            }

            fn appendRun(l: *Local, key: ?Key, value: ?i64, ordinal: u32) void {
                if (l.run_stopped) return;
                const s = l.state;
                const run = &s.runs[l.run_item];
                if (value == null) l.nulls |= 2;
                run.rows += 1;
                const k = key orelse {
                    l.run_stopped = true;
                    s.run_declined.store(true, .monotonic);
                    return;
                };
                if (run.groups != 0) {
                    const prev = &s.groups[run.last];
                    const pk = prev.keyValue().?;
                    if (k < pk) {
                        l.run_stopped = true;
                        s.run_declined.store(true, .monotonic);
                        return;
                    }
                    if (k == pk) {
                        prev.count += 1;
                        if (value) |v| {
                            prev.sum += v;
                            prev.count |= Group.sum_present;
                        }
                        return;
                    }
                }
                // Keep fragments contiguous inside this source's disjoint row reservation. The winner map carries
                // the source ordinal separately, avoiding physical pages for the gaps between repeated keys.
                const id = l.ordinal_base + @as(u32, @intCast(run.groups));
                s.groups[id] = .{
                    .key = @bitCast(k),
                    .count = 1 | (if (value != null) Group.sum_present else @as(u32, 0)),
                    .sum = value orelse 0,
                    .first = ordinal,
                };
                s.winners[ordinal] = id + 1;
                if (run.groups == 0) run.first = id;
                run.last = id;
                run.groups += 1;
            }

            pub fn append(l: *Local, key: ?Key, value: if (has_value) ?i64 else void, ordinal: u32) !void {
                std.debug.assert(admitsRows(codec, ordinal));
                if (comptime codec == .int64) if (l.state.runs.len != 0) {
                    l.appendRun(key, if (has_value) value else null, ordinal);
                    return;
                };
                // Integer keys: their bits, NULL told apart by its flag (full) or a bit no integer sets (top-H).
                const bits: KeyBits = if (codec == .int64)
                    (if (key) |k| @bitCast(k) else 0)
                else
                    inlineKey(key, tagged) catch |e| {
                        l.state.width_refused.store(true, .monotonic);
                        return e;
                    };
                const len: u32 = if (codec == .int64) 0 else if (key) |k| @intCast(k.len) else 0;
                if (cached_count) if (!l.cache_disabled and l.count_cache.entries.len != 0) {
                    const probe = l.cache_seen < l.state.cache_trial_rows;
                    if (probe) l.cache_seen += 1;
                    const hit = try l.count_cache.push(.{
                        .key = bits,
                        .first = ordinal | @as(u32, if (key == null) null_key else 0),
                        .count = 1,
                    }, l, emitCount);
                    if (probe and hit) l.cache_hits += 1;
                    // Flush exactly before bypassing a nonreducing source; start() retries on the next source.
                    if (probe and l.cache_seen == l.state.cache_trial_rows and
                        l.cache_hits < l.count_cache.entries.len)
                    {
                        try l.flushCounts();
                        l.cache_disabled = true;
                    }
                    return;
                };
                if (cached_full) if (!l.cache_disabled and l.full_cache.entries.len != 0) {
                    // Column nullability is recorded per row: a partial's value-NULL bit means only all-NULL.
                    l.nulls |= @intFromBool(key == null) | @as(u32, @intFromBool(value == null)) << 1;
                    const probe = l.cache_seen < l.state.cache_trial_rows;
                    if (probe) l.cache_seen += 1;
                    const hit = try l.full_cache.push(.{
                        .key = bits,
                        .sum = value orelse 0,
                        .first = ordinal | @as(u32, if (key == null) null_key else 0),
                        .count = 1 | @as(u32, if (value != null) full_present else 0),
                    }, l, emitFull);
                    if (probe and hit) l.cache_hits += 1;
                    if (probe and l.cache_seen == l.state.cache_trial_rows and
                        l.cache_hits < l.full_cache.entries.len)
                    {
                        try l.flushCounts();
                        l.cache_disabled = true;
                    }
                    return;
                };
                const h: u64 = if (key == null) 0x621985775bcbd930 else mixKey(bits, len);
                try l.appendRecord(bits, value, ordinal, key == null, len, h, 1);
            }

            // All records, including exact cache evictions, use the same quoted block writer. Keeping this acyclic
            // lets the ordinary producer inline; recursively calling append makes uniform input pay per-row calls.
            inline fn appendRecord(
                l: *Local,
                bits: KeyBits,
                value: if (has_value) ?i64 else void,
                ordinal: u32,
                kn: bool,
                len: u32,
                h: u64,
                weight: u32,
            ) error{ExceededMemoryBudget}!void {
                var flags: u32 = (@as(u32, @truncate(h)) & 0x3fffffff) << 2 | @intFromBool(kn);
                if (has_value) flags |= @as(u32, @intFromBool(value == null)) << 1;
                if (kind == .full) l.nulls |= flags & 3;
                const weighted = (cached_count or cached_full) and weight > 1;
                if (weighted) flags = if (has_value) weight << 2 | (flags & 3) else weight << 1 | @intFromBool(kn);
                const r: Record = if (has_value)
                    .{
                        .key = bits,
                        .value = value orelse 0,
                        .ordinal = ordinal | len << ordinal_bits | @as(u32, if (weighted) partial else 0),
                        .flags = flags,
                    }
                else
                    .{
                        .key = bits,
                        .ordinal = ordinal | len << ordinal_bits | @as(u32, if (weighted) partial else 0),
                        .flags = flags,
                    };
                const p: usize = @intCast(h >> l.state.shift);
                try Records.append(&l.records, l.state.blocks, &l.state.block_cursor, p, r);
                if (cached_count or cached_full) l.contributed_rows += weight;
            }

            pub fn batch(l: *Local, view: eval.Batch, lookup: []const ?usize, offset: usize) !void {
                return l.batchSelected(view, lookup, offset, null);
            }

            pub fn batchSelected(
                l: *Local,
                view: eval.Batch,
                lookup: []const ?usize,
                offset: usize,
                selected: ?*const Selection,
            ) !void {
                if (comptime codec == .int64)
                    if (l.state.runs.len != 0 and l.state.run_declined.load(.monotonic)) return;
                const k = if (codec == .bytes) view.cols[lookup[l.state.key_col].?].string else view.cols[lookup[l.state.key_col].?].i64;
                const v = if (has_value) view.cols[lookup[l.state.value_col].?].i64 else {};
                if (selected) |sel| {
                    var it = sel.iterator();
                    while (it.next()) |row| try l.batchRow(k, v, offset, row);
                } else for (0..view.num_rows) |i| try l.batchRow(k, v, offset, i);
            }

            inline fn batchRow(l: *Local, k: anytype, v: anytype, offset: usize, i: usize) !void {
                const kn = if (k.def_levels) |d| d[i] < k.max_def else false;
                const key = if (kn) null else if (k.codes) |codes| k.dict[codes[i]] else k.values[i];
                const ordinal = l.ordinal_base + @as(u32, @intCast(offset + i));
                if (has_value) {
                    const vn = if (v.def_levels) |d| d[i] < v.max_def else false;
                    try l.append(
                        key,
                        if (vn) null else if (v.codes) |codes| v.dict[codes[i]] else v.values[i],
                        ordinal,
                    );
                } else try l.append(key, {}, ordinal);
            }
        };

        fn sameRecord(r: Record, g: Group) bool {
            const kn = r.flags & 1 != 0;
            return if (codec == .int64)
                (g.first & null_key != 0) == kn and (kn or g.key == r.key)
            else if (!tagged)
                (g.count & null_key != 0) == kn and
                    (kn or (g.first >> ordinal_bits == r.ordinal >> ordinal_bits and g.key == r.key))
            else
                g.key == r.key;
        }

        inline fn foldRecord(r: Record, groups: []Group, slots: []u32, base: usize, n: *usize) void {
            _ = foldAdjacent(r, groups, slots, base, n, null);
        }

        /// Equality uses the complete inline key and flags; adjacent runs skip the hash probe without changing the
        /// aggregate's row order. NULL breaks the shortcut, and the ordinary probe remains authoritative.
        inline fn foldAdjacent(
            r: Record,
            groups: []Group,
            slots: []u32,
            base: usize,
            n: *usize,
            previous: ?usize,
        ) usize {
            const kn = r.flags & 1 != 0;
            const weighted = (cached_count or cached_full) and r.ordinal & partial != 0;
            const weight = if (weighted) r.flags >> (if (has_value) 2 else 1) else 1;
            const ordinal = if (cached_count or cached_full) r.ordinal & Group.first_mask else r.ordinal;
            const id = blk: {
                if (!kn) if (previous) |id| if (sameRecord(r, groups[id])) break :blk id;
                const cap = slots.len;
                const hash = if (weighted)
                    @as(u32, @truncate(if (kn) @as(u64, 0x621985775bcbd930) else mixKey(r.key, 0))) & 0x3fffffff
                else
                    r.flags >> 2;
                var pos: usize = hash & (cap - 1);
                while (slots[pos] != 0) : (pos = (pos + 1) & (cap - 1)) {
                    if (sameRecord(r, groups[slots[pos] - 1 - base])) break;
                }
                if (slots[pos] == 0) {
                    slots[pos] = @intCast(base + n.* + 1);
                    groups[n.*] = .{
                        .key = r.key,
                        .count = if (codec == .bytes and kn) null_key else 0,
                        .first = ordinal | (if (codec == .int64 and kn) null_key else @as(u32, 0)),
                    };
                    n.* += 1;
                }
                break :blk slots[pos] - 1 - base;
            };
            const g = &groups[id];
            g.count += weight;
            const mask = Group.first_mask;
            g.first = (g.first & ~mask) | @min(g.ordinal(), r.ordinal & mask);
            if (has_value and r.flags & 2 == 0) {
                g.sum += r.value;
                g.count |= Group.sum_present;
            }
            return id;
        }

        pub const State = struct {
            gpa: std.mem.Allocator,
            ledger: ?*memory.SharedBudget,
            charged: usize,
            /// Text preserves exact i128 SUMs; Parquet retains its checked physical INT64 lane.
            wide_sums: bool = false,
            output_credit: usize = 0,
            rows: usize,
            key_col: usize,
            /// Unused by COUNT top-H, which reads only the key.
            value_col: usize,
            parts: usize,
            shift: u6,
            cut: if (kind == .full) void else Cut,
            blocks: []Block = &.{},
            block_cursor: std.atomic.Value(u32) = .init(0),
            width_refused: std.atomic.Value(bool) = .init(false),
            /// Exact pair scores reuse the integer SUM top-H record and score layout.
            distinct_top: bool = false,
            locals: []Local = &.{},
            group_count: usize = 0,
            input_nulls: u32 = 0,
            fold_ns: u64 = 0,
            release_ns: u64 = 0,
            // The full answer: every group, contiguous per partition, and their first-appearance order.
            groups: []Group = &.{},
            winners: []u32 = &.{},
            /// The full answer's winner map came from page_allocator (zero-filled), not `gpa`.
            winners_fresh: bool = false,
            bitmap: []u64 = &.{},
            prefix: []u32 = &.{},
            starts: []usize = &.{},
            counts: []usize = &.{},
            /// The full answer's fold claims partitions largest first, so the hottest owner starts at time zero.
            fold_order: []u32 = &.{},
            partition_ns: u64 = 0,
            order_ns: u64 = 0,
            // Top-H: each worker's best groups among the partitions it completed.
            heaps: []Heap = &.{},
            select_ns: u64 = 0,
            // Decoded run partials occupy disjoint source-ordinal ranges of the existing group/winner arrays.
            runs: []Run = &.{},
            run_declined: std.atomic.Value(bool) = .init(false),
            count_caches: []CountEntry = &.{},
            full_caches: []FullEntry = &.{},
            cache_trial_rows: u32 = 0,
            const Run = struct { rows: usize = 0, groups: usize = 0, first: u32 = 0, last: u32 = 0 };

            fn blockCount(rows: usize, workers: usize, parts: usize) usize {
                return Records.blockCount(rows, workers, parts);
            }
            /// What the attempt draws before it consumes anything. The full answer holds a 40-byte group and a
            /// winner slot per input row besides its blocks; top-H holds cache-sized scratch, heaps and stacks.
            pub fn reservation(rows: usize, workers: usize, parts: usize, k: usize, output_credit: usize) usize {
                return reservationMode(rows, workers, parts, k, output_credit, false);
            }

            pub fn runReservation(rows: usize, workers: usize, parts: usize, k: usize, output_credit: usize) usize {
                return reservationMode(rows, workers, parts, k, output_credit, true);
            }

            fn reservationMode(
                rows: usize,
                workers: usize,
                parts: usize,
                k: usize,
                output_credit: usize,
                runs: bool,
            ) usize {
                const shared = blockCount(rows, workers, parts) *| @sizeOf(Block) +|
                    Records.frontierBytes(workers, parts) +| @sizeOf(State) +| (8 << 20);
                // The full answers keep their established quotes, which cover blocks and ordering arrays.
                if (kind == .full) return (if (codec == .int64)
                    // One quote for both producers, so EXPLAIN matches either; runs leave the partials unallocated.
                    rows *| 88 +| (128 << 20) +| workers *| fullCacheEntries() *| @sizeOf(FullEntry)
                else
                    rows *| 96 +| (192 << 20)) +| output_credit;
                return shared +| rows *| table_row_bytes +|
                    workers *| (stack_size + @sizeOf(Local) + @sizeOf(Heap) + @sizeOf(Task)) +|
                    workers *| k *| @sizeOf(Group) +|
                    (if (runs) 0 else workers *| countCacheEntries() *| @sizeOf(CountEntry)) +| output_credit;
            }
            pub fn init(
                a: std.mem.Allocator,
                ledger: ?*memory.SharedBudget,
                rows: usize,
                workers: usize,
                key_col: usize,
                value_col: usize,
                cut: if (kind == .full) void else Cut,
                parts: usize,
                output_credit: usize,
            ) !?*State {
                return initMode(a, ledger, rows, workers, key_col, value_col, cut, parts, output_credit, 0);
            }

            /// Scalar COUNT DISTINCT finishes complete owner partitions with no retained top-H groups.
            pub fn initCardinality(
                a: std.mem.Allocator,
                ledger: ?*memory.SharedBudget,
                rows: usize,
                workers: usize,
                key_col: usize,
                parts: usize,
                output_credit: usize,
            ) !?*State {
                comptime std.debug.assert(kind == .count_top);
                return initMode(
                    a,
                    ledger,
                    rows,
                    workers,
                    key_col,
                    0,
                    .{ .k = 0, .descending = true },
                    parts,
                    output_credit,
                    0,
                );
            }

            /// Exact pair deduplication inside complete integer-key owners, ranked through the existing SUM lane.
            pub fn initDistinctTop(
                a: std.mem.Allocator,
                ledger: ?*memory.SharedBudget,
                rows: usize,
                workers: usize,
                key_col: usize,
                value_col: usize,
                cut: Cut,
                parts: usize,
                output_credit: usize,
            ) !?*State {
                comptime std.debug.assert(kind == .sum_top and codec == .int64);
                const state = (try init(
                    a,
                    ledger,
                    rows,
                    workers,
                    key_col,
                    value_col,
                    cut,
                    parts,
                    output_credit,
                )) orelse return null;
                state.distinct_top = true;
                return state;
            }

            /// The same full-answer state, filled with decoded run partials instead of hash records. Its ordinary
            /// reservation covers the groups, winners and ordering arrays, and is returned before a declined replay.
            pub fn initRuns(
                a: std.mem.Allocator,
                ledger: ?*memory.SharedBudget,
                rows: usize,
                workers: usize,
                key_col: usize,
                value_col: usize,
                cut: if (kind == .full) void else Cut,
                parts: usize,
                items: usize,
                output_credit: usize,
            ) !?*State {
                comptime std.debug.assert(codec == .int64);
                // The ordinary per-row quote also covers one run descriptor per row. Empty-source-heavy inputs
                // beyond that bound keep the record producer, rather than retaining unquoted descriptors.
                if (items > rows) return null;
                return initMode(a, ledger, rows, workers, key_col, value_col, cut, parts, output_credit, items);
            }

            fn initMode(
                a: std.mem.Allocator,
                ledger: ?*memory.SharedBudget,
                rows: usize,
                workers: usize,
                key_col: usize,
                value_col: usize,
                cut: if (kind == .full) void else Cut,
                parts: usize,
                output_credit: usize,
                run_items: usize,
            ) !?*State {
                if (!admitsRows(codec, rows) or workers == 0 or workers > max_workers or parts < 64 or parts > 16384 or
                    !std.math.isPowerOfTwo(parts)) return null;
                const k: usize = if (kind == .full) 0 else cut.k;
                if (kind != .full and (k > 1024 or (kind != .count_top and k == 0))) return null;
                const charge = reservationMode(rows, workers, parts, k, output_credit, run_items != 0);
                if (ledger) |l| _ = l.drawExact(charge) orelse return null;
                const s = a.create(State) catch |e| {
                    if (ledger) |l| l.release(charge);
                    return e;
                };
                s.* = .{
                    .gpa = a,
                    .ledger = ledger,
                    .charged = charge,
                    .output_credit = output_credit,
                    .rows = rows,
                    .key_col = key_col,
                    .value_col = value_col,
                    .cut = cut,
                    .parts = parts,
                    .shift = @intCast(@as(usize, 64) - std.math.log2_int(usize, parts)),
                };
                errdefer s.deinit();
                s.locals = try a.alloc(Local, workers);
                for (s.locals) |*l| l.* = .{ .state = s };
                if (kind != .full) {
                    s.heaps = try a.alloc(Heap, workers);
                    for (s.heaps) |*h| h.* = .initContext(cut.descending);
                    for (s.heaps) |*h| {
                        const storage = try a.alloc(Group, k);
                        h.items = storage[0..0];
                        h.cap = storage.len;
                    }
                }
                if (run_items != 0) {
                    s.runs = try a.alloc(Run, run_items);
                    @memset(s.runs, .{});
                    s.groups = try a.alloc(Group, rows);
                    s.winners = try a.alloc(u32, rows);
                    @memset(s.winners, 0);
                    return s;
                }
                const cache_entries = countCacheEntries();
                if (cache_entries != 0) {
                    s.cache_trial_rows = @intCast(@import("owner_cache.zig").trialRows(
                        cache_entries,
                        @sizeOf(CountEntry),
                    ));
                    s.count_caches = try a.alloc(CountEntry, workers * cache_entries);
                    @memset(s.count_caches, .{});
                    for (s.locals, 0..) |*l, i|
                        l.count_cache.entries = s.count_caches[i * cache_entries ..][0..cache_entries];
                }
                // The full answer's blocks are claimed densely front to back: advise them for huge pages.
                var dense: HugePageAdvisor = .{ .child = a };
                const full_entries = fullCacheEntries();
                if (full_entries != 0) {
                    s.cache_trial_rows = @intCast(@import("owner_cache.zig").trialRows(
                        full_entries,
                        @sizeOf(FullEntry),
                    ));
                    s.full_caches = try a.alloc(FullEntry, workers * full_entries);
                    @memset(s.full_caches, .{});
                    for (s.locals, 0..) |*l, i|
                        l.full_cache.entries = s.full_caches[i * full_entries ..][0..full_entries];
                }
                s.blocks = try (if (kind == .full) dense.allocator() else a).alloc(Block, blockCount(rows, workers, parts));
                for (s.locals) |*l| {
                    l.records = try Records.Frontier.init(a, parts);
                }
                return s;
            }
            pub fn deinit(s: *State) void {
                const a = s.gpa;
                for (s.locals) |l| {
                    l.records.deinit(a);
                }
                a.free(s.count_caches);
                a.free(s.full_caches);
                a.free(s.locals);
                a.free(s.runs);
                for (s.heaps) |*h| h.deinit(a);
                a.free(s.heaps);
                a.free(s.blocks);
                a.free(s.groups);
                if (s.winners_fresh) std.heap.page_allocator.free(s.winners) else a.free(s.winners);
                a.free(s.bitmap);
                a.free(s.prefix);
                a.free(s.starts);
                a.free(s.counts);
                a.free(s.fold_order);
                if (s.ledger) |l| l.release(s.charged);
                a.destroy(s);
            }

            const Task = struct {
                s: *State,
                cursor: *std.atomic.Value(usize),
                pass: enum { fold, order, run_top } = .fold,
                heap: ?*Heap = null,
                groups: []Group = &.{},
                slots: []u32 = &.{},
                records: []Record = &.{},
                count_fold: CountFold = .{},
                group_count: usize = 0,
                err: ?anyerror = null,

                fn run(t: *Task) void {
                    defer t.s.gpa.free(t.groups);
                    defer t.s.gpa.free(t.slots);
                    defer t.s.gpa.free(t.records);
                    defer t.count_fold.deinit(t.s.gpa);
                    const end = switch (t.pass) {
                        .order => (t.s.bitmap.len + 1023) / 1024,
                        .run_top => t.s.runs.len,
                        .fold => t.s.parts,
                    };
                    while (true) {
                        const claim = t.cursor.fetchAdd(1, .monotonic);
                        if (claim >= end) return;
                        const p = if (t.pass == .fold and t.s.fold_order.len != 0) t.s.fold_order[claim] else claim;
                        (switch (t.pass) {
                            .fold => if (kind == .full) t.foldFull(p) else if (kind == .sum_top and codec == .int64)
                                if (t.s.distinct_top) t.foldDistinct(p) else t.foldTop(p)
                            else
                                t.foldTop(p),
                            .order => t.s.orderBlock(p),
                            .run_top => if (kind != .full) t.runTop(p),
                        }) catch |e| {
                            t.err = e;
                            return;
                        };
                    }
                }

                fn runTop(t: *Task, item: usize) void {
                    const r = t.s.runs[item];
                    if (r.groups == 0) return;
                    for (r.first..@as(usize, r.last) + 1) |i| {
                        const g = t.s.groups[i];
                        if (t.s.winners[g.ordinal()] != 0) t.s.keep(t.heap.?, g);
                    }
                }

                fn ensureSlots(t: *Task, cap: usize) ![]u32 {
                    if (t.slots.len < cap) {
                        t.s.gpa.free(t.slots);
                        t.slots = &.{};
                        t.slots = try t.s.gpa.alloc(u32, cap);
                    }
                    @memset(t.slots[0..cap], 0);
                    return t.slots[0..cap];
                }

                /// Every group of partition `p`, in the state's contiguous group array.
                fn foldFull(t: *Task, p: usize) !void {
                    const s = t.s;
                    const lo = s.starts[p];
                    const hi = s.starts[p + 1];
                    if (lo == hi) return;
                    const slots = try t.ensureSlots(try std.math.ceilPowerOfTwo(usize, (hi - lo) * 2));
                    var n: usize = 0;
                    var previous: ?usize = null;
                    for (s.locals) |local| {
                        var id = local.records.heads[p];
                        while (id != none) {
                            const b = &s.blocks[id];
                            if (b.next != none) @prefetch(&s.blocks[b.next], .{ .rw = .read, .locality = 1 });
                            for (b.rows[0..b.len]) |r|
                                previous = foldAdjacent(r, s.groups[lo..hi], slots, lo, &n, previous);
                            id = b.next;
                        }
                    }
                    for (s.groups[lo..][0..n], lo..) |g, i| {
                        if (!s.wide_sums and g.sumValue() != null and std.math.cast(i64, g.sum) == null)
                            return error.AggIntTooWide;
                        s.winners[g.ordinal()] = @intCast(i + 1);
                    }
                    s.counts[p] = n;
                }

                const PairField = enum { value, value_null, key, key_null };
                fn pairField(comptime field: PairField, r: Record) u64 {
                    return switch (field) {
                        .value => @bitCast(r.value),
                        .value_null => @intFromBool(r.flags & 2 != 0),
                        .key => r.key,
                        .key_null => r.flags & 1,
                    };
                }

                fn sortPairField(comptime field: PairField, src: *[]Record, dst: *[]Record) void {
                    if (src.len < 2) return;
                    var varying: u64 = 0;
                    const first = pairField(field, src.*[0]);
                    for (src.*) |r| varying |= first ^ pairField(field, r);
                    if (varying == 0) return;
                    // The digit cap bounds private prefixes to 16 KiB, inside the existing 1 MiB owner stack.
                    const radix_bits = 11;
                    var counts: [1 << radix_bits]usize = undefined;
                    const lo: usize = @ctz(varying);
                    const span = 64 - @clz(varying) - lo;
                    const passes = std.math.divCeil(usize, span, radix_bits) catch unreachable;
                    const bits = std.math.divCeil(usize, span, passes) catch unreachable;
                    const buckets = @as(usize, 1) << @intCast(bits);
                    var shift = lo;
                    while (shift < lo + span) : (shift += bits) {
                        const sh: u6 = @intCast(shift);
                        if ((varying >> sh) & (buckets - 1) == 0) continue;
                        @memset(counts[0..buckets], 0);
                        for (src.*) |r| counts[(pairField(field, r) >> sh) & (buckets - 1)] += 1;
                        var at: usize = 0;
                        for (counts[0..buckets]) |*c| {
                            const n = c.*;
                            c.* = at;
                            at += n;
                        }
                        for (src.*) |r| {
                            const digit = (pairField(field, r) >> sh) & (buckets - 1);
                            dst.*[counts[digit]] = r;
                            counts[digit] += 1;
                        }
                        std.mem.swap([]Record, src, dst);
                    }
                }

                fn foldDistinct(t: *Task, p: usize) !void {
                    comptime std.debug.assert(kind == .sum_top and codec == .int64);
                    comptime std.debug.assert(2 * @sizeOf(Record) <= table_row_bytes);
                    const s = t.s;
                    var rows: usize = 0;
                    for (s.locals) |l| rows += l.records.histogram[p];
                    if (rows == 0) return;
                    if (t.records.len < rows * 2) {
                        s.gpa.free(t.records);
                        t.records = &.{};
                        t.records = try s.gpa.alloc(Record, rows * 2);
                    }
                    var src = t.records[0..rows];
                    var dst = t.records[rows..][0..rows];
                    var at: usize = 0;
                    for (s.locals) |l| {
                        var id = l.records.heads[p];
                        while (id != none) {
                            const b = &s.blocks[id];
                            std.mem.copyForwards(Record, src[at..][0..b.len], b.rows[0..b.len]);
                            at += b.len;
                            id = b.next;
                        }
                    }
                    std.debug.assert(at == rows);
                    // Stable least-significant-field passes retain full values, keys and separate NULL flags.
                    inline for (.{ PairField.value, PairField.value_null, PairField.key, PairField.key_null }) |field|
                        sortPairField(field, &src, &dst);
                    at = 0;
                    while (at < rows) {
                        const head = src[at];
                        var end = at + 1;
                        while (end < rows and head.key == src[end].key and
                            (head.flags & 1) == (src[end].flags & 1)) : (end += 1)
                        {}
                        var first = head.ordinal;
                        var last: ?u64 = null;
                        var score: u64 = 0;
                        for (src[at..end]) |r| {
                            first = @min(first, r.ordinal);
                            if (r.flags & 2 != 0) continue;
                            const x: u64 = @bitCast(r.value);
                            if (last == null or last.? != x) score += 1;
                            last = x;
                        }
                        // NULL arguments leave their key with a present zero score.
                        s.keep(t.heap.?, .{
                            .key = head.key,
                            .sum = score,
                            .count = Group.sum_present,
                            .first = first | (if (head.flags & 1 != 0) null_key else @as(u32, 0)),
                        });
                        t.group_count += 1;
                        at = end;
                    }
                }

                fn foldCount(t: *Task, p: usize) !void {
                    const s = t.s;
                    var rows: usize = 0;
                    for (s.locals) |l| rows += l.records.histogram[p];
                    if (rows == 0) return;
                    const initial = @max(1, cacheBudget() / (@sizeOf(CountLayout.Group) + 16));
                    try t.count_fold.reset(s.gpa, rows, initial);
                    for (s.locals) |l| {
                        var id = l.records.heads[p];
                        while (id != none) {
                            const b = &s.blocks[id];
                            if (b.next != none) @prefetch(&s.blocks[b.next], .{ .rw = .read, .locality = 1 });
                            for (b.rows[0..b.len]) |r| {
                                const kn = r.flags & 1 != 0;
                                try t.count_fold.append(s.gpa, .{
                                    // A temporary 65-bit identity preserves all raw-i64 values plus NULL. Owned
                                    // producer records remain the P1 16B layout; no NULL/payload bit is discarded.
                                    .key = if (kn) @as(u128, 1) << 64 else r.key,
                                    .ordinal = r.ordinal & Group.first_mask,
                                    .weight = if (r.ordinal & partial != 0) r.flags >> 1 else 1,
                                });
                            }
                            id = b.next;
                        }
                    }
                    var complete = t.count_fold.used;
                    for (t.count_fold.items()) |g| {
                        const kn = g.key >> 64 != 0;
                        const legacy: Group = .{
                            .key = @truncate(g.key),
                            .count = g.weight,
                            .first = g.first | @as(u32, if (kn) null_key else 0),
                        };
                        if (s.cut.k != 0) s.keep(t.heap.?, legacy) else complete -= @intFromBool(kn);
                    }
                    t.group_count += complete;
                }

                /// Partition `p`'s complete groups, of which only this worker's best H are kept.
                fn foldTop(t: *Task, p: usize) !void {
                    if (cached_count) return t.foldCount(p);
                    const s = t.s;
                    var rows: usize = 0;
                    for (s.locals) |l| rows += l.records.histogram[p];
                    if (rows == 0) return;
                    if (t.groups.len < rows) {
                        s.gpa.free(t.groups);
                        t.groups = &.{};
                        t.groups = try s.gpa.alloc(Group, rows);
                    }
                    const slots = try t.ensureSlots(try std.math.ceilPowerOfTwo(usize, rows * 2));
                    var n: usize = 0;
                    var previous: ?usize = null;
                    for (s.locals) |l| {
                        var id = l.records.heads[p];
                        while (id != none) {
                            const b = &s.blocks[id];
                            if (b.next != none) @prefetch(&s.blocks[b.next], .{ .rw = .read, .locality = 1 });
                            for (b.rows[0..b.len]) |r| {
                                previous = foldAdjacent(r, t.groups, slots, 0, &n, previous);
                            }
                            id = b.next;
                        }
                    }
                    // Every key is complete in this owner partition before it can be discarded. A worker's top-H
                    // across its completed partitions subsumes every partition's top-H, so their union is sufficient.
                    var complete = n;
                    for (t.groups[0..n]) |g| {
                        if (s.cut.k != 0) s.keep(t.heap.?, g) else {
                            const is_null = if (codec == .int64) g.first & null_key != 0 else g.key >> 120 == 255;
                            complete -= @intFromBool(is_null);
                        }
                    }
                    t.group_count += complete;
                }
            };

            fn keep(s: *State, heap: *Heap, g: Group) void {
                if (heap.count() == s.cut.k) {
                    if (worse(s.cut.descending, g, heap.peek().?) != .gt) return;
                    _ = heap.pop();
                }
                heap.push(s.gpa, g) catch unreachable;
            }

            fn parallel(s: *State, pass: @FieldType(Task, "pass")) !void {
                const tasks = try s.gpa.alloc(Task, s.locals.len);
                defer s.gpa.free(tasks);
                var cursor: std.atomic.Value(usize) = .init(0);
                for (tasks, 0..) |*t, i| t.* = .{ .s = s, .cursor = &cursor, .pass = pass, .heap = if (kind == .full) null else &s.heaps[i] };
                if (tasks.len == 1 or !spawn.runTree(Task, tasks, .{ .stack_size = stack_size }, Task.run))
                    Task.run(&tasks[0]);
                for (tasks) |t| {
                    if (t.err) |e| return e;
                    s.group_count += t.group_count;
                }
            }

            fn orderBlock(s: *State, block: usize) !void {
                for (block * 1024..@min(s.bitmap.len, (block + 1) * 1024)) |wi| {
                    var bits: u64 = 0;
                    for (wi * 64..@min(s.rows, wi * 64 + 64)) |i| if (s.winners[i] != 0) {
                        bits |= @as(u64, 1) << @intCast(i % 64);
                    };
                    s.bitmap[wi] = bits;
                    s.prefix[wi] = @popCount(bits);
                }
            }

            fn finishRuns(s: *State, selected_rows: usize) !void {
                if (s.run_declined.load(.monotonic)) return error.RunOrderDeclined;
                var rows: usize = 0;
                for (s.locals) |local| s.input_nulls |= local.nulls;
                var last: ?u32 = null;
                for (s.runs) |run| {
                    rows += run.rows;
                    s.group_count += run.groups;
                    if (run.groups == 0) continue;
                    if (last) |prev| {
                        const left = &s.groups[prev];
                        const right = &s.groups[run.first];
                        if (left.keyValue().? > right.keyValue().?) return error.RunOrderDeclined;
                        if (left.key == right.key) {
                            left.sum += right.sum;
                            left.count = (left.count & Group.sum_present) | (right.count & Group.sum_present) |
                                @as(u32, @intCast(left.countValue() + right.countValue()));
                            s.winners[right.ordinal()] = 0;
                            s.group_count -= 1;
                            if (run.groups == 1) continue; // Keep the original owner across any number of seams.
                        }
                    }
                    last = run.last;
                }
                if (rows != selected_rows) return error.BadRowCount;
                if (kind != .full) {
                    try s.parallel(.run_top);
                    s.combineTop();
                    return;
                }
                for (s.winners) |winner| if (winner != 0) {
                    const g = s.groups[winner - 1];
                    if (!s.wide_sums and g.sumValue() != null and std.math.cast(i64, g.sum) == null)
                        return error.AggIntTooWide;
                };
                s.bitmap = try s.gpa.alloc(u64, (s.rows + 63) / 64);
                s.prefix = try s.gpa.alloc(u32, s.bitmap.len + 1);
                try s.parallel(.order);
                var total: u32 = 0;
                for (s.prefix[0..s.bitmap.len]) |*n| {
                    const count = n.*;
                    n.* = total;
                    total += count;
                }
                s.prefix[s.bitmap.len] = total;
                if (total != s.group_count) return error.BadRowCount;
            }

            pub fn finish(s: *State) !void {
                return s.finishSelected(s.rows);
            }

            /// Capacity and original ordinals still use the raw row bound; only emitted rows must match survivors.
            pub fn finishSelected(s: *State, selected_rows: usize) !void {
                if (selected_rows > s.rows) return error.BadRowCount;
                if (comptime codec == .int64) if (s.runs.len != 0) {
                    const begin = now();
                    try s.finishRuns(selected_rows);
                    s.fold_ns = @intCast(now() - begin);
                    return;
                };
                for (s.locals) |*l| try l.flushCounts();
                var start = now();
                var contributed: usize = 0;
                for (s.locals) |l| {
                    s.input_nulls |= l.nulls;
                    if (cached_count or cached_full) {
                        contributed += l.contributed_rows;
                    } else {
                        for (l.records.histogram) |n| contributed += n;
                    }
                }
                if (contributed != selected_rows) return error.BadRowCount;
                if (kind == .full) {
                    s.starts = try s.gpa.alloc(usize, s.parts + 1);
                    s.counts = try s.gpa.alloc(usize, s.parts);
                    @memset(s.counts, 0);
                    var at: usize = 0;
                    for (0..s.parts) |p| {
                        s.starts[p] = at;
                        for (s.locals) |l| at += l.records.histogram[p];
                    }
                    s.starts[s.parts] = at;
                    s.fold_order = try s.gpa.alloc(u32, s.parts);
                    for (s.fold_order, 0..) |*o, p| o.* = @intCast(p);
                    std.mem.sort(u32, s.fold_order, s.starts, struct {
                        fn larger(starts: []const usize, x: u32, y: u32) bool {
                            const nx = starts[x + 1] - starts[x];
                            const ny = starts[y + 1] - starts[y];
                            return if (nx != ny) nx > ny else x < y;
                        }
                    }.larger);
                    s.groups = try s.gpa.alloc(Group, s.rows);
                    // The order pass reads every winner slot: advise the map for huge pages. It is mapped afresh
                    // (page_allocator's anonymous mmap or committed Windows pages read as zero), so no serial clear.
                    // rawAlloc, unlike alloc, does not fill safe builds' new memory with `undefined`.
                    if (s.rows != 0) {
                        var dense: HugePageAdvisor = .{ .child = std.heap.page_allocator };
                        const raw = dense.allocator().rawAlloc(s.rows * @sizeOf(u32), .of(u32), @returnAddress()) orelse
                            return error.OutOfMemory;
                        s.winners = @as([*]u32, @ptrCast(@alignCast(raw)))[0..s.rows];
                        s.winners_fresh = true;
                    }
                    s.partition_ns = @intCast(now() - start);
                    start = now();
                }
                try s.parallel(.fold);
                if (kind == .full) for (s.counts) |n| {
                    s.group_count += n;
                };
                s.fold_ns = @intCast(now() - start);
                start = now();
                s.gpa.free(s.blocks);
                s.blocks = &.{};
                s.release_ns = @intCast(now() - start);
                start = now();
                if (kind == .full) {
                    s.bitmap = try s.gpa.alloc(u64, (s.rows + 63) / 64);
                    s.prefix = try s.gpa.alloc(u32, s.bitmap.len + 1);
                    try s.parallel(.order);
                    var total: u32 = 0;
                    for (s.prefix[0..s.bitmap.len]) |*n| {
                        const c = n.*;
                        n.* = total;
                        total += c;
                    }
                    s.prefix[s.bitmap.len] = total;
                    if (total != s.group_count) return error.BadRowCount;
                    s.order_ns = @intCast(now() - start);
                } else {
                    s.combineTop();
                    s.select_ns = @intCast(now() - start);
                }
            }

            fn combineTop(s: *State) void {
                for (s.heaps[1..]) |other| for (other.items) |g| s.keep(&s.heaps[0], g);
                // The stable result sorter receives the surviving H rows in canonical first-appearance order.
                std.mem.sort(Group, s.heaps[0].items, {}, struct {
                    fn less(_: void, x: Group, y: Group) bool {
                        return x.ordinal() < y.ordinal();
                    }
                }.less);
            }

            /// Top-H: the surviving groups in first-appearance order.
            pub fn top(s: *const State) []const Group {
                return s.heaps[0].items;
            }

            /// Traverses the same ordinal bitmap for text and bounded Parquet windows.
            pub const Cursor = struct {
                state: *const State,
                word: usize,
                bits: u64 = 0,

                pub fn next(it: *Cursor) ?u32 {
                    while (it.word < it.state.bitmap.len) {
                        if (it.bits != 0) {
                            const bit = @ctz(it.bits);
                            it.bits &= it.bits - 1;
                            return it.state.winners[it.word * 64 + bit] - 1;
                        }
                        it.word += 1;
                        if (it.word < it.state.bitmap.len) it.bits = it.state.bitmap[it.word];
                    }
                    return null;
                }
            };

            pub fn orderedCursor(s: *const State, lo: usize) Cursor {
                std.debug.assert(lo <= s.group_count);
                if (lo == s.group_count) return .{ .state = s, .word = s.bitmap.len };
                var left: usize = 0;
                var right = s.bitmap.len;
                while (left < right) {
                    const mid = left + (right - left) / 2;
                    if (s.prefix[mid + 1] <= lo) left = mid + 1 else right = mid;
                }
                var bits = s.bitmap[left];
                for (0..lo - s.prefix[left]) |_| bits &= bits - 1;
                return .{ .state = s, .word = left, .bits = bits };
            }

            /// The full answer's groups `lo..hi` in first-appearance order, as indexes into `groups`.
            pub fn window(s: *const State, a: std.mem.Allocator, lo: usize, hi: usize) ![]u32 {
                std.debug.assert(lo <= hi and hi <= s.group_count);
                const ids = try a.alloc(u32, hi - lo);
                var it = s.orderedCursor(lo);
                for (ids) |*id| id.* = it.next().?;
                return ids;
            }
        };
    };
}

fn fullTrial(a: std.mem.Allocator, workers: usize) !void {
    const keys = [_]?[]const u8{ "", null, "a", "a\x00", "abcdefghijklmnop", "γ", "z" };
    const rows = 3001;
    var budget: memory.SharedBudget = .{ .limit = 1 << 30 };
    defer std.debug.assert(budget.used.load(.monotonic) == 0);
    const s = (try Full.State.init(a, &budget, rows, workers, 0, 1, {}, full_parts, 0)).?;
    defer s.deinit();
    var i: usize = rows;
    while (i != 0) {
        i -= 1;
        const key = keys[i % keys.len];
        const value: ?i64 = if (i % keys.len == 2) null else @as(i64, @intCast(i)) - 1500;
        try s.locals[i % workers].append(key, value, @intCast(i));
    }
    try s.finish();
    try std.testing.expectEqual(keys.len, s.group_count);
    const ids = try s.window(a, 0, s.group_count);
    defer a.free(ids);
    for (keys, ids, 0..) |key, id, k| {
        const g = &s.groups[id];
        try std.testing.expectEqual(@as(u32, @intCast(k)), g.ordinal());
        if (key) |bytes|
            try std.testing.expectEqualStrings(bytes, g.keyBytes().?)
        else
            try std.testing.expect(g.keyBytes() == null);
        var sum: i128 = 0;
        var count: i64 = 0;
        var row = k;
        while (row < rows) : (row += keys.len) {
            sum += @as(i128, @intCast(row)) - 1500;
            count += 1;
        }
        try std.testing.expectEqual(count, g.countValue());
        try std.testing.expectEqual(if (k == 2) null else @as(?i128, sum), g.sumValue());
    }
    const part = try s.window(a, 1, 6);
    defer a.free(part);
    try std.testing.expectEqualSlices(u32, ids[1..6], part);
}

test "inline byte groups preserve byte identity, NULLs, exact sums and first order" {
    for ([_]usize{ 1, 2, 4, 8, 16 }) |j| for ([_]?usize{ null, 0, 2 }) |fail_after| {
        if (fail_after) |n| spawn.injectFailureAfter(n);
        defer spawn.resetFailure();
        try fullTrial(std.testing.allocator, j);
    };
}

test "inline byte groups unwind every partial allocation and ledger charge" {
    try std.testing.checkAllAllocationFailures(std.testing.allocator, fullTrial, .{1});
}

test "inline byte groups decline long keys and reject wide sums only after decode" {
    {
        const s = (try Full.State.init(std.testing.allocator, null, 2, 1, 0, 1, {}, full_parts, 0)).?;
        defer s.deinit();
        try std.testing.expectError(error.ExceededMemoryBudget, s.locals[0].append("abcdefghijklmnopq", 1, 0));
        try std.testing.expect(s.width_refused.load(.monotonic));
    }
    for ([_]i64{ std.math.minInt(i64), std.math.maxInt(i64) }) |v| {
        const s = (try Full.State.init(std.testing.allocator, null, 2, 2, 0, 1, {}, full_parts, 0)).?;
        defer s.deinit();
        for (0..2) |i| try s.locals[i].append("same", v, @intCast(i));
        try std.testing.expectError(error.AggIntTooWide, s.finish());
    }
}

test "inline byte cached hashes cover wide partition tables" {
    for ([_]usize{ 131072, 131073 }) |rows| {
        const s = (try Full.State.init(std.testing.allocator, null, rows, 4, 0, 1, {}, full_parts, 0)).?;
        defer s.deinit();
        for (0..rows) |i| try s.locals[i % 4].append("abcdefghijklmnop", 3, @intCast(i));
        try s.finish();
        try std.testing.expectEqual(@as(usize, 1), s.group_count);
        const ids = try s.window(std.testing.allocator, 0, 1);
        defer std.testing.allocator.free(ids);
        const g = &s.groups[ids[0]];
        try std.testing.expectEqualStrings("abcdefghijklmnop", g.keyBytes().?);
        try std.testing.expectEqual(@as(i64, @intCast(rows)), g.countValue());
        try std.testing.expectEqual(@as(?i128, @intCast(rows * 3)), g.sumValue());
    }
}

fn topTrial(comptime kind: Kind, a: std.mem.Allocator, workers: usize, descending: bool, parts: usize, collisions: bool) !void {
    const rows = 2003;
    const Ref = struct { key: ?[]const u8, sum: ?i128 = null, first: u32 };
    var keys: [71][15]u8 = undefined;
    var refs: [71]Ref = undefined;
    var count: usize = 0;
    for (&keys, 0..) |*key, g| for (key, 0..) |*b, i| {
        b.* = @truncate(g * 31 + i);
    };
    var budget: memory.SharedBudget = .{ .limit = 256 << 20 };
    defer std.debug.assert(budget.used.load(.monotonic) == 0);
    const T = Owner(kind, .bytes);
    const s = (try T.State.init(a, &budget, rows, workers, 0, 1, .{ .k = 8, .descending = descending }, parts, 0)) orelse
        return error.OutOfMemory;
    defer s.deinit();
    for (0..rows) |i| {
        const g = (i * 37) % keys.len;
        const key: ?[]const u8 = if (g == 0) null else keys[g][0 .. g % 16];
        const v: ?i64 = if (g % 11 == 0) null else if (i % 2 == 0) std.math.maxInt(i64) else -1234567;
        var at: usize = 0;
        while (at < count) : (at += 1) {
            if ((key == null and refs[at].key == null) or (key != null and refs[at].key != null and
                std.mem.eql(u8, key.?, refs[at].key.?))) break;
        }
        if (at == count) {
            refs[count] = .{ .key = key, .first = @intCast(i) };
            count += 1;
        }
        if (kind == .count_top) {
            refs[at].sum = (refs[at].sum orelse 0) + 1;
        } else if (v) |value| refs[at].sum = (refs[at].sum orelse 0) + value;
        try s.locals[(rows - i) % workers].append(key, if (kind == .sum_top) v else {}, @intCast(i));
    }
    if (collisions) for (s.locals) |l| for (l.records.heads) |head| {
        var id = head;
        while (id != none) {
            const b = &s.blocks[id];
            for (b.rows[0..b.len]) |*r| r.flags &= 3;
            id = b.next;
        }
    };
    try s.finish();
    try std.testing.expectEqual(count, s.group_count);
    std.mem.sort(Ref, refs[0..count], descending, struct {
        fn before(desc: bool, x: Ref, y: Ref) bool {
            if (x.sum == null and y.sum != null) return false;
            if (x.sum != null and y.sum == null) return true;
            if (x.sum != null and x.sum.? != y.sum.?) return if (desc) x.sum.? > y.sum.? else x.sum.? < y.sum.?;
            return x.first < y.first;
        }
    }.before);
    const want = refs[0..@min(count, 8)];
    std.mem.sort(Ref, want, {}, struct {
        fn before(_: void, x: Ref, y: Ref) bool {
            return x.first < y.first;
        }
    }.before);
    try std.testing.expectEqual(want.len, s.top().len);
    for (s.top(), want) |*actual, expected| {
        const key = actual.keyBytes();
        if (expected.key) |value| {
            try std.testing.expectEqualSlices(u8, value, key.?);
        } else try std.testing.expect(key == null);
        try std.testing.expectEqual(expected.sum, T.rank(actual.*));
        try std.testing.expectEqual(expected.first, actual.ordinal());
    }
}

test "owned byte SUM and COUNT cuts preserve full identities, wide sums and first ties across workers and partitions" {
    inline for (.{ Kind.sum_top, Kind.count_top }) |kind| {
        for ([_]usize{ 1, 2, 4, 8, 16 }) |workers| for ([_]bool{ false, true }) |descending|
            for ([_]usize{ 64, 256, 2048 }) |parts| try topTrial(kind, std.testing.allocator, workers, descending, parts, false);
        for ([_]usize{ 1, 16 }) |workers| try topTrial(kind, std.testing.allocator, workers, true, 64, true);
        for ([_]usize{ 0, 2 }) |after| {
            spawn.injectFailureAfter(after);
            defer spawn.resetFailure();
            try topTrial(kind, std.testing.allocator, 16, true, 256, false);
        }
    }
}
test "owned byte SUM and COUNT cuts release all partial allocations and declines an unfunded peak" {
    const Kinds = struct {
        fn sum(a: std.mem.Allocator) !void {
            try topTrial(.sum_top, a, 1, true, 64, false);
        }
        fn count(a: std.mem.Allocator) !void {
            try topTrial(.count_top, a, 1, false, 64, false);
        }
    };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Kinds.sum, .{});
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Kinds.count, .{});
    var budget: memory.SharedBudget = .{ .limit = 1 };
    const result = try SumTop.State.init(std.testing.allocator, &budget, 1, 1, 0, 1, .{ .k = 1, .descending = true }, 64, 0);
    try std.testing.expect(result == null);
    const counted = try CountTop.State.init(std.testing.allocator, &budget, 1, 1, 0, 0, .{ .k = 1, .descending = true }, 64, 0);
    try std.testing.expect(counted == null);
    try std.testing.expectEqual(@as(usize, 0), budget.used.load(.monotonic));
}
test "inline byte identity distinguishes NULL, empty and embedded zero bytes and declines seventeen bytes" {
    // Equal bits are told apart by the length kept beside them; NULL by its own flag (it packs as empty).
    try std.testing.expectEqual(try inlineKey("", false), try inlineKey("\x00", false));
    try std.testing.expectEqual(try inlineKey("a", false), try inlineKey("a\x00", false));
    try std.testing.expect(try inlineKey("a", false) != try inlineKey("b", false));
    for (0..17) |n| {
        var bytes: [16]u8 = @splat(0);
        for (bytes[0..n], 0..) |*b, i| b.* = @intCast(i + 1);
        const back = std.mem.toBytes(try inlineKey(bytes[0..n], false));
        try std.testing.expectEqualSlices(u8, bytes[0..n], back[0..n]);
    }
    try std.testing.expectError(error.ExceededMemoryBudget, inlineKey("0123456789abcdefg", false));
    // Tagged identities (top-H) are distinct by one comparison, NULL included, and stop at 15 bytes.
    try std.testing.expect(try inlineKey("", true) != try inlineKey("\x00", true));
    try std.testing.expect(try inlineKey("a", true) != try inlineKey("a\x00", true));
    try std.testing.expect(try inlineKey(null, true) != try inlineKey("", true));
    _ = try inlineKey("0123456789abcde", true);
    try std.testing.expectError(error.ExceededMemoryBudget, inlineKey("0123456789abcdef", true));
}
test "packed byte top-H cache policy uses shared hardware and supports every forced variant" {
    defer hw.test_cpu = null;
    hw.test_cpu = .{ .l2 = .{ .bytes = 1 << 20, .max_sharing = 2 } };
    const small = SumTop.partitionCount(30000000, 16, batchRows(null), null);
    const small_batch = batchRows(null);
    hw.test_cpu = .{ .l2 = .{ .bytes = 4 << 20, .max_sharing = 2 } };
    try std.testing.expect(SumTop.partitionCount(30000000, 16, batchRows(null), null) < small);
    try std.testing.expect(batchRows(null) > small_batch);
    try std.testing.expectEqual(@as(usize, 2048), SumTop.partitionCount(30000000, 16, 4096, 0));
    try std.testing.expectEqual(@as(usize, 0), batchRows(0));
    try std.testing.expectEqual(@as(usize, 64), SumTop.partitionCount(30000000, 16, 4096, 1));
    try std.testing.expectEqual(@as(usize, 16384), SumTop.partitionCount(30000000, 16, 4096, 999999));
    for ([_]usize{ 64, 1024, 8192, 16384 }) |parts| {
        try topTrial(.sum_top, std.testing.allocator, 1, true, parts, false);
        try topTrial(.count_top, std.testing.allocator, 1, true, parts, false);
    }
    // COUNT records and groups are narrower, so its uncapped table-sized fanout never exceeds SUM's.
    try std.testing.expect(CountTop.partitionCount(30000000, 12, 4096, null) <= SumTop.partitionCount(30000000, 12, 4096, null));

    hw.test_cpu = .{ .l2 = .{ .bytes = 1280 << 10, .max_sharing = 2 }, .l3 = .{ .bytes = 30 << 20 } };
    try std.testing.expectEqual(@as(usize, 4096), SumTop.partitionCount(30000000, 1, 4096, null));
    try std.testing.expectEqual(@as(usize, 2048), SumTop.partitionCount(30000000, 12, 4096, null));
    try std.testing.expectEqual(@as(usize, 4096), SumTop.partitionCount(30000000, 64, 4096, null));
    try std.testing.expectEqual(@as(usize, 4096), SumTop.partitionCount(30000000, 12, 0, null));
    try std.testing.expectEqual(@as(usize, 4096), SumTop.partitionCount(30000000, 12, 65536, null));
    hw.test_cpu.?.l3.bytes = 4 << 20;
    try std.testing.expectEqual(@as(usize, 4096), SumTop.partitionCount(30000000, 1, 4096, null));
    hw.test_cpu.?.l3.bytes = 0;
    try std.testing.expectEqual(@as(usize, 4096), SumTop.partitionCount(30000000, 12, 4096, null));
    hw.test_cpu = .{ .l2 = .{ .bytes = 1 << 20, .max_sharing = 2 }, .l3 = .{ .bytes = 32 << 20 } };
    try std.testing.expectEqual(@as(usize, 2048), SumTop.partitionCount(30000000, 16, batchRows(null), null));
}

fn intKey(key: i64, flags: u32) ?i64 {
    return if (flags & 1 != 0) null else key;
}
fn intValue(value: i64, flags: u32) ?i64 {
    return if (flags & 2 != 0) null else value;
}
fn widen(v: ?i64) ?i128 {
    return if (v) |x| x else null;
}

fn intTrial(a: std.mem.Allocator, workers: usize) !void {
    var budget: memory.SharedBudget = .{ .limit = 1 << 30 };
    defer std.debug.assert(budget.used.load(.monotonic) == 0);
    const s = (try IntFull.State.init(a, &budget, 1001, workers, 0, 1, {}, full_parts, 0)).?;
    defer s.deinit();
    for (0..1001) |i| try s.locals[i % workers].append(intKey(@intCast(i % 71), if (i % 71 == 0) 1 else if (i % 71 == 2) 2 else 0), intValue(@as(i64, @intCast(i)) - 500, if (i % 71 == 0) 1 else if (i % 71 == 2) 2 else 0), @intCast(i));
    try s.finish();
    try std.testing.expectEqual(@as(usize, 71), s.group_count);
    const ids = try s.window(a, 0, s.group_count);
    defer a.free(ids);
    for (ids, 0..) |id, i| {
        const g = s.groups[id];
        try std.testing.expectEqual(@as(u32, @intCast(i)), g.ordinal());
        try std.testing.expectEqual(if (i == 0) null else @as(?i128, @intCast(i)), widen(g.keyValue()));
        var sum: i128 = 0;
        var count: i128 = 0;
        var row = i;
        while (row < 1001) : (row += 71) {
            count += 1;
            sum += @as(i128, @intCast(row)) - 500;
        }
        try std.testing.expectEqual(count, @as(i128, g.countValue()));
        try std.testing.expectEqual(if (i == 2) null else @as(?i128, sum), g.sumValue());
    }
    const slice = try s.window(a, 7, 69);
    defer a.free(slice);
    try std.testing.expectEqualSlices(u32, ids[7..69], slice);
}

test "owned integer groups preserve exact sums, NULLs and first order" {
    for ([_]usize{ 1, 2, 4, 8, 16 }) |j| try intTrial(std.testing.allocator, j);
}
test "owned integer groups release every allocation and reservation on failure" {
    try std.testing.checkAllAllocationFailures(std.testing.allocator, intTrial, .{1});
}

test "owned integer groups reject a wide exact sum before any output window" {
    for ([_]usize{ 1, 2, 8 }) |j| for ([_]i64{ std.math.maxInt(i64), std.math.minInt(i64) }) |value| {
        const s = (try IntFull.State.init(std.testing.allocator, null, 2, j, 0, 1, {}, full_parts, 0)).?;
        defer s.deinit();
        for (0..2) |i| try s.locals[i % j].append(intKey(1, 0), intValue(value, 0), @intCast(i));
        try std.testing.expectError(error.AggIntTooWide, s.finish());
    };
}

test "owned integer local partials flush before an i64 overflow and keep exact cancelling sums and NULLs" {
    const a = std.testing.allocator;
    const max = std.math.maxInt(i64);
    const min = std.math.minInt(i64);
    // Key 1: max, max, min, min, -2 overflows each partial direction before cancelling to -2 + -2.
    const keys = [_]?i64{ 1, 1, null, 1, 1, null, 1, 2 };
    const values = [_]?i64{ max, max, null, min, min, null, -2, 5 };
    for ([_]usize{ 1, 2, 3 }) |j| {
        const s = (try IntFull.State.init(a, null, keys.len, j, 0, 1, {}, full_parts, 0)).?;
        defer s.deinit();
        try std.testing.expect(s.locals[0].full_cache.entries.len != 0);
        for (keys, values, 0..) |k, v, i| try s.locals[i % j].append(k, v, @intCast(i));
        try s.finish();
        try std.testing.expectEqual(@as(usize, 3), s.group_count);
        const ids = try s.window(a, 0, s.group_count);
        defer a.free(ids);
        const g = [_]IntFull.Group{ s.groups[ids[0]], s.groups[ids[1]], s.groups[ids[2]] };
        try std.testing.expectEqual(@as(?i64, 1), g[0].keyValue());
        try std.testing.expectEqual(@as(i64, 5), g[0].countValue());
        try std.testing.expectEqual(@as(?i128, -4), g[0].sumValue());
        try std.testing.expectEqual(@as(?i64, null), g[1].keyValue());
        try std.testing.expectEqual(@as(u32, 2), g[1].ordinal());
        try std.testing.expectEqual(@as(i64, 2), g[1].countValue());
        try std.testing.expectEqual(@as(?i128, null), g[1].sumValue());
        try std.testing.expectEqual(@as(u32, 7), g[2].ordinal());
        try std.testing.expectEqual(@as(?i128, 5), g[2].sumValue());
        try std.testing.expectEqual(@as(u32, 3), s.input_nulls);
    }
}

test "owned integer groups overlap packed records safely for every reduction and partition tail" {
    const a = std.testing.allocator;
    const Ref = struct { key: ?i64, count: i128 = 0, sum: ?i128 = null };
    for ([_]usize{ 1, 2, 3, 4, 7, 31, 64, 65, 127, 257 }) |n| {
        for ([_]usize{ 1, @max(1, n / 2), n }) |ndv| for ([_]usize{ 1, 4, 16 }) |j| {
            const state = (try IntFull.State.init(a, null, n, j, 0, 1, {}, full_parts, 0)).?;
            defer state.deinit();
            var expected: std.ArrayList(Ref) = .empty;
            defer expected.deinit(a);
            for (0..n) |i| {
                const key: ?i64 = if (i % 37 == 0) null else @as(i64, @intCast(i % ndv)) - @as(i64, @intCast(ndv / 2));
                const value: ?i64 = if (i % 13 == 0) null else @as(i64, @intCast((i * 730201) % 1000)) - 500;
                var id = expected.items.len;
                for (expected.items, 0..) |r, ri| if (r.key == key) {
                    id = ri;
                    break;
                };
                if (id == expected.items.len) try expected.append(a, .{ .key = key });
                const ref = &expected.items[id];
                ref.count += 1;
                if (value) |v| ref.sum = (ref.sum orelse 0) + v;
                try state.locals[(n - i) % j].append(intKey(key orelse 0, @as(u32, @intFromBool(key == null)) | (@as(u32, @intFromBool(value == null)) << 1)), intValue(value orelse 0, @as(u32, @intFromBool(key == null)) | (@as(u32, @intFromBool(value == null)) << 1)), @intCast(i));
            }
            try state.finish();
            try std.testing.expectEqual(expected.items.len, state.group_count);
            const ids = try state.window(a, 0, state.group_count);
            defer a.free(ids);
            for (expected.items, ids) |ref, id| {
                const group = state.groups[id];
                try std.testing.expectEqual(if (ref.key) |k| @as(?i128, k) else null, widen(group.keyValue()));
                try std.testing.expectEqual(ref.count, @as(i128, group.countValue()));
                try std.testing.expectEqual(ref.sum, group.sumValue());
            }
        };
    }
}

fn intTopTrial(a: std.mem.Allocator, workers: usize, descending: bool, k: usize) !void {
    const ndv = 1021;
    var budget: memory.SharedBudget = .{ .limit = 1 << 30 };
    defer std.debug.assert(budget.used.load(.monotonic) == 0);
    const state = (try IntSumTop.State.init(a, &budget, ndv * 3, workers, 0, 1, .{
        .k = k,
        .descending = descending,
    }, 64, 0)).?;
    defer state.deinit();
    // Every present SUM is four. Keys, arrival order and first-appearance order differ; duplicates span workers.
    // Group 5 has a NULL sum and group 11 a NULL key. A partial-partition cut would select incorrect intermediate
    // sums, and a key-order tie-break would produce a different answer from these hand-known first ordinals.
    for ([_]usize{ 2, 0, 1 }) |round| for (0..ndv) |i| try state.locals[(ndv - i + round) % workers].append(intKey(@intCast((i * 73) % ndv), if (i == 5) 2 else if (i == 11) 1 else 0), intValue(([_]i64{ 2, -1, 3 })[round], if (i == 5) 2 else if (i == 11) 1 else 0), @intCast(round * ndv + i));
    try state.finish();
    try std.testing.expectEqual(@as(usize, ndv), state.group_count);
    try std.testing.expectEqual(@as(usize, 0), state.groups.len);
    try std.testing.expectEqual(@as(usize, 0), state.winners.len);
    try std.testing.expectEqual(@as(usize, 0), state.bitmap.len);
    try std.testing.expectEqual(@min(k, ndv), state.top().len);
    var ordinal: usize = 0;
    for (state.top()) |g| {
        if (k < ndv and ordinal == 5) ordinal += 1;
        try std.testing.expectEqual(@as(u32, @intCast(ordinal)), g.ordinal());
        const key: ?i128 = if (ordinal == 11) null else @intCast((ordinal * 73) % ndv);
        try std.testing.expectEqual(key, widen(g.keyValue()));
        try std.testing.expectEqual(if (ordinal == 5) null else @as(?i128, 4), g.sumValue());
        ordinal += 1;
    }
}

test "owned integer SUM top-H completes keys before stable tie selection at every worker count" {
    for ([_]usize{ 1, 2, 4, 8, 16 }) |j| for ([_]bool{ false, true }) |descending|
        for ([_]usize{ 7, 31, 1024 }) |k| try intTopTrial(std.testing.allocator, j, descending, k);
}

test "owned integer SUM top-H frees every partial heap and scratch allocation" {
    try std.testing.checkAllAllocationFailures(std.testing.allocator, intTopTrial, .{ 1, true, 7 });
}

test "owned integer SUM top-H retains wide winning and losing sums without output narrowing" {
    for ([_]bool{ false, true }) |descending| {
        const s = (try IntSumTop.State.init(std.testing.allocator, null, 6, 2, 0, 1, .{
            .k = 1,
            .descending = descending,
        }, 64, 0)).?;
        defer s.deinit();
        for (0..6) |i| try s.locals[i % 2].append(intKey(@intCast(i / 2), 0), intValue(if (i < 2) std.math.maxInt(i64) else if (i < 4) std.math.minInt(i64) else 0, 0), @intCast(i));
        try s.finish();
        const g = s.top()[0];
        const value: i128 = if (descending) std.math.maxInt(i64) else std.math.minInt(i64);
        try std.testing.expectEqual(value * 2, g.sumValue().?);
    }
}

test "owner row limits preserve the integer 31-bit range and the byte length field" {
    const t = std.testing;
    try t.expectEqual(@as(usize, 1) << 31, rowLimit(.int64));
    try t.expectEqual(@as(usize, 1) << 27, rowLimit(.bytes));
    for ([_]usize{ (1 << 27) - 1, 1 << 27, 180_000_000, 1 << 30, (1 << 31) - 1 }) |rows| {
        try t.expect(admitsRows(.int64, rows));
    }
    try t.expect(!admitsRows(.int64, 1 << 31));
    try t.expect(!admitsRows(.int64, std.math.maxInt(usize)));
    try t.expect(admitsRows(.bytes, (1 << 27) - 1));
    try t.expect(!admitsRows(.bytes, 1 << 27));
    // The restored range must not widen the hot records or group scratch.
    try t.expectEqual(@as(usize, 24), @sizeOf(IntFull.Record));
    try t.expectEqual(@as(usize, 32), @sizeOf(IntFull.Group));
}

test "integer owner execution admits synthetic rows through the shared 31-bit boundary" {
    inline for (.{ Kind.full, Kind.sum_top }) |kind| {
        const O = Owner(kind, .int64);
        // An empty allocator fails at the State object, proving that a valid row count passed admission without
        // allocating records for that many rows. An out-of-range count must decline before that first allocation.
        var empty: [0]u8 = .{};
        var fixed = std.heap.FixedBufferAllocator.init(&empty);
        const cut = if (kind == .full) {} else Cut{ .k = 7, .descending = true };
        for ([_]usize{ 1 << 27, 180_000_000, (1 << 31) - 1 }) |rows| {
            try std.testing.expectError(error.OutOfMemory, O.State.init(
                fixed.allocator(),
                null,
                rows,
                1,
                0,
                1,
                cut,
                full_parts,
                0,
            ));
        }
        try std.testing.expectEqual(@as(?*O.State, null), try O.State.init(
            fixed.allocator(),
            null,
            rowLimit(.int64),
            1,
            0,
            1,
            cut,
            full_parts,
            0,
        ));
    }
}

test "integer owner folding keeps high ordinals and 31-bit counts separate from NULL and SUM flags" {
    const t = std.testing;
    inline for (.{ Kind.full, Kind.sum_top }) |kind| {
        const O = Owner(kind, .int64);
        var groups: [2]O.Group = undefined;
        var slots: [8]u32 = @splat(0);
        var n: usize = 0;
        const late = (1 << 31) - 2;
        const early = (1 << 27) + 5;
        // NULL and zero share a payload, but remain separate even when ordinals set the former length bits.
        O.foldRecord(.{ .key = 0, .value = 0, .ordinal = late, .flags = 3 }, &groups, &slots, 0, &n);
        O.foldRecord(.{ .key = 0, .value = -7, .ordinal = early + 1, .flags = 0 }, &groups, &slots, 0, &n);
        O.foldRecord(.{ .key = 0, .value = 5, .ordinal = early, .flags = 1 }, &groups, &slots, 0, &n);
        try t.expectEqual(@as(usize, 2), n);
        try t.expectEqual(@as(?i64, null), groups[0].keyValue());
        try t.expectEqual(@as(?i64, 0), groups[1].keyValue());
        try t.expectEqual(@as(u32, early), groups[0].ordinal());
        try t.expectEqual(@as(u32, early + 1), groups[1].ordinal());
        try t.expectEqual(@as(?i128, 5), groups[0].sumValue());
        try t.expectEqual(@as(?i128, -7), groups[1].sumValue());
        // Seed a synthetic large count to exercise both upper boundaries without allocating a huge input.
        groups[0].count = O.Group.sum_present | ((1 << 30) - 1);
        O.foldRecord(.{ .key = 0, .value = 0, .ordinal = late, .flags = 3 }, &groups, &slots, 0, &n);
        try t.expectEqual(@as(i64, 1) << 30, groups[0].countValue());
        groups[0].count = O.Group.sum_present | ((1 << 31) - 2);
        O.foldRecord(.{ .key = 0, .value = 0, .ordinal = late, .flags = 3 }, &groups, &slots, 0, &n);
        try t.expectEqual((@as(i64, 1) << 31) - 1, groups[0].countValue());
        try t.expectEqual(@as(?i64, null), groups[0].keyValue());
        try t.expectEqual(@as(u32, early), groups[0].ordinal());
        try t.expectEqual(@as(?i128, 5), groups[0].sumValue());
    }
}

fn runTrial(a: std.mem.Allocator, workers: usize) !void {
    const keys = [_]i64{ -9, -9, -2, -2, -2, -2, -2, -2, 8, 8, 11, 12 };
    const starts = [_]u32{ 0, 3, 3, 5, 7, 10 };
    var ledger: memory.SharedBudget = .{ .limit = 512 << 20 };
    defer std.debug.assert(ledger.used.load(.monotonic) == 0);
    const s = (try IntFull.State.initRuns(a, &ledger, keys.len, workers, 0, 1, {}, full_parts, starts.len, 0)).?;
    defer s.deinit();
    // Reverse completion order, an empty source, and one group through three source seams.
    var item = starts.len;
    while (item != 0) {
        item -= 1;
        const local = &s.locals[item % workers];
        local.start(starts[item], item);
        const end = if (item + 1 == starts.len) keys.len else starts[item + 1];
        for (starts[item]..end) |row|
            try local.append(keys[row], if (row % 3 == 0) null else @as(i64, @intCast(row)) - 6, @intCast(row));
    }
    try s.finish();
    const ids = try s.window(a, 0, s.group_count);
    defer a.free(ids);
    var group: usize = 0;
    for (keys, 0..) |key, row| {
        if (row != 0 and key == keys[row - 1]) continue;
        const got = s.groups[ids[group]];
        group += 1;
        var count: i64 = 0;
        var sum: ?i128 = null;
        for (row..keys.len) |r| {
            if (keys[r] != key) break;
            count += 1;
            if (r % 3 != 0) sum = (sum orelse 0) + @as(i128, @intCast(r)) - 6;
        }
        try std.testing.expectEqual(@as(?i64, key), got.keyValue());
        try std.testing.expectEqual(@as(u32, @intCast(row)), got.ordinal());
        try std.testing.expectEqual(count, got.countValue());
        try std.testing.expectEqual(sum, got.sumValue());
    }
    try std.testing.expectEqual(group, ids.len);
}

test "decoded owner runs fold empty sources and seam chains in source order at every worker count" {
    for ([_]usize{ 1, 2, 4, 8, 16 }) |j| try runTrial(std.testing.allocator, j);
}

test "decoded owner runs release every failed allocation and ledger reservation" {
    try std.testing.checkAllAllocationFailures(std.testing.allocator, runTrial, .{2});
}

test "decoded owner runs decline NULL, an interior descent or an overlapping source before output" {
    const a = std.testing.allocator;
    for ([_][4]?i64{ .{ 1, null, 2, 3 }, .{ 1, 0, 2, 3 }, .{ 1, 2, 1, 3 } }) |keys| {
        const s = (try IntFull.State.initRuns(a, null, keys.len, 2, 0, 1, {}, full_parts, 2, 0)).?;
        defer s.deinit();
        for (0..2) |item| {
            s.locals[item].start(@intCast(item * 2), item);
            for (item * 2..item * 2 + 2) |row| try s.locals[item].append(keys[row], 1, @intCast(row));
        }
        try std.testing.expectError(error.RunOrderDeclined, s.finish());
    }
}

test "decoded owner runs narrow exact sums only after all seams, including a later cancellation" {
    const a = std.testing.allocator;
    for ([_]i64{ -std.math.maxInt(i64), 0 }) |last| {
        const s = (try IntFull.State.initRuns(a, null, 3, 1, 0, 1, {}, full_parts, 3, 0)).?;
        defer s.deinit();
        for ([_]i64{ std.math.maxInt(i64), 1, last }, 0..) |value, item| {
            s.locals[0].start(@intCast(item), item);
            try s.locals[0].append(7, value, @intCast(item));
        }
        if (last == 0) {
            try std.testing.expectError(error.AggIntTooWide, s.finish());
        } else {
            try s.finish();
            try std.testing.expectEqual(@as(?i128, 1), s.groups[0].sumValue());
            try std.testing.expectEqual(@as(i64, 3), s.groups[0].countValue());
        }
    }
}

fn runTopTrial(a: std.mem.Allocator, workers: usize, descending: bool, k: usize) !void {
    return runTopTrialKind(.sum_top, a, workers, descending, k);
}
fn countRunTopTrial(a: std.mem.Allocator, workers: usize, descending: bool, k: usize) !void {
    return runTopTrialKind(.count_top, a, workers, descending, k);
}
fn runTopTrialKind(comptime kind: Kind, a: std.mem.Allocator, workers: usize, descending: bool, k: usize) !void {
    const O = Owner(kind, .int64);
    const rows = 1024;
    const items = 17;
    var ledger: memory.SharedBudget = .{ .limit = 1 << 30 };
    defer std.debug.assert(ledger.used.load(.monotonic) == 0);
    const cut: Cut = .{ .k = k, .descending = descending };
    const run = (try O.State.initRuns(a, &ledger, rows, workers, 0, 1, cut, 64, items, 0)).?;
    defer run.deinit();
    const hash = (try O.State.init(a, &ledger, rows, workers, 0, 1, cut, 64, 0)).?;
    defer hash.deinit();
    var item: usize = items;
    while (item != 0) {
        item -= 1;
        const lo = rows * item / items;
        const hi = rows * (item + 1) / items;
        run.locals[item % workers].start(@intCast(lo), item);
        for (lo..hi) |row| {
            const key: i64 = @intCast(row / 128); // A group crosses three source seams.
            const value: ?i64 = if (key == 0) null else if (row % 11 == 0) -3 else 1;
            try run.locals[item % workers].append(key, if (kind == .sum_top) value else {}, @intCast(row));
            try hash.locals[item % workers].append(key, if (kind == .sum_top) value else {}, @intCast(row));
        }
    }
    try run.finish();
    try hash.finish();
    try std.testing.expectEqual(hash.group_count, run.group_count);
    try std.testing.expectEqualDeep(hash.top(), run.top());
}

test "decoded owner run top-H completes seam chains before SUM ranking and first-ordinal ties" {
    for ([_]usize{ 1, 2, 4, 16 }) |j| for ([_]bool{ false, true }) |descending|
        for ([_]usize{ 1, 7, 16 }) |k| try runTopTrial(std.testing.allocator, j, descending, k);
    spawn.injectFailureAfter(1);
    defer spawn.resetFailure();
    try runTopTrial(std.testing.allocator, 16, true, 7);
}

test "decoded owner run top-H releases failed heaps, group buffers and ledger reservations" {
    try std.testing.checkAllAllocationFailures(std.testing.allocator, runTopTrial, .{ 1, true, 7 });
}

test "decoded owner runs decline unquoted descriptors for mostly empty sources" {
    var ledger: memory.SharedBudget = .{ .limit = 512 << 20 };
    try std.testing.expect((try IntFull.State.initRuns(
        std.testing.allocator,
        &ledger,
        1,
        1,
        0,
        1,
        {},
        full_parts,
        2,
        0,
    )) == null);
    try std.testing.expectEqual(@as(usize, 0), ledger.used.load(.monotonic));
}

test "decoded owner COUNT top-H preserves seam counts and first-selected ties at every worker count" {
    for ([_]usize{ 1, 2, 4, 8, 16 }) |j| for ([_]bool{ false, true }) |descending|
        for ([_]usize{ 1, 7, 16 }) |k| try countRunTopTrial(std.testing.allocator, j, descending, k);
    spawn.injectFailureAfter(1);
    defer spawn.resetFailure();
    try countRunTopTrial(std.testing.allocator, 16, true, 7);
}

test "decoded owner COUNT top-H releases all failed allocations and ledger charges" {
    try std.testing.checkAllAllocationFailures(std.testing.allocator, countRunTopTrial, .{ 1, true, 7 });
}

test "decoded owner COUNT top-H declines NULL and descending keys before selection" {
    for ([_][4]?i64{ .{ 1, null, 2, 3 }, .{ 1, 0, 2, 3 }, .{ 1, 2, 1, 3 } }) |keys| {
        const s = (try IntCountTop.State.initRuns(
            std.testing.allocator,
            null,
            keys.len,
            2,
            0,
            0,
            .{ .k = 2, .descending = true },
            64,
            2,
            0,
        )).?;
        defer s.deinit();
        for (0..2) |item| {
            s.locals[item].start(@intCast(item * 2), item);
            for (item * 2..item * 2 + 2) |row| try s.locals[item].append(keys[row], {}, @intCast(row));
        }
        try std.testing.expectError(error.RunOrderDeclined, s.finish());
    }
}

test "owner cardinality counts complete integer and byte identities once and excludes NULL with failures" {
    for ([_]usize{ 1, 2, 4, 8, 16 }) |j| try ownerCardinalityTrial(std.testing.allocator, j);
    try std.testing.checkAllAllocationFailures(std.testing.allocator, ownerCardinalityTrial, .{1});
}

fn ownerCardinalityTrial(a: std.mem.Allocator, workers: usize) !void {
    inline for (.{ Codec.int64, Codec.bytes }) |codec| {
        const O = Owner(.count_top, codec);
        const rows = 24;
        var budget: memory.SharedBudget = .{ .limit = 64 << 20 };
        const state = (try O.State.initCardinality(a, &budget, rows, workers, 0, 64, 0)) orelse
            return error.TestUnexpectedResult;
        {
            defer state.deinit();
            for (0..rows) |r| {
                const key = if (codec == .int64) ([_]?i64{
                    null, std.math.minInt(i64), std.math.maxInt(i64), 0, -1, 1,
                })[r % 6] else ([_]?[]const u8{
                    null, "", "a", "a\x00", "\x00", "123456789012345",
                })[r % 6];
                try state.locals[r % workers].append(key, {}, @intCast(r));
            }
            try state.finish();
            try std.testing.expectEqual(@as(usize, 5), state.group_count);
            try std.testing.expectEqual(@as(usize, 0), state.top().len);
        }
        try std.testing.expectEqual(@as(usize, 0), budget.used.load(.monotonic));
        const empty = (try O.State.initCardinality(a, &budget, 0, 1, 0, 64, 0)) orelse
            return error.TestUnexpectedResult;
        {
            defer empty.deinit();
            try empty.finish();
            try std.testing.expectEqual(@as(usize, 0), empty.group_count);
        }
        try std.testing.expectEqual(@as(usize, 0), budget.used.load(.monotonic));
        const all_null = (try O.State.initCardinality(a, &budget, 8, workers, 0, 64, 0)) orelse
            return error.TestUnexpectedResult;
        {
            defer all_null.deinit();
            for (0..8) |r| try all_null.locals[r % workers].append(null, {}, @intCast(r));
            try all_null.finish();
            try std.testing.expectEqual(@as(usize, 0), all_null.group_count);
        }
        try std.testing.expectEqual(@as(usize, 0), budget.used.load(.monotonic));
    }
}

test "distinct owner radix keeps full pairs NULLs and earliest ties across workers and failures" {
    for ([_]usize{ 1, 2, 4, 8, 16 }) |j| for ([_]usize{ 7, 128 }) |h|
        try distinctOwnerTrial(std.testing.allocator, j, h);
    try std.testing.checkAllAllocationFailures(std.testing.allocator, distinctOwnerTrial, .{ 1, 128 });
    for ([_]usize{ 0, 1, 3 }) |after| {
        spawn.injectFailureAfter(after);
        defer spawn.resetFailure();
        try distinctOwnerTrial(std.testing.allocator, 4, 128);
        try std.testing.expect(spawn.failuresInjected() != 0);
    }
}

fn distinctOwnerTrial(a: std.mem.Allocator, workers: usize, h: usize) !void {
    const O = Owner(.sum_top, .int64);
    const Ref = struct { key: ?i64, first: u32, values: std.AutoHashMap(i64, void) };
    var refs: std.ArrayList(Ref) = .empty;
    defer {
        for (refs.items) |*ref| ref.values.deinit();
        refs.deinit(a);
    }
    var budget: memory.SharedBudget = .{ .limit = 64 << 20 };
    const state = (try O.State.initDistinctTop(
        a,
        &budget,
        512,
        workers,
        0,
        1,
        .{ .k = h, .descending = true },
        64,
        0,
    )) orelse return error.TestUnexpectedResult;
    {
        defer state.deinit();
        const keys = [_]?i64{
            null,    0,             1,             -1, std.math.minInt(i64), std.math.maxInt(i64),
            1 << 48, (1 << 48) + 1, (1 << 48) - 1, 11, 37,
        };
        const vals = [_]i64{ std.math.minInt(i64), std.math.maxInt(i64), 0, -1, 1, 1 << 40 };
        for (0..512) |r| {
            const key = keys[(r * 17) % keys.len];
            const value: ?i64 = if (key == 0 or r % 17 == 0) null else vals[(r / keys.len + r / 3) % vals.len];
            try state.locals[r % workers].append(key, value, @intCast(r));
            var at = refs.items.len;
            for (refs.items, 0..) |ref, i| if (ref.key == key) {
                at = i;
                break;
            };
            if (at == refs.items.len) try refs.append(a, .{
                .key = key,
                .first = @intCast(r),
                .values = std.AutoHashMap(i64, void).init(a),
            });
            if (value) |v| try refs.items[at].values.put(v, {});
        }
        try state.finish();
        try std.testing.expectEqual(refs.items.len, state.group_count);
        std.mem.sort(Ref, refs.items, {}, struct {
            fn less(_: void, x: Ref, y: Ref) bool {
                return x.values.count() > y.values.count() or
                    (x.values.count() == y.values.count() and x.first < y.first);
            }
        }.less);
        const n = @min(h, refs.items.len);
        std.mem.sort(Ref, refs.items[0..n], {}, struct {
            fn less(_: void, x: Ref, y: Ref) bool {
                return x.first < y.first;
            }
        }.less);
        try std.testing.expectEqual(n, state.top().len);
        for (refs.items[0..n], state.top()) |ref, got| {
            try std.testing.expectEqual(ref.key, got.keyValue());
            try std.testing.expectEqual(ref.first, got.ordinal());
            try std.testing.expectEqual(@as(i128, @intCast(ref.values.count())), O.rank(got).?);
        }
    }
    try std.testing.expectEqual(@as(usize, 0), budget.used.load(.monotonic));
}

fn hotCountTrial(a: std.mem.Allocator, workers: usize) !void {
    const O = IntCountTop;
    const rows = 8192;
    var budget: memory.SharedBudget = .{ .limit = 256 << 20 };
    {
        const s = (try O.State.init(a, &budget, rows, workers, 0, 0, .{ .k = 16, .descending = true }, 64, 0)).?;
        defer s.deinit();
        var counts: [16]u32 = @splat(0);
        var first: [16]u32 = @splat(none);
        // NULL and zero are equally hot, with early ordinals deliberately arriving after their later partials.
        // The remaining identities evict cache entries repeatedly across logical row-group boundaries.
        for (0..rows) |r| {
            const ordinal: u32 = @intCast(rows - 1 - r);
            const id: usize = if (r % 4 < 2) r % 2 else 2 + r % 14;
            const key: ?i64 = if (id == 0) null else if (id == 1) 0 else @as(i64, @intCast(id)) * 0x123456789;
            counts[id] += 1;
            first[id] = @min(first[id], ordinal);
            const local = &s.locals[(r / 113) % workers];
            if (r % 113 == 0) local.start(0, 0);
            try local.append(key, {}, ordinal);
        }
        var weighted_null = false;
        var weighted_zero = false;
        for (s.locals) |*local| {
            try local.flushCounts();
            for (local.records.heads) |head| {
                var id = head;
                while (id != none) : (id = s.blocks[id].next) {
                    const block = &s.blocks[id];
                    for (block.rows[0..block.len]) |record| if (record.ordinal & null_key != 0) {
                        weighted_null = weighted_null or record.flags & 1 != 0;
                        weighted_zero = weighted_zero or (record.flags & 1 == 0 and record.key == 0);
                    };
                }
            }
        }
        try std.testing.expect(weighted_null and weighted_zero);
        try s.finish();
        try std.testing.expectEqual(@as(usize, 16), s.top().len);
        var seen: u32 = 0;
        for (s.top()) |g| {
            const id: usize = if (g.keyValue()) |key| if (key == 0) 1 else @intCast(@divExact(key, 0x123456789)) else 0;
            try std.testing.expectEqual(@as(i64, counts[id]), g.countValue());
            try std.testing.expectEqual(first[id], g.ordinal());
            seen |= @as(u32, 1) << @intCast(id);
        }
        try std.testing.expectEqual(@as(u32, 65535), seen);
        var emitted: usize = 0;
        var contributed: usize = 0;
        for (s.locals) |local| {
            for (local.records.histogram) |n| emitted += n;
            contributed += local.contributed_rows;
        }
        try std.testing.expectEqual(@as(usize, rows), contributed);
        try std.testing.expect(emitted < rows * 3 / 4);
    }
    try std.testing.expectEqual(@as(usize, 0), budget.used.load(.monotonic));
}

test "hot integer COUNT partials cache NULL and preserve evictions first ordinals and ledger on every failure" {
    defer hw.test_cpu = null;
    // One line forces collisions, including between NULL and an ordinary key, through the shared eviction path.
    hw.test_cpu = .{ .l1d = .{ .bytes = 256, .line_bytes = 64 }, .l2 = .{ .bytes = 4096 } };
    for ([_]usize{ 1, 2, 4, 8, 16 }) |workers| try hotCountTrial(std.testing.allocator, workers);
    try std.testing.checkAllAllocationFailures(std.testing.allocator, hotCountTrial, .{1});
}

test "hot integer COUNT weighted records keep high counts ordinals NULL and zero independent" {
    const O = IntCountTop;
    var groups: [2]O.Group = undefined;
    var slots: [8]u32 = @splat(0);
    var n: usize = 0;
    const early = (1 << 30) + 7;
    const late = (1 << 31) - 2;
    const weight = (1 << 31) - 3;
    O.foldRecord(.{ .key = 0, .ordinal = late | null_key, .flags = weight << 1 | 1 }, &groups, &slots, 0, &n);
    O.foldRecord(.{ .key = 0, .ordinal = early | null_key, .flags = 4 << 1 }, &groups, &slots, 0, &n);
    O.foldRecord(.{ .key = 0, .ordinal = early, .flags = 1 }, &groups, &slots, 0, &n);
    try std.testing.expectEqual(@as(usize, 2), n);
    try std.testing.expectEqual(@as(?i64, null), groups[0].keyValue());
    try std.testing.expectEqual(@as(i64, weight + 1), groups[0].countValue());
    try std.testing.expectEqual(@as(u32, early), groups[0].ordinal());
    try std.testing.expectEqual(@as(?i64, 0), groups[1].keyValue());
    try std.testing.expectEqual(@as(i64, 4), groups[1].countValue());
    try std.testing.expectEqual(@as(u32, early), groups[1].ordinal());
}

test "hot integer COUNT cache capacity follows cache lines and L1 L2 shares" {
    defer hw.test_cpu = null;
    hw.test_cpu = .{ .l1d = .{ .bytes = 32 << 10, .line_bytes = 64 }, .l2 = .{ .bytes = 1 << 20 } };
    const wide = IntCountTop.countCacheEntries();
    try std.testing.expectEqual(@as(usize, 1024), wide);
    hw.test_cpu.?.l1d.max_sharing = 2;
    try std.testing.expectEqual(wide, IntCountTop.countCacheEntries());
    hw.test_cpu.?.l2.bytes = 64 << 10;
    try std.testing.expectEqual(@as(usize, 256), IntCountTop.countCacheEntries());
    hw.test_cpu.?.l1d.line_bytes = 8192;
    try std.testing.expectEqual(@as(usize, 0), IntCountTop.countCacheEntries());
    try std.testing.expectEqual(@as(usize, 0), CountTop.countCacheEntries());
    try std.testing.expectEqual(@as(usize, 16), @sizeOf(IntCountTop.Record));
}

test "hot integer COUNT cache bypasses a nonreducing source and retries later hot NULLs exactly" {
    defer hw.test_cpu = null;
    hw.test_cpu = .{ .l1d = .{ .bytes = 256, .line_bytes = 64 }, .l2 = .{ .bytes = 4096 } };
    const a = std.testing.allocator;
    const rows = 512;
    const s = (try IntCountTop.State.init(a, null, rows, 1, 0, 0, .{ .k = 4, .descending = true }, 64, 0)).?;
    defer s.deinit();
    const local = &s.locals[0];
    local.start(0, 0);
    for (0..256) |r| try local.append(@intCast(r), {}, @intCast(r));
    try std.testing.expect(local.cache_disabled);
    for (local.count_cache.entries) |entry| try std.testing.expectEqual(@as(u32, 0), entry.count);
    local.start(256, 1);
    try std.testing.expect(!local.cache_disabled);
    for (256..rows) |r| try local.append(null, {}, @intCast(r));
    try std.testing.expect(!local.cache_disabled);
    try s.finish();
    const winner = for (s.top()) |group| {
        if (group.keyValue() == null) break group;
    } else return error.TestUnexpectedResult;
    try std.testing.expectEqual(@as(i64, 256), winner.countValue());
    try std.testing.expectEqual(@as(u32, 256), winner.ordinal());
    try std.testing.expectEqual(@as(usize, rows), local.contributed_rows);
}

test "hot integer COUNT run quote excludes its unallocated cache and failed claims preserve histograms" {
    defer hw.test_cpu = null;
    hw.test_cpu = .{ .l1d = .{ .bytes = 32 << 10, .line_bytes = 64 }, .l2 = .{ .bytes = 1 << 20 } };
    const O = IntCountTop;
    const Entry = @typeInfo(@FieldType(O.State, "count_caches")).pointer.child;
    const rows = 128;
    var budget: memory.SharedBudget = .{ .limit = 1 << 30 };
    const ordinary = O.State.reservation(rows, 1, 64, 4, 0);
    const run_quote = O.State.runReservation(rows, 1, 64, 4, 0);
    try std.testing.expectEqual(O.countCacheEntries() * @sizeOf(Entry), ordinary - run_quote);
    const run = (try O.State.initRuns(std.testing.allocator, &budget, rows, 1, 0, 0, .{ .k = 4, .descending = true }, 64, 1, 0)).?;
    try std.testing.expectEqual(run_quote, budget.used.load(.monotonic));
    try std.testing.expectEqual(@as(usize, 0), run.count_caches.len);
    run.deinit();
    try std.testing.expectEqual(@as(usize, 0), budget.used.load(.monotonic));
    const s = (try O.State.init(std.testing.allocator, &budget, rows, 1, 0, 0, .{ .k = 4, .descending = true }, 64, 0)).?;
    defer s.deinit();
    const blocks = s.blocks;
    s.blocks = blocks[0..0];
    defer s.blocks = blocks;
    const local = &s.locals[0];
    var entry: Entry = .{ .key = 9, .first = 0, .count = 2 };
    try std.testing.expectError(error.ExceededMemoryBudget, local.flushCount(&entry));
    try std.testing.expectEqual(@as(u32, 2), entry.count);
    for (local.records.histogram) |n| try std.testing.expectEqual(@as(u32, 0), n);
    try std.testing.expectEqual(@as(usize, 0), local.contributed_rows);
}

fn countScratchGrowthTrial(a: std.mem.Allocator) !void {
    var budget: memory.SharedBudget = .{ .limit = 1 << 30 };
    defer std.debug.assert(budget.used.load(.monotonic) == 0);
    const s = (try IntCountTop.State.init(a, &budget, 1028, 1, 0, 0, .{ .k = 257, .descending = true }, 64, 0)).?;
    defer s.deinit();
    for (0..1028) |i| try s.locals[0].append(if (i % 257 == 0) null else @intCast(i % 257), {}, @intCast(i));
    try s.finish();
    try std.testing.expectEqual(@as(usize, 257), s.group_count);
    for (s.top(), 0..) |g, i| {
        try std.testing.expectEqual(@as(i64, 4), g.countValue());
        try std.testing.expectEqual(@as(u32, @intCast(i)), g.ordinal());
        try std.testing.expectEqual(if (i == 0) null else @as(?i64, @intCast(i)), g.keyValue());
    }
}

test "hot integer COUNT scratch grows from exact groups preserving NULL ties and all failed allocations" {
    defer hw.test_cpu = null;
    hw.test_cpu = .{ .l1d = .{ .bytes = 256, .line_bytes = 64 }, .l2 = .{ .bytes = 128 } };
    try countScratchGrowthTrial(std.testing.allocator);
    try std.testing.checkAllAllocationFailures(std.testing.allocator, countScratchGrowthTrial, .{});
}
