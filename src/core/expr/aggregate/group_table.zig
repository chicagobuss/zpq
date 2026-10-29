const std = @import("std");
const filter_selection = @import("../../filter/selection.zig");
const expr_ast = @import("../ast.zig");
const filter_eval = @import("../../filter/eval.zig");
const core_memory = @import("../../memory.zig");
const core_bytes = @import("../../bytes.zig");
const keyEql = core_bytes.keyEql;
const KeyHashMap = core_bytes.KeyHashMap;
const KeyHashMapUnmanaged = core_bytes.KeyHashMapUnmanaged;
const types = @import("types.zig");
const AggCall = types.AggCall;
const DecSum = types.DecSum;
const AvgState = types.AvgState;
const Error = types.Error;
const SharedBudget = core_memory.SharedBudget;
const KeyArena = @import("keys.zig").KeyArena;
const GroupKeyResolver = @import("keys.zig").GroupKeyResolver;
const keys_mod = @import("keys.zig");
const hashFixed = keys_mod.hashFixed;
const isRowNull = keys_mod.isRowNull;
const expandFixed = keys_mod.expandFixed;
const fixedImage = keys_mod.fixedImage;
const canonicalKey = keys_mod.canonicalKey;
const serializeRowKey = keys_mod.serializeRowKey;
const census_mod = @import("census.zig");
const CountSketch = census_mod.CountSketch;
const Census = census_mod.Census;
const sketchHash = census_mod.sketchHash;
const rawSketchHash = census_mod.rawSketchHash;
const wholeRawHash = census_mod.wholeRawHash;
const distinct = @import("distinct.zig");
const NUM_PARTS = distinct.NUM_PARTS;
const NUMERIC_PARTS = distinct.NUMERIC_PARTS;
const INLINE_CAP = distinct.INLINE_CAP;
const InlineU64 = distinct.InlineU64;
const InlineI128 = distinct.InlineI128;
const InlineBytes = distinct.InlineBytes;
const partOf = distinct.partOf;
const DistinctBytes = distinct.DistinctBytes;
const foldExtremeF = @import("scalar.zig").foldExtremeF;

const DistinctSet = distinct.DistinctSet;
const DistinctAcc = distinct.DistinctAcc;

const Accumulator = @import("accumulator.zig").Accumulator;

const testing = std.testing;
const schema = @import("../../schema.zig");
const filter_ast = @import("../../filter/ast.zig");
const expr_eval = @import("../eval.zig");
const decimal_mod = @import("../../parquet/decimal.zig");
const statistics = @import("../../parquet/statistics.zig");
const agg_coded = @import("../agg_coded.zig");
const AggFunc = types.AggFunc;
const ResultShape = types.ResultShape;
const scalar_mod = @import("scalar.zig");
const grouped_mod = @import("grouped.zig");
const updateOne = scalar_mod.updateOne;
const updateOneFromStats = scalar_mod.updateOneFromStats;
const updateSharedInteger = scalar_mod.updateSharedInteger;
const finalize = scalar_mod.finalize;
const foldMinMaxBytes = scalar_mod.foldMinMaxBytes;
const simdMinMaxF64 = scalar_mod.simdMinMaxF64;
const prefix16 = scalar_mod.prefix16;
const statPresentCount = scalar_mod.statPresentCount;
const testI64Meta = scalar_mod.testI64Meta;
const packFixedKey = keys_mod.packFixedKey;
const splitMix64 = census_mod.splitMix64;
const FingerprintSet = census_mod.FingerprintSet;
const fullestCells = census_mod.fullestCells;
const cellsSeparate = census_mod.cellsSeparate;
const DistinctPairs = grouped_mod.DistinctPairs;
const updateOneGrouped = grouped_mod.updateOneGrouped;
const foldMinMaxBytesGroupedOne = grouped_mod.foldMinMaxBytesGroupedOne;
const WinnerCharge = @import("accumulator.zig").WinnerCharge;
const winnerBytes = @import("accumulator.zig").winnerBytes;
// Known exception: the facade owns writable thread-local state until the later contract pass.
const agg_threadlocals = @import("../agg.zig");

const CachedKey = struct { bytes: []const u8, hash: u64 };
const CachedKeyCtx = struct {
    pub fn hash(_: @This(), key: CachedKey) u64 {
        return key.hash;
    }
    pub fn eql(_: @This(), key: CachedKey, stored: []const u8) bool {
        return keyEql(key.bytes, stored);
    }
};

pub const GroupTable = struct {
    const StringView = struct { bytes: []const u8, first: u64 };

    compact: @import("../agg_state.zig").State = .{},
    // Backing arrays can be page-backed independently of their accumulator payloads.
    buffer_allocator: ?std.mem.Allocator = null,
    // 16-aligned: a private distinct acc stores this pointer with bit 3 set (`DistinctAcc.table_flag`).
    allocator: std.mem.Allocator align(16),
    refs: std.ArrayList(u32) = .empty,
    // Stable arena views remove refs -> chunk -> varint dependencies from repeated direct-string probes.
    // First stays beside the view; the canonical first array is written only when a smaller ordinal arrives.
    string_views: std.ArrayList(StringView) = .empty,
    strings: ?*KeyArena = null,
    slots: []FixedSlot = &.{},
    /// Where each group first appeared, `first_position` of the row that created it (see `getOrInsert`): the ordered
    /// output lists groups in this order.
    first: std.ArrayList(u64) = .empty,
    accumulators: std.ArrayList(Accumulator),
    allocated_bytes: usize,
    max_memory_bytes: usize,
    shared: ?*SharedBudget = null,
    /// Spilled distinct-set objects on a private ceiling. Part of `fixedHeld`, so the next fixed insert does not
    /// wipe the charge. Zero when a shared budget owns those objects.
    set_objects: usize = 0,
    /// Nonzero: every key is this many bytes in `fixed_keys` (see `fixedGroupStride`), and `map` is unused.
    fixed_stride: u8 = 0,
    fixed_len: u32 = 0,
    fixed_keys: []u8 = &.{},
    fixed_slots: []FixedSlot = &.{},
    // A doubling adds only the new half. Existing slots and the new half together are the final capacity.
    fixed_extra: [29]?[*]FixedSlot = @splat(null),
    fixed_extra_count: u8 = 0,
    fixed_base_shift: u8 = 0,
    fixed_capacity: usize = 0,
    /// A certified top-k pass: keys outside the admitted cells get a cell's id (`CountSketch.sinkId`) rather than a
    /// group. The id is the key's alone to take, so callers that memoize ids per key stay correct.
    sketch: ?*CountSketch = null,
    /// Discovery retains only a worker's first H exact keys. The shared ordinal bounds discovery, never counts.
    discovery: ?Discovery = null,

    pub const Discovery = struct { limit: usize, threshold: *std.atomic.Value(u64) };

    pub fn discoveryDone(self: *const GroupTable, pos: u64) bool {
        return if (self.discovery) |d| pos >= d.threshold.load(.monotonic) else false;
    }

    fn discovered(self: *GroupTable, pos: u64) void {
        if (self.discovery) |d| if (self.groupCount() >= d.limit) {
            _ = d.threshold.fetchMin(pos, .monotonic);
        };
    }

    const empty_id: u32 = std.math.maxInt(u32);
    // The projected hash rejects candidates; only a complete normalized key comparison admits one.
    const FixedSlot = extern struct { hash: u32, id: u32 };

    comptime {
        if (@alignOf(GroupTable) < 16) @compileError("private distinct sets use bit 3 of this pointer");
    }

    pub fn freshDistinct(self: *GroupTable) DistinctAcc {
        if (self.shared) |budget| return .initEmpty(budget);
        return .initPrivate(self);
    }

    /// Object charge for a spilled set on a private ceiling, checked against the live `max_memory_bytes`. The set
    /// releases it. A shared budget charges the set instead, so this is not a second bill.
    pub fn chargeSpilledSet(self: *GroupTable) error{ExceededMemoryBudget}!void {
        return self.chargePrivateBytes(@sizeOf(DistinctSet));
    }

    pub fn chargePrivateBytes(self: *GroupTable, n: usize) error{ExceededMemoryBudget}!void {
        var before = @atomicLoad(usize, &self.allocated_bytes, .monotonic);
        while (true) {
            const after = std.math.add(usize, before, n) catch return error.ExceededMemoryBudget;
            if (after > self.max_memory_bytes) return error.ExceededMemoryBudget;
            before = @cmpxchgWeak(usize, &self.allocated_bytes, before, after, .monotonic, .monotonic) orelse break;
        }
        _ = @atomicRmw(usize, &self.set_objects, .Add, n, .monotonic);
    }

    pub fn releaseSpilledSet(self: *GroupTable) void {
        return self.releasePrivateBytes(@sizeOf(DistinctSet));
    }

    pub fn releasePrivateBytes(self: *GroupTable, n: usize) void {
        // Disjoint merge/cleanup partitions can grow or release sets charged to the same source table. Scanning
        // and table growth have joined; only the atomic set-object charges/releases overlap in that phase.
        const objects = @atomicRmw(usize, &self.set_objects, .Sub, n, .monotonic);
        const bytes = @atomicRmw(usize, &self.allocated_bytes, .Sub, n, .monotonic);
        std.debug.assert(objects >= n and bytes >= n);
    }

    pub fn init(allocator: std.mem.Allocator, max_memory_bytes: usize) GroupTable {
        return .{
            .allocator = core_memory.groupAllocator(allocator),
            .accumulators = .empty,
            .allocated_bytes = 0,
            .max_memory_bytes = max_memory_bytes,
        };
    }

    pub fn deinit(self: *GroupTable) void {
        self.deinitRange(0, self.groupCount());
        self.deinitStorage();
    }

    /// Disjoint group ranges can release their owned payloads concurrently. Storage remains live until every
    /// range has joined; callers then use deinitStorage, rather than walking all groups again on the main thread.
    pub fn deinitRange(self: *GroupTable, begin: usize, end: usize) void {
        std.debug.assert(begin <= end and end <= self.groupCount());
        // An allocation failure may leave the last group only partly initialized. Partition the actual cell
        // slice, including that tail; an empty table's destructor must also release any cells it already owns.
        const groups = self.groupCount();
        const cells = self.accumulators.items.len;
        const lo: usize = if (groups == 0) 0 else @intCast(@as(u128, cells) * begin / groups);
        const hi: usize = if (groups == 0) cells else @intCast(@as(u128, cells) * end / groups);
        for (self.accumulators.items[lo..hi]) |*acc| {
            switch (acc.*) {
                .min_bytes => |mb| if (mb) |s| self.allocator.free(s),
                .max_bytes => |mb| if (mb) |s| self.allocator.free(s),
                .distinct => |*d| d.deinit(self.allocator),
                else => {},
            }
        }
    }

    /// Requires that deinitRange has released every group exactly once.
    pub fn deinitStorage(self: *GroupTable) void {
        self.refs.deinit(self.allocator);
        self.string_views.deinit(self.allocator);
        if (self.strings) |strings| {
            strings.deinit(self.allocator);
            self.allocator.destroy(strings);
        }
        self.first.deinit(self.allocator);
        self.accumulators.deinit(self.buffer_allocator orelse self.allocator);
        self.compact.words.deinit(self.buffer_allocator orelse self.allocator);
        if (self.fixed_keys.len != 0) self.allocator.free(self.fixed_keys);
        const first_slots = self.firstSlots();
        for (0..self.fixed_extra_count) |i| {
            const len = first_slots.len << @as(std.math.Log2Int(usize), @intCast(i));
            self.allocator.free(self.fixed_extra[i].?[0..len]);
        }
        self.allocator.free(self.slots);
        if (self.fixed_slots.len != 0) self.allocator.free(self.fixed_slots);
        // A shared table starts at a zero ceiling, so all of `max_memory_bytes` came from the pool. Given back last:
        // another table may draw it as soon as it is back, while this one's memory is still held.
        if (self.shared) |budget| budget.release(self.max_memory_bytes);
    }

    pub inline fn readAcc(self: *const GroupTable, g: usize, i: usize, n: usize) Accumulator {
        return if (self.compact.stride != 0) self.compact.read(g, i) else self.accumulators.items[g * n + i];
    }

    pub inline fn writeAcc(self: *const GroupTable, g: usize, i: usize, n: usize, value: Accumulator) void {
        if (self.compact.stride != 0) self.compact.write(g, i, value) else self.accumulators.items[g * n + i] = value;
    }

    /// Compact records own no pointers. Generic records return their actual cells so ownership moves still detach.
    pub inline fn readGroup(self: *const GroupTable, g: usize, n: usize, scratch: *[16]Accumulator) []Accumulator {
        if (self.compact.stride == 0) return self.accumulators.items[g * n ..][0..n];
        for (scratch[0..n], 0..) |*a, i| a.* = self.compact.read(g, i);
        return scratch[0..n];
    }

    pub inline fn writeGroup(self: *const GroupTable, g: usize, values: []const Accumulator) void {
        if (self.compact.stride != 0) for (values, 0..) |a, i| self.compact.write(g, i, a);
    }

    pub fn groupCount(self: *const GroupTable) usize {
        return if (self.fixed_stride == 0) self.refs.items.len else self.fixed_len;
    }

    /// Key bytes of group `g`. For a fixed-width table this is the inline image, stable until the next insert that
    /// grows `fixed_keys`.
    pub fn keyAt(self: *const GroupTable, g: usize) []const u8 {
        if (self.string_views.items.len != 0) return self.string_views.items[g].bytes;
        if (self.fixed_stride == 0) return self.strings.?.get(self.refs.items[g]);
        const s: usize = self.fixed_stride;
        return self.fixed_keys[g * s ..][0..s];
    }

    /// After a census admit pass over a fixed-width table: the groups whose fingerprint `census` did not admit. Each
    /// was grouped only for its row in the prefix, so with a census bound below 2 it is a key of that one row.
    pub fn unadmittedGroups(self: *const GroupTable, census: *const Census) usize {
        std.debug.assert(self.fixed_stride != 0);
        var n: usize = 0;
        for (0..self.groupCount()) |g| {
            if (!census.admitted.?.contains(census.fingerprint(hashFixed(self.keyAt(g))))) n += 1;
        }
        return n;
    }

    /// Whether a sketch's count pass is running: rows take no group, and aggregates neither fold nor read.
    pub fn countOnly(self: *const GroupTable) bool {
        if (self.discovery != null) return true;
        return if (self.sketch) |s| s.counting() else false;
    }

    /// The hash a sketch or census routes group `g`'s key by.
    pub fn routeHash(self: *const GroupTable, g: usize) u64 {
        return self.hashRoute(self.keyAt(g));
    }

    /// `routeHash` for a key that is not in the table yet: the fixed image, or the bytes `getOrInsert` stores.
    pub fn hashRoute(self: *const GroupTable, key: []const u8) u64 {
        if (self.fixed_stride != 0) {
            if (key.len == self.fixed_stride) return hashFixed(key);
            var buf: [32]u8 = undefined;
            const image = expandFixed(self.fixed_stride, key, &buf) orelse return hashFixed(key);
            return hashFixed(image);
        }
        return if (self.sketch) |s| s.keyHash(key) else sketchHash(key);
    }

    pub fn findKey(self: *const GroupTable, key: []const u8) ?u32 {
        if (self.fixed_stride == 0) return self.probeVariable(key, std.hash.Wyhash.hash(0, key)).id;
        if (key.len == self.fixed_stride) return self.findFixed(key, hashFixed(key));
        var buf: [32]u8 = undefined;
        const image = expandFixed(self.fixed_stride, key, &buf) orelse return null;
        return self.findFixed(image, hashFixed(image));
    }

    /// A sketched string table's id for a lone present string key given unframed when its cell is not admitted; null
    /// when the key takes the usual path (admitted, or too short for `rawSketchHash`).
    pub fn sketchRaw(self: *const GroupTable, raw: []const u8) !?u32 {
        const s = self.sketch.?;
        if (raw.len < 16) return null;
        const hash = if (s.whole_keys) wholeRawHash(raw) else rawSketchHash(raw);
        if (s.census) |c| if (!c.off) {
            if (c.admitted == null) return try c.slot(hash);
            // Whether a key is early enough to be grouped anyway takes its position, which the usual path has.
            if (!c.off) return if (c.hasPrefix() or c.admits(hash, 0)) null else CountSketch.sinkId(0);
        };
        const cell = s.cellOf(hash);
        return if (s.admits(cell)) null else CountSketch.sinkId(cell);
    }

    /// Owning thread only. A table that has stopped growing must not sit on a partly-used block while another worker
    /// is still filling, or admission depends on how row groups happened to be scheduled.
    pub fn releaseSlack(self: *GroupTable) void {
        const budget = self.shared orelse return;
        const slack = self.max_memory_bytes - self.allocated_bytes;
        if (slack == 0) return;
        self.max_memory_bytes = self.allocated_bytes;
        budget.release(slack);
    }

    /// The group of `key`, made if new. `pos` is where the row asking for it sits in the input (`firstPosition`); a
    /// group remembers the least it was asked from.
    pub fn getOrInsert(self: *GroupTable, key: []const u8, agg_calls: []const AggCall, pos: u64) !u32 {
        if (self.strings) |strings| strings.owner = self;
        if (self.discoveryDone(pos)) return CountSketch.sinkId(0);
        if (self.fixed_stride != 0) {
            if (key.len == self.fixed_stride) return self.getOrInsertFixed(key, agg_calls, pos);
            var buf: [32]u8 = undefined;
            const image = expandFixed(self.fixed_stride, key, &buf) orelse return error.BadGroupKey;
            return self.getOrInsertFixed(image, agg_calls, pos);
        }
        if (self.sketch) |s| if (try s.route(s.keyHash(key), pos, key)) |id| return id;
        const hash = std.hash.Wyhash.hash(0, key);
        return self.insertVariable(false, key, true, agg_calls, pos, hash);
    }

    /// A direct string table hashes unframed bytes once. Its arena still stores the canonical key frame, so all
    /// output consumers remain shared. Only a new global key writes that frame and copies its bytes.
    pub fn getOrInsertStringHashed(
        self: *GroupTable,
        key: ?[]const u8,
        agg_calls: []const AggCall,
        pos: u64,
        hash: u64,
    ) !u32 {
        std.debug.assert(self.fixed_stride == 0 and self.sketch == null and self.discovery == null);
        return self.insertVariable(true, key orelse &.{}, key != null, agg_calls, pos, hash);
    }

    fn insertVariable(
        self: *GroupTable,
        comptime raw_string: bool,
        key: []const u8,
        present: bool,
        agg_calls: []const AggCall,
        pos: u64,
        hash: u64,
    ) !u32 {
        if (self.strings) |strings| strings.owner = self;
        const key_len = if (raw_string) (if (present) key.len +| 5 else 1) else key.len;
        const probe = if (raw_string) self.probeString(key, present, hash) else self.probeVariable(key, hash);
        if (probe.id) |id| {
            if (raw_string) {
                const view = &self.string_views.items[id];
                if (pos < view.first) {
                    view.first = pos;
                    self.first.items[id] = pos;
                }
            } else self.first.items[id] = @min(self.first.items[id], pos);
            return id;
        }
        const n = self.groupCount();
        if (n == empty_id) return error.ExceededMemoryBudget;
        if (self.strings == null) {
            try self.admit(@sizeOf(KeyArena));
            self.strings = try self.allocator.create(KeyArena);
            self.strings.?.* = .{
                .allocator = self.allocator,
                .owner = self,
                .reserveFn = stringReserve,
                .settleFn = stringSettle,
            };
            self.allocated_bytes += @sizeOf(KeyArena);
        }
        if (self.strings.?.blocks.items.len == 0) {
            const credit = if (self.shared) |b| @min(b.block, b.limit / 4) else self.max_memory_bytes / 4;
            self.strings.?.bootstrap = @min(KeyArena.chunk_size, if (credit == 0) 1 else @as(usize, 1) << @intCast(std.math.log2_int(usize, credit)));
        }
        const old_slots = self.fixedSlotCount();
        const grow = old_slots == 0 or (n + 1) * 4 > old_slots * 3;
        const cap = if (grow) @max(old_slots * 2, 16) else old_slots;
        const refs_cap = if (n + 1 > self.refs.capacity) std.ArrayList(u32).growCapacity(n + 1) else self.refs.capacity;
        const first_cap = if (n + 1 > self.first.capacity) std.ArrayList(u64).growCapacity(n + 1) else self.first.capacity;
        const views_cap = if (raw_string and n + 1 > self.string_views.capacity)
            std.ArrayList(StringView).growCapacity(n + 1)
        else
            self.string_views.capacity;
        const cells = self.accumulators.items.len + if (self.compact.stride == 0) agg_calls.len else 0;
        const acc_cap = if (cells > self.accumulators.capacity)
            std.ArrayList(Accumulator).growCapacity(cells)
        else
            self.accumulators.capacity;
        const projected = @sizeOf(KeyArena) + try self.strings.?.projected(key_len) + cap * @sizeOf(FixedSlot) +
            refs_cap * @sizeOf(u32) + first_cap * @sizeOf(u64) + views_cap * @sizeOf(StringView) +
            acc_cap * @sizeOf(Accumulator) +
            (if (self.compact.stride != 0) self.compact.projected(n + 1) else 0) + self.set_objects;
        try self.admit(projected -| self.allocated_bytes);
        // A successful remap or later grow may survive a subsequent allocation failure.
        defer self.allocated_bytes = self.variableHeld();
        tryRemapList(u32, self.allocator, &self.refs, refs_cap);
        tryRemapList(u64, self.allocator, &self.first, first_cap);
        if (raw_string) tryRemapList(StringView, self.allocator, &self.string_views, views_cap);
        tryRemapList(Accumulator, self.bufferAllocator(), &self.accumulators, acc_cap);
        if (self.compact.stride != 0)
            tryRemapList(u64, self.bufferAllocator(), &self.compact.words, self.compact.projected(n + 1) / @sizeOf(u64));
        // Direct shared tables also fund the transient old/new metadata overlap before a grow. The arena's
        // append-only chunks already include every live byte in projected(), and never replace a key buffer.
        var overlap: usize = 0;
        if (refs_cap > self.refs.capacity) overlap = @max(overlap, self.refs.capacity * @sizeOf(u32));
        if (first_cap > self.first.capacity) overlap = @max(overlap, self.first.capacity * @sizeOf(u64));
        if (views_cap > self.string_views.capacity)
            overlap = @max(overlap, self.string_views.capacity * @sizeOf(StringView));
        if (acc_cap > self.accumulators.capacity)
            overlap = @max(overlap, self.accumulators.capacity * @sizeOf(Accumulator));
        if (self.compact.stride != 0 and self.compact.projected(n + 1) > self.compact.held())
            overlap = @max(overlap, self.compact.held());
        overlap = @max(overlap, try self.strings.?.projectedOverlap(key_len));
        if (grow) try self.growFixedSlots(cap, projected);
        const extra = try self.reserveOverlap(projected, overlap);
        defer if (extra != 0) self.shared.?.release(extra);
        try self.refs.ensureTotalCapacity(self.allocator, n + 1);
        try self.first.ensureTotalCapacity(self.allocator, n + 1);
        if (raw_string) try self.string_views.ensureTotalCapacity(self.allocator, n + 1);
        if (self.compact.stride != 0) try self.compact.reserve(self.bufferAllocator(), n + 1);
        try self.accumulators.ensureTotalCapacity(self.bufferAllocator(), cells);
        const ref = if (raw_string) blk: {
            var frame: [5]u8 = @splat(0);
            if (present) {
                frame[0] = 1;
                std.mem.writeInt(u32, frame[1..5], std.math.cast(u32, key.len) orelse
                    return error.ExceededMemoryBudget, .little);
            }
            break :blk try self.strings.?.putParts(self.allocator, frame[0..if (present) 5 else 1], key);
        } else try self.strings.?.put(self.allocator, key);
        if (self.compact.stride == 0) for (agg_calls) |call| {
            self.accumulators.appendAssumeCapacity(if (call.func == .count_distinct)
                .{ .distinct = self.freshDistinct() }
            else if (call.result == .bytes and call.func == .min)
                .{ .min_arena = .{ .store = self.strings.?, .key = ref } }
            else if (call.result == .bytes and call.func == .max)
                .{ .max_arena = .{ .store = self.strings.?, .key = ref } }
            else
                Accumulator.init(call));
        };
        const id: u32 = @intCast(n);
        var at = @as(usize, @as(u32, @truncate(hash))) & (self.fixedSlotCount() - 1);
        while (self.fixedSlot(at).id != empty_id) at = (at + 1) & (self.fixedSlotCount() - 1);
        self.fixedSlot(at).* = .{ .hash = @truncate(hash), .id = id };
        self.refs.appendAssumeCapacity(ref);
        self.first.appendAssumeCapacity(pos);
        if (raw_string) self.string_views.appendAssumeCapacity(.{ .bytes = self.strings.?.get(ref), .first = pos });
        if (self.compact.stride != 0) self.compact.append();
        self.discovered(pos);
        return id;
    }

    /// Owning partition lock required: slots/views can grow until publication. Separate passes overlap the
    /// slot -> group view -> arena dependency across rows without treating a hash as identity.
    pub fn prefetchStringSlot(self: *const GroupTable, hash: u64) void {
        if (self.fixedSlotCount() == 0) return;
        const at = @as(usize, @as(u32, @truncate(hash))) & (self.fixedSlotCount() - 1);
        @prefetch(self.fixedSlot(at), .{ .rw = .read, .locality = 2 });
    }

    pub fn prefetchStringKey(self: *const GroupTable, hash: u64) void {
        if (self.fixedSlotCount() == 0 or self.string_views.items.len == 0) return;
        const at = @as(usize, @as(u32, @truncate(hash))) & (self.fixedSlotCount() - 1);
        const slot = self.fixedSlot(at).*;
        if (slot.id == empty_id or slot.hash != @as(u32, @truncate(hash))) return;
        const view = &self.string_views.items[slot.id];
        @prefetch(view, .{ .rw = .read, .locality = 2 });
        const bytes = view.bytes;
        if (bytes.len != 0) {
            @prefetch(bytes.ptr, .{ .rw = .read, .locality = 2 });
            @prefetch(bytes.ptr + bytes.len - 1, .{ .rw = .read, .locality = 2 });
        }
    }

    fn probeString(self: *const GroupTable, key: []const u8, present: bool, full_hash: u64) FixedProbe {
        if (self.fixedSlotCount() == 0) return .{ .id = null, .slot = 0 };
        const hash: u32 = @truncate(full_hash);
        const mask = self.fixedSlotCount() - 1;
        var i = @as(usize, hash) & mask;
        while (true) : (i = (i + 1) & mask) {
            const slot = self.fixedSlot(i).*;
            if (slot.id == empty_id) return .{ .id = null, .slot = i };
            if (slot.hash != hash) continue;
            const stored = self.string_views.items[slot.id].bytes;
            if (present) {
                if (stored.len == key.len +| 5 and stored[0] == 1 and keyEql(stored[5..], key))
                    return .{ .id = slot.id, .slot = i };
            } else if (stored.len == 1 and stored[0] == 0) return .{ .id = slot.id, .slot = i };
        }
    }

    fn stringReserve(ctx: *anyopaque, n: usize) Error!void {
        const self: *GroupTable = @ptrCast(@alignCast(ctx));
        try self.admit(n);
    }

    fn stringSettle(ctx: *anyopaque) void {
        const self: *GroupTable = @ptrCast(@alignCast(ctx));
        self.allocated_bytes = self.variableHeld();
    }

    fn variableHeld(self: *const GroupTable) usize {
        return (if (self.strings) |strings| strings.held() + @sizeOf(KeyArena) else 0) + self.fixedSlotCount() * @sizeOf(FixedSlot) +
            self.refs.capacity * @sizeOf(u32) + self.first.capacity * @sizeOf(u64) +
            self.string_views.capacity * @sizeOf(StringView) + self.accumulators.capacity * @sizeOf(Accumulator) +
            self.compact.held() + self.set_objects;
    }

    fn probeVariable(self: *const GroupTable, key: []const u8, full_hash: u64) FixedProbe {
        if (self.fixedSlotCount() == 0) return .{ .id = null, .slot = 0 };
        const hash: u32 = @truncate(full_hash);
        const mask = self.fixedSlotCount() - 1;
        var i = @as(usize, hash) & mask;
        while (true) : (i = (i + 1) & mask) {
            const slot = self.fixedSlot(i).*;
            if (slot.id == empty_id) return .{ .id = null, .slot = i };
            if (slot.hash == hash and keyEql(self.keyAt(slot.id), key)) return .{ .id = slot.id, .slot = i };
        }
    }

    /// `key` is the fixed image (`fixed_stride` bytes). Copied in; the caller may reuse its buffer.
    pub fn getOrInsertFixed(self: *GroupTable, key: []const u8, agg_calls: []const AggCall, pos: u64) !u32 {
        return self.getOrInsertFixedHashed(key, agg_calls, pos, hashFixed(key));
    }

    pub fn getOrInsertFixedHashed(
        self: *GroupTable,
        key: []const u8,
        agg_calls: []const AggCall,
        pos: u64,
        full_hash: u64,
    ) !u32 {
        std.debug.assert(key.len == self.fixed_stride);
        if (self.discoveryDone(pos)) return CountSketch.sinkId(0);
        if (self.sketch) |s| if (try s.route(full_hash, pos, key)) |id| return id;
        const hash: u32 = @truncate(full_hash);
        const probe = self.probeFixed(key, full_hash);
        if (probe.id) |id| {
            self.first.items[id] = @min(self.first.items[id], pos);
            return id;
        }
        if (self.fixed_len == std.math.maxInt(u32)) return error.OutOfMemory;
        // A failed growth has already replaced a buffer; charge what is still held so the shared budget is not told
        // the old, smaller size.
        errdefer self.allocated_bytes = self.fixedHeld();

        const stride: usize = self.fixed_stride;
        const groups_now: usize = self.fixed_len;
        const old_slots = self.fixedSlotCount();
        const grow_slots = old_slots == 0 or (groups_now + 1) * 4 > old_slots * 3;
        const slot_cap: usize = if (grow_slots) @max(old_slots * 2, 16) else old_slots;
        const key_need = (groups_now + 1) * stride;
        const key_cap: usize = if (key_need > self.fixed_keys.len)
            @max(key_need, self.fixed_keys.len * 2)
        else
            self.fixed_keys.len;
        const acc_need = self.accumulators.items.len + if (self.compact.stride != 0) @as(usize, 0) else agg_calls.len;
        const acc_cap = if (self.accumulators.capacity >= acc_need)
            self.accumulators.capacity
        else
            std.ArrayList(Accumulator).growCapacity(acc_need);
        const first_need = self.first.items.len + 1;
        const first_cap = if (self.first.capacity >= first_need)
            self.first.capacity
        else
            std.ArrayList(u64).growCapacity(first_need);
        // `set_objects` is already in `allocated_bytes`. Leave it out and the reset to `fixedHeld` steps past the
        // ceiling this admit just accepted.
        const compact_cap = if (self.compact.stride != 0) self.compact.projected(groups_now + 1) else 0;
        const projected = key_cap + slot_cap * @sizeOf(FixedSlot) +
            first_cap * @sizeOf(u64) + acc_cap * @sizeOf(Accumulator) +
            compact_cap + self.set_objects;
        if (projected > self.allocated_bytes) try self.admit(projected - self.allocated_bytes);

        if (grow_slots) {
            @branchHint(.cold);
            try self.growFixedSlots(slot_cap, projected);
        }
        if (key_cap > self.fixed_keys.len or acc_cap > self.accumulators.capacity or
            first_cap > self.first.capacity or compact_cap > self.compact.held())
        {
            @branchHint(.cold);
            if (key_cap > self.fixed_keys.len) if (core_memory.remapWithoutCopy(self.allocator, self.fixed_keys, key_cap)) |keys| {
                self.fixed_keys = keys;
            };
            tryRemapList(Accumulator, self.bufferAllocator(), &self.accumulators, acc_cap);
            tryRemapList(u64, self.allocator, &self.first, first_cap);
            if (self.compact.stride != 0)
                tryRemapList(u64, self.bufferAllocator(), &self.compact.words, compact_cap / @sizeOf(u64));
            var overlap: usize = if (key_cap != self.fixed_keys.len) self.fixed_keys.len else 0;
            if (acc_cap > self.accumulators.capacity)
                overlap = @max(overlap, self.accumulators.capacity * @sizeOf(Accumulator));
            if (first_cap > self.first.capacity) overlap = @max(overlap, self.first.capacity * @sizeOf(u64));
            if (compact_cap > self.compact.held()) overlap = @max(overlap, self.compact.held());
            const extra = try self.reserveOverlap(projected, overlap);
            defer if (extra != 0) self.shared.?.release(extra);
            if (key_cap != self.fixed_keys.len) {
                const grown = try self.allocator.alloc(u8, key_cap);
                @memcpy(grown[0..self.fixed_keys.len], self.fixed_keys);
                if (self.fixed_keys.len != 0) self.allocator.free(self.fixed_keys);
                self.fixed_keys = grown;
            }
            if (self.compact.stride != 0) try self.compact.reserve(self.bufferAllocator(), groups_now + 1);
            try self.accumulators.ensureTotalCapacity(self.bufferAllocator(), acc_need);
            try self.first.ensureTotalCapacity(self.allocator, first_need);
        }

        // The group's state first, its count(DISTINCT) sets charged to the shared budget as the byte table's are: a
        // failure here leaves the table as it was.
        const acc_start = self.accumulators.items.len;
        errdefer {
            for (self.accumulators.items[acc_start..]) |*acc| {
                if (acc.* == .distinct) acc.distinct.deinit(self.allocator);
            }
            self.accumulators.shrinkRetainingCapacity(acc_start);
        }
        if (self.compact.stride == 0) for (agg_calls) |call| {
            var acc = Accumulator.init(call);
            if (call.func == .count_distinct) acc = .{ .distinct = self.freshDistinct() };
            self.accumulators.appendAssumeCapacity(acc);
        };

        const id: u32 = self.fixed_len;
        @memcpy(self.fixed_keys[@as(usize, id) * stride ..][0..stride], key);
        var i = probe.slot;
        if (grow_slots) {
            i = @as(usize, hash) & (self.fixedSlotCount() - 1);
            while (self.fixedSlot(i).id != empty_id) i = (i + 1) & (self.fixedSlotCount() - 1);
        }
        self.fixedSlot(i).* = .{ .hash = hash, .id = id };
        if (self.compact.stride != 0) self.compact.append();
        self.first.appendAssumeCapacity(pos);
        self.fixed_len = id + 1;
        self.allocated_bytes = self.fixedHeld();
        self.discovered(pos);
        return id;
    }

    /// Grow an empty fixed-width table to hold `groups` groups of `n_calls` accumulators without rehashing, charged
    /// as `getOrInsertFixed` charges its growth. A table that already has groups is left as it is.
    pub fn reserveFixed(self: *GroupTable, groups: usize, n_calls: usize) !void {
        std.debug.assert(self.fixed_stride != 0);
        if (self.fixed_len != 0 or groups == 0) return;
        // The load factor `getOrInsertFixed` keeps: more than 3/4 full grows.
        const slot_cap = std.math.ceilPowerOfTwo(usize, @max(16, groups * 4 / 3 + 1)) catch return error.OutOfMemory;
        if (slot_cap <= self.fixedSlotCount()) return;
        const key_cap = groups * self.fixed_stride;
        const projected = key_cap + slot_cap * @sizeOf(FixedSlot) + groups * @sizeOf(u64) +
            (if (self.compact.stride == 0)
                groups * n_calls * @sizeOf(Accumulator)
            else
                self.compact.projected(groups)) + self.set_objects;
        errdefer self.allocated_bytes = self.fixedHeld();
        if (projected > self.allocated_bytes) try self.admit(projected - self.allocated_bytes);
        try self.growFixedSlots(slot_cap, projected);
        if (key_cap > self.fixed_keys.len) if (core_memory.remapWithoutCopy(self.allocator, self.fixed_keys, key_cap)) |keys| {
            self.fixed_keys = keys;
        };
        tryRemapList(u64, self.allocator, &self.first, groups);
        if (self.compact.stride != 0)
            tryRemapList(u64, self.bufferAllocator(), &self.compact.words, self.compact.projected(groups) / @sizeOf(u64))
        else
            tryRemapList(Accumulator, self.bufferAllocator(), &self.accumulators, groups * n_calls);
        var overlap: usize = if (key_cap > self.fixed_keys.len) self.fixed_keys.len else 0;
        if (groups > self.first.capacity) overlap = @max(overlap, self.first.capacity * @sizeOf(u64));
        if (self.compact.stride != 0) {
            if (self.compact.projected(groups) > self.compact.held()) overlap = @max(overlap, self.compact.held());
        } else if (groups * n_calls > self.accumulators.capacity) {
            overlap = @max(overlap, self.accumulators.capacity * @sizeOf(Accumulator));
        }
        const extra = try self.reserveOverlap(projected, overlap);
        defer if (extra != 0) self.shared.?.release(extra);
        if (key_cap > self.fixed_keys.len) {
            const keys = try self.allocator.alloc(u8, key_cap);
            if (self.fixed_keys.len != 0) self.allocator.free(self.fixed_keys);
            self.fixed_keys = keys;
        }
        if (self.compact.stride != 0)
            try self.compact.reserve(self.bufferAllocator(), groups)
        else
            try self.accumulators.ensureTotalCapacityPrecise(self.bufferAllocator(), groups * n_calls);
        try self.first.ensureTotalCapacityPrecise(self.allocator, groups);
        self.allocated_bytes = self.fixedHeld();
    }

    fn fixedHeld(self: *const GroupTable) usize {
        return self.fixed_keys.len + self.fixedSlotCount() * @sizeOf(FixedSlot) +
            self.first.capacity * @sizeOf(u64) + self.accumulators.capacity * @sizeOf(Accumulator) +
            self.compact.held() + self.set_objects;
    }

    fn findFixed(self: *const GroupTable, key: []const u8, hash: u64) ?u32 {
        return self.probeFixed(key, hash).id;
    }

    const FixedProbe = struct { id: ?u32, slot: usize };

    pub inline fn fixedSlotCount(self: *const GroupTable) usize {
        return self.fixed_capacity;
    }

    inline fn firstSlots(self: *const GroupTable) []FixedSlot {
        return if (self.fixed_stride == 0) self.slots else self.fixed_slots;
    }

    pub inline fn fixedSlot(self: *const GroupTable, index: usize) *FixedSlot {
        const first_slots = self.firstSlots();
        const first = first_slots.len;
        if (index < first) return &first_slots[index];
        const half = self.fixed_capacity / 2;
        if (index >= half) return &self.fixed_extra[self.fixed_extra_count - 1].?[index - half];
        const high = std.math.log2_int(usize, index);
        const part = high - self.fixed_base_shift;
        const base = @as(usize, 1) << @as(std.math.Log2Int(usize), @intCast(high));
        return &self.fixed_extra[part].?[index - base];
    }

    fn clearFixedSlots(self: *GroupTable) void {
        const first_slots = self.firstSlots();
        @memset(first_slots, .{ .hash = 0, .id = empty_id });
        for (0..self.fixed_extra_count) |i| {
            const len = first_slots.len << @as(std.math.Log2Int(usize), @intCast(i));
            @memset(self.fixed_extra[i].?[0..len], .{ .hash = 0, .id = empty_id });
        }
    }

    fn growFixedSlots(self: *GroupTable, target: usize, projected: usize) !void {
        return self.growFixedSlotsImpl(target, projected, false);
    }

    fn growFixedSlotsImpl(self: *GroupTable, target: usize, projected: usize, force_bits: bool) !void {
        if (self.firstSlots().len == 0) {
            const first = try self.allocator.alloc(FixedSlot, target);
            if (self.fixed_stride == 0) self.slots = first else self.fixed_slots = first;
            self.fixed_capacity = target;
            self.fixed_base_shift = @intCast(std.math.log2_int(usize, target));
            self.clearFixedSlots();
            return;
        }
        // Keep the common lookup path contiguous when the ledger can fund both tables. If that overlap does not
        // fit, retain the old slots and grow in segments instead, preserving the final-capacity fit behavior.
        if (!force_bits and self.fixed_extra_count == 0) {
            const old = self.firstSlots();
            const overlap = old.len * @sizeOf(FixedSlot);
            const slack = self.max_memory_bytes -| @max(projected, self.allocated_bytes);
            const need = overlap -| slack;
            const credit: ?usize = if (need == 0) 0 else if (self.shared) |b| b.drawExact(need) else null;
            if (credit) |held| {
                defer if (held != 0) self.shared.?.release(held);
                const next = try self.allocator.alloc(FixedSlot, target);
                @memset(next, .{ .hash = 0, .id = empty_id });
                for (old) |entry| if (entry.id != empty_id) {
                    var at = @as(usize, entry.hash) & (target - 1);
                    while (next[at].id != empty_id) at = (at + 1) & (target - 1);
                    next[at] = entry;
                };
                self.allocator.free(old);
                if (self.fixed_stride == 0) self.slots = next else self.fixed_slots = next;
                self.fixed_capacity = target;
                self.fixed_base_shift = @intCast(std.math.log2_int(usize, target));
                return;
            }
        }
        while (self.fixedSlotCount() < target) {
            const old = self.fixedSlotCount();
            if (self.fixed_extra_count == self.fixed_extra.len) return error.ExceededMemoryBudget;
            // For the rare table using the top id bit, keep the migration marks separately and fund them first.
            const needs_bits = force_bits or self.groupCount() >= 0x80000000;
            const bit_bytes = if (needs_bits) old / 8 else 0;
            const slack = self.max_memory_bytes -| @max(projected, self.allocated_bytes);
            const need = bit_bytes -| slack;
            const extra_credit = if (need == 0) 0 else if (self.shared) |b|
                b.drawExact(need) orelse return error.ExceededMemoryBudget
            else
                return error.ExceededMemoryBudget;
            defer if (extra_credit != 0) self.shared.?.release(extra_credit);
            const bits: []u8 = if (needs_bits) try self.allocator.alloc(u8, bit_bytes) else &.{};
            defer if (needs_bits) self.allocator.free(bits);
            if (needs_bits) {
                @memset(bits, 0);
                for (0..old) |i| if (self.fixedSlot(i).id != empty_id) {
                    bits[i / 8] |= @as(u8, 1) << @intCast(i % 8);
                };
            }
            const extra = try self.allocator.alloc(FixedSlot, old);
            @memset(extra, .{ .hash = 0, .id = empty_id });
            self.fixed_extra[self.fixed_extra_count] = extra.ptr;
            self.fixed_extra_count += 1;
            self.fixed_capacity = old * 2;
            const mask = self.fixedSlotCount() - 1;
            const migrated: u32 = 0x80000000;
            for (0..old) |i| {
                const source = self.fixedSlot(i);
                if (!fixedUnmigrated(source.*, i, old, bits)) continue;
                var entry = source.*;
                source.* = .{ .hash = 0, .id = empty_id };
                if (needs_bits) bits[i / 8] &= ~(@as(u8, 1) << @intCast(i % 8));
                while (true) {
                    var at = @as(usize, entry.hash) & mask;
                    while (true) : (at = (at + 1) & mask) {
                        const dest = self.fixedSlot(at);
                        if (dest.id == empty_id) {
                            dest.* = entry;
                            if (!needs_bits) dest.id |= migrated;
                            break;
                        }
                        if (fixedUnmigrated(dest.*, at, old, bits)) {
                            const displaced = dest.*;
                            dest.* = entry;
                            if (!needs_bits) dest.id |= migrated else bits[at / 8] &= ~(@as(u8, 1) << @intCast(at % 8));
                            entry = displaced;
                            break;
                        }
                    }
                    if (self.fixedSlot(at).id == (if (needs_bits) entry.id else entry.id | migrated)) break;
                }
            }
            if (!needs_bits) for (0..self.fixedSlotCount()) |i| {
                const slot = self.fixedSlot(i);
                if (slot.id != empty_id) slot.id &= ~migrated;
            };
        }
    }

    fn fixedUnmigrated(slot: FixedSlot, index: usize, old: usize, bits: []const u8) bool {
        if (index >= old or slot.id == empty_id) return false;
        return if (bits.len == 0)
            slot.id & 0x80000000 == 0
        else
            bits[index / 8] & (@as(u8, 1) << @intCast(index % 8)) != 0;
    }

    inline fn probeFixed(self: *const GroupTable, key: []const u8, full_hash: u64) FixedProbe {
        const count = self.fixedSlotCount();
        if (count == 0) return .{ .id = null, .slot = 0 };
        const mask = count - 1;
        const stride: usize = self.fixed_stride;
        const hash: u32 = @truncate(full_hash);
        var i = @as(usize, hash) & mask;
        var n: usize = 0;
        if (self.fixed_extra_count == 0) {
            @branchHint(.likely);
            const slots = self.fixed_slots;
            while (n < count) : (n += 1) {
                const slot = slots[i];
                if (slot.id == empty_id) return .{ .id = null, .slot = i };
                if (slot.hash == hash and keyEql(self.fixed_keys[@as(usize, slot.id) * stride ..][0..stride], key))
                    return .{ .id = slot.id, .slot = i };
                i = (i + 1) & mask;
            }
            unreachable;
        }
        while (n < count) : (n += 1) {
            const slot = self.fixedSlot(i).*;
            if (slot.id == empty_id) return .{ .id = null, .slot = i };
            if (slot.hash == hash and keyEql(self.fixed_keys[@as(usize, slot.id) * stride ..][0..stride], key))
                return .{ .id = slot.id, .slot = i };
            i = (i + 1) & mask;
        }
        unreachable; // Growth keeps at least one slot empty.
    }

    /// Local fixed tables keep their buffers and their charge across flushes. No key id survives a flush.
    pub fn clearFixedRetainingCapacity(self: *GroupTable) void {
        std.debug.assert(self.fixed_stride != 0);
        for (self.accumulators.items) |*acc| switch (acc.*) {
            .min_bytes, .max_bytes => |s| if (s) |b| self.allocator.free(b),
            .distinct => |*d| d.deinit(self.allocator),
            else => {},
        };
        self.accumulators.clearRetainingCapacity();
        self.first.clearRetainingCapacity();
        self.compact.words.clearRetainingCapacity();
        self.fixed_len = 0;
        self.clearFixedSlots();
        self.allocated_bytes = self.fixedHeld();
        self.releaseSlack();
    }

    pub fn clearRetainingCapacity(self: *GroupTable) void {
        if (self.fixed_stride != 0) return self.clearFixedRetainingCapacity();
        self.deinitRange(0, self.groupCount());
        self.accumulators.clearRetainingCapacity();
        self.refs.clearRetainingCapacity();
        self.first.clearRetainingCapacity();
        self.compact.words.clearRetainingCapacity();
        if (self.strings) |strings| strings.reset();
        self.clearFixedSlots();
        self.allocated_bytes = self.variableHeld();
        self.releaseSlack();
    }

    /// Amortized conservative group width for planning. Runtime admission quotes the actual slot/list capacities
    /// and arena chunks before allocating; a spilled distinct set carries its own object charge.
    pub fn variableEntryBytes(key_len: usize, calls: usize) usize {
        if (key_len > std.math.maxInt(usize) - 15) return std.math.maxInt(usize);
        return key_len +| 5 +| 32 +| (@sizeOf(u32) + @sizeOf(u64)) * 3 / 2 +|
            (@sizeOf(Accumulator) *| calls *| 3 / 2);
    }

    /// Whole variable-table quote, including the first arena chunk and worst-case chunk packing.
    pub fn variableTableBytes(groups: usize, key_len: usize, calls: usize) usize {
        if (groups == 0) return 0;
        if (groups >= empty_id or key_len >= std.math.maxInt(u32)) return std.math.maxInt(usize);
        const record = key_len + 5;
        const per_block = @max(1, KeyArena.chunk_size / record);
        const blocks = if (record > KeyArena.chunk_size) groups else (groups + per_block - 1) / per_block;
        const block_bytes = std.mem.alignForward(usize, record, KeyArena.chunk_size);
        const data = blocks *| block_bytes;
        const chunks = data / KeyArena.chunk_size;
        const slots = std.math.ceilPowerOfTwo(usize, @max(16, groups * 4 / 3 + 1)) catch return std.math.maxInt(usize);
        const rows = std.ArrayList(u32).growCapacity(groups);
        const cells = std.ArrayList(Accumulator).growCapacity(groups * calls);
        return @sizeOf(KeyArena) +| data +| 2 *| (blocks *| @sizeOf(KeyArena.Block) +| chunks *| @sizeOf([*]u8)) +|
            slots *| @sizeOf(FixedSlot) +| rows *| (@sizeOf(u32) + @sizeOf(u64)) +| cells *| @sizeOf(Accumulator);
    }

    inline fn variableEntrySize(self: *const GroupTable, key_len: usize, calls: usize) usize {
        if (self.compact.stride == 0) return variableEntryBytes(key_len, calls);
        return variableEntryBytes(key_len, 0) + self.compact.projected(self.groupCount() + 1) - self.compact.held();
    }

    fn admit(self: *GroupTable, entry_size: usize) !void {
        if (self.allocated_bytes + entry_size <= self.max_memory_bytes) return;
        const budget = self.shared orelse return error.ExceededMemoryBudget;
        if (budget.tight()) self.releaseSlack();
        const deficit = (self.allocated_bytes + entry_size) - self.max_memory_bytes;
        const got = budget.draw(deficit) orelse blk: {
            self.releaseSlack();
            break :blk budget.draw(entry_size) orelse return error.ExceededMemoryBudget;
        };
        self.max_memory_bytes += got;
    }

    fn reserveOverlap(self: *GroupTable, projected: usize, overlap: usize) error{ExceededMemoryBudget}!usize {
        const slack = self.max_memory_bytes -| @max(projected, self.allocated_bytes);
        const need = overlap -| slack;
        if (need == 0) return 0;
        const budget = self.shared orelse return error.ExceededMemoryBudget;
        return budget.drawExact(need) orelse error.ExceededMemoryBudget;
    }

    fn bufferAllocator(self: *GroupTable) std.mem.Allocator {
        if (self.buffer_allocator) |a| return a;
        const limit = if (self.shared) |b| b.limit else self.max_memory_bytes;
        const a = core_memory.groupAllocatorForBudget(self.allocator, limit);
        self.buffer_allocator = a;
        return a;
    }

    fn tryRemapList(comptime T: type, allocator: std.mem.Allocator, list: *std.ArrayList(T), cap: usize) void {
        if (cap <= list.capacity) return;
        const resized = core_memory.remapWithoutCopy(allocator, list.allocatedSlice(), cap) orelse return;
        list.items.ptr = resized.ptr;
        list.capacity = resized.len;
    }
};

test "GroupTable basic grouping, float normalization, and memory capping" {
    const allocator = std.testing.allocator;

    var gt = GroupTable.init(allocator, 1024);
    defer gt.deinit();

    const agg_calls = &[_]AggCall{
        .{
            .func = .sum,
            .alias = "sum_val",
            .result = .i64,
            .arg = null,
            .where = null,
        },
    };

    const gid1 = try gt.getOrInsert("group_a", agg_calls, 0);
    const gid2 = try gt.getOrInsert("group_b", agg_calls, 1);
    const gid1_dup = try gt.getOrInsert("group_a", agg_calls, 0);

    try std.testing.expect(gid1 == 0);
    try std.testing.expect(gid2 == 1);
    try std.testing.expect(gid1_dup == 0);

    var f32_vals_1 = [_]f32{0.0};
    const f32_col_1 = filter_eval.Batch.Column{
        .f32 = .{ .values = &f32_vals_1 },
    };
    var f32_vals_2 = [_]f32{-0.0};
    const f32_col_2 = filter_eval.Batch.Column{
        .f32 = .{ .values = &f32_vals_2 },
    };
    var key_scratch: std.ArrayList(u8) = .empty;
    defer key_scratch.deinit(allocator);

    try serializeRowKey(&key_scratch, allocator, &[_]filter_eval.Batch.Column{f32_col_1}, 0);
    const key1 = try allocator.dupe(u8, key_scratch.items);
    defer allocator.free(key1);

    try serializeRowKey(&key_scratch, allocator, &[_]filter_eval.Batch.Column{f32_col_2}, 0);
    const key2 = try allocator.dupe(u8, key_scratch.items);
    defer allocator.free(key2);

    try std.testing.expectEqualSlices(u8, key1, key2);

    var f32_nan_1 = [_]f32{std.math.nan(f32)};
    const f32_nan_col_1 = filter_eval.Batch.Column{
        .f32 = .{ .values = &f32_nan_1 },
    };
    var f32_nan_2 = [_]f32{std.math.nan(f32)};
    const f32_nan_col_2 = filter_eval.Batch.Column{
        .f32 = .{ .values = &f32_nan_2 },
    };
    try serializeRowKey(&key_scratch, allocator, &[_]filter_eval.Batch.Column{f32_nan_col_1}, 0);
    const nan_key1 = try allocator.dupe(u8, key_scratch.items);
    defer allocator.free(nan_key1);

    try serializeRowKey(&key_scratch, allocator, &[_]filter_eval.Batch.Column{f32_nan_col_2}, 0);
    const nan_key2 = try allocator.dupe(u8, key_scratch.items);
    defer allocator.free(nan_key2);

    try std.testing.expectEqualSlices(u8, nan_key1, nan_key2);

    var bool_true = [_]bool{true};
    const bool_col = filter_eval.Batch.Column{ .boolean = .{ .values = &bool_true } };
    var i64_one = [_]i64{1};
    const i64_col = filter_eval.Batch.Column{ .i64 = .{ .values = &i64_one } };
    try serializeRowKey(&key_scratch, allocator, &[_]filter_eval.Batch.Column{bool_col}, 0);
    const bool_key = try allocator.dupe(u8, key_scratch.items);
    defer allocator.free(bool_key);
    try serializeRowKey(&key_scratch, allocator, &[_]filter_eval.Batch.Column{i64_col}, 0);
    try std.testing.expectEqualSlices(u8, bool_key, key_scratch.items);
    try std.testing.expectEqual(@as(usize, 1 + 8), bool_key.len);

    var small_gt = GroupTable.init(allocator, 10);
    defer small_gt.deinit();
    try std.testing.expectError(error.ExceededMemoryBudget, small_gt.getOrInsert("too_large_key_that_exceeds_budget", agg_calls, 0));
}


test "compact fixed slots compare complete keys under projected-hash collisions and growth" {
    try std.testing.expectEqual(@as(usize, 8), @sizeOf(GroupTable.FixedSlot));
    const calls = [_]AggCall{.{ .func = .count, .arg = null, .where = null, .alias = "c", .result = .i64 }};
    var table = GroupTable.init(std.testing.allocator, 1 << 20);
    table.fixed_stride = 9;
    defer table.deinit();
    for (0..256) |g| {
        var key: [9]u8 = @splat(0);
        key[0] = 1;
        std.mem.writeInt(u64, key[1..9], g, .little);
        const id = try table.getOrInsertFixedHashed(&key, &calls, 1000 + g, @as(u64, g) << 32);
        try std.testing.expectEqual(@as(u32, @intCast(g)), id);
    }
    for (0..256) |g| {
        var key: [9]u8 = @splat(0);
        key[0] = 1;
        std.mem.writeInt(u64, key[1..9], g, .little);
        try std.testing.expectEqual(@as(?u32, @intCast(g)), table.findFixed(&key, @as(u64, g) << 32));
        const id = try table.getOrInsertFixedHashed(&key, &calls, g, 0);
        try std.testing.expectEqual(@as(u32, @intCast(g)), id);
        try std.testing.expectEqual(@as(u64, g), table.first.items[id]);
    }
}

test "fixed local tables retain charged capacity and discard every old id" {
    const arg: expr_ast.Expr = .{ .col_ref = .{ .col_idx = 0, .physical_type = .INT64, .expr_type = .i64 } };
    const calls = [_]AggCall{
        .{ .func = .count, .arg = null, .where = null, .alias = "c", .result = .i64 },
        .{ .func = .sum, .arg = arg, .where = null, .alias = "s", .result = .i64 },
    };
    for ([_]bool{ false, true }) |compact| {
        var budget: SharedBudget = .{ .limit = 1 << 20 };
        var table = GroupTable.init(std.testing.allocator, 0);
        table.shared = &budget;
        table.fixed_stride = 9;
        if (compact) table.compact.configure(&calls);
        defer table.deinit();
        var capacity: usize = 0;
        for (0..4) |epoch| {
            var last: [9]u8 = @splat(0);
            for (0..64) |g| {
                last[0] = 1;
                std.mem.writeInt(u64, last[1..9], epoch * 64 + g, .little);
                const id = try table.getOrInsertFixed(&last, &calls, epoch * 64 + g);
                try std.testing.expectEqual(@as(u32, @intCast(g)), id);
                try std.testing.expectEqual(@as(u64, 0), table.readAcc(id, 0, calls.len).count);
                try std.testing.expectEqual(@as(?i128, null), table.readAcc(id, 1, calls.len).sum_i);
                table.writeAcc(id, 0, calls.len, .{ .count = 1 });
                table.writeAcc(id, 1, calls.len, .{ .sum_i = @intCast(g) });
            }
            if (epoch == 0) capacity = table.allocated_bytes;
            table.clearFixedRetainingCapacity();
            try std.testing.expectEqual(@as(usize, 0), table.groupCount());
            try std.testing.expectEqual(capacity, table.allocated_bytes);
            try std.testing.expectEqual(capacity, budget.used.load(.monotonic));
            try std.testing.expectEqual(@as(?u32, null), table.findFixed(&last, hashFixed(&last)));
        }
    }
}


/// The rows of `ids` a sketched table gave a cell instead of a group (`CountSketch.sinkId`) leave `sel`, so no
/// aggregate folds them; in the count pass each also adds one to its cell.
pub fn sinkSketchRows(table: *const GroupTable, ids: []const u32, sel: *filter_selection.SelectionVector) !void {
    const s = table.sketch orelse return;
    if (s.census) |c| if (c.admitted == null and !c.off) {
        // Count pass: every row holds a slot naming its hash, which it records and adds to its cell.
        defer c.batch.clearRetainingCapacity();
        // An abandoned count(DISTINCT) census needs nothing more from this pass: stop mid row group.
        if (c.distinct) if (c.dropped) |d| if (d.load(.monotonic)) return error.CensusAbandoned;
        for (ids, 0..) |id, r| if (sel.isActive(r)) {
            // Any worker may abandon it meanwhile; a row group is a million rows, too long to finish first.
            if (c.distinct and r % 4096 == 0) if (c.dropped.?.load(.monotonic)) return error.CensusAbandoned;
            const hash = c.batch.items[std.math.maxInt(u32) - id];
            if (!c.off) {
                try c.record(hash);
                if (!c.checked and c.dropped != null and c.recorded >= c.check_after) try c.judge(s.cells);
            }
            s.cells[s.cellOf(hash)] +|= 1;
            sel.set(r, false);
        };
        if (c.dropped) |d| if (d.load(.monotonic)) {
            if (c.distinct) return error.CensusAbandoned;
            if (!c.off) c.stop();
        };
        return;
    };
    const hot_lanes = if (s.hot) |hot| hot.counts.len else 0;
    const floor = CountSketch.sinkId(@intCast(s.cells.len + hot_lanes - 1));
    for (ids, 0..) |id, r| {
        if (id < floor or !sel.isActive(r)) continue;
        if (s.admit == null and (if (s.census) |c| c.off else true)) {
            const lane = std.math.maxInt(u32) - id;
            if (lane >= s.cells.len) s.hot.?.add(lane - s.cells.len) else s.cells[lane] +|= 1;
        }
        sel.set(r, false);
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


test "STRGROUP slots grow without key relocation and local clear reuses arena capacity" {
    const a = std.testing.allocator;
    var table = GroupTable.init(a, 64 << 20);
    defer table.deinit();
    const calls = [_]AggCall{.{ .func = .count, .result = .i64, .arg = null, .where = null, .alias = "c" }};
    var buf: [128]u8 = undefined;
    var first_ptr: ?[*]const u8 = null;
    for (0..20000) |i| {
        const key = try std.fmt.bufPrint(&buf, "long-key-prefix-with-embedded-\x00-byte-{d}", .{i});
        const id = try table.getOrInsert(key, &calls, i + 1);
        try std.testing.expectEqual(@as(u32, @intCast(i)), id);
        if (i == 0) first_ptr = table.keyAt(0).ptr;
        try std.testing.expect(first_ptr.? == table.keyAt(0).ptr);
    }
    for (0..20000) |i| {
        const key = try std.fmt.bufPrint(&buf, "long-key-prefix-with-embedded-\x00-byte-{d}", .{i});
        try std.testing.expectEqual(@as(?u32, @intCast(i)), table.findKey(key));
        try std.testing.expectEqual(@as(u32, @intCast(i)), try table.getOrInsert(key, &calls, 0));
        try std.testing.expectEqual(@as(u64, 0), table.first.items[i]);
    }
    const blocks = table.strings.?.blocks.items.len;
    const slots = table.slots.ptr;
    table.clearRetainingCapacity();
    try std.testing.expectEqual(@as(usize, 0), table.groupCount());
    for (0..1000) |i| {
        const key = try std.fmt.bufPrint(&buf, "reused-{d}", .{i});
        _ = try table.getOrInsert(key, &calls, i);
    }
    try std.testing.expect(slots == table.slots.ptr);
    try std.testing.expectEqual(blocks, table.strings.?.blocks.items.len);
}

test "STRGROUP failed arena inserts and winner allocations preserve ids and ledger cleanup" {
    const Trial = struct {
        fn run(a: std.mem.Allocator) !void {
            var budget: SharedBudget = .{ .limit = 16 << 20 };
            var table = GroupTable.init(a, 0);
            table.shared = &budget;
            defer {
                table.deinit();
                std.debug.assert(budget.used.load(.monotonic) == 0);
            }
            const calls = [_]AggCall{.{ .func = .min, .result = .bytes, .arg = null, .where = null, .alias = "lo" }};
            var buf: [128]u8 = undefined;
            for (0..64) |i| {
                const key = try std.fmt.bufPrint(&buf, "variable-key-{d}", .{i});
                const id = try table.getOrInsert(key, &calls, i);
                try table.accumulators.items[id].min_arena.consider("winner-b", true);
                try table.accumulators.items[id].min_arena.consider("winner-a", true);
                try std.testing.expectEqualStrings("winner-a", table.accumulators.items[id].stringValue().?);
            }
            const before = table.groupCount();
            table.releaseSlack();
            budget.limit = budget.used.load(.monotonic);
            const long = try a.alloc(u8, 131073);
            defer a.free(long);
            @memset(long, 'x');
            try std.testing.expectError(error.ExceededMemoryBudget, table.getOrInsert(long, &calls, 0));
            try std.testing.expectEqual(before, table.groupCount());
            try std.testing.expectEqual(@as(?u32, 0), table.findKey("variable-key-0"));
        }
    };
    try Trial.run(std.testing.allocator);
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Trial.run, .{});
}

test "SharedBudget: tables share one budget instead of owning fixed slices" {
    const allocator = std.testing.allocator;
    const agg_calls = [_]AggCall{.{ .func = .count, .arg = null, .where = null, .alias = "n", .result = .i64 }};

    // Two tables under one budget, as parallel workers are: the only busy one may take the whole budget.
    var budget = SharedBudget{ .limit = 4096 };
    var a = GroupTable.init(allocator, 0);
    a.shared = &budget;
    defer a.deinit();
    var b = GroupTable.init(allocator, 0);
    b.shared = &budget;
    defer b.deinit();

    var key_buf: [16]u8 = undefined;
    var placed: usize = 0;
    while (placed < 500) : (placed += 1) {
        const key = std.fmt.bufPrint(&key_buf, "k{d}", .{placed}) catch unreachable;
        _ = b.getOrInsert(key, &agg_calls, 0) catch break;
    }
    try std.testing.expect(b.refs.items.len > 0);
    try std.testing.expect(budget.used.load(.monotonic) > 4096 / 2);
    try std.testing.expect(a.max_memory_bytes == 0); // the idle table reserved nothing

    var full = SharedBudget{ .limit = 8 };
    var c = GroupTable.init(allocator, 0);
    c.shared = &full;
    defer c.deinit();
    try std.testing.expectError(error.ExceededMemoryBudget, c.getOrInsert("too-big", &agg_calls, 0));

    // Freed tables return their quota: workers merge, then free.
    var recycle = SharedBudget{ .limit = 4096 };
    {
        var tmp = GroupTable.init(allocator, 0);
        tmp.shared = &recycle;
        _ = try tmp.getOrInsert("k", &agg_calls, 0);
        try std.testing.expect(recycle.used.load(.monotonic) > 0);
        tmp.deinit();
    }
    try std.testing.expectEqual(@as(usize, 0), recycle.used.load(.monotonic));
}

test "string min/max by 16-byte prefix agree with a byte-by-byte fold: shared prefixes, zero bytes, nulls" {
    var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_state.deinit();
    const a = arena_state.allocator();
    var prng = std.Random.DefaultPrng.init(0xb17e);
    const r = prng.random();
    const alphabet = [_]u8{ 0, 1, 'a', 'b', 0xff };
    for (0..200) |_| {
        const n = r.uintLessThan(usize, 300) + 1;
        const vals = try a.alloc([]const u8, n);
        const defs = try a.alloc(u32, n);
        for (vals, defs) |*v, *d| {
            const len = r.uintLessThan(usize, 22);
            const buf = try a.alloc(u8, len);
            // A long shared prefix in most rows, so the full compare runs too.
            for (buf, 0..) |*c, k| {
                c.* = if (k < 17 and r.uintLessThan(u8, 6) != 0) 'a' else alphabet[r.uintLessThan(usize, alphabet.len)];
            }
            v.* = buf;
            d.* = @intFromBool(r.uintLessThan(u8, 5) != 0);
        }
        var sel = try filter_selection.SelectionVector.init(a, n);
        for (0..n) |i| if (r.uintLessThan(u8, 3) == 0) sel.set(i, false);
        const col: filter_eval.Batch.Column = .{ .string = .{ .values = vals, .def_levels = defs, .max_def = 1 } };
        inline for (.{ true, false }) |is_min| {
            var want: ?[]const u8 = null;
            for (vals, defs, 0..) |v, d, i| {
                if (!sel.isActive(i) or d != 1) continue;
                const ord = if (want) |w| std.mem.order(u8, v, w) else .lt;
                if (want == null or (is_min and ord == .lt) or (!is_min and ord == .gt)) want = v;
            }
            var state: Accumulator = if (is_min) .{ .min_bytes = null } else .{ .max_bytes = null };
            try foldMinMaxBytes(a, is_min, &state, col, &sel);
            const got = if (is_min) state.min_bytes else state.max_bytes;
            if (want) |w| try testing.expectEqualSlices(u8, w, got.?) else try testing.expect(got == null);
        }
    }
}

/// Both NaN, or the same bits (so -0 and +0 differ).
fn sameF(want: f64, got: f64) bool {
    if (std.math.isNan(want)) return std.math.isNan(got);
    return @as(u64, @bitCast(want)) == @as(u64, @bitCast(got));
}

test "float min/max: NaN above every number, -0 below +0, the same in any order and on every path" {
    const nan = std.math.nan(f64);
    const inf = std.math.inf(f64);
    const cases = [_]struct { vals: []const f64, lo: f64, hi: f64 }{
        .{ .vals = &.{ nan, 1, 3 }, .lo = 1, .hi = nan },
        .{ .vals = &.{ 3, nan, 1 }, .lo = 1, .hi = nan },
        .{ .vals = &.{ nan, nan }, .lo = nan, .hi = nan },
        .{ .vals = &.{ 0.0, -0.0 }, .lo = -0.0, .hi = 0.0 },
        .{ .vals = &.{ -0.0, 0.0 }, .lo = -0.0, .hi = 0.0 },
        .{ .vals = &.{ -inf, inf, nan }, .lo = -inf, .hi = nan },
        .{ .vals = &.{ -inf, -inf }, .lo = -inf, .hi = -inf },
    };
    var prng = std.Random.DefaultPrng.init(0x7a7);
    const r = prng.random();
    for (cases) |c| for (0..20) |_| {
        // Values in a random order (each at least once), past 64 rows so the SIMD lanes run; folded one at a time,
        // through the kernel, and as two halves merged: all give the same answer.
        var vals: [130]f64 = undefined;
        for (&vals) |*v| v.* = c.vals[r.uintLessThan(usize, c.vals.len)];
        for (c.vals, 0..) |v, k| vals[k * 7] = v;
        var sel = try filter_selection.SelectionVector.init(std.testing.allocator, vals.len);
        defer sel.deinit();
        inline for (.{ true, false }) |is_min| {
            const want = if (is_min) c.lo else c.hi;
            var one: ?f64 = null;
            for (vals) |v| foldExtremeF(is_min, &one, v);
            var half_a: ?f64 = null;
            var half_b: ?f64 = null;
            for (vals[0..65]) |v| foldExtremeF(is_min, &half_a, v);
            for (vals[65..]) |v| foldExtremeF(is_min, &half_b, v);
            foldExtremeF(is_min, &half_b, half_a.?);
            for ([_]f64{ one.?, half_b.?, simdMinMaxF64(is_min, &vals, &sel).? }) |got| {
                if (!sameF(want, got)) {
                    std.debug.print("{s} of {any}: want {d}, got {d}\n", .{ if (is_min) "min" else "max", c.vals, want, got });
                    return error.TestUnexpectedResult;
                }
            }
        }
    };
}

test "zero float bounds never prove sign even with known zero NaNs" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    inline for (.{ schema.Type.FLOAT, schema.Type.DOUBLE }) |physical| {
        const T = if (physical == .FLOAT) f32 else f64;
        const U = if (physical == .FLOAT) u32 else u64;
        var meta = try testI64Meta(a, .empty, 100, .REQUIRED);
        meta.schema.items[1].type = physical;
        const arg: expr_ast.Expr = .{ .col_ref = .{ .col_idx = 0, .physical_type = physical, .expr_type = .f64 } };
        const Case = struct { lo: T, hi: T, min_folds: bool, max_folds: bool };
        for ([_]Case{
            .{ .lo = -0.0, .hi = 0.0, .min_folds = false, .max_folds = false },
            .{ .lo = 0.0, .hi = 1.0, .min_folds = false, .max_folds = true },
            .{ .lo = -1.0, .hi = -0.0, .min_folds = true, .max_folds = false },
            .{ .lo = 1.5, .hi = 2.5, .min_folds = true, .max_folds = true },
        }) |c| {
            var lo: [@sizeOf(T)]u8 = undefined;
            var hi: [@sizeOf(T)]u8 = undefined;
            std.mem.writeInt(U, &lo, @bitCast(c.lo), .little);
            std.mem.writeInt(U, &hi, @bitCast(c.hi), .little);
            var path: schema.StringList = .empty;
            try path.append(a, "x");
            var chunks: std.ArrayListUnmanaged(schema.ColumnChunk) = .empty;
            try chunks.append(a, .{
                .file_path = null,
                .file_offset = 0,
                .meta_data = .{
                    .type = physical,
                    .encodings = .empty,
                    .path_in_schema = path,
                    .codec = .UNCOMPRESSED,
                    .num_values = 100,
                    .total_uncompressed_size = 0,
                    .total_compressed_size = 0,
                    .data_page_offset = 0,
                    .index_page_offset = null,
                    .dictionary_page_offset = null,
                    .statistics = .{ .min_value = &lo, .max_value = &hi, .null_count = 0, .nan_count = 0 },
                },
            });
            const rg: schema.RowGroup = .{ .columns = chunks, .total_byte_size = 0, .num_rows = 100 };
            inline for (.{ AggFunc.min, AggFunc.max }) |func| {
                const call: AggCall = .{ .func = func, .arg = arg, .where = null, .alias = "x", .result = .f64 };
                var state = Accumulator.init(call);
                try testing.expectEqual(if (func == .min) c.min_folds else c.max_folds, try updateOneFromStats(&state, call, &rg, &meta));
            }
        }
    }
}

test "fixed-width group keys match the serialized framing and the byte table" {
    const a = std.testing.allocator;
    const calls = [_]AggCall{.{ .func = .count, .arg = null, .where = null, .alias = "c", .result = .i64 }};

    var ints = [_]i64{ 7, -1, 7 };
    var idef = [_]u32{ 1, 1, 0 };
    const icol: filter_eval.Batch.Column = .{ .i64 = .{
        .values = &ints,
        .def_levels = &idef,
        .max_def = 1,
        .has_nulls = true,
    } };
    var other = [_]i64{4};
    const pair = [_]filter_eval.Batch.Column{ icol, .{ .i64 = .{ .values = &other } } };
    var floats = [_]f64{ -0.0, std.math.nan(f64) };
    const fcol: filter_eval.Batch.Column = .{ .f64 = .{ .values = &floats } };

    var image: [32]u8 = undefined;
    var ser: std.ArrayList(u8) = .empty;
    defer ser.deinit(a);
    const samples = [_]struct { cols: []const filter_eval.Batch.Column, row: usize }{
        .{ .cols = &.{icol}, .row = 0 },
        .{ .cols = &.{icol}, .row = 1 },
        .{ .cols = &.{icol}, .row = 2 },
        .{ .cols = &pair, .row = 0 },
        .{ .cols = &.{fcol}, .row = 0 },
        .{ .cols = &.{fcol}, .row = 1 },
    };
    for (samples) |s| {
        const n = packFixedKey(s.cols, s.row, &image).?;
        try serializeRowKey(&ser, a, s.cols, s.row);
        var back: [32]u8 = undefined;
        const expanded = expandFixed(n, ser.items, &back).?;
        try std.testing.expectEqualSlices(u8, image[0..n], expanded);
        var canon_buf: [48]u8 = undefined;
        try std.testing.expectEqualSlices(u8, ser.items, canonicalKey(image[0..n], &canon_buf));
    }

    var fixed = GroupTable.init(a, 1 << 20);
    fixed.fixed_stride = 9;
    defer fixed.deinit();
    var bytes = GroupTable.init(a, 1 << 20);
    defer bytes.deinit();
    for ([_]usize{ 0, 2, 1, 0 }, 0..) |row, nvisit| {
        const n = packFixedKey(&.{icol}, row, &image).?;
        try serializeRowKey(&ser, a, &.{icol}, row);
        // The repeated value is asked from an earlier position the second time, and must keep that one.
        const pos: u64 = if (nvisit == 3) 40 else @intCast(100 - row);
        const fid = try fixed.getOrInsert(ser.items, &calls, pos);
        const bid = try bytes.getOrInsert(ser.items, &calls, pos);
        try std.testing.expectEqual(bid, fid);
        try std.testing.expectEqual(bytes.first.items[bid], fixed.first.items[fid]);
        try std.testing.expectEqual(fid, fixed.findKey(image[0..n]).?);
    }
    try std.testing.expectEqual(@as(usize, 3), fixed.groupCount());
    try std.testing.expectEqual(@as(u64, 40), fixed.first.items[0]);

    var tiny = GroupTable.init(a, 0);
    tiny.fixed_stride = 9;
    defer tiny.deinit();
    try std.testing.expectError(error.ExceededMemoryBudget, tiny.getOrInsertFixed(image[0..9], &calls, 0));
}

test "fixed-width group insert frees every partial allocation" {
    const calls = [_]AggCall{.{ .func = .count, .arg = null, .where = null, .alias = "c", .result = .i64 }};
    const insert = struct {
        fn run(allocator: std.mem.Allocator) !void {
            var t = GroupTable.init(allocator, 1 << 30);
            t.fixed_stride = 9;
            defer t.deinit();
            var i: u64 = 0;
            while (i < 40) : (i += 1) {
                var image: [9]u8 = @splat(0);
                image[0] = 1;
                std.mem.writeInt(u64, image[1..9], i, .little);
                _ = try t.getOrInsertFixed(&image, &calls, i);
                if (i % 3 == 0) _ = try t.getOrInsertFixed(&image, &calls, 1000 + i);
            }
            try std.testing.expectEqual(@as(usize, 40), t.groupCount());
        }
    }.run;
    // In-place resize would make the failure count depend on what else the thread allocated.
    const NoResize = struct {
        child: std.mem.Allocator,
        fn allocator(self: *@This()) std.mem.Allocator {
            return .{ .ptr = self, .vtable = &.{
                .alloc = alloc,
                .resize = std.mem.Allocator.noResize,
                .remap = std.mem.Allocator.noRemap,
                .free = free,
            } };
        }
        fn alloc(ctx: *anyopaque, len: usize, alignment: std.mem.Alignment, ret_addr: usize) ?[*]u8 {
            const self: *@This() = @ptrCast(@alignCast(ctx));
            return self.child.rawAlloc(len, alignment, ret_addr);
        }
        fn free(ctx: *anyopaque, memory: []u8, alignment: std.mem.Alignment, ret_addr: usize) void {
            const self: *@This() = @ptrCast(@alignCast(ctx));
            self.child.rawFree(memory, alignment, ret_addr);
        }
    };
    var fixed: NoResize = .{ .child = std.testing.allocator };
    try std.testing.checkAllAllocationFailures(fixed.allocator(), insert, .{});
}

test "a lone string key reaches the same sketch cell framed or unframed" {
    var prng = std.Random.DefaultPrng.init(0x5ce7c4);
    const rand = prng.random();
    var raw: [200]u8 = undefined;
    var framed: [205]u8 = undefined;
    for (16..raw.len) |len| {
        rand.bytes(raw[0..len]);
        framed[0] = 1;
        std.mem.writeInt(u32, framed[1..5], @intCast(len), .little);
        @memcpy(framed[5..][0..len], raw[0..len]);
        try std.testing.expectEqual(sketchHash(framed[0 .. len + 5]), rawSketchHash(raw[0..len]));
    }
}

test "fixed-width groups charge their count(DISTINCT) sets to the shared budget, and a failed insert leaves nothing" {
    const calls = [_]AggCall{
        .{ .func = .count, .arg = null, .where = null, .alias = "n", .result = .i64 },
        .{ .func = .count_distinct, .arg = null, .where = null, .alias = "u", .result = .i64 },
    };
    var budget: SharedBudget = .{ .limit = 1 << 20 };
    var table = GroupTable.init(std.testing.allocator, 0);
    table.fixed_stride = 9;
    table.shared = &budget;
    defer table.deinit();
    var image: [32]u8 = @splat(0);
    image[0] = 1;
    const g = try table.getOrInsertFixed(image[0..9], &calls, 0);
    const slot = &table.accumulators.items[g * calls.len + 1].distinct;
    try std.testing.expect(slot.heap() == null);
    try std.testing.expect(slot.budgetPtr() == &budget);

    try std.testing.checkAllAllocationFailures(std.testing.allocator, struct {
        fn run(gpa: std.mem.Allocator) !void {
            var b: SharedBudget = .{ .limit = 1 << 20 };
            var t = GroupTable.init(gpa, 0);
            t.fixed_stride = 9;
            t.shared = &b;
            defer t.deinit();
            var img: [32]u8 = @splat(0);
            for (0..3) |i| {
                img[0] = 1;
                img[1] = @intCast(i);
                _ = try t.getOrInsertFixed(img[0..9], &calls, i);
            }
        }
    }.run, .{});
}

test "string min/max folds keep the old winner when copying a new one fails" {
    // The second batch's winner beats the first; its copy is the allocation that fails. The slot must still hold the
    // first winner, live, so freeing the accumulator afterwards frees it exactly once.
    const batches = [2][2][]const u8{ .{ "m", "q" }, .{ "a", "z" } };
    inline for (.{ true, false }) |grouped| inline for (.{ true, false }) |is_min| {
        var failing = std.testing.FailingAllocator.init(std.testing.allocator, .{ .fail_index = 1 });
        const persist = failing.allocator();
        var state: Accumulator = if (is_min) .{ .min_bytes = null } else .{ .max_bytes = null };
        defer if (if (is_min) state.min_bytes else state.max_bytes) |s| persist.free(s);
        for (batches, 0..) |vals, bi| {
            const col: filter_eval.Batch.Column = .{ .string = .{ .values = &vals } };
            const folded = if (grouped)
                foldMinMaxBytesGroupedOne(persist, is_min, &state, col, if (is_min) 0 else 1)
            else blk: {
                var sel = try filter_selection.SelectionVector.init(std.testing.allocator, vals.len);
                defer sel.deinit();
                break :blk foldMinMaxBytes(persist, is_min, &state, col, &sel);
            };
            if (bi == 0) try folded else try std.testing.expectError(error.OutOfMemory, folded);
        }
        const kept = (if (is_min) state.min_bytes else state.max_bytes).?;
        try std.testing.expectEqualStrings(if (is_min) "m" else "q", kept);
    };
}

test "a table charges each group's count(DISTINCT) set object, fixed-width keys or not" {
    const calls = [_]AggCall{
        .{ .func = .count_distinct, .arg = null, .where = null, .alias = "u", .result = .i64 },
        .{ .func = .count, .arg = null, .where = null, .alias = "c", .result = .i64 },
    };
    const n = 500;
    var plain = GroupTable.init(std.testing.allocator, 1 << 30);
    defer plain.deinit();
    var without = GroupTable.init(std.testing.allocator, 1 << 30);
    defer without.deinit();
    var fixed = GroupTable.init(std.testing.allocator, 1 << 30);
    fixed.fixed_stride = 9;
    defer fixed.deinit();
    var fixed_without = GroupTable.init(std.testing.allocator, 1 << 30);
    fixed_without.fixed_stride = 9;
    defer fixed_without.deinit();
    for (0..n) |i| {
        var image: [9]u8 = undefined;
        image[0] = 1;
        std.mem.writeInt(i64, image[1..9], @intCast(i), .little);
        _ = try plain.getOrInsert(&image, &calls, i);
        _ = try without.getOrInsert(&image, calls[1..], i);
        _ = try fixed.getOrInsertFixed(&image, &calls, i);
        _ = try fixed_without.getOrInsertFixed(&image, calls[1..], i);
    }
    // Variable tables quote actual array capacities; no distinct-set object exists before a spill.
    const without_cells = without.accumulators.capacity * @sizeOf(Accumulator);
    const plain_cells = plain.accumulators.capacity * @sizeOf(Accumulator);
    try std.testing.expectEqual(without.allocated_bytes - without_cells + plain_cells, plain.allocated_bytes);
    for ([_]*const GroupTable{ &plain, &fixed }) |t| {
        for (t.accumulators.items) |acc| {
            if (acc == .distinct) try std.testing.expect(acc.distinct.heap() == null);
        }
    }
}

fn compactAllocationTrial(gpa: std.mem.Allocator, fixed: bool) !void {
    // Keep the allocation-failure sweep deterministic: whether an address can grow in place varies across runs.
    // Disabling remap here forces every replacement through the allocating fallback, so every such allocation is
    // still injected and checked; separate tests exercise the page-backed no-copy path.
    const NoRemap = struct {
        child: std.mem.Allocator,
        fn allocator(self: *@This()) std.mem.Allocator {
            return .{ .ptr = self, .vtable = &.{
                .alloc = alloc,
                .resize = std.mem.Allocator.noResize,
                .remap = std.mem.Allocator.noRemap,
                .free = free,
            } };
        }
        fn alloc(ctx: *anyopaque, n: usize, alignment: std.mem.Alignment, ra: usize) ?[*]u8 {
            const self: *@This() = @ptrCast(@alignCast(ctx));
            return self.child.rawAlloc(n, alignment, ra);
        }
        fn free(ctx: *anyopaque, bytes: []u8, alignment: std.mem.Alignment, ra: usize) void {
            const self: *@This() = @ptrCast(@alignCast(ctx));
            self.child.rawFree(bytes, alignment, ra);
        }
    };
    var stable: NoRemap = .{ .child = gpa };
    const arg: expr_ast.Expr = .{ .col_ref = .{ .col_idx = 0, .physical_type = .INT64, .expr_type = .i64 } };
    const calls = [_]AggCall{
        .{ .func = .count, .arg = null, .where = null, .alias = "c", .result = .i64 },
        .{ .func = .sum, .arg = arg, .where = null, .alias = "s", .result = .i64 },
        .{ .func = .avg, .arg = arg, .where = null, .alias = "a", .result = .avg_f64 },
        .{ .func = .min, .arg = arg, .where = null, .alias = "lo", .result = .i64 },
        .{ .func = .max, .arg = arg, .where = null, .alias = "hi", .result = .i64 },
    };
    var budget: SharedBudget = .{ .limit = 1 << 20 };
    var table = GroupTable.init(stable.allocator(), 0);
    table.shared = &budget;
    table.fixed_stride = if (fixed) 9 else 0;
    table.compact.configure(&calls);
    defer table.deinit();
    try std.testing.expectEqual(@as(usize, 56), table.compact.stride * 8);
    for (0..90) |k| {
        var key: [9]u8 = @splat(0);
        key[0] = 1;
        std.mem.writeInt(u64, key[1..9], k, .little);
        const g = try table.getOrInsert(&key, &calls, k);
        try std.testing.expectEqual(@as(?i128, null), table.readAcc(g, 1, calls.len).sum_i);
        table.writeAcc(g, 0, calls.len, .{ .count = 2 });
        table.writeAcc(g, 2, calls.len, .{ .avg = .{ .sum = 0, .isum = -5, .count = 1 } });
        table.writeAcc(g, 3, calls.len, .{ .min_i = -5 });
        table.writeAcc(g, 4, calls.len, .{ .max_i = -5 });
        try std.testing.expectEqual(@as(?i128, -5), table.readAcc(g, 1, calls.len).sum_i);
        try std.testing.expectEqual(@as(?i128, -5), table.compact.integer(g, 1));
        try std.testing.expectEqual(@as(?i128, -5), table.readAcc(g, 3, calls.len).min_i);
        try std.testing.expectEqual(g, try table.getOrInsert(&key, &calls, k + 1));
    }
}

test "compact count and integer tuple allocation failures leave tables owned" {
    try std.testing.checkAllAllocationFailures(std.testing.allocator, compactAllocationTrial, .{false});
    try std.testing.checkAllAllocationFailures(std.testing.allocator, compactAllocationTrial, .{true});
}

test "compact states refuse float tuples and only share identical integer inputs" {
    const arg: expr_ast.Expr = .{ .col_ref = .{ .col_idx = 0, .physical_type = .INT64, .expr_type = .i64 } };
    var calls = [_]AggCall{
        .{ .func = .count, .arg = null, .where = null, .alias = "c", .result = .i64 },
        .{ .func = .sum, .arg = arg, .where = null, .alias = "s", .result = .i64 },
        .{ .func = .avg, .arg = arg, .where = null, .alias = "a", .result = .avg_f64 },
    };
    var state: @import("../agg_state.zig").State = .{};
    state.configure(calls[0..1]);
    try std.testing.expectEqual(@as(usize, 8), state.stride * 8);
    state.configure(calls[0..2]);
    try std.testing.expectEqual(@as(usize, 32), state.stride * 8);
    state.configure(&calls);
    try std.testing.expect(state.fields[1].shared_sum);
    calls[1].offset = 1;
    state.configure(&calls);
    try std.testing.expect(!state.fields[1].shared_sum);
    calls[1].offset = 0;
    calls[2].arg.?.col_ref.col_idx = 1;
    state.configure(&calls);
    try std.testing.expect(!state.fields[1].shared_sum);
    var reordered = [_]AggCall{ calls[1], calls[0], calls[2] };
    reordered[2].arg = arg;
    state.configure(&reordered);
    try std.testing.expect(!state.fields[0].shared_sum);
    calls[2].arg.?.col_ref.expr_type = .f64;
    state.configure(&calls);
    try std.testing.expectEqual(@as(usize, 0), state.stride);
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

test "compact variable insertion keeps retained capacity charged after allocation failure" {
    try std.testing.checkAllAllocationFailures(std.testing.allocator, struct {
        fn run(a: std.mem.Allocator) !void {
            const arg: expr_ast.Expr = .{ .col_ref = .{
                .col_idx = 0,
                .physical_type = .INT64,
                .expr_type = .i64,
            } };
            const calls = [_]AggCall{
                .{ .func = .count, .arg = null, .where = null, .alias = "c", .result = .i64 },
                .{ .func = .sum, .arg = arg, .where = null, .alias = "s", .result = .i64 },
            };
            var budget: SharedBudget = .{ .limit = 1 << 20, .block = 1024 };
            var table = GroupTable.init(a, 0);
            table.shared = &budget;
            table.compact.configure(&calls);
            defer table.deinit();
            _ = table.getOrInsert("key", &calls, 0) catch |err| {
                table.releaseSlack();
                try std.testing.expect(budget.used.load(.monotonic) >= table.compact.held());
                return err;
            };
        }
    }.run, .{});
}

test "boundary: a footer null count outside the row group's rows answers no count from statistics" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var path: schema.StringList = .empty;
    try path.append(a, "x");
    const meta = try testI64Meta(a, .empty, 100, .OPTIONAL);
    var rg: schema.RowGroup = .{ .columns = .empty, .total_byte_size = 800, .num_rows = 100 };
    try rg.columns.append(a, .{ .file_path = null, .file_offset = 0, .meta_data = .{
        .type = .INT64,
        .encodings = .empty,
        .path_in_schema = path,
        .codec = .UNCOMPRESSED,
        .num_values = 100,
        .total_uncompressed_size = 800,
        .total_compressed_size = 800,
        .data_page_offset = 0,
        .index_page_offset = null,
        .dictionary_page_offset = null,
        .statistics = null,
    } });
    const cm = &rg.columns.items[0].meta_data.?;
    const Case = struct { nc: i64, want: ?i64 };
    for ([_]Case{
        .{ .nc = 0, .want = 100 },
        .{ .nc = 100, .want = 0 },
        .{ .nc = 37, .want = 63 },
        .{ .nc = 101, .want = null },
        .{ .nc = -1, .want = null },
        .{ .nc = std.math.minInt(i64), .want = null },
        .{ .nc = std.math.maxInt(i64), .want = null },
    }) |case| {
        cm.statistics = .{ .null_count = case.nc };
        try testing.expectEqual(case.want, statPresentCount(&rg, 0, cm, &meta));
    }
}

test "regression: a one-row min/max of a UINT64 column keeps values of 2^63 and up as the column's bits" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const arg: expr_ast.Expr = .{ .col_ref = .{ .col_idx = 0, .physical_type = .INT64, .expr_type = .i64, .unsigned_64 = true } };
    for ([_]AggFunc{ .min, .max }) |func| {
        const call: AggCall = .{ .func = func, .arg = arg, .where = null, .alias = "m", .result = .i64 };
        var state = Accumulator.init(call);
        if (func == .min) state.min_i = std.math.maxInt(u64) else state.max_i = std.math.maxInt(u64);
        const cols = try finalize(a, call, state);
        try std.testing.expectEqual(@as(i64, -1), cols[0].col.i64.values[0]);
    }
}

test "regression: a count(DISTINCT) merge sizes its tables for the result, so none grows while hash-ordered values arrive" {
    // F3: one worker's set merged into another's slot by slot (hash order) while the destination grew as values came
    // took 40 s at -j 3 against 0.4 s presized. A table that grows mid-merge allocates once per doubling; sized
    // first, each destination table allocates at most once. That allocation count is what this pins.
    const gpa = std.testing.allocator;
    const n: u64 = 60_000;
    const Dest = enum { empty, inline_two, small_map, partitioned };
    inline for (.{ u64, i128 }) |T| for ([_]bool{ false, true }) |other_parted| for (std.enums.values(Dest)) |dest_shape| {
        var other: DistinctSet = .{};
        defer other.deinit(gpa);
        if (other_parted) try other.initPartitions(gpa, null);
        for (0..n) |i| {
            const v: T = @intCast(i * 7919);
            if (T == u64) try other.insertU64(gpa, v) else try other.insertI128(gpa, v);
        }
        var dest: DistinctSet = .{};
        if (dest_shape == .partitioned) try dest.initPartitions(gpa, null);
        // Values both sets hold (the first few multiples), and some only the destination does (odd ones).
        const own: usize = switch (dest_shape) {
            .empty => 0,
            .inline_two => 2,
            .small_map, .partitioned => 50,
        };
        for (0..own) |i| {
            const v: T = @intCast(if (i % 2 == 0) i * 7919 else i * 7919 + 1);
            if (T == u64) try dest.insertU64(gpa, v) else try dest.insertI128(gpa, v);
        }
        const before = dest.count();
        var counting = std.testing.FailingAllocator.init(gpa, .{});
        try dest.merge(&other, counting.allocator());
        try std.testing.expectEqual(@as(usize, n) + before - (own + 1) / 2, dest.count());
        const tables: usize = if (dest_shape == .partitioned) NUMERIC_PARTS else 1;
        std.testing.expect(counting.allocations <= tables) catch |err| {
            std.debug.print("{s} {s} other_parted={}: {d} allocations\n", .{ @typeName(T), @tagName(dest_shape), other_parted, counting.allocations });
            return err;
        };
        dest.deinit(gpa);
    };
}

fn compactBoundsTrial(a: std.mem.Allocator) !void {
    var bounds: Census = .{ .gpa = a, .item_rows = &.{} };
    defer bounds.deinit();
    // Different hashes in ignored bits deliberately collide; each occurrence must survive in the complete class.
    const h: u64 = 0x37ab_1234_5678_0000;
    for (0..1000) |i| try bounds.record(h | @as(u64, @intCast(i)));
    const part: usize = @intCast(bounds.fingerprint(h) >> 40);
    try std.testing.expectEqual(@as(usize, 1000), bounds.partLen(part));
    try std.testing.expectEqual(bounds.fingerprint(h), bounds.fingerprint(h ^ (@as(u64, 255) << 48)));
    for (bounds.full[part].items) |chunk| for (chunk) |fragment| {
        try std.testing.expectEqual(@as(u32, @truncate(bounds.fingerprint(h))), fragment);
    };
    for (bounds.open[part].?[0..bounds.fill[part]]) |fragment| {
        try std.testing.expectEqual(@as(u32, @truncate(bounds.fingerprint(h))), fragment);
    }
    for (0..Census.N_PARTS) |p| try bounds.record(@as(u64, @intCast(p)) << 56);
    try std.testing.expectEqual(@as(u64, 1000 + Census.N_PARTS), bounds.recorded);
}

test "hash identity bounds use four-byte rows and bounded frontiers and free all failed allocations" {
    try std.testing.expectEqual(@as(usize, 512), @sizeOf(Census.Chunk));
    try std.testing.expectEqual(@as(usize, 4), @sizeOf(@TypeOf(@as(Census.Chunk, undefined)[0])));
    try std.testing.checkAllAllocationFailures(std.testing.allocator, compactBoundsTrial, .{});
}

test "bounded first discovery keeps at most H exact keys and only lowers the shared ordinal" {
    const calls = [_]AggCall{.{ .func = .count, .arg = null, .where = null, .alias = "c", .result = .i64 }};
    for ([_]bool{ false, true }) |fixed| {
        var threshold: std.atomic.Value(u64) = .init(std.math.maxInt(u64));
        var early = GroupTable.init(std.testing.allocator, 1 << 20);
        defer early.deinit();
        var late = GroupTable.init(std.testing.allocator, 1 << 20);
        defer late.deinit();
        early.fixed_stride = if (fixed) 9 else 0;
        late.fixed_stride = early.fixed_stride;
        early.discovery = .{ .limit = 3, .threshold = &threshold };
        late.discovery = early.discovery;
        try std.testing.expect(early.countOnly());
        for (0..3) |i| {
            var key: [9]u8 = @splat(0);
            key[0] = 1;
            std.mem.writeInt(u64, key[1..9], i, .little);
            _ = try late.getOrInsert(&key, &calls, 100 + i);
        }
        try std.testing.expectEqual(@as(u64, 102), threshold.load(.monotonic));
        for (0..3) |i| {
            var key: [9]u8 = @splat(0);
            key[0] = 1;
            std.mem.writeInt(u64, key[1..9], i, .little);
            _ = try early.getOrInsert(&key, &calls, 1 + i);
        }
        try std.testing.expectEqual(@as(u64, 3), threshold.load(.monotonic));
        for (3..1000) |i| {
            var key: [9]u8 = @splat(0);
            key[0] = 1;
            std.mem.writeInt(u64, key[1..9], i, .little);
            try std.testing.expectEqual(CountSketch.sinkId(0), try early.getOrInsert(&key, &calls, i + 1));
            try std.testing.expectEqual(CountSketch.sinkId(0), try late.getOrInsert(&key, &calls, i + 100));
        }
        try std.testing.expectEqual(@as(usize, 3), early.groupCount());
        try std.testing.expectEqual(@as(usize, 3), late.groupCount());
        try std.testing.expectEqual(@as(u64, 3), threshold.load(.monotonic));
    }
}

test "distinct bitmaps retain the hash reservation so a late outlier has identical budget admission" {
    const hw = @import("../../hw.zig");
    defer hw.test_cpu = null;
    for ([_]usize{ 144, 256, 512, 1024, 4096, 8192 }) |limit| {
        var outcomes: [2]bool = undefined;
        for ([_]usize{ 1, 64 << 10 }, 0..) |cache, mode| {
            hw.test_cpu = .{ .l2 = .{ .bytes = cache, .max_sharing = 1 } };
            var budget: SharedBudget = .{ .limit = limit };
            {
                var set: DistinctSet = .{ .shared = &budget };
                defer set.deinit(testing.allocator);
                outcomes[mode] = true;
                for (0..129) |i| {
                    const v: u64 = if (i == 128) 1 << 48 else i;
                    set.insertU64(testing.allocator, v) catch |e| {
                        try testing.expectEqual(error.ExceededMemoryBudget, e);
                        outcomes[mode] = false;
                        break;
                    };
                }
            }
            try testing.expectEqual(@as(usize, 0), budget.used.load(.monotonic));
        }
        try testing.expectEqual(outcomes[0], outcomes[1]);
    }
}

test "shared integer scalar folds preserve exact lanes, NULLs, selections and refusal before mutation" {
    try std.testing.checkAllAllocationFailures(std.testing.allocator, testSharedInteger, .{});
}

fn testSharedInteger(a: std.mem.Allocator) !void {
    const n = 193;
    var values: [n]i64 = undefined;
    var narrow: [n]i32 = undefined;
    var defs: [n]u32 = undefined;
    for (&values, &narrow, &defs, 0..) |*v, *v32, *d, r| {
        v.* = switch (r % 5) {
            0 => std.math.minInt(i64),
            1 => std.math.maxInt(i64),
            else => @as(i64, @intCast(r)) - 97,
        };
        v32.* = @intCast(@as(i64, @intCast(r)) - 97);
        d.* = @intFromBool(r % 11 != 0);
    }
    const arg: expr_ast.Expr = .{ .col_ref = .{ .col_idx = 0, .physical_type = .INT64, .expr_type = .i64 } };
    const calls = [_]AggCall{
        .{ .func = .count, .arg = null, .result = .i64, .where = null, .alias = "all" },
        .{ .func = .sum, .arg = arg, .result = .i64, .where = null, .alias = "s", .offset = -17 },
        .{ .func = .min, .arg = arg, .result = .i64, .where = null, .alias = "lo" },
        .{ .func = .max, .arg = arg, .result = .i64, .where = null, .alias = "hi" },
        .{ .func = .avg, .arg = arg, .result = .avg_f64, .where = null, .alias = "a" },
        .{ .func = .count, .arg = arg, .result = .i64, .where = null, .alias = "present" },
        .{ .func = .sum, .arg = arg, .result = .i64, .where = null, .alias = "again", .offset = 31 },
    };
    for ([_]bool{ false, true }) |is_narrow| {
        const col: filter_eval.Batch.Column = if (is_narrow)
            .{ .i32 = .{ .values = &narrow, .def_levels = &defs, .max_def = 1, .has_nulls = true } }
        else
            .{ .i64 = .{ .values = &values, .def_levels = &defs, .max_def = 1, .has_nulls = true } };
        const batch: filter_eval.Batch = .{ .cols = &.{col}, .num_rows = n };
        for (0..4) |mode| {
            var sel = try filter_selection.SelectionVector.init(a, n);
            defer sel.deinit();
            for (0..n) |r| sel.set(r, switch (mode) {
                0 => true,
                1 => false,
                2 => r % 3 != 0,
                else => r % 71 == 0,
            });
            // Exercise every compile-time operation mask and repeat updates from nonempty states.
            for (0..8) |mask| {
                var handled: [calls.len]bool = @splat(false);
                handled[1] = mask & 1 == 0;
                handled[2] = mask & 2 == 0;
                handled[3] = mask & 4 == 0;
                handled[4] = mask & 1 == 0;
                handled[6] = mask & 1 == 0;
                // At least two value calls are needed to justify the common-input kernel.
                if (mask == 0) handled[2] = false;
                var want: [calls.len]Accumulator = undefined;
                var got: [calls.len]Accumulator = undefined;
                for (calls, &want, &got) |call, *w, *g| {
                    w.* = .init(call);
                    g.* = .init(call);
                }
                for (0..2) |_| {
                    for (calls, &want, handled) |call, *w, h| if (!h) {
                        try updateOne(a, a, w, call, &batch, &.{0}, &sel);
                    };
                    try std.testing.expect(try updateSharedInteger(a, &calls, &got, &handled, &batch, &.{0}, &sel));
                }
                for (want, got) |w, g| try std.testing.expectEqualDeep(w, g);
            }
        }
    }
    var sel = try filter_selection.SelectionVector.init(a, n);
    defer sel.deinit();
    var states: [calls.len]Accumulator = undefined;
    for (calls, &states) |call, *state| state.* = .init(call);
    const before = states;
    var unsupported = calls;
    unsupported[calls.len - 1].arg.?.col_ref.unsigned_64 = true;
    const batch: filter_eval.Batch = .{ .cols = &.{.{ .i64 = .{ .values = &values } }}, .num_rows = n };
    try std.testing.expect(!try updateSharedInteger(
        a,
        &unsupported,
        &states,
        &@as([calls.len]bool, @splat(false)),
        &batch,
        &.{0},
        &sel,
    ));
    try std.testing.expectEqualDeep(before, states);
    unsupported[calls.len - 1].arg.?.col_ref.unsigned_64 = false;
    unsupported[calls.len - 1].arg.?.col_ref.unsigned_32 = true;
    try std.testing.expect(!try updateSharedInteger(
        a,
        &unsupported,
        &states,
        &@as([calls.len]bool, @splat(false)),
        &batch,
        &.{0},
        &sel,
    ));
    try std.testing.expectEqualDeep(before, states);
}

test "shared integer duplicate counts decline levels-only and short value lanes before mutation" {
    const a = std.testing.allocator;
    const n = 193;
    var defs: [n]u32 = undefined;
    var vals: [n]i64 = @splat(7);
    var narrow: [n]i32 = @splat(7);
    for (&defs, 0..) |*d, i| d.* = @intFromBool(i % 11 != 0);
    for ([_]bool{ false, true }) |is32| for ([_]usize{ 0, n - 1 }) |len| {
        const col: filter_eval.Batch.Column = if (is32)
            .{ .i32 = .{
                .values = narrow[0..len],
                .def_levels = &defs,
                .max_def = 1,
                .has_nulls = true,
                .levels_only_rows = if (len == 0) n else 0,
            } }
        else
            .{ .i64 = .{
                .values = vals[0..len],
                .def_levels = &defs,
                .max_def = 1,
                .has_nulls = true,
                .levels_only_rows = if (len == 0) n else 0,
            } };
        const arg: expr_ast.Expr = .{ .col_ref = .{
            .col_idx = 0,
            .physical_type = if (is32) .INT32 else .INT64,
            .expr_type = .i64,
            .nullable = true,
        } };
        const calls = [_]AggCall{
            .{ .func = .count, .arg = arg, .where = null, .alias = "a", .result = .i64 },
            .{ .func = .count, .arg = arg, .where = null, .alias = "b", .result = .i64 },
        };
        const batch: filter_eval.Batch = .{ .cols = &.{col}, .num_rows = n };
        for (0..3) |mode| {
            var sel = try filter_selection.SelectionVector.init(a, n);
            defer sel.deinit();
            for (0..n) |r| sel.set(r, switch (mode) {
                0 => true,
                1 => false,
                else => r % 3 != 0,
            });
            var states: [2]Accumulator = .{ .{ .count = 7 }, .{ .count = 19 } };
            const before = states;
            try std.testing.expect(!try updateSharedInteger(
                a,
                &calls,
                &states,
                &.{ false, false },
                &batch,
                &.{0},
                &sel,
            ));
            try std.testing.expectEqualDeep(before, states);
            if (len == 0) {
                var present: u64 = 0;
                for (defs, 0..) |d, r| if (d != 0 and sel.isActive(r)) {
                    present += 1;
                };
                for (calls, &states) |call, *state| try updateOne(a, a, state, call, &batch, &.{0}, &sel);
                try std.testing.expectEqual(7 + present, states[0].count);
                try std.testing.expectEqual(19 + present, states[1].count);
            }
        }
    };
}

test "M1 parallel DISTINCT source charges and releases preserve the private ceiling" {
    const a = std.testing.allocator;
    var table = GroupTable.init(a, 8 * @sizeOf(DistinctSet));
    defer table.deinit();
    const Task = struct {
        table: *GroupTable,
        allocator: std.mem.Allocator,
        cursor: *std.atomic.Value(usize),
        err: ?anyerror = null,
        fn run(t: *@This()) void {
            while (t.err == null) {
                const job = t.cursor.fetchAdd(1, .monotonic);
                if (job >= 256) return;
                var set = t.table.freshDistinct();
                defer set.deinit(t.allocator);
                for (0..4) |i| set.insertU64(t.allocator, job * 7 + i) catch |e| {
                    t.err = e;
                    return;
                };
                std.debug.assert(set.count() == 4);
            }
        }
    };
    var tasks: [8]Task = undefined;
    var cursor = std.atomic.Value(usize).init(0);
    for (&tasks) |*t| t.* = .{ .table = &table, .allocator = a, .cursor = &cursor };
    const spawn = @import("../../spawn.zig");
    if (!spawn.runTree(Task, &tasks, .{ .stack_size = 1 << 20 }, Task.run)) tasks[0].run();
    for (tasks) |t| if (t.err) |e| return e;
    try std.testing.expectEqual(@as(usize, 0), table.allocated_bytes);
    try std.testing.expectEqual(@as(usize, 0), table.set_objects);
}

test "STRGROUP whole-table quote includes first chunks large records and actual capacities" {
    const a = testing.allocator;
    const calls = [_]AggCall{.{ .func = .count, .result = .i64, .arg = null, .where = null, .alias = "c" }};
    for ([_]usize{ 0, 127, 33000, 70000 }) |width| for ([_]usize{ 1, 33, 300 }) |groups| {
        var table = GroupTable.init(a, 256 << 20);
        defer table.deinit();
        const key = try a.alloc(u8, width);
        defer a.free(key);
        @memset(key, 'x');
        for (0..groups) |i| {
            if (width >= 4) std.mem.writeInt(u32, key[0..4], @intCast(i), .little);
            _ = try table.getOrInsert(key, &calls, i);
        }
        try testing.expect(table.allocated_bytes <= GroupTable.variableTableBytes(groups, width, calls.len));
    };
}

test "grouped string winner replacement funds both live copies before allocating" {
    const a = testing.allocator;
    const old = try a.alloc(u8, 1 << 20);
    defer a.free(old);
    const next = try a.alloc(u8, 1 << 20);
    defer a.free(next);
    for ([_]bool{ false, true }) |is_min| {
        @memset(old, if (is_min) 'z' else 'a');
        @memset(next, if (is_min) 'a' else 'z');
        const values = [_][]const u8{ old, next };
        const col: filter_eval.Batch.Column = .{ .string = .{ .values = &values } };
        var ledger: SharedBudget = .{ .limit = 3 << 19 };
        var charge: WinnerCharge = .{ .ledger = &ledger };
        var state: Accumulator = if (is_min) .{ .min_bytes = null } else .{ .max_bytes = null };
        agg_threadlocals.winner_charge = &charge;
        defer agg_threadlocals.winner_charge = null;
        defer {
            const value = if (is_min) state.min_bytes else state.max_bytes;
            if (value) |bytes| a.free(bytes);
            charge.settle(0);
            std.debug.assert(ledger.used.load(.monotonic) == 0);
        }
        try foldMinMaxBytesGroupedOne(a, is_min, &state, col, 0);
        const before = ledger.used.load(.monotonic);
        try testing.expectEqual(winnerBytes(old.len), before);
        try testing.expectError(error.ExceededMemoryBudget, foldMinMaxBytesGroupedOne(a, is_min, &state, col, 1));
        try testing.expectEqual(before, ledger.used.load(.monotonic));
        try testing.expectEqualStrings(old, (if (is_min) state.min_bytes else state.max_bytes).?);
        try testing.expect(ledger.peak.load(.monotonic) <= ledger.limit);
    }
}

fn cachedDistinctTrial(a: std.mem.Allocator) !void {
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    var left: DistinctSet = .{};
    defer left.deinit(a);
    var right: DistinctSet = .{};
    defer right.deinit(a);
    for (0..4096) |i| {
        const text = try std.fmt.allocPrint(arena.allocator(), "long-shared-prefix-{d:0>10}-suffix", .{i % 1001});
        try left.insertBytes(a, text);
        if (i % 2 == 0) try right.insertBytes(a, text);
    }
    try left.insertBytes(a, "");
    try right.merge(&left, a);
    try testing.expectEqual(@as(usize, 1002), right.count());
    var it = right.val_set.bytes.map.keyIterator();
    while (it.next()) |key| try testing.expectEqual(std.hash.Wyhash.hash(0, key.bytes), key.hash);
}

test "cached string DISTINCT grows and merges exact bytes through allocation refusals" {
    try cachedDistinctTrial(testing.allocator);
    try testing.checkAllAllocationFailures(testing.allocator, cachedDistinctTrial, .{});
}

test "shared string identity frames NULL empty embedded zero and large keys once through stored hash growth" {
    const a = testing.allocator;
    var table = GroupTable.init(a, 32 << 20);
    defer table.deinit();
    const big = try a.alloc(u8, 70001);
    defer a.free(big);
    @memset(big, 'x');
    const keys = [_]?[]const u8{ null, "", "a\x00b", "\x00", big };
    for (0..500) |i| {
        var bytes: [64]u8 = undefined;
        const raw = try std.fmt.bufPrint(&bytes, "key-{d}", .{i});
        _ = try table.getOrInsertStringHashed(raw, &.{}, 900 + i, 7);
    }
    for (keys, 0..) |key, i| {
        const id = try table.getOrInsertStringHashed(key, &.{}, 800 + i, 7);
        try testing.expectEqual(@as(u32, @intCast(500 + i)), id);
        try testing.expectEqual(id, try table.getOrInsertStringHashed(key, &.{}, i, 7));
        try testing.expectEqual(@as(u64, @intCast(i)), table.first.items[id]);
        const frame = table.keyAt(id);
        if (key) |raw| {
            try testing.expectEqual(@as(u8, 1), frame[0]);
            try testing.expectEqualStrings(raw, frame[5..]);
            try testing.expectEqual(raw.len, std.mem.readInt(u32, frame[1..5], .little));
        } else try testing.expectEqualSlices(u8, &.{0}, frame);
    }
    try testing.expectEqual(@as(usize, 505), table.groupCount());
}

test "a riding census drops on every worker's merged cells, not on one worker's slice" {
    const a = testing.allocator;
    const n_cells = 64;
    const admit_n = 8;
    const k = 2;
    var item_rows = [_]u64{0};
    for ([_]bool{ false, true }) |second_separates| {
        var dropped: std.atomic.Value(bool) = .init(false);
        var slot_cells: [2][n_cells]u32 = undefined;
        var slots = [_][]u32{ &slot_cells[0], &slot_cells[1] };
        var merged: Census.MergedCells = .{ .slots = &slots, .quorum = 2 };
        var censuses: [2]Census = undefined;
        for (&censuses, 0..) |*c, wi| c.* = .{
            .worker = wi,
            .gpa = a,
            .item_rows = &item_rows,
            .dropped = &dropped,
            .merged = &merged,
            .cut_k = k,
            .admit_n = admit_n,
        };
        defer for (&censuses) |*c| c.deinit();
        // The first worker's slice holds two hot cells over a sparse floor: its own cells separate the cut.
        var first: [n_cells]u32 = @splat(1);
        first[0] = 40;
        first[1] = 40;
        try std.testing.expect(cellsSeparate(blk: {
            var r: [admit_n + 1]u32 = undefined;
            fullestCells(&first, &r);
            break :blk &r;
        }, k, admit_n));
        try censuses[0].judge(&first);
        // Alone it decides nothing: the quorum is two.
        try std.testing.expect(!dropped.load(.monotonic) and !censuses[0].off);
        // The second worker's slice is either as decisive, or a dense floor that buries the hot cells once merged.
        var second: [n_cells]u32 = @splat(if (second_separates) 1 else 60);
        if (second_separates) {
            second[0] = 40;
            second[1] = 40;
        }
        try censuses[1].judge(&second);
        try std.testing.expectEqual(second_separates, dropped.load(.monotonic));
    }
    // Without merged cells a worker still judges its own.
    var dropped: std.atomic.Value(bool) = .init(false);
    var alone: Census = .{ .gpa = a, .item_rows = &item_rows, .dropped = &dropped, .cut_k = k, .admit_n = admit_n };
    defer alone.deinit();
    var own: [n_cells]u32 = @splat(1);
    own[0] = 40;
    own[1] = 40;
    try alone.judge(&own);
    try std.testing.expect(dropped.load(.monotonic));
}

test "distinct admission numeric fanout preserves bitmap promotion" {
    const a = testing.allocator;
    var budget: SharedBudget = .{ .limit = 64 << 20 };
    {
        var set: DistinctSet = .{};
        defer set.deinit(a);
        try set.initPartitions(a, &budget);
        try testing.expectEqual(@as(usize, 16), set.parts.?.len);
        for (0..65536) |i| try set.insertU64(a, (i * 97) % 100000);
        try testing.expectEqual(@as(usize, 65536), set.count());
        for (set.parts.?) |part| try testing.expect(part.val_set == .bitmap);
    }
    try testing.expectEqual(@as(usize, 0), budget.used.load(.monotonic));
}

test "distinct admission string fanout stays wide and merges across fanouts" {
    const a = testing.allocator;
    var narrow: DistinctSet = .{};
    defer narrow.deinit(a);
    try narrow.initPartitions(a, null);
    var wide: DistinctSet = .{};
    defer wide.deinit(a);
    try wide.initStringPartitions(a, null);
    try testing.expectEqual(@as(usize, 64), wide.parts.?.len);
    try narrow.insertBytes(a, "");
    try wide.insertBytes(a, "long shared string");
    try wide.insertBytes(a, "");
    try narrow.merge(&wide, a);
    try testing.expectEqual(@as(usize, 2), narrow.count());
    try wide.merge(&narrow, a);
    try testing.expectEqual(@as(usize, 2), wide.count());
}

test "variable hash growth bitmap migration preserves supplied hashes" {
    var budget: SharedBudget = .{ .limit = 1 << 20, .block = 1 };
    var table = GroupTable.init(std.testing.allocator, 0);
    table.shared = &budget;
    defer table.deinit();
    const calls = [_]AggCall{.{ .func = .count, .arg = null, .where = null, .alias = "c", .result = .i64 }};
    var buf: [24]u8 = undefined;
    for (0..12) |i| {
        const key = try std.fmt.bufPrint(&buf, "key-{d}", .{i});
        _ = try table.getOrInsertStringHashed(key, &calls, i, 0);
    }
    table.releaseSlack();
    const projected = table.variableHeld() + 16 * @sizeOf(GroupTable.FixedSlot);
    budget.limit = projected;
    try table.admit(projected - table.allocated_bytes);
    try std.testing.expectError(error.ExceededMemoryBudget, table.growFixedSlotsImpl(32, projected, true));
    budget.limit += 2;
    try table.growFixedSlotsImpl(32, projected, true);
    table.allocated_bytes = table.variableHeld();
    try std.testing.expectEqual(projected, budget.used.load(.monotonic));
    for (0..12) |i| {
        const key = try std.fmt.bufPrint(&buf, "key-{d}", .{i});
        try std.testing.expectEqual(@as(?u32, @intCast(i)), table.probeString(key, true, 0).id);
    }
}

test "fixed hash growth bitmap admission and migration preserve supplied hashes" {
    var budget: SharedBudget = .{ .limit = 1 << 20, .block = 1 };
    var table = GroupTable.init(std.testing.allocator, 0);
    table.shared = &budget;
    table.fixed_stride = 9;
    defer table.deinit();
    const calls = [_]AggCall{.{ .func = .count, .arg = null, .where = null, .alias = "c", .result = .i64 }};
    var key: [9]u8 = @splat(0);
    for (0..12) |i| {
        std.mem.writeInt(u64, key[1..9], i, .little);
        _ = try table.getOrInsertFixedHashed(&key, &calls, i, 0);
    }
    table.releaseSlack();
    const projected = table.fixedHeld() + 16 * @sizeOf(GroupTable.FixedSlot);
    budget.limit = projected;
    try table.admit(projected - table.allocated_bytes);
    try std.testing.expectError(error.ExceededMemoryBudget, table.growFixedSlotsImpl(32, projected, true));
    try std.testing.expectEqual(@as(usize, 16), table.fixedSlotCount());
    budget.limit += 2;
    try table.growFixedSlotsImpl(32, projected, true);
    table.allocated_bytes = table.fixedHeld();
    try std.testing.expectEqual(projected, budget.used.load(.monotonic));
    for (0..12) |i| {
        std.mem.writeInt(u64, key[1..9], i, .little);
        try std.testing.expectEqual(@as(?u32, @intCast(i)), table.findFixed(&key, 0));
    }
}

test "fixed hash growth is ledger funded and refusal preserves its keys" {
    const Counter = struct {
        budget: *SharedBudget,
        live: usize = 0,
        unpaid: usize = 0,
        observing: bool = false,
        fn allocator(self: *@This()) std.mem.Allocator {
            return .{ .ptr = self, .vtable = &.{
                .alloc = alloc,
                .resize = std.mem.Allocator.noResize,
                .remap = std.mem.Allocator.noRemap,
                .free = free,
            } };
        }
        fn alloc(ctx: *anyopaque, n: usize, alignment: std.mem.Alignment, ra: usize) ?[*]u8 {
            const self: *@This() = @ptrCast(@alignCast(ctx));
            const p = std.testing.allocator.rawAlloc(n, alignment, ra) orelse return null;
            self.live += n;
            if (self.observing)
                self.unpaid = @max(self.unpaid, self.live -| self.budget.used.load(.monotonic));
            return p;
        }
        fn free(ctx: *anyopaque, bytes: []u8, alignment: std.mem.Alignment, ra: usize) void {
            const self: *@This() = @ptrCast(@alignCast(ctx));
            std.testing.allocator.rawFree(bytes, alignment, ra);
            self.live -= bytes.len;
        }
    };
    var budget: SharedBudget = .{ .limit = 1 << 20, .block = 1 };
    var counter: Counter = .{ .budget = &budget };
    var table = GroupTable.init(counter.allocator(), 0);
    table.shared = &budget;
    table.fixed_stride = 9;
    var table_live = true;
    defer if (table_live) table.deinit();
    const calls = [_]AggCall{.{ .func = .count, .arg = null, .where = null, .alias = "c", .result = .i64 }};
    var key: [9]u8 = @splat(0);
    for (0..12) |i| {
        std.mem.writeInt(u64, key[1..9], i, .little);
        _ = try table.getOrInsertFixed(&key, &calls, i);
    }
    table.releaseSlack();
    budget.limit = budget.used.load(.monotonic);
    std.mem.writeInt(u64, key[1..9], 12, .little);
    try std.testing.expectError(error.ExceededMemoryBudget, table.getOrInsertFixed(&key, &calls, 12));
    try std.testing.expectEqual(@as(usize, 12), table.groupCount());
    const projected = table.fixedHeld() + 16 * @sizeOf(GroupTable.FixedSlot);
    budget.limit = projected;
    try table.admit(projected - table.allocated_bytes);
    counter.observing = true;
    try table.growFixedSlots(32, projected);
    counter.observing = false;
    try std.testing.expectEqual(@as(u8, 1), table.fixed_extra_count);
    table.allocated_bytes = table.fixedHeld();
    try std.testing.expectEqual(@as(usize, 0), counter.unpaid);
    budget.limit = 1 << 20;
    for (12..500) |i| {
        std.mem.writeInt(u64, key[1..9], i, .little);
        _ = try table.getOrInsertFixed(&key, &calls, i);
    }
    for (0..500) |i| {
        std.mem.writeInt(u64, key[1..9], i, .little);
        try std.testing.expectEqual(@as(?u32, @intCast(i)), table.findFixed(&key, hashFixed(&key)));
    }
    table.deinit();
    table_live = false;
    try std.testing.expectEqual(@as(usize, 0), counter.live);
    try std.testing.expectEqual(@as(usize, 0), budget.used.load(.monotonic));
}

test "fixed hash growth uses funded contiguous replacement when headroom permits" {
    var budget: SharedBudget = .{ .limit = 1 << 20, .block = 1 };
    var table = GroupTable.init(std.testing.allocator, 0);
    table.shared = &budget;
    table.fixed_stride = 9;
    defer table.deinit();
    const calls = [_]AggCall{.{ .func = .count, .arg = null, .where = null, .alias = "c", .result = .i64 }};
    var key: [9]u8 = @splat(0);
    for (0..12) |i| {
        std.mem.writeInt(u64, key[1..9], i, .little);
        _ = try table.getOrInsertFixed(&key, &calls, i);
    }
    table.releaseSlack();
    const projected = table.fixedHeld() + 16 * @sizeOf(GroupTable.FixedSlot);
    try table.admit(projected - table.allocated_bytes);
    try table.growFixedSlots(32, projected);
    table.allocated_bytes = table.fixedHeld();
    try std.testing.expectEqual(@as(u8, 0), table.fixed_extra_count);
    try std.testing.expectEqual(@as(usize, 32), table.firstSlots().len);
    try std.testing.expect(budget.peak.load(.monotonic) >= projected + 16 * @sizeOf(GroupTable.FixedSlot));
    try std.testing.expectEqual(projected, budget.used.load(.monotonic));
    for (0..12) |i| {
        std.mem.writeInt(u64, key[1..9], i, .little);
        try std.testing.expectEqual(@as(?u32, @intCast(i)), table.findFixed(&key, hashFixed(&key)));
    }
}

test "group buffer overlap is funded before replacement" {
    const Counter = struct {
        budget: *SharedBudget,
        live: usize = 0,
        unpaid: usize = 0,
        fn allocator(self: *@This()) std.mem.Allocator {
            return .{ .ptr = self, .vtable = &.{ .alloc = alloc, .resize = std.mem.Allocator.noResize, .remap = std.mem.Allocator.noRemap, .free = free } };
        }
        fn alloc(ctx: *anyopaque, n: usize, alignment: std.mem.Alignment, ra: usize) ?[*]u8 {
            const self: *@This() = @ptrCast(@alignCast(ctx));
            const p = std.testing.allocator.rawAlloc(n, alignment, ra) orelse return null;
            self.live += n;
            self.unpaid = @max(self.unpaid, self.live -| self.budget.used.load(.monotonic));
            return p;
        }
        fn free(ctx: *anyopaque, bytes: []u8, alignment: std.mem.Alignment, ra: usize) void {
            const self: *@This() = @ptrCast(@alignCast(ctx));
            std.testing.allocator.rawFree(bytes, alignment, ra);
            self.live -= bytes.len;
        }
    };
    var budget: SharedBudget = .{ .limit = 1 << 20, .block = 1 };
    var counter: Counter = .{ .budget = &budget };
    const calls = [_]AggCall{.{ .func = .count, .arg = null, .where = null, .alias = "c", .result = .i64 }};
    {
        var table = GroupTable.init(counter.allocator(), 0);
        table.shared = &budget;
        table.fixed_stride = 9;
        defer table.deinit();
        var key: [9]u8 = @splat(0);
        for (0..500) |i| {
            std.mem.writeInt(u64, key[1..9], i, .little);
            _ = try table.getOrInsertFixed(&key, &calls, i);
        }
        try std.testing.expectEqual(@as(usize, 0), counter.unpaid);
    }
    try std.testing.expectEqual(@as(usize, 0), counter.live);
    try std.testing.expectEqual(@as(usize, 0), budget.used.load(.monotonic));
    {
        var table = GroupTable.init(counter.allocator(), 0);
        table.shared = &budget;
        defer table.deinit();
        var key: [32]u8 = undefined;
        for (0..500) |i| {
            const value = try std.fmt.bufPrint(&key, "variable-{d}", .{i});
            _ = try table.getOrInsert(value, &calls, i);
        }
        try std.testing.expectEqual(@as(usize, 0), counter.unpaid);
    }
    try std.testing.expectEqual(@as(usize, 0), counter.live);
    try std.testing.expectEqual(@as(usize, 0), budget.used.load(.monotonic));
    {
        const key = try std.testing.allocator.alloc(u8, 1 << 20);
        defer std.testing.allocator.free(key);
        @memset(key, 'g');
        budget.limit = 1_153_433;
        var table = GroupTable.init(counter.allocator(), 0);
        table.shared = &budget;
        defer table.deinit();
        _ = try table.getOrInsert(key, &calls, 0);
        try std.testing.expectEqual(@as(usize, 0), counter.unpaid);
        try std.testing.expect(counter.live <= budget.used.load(.monotonic));
    }
    try std.testing.expectEqual(@as(usize, 0), counter.live);
    try std.testing.expectEqual(@as(usize, 0), budget.used.load(.monotonic));
}