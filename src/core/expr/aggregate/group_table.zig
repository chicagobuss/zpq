const std = @import("std");
const filter_selection = @import("../../filter/selection.zig");
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
const KeyArena = @import("contracts.zig").KeyArena;
const keys_mod = @import("contracts.zig");
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
const Ceiling = @import("ceiling.zig").Ceiling;
const NUM_PARTS = distinct.NUM_PARTS;
const NUMERIC_PARTS = distinct.NUMERIC_PARTS;
const INLINE_CAP = distinct.INLINE_CAP;
const InlineU64 = distinct.InlineU64;
const InlineI128 = distinct.InlineI128;
const InlineBytes = distinct.InlineBytes;
const partOf = distinct.partOf;
const DistinctBytes = distinct.DistinctBytes;

const DistinctSet = distinct.DistinctSet;
const DistinctAcc = distinct.DistinctAcc;

const Accumulator = @import("accumulator.zig").Accumulator;

const testing = std.testing;
const schema = @import("../../schema.zig");
const statistics = @import("../../parquet/statistics.zig");
const agg_coded = @import("../agg_coded.zig");
const AggFunc = types.AggFunc;
const ResultShape = types.ResultShape;
const packFixedKey = keys_mod.packFixedKey;
const splitMix64 = census_mod.splitMix64;
const FingerprintSet = census_mod.FingerprintSet;
const fullestCells = census_mod.fullestCells;
const cellsSeparate = census_mod.cellsSeparate;

pub const GroupTable = struct {
    const StringView = struct { bytes: []const u8, first: u64 };

    compact: @import("../agg_state.zig").State = .{},
    // Backing arrays can be page-backed independently of their accumulator payloads.
    buffer_allocator: ?std.mem.Allocator = null,
    allocator: std.mem.Allocator,
    /// What a DISTINCT set spilled on this table's private ceiling charges. 16-aligned: an inline acc stores this
    /// address with bit 3 set (`DistinctAcc.ceiling_flag`).
    ceiling: Ceiling align(16) = .{ .vtable = &ceiling_vtable },
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
    pub const FixedSlot = extern struct { hash: u32, id: u32 };

    comptime {
        if (@offsetOf(GroupTable, "ceiling") % 16 != 0) @compileError("private distinct sets use bit 3 of this pointer");
    }

    const ceiling_vtable: Ceiling.VTable = .{ .charge = chargeCeiling, .release = releaseCeiling };

    fn chargeCeiling(c: *Ceiling, n: usize) error{ExceededMemoryBudget}!void {
        const self: *GroupTable = @alignCast(@fieldParentPtr("ceiling", c));
        return self.chargePrivateBytes(n);
    }

    fn releaseCeiling(c: *Ceiling, n: usize) void {
        const self: *GroupTable = @alignCast(@fieldParentPtr("ceiling", c));
        self.releasePrivateBytes(n);
    }

    pub fn freshDistinct(self: *GroupTable) DistinctAcc {
        if (self.shared) |budget| return .initEmpty(budget);
        return .initPrivate(&self.ceiling);
    }

    /// Charge `n` bytes of a spilled set on a private ceiling against the live `max_memory_bytes`. The set releases
    /// them. A shared budget charges the set instead, so this is not a second bill.
    pub fn chargePrivateBytes(self: *GroupTable, n: usize) error{ExceededMemoryBudget}!void {
        var before = @atomicLoad(usize, &self.allocated_bytes, .monotonic);
        while (true) {
            const after = std.math.add(usize, before, n) catch return error.ExceededMemoryBudget;
            if (after > self.max_memory_bytes) return error.ExceededMemoryBudget;
            before = @cmpxchgWeak(usize, &self.allocated_bytes, before, after, .monotonic, .monotonic) orelse break;
        }
        _ = @atomicRmw(usize, &self.set_objects, .Add, n, .monotonic);
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

    pub fn probeString(self: *const GroupTable, key: []const u8, present: bool, full_hash: u64) FixedProbe {
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

    pub fn variableHeld(self: *const GroupTable) usize {
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

    pub fn fixedHeld(self: *const GroupTable) usize {
        return self.fixed_keys.len + self.fixedSlotCount() * @sizeOf(FixedSlot) +
            self.first.capacity * @sizeOf(u64) + self.accumulators.capacity * @sizeOf(Accumulator) +
            self.compact.held() + self.set_objects;
    }

    pub fn findFixed(self: *const GroupTable, key: []const u8, hash: u64) ?u32 {
        return self.probeFixed(key, hash).id;
    }

    const FixedProbe = struct { id: ?u32, slot: usize };

    pub inline fn fixedSlotCount(self: *const GroupTable) usize {
        return self.fixed_capacity;
    }

    pub inline fn firstSlots(self: *const GroupTable) []FixedSlot {
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

    pub fn growFixedSlots(self: *GroupTable, target: usize, projected: usize) !void {
        return self.growFixedSlotsImpl(target, projected, false);
    }

    pub fn growFixedSlotsImpl(self: *GroupTable, target: usize, projected: usize, force_bits: bool) !void {
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

    pub fn admit(self: *GroupTable, entry_size: usize) !void {
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
