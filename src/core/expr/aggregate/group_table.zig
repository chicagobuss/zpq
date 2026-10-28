const std = @import("std");
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

pub const DistinctSet = struct {
    const u64_entry_bytes: usize = 16;
    // Capacity is independently funded by map_extra; 32 bytes covers a near-full string map per key.
    const bytes_entry_bytes: usize = 32;
    /// The arena is separately allocated so its backing allocator remains stable when a set moves during merge.
    const StringArena = struct {
        owner: *DistinctSet,
        child: std.mem.Allocator,
        arena: std.heap.ArenaAllocator,
        budget_denied: bool = false,

        fn backing(self: *StringArena) std.mem.Allocator {
            return .{ .ptr = self, .vtable = &.{ .alloc = alloc, .resize = resize, .remap = std.mem.Allocator.noRemap, .free = free } };
        }

        fn alloc(ctx: *anyopaque, len: usize, alignment: std.mem.Alignment, ra: usize) ?[*]u8 {
            const self: *StringArena = @ptrCast(@alignCast(ctx));
            self.budget_denied = false;
            self.owner.drawMore(len) catch {
                self.budget_denied = true;
                return null;
            };
            const ptr = self.child.rawAlloc(len, alignment, ra) orelse return null;
            self.owner.allocated_bytes += len;
            return ptr;
        }

        fn resize(ctx: *anyopaque, mem: []u8, alignment: std.mem.Alignment, len: usize, ra: usize) bool {
            const self: *StringArena = @ptrCast(@alignCast(ctx));
            self.budget_denied = false;
            const growth = len -| mem.len;
            self.owner.drawMore(growth) catch {
                self.budget_denied = true;
                return false;
            };
            if (!self.child.rawResize(mem, alignment, len, ra)) return false;
            self.owner.allocated_bytes += growth;
            self.owner.allocated_bytes -= mem.len -| len;
            return true;
        }

        fn free(ctx: *anyopaque, mem: []u8, alignment: std.mem.Alignment, ra: usize) void {
            const self: *StringArena = @ptrCast(@alignCast(ctx));
            self.child.rawFree(mem, alignment, ra);
            self.owner.allocated_bytes -= mem.len;
        }
    };
    val_set: union(enum) {
        none,
        inline_u64: InlineU64,
        inline_i128: InlineI128,
        inline_bytes: InlineBytes,
        u64: std.AutoHashMapUnmanaged(u64, void),
        bitmap: U64Bitmap,
        i128: std.AutoHashMapUnmanaged(i128, void),
        bytes: DistinctBytes,
    } = .none,
    allocated_bytes: usize = 0,
    /// Capacity not already covered by the per-value estimate, including a table preallocated for a merge.
    map_extra: usize = 0,
    max_memory_bytes: usize = 0,
    shared: ?*SharedBudget = null,
    /// Charged against this table's private ceiling instead of `shared`. The table must outlive the set.
    private_table: ?*GroupTable = null,
    parts: ?[]DistinctSet = null,
    string_arena: ?*StringArena = null,

    pub fn initPartitions(self: *DistinctSet, allocator: std.mem.Allocator, budget: ?*SharedBudget) !void {
        return self.initPartitionCount(allocator, budget, NUMERIC_PARTS);
    }

    pub fn initStringPartitions(self: *DistinctSet, allocator: std.mem.Allocator, budget: ?*SharedBudget) !void {
        return self.initPartitionCount(allocator, budget, NUM_PARTS);
    }

    fn initPartitionCount(self: *DistinctSet, allocator: std.mem.Allocator, budget: ?*SharedBudget, n: usize) !void {
        const previous_shared = self.shared;
        const previous_credit = self.max_memory_bytes;
        const previous_allocated = self.allocated_bytes;
        std.debug.assert(previous_credit == 0 and previous_allocated == 0 and self.parts == null);
        self.shared = budget;
        errdefer {
            if (budget) |b| b.release(self.max_memory_bytes - previous_credit);
            self.max_memory_bytes = previous_credit;
            self.allocated_bytes = previous_allocated;
            self.shared = previous_shared;
        }
        try self.charge(n * @sizeOf(DistinctSet));
        const p = try allocator.alloc(DistinctSet, n);
        for (p) |*part| part.* = .{ .shared = budget };
        self.parts = p;
    }

    /// Count `bytes` this set holds outside its values (the set object itself, a partition array) against its budget.
    pub fn charge(self: *DistinctSet, bytes: usize) error{ExceededMemoryBudget}!void {
        try self.drawMore(bytes);
        self.allocated_bytes += bytes;
    }

    /// Give this set's reservation, partitions' included, back to its budget, and forget the budget: for a set that
    /// outlives the budget it drew from.
    pub fn detach(self: *DistinctSet) void {
        if (self.parts) |parts| for (parts) |*p| p.detach();
        if (self.shared) |budget| budget.release(self.max_memory_bytes);
        self.shared = null;
        self.releasePrivate();
        self.max_memory_bytes = 0;
    }

    fn releasePrivate(self: *DistinctSet) void {
        if (self.private_table) |t| {
            t.releasePrivateBytes(self.max_memory_bytes);
            t.releaseSpilledSet();
            self.private_table = null;
            self.max_memory_bytes = 0;
        }
    }

    pub fn setSharedBudget(self: *DistinctSet, budget: *SharedBudget) void {
        self.shared = budget;
        if (self.parts) |parts| {
            for (parts) |*p| p.shared = budget;
        }
    }

    pub fn deinit(self: *DistinctSet, allocator: std.mem.Allocator) void {
        if (self.parts) |parts| {
            for (parts) |*p| p.deinit(allocator);
            allocator.free(parts);
            self.parts = null;
        }
        if (self.string_arena) |arena| {
            arena.owner = self;
            arena.arena.deinit();
            allocator.destroy(arena);
            self.allocated_bytes -= @sizeOf(StringArena);
            self.string_arena = null;
        } else {
            switch (self.val_set) {
                .inline_bytes => |s| {
                    for (s.vals[0..s.len]) |k| allocator.free(k);
                },
                .bytes => |*s| {
                    var it = s.keyIterator();
                    while (it.next()) |k| allocator.free(k.*);
                },
                else => {},
            }
        }
        switch (self.val_set) {
            .none, .inline_u64, .inline_i128, .inline_bytes => {},
            .u64 => |*s| s.deinit(allocator),
            .bitmap => |b| allocator.free(b.words),
            .i128 => |*s| s.deinit(allocator),
            .bytes => |*s| s.deinit(allocator),
        }
        self.releasePrivate();
        if (self.shared) |budget| budget.release(self.max_memory_bytes);
        self.* = .{};
    }

    pub fn releaseSlack(self: *DistinctSet) void {
        if (self.parts) |parts| {
            for (parts) |*p| p.releaseSlack();
            return;
        }
        const budget = self.shared orelse return;
        self.refreshMapExtra();
        const slack = self.max_memory_bytes - self.allocated_bytes;
        if (slack == 0) return;
        self.max_memory_bytes = self.allocated_bytes;
        budget.release(slack);
    }

    /// A numeric merge source is about to be consumed and freed, so it no longer needs speculative per-value
    /// reservation for future growth. Keep its actual live map or bitmap, plus non-value charges, funded.
    pub fn releaseConsumedNumericSurplus(self: *DistinctSet) void {
        if (self.shared == null or self.parts != null) return;
        const physical, const identity = switch (self.val_set) {
            .u64 => |s| .{ mapBytes(u64, s.capacity()), @as(usize, s.count()) * u64_entry_bytes + self.map_extra },
            .i128 => |s| .{ mapBytes(i128, s.capacity()), @as(usize, s.count()) * 32 + self.map_extra },
            .bitmap => |b| .{ b.words.len * @sizeOf(u64), b.count * u64_entry_bytes },
            else => return,
        };
        const other = self.allocated_bytes -| identity;
        const held = other + physical;
        if (held >= self.allocated_bytes) return;
        self.allocated_bytes = held;
        self.map_extra = 0; // This source is read-only until deinit.
        self.releaseSlack();
    }

    /// Drawn in proportion to the set, never a block: a set per group is a holder too many to each strand a block's
    /// slack (a GROUP BY's count(DISTINCT) held a 64 KiB block per group of a few values), and an exact draw per
    /// entry put every insert of every worker on the ledger's one counter. An eighth of what the set holds, and at
    /// least `draw_floor`: a big set draws a logarithmic number of times, a small one a few, stranding 4 KiB.
    const draw_floor: usize = 4 << 10;

    pub fn drawMore(self: *DistinctSet, need: usize) error{ExceededMemoryBudget}!void {
        if (self.allocated_bytes + need <= self.max_memory_bytes) return;
        self.refreshMapExtra();
        if (self.allocated_bytes + need <= self.max_memory_bytes) return;
        const budget = self.shared orelse {
            if (self.private_table) |t| {
                const deficit = self.allocated_bytes + need - self.max_memory_bytes;
                try t.chargePrivateBytes(deficit);
                self.max_memory_bytes += deficit;
            }
            return;
        };
        if (budget.tight()) self.releaseSlack();
        const deficit = (self.allocated_bytes + need) - self.max_memory_bytes;
        const ahead = if (budget.tight()) deficit else @max(deficit, self.allocated_bytes / 8, draw_floor);
        const got = budget.drawExact(ahead) orelse budget.drawExact(deficit) orelse blk: {
            self.releaseSlack();
            break :blk budget.drawExact(need) orelse return error.ExceededMemoryBudget;
        };
        self.max_memory_bytes += got;
    }

    pub fn count(self: *const DistinctSet) usize {
        if (self.parts) |parts| {
            var total: usize = 0;
            for (parts) |p| total += p.count();
            return total;
        }
        return switch (self.val_set) {
            .none => 0,
            .inline_u64 => |s| s.len,
            .inline_i128 => |s| s.len,
            .inline_bytes => |s| s.len,
            .u64 => |s| s.count(),
            .bitmap => |b| b.count,
            .i128 => |s| s.count(),
            .bytes => |s| s.count(),
        };
    }

    const ValSet = @FieldType(DistinctSet, "val_set");
    const ValKind = std.meta.Tag(ValSet);

    fn mapBytes(comptime K: type, cap: usize) usize {
        if (cap == 0) return 0;
        const Header = struct { values: [*]void, keys: [*]K, capacity: u32 };
        const key_start = std.mem.alignForward(usize, @sizeOf(Header) + cap, @alignOf(K));
        return std.mem.alignForward(usize, key_start + cap * @sizeOf(K), @max(@alignOf(Header), @alignOf(K)));
    }

    fn growthCapacity(capacity: u32, available: u32, additional: u32) !u32 {
        if (additional <= available) return capacity;
        const pct = std.hash_map.default_max_load_percentage;
        const load = @as(u64, capacity) * pct / 100 - available;
        const want = (load + additional) * 100 / pct + 1;
        const want32 = std.math.cast(u32, want) orelse return error.ExceededMemoryBudget;
        const next = std.math.ceilPowerOfTwo(u32, want32) catch return error.ExceededMemoryBudget;
        return @max(next, 8);
    }

    fn growthBytes(comptime K: type, capacity: u32, available: u32, additional: u32) !usize {
        const next = try growthCapacity(capacity, available, additional);
        return if (next == capacity) 0 else mapBytes(K, next);
    }

    fn settleMap(self: *DistinctSet, comptime K: type, cap: usize, count_: usize, entry_size: usize) void {
        const extra = mapBytes(K, cap) -| (count_ * entry_size);
        self.allocated_bytes = self.allocated_bytes - self.map_extra + extra;
        self.map_extra = extra;
    }

    fn refreshMapExtra(self: *DistinctSet) void {
        if (self.map_extra == 0) return;
        // A getOrPut may have inserted one not-yet-charged entry. Keep one entry's credit until the next draw so
        // this remains safe both before and after the caller records that value.
        const required = switch (self.val_set) {
            .u64 => |s| mapBytes(u64, s.capacity()) -| ((@as(usize, s.count()) -| 1) * u64_entry_bytes),
            .i128 => |s| mapBytes(i128, s.capacity()) -| ((@as(usize, s.count()) -| 1) * 32),
            .bytes => |s| mapBytes(DistinctBytes.Key, s.map.capacity()) -| ((@as(usize, s.count()) -| 1) * bytes_entry_bytes),
            else => return,
        };
        if (required >= self.map_extra) return;
        self.allocated_bytes -= self.map_extra - required;
        self.map_extra = required;
    }

    /// std's unmanaged map stores metadata and keys in one allocation. When copying both tables cannot fit, grow
    /// a page-backed allocation without copying it, move its key region within the mapping, then rehash in place.
    fn remapMap(
        self: *DistinctSet,
        comptime K: type,
        map: anytype,
        allocator: std.mem.Allocator,
        additional: u32,
        entry_bytes: usize,
        new_value_bytes: usize,
        ctx: anytype,
    ) !bool {
        const old_cap = map.capacity();
        if (old_cap == 0) return false;
        const new_cap = try growthCapacity(old_cap, map.available, additional);
        if (new_cap == old_cap) return true;
        const Header = struct { values: [*]void, keys: [*]K, capacity: u32 };
        const new_extra = mapBytes(K, new_cap) -| (@as(usize, map.count()) * entry_bytes);
        try self.drawMore(new_value_bytes + (new_extra -| self.map_extra));
        const meta_addr = @intFromPtr(map.metadata.?);
        const base: [*]align(@max(@alignOf(Header), @alignOf(K))) u8 = @ptrFromInt(meta_addr - @sizeOf(Header));
        const old_keys = std.mem.alignForward(usize, @sizeOf(Header) + old_cap, @alignOf(K));
        const old_header: *const Header = @ptrCast(@alignCast(base));
        // This path follows the pinned Zig stdlib's map layout. Decline safely if a future layout changes.
        if (old_header.capacity != old_cap or @intFromPtr(old_header.keys) != @intFromPtr(base) + old_keys) return false;
        const old_mem = base[0..mapBytes(K, old_cap)];
        const map_allocator = core_memory.distinctMapAllocator(allocator, self.shared);
        const mem = core_memory.remapWithoutCopy(map_allocator, old_mem, mapBytes(K, new_cap)) orelse return false;
        const new_keys = std.mem.alignForward(usize, @sizeOf(Header) + new_cap, @alignOf(K));
        const key_bytes = @as(usize, old_cap) * @sizeOf(K);
        std.mem.copyBackwards(u8, mem[new_keys..][0..key_bytes], mem[old_keys..][0..key_bytes]);
        @memset(mem[@sizeOf(Header) + old_cap ..][0 .. new_cap - old_cap], 0);
        const hdr: *Header = @ptrCast(@alignCast(mem.ptr));
        hdr.keys = @ptrCast(@alignCast(mem.ptr + new_keys));
        hdr.values = @ptrCast(mem.ptr + new_keys + @as(usize, new_cap) * @sizeOf(K));
        hdr.capacity = new_cap;
        map.metadata = @ptrCast(mem.ptr + @sizeOf(Header));
        map.available = @intCast((@as(usize, new_cap) * std.hash_map.default_max_load_percentage) / 100 - map.count());
        map.rehash(ctx);
        self.settleMap(K, new_cap, map.count(), entry_bytes);
        return true;
    }

    /// The kind of table this set (or its partitions) keeps its values in, once past the inline few; null while none.
    fn mapKind(self: *const DistinctSet) ?ValKind {
        if (self.parts) |parts| {
            for (parts) |*p| if (p.mapKind()) |k| return k;
            return null;
        }
        return switch (self.val_set) {
            .u64, .i128, .bytes => |_, tag| tag,
            else => null,
        };
    }

    /// Room for `extra` more values of `kind` in this set's own table, a set still inline (or empty) becoming that
    /// table now, sized. A partitioned set gives each partition about its share: the partitions split values by hash
    /// bits the tables' slots do not use, and one that gets more than its share grows only near the end of a merge,
    /// when what has arrived is spread evenly over its slots.
    fn reserveFor(self: *DistinctSet, allocator: std.mem.Allocator, kind: ValKind, extra: usize) !void {
        if (extra <= INLINE_CAP) return;
        if (self.parts) |parts| {
            const share = extra / parts.len + extra / (4 * parts.len) + 16;
            for (parts) |*p| try p.reserveFor(allocator, kind, share);
            return;
        }
        const n = std.math.cast(u32, extra) orelse return;
        switch (kind) {
            inline .u64, .i128, .bytes => |map_tag| {
                const K = switch (map_tag) {
                    .u64 => u64,
                    .i128 => i128,
                    else => DistinctBytes.Key,
                };
                const entry_size: usize = switch (map_tag) {
                    .u64 => u64_entry_bytes,
                    .i128 => 32,
                    else => bytes_entry_bytes,
                };
                const inline_tag: ValKind = switch (map_tag) {
                    .u64 => .inline_u64,
                    .i128 => .inline_i128,
                    else => .inline_bytes,
                };
                switch (self.val_set) {
                    map_tag => |*m| {
                        const cap = if (map_tag == .bytes) m.map.capacity() else m.capacity();
                        const available = if (map_tag == .bytes) m.map.available else m.available;
                        self.drawMore(try growthBytes(K, cap, available, n)) catch |err| {
                            const raw_map = if (map_tag == .bytes) &m.map else m;
                            const ctx = if (map_tag == .bytes) DistinctBytes.Context{} else std.hash_map.AutoContext(K){};
                            if (!try self.remapMap(K, raw_map, allocator, n, entry_size, 0, ctx)) return err;
                        };
                        try m.ensureUnusedCapacity(core_memory.distinctMapAllocator(allocator, self.shared), n);
                        const new_cap = if (map_tag == .bytes) m.map.capacity() else m.capacity();
                        self.settleMap(K, new_cap, m.count(), entry_size);
                    },
                    .none, inline_tag => {
                        var map: @FieldType(ValSet, @tagName(map_tag)) = .empty;
                        const held = switch (self.val_set) {
                            inline_tag => |inl| inl.vals[0..inl.len],
                            else => &.{},
                        };
                        try self.drawMore(try growthBytes(K, 0, 0, @intCast(held.len + n)));
                        try map.ensureTotalCapacity(core_memory.distinctMapAllocator(allocator, self.shared), @intCast(held.len + n));
                        for (held) |v| map.putAssumeCapacity(v, {});
                        self.val_set = @unionInit(ValSet, @tagName(map_tag), map);
                        const new_cap = if (map_tag == .bytes) map.map.capacity() else map.capacity();
                        self.settleMap(K, new_cap, held.len, entry_size);
                    },
                    // Another kind: insertion reports it.
                    else => {},
                }
            },
            else => {},
        }
    }

    pub fn insertU64(self: *DistinctSet, allocator: std.mem.Allocator, v: u64) !void {
        if (self.parts) |parts| {
            const h = std.hash.Wyhash.hash(0, std.mem.asBytes(&v));
            const p = partOf(h, parts.len);
            return parts[p].insertU64(allocator, v);
        }
        switch (self.val_set) {
            .none => {
                const entry_size = u64_entry_bytes;
                try self.drawMore(entry_size);
                var s: InlineU64 = .{ .len = 1, .vals = undefined };
                s.vals[0] = v;
                self.val_set = .{ .inline_u64 = s };
                self.allocated_bytes += entry_size;
            },
            .inline_u64 => |*s| {
                for (s.vals[0..s.len]) |existing| {
                    if (existing == v) return;
                }
                const entry_size = u64_entry_bytes;
                try self.drawMore(entry_size);
                if (s.len < INLINE_CAP) {
                    s.vals[s.len] = v;
                    s.len += 1;
                    self.allocated_bytes += entry_size;
                    return;
                }
                var map: std.AutoHashMapUnmanaged(u64, void) = .empty;
                try self.drawMore(entry_size + try growthBytes(u64, 0, 0, INLINE_CAP + 1));
                try map.ensureTotalCapacity(core_memory.distinctMapAllocator(allocator, self.shared), INLINE_CAP + 1);
                for (s.vals[0..s.len]) |val| map.putAssumeCapacity(val, {});
                map.putAssumeCapacity(v, {});
                self.allocated_bytes += entry_size;
                self.val_set = .{ .u64 = map };
                self.settleMap(u64, map.capacity(), map.count(), u64_entry_bytes);
            },
            .bitmap => try self.insertBitmap(allocator, v),
            .u64 => |*s| {
                if (s.available == 0) {
                    @branchHint(.cold);
                    if (s.contains(v)) return;
                    if (try self.bitmapFromMap(allocator, v)) return;
                    self.drawMore(u64_entry_bytes + try growthBytes(u64, s.capacity(), s.available, 1)) catch |err| {
                        if (!try self.remapMap(u64, s, allocator, 1, u64_entry_bytes, u64_entry_bytes, std.hash_map.AutoContext(u64){})) return err;
                    };
                    try s.ensureUnusedCapacity(core_memory.distinctMapAllocator(allocator, self.shared), 1);
                    self.settleMap(u64, s.capacity(), s.count(), u64_entry_bytes);
                }
                const gop = s.getOrPutAssumeCapacity(v);
                if (gop.found_existing) return;
                const entry_size = u64_entry_bytes;
                self.drawMore(entry_size) catch |err| {
                    _ = s.remove(v);
                    return err;
                };
                self.allocated_bytes += entry_size;
            },
            else => return error.UnsupportedAggType, // one column feeds one value type: anything else is a bug, never a dropped value
        }
    }

    const U64Bitmap = struct {
        base: u64,
        words: []u64,
        count: usize,

        fn last(b: U64Bitmap) u64 {
            return b.base + (b.words.len * 64 - 1);
        }

        pub fn iterator(b: U64Bitmap) std.bit_set.Dynamic.Iterator(.{}) {
            return b.view().iterator(.{});
        }

        fn view(b: U64Bitmap) std.bit_set.Dynamic {
            return .{ .bit_length = b.words.len * 64, .masks = @ptrCast(b.words.ptr) };
        }

        fn insert(b: *U64Bitmap, v: u64) void {
            const offset: usize = @intCast(v - b.base);
            const mask = @as(u64, 1) << @as(u6, @truncate(offset));
            const word = &b.words[offset / 64];
            b.count += @intFromBool(word.* & mask == 0);
            word.* |= mask;
        }
    };

    /// Actual identities prove this range. Prefer bits only when their clear/OR traffic fits one cache share and
    /// their storage costs less than the hash entries they replace. Wide/scrambled values keep the exact map.
    fn bitmapShape(lo: u64, hi: u64, entries: usize) ?struct { base: u64, words: usize } {
        const base = lo & ~@as(u64, 63);
        const need = (hi - base) / 64 + 1;
        const room = @min(@import("../hw.zig").l2BytesPerWorkerOr(256 << 10), entries *| 8) / 8;
        if (need > room) return null;
        const n: usize = @intCast(need);
        const rounded = std.math.ceilPowerOfTwo(usize, n) catch n;
        const available = (std.math.maxInt(u64) - base) / 64 + 1;
        return .{ .base = base, .words = if (rounded <= room and rounded <= available) rounded else n };
    }

    fn bitmapFromMap(self: *DistinctSet, allocator: std.mem.Allocator, v: u64) !bool {
        var old = self.val_set.u64;
        var lo = v;
        var hi = v;
        var it = old.keyIterator();
        while (it.next()) |key| {
            lo = @min(lo, key.*);
            hi = @max(hi, key.*);
            if (bitmapShape(lo, hi, old.count() + 1) == null) return false;
        }
        const shape = bitmapShape(lo, hi, old.count() + 1) orelse return false;
        // Retain the established per-identity reservation: a late wide value can always return to the map
        // under the same budget, without replaying an input window. Bits use at most half of that credit.
        const physical = mapBytes(u64, old.capacity()) + shape.words * @sizeOf(u64);
        const covered = @as(usize, old.count()) * u64_entry_bytes + self.map_extra;
        try self.drawMore(u64_entry_bytes + (physical -| covered));
        const words = try allocator.alloc(u64, shape.words);
        @memset(words, 0);
        var bits: U64Bitmap = .{ .base = shape.base, .words = words, .count = 0 };
        it = old.keyIterator();
        while (it.next()) |key| bits.insert(key.*);
        bits.insert(v);
        self.allocated_bytes += u64_entry_bytes;
        old.deinit(allocator);
        self.allocated_bytes -= self.map_extra;
        self.map_extra = 0;
        self.val_set = .{ .bitmap = bits };
        return true;
    }

    /// Grow around complete stored words, preserving their identity alignment. False leaves a range too sparse
    /// for a bitmap unchanged; the caller can merge or insert its complete values through the ordinary map.
    fn bitmapRange(self: *DistinctSet, allocator: std.mem.Allocator, lo: u64, hi: u64, extra: usize) !bool {
        const old = self.val_set.bitmap;
        if (lo >= old.base and hi <= old.last()) return true;
        const shape = bitmapShape(@min(lo, old.base), @max(hi, old.last()), old.count +| extra) orelse return false;
        const physical = (old.words.len + shape.words) * @sizeOf(u64);
        try self.drawMore(physical -| (old.count * u64_entry_bytes));
        const words = try allocator.alloc(u64, shape.words);
        @memset(words, 0);
        const start: usize = @intCast((old.base - shape.base) / 64);
        @memcpy(words[start..][0..old.words.len], old.words);
        allocator.free(old.words);
        self.val_set = .{ .bitmap = .{ .base = shape.base, .words = words, .count = old.count } };
        return true;
    }

    fn insertBitmap(self: *DistinctSet, allocator: std.mem.Allocator, v: u64) !void {
        const old = self.val_set.bitmap;
        if (v >= old.base and v <= old.last()) {
            const offset: usize = @intCast(v - old.base);
            if (old.words[offset / 64] & (@as(u64, 1) << @as(u6, @truncate(offset))) != 0) return;
        }
        try self.drawMore(u64_entry_bytes);
        if (try self.bitmapRange(allocator, v, v, 1)) {
            self.val_set.bitmap.insert(v);
            self.allocated_bytes += u64_entry_bytes;
            return;
        }
        var map: std.AutoHashMapUnmanaged(u64, void) = .empty;
        errdefer map.deinit(allocator);
        const target = std.math.cast(u32, old.count + 1) orelse return error.OutOfMemory;
        const physical = old.words.len * @sizeOf(u64) + try growthBytes(u64, 0, 0, target);
        try self.drawMore(u64_entry_bytes + (physical -| (old.count * u64_entry_bytes)));
        try map.ensureTotalCapacity(core_memory.distinctMapAllocator(allocator, self.shared), target);
        var it = old.iterator();
        while (it.next()) |bit| map.putAssumeCapacity(old.base + bit, {});
        map.putAssumeCapacity(v, {});
        self.val_set = .{ .u64 = map };
        self.allocated_bytes += u64_entry_bytes;
        allocator.free(old.words);
        self.settleMap(u64, map.capacity(), map.count(), u64_entry_bytes);
    }

    pub fn insertI128(self: *DistinctSet, allocator: std.mem.Allocator, v: i128) !void {
        if (self.parts) |parts| {
            const h = std.hash.Wyhash.hash(0, std.mem.asBytes(&v));
            const p = partOf(h, parts.len);
            return parts[p].insertI128(allocator, v);
        }
        switch (self.val_set) {
            .none => {
                const entry_size = 32;
                try self.drawMore(entry_size);
                var s: InlineI128 = .{ .len = 1, .vals = undefined };
                s.vals[0] = v;
                self.val_set = .{ .inline_i128 = s };
                self.allocated_bytes += entry_size;
            },
            .inline_i128 => |*s| {
                for (s.vals[0..s.len]) |existing| {
                    if (existing == v) return;
                }
                const entry_size = 32;
                try self.drawMore(entry_size);
                if (s.len < INLINE_CAP) {
                    s.vals[s.len] = v;
                    s.len += 1;
                    self.allocated_bytes += entry_size;
                    return;
                }
                var map: std.AutoHashMapUnmanaged(i128, void) = .empty;
                try self.drawMore(entry_size + try growthBytes(i128, 0, 0, INLINE_CAP + 1));
                try map.ensureTotalCapacity(core_memory.distinctMapAllocator(allocator, self.shared), INLINE_CAP + 1);
                for (s.vals[0..s.len]) |val| map.putAssumeCapacity(val, {});
                map.putAssumeCapacity(v, {});
                self.allocated_bytes += entry_size;
                self.val_set = .{ .i128 = map };
                self.settleMap(i128, map.capacity(), map.count(), 32);
            },
            .i128 => |*s| {
                if (s.available == 0) {
                    @branchHint(.cold);
                    if (s.contains(v)) return;
                    self.drawMore(32 + try growthBytes(i128, s.capacity(), s.available, 1)) catch |err| {
                        if (!try self.remapMap(i128, s, allocator, 1, 32, 32, std.hash_map.AutoContext(i128){})) return err;
                    };
                    try s.ensureUnusedCapacity(core_memory.distinctMapAllocator(allocator, self.shared), 1);
                    self.settleMap(i128, s.capacity(), s.count(), 32);
                }
                const gop = s.getOrPutAssumeCapacity(v);
                if (gop.found_existing) return;
                const entry_size = 32;
                self.drawMore(entry_size) catch |err| {
                    _ = s.remove(v);
                    return err;
                };
                self.allocated_bytes += entry_size;
            },
            else => return error.UnsupportedAggType, // one column feeds one value type: anything else is a bug, never a dropped value
        }
    }

    pub fn insertF64(self: *DistinctSet, allocator: std.mem.Allocator, val: f64) !void {
        var f = val;
        if (std.math.isNan(f)) {
            f = std.math.nan(f64);
        } else if (f == 0.0) {
            f = 0.0;
        }
        return self.insertU64(allocator, @bitCast(f));
    }

    fn getOrCreateArena(self: *DistinctSet, allocator: std.mem.Allocator) !*StringArena {
        if (self.string_arena == null) {
            try self.drawMore(@sizeOf(StringArena));
            const arena = try allocator.create(StringArena);
            arena.* = .{ .owner = self, .child = allocator, .arena = undefined };
            arena.arena = std.heap.ArenaAllocator.init(arena.backing());
            self.allocated_bytes += @sizeOf(StringArena);
            self.string_arena = arena;
        }
        const arena = self.string_arena.?;
        arena.owner = self;
        return arena;
    }

    fn ownString(self: *DistinctSet, allocator: std.mem.Allocator, value: []const u8) ![]const u8 {
        const arena = try self.getOrCreateArena(allocator);
        arena.budget_denied = false;
        return arena.arena.allocator().dupe(u8, value) catch |err| {
            if (arena.budget_denied) return error.ExceededMemoryBudget;
            return err;
        };
    }

    pub fn insertBytes(self: *DistinctSet, allocator: std.mem.Allocator, v: []const u8) !void {
        return self.insertBytesHashed(allocator, v, std.hash.Wyhash.hash(0, v));
    }

    fn insertBytesHashed(self: *DistinctSet, allocator: std.mem.Allocator, v: []const u8, h: u64) !void {
        if (self.parts) |parts| return parts[partOf(h, parts.len)].insertBytesHashed(allocator, v, h);
        switch (self.val_set) {
            .none => {
                const entry_size = bytes_entry_bytes;
                try self.drawMore(entry_size);
                self.allocated_bytes += entry_size;
                errdefer self.allocated_bytes -= entry_size;
                const owned = try self.ownString(allocator, v);
                var s: InlineBytes = .{ .len = 1, .vals = undefined };
                s.vals[0] = owned;
                self.val_set = .{ .inline_bytes = s };
            },
            .inline_bytes => |*s| {
                for (s.vals[0..s.len]) |existing| {
                    if (keyEql(existing, v)) return;
                }
                const entry_size = bytes_entry_bytes;
                try self.drawMore(entry_size);
                self.allocated_bytes += entry_size;
                errdefer self.allocated_bytes -= entry_size;
                const owned = try self.ownString(allocator, v);
                if (s.len < INLINE_CAP) {
                    s.vals[s.len] = owned;
                    s.len += 1;
                    return;
                }
                var map: DistinctBytes = .empty;
                try self.drawMore(try growthBytes(DistinctBytes.Key, 0, 0, INLINE_CAP + 1));
                try map.ensureTotalCapacity(core_memory.distinctMapAllocator(allocator, self.shared), INLINE_CAP + 1);
                for (s.vals[0..s.len]) |val| map.putAssumeCapacity(val, {});
                map.putHashed(owned, h);
                self.val_set = .{ .bytes = map };
                self.settleMap(DistinctBytes.Key, map.map.capacity(), map.count(), bytes_entry_bytes);
            },
            .bytes => |*s| {
                if (s.map.available == 0) {
                    @branchHint(.cold);
                    if (s.map.contains(.{ .bytes = v, .hash = h })) return;
                    self.drawMore(bytes_entry_bytes + try growthBytes(DistinctBytes.Key, s.map.capacity(), s.map.available, 1)) catch |err| {
                        if (!try self.remapMap(DistinctBytes.Key, &s.map, allocator, 1, bytes_entry_bytes, bytes_entry_bytes, DistinctBytes.Context{})) return err;
                    };
                    try s.ensureUnusedCapacity(core_memory.distinctMapAllocator(allocator, self.shared), 1);
                    self.settleMap(DistinctBytes.Key, s.map.capacity(), s.count(), bytes_entry_bytes);
                }
                const gop = s.getHashedAssumeCapacity(v, h);
                if (gop.found_existing) return;
                const entry_size = bytes_entry_bytes;
                self.drawMore(entry_size) catch |err| {
                    s.removeHashed(v, h);
                    return err;
                };
                self.allocated_bytes += entry_size;
                errdefer self.allocated_bytes -= entry_size;
                const owned = self.ownString(allocator, v) catch |err| {
                    s.removeHashed(v, h);
                    return err;
                };
                gop.key_ptr.* = owned;
            },
            else => return error.UnsupportedAggType, // one column feeds one value type: anything else is a bug, never a dropped value
        }
    }

    pub fn insertColRow(self: *DistinctSet, allocator: std.mem.Allocator, col: filter_eval.Batch.Column, r: usize) !void {
        if (isRowNull(col, r)) return;
        switch (col) {
            .i32 => |c| try self.insertU64(allocator, @as(u64, @bitCast(@as(i64, c.values[r])))),
            .i64 => |c| try self.insertU64(allocator, @as(u64, @bitCast(c.values[r]))),
            .f32 => |c| try self.insertF64(allocator, @as(f64, c.values[r])),
            .f64 => |c| try self.insertF64(allocator, c.values[r]),
            .string => |c| try self.insertBytes(allocator, c.values[r]),
            .boolean => |c| try self.insertU64(allocator, if (c.values[r]) 1 else 0),
            .i128 => |c| try self.insertI128(allocator, c.values[r]),
        }
    }

    pub fn merge(self: *DistinctSet, other: *const DistinctSet, allocator: std.mem.Allocator) !void {
        if (self.parts == null and other.parts == null and other.val_set == .bitmap) {
            const src = other.val_set.bitmap;
            if (self.val_set == .none) {
                try self.drawMore(src.count * u64_entry_bytes);
                const words = try allocator.dupe(u64, src.words);
                self.val_set = .{ .bitmap = .{ .base = src.base, .words = words, .count = src.count } };
                self.allocated_bytes += src.count * u64_entry_bytes;
                return;
            }
            if (self.val_set == .bitmap) {
                const old = self.val_set.bitmap;
                var novel = src.count;
                for (src.words, 0..) |word, wi| {
                    const base = src.base + wi * 64;
                    if (base >= old.base and base <= old.last())
                        novel -= @popCount(word & old.words[@intCast((base - old.base) / 64)]);
                }
                try self.drawMore(novel * u64_entry_bytes);
                if (try self.bitmapRange(allocator, src.base, src.last(), novel)) {
                    const dst = &self.val_set.bitmap;
                    const start: usize = @intCast((src.base - dst.base) / 64);
                    for (dst.words[start..][0..src.words.len], src.words) |*word, bits| {
                        dst.count += @popCount(bits & ~word.*);
                        word.* |= bits;
                    }
                    self.allocated_bytes += novel * u64_entry_bytes;
                    return;
                }
            }
        }
        // A table's values come out in its slot order, which is hash order. Inserted into a table that grows as they
        // arrive, a larger table's hash-ordered prefix wraps onto the smaller one's slots unevenly, some ranges a lap
        // ahead of the rest, and at the 80% load where it grows those ranges are full: long linear-probe runs, and a
        // merge of two workers' sets took 40 s where it takes under one (which -j hit it depended on how the rows
        // split). Sized for the result first, every table stays at or below its final load throughout.
        if (other.mapKind()) |kind| try self.reserveFor(allocator, kind, other.count());
        if (self.parts) |parts| {
            if (other.parts) |oparts| {
                if (parts.len == oparts.len) {
                    for (parts, oparts) |*p, op| try p.merge(&op, allocator);
                } else for (oparts) |*op| try self.merge(op, allocator);
                return;
            }
            if (other.val_set != .none) {
                switch (other.val_set) {
                    .none => {},
                    .inline_u64 => |s| {
                        for (s.vals[0..s.len]) |k| try self.insertU64(allocator, k);
                    },
                    .inline_i128 => |s| {
                        for (s.vals[0..s.len]) |k| try self.insertI128(allocator, k);
                    },
                    .inline_bytes => |s| {
                        for (s.vals[0..s.len]) |k| try self.insertBytes(allocator, k);
                    },
                    .bitmap => |bits| {
                        var it = bits.iterator();
                        while (it.next()) |bit| try self.insertU64(allocator, bits.base + bit);
                    },
                    .u64 => |s| {
                        var it = s.keyIterator();
                        while (it.next()) |k| try self.insertU64(allocator, k.*);
                    },
                    .i128 => |s| {
                        var it = s.keyIterator();
                        while (it.next()) |k| try self.insertI128(allocator, k.*);
                    },
                    .bytes => |s| {
                        var it = s.map.keyIterator();
                        while (it.next()) |k| try self.insertBytesHashed(allocator, k.bytes, k.hash);
                    },
                }
                return;
            }
        }
        if (other.parts) |oparts| {
            for (oparts) |op| {
                try self.merge(&op, allocator);
            }
            return;
        }
        switch (other.val_set) {
            .none => {},
            .inline_u64 => |s| {
                for (s.vals[0..s.len]) |k| try self.insertU64(allocator, k);
            },
            .inline_i128 => |s| {
                for (s.vals[0..s.len]) |k| try self.insertI128(allocator, k);
            },
            .inline_bytes => |s| {
                for (s.vals[0..s.len]) |k| try self.insertBytes(allocator, k);
            },
            .bitmap => |bits| {
                var it = bits.iterator();
                while (it.next()) |bit| try self.insertU64(allocator, bits.base + bit);
            },
            .u64 => |s| {
                var it = s.keyIterator();
                while (it.next()) |k| try self.insertU64(allocator, k.*);
            },
            .i128 => |s| {
                var it = s.keyIterator();
                while (it.next()) |k| try self.insertI128(allocator, k.*);
            },
            .bytes => |s| {
                var it = s.map.keyIterator();
                while (it.next()) |k| try self.insertBytesHashed(allocator, k.bytes, k.hash);
            },
        }
    }
};

/// Grouped count(DISTINCT) held in the accumulator itself.
///
/// `raw[1..]` stores up to `cap` u64 values. `raw[0]` is a pointer with a 3-bit tag in the low bits (0..cap inline,
/// `heap_mark` spilled). Bit 3 is free on an inline acc because `SharedBudget` and `GroupTable` are 16-aligned:
/// clear means the shared budget, set means the private group table. A spilled `DistinctSet` is only 8-aligned, so
/// there bit 3 is a real address bit and the tag alone means heap. 32 bytes is the payload `DecSum` already forces.
/// One to three u64s cost no heap object. The fourth, or any i128 or byte value, spills. A shared budget is charged
/// on the set; a private table is charged on the table at spill, so a ceiling tightened after the group exists
/// still applies.
pub const DistinctAcc = struct {
    raw: [4]u64 = .{ 0, 0, 0, 0 },

    pub const cap: u8 = 3;
    const heap_mark: u64 = 7;
    /// Bit 3. Not a tag value: the low 3 bits stay the count, and this bit says the pointer is a `GroupTable`.
    const table_flag: u64 = 8;

    comptime {
        if (@sizeOf(DistinctAcc) > @sizeOf(DecSum)) @compileError("DistinctAcc grew past the accumulator payload");
        if (@alignOf(DistinctSet) < 8 or @alignOf(SharedBudget) < 16) @compileError("tag bits need aligned pointees");
        if (table_flag & 7 != 0) @compileError("table flag collides with the tag");
    }

    pub fn initEmpty(budget: ?*SharedBudget) DistinctAcc {
        return .{ .raw = .{ @intFromPtr(budget), 0, 0, 0 } };
    }

    /// Inline acc whose spill charges `table`'s private ceiling. `table` must not move while this acc is live.
    pub fn initPrivate(table: *GroupTable) DistinctAcc {
        return .{ .raw = .{ @intFromPtr(table) | table_flag, 0, 0, 0 } };
    }

    pub fn fromSet(ds: *DistinctSet) DistinctAcc {
        return .{ .raw = .{ @intFromPtr(ds) | heap_mark, 0, 0, 0 } };
    }

    fn tag(self: DistinctAcc) u8 {
        return @intCast(self.raw[0] & 7);
    }

    pub fn heap(self: DistinctAcc) ?*DistinctSet {
        if ((self.raw[0] & 7) != heap_mark) return null;
        return @ptrFromInt(self.raw[0] & ~@as(u64, 7));
    }

    /// Budget stashed while the values are inline; a spilled set keeps it on the set. Null when the pointer is a
    /// private table (that is not a `SharedBudget`).
    pub fn budgetPtr(self: DistinctAcc) ?*SharedBudget {
        if ((self.raw[0] & 7) == heap_mark) return if (self.heap()) |ds| ds.shared else null;
        if ((self.raw[0] & table_flag) != 0) return null;
        const p = self.raw[0] & ~@as(u64, 7);
        if (p == 0) return null;
        return @ptrFromInt(p);
    }

    fn tablePtr(self: DistinctAcc) ?*GroupTable {
        if ((self.raw[0] & 7) == heap_mark or (self.raw[0] & table_flag) == 0) return null;
        return @ptrFromInt(self.raw[0] & ~@as(u64, 15));
    }

    /// A 16-aligned budget pointer has bit 3 clear, so this also drops a private-table pointer.
    fn setBudget(self: *DistinctAcc, budget: ?*SharedBudget) void {
        self.raw[0] = @intFromPtr(budget) | (self.raw[0] & 7);
    }

    /// Pointer bits to copy onto an empty dest: the inline budget or table, or the spilled set's, never the set.
    fn chargeWord(self: DistinctAcc) u64 {
        if (self.heap()) |ds| {
            if (ds.private_table) |t| return @intFromPtr(t) | table_flag;
            return @intFromPtr(ds.shared);
        }
        return self.raw[0] & ~@as(u64, 7);
    }

    pub fn count(self: DistinctAcc) usize {
        if (self.heap()) |ds| return ds.count();
        return self.tag();
    }

    pub fn deinit(self: *DistinctAcc, allocator: std.mem.Allocator) void {
        if (self.heap()) |ds| {
            ds.deinit(allocator);
            allocator.destroy(ds);
        }
        self.* = .{};
    }

    pub fn detach(self: *DistinctAcc) void {
        // Inline state never reserved. Drop the pointer anyway: the budget's scope may already have ended.
        if (self.heap()) |ds| ds.detach() else self.setBudget(null);
    }

    pub fn insertU64(self: *DistinctAcc, allocator: std.mem.Allocator, v: u64) !void {
        const t = self.tag();
        if (t == heap_mark) return self.heap().?.insertU64(allocator, v);
        var i: u8 = 0;
        while (i < t) : (i += 1) {
            if (self.raw[1 + @as(usize, i)] == v) return;
        }
        if (t < cap) {
            self.raw[1 + @as(usize, t)] = v;
            self.raw[0] = (self.raw[0] & ~@as(u64, 7)) | @as(u64, t + 1);
            return;
        }
        const ds = try self.ensureHeap(allocator);
        try ds.insertU64(allocator, v);
    }

    pub fn insertF64(self: *DistinctAcc, allocator: std.mem.Allocator, val: f64) !void {
        var f = val;
        if (std.math.isNan(f)) {
            f = std.math.nan(f64);
        } else if (f == 0.0) {
            f = 0.0;
        }
        return self.insertU64(allocator, @bitCast(f));
    }

    pub fn insertI128(self: *DistinctAcc, allocator: std.mem.Allocator, v: i128) !void {
        if (self.tag() != 0 and self.tag() != heap_mark) return error.UnsupportedAggType;
        const ds = try self.ensureHeap(allocator);
        try ds.insertI128(allocator, v);
    }

    pub fn insertBytes(self: *DistinctAcc, allocator: std.mem.Allocator, v: []const u8) !void {
        if (self.tag() != 0 and self.tag() != heap_mark) return error.UnsupportedAggType;
        const ds = try self.ensureHeap(allocator);
        try ds.insertBytes(allocator, v);
    }

    pub fn insertColRow(
        self: *DistinctAcc,
        allocator: std.mem.Allocator,
        col: filter_eval.Batch.Column,
        r: usize,
    ) !void {
        if (isRowNull(col, r)) return;
        switch (col) {
            .i32 => |c| try self.insertU64(allocator, @as(u64, @bitCast(@as(i64, c.values[r])))),
            .i64 => |c| try self.insertU64(allocator, @bitCast(c.values[r])),
            .f32 => |c| try self.insertF64(allocator, @floatCast(c.values[r])),
            .f64 => |c| try self.insertF64(allocator, c.values[r]),
            .boolean => |c| try self.insertU64(allocator, if (c.values[r]) 1 else 0),
            .string => |c| try self.insertBytes(allocator, c.values[r]),
            .i128 => |c| try self.insertI128(allocator, c.values[r]),
        }
    }

    /// Spill inline values into a heap set. Already-spilled state is returned as it is. On failure the inline
    /// values are still here.
    pub fn ensureHeap(self: *DistinctAcc, allocator: std.mem.Allocator) !*DistinctSet {
        if (self.heap()) |ds| return ds;
        const budget = self.budgetPtr();
        const n = self.tag();
        var saved: [cap]u64 = undefined;
        var i: u8 = 0;
        while (i < n) : (i += 1) saved[i] = self.raw[1 + @as(usize, i)];

        const ds = try allocator.create(DistinctSet);
        errdefer allocator.destroy(ds);
        ds.* = .{ .shared = budget };
        errdefer ds.deinit(allocator);
        // The object is memory the group now holds, charged when it spills rather than for every group. A shared
        // budget lives on the set. A private ceiling has nothing to draw on, so the table counts the object itself.
        if (self.tablePtr()) |t| {
            try t.chargeSpilledSet();
            ds.private_table = t;
        } else {
            try ds.charge(@sizeOf(DistinctSet));
        }
        i = 0;
        while (i < n) : (i += 1) try ds.insertU64(allocator, saved[i]);
        self.* = .fromSet(ds);
        return ds;
    }

    /// Copy `src`'s values in. `src` keeps whatever it owns.
    pub fn mergeFrom(self: *DistinctAcc, src: DistinctAcc, allocator: std.mem.Allocator) !void {
        // An empty dest has nothing to charge the copy to. Take the source's budget or private table, and leave a
        // dest that already has one alone — otherwise a full source budget would let the copy through unpaid.
        if (self.tag() == 0 and (self.raw[0] & ~@as(u64, 7)) == 0) self.raw[0] |= src.chargeWord();
        if (src.heap()) |ss| {
            const dst = try self.ensureHeap(allocator);
            try dst.merge(ss, allocator);
            return;
        }
        const n = src.tag();
        var i: u8 = 0;
        while (i < n) : (i += 1) try self.insertU64(allocator, src.raw[1 + @as(usize, i)]);
    }

    /// Move `src` into `self`, leaving `src` empty. The larger spilled set is kept and the smaller folded in.
    pub fn takeFrom(self: *DistinctAcc, src: *DistinctAcc, allocator: std.mem.Allocator) !void {
        if (src.tag() == 0) return;
        if (self.tag() == 0) {
            // `budgetPtr` is null for a private table, so keep the raw charge word. A heap src carries its own.
            const kept = self.raw[0] & ~@as(u64, 7);
            self.* = src.*;
            src.* = .{};
            if (self.heap() == null and (self.raw[0] & ~@as(u64, 7)) == 0) self.raw[0] |= kept;
            return;
        }
        if (self.heap() == null and src.heap() == null) {
            const n = src.tag();
            var i: u8 = 0;
            while (i < n) : (i += 1) try self.insertU64(allocator, src.raw[1 + @as(usize, i)]);
            src.* = .{};
            return;
        }
        const dst_set = try self.ensureHeap(allocator);
        if (src.heap()) |ss| {
            src.* = .{};
            if (ss.count() > dst_set.count()) {
                const tmp = dst_set.*;
                dst_set.* = ss.*;
                ss.* = tmp;
            }
            defer {
                ss.deinit(allocator);
                allocator.destroy(ss);
            }
            try dst_set.merge(ss, allocator);
            return;
        }
        const n = src.tag();
        var i: u8 = 0;
        while (i < n) : (i += 1) try self.insertU64(allocator, src.raw[1 + @as(usize, i)]);
        src.* = .{};
    }
};

/// Accumulator state. One per AggCall, lives across all RGs of all
/// input files. Tagged union so the dispatcher picks the right
/// kernel at update time without re-checking AggFunc + arg type.
pub const Accumulator = union(enum) {
    count: u64,
    distinct: DistinctAcc,
    /// Integer sum, i128. Keeps i64-column sums precise on big datasets
    /// (10^9 rows × 10^9 values = 10^18) and carries unsigned-64 sums and
    /// i64-overflowing sums (e.g. wide DELTA columns) without wrapping. The
    /// JSON output emits the full i128; only the 1-row result *parquet* column
    /// is INT64 and errors `AggIntTooWide` past i64 range.
    /// `null` until a non-null value is folded in: SQL's sum over no values is
    /// NULL, not 0.
    sum_i: ?i128,
    sum_f: ?f64,
    /// Min/max — `null` means no values seen yet. Distinguishes
    /// "all rows filtered out" from "min was 0". i128 so an unsigned-64
    /// extremum (up to 2^64-1) is representable; signed i64 values widen in.
    min_i: ?i128,
    min_f: ?f64,
    max_i: ?i128,
    max_f: ?f64,
    /// String (BYTE_ARRAY/UTF8) min/max — bytewise/unsigned comparison
    /// (Parquet's TYPE_DEFINED order; == DuckDB's binary collation). The
    /// slice is OWNED: dup'd into the persist allocator on each new winner
    /// so it outlives the per-RG decode arena and the cross-worker merge.
    /// `null` = no value seen yet.
    min_bytes: ?[]const u8,
    max_bytes: ?[]const u8,
    min_arena: ArenaBytes,
    max_arena: ArenaBytes,
    /// Avg always lands as f64. Internal sum is f64 even for int
    /// input — for huge int sums this loses precision past 2^53;
    /// document the caveat. DECIMAL avg is exact: `avg_d`.
    avg: AvgState,
    /// Exact DECIMAL folds over unscaled values (the scale is the call's): sum and avg keep a wide sum and a count,
    /// min/max the extreme seen, null until one is.
    sum_d: DecSum,
    avg_d: DecSum,
    min_d: ?i128,
    max_d: ?i128,

    pub const ArenaBytes = struct {
        store: *KeyArena,
        ref: ?u32 = null,
        key: ?u32 = null,
        skip: u8 = 0,

        pub fn bytes(self: ArenaBytes) ?[]const u8 {
            return if (self.ref) |ref| self.store.get(ref)[self.skip..] else null;
        }

        pub fn consider(self: *ArenaBytes, value: []const u8, is_min: bool) !void {
            if (self.bytes()) |cur| {
                const order = std.mem.order(u8, value, cur);
                if (if (is_min) order != .lt else order != .gt) return;
            }
            if (self.key) |key| {
                const framed = self.store.get(key);
                if (framed.len == value.len + 5 and framed[0] == 1 and
                    std.mem.readInt(u32, framed[1..5], .little) == value.len and keyEql(framed[5..], value))
                {
                    self.ref = key;
                    self.skip = 5;
                    return;
                }
            }
            self.ref = try self.store.append(value);
            self.skip = 0;
        }
    };

    pub fn stringValue(self: Accumulator) ?[]const u8 {
        return switch (self) {
            .min_bytes, .max_bytes => |b| b,
            .min_arena, .max_arena => |b| b.bytes(),
            else => null,
        };
    }

    pub fn init(call: AggCall) Accumulator {
        return switch (call.func) {
            .count => .{ .count = 0 },
            .count_distinct => .{ .distinct = .{} },
            .sum => switch (call.result) {
                .i64 => .{ .sum_i = null },
                .f64 => .{ .sum_f = null },
                .dec => .{ .sum_d = .{} },
                .bytes, .avg_f64, .avg_dec => unreachable,
            },
            .min => switch (call.result) {
                .i64 => .{ .min_i = null },
                .f64 => .{ .min_f = null },
                .bytes => .{ .min_bytes = null },
                .dec => .{ .min_d = null },
                .avg_f64, .avg_dec => unreachable,
            },
            .max => switch (call.result) {
                .i64 => .{ .max_i = null },
                .f64 => .{ .max_f = null },
                .bytes => .{ .max_bytes = null },
                .dec => .{ .max_d = null },
                .avg_f64, .avg_dec => unreachable,
            },
            .avg => if (call.result == .avg_dec) .{ .avg_d = .{} } else .{ .avg = .{ .sum = 0, .count = 0 } },
        };
    }

    /// Combine `src` into `dst`. Both must be the same variant — used
    /// to merge per-thread / per-file accumulators back into a single
    /// final result after a parallel multi-file scan. A null min/max
    /// means "no values seen"; merging skips it.
    /// A count past i64 (footers whose bounded row counts add up past it across files) is `RowCountTooLarge`.
    pub fn merge(dst: *Accumulator, src: Accumulator, allocator: std.mem.Allocator) !void {
        const strings: ?bool = switch (dst.*) {
            .min_bytes, .min_arena => true,
            .max_bytes, .max_arena => false,
            else => null,
        };
        if (strings) |is_min| {
            const sv = src.stringValue() orelse return;
            const cur = dst.stringValue();
            const wins = cur == null or (if (is_min)
                std.mem.order(u8, sv, cur.?) == .lt
            else
                std.mem.order(u8, sv, cur.?) == .gt);
            const loser = if (wins) dst.* else src;
            switch (loser) {
                .min_bytes, .max_bytes => |b| if (b) |owned| allocator.free(owned),
                else => {},
            }
            if (wins) dst.* = src;
            return;
        }
        switch (dst.*) {
            .count => |*c| c.* = try addRowCount(c.*, src.count),
            .distinct => |*d| try d.mergeFrom(src.distinct, allocator),
            .sum_i => |*s| if (src.sum_i) |sv| {
                s.* = (s.* orelse 0) + sv;
            },
            .sum_f => |*s| if (src.sum_f) |sv| {
                s.* = (s.* orelse 0) + sv;
            },
            .min_i => |*m| if (src.min_i) |sv| {
                if (m.* == null or sv < m.*.?) m.* = sv;
            },
            .min_f => |*m| if (src.min_f) |sv| foldExtremeF(true, m, sv),
            .max_i => |*m| if (src.max_i) |sv| {
                if (m.* == null or sv > m.*.?) m.* = sv;
            },
            .max_f => |*m| if (src.max_f) |sv| foldExtremeF(false, m, sv),
            // Pointer copy: both src and the merged dst hold slices dup'd
            // into the shared persist allocator. We free the loser to prevent leaks.
            .min_bytes => |*m| if (src.min_bytes) |sv| {
                if (m.* == null) {
                    m.* = sv;
                } else {
                    const ord = std.mem.order(u8, sv, m.*.?);
                    if (ord == .lt) {
                        allocator.free(m.*.?);
                        m.* = sv;
                    } else {
                        allocator.free(sv);
                    }
                }
            },
            .max_bytes => |*m| if (src.max_bytes) |sv| {
                if (m.* == null) {
                    m.* = sv;
                } else {
                    const ord = std.mem.order(u8, sv, m.*.?);
                    if (ord == .gt) {
                        allocator.free(m.*.?);
                        m.* = sv;
                    } else {
                        allocator.free(sv);
                    }
                }
            },
            .min_arena, .max_arena => unreachable,
            .avg => |*a| {
                a.sum += src.avg.sum;
                a.isum += src.avg.isum;
                a.count += src.avg.count;
            },
            .sum_d => |*d| d.add(src.sum_d.get(), src.sum_d.n),
            .avg_d => |*d| d.add(src.avg_d.get(), src.avg_d.n),
            .min_d => |*m| if (src.min_d) |sv| {
                if (m.* == null or sv < m.*.?) m.* = sv;
            },
            .max_d => |*m| if (src.max_d) |sv| {
                if (m.* == null or sv > m.*.?) m.* = sv;
            },
        }
    }
};

pub fn addRowCount(a: u64, b: u64) error{RowCountTooLarge}!u64 {
    const sum = std.math.add(u64, a, b) catch return error.RowCountTooLarge;
    if (sum > std.math.maxInt(i64)) return error.RowCountTooLarge;
    return sum;
}

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

    compact: @import("agg_state.zig").State = .{},
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
    fn chargeSpilledSet(self: *GroupTable) error{ExceededMemoryBudget}!void {
        return self.chargePrivateBytes(@sizeOf(DistinctSet));
    }

    fn chargePrivateBytes(self: *GroupTable, n: usize) error{ExceededMemoryBudget}!void {
        var before = @atomicLoad(usize, &self.allocated_bytes, .monotonic);
        while (true) {
            const after = std.math.add(usize, before, n) catch return error.ExceededMemoryBudget;
            if (after > self.max_memory_bytes) return error.ExceededMemoryBudget;
            before = @cmpxchgWeak(usize, &self.allocated_bytes, before, after, .monotonic, .monotonic) orelse break;
        }
        _ = @atomicRmw(usize, &self.set_objects, .Add, n, .monotonic);
    }

    fn releaseSpilledSet(self: *GroupTable) void {
        return self.releasePrivateBytes(@sizeOf(DistinctSet));
    }

    fn releasePrivateBytes(self: *GroupTable, n: usize) void {
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

    fn getOrInsertFixedHashed(
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

    inline fn fixedSlotCount(self: *const GroupTable) usize {
        return self.fixed_capacity;
    }

    inline fn firstSlots(self: *const GroupTable) []FixedSlot {
        return if (self.fixed_stride == 0) self.slots else self.fixed_slots;
    }

    inline fn fixedSlot(self: *const GroupTable, index: usize) *FixedSlot {
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

