const std = @import("std");
const testing = std.testing;
const filter_eval = @import("../../filter/eval.zig");
const SharedBudget = @import("../../memory.zig").SharedBudget;
const DecSum = @import("types.zig").DecSum;
const AggCall = @import("types.zig").AggCall;
const GroupTable = @import("group_table.zig").GroupTable;
const isRowNull = @import("keys.zig").isRowNull;
const keyEql = @import("../../bytes.zig").keyEql;

pub const NUM_PARTS: usize = 64;
pub const NUMERIC_PARTS: usize = 16;

/// A value's partition, from the middle of its hash: each partition's map takes its fingerprint from the top bits and
/// its slot from the low ones, and fixing either inside a partition would make its probes collide more.
pub inline fn partOf(h: u64, parts: usize) usize {
    return @intCast((h >> 32) & (parts - 1));
}

pub const INLINE_CAP: usize = 2;
pub const InlineU64 = struct { len: u8, vals: [INLINE_CAP]u64 };
pub const InlineI128 = struct { len: u8, vals: [INLINE_CAP]i128 };
pub const InlineBytes = struct { len: u8, vals: [INLINE_CAP][]const u8 };

/// Hashes travel with owned string keys, so partition routing, table growth and merge do not rescan bytes.
pub const DistinctBytes = struct {
    const Key = struct { bytes: []const u8, hash: u64 };
    const Context = struct {
        pub fn hash(_: @This(), key: Key) u64 {
            return key.hash;
        }
        pub fn eql(_: @This(), a: Key, b: Key) bool {
            return a.hash == b.hash and keyEql(a.bytes, b.bytes);
        }
    };
    const Map = std.HashMapUnmanaged(Key, void, Context, std.hash_map.default_max_load_percentage);
    map: Map = .empty,
    pub const empty: DistinctBytes = .{};
    pub fn count(s: DistinctBytes) u32 {
        return s.map.count();
    }
    pub fn deinit(s: *DistinctBytes, a: std.mem.Allocator) void {
        s.map.deinit(a);
    }
    pub fn ensureTotalCapacity(s: *DistinctBytes, a: std.mem.Allocator, n: u32) !void {
        try s.map.ensureTotalCapacity(a, n);
    }
    pub fn ensureUnusedCapacity(s: *DistinctBytes, a: std.mem.Allocator, n: u32) !void {
        try s.map.ensureUnusedCapacity(a, n);
    }
    pub fn putAssumeCapacity(s: *DistinctBytes, bytes: []const u8, _: void) void {
        s.putHashed(bytes, std.hash.Wyhash.hash(0, bytes));
    }
    fn putHashed(s: *DistinctBytes, bytes: []const u8, hash: u64) void {
        s.map.putAssumeCapacity(.{ .bytes = bytes, .hash = hash }, {});
    }
    const Result = struct { key_ptr: *[]const u8, found_existing: bool };
    fn getHashedAssumeCapacity(s: *DistinctBytes, bytes: []const u8, hash: u64) Result {
        const got = s.map.getOrPutAssumeCapacity(.{ .bytes = bytes, .hash = hash });
        return .{ .key_ptr = &got.key_ptr.bytes, .found_existing = got.found_existing };
    }
    fn removeHashed(s: *DistinctBytes, bytes: []const u8, hash: u64) void {
        _ = s.map.remove(.{ .bytes = bytes, .hash = hash });
    }
    const KeyIterator = struct {
        inner: Map.KeyIterator,
        pub fn next(it: *@This()) ?*[]const u8 {
            const key = it.inner.next() orelse return null;
            return &key.bytes;
        }
    };
    pub fn keyIterator(s: *const DistinctBytes) KeyIterator {
        return .{ .inner = s.map.keyIterator() };
    }
};
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

test "inline distinct matches a set, spills at the fourth value, and charges that object" {
    const a = testing.allocator;
    var budget = SharedBudget{ .limit = 1 << 20, .block = 1 };
    var acc = DistinctAcc.initEmpty(&budget);
    defer acc.deinit(a);
    const used0 = budget.used.load(.monotonic);
    try acc.insertU64(a, 10);
    try acc.insertU64(a, 10);
    try acc.insertU64(a, 20);
    try acc.insertU64(a, 30);
    try std.testing.expect(acc.heap() == null);
    try std.testing.expectEqual(@as(usize, 3), acc.count());
    try std.testing.expectEqual(used0, budget.used.load(.monotonic));

    try acc.insertU64(a, 40);
    try std.testing.expect(acc.heap() != null);
    try std.testing.expectEqual(@as(usize, 4), acc.count());
    try std.testing.expect(acc.heap().?.shared == &budget);
    try std.testing.expect(budget.used.load(.monotonic) >= used0 + @sizeOf(DistinctSet));
    try std.testing.expect(acc.heap().?.allocated_bytes >= @sizeOf(DistinctSet));

    // Same values through a set: the count agrees, duplicates included.
    var set = DistinctSet{};
    defer set.deinit(a);
    for ([_]u64{ 10, 10, 20, 30, 40, 40 }) |v| try set.insertU64(a, v);
    try std.testing.expectEqual(set.count(), acc.count());
}

test "inline distinct merge stays exact below and above the spill" {
    const a = testing.allocator;
    var dst = DistinctAcc.initEmpty(null);
    defer dst.deinit(a);
    var src = DistinctAcc.initEmpty(null);
    defer src.deinit(a);
    try dst.insertU64(a, 1);
    try dst.insertU64(a, 2);
    try src.insertU64(a, 2);
    try src.insertU64(a, 3);
    try dst.takeFrom(&src, a);
    try std.testing.expect(dst.heap() == null);
    try std.testing.expectEqual(@as(usize, 3), dst.count());
    try std.testing.expectEqual(@as(usize, 0), src.count());

    var more = DistinctAcc.initEmpty(null);
    defer more.deinit(a);
    try more.insertU64(a, 4);
    try dst.takeFrom(&more, a);
    try std.testing.expect(dst.heap() != null);
    try std.testing.expectEqual(@as(usize, 4), dst.count());
}

test "inline distinct: allocation failure while spilling leaves no leak" {
    try testing.checkAllAllocationFailures(testing.allocator, struct {
        fn run(gpa: std.mem.Allocator) !void {
            var acc = DistinctAcc.initEmpty(null);
            defer acc.deinit(gpa);
            for (0..6) |i| try acc.insertU64(gpa, i);
            try testing.expectEqual(@as(usize, 6), acc.count());
            var bytes = DistinctAcc.initEmpty(null);
            defer bytes.deinit(gpa);
            const vals = [_][]const u8{ "a", "b", "a" };
            for (vals) |v| try bytes.insertBytes(gpa, v);
            try testing.expectEqual(@as(usize, 2), bytes.count());
        }
    }.run, .{});
}

test "inline distinct: a budget that fits three values refuses the spill" {
    const a = testing.allocator;
    // Three inline values draw nothing. The spill needs the set object; a ceiling under that fails and keeps the three.
    var budget = SharedBudget{ .limit = @sizeOf(DistinctSet) / 2, .block = 1 };
    var acc = DistinctAcc.initEmpty(&budget);
    defer acc.deinit(a);
    try acc.insertU64(a, 1);
    try acc.insertU64(a, 2);
    try acc.insertU64(a, 3);
    try std.testing.expectError(error.ExceededMemoryBudget, acc.insertU64(a, 4));
    try std.testing.expect(acc.heap() == null);
    try std.testing.expectEqual(@as(usize, 3), acc.count());
    try std.testing.expectEqual(@as(usize, 0), budget.used.load(.monotonic));
}

test "inline distinct canonicalizes float nan and zero like DistinctSet" {
    const a = testing.allocator;
    var acc = DistinctAcc.initEmpty(null);
    defer acc.deinit(a);
    var set = DistinctSet{};
    defer set.deinit(a);
    const vals = [_]f64{ 1.0, -0.0, 0.0, std.math.nan(f64), -std.math.nan(f64), 1.0 };
    for (vals) |v| {
        try acc.insertF64(a, v);
        try set.insertF64(a, v);
    }
    try std.testing.expectEqual(set.count(), acc.count());
    try std.testing.expect(acc.heap() == null);
}


test "distinct bitmaps preserve complete identities across ranges, partitions and late wide values with failures" {
    try std.testing.checkAllAllocationFailures(std.testing.allocator, distinctBitmapTrial, .{});
}

fn distinctBitmapTrial(a: std.mem.Allocator) !void {
    const hw = @import("../../hw.zig");
    hw.test_cpu = .{ .l2 = .{ .bytes = 64 << 10, .max_sharing = 1 } };
    defer hw.test_cpu = null;
    for ([_]bool{ false, true }) |partitioned| {
        var budget: SharedBudget = .{ .limit = 1 << 20 };
        {
            var left: DistinctSet = .{ .shared = &budget };
            defer left.deinit(a);
            var right: DistinctSet = .{ .shared = &budget };
            defer right.deinit(a);
            if (partitioned) {
                try left.initPartitions(a, &budget);
                try right.initPartitions(a, &budget);
            }
            // Different aligned bases and overlap. OR must preserve identity, never add cardinalities.
            for (128..384) |v| try left.insertU64(a, v);
            for (0..256) |v| try right.insertU64(a, v);
            if (!partitioned) {
                try testing.expect(left.val_set == .bitmap);
                try testing.expect(right.val_set == .bitmap);
            }
            try left.merge(&right, a);
            try testing.expectEqual(@as(usize, 384), left.count());
            try left.merge(&right, a);
            try testing.expectEqual(@as(usize, 384), left.count());
            // A late sparse value migrates the whole set without another input pass.
            try left.insertU64(a, @as(u64, 1) << 48);
            try testing.expectEqual(@as(usize, 385), left.count());
            if (!partitioned) try testing.expect(left.val_set == .u64);
            for (0..384) |v| try left.insertU64(a, v);
            try testing.expectEqual(@as(usize, 385), left.count());
            var empty: DistinctSet = .{ .shared = &budget };
            defer empty.deinit(a);
            try empty.merge(&right, a);
            try empty.merge(&left, a);
            try testing.expectEqual(@as(usize, 385), empty.count());
            var negative: DistinctSet = .{ .shared = &budget };
            defer negative.deinit(a);
            for (0..128) |i| try negative.insertU64(a, std.math.maxInt(u64) - i);
            try testing.expect(negative.val_set == .bitmap);
            try testing.expectEqual(std.math.maxInt(u64), negative.val_set.bitmap.last());
            try negative.merge(&right, a);
            try testing.expectEqual(@as(usize, 384), negative.count());
            try testing.expect(negative.val_set == .u64);
        }
        try testing.expectEqual(@as(usize, 0), budget.used.load(.monotonic));
    }
}
test "round 4: detach clears an inline budget" {
    const a = testing.allocator;
    var budget = SharedBudget{ .limit = 4096, .block = 1 };
    var acc = DistinctAcc.initEmpty(&budget);
    defer acc.deinit(a);
    try acc.insertU64(a, 1);
    acc.detach();
    // The count stays. A later spill must not draw on the budget this call just gave up.
    try testing.expect(acc.budgetPtr() == null);
    try testing.expectEqual(@as(usize, 1), acc.count());
    try acc.insertU64(a, 2);
    try acc.insertU64(a, 3);
    try acc.insertU64(a, 4);
    try testing.expectEqual(@as(usize, 0), budget.used.load(.monotonic));
}

test "round 4: private table ceiling includes a spilled object" {
    const a = testing.allocator;
    const calls = [_]AggCall{
        .{ .func = .count_distinct, .arg = null, .where = null, .alias = "d", .result = .i64 },
    };
    for ([_]u8{ 0, 9 }) |stride| {
        var t = GroupTable.init(a, 1 << 20);
        t.fixed_stride = stride;
        defer t.deinit();
        var image: [9]u8 = @splat(0);
        image[0] = 1;
        const gid = if (stride == 0)
            try t.getOrInsert("a", &calls, 0)
        else
            try t.getOrInsertFixed(&image, &calls, 0);
        // Clamped after the group exists, so the spill itself has to see the live ceiling.
        t.max_memory_bytes = t.allocated_bytes;
        const slot = &t.accumulators.items[gid].distinct;
        for (0..3) |v| try slot.insertU64(a, v);
        try testing.expectError(error.ExceededMemoryBudget, slot.insertU64(a, 3));
        try testing.expect(slot.heap() == null);
        try testing.expectEqual(@as(usize, 3), slot.count());
        try testing.expectEqual(t.max_memory_bytes, t.allocated_bytes);
    }
}

test "round 4: a spilled object stays inside a private ceiling after the next insert" {
    const a = testing.allocator;
    const calls = [_]AggCall{
        .{ .func = .count_distinct, .arg = null, .where = null, .alias = "d", .result = .i64 },
    };
    for ([_]u8{ 0, 9 }) |stride| {
        var t = GroupTable.init(a, 1 << 20);
        t.fixed_stride = stride;
        defer t.deinit();
        var image: [9]u8 = @splat(0);
        image[0] = 1;
        const gid = if (stride == 0)
            try t.getOrInsert("a", &calls, 0)
        else
            try t.getOrInsertFixed(&image, &calls, 0);
        const before = t.allocated_bytes;
        for (0..4) |v| try t.accumulators.items[gid].distinct.insertU64(a, v);
        const set_credit = t.accumulators.items[gid].distinct.heap().?.max_memory_bytes;
        try testing.expect(set_credit > 0);
        try testing.expectEqual(before + @sizeOf(DistinctSet) + set_credit, t.allocated_bytes);
        image[0] = 2;
        _ = if (stride == 0)
            try t.getOrInsert("b", &calls, 1)
        else
            try t.getOrInsertFixed(&image, &calls, 1);
        try testing.expect(t.allocated_bytes >= before + @sizeOf(DistinctSet));
        const held = t.allocated_bytes;
        t.accumulators.items[gid].distinct.deinit(a);
        try testing.expectEqual(held - @sizeOf(DistinctSet) - set_credit, t.allocated_bytes);
    }
}

test "round 4: shared budget still owns the spilled object, not the table" {
    const a = testing.allocator;
    const calls = [_]AggCall{
        .{ .func = .count_distinct, .arg = null, .where = null, .alias = "d", .result = .i64 },
    };
    var budget = SharedBudget{ .limit = 1 << 20, .block = 1 };
    for ([_]u8{ 0, 9 }) |stride| {
        var t = GroupTable.init(a, 0);
        t.fixed_stride = stride;
        t.shared = &budget;
        defer t.deinit();
        var image: [9]u8 = @splat(0);
        image[0] = 1;
        const gid = if (stride == 0)
            try t.getOrInsert("a", &calls, 0)
        else
            try t.getOrInsertFixed(&image, &calls, 0);
        const table_before = t.allocated_bytes;
        const used_before = budget.used.load(.monotonic);
        for (0..4) |v| try t.accumulators.items[gid].distinct.insertU64(a, v);
        try testing.expectEqual(table_before, t.allocated_bytes);
        try testing.expect(budget.used.load(.monotonic) >= used_before + @sizeOf(DistinctSet));
    }
}

test "round 4: owned merges preserve reservations on allocation failure" {
    const Helper = struct {
        fn run(a: std.mem.Allocator, dn: usize, sn: usize) !void {
            var budget = SharedBudget{ .limit = 1 << 20, .block = 1 };
            defer testing.expectEqual(@as(usize, 0), budget.used.load(.monotonic)) catch
                @panic("reservation leaked");
            var src = DistinctAcc.initEmpty(&budget);
            defer src.deinit(a);
            var dst = DistinctAcc.initEmpty(&budget);
            defer dst.deinit(a);
            for (0..dn) |v| try dst.insertU64(a, v);
            for (2..2 + sn) |v| try src.insertU64(a, v);
            try dst.takeFrom(&src, a);
            const overlap = if (dn > 2) @min(dn - 2, sn) else 0;
            try testing.expectEqual(dn + sn - overlap, dst.count());
            try testing.expectEqual(@as(usize, 0), src.count());
            const reserved = if (dst.heap()) |d| d.max_memory_bytes else 0;
            try testing.expectEqual(reserved, budget.used.load(.monotonic));
        }
    };
    // A failed grow for a duplicate can legitimately return the existing hash-map entry.
    for (0..8) |dn| for (0..8) |sn| for (0..16) |fail_index| {
        var failing = testing.FailingAllocator.init(testing.allocator, .{ .fail_index = fail_index });
        Helper.run(failing.allocator(), dn, sn) catch |err| if (err != error.OutOfMemory) return err;
        try testing.expectEqual(failing.allocated_bytes, failing.freed_bytes);
    };
}
