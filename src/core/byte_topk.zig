//! Exact SUM or COUNT top-H over complete partitions of owned, length-tagged byte keys.
const std = @import("std");
const eval = @import("filter/eval.zig");
const memory = @import("memory.zig");
const spawn = @import("spawn.zig");
const hw = @import("hw.zig");
const now = @import("../clock.zig").monoNs;

const block_claim = 128;
const none = std.math.maxInt(u32);
const ordinal_limit = 1 << 27;
const sum_present = 1;
const null_identity = 255;
const stack_size = 1 << 20;
const cache_headroom = 64 << 10;
const producer_row_bytes = 128;

pub const Cut = struct { k: usize, descending: bool };
/// SUM ranks one signed INT64 contribution per row. COUNT(*) ranks rows per key and owns no value lane.
pub const Kind = enum { sum, count };

const SumRecord = extern struct {
    key: u128 align(8),
    value: i64,
    ordinal: u32,
    /// Value NULL bit followed by a cached hash. The key identity includes key NULL and exact length.
    flags: u32,
};
const CountRecord = extern struct {
    key: u128 align(8),
    ordinal: u32,
    /// Cached hash in the same bit position as SUM's, above an always-clear value NULL bit.
    flags: u32,
};
pub const SumGroup = struct {
    key: u128 align(8),
    sum: i128 align(8) = 0,
    first: u32,
    flags: u32 = 0,

    pub fn sumValue(g: SumGroup) ?i128 {
        return if (g.flags & sum_present != 0) g.sum else null;
    }
    pub fn rank(g: SumGroup) ?i128 {
        return g.sumValue();
    }
    pub fn keyBytes(g: SumGroup, storage: *[16]u8) ?[]const u8 {
        return unpack(g.key, storage);
    }
};
pub const CountGroup = struct {
    key: u128 align(8),
    count: u64 = 0,
    first: u32,

    pub fn rank(g: CountGroup) ?i128 {
        return g.count;
    }
    pub fn keyBytes(g: CountGroup, storage: *[16]u8) ?[]const u8 {
        return unpack(g.key, storage);
    }
};
comptime {
    std.debug.assert(@sizeOf(SumRecord) == 32 and @sizeOf(SumGroup) == 40);
    std.debug.assert(@sizeOf(CountRecord) == 24 and @sizeOf(CountGroup) == 32);
}

/// No two lengths, NULL or empty strings alias, including keys containing zero bytes.
fn pack(key: ?[]const u8) error{ExceededMemoryBudget}!u128 {
    const k = key orelse return null_identity;
    if (k.len > 15) return error.ExceededMemoryBudget;
    var bytes: [16]u8 = @splat(0);
    if (k.len >= 8) {
        bytes[0..8].* = k[0..8].*;
        bytes[k.len - 8 ..][0..8].* = k[k.len - 8 ..][0..8].*;
    } else if (k.len >= 4) {
        bytes[0..4].* = k[0..4].*;
        bytes[k.len - 4 ..][0..4].* = k[k.len - 4 ..][0..4].*;
    } else for (k, 0..) |b, i| bytes[i] = b;
    bytes[15] = @intCast(k.len);
    return std.mem.readInt(u128, &bytes, .big);
}
fn unpack(key: u128, storage: *[16]u8) ?[]const u8 {
    if (key == null_identity) return null;
    const length: u8 = @truncate(key);
    std.mem.writeInt(u128, storage, key, .big);
    return storage[0..length];
}

fn cacheBudget() usize {
    return hw.l2BytesPerWorkerOr(640 << 10);
}
pub fn batchRows(forced: ?usize) usize {
    if (forced) |n| return if (n == 0) 0 else powerOfTwo(n, 64, 16384);
    // Decoded views, integer/code/level lanes and an emitted record share the producer's cache headroom.
    return powerOfTwo((cacheBudget() -| cache_headroom) / producer_row_bytes, 64, 16384);
}
fn powerOfTwo(n: usize, lo: usize, hi: usize) usize {
    return @as(usize, 1) << @intCast(std.math.log2_int(usize, std.math.clamp(n, lo, hi)));
}

pub const Sum = Topk(.sum);
pub const Count = Topk(.count);

pub fn Topk(comptime kind: Kind) type {
    return struct {
        pub const Record = if (kind == .sum) SumRecord else CountRecord;
        // SUM keeps its sixteen-record block; COUNT fills the same 512-byte write-combining footprint.
        const block_rows = if (kind == .sum) 16 else (512 - 8) / @sizeOf(Record);
        const Block = extern struct { next: u32 = none, len: u32 = 0, rows: [block_rows]Record };
        pub const Group = if (kind == .sum) SumGroup else CountGroup;
        /// Worst-case group scratch plus fewer than four u32 hash slots per input row after rounding.
        const table_row_bytes = @sizeOf(Group) + 16;

        fn worse(descending: bool, a: Group, b: Group) std.math.Order {
            const av = a.rank();
            const bv = b.rank();
            if (av != null and bv == null) return .gt;
            if (av == null and bv != null) return .lt;
            if (av != null) {
                const order = if (descending) std.math.order(av.?, bv.?) else std.math.order(bv.?, av.?);
                if (order != .eq) return order;
            }
            return std.math.order(b.first, a.first);
        }
        const Heap = std.PriorityQueue(Group, bool, worse);

        pub fn partitionCount(rows: usize, workers: usize, producer_rows: usize, forced: ?usize) usize {
            if (forced) |n| return powerOfTwo(if (n == 0) 2048 else n, 64, 16384);
            // A conservative uniform-partition cache estimate; skew changes performance, never key identity.
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
            heads: []u32 = &.{},
            tails: []u32 = &.{},
            histogram: []u32 = &.{},
            free_next: u32 = 0,
            free_end: u32 = 0,

            fn append(l: *Local, key: ?[]const u8, value: if (kind == .sum) ?i64 else void, ordinal: u32) !void {
                const identity = pack(key) catch |e| {
                    l.state.width_refused.store(true, .monotonic);
                    return e;
                };
                const bytes = std.mem.toBytes(std.mem.nativeToBig(u128, identity));
                const hash = std.hash.Wyhash.hash(0, &bytes);
                const r: Record = if (kind == .sum) .{
                    .key = identity,
                    .value = value orelse 0,
                    .ordinal = ordinal,
                    .flags = (@as(u32, @truncate(hash)) << 1) | @intFromBool(value == null),
                } else .{ .key = identity, .ordinal = ordinal, .flags = @as(u32, @truncate(hash)) << 1 };
                const p: usize = @intCast(hash >> l.state.shift);
                l.histogram[p] += 1;
                var id = l.tails[p];
                if (id == none or l.state.blocks[id].len == block_rows) {
                    if (l.free_next == l.free_end) {
                        const at = l.state.block_cursor.fetchAdd(block_claim, .monotonic);
                        if (at >= l.state.blocks.len) return error.ExceededMemoryBudget;
                        l.free_next = at;
                        l.free_end = @intCast(@min(l.state.blocks.len, @as(usize, at) + block_claim));
                    }
                    const fresh = l.free_next;
                    l.free_next += 1;
                    l.state.blocks[fresh].next = none;
                    l.state.blocks[fresh].len = 0;
                    if (id == none) l.heads[p] = fresh else l.state.blocks[id].next = fresh;
                    id = fresh;
                    l.tails[p] = id;
                }
                const b = &l.state.blocks[id];
                b.rows[b.len] = r;
                b.len += 1;
            }

            pub fn batch(l: *Local, view: eval.Batch, lookup: []const ?usize, offset: usize) !void {
                const k = view.cols[lookup[l.state.key_col].?].string;
                if (kind == .count) {
                    for (0..view.num_rows) |i| {
                        const kn = if (k.def_levels) |d| d[i] < k.max_def else false;
                        try l.append(
                            if (kn) null else if (k.codes) |codes| k.dict[codes[i]] else k.values[i],
                            {},
                            l.ordinal_base + @as(u32, @intCast(offset + i)),
                        );
                    }
                    return;
                }
                const v = view.cols[lookup[l.state.value_col].?].i64;
                for (0..view.num_rows) |i| {
                    const kn = if (k.def_levels) |d| d[i] < k.max_def else false;
                    const vn = if (v.def_levels) |d| d[i] < v.max_def else false;
                    try l.append(
                        if (kn) null else if (k.codes) |codes| k.dict[codes[i]] else k.values[i],
                        if (vn) null else if (v.codes) |codes| v.dict[codes[i]] else v.values[i],
                        l.ordinal_base + @as(u32, @intCast(offset + i)),
                    );
                }
            }
        };

        pub const State = struct {
            gpa: std.mem.Allocator,
            ledger: ?*memory.SharedBudget,
            charged: usize,
            output_credit: usize,
            rows: usize,
            key_col: usize,
            /// Unused by COUNT, which reads only the key.
            value_col: usize,
            parts: usize,
            shift: u6,
            cut: Cut,
            blocks: []Block = &.{},
            locals: []Local = &.{},
            heaps: []Heap = &.{},
            block_cursor: std.atomic.Value(u32) = .init(0),
            width_refused: std.atomic.Value(bool) = .init(false),
            group_count: usize = 0,
            fold_ns: u64 = 0,
            release_ns: u64 = 0,
            select_ns: u64 = 0,

            fn blockCount(rows: usize, workers: usize, parts: usize) usize {
                return (rows + block_rows - 1) / block_rows + workers * (parts + block_claim);
            }
            pub fn reservation(rows: usize, workers: usize, parts: usize, k: usize, output_credit: usize) usize {
                return blockCount(rows, workers, parts) *| @sizeOf(Block) +|
                    rows *| table_row_bytes +| workers *| (stack_size + @sizeOf(Local) + @sizeOf(Heap) + @sizeOf(Task)) +|
                    workers *| parts *| 3 *| @sizeOf(u32) +| workers *| k *| @sizeOf(Group) +|
                    output_credit +| @sizeOf(State) +| (8 << 20);
            }
            pub fn init(
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
                if (rows >= ordinal_limit or workers == 0 or workers > 64 or cut.k == 0 or cut.k > 1024 or
                    parts < 64 or parts > 16384 or !std.math.isPowerOfTwo(parts)) return null;
                const charge = reservation(rows, workers, parts, cut.k, output_credit);
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
                s.heaps = try a.alloc(Heap, workers);
                for (s.heaps) |*h| h.* = .initContext(cut.descending);
                for (s.heaps) |*h| {
                    const storage = try a.alloc(Group, cut.k);
                    h.items = storage[0..0];
                    h.cap = storage.len;
                }
                s.blocks = try a.alloc(Block, blockCount(rows, workers, parts));
                for (s.locals) |*l| {
                    l.heads = try a.alloc(u32, parts);
                    l.tails = try a.alloc(u32, parts);
                    l.histogram = try a.alloc(u32, parts);
                    @memset(l.heads, none);
                    @memset(l.tails, none);
                    @memset(l.histogram, 0);
                }
                return s;
            }
            pub fn deinit(s: *State) void {
                const a = s.gpa;
                for (s.locals) |l| {
                    a.free(l.heads);
                    a.free(l.tails);
                    a.free(l.histogram);
                }
                a.free(s.locals);
                for (s.heaps) |*h| h.deinit(a);
                a.free(s.heaps);
                a.free(s.blocks);
                if (s.ledger) |l| l.release(s.charged);
                a.destroy(s);
            }

            fn keep(s: *State, heap: *Heap, g: Group) void {
                if (heap.count() == s.cut.k) {
                    if (worse(s.cut.descending, g, heap.peek().?) != .gt) return;
                    _ = heap.pop();
                }
                heap.push(s.gpa, g) catch unreachable;
            }
            const Task = struct {
                s: *State,
                heap: *Heap,
                cursor: *std.atomic.Value(usize),
                groups: []Group = &.{},
                slots: []u32 = &.{},
                group_count: usize = 0,
                err: ?anyerror = null,

                fn ensure(t: *Task, n: usize, cap: usize) !void {
                    if (t.groups.len < n) {
                        t.s.gpa.free(t.groups);
                        t.groups = &.{};
                        t.groups = try t.s.gpa.alloc(Group, n);
                    }
                    if (t.slots.len < cap) {
                        t.s.gpa.free(t.slots);
                        t.slots = &.{};
                        t.slots = try t.s.gpa.alloc(u32, cap);
                    }
                }
                fn fold(t: *Task, p: usize) !void {
                    const s = t.s;
                    var rows: usize = 0;
                    for (s.locals) |l| rows += l.histogram[p];
                    if (rows == 0) return;
                    const cap = try std.math.ceilPowerOfTwo(usize, rows * 2);
                    try t.ensure(rows, cap);
                    const slots = t.slots[0..cap];
                    @memset(slots, 0);
                    var n: usize = 0;
                    for (s.locals) |l| {
                        var id = l.heads[p];
                        while (id != none) {
                            const b = &s.blocks[id];
                            if (b.next != none) @prefetch(&s.blocks[b.next], .{ .rw = .read, .locality = 1 });
                            for (b.rows[0..b.len]) |r| {
                                var pos: usize = (r.flags >> 1) & (cap - 1);
                                while (slots[pos] != 0) : (pos = (pos + 1) & (cap - 1)) {
                                    if (t.groups[slots[pos] - 1].key == r.key) break;
                                }
                                if (slots[pos] == 0) {
                                    slots[pos] = @intCast(n + 1);
                                    t.groups[n] = .{ .key = r.key, .first = r.ordinal };
                                    n += 1;
                                }
                                const g = &t.groups[slots[pos] - 1];
                                g.first = @min(g.first, r.ordinal);
                                if (kind == .count) {
                                    g.count += 1;
                                } else if (r.flags & 1 == 0) {
                                    g.sum += r.value;
                                    g.flags |= sum_present;
                                }
                            }
                            id = b.next;
                        }
                    }
                    // Every key is complete in this owner partition before it can be discarded. A worker's top-H
                    // across its completed partitions subsumes every partition's top-H, so their union is sufficient.
                    for (t.groups[0..n]) |g| s.keep(t.heap, g);
                    t.group_count += n;
                }
                fn run(t: *Task) void {
                    defer t.s.gpa.free(t.groups);
                    defer t.s.gpa.free(t.slots);
                    while (true) {
                        const p = t.cursor.fetchAdd(1, .monotonic);
                        if (p >= t.s.parts) return;
                        t.fold(p) catch |e| {
                            t.err = e;
                            return;
                        };
                    }
                }
            };

            pub fn finish(s: *State) !void {
                var start = now();
                var contributed: usize = 0;
                for (s.locals) |l| for (l.histogram) |n| {
                    contributed += n;
                };
                if (contributed != s.rows) return error.BadRowCount;
                const tasks = try s.gpa.alloc(Task, s.locals.len);
                defer s.gpa.free(tasks);
                var cursor: std.atomic.Value(usize) = .init(0);
                for (tasks, s.heaps) |*t, *h| t.* = .{ .s = s, .heap = h, .cursor = &cursor };
                if (tasks.len == 1 or !spawn.runTree(Task, tasks, .{ .stack_size = stack_size }, Task.run))
                    Task.run(&tasks[0]);
                for (tasks) |t| {
                    if (t.err) |e| return e;
                    s.group_count += t.group_count;
                }
                s.fold_ns = @intCast(now() - start);
                start = now();
                s.gpa.free(s.blocks);
                s.blocks = &.{};
                s.release_ns = @intCast(now() - start);
                start = now();
                for (s.heaps[1..]) |other| for (other.items) |g| s.keep(&s.heaps[0], g);
                // The normal stable result sorter receives the surviving H rows in canonical first-appearance order.
                std.mem.sort(Group, s.heaps[0].items, {}, struct {
                    fn less(_: void, a: Group, b: Group) bool {
                        return a.first < b.first;
                    }
                }.less);
                s.select_ns = @intCast(now() - start);
            }
            pub fn top(s: *const State) []const Group {
                return s.heaps[0].items;
            }
        };

    };
}

fn trial(comptime kind: Kind, a: std.mem.Allocator, workers: usize, descending: bool, parts: usize, collisions: bool) !void {
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
    const T = Topk(kind);
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
        if (kind == .count) {
            refs[at].sum = (refs[at].sum orelse 0) + 1;
        } else if (v) |value| refs[at].sum = (refs[at].sum orelse 0) + value;
        try s.locals[(rows - i) % workers].append(key, if (kind == .sum) v else {}, @intCast(i));
    }
    if (collisions) for (s.locals) |l| for (l.heads) |head| {
        var id = head;
        while (id != none) {
            const b = &s.blocks[id];
            for (b.rows[0..b.len]) |*r| r.flags &= 1;
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
    for (s.top(), want) |actual, expected| {
        var storage: [16]u8 = undefined;
        const key = actual.keyBytes(&storage);
        if (expected.key) |value| {
            try std.testing.expectEqualSlices(u8, value, key.?);
        } else try std.testing.expect(key == null);
        try std.testing.expectEqual(expected.sum, actual.rank());
        try std.testing.expectEqual(expected.first, actual.first);
    }
}

test "packed byte SUM and COUNT cuts preserve full identities, wide sums and first ties across workers and partitions" {
    inline for (.{ Kind.sum, Kind.count }) |kind| {
        for ([_]usize{ 1, 2, 4, 8, 16 }) |workers| for ([_]bool{ false, true }) |descending|
            for ([_]usize{ 64, 256, 2048 }) |parts| try trial(kind, std.testing.allocator, workers, descending, parts, false);
        for ([_]usize{ 1, 16 }) |workers| try trial(kind, std.testing.allocator, workers, true, 64, true);
        for ([_]usize{ 0, 2 }) |after| {
            spawn.injectFailureAfter(after);
            defer spawn.resetFailure();
            try trial(kind, std.testing.allocator, 16, true, 256, false);
        }
    }
}
test "packed byte SUM and COUNT cuts release all partial allocations and declines an unfunded peak" {
    const Kinds = struct {
        fn sum(a: std.mem.Allocator) !void {
            try trial(.sum, a, 1, true, 64, false);
        }
        fn count(a: std.mem.Allocator) !void {
            try trial(.count, a, 1, false, 64, false);
        }
    };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Kinds.sum, .{});
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Kinds.count, .{});
    var budget: memory.SharedBudget = .{ .limit = 1 };
    const result = try Sum.State.init(std.testing.allocator, &budget, 1, 1, 0, 1, .{ .k = 1, .descending = true }, 64, 0);
    try std.testing.expect(result == null);
    const counted = try Count.State.init(std.testing.allocator, &budget, 1, 1, 0, 0, .{ .k = 1, .descending = true }, 64, 0);
    try std.testing.expect(counted == null);
    try std.testing.expectEqual(@as(usize, 0), budget.used.load(.monotonic));
}
test "packed byte identity distinguishes NULL, empty and embedded zero bytes and declines sixteen bytes" {
    try std.testing.expect(try pack(null) != try pack(""));
    try std.testing.expect(try pack("") != try pack("\x00"));
    try std.testing.expect(try pack("a") != try pack("a\x00"));
    for (0..16) |n| {
        const bytes: [15]u8 = @splat(0);
        const key = try pack(bytes[0..n]);
        try std.testing.expectEqual(@as(u8, @intCast(n)), @as(u8, @truncate(key)));
    }
    try std.testing.expectError(error.ExceededMemoryBudget, pack("0123456789abcdef"));
}
test "packed byte SUM cache policy uses shared hardware and supports every forced variant" {
    defer hw.test_cpu = null;
    hw.test_cpu = .{ .l2 = .{ .bytes = 1 << 20, .max_sharing = 2 } };
    const small = Sum.partitionCount(30000000, 16, batchRows(null), null);
    const small_batch = batchRows(null);
    hw.test_cpu = .{ .l2 = .{ .bytes = 4 << 20, .max_sharing = 2 } };
    try std.testing.expect(Sum.partitionCount(30000000, 16, batchRows(null), null) < small);
    try std.testing.expect(batchRows(null) > small_batch);
    try std.testing.expectEqual(@as(usize, 2048), Sum.partitionCount(30000000, 16, 4096, 0));
    try std.testing.expectEqual(@as(usize, 0), batchRows(0));
    try std.testing.expectEqual(@as(usize, 64), Sum.partitionCount(30000000, 16, 4096, 1));
    try std.testing.expectEqual(@as(usize, 16384), Sum.partitionCount(30000000, 16, 4096, 999999));
    for ([_]usize{ 64, 1024, 8192, 16384 }) |parts| {
        try trial(.sum, std.testing.allocator, 1, true, parts, false);
        try trial(.count, std.testing.allocator, 1, true, parts, false);
    }
    // COUNT records and groups are narrower, so its uncapped table-sized fanout never exceeds SUM's.
    try std.testing.expect(Count.partitionCount(30000000, 12, 4096, null) <= Sum.partitionCount(30000000, 12, 4096, null));

    hw.test_cpu = .{ .l2 = .{ .bytes = 1280 << 10, .max_sharing = 2 }, .l3 = .{ .bytes = 30 << 20 } };
    try std.testing.expectEqual(@as(usize, 4096), Sum.partitionCount(30000000, 1, 4096, null));
    try std.testing.expectEqual(@as(usize, 2048), Sum.partitionCount(30000000, 12, 4096, null));
    try std.testing.expectEqual(@as(usize, 4096), Sum.partitionCount(30000000, 64, 4096, null));
    try std.testing.expectEqual(@as(usize, 4096), Sum.partitionCount(30000000, 12, 0, null));
    try std.testing.expectEqual(@as(usize, 4096), Sum.partitionCount(30000000, 12, 65536, null));
    hw.test_cpu.?.l3.bytes = 4 << 20;
    try std.testing.expectEqual(@as(usize, 4096), Sum.partitionCount(30000000, 1, 4096, null));
    hw.test_cpu.?.l3.bytes = 0;
    try std.testing.expectEqual(@as(usize, 4096), Sum.partitionCount(30000000, 12, 4096, null));
    hw.test_cpu = .{ .l2 = .{ .bytes = 1 << 20, .max_sharing = 2 }, .l3 = .{ .bytes = 32 << 20 } };
    try std.testing.expectEqual(@as(usize, 2048), Sum.partitionCount(30000000, 16, batchRows(null), null));
}
