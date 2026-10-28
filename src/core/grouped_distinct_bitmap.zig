//! Exact (group, bounded integer) sets. Larger matrices route compact rows to disjoint cache-sized owners.
const std = @import("std");
const spawn = @import("spawn.zig");
const blocking = @import("blocking.zig");

pub const Mode = enum { auto, private, partitioned, off };
pub const none: u32 = std.math.maxInt(u32);
const Record = extern struct { group: u32, value: u32, ordinal: u32 };
const Length = struct { value: usize align(64) = 0 };
const Partition = struct { mutex: std.Io.Mutex align(64) = .init };

pub const Geometry = struct {
    groups: usize,
    lo: i64,
    width: usize,
    words: usize,
    workers: usize,
    private: bool,
    group_shift: std.math.Log2Int(usize),
    parts: usize,
    batch_rows: usize,

    pub fn plan(groups: usize, lo: i64, hi: i64, workers: usize, l2: usize, l3: usize, mode: Mode) ?Geometry {
        if (mode == .off or groups == 0 or groups > none or workers == 0 or lo > hi) return null;
        const width = std.math.cast(usize, @as(i128, hi) - lo + 1) orelse return null;
        // The record's last value image is reserved for NULL, which still carries group presence.
        if (width == 0 or width > none) return null;
        const words = std.math.divCeil(usize, width, 64) catch return null;
        const row_bytes = std.math.mul(usize, words, @sizeOf(u64)) catch return null;
        const matrix = std.math.mul(usize, row_bytes, groups) catch return null;
        if (mode == .auto and (l2 == 0 or l3 == 0 or matrix > l3 / 2)) return null;
        const private = mode == .private or (mode == .auto and matrix <= l2 / 2);
        const per_part = @max(1, l2 / 2 / row_bytes);
        const power = std.math.floorPowerOfTwo(usize, @min(groups, per_part));
        const shift: std.math.Log2Int(usize) = @intCast(@ctz(power));
        const parts = if (private) 0 else (groups - 1) / power + 1;
        return .{
            .groups = groups,
            .lo = lo,
            .width = width,
            .words = words,
            .workers = workers,
            .private = private,
            .group_shift = shift,
            .parts = parts,
            .batch_rows = if (private) 0 else std.math.floorPowerOfTwo(usize, @max(1, l2 / 4 / parts / @sizeOf(Record))),
        };
    }

    pub fn index(g: Geometry, value: ?i64) error{BitmapDomainDeclined}!u32 {
        const v = value orelse return none;
        const offset = @as(i128, v) - g.lo;
        if (offset < 0 or offset >= g.width) return error.BitmapDomainDeclined;
        return @intCast(offset);
    }

    pub fn bytes(g: Geometry) ?usize {
        const copies: usize = if (g.private) g.workers else 1;
        var n = std.math.mul(usize, g.groups, g.words) catch return null;
        n = std.math.mul(usize, n, copies * @sizeOf(u64)) catch return null;
        n = std.math.add(usize, n, g.groups *| (copies *| @sizeOf(u32) +| @sizeOf(u64))) catch return null;
        n = std.math.add(usize, n, g.workers *| @sizeOf(State.Task)) catch return null;
        if (!g.private) {
            const buffers = std.math.mul(usize, g.workers, g.parts) catch return null;
            const entries = std.math.mul(usize, buffers, g.batch_rows) catch return null;
            const payload = std.math.mul(usize, entries, @sizeOf(Record)) catch return null;
            n = std.math.add(usize, n, payload) catch return null;
            n = std.math.add(usize, n, buffers *| @sizeOf(Length)) catch return null;
            n = std.math.add(usize, n, g.parts *| @sizeOf(Partition)) catch return null;
        }
        return n;
    }
};

/// `a` must be a query-ledger allocator. No allocation or borrowed decoder view survives deinit.
pub const State = struct {
    a: std.mem.Allocator,
    geometry: Geometry,
    bits: []u64,
    first: []u32,
    counts: []u64,
    buffer: []align(64) Record,
    lengths: []Length,
    partitions: []Partition,
    tasks: []Task,

    pub fn init(a: std.mem.Allocator, g: Geometry) !State {
        _ = g.bytes() orelse return error.BitmapDomainDeclined;
        const copies: usize = if (g.private) g.workers else 1;
        const bits = try a.alloc(u64, g.groups * g.words * copies);
        errdefer a.free(bits);
        @memset(bits, 0);
        const first = try a.alloc(u32, g.groups * copies);
        errdefer a.free(first);
        @memset(first, none);
        const counts = try a.alloc(u64, g.groups);
        errdefer a.free(counts);
        @memset(counts, 0);
        const buffers = g.workers * g.parts;
        const buffer = try a.alignedAlloc(Record, .@"64", buffers * g.batch_rows);
        errdefer a.free(buffer);
        const lengths = try a.alloc(Length, buffers);
        errdefer a.free(lengths);
        @memset(lengths, .{});
        const partitions = try a.alloc(Partition, g.parts);
        errdefer a.free(partitions);
        @memset(partitions, .{});
        const tasks = try a.alloc(Task, g.workers);
        return .{
            .a = a,
            .geometry = g,
            .bits = bits,
            .first = first,
            .counts = counts,
            .buffer = buffer,
            .lengths = lengths,
            .partitions = partitions,
            .tasks = tasks,
        };
    }

    pub fn deinit(s: *State) void {
        s.a.free(s.partitions);
        s.a.free(s.lengths);
        s.a.free(s.buffer);
        s.a.free(s.counts);
        s.a.free(s.first);
        s.a.free(s.bits);
        s.a.free(s.tasks);
        s.* = undefined;
    }

    pub inline fn add(s: *State, worker: usize, group: u32, value: u32, ordinal: u32) !void {
        const g = s.geometry;
        if (group >= g.groups or worker >= g.workers or (value != none and value >= g.width))
            return error.BitmapDomainDeclined;
        if (g.private) {
            const slot = worker * g.groups + group;
            // Presence is usually already known; avoid a store to adjacent workers' first-ordinal cache lines.
            if (ordinal < s.first[slot]) s.first[slot] = ordinal;
            if (value != none) s.bits[slot * g.words + value / 64] |= @as(u64, 1) << @intCast(value % 64);
        } else {
            const part = @as(usize, group) >> g.group_shift;
            const slot = worker * g.parts + part;
            const len = &s.lengths[slot].value;
            s.buffer[slot * g.batch_rows + len.*] = .{ .group = group, .value = value, .ordinal = ordinal };
            len.* += 1;
            if (len.* == g.batch_rows) s.flush(part, slot);
        }
    }

    fn apply(s: *State, rows: []const Record) void {
        const g = s.geometry;
        for (rows) |r| {
            if (r.ordinal < s.first[r.group]) s.first[r.group] = r.ordinal;
            if (r.value != none)
                s.bits[@as(usize, r.group) * g.words + r.value / 64] |= @as(u64, 1) << @intCast(r.value % 64);
        }
    }

    fn flush(s: *State, part: usize, slot: usize) void {
        const mutex = &s.partitions[part].mutex;
        // Match the shared-partition merge: brief spinning, then sleeping supplies bounded producer backpressure.
        for (0..64) |_| {
            if (mutex.tryLock()) break;
            std.atomic.spinLoopHint();
        } else mutex.lockUncancelable(blocking.io);
        defer mutex.unlock(blocking.io);
        const len = &s.lengths[slot].value;
        s.apply(s.buffer[slot * s.geometry.batch_rows ..][0..len.*]);
        len.* = 0;
    }

    pub fn finish(s: *State, stack_size: usize) void {
        var cursor: std.atomic.Value(usize) = .init(0);
        for (s.tasks) |*t| t.* = .{ .state = s, .cursor = &cursor };
        if (s.tasks.len == 1 or !spawn.runTree(Task, s.tasks, .{ .stack_size = stack_size }, Task.run))
            Task.run(&s.tasks[0]);
    }

    const Task = struct {
        state: *State,
        cursor: *std.atomic.Value(usize),

        fn run(t: *Task) void {
            const s = t.state;
            const g = s.geometry;
            const n = if (g.private) g.groups else g.parts;
            while (true) {
                const at = t.cursor.fetchAdd(1, .monotonic);
                if (at >= n) return;
                if (g.private) {
                    var first = none;
                    for (0..g.workers) |w| first = @min(first, s.first[w * g.groups + at]);
                    var count: u64 = 0;
                    for (0..g.words) |word| {
                        var union_bits: u64 = 0;
                        for (0..g.workers) |w| union_bits |= s.bits[(w * g.groups + at) * g.words + word];
                        count += @popCount(union_bits);
                    }
                    // Other tasks still read each worker's first array: write only its own group slot.
                    s.first[at] = first;
                    s.counts[at] = count;
                } else {
                    // Producers have joined. Each task exclusively owns this partition's remaining buffers and bits.
                    for (0..g.workers) |w| {
                        const slot = w * g.parts + at;
                        const len = &s.lengths[slot].value;
                        s.apply(s.buffer[slot * g.batch_rows ..][0..len.*]);
                        len.* = 0;
                    }
                    const begin = at << g.group_shift;
                    const end = @min(g.groups, (at + 1) << g.group_shift);
                    for (begin..end) |id| {
                        var count: u64 = 0;
                        for (s.bits[id * g.words ..][0..g.words]) |word| count += @popCount(word);
                        s.counts[id] = count;
                    }
                }
            }
        }
    };
};

test "grouped DISTINCT bitmap private and partitioned owners preserve NULL presence first ordinals and exact unions" {
    const a = std.testing.allocator;
    for ([_]Mode{ .private, .partitioned }) |mode| {
        const g = Geometry.plan(4, -3, 70, 4, 512, 4096, mode).?;
        var s = try State.init(a, g);
        defer s.deinit();
        for (0..128) |i| {
            const group: u32 = @intCast(i % 4);
            const value = if (group == 3) none else try g.index(@intCast(@as(i64, @intCast(i % 13)) - 3));
            try s.add((i / 4) % 4, group, value, @intCast(127 - i));
        }
        s.finish(1 << 20);
        for (0..3) |id| try std.testing.expectEqual(@as(u64, 13), s.counts[id]);
        try std.testing.expectEqual(@as(u64, 0), s.counts[3]);
        try std.testing.expectEqualSlices(u32, &.{ 3, 2, 1, 0 }, s.first[0..4]);
    }
}

test "grouped DISTINCT bitmap domain checks handle both signed endpoints without overflow" {
    const g = Geometry.plan(2, -3, 70, 1, 1024, 4096, .private).?;
    try std.testing.expectEqual(@as(u32, 0), try g.index(-3));
    try std.testing.expectEqual(@as(u32, 73), try g.index(70));
    try std.testing.expectEqual(none, try g.index(null));
    try std.testing.expectError(error.BitmapDomainDeclined, g.index(-4));
    try std.testing.expectError(error.BitmapDomainDeclined, g.index(71));
    try std.testing.expect(Geometry.plan(2, std.math.minInt(i64), std.math.maxInt(i64), 1, 1024, 4096, .private) == null);
}

test "grouped DISTINCT bitmap memory refusal releases every ledger draw" {
    const memory = @import("memory.zig");
    for ([_]Mode{ .private, .partitioned }) |mode| {
        const g = Geometry.plan(4, 0, 255, 4, 512, 4096, mode).?;
        var ledger: memory.SharedBudget = .{ .limit = g.bytes().? - 1 };
        var charged: memory.Charged = .{ .child = std.testing.allocator, .budget = &ledger };
        try std.testing.expectError(error.OutOfMemory, State.init(charged.allocator(), g));
        try std.testing.expect(charged.refused.load(.monotonic));
        try std.testing.expectEqual(@as(usize, 0), ledger.used.load(.monotonic));
    }
}
