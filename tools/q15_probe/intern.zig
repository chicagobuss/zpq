const std = @import("std");
const common = @import("main.zig");
const bytes = @import("bytes");
const A = std.heap.smp_allocator;
const Row = common.Row;
const Result = common.Result;
const Heap = common.Heap;
const now = common.now;
const parts = 4096;
const none = std.math.maxInt(u32);
const br = 32;
const claim = 64;
const batch_size = 4096;
fn mix(v: u64) u64 {
    var x = v;
    x ^= x >> 30;
    x *%= 0xbf58476d1ce4e5b9;
    x ^= x >> 27;
    x *%= 0x94d049bb133111eb;
    return x ^ (x >> 31);
}
fn key(data: []const u8, r: Row) []const u8 {
    return data[r.offset..][0..r.len];
}
fn pairHash(h: u64, f: u16) u64 {
    return mix(h ^ (@as(u64, f) *% 0x9e3779b97f4a7c15));
}
fn keep(h: *Heap, r: Result) void {
    if (h.count() == 10) {
        if (common.worse({}, r, h.peek().?) != .gt) return;
        _ = h.pop();
    }
    h.push(A, r) catch @panic("oom");
}
fn parallel(comptime T: type, ws: []T, comptime run: fn (*T) void) void {
    var ts: [63]std.Thread = undefined;
    for (ws[1..], ts[0 .. ws.len - 1]) |*w, *t| t.* = std.Thread.spawn(.{ .stack_size = 2 << 20 }, run, .{w}) catch @panic("spawn");
    run(&ws[0]);
    for (ts[0 .. ws.len - 1]) |t| t.join();
}
const Key = struct { text: []const u8, hash: u64 };
const KeyContext = struct {
    pub fn hash(_: @This(), k: Key) u64 {
        return k.hash;
    }
    pub fn eql(_: @This(), a: Key, b: Key) bool {
        return a.hash == b.hash and bytes.keyEql(a.text, b.text);
    }
};
const InternMap = std.HashMapUnmanaged(Key, u32, KeyContext, 80);
const Lock = struct {
    held: std.atomic.Value(bool) = .init(false),
    fn lock(l: *Lock) void {
        while (l.held.cmpxchgWeak(false, true, .acquire, .monotonic) != null) std.atomic.spinLoopHint();
    }
    fn unlock(l: *Lock) void {
        l.held.store(false, .release);
    }
};
const Shard = struct {
    lock: Lock = .{},
    map: InternMap = .empty,
    arena: std.heap.ArenaAllocator = .init(A),
    payload: u64 = 0,
    fn intern(s: *Shard, k: Key, canonical_ordinal: u32) u32 {
        const entry = s.map.getOrPut(A, k) catch @panic("oom");
        if (!entry.found_existing) {
            entry.key_ptr.text = s.arena.allocator().dupe(u8, k.text) catch @panic("oom");
            entry.value_ptr.* = canonical_ordinal;
            s.payload += k.text.len;
        }
        return entry.value_ptr.*;
    }
};
const Record = extern struct { key: u64, count: u32, first: u32 };
const Block = extern struct { next: u32, len: u32, rows: [br]Record };
const Temp = struct { hash: u64, ordinal: u32, next: u32, token: u32 = 0 };
const State = struct {
    data: []const u8,
    rows: []align(1) const Row,
    shards: []Shard,
    locals: []Local,
    blocks: []Block,
    block_cursor: std.atomic.Value(u32) = .init(0),
    cursor: std.atomic.Value(usize) = .init(0),
    groups: [parts]u32 = @splat(0),
    counted: [parts]u64 = @splat(0),
};
const Local = struct {
    state: *State,
    index: usize,
    heads: [parts]u32 = @splat(none),
    tails: [parts]u32 = @splat(none),
    histogram: [parts]u32 = @splat(0),
    free_next: u32 = 0,
    free_end: u32 = 0,
    heap: Heap = .initContext({}),
    scratch: []Record = &.{},
    slots: []u32 = &.{},
    key_ns: u64 = 0,
    intern_ns: u64 = 0,
    emit_ns: u64 = 0,
    fn append(l: *Local, r: Record) void {
        const p = mix(r.key) >> 52;
        l.histogram[p] += 1;
        var tail = l.tails[p];
        if (tail == none or l.state.blocks[tail].len == br) {
            if (l.free_next == l.free_end) {
                const start = l.state.block_cursor.fetchAdd(claim, .monotonic);
                if (start >= l.state.blocks.len) @panic("block capacity");
                l.free_next = start;
                l.free_end = @intCast(@min(l.state.blocks.len, @as(usize, start) + claim));
            }
            const fresh = l.free_next;
            l.free_next += 1;
            l.state.blocks[fresh].next = none;
            l.state.blocks[fresh].len = 0;
            if (tail == none) l.heads[p] = fresh else l.state.blocks[tail].next = fresh;
            l.tails[p] = fresh;
            tail = fresh;
        }
        const b = &l.state.blocks[tail];
        b.rows[b.len] = r;
        b.len += 1;
    }
    fn scan(l: *Local) void {
        const s = l.state;
        const lo = s.rows.len * l.index / s.locals.len;
        const hi = s.rows.len * (l.index + 1) / s.locals.len;
        var temp: [batch_size]Temp = undefined;
        var base = lo;
        while (base < hi) : (base += @min(batch_size, hi - base)) {
            const n = @min(batch_size, hi - base);
            var heads: [256]u32 = @splat(none);
            var started = now();
            for (temp[0..n], base..) |*t, i| {
                const h = std.hash.Wyhash.hash(0, key(s.data, s.rows[i]));
                const p = h % s.shards.len;
                t.* = .{ .hash = h, .ordinal = @intCast(i), .next = heads[p] };
                heads[p] = @intCast(i - base);
            }
            l.key_ns += now() - started;
            started = now();
            for (0..s.shards.len) |j| {
                const p = (j + l.index) % s.shards.len;
                if (heads[p] == none) continue;
                const shard = &s.shards[p];
                shard.lock.lock();
                var id = heads[p];
                while (id != none) {
                    const t = &temp[id];
                    const row = s.rows[t.ordinal];
                    t.token = shard.intern(.{ .text = key(s.data, row), .hash = t.hash }, t.ordinal);
                    id = t.next;
                }
                shard.lock.unlock();
            }
            l.intern_ns += now() - started;
            started = now();
            for (temp[0..n]) |t| l.append(.{ .key = (@as(u64, t.token) << 16) | s.rows[t.ordinal].f, .count = 1, .first = t.ordinal });
            l.emit_ns += now() - started;
        }
    }
    fn fold(l: *Local) void {
        const s = l.state;
        while (true) {
            const p = s.cursor.fetchAdd(1, .monotonic);
            if (p >= parts) return;
            var n: usize = 0;
            for (s.locals) |w| n += w.histogram[p];
            if (n == 0) continue;
            const cap = std.math.ceilPowerOfTwoAssert(usize, n * 2);
            if (l.scratch.len < n) {
                A.free(l.scratch);
                l.scratch = A.alloc(Record, n) catch @panic("oom");
            }
            if (l.slots.len < cap) {
                A.free(l.slots);
                l.slots = A.alloc(u32, cap) catch @panic("oom");
            }
            const slots = l.slots[0..cap];
            @memset(slots, 0);
            var used: usize = 0;
            for (s.locals) |w| {
                var id = w.heads[p];
                while (id != none) {
                    const b = &s.blocks[id];
                    if (b.next != none) @prefetch(&s.blocks[b.next], .{ .rw = .read, .locality = 1, .cache = .data });
                    for (b.rows[0..b.len]) |r| {
                        var pos: usize = mix(r.key) & (cap - 1);
                        while (slots[pos] != 0 and l.scratch[slots[pos] - 1].key != r.key) pos = (pos + 1) & (cap - 1);
                        if (slots[pos] == 0) {
                            l.scratch[used] = r;
                            used += 1;
                            slots[pos] = @intCast(used);
                        } else {
                            const g = &l.scratch[slots[pos] - 1];
                            g.count += r.count;
                            g.first = @min(g.first, r.first);
                        }
                    }
                    id = b.next;
                }
            }
            s.groups[p] = @intCast(used);
            for (l.scratch[0..used]) |g| {
                s.counted[p] += g.count;
                keep(&l.heap, .{ .count = g.count, .first = g.first, .f = @intCast(g.key & 65535) });
            }
        }
    }
};
fn printTop(h: *Heap) void {
    std.mem.sort(Result, h.items, {}, struct {
        fn less(_: void, a: Result, b: Result) bool {
            return common.worse({}, a, b) == .gt;
        }
    }.less);
    std.debug.print("[", .{});
    for (h.items, 0..) |r, i| std.debug.print("{s}{{\"count\":{d},\"first\":{d},\"f\":{d}}}", .{ if (i == 0) "" else ",", r.count, r.first, r.f });
    std.debug.print("]", .{});
}
fn intern(data: []const u8, rows: []align(1) const Row, jobs: usize, shards_n: usize) !void {
    const started = now();
    const shards = try A.alloc(Shard, shards_n);
    defer A.free(shards);
    for (shards) |*s| s.* = .{};
    defer for (shards) |*s| {
        s.map.deinit(A);
        s.arena.deinit();
    };
    const blocks = try A.alloc(Block, (rows.len + br - 1) / br + jobs * (parts + claim - 1));
    defer A.free(blocks);
    const ws = try A.alloc(Local, jobs);
    defer A.free(ws);
    var s: State = .{ .data = data, .rows = rows, .shards = shards, .locals = ws, .blocks = blocks };
    for (ws, 0..) |*w, i| {
        w.* = .{ .state = &s, .index = i };
        try w.heap.ensureTotalCapacity(A, 10);
    }
    defer for (ws) |*w| {
        w.heap.deinit(A);
        A.free(w.scratch);
        A.free(w.slots);
    };
    parallel(Local, ws, Local.scan);
    const scan_end = now();
    parallel(Local, ws, Local.fold);
    const fold_end = now();
    const h = &ws[0].heap;
    for (ws[1..]) |w| for (w.heap.items) |r| keep(h, r);
    var groups: u64 = 0;
    var counted: u64 = 0;
    for (s.groups, s.counted) |g, c| {
        groups += g;
        counted += c;
    }
    var keys: u64 = 0;
    var payload: u64 = 0;
    var hash_ns: u64 = 0;
    var intern_ns: u64 = 0;
    var emit_ns: u64 = 0;
    for (shards) |sh| {
        keys += sh.map.count();
        payload += sh.payload;
    }
    for (ws) |w| {
        hash_ns += w.key_ns;
        intern_ns += w.intern_ns;
        emit_ns += w.emit_ns;
    }
    std.debug.print("{{\"kind\":\"intern\",\"jobs\":{d},\"shards\":{d},\"total_ns\":{d},\"scan_ns\":{d},\"fold_ns\":{d},\"hash_busy_ns\":{d},\"intern_busy_ns\":{d},\"emit_busy_ns\":{d},\"groups\":{d},\"counted\":{d},\"keys\":{d},\"owned_key_bytes\":{d},\"record_bytes\":{d},\"rss_kib\":{d},\"top\":", .{ jobs, shards_n, now() - started, scan_end - started, fold_end - scan_end, hash_ns, intern_ns, emit_ns, groups, counted, keys, payload, s.block_cursor.load(.monotonic) * @as(u64, @sizeOf(Block)), std.posix.getrusage(0).maxrss });
    printTop(h);
    std.debug.print("}}\n", .{});
}
pub fn main(init: std.process.Init) !void {
    const args = try init.minimal.args.toSlice(init.arena.allocator());
    if (args.len < 4) return error.Usage;
    const path = args[1];
    const jobs = try std.fmt.parseInt(usize, args[2], 10);
    const shards = try std.fmt.parseInt(usize, args[3], 10);
    if (jobs == 0 or jobs > 64 or shards == 0 or shards > 256) return error.BadArgument;
    const data = try common.mapped(init.io, try std.fmt.allocPrint(init.arena.allocator(), "{s}/keys.bin", .{path}));
    defer std.posix.munmap(data);
    const raw = try common.mapped(init.io, try std.fmt.allocPrint(init.arena.allocator(), "{s}/rows.bin", .{path}));
    defer std.posix.munmap(raw);
    const rows: []align(1) const Row = std.mem.bytesAsSlice(Row, raw);
    if (rows.len >= 1 << 25 or data.len >= @as(u64, 1) << 32) return error.BadInput;
    for (rows) |r| if (r.len > 255 or r.f >= 1000 or @as(usize, r.offset) + r.len > data.len) return error.BadInput;
    var touch: u64 = 0;
    var i: usize = 0;
    while (i < data.len) : (i += 4096) touch +%= data[i];
    std.mem.doNotOptimizeAway(touch);
    try intern(data, rows, jobs, shards);
}
