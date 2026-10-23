//! Exact composite COUNT top-H over retained key payloads. Cached radix digits never establish identity alone.
const std = @import("std");
const memory = @import("memory.zig");
const retained = @import("retained_pages.zig");
const huge_pages = @import("huge_pages.zig");
const eval = @import("filter/eval.zig");
const spawn = @import("spawn.zig");
const hw = @import("hw.zig");
const now = @import("../clock.zig").monoNs;
const parts = 4096;
const block_rows = 16;
const block_claim = 64;
const legacy_cache_rows = 4096;
const none = std.math.maxInt(u32);
const ordinal_mask: u32 = (1 << 30) - 1;
const key_null: u32 = 1 << 31;
const f_null: u32 = 1 << 30;
const fold_stack_size = 4 << 20;
pub const output_floor = 64 << 20;

fn cacheRowsForBudget(bytes: usize) usize {
    // Keep local metadata and one radix frame hot, then leave half the remainder for input and nested frames.
    const overhead = @sizeOf(Local) + 3 * 2048 * @sizeOf(usize);
    const rows = std.math.clamp((bytes -| overhead) / (4 * @sizeOf(Record)), 16, 16384);
    return @as(usize, 1) << @intCast(std.math.log2_int(usize, rows));
}

/// Zero forces the legacy window; positive overrides force a bounded power-of-two window on every host.
pub fn cacheRows(forced: ?usize) usize {
    if (forced) |rows| {
        if (rows == 0) return legacy_cache_rows;
        return @as(usize, 1) << @intCast(std.math.log2_int(usize, std.math.clamp(rows, 16, 16384)));
    }
    return cacheRowsForBudget(hw.l2BytesPerWorkerOr(640 << 10));
}

/// Both views belong to retained owners. A local prefix is shortened against every non-NULL key, then the
/// completed locals are intersected before refinement starts. No footer value establishes skipped bytes.
fn commonPrefix(a: []const u8, b: []const u8) []const u8 {
    const n = @min(a.len, b.len);
    if (a.ptr == b.ptr or std.mem.eql(u8, a[0..n], b[0..n])) return a[0..n];
    var i: usize = 0;
    while (i < n and a[i] == b[i]) : (i += 1) {}
    return a[0..i];
}

pub const Record = extern struct {
    word: u64 align(4),
    ptr: usize align(4),
    ordinal: u32,
    len: u32,
    f: i32,

    pub fn first(r: Record) u32 {
        return r.ordinal & ordinal_mask;
    }
    pub fn key(r: Record) ?[]const u8 {
        if (r.ordinal & key_null != 0) return null;
        if (r.len == 0) return "";
        const p: [*]const u8 = @ptrFromInt(r.ptr);
        return p[0..r.len];
    }
    pub fn fValue(r: Record) ?i64 {
        return if (r.ordinal & f_null != 0) null else r.f;
    }
};
pub const Group = struct { record: Record, count: u32 };
comptime {
    std.debug.assert(@sizeOf(Record) == 28);
}
fn worse(_: void, a: Group, b: Group) std.math.Order {
    const order = std.math.order(a.count, b.count);
    return if (order != .eq) order else std.math.order(b.record.first(), a.record.first());
}
const Heap = std.PriorityQueue(Group, void, worse);
const Block = struct { next: u32 = none, len: u32 = 0, records: [block_rows]Record };
const Phase = enum { f, length, key };
fn digitWord(r: Record, phase: Phase) u64 {
    return switch (phase) {
        .f => (@as(u64, @intFromBool(r.ordinal & f_null != 0)) << 32) | @as(u32, @bitCast(r.f)),
        .length => (@as(u64, @intFromBool(r.ordinal & key_null != 0)) << 32) | r.len,
        .key => r.word,
    };
}
fn cached(key: []const u8, at: usize) u64 {
    if (at + 8 <= key.len) return std.mem.readInt(u64, key[at..][0..8], .big);
    var b: [8]u8 = @splat(0);
    if (at < key.len) @memcpy(b[0 .. key.len - at], key[at..]);
    return std.mem.readInt(u64, &b, .big);
}
const Cache = struct {
    records: []Record,
    fn len(v: Cache) usize {
        return v.records.len;
    }
    fn at(v: Cache, i: usize) *Record {
        return &v.records[i];
    }
    fn sub(v: Cache, start: usize, n: usize) Cache {
        return .{ .records = v.records[start..][0..n] };
    }
};
const Run = struct {
    blocks: []Block,
    ids: []const u32,
    start: usize,
    count: usize,
    fn len(v: Run) usize {
        return v.count;
    }
    fn at(v: Run, i: usize) *Record {
        const p = v.start + i;
        return &v.blocks[v.ids[p / block_rows]].records[p % block_rows];
    }
    fn sub(v: Run, start: usize, n: usize) Run {
        var x = v;
        x.start += start;
        x.count = n;
        return x;
    }
    fn order(v: Run, a: usize, b: usize) std.math.Order {
        const x = v.at(a).*;
        const y = v.at(b).*;
        for ([_]Phase{ .f, .length }) |phase| {
            const result = std.math.order(digitWord(x, phase), digitWord(y, phase));
            if (result != .eq) return result;
        }
        return std.mem.order(u8, x.key() orelse "", y.key() orelse "");
    }
    pub fn lessThan(v: Run, a: usize, b: usize) bool {
        return v.order(a, b) == .lt;
    }
    pub fn swap(v: Run, a: usize, b: usize) void {
        std.mem.swap(Record, v.at(a), v.at(b));
    }
};

pub const Local = struct {
    state: *State,
    ordinal_base: u32 = 0,
    owner: ?*retained.Chunk = null,
    heads: [parts]u32 = @splat(none),
    tails: [parts]u32 = @splat(none),
    histogram: [parts]u32 = @splat(0),
    block_counts: [parts]u32 = @splat(0),
    free_next: u32 = 0,
    free_end: u32 = 0,
    heap: Heap = .initContext({}),
    a: []Record = &.{},
    b: []Record = &.{},
    groups: u64 = 0,
    reloads: u64 = 0,
    radix_rows: u64 = 0,
    moves: u64 = 0,
    pruned: u64 = 0,
    common: ?[]const u8 = null,

    pub fn start(l: *Local, ordinal_base: u32, item: usize) void {
        l.ordinal_base = ordinal_base;
        l.owner = &l.state.pages.chunks[item];
    }
    fn append(l: *Local, r: Record) !void {
        const fbits: u32 = @bitCast(r.f);
        const p = ((fbits & 1023) * 4 + ((r.len >> 6) & 3)) ^
            (if (r.ordinal & f_null != 0) @as(u32, 4095) else 0);
        l.histogram[p] += 1;
        var tail = l.tails[p];
        if (tail == none or l.state.blocks[tail].len == block_rows) {
            if (l.free_next == l.free_end) {
                const at = l.state.block_cursor.fetchAdd(block_claim, .monotonic);
                if (at >= l.state.blocks.len) return error.ExceededMemoryBudget;
                l.free_next = at;
                l.free_end = @intCast(@min(l.state.blocks.len, @as(usize, at) + block_claim));
            }
            const id = l.free_next;
            l.free_next += 1;
            l.state.blocks[id].next = none;
            l.state.blocks[id].len = 0;
            if (tail == none) l.heads[p] = id else l.state.blocks[tail].next = id;
            l.tails[p] = id;
            tail = id;
            l.block_counts[p] += 1;
        }
        const b = &l.state.blocks[tail];
        b.records[b.len] = r;
        b.len += 1;
    }
    pub fn batch(l: *Local, view: eval.Batch, lookup: []const ?usize, offset: usize) !void {
        const k = view.cols[lookup[l.state.key_col].?].string;
        const f = view.cols[lookup[l.state.f_col].?];
        for (0..view.num_rows) |i| {
            const kn = if (k.def_levels) |d| d[i] < k.max_def else false;
            const key: []const u8 = if (kn) "" else if (k.codes) |codes| k.dict[codes[i]] else k.values[i];
            if (!kn and !l.owner.?.contains(key)) return error.RetainedOwnershipViolation;
            const fv: ?i32 = switch (f) {
                .i64 => |c| if (c.def_levels != null and c.def_levels.?[i] < c.max_def) null else @intCast(if (c.codes) |codes| c.dict[codes[i]] else c.values[i]),
                .i32 => |c| if (c.def_levels != null and c.def_levels.?[i] < c.max_def) null else if (c.codes) |codes| c.dict[codes[i]] else c.values[i],
                else => return error.UnexpectedPage,
            };
            if (!kn) l.common = if (l.common) |p| commonPrefix(p, key) else key;
            const r: Record = .{ .word = cached(key, key.len -| 8), .ptr = if (key.len == 0) 0 else @intFromPtr(key.ptr), .ordinal = (l.ordinal_base + @as(u32, @intCast(offset + i))) |
                (if (kn) key_null else 0) | (if (fv == null) f_null else 0), .len = @intCast(key.len), .f = fv orelse 0 };
            try l.append(r);
        }
    }
    fn keep(l: *Local, comptime V: type, v: V) void {
        var first = v.at(0).*;
        for (1..v.len()) |i| if (v.at(i).first() < first.first()) {
            first = v.at(i).*;
        };
        l.groups += 1;
        const g: Group = .{ .record = first, .count = @intCast(v.len()) };
        if (l.heap.count() == l.state.k) {
            if (worse({}, g, l.heap.peek().?) != .gt) return;
            _ = l.heap.pop();
        }
        l.heap.push(l.state.gpa, g) catch unreachable;
        if (l.heap.count() == l.state.k) _ = l.state.threshold.fetchMax(l.heap.peek().?.count, .monotonic);
    }

    /// Iterate the largest child. Every recursive child has at most half the parent rows, bounding stack use.
    fn refine(l: *Local, comptime V: type, initial: V, temporary: []Record, phase_start: Phase, position_start: usize) void {
        var v = initial;
        var tmp = temporary;
        var phase = phase_start;
        var position = position_start;
        while (true) {
            if (v.len() < l.state.threshold.load(.monotonic)) {
                l.pruned += v.len();
                return;
            }
            if (V == Run) if (v.len() <= l.a.len) {
                for (0..v.len()) |i| l.a[i] = v.at(i).*;
                l.refine(Cache, .{ .records = l.a[0..v.len()] }, l.b[0..v.len()], phase, position);
                return;
            };
            if (v.len() == 1) {
                l.keep(V, v);
                return;
            }
            var diff: u64 = 0;
            const first = digitWord(v.at(0).*, phase);
            for (1..v.len()) |i| diff |= digitWord(v.at(i).*, phase) ^ first;
            if (diff == 0) {
                switch (phase) {
                    .f => phase = .length,
                    .length => {
                        phase = .key;
                        position = 0;
                    },
                    .key => {
                        const len = v.at(0).len;
                        const head = l.state.prefix_len;
                        const covered = head + position * 8;
                        if (covered >= len -| 8) {
                            l.keep(V, v);
                            return;
                        }
                        const depth = head + position * 8;
                        position += 1;
                        for (0..v.len()) |i| {
                            const r = v.at(i);
                            r.word = cached(r.key().?, depth);
                        }
                        l.reloads += v.len();
                    },
                }
                continue;
            }
            var counts: [2048]usize = @splat(0);
            var starts: [2048]usize = undefined;
            var bucket_n: usize = 0;
            if (V == Cache and v.len() <= 32) {
                for (1..v.len()) |i| {
                    const r = v.at(i).*;
                    var j = i;
                    while (j > 0 and digitWord(v.at(j - 1).*, phase) > digitWord(r, phase)) : (j -= 1) {
                        v.at(j).* = v.at(j - 1).*;
                        l.moves += 1;
                    }
                    v.at(j).* = r;
                    l.moves += 1;
                }
                var at: usize = 0;
                while (at < v.len()) {
                    var end = at + 1;
                    while (end < v.len() and digitWord(v.at(end).*, phase) == digitWord(v.at(at).*, phase)) : (end += 1) {}
                    starts[bucket_n] = at;
                    counts[bucket_n] = end - at;
                    bucket_n += 1;
                    at = end;
                }
            } else {
                const common: u7 = @intCast(@clz(diff));
                const bits: u7 = @min(11, @min(64 - common, @max(1, std.math.log2_int(usize, v.len()))));
                const shift: u6 = @intCast(64 - common - bits);
                bucket_n = @as(usize, 1) << @intCast(bits);
                const mask: u64 = bucket_n - 1;
                for (0..v.len()) |i| counts[(digitWord(v.at(i).*, phase) >> shift) & mask] += 1;
                var at: usize = 0;
                for (counts[0..bucket_n], starts[0..bucket_n]) |n, *p| {
                    p.* = at;
                    at += n;
                }
                var next = starts;
                if (V == Cache) {
                    for (0..v.len()) |i| {
                        const r = v.at(i).*;
                        const b = (digitWord(r, phase) >> shift) & mask;
                        tmp[next[b]] = r;
                        next[b] += 1;
                    }
                    const old = v.records;
                    v = .{ .records = tmp };
                    tmp = old;
                } else {
                    for (0..bucket_n) |b| {
                        const end = starts[b] + counts[b];
                        while (next[b] < end) {
                            const r = v.at(next[b]);
                            const d = (digitWord(r.*, phase) >> shift) & mask;
                            if (d == b) {
                                next[b] += 1;
                                continue;
                            }
                            std.mem.swap(Record, r, v.at(next[d]));
                            next[d] += 1;
                        }
                    }
                }
                l.radix_rows += v.len();
            }
            var largest: usize = 0;
            for (counts[0..bucket_n], 0..) |n, i| if (n > counts[largest]) {
                largest = i;
            };
            for (counts[0..bucket_n], starts[0..bucket_n], 0..) |n, at, i| if (n != 0 and i != largest) {
                const child_tmp = if (V == Cache) tmp[at..][0..n] else &.{};
                l.refine(V, v.sub(at, n), child_tmp, phase, position);
            };
            const at = starts[largest];
            const n = counts[largest];
            v = v.sub(at, n);
            if (V == Cache) tmp = tmp[at..][0..n];
        }
    }
    fn run(l: *Local) void {
        l.runImpl(false);
    }
    fn runWithoutWorker(l: *Local) void {
        l.runImpl(true);
    }
    fn runImpl(l: *Local, comptime caller_stack: bool) void {
        const s = l.state;
        while (true) {
            const p = s.cursor.fetchAdd(1, .monotonic);
            if (p >= parts) return;
            const n = s.counts[p];
            if (n == 0) continue;
            const ids = s.ids[s.starts[p]..s.starts[p + 1]];
            var used: usize = 0;
            for (s.locals) |*w| {
                var id = w.heads[p];
                while (id != none) {
                    ids[used] = id;
                    used += 1;
                    id = s.blocks[id].next;
                }
            }
            std.debug.assert(used == ids.len);
            var last = ids.len - 1;
            const dense = (n + block_rows - 1) / block_rows;
            for (0..dense) |i| {
                const b = &s.blocks[ids[i]];
                while (b.len < block_rows and i < last) {
                    const tail = &s.blocks[ids[last]];
                    const take = @min(block_rows - b.len, tail.len);
                    @memcpy(b.records[b.len..][0..take], tail.records[tail.len - take ..][0..take]);
                    b.len += take;
                    tail.len -= take;
                    if (tail.len == 0) last -= 1;
                }
            }
            const v: Run = .{ .blocks = s.blocks, .ids = ids[0..dense], .start = 0, .count = n };
            if (caller_stack) {
                // Root spawn failure leaves only the caller's possibly small stack. Heapsort is iterative,
                // uses the existing blocks, and compares full identities without any allocation or recursion.
                std.sort.heapContext(0, n, v);
                var at: usize = 0;
                while (at < n) {
                    var end = at + 1;
                    while (end < n and v.order(at, end) == .eq) : (end += 1) {}
                    l.keep(Run, v.sub(at, end - at));
                    at = end;
                }
            } else l.refine(Run, v, &.{}, .f, 0);
        }
    }
};

pub const State = struct {
    gpa: std.mem.Allocator,
    ledger: ?*memory.SharedBudget,
    charged: usize,
    rows: usize,
    key_col: usize,
    f_col: usize,
    k: usize,
    pages: retained.Store = .{},
    blocks: []Block = &.{},
    ids: []u32 = &.{},
    locals: []Local = &.{},
    initialized: usize = 0,
    block_cursor: std.atomic.Value(u32) = .init(0),
    cursor: std.atomic.Value(usize) = .init(0),
    prefix_len: usize = 0,
    threshold: std.atomic.Value(u32) = .init(0),
    counts: [parts]usize = @splat(0),
    starts: [parts + 1]usize = @splat(0),
    fold_ns: u64 = 0,
    decode_budget: memory.SharedBudget = .{ .limit = 0 },
    output_credit: usize = output_floor,
    phase_credit: usize = 0,

    fn blockCapacity(rows: usize, workers: usize) usize {
        return (rows + block_rows - 1) / block_rows + workers * (parts + block_claim - 1);
    }
    pub fn reservation(rows: usize, workers: usize, k: usize, retained_bytes: usize, items: usize, cache_count: usize) usize {
        return retained_bytes +| items *| @sizeOf(retained.Chunk) +|
            blockCapacity(rows, workers) *| (@sizeOf(Block) + @sizeOf(u32)) +|
            workers *| (@sizeOf(Local) + 2 * cache_count * @sizeOf(Record) + 2 * k * @sizeOf(Group) + fold_stack_size) +| @sizeOf(State) +| output_floor;
    }
    /// Decoder workspace is dead before output keys are copied. Fund their maximum, rather than adding peaks
    /// that cannot coexist. The fixed output floor is already included by reservation().
    pub fn phaseFunding(working: usize, output_credit: usize) usize {
        return @max(working, output_credit -| output_floor);
    }
    pub fn init(a: std.mem.Allocator, ledger: ?*memory.SharedBudget, rows: usize, workers: usize, key_col: usize, f_col: usize, k: usize, capacities: []const usize) !?*State {
        return initPrepaid(a, ledger, rows, workers, key_col, f_col, k, capacities, 0, output_floor, cacheRows(null));
    }
    pub fn initPrepaid(a: std.mem.Allocator, ledger: ?*memory.SharedBudget, rows: usize, workers: usize, key_col: usize, f_col: usize, k: usize, capacities: []const usize, working: usize, output_credit: usize, cache_count: usize) !?*State {
        return initAllocated(a, ledger, rows, workers, key_col, f_col, k, capacities, working, output_credit, cache_count) catch null;
    }
    fn initAllocated(a: std.mem.Allocator, ledger: ?*memory.SharedBudget, rows: usize, workers: usize, key_col: usize, f_col: usize, k: usize, capacities: []const usize, working: usize, output_credit: usize, cache_count: usize) !?*State {
        if (rows >= 1 << 30 or workers == 0 or workers > 64 or k == 0 or k > 1024 or cache_count < 16 or cache_count > 16384) return null;
        var raw: usize = 0;
        for (capacities) |cap| raw +|= cap;
        const phase_credit = phaseFunding(working, output_credit);
        const charged = reservation(rows, workers, k, raw, capacities.len, cache_count) +| phase_credit;
        if (ledger) |l| _ = l.drawExact(charged) orelse return null;
        const s = a.create(State) catch |e| {
            if (ledger) |l| l.release(charged);
            return e;
        };
        s.* = .{ .gpa = a, .ledger = ledger, .charged = charged, .rows = rows, .key_col = key_col, .f_col = f_col, .k = k, .decode_budget = .{ .limit = working }, .output_credit = @max(output_floor, output_credit), .phase_credit = phase_credit };
        errdefer s.deinit();
        s.pages = try retained.Store.init(a, capacities);
        const cap = blockCapacity(rows, workers);
        var advisor: huge_pages.HugePageAdvisor = .{ .child = a };
        s.blocks = try advisor.allocator().alloc(Block, cap);
        s.ids = try a.alloc(u32, cap);
        s.locals = try a.alloc(Local, workers);
        for (s.locals) |*l| {
            l.* = .{ .state = s };
            s.initialized += 1;
            try l.heap.ensureTotalCapacity(a, k);
            l.a = try a.alloc(Record, cache_count);
            l.b = try a.alloc(Record, cache_count);
        }
        return s;
    }
    pub fn releaseDecode(s: *State) void {
        std.debug.assert(s.decode_budget.used.load(.monotonic) == 0);
        const keep = s.output_credit - output_floor;
        const released = s.phase_credit - keep;
        if (s.ledger) |ledger| ledger.release(released);
        s.charged -= released;
        s.phase_credit = keep;
    }
    pub fn deinit(s: *State) void {
        const a = s.gpa;
        for (s.locals[0..s.initialized]) |*l| {
            l.heap.deinit(a);
            a.free(l.a);
            a.free(l.b);
        }
        a.free(s.locals);
        a.free(s.blocks);
        a.free(s.ids);
        s.pages.deinit(a);
        if (s.ledger) |l| l.release(s.charged);
        a.destroy(s);
    }
    pub fn finish(s: *State) ![]const Group {
        const begin = now();
        var at: usize = 0;
        var rows: usize = 0;
        for (0..parts) |p| {
            s.starts[p] = at;
            for (s.locals) |*l| {
                at += l.block_counts[p];
                s.counts[p] += l.histogram[p];
            }
            rows += s.counts[p];
        }
        s.starts[parts] = at;
        if (rows != s.rows) return error.ShortDecode;
        var common: ?[]const u8 = null;
        for (s.locals) |*l| if (l.common) |p| {
            common = if (common) |previous| commonPrefix(previous, p) else p;
        };
        s.prefix_len = if (common) |p| p.len else 0;
        if (!spawn.runTree(Local, s.locals, .{ .stack_size = fold_stack_size }, Local.run))
            Local.runWithoutWorker(&s.locals[0]);
        const h = &s.locals[0].heap;
        for (s.locals[1..]) |*l| for (l.heap.items) |g| {
            if (h.count() == s.k) {
                if (worse({}, g, h.peek().?) != .gt) continue;
                _ = h.pop();
            }
            h.push(s.gpa, g) catch unreachable;
        };
        std.mem.sort(Group, h.items, {}, struct {
            fn less(_: void, a: Group, b: Group) bool {
                return a.record.first() < b.record.first();
            }
        }.less);
        s.fold_ns = @intCast(now() - begin);
        return h.items;
    }
};

fn allocationTrial(a: std.mem.Allocator) !void {
    var budget: memory.SharedBudget = .{ .limit = 1 << 30 };
    defer std.debug.assert(budget.used.load(.monotonic) == 0);
    const s = (try State.init(a, &budget, 0, 2, 0, 1, 10, &.{ 256, 256 })) orelse return error.OutOfMemory;
    defer s.deinit();
    try std.testing.expectEqual(@as(usize, 0), (try s.finish()).len);
}
test "cached composite groups release every partially initialized owner" {
    try allocationTrial(std.testing.allocator);
    try std.testing.checkAllAllocationFailures(std.testing.allocator, allocationTrial, .{});
    var budget: memory.SharedBudget = .{ .limit = 1 };
    try std.testing.expect((try State.init(std.testing.allocator, &budget, 0, 1, 0, 1, 10, &.{256})) == null);
    try std.testing.expectEqual(@as(usize, 0), budget.used.load(.monotonic));
}

fn exactTrial(
    a: std.mem.Allocator,
    jobs: usize,
    mode: usize,
    fail_after: ?usize,
    cache_count: usize,
    small_caller: bool,
) !void {
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const temp = arena.allocator();
    const n: usize = if (mode == 1) 100000 else 20000;
    const domain: usize = if (mode == 1) 1 else if (mode == 2) n else 200;
    const top_n: usize = if (small_caller) 1024 else 10;
    const key_bytes = try temp.alloc([]const u8, domain);
    for (key_bytes, 0..) |*key, g| {
        const len = if (mode == 0 and g == 1) 0 else if (mode == 0) (g * 37) % 400 else 80;
        const b = try temp.alloc(u8, len);
        for (b, 0..) |*v, i| v.* = @truncate(g * 73 + i * 13);
        if (len >= 24) @memset(b[0..24], 0xab);
        if (len >= 8) std.mem.writeInt(u64, b[len - 8 ..][0..8], g, .little);
        key.* = b;
    }
    var budget: memory.SharedBudget = .{ .limit = 1 << 30 };
    defer std.debug.assert(budget.used.load(.monotonic) == 0);
    const work_n = 5;
    var capacities: [work_n]usize = @splat(0);
    var payload: usize = 0;
    for (key_bytes) |k| payload += k.len;
    @memset(&capacities, payload);
    const s = (try State.initPrepaid(a, &budget, n, jobs, 0, 1, top_n, &capacities, 0, output_floor, cache_count)).?;
    defer s.deinit();
    var expected: std.ArrayList(Group) = .empty;
    defer expected.deinit(a);
    for (0..work_n) |w| {
        const lo = n * w / work_n;
        const hi = n * (w + 1) / work_n;
        const values = try temp.alloc([]const u8, hi - lo);
        const ks = try temp.alloc(u32, hi - lo);
        const fs = try temp.alloc(u32, hi - lo);
        const fvalues = try temp.alloc(i64, hi - lo);
        const refs = try temp.alloc([]const u8, domain);
        for (key_bytes, refs) |key, *ref| {
            const b = try s.pages.chunks[w].take(key.len);
            @memcpy(b, key);
            ref.* = b;
        }
        for (lo..hi, 0..) |ordinal, i| {
            const g = if (mode == 2) ordinal else (ordinal * 37) % domain;
            const kn = mode == 0 and g % 19 == 0;
            const fnul = mode == 0 and g % 23 == 0;
            values[i] = refs[g];
            ks[i] = @intFromBool(!kn);
            fs[i] = @intFromBool(!fnul);
            fvalues[i] = if (mode == 0) @as(i64, @intCast(g)) * 100003 - 7000000 else 0;
            const r: Record = .{ .word = 0, .ptr = if (kn or refs[g].len == 0) 0 else @intFromPtr(refs[g].ptr), .ordinal = @as(u32, @intCast(ordinal)) | (if (kn) key_null else 0) | (if (fnul) f_null else 0), .len = if (kn) 0 else @intCast(refs[g].len), .f = if (fnul) 0 else @intCast(fvalues[i]) };
            var found = false;
            if (mode != 2) for (expected.items) |*entry| {
                if ((entry.record.ordinal & (key_null | f_null)) != (r.ordinal & (key_null | f_null)) or entry.record.f != r.f) continue;
                const x = entry.record.key();
                const y = r.key();
                if ((x == null and y == null) or (x != null and y != null and std.mem.eql(u8, x.?, y.?))) {
                    entry.count += 1;
                    found = true;
                    break;
                }
            };
            if (!found) try expected.append(a, .{ .record = r, .count = 1 });
        }
        const cols = [_]eval.Batch.Column{
            .{ .string = .{ .values = values, .def_levels = ks, .max_def = 1 } },
            .{ .i64 = .{ .values = fvalues, .def_levels = fs, .max_def = 1 } },
        };
        const local = &s.locals[w % jobs];
        local.start(@intCast(lo), w);
        try local.batch(.{ .cols = &cols, .num_rows = hi - lo }, &.{ 0, 1 }, 0);
    }
    std.mem.sort(Group, expected.items, {}, struct {
        fn less(_: void, x: Group, y: Group) bool {
            return worse({}, x, y) == .gt;
        }
    }.less);
    const want = expected.items[0..@min(expected.items.len, top_n)];
    std.mem.sort(Group, want, {}, struct {
        fn less(_: void, x: Group, y: Group) bool {
            return x.record.first() < y.record.first();
        }
    }.less);
    if (fail_after) |f| spawn.injectFailureAfter(f);
    defer spawn.resetFailure();
    const got = if (small_caller) try finishOnSmallCaller(s) else try s.finish();
    if (fail_after != null and fail_after.? == 0) try std.testing.expect(spawn.failuresInjected() > 0);
    try std.testing.expectEqual(@as(usize, if (mode == 0) 0 else if (mode == 1) 80 else 24), s.prefix_len);
    try std.testing.expectEqual(want.len, got.len);
    for (want, got) |x, y| {
        try std.testing.expectEqual(x.count, y.count);
        try std.testing.expectEqual(x.record.first(), y.record.first());
        try std.testing.expectEqual(x.record.fValue(), y.record.fValue());
        if (x.record.key()) |k| try std.testing.expectEqualSlices(u8, k, y.record.key().?) else try std.testing.expect(y.record.key() == null);
    }
}
test "cached composite groups are exact across metadata ranges, NULLs, skew, ties and spawn failures" {
    for ([_]usize{ 1, 2, 4, 8, 16 }) |j| for (0..3) |mode| for ([_]?usize{ null, 0, 2 }) |failed|
        try exactTrial(std.testing.allocator, j, mode, failed, cacheRows(null), false);
}

fn finishOnSmallCaller(s: *State) ![]const Group {
    const Caller = struct {
        state: *State,
        result: anyerror![]const Group = error.TestUnexpectedResult,
        fn run(c: *@This()) void {
            c.result = c.state.finish();
        }
    };
    var caller: Caller = .{ .state = s };
    // Debug tracing also occupies static TLS in the pthread allocation. Allow for it while retaining a
    // small caller stack. Bypass the injected failure only to create the library caller being modeled.
    const thread = try std.Thread.spawn(.{ .stack_size = 640 << 10 }, Caller.run, .{&caller});
    thread.join();
    return caller.result;
}

test "retained grouping survives root spawn refusal on a small caller stack" {
    for ([_]usize{ 1, 16 }) |jobs| for (0..3) |mode|
        try exactTrial(std.testing.allocator, jobs, mode, 0, cacheRows(null), true);
}

test "retained grouping prepays output copies and declines failed allocation before input" {
    const a = std.testing.allocator;
    var budget: memory.SharedBudget = .{ .limit = 512 << 20 };
    const output = 128 << 20;
    const s = (try State.initPrepaid(a, &budget, 0, 1, 0, 1, 1000, &.{0}, 160 << 20, output, cacheRows(null))).?;
    const rest = budget.drawExact(budget.available()).?;
    s.releaseDecode();
    try std.testing.expect(s.charged >= output);
    try std.testing.expectEqual(@as(usize, output), s.output_credit);
    try std.testing.expectEqual(@as(usize, 64 << 20), s.phase_credit);
    budget.release(rest);
    s.deinit();
    try std.testing.expectEqual(@as(usize, 0), budget.used.load(.monotonic));

    var failure = std.testing.FailingAllocator.init(a, .{ .fail_index = 1 });
    try std.testing.expect((try State.init(failure.allocator(), &budget, 0, 1, 0, 1, 10, &.{4096})) == null);
    try std.testing.expect(failure.has_induced_failure);
    try std.testing.expectEqual(failure.allocated_bytes, failure.freed_bytes);
    try std.testing.expectEqual(@as(usize, 0), budget.used.load(.monotonic));
}

test "retained grouping rejects a key from another owner as an internal failure" {
    var budget: memory.SharedBudget = .{ .limit = 512 << 20 };
    {
        const s = (try State.init(std.testing.allocator, &budget, 1, 1, 0, 1, 1, &.{ 8, 8 })).?;
        defer s.deinit();
        const key = try s.pages.chunks[1].take(1);
        key[0] = 'a';
        const cols = [_]eval.Batch.Column{
            .{ .string = .{ .values = &.{key} } },
            .{ .i32 = .{ .values = &.{0} } },
        };
        s.locals[0].start(0, 0);
        try std.testing.expectError(error.RetainedOwnershipViolation, s.locals[0].batch(
            .{ .cols = &cols, .num_rows = 1 },
            &.{ 0, 1 },
            0,
        ));
        try std.testing.expectEqual(@as(u32, 0), s.locals[0].histogram[0]);
    }
    try std.testing.expectEqual(@as(usize, 0), budget.used.load(.monotonic));
}

test "retained grouping block exhaustion is checked in release builds" {
    const s = (try State.init(std.testing.allocator, null, 1, 1, 0, 1, 1, &.{0})).?;
    defer s.deinit();
    s.block_cursor.store(@intCast(s.blocks.len), .monotonic);
    try std.testing.expectError(error.ExceededMemoryBudget, s.locals[0].append(.{
        .word = 0,
        .ptr = 0,
        .ordinal = 0,
        .len = 0,
        .f = 0,
    }));
}

test "retained scratch budgets and forced variants preserve exact groups and first ordinals" {
    try std.testing.expectEqual(@as(usize, 4096), cacheRowsForBudget(640 << 10));
    try std.testing.expectEqual(@as(usize, 2048), cacheRowsForBudget(512 << 10));
    try std.testing.expectEqual(@as(usize, 8192), cacheRowsForBudget(1024 << 10));
    try std.testing.expectEqual(@as(usize, 4096), cacheRows(0));
    try std.testing.expectEqual(@as(usize, 16), cacheRows(1));
    try std.testing.expectEqual(@as(usize, 16384), cacheRows(std.math.maxInt(usize)));
    for ([_]usize{ 16, 2048, 16384 }) |rows| for ([_]usize{ 1, 2, 4, 8, 16 }) |j| for (0..3) |mode|
        try exactTrial(std.testing.allocator, j, mode, null, rows, false);
}

test "retained scratch follows shared cache budgets while forced windows remain host independent" {
    defer hw.test_cpu = null;
    hw.test_cpu = .{ .l2 = .{ .bytes = 1 << 20, .max_sharing = 2 } };
    try std.testing.expectEqual(@as(usize, 2048), cacheRows(null));
    hw.test_cpu.?.l2.bytes = 2 << 20;
    try std.testing.expectEqual(@as(usize, 8192), cacheRows(null));
    hw.test_cpu.?.l2.bytes = 0;
    try std.testing.expectEqual(@as(usize, 4096), cacheRows(null));
    for ([_]usize{ 0, 16, 2048, 16384 }) |forced|
        try std.testing.expectEqual(if (forced == 0) @as(usize, 4096) else forced, cacheRows(forced));
}
