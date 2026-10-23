//! Inline short byte keys and exact COUNT/SUM state. Longer keys decline to the general executor.
const std = @import("std");
const eval = @import("filter/eval.zig");
const memory = @import("memory.zig");
const spawn = @import("spawn.zig");
const now = @import("../clock.zig").monoNs;

const parts_n = 2048;
const block_rows = 16;
const block_claim = 128;
const none = std.math.maxInt(u32);
const ordinal_bits = 27;
const ordinal_mask: u32 = (1 << ordinal_bits) - 1;
const present: u32 = 1 << 30;
const null_key: u32 = 1 << 31;

pub const Record = extern struct {
    key: [16]u8,
    value: i64,
    /// Low 27 bits are the ordinal, high five bits the exact length.
    ordinal: u32,
    /// Key/value NULL bits, then 30 low hash bits. Every admitted table needs at most 28 bucket bits.
    flags: u32,

    fn init(key: ?[]const u8, value: ?i64, ordinal: u32) !Record {
        var r: Record = .{
            .key = @splat(0),
            .value = value orelse 0,
            .ordinal = ordinal,
            .flags = @intFromBool(key == null) | (@as(u32, @intFromBool(value == null)) << 1),
        };
        if (key) |k| {
            if (k.len > r.key.len) return error.ExceededMemoryBudget;
            r.ordinal |= @as(u32, @intCast(k.len)) << ordinal_bits;
            if (k.len >= 8) {
                r.key[0..8].* = k[0..8].*;
                r.key[k.len - 8 ..][0..8].* = k[k.len - 8 ..][0..8].*;
            } else for (k, 0..) |b, i| r.key[i] = b;
        }
        return r;
    }
};

pub const Group = struct {
    // Eight-byte alignment keeps this state at 40 bytes without narrowing the exact sum.
    sum: i128 align(8),
    key: [16]u8,
    /// Low 30 bits count rows; high bits mark a present SUM and a NULL key.
    count: u32,
    /// Low 27 bits are the ordinal; high five bits hold the key length.
    first: u32,

    pub fn ordinal(g: Group) u32 {
        return g.first & ordinal_mask;
    }
    pub fn keyBytes(g: *const Group) ?[]const u8 {
        return if (g.count & null_key != 0) null else g.key[0 .. g.first >> ordinal_bits];
    }
    pub fn countValue(g: Group) i64 {
        return g.count & (present - 1);
    }
    pub fn sumValue(g: Group) ?i128 {
        return if (g.count & present != 0) g.sum else null;
    }
};

comptime {
    std.debug.assert(@sizeOf(Group) == 40 and @sizeOf(Record) == 32);
}

inline fn hash(r: Record) u64 {
    return if (r.flags & 1 != 0) 0x621985775bcbd930 else std.hash.Wyhash.hash(0, r.key[0 .. r.ordinal >> ordinal_bits]);
}

// Keep the link and count beside the first records, so advancing a run needs one header fetch.
const Block = extern struct { next: u32 = none, len: u32 = 0, rows: [block_rows]Record };

pub const Local = struct {
    state: *State,
    key_col: usize,
    value_col: usize,
    ordinal_base: u32 = 0,
    heads: [parts_n]u32 = @splat(none),
    tails: [parts_n]u32 = @splat(none),
    histogram: [parts_n]u32 = @splat(0),
    nulls: u32 = 0,
    free_next: u32 = 0,
    free_end: u32 = 0,

    fn append(l: *Local, input: Record) !void {
        var r = input;
        const h = hash(r);
        r.flags |= (@as(u32, @truncate(h)) & 0x3fffffff) << 2;
        l.nulls |= r.flags & 3;
        const p = h >> 53;
        l.histogram[p] += 1;
        var tail = l.tails[p];
        if (tail == none or l.state.blocks[tail].len == block_rows) {
            if (l.free_next == l.free_end) {
                const start = l.state.block_cursor.fetchAdd(block_claim, .monotonic);
                if (start >= l.state.blocks.len) return error.ExceededMemoryBudget;
                l.free_next = start;
                l.free_end = @intCast(@min(l.state.blocks.len, @as(usize, start) + block_claim));
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

    pub fn batch(l: *Local, batch_view: eval.Batch, lookup: []const ?usize, offset: usize) !void {
        const k = batch_view.cols[lookup[l.key_col].?].string;
        const v = batch_view.cols[lookup[l.value_col].?].i64;
        for (0..batch_view.num_rows) |i| {
            const kn = if (k.def_levels) |d| d[i] < k.max_def else false;
            const vn = if (v.def_levels) |d| d[i] < v.max_def else false;
            try l.append(try Record.init(
                if (kn) null else if (k.codes) |codes| k.dict[codes[i]] else k.values[i],
                if (vn) null else if (v.codes) |codes| v.dict[codes[i]] else v.values[i],
                l.ordinal_base + @as(u32, @intCast(offset + i)),
            ));
        }
    }
};

pub const State = struct {
    gpa: std.mem.Allocator,
    ledger: ?*memory.SharedBudget,
    charged: usize,
    rows: usize,
    blocks: []Block = &.{},
    block_cursor: std.atomic.Value(u32) = .init(0),
    locals: []Local = &.{},
    groups: []Group = &.{},
    winners: []u32 = &.{},
    bitmap: []u64 = &.{},
    prefix: []u32 = &.{},
    starts: [parts_n + 1]usize = @splat(0),
    counts: [parts_n]usize = @splat(0),
    group_count: usize = 0,
    input_nulls: u32 = 0,
    partition_ns: u64 = 0,
    fold_ns: u64 = 0,
    release_ns: u64 = 0,
    order_ns: u64 = 0,

    pub fn reservation(rows: usize) usize {
        return rows *| 96 +| (192 << 20);
    }

    pub fn init(
        a: std.mem.Allocator,
        ledger: ?*memory.SharedBudget,
        rows: usize,
        workers: usize,
        key_col: usize,
        value_col: usize,
    ) !?*State {
        if (rows >= 1 << ordinal_bits or workers > 64) return null;
        const charged = reservation(rows);
        if (ledger) |l| _ = l.drawExact(charged) orelse return null;
        const s = a.create(State) catch |e| {
            if (ledger) |l| l.release(charged);
            return e;
        };
        s.* = .{ .gpa = a, .ledger = ledger, .charged = charged, .rows = rows };
        errdefer s.deinit();
        s.blocks = try a.alloc(Block, (rows + block_rows - 1) / block_rows +
            workers * (parts_n + block_claim - 1));
        s.locals = try a.alloc(Local, workers);
        for (s.locals) |*l| l.* = .{ .state = s, .key_col = key_col, .value_col = value_col };
        return s;
    }

    pub fn deinit(s: *State) void {
        const a = s.gpa;
        a.free(s.blocks);
        a.free(s.locals);
        a.free(s.groups);
        a.free(s.winners);
        a.free(s.bitmap);
        a.free(s.prefix);
        if (s.ledger) |l| l.release(s.charged);
        a.destroy(s);
    }

    const Task = struct {
        s: *State,
        cursor: *std.atomic.Value(usize),
        pass: enum { fold, order },
        err: ?anyerror = null,
        fn run(t: *Task) void {
            var slots: []u32 = &.{};
            defer t.s.gpa.free(slots);
            const end = switch (t.pass) {
                .fold => parts_n,
                .order => (t.s.bitmap.len + 1023) / 1024,
            };
            while (true) {
                const p = t.cursor.fetchAdd(1, .monotonic);
                if (p >= end) return;
                (switch (t.pass) {
                    .fold => t.s.fold(p, &slots),
                    .order => t.s.orderBlock(p),
                }) catch |e| {
                    t.err = e;
                    return;
                };
            }
        }
    };

    fn parallel(s: *State, workers: usize, pass: @FieldType(Task, "pass")) !void {
        const tasks = try s.gpa.alloc(Task, @max(1, workers));
        defer s.gpa.free(tasks);
        var cursor = std.atomic.Value(usize).init(0);
        for (tasks) |*t| t.* = .{ .s = s, .cursor = &cursor, .pass = pass };
        if (tasks.len == 1 or !spawn.runTree(Task, tasks, .{ .stack_size = 1 << 20 }, Task.run)) Task.run(&tasks[0]);
        for (tasks) |t| if (t.err) |e| return e;
    }

    fn fold(s: *State, p: usize, scratch: *[]u32) !void {
        const lo = s.starts[p];
        const hi = s.starts[p + 1];
        if (lo == hi) return;
        const cap = try std.math.ceilPowerOfTwo(usize, (hi - lo) * 2);
        if (scratch.len < cap) {
            s.gpa.free(scratch.*);
            scratch.* = &.{};
            scratch.* = try s.gpa.alloc(u32, cap);
        }
        const slots = scratch.*[0..cap];
        @memset(slots, 0);
        var n: usize = 0;
        for (s.locals) |local| {
            var id = local.heads[p];
            while (id != none) {
                const b = &s.blocks[id];
                const next = b.next;
                if (next != none) @prefetch(&s.blocks[next], .{ .rw = .read, .locality = 1, .cache = .data });
                for (b.rows[0..b.len]) |r| {
                    const kn = r.flags & 1 != 0;
                    const len = r.ordinal >> ordinal_bits;
                    const slot_hash = r.flags >> 2;
                    var pos: usize = slot_hash & (cap - 1);
                    while (slots[pos] != 0) : (pos = (pos + 1) & (cap - 1)) {
                        const g = &s.groups[slots[pos] - 1];
                        if ((g.count & null_key != 0) == kn and (kn or
                            (g.first >> ordinal_bits == len and std.mem.eql(u8, &g.key, &r.key)))) break;
                    }
                    if (slots[pos] == 0) {
                        slots[pos] = @intCast(lo + n + 1);
                        s.groups[lo + n] = .{
                            .sum = 0,
                            .key = r.key,
                            .count = if (kn) null_key else 0,
                            .first = r.ordinal,
                        };
                        n += 1;
                    }
                    const g = &s.groups[slots[pos] - 1];
                    g.count += 1;
                    g.first = (g.first & ~ordinal_mask) | @min(g.ordinal(), r.ordinal & ordinal_mask);
                    if (r.flags & 2 == 0) {
                        g.sum += r.value;
                        g.count |= present;
                    }
                }
                id = next;
            }
        }
        for (s.groups[lo..][0..n], lo..) |g, i| {
            if (g.count & present != 0 and std.math.cast(i64, g.sum) == null) return error.AggIntTooWide;
            s.winners[g.ordinal()] = @intCast(i + 1);
        }
        s.counts[p] = n;
    }

    fn orderBlock(s: *State, block: usize) void {
        for (block * 1024..@min(s.bitmap.len, (block + 1) * 1024)) |wi| {
            var bits: u64 = 0;
            for (wi * 64..@min(s.rows, wi * 64 + 64)) |i| if (s.winners[i] != 0) {
                bits |= @as(u64, 1) << @intCast(i % 64);
            };
            s.bitmap[wi] = bits;
            s.prefix[wi] = @popCount(bits);
        }
    }

    pub fn finish(s: *State, workers: usize) !void {
        var start = now();
        for (s.locals) |l| s.input_nulls |= l.nulls;
        var at: usize = 0;
        for (0..parts_n) |p| {
            s.starts[p] = at;
            for (s.locals) |l| at += l.histogram[p];
        }
        if (at != s.rows) return error.BadRowCount;
        s.starts[parts_n] = at;
        s.groups = try s.gpa.alloc(Group, s.rows);
        s.winners = try s.gpa.alloc(u32, s.rows);
        @memset(s.winners, 0);
        s.partition_ns = @intCast(now() - start);
        start = now();
        try s.parallel(workers, .fold);
        for (s.counts) |n| s.group_count += n;
        s.fold_ns = @intCast(now() - start);
        start = now();
        s.gpa.free(s.blocks);
        s.blocks = &.{};
        s.release_ns = @intCast(now() - start);
        start = now();
        s.bitmap = try s.gpa.alloc(u64, (s.rows + 63) / 64);
        s.prefix = try s.gpa.alloc(u32, s.bitmap.len + 1);
        try s.parallel(workers, .order);
        var total: u32 = 0;
        for (s.prefix[0..s.bitmap.len]) |*n| {
            const count = n.*;
            n.* = total;
            total += count;
        }
        s.prefix[s.bitmap.len] = total;
        if (total != s.group_count) return error.BadRowCount;
        s.order_ns = @intCast(now() - start);
    }

    pub fn window(s: *const State, a: std.mem.Allocator, lo: usize, hi: usize) ![]u32 {
        std.debug.assert(lo <= hi and hi <= s.group_count);
        const ids = try a.alloc(u32, hi - lo);
        if (ids.len == 0) return ids;
        var left: usize = 0;
        var right = s.bitmap.len;
        while (left < right) {
            const mid = left + (right - left) / 2;
            if (s.prefix[mid + 1] <= lo) left = mid + 1 else right = mid;
        }
        var wi = left;
        var skip = lo - s.prefix[wi];
        var n: usize = 0;
        while (n < ids.len) : (wi += 1) {
            var bits = s.bitmap[wi];
            while (bits != 0) {
                const bit = @ctz(bits);
                bits &= bits - 1;
                if (skip != 0) {
                    skip -= 1;
                    continue;
                }
                ids[n] = s.winners[wi * 64 + bit] - 1;
                n += 1;
                if (n == ids.len) break;
            }
        }
        return ids;
    }
};

fn trial(a: std.mem.Allocator, workers: usize) !void {
    const keys = [_]?[]const u8{ "", null, "a", "a\x00", "abcdefghijklmnop", "γ", "z" };
    const rows = 3001;
    var budget: memory.SharedBudget = .{ .limit = 1 << 30 };
    defer std.debug.assert(budget.used.load(.monotonic) == 0);
    const s = (try State.init(a, &budget, rows, workers, 0, 1)).?;
    defer s.deinit();
    var i: usize = rows;
    while (i != 0) {
        i -= 1;
        const key = keys[i % keys.len];
        const value: ?i64 = if (i % keys.len == 2) null else @as(i64, @intCast(i)) - 1500;
        try s.locals[i % workers].append(try Record.init(key, value, @intCast(i)));
    }
    try s.finish(workers);
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
        try trial(std.testing.allocator, j);
    };
}

test "inline byte groups unwind every partial allocation and ledger charge" {
    try std.testing.checkAllAllocationFailures(std.testing.allocator, trial, .{1});
}

test "inline byte groups decline long keys and reject wide sums only after decode" {
    try std.testing.expectError(error.ExceededMemoryBudget, Record.init("abcdefghijklmnopq", 1, 0));
    for ([_]i64{ std.math.minInt(i64), std.math.maxInt(i64) }) |v| {
        const s = (try State.init(std.testing.allocator, null, 2, 2, 0, 1)).?;
        defer s.deinit();
        for (0..2) |i| try s.locals[i].append(try Record.init("same", v, @intCast(i)));
        try std.testing.expectError(error.AggIntTooWide, s.finish(2));
    }
}

test "inline byte cached hashes cover wide partition tables" {
    for ([_]usize{ 131072, 131073 }) |rows| {
        const s = (try State.init(std.testing.allocator, null, rows, 4, 0, 1)).?;
        defer s.deinit();
        for (0..rows) |i| try s.locals[i % 4].append(try Record.init("abcdefghijklmnop", 3, @intCast(i)));
        try s.finish(4);
        try std.testing.expectEqual(@as(usize, 1), s.group_count);
        const ids = try s.window(std.testing.allocator, 0, 1);
        defer std.testing.allocator.free(ids);
        const g = &s.groups[ids[0]];
        try std.testing.expectEqualStrings("abcdefghijklmnop", g.keyBytes().?);
        try std.testing.expectEqual(@as(i64, @intCast(rows)), g.countValue());
        try std.testing.expectEqual(@as(?i128, @intCast(rows * 3)), g.sumValue());
    }
}
