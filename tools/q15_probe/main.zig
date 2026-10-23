const std = @import("std");
const A = std.heap.smp_allocator;
const OM: u32 = (1 << 25) - 1;
const NP = 2000;
pub const Row = extern struct { offset: u32, len: u16, f: u16 };
const Record = extern struct {
    word: u64,
    offset: u32,
    ord_len: u32,
    fn ordinal(r: Record) u32 {
        return r.ord_len & OM;
    }
};
pub const Result = struct { count: u32, first: u32, f: u16 };
pub fn worse(_: void, x: Result, y: Result) std.math.Order {
    const o = std.math.order(x.count, y.count);
    return if (o != .eq) o else std.math.order(y.first, x.first);
}
pub const Heap = std.PriorityQueue(Result, void, worse);
const Level = struct {
    calls: u64 = 0,
    rows: u64 = 0,
    radix_rows: u64 = 0,
    reload_rows: u64 = 0,
    prune_rows: u64 = 0,
    leaves: u64 = 0,
    sort_rows: u64 = 0,
    sort_moves: u64 = 0,
    ns: u64 = 0,
};
pub fn now() u64 {
    var t: std.posix.timespec = undefined;
    _ = std.posix.system.clock_gettime(std.Io.Threaded.clockToPosix(.awake), &t);
    return @as(u64, @intCast(t.sec)) * 1_000_000_000 + @as(u64, @intCast(t.nsec));
}
fn word(data: []const u8, off: usize, len: usize, depth: usize) u64 {
    if (depth + 8 <= len) return std.mem.readInt(u64, data[off + depth ..][0..8], .big);
    var b: [8]u8 = @splat(0);
    if (depth < len) @memcpy(b[0 .. len - depth], data[off + depth .. off + len]);
    return std.mem.readInt(u64, &b, .big);
}
const Shared = struct {
    data: []const u8,
    rows: []align(1) const Row,
    records: []Record,
    workers: []Worker,
    depth: usize,
    cutoff: usize,
    bits: u6,
    adaptive: bool,
    prune: bool,
    tail: bool,
    starts: [NP + 1]usize = @splat(0),
    cursor: std.atomic.Value(usize) = .init(0),
    threshold: std.atomic.Value(u32) = .init(0),
    bad_prefix: std.atomic.Value(bool) = .init(false),
};
fn cachedWord(s: *const Shared, off: usize, len: usize, depth: usize) u64 {
    const actual = if (!s.tail) depth else if (depth == s.depth) len -| 8 else depth - 8;
    return word(s.data, off, len, actual);
}
const Worker = struct {
    s: *Shared,
    index: usize,
    hist: [NP]u32 = @splat(0),
    dest: [NP]usize = undefined,
    heap: Heap = .initContext({}),
    levels: [32]Level = @splat(.{}),
    sizes: [32]u64 = @splat(0),
    small_ns: u64 = 0,
    groups: u64 = 0,
    leaf_rows: u64 = 0,
    max_part: usize = 0,
    a: []Record = &.{},
    b: []Record = &.{},

    fn keep(w: *Worker, rows: []const Record, f: u16) void {
        var first: u32 = OM;
        for (rows) |r| first = @min(first, r.ordinal());
        w.groups += 1;
        w.leaf_rows += rows.len;
        const r: Result = .{ .count = @intCast(rows.len), .first = first, .f = f };
        if (w.heap.count() == 10) {
            if (worse({}, r, w.heap.peek().?) != .gt) return;
            _ = w.heap.pop();
        }
        w.heap.push(A, r) catch @panic("oom");
        if (w.heap.count() == 10 and w.s.prune)
            _ = w.s.threshold.fetchMax(w.heap.peek().?.count, .monotonic);
    }
    fn recurse(w: *Worker, input: []Record, tmp: []Record, depth_in: usize, len: usize, f: u16) void {
        var depth = depth_in;
        var a = input;
        const b = tmp;
        while (true) {
            const level = &w.levels[@min(depth / 8, w.levels.len - 1)];
            level.calls += 1;
            level.rows += a.len;
            w.sizes[std.math.log2_int(usize, a.len)] += 1;
            if (a.len < w.s.threshold.load(.monotonic) and w.s.prune) {
                level.prune_rows += a.len;
                return;
            }
            if (a.len == 1) {
                level.leaves += 1;
                w.keep(a, f);
                return;
            }
            const started = now();
            var diff: u64 = 0;
            for (a[1..]) |r| diff |= r.word ^ a[0].word;
            if (diff == 0) {
                if (depth + 8 >= len) {
                    level.leaves += 1;
                    w.keep(a, f);
                    level.ns += now() - started;
                    return;
                }
                depth += 8;
                for (a) |*r| r.word = cachedWord(w.s, r.offset, len, depth);
                level.reload_rows += a.len;
                level.ns += now() - started;
                continue;
            }
            if (a.len <= w.s.cutoff) {
                level.sort_rows += a.len;
                for (1..a.len) |i| {
                    const r = a[i];
                    var j = i;
                    while (j > 0 and a[j - 1].word > r.word) : (j -= 1) {
                        a[j] = a[j - 1];
                        level.sort_moves += 1;
                    }
                    a[j] = r;
                    level.sort_moves += 1;
                }
                const end = now();
                level.ns += end - started;
                w.small_ns += end - started;
                var lo: usize = 0;
                while (lo < a.len) {
                    var hi = lo + 1;
                    while (hi < a.len and a[hi].word == a[lo].word) : (hi += 1) {}
                    w.recurse(a[lo..hi], b[lo..hi], depth, len, f);
                    lo = hi;
                }
                return;
            }
            const common: u7 = @intCast(@clz(diff));
            var digit_bits: u7 = @min(w.s.bits, 64 - common);
            if (w.s.adaptive) digit_bits = @min(digit_bits, @max(1, std.math.log2_int(usize, a.len)));
            const shift: u6 = @intCast(64 - common - digit_bits);
            const buckets = @as(usize, 1) << @intCast(digit_bits);
            const mask: u64 = buckets - 1;
            var counts: [2048]usize = @splat(0);
            var pos: [2048]usize = undefined;
            for (a) |r| counts[(r.word >> shift) & mask] += 1;
            var at: usize = 0;
            for (counts[0..buckets], pos[0..buckets]) |n, *p| {
                p.* = at;
                at += n;
            }
            for (a) |r| {
                const d = (r.word >> shift) & mask;
                b[pos[d]] = r;
                pos[d] += 1;
            }
            level.radix_rows += a.len;
            level.ns += now() - started;
            at = 0;
            for (counts[0..buckets]) |n| {
                if (n != 0) w.recurse(b[at..][0..n], a[at..][0..n], depth, len, f);
                at += n;
            }
            return;
        }
    }
    fn histogram(w: *Worker) void {
        const lo = w.s.rows.len * w.index / w.s.workers.len;
        const hi = w.s.rows.len * (w.index + 1) / w.s.workers.len;
        for (w.s.rows[lo..hi]) |r| w.hist[@as(usize, r.f) * 2 + (r.len >> 7)] += 1;
    }
    fn scatter(w: *Worker) void {
        var buffers: [NP][4]Record = undefined;
        var fill: [NP]u8 = @splat(0);
        const lo = w.s.rows.len * w.index / w.s.workers.len;
        const hi = w.s.rows.len * (w.index + 1) / w.s.workers.len;
        for (w.s.rows[lo..hi], lo..) |r, i| {
            const p = @as(usize, r.f) * 2 + (r.len >> 7);
            if (w.s.depth > r.len or (w.s.depth != 0 and !std.mem.eql(u8, w.s.data[r.offset..][0..w.s.depth], "https://example.org/"[0..w.s.depth])))
                w.s.bad_prefix.store(true, .monotonic);
            buffers[p][fill[p]] = .{
                .word = cachedWord(w.s, r.offset, r.len, w.s.depth),
                .offset = r.offset,
                .ord_len = @as(u32, @intCast(i)) | (@as(u32, r.len & 127) << 25),
            };
            fill[p] += 1;
            if (fill[p] == 4) {
                @memcpy(w.s.records[w.dest[p]..][0..4], &buffers[p]);
                w.dest[p] += 4;
                fill[p] = 0;
            }
        }
        for (fill, 0..) |n, p| if (n != 0) @memcpy(w.s.records[w.dest[p]..][0..n], buffers[p][0..n]);
    }
    fn fold(w: *Worker) void {
        while (true) {
            const p = w.s.cursor.fetchAdd(1, .monotonic);
            if (p >= NP) return;
            const lo = w.s.starts[p];
            const hi = w.s.starts[p + 1];
            const n = hi - lo;
            if (n == 0) continue;
            w.max_part = @max(w.max_part, n);
            if (w.a.len < n) {
                A.free(w.a);
                A.free(w.b);
                w.a = A.alloc(Record, n) catch @panic("oom");
                w.b = A.alloc(Record, n) catch @panic("oom");
            }
            var counts: [128]usize = @splat(0);
            var pos: [128]usize = undefined;
            for (w.s.records[lo..hi]) |r| counts[r.ord_len >> 25] += 1;
            var at: usize = 0;
            for (counts, &pos) |c, *v| {
                v.* = at;
                at += c;
            }
            for (w.s.records[lo..hi]) |r| {
                const d = r.ord_len >> 25;
                w.a[pos[d]] = r;
                pos[d] += 1;
            }
            at = 0;
            for (counts, 0..) |c, low| {
                if (c != 0) w.recurse(w.a[at..][0..c], w.b[at..][0..c], w.s.depth, (p % 2) * 128 + low, @intCast(p / 2));
                at += c;
            }
        }
    }
};
fn parallel(ws: []Worker, comptime run: fn (*Worker) void) void {
    var threads: [63]std.Thread = undefined;
    for (ws[1..], threads[0 .. ws.len - 1]) |*w, *t| t.* = std.Thread.spawn(.{ .stack_size = 4 << 20 }, run, .{w}) catch @panic("spawn");
    run(&ws[0]);
    for (threads[0 .. ws.len - 1]) |t| t.join();
}
pub fn mapped(io: std.Io, path: []const u8) ![]align(std.heap.page_size_min) const u8 {
    const f = try std.Io.Dir.cwd().openFile(io, path, .{});
    defer f.close(io);
    const st = try f.stat(io);
    return try std.posix.mmap(null, @intCast(st.size), .{ .READ = true }, .{ .TYPE = .PRIVATE }, f.handle, 0);
}
pub fn main(init: std.process.Init) !void {
    const args = try init.minimal.args.toSlice(init.arena.allocator());
    if (args.len < 6) return error.Usage;
    const path = args[1];
    const jobs = try std.fmt.parseInt(usize, args[2], 10);
    const cutoff = try std.fmt.parseInt(usize, args[3], 10);
    const prefix = try std.fmt.parseInt(usize, args[4], 10);
    const bits = try std.fmt.parseInt(u6, args[5], 10);
    const tail = args.len > 6 and std.mem.startsWith(u8, args[6], "tail");
    const prune = args.len <= 6 or !std.mem.endsWith(u8, args[6], "full");
    const data = try mapped(init.io, try std.fmt.allocPrint(init.arena.allocator(), "{s}/keys.bin", .{path}));
    defer std.posix.munmap(data);
    const raw_rows = try mapped(init.io, try std.fmt.allocPrint(init.arena.allocator(), "{s}/rows.bin", .{path}));
    defer std.posix.munmap(raw_rows);
    const rows: []align(1) const Row = std.mem.bytesAsSlice(Row, raw_rows);
    if (rows.len >= 1 << 25 or jobs < 1 or jobs > 64 or bits == 0 or bits > 11 or prefix > "https://example.org/".len) return error.BadArgument;
    for (rows) |r| if (r.len > 255 or r.f >= 1000 or @as(usize, r.offset) + r.len > data.len) return error.BadInput;
    // Fault inputs in before kernel timing; decoding is measured separately by the engine's physical-work proxy.
    var touch: u64 = 0;
    var i: usize = 0;
    while (i < data.len) : (i += 4096) touch +%= data[i];
    std.mem.doNotOptimizeAway(touch);
    const started = now();
    const records = try A.alloc(Record, rows.len);
    defer A.free(records);
    const workers = try A.alloc(Worker, jobs);
    defer A.free(workers);
    var s: Shared = .{ .data = data, .rows = rows, .records = records, .workers = workers, .depth = prefix, .cutoff = cutoff, .bits = bits, .adaptive = true, .prune = prune, .tail = tail };
    for (workers, 0..) |*w, wi| {
        w.* = .{ .s = &s, .index = wi };
        try w.heap.ensureTotalCapacity(A, 10);
    }
    defer for (workers) |*w| {
        w.heap.deinit(A);
        A.free(w.a);
        A.free(w.b);
    };
    parallel(workers, Worker.histogram);
    var at: usize = 0;
    for (0..NP) |p| {
        s.starts[p] = at;
        for (workers) |*w| {
            w.dest[p] = at;
            at += w.hist[p];
        }
    }
    s.starts[NP] = at;
    const hist_end = now();
    parallel(workers, Worker.scatter);
    if (s.bad_prefix.load(.monotonic)) return error.PrefixNotVerified;
    const scatter_end = now();
    parallel(workers, Worker.fold);
    const fold_end = now();
    const h = &workers[0].heap;
    for (workers[1..]) |*w| for (w.heap.items) |r| {
        if (h.count() == 10) {
            if (worse({}, r, h.peek().?) != .gt) continue;
            _ = h.pop();
        }
        try h.push(A, r);
    };
    std.mem.sort(Result, h.items, {}, struct {
        fn less(_: void, a: Result, b: Result) bool {
            return worse({}, a, b) == .gt;
        }
    }.less);
    const end = now();
    std.debug.print("{{\"rows\":{d},\"jobs\":{d},\"cutoff\":{d},\"prefix\":{d},\"bits\":{d},\"prune\":{},\"total_ns\":{d},\"hist_ns\":{d},\"scatter_ns\":{d},\"fold_ns\":{d},\"combine_ns\":{d},\"rss_kib\":{d},\"threshold\":{d},\"top\":[", .{
        rows.len,               jobs,           cutoff,                        prefix,                       bits, prune, end - started, hist_end - started, scatter_end - hist_end,
        fold_end - scatter_end, end - fold_end, std.posix.getrusage(0).maxrss, s.threshold.load(.monotonic),
    });
    for (h.items, 0..) |r, j| std.debug.print("{s}{{\"count\":{d},\"first\":{d},\"f\":{d}}}", .{ if (j == 0) "" else ",", r.count, r.first, r.f });
    std.debug.print("],\"levels\":[", .{});
    var groups: u64 = 0;
    var leaf_rows: u64 = 0;
    var max_part: usize = 0;
    var sizes: [32]u64 = @splat(0);
    for (workers) |w| {
        groups += w.groups;
        leaf_rows += w.leaf_rows;
        max_part = @max(max_part, w.max_part);
        for (&sizes, w.sizes) |*x, y| x.* += y;
    }
    for (0..32) |d| {
        var l: Level = .{};
        inline for (comptime std.meta.fieldNames(Level)) |field| {
            for (workers) |w| @field(l, field) += @field(w.levels[d], field);
        }
        std.debug.print("{s}{{\"level\":{d}", .{ if (d == 0) "" else ",", d });
        inline for (comptime std.meta.fieldNames(Level)) |field| std.debug.print(",\"{s}\":{d}", .{ field, @field(l, field) });
        std.debug.print("}}", .{});
    }
    std.debug.print("],\"groups_finished\":{d},\"rows_finished\":{d},\"max_part\":{d},\"bucket_log2_hist\":[", .{ groups, leaf_rows, max_part });
    for (sizes, 0..) |v, j| std.debug.print("{s}{d}", .{ if (j == 0) "" else ",", v });
    std.debug.print("]}}\n", .{});
}
