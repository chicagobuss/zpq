const std = @import("std");
const common = @import("main.zig");
const A = std.heap.smp_allocator;
const Row = common.Row;
const now = common.now;
const shard_count = 256;
const batch_size = 4096;
const none = std.math.maxInt(u32);
fn mix(v: u64) u64 {
    var x = v;
    x ^= x >> 30;
    x *%= 0xbf58476d1ce4e5b9;
    x ^= x >> 27;
    x *%= 0x94d049bb133111eb;
    return x ^ (x >> 31);
}
const Lock = struct {
    held: std.atomic.Value(bool) = .init(false),
    fn lock(l: *Lock) void {
        while (l.held.cmpxchgWeak(false, true, .acquire, .monotonic) != null) std.atomic.spinLoopHint();
    }
    fn unlock(l: *Lock) void {
        l.held.store(false, .release);
    }
};
fn parallel(comptime T: type, ws: []T, comptime run: fn (*T) void) void {
    var ts: [63]std.Thread = undefined;
    for (ws[1..], ts[0 .. ws.len - 1]) |*w, *t| t.* = std.Thread.spawn(.{}, run, .{w}) catch @panic("spawn");
    run(&ws[0]);
    for (ts[0 .. ws.len - 1]) |t| t.join();
}
fn Probe(comptime C: type) type {
    return struct {
        const Self = @This();
        const width = (16 << 20) / @sizeOf(C);
        const per_shard = width / shard_count;
        const Temp = struct { h: u64, next: u32 };
        data: []const u8,
        rows: []align(1) const Row,
        counters: []C,
        jobs: usize,
        threshold: u32,
        locks: [shard_count]Lock = @splat(.{}),
        const Worker = struct {
            s: *Self,
            index: usize,
            kept: u64 = 0,
            saturated: u64 = 0,
            max_estimate: u32 = 0,
            hash_ns: u64 = 0,
            update_ns: u64 = 0,
            fn hash(w: *Worker, i: usize) u64 {
                const r = w.s.rows[i];
                const h = std.hash.Wyhash.hash(0, w.s.data[r.offset..][0..r.len]);
                return mix(h ^ (@as(u64, r.f) *% 0x9e3779b97f4a7c15));
            }
            fn build(w: *Worker) void {
                const s = w.s;
                const lo = s.rows.len * w.index / s.jobs;
                const hi = s.rows.len * (w.index + 1) / s.jobs;
                var temps: [batch_size]Temp = undefined;
                var base = lo;
                while (base < hi) : (base += @min(batch_size, hi - base)) {
                    var heads: [shard_count]u32 = @splat(none);
                    const n = @min(batch_size, hi - base);
                    var started = now();
                    for (temps[0..n], base..) |*t, i| {
                        const h = w.hash(i);
                        const p = h >> 56;
                        t.* = .{ .h = h, .next = heads[p] };
                        heads[p] = @intCast(i - base);
                    }
                    w.hash_ns += now() - started;
                    started = now();
                    for (0..shard_count) |j| {
                        const p = (j + w.index) % shard_count;
                        if (heads[p] == none) continue;
                        s.locks[p].lock();
                        var id = heads[p];
                        while (id != none) {
                            const t = temps[id];
                            const a = p * per_shard + (t.h & (per_shard - 1));
                            const b = width + p * per_shard + (mix(t.h ^ 0xd6e8feb86659fd93) & (per_shard - 1));
                            s.counters[a] +|= 1;
                            s.counters[b] +|= 1;
                            id = t.next;
                        }
                        s.locks[p].unlock();
                    }
                    w.update_ns += now() - started;
                }
            }
            fn filter(w: *Worker) void {
                const s = w.s;
                const lo = s.rows.len * w.index / s.jobs;
                const hi = s.rows.len * (w.index + 1) / s.jobs;
                for (lo..hi) |i| {
                    const h = w.hash(i);
                    const p = h >> 56;
                    const a = p * per_shard + (h & (per_shard - 1));
                    const b = width + p * per_shard + (mix(h ^ 0xd6e8feb86659fd93) & (per_shard - 1));
                    const estimate = @min(s.counters[a], s.counters[b]);
                    const saturated = estimate == std.math.maxInt(C);
                    if (saturated or estimate >= s.threshold) w.kept += 1;
                    if (saturated) w.saturated += 1;
                    w.max_estimate = @max(w.max_estimate, estimate);
                }
            }
        };
        fn run(data: []const u8, rows: []align(1) const Row, jobs: usize, threshold: u32) !void {
            const started = now();
            const counters = try A.alloc(C, width * 2);
            defer A.free(counters);
            @memset(counters, 0);
            const ws = try A.alloc(Worker, jobs);
            defer A.free(ws);
            var s: Self = .{ .data = data, .rows = rows, .counters = counters, .jobs = jobs, .threshold = threshold };
            for (ws, 0..) |*w, i| w.* = .{ .s = &s, .index = i };
            const init_end = now();
            parallel(Worker, ws, Worker.build);
            const build_end = now();
            parallel(Worker, ws, Worker.filter);
            const end = now();
            var kept: u64 = 0;
            var saturated: u64 = 0;
            var hash_ns: u64 = 0;
            var update_ns: u64 = 0;
            for (ws) |w| {
                kept += w.kept;
                saturated += w.saturated;
                hash_ns += w.hash_ns;
                update_ns += w.update_ns;
            }
            std.debug.print("{{\"kind\":\"cms_oracle_lower_bound\",\"counter_bits\":{d},\"jobs\":{d},\"bytes\":{d},\"oracle_T\":{d},\"total_ns\":{d},\"init_ns\":{d},\"build_ns\":{d},\"filter_ns\":{d},\"hash_busy_ns\":{d},\"update_busy_ns\":{d},\"survivor_rows\":{d},\"saturated_rows\":{d},\"rss_kib\":{d}}}\n", .{ @bitSizeOf(C), jobs, width * 2 * @sizeOf(C), threshold, end - started, init_end - started, build_end - init_end, end - build_end, hash_ns, update_ns, kept, saturated, std.posix.getrusage(0).maxrss });
        }
    };
}
pub fn main(init: std.process.Init) !void {
    const args = try init.minimal.args.toSlice(init.arena.allocator());
    if (args.len < 5) return error.Usage;
    const path = args[1];
    const jobs = try std.fmt.parseInt(usize, args[2], 10);
    const threshold = try std.fmt.parseInt(u32, args[3], 10);
    const bits = try std.fmt.parseInt(u8, args[4], 10);
    if (jobs == 0 or jobs > 64) return error.BadArgument;
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
    switch (bits) {
        8 => try Probe(u8).run(data, rows, jobs, threshold),
        16 => try Probe(u16).run(data, rows, jobs, threshold),
        else => return error.BadArgument,
    }
}
