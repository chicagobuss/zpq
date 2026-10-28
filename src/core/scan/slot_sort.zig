const std = @import("std");
const testing = std.testing;
const expr_agg = @import("../expr/agg.zig");
const spawn_util = @import("../spawn.zig");
const MergedSlot = @import("count_cut.zig").MergedSlot;
const WORKER_STACK_SIZE = @import("types.zig").WORKER_STACK_SIZE;

/// The widest digit a pass sorts by: 8192 buckets a chunk.
const radix_bits_max = 13;
/// Fewer slots than this per chunk sort on fewer threads: a chunk's pass over its buckets would cost more than its
/// share of the slots.
const radix_chunk_min = 1 << 16;

/// Sort `slots` by `first`, the input row that created each group, so unique per group: answers list groups in
/// first-appearance order, the same at every `-j`. An LSD radix sort, so every pass splits into chunks of slots each
/// worker counts and scatters on its own. `first` packs a row group ordinal above a row index (`firstPosition`), so
/// most of its 64 bits are the same in every slot: the sort key is `first` with the row index's unused high bits
/// squeezed out (`SlotRadix.key`, order-preserving), and its varying bits are split into as few passes as cover them.
pub fn sortSlotsByFirst(gpa: std.mem.Allocator, slots: []MergedSlot, n_workers: usize) error{OutOfMemory}!void {
    if (slots.len < 2) return;
    const n_chunks = @max(1, @min(n_workers, slots.len / radix_chunk_min));
    const scratch = try gpa.alloc(MergedSlot, slots.len);
    defer gpa.free(scratch);
    const counts = try gpa.alloc(usize, n_chunks << radix_bits_max);
    defer gpa.free(counts);
    const spans = try gpa.alloc(SlotRadix.Span, n_chunks);
    defer gpa.free(spans);
    const tasks = try gpa.alloc(SlotRadix.Task, n_chunks);
    defer gpa.free(tasks);

    var sort: SlotRadix = .{ .src = slots, .dst = scratch, .counts = counts, .spans = spans, .n_chunks = n_chunks };
    for (tasks) |*t| t.* = .{ .sort = &sort };
    sort.runPhase(tasks, .span);
    var any: u64 = 0;
    var all: u64 = std.math.maxInt(u64);
    for (spans) |s| {
        any |= s.any;
        all &= s.all;
    }
    // Every slot's row index is below 2^row_bits. `key` moves bits, so it maps an OR or an AND of firsts to the same
    // of their keys.
    sort.row_bits = @intCast(64 - @clz(any & 0xffff_ffff));
    const varying = sort.key(any) ^ sort.key(all);
    if (varying == 0) return;
    const lo: usize = @ctz(varying);
    const span_bits = 64 - @clz(varying) - lo;
    const passes = std.math.divCeil(usize, span_bits, radix_bits_max) catch unreachable;
    sort.bits = @intCast(std.math.divCeil(usize, span_bits, passes) catch unreachable);
    const buckets = @as(usize, 1) << sort.bits;

    var shift: usize = lo;
    while (shift < lo + span_bits) : (shift += sort.bits) {
        sort.shift = @intCast(shift);
        if ((varying >> sort.shift) & (buckets - 1) == 0) continue;
        sort.runPhase(tasks, .count);
        // Bucket b starts after every chunk's slots of the buckets below it; within b, chunk c's after chunks < c, so
        // each pass is stable, as LSD needs.
        var at: usize = 0;
        for (0..buckets) |b| {
            for (0..n_chunks) |c| {
                const n = counts[c * buckets + b];
                counts[c * buckets + b] = at;
                at += n;
            }
        }
        sort.runPhase(tasks, .scatter);
        std.mem.swap([]MergedSlot, &sort.src, &sort.dst);
    }
    if (sort.src.ptr != slots.ptr) {
        sort.dst = slots;
        sort.runPhase(tasks, .copy);
    }
}

const SlotRadix = struct {
    src: []MergedSlot,
    dst: []MergedSlot,
    /// Per chunk, a count per bucket of the current digit, then where the chunk writes its next slot of that bucket.
    counts: []usize,
    spans: []Span,
    n_chunks: usize,
    shift: u6 = 0,
    /// The digit's width, at most `radix_bits_max`.
    bits: u4 = radix_bits_max,
    /// Bits of the row index in `key`: as many as the largest row index of any slot takes.
    row_bits: u6 = 32,
    phase: Phase = .span,
    cursor: std.atomic.Value(usize) = .init(0),

    /// The bits set in any of a chunk's `first`s, and in all of them.
    const Span = struct { any: u64, all: u64 };
    const Phase = enum { span, count, scatter, copy };
    const Task = struct { sort: *SlotRadix };

    fn runPhase(s: *SlotRadix, tasks: []Task, phase: Phase) void {
        s.phase = phase;
        s.cursor.store(0, .monotonic);
        const config: std.Thread.SpawnConfig = .{ .stack_size = WORKER_STACK_SIZE };
        if (tasks.len == 1 or !spawn_util.runTree(Task, tasks, config, run)) run(&tasks[0]);
    }

    /// Claim chunks until none remain: one a failed spawn left unclaimed is taken by a thread that did start.
    fn run(t: *Task) void {
        const s = t.sort;
        while (true) {
            const c = s.cursor.fetchAdd(1, .monotonic);
            if (c >= s.n_chunks) return;
            const lo = s.src.len * c / s.n_chunks;
            const hi = s.src.len * (c + 1) / s.n_chunks;
            const buckets = @as(usize, 1) << s.bits;
            const mine = s.counts[c * buckets ..][0..buckets];
            switch (s.phase) {
                .span => {
                    var span: Span = .{ .any = 0, .all = std.math.maxInt(u64) };
                    for (s.src[lo..hi]) |slot| {
                        span.any |= slot.first;
                        span.all &= slot.first;
                    }
                    s.spans[c] = span;
                },
                .count => {
                    @memset(mine, 0);
                    for (s.src[lo..hi]) |slot| mine[s.digit(slot.first)] += 1;
                },
                .scatter => for (s.src[lo..hi]) |slot| {
                    const d = s.digit(slot.first);
                    s.dst[mine[d]] = slot;
                    mine[d] += 1;
                },
                .copy => @memcpy(s.dst[lo..hi], s.src[lo..hi]),
            }
        }
    }

    /// `first`'s row group ordinal right above its row index's `row_bits`: the same order on fewer bits.
    fn key(s: *const SlotRadix, first: u64) u64 {
        const row_mask = (@as(u64, 1) << s.row_bits) - 1;
        return (first >> 32) << s.row_bits | (first & row_mask);
    }

    fn digit(s: *const SlotRadix, first: u64) usize {
        return @intCast((s.key(first) >> s.shift) & ((@as(u64, 1) << s.bits) - 1));
    }
};

fn slotFirstLess(_: void, a: MergedSlot, b: MergedSlot) bool {
    return a.first < b.first;
}

test "sortSlotsByFirst: a comparison sort's order at every -j, and when spawns fail" {
    var prng = std.Random.DefaultPrng.init(0x51075);
    const random = prng.random();
    const Spread = enum {
        /// `firstPosition`s over 29 row groups: the key squeezes the row index's unused high bits out.
        positions,
        /// Row indexes that take all 32 bits, so the key is `first` itself.
        high_rows,
        /// Row index 0 everywhere: the key is the row group ordinal alone.
        ordinals,
        /// Every bit differs: five passes, the last one short.
        wide,
        /// Below 2^13: one pass, so the answer is left in the scratch and copied back.
        narrow,
    };
    for ([_]usize{ 0, 1, 2, 1000, 300_000 }) |n| for (std.enums.values(Spread)) |spread| {
        if (spread == .narrow and n > 1 << radix_bits_max) continue;
        const input = try testing.allocator.alloc(MergedSlot, n);
        defer testing.allocator.free(input);
        for (input, 0..) |*s, i| s.* = .{
            .key = "",
            .first = switch (spread) {
                .positions => expr_agg.firstPosition(i % 29, i / 29),
                .high_rows => expr_agg.firstPosition(i % 3, 0xffff_0000 - i / 3),
                .ordinals => expr_agg.firstPosition(i, 0),
                // An odd multiplier permutes the u64s, so these stay unique.
                .wide => @as(u64, i) *% 0x9E3779B97F4A7C15,
                .narrow => i,
            },
            .idx = i,
        };
        random.shuffle(MergedSlot, input);
        const want = try testing.allocator.dupe(MergedSlot, input);
        defer testing.allocator.free(want);
        std.sort.pdq(MergedSlot, want, {}, slotFirstLess);

        const Run = struct { j: usize, fail_after: ?usize = null };
        for ([_]Run{ .{ .j = 1 }, .{ .j = 3 }, .{ .j = 8 }, .{ .j = 8, .fail_after = 0 }, .{ .j = 8, .fail_after = 1 }, .{ .j = 8, .fail_after = 2 } }) |run| {
            const got = try testing.allocator.dupe(MergedSlot, input);
            defer testing.allocator.free(got);
            if (run.fail_after) |f| spawn_util.injectFailureAfter(f);
            defer spawn_util.resetFailure();
            try sortSlotsByFirst(testing.allocator, got, run.j);
            // Only the largest input has the chunks to spread over threads.
            if (run.fail_after != null and n >= 2 * radix_chunk_min) try testing.expect(spawn_util.failuresInjected() > 0);
            for (want, got) |w, g| {
                try testing.expectEqual(w.first, g.first);
                try testing.expectEqual(w.idx, g.idx);
            }
        }
    };
}

test "sortSlotsByFirst: an allocation failure leaves the slots as they were" {
    var slots: [5]MergedSlot = undefined;
    for (&slots, 0..) |*s, i| s.* = .{ .key = "", .first = 40 - i, .idx = i };
    const before = slots;
    var fail_index: usize = 0;
    while (true) : (fail_index += 1) {
        var failing = std.testing.FailingAllocator.init(testing.allocator, .{ .fail_index = fail_index });
        sortSlotsByFirst(failing.allocator(), &slots, 1) catch |err| {
            try testing.expectEqual(error.OutOfMemory, err);
            for (before, slots) |b, s| try testing.expectEqual(b.idx, s.idx);
            continue;
        };
        break;
    }
    try testing.expect(fail_index > 0);
    for (slots, 0..) |s, i| try testing.expectEqual(slots.len - 1 - i, s.idx);
}

