//! Shared COUNT/DISTINCT certificate policy. Samples choose state; complete bounds prove the answer.
const std = @import("std");
const hw = @import("hw.zig");
const consumer = @import("consumer.zig");
const schema = @import("schema.zig");

pub const Source = struct {
    rg: *const schema.RowGroup,
    meta: *const schema.FileMetaData,
    src: consumer.RGSrc,
};

/// Equal input-wide strata, with a short contiguous window centered in each. Quotients and remainders keep
/// positions in range without multiplying two input sizes. Row and sample remainders go to the same strata,
/// so a stratum can always hold its window, even when sampling nearly every row.
pub const Windows = struct {
    rows: u64,
    samples: u64,
    strata: u64,

    pub const Window = struct { start: u64, len: u64 };

    pub fn init(rows: u64, samples: u64, strata: u64) Windows {
        const n = @min(rows, samples);
        return .{ .rows = rows, .samples = n, .strata = @min(n, @max(1, strata)) };
    }

    pub fn at(self: Windows, i: u64) Window {
        std.debug.assert(i < self.strata);
        const row_base = self.rows / self.strata;
        const row_extra = self.rows % self.strata;
        const lo = row_base * i + @min(i, row_extra);
        const size = row_base + @intFromBool(i < row_extra);
        const n = self.samples / self.strata + @intFromBool(i < self.samples % self.strata);
        return .{ .start = lo + (size - n) / 2, .len = n };
    }
};

pub const Sample = struct {
    counts: std.AutoHashMapUnmanaged(i64, u64) = .empty,
    rows: u64 = 0,
    fmin: i64 = std.math.maxInt(i64),
    fmax: i64 = std.math.minInt(i64),

    pub fn deinit(self: *Sample, a: std.mem.Allocator) void {
        self.counts.deinit(a);
        self.* = .{};
    }

    fn add(self: *Sample, a: std.mem.Allocator, k: ?i64, f: ?i64) !void {
        self.rows += 1;
        // NULL stays cold. A sampled range is only membership policy; late/out-of-range values stay cold too.
        const key = k orelse return;
        const value = f orelse return;
        const entry = try self.counts.getOrPut(a, key);
        if (!entry.found_existing) entry.value_ptr.* = 0;
        entry.value_ptr.* += 1;
        self.fmin = @min(self.fmin, value);
        self.fmax = @max(self.fmax, value);
    }
};

fn integer(col: @import("filter/eval.zig").Batch.Column, row: usize) ?i64 {
    return switch (col) {
        inline .i32, .i64 => |c| if (c.def_levels != null and c.def_levels.?[row] < c.max_def)
            null
        else
            c.values[row],
        else => unreachable,
    };
}

const IntegerRange = struct { min: i64, max: i64 };

// Bounds choose membership only. A narrow (even false) footer range leaves other values cold; an absent or wide
// range falls back to sampling real values, so a rare outlier cannot prevent finding a useful hot interval.
fn secondaryHint(sources: []const Source, ci: usize) ?IntegerRange {
    var range: IntegerRange = .{ .min = std.math.maxInt(i64), .max = std.math.minInt(i64) };
    for (sources) |source| {
        if (source.rg.num_rows <= 0) continue;
        const bounds = @import("parquet/statistics.zig").chunkStats(
            source.rg,
            ci,
            source.meta,
            .corroborated,
        ).bounds orelse
            return null;
        const cm = source.rg.columns.items[ci].meta_data orelse return null;
        const elem = source.meta.getColumnSchema(cm.path_in_schema.items) orelse return null;
        if (!@import("parquet/statistics.zig").signedIntOrder(elem)) return null;
        const lo: i64, const hi: i64 = switch (cm.type) {
            .INT32 => if (bounds.min.len == 4 and bounds.max.len == 4)
                .{ std.mem.readInt(i32, bounds.min[0..4], .little), std.mem.readInt(i32, bounds.max[0..4], .little) }
            else
                return null,
            .INT64 => if (bounds.min.len == 8 and bounds.max.len == 8)
                .{ std.mem.readInt(i64, bounds.min[0..8], .little), std.mem.readInt(i64, bounds.max[0..8], .little) }
            else
                return null,
            else => return null,
        };
        range.min = @min(range.min, lo);
        range.max = @max(range.max, hi);
    }
    const width = @as(i128, range.max) - range.min + 1;
    return if (width > 0 and width <= hw.l2BytesPerWorkerOr(640 << 10) / 12) range else null;
}

const SampleTasks = struct {
    a: std.mem.Allocator,
    sources: []const Source,
    bases: []const u64,
    windows: Windows,
    key: usize,
    second: ?usize,
    hint: ?IntegerRange,
    options: consumer.DecodeOptions,
    cursor: std.atomic.Value(usize) = .init(0),

    const Task = struct {
        shared: *SampleTasks,
        result: Sample = .{},
        err: ?anyerror = null,

        fn run(task: *Task) void {
            const s = task.shared;
            while (true) {
                const i = s.cursor.fetchAdd(1, .monotonic);
                if (i >= s.sources.len) return;
                sampleSource(s, i, &task.result) catch |e| {
                    task.err = e;
                    return;
                };
            }
        }
    };
};

fn sampleSource(s: *const SampleTasks, i: usize, result: *Sample) !void {
    const source = s.sources[i];
    const base = s.bases[i];
    const end = base + @as(u64, @intCast(@max(0, source.rg.num_rows)));
    if (end == base) return;
    // Find the first overlapping window without revisiting every stratum in every row group.
    var wi: u64 = 0;
    var high = s.windows.strata;
    while (wi < high) {
        const middle = wi + (high - wi) / 2;
        const w = s.windows.at(middle);
        if (w.start + w.len <= base) wi = middle + 1 else high = middle;
    }
    if (wi == s.windows.strata or s.windows.at(wi).start >= end) return;
    var held = std.heap.ArenaAllocator.init(s.a);
    defer held.deinit();
    var first = try consumer.SliceReader.init(
        held.allocator(),
        s.a,
        source.rg,
        source.meta,
        source.src,
        s.key,
        s.options,
    ) orelse
        return error.SampleUnsupported;
    defer first.deinit();
    var other: ?consumer.SliceReader = if (if (s.hint == null) s.second else null) |ci|
        try consumer.SliceReader.init(held.allocator(), s.a, source.rg, source.meta, source.src, ci, s.options) orelse
            return error.SampleUnsupported
    else
        null;
    defer if (other) |*r| r.deinit();
    var scratch = std.heap.ArenaAllocator.init(s.a);
    defer scratch.deinit();
    var position = base;
    while (wi < s.windows.strata) : (wi += 1) {
        const window = s.windows.at(wi);
        if (window.start >= end) break;
        const lo = @max(base, window.start);
        const hi = @min(end, window.start + window.len);
        _ = scratch.reset(.retain_capacity);
        try first.skip(scratch.allocator(), @intCast(lo - position));
        if (other) |*r| try r.skip(scratch.allocator(), @intCast(lo - position));
        const n: usize = @intCast(hi - lo);
        const ks = try first.next(scratch.allocator(), n);
        const fs = if (other) |*r| try r.next(scratch.allocator(), n) else null;
        for (0..n) |r| try result.add(s.a, integer(ks, r), if (fs) |f| integer(f, r) else 0);
        position = hi;
    }
}

/// Samples only choose membership. Row-group tasks use the existing pool and private decode/sample state,
/// so input-wide windows do not serialize cold faults. Joining precedes every merge, failure or release.
pub fn sample(
    a: std.mem.Allocator,
    sources: []const Source,
    key: usize,
    second: ?usize,
    h: usize,
    workers: usize,
    options: consumer.DecodeOptions,
) !Sample {
    var result: Sample = .{};
    errdefer result.deinit(a);
    const bases = try a.alloc(u64, sources.len);
    defer a.free(bases);
    var rows: u64 = 0;
    for (sources, bases) |source, *base| {
        base.* = rows;
        rows = std.math.add(u64, rows, @intCast(@max(0, source.rg.num_rows))) catch return result;
    }
    const capacity = hw.l2BytesPerWorkerOr(640 << 10) / (2 * (8 + 8 + 1));
    if (h > capacity or rows == 0) return result;
    var shared: SampleTasks = .{
        .a = a,
        .sources = sources,
        .bases = bases,
        .windows = Windows.init(rows, @min(rows, capacity), @max(sources.len, workers)),
        .key = key,
        .second = second,
        .hint = if (second) |ci| secondaryHint(sources, ci) else null,
        .options = options,
    };
    const tasks = try a.alloc(SampleTasks.Task, @min(sources.len, @max(1, workers)));
    defer a.free(tasks);
    for (tasks) |*task| task.* = .{ .shared = &shared };
    defer for (tasks) |*task| task.result.deinit(a);
    const spawn = @import("spawn.zig");
    if (tasks.len == 1 or !spawn.runTree(SampleTasks.Task, tasks, .{}, SampleTasks.Task.run)) tasks[0].run();
    for (tasks) |task| {
        if (task.err) |e| return e;
        result.rows += task.result.rows;
        result.fmin = @min(result.fmin, task.result.fmin);
        result.fmax = @max(result.fmax, task.result.fmax);
        var it = task.result.counts.iterator();
        while (it.next()) |entry| {
            const out = try result.counts.getOrPut(a, entry.key_ptr.*);
            if (!out.found_existing) out.value_ptr.* = 0;
            out.value_ptr.* += entry.value_ptr.*;
        }
    }
    if (shared.hint) |range| {
        result.fmin = range.min;
        result.fmax = range.max;
    }
    return result;
}

test "sample windows cover the input, stay disjoint and spend exactly their row budget" {
    const t = std.testing;
    for (0..70) |rows| for (0..90) |requested| for (1..12) |strata| {
        const windows = Windows.init(rows, requested, strata);
        var end: u64 = 0;
        var n: u64 = 0;
        for (0..@intCast(windows.strata)) |i| {
            const w = windows.at(i);
            try t.expect(w.start >= end and w.len > 0);
            end = w.start + w.len;
            try t.expect(end <= rows);
            n += w.len;
        }
        try t.expectEqual(@min(rows, requested), n);
    };
    const wide = Windows.init(std.math.maxInt(u64), 65536, 31);
    try t.expect(wide.at(30).start > std.math.maxInt(u64) / 2);
}

fn sampleAllocation(a: std.mem.Allocator) !void {
    var s: Sample = .{};
    defer s.deinit(a);
    for (0..200) |i| try s.add(a, @intCast(i % 17), -@as(i64, @intCast(i)));
    try std.testing.expectEqual(@as(u64, 200), s.rows);
    try std.testing.expectEqual(@as(usize, 17), s.counts.count());
    try std.testing.expectEqual(@as(i64, -199), s.fmin);
    try std.testing.expectEqual(@as(i64, 0), s.fmax);
}

test "sample owns bounded exact keys, ignores NULL membership and frees failed allocations" {
    try std.testing.checkAllAllocationFailures(std.testing.allocator, sampleAllocation, .{});
    var s: Sample = .{};
    defer s.deinit(std.testing.allocator);
    try s.add(std.testing.allocator, null, 1);
    try s.add(std.testing.allocator, 1, null);
    try std.testing.expectEqual(@as(u64, 2), s.rows);
    try std.testing.expectEqual(@as(usize, 0), s.counts.count());
}

pub const HotPlan = struct {
    keys: []i64,
    ids: std.AutoHashMapUnmanaged(i64, usize) = .empty,
    second: bool,
    fmin: i64,
    width: usize,

    pub fn deinit(self: *HotPlan, a: std.mem.Allocator) void {
        self.ids.deinit(a);
        a.free(self.keys);
    }

    pub fn lanes(self: HotPlan) usize {
        return self.keys.len * self.width;
    }

    /// The canonical fixed key image includes a presence byte per integer. NULL and values outside the sampled
    /// interval remain cold; the sample never proves a range or removes those rows from the bound.
    pub inline fn slot(self: *const HotPlan, key: []const u8) ?usize {
        if (key.len != (if (self.second) @as(usize, 18) else 9) or key[0] == 0) return null;
        const k = std.mem.readInt(i64, key[1..9], .little);
        const hot = self.ids.get(k) orelse return null;
        const offset: usize = if (self.second) f: {
            if (key[9] == 0) return null;
            const value = std.mem.readInt(i64, key[10..18], .little);
            const d: i128 = @as(i128, value) - self.fmin;
            if (d < 0 or d >= self.width) return null;
            break :f @intCast(d);
        } else 0;
        return hot * self.width + offset;
    }

    pub fn image(self: HotPlan, slot_id: usize, out: *[18]u8) []const u8 {
        out[0] = 1;
        std.mem.writeInt(i64, out[1..9], self.keys[slot_id / self.width], .little);
        if (!self.second) return out[0..9];
        out[9] = 1;
        std.mem.writeInt(i64, out[10..18], @intCast(@as(i128, self.fmin) + slot_id % self.width), .little);
        return out;
    }
};

pub const Hot = struct {
    plan: *const HotPlan,
    counts: []u32,
    first: []u64,
    overflow: bool = false,

    pub fn init(a: std.mem.Allocator, plan: *const HotPlan) !Hot {
        const counts = try a.alloc(u32, plan.lanes());
        errdefer a.free(counts);
        const first = try a.alloc(u64, plan.lanes());
        @memset(counts, 0);
        @memset(first, std.math.maxInt(u64));
        return .{ .plan = plan, .counts = counts, .first = first };
    }

    pub fn deinit(self: Hot, a: std.mem.Allocator) void {
        a.free(self.counts);
        a.free(self.first);
    }

    pub inline fn add(self: *Hot, id: usize) void {
        if (self.counts[id] == std.math.maxInt(u32)) self.overflow = true;
        self.counts[id] +|= 1;
    }
};

/// Retain primary keys whose estimated pair counts pay for cold-cell noise. This is a cost decision only. Dense
/// count/first lanes and cold cells must share a worker's L2, including lookup state and decode headroom.
pub fn hotPlan(a: std.mem.Allocator, s: Sample, second: bool, h: usize, cells: usize) !?HotPlan {
    if (s.counts.count() == 0 or s.rows == 0 or h == 0) return null;
    const width = if (second) std.math.cast(usize, @as(i128, s.fmax) - s.fmin + 1) orelse return null else 1;
    if (width == 0) return null;
    const room = hw.l2BytesPerWorkerOr(640 << 10) -| cells *| @sizeOf(u32) -| (64 << 10);
    const capacity = @min(s.counts.count(), room / (@as(usize, 12) *| width +| 64));
    if (capacity == 0) return null;
    const Entry = struct { key: i64, count: u64 };
    const ranked = try a.alloc(Entry, s.counts.count());
    defer a.free(ranked);
    var it = s.counts.iterator();
    var n: usize = 0;
    while (it.next()) |e| : (n += 1) ranked[n] = .{ .key = e.key_ptr.*, .count = e.value_ptr.* };
    std.mem.sortUnstable(Entry, ranked, {}, struct {
        fn less(_: void, x: Entry, y: Entry) bool {
            return x.count > y.count or (x.count == y.count and x.key < y.key);
        }
    }.less);
    n = 0;
    const noise_work = 1 + std.math.log2_int(usize, h);
    for (ranked[0..capacity]) |e| {
        if (@as(u128, e.count) * cells <= @as(u128, s.rows) * width * noise_work) break;
        n += 1;
    }
    if (n == 0 or n * width < h) return null;
    var plan: HotPlan = .{
        .keys = try a.alloc(i64, n),
        .second = second,
        .fmin = if (second) s.fmin else 0,
        .width = width,
    };
    errdefer plan.deinit(a);
    try plan.ids.ensureTotalCapacity(a, @intCast(n));
    for (ranked[0..n], plan.keys, 0..) |e, *key, id| {
        key.* = e.key;
        plan.ids.putAssumeCapacity(e.key, id);
    }
    return plan;
}

fn hotAllocation(a: std.mem.Allocator) !void {
    var s: Sample = .{};
    defer s.deinit(a);
    for (0..300) |i| try s.add(a, @intCast(i % 3), @as(i64, @intCast(i % 5)) - 2);
    var plan = (try hotPlan(a, s, true, 3, 256)) orelse return error.TestUnexpectedResult;
    defer plan.deinit(a);
    var hot = try Hot.init(a, &plan);
    defer hot.deinit(a);
    for (0..plan.lanes()) |id| {
        var bytes: [18]u8 = undefined;
        try std.testing.expectEqual(id, plan.slot(plan.image(id, &bytes)).?);
        hot.add(id);
        try std.testing.expectEqual(@as(u32, 1), hot.counts[id]);
    }
    var bytes: [18]u8 = undefined;
    _ = plan.image(0, &bytes);
    bytes[0] = 0;
    try std.testing.expect(plan.slot(&bytes) == null);
    bytes[0] = 1;
    std.mem.writeInt(i64, bytes[10..18], std.math.minInt(i64), .little);
    try std.testing.expect(plan.slot(&bytes) == null);
    hot.counts[0] = std.math.maxInt(u32);
    hot.add(0);
    try std.testing.expect(hot.overflow);
}

test "hot certificate membership compares complete keys, preserves late cold values and frees failed allocations" {
    try std.testing.checkAllAllocationFailures(std.testing.allocator, hotAllocation, .{});
}
