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

/// Only bare signed integer columns are admitted by the caller. Real page encodings feed the existing slice
/// reader; whole pages between windows are skipped by checked headers. Scratch and sample keys use the caller's
/// charged allocator. Errors are optional-probe failures: the caller discards the sample and runs its full pass.
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
    var rows: u64 = 0;
    for (sources) |source| rows = std.math.add(u64, rows, @intCast(@max(0, source.rg.num_rows))) catch return result;
    // A sample table and its decode window share one worker's L2 allowance. A cut larger than that sample
    // declines this policy; cache capacity, rather than a fixture constant, bounds sampling effort.
    const capacity = hw.l2BytesPerWorkerOr(640 << 10) / (2 * (8 + 8 + 1));
    if (h > capacity) return result;
    const count = @min(rows, capacity);
    const windows = Windows.init(rows, count, @max(sources.len, workers));
    var wi: u64 = 0;
    var base: u64 = 0;
    for (sources) |source| {
        const end = base + @as(u64, @intCast(@max(0, source.rg.num_rows)));
        defer base = end;
        if (wi == windows.strata) break;
        var window = windows.at(wi);
        if (window.start >= end) continue;
        var held = std.heap.ArenaAllocator.init(a);
        defer held.deinit();
        var first = try consumer.SliceReader.init(held.allocator(), a, source.rg, source.meta, source.src, key, options) orelse return error.SampleUnsupported;
        defer first.deinit();
        var other: ?consumer.SliceReader = if (second) |ci|
            try consumer.SliceReader.init(held.allocator(), a, source.rg, source.meta, source.src, ci, options) orelse
                return error.SampleUnsupported
        else
            null;
        defer if (other) |*r| r.deinit();
        var scratch = std.heap.ArenaAllocator.init(a);
        defer scratch.deinit();
        var position = base;
        while (wi < windows.strata) {
            window = windows.at(wi);
            if (window.start >= end) break;
            const lo = @max(base, window.start);
            const hi = @min(end, window.start + window.len);
            _ = scratch.reset(.retain_capacity);
            try first.skip(scratch.allocator(), @intCast(lo - position));
            if (other) |*r| try r.skip(scratch.allocator(), @intCast(lo - position));
            const n: usize = @intCast(hi - lo);
            const ks = try first.next(scratch.allocator(), n);
            const fs = if (other) |*r| try r.next(scratch.allocator(), n) else null;
            for (0..n) |r| try result.add(a, integer(ks, r), if (fs) |f| integer(f, r) else 0);
            position = hi;
            // A stratum may cross row groups/files; consume the remainder there, once.
            if (hi < window.start + window.len) break;
            wi += 1;
        }
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
