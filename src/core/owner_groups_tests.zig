const std = @import("std");
const memory = @import("memory.zig");
const spawn = @import("spawn.zig");
const hw = @import("hw.zig");
const none = std.math.maxInt(u32);
const ac = @import("owner_groups.zig");
const Codec = ac.Codec;
const Cut = ac.Cut;
const Full = ac.Full;
const IntCountTop = ac.IntCountTop;
const IntFull = ac.IntFull;
const IntSumTop = ac.IntSumTop;
const Kind = ac.Kind;
const Owner = ac.Owner;
const SumTop = ac.SumTop;
const inlineKey = ac.inlineKey;
const rowLimit = ac.rowLimit;
const CountTop = ac.CountTop;
const admitsRows = ac.admitsRows;
const batchRows = ac.batchRows;
const full_parts = ac.full_parts;
const null_key = ac.null_key;

test {
    _ = @import("owner_lanes.zig");
    _ = @import("owner_cache.zig");
    _ = @import("owner_records.zig");
    _ = @import("owner_fold.zig");
    _ = @import("owner_packed.zig");
    _ = @import("owner_pairs.zig");
    _ = @import("owner_sink.zig");
    _ = @import("owner_result.zig");
    _ = @import("owner_cost.zig");
}

fn fullTrial(a: std.mem.Allocator, workers: usize) !void {
    const keys = [_]?[]const u8{ "", null, "a", "a\x00", "abcdefghijklmnop", "γ", "z" };
    const rows = 3001;
    var budget: memory.SharedBudget = .{ .limit = 1 << 30 };
    defer std.debug.assert(budget.used.load(.monotonic) == 0);
    const s = (try Full.State.init(a, &budget, rows, workers, 0, 1, {}, full_parts, 0)).?;
    defer s.deinit();
    var i: usize = rows;
    while (i != 0) {
        i -= 1;
        const key = keys[i % keys.len];
        const value: ?i64 = if (i % keys.len == 2) null else @as(i64, @intCast(i)) - 1500;
        try s.locals[i % workers].append(key, value, @intCast(i));
    }
    try s.finish();
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
        try fullTrial(std.testing.allocator, j);
    };
}

test "inline byte groups unwind every partial allocation and ledger charge" {
    try std.testing.checkAllAllocationFailures(std.testing.allocator, fullTrial, .{1});
}

test "inline byte groups decline long keys and reject wide sums only after decode" {
    {
        const s = (try Full.State.init(std.testing.allocator, null, 2, 1, 0, 1, {}, full_parts, 0)).?;
        defer s.deinit();
        try std.testing.expectError(error.ExceededMemoryBudget, s.locals[0].append("abcdefghijklmnopq", 1, 0));
        try std.testing.expect(s.width_refused.load(.monotonic));
    }
    for ([_]i64{ std.math.minInt(i64), std.math.maxInt(i64) }) |v| {
        const s = (try Full.State.init(std.testing.allocator, null, 2, 2, 0, 1, {}, full_parts, 0)).?;
        defer s.deinit();
        for (0..2) |i| try s.locals[i].append("same", v, @intCast(i));
        try std.testing.expectError(error.AggIntTooWide, s.finish());
    }
}

test "inline byte cached hashes cover wide partition tables" {
    for ([_]usize{ 131072, 131073 }) |rows| {
        const s = (try Full.State.init(std.testing.allocator, null, rows, 4, 0, 1, {}, full_parts, 0)).?;
        defer s.deinit();
        for (0..rows) |i| try s.locals[i % 4].append("abcdefghijklmnop", 3, @intCast(i));
        try s.finish();
        try std.testing.expectEqual(@as(usize, 1), s.group_count);
        const ids = try s.window(std.testing.allocator, 0, 1);
        defer std.testing.allocator.free(ids);
        const g = &s.groups[ids[0]];
        try std.testing.expectEqualStrings("abcdefghijklmnop", g.keyBytes().?);
        try std.testing.expectEqual(@as(i64, @intCast(rows)), g.countValue());
        try std.testing.expectEqual(@as(?i128, @intCast(rows * 3)), g.sumValue());
    }
}

fn topTrial(comptime kind: Kind, a: std.mem.Allocator, workers: usize, descending: bool, parts: usize, collisions: bool) !void {
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
    const T = Owner(kind, .bytes);
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
        if (kind == .count_top) {
            refs[at].sum = (refs[at].sum orelse 0) + 1;
        } else if (v) |value| refs[at].sum = (refs[at].sum orelse 0) + value;
        try s.locals[(rows - i) % workers].append(key, if (kind == .sum_top) v else {}, @intCast(i));
    }
    if (collisions) for (s.locals) |l| for (l.records.heads) |head| {
        var id = head;
        while (id != none) {
            const b = &s.blocks[id];
            for (b.rows[0..b.len]) |*r| r.flags &= 3;
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
    for (s.top(), want) |*actual, expected| {
        const key = actual.keyBytes();
        if (expected.key) |value| {
            try std.testing.expectEqualSlices(u8, value, key.?);
        } else try std.testing.expect(key == null);
        try std.testing.expectEqual(expected.sum, T.rank(actual.*));
        try std.testing.expectEqual(expected.first, actual.ordinal());
    }
}

test "owned byte SUM and COUNT cuts preserve full identities, wide sums and first ties across workers and partitions" {
    inline for (.{ Kind.sum_top, Kind.count_top }) |kind| {
        for ([_]usize{ 1, 2, 4, 8, 16 }) |workers| for ([_]bool{ false, true }) |descending|
            for ([_]usize{ 64, 256, 2048 }) |parts| try topTrial(kind, std.testing.allocator, workers, descending, parts, false);
        for ([_]usize{ 1, 16 }) |workers| try topTrial(kind, std.testing.allocator, workers, true, 64, true);
        for ([_]usize{ 0, 2 }) |after| {
            spawn.injectFailureAfter(after);
            defer spawn.resetFailure();
            try topTrial(kind, std.testing.allocator, 16, true, 256, false);
        }
    }
}

test "owned byte SUM and COUNT cuts release all partial allocations and declines an unfunded peak" {
    const Kinds = struct {
        fn sum(a: std.mem.Allocator) !void {
            try topTrial(.sum_top, a, 1, true, 64, false);
        }
        fn count(a: std.mem.Allocator) !void {
            try topTrial(.count_top, a, 1, false, 64, false);
        }
    };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Kinds.sum, .{});
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Kinds.count, .{});
    var budget: memory.SharedBudget = .{ .limit = 1 };
    const result = try SumTop.State.init(std.testing.allocator, &budget, 1, 1, 0, 1, .{ .k = 1, .descending = true }, 64, 0);
    try std.testing.expect(result == null);
    const counted = try CountTop.State.init(std.testing.allocator, &budget, 1, 1, 0, 0, .{ .k = 1, .descending = true }, 64, 0);
    try std.testing.expect(counted == null);
    try std.testing.expectEqual(@as(usize, 0), budget.used.load(.monotonic));
}

test "inline byte identity distinguishes NULL, empty and embedded zero bytes and declines seventeen bytes" {
    // Equal bits are told apart by the length kept beside them; NULL by its own flag (it packs as empty).
    try std.testing.expectEqual(try inlineKey("", false), try inlineKey("\x00", false));
    try std.testing.expectEqual(try inlineKey("a", false), try inlineKey("a\x00", false));
    try std.testing.expect(try inlineKey("a", false) != try inlineKey("b", false));
    for (0..17) |n| {
        var bytes: [16]u8 = @splat(0);
        for (bytes[0..n], 0..) |*b, i| b.* = @intCast(i + 1);
        const back = std.mem.toBytes(try inlineKey(bytes[0..n], false));
        try std.testing.expectEqualSlices(u8, bytes[0..n], back[0..n]);
    }
    try std.testing.expectError(error.ExceededMemoryBudget, inlineKey("0123456789abcdefg", false));
    // Tagged identities (top-H) are distinct by one comparison, NULL included, and stop at 15 bytes.
    try std.testing.expect(try inlineKey("", true) != try inlineKey("\x00", true));
    try std.testing.expect(try inlineKey("a", true) != try inlineKey("a\x00", true));
    try std.testing.expect(try inlineKey(null, true) != try inlineKey("", true));
    _ = try inlineKey("0123456789abcde", true);
    try std.testing.expectError(error.ExceededMemoryBudget, inlineKey("0123456789abcdef", true));
}

test "packed byte top-H cache policy uses shared hardware and supports every forced variant" {
    defer hw.test_cpu = null;
    hw.test_cpu = .{ .l2 = .{ .bytes = 1 << 20, .max_sharing = 2 } };
    const small = SumTop.partitionCount(30000000, 16, batchRows(null), null);
    const small_batch = batchRows(null);
    hw.test_cpu = .{ .l2 = .{ .bytes = 4 << 20, .max_sharing = 2 } };
    try std.testing.expect(SumTop.partitionCount(30000000, 16, batchRows(null), null) < small);
    try std.testing.expect(batchRows(null) > small_batch);
    try std.testing.expectEqual(@as(usize, 2048), SumTop.partitionCount(30000000, 16, 4096, 0));
    try std.testing.expectEqual(@as(usize, 0), batchRows(0));
    try std.testing.expectEqual(@as(usize, 64), SumTop.partitionCount(30000000, 16, 4096, 1));
    try std.testing.expectEqual(@as(usize, 16384), SumTop.partitionCount(30000000, 16, 4096, 999999));
    for ([_]usize{ 64, 1024, 8192, 16384 }) |parts| {
        try topTrial(.sum_top, std.testing.allocator, 1, true, parts, false);
        try topTrial(.count_top, std.testing.allocator, 1, true, parts, false);
    }
    // COUNT records and groups are narrower, so its uncapped table-sized fanout never exceeds SUM's.
    try std.testing.expect(CountTop.partitionCount(30000000, 12, 4096, null) <= SumTop.partitionCount(30000000, 12, 4096, null));

    hw.test_cpu = .{ .l2 = .{ .bytes = 1280 << 10, .max_sharing = 2 }, .l3 = .{ .bytes = 30 << 20 } };
    try std.testing.expectEqual(@as(usize, 4096), SumTop.partitionCount(30000000, 1, 4096, null));
    try std.testing.expectEqual(@as(usize, 2048), SumTop.partitionCount(30000000, 12, 4096, null));
    try std.testing.expectEqual(@as(usize, 4096), SumTop.partitionCount(30000000, 64, 4096, null));
    try std.testing.expectEqual(@as(usize, 4096), SumTop.partitionCount(30000000, 12, 0, null));
    try std.testing.expectEqual(@as(usize, 4096), SumTop.partitionCount(30000000, 12, 65536, null));
    hw.test_cpu.?.l3.bytes = 4 << 20;
    try std.testing.expectEqual(@as(usize, 4096), SumTop.partitionCount(30000000, 1, 4096, null));
    hw.test_cpu.?.l3.bytes = 0;
    try std.testing.expectEqual(@as(usize, 4096), SumTop.partitionCount(30000000, 12, 4096, null));
    hw.test_cpu = .{ .l2 = .{ .bytes = 1 << 20, .max_sharing = 2 }, .l3 = .{ .bytes = 32 << 20 } };
    try std.testing.expectEqual(@as(usize, 2048), SumTop.partitionCount(30000000, 16, batchRows(null), null));
}

fn intKey(key: i64, flags: u32) ?i64 {
    return if (flags & 1 != 0) null else key;
}

fn intValue(value: i64, flags: u32) ?i64 {
    return if (flags & 2 != 0) null else value;
}

fn widen(v: ?i64) ?i128 {
    return if (v) |x| x else null;
}

fn intTrial(a: std.mem.Allocator, workers: usize) !void {
    var budget: memory.SharedBudget = .{ .limit = 1 << 30 };
    defer std.debug.assert(budget.used.load(.monotonic) == 0);
    const s = (try IntFull.State.init(a, &budget, 1001, workers, 0, 1, {}, full_parts, 0)).?;
    defer s.deinit();
    for (0..1001) |i| try s.locals[i % workers].append(intKey(@intCast(i % 71), if (i % 71 == 0) 1 else if (i % 71 == 2) 2 else 0), intValue(@as(i64, @intCast(i)) - 500, if (i % 71 == 0) 1 else if (i % 71 == 2) 2 else 0), @intCast(i));
    try s.finish();
    try std.testing.expectEqual(@as(usize, 71), s.group_count);
    const ids = try s.window(a, 0, s.group_count);
    defer a.free(ids);
    for (ids, 0..) |id, i| {
        const g = s.groups[id];
        try std.testing.expectEqual(@as(u32, @intCast(i)), g.ordinal());
        try std.testing.expectEqual(if (i == 0) null else @as(?i128, @intCast(i)), widen(g.keyValue()));
        var sum: i128 = 0;
        var count: i128 = 0;
        var row = i;
        while (row < 1001) : (row += 71) {
            count += 1;
            sum += @as(i128, @intCast(row)) - 500;
        }
        try std.testing.expectEqual(count, @as(i128, g.countValue()));
        try std.testing.expectEqual(if (i == 2) null else @as(?i128, sum), g.sumValue());
    }
    const slice = try s.window(a, 7, 69);
    defer a.free(slice);
    try std.testing.expectEqualSlices(u32, ids[7..69], slice);
}

test "owned integer groups preserve exact sums, NULLs and first order" {
    for ([_]usize{ 1, 2, 4, 8, 16 }) |j| try intTrial(std.testing.allocator, j);
}

test "owned integer groups release every allocation and reservation on failure" {
    try std.testing.checkAllAllocationFailures(std.testing.allocator, intTrial, .{1});
}

test "owned integer groups reject a wide exact sum before any output window" {
    for ([_]usize{ 1, 2, 8 }) |j| for ([_]i64{ std.math.maxInt(i64), std.math.minInt(i64) }) |value| {
        const s = (try IntFull.State.init(std.testing.allocator, null, 2, j, 0, 1, {}, full_parts, 0)).?;
        defer s.deinit();
        for (0..2) |i| try s.locals[i % j].append(intKey(1, 0), intValue(value, 0), @intCast(i));
        try std.testing.expectError(error.AggIntTooWide, s.finish());
    };
}

test "owned integer local partials flush before an i64 overflow and keep exact cancelling sums and NULLs" {
    const a = std.testing.allocator;
    const max = std.math.maxInt(i64);
    const min = std.math.minInt(i64);
    // Key 1: max, max, min, min, -2 overflows each partial direction before cancelling to -2 + -2.
    const keys = [_]?i64{ 1, 1, null, 1, 1, null, 1, 2 };
    const values = [_]?i64{ max, max, null, min, min, null, -2, 5 };
    for ([_]usize{ 1, 2, 3 }) |j| {
        const s = (try IntFull.State.init(a, null, keys.len, j, 0, 1, {}, full_parts, 0)).?;
        defer s.deinit();
        try std.testing.expect(s.locals[0].full_cache.entries.len != 0);
        for (keys, values, 0..) |k, v, i| try s.locals[i % j].append(k, v, @intCast(i));
        try s.finish();
        try std.testing.expectEqual(@as(usize, 3), s.group_count);
        const ids = try s.window(a, 0, s.group_count);
        defer a.free(ids);
        const g = [_]IntFull.Group{ s.groups[ids[0]], s.groups[ids[1]], s.groups[ids[2]] };
        try std.testing.expectEqual(@as(?i64, 1), g[0].keyValue());
        try std.testing.expectEqual(@as(i64, 5), g[0].countValue());
        try std.testing.expectEqual(@as(?i128, -4), g[0].sumValue());
        try std.testing.expectEqual(@as(?i64, null), g[1].keyValue());
        try std.testing.expectEqual(@as(u32, 2), g[1].ordinal());
        try std.testing.expectEqual(@as(i64, 2), g[1].countValue());
        try std.testing.expectEqual(@as(?i128, null), g[1].sumValue());
        try std.testing.expectEqual(@as(u32, 7), g[2].ordinal());
        try std.testing.expectEqual(@as(?i128, 5), g[2].sumValue());
        try std.testing.expectEqual(@as(u32, 3), s.input_nulls);
    }
}

test "regression: a full-answer cache whose trial fails stays off for the worker's later items" {
    // rg64k q06 (615ba9f2, 1.1x slower): each 64K-row group re-armed the cache and spent half its rows in a trial
    // that hit 6% of the time.
    const a = std.testing.allocator;
    // The trial's length follows the host's L2 share and cache line; leave as many rows again after it.
    const trial = blk: {
        const probe = (try IntFull.State.init(a, null, 1 << 18, 1, 0, 1, {}, full_parts, 0)).?;
        defer probe.deinit();
        break :blk probe.cache_trial_rows;
    };
    const rows: usize = @max(1 << 18, 2 * @as(usize, trial));
    const s = (try IntFull.State.init(a, null, rows, 1, 0, 1, {}, full_parts, 0)).?;
    defer s.deinit();
    const l = &s.locals[0];
    try std.testing.expect(l.full_cache.entries.len != 0);
    try std.testing.expectEqual(trial, s.cache_trial_rows);
    var row: u32 = 0;
    l.start(0, 0);
    // Every key new: no partial absorbs another, so the trial gives up on the cache.
    while (row < trial) : (row += 1) try l.append(@as(i64, row), 1, row);
    try std.testing.expect(l.full_bypass);
    l.start(row, 1);
    try std.testing.expect(l.full_bypass);
    while (row < rows) : (row += 1) try l.append(@as(i64, row % 7), 1, row);
    try s.finish();
    try std.testing.expectEqual(@as(usize, trial), s.group_count);
}

test "owned integer groups overlap packed records safely for every reduction and partition tail" {
    const a = std.testing.allocator;
    const Ref = struct { key: ?i64, count: i128 = 0, sum: ?i128 = null };
    for ([_]usize{ 1, 2, 3, 4, 7, 31, 64, 65, 127, 257 }) |n| {
        for ([_]usize{ 1, @max(1, n / 2), n }) |ndv| for ([_]usize{ 1, 4, 16 }) |j| {
            const state = (try IntFull.State.init(a, null, n, j, 0, 1, {}, full_parts, 0)).?;
            defer state.deinit();
            var expected: std.ArrayList(Ref) = .empty;
            defer expected.deinit(a);
            for (0..n) |i| {
                const key: ?i64 = if (i % 37 == 0) null else @as(i64, @intCast(i % ndv)) - @as(i64, @intCast(ndv / 2));
                const value: ?i64 = if (i % 13 == 0) null else @as(i64, @intCast((i * 730201) % 1000)) - 500;
                var id = expected.items.len;
                for (expected.items, 0..) |r, ri| if (r.key == key) {
                    id = ri;
                    break;
                };
                if (id == expected.items.len) try expected.append(a, .{ .key = key });
                const ref = &expected.items[id];
                ref.count += 1;
                if (value) |v| ref.sum = (ref.sum orelse 0) + v;
                try state.locals[(n - i) % j].append(intKey(key orelse 0, @as(u32, @intFromBool(key == null)) | (@as(u32, @intFromBool(value == null)) << 1)), intValue(value orelse 0, @as(u32, @intFromBool(key == null)) | (@as(u32, @intFromBool(value == null)) << 1)), @intCast(i));
            }
            try state.finish();
            try std.testing.expectEqual(expected.items.len, state.group_count);
            const ids = try state.window(a, 0, state.group_count);
            defer a.free(ids);
            for (expected.items, ids) |ref, id| {
                const group = state.groups[id];
                try std.testing.expectEqual(if (ref.key) |k| @as(?i128, k) else null, widen(group.keyValue()));
                try std.testing.expectEqual(ref.count, @as(i128, group.countValue()));
                try std.testing.expectEqual(ref.sum, group.sumValue());
            }
        };
    }
}

fn intTopTrial(a: std.mem.Allocator, workers: usize, descending: bool, k: usize) !void {
    const ndv = 1021;
    var budget: memory.SharedBudget = .{ .limit = 1 << 30 };
    defer std.debug.assert(budget.used.load(.monotonic) == 0);
    const state = (try IntSumTop.State.init(a, &budget, ndv * 3, workers, 0, 1, .{
        .k = k,
        .descending = descending,
    }, 64, 0)).?;
    defer state.deinit();
    // Every present SUM is four. Keys, arrival order and first-appearance order differ; duplicates span workers.
    // Group 5 has a NULL sum and group 11 a NULL key. A partial-partition cut would select incorrect intermediate
    // sums, and a key-order tie-break would produce a different answer from these hand-known first ordinals.
    for ([_]usize{ 2, 0, 1 }) |round| for (0..ndv) |i| try state.locals[(ndv - i + round) % workers].append(intKey(@intCast((i * 73) % ndv), if (i == 5) 2 else if (i == 11) 1 else 0), intValue(([_]i64{ 2, -1, 3 })[round], if (i == 5) 2 else if (i == 11) 1 else 0), @intCast(round * ndv + i));
    try state.finish();
    try std.testing.expectEqual(@as(usize, ndv), state.group_count);
    try std.testing.expectEqual(@as(usize, 0), state.groups.len);
    try std.testing.expectEqual(@as(usize, 0), state.winners.len);
    try std.testing.expectEqual(@as(usize, 0), state.bitmap.len);
    try std.testing.expectEqual(@min(k, ndv), state.top().len);
    var ordinal: usize = 0;
    for (state.top()) |g| {
        if (k < ndv and ordinal == 5) ordinal += 1;
        try std.testing.expectEqual(@as(u32, @intCast(ordinal)), g.ordinal());
        const key: ?i128 = if (ordinal == 11) null else @intCast((ordinal * 73) % ndv);
        try std.testing.expectEqual(key, widen(g.keyValue()));
        try std.testing.expectEqual(if (ordinal == 5) null else @as(?i128, 4), g.sumValue());
        ordinal += 1;
    }
}

test "owned integer SUM top-H completes keys before stable tie selection at every worker count" {
    for ([_]usize{ 1, 2, 4, 8, 16 }) |j| for ([_]bool{ false, true }) |descending|
        for ([_]usize{ 7, 31, 1024 }) |k| try intTopTrial(std.testing.allocator, j, descending, k);
}

test "owned integer SUM top-H frees every partial heap and scratch allocation" {
    try std.testing.checkAllAllocationFailures(std.testing.allocator, intTopTrial, .{ 1, true, 7 });
}

test "owned integer SUM top-H retains wide winning and losing sums without output narrowing" {
    for ([_]bool{ false, true }) |descending| {
        const s = (try IntSumTop.State.init(std.testing.allocator, null, 6, 2, 0, 1, .{
            .k = 1,
            .descending = descending,
        }, 64, 0)).?;
        defer s.deinit();
        for (0..6) |i| try s.locals[i % 2].append(intKey(@intCast(i / 2), 0), intValue(if (i < 2) std.math.maxInt(i64) else if (i < 4) std.math.minInt(i64) else 0, 0), @intCast(i));
        try s.finish();
        const g = s.top()[0];
        const value: i128 = if (descending) std.math.maxInt(i64) else std.math.minInt(i64);
        try std.testing.expectEqual(value * 2, g.sumValue().?);
    }
}

test "owner row limits preserve the integer 31-bit range and the byte length field" {
    const t = std.testing;
    try t.expectEqual(@as(usize, 1) << 31, rowLimit(.int64));
    try t.expectEqual(@as(usize, 1) << 27, rowLimit(.bytes));
    for ([_]usize{ (1 << 27) - 1, 1 << 27, 180_000_000, 1 << 30, (1 << 31) - 1 }) |rows| {
        try t.expect(admitsRows(.int64, rows));
    }
    try t.expect(!admitsRows(.int64, 1 << 31));
    try t.expect(!admitsRows(.int64, std.math.maxInt(usize)));
    try t.expect(admitsRows(.bytes, (1 << 27) - 1));
    try t.expect(!admitsRows(.bytes, 1 << 27));
    // The restored range must not widen the hot records or group scratch.
    try t.expectEqual(@as(usize, 24), @sizeOf(IntFull.Record));
    try t.expectEqual(@as(usize, 32), @sizeOf(IntFull.Group));
}

test "integer owner execution admits synthetic rows through the shared 31-bit boundary" {
    inline for (.{ Kind.full, Kind.sum_top }) |kind| {
        const O = Owner(kind, .int64);
        // An empty allocator fails at the State object, proving that a valid row count passed admission without
        // allocating records for that many rows. An out-of-range count must decline before that first allocation.
        var empty: [0]u8 = .{};
        var fixed = std.heap.FixedBufferAllocator.init(&empty);
        const cut = if (kind == .full) {} else Cut{ .k = 7, .descending = true };
        for ([_]usize{ 1 << 27, 180_000_000, (1 << 31) - 1 }) |rows| {
            try std.testing.expectError(error.OutOfMemory, O.State.init(
                fixed.allocator(),
                null,
                rows,
                1,
                0,
                1,
                cut,
                full_parts,
                0,
            ));
        }
        try std.testing.expectEqual(@as(?*O.State, null), try O.State.init(
            fixed.allocator(),
            null,
            rowLimit(.int64),
            1,
            0,
            1,
            cut,
            full_parts,
            0,
        ));
    }
}

test "integer owner folding keeps high ordinals and 31-bit counts separate from NULL and SUM flags" {
    const t = std.testing;
    inline for (.{ Kind.full, Kind.sum_top }) |kind| {
        const O = Owner(kind, .int64);
        var groups: [2]O.Group = undefined;
        var slots: [8]u32 = @splat(0);
        var n: usize = 0;
        const late = (1 << 31) - 2;
        const early = (1 << 27) + 5;
        // NULL and zero share a payload, but remain separate even when ordinals set the former length bits.
        O.foldRecord(.{ .key = 0, .value = 0, .ordinal = late, .flags = 3 }, &groups, &slots, 0, &n);
        O.foldRecord(.{ .key = 0, .value = -7, .ordinal = early + 1, .flags = 0 }, &groups, &slots, 0, &n);
        O.foldRecord(.{ .key = 0, .value = 5, .ordinal = early, .flags = 1 }, &groups, &slots, 0, &n);
        try t.expectEqual(@as(usize, 2), n);
        try t.expectEqual(@as(?i64, null), groups[0].keyValue());
        try t.expectEqual(@as(?i64, 0), groups[1].keyValue());
        try t.expectEqual(@as(u32, early), groups[0].ordinal());
        try t.expectEqual(@as(u32, early + 1), groups[1].ordinal());
        try t.expectEqual(@as(?i128, 5), groups[0].sumValue());
        try t.expectEqual(@as(?i128, -7), groups[1].sumValue());
        // Seed a synthetic large count to exercise both upper boundaries without allocating a huge input.
        groups[0].count = O.Group.sum_present | ((1 << 30) - 1);
        O.foldRecord(.{ .key = 0, .value = 0, .ordinal = late, .flags = 3 }, &groups, &slots, 0, &n);
        try t.expectEqual(@as(i64, 1) << 30, groups[0].countValue());
        groups[0].count = O.Group.sum_present | ((1 << 31) - 2);
        O.foldRecord(.{ .key = 0, .value = 0, .ordinal = late, .flags = 3 }, &groups, &slots, 0, &n);
        try t.expectEqual((@as(i64, 1) << 31) - 1, groups[0].countValue());
        try t.expectEqual(@as(?i64, null), groups[0].keyValue());
        try t.expectEqual(@as(u32, early), groups[0].ordinal());
        try t.expectEqual(@as(?i128, 5), groups[0].sumValue());
    }
}

fn runTrial(a: std.mem.Allocator, workers: usize) !void {
    const keys = [_]i64{ -9, -9, -2, -2, -2, -2, -2, -2, 8, 8, 11, 12 };
    const starts = [_]u32{ 0, 3, 3, 5, 7, 10 };
    var ledger: memory.SharedBudget = .{ .limit = 512 << 20 };
    defer std.debug.assert(ledger.used.load(.monotonic) == 0);
    const s = (try IntFull.State.initRuns(a, &ledger, keys.len, workers, 0, 1, {}, full_parts, starts.len, 0)).?;
    defer s.deinit();
    // Reverse completion order, an empty source, and one group through three source seams.
    var item = starts.len;
    while (item != 0) {
        item -= 1;
        const local = &s.locals[item % workers];
        local.start(starts[item], item);
        const end = if (item + 1 == starts.len) keys.len else starts[item + 1];
        for (starts[item]..end) |row|
            try local.append(keys[row], if (row % 3 == 0) null else @as(i64, @intCast(row)) - 6, @intCast(row));
    }
    try s.finish();
    const ids = try s.window(a, 0, s.group_count);
    defer a.free(ids);
    var group: usize = 0;
    for (keys, 0..) |key, row| {
        if (row != 0 and key == keys[row - 1]) continue;
        const got = s.groups[ids[group]];
        group += 1;
        var count: i64 = 0;
        var sum: ?i128 = null;
        for (row..keys.len) |r| {
            if (keys[r] != key) break;
            count += 1;
            if (r % 3 != 0) sum = (sum orelse 0) + @as(i128, @intCast(r)) - 6;
        }
        try std.testing.expectEqual(@as(?i64, key), got.keyValue());
        try std.testing.expectEqual(@as(u32, @intCast(row)), got.ordinal());
        try std.testing.expectEqual(count, got.countValue());
        try std.testing.expectEqual(sum, got.sumValue());
    }
    try std.testing.expectEqual(group, ids.len);
}

test "decoded owner runs fold empty sources and seam chains in source order at every worker count" {
    for ([_]usize{ 1, 2, 4, 8, 16 }) |j| try runTrial(std.testing.allocator, j);
}

test "decoded owner runs release every failed allocation and ledger reservation" {
    try std.testing.checkAllAllocationFailures(std.testing.allocator, runTrial, .{2});
}

test "decoded owner runs decline NULL, an interior descent or an overlapping source before output" {
    const a = std.testing.allocator;
    for ([_][4]?i64{ .{ 1, null, 2, 3 }, .{ 1, 0, 2, 3 }, .{ 1, 2, 1, 3 } }) |keys| {
        const s = (try IntFull.State.initRuns(a, null, keys.len, 2, 0, 1, {}, full_parts, 2, 0)).?;
        defer s.deinit();
        for (0..2) |item| {
            s.locals[item].start(@intCast(item * 2), item);
            for (item * 2..item * 2 + 2) |row| try s.locals[item].append(keys[row], 1, @intCast(row));
        }
        try std.testing.expectError(error.RunOrderDeclined, s.finish());
    }
}

test "decoded owner runs narrow exact sums only after all seams, including a later cancellation" {
    const a = std.testing.allocator;
    for ([_]i64{ -std.math.maxInt(i64), 0 }) |last| {
        const s = (try IntFull.State.initRuns(a, null, 3, 1, 0, 1, {}, full_parts, 3, 0)).?;
        defer s.deinit();
        for ([_]i64{ std.math.maxInt(i64), 1, last }, 0..) |value, item| {
            s.locals[0].start(@intCast(item), item);
            try s.locals[0].append(7, value, @intCast(item));
        }
        if (last == 0) {
            try std.testing.expectError(error.AggIntTooWide, s.finish());
        } else {
            try s.finish();
            try std.testing.expectEqual(@as(?i128, 1), s.groups[0].sumValue());
            try std.testing.expectEqual(@as(i64, 3), s.groups[0].countValue());
        }
    }
}

fn runTopTrial(a: std.mem.Allocator, workers: usize, descending: bool, k: usize) !void {
    return runTopTrialKind(.sum_top, a, workers, descending, k);
}

fn countRunTopTrial(a: std.mem.Allocator, workers: usize, descending: bool, k: usize) !void {
    return runTopTrialKind(.count_top, a, workers, descending, k);
}

fn runTopTrialKind(comptime kind: Kind, a: std.mem.Allocator, workers: usize, descending: bool, k: usize) !void {
    const O = Owner(kind, .int64);
    const rows = 1024;
    const items = 17;
    var ledger: memory.SharedBudget = .{ .limit = 1 << 30 };
    defer std.debug.assert(ledger.used.load(.monotonic) == 0);
    const cut: Cut = .{ .k = k, .descending = descending };
    const run = (try O.State.initRuns(a, &ledger, rows, workers, 0, 1, cut, 64, items, 0)).?;
    defer run.deinit();
    const hash = (try O.State.init(a, &ledger, rows, workers, 0, 1, cut, 64, 0)).?;
    defer hash.deinit();
    var item: usize = items;
    while (item != 0) {
        item -= 1;
        const lo = rows * item / items;
        const hi = rows * (item + 1) / items;
        run.locals[item % workers].start(@intCast(lo), item);
        for (lo..hi) |row| {
            const key: i64 = @intCast(row / 128); // A group crosses three source seams.
            const value: ?i64 = if (key == 0) null else if (row % 11 == 0) -3 else 1;
            try run.locals[item % workers].append(key, if (kind == .sum_top) value else {}, @intCast(row));
            try hash.locals[item % workers].append(key, if (kind == .sum_top) value else {}, @intCast(row));
        }
    }
    try run.finish();
    try hash.finish();
    try std.testing.expectEqual(hash.group_count, run.group_count);
    try std.testing.expectEqualDeep(hash.top(), run.top());
}

test "decoded owner run top-H completes seam chains before SUM ranking and first-ordinal ties" {
    for ([_]usize{ 1, 2, 4, 16 }) |j| for ([_]bool{ false, true }) |descending|
        for ([_]usize{ 1, 7, 16 }) |k| try runTopTrial(std.testing.allocator, j, descending, k);
    spawn.injectFailureAfter(1);
    defer spawn.resetFailure();
    try runTopTrial(std.testing.allocator, 16, true, 7);
}

test "decoded owner run top-H releases failed heaps, group buffers and ledger reservations" {
    try std.testing.checkAllAllocationFailures(std.testing.allocator, runTopTrial, .{ 1, true, 7 });
}

test "decoded owner runs decline unquoted descriptors for mostly empty sources" {
    var ledger: memory.SharedBudget = .{ .limit = 512 << 20 };
    try std.testing.expect((try IntFull.State.initRuns(
        std.testing.allocator,
        &ledger,
        1,
        1,
        0,
        1,
        {},
        full_parts,
        2,
        0,
    )) == null);
    try std.testing.expectEqual(@as(usize, 0), ledger.used.load(.monotonic));
}

test "decoded owner COUNT top-H preserves seam counts and first-selected ties at every worker count" {
    for ([_]usize{ 1, 2, 4, 8, 16 }) |j| for ([_]bool{ false, true }) |descending|
        for ([_]usize{ 1, 7, 16 }) |k| try countRunTopTrial(std.testing.allocator, j, descending, k);
    spawn.injectFailureAfter(1);
    defer spawn.resetFailure();
    try countRunTopTrial(std.testing.allocator, 16, true, 7);
}

test "decoded owner COUNT top-H releases all failed allocations and ledger charges" {
    try std.testing.checkAllAllocationFailures(std.testing.allocator, countRunTopTrial, .{ 1, true, 7 });
}

test "decoded owner COUNT top-H declines NULL and descending keys before selection" {
    for ([_][4]?i64{ .{ 1, null, 2, 3 }, .{ 1, 0, 2, 3 }, .{ 1, 2, 1, 3 } }) |keys| {
        const s = (try IntCountTop.State.initRuns(
            std.testing.allocator,
            null,
            keys.len,
            2,
            0,
            0,
            .{ .k = 2, .descending = true },
            64,
            2,
            0,
        )).?;
        defer s.deinit();
        for (0..2) |item| {
            s.locals[item].start(@intCast(item * 2), item);
            for (item * 2..item * 2 + 2) |row| try s.locals[item].append(keys[row], {}, @intCast(row));
        }
        try std.testing.expectError(error.RunOrderDeclined, s.finish());
    }
}

test "owner cardinality counts complete integer and byte identities once and excludes NULL with failures" {
    for ([_]usize{ 1, 2, 4, 8, 16 }) |j| try ownerCardinalityTrial(std.testing.allocator, j);
    try std.testing.checkAllAllocationFailures(std.testing.allocator, ownerCardinalityTrial, .{1});
}

fn ownerCardinalityTrial(a: std.mem.Allocator, workers: usize) !void {
    inline for (.{ Codec.int64, Codec.bytes }) |codec| {
        const O = Owner(.count_top, codec);
        const rows = 24;
        var budget: memory.SharedBudget = .{ .limit = 64 << 20 };
        const state = (try O.State.initCardinality(a, &budget, rows, workers, 0, 64, 0)) orelse
            return error.TestUnexpectedResult;
        {
            defer state.deinit();
            for (0..rows) |r| {
                const key = if (codec == .int64) ([_]?i64{
                    null, std.math.minInt(i64), std.math.maxInt(i64), 0, -1, 1,
                })[r % 6] else ([_]?[]const u8{
                    null, "", "a", "a\x00", "\x00", "123456789012345",
                })[r % 6];
                try state.locals[r % workers].append(key, {}, @intCast(r));
            }
            try state.finish();
            try std.testing.expectEqual(@as(usize, 5), state.group_count);
            try std.testing.expectEqual(@as(usize, 0), state.top().len);
        }
        try std.testing.expectEqual(@as(usize, 0), budget.used.load(.monotonic));
        const empty = (try O.State.initCardinality(a, &budget, 0, 1, 0, 64, 0)) orelse
            return error.TestUnexpectedResult;
        {
            defer empty.deinit();
            try empty.finish();
            try std.testing.expectEqual(@as(usize, 0), empty.group_count);
        }
        try std.testing.expectEqual(@as(usize, 0), budget.used.load(.monotonic));
        const all_null = (try O.State.initCardinality(a, &budget, 8, workers, 0, 64, 0)) orelse
            return error.TestUnexpectedResult;
        {
            defer all_null.deinit();
            for (0..8) |r| try all_null.locals[r % workers].append(null, {}, @intCast(r));
            try all_null.finish();
            try std.testing.expectEqual(@as(usize, 0), all_null.group_count);
        }
        try std.testing.expectEqual(@as(usize, 0), budget.used.load(.monotonic));
    }
}

test "distinct owner radix keeps full pairs NULLs and earliest ties across workers and failures" {
    for ([_]usize{ 1, 2, 4, 8, 16 }) |j| for ([_]usize{ 7, 128 }) |h|
        try distinctOwnerTrial(std.testing.allocator, j, h);
    try std.testing.checkAllAllocationFailures(std.testing.allocator, distinctOwnerTrial, .{ 1, 128 });
    for ([_]usize{ 0, 1, 3 }) |after| {
        spawn.injectFailureAfter(after);
        defer spawn.resetFailure();
        try distinctOwnerTrial(std.testing.allocator, 4, 128);
        try std.testing.expect(spawn.failuresInjected() != 0);
    }
}

fn distinctOwnerTrial(a: std.mem.Allocator, workers: usize, h: usize) !void {
    const O = Owner(.sum_top, .int64);
    const Ref = struct { key: ?i64, first: u32, values: std.AutoHashMap(i64, void) };
    var refs: std.ArrayList(Ref) = .empty;
    defer {
        for (refs.items) |*ref| ref.values.deinit();
        refs.deinit(a);
    }
    var budget: memory.SharedBudget = .{ .limit = 64 << 20 };
    const state = (try O.State.initDistinctTop(
        a,
        &budget,
        512,
        workers,
        0,
        1,
        .{ .k = h, .descending = true },
        64,
        0,
    )) orelse return error.TestUnexpectedResult;
    {
        defer state.deinit();
        const keys = [_]?i64{
            null,    0,             1,             -1, std.math.minInt(i64), std.math.maxInt(i64),
            1 << 48, (1 << 48) + 1, (1 << 48) - 1, 11, 37,
        };
        const vals = [_]i64{ std.math.minInt(i64), std.math.maxInt(i64), 0, -1, 1, 1 << 40 };
        for (0..512) |r| {
            const key = keys[(r * 17) % keys.len];
            const value: ?i64 = if (key == 0 or r % 17 == 0) null else vals[(r / keys.len + r / 3) % vals.len];
            try state.locals[r % workers].append(key, value, @intCast(r));
            var at = refs.items.len;
            for (refs.items, 0..) |ref, i| if (ref.key == key) {
                at = i;
                break;
            };
            if (at == refs.items.len) try refs.append(a, .{
                .key = key,
                .first = @intCast(r),
                .values = std.AutoHashMap(i64, void).init(a),
            });
            if (value) |v| try refs.items[at].values.put(v, {});
        }
        try state.finish();
        try std.testing.expectEqual(refs.items.len, state.group_count);
        std.mem.sort(Ref, refs.items, {}, struct {
            fn less(_: void, x: Ref, y: Ref) bool {
                return x.values.count() > y.values.count() or
                    (x.values.count() == y.values.count() and x.first < y.first);
            }
        }.less);
        const n = @min(h, refs.items.len);
        std.mem.sort(Ref, refs.items[0..n], {}, struct {
            fn less(_: void, x: Ref, y: Ref) bool {
                return x.first < y.first;
            }
        }.less);
        try std.testing.expectEqual(n, state.top().len);
        for (refs.items[0..n], state.top()) |ref, got| {
            try std.testing.expectEqual(ref.key, got.keyValue());
            try std.testing.expectEqual(ref.first, got.ordinal());
            try std.testing.expectEqual(@as(i128, @intCast(ref.values.count())), O.rank(got).?);
        }
    }
    try std.testing.expectEqual(@as(usize, 0), budget.used.load(.monotonic));
}

fn hotCountTrial(a: std.mem.Allocator, workers: usize) !void {
    const O = IntCountTop;
    const rows = 8192;
    var budget: memory.SharedBudget = .{ .limit = 256 << 20 };
    {
        const s = (try O.State.init(a, &budget, rows, workers, 0, 0, .{ .k = 16, .descending = true }, 64, 0)).?;
        defer s.deinit();
        var counts: [16]u32 = @splat(0);
        var first: [16]u32 = @splat(none);
        // NULL and zero are equally hot, with early ordinals deliberately arriving after their later partials.
        // The remaining identities evict cache entries repeatedly across logical row-group boundaries.
        for (0..rows) |r| {
            const ordinal: u32 = @intCast(rows - 1 - r);
            const id: usize = if (r % 4 < 2) r % 2 else 2 + r % 14;
            const key: ?i64 = if (id == 0) null else if (id == 1) 0 else @as(i64, @intCast(id)) * 0x123456789;
            counts[id] += 1;
            first[id] = @min(first[id], ordinal);
            const local = &s.locals[(r / 113) % workers];
            if (r % 113 == 0) local.start(0, 0);
            try local.append(key, {}, ordinal);
        }
        var weighted_null = false;
        var weighted_zero = false;
        for (s.locals) |*local| {
            try local.flushCounts();
            for (local.records.heads) |head| {
                var id = head;
                while (id != none) : (id = s.blocks[id].next) {
                    const block = &s.blocks[id];
                    for (block.rows[0..block.len]) |record| if (record.ordinal & null_key != 0) {
                        weighted_null = weighted_null or record.flags & 1 != 0;
                        weighted_zero = weighted_zero or (record.flags & 1 == 0 and record.key == 0);
                    };
                }
            }
        }
        try std.testing.expect(weighted_null and weighted_zero);
        try s.finish();
        try std.testing.expectEqual(@as(usize, 16), s.top().len);
        var seen: u32 = 0;
        for (s.top()) |g| {
            const id: usize = if (g.keyValue()) |key| if (key == 0) 1 else @intCast(@divExact(key, 0x123456789)) else 0;
            try std.testing.expectEqual(@as(i64, counts[id]), g.countValue());
            try std.testing.expectEqual(first[id], g.ordinal());
            seen |= @as(u32, 1) << @intCast(id);
        }
        try std.testing.expectEqual(@as(u32, 65535), seen);
        var emitted: usize = 0;
        var contributed: usize = 0;
        for (s.locals) |local| {
            for (local.records.histogram) |n| emitted += n;
            contributed += local.contributed_rows;
        }
        try std.testing.expectEqual(@as(usize, rows), contributed);
        try std.testing.expect(emitted < rows * 3 / 4);
    }
    try std.testing.expectEqual(@as(usize, 0), budget.used.load(.monotonic));
}

test "hot integer COUNT partials cache NULL and preserve evictions first ordinals and ledger on every failure" {
    defer hw.test_cpu = null;
    // One line forces collisions, including between NULL and an ordinary key, through the shared eviction path.
    hw.test_cpu = .{ .l1d = .{ .bytes = 256, .line_bytes = 64 }, .l2 = .{ .bytes = 4096 } };
    for ([_]usize{ 1, 2, 4, 8, 16 }) |workers| try hotCountTrial(std.testing.allocator, workers);
    try std.testing.checkAllAllocationFailures(std.testing.allocator, hotCountTrial, .{1});
}

test "hot integer COUNT weighted records keep high counts ordinals NULL and zero independent" {
    const O = IntCountTop;
    var groups: [2]O.Group = undefined;
    var slots: [8]u32 = @splat(0);
    var n: usize = 0;
    const early = (1 << 30) + 7;
    const late = (1 << 31) - 2;
    const weight = (1 << 31) - 3;
    O.foldRecord(.{ .key = 0, .ordinal = late | null_key, .flags = (weight << 1) | 1 }, &groups, &slots, 0, &n);
    O.foldRecord(.{ .key = 0, .ordinal = early | null_key, .flags = 4 << 1 }, &groups, &slots, 0, &n);
    O.foldRecord(.{ .key = 0, .ordinal = early, .flags = 1 }, &groups, &slots, 0, &n);
    try std.testing.expectEqual(@as(usize, 2), n);
    try std.testing.expectEqual(@as(?i64, null), groups[0].keyValue());
    try std.testing.expectEqual(@as(i64, weight + 1), groups[0].countValue());
    try std.testing.expectEqual(@as(u32, early), groups[0].ordinal());
    try std.testing.expectEqual(@as(?i64, 0), groups[1].keyValue());
    try std.testing.expectEqual(@as(i64, 4), groups[1].countValue());
    try std.testing.expectEqual(@as(u32, early), groups[1].ordinal());
}

test "hot integer COUNT cache capacity follows cache lines and L1 L2 shares" {
    defer hw.test_cpu = null;
    hw.test_cpu = .{ .l1d = .{ .bytes = 32 << 10, .line_bytes = 64 }, .l2 = .{ .bytes = 1 << 20 } };
    const wide = IntCountTop.countCacheEntries();
    try std.testing.expectEqual(@as(usize, 1024), wide);
    hw.test_cpu.?.l1d.max_sharing = 2;
    try std.testing.expectEqual(wide, IntCountTop.countCacheEntries());
    hw.test_cpu.?.l2.bytes = 64 << 10;
    try std.testing.expectEqual(@as(usize, 256), IntCountTop.countCacheEntries());
    hw.test_cpu.?.l1d.line_bytes = 8192;
    try std.testing.expectEqual(@as(usize, 0), IntCountTop.countCacheEntries());
    try std.testing.expectEqual(@as(usize, 0), CountTop.countCacheEntries());
    try std.testing.expectEqual(@as(usize, 16), @sizeOf(IntCountTop.Record));
}

test "hot integer COUNT cache bypasses a nonreducing source and retries later hot NULLs exactly" {
    defer hw.test_cpu = null;
    hw.test_cpu = .{ .l1d = .{ .bytes = 256, .line_bytes = 64 }, .l2 = .{ .bytes = 4096 } };
    const a = std.testing.allocator;
    const rows = 512;
    const s = (try IntCountTop.State.init(a, null, rows, 1, 0, 0, .{ .k = 4, .descending = true }, 64, 0)).?;
    defer s.deinit();
    const local = &s.locals[0];
    local.start(0, 0);
    for (0..256) |r| try local.append(@intCast(r), {}, @intCast(r));
    try std.testing.expect(local.cache_disabled);
    for (local.count_cache.entries) |entry| try std.testing.expectEqual(@as(u32, 0), entry.count);
    local.start(256, 1);
    try std.testing.expect(!local.cache_disabled);
    for (256..rows) |r| try local.append(null, {}, @intCast(r));
    try std.testing.expect(!local.cache_disabled);
    try s.finish();
    const winner = for (s.top()) |group| {
        if (group.keyValue() == null) break group;
    } else return error.TestUnexpectedResult;
    try std.testing.expectEqual(@as(i64, 256), winner.countValue());
    try std.testing.expectEqual(@as(u32, 256), winner.ordinal());
    try std.testing.expectEqual(@as(usize, rows), local.contributed_rows);
}

test "hot integer COUNT run quote excludes its unallocated cache and failed claims preserve histograms" {
    defer hw.test_cpu = null;
    hw.test_cpu = .{ .l1d = .{ .bytes = 32 << 10, .line_bytes = 64 }, .l2 = .{ .bytes = 1 << 20 } };
    const O = IntCountTop;
    const Entry = @typeInfo(@FieldType(O.State, "count_caches")).pointer.child;
    const rows = 128;
    var budget: memory.SharedBudget = .{ .limit = 1 << 30 };
    const ordinary = O.State.reservation(rows, 1, 64, 4, 0);
    const run_quote = O.State.runReservation(rows, 1, 64, 4, 0);
    try std.testing.expectEqual(O.countCacheEntries() * @sizeOf(Entry), ordinary - run_quote);
    const run = (try O.State.initRuns(std.testing.allocator, &budget, rows, 1, 0, 0, .{ .k = 4, .descending = true }, 64, 1, 0)).?;
    try std.testing.expectEqual(run_quote, budget.used.load(.monotonic));
    try std.testing.expectEqual(@as(usize, 0), run.count_caches.len);
    run.deinit();
    try std.testing.expectEqual(@as(usize, 0), budget.used.load(.monotonic));
    const s = (try O.State.init(std.testing.allocator, &budget, rows, 1, 0, 0, .{ .k = 4, .descending = true }, 64, 0)).?;
    defer s.deinit();
    const blocks = s.blocks;
    s.blocks = blocks[0..0];
    defer s.blocks = blocks;
    const local = &s.locals[0];
    var entry: Entry = .{ .key = 9, .first = 0, .count = 2 };
    try std.testing.expectError(error.ExceededMemoryBudget, local.flushCount(&entry));
    try std.testing.expectEqual(@as(u32, 2), entry.count);
    for (local.records.histogram) |n| try std.testing.expectEqual(@as(u32, 0), n);
    try std.testing.expectEqual(@as(usize, 0), local.contributed_rows);
}

fn countScratchGrowthTrial(a: std.mem.Allocator) !void {
    var budget: memory.SharedBudget = .{ .limit = 1 << 30 };
    defer std.debug.assert(budget.used.load(.monotonic) == 0);
    const s = (try IntCountTop.State.init(a, &budget, 1028, 1, 0, 0, .{ .k = 257, .descending = true }, 64, 0)).?;
    defer s.deinit();
    for (0..1028) |i| try s.locals[0].append(if (i % 257 == 0) null else @intCast(i % 257), {}, @intCast(i));
    try s.finish();
    try std.testing.expectEqual(@as(usize, 257), s.group_count);
    for (s.top(), 0..) |g, i| {
        try std.testing.expectEqual(@as(i64, 4), g.countValue());
        try std.testing.expectEqual(@as(u32, @intCast(i)), g.ordinal());
        try std.testing.expectEqual(if (i == 0) null else @as(?i64, @intCast(i)), g.keyValue());
    }
}

test "hot integer COUNT scratch grows from exact groups preserving NULL ties and all failed allocations" {
    defer hw.test_cpu = null;
    hw.test_cpu = .{ .l1d = .{ .bytes = 256, .line_bytes = 64 }, .l2 = .{ .bytes = 128 } };
    try countScratchGrowthTrial(std.testing.allocator);
    try std.testing.checkAllAllocationFailures(std.testing.allocator, countScratchGrowthTrial, .{});
}

fn streamingCountTrial(a: std.mem.Allocator, workers: usize, descending: bool, divisor: usize) !void {
    const rows = 4096;
    const items = 17;
    const cut: Cut = .{ .k = 10, .descending = descending };
    var budget: memory.SharedBudget = .{ .limit = 1 << 30 };
    defer std.debug.assert(budget.used.load(.monotonic) == 0);
    const streaming = (try IntCountTop.State.initStreamRuns(a, &budget, rows, workers, 0, cut, 64, items, 0)).?;
    defer streaming.deinit();
    const materialized = (try IntCountTop.State.initRuns(a, &budget, rows, workers, 0, 0, cut, 64, items, 0)).?;
    defer materialized.deinit();
    try std.testing.expectEqual(@as(usize, 0), streaming.groups.len);
    try std.testing.expectEqual(@as(usize, 0), streaming.winners.len);
    try std.testing.expectEqual(items, streaming.boundaries.len);
    // Work arrives out of source order and is spread over different worker heaps.
    var item: usize = items;
    while (item != 0) {
        item -= 1;
        const lo = rows * item / items;
        const hi = rows * (item + 1) / items;
        const w = item % workers;
        streaming.locals[w].start(@intCast(lo), item);
        materialized.locals[w].start(@intCast(lo), item);
        for (lo..hi) |row| {
            const key: i64 = @intCast(row / divisor);
            try streaming.locals[w].append(key, {}, @intCast(row));
            try materialized.locals[w].append(key, {}, @intCast(row));
        }
    }
    try streaming.finish();
    try materialized.finish();
    try std.testing.expectEqual(materialized.group_count, streaming.group_count);
    try std.testing.expectEqualDeep(materialized.top(), streaming.top());
}

test "streaming COUNT top-H reconciles source seams and first-ordinal ties at one and four workers" {
    for ([_]usize{ 1, 4 }) |j| for ([_]bool{ false, true }) |descending|
        for ([_]usize{ 3, 241, 1024 }) |divisor|
            try streamingCountTrial(std.testing.allocator, j, descending, divisor);
    try std.testing.checkAllAllocationFailures(std.testing.allocator, streamingCountTrial, .{ 1, true, 3 });
}

test "streaming COUNT top-H declines NULL and partially ordered sources before publishing" {
    for ([_][4]?i64{ .{ 1, null, 2, 3 }, .{ 1, 0, 2, 3 }, .{ 1, 2, 1, 3 } }) |keys| {
        const s = (try IntCountTop.State.initStreamRuns(std.testing.allocator, null, keys.len, 2, 0, .{ .k = 2, .descending = true }, 64, 2, 0)).?;
        defer s.deinit();
        for (0..2) |item| {
            s.locals[item].start(@intCast(item * 2), item);
            for (item * 2..item * 2 + 2) |row| try s.locals[item].append(keys[row], {}, @intCast(row));
        }
        try std.testing.expectError(error.RunOrderDeclined, s.finish());
    }
}
