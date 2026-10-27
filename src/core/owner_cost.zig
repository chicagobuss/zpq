//! A selected-row work estimate chooses profitability; original row bounds still fund record storage and scratch.
const std = @import("std");
const hw = @import("hw.zig");
const lanes = @import("owner_lanes.zig");
const owners = @import("owner_packed.zig");

pub const Quote = struct {
    workers: usize,
    parts: usize,
    records: usize,
    finish: usize,
    result: usize,
    peak: usize,
    work: usize,
};

pub fn quote(
    comptime Bits: type,
    comptime kinds: []const lanes.Lane,
    comptime Ordinal: type,
    rows: usize,
    selected: usize,
    workers: usize,
    result: usize,
    forced_parts: ?usize,
) ?Quote {
    return quoteStorage(Bits, kinds, Ordinal, rows, selected, workers, result, forced_parts, false);
}

pub fn quoteStorage(
    comptime Bits: type,
    comptime kinds: []const lanes.Lane,
    comptime Ordinal: type,
    rows: usize,
    selected: usize,
    workers: usize,
    result: usize,
    forced_parts: ?usize,
    compact: bool,
) ?Quote {
    const O = owners.Owner(Bits, kinds, Ordinal);
    const L = lanes.Layout(Bits, kinds, Ordinal);
    if (rows == 0 or selected > rows or workers == 0 or workers > 64 or !L.admitsRows(rows)) return null;
    const l2 = hw.l2BytesPerWorkerOr(640 << 10);
    const table_bytes = selected *| (@sizeOf(O.Group) + 8);
    const target = std.math.divCeil(usize, table_bytes, @max(1, l2 / 2)) catch return null;
    const parts = if (forced_parts) |n|
        std.math.ceilPowerOfTwo(usize, std.math.clamp(if (n == 0) 2048 else n, 64, 16384)) catch return null
    else
        std.math.ceilPowerOfTwo(usize, std.math.clamp(target, 64, 16384)) catch return null;
    const records = O.State.chargeStorage(rows, workers, parts, compact);
    // For any partition histogram, the W retained maxima sum to <= rows. At a half-full load factor,
    // rounding each pointer table to a power of two costs strictly fewer than four u32 slots per row.
    const finish = rows *| (@sizeOf(O.Group) + 4 * @sizeOf(u32));
    const peak = records +| finish +| result;
    if (peak == std.math.maxInt(usize)) return null;
    const record_bytes: usize = if (compact) @sizeOf(lanes.SharedRecord) else @sizeOf(O.Record);
    return .{
        .workers = workers,
        .parts = parts,
        .records = records,
        .finish = finish,
        .result = result,
        .peak = peak,
        .work = selected *| (record_bytes + @sizeOf(O.Group) + 8),
    };
}

test "owner cost selected work never discounts physical records or exact histogram scratch" {
    const L = lanes.Layout(u128, &.{.sum}, u32);
    const Fold = @import("owner_fold.zig").Table(L);
    const raw = quote(u128, &.{.sum}, u32, 65536, 65536, 8, 4096, 64).?;
    const sparse = quote(u128, &.{.sum}, u32, 65536, 64, 8, 4096, 64).?;
    try std.testing.expectEqual(raw.records, sparse.records);
    try std.testing.expectEqual(raw.peak, sparse.peak);
    try std.testing.expect(raw.work > sparse.work);
    const Rows = struct {
        fn at(rows: []const usize, p: usize) usize {
            return rows[p];
        }
    };
    for (1..65) |parts| {
        var rows: [64]usize = @splat(0);
        for (0..65536) |i| rows[i % parts] += 1;
        try std.testing.expect(Fold.concurrentBytes(rows[0..parts], parts, 8, Rows.at) <= raw.finish);
    }
    try std.testing.expectEqual(null, quote(u64, &.{}, u32, 1 << 31, 1, 8, 0, null));
    try std.testing.expectEqual(null, quote(u64, &.{}, u32, 8, 9, 1, 0, null));
}

test "owner shared storage charge includes both worst-case scatters and never discounts on selectivity" {
    const kinds = &.{ lanes.Lane.sum, lanes.Lane.min, lanes.Lane.max, lanes.Lane.present_count };
    const O = owners.Owner(u64, kinds, u32);
    const full = quoteStorage(u64, kinds, u32, 1 << 20, 1 << 20, 16, 4096, 64, true).?;
    const sparse = quoteStorage(u64, kinds, u32, 1 << 20, 64, 16, 4096, 64, true).?;
    const wide = quote(u64, kinds, u32, 1 << 20, 1 << 20, 16, 4096, 64).?;
    try std.testing.expectEqual(O.State.chargeStorage(1 << 20, 16, 64, true), full.records);
    try std.testing.expectEqual(full.records, sparse.records);
    try std.testing.expectEqual(full.peak, sparse.peak);
    try std.testing.expect(full.records < wide.records and full.work < wide.work);
    try std.testing.expectEqual(null, quoteStorage(u128, kinds, u32, 4096, 4096, 1, 0, 64, true));
}
