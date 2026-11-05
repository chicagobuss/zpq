const expr_agg = @import("../expr/agg.zig");
const NdvStat = @import("../ndv.zig").Stat;

/// The one count(DISTINCT) call `foldDistinctPairs` can answer: a plain numeric column, the only count(DISTINCT) of
/// the query, grouped by a fixed-width key of at most 16 bytes. Null otherwise.
pub fn distinctPairsCall(calls: []const expr_agg.AggCall, fixed_stride: u8) ?usize {
    if (fixed_stride == 0 or fixed_stride > 16) return null;
    var found: ?usize = null;
    for (calls, 0..) |call, i| {
        if (call.func != .count_distinct) continue;
        if (found != null) return null;
        const arg = call.arg orelse return null;
        if (arg != .col_ref) return null;
        switch (arg.col_ref.expr_type) {
            .i64, .f64 => {},
            else => return null,
        }
        found = i;
    }
    return found;
}

/// Whether pairs beat per-group sets: only for a group count the dictionaries bound to at most 2^20, each group
/// taking 64 rows or more. Near-unique keys keep their few values inline in the sets, cheaper than sorting a pair a
/// row and looking every group up afterwards; there, and wherever nothing bounds the groups, the sets stay.
pub fn pairsPay(stat: ?NdvStat) bool {
    const st = stat orelse return false;
    const est = st.estimate orelse return false;
    return est > 0 and est <= 1 << 20 and st.rows / est >= 64;
}

/// Whether a worker's sets, after taking `rows` values (`DistinctPairs.seen`), are the kind pairs beat: groups of 8
/// rows or more on average, nearly every row a value its group has not seen (nine in ten). Near-unique keys keep a few
/// values inline in each set, and input whose values repeat (skew, or a few huge groups) keeps small or cache-resident
/// sets; the sets win there. Rows a group gathers only grow, so too few of them is `.wait`: small row groups, or files
/// each holding most of the keys, reach eight a group only after several items. The share of new values only falls,
/// so too few is final. The row test runs first: it is free, and walking every set at each check cost near-unique
/// keys ~4%.
pub fn pairsPayRuntime(table: *const expr_agg.GroupTable, agg_idx: usize, rows: u64) enum { pay, wait, never } {
    const groups = table.groupCount();
    if (groups == 0 or rows < 8 * @as(u64, groups)) return .wait;
    const n_calls = table.accumulators.items.len / groups;
    var values: u64 = 0;
    for (0..groups) |g| {
        const acc = table.accumulators.items[g * n_calls + agg_idx];
        if (acc == .distinct) values += acc.distinct.count();
    }
    return if (10 * values >= 9 * rows) .pay else .never;
}

/// Whether pairs pay for a count(DISTINCT) over the groups a count cut retained, each group's rows counted exactly
/// by the cut's first pass, on `workers`, the workers the pass plans. Pairs pay by spreading their work over many
/// groups: the sorted pairs partition by group, and the pair owner's second stage reduces its pairs by group, so a
/// group holding more than a worker's share of the retained rows is a serial tail of its own. Per-group sets merge
/// one group's values across workers in parallel (`parallel_distinct`), so they win there. The test is on the
/// groups' distribution alone; fewer groups than workers can never pass it, since their largest then holds more
/// than a worker's share.
pub fn retainedPairsPay(counts: []const u64, workers: usize) bool {
    var total: u64 = 0;
    var largest: u64 = 0;
    for (counts) |c| {
        total +|= c;
        largest = @max(largest, c);
    }
    return total != 0 and largest *| workers <= total;
}

test "retained groups keep pairs only where every worker has a group's share to merge" {
    const testing = @import("std").testing;
    // Q9: ten groups, one holding 43% of their rows.
    const q9 = [_]u64{ 18_295_832, 6_687_587, 4_261_812, 3_320_229, 1_843_518, 1_792_369, 1_755_192, 1_542_717, 1_516_690, 1_435_578 };
    try testing.expect(!retainedPairsPay(&q9, 16));
    try testing.expect(!retainedPairsPay(&q9, 4));
    try testing.expect(!retainedPairsPay(&q9, 1 << 10));
    var even: [64]u64 = @splat(1000);
    try testing.expect(retainedPairsPay(&even, 16));
    try testing.expect(retainedPairsPay(&even, 64));
    try testing.expect(!retainedPairsPay(&even, 65));
    even[0] = 64_000;
    try testing.expect(!retainedPairsPay(&even, 16));
    try testing.expect(retainedPairsPay(&q9, 1));
    try testing.expect(!retainedPairsPay(&.{}, 1));
}
