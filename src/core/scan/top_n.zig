const std = @import("std");

pub const Admission = enum { skip, append, replace };

/// The queue keeps its worst entry at the front. Equal entries do not displace earlier input.
pub fn admission(limit: usize, count: usize, candidate_vs_worst: std.math.Order) Admission {
    if (limit == 0) return .skip;
    if (count < limit) return .append;
    return if (candidate_vs_worst == .gt) .replace else .skip;
}

/// Turn the final ascending result order and its stable input-order tie into a worst-first heap order.
pub fn worstFirst(order: std.math.Order, tie: std.math.Order) std.math.Order {
    return (if (order == .eq) tie else order).invert();
}

test "top N admission is bounded and keeps earlier ties" {
    const t = std.testing;
    try t.expectEqual(Admission.skip, admission(0, 0, .gt));
    try t.expectEqual(Admission.append, admission(2, 1, .lt));
    try t.expectEqual(Admission.skip, admission(2, 2, .eq));
    try t.expectEqual(Admission.skip, admission(2, 2, .lt));
    try t.expectEqual(Admission.replace, admission(2, 2, .gt));
    try t.expectEqual(std.math.Order.gt, worstFirst(.lt, .gt));
    try t.expectEqual(std.math.Order.gt, worstFirst(.eq, .lt));
    try t.expectEqual(std.math.Order.lt, worstFirst(.eq, .gt));
}
