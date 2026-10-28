const std = @import("std");
const testing = std.testing;
const expr_agg = @import("../expr/agg.zig");

/// `acc` as the copying merge leaves it after merging it into a fresh accumulator. Merging into a fresh one neither
/// allocates nor frees (`gpa` goes unused), and a string winner stays borrowed. Applying it twice changes nothing: a
/// fresh sum adds into +0.0, so never ends -0.0.
pub fn foldedAcc(gpa: std.mem.Allocator, call: expr_agg.AggCall, acc: expr_agg.Accumulator) !expr_agg.Accumulator {
    // Only a count pairs folded merges in place (`countedCalls`); a fresh count(DISTINCT) state takes it as it is.
    if (call.func == .count_distinct) {
        std.debug.assert(acc == .count);
        return acc;
    }
    if (foldsToItself(acc)) return acc;
    var out = expr_agg.Accumulator.init(call);
    try out.merge(acc, gpa);
    return out;
}

/// Whether merging `acc` into a fresh accumulator gives every bit of it back. Not a float sum of -0.0 (it becomes
/// +0.0) or NaN (left to the addition), and not a count past i64 (`Accumulator.merge` refuses it); integer and
/// DECIMAL sums add into zero exactly, and an extremum or a string winner is taken as is.
fn foldsToItself(acc: expr_agg.Accumulator) bool {
    const plainFloat = struct {
        fn f(x: f64) bool {
            return !std.math.isNan(x) and !(x == 0 and std.math.signbit(x));
        }
    }.f;
    return switch (acc) {
        .count => |c| c <= std.math.maxInt(i64),
        .distinct => false,
        .sum_f => |x| if (x) |v| plainFloat(v) else true,
        .avg => |a| plainFloat(a.sum),
        .sum_i, .min_i, .min_f, .max_i, .max_f, .min_bytes, .max_bytes, .min_arena, .max_arena, .sum_d, .avg_d, .min_d, .max_d => true,
    };
}

pub fn sameAccBits(a: expr_agg.Accumulator, b: expr_agg.Accumulator) bool {
    const F = struct {
        fn f(x: ?f64, y: ?f64) bool {
            if (x == null or y == null) return x == null and y == null;
            return @as(u64, @bitCast(x.?)) == @as(u64, @bitCast(y.?));
        }
        fn s(x: ?[]const u8, y: ?[]const u8) bool {
            if (x == null or y == null) return x == null and y == null;
            return x.?.ptr == y.?.ptr and x.?.len == y.?.len;
        }
    };
    if (std.meta.activeTag(a) != std.meta.activeTag(b)) return false;
    return switch (a) {
        .count => |c| c == b.count,
        .distinct => false,
        .sum_i => |v| v == b.sum_i,
        .min_i => |v| v == b.min_i,
        .max_i => |v| v == b.max_i,
        .min_d => |v| v == b.min_d,
        .max_d => |v| v == b.max_d,
        .sum_f => |v| F.f(v, b.sum_f),
        .min_f => |v| F.f(v, b.min_f),
        .max_f => |v| F.f(v, b.max_f),
        .min_bytes => |v| F.s(v, b.min_bytes),
        .max_bytes => |v| F.s(v, b.max_bytes),
        .min_arena => |v| std.meta.eql(v, b.min_arena),
        .max_arena => |v| std.meta.eql(v, b.max_arena),
        .avg => |v| F.f(v.sum, b.avg.sum) and v.count == b.avg.count and v.isum == b.avg.isum,
        .sum_d => |v| v.lo == b.sum_d.lo and v.hi == b.sum_d.hi and v.n == b.sum_d.n,
        .avg_d => |v| v.lo == b.avg_d.lo and v.hi == b.avg_d.hi and v.n == b.avg_d.n,
    };
}

test "foldsToItself: merging such an accumulator into a fresh one gives every bit back, and only those" {
    const call = struct {
        fn c(func: expr_agg.AggFunc, result: expr_agg.ResultShape) expr_agg.AggCall {
            return .{ .func = func, .alias = "a", .result = result, .arg = null, .where = null };
        }
    }.c;
    const nan: f64 = std.math.nan(f64);
    const inf: f64 = std.math.inf(f64);
    const big = std.math.maxInt(i64);
    var dec: expr_agg.DecSum = .{};
    dec.add(-(@as(i192, 1) << 150) + 7, 3);
    const Case = struct { call: expr_agg.AggCall, acc: expr_agg.Accumulator };
    const cases = [_]Case{
        .{ .call = call(.count, .i64), .acc = .{ .count = 0 } },
        .{ .call = call(.count, .i64), .acc = .{ .count = big } },
        .{ .call = call(.count, .i64), .acc = .{ .count = big + 1 } },
        .{ .call = call(.sum, .i64), .acc = .{ .sum_i = null } },
        .{ .call = call(.sum, .i64), .acc = .{ .sum_i = -(@as(i128, 1) << 100) } },
        .{ .call = call(.sum, .f64), .acc = .{ .sum_f = null } },
        .{ .call = call(.sum, .f64), .acc = .{ .sum_f = -0.0 } },
        .{ .call = call(.sum, .f64), .acc = .{ .sum_f = 0.0 } },
        .{ .call = call(.sum, .f64), .acc = .{ .sum_f = nan } },
        .{ .call = call(.sum, .f64), .acc = .{ .sum_f = -inf } },
        .{ .call = call(.sum, .f64), .acc = .{ .sum_f = -2.5 } },
        .{ .call = call(.min, .f64), .acc = .{ .min_f = -0.0 } },
        .{ .call = call(.max, .f64), .acc = .{ .max_f = nan } },
        .{ .call = call(.min, .i64), .acc = .{ .min_i = null } },
        .{ .call = call(.max, .i64), .acc = .{ .max_i = 3 } },
        .{ .call = call(.min, .bytes), .acc = .{ .min_bytes = "pear" } },
        .{ .call = call(.max, .bytes), .acc = .{ .max_bytes = null } },
        .{ .call = call(.avg, .avg_f64), .acc = .{ .avg = .{ .sum = -0.0, .count = 2, .isum = 0 } } },
        .{ .call = call(.avg, .avg_f64), .acc = .{ .avg = .{ .sum = 1.5, .count = 2, .isum = -9 } } },
        .{ .call = call(.sum, .dec), .acc = .{ .sum_d = dec } },
        .{ .call = call(.avg, .avg_dec), .acc = .{ .avg_d = dec } },
        .{ .call = call(.min, .dec), .acc = .{ .min_d = -5 } },
        .{ .call = call(.max, .dec), .acc = .{ .max_d = null } },
    };
    var itself: usize = 0;
    for (cases) |c| {
        var out = expr_agg.Accumulator.init(c.call);
        const merged = out.merge(c.acc, testing.allocator);
        if (foldsToItself(c.acc)) {
            try merged;
            try testing.expect(sameAccBits(c.acc, out));
            itself += 1;
        }
        // The rest (-0.0 and NaN float sums, a count past i64) the merge changes, or refuses, and `foldedAcc` with it.
        const folded = foldedAcc(testing.allocator, c.call, c.acc);
        if (merged) {
            try testing.expect(sameAccBits(try folded, out));
        } else |e| try testing.expectError(e, folded);
    }
    try testing.expectEqual(@as(usize, cases.len - 4), itself);
}

