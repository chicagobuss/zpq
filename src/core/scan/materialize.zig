const std = @import("std");
const expr_ast = @import("../expr/ast.zig");
const expr_agg = @import("../expr/agg.zig");
const AggValue = @import("output_values.zig").AggValue;
const hll = @import("../expr/aggregate/hll.zig");
const argminmax = @import("../expr/aggregate/argminmax.zig");
const quantile = @import("../expr/aggregate/quantile.zig");

fn decValue(v: ?i128, spec: expr_ast.Dec) AggValue {
    return if (v) |x| .{ .dec = .{ .v = x, .spec = spec } } else .null_val;
}

/// Convert one agg's final accumulator state into a wire-friendly value.
pub fn materializeOne(gpa: std.mem.Allocator, call: expr_agg.AggCall, state: expr_agg.Accumulator) !AggValue {
    return switch (call.func) {
        .count => .{ .i = @intCast(state.count) },
        .count_distinct => switch (state) {
            .count => |cnt| .{ .i = @intCast(cnt) },
            .distinct => |d| .{ .i = @intCast(d.count()) },
            else => .{ .i = 0 },
        },
        // i128 carries the full sum (incl. i64 overflow + unsigned-64);
        // the JSON layer prints it directly, no narrowing/overflow error.
        // sum/min/max over no values are NULL, never a 0 or "" stand-in.
        .sum => switch (call.result) {
            .i64 => if (state.sum_i) |v| .{ .i = v } else .null_val,
            .f64 => if (state.sum_f) |v| .{ .f = v } else .null_val,
            .dec => decValue(try state.sum_d.result(), expr_agg.resultDec(call)),
            .bytes, .avg_f64, .avg_dec => unreachable,
        },
        .min => switch (call.result) {
            .i64 => if (state.min_i) |v| .{ .i = v } else .null_val,
            .f64 => if (state.min_f) |v| .{ .f = v } else .null_val,
            .bytes => if (state.stringValue()) |b| .{ .s = try gpa.dupe(u8, b) } else .null_val,
            .dec => decValue(state.min_d, expr_agg.resultDec(call)),
            .avg_f64, .avg_dec => unreachable,
        },
        .max => switch (call.result) {
            .i64 => if (state.max_i) |v| .{ .i = v } else .null_val,
            .f64 => if (state.max_f) |v| .{ .f = v } else .null_val,
            .bytes => if (state.stringValue()) |b| .{ .s = try gpa.dupe(u8, b) } else .null_val,
            .dec => decValue(state.max_d, expr_agg.resultDec(call)),
            .avg_f64, .avg_dec => unreachable,
        },
        .avg => if (call.state or (call.compared and call.result == .avg_dec))
            try avgStateValue(call, state)
        else if (call.result == .avg_dec)
            decValue(try expr_agg.avgDecMean(call, state.avg_d), expr_agg.resultDec(call))
        else if (expr_agg.avgMean(state.avg)) |m| .{ .f = m } else .null_val,
        .var_samp, .var_pop, .stddev_samp, .stddev_pop => if (state.moments.result(call.func)) |v| .{ .f = v } else .null_val,
        .approx_count_distinct => .{ .i = hll.count(state.blob.bytes) },
        .arg_max, .arg_min => blk: {
            const rec = state.blob.used();
            if (rec.len == 0) break :blk .null_val;
            const arg = argminmax.argOf(rec);
            break :blk switch (call.result) {
                .i64 => .{ .i = std.mem.readInt(i64, arg[0..8], .little) },
                .f64 => .{ .f = @bitCast(std.mem.readInt(u64, arg[0..8], .little)) },
                .bytes => .{ .s = try gpa.dupe(u8, arg) },
                else => unreachable,
            };
        },
        .string_agg => if (try state.blob.joinText(gpa, call.sep)) |joined| .{ .s = joined } else .null_val,
        .quantile_cont => if (quantile.cont(state.blob.f64s(), call.q)) |v| .{ .f = v } else .null_val,
    };
}

/// avg_state's mergeable pair, also the cell of a `compared` avg over a DECIMAL: the exact sum and count, at the
/// argument's type.
fn avgStateValue(call: expr_agg.AggCall, state: expr_agg.Accumulator) !AggValue {
    if (call.result != .avg_dec)
        return .{ .avg = .{ .sum = expr_agg.avgSum(state.avg), .count = @intCast(state.avg.count) } };
    return .{ .avg_dec = .{
        .sum = state.avg_d.wide(),
        .count = @intCast(state.avg_d.n),
        .mean = try state.avg_d.mean(),
        .spec = call.arg.?.decSpec().?,
    } };
}

