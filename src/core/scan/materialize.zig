const std = @import("std");
const expr_ast = @import("../expr/ast.zig");
const expr_agg = @import("../expr/agg.zig");
const AggValue = @import("output_values.zig").AggValue;

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
        .avg => if (call.state)
            try avgStateValue(call, state)
        else if (call.result == .avg_dec)
            decValue(try expr_agg.avgDecMean(call, state.avg_d), expr_agg.resultDec(call))
        else if (expr_agg.avgMean(state.avg)) |m| .{ .f = m } else .null_val,
    };
}

/// avg_state's mergeable pair. A DECIMAL one keeps its mean at the argument's scale only to rank and filter by.
fn avgStateValue(call: expr_agg.AggCall, state: expr_agg.Accumulator) !AggValue {
    if (call.result != .avg_dec)
        return .{ .avg = .{ .sum = expr_agg.avgSum(state.avg), .count = @intCast(state.avg.count) } };
    return .{ .avg_dec = .{
        .sum = state.avg_d.wide(),
        .count = @intCast(state.avg_d.n),
        .mean = try state.avg_d.mean(),
        .spec = expr_agg.resultDec(call),
    } };
}

