const std = @import("std");
const testing = std.testing;
const expr_ast = @import("../ast.zig");
const expr_eval = @import("../eval.zig");
const filter_ast = @import("../../filter/ast.zig");
const filter_eval = @import("../../filter/eval.zig");

pub const AggFunc = enum {
    count,
    count_distinct,
    sum,
    min,
    max,
    avg,
};

/// One aggregate call. `arg` is null only for `count(*)`. `where` is
/// null when there's no FILTER clause (apply to all surviving rows).
pub const AggCall = struct {
    func: AggFunc,
    /// Value expression. Null iff `func == .count` and the source
    /// was `count(*)`. For other funcs, must be present and resolve
    /// to a numeric type (i64 / f64).
    arg: ?expr_ast.Expr,
    /// Optional FILTER (WHERE ...) predicate. Null = no per-agg gate.
    where: ?filter_ast.Filter,
    /// User-supplied alias (or auto-generated if AS was omitted).
    alias: []const u8,
    /// Set when AS was omitted: the call's source text (`sum(x)`, `count(*)`), the output name to fall back on when
    /// the bare function name `alias` defaults to would clash with another output column.
    default_name: ?[]const u8 = null,
    /// Result lane resolved at parse time.
    result: ResultShape,
    /// Added to every value `sum` folds: `sum(x + c)` folds `x` and adds `c` per value, as many times as there are
    /// values, so the constant is never materialized per row (see the parser's `sumOffset`).
    offset: i64 = 0,
    /// `avg_state(x)`: an avg that answers its mergeable sum and count instead of the mean. It folds exactly as
    /// `avg(x)` does (same `func` and `result`); only what it answers differs.
    state: bool = false,
    /// `min`/`max` of `make_date(col)`, folded over `col` itself (`arg`): the answer is the DATE those days are.
    as_date: bool = false,
};

/// How an aggregate folds, and what it emits in the final 1-row parquet:
///   - single i64: count, sum/min/max of integer columns
///   - single f64: sum/min/max of float columns, avg's mean of a non-DECIMAL argument
///   - exact DECIMAL sum/min/max, and avg's mean of a DECIMAL argument (`avgDec`)
///   - avg_state's pair: `<alias>__sum` (f64, or DECIMAL(38, s)) + `<alias>__count: i64`
pub const ResultShape = enum {
    i64,
    f64,
    /// avg over a non-DECIMAL argument: a DOUBLE mean, or avg_state's f64 sum and count.
    avg_f64,
    /// String (BYTE_ARRAY/UTF8) min/max — bytewise/unsigned order.
    bytes,
    /// DECIMAL sum (DECIMAL(38, s)), min or max (the argument's type): exact. See `resultDec`.
    dec,
    /// avg over a DECIMAL: the exact sum and count, folded to the mean (`avgDec`) or answered as avg_state's pair.
    avg_dec,
};

/// The DECIMAL type a `.dec`/`.avg_dec` call's result has: sum widens to 38 digits at the argument's scale, as
/// DuckDB and Trino type it; min, max and avg_state (whose pair re-aggregates at it) keep the argument's type; avg's
/// mean is `avgDec` of it.
pub fn resultDec(call: AggCall) expr_ast.Dec {
    const arg = call.arg.?.decSpec().?;
    if (call.func == .avg and !call.state) return avgDec(arg);
    return if (call.func == .sum) arg.sum() else arg;
}

/// avg over DECIMAL(p, s) answers DECIMAL(p', s + p' - p), p' = min(38, p + 4): up to 4 more fractional digits, as
/// DataFusion's (p + 4, s + 4) whenever p <= 34, but never fewer integer digits, so at p = 38 the argument's own type,
/// as Trino types it. Never overflows: |mean| <= max |v|, which lies on the s grid and so on the finer s' one, and
/// rounding half away from zero to that grid cannot pass it.
pub fn avgDec(arg: expr_ast.Dec) expr_ast.Dec {
    const p = @min(expr_ast.Dec.max_precision, arg.precision + 4);
    return .{ .precision = p, .scale = arg.scale + (p - arg.precision) };
}

/// The rounded DECIMAL mean `call` ranks and answers by: at `avgDec` scale for avg, at the argument's for avg_state.
pub fn avgDecMean(call: AggCall, st: DecSum) expr_eval.Error!?i128 {
    const arg = call.arg.?.decSpec().?;
    return st.meanAt(resultDec(call).scale - arg.scale);
}

/// An exact DECIMAL sum. Wide enough that no run of i128 values can overflow it before 2^64 of them, so only the
/// final sum has to fit 38 digits — as in Trino, a sum may pass through larger values and come back.
pub const DecSum = struct {
    /// A 192-bit two's-complement sum split in two, so the state is 32 bytes like the other accumulators': no run of
    /// fewer than 2^64 values under 2^127 reaches 2^191.
    lo: u128 = 0,
    hi: i64 = 0,
    n: u64 = 0,

    pub fn get(self: DecSum) i192 {
        return (@as(i192, self.hi) << 128) | self.lo;
    }

    pub fn add(self: *DecSum, v: i192, n: u64) void {
        const sum = self.get() + v;
        self.lo = @truncate(@as(u192, @bitCast(sum)));
        self.hi = @intCast(sum >> 128);
        self.n += n;
    }

    /// The exact sum at any width (up to 192 bits), null over no values: what an avg reports beside its mean.
    pub fn wide(self: DecSum) ?i192 {
        return if (self.n == 0) null else self.get();
    }

    /// The sum as a 38-digit DECIMAL: null over no values, an error past 38 digits rather than a wrapped value.
    pub fn result(self: DecSum) expr_eval.Error!?i128 {
        if (self.n == 0) return null;
        return try fit38(self.get());
    }

    /// The mean rounded half away from zero to the argument's scale (Trino's rule for avg over a DECIMAL).
    pub fn mean(self: DecSum) expr_eval.Error!?i128 {
        return self.meanAt(0);
    }

    /// The mean at `shift` more digits of scale (at most 4), rounded half away from zero. |sum| * 10^shift can pass
    /// 192 bits, so it divides the whole quotient first: the remainder is under 2^64, its shifted double under 2^79.
    pub fn meanAt(self: DecSum, shift: u8) expr_eval.Error!?i128 {
        std.debug.assert(shift <= 4);
        if (self.n == 0) return null;
        const v = self.get();
        const n: u192 = self.n;
        const q = @abs(v) / n;
        const r: u128 = @intCast(@abs(v) % n);
        const scale: u128 = @intCast(expr_ast.pow10(shift));
        const frac = (2 * r * scale + self.n) / (2 * @as(u128, self.n));
        const whole = std.math.mul(u192, q, scale) catch return error.DecimalOverflow;
        const m = std.math.add(u192, whole, frac) catch return error.DecimalOverflow;
        const x = std.math.cast(i192, m) orelse return error.DecimalOverflow;
        return try fit38(if (v < 0) -x else x);
    }
};

fn fit38(v: i192) expr_eval.Error!i128 {
    const bound: i192 = expr_ast.pow10(expr_ast.Dec.max_precision);
    if (v >= bound or v <= -bound) return error.DecimalOverflow;
    return @intCast(v);
}

test "avg over DECIMAL(p, s) types as DECIMAL(min(38, p + 4), s + that widening)" {
    const Case = struct { p: u8, s: u8, want_p: u8, want_s: u8 };
    for ([_]Case{
        .{ .p = 9, .s = 2, .want_p = 13, .want_s = 6 },
        .{ .p = 34, .s = 0, .want_p = 38, .want_s = 4 },
        .{ .p = 34, .s = 33, .want_p = 38, .want_s = 37 },
        .{ .p = 35, .s = 0, .want_p = 38, .want_s = 3 },
        .{ .p = 35, .s = 34, .want_p = 38, .want_s = 37 },
        .{ .p = 36, .s = 35, .want_p = 38, .want_s = 37 },
        .{ .p = 38, .s = 0, .want_p = 38, .want_s = 0 },
        .{ .p = 38, .s = 37, .want_p = 38, .want_s = 37 },
        .{ .p = 38, .s = 38, .want_p = 38, .want_s = 38 },
    }) |c| {
        const got = avgDec(.{ .precision = c.p, .scale = c.s });
        try testing.expectEqual(c.want_p, got.precision);
        try testing.expectEqual(c.want_s, got.scale);
        // Never fewer integer digits than the argument's, so no mean of its values overflows.
        try testing.expectEqual(c.p - c.s, got.precision - got.scale);
    }
}

test "DecSum.meanAt rounds half away from zero at up to 4 more digits, past 192-bit products" {
    const Case = struct { sum: i192, n: u64, shift: u8, want: ?i128 };
    const big: i192 = expr_ast.pow10(37);
    for ([_]Case{
        .{ .sum = 0, .n = 0, .shift = 4, .want = null },
        .{ .sum = 10, .n = 4, .shift = 0, .want = 3 }, // 2.5 -> 3
        .{ .sum = -10, .n = 4, .shift = 0, .want = -3 },
        .{ .sum = 10, .n = 4, .shift = 4, .want = 25_000 },
        .{ .sum = 2, .n = 3, .shift = 4, .want = 6_667 }, // 0.66666.. -> 0.6667
        .{ .sum = -2, .n = 3, .shift = 4, .want = -6_667 },
        .{ .sum = 1, .n = 80_000, .shift = 4, .want = 0 }, // 0.0000125 -> 0.0000
        .{ .sum = 1, .n = 20_000, .shift = 4, .want = 1 }, // 0.00005 -> 0.0001
        .{ .sum = -1, .n = 20_000, .shift = 4, .want = -1 },
        // DECIMAL(38, 0) values near 10^37: the sum passes 38 digits, the mean is the input's exact value.
        .{ .sum = 3 * big, .n = 3, .shift = 0, .want = @intCast(big) },
        // DECIMAL(36, 35) mean of 5: at scale 37 it is 5 * 10^37 unscaled, which fits 38 digits.
        .{ .sum = 2 * 5 * expr_ast.pow10(35), .n = 2, .shift = 2, .want = 5 * expr_ast.pow10(37) },
        // 2^64 - 1 values of nearly 10^38 - 1, as the widest sum DecSum holds: |sum| * 10^4 needs 205 bits.
        .{
            .sum = (expr_ast.pow10(38) - 1) * @as(i192, std.math.maxInt(u64)),
            .n = std.math.maxInt(u64),
            .shift = 0,
            .want = expr_ast.pow10(38) - 1,
        },
        .{
            .sum = -(expr_ast.pow10(34) - 1) * @as(i192, std.math.maxInt(u64)),
            .n = std.math.maxInt(u64),
            .shift = 4,
            .want = -(expr_ast.pow10(34) - 1) * 10_000,
        },
    }) |c| {
        var d: DecSum = .{};
        if (c.n > 0) d.add(c.sum, c.n);
        try testing.expectEqual(c.want, try d.meanAt(c.shift));
    }
    // Against the 256-bit definition, round(|sum| * 10^shift / n) with its sign.
    var rng = std.Random.DefaultPrng.init(0xa5_9d_ec);
    const r = rng.random();
    for (0..20_000) |_| {
        const n = r.intRangeAtMost(u64, 1, if (r.boolean()) 1000 else std.math.maxInt(u64));
        const shift = r.intRangeAtMost(u8, 0, 4);
        const bound: i192 = expr_ast.pow10(r.intRangeAtMost(u8, shift + 1, 38));
        // A mean under 10^(38 - shift) - 1, so the shifted, rounded one fits.
        const per: i192 = @divTrunc(bound, @as(i192, expr_ast.pow10(shift))) - 1;
        const sum = r.intRangeLessThan(i192, -per, per) * @as(i192, n) + r.intRangeLessThan(i192, 0, @as(i192, n));
        var d: DecSum = .{};
        d.add(sum, n);
        const num = @as(u256, @abs(sum)) * @as(u256, @intCast(expr_ast.pow10(shift)));
        const q: i256 = @intCast((2 * num + n) / (2 * @as(u256, n)));
        const want: i128 = @intCast(if (sum < 0) -q else q);
        try testing.expectEqual(@as(?i128, want), try d.meanAt(shift));
        if (shift == 0) try testing.expectEqual(@as(?i128, want), try d.mean());
    }
}


pub const Error = error{
    BadAggArg,
    UnsupportedAggType,
    EmptyAggregate,
    DivideByZero,
    ExceededMemoryBudget,
    /// An integer aggregate result (i128 internally) exceeds the i64 range of
    /// the 1-row result *parquet* column. The JSON output path has no such
    /// limit; this only affects `--aggregate -o out.parquet`.
    AggIntTooWide,
} || std.mem.Allocator.Error || expr_eval.Error || filter_eval.Error;

