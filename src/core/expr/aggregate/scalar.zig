const std = @import("std");
const filter_eval = @import("../../filter/eval.zig");
const types = @import("types.zig");
const OutputCol = types.OutputCol;
const AggFunc = types.AggFunc;
const DecSum = types.DecSum;
const AvgState = types.AvgState;
const resultDec = types.resultDec;
const avgDecMean = types.avgDecMean;
const avgMean = types.avgMean;
const avgSum = types.avgSum;
const testing = std.testing;
const expr_ast = @import("../ast.zig");
const schema = @import("../../schema.zig");
const decimal_mod = @import("../../parquet/decimal.zig");
const statistics = @import("../../parquet/statistics.zig");
const AggCall = @import("types.zig").AggCall;
const Error = @import("types.zig").Error;
const accumulator_mod = @import("accumulator.zig");
const Accumulator = accumulator_mod.Accumulator;
const addRowCount = accumulator_mod.addRowCount;
const filter_selection = @import("../../filter/selection.zig");

/// `acc` plus the selected values, in the one order every float sum and avg folds in, whatever the decode slicing,
/// the worker count, the decoder (dictionary or plain) or the target: a row group's rows in 64-row blocks aligned
/// to its first row, the blocks added to `acc` in row order, each block's sum taken alone: over 16 fixed lanes and
/// a fixed pairwise reduction when all 64 rows are selected, else its selected rows in row order. A slice starts on a
/// block boundary (see `consumer.sliceRows`) and carries `acc` on, so cutting a row group changes no bit. The lane
/// count is fixed rather than the target's vector width, so x86_64 and arm64 builds agree too.
pub fn sumBlocksF64(acc: f64, vals: []const f64, sel: *const filter_selection.SelectionVector) f64 {
    var total = acc;
    var w: usize = 0;
    while ((w + 1) * 64 <= vals.len) : (w += 1) {
        const word = sel.mask[w];
        if (word == 0) continue;
        const block = vals[w * 64 ..][0..64];
        total += if (word == std.math.maxInt(u64)) fullBlockSum(block) else partialBlockSum(block, word);
    }
    const tail = vals.len - w * 64;
    if (tail != 0) {
        const word = sel.mask[w] & ((@as(u64, 1) << @intCast(tail)) - 1);
        if (word != 0) total += partialBlockSum(vals[w * 64 ..], word);
    }
    return total;
}

/// `sumBlocksF64` over the rows `sel` selects when their values were gathered, in row order, into `vals`: each block
/// takes the next values it selects, so the bits are those of summing them in place.
pub fn sumBlocksGatheredF64(acc: f64, vals: []const f64, sel: *const filter_selection.SelectionVector) f64 {
    var total = acc;
    var at: usize = 0;
    const full_words = sel.len / 64;
    for (sel.mask[0 .. (sel.len + 63) / 64], 0..) |mask, w| {
        const word = if (w < full_words) mask else mask & ((@as(u64, 1) << @intCast(sel.len % 64)) - 1);
        if (word == 0) continue;
        const n = @popCount(word);
        if (word == std.math.maxInt(u64)) {
            total += fullBlockSum(vals[at..][0..64]);
        } else {
            var sum = vals[at];
            for (vals[at + 1 ..][0 .. n - 1]) |v| sum += v;
            total += sum;
        }
        at += n;
    }
    return total;
}

/// `sumBlocksF64` over a dictionary-coded column, `dict[codes[r]]` for row r: the same blocks, the same bits, without
/// materializing the values.
pub fn sumBlocksCodedF64(
    acc: f64,
    codes: []const u32,
    dict: []const f64,
    sel: *const filter_selection.SelectionVector,
) f64 {
    var total = acc;
    var block: [64]f64 = undefined;
    var w: usize = 0;
    while ((w + 1) * 64 <= codes.len) : (w += 1) {
        const word = sel.mask[w];
        if (word == 0) continue;
        const c = codes[w * 64 ..][0..64];
        if (word == std.math.maxInt(u64)) {
            for (&block, c) |*b, k| b.* = dict[k];
            total += fullBlockSum(&block);
        } else total += partialCodedSum(c, dict, word);
    }
    const tail = codes.len - w * 64;
    if (tail != 0) {
        const word = sel.mask[w] & ((@as(u64, 1) << @intCast(tail)) - 1);
        if (word != 0) total += partialCodedSum(codes[w * 64 ..], dict, word);
    }
    return total;
}

inline fn partialCodedSum(codes: []const u32, dict: []const f64, word: u64) f64 {
    var bits = word;
    var sum = dict[codes[@ctz(bits)]];
    bits &= bits - 1;
    while (bits != 0) : (bits &= bits - 1) sum += dict[codes[@ctz(bits)]];
    return sum;
}

/// `sumBlocksF64` over `n` rows all holding `v`, all selected, in O(n / 64): every full block sums to the same value.
/// Bit for bit what decoding the chunk and summing it gives, so a constant chunk may answer from its statistics.
pub fn sumConstantF64(acc: f64, v: f64, n: u64) f64 {
    var total = acc;
    const block: [64]f64 = @splat(v);
    const b = fullBlockSum(&block);
    for (0..n / 64) |_| total += b;
    if (n % 64 != 0) {
        var t = v;
        for (1..n % 64) |_| t += v;
        total += t;
    }
    return total;
}

const VF16 = @Vector(16, f64);

inline fn fullBlockSum(block: *const [64]f64) f64 {
    var lanes: VF16 = block[0..16].*;
    lanes += @as(VF16, block[16..32].*);
    lanes += @as(VF16, block[32..48].*);
    lanes += @as(VF16, block[48..64].*);
    const l: [16]f64 = lanes;
    var half: [8]f64 = undefined;
    for (&half, 0..) |*h, i| h.* = l[i] + l[i + 8];
    var quarter: [4]f64 = undefined;
    for (&quarter, 0..) |*q, i| q.* = half[i] + half[i + 4];
    return (quarter[0] + quarter[2]) + (quarter[1] + quarter[3]);
}

inline fn partialBlockSum(block: []const f64, word: u64) f64 {
    var bits = word;
    var sum = block[@ctz(bits)];
    bits &= bits - 1;
    while (bits != 0) : (bits &= bits - 1) sum += block[@ctz(bits)];
    return sum;
}

test "regression: sumBlocksF64 gives the same bits however a row group is cut into 64-row-aligned slices" {
    const gpa = std.testing.allocator;
    var prng = std.Random.DefaultPrng.init(0x5eed);
    const rnd = prng.random();
    const n = 64 * 37 + 21;
    const vals = try gpa.alloc(f64, n);
    defer gpa.free(vals);
    for (vals) |*v| {
        const magnitude = std.math.pow(f64, 10, @floatFromInt(rnd.intRangeAtMost(i32, -3, 16)));
        v.* = (rnd.float(f64) - 0.5) * magnitude;
    }
    for ([_]u32{ 1, 2, 7, 100 }) |keep_one_in| {
        var sel = try filter_selection.SelectionVector.init(gpa, n);
        defer sel.deinit();
        for (0..n) |i| sel.set(i, keep_one_in == 1 or rnd.uintLessThan(u32, keep_one_in) == 0);
        const whole = sumBlocksF64(0, vals, &sel);
        for ([_]usize{ 64, 128, 64 * 5, 64 * 36 }) |slice| {
            var acc: f64 = 0;
            var lo: usize = 0;
            while (lo < n) : (lo += slice) {
                const hi = @min(n, lo + slice);
                var part = try filter_selection.SelectionVector.init(gpa, hi - lo);
                defer part.deinit();
                for (lo..hi) |i| part.set(i - lo, sel.isActive(i));
                acc = sumBlocksF64(acc, vals[lo..hi], &part);
            }
            try std.testing.expectEqual(@as(u64, @bitCast(whole)), @as(u64, @bitCast(acc)));
        }
    }
}

test "regression: sumConstantF64 is sumBlocksF64 over the constant's rows" {
    const gpa = std.testing.allocator;
    for ([_]f64{ 0.1, -3.3e-5, 1e300, 7 }) |v| for ([_]u64{ 1, 63, 64, 65, 1000, 64 * 50 }) |n| {
        const vals = try gpa.alloc(f64, n);
        defer gpa.free(vals);
        @memset(vals, v);
        var sel = try filter_selection.SelectionVector.init(gpa, n);
        defer sel.deinit();
        const want = sumBlocksF64(0.25, vals, &sel);
        try std.testing.expectEqual(@as(u64, @bitCast(want)), @as(u64, @bitCast(sumConstantF64(0.25, v, n))));
    };
}

test "regression: sumBlocksCodedF64 is sumBlocksF64 over the column the codes stand for" {
    const gpa = std.testing.allocator;
    var prng = std.Random.DefaultPrng.init(9);
    const rnd = prng.random();
    const dict = [_]f64{ 1e16, -3.25, 0.1, 7e-9, -1e16 };
    const n = 64 * 9 + 13;
    const codes = try gpa.alloc(u32, n);
    defer gpa.free(codes);
    const vals = try gpa.alloc(f64, n);
    defer gpa.free(vals);
    for (codes, vals) |*c, *v| {
        c.* = rnd.uintLessThan(u32, dict.len);
        v.* = dict[c.*];
    }
    for ([_]u32{ 1, 3 }) |keep| {
        var sel = try filter_selection.SelectionVector.init(gpa, n);
        defer sel.deinit();
        for (0..n) |i| sel.set(i, keep == 1 or rnd.uintLessThan(u32, keep) == 0);
        const want = sumBlocksF64(0, vals, &sel);
        const got = sumBlocksCodedF64(0, codes, &dict, &sel);
        try std.testing.expectEqual(@as(u64, @bitCast(want)), @as(u64, @bitCast(got)));
    }
}

test "regression: sumBlocksGatheredF64 sums gathered values in their rows' blocks" {
    const gpa = std.testing.allocator;
    var prng = std.Random.DefaultPrng.init(11);
    const rnd = prng.random();
    const n = 64 * 7 + 29;
    const vals = try gpa.alloc(f64, n);
    defer gpa.free(vals);
    for (vals) |*v| {
        const magnitude = std.math.pow(f64, 10, @floatFromInt(rnd.intRangeAtMost(i32, -3, 16)));
        v.* = (rnd.float(f64) - 0.5) * magnitude;
    }
    for ([_]u32{ 1, 2, 50 }) |keep| {
        var sel = try filter_selection.SelectionVector.init(gpa, n);
        defer sel.deinit();
        for (0..n) |i| sel.set(i, keep == 1 or rnd.uintLessThan(u32, keep) == 0);
        var gathered: std.ArrayList(f64) = .empty;
        defer gathered.deinit(gpa);
        for (vals, 0..) |v, i| if (sel.isActive(i)) try gathered.append(gpa, v);
        const want = sumBlocksF64(0, vals, &sel);
        const got = sumBlocksGatheredF64(0, gathered.items, &sel);
        try std.testing.expectEqual(@as(u64, @bitCast(want)), @as(u64, @bitCast(got)));
    }
}


/// Float min/max order every path shares, as DuckDB, Postgres and Spark order floats: NaN sorts above every number,
/// and of two equal zeros -0 is the lesser. It is a total order, so a min or max is the same whatever order values and
/// partial results meet in: min skips NaN unless every value is NaN, max is NaN if any value is.
pub inline fn minTotal(a: f64, b: f64) f64 {
    if (std.math.isNan(a)) return b;
    if (std.math.isNan(b)) return a;
    if (a < b) return a;
    if (b < a) return b;
    return if (std.math.signbit(a)) a else b;
}

pub inline fn maxTotal(a: f64, b: f64) f64 {
    if (std.math.isNan(a)) return a;
    if (std.math.isNan(b)) return b;
    if (a > b) return a;
    if (b > a) return b;
    return if (std.math.signbit(a)) b else a;
}

/// Fold `v` into a float min (`is_min`) or max accumulator slot.
pub inline fn foldExtremeF(comptime is_min: bool, slot: *?f64, v: f64) void {
    slot.* = if (slot.*) |cur| (if (is_min) minTotal(cur, v) else maxTotal(cur, v)) else v;
}


/// Whether this agg can be answered from row-group statistics
/// alone, given whether the outer query has a `--filter`.
///
/// Eligibility:
///   - func is `count`, `min`, `max`, or constant-column `sum`
///   - no per-agg `FILTER (WHERE ...)` predicate
///   - no outer filter (when present, stats reflect ALL rows
///     including filtered-out ones; trusting them would be wrong)
///
/// `count(*)` is always eligible (num_rows from RG metadata).
/// `count(required_col)` is also answerable from RG row count alone.
/// `count(nullable_col)` needs `null_count`; otherwise the caller
/// falls back to decode. `updateOneFromStats` returns false in that
/// case.
pub fn canStatShortCircuit(call: AggCall, has_outer_filter: bool, trust_stats: bool) bool {
    if (call.func == .count_distinct) return false;
    if (has_outer_filter) return false;
    if (call.where != null) return false;
    // count(*) is answered from RowGroup.num_rows — structural, always exact.
    // Every other short-circuit (min/max/sum, and count(col) via null_count)
    // reads file-written statistics, which real-world writers (e.g. parquet-mr
    // 1.8.2 / Spark) sometimes emit *inaccurately* — yielding a silently wrong
    // answer. Trust those only under --trust-stats; otherwise decode for the
    // true value. Row-group pruning retains the documented corroborated-statistics trust boundary.
    const is_count_star = call.func == .count and call.arg == null;
    if (!trust_stats and !is_count_star) return false;
    // BOOLEAN columns widen to i64 (0/1) at decode time but have no
    // stat-bytes folding path (1-byte stats aren't wired into the int/float
    // stat decoders), so force the decode path for any bool-column agg.
    if (call.arg) |a| {
        // BOOLEAN (1-byte) and INT96 (12-byte) stats aren't wired into the
        // int/float stat-bytes decoders — force the decode path for both.
        if (a == .col_ref and (a.col_ref.physical_type == .BOOLEAN or a.col_ref.physical_type == .INT96)) return false;
    }
    return switch (call.func) {
        .count => true,
        .count_distinct => false,
        // String min/max can't trust row-group stats: `min_value`/`max_value`
        // may be TRUNCATED (a rounded bound, not a present value), and this
        // stats path doesn't consult the `is_*_value_exact` flags — so
        // always decode.
        .min, .max => call.result != .bytes,
        // `sum` is short-circuitable per-RG only when min == max for
        // that RG (constant-column case): the RG contributes
        // num_rows × min to the sum. updateOneFromStats decides per-RG
        // and falls through to decode for non-constant RGs.
        .sum => true,
        .avg => false,
    };
}

/// Check whether `call`'s column reference (if any) has the
/// statistics fields populated for EVERY row group of `metas` — the
/// strict precondition under which `updateOneFromStats` will succeed
/// without falling back to decode.
///
/// `ci` is the leaf-column index this call references on column-typed
/// aggs (min/max/count(col)). For `count(*)` (no `arg`) the result is
/// always true regardless of `ci` — RG num_rows is always present.
///
/// Used by the planner to decide whether to drop a column from the
/// fetch set: if every agg referencing the column has guaranteed
/// stats coverage, the column never needs to be decoded, so it never
/// needs to be fetched. Returns false the moment any RG misses the
/// required stat field, which keeps the runtime path simple — there's
/// no recovery from "I planned to skip this column but stats turned
/// out to be missing" once the fetch was elided.
pub fn statsCoverageComplete(
    call: AggCall,
    metas: []const schema.FileMetaData,
    ci: usize,
    trust_stats: bool,
) bool {
    if (!canStatShortCircuit(call, false, trust_stats)) return false;
    if (call.func == .count and call.arg == null) return true; // count(*)
    const arg_ci = statArgColumn(call) orelse return false;
    if (arg_ci != ci) return false;

    for (metas) |m| {
        for (m.row_groups.items) |rg| {
            if (ci >= rg.columns.items.len) return false;
            const cm = rg.columns.items[ci].meta_data orelse return false;
            switch (call.func) {
                .count => {
                    const levels = columnLevelsForStats(&m, &cm) orelse return false;
                    if (levels.max_rep > 0) return false;
                    _ = statPresentCount(&rg, ci, &cm, &m) orelse return false;
                },
                // The same plan `updateOneFromStats` folds from: any row group it would decode instead keeps the
                // column in the fetch set. Sum needs every row group constant (min == max) to drop the column.
                .min, .max, .sum => if (planStatFold(call, &rg, ci, &m) == null) return false,
                else => unreachable,
            }
        }
    }
    return true;
}

/// `a + b` for counts a footer claims: each file's is bounded (`metadata.max_row_count`), their sum across files is
/// not. Past i64, the type count() answers in, it is an error rather than a count that cannot be written.


/// Update one accumulator from a row group's metadata WITHOUT
/// decoding. Returns true on success, false if the column's stats
/// aren't populated (caller falls back to the decode path).
///
/// `col_idx_lookup` is the same `column_lookup` used by the decode
/// path: indexed by the AST col_idx, returns the leaf index in
/// `rg.columns[]`. For count(*) this isn't needed (no arg).
pub fn updateOneFromStats(
    state: *Accumulator,
    call: AggCall,
    rg: *const schema.RowGroup,
    file_meta: *const schema.FileMetaData,
) !bool {
    switch (call.func) {
        .count_distinct => return false,
        .count => {
            // count(*): every row counts, regardless of null content.
            if (call.arg == null) {
                state.count = try addRowCount(state.count, @intCast(rg.num_rows));
                return true;
            }
            // count(col): strict SQL semantics — exclude nulls. Use
            // null_count from stats if available (cheap, exact); fall
            // back to decode when stats are missing or don't carry
            // null_count.
            const col_idx = statArgColumn(call) orelse return false;
            if (col_idx >= rg.columns.items.len) return false;
            const cm = rg.columns.items[col_idx].meta_data orelse return false;
            const present = statPresentCount(rg, col_idx, &cm, file_meta) orelse return false;
            state.count = try addRowCount(state.count, @intCast(present));
            return true;
        },
        .min, .max => {
            // Only a bare col_ref is stat-eligible; an expression like `cost * qty` decodes.
            const plan = planStatFold(call, rg, statArgColumn(call) orelse return false, file_meta) orelse return false;
            // A chunk with no non-null values contributes nothing, whatever bounds its writer left behind.
            const cm = rg.columns.items[statArgColumn(call).?].meta_data.?;
            if (statPresentCount(rg, statArgColumn(call).?, &cm, file_meta) == 0) return true;
            const bytes = if (call.func == .min) plan.bounds.min else plan.bounds.max;
            switch (plan.lane) {
                // DECIMAL stat bytes are in the physical wire format: decode them through the same
                // byte → i128 → f64-with-scale pipeline the value path uses, then fold into .min_f / .max_f.
                .decimal => |k| try foldDecimalStatBytes(state, call, k, bytes),
                .int, .float => try foldStatBytes(state, call, plan.physical, bytes),
            }
            return true;
        },
        .sum => {
            // Constant-column case: when this RG's min == max, every value in it equals that constant, so it
            // contributes present × min. A non-constant RG decodes; the constant ones still fold from stats.
            const plan = planStatFold(call, rg, statArgColumn(call) orelse return false, file_meta) orelse return false;
            if (plan.present == 0) return true; // nothing to add; an all-null sum stays NULL
            switch (plan.lane) {
                .decimal => |k| state.sum_d.add(
                    @as(i192, decimal_mod.statToI128(plan.bounds.min, k).?) * plan.present,
                    @intCast(plan.present),
                ),
                .int, .float => {
                    try foldConstantSumStatBytes(state, plan.physical, plan.bounds.min, plan.present);
                    if (state.* == .sum_i) state.sum_i.? += @as(i128, call.offset) * plan.present;
                },
            }
            return true;
        },
        .avg => return false, // never short-circuitable
    }
}

const StatLane = union(enum) { decimal: decimal_mod.Kind, int, float };

/// Everything `updateOneFromStats` needs to answer `call` from one row group's statistics.
const StatFold = struct {
    lane: StatLane,
    physical: schema.Type,
    bounds: statistics.Bounds,
    /// Non-null rows; only computed (and required) for sum.
    present: i64 = 0,
};

/// Whether, and how, `call`'s column can be folded from this row group's statistics; null means decode it. The one
/// decision both `updateOneFromStats` and `statsCoverageComplete` act on: if coverage accepted a row group the fold
/// then declined, the column would already be dropped from the fetch set with nothing left to decode.
fn planStatFold(
    call: AggCall,
    rg: *const schema.RowGroup,
    col_idx: usize,
    file_meta: *const schema.FileMetaData,
) ?StatFold {
    if (col_idx >= rg.columns.items.len) return null;
    const cm = rg.columns.items[col_idx].meta_data orelse return null;
    const levels = columnLevelsForStats(file_meta, &cm) orelse return null;
    if (levels.max_rep > 0) return null;
    // Bounds in the column's own order: the deprecated pair only where signed is that order. Every fold but count(*)
    // runs under `--trust-stats` alone (`canStatShortCircuit`), which trusts plausible statistics, but never answers
    // with a bound the writer marked inexact (cut): that one only prunes.
    const vetted = statistics.chunkStats(rg, col_idx, file_meta, .plausible);
    const bounds = vetted.bounds orelse return null;
    switch (call.func) {
        .min => if (!vetted.min_exact) return null,
        .max => if (!vetted.max_exact) return null,
        else => if (!vetted.min_exact or !vetted.max_exact) return null,
    }
    const lane = statFoldLane(call, cm.type, file_meta.getColumnSchema(cm.path_in_schema.items)) orelse return null;
    // Bounds leave NaN out. A min skips NaN anyway (and an all-NaN chunk has no bounds), but a max or a constant sum
    // can take them only when the chunk is known to hold none.
    if (lane == .float and call.func != .min) {
        const nan_free = if (cm.statistics) |st| st.nan_count == 0 else false;
        if (!nan_free) return null;
    }
    // Nor do bounds prove which zero occurs: writers record -0 and +0 as a chunk's bounds whenever it holds any zero.
    // A zero answer decodes, so no zero of the other sign is invented.
    if (lane == .float) {
        const b = if (call.func == .max) bounds.max else bounds.min;
        const v: ?f64 = switch (cm.type) {
            .FLOAT => if (b.len >= 4) @as(f64, @as(f32, @bitCast(std.mem.readInt(u32, b[0..4], .little)))) else null,
            .DOUBLE => if (b.len >= 8) @as(f64, @bitCast(std.mem.readInt(u64, b[0..8], .little))) else null,
            else => null,
        };
        if (v == null or v.? == 0) return null;
    }
    var plan: StatFold = .{ .lane = lane, .physical = cm.type, .bounds = bounds };
    if (call.func == .sum) {
        if (!std.mem.eql(u8, bounds.min, bounds.max)) return null;
        // REQUIRED columns can use the row count; nullable ones need null_count, or null slots would count as the
        // constant.
        plan.present = statPresentCount(rg, col_idx, &cm, file_meta) orelse return null;
        if (lane == .decimal and decimal_mod.statToI128(bounds.min, lane.decimal) == null) return null;
        // A float constant sums block by block as its rows would (`sumConstantF64`), which needs every row present:
        // a NULL moves the rows after it within their blocks.
        if (lane == .float and plan.present != rg.num_rows) return null;
    }
    return plan;
}

/// The stat-bytes lane for `call`, which must match the accumulator `call.result` gives it.
fn statFoldLane(call: AggCall, physical: schema.Type, elem_opt: ?schema.SchemaElement) ?StatLane {
    const accum_is_float = call.result == .f64;
    if (elem_opt) |se| {
        // Unsigned bounds are unsigned-ordered; the int fold would read 0xFF..FF as -1. Decode instead, which
        // zero-extends (<=32) or folds as u64.
        if (schema.isUnsignedInt(se)) return null;
        if (decimal_mod.kindFromSchema(&se)) |k| return if (call.result == .dec) .{ .decimal = k } else null;
        // FLOAT16 decodes to the f64 lane, but its bounds are 2-byte halves no fold here reads.
        if (schema.isFloat16(se)) return null;
    }
    return switch (physical) {
        .INT32, .INT64 => if (accum_is_float) null else .int,
        .FLOAT, .DOUBLE => if (accum_is_float) .float else null,
        else => null,
    };
}

fn statArgColumn(call: AggCall) ?usize {
    const arg = call.arg orelse return null;
    return switch (arg) {
        .col_ref => |c| c.col_idx,
        else => null,
    };
}

fn columnLevelsForStats(file_meta: *const schema.FileMetaData, cm: *const schema.ColumnMetaData) ?schema.Levels {
    _ = file_meta.getColumnSchema(cm.path_in_schema.items) orelse return null;
    return file_meta.getColumnLevels(cm.path_in_schema.items);
}

/// Non-null rows of chunk `col_idx` (meta `cm`), from its null_count. A count `chunkStats` refuses, one outside the
/// row group's rows among them, is no count: `num_rows - nc` would answer more rows than there are (a negative count)
/// or overflow (minInt). The column is decoded instead.
pub fn statPresentCount(
    rg: *const schema.RowGroup,
    col_idx: usize,
    cm: *const schema.ColumnMetaData,
    file_meta: *const schema.FileMetaData,
) ?i64 {
    const levels = columnLevelsForStats(file_meta, cm) orelse return null;
    if (levels.max_rep > 0) return null;
    if (statistics.chunkStats(rg, col_idx, file_meta, .plausible).null_count) |nc| return rg.num_rows - nc;
    return if (levels.max_def == 0) rg.num_rows else null;
}

/// Decode a non-DECIMAL stat min/max bytes slice as the column's
/// physical type and add `present × value` into the accumulator.
fn foldConstantSumStatBytes(
    state: *Accumulator,
    parquet_type: schema.Type,
    bytes: []const u8,
    present: i64,
) !void {
    switch (state.*) {
        .sum_i => |*s| {
            const v: i64 = switch (parquet_type) {
                .INT32 => blk: {
                    if (bytes.len < 4) return error.BadAggArg;
                    break :blk @as(i64, std.mem.readInt(i32, bytes[0..4], .little));
                },
                .INT64 => blk: {
                    if (bytes.len < 8) return error.BadAggArg;
                    break :blk std.mem.readInt(i64, bytes[0..8], .little);
                },
                else => return error.UnsupportedAggType,
            };
            s.* = (s.* orelse 0) + @as(i128, v) * @as(i128, present);
        },
        .sum_f => |*s| {
            const v: f64 = switch (parquet_type) {
                .FLOAT => blk: {
                    if (bytes.len < 4) return error.BadAggArg;
                    const u = std.mem.readInt(u32, bytes[0..4], .little);
                    break :blk @as(f64, @as(f32, @bitCast(u)));
                },
                .DOUBLE => blk: {
                    if (bytes.len < 8) return error.BadAggArg;
                    const u = std.mem.readInt(u64, bytes[0..8], .little);
                    break :blk @as(f64, @bitCast(u));
                },
                else => return error.UnsupportedAggType,
            };
            s.* = sumConstantF64(s.* orelse 0, v, @intCast(present));
        },
        else => return error.UnsupportedAggType,
    }
}

/// Fold a DECIMAL column's stat min/max bytes, exactly, into a .min_d / .max_d accumulator.
fn foldDecimalStatBytes(
    state: *Accumulator,
    call: AggCall,
    kind: decimal_mod.Kind,
    bytes: []const u8,
) !void {
    const v = decimal_mod.statToI128(bytes, kind) orelse return switch (kind.physical) {
        .INT32, .INT64, .FIXED_LEN_BYTE_ARRAY, .BYTE_ARRAY => error.BadAggArg,
        else => error.UnsupportedAggType,
    };
    switch (state.*) {
        .min_d => |*m| m.* = if (m.*) |cur| @min(cur, v) else v,
        .max_d => |*m| m.* = if (m.*) |cur| @max(cur, v) else v,
        else => return error.UnsupportedAggType,
    }
    _ = call;
}

fn foldStatBytes(
    state: *Accumulator,
    call: AggCall,
    parquet_type: schema.Type,
    bytes: []const u8,
) !void {
    // Stats are stored as little-endian byte strings of the typed
    // value. Decode + fold into the accumulator.
    switch (state.*) {
        .min_i, .max_i => |*slot| {
            const v: i64 = switch (parquet_type) {
                .INT32 => blk: {
                    if (bytes.len < 4) return error.BadAggArg;
                    break :blk @as(i64, std.mem.readInt(i32, bytes[0..4], .little));
                },
                .INT64 => blk: {
                    if (bytes.len < 8) return error.BadAggArg;
                    break :blk std.mem.readInt(i64, bytes[0..8], .little);
                },
                else => return error.UnsupportedAggType,
            };
            slot.* = if (slot.*) |current|
                (if (call.func == .min) @min(current, v) else @max(current, v))
            else
                v;
        },
        .min_f, .max_f => |*slot| {
            const v: f64 = switch (parquet_type) {
                .FLOAT => blk: {
                    if (bytes.len < 4) return error.BadAggArg;
                    const u = std.mem.readInt(u32, bytes[0..4], .little);
                    const f: f32 = @bitCast(u);
                    break :blk @as(f64, f);
                },
                .DOUBLE => blk: {
                    if (bytes.len < 8) return error.BadAggArg;
                    const u = std.mem.readInt(u64, bytes[0..8], .little);
                    break :blk @as(f64, @bitCast(u));
                },
                else => return error.UnsupportedAggType,
            };
            if (call.func == .min) foldExtremeF(true, slot, v) else foldExtremeF(false, slot, v);
        },
        else => return error.UnsupportedAggType,
    }
}


test "canStatShortCircuit eligibility rules" {
    // count/min/max with no FILTER and no outer filter → eligible
    const a_count: AggCall = .{ .func = .count, .arg = null, .where = null, .alias = "n", .result = .i64 };
    const a_min_ref: expr_ast.Expr = .{ .col_ref = .{ .col_idx = 0, .physical_type = .INT64, .expr_type = .i64 } };
    const a_min: AggCall = .{ .func = .min, .arg = a_min_ref, .where = null, .alias = "lo", .result = .i64 };
    // count(*) is answered from num_rows — eligible regardless of trust_stats.
    try testing.expect(canStatShortCircuit(a_count, false, false));
    try testing.expect(canStatShortCircuit(a_count, false, true));
    // min reads file stats → eligible ONLY under --trust-stats.
    try testing.expect(canStatShortCircuit(a_min, false, true));
    try testing.expect(!canStatShortCircuit(a_min, false, false));

    // Outer filter present → ineligible (stats reflect ALL rows)
    try testing.expect(!canStatShortCircuit(a_count, true, true));
    try testing.expect(!canStatShortCircuit(a_min, true, true));

    // Per-agg WHERE → ineligible
    const a_min_where: AggCall = .{
        .func = .min,
        .arg = a_min_ref,
        .where = .{ .int64 = .{ .col_idx = 0, .op = .GtEq, .value = 0 } },
        .alias = "lo",
        .result = .i64,
    };
    try testing.expect(!canStatShortCircuit(a_min_where, false, true));

    // sum is eligible at this stage: per-RG, updateOneFromStats
    // returns true only when min == max (constant-column case) and
    // returns false otherwise to fall through to decode. avg is
    // still never eligible (needs values to compute a meaningful
    // weighted answer).
    const a_sum: AggCall = .{ .func = .sum, .arg = a_min_ref, .where = null, .alias = "s", .result = .i64 };
    const a_avg: AggCall = .{ .func = .avg, .arg = a_min_ref, .where = null, .alias = "m", .result = .avg_f64 };
    // sum reads stats → eligible only under --trust-stats; avg never.
    try testing.expect(canStatShortCircuit(a_sum, false, true));
    try testing.expect(!canStatShortCircuit(a_sum, false, false));
    try testing.expect(!canStatShortCircuit(a_avg, false, true));

    // Outer filter / per-agg WHERE still disqualify sum.
    try testing.expect(!canStatShortCircuit(a_sum, true, true));
    const a_sum_where: AggCall = .{
        .func = .sum,
        .arg = a_min_ref,
        .where = .{ .int64 = .{ .col_idx = 0, .op = .GtEq, .value = 0 } },
        .alias = "s",
        .result = .i64,
    };
    try testing.expect(!canStatShortCircuit(a_sum_where, false, true));
}


pub fn finalize(
    arena: std.mem.Allocator,
    call: AggCall,
    state: Accumulator,
) Error![]OutputCol {
    return switch (call.func) {
        .count => try emitOneI64(arena, call.alias, @intCast(state.count), false),
        .count_distinct => try emitOneI64(arena, call.alias, @intCast(state.distinct.count()), false),
        .sum => switch (call.result) {
            .i64 => try emitOneI64(arena, call.alias, state.sum_i, true),
            .f64 => try emitOneF64(arena, call.alias, state.sum_f),
            .dec => try emitOneDec(arena, call.alias, try state.sum_d.result(), resultDec(call)),
            .bytes, .avg_f64, .avg_dec => unreachable,
        },
        .min => switch (call.result) {
            .i64 => try emitOneI64(arena, call.alias, u64Bits(call, state.min_i), true),
            .f64 => try emitOneF64(arena, call.alias, state.min_f),
            .bytes => try emitOneString(arena, call.alias, state.stringValue()),
            .dec => try emitOneDec(arena, call.alias, state.min_d, resultDec(call)),
            .avg_f64, .avg_dec => unreachable,
        },
        .max => switch (call.result) {
            .i64 => try emitOneI64(arena, call.alias, u64Bits(call, state.max_i), true),
            .f64 => try emitOneF64(arena, call.alias, state.max_f),
            .bytes => try emitOneString(arena, call.alias, state.stringValue()),
            .dec => try emitOneDec(arena, call.alias, state.max_d, resultDec(call)),
            .avg_f64, .avg_dec => unreachable,
        },
        .avg => if (call.state)
            (if (call.result == .avg_dec)
                try emitAvgDecPair(arena, call.alias, state.avg_d, resultDec(call))
            else
                try emitAvgPair(arena, call.alias, state.avg))
        else if (call.result == .avg_dec)
            try emitOneDec(arena, call.alias, try avgDecMean(call, state.avg_d), resultDec(call))
        else
            try emitOneF64(arena, call.alias, avgMean(state.avg)),
    };
}

/// A one-row exact DECIMAL column, typed DECIMAL(`spec`) at the physical width its precision takes.
fn emitOneDec(arena: std.mem.Allocator, name: []const u8, v: ?i128, spec: expr_ast.Dec) Error![]OutputCol {
    const out = try arena.alloc(OutputCol, 1);
    out[0] = .{
        .name = name,
        .col = .{ .i128 = try oneRow(i128, arena, v, 0, true) },
        .parquet_type = spec.physical(),
        .nullable = true,
        .dec = spec,
    };
    return out;
}

/// avg_state over a DECIMAL as `<alias>__sum` (DECIMAL(38, s), exact) and `<alias>__count`, which re-aggregate
/// exactly.
fn emitAvgDecPair(arena: std.mem.Allocator, name: []const u8, st: DecSum, spec: expr_ast.Dec) Error![]OutputCol {
    const sum_name = try std.fmt.allocPrint(arena, "{s}__sum", .{name});
    const count_name = try std.fmt.allocPrint(arena, "{s}__count", .{name});
    const sum = try emitOneDec(arena, sum_name, try st.result(), spec.sum());
    const count = try emitOneI64(arena, count_name, st.n, false);
    const out = try arena.alloc(OutputCol, 2);
    out[0] = sum[0];
    out[1] = count[0];
    return out;
}

/// min/max of an unsigned 64-bit column as the raw bits it is stored as, so an extremum of 2^63 or more still fits the
/// INT64 column; the writer annotates it unsigned.
fn u64Bits(call: AggCall, v: ?i128) ?i128 {
    const x = v orelse return null;
    const unsigned = if (call.arg) |a| a == .col_ref and a.col_ref.unsigned_64 else false;
    return if (unsigned) u64Lane(x) else x;
}

/// The INT64 lane's form of an unsigned 64-bit value `x` (0 <= x < 2^64): its low 64 bits. By truncation, never an
/// `@intCast` to u64: that narrowing of an unwrapped `?i128`, bit-cast and widened back, was folded to `x` itself by
/// the optimizer (ReleaseFast and ReleaseSafe; Debug was right), so a value of 2^63 and up reached an INT64 column
/// unconverted. No range assertion either: it would hand the optimizer the same fact.
pub fn u64Lane(x: i128) i64 {
    return @bitCast(@as(u64, @truncate(@as(u128, @bitCast(x)))));
}

/// A one-row output column. NULL is a placeholder value under def level 0, so it needs `nullable`.
fn oneRow(
    comptime T: type,
    arena: std.mem.Allocator,
    v: ?T,
    placeholder: T,
    nullable: bool,
) Error!filter_eval.ColumnT(T) {
    std.debug.assert(nullable or v != null);
    const slot = try arena.alloc(T, 1);
    slot[0] = v orelse placeholder;
    if (!nullable) return .{ .values = slot };
    if (v != null) return .{ .values = slot, .max_def = 1 };
    const def = try arena.alloc(u32, 1);
    def[0] = 0;
    return .{ .values = slot, .def_levels = def, .max_def = 1, .has_nulls = true };
}

fn emitOneI64(arena: std.mem.Allocator, name: []const u8, v: ?i128, nullable: bool) Error![]OutputCol {
    // The result parquet column is INT64; a value outside i64 range (an
    // overflowing sum or an unsigned-64 extremum) can't be stored. Error
    // rather than silently wrap — the JSON path carries the full i128.
    const narrowed: ?i64 = if (v) |x| std.math.cast(i64, x) orelse return error.AggIntTooWide else null;
    const out = try arena.alloc(OutputCol, 1);
    out[0] = .{
        .name = name,
        .col = .{ .i64 = try oneRow(i64, arena, narrowed, 0, nullable) },
        .parquet_type = .INT64,
        .nullable = nullable,
    };
    return out;
}

fn emitOneString(arena: std.mem.Allocator, name: []const u8, v: ?[]const u8) Error![]OutputCol {
    const out = try arena.alloc(OutputCol, 1);
    out[0] = .{
        .name = name,
        .col = .{ .string = try oneRow([]const u8, arena, v, "", true) },
        .parquet_type = .BYTE_ARRAY,
        .nullable = true,
    };
    return out;
}

fn emitOneF64(arena: std.mem.Allocator, name: []const u8, v: ?f64) Error![]OutputCol {
    const out = try arena.alloc(OutputCol, 1);
    out[0] = .{
        .name = name,
        .col = .{ .f64 = try oneRow(f64, arena, v, 0, true) },
        .parquet_type = .DOUBLE,
        .nullable = true,
    };
    return out;
}

fn emitAvgPair(arena: std.mem.Allocator, name: []const u8, st: AvgState) Error![]OutputCol {
    // Two columns: <alias>__sum (DOUBLE) and <alias>__count (INT64).
    // Caller running `zpq query --aggregate "avg_state(...) AS x"` over
    // the tier-1 outputs gets sum(x__sum) / sum(x__count) — algebraically
    // correct re-aggregation. With no values the sum is NULL like sum()'s,
    // and the count 0 like count()'s.
    const sum_name = try std.fmt.allocPrint(arena, "{s}__sum", .{name});
    const count_name = try std.fmt.allocPrint(arena, "{s}__count", .{name});
    const sum = try emitOneF64(arena, sum_name, avgSum(st));
    const count = try emitOneI64(arena, count_name, st.count, false);
    const out = try arena.alloc(OutputCol, 2);
    out[0] = sum[0];
    out[1] = count[0];
    return out;
}

test "boundary: an unsigned 64-bit value's INT64 lane is its low bits, as one-row min/max write it" {
    // Run in ReleaseFast too (see `u64Lane`): the narrowing this replaced answered wrong only when optimized.
    const U = struct { v: i128, lane: i64 };
    for ([_]U{
        .{ .v = 0, .lane = 0 },
        .{ .v = 1, .lane = 1 },
        .{ .v = std.math.maxInt(i64), .lane = std.math.maxInt(i64) },
        .{ .v = @as(i128, std.math.maxInt(i64)) + 1, .lane = std.math.minInt(i64) },
        .{ .v = std.math.maxInt(u64), .lane = -1 },
    }) |u| {
        try std.testing.expectEqual(u.lane, u64Lane(u.v));
        var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
        defer arena.deinit();
        const unsigned_arg: expr_ast.Expr = .{ .col_ref = .{ .col_idx = 0, .physical_type = .INT64, .expr_type = .i64, .unsigned_64 = true } };
        for ([_]AggFunc{ .min, .max }) |func| {
            const call: AggCall = .{ .func = func, .arg = unsigned_arg, .where = null, .alias = "m", .result = .i64 };
            var state = Accumulator.init(call);
            if (func == .min) state.min_i = u.v else state.max_i = u.v;
            const cols = try finalize(arena.allocator(), call, state);
            try std.testing.expectEqual(u.lane, cols[0].col.i64.values[0]);
        }
    }
    // A signed column's extremes pass through unchanged, minInt and -1 included.
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const signed_arg: expr_ast.Expr = .{ .col_ref = .{ .col_idx = 0, .physical_type = .INT64, .expr_type = .i64 } };
    for ([_]i64{ std.math.minInt(i64), -1, 0, std.math.maxInt(i64) }) |v| {
        const call: AggCall = .{ .func = .max, .arg = signed_arg, .where = null, .alias = "m", .result = .i64 };
        var state = Accumulator.init(call);
        state.max_i = v;
        const cols = try finalize(arena.allocator(), call, state);
        try std.testing.expectEqual(v, cols[0].col.i64.values[0]);
    }
}

