//! Aggregate kernels — sum / count / min / max / avg with optional
//! per-aggregate `FILTER (WHERE pred)` clause.
//!
//! Architectural shape:
//!
//!   AggCall — one call site (e.g. `sum(cost) FILTER (WHERE foo='bar') AS total`)
//!   Accumulator — running state for one AggCall, updated per-RG
//!   updateOne(state, batch, sel, call) — per-RG update path
//!   finalize(state, call) — fold accumulator into a Batch.Column for emit
//!
//! Conditional aggs (the FILTER clause) compose with the outer query
//! `--filter`: the per-agg SelectionVector starts as a copy of the
//! query's surviving rows, then ANDs in the per-agg predicate.
//!
//! The output of an aggregate workload is a 1-row parquet file. For
//! sum/count/min/max we emit one column per agg. For avg we emit two
//! columns (`<alias>__sum`, `<alias>__count`) so downstream re-
//! aggregation across N files preserves the algebra: a tier-2
//! re-aggregate is `sum(<alias>__sum) / sum(<alias>__count)`. You
//! cannot average means; you can sum sums and counts and divide once
//! at the end.

const std = @import("std");
const schema = @import("../schema.zig");
const filter_ast = @import("../filter/ast.zig");
const filter_eval = @import("../filter/eval.zig");
const filter_selection = @import("../filter/selection.zig");
const expr_ast = @import("ast.zig");
const expr_eval = @import("eval.zig");
const decimal_mod = @import("../parquet/decimal.zig");
const statistics = @import("../parquet/statistics.zig");

pub const Error = error{
    BadAggArg,
    UnsupportedAggType,
    EmptyAggregate,
    DivideByZero,
    /// An integer aggregate result (i128 internally) exceeds the i64 range of
    /// the 1-row result *parquet* column. The JSON output path has no such
    /// limit; this only affects `--aggregate -o out.parquet`.
    AggIntTooWide,
} || std.mem.Allocator.Error || expr_eval.Error || filter_eval.Error;

pub const AggFunc = enum {
    count,
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
};

/// What an aggregate emits in the final 1-row parquet:
///   - single i64: count, sum/min/max of integer columns
///   - single f64: sum/min/max of float columns
///   - two-column avg: `<alias>__sum: f64` + `<alias>__count: i64`
pub const ResultShape = enum {
    i64,
    f64,
    avg_f64,
    /// String (BYTE_ARRAY/UTF8) min/max — bytewise/unsigned order.
    bytes,
};

/// Accumulator state. One per AggCall, lives across all RGs of all
/// input files. Tagged union so the dispatcher picks the right
/// kernel at update time without re-checking AggFunc + arg type.
pub const Accumulator = union(enum) {
    count: u64,
    /// Integer sum, i128. Keeps i64-column sums precise on big datasets
    /// (10^9 rows × 10^9 values = 10^18) and carries unsigned-64 sums and
    /// i64-overflowing sums (e.g. wide DELTA columns) without wrapping. The
    /// JSON output emits the full i128; only the 1-row result *parquet* column
    /// is INT64 and errors `AggIntTooWide` past i64 range.
    /// `null` until a non-null value is folded in: SQL's sum over no values is
    /// NULL, not 0.
    sum_i: ?i128,
    sum_f: ?f64,
    /// Min/max — `null` means no values seen yet. Distinguishes
    /// "all rows filtered out" from "min was 0". i128 so an unsigned-64
    /// extremum (up to 2^64-1) is representable; signed i64 values widen in.
    min_i: ?i128,
    min_f: ?f64,
    max_i: ?i128,
    max_f: ?f64,
    /// String (BYTE_ARRAY/UTF8) min/max — bytewise/unsigned comparison
    /// (Parquet's TYPE_DEFINED order; == DuckDB's binary collation). The
    /// slice is OWNED: dup'd into the persist allocator on each new winner
    /// so it outlives the per-RG decode arena and the cross-worker merge.
    /// `null` = no value seen yet.
    min_bytes: ?[]const u8,
    max_bytes: ?[]const u8,
    /// Avg always lands as f64. Internal sum is f64 even for int
    /// input — for huge int sums this loses precision past 2^53;
    /// document the caveat. Decimal-precision avg is a future slice.
    avg: AvgState,

    pub fn init(call: AggCall) Accumulator {
        return switch (call.func) {
            .count => .{ .count = 0 },
            .sum => switch (call.result) {
                .i64 => .{ .sum_i = null },
                .f64 => .{ .sum_f = null },
                .bytes, .avg_f64 => unreachable,
            },
            .min => switch (call.result) {
                .i64 => .{ .min_i = null },
                .f64 => .{ .min_f = null },
                .bytes => .{ .min_bytes = null },
                .avg_f64 => unreachable,
            },
            .max => switch (call.result) {
                .i64 => .{ .max_i = null },
                .f64 => .{ .max_f = null },
                .bytes => .{ .max_bytes = null },
                .avg_f64 => unreachable,
            },
            .avg => .{ .avg = .{ .sum = 0, .count = 0 } },
        };
    }

    /// Combine `src` into `dst`. Both must be the same variant — used
    /// to merge per-thread / per-file accumulators back into a single
    /// final result after a parallel multi-file scan. A null min/max
    /// means "no values seen"; merging skips it.
    pub fn merge(dst: *Accumulator, src: Accumulator, allocator: std.mem.Allocator) void {
        switch (dst.*) {
            .count => |*c| c.* += src.count,
            .sum_i => |*s| if (src.sum_i) |sv| {
                s.* = (s.* orelse 0) + sv;
            },
            .sum_f => |*s| if (src.sum_f) |sv| {
                s.* = (s.* orelse 0) + sv;
            },
            .min_i => |*m| if (src.min_i) |sv| {
                if (m.* == null or sv < m.*.?) m.* = sv;
            },
            .min_f => |*m| if (src.min_f) |sv| {
                if (m.* == null or sv < m.*.?) m.* = sv;
            },
            .max_i => |*m| if (src.max_i) |sv| {
                if (m.* == null or sv > m.*.?) m.* = sv;
            },
            .max_f => |*m| if (src.max_f) |sv| {
                if (m.* == null or sv > m.*.?) m.* = sv;
            },
            // Pointer copy: both src and the merged dst hold slices dup'd
            // into the shared persist allocator. We free the loser to prevent leaks.
            .min_bytes => |*m| if (src.min_bytes) |sv| {
                if (m.* == null) {
                    m.* = sv;
                } else {
                    const ord = std.mem.order(u8, sv, m.*.?);
                    if (ord == .lt) {
                        allocator.free(m.*.?);
                        m.* = sv;
                    } else {
                        allocator.free(sv);
                    }
                }
            },
            .max_bytes => |*m| if (src.max_bytes) |sv| {
                if (m.* == null) {
                    m.* = sv;
                } else {
                    const ord = std.mem.order(u8, sv, m.*.?);
                    if (ord == .gt) {
                        allocator.free(m.*.?);
                        m.* = sv;
                    } else {
                        allocator.free(sv);
                    }
                }
            },
            .avg => |*a| {
                a.sum += src.avg.sum;
                a.count += src.avg.count;
            },
        }
    }
};

pub const AvgState = struct {
    sum: f64,
    count: u64,
};

pub const ResolveError = error{ BadAggArg, UnsupportedAggType };

/// Determine the result shape of an agg call from its function and
/// arg type. Called at parse time so the AST carries the result
/// shape forward to accumulator init + finalization.
pub fn resolveResult(func: AggFunc, arg: ?expr_ast.Expr) ResolveError!ResultShape {
    return switch (func) {
        .count => .i64,
        .avg => .avg_f64,
        .sum => blk: {
            const e = arg orelse return error.BadAggArg;
            break :blk switch (e.typeOf()) {
                .i64 => .i64,
                .f64 => .f64,
                .str => error.UnsupportedAggType, // sum of strings is meaningless
            };
        },
        .min, .max => blk: {
            const e = arg orelse return error.BadAggArg;
            break :blk switch (e.typeOf()) {
                .i64 => .i64,
                .f64 => .f64,
                .str => .bytes, // bytewise/unsigned min/max over BYTE_ARRAY
            };
        },
    };
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
    if (has_outer_filter) return false;
    if (call.where != null) return false;
    // count(*) is answered from RowGroup.num_rows — structural, always exact.
    // Every other short-circuit (min/max/sum, and count(col) via null_count)
    // reads file-written statistics, which real-world writers (e.g. parquet-mr
    // 1.8.2 / Spark) sometimes emit *inaccurately* — yielding a silently wrong
    // answer. Trust those only under --trust-stats; otherwise decode for the
    // true value (row-group pruning still uses stats — that path is safe
    // because it only ever skips provably-non-matching groups).
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
                    _ = statPresentCount(&rg, &cm, cm.statistics, &m) orelse return false;
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
        .count => {
            // count(*): every row counts, regardless of null content.
            if (call.arg == null) {
                state.count += @intCast(rg.num_rows);
                return true;
            }
            // count(col): strict SQL semantics — exclude nulls. Use
            // null_count from stats if available (cheap, exact); fall
            // back to decode when stats are missing or don't carry
            // null_count.
            const col_idx = statArgColumn(call) orelse return false;
            if (col_idx >= rg.columns.items.len) return false;
            const cm = rg.columns.items[col_idx].meta_data orelse return false;
            const present = statPresentCount(rg, &cm, cm.statistics, file_meta) orelse return false;
            state.count += @intCast(present);
            return true;
        },
        .min, .max => {
            // Only a bare col_ref is stat-eligible; an expression like `cost * qty` decodes.
            const plan = planStatFold(call, rg, statArgColumn(call) orelse return false, file_meta) orelse return false;
            // A chunk with no non-null values contributes nothing, whatever bounds its writer left behind.
            const cm = rg.columns.items[statArgColumn(call).?].meta_data.?;
            if (statPresentCount(rg, &cm, cm.statistics, file_meta) == 0) return true;
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
                .decimal => |k| state.sum_f = (state.sum_f orelse 0) + decimal_mod.statToF64(plan.bounds.min, k).? *
                    @as(f64, @floatFromInt(plan.present)),
                .int, .float => try foldConstantSumStatBytes(state, plan.physical, plan.bounds.min, plan.present),
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
    // Bounds in the column's own order: the deprecated pair only where signed is that order.
    const bounds = statistics.chunkBounds(rg, col_idx, file_meta) orelse return null;
    const lane = statFoldLane(call, cm.type, file_meta.getColumnSchema(cm.path_in_schema.items)) orelse return null;
    var plan: StatFold = .{ .lane = lane, .physical = cm.type, .bounds = bounds };
    if (call.func == .sum) {
        if (!std.mem.eql(u8, bounds.min, bounds.max)) return null;
        // REQUIRED columns can use the row count; nullable ones need null_count, or null slots would count as the
        // constant.
        plan.present = statPresentCount(rg, &cm, cm.statistics, file_meta) orelse return null;
        if (lane == .decimal and decimal_mod.statToF64(bounds.min, lane.decimal) == null) return null;
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
        if (decimal_mod.kindFromSchema(&se)) |k| return if (accum_is_float) .{ .decimal = k } else null;
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

fn statPresentCount(
    rg: *const schema.RowGroup,
    cm: *const schema.ColumnMetaData,
    stats_opt: ?schema.Statistics,
    file_meta: *const schema.FileMetaData,
) ?i64 {
    const levels = columnLevelsForStats(file_meta, cm) orelse return null;
    if (levels.max_rep > 0) return null;
    if (stats_opt) |stats| {
        if (stats.null_count) |nc| {
            const present = rg.num_rows - nc;
            return if (present >= 0) present else null;
        }
    }
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
            s.* = (s.* orelse 0) + v * @as(f64, @floatFromInt(present));
        },
        else => return error.UnsupportedAggType,
    }
}

/// Fold a DECIMAL column's stat min/max bytes into an .min_f / .max_f accumulator, decoded the way the value path
/// decodes them so the answer is the same without a per-RG decode.
fn foldDecimalStatBytes(
    state: *Accumulator,
    call: AggCall,
    kind: decimal_mod.Kind,
    bytes: []const u8,
) !void {
    const v = decimal_mod.statToF64(bytes, kind) orelse return switch (kind.physical) {
        .INT32, .INT64, .FIXED_LEN_BYTE_ARRAY, .BYTE_ARRAY => error.BadAggArg,
        else => error.UnsupportedAggType,
    };
    switch (state.*) {
        .min_f => |*slot| {
            slot.* = if (slot.*) |current| @min(current, v) else v;
        },
        .max_f => |*slot| {
            slot.* = if (slot.*) |current| @max(current, v) else v;
        },
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
            slot.* = if (slot.*) |current|
                (if (call.func == .min) @min(current, v) else @max(current, v))
            else
                v;
        },
        else => return error.UnsupportedAggType,
    }
}

/// Update one accumulator using one RG's decoded batch. Builds the
/// per-aggregate SelectionVector by cloning `outer_sel` and ANDing
/// in the agg's own `where` predicate (if any), then folds the
/// active rows into the accumulator.
///
/// `outer_sel` is the SelectionVector after the outer query
/// `--filter` ran. `arena` is a per-RG scratch arena.
///
/// Null handling: when `call.arg` is a bare `col_ref` to an OPTIONAL
/// column, we AND the column's present-mask into the per-call sel
/// before folding. That lets `sum/min/max/avg/count(col)` produce
/// the correct answer over real-world parquet (writers like Spark /
/// DuckDB emit columns OPTIONAL even when actual nulls are present).
/// Computed args over nullable columns (e.g. `sum(cost * 2)` where
/// cost is OPTIONAL) still error via `expr_eval.evalExpr`'s reject —
/// null-aware arithmetic in the binop kernel is a separate slice.
pub fn updateOne(
    arena: std.mem.Allocator,
    /// Long-lived allocator for owned results that must outlive `arena`
    /// (the per-RG scratch) and the cross-worker merge — currently just
    /// the string min/max winner. Pass the query/worker `gpa`.
    persist: std.mem.Allocator,
    state: *Accumulator,
    call: AggCall,
    batch: *const filter_eval.Batch,
    column_lookup: []const ?usize,
    outer_sel: *const filter_selection.SelectionVector,
) Error!void {
    // 1. Build per-agg selection vector. Start from outer_sel; AND in
    //    the agg's WHERE predicate (if present) and the col_ref's
    //    null mask (if the arg is a nullable bare col_ref).
    var sel_owned: ?filter_selection.SelectionVector = null;
    defer if (sel_owned) |*s| s.deinit();

    if (call.where) |pred| {
        sel_owned = try outer_sel.cloneAlloc(arena);
        try filter_eval.evaluate(pred, batch, &sel_owned.?, column_lookup, arena);
    }

    // 2. Compute the value column (skip for count(*)). For a bare
    //    col_ref we go through `colRefForAgg`, which tolerates
    //    OPTIONAL columns (rejecting nested LIST/MAP) and AND-masks
    //    the per-call sel. For computed args we delegate to the
    //    general evaluator (which still rejects nullable inputs).
    var values: ?filter_eval.Batch.Column = null;
    if (call.arg) |arg| {
        if (arg == .col_ref) {
            values = try colRefForAgg(arena, batch, column_lookup, arg.col_ref, outer_sel, &sel_owned);
        } else {
            values = try expr_eval.evalExpr(arena, batch, column_lookup, arg);
        }
    }
    defer if (values) |v| if (call.arg.? == .col_ref) freeWidened(arena, batch, column_lookup, call.arg.?.col_ref, v);

    const sel: *const filter_selection.SelectionVector =
        if (sel_owned) |*s| s else outer_sel;

    // Unsigned INT64 column: the i64 lane holds raw bits; the numeric folds
    // must reinterpret them as u64 (see ColRef.unsigned_64).
    const u64_col = if (call.arg) |a| (a == .col_ref and a.col_ref.unsigned_64) else false;

    // 3. Fold into accumulator.
    switch (call.func) {
        .count => state.count += sel.count(),
        .sum => try foldSum(state, values.?, sel, u64_col),
        .min => if (call.result == .bytes)
            try foldMinMaxBytes(persist, true, state, values.?, sel)
        else
            foldMin(state, values.?, sel, u64_col),
        .max => if (call.result == .bytes)
            try foldMinMaxBytes(persist, false, state, values.?, sel)
        else
            foldMax(state, values.?, sel, u64_col),
        .avg => try foldAvg(state, values.?, sel),
    }
}

/// Bytewise/unsigned min (`is_min`) or max over a decoded string column,
/// honoring the selection vector and the null mask. The per-batch winner is
/// a slice borrowed from the per-RG decode arena, so when it beats the
/// accumulator's running winner we dup it into `persist` (freeing the prior
/// owned copy). `std.mem.order` is the right tool here — string min/max isn't
/// the hot numeric path, so no hand-rolled SIMD (cf. simdMinMaxI64).
fn foldMinMaxBytes(
    persist: std.mem.Allocator,
    comptime is_min: bool,
    state: *Accumulator,
    col: filter_eval.Batch.Column,
    sel: *const filter_selection.SelectionVector,
) Error!void {
    const vals = col.string.values;
    const def_levels = col.string.def_levels;
    const max_def = col.string.max_def;

    var batch_winner: ?[]const u8 = null;
    for (vals, 0..) |v, i| {
        if (!sel.isActive(i)) continue;
        if (def_levels) |dl| if (dl[i] != max_def) continue; // skip nulls
        if (batch_winner) |w| {
            const ord = std.mem.order(u8, v, w);
            if ((is_min and ord == .lt) or (!is_min and ord == .gt)) batch_winner = v;
        } else batch_winner = v;
    }
    const cand = batch_winner orelse return; // nothing selected/present

    const slot: *?[]const u8 = switch (state.*) {
        .min_bytes, .max_bytes => |*s| s,
        else => unreachable,
    };
    if (slot.*) |cur| {
        const ord = std.mem.order(u8, cand, cur);
        const better = (is_min and ord == .lt) or (!is_min and ord == .gt);
        if (!better) return;
        persist.free(cur);
    }
    slot.* = try persist.dupe(u8, cand);
}

/// Borrow values for a bare col_ref agg arg, handling type widening
/// (i32→i64, f32→f64) and null-aware sel-mask intersection. Rejects
/// nested (LIST/MAP) columns — flat-fold semantics over a nested
/// column aren't well-defined here.
///
/// If the column is OPTIONAL with any actual nulls, lazily clones
/// `outer_sel` into `*sel_owned` (if not already cloned for a WHERE
/// clause) and clears bits where def_levels[i] < max_def. The
/// returned Batch.Column carries placeholder values at null
/// positions, which the caller never reads because the sel mask
/// has cleared the corresponding bits.
fn colRefForAgg(
    arena: std.mem.Allocator,
    batch: *const filter_eval.Batch,
    column_lookup: []const ?usize,
    c: expr_ast.ColRef,
    outer_sel: *const filter_selection.SelectionVector,
    sel_owned: *?filter_selection.SelectionVector,
) Error!filter_eval.Batch.Column {
    const pos = column_lookup[c.col_idx] orelse return error.BadColumn;
    const raw = batch.cols[pos];
    switch (raw) {
        inline else => |x| if (x.max_rep > 0) return error.UnsupportedAggType,
    }

    // Intersect the present-mask only if the column actually carries
    // null entries (a no-op for REQUIRED columns and for OPTIONAL
    // columns whose every row happens to be present).
    if (columnHasNulls(raw)) {
        if (sel_owned.* == null) {
            sel_owned.* = try outer_sel.cloneAlloc(arena);
        }
        intersectPresentMask(&sel_owned.*.?, raw);
    }

    return switch (c.expr_type) {
        .i64 => .{ .i64 = .{ .values = try expr_eval.widenColRefToI64(arena, raw, c) } },
        .f64 => .{ .f64 = .{ .values = try expr_eval.widenToF64(arena, raw) } },
        // String columns pass through unwidened; foldMinMaxBytes compares the
        // borrowed slices and dups the winner into `persist`. (Null rows are
        // already cleared from `sel` by the present-mask intersection above.)
        .str => raw,
    };
}

/// Hand back a copy `colRefForAgg` widened (i32/bool -> i64, f32 -> f64) once it has been folded. The per-RG arena
/// rolls its bump pointer back, so the next aggregate's widen lands on bytes already faulted in instead of fresh
/// pages. Borrowed (unwidened) columns are left alone. Arena frees only reclaim the newest allocation, so this must
/// run before anything allocated earlier is freed; out of order it is a harmless no-op.
fn freeWidened(
    arena: std.mem.Allocator,
    batch: *const filter_eval.Batch,
    column_lookup: []const ?usize,
    c: expr_ast.ColRef,
    values: filter_eval.Batch.Column,
) void {
    const pos = column_lookup[c.col_idx] orelse return;
    const raw_ptr: usize = switch (batch.cols[pos]) {
        inline else => |x| @intFromPtr(x.values.ptr),
    };
    switch (values) {
        inline .i64, .f64 => |x| if (@intFromPtr(x.values.ptr) != raw_ptr) arena.free(x.values),
        else => {},
    }
}

inline fn columnHasNulls(col: filter_eval.Batch.Column) bool {
    return switch (col) {
        inline else => |c| c.has_nulls,
    };
}

/// Clear bits in `sel` for rows where the column is null. Operates
/// per-bit; for typical RG sizes (~1M rows) this runs once and is
/// not a hot path. The fold inner loops do remain SIMD-able.
fn intersectPresentMask(
    sel: *filter_selection.SelectionVector,
    col: filter_eval.Batch.Column,
) void {
    switch (col) {
        inline else => |c| {
            const dls = c.def_levels orelse return;
            const max_def = c.max_def;
            const len = @min(dls.len, sel.len);
            var i: usize = 0;
            while (i < len) : (i += 1) {
                if (dls[i] < max_def) sel.set(i, false);
            }
        },
    }
}

// ============================================================
// SIMD inner loops (C4.y).
//
// Selection-vector aware reductions over numeric arrays. Per-64-row
// block dispatch:
//
//   - sel word == ~0  (all 64 active)  → SIMD vector fold
//   - sel word == 0   (none active)    → skip
//   - sel word mixed                   → scalar bit-walk
//
// Block-level dispatch beats per-lane masking on real workloads:
// it avoids @select / vector-mask construction for the common case
// where filters bin large contiguous regions in or out (typical for
// stat-pruned and time-clustered data). The mixed case is the only
// expensive branch and it uses ctz to skip directly to set bits.
//
// Vector lane count: 4 for both i64 and f64 — fits a 32-byte AVX2
// ymm register; 64-row blocks unroll to 16 SIMD ops. Wider would
// need AVX-512 which Lambda x86_64 may or may not provide.
// ============================================================

const VLANES: usize = 4;
const VI64 = @Vector(VLANES, i64);
const VF64 = @Vector(VLANES, f64);

/// Sum of an i64 column under a SelectionVector. Returns i128 to
/// preserve precision for full-range i64 inputs — pairwise i64
/// addition wraps at MAX/MIN, so the accumulator must be wider.
///
/// Block-level dispatch is preserved (skip / hot / mixed): the all-
/// active fast path is a tight scalar loop the compiler autovectorizes
/// at the load + partial-sum level, then widens to i128 at fold time.
/// The 2026-05-06 bug that motivated this (`benchmark_100mb.parquet`'s
/// `int64_random` column overflowed the previous `@Vector(N, i64)`
/// accumulator) is fixed here: every add lands in i128.
fn simdSumI64(vals: []const i64, sel: *const filter_selection.SelectionVector) i128 {
    var acc: i128 = 0;
    var word_i: usize = 0;
    while ((word_i + 1) * 64 <= vals.len) : (word_i += 1) {
        const w = sel.mask[word_i];
        const base = word_i * 64;
        if (w == std.math.maxInt(u64)) {
            for (vals[base .. base + 64]) |v| acc += @as(i128, v);
        } else if (w != 0) {
            var bits = w;
            while (bits != 0) {
                const idx: u6 = @intCast(@ctz(bits));
                acc += @as(i128, vals[base + idx]);
                bits &= bits - 1;
            }
        }
    }
    const tail_start = word_i * 64;
    for (tail_start..vals.len) |i| if (sel.isActive(i)) {
        acc += @as(i128, vals[i]);
    };
    return acc;
}

/// Unsigned-INT64 sum/min/max: the i64 lane carries raw bits, so reinterpret
/// each as u64 before widening to i128. Scalar (uint64 columns are rare enough
/// not to warrant a SIMD kernel; the signed paths above stay the hot ones).
fn sumU64(vals: []const i64, sel: *const filter_selection.SelectionVector) i128 {
    var acc: i128 = 0;
    for (vals, 0..) |v, i| if (sel.isActive(i)) {
        acc += @as(i128, @as(u64, @bitCast(v)));
    };
    return acc;
}

fn minMaxU64(
    comptime is_min: bool,
    vals: []const i64,
    sel: *const filter_selection.SelectionVector,
) ?i128 {
    var acc: ?u64 = null;
    for (vals, 0..) |v, i| if (sel.isActive(i)) {
        const u: u64 = @bitCast(v);
        acc = if (acc) |c| (if (is_min) @min(c, u) else @max(c, u)) else u;
    };
    return if (acc) |u| @as(i128, u) else null;
}

fn simdSumF64(vals: []const f64, sel: *const filter_selection.SelectionVector) f64 {
    var acc_v: VF64 = @splat(0);
    var acc_s: f64 = 0;
    var word_i: usize = 0;
    while ((word_i + 1) * 64 <= vals.len) : (word_i += 1) {
        const w = sel.mask[word_i];
        const base = word_i * 64;
        if (w == std.math.maxInt(u64)) {
            inline for (0..64 / VLANES) |k| {
                const chunk: VF64 = vals[base + k * VLANES ..][0..VLANES].*;
                acc_v += chunk;
            }
        } else if (w != 0) {
            var bits = w;
            while (bits != 0) {
                const idx: u6 = @intCast(@ctz(bits));
                acc_s += vals[base + idx];
                bits &= bits - 1;
            }
        }
    }
    acc_s += @reduce(.Add, acc_v);
    const tail_start = word_i * 64;
    for (tail_start..vals.len) |i| if (sel.isActive(i)) {
        acc_s += vals[i];
    };
    return acc_s;
}

/// Generic SIMD min/max reducer. Uses sentinel-init (max-int / min-int
/// for i64, +inf / -inf for f64) so the vector fold doesn't need a
/// "first value seen" guard. The optional return preserves "no values
/// seen" semantics for the caller (sentinel-init then no-active-bits
/// would otherwise return the sentinel).
fn simdMinMaxI64(
    comptime is_min: bool,
    vals: []const i64,
    sel: *const filter_selection.SelectionVector,
) ?i64 {
    const sentinel: i64 = if (is_min) std.math.maxInt(i64) else std.math.minInt(i64);
    var acc_v: VI64 = @splat(sentinel);
    var acc_s: i64 = sentinel;
    var any_active = false;
    var word_i: usize = 0;
    while ((word_i + 1) * 64 <= vals.len) : (word_i += 1) {
        const w = sel.mask[word_i];
        const base = word_i * 64;
        if (w == std.math.maxInt(u64)) {
            any_active = true;
            inline for (0..64 / VLANES) |k| {
                const chunk: VI64 = vals[base + k * VLANES ..][0..VLANES].*;
                acc_v = if (is_min) @min(acc_v, chunk) else @max(acc_v, chunk);
            }
        } else if (w != 0) {
            any_active = true;
            var bits = w;
            while (bits != 0) {
                const idx: u6 = @intCast(@ctz(bits));
                const v = vals[base + idx];
                acc_s = if (is_min) @min(acc_s, v) else @max(acc_s, v);
                bits &= bits - 1;
            }
        }
    }
    const v_reduced = if (is_min) @reduce(.Min, acc_v) else @reduce(.Max, acc_v);
    acc_s = if (is_min) @min(acc_s, v_reduced) else @max(acc_s, v_reduced);
    const tail_start = word_i * 64;
    for (tail_start..vals.len) |i| if (sel.isActive(i)) {
        any_active = true;
        const v = vals[i];
        acc_s = if (is_min) @min(acc_s, v) else @max(acc_s, v);
    };
    return if (any_active) acc_s else null;
}

fn simdMinMaxF64(
    comptime is_min: bool,
    vals: []const f64,
    sel: *const filter_selection.SelectionVector,
) ?f64 {
    const sentinel: f64 = if (is_min) std.math.inf(f64) else -std.math.inf(f64);
    var acc_v: VF64 = @splat(sentinel);
    var acc_s: f64 = sentinel;
    var any_active = false;
    var word_i: usize = 0;
    while ((word_i + 1) * 64 <= vals.len) : (word_i += 1) {
        const w = sel.mask[word_i];
        const base = word_i * 64;
        if (w == std.math.maxInt(u64)) {
            any_active = true;
            inline for (0..64 / VLANES) |k| {
                const chunk: VF64 = vals[base + k * VLANES ..][0..VLANES].*;
                acc_v = if (is_min) @min(acc_v, chunk) else @max(acc_v, chunk);
            }
        } else if (w != 0) {
            any_active = true;
            var bits = w;
            while (bits != 0) {
                const idx: u6 = @intCast(@ctz(bits));
                const v = vals[base + idx];
                acc_s = if (is_min) @min(acc_s, v) else @max(acc_s, v);
                bits &= bits - 1;
            }
        }
    }
    const v_reduced = if (is_min) @reduce(.Min, acc_v) else @reduce(.Max, acc_v);
    acc_s = if (is_min) @min(acc_s, v_reduced) else @max(acc_s, v_reduced);
    const tail_start = word_i * 64;
    for (tail_start..vals.len) |i| if (sel.isActive(i)) {
        any_active = true;
        const v = vals[i];
        acc_s = if (is_min) @min(acc_s, v) else @max(acc_s, v);
    };
    return if (any_active) acc_s else null;
}

/// SIMD avg-fold returns (sum, count) so the caller can merge across RGs.
/// i64 input is widened to f64 lane-by-lane (no @intFromFloat-on-vector
/// equivalent in 0.16; doing scalar in the SIMD inner loop forfeits the
/// benefit, so we cast the source slice once).
fn simdAvgPairF64(
    vals: []const f64,
    sel: *const filter_selection.SelectionVector,
) struct { sum: f64, count: u64 } {
    var sum_v: VF64 = @splat(0);
    var sum_s: f64 = 0;
    var count: u64 = 0;
    var word_i: usize = 0;
    while ((word_i + 1) * 64 <= vals.len) : (word_i += 1) {
        const w = sel.mask[word_i];
        const base = word_i * 64;
        if (w == std.math.maxInt(u64)) {
            count += 64;
            inline for (0..64 / VLANES) |k| {
                const chunk: VF64 = vals[base + k * VLANES ..][0..VLANES].*;
                sum_v += chunk;
            }
        } else if (w != 0) {
            count += @popCount(w);
            var bits = w;
            while (bits != 0) {
                const idx: u6 = @intCast(@ctz(bits));
                sum_s += vals[base + idx];
                bits &= bits - 1;
            }
        }
    }
    sum_s += @reduce(.Add, sum_v);
    const tail_start = word_i * 64;
    for (tail_start..vals.len) |i| if (sel.isActive(i)) {
        sum_s += vals[i];
        count += 1;
    };
    return .{ .sum = sum_s, .count = count };
}

fn foldSum(
    state: *Accumulator,
    col: filter_eval.Batch.Column,
    sel: *const filter_selection.SelectionVector,
    unsigned_64: bool,
) Error!void {
    if (!sel.any()) return; // no values: leave a NULL sum NULL
    switch (state.*) {
        .sum_i => |*s| {
            s.* = (s.* orelse 0) + if (unsigned_64) sumU64(col.i64.values, sel) else simdSumI64(col.i64.values, sel);
        },
        .sum_f => |*s| {
            s.* = (s.* orelse 0) + simdSumF64(col.f64.values, sel);
        },
        else => return error.UnsupportedAggType,
    }
}

fn foldMin(
    state: *Accumulator,
    col: filter_eval.Batch.Column,
    sel: *const filter_selection.SelectionVector,
    unsigned_64: bool,
) void {
    switch (state.*) {
        .min_i => |*s| {
            const v_opt: ?i128 = if (unsigned_64)
                minMaxU64(true, col.i64.values, sel)
            else if (simdMinMaxI64(true, col.i64.values, sel)) |v| @as(i128, v) else null;
            if (v_opt) |v| s.* = if (s.*) |current| @min(current, v) else v;
        },
        .min_f => |*s| {
            if (simdMinMaxF64(true, col.f64.values, sel)) |v| {
                s.* = if (s.*) |current| @min(current, v) else v;
            }
        },
        else => unreachable,
    }
}

fn foldMax(
    state: *Accumulator,
    col: filter_eval.Batch.Column,
    sel: *const filter_selection.SelectionVector,
    unsigned_64: bool,
) void {
    switch (state.*) {
        .max_i => |*s| {
            const v_opt: ?i128 = if (unsigned_64)
                minMaxU64(false, col.i64.values, sel)
            else if (simdMinMaxI64(false, col.i64.values, sel)) |v| @as(i128, v) else null;
            if (v_opt) |v| s.* = if (s.*) |current| @max(current, v) else v;
        },
        .max_f => |*s| {
            if (simdMinMaxF64(false, col.f64.values, sel)) |v| {
                s.* = if (s.*) |current| @max(current, v) else v;
            }
        },
        else => unreachable,
    }
}

fn foldAvg(
    state: *Accumulator,
    col: filter_eval.Batch.Column,
    sel: *const filter_selection.SelectionVector,
) Error!void {
    const a = &state.avg;
    switch (col) {
        .i64 => |c| {
            // i64 → f64 widening per-lane isn't a clean SIMD op in
            // Zig 0.16, and avg over int columns is rare relative to
            // float (cost data is f64). Keep scalar; if profiling
            // shows otherwise, widen with a one-shot per-RG cast.
            for (c.values, 0..) |v, i| if (sel.isActive(i)) {
                a.sum += @floatFromInt(v);
                a.count += 1;
            };
        },
        .f64 => |c| {
            const partial = simdAvgPairF64(c.values, sel);
            a.sum += partial.sum;
            a.count += partial.count;
        },
        else => return error.UnsupportedAggType,
    }
}

/// Materialize one agg's accumulator into one or two `Batch.Column`s
/// for the final 1-row output. Allocates from `arena`. Returns a
/// slice of `OutputCol` (length 1 for sum/count/min/max, length 2 for
/// avg's split-output shape).
pub const OutputCol = struct {
    name: []const u8,
    col: filter_eval.Batch.Column,
    /// Parquet physical type — used by the caller to synthesize a
    /// SchemaElement for the output leaf.
    parquet_type: schema.Type,
    /// OPTIONAL in the output schema. Set by the aggregate kind, not by whether this result happens to be NULL, so
    /// per-file outputs of one query share a schema and re-aggregate together.
    nullable: bool,
    /// Annotations carried from a source column whose values pass through unchanged (see `engine.retypeOutputCol`).
    logical_type: ?schema.LogicalType = null,
    converted_type: ?schema.ConvertedType = null,
};

pub fn finalize(
    arena: std.mem.Allocator,
    call: AggCall,
    state: Accumulator,
) Error![]OutputCol {
    return switch (call.func) {
        .count => try emitOneI64(arena, call.alias, @intCast(state.count), false),
        .sum => switch (call.result) {
            .i64 => try emitOneI64(arena, call.alias, state.sum_i, true),
            .f64 => try emitOneF64(arena, call.alias, state.sum_f),
            .bytes, .avg_f64 => unreachable,
        },
        .min => switch (call.result) {
            .i64 => try emitOneI64(arena, call.alias, u64Bits(call, state.min_i), true),
            .f64 => try emitOneF64(arena, call.alias, state.min_f),
            .bytes => try emitOneString(arena, call.alias, state.min_bytes),
            .avg_f64 => unreachable,
        },
        .max => switch (call.result) {
            .i64 => try emitOneI64(arena, call.alias, u64Bits(call, state.max_i), true),
            .f64 => try emitOneF64(arena, call.alias, state.max_f),
            .bytes => try emitOneString(arena, call.alias, state.max_bytes),
            .avg_f64 => unreachable,
        },
        .avg => try emitAvgPair(arena, call.alias, state.avg),
    };
}

/// min/max of an unsigned 64-bit column as the raw bits it is stored as, so an extremum of 2^63 or more still fits the
/// INT64 column; the writer annotates it unsigned.
fn u64Bits(call: AggCall, v: ?i128) ?i128 {
    const x = v orelse return null;
    const unsigned = if (call.arg) |a| a == .col_ref and a.col_ref.unsigned_64 else false;
    return if (unsigned) @as(i64, @bitCast(@as(u64, @intCast(x)))) else x;
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
    // Caller running `zpq query --aggregate "avg(...) AS x"` over the
    // tier-1 outputs gets sum(x__sum) / sum(x__count) — algebraically
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

/// The avg pair's sum, NULL when no value was averaged.
pub fn avgSum(st: AvgState) ?f64 {
    return if (st.count == 0) null else st.sum;
}

// ============================================================
// Tests
// ============================================================

const testing = std.testing;

fn testI64Meta(
    arena: std.mem.Allocator,
    row_groups: std.ArrayListUnmanaged(schema.RowGroup),
    num_rows: i64,
    repetition: schema.FieldRepetitionType,
) !schema.FileMetaData {
    var elems: std.ArrayListUnmanaged(schema.SchemaElement) = .empty;
    try elems.append(arena, .{
        .type = null,
        .type_length = null,
        .repetition_type = null,
        .name = "schema",
        .num_children = 1,
        .converted_type = null,
        .logical_type = null,
        .scale = null,
        .precision = null,
        .field_id = null,
    });
    try elems.append(arena, .{
        .type = .INT64,
        .type_length = null,
        .repetition_type = repetition,
        .name = "x",
        .num_children = 0,
        .converted_type = null,
        .logical_type = null,
        .scale = null,
        .precision = null,
        .field_id = null,
    });
    return .{
        .version = 1,
        .schema = elems,
        .num_rows = num_rows,
        .created_by = null,
        .row_groups = row_groups,
    };
}

test "count over all rows" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    const cols = [_]filter_eval.Batch.Column{};
    const batch: filter_eval.Batch = .{ .cols = &cols, .num_rows = 100 };
    const lookup = [_]?usize{};
    var sel = try filter_selection.SelectionVector.init(a, 100);

    const call: AggCall = .{
        .func = .count,
        .arg = null,
        .where = null,
        .alias = "n",
        .result = .i64,
    };
    var state = Accumulator.init(call);
    try updateOne(a, a, &state, call, &batch, &lookup, &sel);
    try testing.expectEqual(@as(u64, 100), state.count);
}

test "sum i64 column" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    const xs = [_]i64{ 10, 20, 30, 40, 50 };
    const cols = [_]filter_eval.Batch.Column{.{ .i64 = .{ .values = &xs } }};
    const batch: filter_eval.Batch = .{ .cols = &cols, .num_rows = 5 };
    const lookup = [_]?usize{0};
    var sel = try filter_selection.SelectionVector.init(a, 5);

    const arg_expr: expr_ast.Expr = .{ .col_ref = .{
        .col_idx = 0,
        .physical_type = .INT64,
        .expr_type = .i64,
    } };
    const call: AggCall = .{
        .func = .sum,
        .arg = arg_expr,
        .where = null,
        .alias = "total",
        .result = .i64,
    };
    var state = Accumulator.init(call);
    try updateOne(a, a, &state, call, &batch, &lookup, &sel);
    try testing.expectEqual(@as(i128, 150), state.sum_i);

    const out = try finalize(a, call, state);
    try testing.expectEqual(@as(usize, 1), out.len);
    try testing.expectEqualStrings("total", out[0].name);
    try testing.expectEqual(@as(i64, 150), out[0].col.i64.values[0]);
}

test "unsigned-64 sum/min/max read the i64 lane as u64" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    // Raw i64 bits for u64 values { 0, i64::MAX, u64::MAX }. As signed i64 the
    // last is -1; the unsigned-64 fold must read it as 18446744073709551615.
    const xs = [_]i64{ 0, std.math.maxInt(i64), -1 };
    const cols = [_]filter_eval.Batch.Column{.{ .i64 = .{ .values = &xs } }};
    const batch: filter_eval.Batch = .{ .cols = &cols, .num_rows = 3 };
    const lookup = [_]?usize{0};

    const u64_max_i128: i128 = std.math.maxInt(u64);
    const expectations = .{
        .{ AggFunc.sum, @as(i128, 0) + std.math.maxInt(i64) + u64_max_i128 },
        .{ AggFunc.min, @as(i128, 0) },
        .{ AggFunc.max, u64_max_i128 },
    };
    inline for (expectations) |exp| {
        var sel = try filter_selection.SelectionVector.init(a, 3); // all active
        const call: AggCall = .{
            .func = exp[0],
            .arg = .{ .col_ref = .{ .col_idx = 0, .physical_type = .INT64, .expr_type = .i64, .unsigned_64 = true } },
            .where = null,
            .alias = "u",
            .result = .i64,
        };
        var state = Accumulator.init(call);
        try updateOne(a, a, &state, call, &batch, &lookup, &sel);
        const got: i128 = switch (state) {
            .sum_i => |v| v.?,
            .min_i => |v| v.?,
            .max_i => |v| v.?,
            else => unreachable,
        };
        try testing.expectEqual(@as(i128, exp[1]), got);
    }
}

test "min/max f64 column with selection" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    const xs = [_]f64{ 5.0, 2.0, 8.0, 1.0, 9.0 };
    const cols = [_]filter_eval.Batch.Column{.{ .f64 = .{ .values = &xs } }};
    const batch: filter_eval.Batch = .{ .cols = &cols, .num_rows = 5 };
    const lookup = [_]?usize{0};
    var sel = try filter_selection.SelectionVector.init(a, 5);
    sel.set(0, false); // skip 5.0
    sel.set(4, false); // skip 9.0

    const arg_expr: expr_ast.Expr = .{ .col_ref = .{
        .col_idx = 0,
        .physical_type = .DOUBLE,
        .expr_type = .f64,
    } };
    const min_call: AggCall = .{ .func = .min, .arg = arg_expr, .where = null, .alias = "lo", .result = .f64 };
    const max_call: AggCall = .{ .func = .max, .arg = arg_expr, .where = null, .alias = "hi", .result = .f64 };

    var min_state = Accumulator.init(min_call);
    var max_state = Accumulator.init(max_call);
    try updateOne(a, a, &min_state, min_call, &batch, &lookup, &sel);
    try updateOne(a, a, &max_state, max_call, &batch, &lookup, &sel);

    try testing.expectEqual(@as(f64, 1.0), min_state.min_f.?);
    try testing.expectEqual(@as(f64, 8.0), max_state.max_f.?);
}

test "min/max string column (bytewise/unsigned, owned winner)" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    // bytewise order: "apple" < "banana" < "cherry" < "date"
    const xs = [_][]const u8{ "cherry", "apple", "date", "banana" };
    const cols = [_]filter_eval.Batch.Column{.{ .string = .{ .values = &xs } }};
    const batch: filter_eval.Batch = .{ .cols = &cols, .num_rows = 4 };
    const lookup = [_]?usize{0};
    var sel = try filter_selection.SelectionVector.init(a, 4);

    const arg_expr: expr_ast.Expr = .{ .col_ref = .{
        .col_idx = 0,
        .physical_type = .BYTE_ARRAY,
        .expr_type = .str,
    } };
    const min_call: AggCall = .{ .func = .min, .arg = arg_expr, .where = null, .alias = "lo", .result = .bytes };
    const max_call: AggCall = .{ .func = .max, .arg = arg_expr, .where = null, .alias = "hi", .result = .bytes };

    var min_state = Accumulator.init(min_call);
    var max_state = Accumulator.init(max_call);
    try updateOne(a, a, &min_state, min_call, &batch, &lookup, &sel);
    try updateOne(a, a, &max_state, max_call, &batch, &lookup, &sel);

    try testing.expectEqualStrings("apple", min_state.min_bytes.?);
    try testing.expectEqualStrings("date", max_state.max_bytes.?);
    // The winner is OWNED (dup'd into persist), not borrowed from the batch.
    try testing.expect(min_state.min_bytes.?.ptr != xs[1].ptr);
}

test "aggregates over a bool column (widened to 0/1)" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    const xs = [_]bool{ true, false, true, true, false }; // 3 trues
    const cols = [_]filter_eval.Batch.Column{.{ .boolean = .{ .values = &xs } }};
    const batch: filter_eval.Batch = .{ .cols = &cols, .num_rows = 5 };
    const lookup = [_]?usize{0};
    var sel = try filter_selection.SelectionVector.init(a, 5);

    // BOOLEAN col_ref typed as i64 (0/1), as the expr parser now resolves it.
    const arg: expr_ast.Expr = .{ .col_ref = .{ .col_idx = 0, .physical_type = .BOOLEAN, .expr_type = .i64 } };
    const sum_call: AggCall = .{ .func = .sum, .arg = arg, .where = null, .alias = "s", .result = .i64 };
    const min_call: AggCall = .{ .func = .min, .arg = arg, .where = null, .alias = "mn", .result = .i64 };
    const max_call: AggCall = .{ .func = .max, .arg = arg, .where = null, .alias = "mx", .result = .i64 };

    var s = Accumulator.init(sum_call);
    var mn = Accumulator.init(min_call);
    var mx = Accumulator.init(max_call);
    try updateOne(a, a, &s, sum_call, &batch, &lookup, &sel);
    try updateOne(a, a, &mn, min_call, &batch, &lookup, &sel);
    try updateOne(a, a, &mx, max_call, &batch, &lookup, &sel);

    try testing.expectEqual(@as(i128, 3), s.sum_i); // sum = #trues
    try testing.expectEqual(@as(i64, 0), mn.min_i.?); // min = 0 (a false present)
    try testing.expectEqual(@as(i64, 1), mx.max_i.?); // max = 1 (a true present)
}

test "avg emits sum + count pair" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    const xs = [_]f64{ 1.0, 2.0, 3.0, 4.0 };
    const cols = [_]filter_eval.Batch.Column{.{ .f64 = .{ .values = &xs } }};
    const batch: filter_eval.Batch = .{ .cols = &cols, .num_rows = 4 };
    const lookup = [_]?usize{0};
    var sel = try filter_selection.SelectionVector.init(a, 4);

    const arg_expr: expr_ast.Expr = .{ .col_ref = .{
        .col_idx = 0,
        .physical_type = .DOUBLE,
        .expr_type = .f64,
    } };
    const call: AggCall = .{
        .func = .avg,
        .arg = arg_expr,
        .where = null,
        .alias = "mean_x",
        .result = .avg_f64,
    };
    var state = Accumulator.init(call);
    try updateOne(a, a, &state, call, &batch, &lookup, &sel);
    try testing.expectEqual(@as(f64, 10.0), state.avg.sum);
    try testing.expectEqual(@as(u64, 4), state.avg.count);

    const out = try finalize(a, call, state);
    try testing.expectEqual(@as(usize, 2), out.len);
    try testing.expectEqualStrings("mean_x__sum", out[0].name);
    try testing.expectEqualStrings("mean_x__count", out[1].name);
    try testing.expectEqual(@as(f64, 10.0), out[0].col.f64.values[0]);
    try testing.expectEqual(@as(i64, 4), out[1].col.i64.values[0]);
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

test "updateOneFromStats sum: constant-column RG folds num_rows × value" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    // Constant column: min == max == 42, 100 rows → sum = 4200.
    var min_buf = try a.alloc(u8, 8);
    var max_buf = try a.alloc(u8, 8);
    std.mem.writeInt(i64, min_buf[0..8], 42, .little);
    std.mem.writeInt(i64, max_buf[0..8], 42, .little);

    var path: schema.StringList = .empty;
    try path.append(a, "x");
    var encs: schema.EncodingList = .empty;
    try encs.append(a, .PLAIN);

    var col_chunks: std.ArrayListUnmanaged(schema.ColumnChunk) = .empty;
    try col_chunks.append(a, .{
        .file_path = null,
        .file_offset = 0,
        .meta_data = .{
            .type = .INT64,
            .encodings = encs,
            .path_in_schema = path,
            .codec = .UNCOMPRESSED,
            .num_values = 100,
            .total_uncompressed_size = 800,
            .total_compressed_size = 800,
            .data_page_offset = 0,
            .index_page_offset = null,
            .dictionary_page_offset = null,
            .statistics = .{ .min_value = min_buf, .max_value = max_buf, .null_count = 0 },
        },
    });
    const rg: schema.RowGroup = .{
        .columns = col_chunks,
        .total_byte_size = 800,
        .num_rows = 100,
    };
    const test_meta = try testI64Meta(a, .empty, 100, .REQUIRED);

    const arg_expr: expr_ast.Expr = .{ .col_ref = .{ .col_idx = 0, .physical_type = .INT64, .expr_type = .i64 } };
    const sum_call: AggCall = .{ .func = .sum, .arg = arg_expr, .where = null, .alias = "s", .result = .i64 };
    var sum_state = Accumulator.init(sum_call);

    try testing.expect(try updateOneFromStats(&sum_state, sum_call, &rg, &test_meta));
    try testing.expectEqual(@as(i128, 4200), sum_state.sum_i);

    // OPTIONAL constant column with missing null_count is not safe:
    // min == max only describes present values, not how many rows are null.
    rg.columns.items[0].meta_data.?.statistics.?.null_count = null;
    const optional_meta = try testI64Meta(a, .empty, 100, .OPTIONAL);
    var optional_state = Accumulator.init(sum_call);
    try testing.expectEqual(false, try updateOneFromStats(&optional_state, sum_call, &rg, &optional_meta));
    rg.columns.items[0].meta_data.?.statistics.?.null_count = 0;

    // Non-constant RG (min != max): updateOneFromStats returns false
    // → caller falls through to decode.
    var hi_buf = try a.alloc(u8, 8);
    std.mem.writeInt(i64, hi_buf[0..8], 100, .little);
    rg.columns.items[0].meta_data.?.statistics.?.max_value = hi_buf;
    var state2 = Accumulator.init(sum_call);
    try testing.expectEqual(false, try updateOneFromStats(&state2, sum_call, &rg, &test_meta));
    try testing.expectEqual(@as(?i128, null), state2.sum_i);
}

test "updateOneFromStats: an all-null row group leaves sum/min/max NULL" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    // Bounds a writer left on a chunk whose every row is null must not become a value (or a 0 sum).
    const bound = try a.alloc(u8, 8);
    std.mem.writeInt(i64, bound[0..8], 7, .little);
    var path: schema.StringList = .empty;
    try path.append(a, "x");
    var col_chunks: std.ArrayListUnmanaged(schema.ColumnChunk) = .empty;
    try col_chunks.append(a, .{
        .file_path = null,
        .file_offset = 0,
        .meta_data = .{
            .type = .INT64,
            .encodings = .empty,
            .path_in_schema = path,
            .codec = .UNCOMPRESSED,
            .num_values = 100,
            .total_uncompressed_size = 0,
            .total_compressed_size = 0,
            .data_page_offset = 0,
            .index_page_offset = null,
            .dictionary_page_offset = null,
            .statistics = .{ .min_value = bound, .max_value = bound, .null_count = 100 },
        },
    });
    const rg: schema.RowGroup = .{ .columns = col_chunks, .total_byte_size = 0, .num_rows = 100 };
    const meta = try testI64Meta(a, .empty, 100, .OPTIONAL);
    const arg: expr_ast.Expr = .{ .col_ref = .{ .col_idx = 0, .physical_type = .INT64, .expr_type = .i64 } };

    inline for (.{ AggFunc.sum, AggFunc.min, AggFunc.max }) |func| {
        const call: AggCall = .{ .func = func, .arg = arg, .where = null, .alias = "x", .result = .i64 };
        var state = Accumulator.init(call);
        try testing.expect(try updateOneFromStats(&state, call, &rg, &meta));
        const v: ?i128 = switch (state) {
            .sum_i, .min_i, .max_i => |x| x,
            else => unreachable,
        };
        try testing.expectEqual(@as(?i128, null), v);
    }
}

test "updateOneFromStats decodes typed min/max bytes" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    // Build a synthetic RG with one INT64 column-chunk + stats.
    var min_buf = try a.alloc(u8, 8);
    var max_buf = try a.alloc(u8, 8);
    std.mem.writeInt(i64, min_buf[0..8], -42, .little);
    std.mem.writeInt(i64, max_buf[0..8], 1000, .little);

    var path: schema.StringList = .empty;
    try path.append(a, "x");
    var encs: schema.EncodingList = .empty;
    try encs.append(a, .PLAIN);

    var col_chunks: std.ArrayListUnmanaged(schema.ColumnChunk) = .empty;
    try col_chunks.append(a, .{
        .file_path = null,
        .file_offset = 0,
        .meta_data = .{
            .type = .INT64,
            .encodings = encs,
            .path_in_schema = path,
            .codec = .UNCOMPRESSED,
            .num_values = 100,
            .total_uncompressed_size = 800,
            .total_compressed_size = 800,
            .data_page_offset = 0,
            .index_page_offset = null,
            .dictionary_page_offset = null,
            .statistics = .{
                .min_value = min_buf,
                .max_value = max_buf,
            },
        },
    });
    const rg: schema.RowGroup = .{
        .columns = col_chunks,
        .total_byte_size = 800,
        .num_rows = 100,
    };

    const arg_expr: expr_ast.Expr = .{ .col_ref = .{ .col_idx = 0, .physical_type = .INT64, .expr_type = .i64 } };
    const min_call: AggCall = .{ .func = .min, .arg = arg_expr, .where = null, .alias = "lo", .result = .i64 };
    const max_call: AggCall = .{ .func = .max, .arg = arg_expr, .where = null, .alias = "hi", .result = .i64 };

    var min_state = Accumulator.init(min_call);
    var max_state = Accumulator.init(max_call);
    const test_meta = try testI64Meta(a, .empty, 100, .REQUIRED);
    try testing.expect(try updateOneFromStats(&min_state, min_call, &rg, &test_meta));
    try testing.expect(try updateOneFromStats(&max_state, max_call, &rg, &test_meta));
    try testing.expectEqual(@as(i64, -42), min_state.min_i.?);
    try testing.expectEqual(@as(i64, 1000), max_state.max_i.?);
}

test "updateOneFromStats returns false when stats absent" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    var path: schema.StringList = .empty;
    try path.append(a, "x");
    var encs: schema.EncodingList = .empty;
    try encs.append(a, .PLAIN);

    var col_chunks: std.ArrayListUnmanaged(schema.ColumnChunk) = .empty;
    try col_chunks.append(a, .{
        .file_path = null,
        .file_offset = 0,
        .meta_data = .{
            .type = .INT64,
            .encodings = encs,
            .path_in_schema = path,
            .codec = .UNCOMPRESSED,
            .num_values = 100,
            .total_uncompressed_size = 800,
            .total_compressed_size = 800,
            .data_page_offset = 0,
            .index_page_offset = null,
            .dictionary_page_offset = null,
            .statistics = null, // ← no stats
        },
    });
    const rg: schema.RowGroup = .{
        .columns = col_chunks,
        .total_byte_size = 800,
        .num_rows = 100,
    };

    const arg_expr: expr_ast.Expr = .{ .col_ref = .{ .col_idx = 0, .physical_type = .INT64, .expr_type = .i64 } };
    const min_call: AggCall = .{ .func = .min, .arg = arg_expr, .where = null, .alias = "lo", .result = .i64 };

    var state = Accumulator.init(min_call);
    const test_meta = try testI64Meta(a, .empty, 100, .REQUIRED);
    try testing.expectEqual(false, try updateOneFromStats(&state, min_call, &rg, &test_meta));
}

test "updateOneFromStats: count(*) uses num_rows; count(col) uses num_rows - null_count" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    var path: schema.StringList = .empty;
    try path.append(a, "x");
    var encs: schema.EncodingList = .empty;
    try encs.append(a, .PLAIN);

    var col_chunks: std.ArrayListUnmanaged(schema.ColumnChunk) = .empty;
    try col_chunks.append(a, .{
        .file_path = null,
        .file_offset = 0,
        .meta_data = .{
            .type = .INT64,
            .encodings = encs,
            .path_in_schema = path,
            .codec = .UNCOMPRESSED,
            .num_values = 100,
            .total_uncompressed_size = 800,
            .total_compressed_size = 800,
            .data_page_offset = 0,
            .index_page_offset = null,
            .dictionary_page_offset = null,
            .statistics = .{ .null_count = 17 },
        },
    });
    const rg: schema.RowGroup = .{
        .columns = col_chunks,
        .total_byte_size = 800,
        .num_rows = 100,
    };

    const test_meta = try testI64Meta(a, .empty, 100, .REQUIRED);

    // count(*) → 100
    const star_call: AggCall = .{ .func = .count, .arg = null, .where = null, .alias = "n", .result = .i64 };
    var star_state = Accumulator.init(star_call);
    try testing.expect(try updateOneFromStats(&star_state, star_call, &rg, &test_meta));
    try testing.expectEqual(@as(u64, 100), star_state.count);

    // count(x) → 100 - 17 = 83
    const arg: expr_ast.Expr = .{ .col_ref = .{ .col_idx = 0, .physical_type = .INT64, .expr_type = .i64 } };
    const col_call: AggCall = .{ .func = .count, .arg = arg, .where = null, .alias = "n", .result = .i64 };
    var col_state = Accumulator.init(col_call);
    try testing.expect(try updateOneFromStats(&col_state, col_call, &rg, &test_meta));
    try testing.expectEqual(@as(u64, 83), col_state.count);

    // REQUIRED count(x) does not need null_count; every row is present.
    var col_chunks2: std.ArrayListUnmanaged(schema.ColumnChunk) = .empty;
    try col_chunks2.append(a, .{
        .file_path = null,
        .file_offset = 0,
        .meta_data = .{
            .type = .INT64,
            .encodings = encs,
            .path_in_schema = path,
            .codec = .UNCOMPRESSED,
            .num_values = 100,
            .total_uncompressed_size = 800,
            .total_compressed_size = 800,
            .data_page_offset = 0,
            .index_page_offset = null,
            .dictionary_page_offset = null,
            .statistics = .{}, // present but no null_count
        },
    });
    const rg2: schema.RowGroup = .{
        .columns = col_chunks2,
        .total_byte_size = 800,
        .num_rows = 100,
    };
    var col_state2 = Accumulator.init(col_call);
    try testing.expect(try updateOneFromStats(&col_state2, col_call, &rg2, &test_meta));
    try testing.expectEqual(@as(u64, 100), col_state2.count);
}

test "statsCoverageComplete: prunable iff every RG has the right stat field" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    // Build a 2-RG file: RG0 has min/max for col 0, RG1 doesn't.
    var min_buf = try a.alloc(u8, 8);
    var max_buf = try a.alloc(u8, 8);
    std.mem.writeInt(i64, min_buf[0..8], 1, .little);
    std.mem.writeInt(i64, max_buf[0..8], 100, .little);

    var path: schema.StringList = .empty;
    try path.append(a, "x");
    var encs: schema.EncodingList = .empty;
    try encs.append(a, .PLAIN);

    const cm_with_stats: schema.ColumnMetaData = .{
        .type = .INT64,
        .encodings = encs,
        .path_in_schema = path,
        .codec = .UNCOMPRESSED,
        .num_values = 100,
        .total_uncompressed_size = 800,
        .total_compressed_size = 800,
        .data_page_offset = 0,
        .index_page_offset = null,
        .dictionary_page_offset = null,
        .statistics = .{ .min_value = min_buf, .max_value = max_buf },
    };
    const cm_without_stats: schema.ColumnMetaData = .{
        .type = .INT64,
        .encodings = encs,
        .path_in_schema = path,
        .codec = .UNCOMPRESSED,
        .num_values = 100,
        .total_uncompressed_size = 800,
        .total_compressed_size = 800,
        .data_page_offset = 0,
        .index_page_offset = null,
        .dictionary_page_offset = null,
        .statistics = null,
    };

    var rg0_chunks: std.ArrayListUnmanaged(schema.ColumnChunk) = .empty;
    try rg0_chunks.append(a, .{ .file_path = null, .file_offset = 0, .meta_data = cm_with_stats });
    var rg1_chunks: std.ArrayListUnmanaged(schema.ColumnChunk) = .empty;
    try rg1_chunks.append(a, .{ .file_path = null, .file_offset = 0, .meta_data = cm_without_stats });

    var rgs_complete: std.ArrayListUnmanaged(schema.RowGroup) = .empty;
    try rgs_complete.append(a, .{ .columns = rg0_chunks, .total_byte_size = 800, .num_rows = 100 });
    try rgs_complete.append(a, .{ .columns = rg0_chunks, .total_byte_size = 800, .num_rows = 100 });
    var rgs_partial: std.ArrayListUnmanaged(schema.RowGroup) = .empty;
    try rgs_partial.append(a, .{ .columns = rg0_chunks, .total_byte_size = 800, .num_rows = 100 });
    try rgs_partial.append(a, .{ .columns = rg1_chunks, .total_byte_size = 800, .num_rows = 100 });

    const meta_complete = try testI64Meta(a, rgs_complete, 200, .REQUIRED);
    const meta_partial = try testI64Meta(a, rgs_partial, 200, .REQUIRED);

    const arg: expr_ast.Expr = .{ .col_ref = .{ .col_idx = 0, .physical_type = .INT64, .expr_type = .i64 } };
    const max_call: AggCall = .{ .func = .max, .arg = arg, .where = null, .alias = "hi", .result = .i64 };
    const sum_call: AggCall = .{ .func = .sum, .arg = arg, .where = null, .alias = "s", .result = .i64 };
    const star_call: AggCall = .{ .func = .count, .arg = null, .where = null, .alias = "n", .result = .i64 };

    // max with full stats coverage → prunable, but ONLY under --trust-stats
    // (reading min/max stats for the answer). Untrusted → must decode → not
    // prunable, so the column can't be dropped from the fetch set.
    try testing.expect(statsCoverageComplete(max_call, &.{meta_complete}, 0, true));
    try testing.expect(!statsCoverageComplete(max_call, &.{meta_complete}, 0, false));
    // max with one RG missing stats → NOT prunable (even when trusting)
    try testing.expect(!statsCoverageComplete(max_call, &.{meta_partial}, 0, true));
    // sum is only prunable when every RG is constant; these stats are variable.
    try testing.expect(!statsCoverageComplete(sum_call, &.{meta_complete}, 0, true));
    const left = try a.create(expr_ast.Expr);
    left.* = arg;
    const right = try a.create(expr_ast.Expr);
    right.* = .{ .literal = .{ .i64 = 1 } };
    const computed_arg: expr_ast.Expr = .{ .binop = .{
        .op = .add,
        .left = left,
        .right = right,
        .result_type = .i64,
        .depth = 1 + arg.depth(),
    } };
    const computed_sum: AggCall = .{ .func = .sum, .arg = computed_arg, .where = null, .alias = "s", .result = .i64 };
    try testing.expect(!statsCoverageComplete(computed_sum, &.{meta_complete}, 0, true));
    // count(*) is answered from num_rows — always prunable, trust or not.
    try testing.expect(statsCoverageComplete(star_call, &.{meta_complete}, 0, true));
    try testing.expect(statsCoverageComplete(star_call, &.{meta_complete}, 0, false));
    try testing.expect(statsCoverageComplete(star_call, &.{meta_partial}, 0, false));

    // `updateOneFromStats` never folds an unsigned column, so its stats cannot let the column go unfetched.
    meta_complete.schema.items[1].converted_type = .UINT_64;
    try testing.expect(!statsCoverageComplete(max_call, &.{meta_complete}, 0, true));
    try testing.expect(statsCoverageComplete(star_call, &.{meta_complete}, 0, true));

    // FLOAT16 decodes to the f64 lane, but no fold reads its 2-byte bounds: it too must stay fetched.
    const leaf = &meta_complete.schema.items[1];
    leaf.converted_type = null;
    leaf.type = .FIXED_LEN_BYTE_ARRAY;
    leaf.type_length = 2;
    leaf.logical_type = .{ .FLOAT16 = .{} };
    const half: expr_ast.Expr = .{ .col_ref = .{
        .col_idx = 0,
        .physical_type = .FIXED_LEN_BYTE_ARRAY,
        .expr_type = .f64,
    } };
    const half_max: AggCall = .{ .func = .max, .arg = half, .where = null, .alias = "hi", .result = .f64 };
    try testing.expect(!statsCoverageComplete(half_max, &.{meta_complete}, 0, true));
}

test "simdSumI64 widens correctly across positive and negative i64 extremes" {
    // Mix of full-range i64 values that cancel close to zero in true
    // arithmetic but would wrap into i64::MAX/MIN territory if
    // accumulated as i64. The expected sum stays well within i64,
    // so finalize won't error — this isolates the kernel itself.
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    const N: usize = 256;
    const xs = try a.alloc(i64, N);
    var i: usize = 0;
    while (i < N) : (i += 1) {
        // Alternating ±(2^62 + 1). Pairwise sum stays bounded so the
        // i64-accumulator path could pass; but if a kernel does e.g.
        // sum-then-narrow, it'd see (2^62 + 1) + (2^62 + 1) = 2^63 + 2
        // (overflow) on adjacent positives. The widening path stays
        // exact.
        const v: i64 = (@as(i64, 1) << 62) + 1;
        xs[i] = if (i % 2 == 0) v else -v;
    }
    var sel = try filter_selection.SelectionVector.init(a, N);
    const got = simdSumI64(xs, &sel);
    try testing.expectEqual(@as(i128, 0), got);

    // Asymmetric: 200 positives, 56 negatives — true sum 144 × (2^62+1).
    for (xs[0..200]) |*x| x.* = (@as(i64, 1) << 62) + 1;
    for (xs[200..]) |*x| x.* = -((@as(i64, 1) << 62) + 1);
    const got2 = simdSumI64(xs, &sel);
    const expected2: i128 = @as(i128, 144) * (@as(i128, 1 << 62) + 1);
    try testing.expectEqual(expected2, got2);
}

test "sum i64 stays exact on values that would overflow a vector i64 accumulator" {
    // Per-lane overflow regression: 128 copies of (i64::MAX / 2 + 1).
    // True sum is 128 × (i64::MAX / 2 + 1) ≈ 2^69, well outside i64.
    // The previous SIMD path (`@Vector(4, i64)` running accumulator)
    // wrapped after just two iterations on this input. The current
    // path widens every add to i128 so the result is exact.
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    const N: usize = 128;
    const big: i64 = (std.math.maxInt(i64) / 2) + 1;
    const xs = try a.alloc(i64, N);
    for (xs) |*x| x.* = big;

    const cols = [_]filter_eval.Batch.Column{.{ .i64 = .{ .values = xs } }};
    const batch: filter_eval.Batch = .{ .cols = &cols, .num_rows = N };
    const lookup = [_]?usize{0};
    var sel = try filter_selection.SelectionVector.init(a, N);

    const arg_expr: expr_ast.Expr = .{ .col_ref = .{
        .col_idx = 0,
        .physical_type = .INT64,
        .expr_type = .i64,
    } };
    const call: AggCall = .{
        .func = .sum,
        .arg = arg_expr,
        .where = null,
        .alias = "s",
        .result = .i64,
    };
    var state = Accumulator.init(call);
    try updateOne(a, a, &state, call, &batch, &lookup, &sel);

    const expected: i128 = @as(i128, N) * @as(i128, big);
    try testing.expectEqual(expected, state.sum_i);
    // Also verify the unsigned magnitude exceeds i64::MAX so this
    // really is the overflow regime (defensive — catches the case
    // where a future refactor accidentally narrows somewhere).
    try testing.expect(expected > std.math.maxInt(i64));
}

test "sum/count over OPTIONAL i64 column with real nulls" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    // Column is OPTIONAL, max_def = 1. Rows 1 and 3 are null
    // (def == 0); rows 0/2/4 are present with values 10/30/50.
    // Placeholder values at null positions are deliberately huge
    // so a buggy fold (one that ignores def_levels) would visibly
    // produce a wrong answer.
    const xs = [_]i64{ 10, 999_999, 30, 999_999, 50 };
    const dls = [_]u32{ 1, 0, 1, 0, 1 };
    const cols = [_]filter_eval.Batch.Column{
        .{ .i64 = .{
            .values = &xs,
            .def_levels = &dls,
            .max_def = 1,
            .has_nulls = true,
        } },
    };
    const batch: filter_eval.Batch = .{ .cols = &cols, .num_rows = 5 };
    const lookup = [_]?usize{0};
    var sel = try filter_selection.SelectionVector.init(a, 5);

    const arg_expr: expr_ast.Expr = .{ .col_ref = .{
        .col_idx = 0,
        .physical_type = .INT64,
        .expr_type = .i64,
    } };
    const sum_call: AggCall = .{
        .func = .sum,
        .arg = arg_expr,
        .where = null,
        .alias = "s",
        .result = .i64,
    };
    const count_call: AggCall = .{
        .func = .count,
        .arg = arg_expr,
        .where = null,
        .alias = "n",
        .result = .i64,
    };
    const min_call: AggCall = .{
        .func = .min,
        .arg = arg_expr,
        .where = null,
        .alias = "lo",
        .result = .i64,
    };
    const max_call: AggCall = .{
        .func = .max,
        .arg = arg_expr,
        .where = null,
        .alias = "hi",
        .result = .i64,
    };

    var sum_state = Accumulator.init(sum_call);
    var count_state = Accumulator.init(count_call);
    var min_state = Accumulator.init(min_call);
    var max_state = Accumulator.init(max_call);
    try updateOne(a, a, &sum_state, sum_call, &batch, &lookup, &sel);
    try updateOne(a, a, &count_state, count_call, &batch, &lookup, &sel);
    try updateOne(a, a, &min_state, min_call, &batch, &lookup, &sel);
    try updateOne(a, a, &max_state, max_call, &batch, &lookup, &sel);

    try testing.expectEqual(@as(i128, 90), sum_state.sum_i);
    try testing.expectEqual(@as(u64, 3), count_state.count);
    try testing.expectEqual(@as(i64, 10), min_state.min_i.?);
    try testing.expectEqual(@as(i64, 50), max_state.max_i.?);
}

test "conditional agg via FILTER predicate" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    // foo: [bar, baz, bar, baz, bar]
    // cost: [10, 20, 30, 40, 50]
    // sum(cost) FILTER (WHERE foo='bar') should be 10+30+50 = 90
    const foo_vals = [_][]const u8{ "bar", "baz", "bar", "baz", "bar" };
    const cost_vals = [_]i64{ 10, 20, 30, 40, 50 };
    const cols = [_]filter_eval.Batch.Column{
        .{ .string = .{ .values = &foo_vals } },
        .{ .i64 = .{ .values = &cost_vals } },
    };
    const batch: filter_eval.Batch = .{ .cols = &cols, .num_rows = 5 };
    const lookup = [_]?usize{ 0, 1 };
    var sel = try filter_selection.SelectionVector.init(a, 5);

    const arg_expr: expr_ast.Expr = .{ .col_ref = .{
        .col_idx = 1,
        .physical_type = .INT64,
        .expr_type = .i64,
    } };
    // WHERE foo='bar' as a filter AST predicate.
    const where: filter_ast.Filter = .{ .string = .{
        .col_idx = 0,
        .op = .Eq,
        .value = "bar",
    } };
    const call: AggCall = .{
        .func = .sum,
        .arg = arg_expr,
        .where = where,
        .alias = "bar_total",
        .result = .i64,
    };
    var state = Accumulator.init(call);
    try updateOne(a, a, &state, call, &batch, &lookup, &sel);
    try testing.expectEqual(@as(i128, 90), state.sum_i);
}

test "sum/avg/min/max over no values are NULL; count is 0" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    // Column 0: all null. Column 1: present, but rows are deselected below. Column 2: strings.
    const nulls = [_]f64{ 7, 7, 7 };
    const zeros = [_]u32{ 0, 0, 0 };
    const ints = [_]i64{ 1, 2, 3 };
    const strs = [_][]const u8{ "a", "b", "c" };
    const cols = [_]filter_eval.Batch.Column{
        .{ .f64 = .{ .values = &nulls, .def_levels = &zeros, .max_def = 1, .has_nulls = true } },
        .{ .i64 = .{ .values = &ints } },
        .{ .string = .{ .values = &strs } },
    };
    const batch: filter_eval.Batch = .{ .cols = &cols, .num_rows = 3 };
    const lookup = [_]?usize{ 0, 1, 2 };
    var all = try filter_selection.SelectionVector.init(a, 3);
    var none = try filter_selection.SelectionVector.init(a, 3);
    for (0..3) |i| none.set(i, false);

    const f_null: expr_ast.Expr = .{ .col_ref = .{ .col_idx = 0, .physical_type = .DOUBLE, .expr_type = .f64 } };
    const i_col: expr_ast.Expr = .{ .col_ref = .{ .col_idx = 1, .physical_type = .INT64, .expr_type = .i64 } };
    const s_col: expr_ast.Expr = .{ .col_ref = .{ .col_idx = 2, .physical_type = .BYTE_ARRAY, .expr_type = .str } };
    const never: filter_ast.Filter = .{ .int64 = .{ .col_idx = 1, .op = .Lt, .value = 0 } };
    const Case = struct { call: AggCall, sel: *filter_selection.SelectionVector };
    const cases = [_]Case{
        // all-null input
        .{ .call = .{ .func = .sum, .arg = f_null, .where = null, .alias = "a", .result = .f64 }, .sel = &all },
        .{ .call = .{ .func = .min, .arg = f_null, .where = null, .alias = "b", .result = .f64 }, .sel = &all },
        .{ .call = .{ .func = .avg, .arg = f_null, .where = null, .alias = "c", .result = .avg_f64 }, .sel = &all },
        // zero rows selected by the outer filter
        .{ .call = .{ .func = .sum, .arg = i_col, .where = null, .alias = "d", .result = .i64 }, .sel = &none },
        .{ .call = .{ .func = .max, .arg = i_col, .where = null, .alias = "e", .result = .i64 }, .sel = &none },
        .{ .call = .{ .func = .min, .arg = s_col, .where = null, .alias = "f", .result = .bytes }, .sel = &none },
        // a FILTER clause that selects nothing
        .{ .call = .{ .func = .sum, .arg = i_col, .where = never, .alias = "g", .result = .i64 }, .sel = &all },
        .{ .call = .{ .func = .max, .arg = s_col, .where = never, .alias = "h", .result = .bytes }, .sel = &all },
    };
    for (cases) |c| {
        var state = Accumulator.init(c.call);
        try updateOne(a, a, &state, c.call, &batch, &lookup, c.sel);
        // A second, equally empty partial merged in must not turn NULL into 0.
        state.merge(Accumulator.init(c.call), a);
        const outs = try finalize(a, c.call, state);
        // avg's count column stays a REQUIRED 0; every other output is a NULL.
        for (outs) |o| {
            if (std.mem.endsWith(u8, o.name, "__count")) {
                try testing.expect(!o.nullable);
                try testing.expectEqual(@as(i64, 0), o.col.i64.values[0]);
                continue;
            }
            try testing.expect(o.nullable);
            switch (o.col) {
                inline else => |col| {
                    try testing.expect(col.has_nulls);
                    try testing.expectEqualSlices(u32, &.{0}, col.def_levels.?);
                },
            }
        }
    }

    // count(*) and count(col) over the same inputs are 0, not NULL.
    const count_col: AggCall = .{ .func = .count, .arg = f_null, .where = null, .alias = "n", .result = .i64 };
    var cs = Accumulator.init(count_col);
    try updateOne(a, a, &cs, count_col, &batch, &lookup, &all);
    const cout = try finalize(a, count_col, cs);
    try testing.expect(!cout[0].nullable);
    try testing.expectEqual(@as(i64, 0), cout[0].col.i64.values[0]);

    // NULL is the identity for merging partials: NULL + 6 = 6.
    const sum_call: AggCall = .{ .func = .sum, .arg = i_col, .where = null, .alias = "s", .result = .i64 };
    var empty = Accumulator.init(sum_call);
    var full = Accumulator.init(sum_call);
    try updateOne(a, a, &full, sum_call, &batch, &lookup, &all);
    empty.merge(full, a);
    try testing.expectEqual(@as(?i128, 6), empty.sum_i);
}

test "grouped sum/min over a group with only null values stays NULL" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    // Group 0 holds rows 0 and 2 (both null); group 1 holds row 1 (value 5).
    const vals = [_]i64{ 9, 5, 9 };
    const dls = [_]u32{ 0, 1, 0 };
    const cols = [_]filter_eval.Batch.Column{
        .{ .i64 = .{ .values = &vals, .def_levels = &dls, .max_def = 1, .has_nulls = true } },
    };
    const batch: filter_eval.Batch = .{ .cols = &cols, .num_rows = 3 };
    const lookup = [_]?usize{0};
    const sel = try filter_selection.SelectionVector.init(a, 3);
    const gids = [_]u32{ 0, 1, 0 };
    const arg: expr_ast.Expr = .{ .col_ref = .{ .col_idx = 0, .physical_type = .INT64, .expr_type = .i64 } };
    const calls = [_]AggCall{
        .{ .func = .sum, .arg = arg, .where = null, .alias = "s", .result = .i64 },
        .{ .func = .min, .arg = arg, .where = null, .alias = "m", .result = .i64 },
    };
    var accs: [4]Accumulator = undefined;
    for (0..2) |g| for (calls, 0..) |c, i| {
        accs[g * 2 + i] = Accumulator.init(c);
    };
    for (calls, 0..) |c, i| try updateOneGrouped(a, a, &accs, &gids, i, calls.len, c, &batch, &lookup, &sel);
    try testing.expectEqual(@as(?i128, null), accs[0].sum_i);
    try testing.expectEqual(@as(?i128, null), accs[1].min_i);
    try testing.expectEqual(@as(?i128, 5), accs[2].sum_i);
    try testing.expectEqual(@as(?i128, 5), accs[3].min_i);
}

pub fn isRowNull(col: filter_eval.Batch.Column, r: usize) bool {
    return switch (col) {
        inline else => |c| if (c.def_levels) |dl| dl[r] < c.max_def else false,
    };
}

/// Memo of (ptr, len) -> group id for the dominant GROUP BY shape, a single string key. Dictionary-encoded values are
/// slices into one cached buffer, so this collapses one key hash per *row* into one per *distinct dictionary value*.
/// Strictly an accelerator: it hits only on exact (ptr, len) identity, `getOrInsert` stays the sole creator of groups,
/// and null rows take the slow path that preserves null-vs-empty-string semantics.
///
/// LIFETIME: keys are raw pointers into the decode arena. One resolver per scan call, never across row groups — a
/// reset arena can reissue an address and alias a stale entry to the wrong group.
pub const GroupKeyResolver = struct {
    scratch: std.ArrayList(u8),
    memo: std.HashMapUnmanaged(SliceId, u32, SliceIdContext, std.hash_map.default_max_load_percentage),
    fast_path: bool,

    const SliceId = struct { ptr: usize, len: usize };

    /// `AutoHashMap` would wyhash all 16 bytes of `SliceId` on this per-row path. Mixing the pointer alone suffices —
    /// dictionary entries sit at well-spread addresses — and `len` still participates in `eql`, so identity stays exact
    /// under any collision.
    const SliceIdContext = struct {
        pub fn hash(_: SliceIdContext, k: SliceId) u64 {
            const p: u64 = k.ptr;
            return (p ^ (p >> 32)) *% 0x9E3779B97F4A7C15;
        }
        pub fn eql(_: SliceIdContext, a: SliceId, b: SliceId) bool {
            return a.ptr == b.ptr and a.len == b.len;
        }
    };

    pub fn init(key_cols: []const filter_eval.Batch.Column) GroupKeyResolver {
        return .{
            .scratch = .empty,
            .memo = .empty,
            .fast_path = key_cols.len == 1 and key_cols[0] == .string,
        };
    }

    pub fn deinit(self: *GroupKeyResolver, allocator: std.mem.Allocator) void {
        self.scratch.deinit(allocator);
        self.memo.deinit(allocator);
    }

    pub fn resolve(
        self: *GroupKeyResolver,
        allocator: std.mem.Allocator,
        table: *GroupTable,
        key_cols: []const filter_eval.Batch.Column,
        row: usize,
        agg_calls: []const AggCall,
    ) !u32 {
        if (self.fast_path and !isRowNull(key_cols[0], row)) {
            const s = key_cols[0].string.values[row];
            const id: SliceId = .{ .ptr = @intFromPtr(s.ptr), .len = s.len };
            const gop = try self.memo.getOrPut(allocator, id);
            if (gop.found_existing) return gop.value_ptr.*;
            // On error the entry just reserved would hold an undefined value.
            errdefer _ = self.memo.remove(id);
            try serializeRowKey(&self.scratch, allocator, key_cols, row);
            const gid = try table.getOrInsert(self.scratch.items, agg_calls);
            gop.value_ptr.* = gid;
            return gid;
        }

        try serializeRowKey(&self.scratch, allocator, key_cols, row);
        return table.getOrInsert(self.scratch.items, agg_calls);
    }
};

pub fn serializeRowKey(
    list: *std.ArrayList(u8),
    allocator: std.mem.Allocator,
    key_cols: []const filter_eval.Batch.Column,
    row: usize,
) !void {
    list.clearRetainingCapacity();

    for (key_cols) |col| {
        if (isRowNull(col, row)) {
            try list.append(allocator, 0); // 0 = null
        } else {
            try list.append(allocator, 1); // 1 = present
            switch (col) {
                .i32 => |c| {
                    var buf: [8]u8 = undefined;
                    std.mem.writeInt(i64, &buf, @intCast(c.values[row]), .little);
                    try list.appendSlice(allocator, &buf);
                },
                .i64 => |c| {
                    var buf: [8]u8 = undefined;
                    std.mem.writeInt(i64, &buf, c.values[row], .little);
                    try list.appendSlice(allocator, &buf);
                },
                .f32 => |c| {
                    var f: f64 = @floatCast(c.values[row]);
                    if (std.math.isNan(f)) {
                        f = std.math.nan(f64);
                    } else if (f == 0.0) {
                        f = 0.0;
                    }
                    var buf: [8]u8 = undefined;
                    std.mem.writeInt(u64, &buf, @bitCast(f), .little);
                    try list.appendSlice(allocator, &buf);
                },
                .f64 => |c| {
                    var f = c.values[row];
                    if (std.math.isNan(f)) {
                        f = std.math.nan(f64);
                    } else if (f == 0.0) {
                        f = 0.0;
                    }
                    var buf: [8]u8 = undefined;
                    std.mem.writeInt(u64, &buf, @bitCast(f), .little);
                    try list.appendSlice(allocator, &buf);
                },
                .string => |c| {
                    const s = c.values[row];
                    var len_buf: [4]u8 = undefined;
                    std.mem.writeInt(u32, &len_buf, @intCast(s.len), .little);
                    try list.appendSlice(allocator, &len_buf);
                    try list.appendSlice(allocator, s);
                },
                .boolean => |c| {
                    // Same framing as evalGroupKeyExpr: BOOLEAN widens to the
                    // i64 lane. deserializeKeyColumn only knows {i64,f64,string},
                    // so a 1-byte write here would disagree with the reader.
                    var buf: [8]u8 = undefined;
                    std.mem.writeInt(i64, &buf, if (c.values[row]) 1 else 0, .little);
                    try list.appendSlice(allocator, &buf);
                },
            }
        }
    }
}

/// One `max_memory` shared by every group table filling in parallel, so required headroom stops scaling with `-j`
/// (see `scan.zig`). Usage never exceeds `limit`, but admission near it is approximate — tables draw in blocks and can
/// sit on part of one — because exact per-entry accounting would put a contended atomic on every insertion. Stranded
/// residue is at most ~5% of the budget in aggregate regardless of worker count (see `blockFor`).
pub const SharedBudget = struct {
    used: std.atomic.Value(usize) = .init(0),
    limit: usize,
    block: usize = 64 * 1024,

    /// Blocks keep the shared counter off the per-group path: the counter is cheap, but many workers invalidating one
    /// cache line is not. The cap stops a barely-grouping worker from stranding quota an active table needs.
    const BlockMax: usize = 64 * 1024;

    /// Caps blocks so all tables together strand at most ~5% of the budget. A per-worker fraction would be far wider:
    /// early workers can reserve most of the pool between them.
    pub fn blockFor(limit: usize, n_tables: usize) usize {
        return @max(1, @min(BlockMax, limit / @max(1, n_tables) / 20));
    }

    fn blockSize(self: *const SharedBudget) usize {
        return self.block;
    }

    /// Near the ceiling, held slack can fail a table while total use is under the limit, so blocks are dropped once
    /// the pool is tight — exact admission where it is observable, atomic off the hot path elsewhere.
    pub fn tight(self: *const SharedBudget) bool {
        return self.used.load(.monotonic) >= self.limit - self.limit / 4;
    }

    pub fn draw(self: *SharedBudget, need: usize) ?usize {
        const first: usize = if (self.tight()) need else @max(need, self.blockSize());
        for ([_]usize{ first, need }) |want| {
            var cur = self.used.load(.monotonic);
            while (cur + want <= self.limit) {
                if (self.used.cmpxchgWeak(cur, cur + want, .monotonic, .monotonic)) |actual| {
                    cur = actual;
                    continue;
                }
                return want;
            }
        }
        return null;
    }

    pub fn release(self: *SharedBudget, bytes: usize) void {
        if (bytes == 0) return;
        _ = self.used.fetchSub(bytes, .monotonic);
    }
};

pub const GroupTable = struct {
    allocator: std.mem.Allocator,
    keys: std.ArrayList([]const u8),
    accumulators: std.ArrayList(Accumulator),
    map: std.StringHashMap(u32),
    allocated_bytes: usize,
    max_memory_bytes: usize,
    shared: ?*SharedBudget = null,

    const MapNodeOverhead = 48; // Size of StringHashMap node + bucket overhead

    pub fn init(allocator: std.mem.Allocator, max_memory_bytes: usize) GroupTable {
        return .{
            .allocator = allocator,
            .keys = .empty,
            .accumulators = .empty,
            .map = std.StringHashMap(u32).init(allocator),
            .allocated_bytes = 0,
            .max_memory_bytes = max_memory_bytes,
        };
    }

    pub fn deinit(self: *GroupTable) void {
        // A shared table starts at a zero ceiling, so all of `max_memory_bytes` came from the pool.
        if (self.shared) |budget| budget.release(self.max_memory_bytes);
        for (self.keys.items) |key| {
            self.allocator.free(key);
        }
        self.keys.deinit(self.allocator);

        for (self.accumulators.items) |acc| {
            switch (acc) {
                .min_bytes => |mb| if (mb) |s| self.allocator.free(s),
                .max_bytes => |mb| if (mb) |s| self.allocator.free(s),
                else => {},
            }
        }
        self.accumulators.deinit(self.allocator);
        self.map.deinit();
    }

    /// Owning thread only. A table that has stopped growing must not sit on a partly-used block while another worker
    /// is still filling, or admission depends on how row groups happened to be scheduled.
    pub fn releaseSlack(self: *GroupTable) void {
        const budget = self.shared orelse return;
        const slack = self.max_memory_bytes - self.allocated_bytes;
        if (slack == 0) return;
        self.max_memory_bytes = self.allocated_bytes;
        budget.release(slack);
    }

    pub fn getOrInsert(self: *GroupTable, key: []const u8, agg_calls: []const AggCall) !u32 {
        if (self.map.get(key)) |id| {
            return id;
        }

        const entry_size = key.len + @sizeOf(Accumulator) * agg_calls.len + MapNodeOverhead;
        if (self.allocated_bytes + entry_size > self.max_memory_bytes) {
            const budget = self.shared orelse return error.ExceededMemoryBudget;
            // Checked on the draw path rather than per new group, to keep the shared atomic off the insertion path.
            if (budget.tight()) self.releaseSlack();
            const deficit = (self.allocated_bytes + entry_size) - self.max_memory_bytes;
            const got = budget.draw(deficit) orelse blk: {
                // Our own unused block may be part of why the pool is empty.
                self.releaseSlack();
                break :blk budget.draw(entry_size) orelse
                    return error.ExceededMemoryBudget;
            };
            self.max_memory_bytes += got;
        }

        const key_copy = try self.allocator.dupe(u8, key);
        errdefer self.allocator.free(key_copy);

        const group_id = @as(u32, @intCast(self.keys.items.len));

        const old_accumulators_len = self.accumulators.items.len;
        try self.accumulators.ensureUnusedCapacity(self.allocator, agg_calls.len);
        errdefer self.accumulators.shrinkRetainingCapacity(old_accumulators_len);
        for (agg_calls) |call| {
            self.accumulators.appendAssumeCapacity(Accumulator.init(call));
        }

        try self.map.put(key_copy, group_id);
        errdefer _ = self.map.remove(key_copy);

        try self.keys.append(self.allocator, key_copy);

        self.allocated_bytes += entry_size;

        return group_id;
    }
};

pub fn updateOneGrouped(
    arena: std.mem.Allocator,
    persist: std.mem.Allocator,
    accs: []Accumulator,
    group_id_arr: []const u32,
    agg_idx: usize,
    agg_len: usize,
    call: AggCall,
    batch: *const filter_eval.Batch,
    column_lookup: []const ?usize,
    outer_sel: *const filter_selection.SelectionVector,
) Error!void {
    var sel_owned: ?filter_selection.SelectionVector = null;
    defer if (sel_owned) |*s| s.deinit();

    if (call.where) |pred| {
        sel_owned = try outer_sel.cloneAlloc(arena);
        try filter_eval.evaluate(pred, batch, &sel_owned.?, column_lookup, arena);
    }

    var values: ?filter_eval.Batch.Column = null;
    if (call.arg) |arg| {
        if (arg == .col_ref) {
            values = try colRefForAgg(arena, batch, column_lookup, arg.col_ref, outer_sel, &sel_owned);
        } else {
            values = try expr_eval.evalExpr(arena, batch, column_lookup, arg);
        }
    }
    defer if (values) |v| if (call.arg.? == .col_ref) freeWidened(arena, batch, column_lookup, call.arg.?.col_ref, v);

    const sel: *const filter_selection.SelectionVector =
        if (sel_owned) |*s| s else outer_sel;

    const u64_col = if (call.arg) |a| (a == .col_ref and a.col_ref.unsigned_64) else false;

    var r: usize = 0;
    while (r < sel.len) : (r += 1) {
        if (sel.isActive(r)) {
            const gid = group_id_arr[r];
            const state = &accs[gid * agg_len + agg_idx];
            switch (call.func) {
                .count => state.count += 1,
                .sum => try foldSumGroupedOne(state, values.?, r, u64_col),
                .min => if (call.result == .bytes)
                    try foldMinMaxBytesGroupedOne(persist, true, state, values.?, r)
                else
                    foldMinGroupedOne(state, values.?, r, u64_col),
                .max => if (call.result == .bytes)
                    try foldMinMaxBytesGroupedOne(persist, false, state, values.?, r)
                else
                    foldMaxGroupedOne(state, values.?, r, u64_col),
                .avg => try foldAvgGroupedOne(state, values.?, r),
            }
        }
    }
}

fn foldSumGroupedOne(state: *Accumulator, col: filter_eval.Batch.Column, r: usize, unsigned_64: bool) Error!void {
    switch (state.*) {
        .sum_i => |*s| {
            const v = switch (col) {
                .i32 => |c| @as(i128, c.values[r]),
                .i64 => |c| if (unsigned_64) @as(i128, @intCast(@as(u64, @bitCast(c.values[r])))) else @as(i128, c.values[r]),
                else => return error.UnsupportedAggType,
            };
            s.* = (s.* orelse 0) + v;
        },
        .sum_f => |*s| {
            const v = switch (col) {
                .f32 => |c| @as(f64, c.values[r]),
                .f64 => |c| c.values[r],
                else => return error.UnsupportedAggType,
            };
            s.* = (s.* orelse 0) + v;
        },
        else => return error.UnsupportedAggType,
    }
}

fn foldMinGroupedOne(state: *Accumulator, col: filter_eval.Batch.Column, r: usize, unsigned_64: bool) void {
    switch (state.*) {
        .min_i => |*m| {
            const v = switch (col) {
                .i32 => |c| @as(i128, c.values[r]),
                .i64 => |c| if (unsigned_64) @as(i128, @intCast(@as(u64, @bitCast(c.values[r])))) else @as(i128, c.values[r]),
                else => unreachable,
            };
            if (m.* == null or v < m.*.?) m.* = v;
        },
        .min_f => |*m| {
            const v = switch (col) {
                .f32 => |c| @as(f64, c.values[r]),
                .f64 => |c| c.values[r],
                else => unreachable,
            };
            if (m.* == null or v < m.*.?) m.* = v;
        },
        else => unreachable,
    }
}

fn foldMaxGroupedOne(state: *Accumulator, col: filter_eval.Batch.Column, r: usize, unsigned_64: bool) void {
    switch (state.*) {
        .max_i => |*m| {
            const v = switch (col) {
                .i32 => |c| @as(i128, c.values[r]),
                .i64 => |c| if (unsigned_64) @as(i128, @intCast(@as(u64, @bitCast(c.values[r])))) else @as(i128, c.values[r]),
                else => unreachable,
            };
            if (m.* == null or v > m.*.?) m.* = v;
        },
        .max_f => |*m| {
            const v = switch (col) {
                .f32 => |c| @as(f64, c.values[r]),
                .f64 => |c| c.values[r],
                else => unreachable,
            };
            if (m.* == null or v > m.*.?) m.* = v;
        },
        else => unreachable,
    }
}

fn foldMinMaxBytesGroupedOne(
    persist: std.mem.Allocator,
    is_min: bool,
    state: *Accumulator,
    col: filter_eval.Batch.Column,
    r: usize,
) !void {
    const sv = col.string.values[r];
    const m_ptr = if (is_min) &state.min_bytes else &state.max_bytes;
    if (m_ptr.* == null) {
        m_ptr.* = try persist.dupe(u8, sv);
    } else {
        const ord = std.mem.order(u8, sv, m_ptr.*.?);
        const condition = if (is_min) (ord == .lt) else (ord == .gt);
        if (condition) {
            persist.free(m_ptr.*.?);
            m_ptr.* = try persist.dupe(u8, sv);
        }
    }
}

fn foldAvgGroupedOne(state: *Accumulator, col: filter_eval.Batch.Column, r: usize) !void {
    const v = switch (col) {
        .i32 => |c| @as(f64, @floatFromInt(c.values[r])),
        .i64 => |c| @as(f64, @floatFromInt(c.values[r])),
        .f32 => |c| @as(f64, c.values[r]),
        .f64 => |c| c.values[r],
        else => return error.UnsupportedAggType,
    };
    state.avg.sum += v;
    state.avg.count += 1;
}

test "GroupTable basic grouping, float normalization, and memory capping" {
    const allocator = std.testing.allocator;

    var gt = GroupTable.init(allocator, 1024);
    defer gt.deinit();

    const agg_calls = &[_]AggCall{
        .{
            .func = .sum,
            .alias = "sum_val",
            .result = .i64,
            .arg = null,
            .where = null,
        },
    };

    const gid1 = try gt.getOrInsert("group_a", agg_calls);
    const gid2 = try gt.getOrInsert("group_b", agg_calls);
    const gid1_dup = try gt.getOrInsert("group_a", agg_calls);

    try std.testing.expect(gid1 == 0);
    try std.testing.expect(gid2 == 1);
    try std.testing.expect(gid1_dup == 0);

    var f32_vals_1 = [_]f32{0.0};
    const f32_col_1 = filter_eval.Batch.Column{
        .f32 = .{ .values = &f32_vals_1 },
    };
    var f32_vals_2 = [_]f32{-0.0};
    const f32_col_2 = filter_eval.Batch.Column{
        .f32 = .{ .values = &f32_vals_2 },
    };
    var key_scratch: std.ArrayList(u8) = .empty;
    defer key_scratch.deinit(allocator);

    try serializeRowKey(&key_scratch, allocator, &[_]filter_eval.Batch.Column{f32_col_1}, 0);
    const key1 = try allocator.dupe(u8, key_scratch.items);
    defer allocator.free(key1);

    try serializeRowKey(&key_scratch, allocator, &[_]filter_eval.Batch.Column{f32_col_2}, 0);
    const key2 = try allocator.dupe(u8, key_scratch.items);
    defer allocator.free(key2);

    try std.testing.expectEqualSlices(u8, key1, key2);

    var f32_nan_1 = [_]f32{std.math.nan(f32)};
    const f32_nan_col_1 = filter_eval.Batch.Column{
        .f32 = .{ .values = &f32_nan_1 },
    };
    var f32_nan_2 = [_]f32{std.math.nan(f32)};
    const f32_nan_col_2 = filter_eval.Batch.Column{
        .f32 = .{ .values = &f32_nan_2 },
    };
    try serializeRowKey(&key_scratch, allocator, &[_]filter_eval.Batch.Column{f32_nan_col_1}, 0);
    const nan_key1 = try allocator.dupe(u8, key_scratch.items);
    defer allocator.free(nan_key1);

    try serializeRowKey(&key_scratch, allocator, &[_]filter_eval.Batch.Column{f32_nan_col_2}, 0);
    const nan_key2 = try allocator.dupe(u8, key_scratch.items);
    defer allocator.free(nan_key2);

    try std.testing.expectEqualSlices(u8, nan_key1, nan_key2);

    var bool_true = [_]bool{true};
    const bool_col = filter_eval.Batch.Column{ .boolean = .{ .values = &bool_true } };
    var i64_one = [_]i64{1};
    const i64_col = filter_eval.Batch.Column{ .i64 = .{ .values = &i64_one } };
    try serializeRowKey(&key_scratch, allocator, &[_]filter_eval.Batch.Column{bool_col}, 0);
    const bool_key = try allocator.dupe(u8, key_scratch.items);
    defer allocator.free(bool_key);
    try serializeRowKey(&key_scratch, allocator, &[_]filter_eval.Batch.Column{i64_col}, 0);
    try std.testing.expectEqualSlices(u8, bool_key, key_scratch.items);
    try std.testing.expectEqual(@as(usize, 1 + 8), bool_key.len);

    var small_gt = GroupTable.init(allocator, 10);
    defer small_gt.deinit();
    try std.testing.expectError(error.ExceededMemoryBudget, small_gt.getOrInsert("too_large_key_that_exceeds_budget", agg_calls));
}

test "SharedBudget: tables share one budget instead of owning fixed slices" {
    const allocator = std.testing.allocator;
    const agg_calls = [_]AggCall{.{ .func = .count, .arg = null, .where = null, .alias = "n", .result = .i64 }};

    // Two tables under one budget, as parallel workers are: the only busy one may take the whole budget.
    var budget = SharedBudget{ .limit = 4096 };
    var a = GroupTable.init(allocator, 0);
    a.shared = &budget;
    defer a.deinit();
    var b = GroupTable.init(allocator, 0);
    b.shared = &budget;
    defer b.deinit();

    var key_buf: [16]u8 = undefined;
    var placed: usize = 0;
    while (placed < 500) : (placed += 1) {
        const key = std.fmt.bufPrint(&key_buf, "k{d}", .{placed}) catch unreachable;
        _ = b.getOrInsert(key, &agg_calls) catch break;
    }
    try std.testing.expect(b.keys.items.len > 0);
    try std.testing.expect(budget.used.load(.monotonic) > 4096 / 2);
    try std.testing.expect(a.max_memory_bytes == 0); // the idle table reserved nothing

    var full = SharedBudget{ .limit = 8 };
    var c = GroupTable.init(allocator, 0);
    c.shared = &full;
    defer c.deinit();
    try std.testing.expectError(error.ExceededMemoryBudget, c.getOrInsert("too-big", &agg_calls));

    // Freed tables return their quota: workers merge, then free.
    var recycle = SharedBudget{ .limit = 4096 };
    {
        var tmp = GroupTable.init(allocator, 0);
        tmp.shared = &recycle;
        _ = try tmp.getOrInsert("k", &agg_calls);
        try std.testing.expect(recycle.used.load(.monotonic) > 0);
        tmp.deinit();
    }
    try std.testing.expectEqual(@as(usize, 0), recycle.used.load(.monotonic));
}
