//! Window functions over a Parquet file that is already in window order. The file's rows are sorted by the window's
//! PARTITION BY columns and then its ORDER BY columns (the SQL layer has the engine's own ORDER BY do that), so a
//! partition is a run of rows equal on the first and a peer group a run equal on the second. One pass finds the runs,
//! each call is computed over them, and the file's columns plus one column per call are written to a new file.
//!
//! The whole file is held in memory (decoded, then the answer beside it): the same bound as an ORDER BY with no LIMIT,
//! refused as `ExceededMemoryBudget` past `--max-memory`. Columns are carried in the lane they were decoded in, so a
//! value passes through to the output exactly as it was read; only the new columns are computed.
const std = @import("std");
const schema = @import("../core/schema.zig");
const metadata = @import("../core/parquet/metadata.zig");
const column_mod = @import("../core/parquet/column.zig");
const decimal_mod = @import("../core/parquet/decimal.zig");
const consumer = @import("../core/consumer.zig");
const consumer_column = @import("../core/consumer/column.zig");
const filter_eval = @import("../core/filter/eval.zig");
const expr_agg = @import("../core/expr/agg.zig");
const expr_ast = @import("../core/expr/ast.zig");
const streaming = @import("../core/writer/streaming.zig");
const local_fs = @import("../local_fs.zig");
const write = @import("write.zig");
const core_memory = @import("../core/memory.zig");
const sql_literals = @import("../core/expr/sql_literals.zig");

const Column = filter_eval.Batch.Column;
const OutputCol = expr_agg.OutputCol;

const plan_types = @import("../core/expr/window_plan.zig");
pub const Func = plan_types.Func;
pub const Bound = plan_types.Bound;
pub const Frame = plan_types.Frame;
pub const Default = plan_types.Default;

/// A call with its argument resolved to a column of the table.
pub const Call = struct {
    func: Func,
    arg: ?usize = null,
    offset: u32 = 1,
    default: Default = .null,
    frame: Frame = .{},
};

pub const Spec = struct {
    partition: []const usize,
    order: []const usize,
    /// The ORDER BY key sorts descending (it is then the only one, where a call reaches by an offset).
    descending: bool = false,
    calls: []const Call,
};

pub const Error = error{
    /// A column of a type the pass does not carry (nested, INT96, fixed-length bytes that are no DECIMAL).
    WindowColumnType,
    /// More rows than the pass indexes.
    WindowTooManyRows,
    /// `lag` / `lead` default of a type the column's lane does not take.
    WindowDefaultType,
    /// A SUM past what its result type holds.
    WindowSumOverflow,
    /// A window function over a type it has no meaning for (SUM of text).
    WindowArgumentType,
    /// SUM or AVG of a DOUBLE or FLOAT over a window.
    WindowFloatAggregate,
    /// A RANGE offset on a key that is no integer, DECIMAL, DOUBLE, DATE or TIMESTAMP (without a time zone), or of a kind the
    /// key does not take (an INTERVAL on a number, a number on a TIMESTAMP).
    WindowRangeKey,
    /// A RANGE offset that reaches the wrong way: DuckDB's "Invalid RANGE PRECEDING value".
    WindowRangeOffset,
    /// A key and its RANGE offset that add up past the key's type (a DECIMAL's 38 digits, a DATE's range).
    WindowRangeOverflow,
    /// A key or a RANGE offset that does not fit the type DuckDB brings both to before the sum: 38 digits, once a
    /// DECIMAL key and a fractional offset are at one scale, which DuckDB reports as a Conversion Error.
    WindowRangeCast,
    ExceededMemoryBudget,
};

/// The columns of a file in memory, each in the lane it decoded to; a DECIMAL is its unscaled integers.
pub const Table = struct {
    rows: usize,
    cols: []const Column,
    elems: []const schema.SchemaElement,
    /// Column `i`'s DECIMAL type, when it has one.
    decs: []const ?expr_ast.Dec,
};

// ---- cells

fn nullAt(c: Column, i: usize) bool {
    return switch (c) {
        inline else => |x| if (x.def_levels) |dl| dl[i] < x.max_def else false,
    };
}

fn floatSame(a: anytype, b: @TypeOf(a)) bool {
    return a == b or (std.math.isNan(a) and std.math.isNan(b));
}

/// Whether rows `i` and `j` hold the same value: NULL equals NULL and NaN equals NaN, as PARTITION BY and ORDER BY
/// peers do.
fn sameAt(c: Column, i: usize, j: usize) bool {
    const ni = nullAt(c, i);
    const nj = nullAt(c, j);
    if (ni or nj) return ni and nj;
    return switch (c) {
        .f32 => |x| floatSame(x.values[i], x.values[j]),
        .f64 => |x| floatSame(x.values[i], x.values[j]),
        .string => |x| std.mem.eql(u8, x.values[i], x.values[j]),
        inline else => |x| x.values[i] == x.values[j],
    };
}

fn floatOrder(a: anytype, b: @TypeOf(a)) std.math.Order {
    const na = std.math.isNan(a);
    const nb = std.math.isNan(b);
    if (na or nb) return if (na and nb) .eq else if (na) .gt else .lt;
    return std.math.order(a, b);
}

/// Rows `i` and `j`, neither NULL, in value order (NaN above every number, as DuckDB orders it). An unsigned integer sits in
/// its signed lane as raw bits, and is ordered as the unsigned number it is.
fn orderAt(c: Column, i: usize, j: usize, unsigned: bool) std.math.Order {
    if (unsigned) switch (c) {
        .i32 => |x| return std.math.order(@as(u32, @bitCast(x.values[i])), @as(u32, @bitCast(x.values[j]))),
        .i64 => |x| return std.math.order(@as(u64, @bitCast(x.values[i])), @as(u64, @bitCast(x.values[j]))),
        else => {},
    };
    return switch (c) {
        .f32 => |x| floatOrder(x.values[i], x.values[j]),
        .f64 => |x| floatOrder(x.values[i], x.values[j]),
        .string => |x| std.mem.order(u8, x.values[i], x.values[j]),
        .boolean => |x| std.math.order(@intFromBool(x.values[i]), @intFromBool(x.values[j])),
        inline else => |x| std.math.order(x.values[i], x.values[j]),
    };
}

// ---- partitions and peers

const Runs = struct {
    /// Per row: the first row of its partition, the row after its partition's last, the first row of its peer group
    /// and the row after the group's last; and the number of peer groups the partition has had up to the row.
    pstart: []u32,
    pend: []u32,
    gstart: []u32,
    gend: []u32,
    dense: []u32,
};

fn runs(a: std.mem.Allocator, t: Table, spec: Spec) !Runs {
    const n = t.rows;
    const r: Runs = .{
        .pstart = try a.alloc(u32, n),
        .pend = try a.alloc(u32, n),
        .gstart = try a.alloc(u32, n),
        .gend = try a.alloc(u32, n),
        .dense = try a.alloc(u32, n),
    };
    for (0..n) |i| {
        var new_part = i == 0;
        var new_peer = i == 0;
        if (i > 0) {
            for (spec.partition) |c| if (!sameAt(t.cols[c], i - 1, i)) {
                new_part = true;
            };
            new_peer = new_part;
            if (!new_peer) for (spec.order) |c| if (!sameAt(t.cols[c], i - 1, i)) {
                new_peer = true;
            };
        }
        const me: u32 = @intCast(i);
        r.pstart[i] = if (new_part) me else r.pstart[i - 1];
        r.gstart[i] = if (new_peer) me else r.gstart[i - 1];
        r.dense[i] = if (new_part) 1 else r.dense[i - 1] + @intFromBool(new_peer);
    }
    var i = n;
    while (i > 0) {
        i -= 1;
        const next: u32 = @intCast(i + 1);
        r.pend[i] = if (i + 1 == n or r.pstart[i + 1] == next) next else r.pend[i + 1];
        r.gend[i] = if (i + 1 == n or r.gstart[i + 1] == next) next else r.gend[i + 1];
    }
    return r;
}

// ---- frames

const Span = struct { lo: u32, hi: u32 };

/// The rows `[lo, hi)` the call's frame covers for row `i`; `lo >= hi` is an empty frame. A RANGE frame is read off the
/// peers (with no ORDER BY every row of the partition is one), and has no offsets: those are `rangeFrames`'.
fn frameOf(f: Frame, r: Runs, i: usize) Span {
    const ps: i64 = r.pstart[i];
    const pe: i64 = r.pend[i];
    const row: i64 = @intCast(i);
    var lo: i64 = undefined;
    var hi: i64 = undefined;
    if (!f.rows) {
        lo = switch (f.start) {
            .unbounded_preceding => ps,
            .current_row => r.gstart[i],
            else => unreachable,
        };
        hi = switch (f.end) {
            .current_row => r.gend[i],
            .unbounded_following => pe,
            else => unreachable,
        };
    } else {
        lo = switch (f.start) {
            .unbounded_preceding => ps,
            .preceding => |k| row - k,
            .current_row => row,
            .following => |k| row + k,
            .unbounded_following => pe,
            else => unreachable,
        };
        hi = switch (f.end) {
            .unbounded_preceding => ps,
            .preceding => |k| row - k + 1,
            .current_row => row + 1,
            .following => |k| row + k + 1,
            .unbounded_following => pe,
            else => unreachable,
        };
    }
    // Both ends inside the partition, a frame that ends before it starts empty.
    lo = std.math.clamp(lo, ps, pe);
    hi = std.math.clamp(hi, lo, pe);
    return .{ .lo = @intCast(lo), .hi = @intCast(hi) };
}

fn reaches(b: Bound) bool {
    return b == .reach_preceding or b == .reach_following;
}

/// A value a RANGE offset moves a key to: an exact integer in the units the key and its offset share, or a double.
const Reached = union(enum) { exact: i256, float: f64 };

const us_per_day: i256 = 86_400_000_000;

/// The widest a DECIMAL holds either way: 38 digits, unscaled.
const decimal_limit: i256 = expr_ast.pow10(38) - 1;

/// The most days from the epoch a DATE holds either way (5881580-07-10 and -5877641-06-25, `calendar.max_date_days`);
/// one more is DuckDB's infinity, and its error to reach.
const max_date_days: i256 = std.math.maxInt(i32) - 1;

/// The ORDER BY key of a RANGE frame with offsets.
const RangeKey = struct {
    col: Column,
    unsigned: bool,
    descending: bool,
    kind: Kind,

    const Kind = union(enum) {
        /// An integer type, with the range it holds.
        integer: struct { lo: i256, hi: i256 },
        /// A DECIMAL of this scale.
        decimal: u8,
        double,
        /// Days since the epoch.
        date,
        /// Microseconds since the epoch, without a time zone.
        timestamp,
    };

    /// An offset against this key: the units both are brought to, and how the key moves by it.
    const Offset = struct {
        /// The key's value in the common units is `value * mult`.
        mult: i256 = 1,
        off: i256 = 0,
        off_f: f64 = 0,
        interval: ?plan_types.Interval = null,
        /// The range of the type the sum is computed in, when it can leave it.
        limit: ?struct { lo: i256, hi: i256 } = null,
        /// The most a key may be, in the key's own units, when the offset's scale is the wider one and so casts the
        /// key column up: DuckDB casts it before the pass, so one key past this is an error for the query.
        key_max: ?i256 = null,
    };

    fn of(t: Table, ci: usize, descending: bool) !RangeKey {
        const e = t.elems[ci];
        const unsigned = schema.isUnsignedInt(e);
        const kind: Kind = blk: {
            if (t.decs[ci]) |d| break :blk .{ .decimal = d.scale };
            if (e.logical_type) |lt| switch (lt) {
                .DATE => break :blk .date,
                .TIMESTAMP => |ts| {
                    if (ts.isAdjustedToUTC or ts.unit != .MICROS) return error.WindowRangeKey;
                    break :blk .timestamp;
                },
                .INTEGER => |it| break :blk .{ .integer = integerLimits(@intCast(it.bitWidth), it.isSigned) },
                else => return error.WindowRangeKey,
            };
            if (e.converted_type) |ct| switch (ct) {
                .DATE => break :blk .date,
                .INT_8 => break :blk .{ .integer = integerLimits(8, true) },
                .INT_16 => break :blk .{ .integer = integerLimits(16, true) },
                .INT_32 => break :blk .{ .integer = integerLimits(32, true) },
                .INT_64 => break :blk .{ .integer = integerLimits(64, true) },
                .UINT_8 => break :blk .{ .integer = integerLimits(8, false) },
                .UINT_16 => break :blk .{ .integer = integerLimits(16, false) },
                .UINT_32 => break :blk .{ .integer = integerLimits(32, false) },
                .UINT_64 => break :blk .{ .integer = integerLimits(64, false) },
                else => return error.WindowRangeKey,
            };
            break :blk switch (e.type orelse return error.WindowRangeKey) {
                .INT32 => .{ .integer = integerLimits(32, true) },
                .INT64 => .{ .integer = integerLimits(64, true) },
                .DOUBLE => .double,
                else => return error.WindowRangeKey,
            };
        };
        return .{ .col = t.cols[ci], .unsigned = unsigned, .descending = descending, .kind = kind };
    }

    fn integerLimits(bits: u8, signed: bool) @FieldType(Kind, "integer") {
        const one: i256 = 1;
        return if (signed)
            .{ .lo = -(one << (bits - 1)), .hi = (one << (bits - 1)) - 1 }
        else
            .{ .lo = 0, .hi = (one << bits) - 1 };
    }

    /// Row `j`'s key as an integer (not a DOUBLE's).
    fn lane(self: RangeKey, j: usize) i256 {
        return switch (self.col) {
            .i32 => |x| if (self.unsigned) @as(u32, @bitCast(x.values[j])) else x.values[j],
            .i64 => |x| if (self.unsigned) @as(u64, @bitCast(x.values[j])) else x.values[j],
            .i128 => |x| x.values[j],
            else => unreachable,
        };
    }

    /// `reach` against this key, or the error for an offset of a kind the key does not take.
    fn resolve(self: RangeKey, reach: plan_types.Reach) !Offset {
        switch (reach) {
            .number => |n| switch (self.kind) {
                .double => return .{ .off_f = expr_ast.decimalToDouble(n.mant, n.scale) },
                // DuckDB adds a DECIMAL key and its offset at the wider of the two scales in 38 digits: the side that
                // does not fit the cast there is a Conversion Error, and a sum that leaves it an Out of Range one.
                .decimal => |ks| {
                    const s = @max(ks, n.scale);
                    if (!fits38(n.mant, s - n.scale)) return error.WindowRangeCast;
                    var o = try numberOffset(ks, n);
                    o.limit = .{ .lo = -decimal_limit, .hi = decimal_limit };
                    if (s > ks) o.key_max = expr_ast.pow10(38 - (s - ks));
                    return o;
                },
                .integer => |k| {
                    var o = try numberOffset(0, n);
                    if (n.scale == 0) {
                        // DuckDB adds in the key's type, or in the narrowest of INTEGER, BIGINT and HUGEINT that holds
                        // the key's and the offset's both; what leaves it is an error.
                        var range = k;
                        if (n.mant > k.hi) range = widest(k, n.mant);
                        o.limit = .{ .lo = range.lo, .hi = range.hi };
                    } else {
                        // A fractional offset casts the key to DECIMAL(38, its scale) and adds there: a key too big for
                        // that cast is a Conversion Error, and the sum is a DECIMAL's of 38 digits.
                        o.limit = .{ .lo = -decimal_limit, .hi = decimal_limit };
                        o.key_max = expr_ast.pow10(38 - n.scale);
                    }
                    return o;
                },
                .date => {
                    // DuckDB's -(DATE, INTEGER): whole days that fit an INTEGER, summed into the days a DATE holds.
                    if (n.scale != 0 or n.bits > 32) return error.WindowRangeKey;
                    return .{ .off = n.mant, .limit = .{ .lo = -max_date_days, .hi = max_date_days } };
                },
                .timestamp => return error.WindowRangeKey,
            },
            .interval => |iv| switch (self.kind) {
                .date => return .{ .mult = us_per_day, .interval = iv },
                .timestamp => return .{ .interval = iv },
                else => return error.WindowRangeKey,
            },
        }
    }

    fn numberOffset(key_scale: u8, n: plan_types.Number) !Offset {
        const scale = @max(key_scale, n.scale);
        return .{
            .mult = expr_ast.pow10(scale - key_scale),
            .off = @as(i256, n.mant) * expr_ast.pow10(scale - n.scale),
        };
    }

    /// Whether `v` with `shift` more digits still has 38 of them, `|v| * 10^shift <= 10^38 - 1`, without the multiply.
    fn fits38(v: i128, shift: u8) bool {
        return @abs(@as(i256, v)) < expr_ast.pow10(38 - shift);
    }

    /// DuckDB casts the key column to the type it adds the offset in before the pass: a key that does not fit is an
    /// error for the query, whatever row it is on or partition it is in.
    fn admits(self: RangeKey, o: Offset, rows: usize) !void {
        const max = o.key_max orelse return;
        for (0..rows) |j| {
            if (nullAt(self.col, j)) continue;
            if (@abs(self.lane(j)) >= max) return error.WindowRangeCast;
        }
    }

    fn widest(k: @FieldType(Kind, "integer"), mant: i128) @FieldType(Kind, "integer") {
        for ([_]u8{ 32, 64, 128 }) |bits| {
            const r = integerLimits(bits, true);
            if (r.lo <= k.lo and r.hi >= k.hi and mant <= r.hi) return r;
        }
        return integerLimits(128, true);
    }

    /// Row `j`'s key moved by the offset, up (`plus`) or down.
    fn moved(self: RangeKey, o: Offset, j: usize, plus: bool) !Reached {
        if (self.kind == .double) {
            const v = self.col.f64.values[j];
            return .{ .float = if (plus) v + o.off_f else v - o.off_f };
        }
        if (o.interval) |iv| {
            const v = self.lane(j);
            const at: sql_literals.Moment = if (self.kind == .date)
                .{ .days = @intCast(v), .micros = 0 }
            else
                .{ .days = @intCast(@divFloor(v, us_per_day)), .micros = @intCast(@mod(v, us_per_day)) };
            const to = sql_literals.addInterval(at, .{ .months = iv.months, .days = iv.days, .micros = iv.micros }, if (plus) 1 else -1) orelse
                return error.WindowRangeOverflow;
            return .{ .exact = @as(i256, to.days) * us_per_day + to.micros };
        }
        const v = self.lane(j) * o.mult;
        const b = if (plus) v + o.off else v - o.off;
        if (o.limit) |l| if (b < l.lo or b > l.hi) return error.WindowRangeOverflow;
        return .{ .exact = b };
    }

    /// Row `j`'s key against `b`, in the units of the offset that made `b`.
    fn against(self: RangeKey, o: Offset, j: usize, b: Reached) std.math.Order {
        return switch (b) {
            .float => |f| floatOrder(self.col.f64.values[j], f),
            .exact => |e| std.math.order(self.lane(j) * o.mult, e),
        };
    }

    /// Whether row `j` comes before `b` in the order of the sort.
    fn before(self: RangeKey, o: Offset, j: usize, b: Reached) bool {
        const ord = self.against(o, j, b);
        return if (self.descending) ord == .gt else ord == .lt;
    }

    fn after(self: RangeKey, o: Offset, j: usize, b: Reached) bool {
        const ord = self.against(o, j, b);
        return if (self.descending) ord == .lt else ord == .gt;
    }

    /// The first row of `[lo, hi)` that does not come before `b`.
    fn lowerBound(self: RangeKey, o: Offset, lo: usize, hi: usize, b: Reached) usize {
        var l = lo;
        var h = hi;
        while (l < h) {
            const mid = l + (h - l) / 2;
            if (self.before(o, mid, b)) l = mid + 1 else h = mid;
        }
        return l;
    }

    /// The first row of `[lo, hi)` that comes after `b`.
    fn upperBound(self: RangeKey, o: Offset, lo: usize, hi: usize, b: Reached) usize {
        var l = lo;
        var h = hi;
        while (l < h) {
            const mid = l + (h - l) / 2;
            if (self.after(o, mid, b)) h = mid else l = mid + 1;
        }
        return l;
    }
};

/// The frames of a RANGE frame with an offset, as DuckDB finds them: the key is the only ORDER BY expression, a row whose key
/// is NULL has its peers for the frame of an offset, and any other row's is searched among the rows with a key. An offset
/// that puts the bound past the row's own key the wrong way is an error.
fn rangeFrames(t: Table, r: Runs, spec: Spec, f: Frame, out: []Span) !void {
    const key = try RangeKey.of(t, spec.order[0], spec.descending);
    const start: ?RangeKey.Offset = switch (f.start) {
        .reach_preceding, .reach_following => |reach| try key.resolve(reach),
        else => null,
    };
    const end: ?RangeKey.Offset = switch (f.end) {
        .reach_preceding, .reach_following => |reach| try key.resolve(reach),
        else => null,
    };
    if (start) |o| try key.admits(o, t.rows);
    if (end) |o| try key.admits(o, t.rows);
    var at: usize = 0;
    while (at < t.rows) {
        const ps: usize = r.pstart[at];
        const pe: usize = r.pend[at];
        // The NULLs of a partition are the rows at one end of it.
        var valid_lo = ps;
        while (valid_lo < pe and nullAt(key.col, valid_lo)) valid_lo += 1;
        var valid_hi = pe;
        while (valid_hi > valid_lo and nullAt(key.col, valid_hi - 1)) valid_hi -= 1;
        for (ps..pe) |row| {
            const peer_lo: usize = r.gstart[row];
            const peer_hi: usize = r.gend[row];
            const keyless = nullAt(key.col, row);
            var lo: usize = switch (f.start) {
                .unbounded_preceding => ps,
                .current_row => peer_lo,
                .reach_preceding => if (keyless) peer_lo else blk: {
                    const o = start.?;
                    const b = try key.moved(o, row, key.descending);
                    if (key.before(o, row, b)) return error.WindowRangeOffset;
                    break :blk key.lowerBound(o, valid_lo, row + 1, b);
                },
                .reach_following => if (keyless) peer_lo else blk: {
                    const o = start.?;
                    const b = try key.moved(o, row, !key.descending);
                    if (key.after(o, row, b)) return error.WindowRangeOffset;
                    break :blk key.lowerBound(o, peer_lo, valid_hi, b);
                },
                else => unreachable,
            };
            var hi: usize = switch (f.end) {
                .current_row => peer_hi,
                .unbounded_following => pe,
                .reach_preceding => if (keyless) peer_hi else blk: {
                    const o = end.?;
                    const b = try key.moved(o, row, key.descending);
                    if (key.before(o, row, b)) return error.WindowRangeOffset;
                    break :blk key.upperBound(o, valid_lo, peer_hi, b);
                },
                .reach_following => if (keyless) peer_hi else blk: {
                    const o = end.?;
                    const b = try key.moved(o, row, !key.descending);
                    if (key.after(o, row, b)) return error.WindowRangeOffset;
                    break :blk key.upperBound(o, row, valid_hi, b);
                },
                else => unreachable,
            };
            // A frame that ends before it starts is empty.
            lo = @min(lo, pe);
            hi = std.math.clamp(hi, lo, pe);
            out[row] = .{ .lo = @intCast(lo), .hi = @intCast(hi) };
        }
        at = pe;
    }
}

/// Every row's frame for `call`, in window order.
fn frames(a: std.mem.Allocator, t: Table, r: Runs, spec: Spec, call: Call) ![]const Span {
    const out = try a.alloc(Span, t.rows);
    if (!call.frame.rows and (reaches(call.frame.start) or reaches(call.frame.end))) {
        try rangeFrames(t, r, spec, call.frame, out);
    } else for (out, 0..) |*s, i| {
        s.* = frameOf(call.frame, r, i);
    }
    return out;
}

// ---- results

/// A computed column and the type to write it as.
const Out = struct {
    col: Column,
    physical: schema.Type,
    /// The column whose annotations the result keeps (a value passed through: lag, lead, min, max).
    from: ?usize = null,
    dec: ?expr_ast.Dec = null,
    nullable: bool,
};

fn i64Out(values: []const i64, def: ?[]const u32, nullable: bool) Out {
    return .{
        .col = .{ .i64 = .{ .values = values, .def_levels = def, .max_def = if (def != null) 1 else 0, .has_nulls = def != null } },
        .physical = .INT64,
        .nullable = nullable,
    };
}

/// The rows `pick` names, in `c`'s lane: a negative pick is NULL, or `default` where one is given.
fn gather(a: std.mem.Allocator, c: Column, pick: []const i64, default: Default) !Column {
    const n = pick.len;
    const def = try a.alloc(u32, n);
    var any_null = false;
    switch (c) {
        inline else => |x, tag| {
            const T = @typeInfo(@TypeOf(x.values)).pointer.child;
            const out = try a.alloc(T, n);
            for (pick, out, def) |p, *o, *d| {
                if (p >= 0) {
                    const j: usize = @intCast(p);
                    o.* = x.values[j];
                    d.* = if (nullAt(c, j)) 0 else 1;
                    if (d.* == 0) any_null = true;
                    continue;
                }
                o.* = if (T == []const u8) "" else std.mem.zeroes(T);
                switch (default) {
                    .null => {
                        d.* = 0;
                        any_null = true;
                    },
                    .int => |v| {
                        d.* = 1;
                        o.* = switch (tag) {
                            .i32 => std.math.cast(i32, v) orelse return error.WindowDefaultType,
                            .i64 => v,
                            .f32 => @floatFromInt(v),
                            .f64 => @floatFromInt(v),
                            else => return error.WindowDefaultType,
                        };
                    },
                    .float => |v| {
                        d.* = 1;
                        o.* = switch (tag) {
                            .f32 => @floatCast(v),
                            .f64 => v,
                            else => return error.WindowDefaultType,
                        };
                    },
                    .string => |v| {
                        d.* = 1;
                        o.* = switch (tag) {
                            .string => v,
                            else => return error.WindowDefaultType,
                        };
                    },
                }
            }
            return @unionInit(Column, @tagName(tag), .{ .values = out, .def_levels = def, .max_def = 1, .has_nulls = any_null });
        },
    }
}

fn argOf(call: Call) !usize {
    return call.arg orelse error.WindowArgumentType;
}

fn compute(a: std.mem.Allocator, t: Table, r: Runs, spec: Spec, call: Call) !Out {
    const n = t.rows;
    switch (call.func) {
        .row_number, .rank, .dense_rank => {
            const out = try a.alloc(i64, n);
            for (out, 0..) |*o, i| o.* = switch (call.func) {
                .row_number => @as(i64, @intCast(i - r.pstart[i])) + 1,
                .rank => @as(i64, @intCast(r.gstart[i] - r.pstart[i])) + 1,
                else => r.dense[i],
            };
            return i64Out(out, null, false);
        },
        .percent_rank, .cume_dist => {
            // DuckDB's: the rows before the row's peers over the partition's other rows; the rows up to its peers' last over all.
            const out = try a.alloc(f64, n);
            for (out, 0..) |*o, i| {
                const size: f64 = @floatFromInt(r.pend[i] - r.pstart[i]);
                o.* = if (call.func == .percent_rank)
                    (if (size > 1) @as(f64, @floatFromInt(r.gstart[i] - r.pstart[i])) / (size - 1) else 0)
                else
                    @as(f64, @floatFromInt(r.gend[i] - r.pstart[i])) / size;
            }
            return .{ .col = .{ .f64 = .{ .values = out } }, .physical = .DOUBLE, .nullable = false };
        },
        .lag, .lead => {
            const ci = try argOf(call);
            const pick = try a.alloc(i64, n);
            for (pick, 0..) |*p, i| {
                const row: i64 = @intCast(i);
                const j = if (call.func == .lag) row - call.offset else row + call.offset;
                p.* = if (j >= r.pstart[i] and j < r.pend[i]) j else -1;
            }
            return .{ .col = try gather(a, t.cols[ci], pick, call.default), .physical = elemType(t, ci), .from = ci, .dec = t.decs[ci], .nullable = true };
        },
        .min, .max => {
            const ci = try argOf(call);
            const c = t.cols[ci];
            const unsigned = ci < t.elems.len and schema.isUnsignedInt(t.elems[ci]);
            const pick = try a.alloc(i64, n);
            const queue = try a.alloc(u32, n);
            const spans = try frames(a, t, r, spec, call);
            const want: std.math.Order = if (call.func == .min) .lt else .gt;
            var i: usize = 0;
            while (i < n) {
                const ps = r.pstart[i];
                const pe = r.pend[i];
                var head: usize = 0;
                var tail: usize = 0;
                var added: usize = ps;
                for (ps..pe) |row| {
                    const span = spans[row];
                    while (added < span.hi) : (added += 1) {
                        if (nullAt(c, added)) continue;
                        // A later value at least as good as the ones queued behind it replaces them.
                        while (tail > head) {
                            const back = queue[tail - 1];
                            const o = orderAt(c, added, back, unsigned);
                            if (o == want or o == .eq) tail -= 1 else break;
                        }
                        queue[tail] = @intCast(added);
                        tail += 1;
                    }
                    while (head < tail and queue[head] < span.lo) head += 1;
                    pick[row] = if (head < tail) @as(i64, queue[head]) else -1;
                }
                i = pe;
            }
            return .{ .col = try gather(a, c, pick, .null), .physical = elemType(t, ci), .from = ci, .dec = t.decs[ci], .nullable = true };
        },
        .first_value, .last_value, .nth_value => {
            const ci = try argOf(call);
            const pick = try a.alloc(i64, n);
            const spans = try frames(a, t, r, spec, call);
            for (pick, 0..) |*p, i| {
                const s = spans[i];
                if (s.lo >= s.hi) {
                    p.* = -1;
                    continue;
                }
                p.* = switch (call.func) {
                    .first_value => @intCast(s.lo),
                    .last_value => @intCast(s.hi - 1),
                    else => if (call.offset - 1 < s.hi - s.lo) @as(i64, @intCast(s.lo + call.offset - 1)) else -1,
                };
            }
            return .{ .col = try gather(a, t.cols[ci], pick, .null), .physical = elemType(t, ci), .from = ci, .dec = t.decs[ci], .nullable = true };
        },
        .ntile => {
            // DuckDB's (SQLite's): the first `rows % buckets` buckets hold one row more than the rest; more buckets than
            // rows is a bucket a row.
            const out = try a.alloc(i64, n);
            for (out, 0..) |*o, i| {
                const total: i64 = @as(i64, r.pend[i]) - r.pstart[i];
                const buckets: i64 = @min(@as(i64, call.offset), total);
                const size = @divTrunc(total, buckets);
                const large = total - buckets * size;
                const small_from = large * (size + 1);
                const idx: i64 = @as(i64, @intCast(i)) - r.pstart[i];
                o.* = if (idx < small_from) 1 + @divTrunc(idx, size + 1) else 1 + large + @divTrunc(idx - small_from, size);
            }
            return i64Out(out, null, false);
        },
        .count_star, .count => {
            const out = try a.alloc(i64, n);
            const spans = try frames(a, t, r, spec, call);
            if (call.func == .count_star) {
                for (out, spans) |*o, s| o.* = s.hi - s.lo;
                return i64Out(out, null, false);
            }
            const c = t.cols[try argOf(call)];
            const pre = try a.alloc(u32, n + 1);
            pre[0] = 0;
            for (0..n) |i| pre[i + 1] = pre[i] + @intFromBool(!nullAt(c, i));
            for (out, spans) |*o, s| o.* = pre[s.hi] - pre[s.lo];
            return i64Out(out, null, false);
        },
        .sum, .avg => return sumOrAvg(a, t, r, spec, call),
    }
}

fn elemType(t: Table, ci: usize) schema.Type {
    if (t.decs[ci]) |d| return d.physical();
    return t.elems[ci].type.?;
}

/// SUM and AVG over the frame. Integers and DECIMALs sum exactly (a prefix of i128); a DOUBLE sums left to right from
/// the frame's first row, a running total when the frame starts at the partition's.
/// The bits of the integer type DuckDB reads column `ci` as, or an error for a column it does not sum as an integer or DECIMAL.
fn integerWidth(t: Table, ci: usize) !u8 {
    if (t.decs[ci] != null) return 128;
    const e = t.elems[ci];
    if (e.logical_type) |lt| switch (lt) {
        .INTEGER => |i| return if (i.isSigned) @intCast(i.bitWidth) else error.WindowArgumentType,
        else => return error.WindowArgumentType,
    };
    if (e.converted_type) |ct| switch (ct) {
        .INT_8 => return 8,
        .INT_16 => return 16,
        .INT_32 => return 32,
        .INT_64 => return 64,
        else => return error.WindowArgumentType,
    };
    return switch (e.type orelse return error.WindowArgumentType) {
        .INT32 => 32,
        .INT64 => 64,
        else => error.WindowArgumentType,
    };
}

/// AVG of `k` values that sum to `total`, as DuckDB finalizes it: up to 16 bits (a DECIMAL of up to 4 digits) the sum over the
/// count and the scale in doubles, wider in long doubles and rounded once to a double.
fn averageOf(total: i128, k: u32, width: u8, dec: ?expr_ast.Dec) f64 {
    const scale: f64 = if (dec) |d| @floatFromInt(expr_ast.pow10(d.scale)) else 1;
    const small = if (dec) |d| d.precision <= 4 else width <= 16;
    if (small) return @as(f64, @floatFromInt(total)) / (@as(f64, @floatFromInt(k)) * scale);
    const divisor: f80 = @as(f80, @floatFromInt(k)) * @as(f80, scale);
    return @floatCast(@as(f80, @floatFromInt(total)) / divisor);
}

/// Row `i` of an integer or DECIMAL column as a 256-bit integer, 0 for NULL.
fn integerAt(c: Column, i: usize) i256 {
    if (nullAt(c, i)) return 0;
    return switch (c) {
        .i32 => |x| x.values[i],
        .i64 => |x| x.values[i],
        .i128 => |x| x.values[i],
        else => unreachable,
    };
}

fn sumOrAvg(a: std.mem.Allocator, t: Table, r: Runs, spec: Spec, call: Call) !Out {
    const n = t.rows;
    const spans = try frames(a, t, r, spec, call);
    const ci = try argOf(call);
    const c = t.cols[ci];
    const is_avg = call.func == .avg;
    const def = try a.alloc(u32, n);
    var any_null = false;
    const cnt = try a.alloc(u32, n + 1);
    cnt[0] = 0;
    for (0..n) |i| cnt[i + 1] = cnt[i] + @intFromBool(!nullAt(c, i));

    switch (c) {
        .i32, .i64, .i128 => {
            // Only the types DuckDB sums as integers and DECIMALs: a DATE, a time or an unsigned integer is another function's.
            const width = try integerWidth(t, ci);
            // The sum of each row's frame, exact: the frames of a partition move forward, so a sliding total adds each row
            // once and takes it out once, in 256 bits, which no frame's sum or step of it can overflow.
            const sums = try a.alloc(i128, n);
            var at: usize = 0;
            while (at < n) {
                const ps: usize = r.pstart[at];
                const pe: usize = r.pend[at];
                var total: i256 = 0;
                var lo_p: usize = ps;
                var hi_p: usize = ps;
                for (ps..pe) |row| {
                    const span = spans[row];
                    std.debug.assert(span.lo >= lo_p);
                    while (hi_p < span.hi) : (hi_p += 1) total += integerAt(c, hi_p);
                    while (lo_p < span.lo) : (lo_p += 1) total -= integerAt(c, lo_p);
                    sums[row] = std.math.cast(i128, total) orelse return error.WindowSumOverflow;
                }
                at = pe;
            }
            if (is_avg) {
                const out = try a.alloc(f64, n);
                for (out, def, spans, 0..) |*o, *d, s, i| {
                    const k = cnt[s.hi] - cnt[s.lo];
                    d.* = @intFromBool(k > 0);
                    if (k == 0) {
                        o.* = 0;
                        any_null = true;
                        continue;
                    }
                    o.* = averageOf(sums[i], k, width, t.decs[ci]);
                }
                return .{ .col = .{ .f64 = .{ .values = out, .def_levels = def, .max_def = 1, .has_nulls = any_null } }, .physical = .DOUBLE, .nullable = true };
            }
            if (t.decs[ci]) |dd| {
                const out = try a.alloc(i128, n);
                const limit = expr_ast.pow10(38);
                for (out, def, spans, 0..) |*o, *d, s, i| {
                    d.* = @intFromBool(cnt[s.hi] > cnt[s.lo]);
                    o.* = sums[i];
                    if (d.* == 0) any_null = true;
                    if (o.* >= limit or o.* <= -limit) return error.WindowSumOverflow;
                }
                const wide: expr_ast.Dec = .{ .precision = 38, .scale = dd.scale };
                return .{ .col = .{ .i128 = .{ .values = out, .def_levels = def, .max_def = 1, .has_nulls = any_null } }, .physical = wide.physical(), .dec = wide, .nullable = true };
            }
            if (c == .i128) return error.WindowArgumentType;
            // BIGINT, as zpq's own sum is (DuckDB's is a HUGEINT, which it writes to Parquet as a DOUBLE): a frame past
            // 64 bits is an error, by frame.
            const out = try a.alloc(i64, n);
            for (out, def, spans, 0..) |*o, *d, s, i| {
                d.* = @intFromBool(cnt[s.hi] > cnt[s.lo]);
                if (d.* == 0) any_null = true;
                o.* = std.math.cast(i64, sums[i]) orelse return error.WindowSumOverflow;
            }
            return .{ .col = .{ .i64 = .{ .values = out, .def_levels = def, .max_def = 1, .has_nulls = any_null } }, .physical = .INT64, .nullable = true };
        },
        // DuckDB adds a frame of DOUBLEs in the order of a segment tree (blocks of 16 rows, then their blocks), and AVG with
        // compensated sums; neither is a left-to-right sum, and the last bits differ from one in a partition of 32 rows.
        .f32, .f64 => return error.WindowFloatAggregate,
        else => return error.WindowArgumentType,
    }
}

/// The window columns for `spec`'s calls over `t`, in call order.
pub fn columns(a: std.mem.Allocator, t: Table, spec: Spec) ![]const Out {
    if (t.rows > std.math.maxInt(u32) - 1) return error.WindowTooManyRows;
    const r = try runs(a, t, spec);
    const out = try a.alloc(Out, spec.calls.len);
    for (spec.calls, out) |call, *o| o.* = try compute(a, t, r, spec, call);
    return out;
}

// ---- files

const Loaded = struct { table: Table, names: []const []const u8 };

fn chunkOf(bytes: []const u8, cm: schema.ColumnMetaData) ![]const u8 {
    const start: usize = @intCast(cm.dictionary_page_offset orelse cm.data_page_offset);
    const len: usize = @intCast(cm.total_compressed_size);
    if (start > bytes.len or len > bytes.len - start) return error.MissingChunkBytes;
    return bytes[start..][0..len];
}

/// Leaf `ci` of every row group, decoded in its lane and joined into one column.
fn loadColumn(a: std.mem.Allocator, bytes: []const u8, meta: *const schema.FileMetaData, ci: usize, rows: usize) !Column {
    const elem = meta.schema.items[ci + 1];
    const kind = decimal_mod.kindFromSchema(&elem);
    const phys = elem.type orelse return error.WindowColumnType;
    if (kind == null) switch (phys) {
        .INT32, .INT64, .FLOAT, .DOUBLE, .BOOLEAN, .BYTE_ARRAY => {},
        else => return error.WindowColumnType,
    };
    const which: LaneTag = if (kind != null) .i128 else lane(phys);
    switch (which) {
        inline else => |tag| {
            const T = LaneType(tag);
            const values = try a.alloc(T, rows);
            var defs: ?[]u32 = null;
            var max_def: u32 = 0;
            var at: usize = 0;
            var any_null = false;
            for (meta.row_groups.items) |*rg| {
                const cm = rg.columns.items[ci].meta_data orelse return error.ColumnMetaMissing;
                const chunk = try chunkOf(bytes, cm);
                const levels = meta.getColumnLevels(cm.path_in_schema.items);
                const n: usize = @intCast(rg.num_rows);
                const options = column_mod.chunkOptions(.{}, meta, cm);
                const got = if (comptime tag == .i128)
                    try decimal_mod.decodeColumnAsI128WithOptions(a, chunk, cm.codec, levels, n, kind.?, options)
                else
                    try consumer_column.decodeColumnTWithOptions(T, a, chunk, cm.codec, levels, n, options);
                if (got.values.len != n) return error.WindowColumnType;
                @memcpy(values[at..][0..n], got.values);
                const md: u32 = @intCast(levels.max_def);
                max_def = md;
                if (md > 0) {
                    if (defs == null) {
                        defs = try a.alloc(u32, rows);
                        @memset(defs.?, md);
                    }
                    if (got.def_levels) |dl| {
                        @memcpy(defs.?[at..][0..n], dl);
                        for (dl) |d| if (d < md) {
                            any_null = true;
                        };
                    }
                }
                at += n;
            }
            return @unionInit(Column, @tagName(tag), .{ .values = values, .def_levels = defs, .max_def = max_def, .has_nulls = any_null });
        },
    }
}

const LaneTag = enum { i32, i64, f32, f64, string, boolean, i128 };

fn lane(phys: schema.Type) LaneTag {
    return switch (phys) {
        .INT32 => .i32,
        .INT64 => .i64,
        .FLOAT => .f32,
        .DOUBLE => .f64,
        .BOOLEAN => .boolean,
        else => .string,
    };
}

fn LaneType(comptime tag: LaneTag) type {
    return switch (tag) {
        .i32 => i32,
        .i64 => i64,
        .f32 => f32,
        .f64 => f64,
        .string => []const u8,
        .boolean => bool,
        .i128 => i128,
    };
}

fn loadTable(a: std.mem.Allocator, bytes: []const u8, meta: *const schema.FileMetaData) !Loaded {
    const ncols = meta.schema.items.len - 1;
    var rows: usize = 0;
    for (meta.row_groups.items) |*rg| rows += @intCast(rg.num_rows);
    const cols = try a.alloc(Column, ncols);
    const elems = try a.alloc(schema.SchemaElement, ncols);
    const decs = try a.alloc(?expr_ast.Dec, ncols);
    const names = try a.alloc([]const u8, ncols);
    for (0..ncols) |ci| {
        const elem = meta.schema.items[ci + 1];
        if ((elem.num_children orelse 0) > 0) return error.WindowColumnType;
        if (elem.repetition_type == .REPEATED) return error.WindowColumnType;
        elems[ci] = elem;
        names[ci] = elem.name;
        decs[ci] = null;
        if (decimal_mod.kindFromSchema(&elem)) |k| {
            if (k.precision > 38 or k.precision < 1 or k.scale < 0) return error.WindowColumnType;
            decs[ci] = .{ .precision = @intCast(k.precision), .scale = @intCast(k.scale) };
        }
        cols[ci] = try loadColumn(a, bytes, meta, ci, rows);
    }
    return .{ .table = .{ .rows = rows, .cols = cols, .elems = elems, .decs = decs }, .names = names };
}

/// An output column for a decoded one, typed as the file typed it.
fn passThrough(name: []const u8, c: Column, elem: schema.SchemaElement, dec: ?expr_ast.Dec) OutputCol {
    return .{
        .name = name,
        .col = c,
        .parquet_type = if (dec) |d| d.physical() else elem.type.?,
        .type_length = if (dec != null) null else elem.type_length,
        .nullable = (elem.repetition_type orelse .REQUIRED) != .REQUIRED,
        .logical_type = if (dec != null) null else elem.logical_type,
        .logical_type_raw = if (dec != null) &.{} else elem.logical_type_raw,
        .converted_type = if (dec != null) null else elem.converted_type,
        .dec = dec,
    };
}

pub const Request = struct {
    input: []const u8,
    output: []const u8,
    pass: plan_types.Pass,
    max_memory: usize = std.math.maxInt(usize),
};

fn indexOf(names: []const []const u8, name: []const u8) !usize {
    for (names, 0..) |n, i| if (std.mem.eql(u8, n, name)) return i;
    return error.WindowColumnType;
}

/// An allocator over `inner` that draws every byte it hands out on a ledger and refuses past its limit. The pass's arena
/// frees nothing before it ends, so nothing is given back: a count that can only be higher than what is held.
const Ledgered = struct {
    inner: std.mem.Allocator,
    ledger: *core_memory.SharedBudget,

    const vtable: std.mem.Allocator.VTable = .{ .alloc = alloc, .resize = resize, .remap = remap, .free = free };

    fn allocator(self: *Ledgered) std.mem.Allocator {
        return .{ .ptr = self, .vtable = &vtable };
    }

    fn alloc(ctx: *anyopaque, len: usize, alignment: std.mem.Alignment, ret_addr: usize) ?[*]u8 {
        const self: *Ledgered = @ptrCast(@alignCast(ctx));
        _ = self.ledger.drawExact(len) orelse return null;
        return self.inner.rawAlloc(len, alignment, ret_addr);
    }

    fn resize(ctx: *anyopaque, memory: []u8, alignment: std.mem.Alignment, new_len: usize, ret_addr: usize) bool {
        const self: *Ledgered = @ptrCast(@alignCast(ctx));
        if (new_len > memory.len) _ = self.ledger.drawExact(new_len - memory.len) orelse return false;
        return self.inner.rawResize(memory, alignment, new_len, ret_addr);
    }

    fn remap(ctx: *anyopaque, memory: []u8, alignment: std.mem.Alignment, new_len: usize, ret_addr: usize) ?[*]u8 {
        const self: *Ledgered = @ptrCast(@alignCast(ctx));
        if (new_len > memory.len) _ = self.ledger.drawExact(new_len - memory.len) orelse return null;
        return self.inner.rawRemap(memory, alignment, new_len, ret_addr);
    }

    fn free(ctx: *anyopaque, memory: []u8, alignment: std.mem.Alignment, ret_addr: usize) void {
        const self: *Ledgered = @ptrCast(@alignCast(ctx));
        self.inner.rawFree(memory, alignment, ret_addr);
    }
};

const SortIdx = struct { col: usize, descending: bool, nulls_first: bool, unsigned: bool = false };

const SortOrder = struct {
    cols: []const Column,
    keys: []const SortIdx,

    /// Row `x` before row `y` by the keys; equal rows keep their order (the sort is stable).
    fn less(self: SortOrder, x: u32, y: u32) bool {
        for (self.keys) |k| {
            const c = self.cols[k.col];
            const nx = nullAt(c, x);
            const ny = nullAt(c, y);
            if (nx or ny) {
                if (nx and ny) continue;
                return if (nx) k.nulls_first else !k.nulls_first;
            }
            const o = orderAt(c, x, y, k.unsigned);
            if (o == .eq) continue;
            return (o == .lt) != k.descending;
        }
        return false;
    }
};

/// The input rows' indices in the order of `keys`; the rows themselves are not moved.
fn sortedRows(a: std.mem.Allocator, cols: []const Column, keys: []const SortIdx, rows: usize) ![]u32 {
    const perm = try a.alloc(u32, rows);
    for (perm, 0..) |*p, i| p.* = @intCast(i);
    if (keys.len > 0) std.sort.block(u32, perm, SortOrder{ .cols = cols, .keys = keys }, SortOrder.less);
    return perm;
}

/// What the pass will hold, counted before anything is decoded: every column in its lane, the strings' bytes, the levels of
/// a column that can be NULL, the key and argument columns again in window order, the run arrays and each call's answer.
fn estimate(meta: *const schema.FileMetaData, rows: u64, ncalls: usize, referenced: []const bool) u64 {
    var held: u64 = 0;
    for (meta.schema.items[1..], 0..) |elem, ci| {
        var width: u64 = 8;
        if (decimal_mod.kindFromSchema(&elem) != null) width = 16 else switch (elem.type orelse .INT64) {
            .INT32, .FLOAT => width = 4,
            .BOOLEAN => width = 1,
            .BYTE_ARRAY => width = 16,
            else => {},
        }
        var bytes: u64 = rows * width;
        if ((elem.repetition_type orelse .REQUIRED) != .REQUIRED) bytes += rows * 4;
        if (elem.type == .BYTE_ARRAY) for (meta.row_groups.items) |*rg| {
            bytes += @intCast((rg.columns.items[ci].meta_data orelse continue).total_uncompressed_size);
        };
        held += bytes;
        if (ci < referenced.len and referenced[ci]) held += bytes;
    }
    // Two index arrays and the five run arrays, and per call a value, its levels, a prefix of counts and the frames.
    return held + rows * (8 + 8 + 20 + ncalls * 40);
}

/// Read `req.input`, sort a view of it by the pass's keys, compute the calls, and write the file's columns, in the order
/// they were read, and the calls' to `req.output`. The rows keep the file's order: a later pass, or the final query, sees
/// them as the query's input had them.
pub fn run(gpa: std.mem.Allocator, req: Request) !void {
    return runLedgered(gpa, req) catch |err| switch (err) {
        error.OutOfMemory => error.ExceededMemoryBudget,
        else => err,
    };
}

fn runLedgered(gpa: std.mem.Allocator, req: Request) !void {
    var arena_state = std.heap.ArenaAllocator.init(gpa);
    defer arena_state.deinit();
    var ledger: core_memory.SharedBudget = .{ .limit = req.max_memory };
    var counted: Ledgered = .{ .inner = arena_state.allocator(), .ledger = &ledger };
    const a = counted.allocator();

    const mapped = try local_fs.mapFile(req.input);
    defer mapped.unmap();
    const meta = try metadata.open(a, mapped.bytes);
    const ncols = meta.schema.items.len -| 1;

    // The columns the window reads: its keys and its calls' arguments.
    const names = try a.alloc([]const u8, ncols);
    for (meta.schema.items[1..], names) |elem, *n| n.* = elem.name;
    const referenced = try a.alloc(bool, ncols);
    @memset(referenced, false);
    const part = try a.alloc(usize, req.pass.partition.len);
    for (req.pass.partition, part) |n, *p| p.* = try indexOf(names, n);
    const order = try a.alloc(usize, req.pass.order.len);
    for (req.pass.order, order) |n, *p| p.* = try indexOf(names, n);
    const keys = try a.alloc(SortIdx, req.pass.sort.len);
    for (req.pass.sort, keys) |k, *out| {
        const col = try indexOf(names, k.column);
        out.* = .{ .col = col, .descending = k.descending, .nulls_first = k.nulls_first orelse false, .unsigned = schema.isUnsignedInt(meta.schema.items[col + 1]) };
    }
    const calls = try a.alloc(Call, req.pass.calls.len);
    for (req.pass.calls, calls) |nc, *c| c.* = .{
        .func = nc.func,
        .arg = if (nc.arg) |n| try indexOf(names, n) else null,
        .offset = nc.offset,
        .default = nc.default,
        .frame = nc.frame,
    };
    for (part) |c| referenced[c] = true;
    for (order) |c| referenced[c] = true;
    for (calls) |c| if (c.arg) |ci| {
        referenced[ci] = true;
    };

    var rows_total: u64 = 0;
    for (meta.row_groups.items) |*rg| rows_total += @intCast(rg.num_rows);
    if (estimate(&meta, rows_total, calls.len, referenced) > req.max_memory) return error.ExceededMemoryBudget;

    const loaded = try loadTable(a, mapped.bytes, &meta);
    const t = loaded.table;

    // The pass computes over a view in window order: only the columns it reads, gathered by the sort. With no keys the file's
    // order is the window's.
    const perm = try sortedRows(a, t.cols, keys, t.rows);
    const sorted: Table = if (keys.len == 0) t else blk: {
        const pick = try a.alloc(i64, t.rows);
        for (pick, perm) |*p, row| p.* = row;
        const view = try a.alloc(Column, ncols);
        for (view, t.cols, referenced) |*v, c, used| v.* = if (used) try gather(a, c, pick, .null) else .{ .i64 = .{ .values = &.{} } };
        break :blk .{ .rows = t.rows, .cols = view, .elems = t.elems, .decs = t.decs };
    };
    const order_desc = order.len > 0 and keys[keys.len - 1].descending;
    const outs = try columns(a, sorted, .{ .partition = part, .order = order, .descending = order_desc, .calls = calls });

    // Back to the file's order: row `i` takes the answer of the window position that held it.
    const results: []const Out = if (keys.len == 0) outs else blk: {
        const back = try a.alloc(i64, t.rows);
        for (perm, 0..) |row, position| back[row] = @intCast(position);
        const moved = try a.alloc(Out, outs.len);
        for (outs, moved) |o, *m| {
            m.* = o;
            m.col = try gather(a, o.col, back, .null);
            // An answer that is never NULL is written as a column that cannot be.
            if (!o.nullable) switch (m.col) {
                inline else => |*col| {
                    col.def_levels = null;
                    col.max_def = 0;
                    col.has_nulls = false;
                },
            };
        }
        break :blk moved;
    };

    const all = try a.alloc(OutputCol, t.cols.len + results.len);
    for (t.cols, 0..) |c, i| all[i] = passThrough(loaded.names[i], c, t.elems[i], t.decs[i]);
    for (results, req.pass.results, 0..) |o, name, i| {
        var col: OutputCol = if (o.from) |from| passThrough(name, o.col, t.elems[from], t.decs[from]) else .{
            .name = name,
            .col = o.col,
            .parquet_type = o.physical,
            .nullable = o.nullable,
            .dec = o.dec,
        };
        col.name = name;
        col.nullable = o.nullable;
        all[t.cols.len + i] = col;
    }

    const fd = try write.createFile(req.output);
    var open = true;
    defer if (open) local_fs.close(fd);
    errdefer local_fs.removeFile(req.output);
    var fd_sink: write.FdSink = .{ .fd = fd };
    const sink: streaming.Sink = fd_sink.sink();
    _ = try consumer.writeAggregateRows(a, sink, all, @intCast(t.rows), .ZSTD, .{}, .{});
    open = false;
    try local_fs.closeChecked(fd);
}

test "window: runs find partitions and peers" {
    const T = std.testing;
    var arena_state = std.heap.ArenaAllocator.init(T.allocator);
    defer arena_state.deinit();
    const a = arena_state.allocator();
    const p = [_]i64{ 1, 1, 1, 2, 2, 3 };
    const o = [_]i64{ 5, 5, 7, 1, 2, 9 };
    const cols = [_]Column{
        .{ .i64 = .{ .values = &p } },
        .{ .i64 = .{ .values = &o } },
    };
    const t: Table = .{ .rows = 6, .cols = &cols, .elems = &.{}, .decs = &.{ null, null } };
    const r = try runs(a, t, .{ .partition = &.{0}, .order = &.{1}, .calls = &.{} });
    try T.expectEqualSlices(u32, &.{ 0, 0, 0, 3, 3, 5 }, r.pstart);
    try T.expectEqualSlices(u32, &.{ 3, 3, 3, 5, 5, 6 }, r.pend);
    try T.expectEqualSlices(u32, &.{ 0, 0, 2, 3, 4, 5 }, r.gstart);
    try T.expectEqualSlices(u32, &.{ 2, 2, 3, 4, 5, 6 }, r.gend);
    try T.expectEqualSlices(u32, &.{ 1, 1, 2, 1, 2, 1 }, r.dense);
}

test "window: ranking, offsets and frames over partitions of a sorted table" {
    const T = std.testing;
    var arena_state = std.heap.ArenaAllocator.init(T.allocator);
    defer arena_state.deinit();
    const a = arena_state.allocator();
    // Partitions 1 (rows 0..3) and 2 (rows 3..6); the order key ties in rows 0 and 1.
    const p = [_]i64{ 1, 1, 1, 2, 2, 2 };
    const o = [_]i64{ 5, 5, 7, 1, 2, 2 };
    const v = [_]i64{ 10, 20, 30, 40, 50, 60 };
    const cols = [_]Column{
        .{ .i64 = .{ .values = &p } },
        .{ .i64 = .{ .values = &o } },
        .{ .i64 = .{ .values = &v } },
    };
    const elems = [_]schema.SchemaElement{ intElem(), intElem(), intElem() };
    const t: Table = .{ .rows = 6, .cols = &cols, .elems = &elems, .decs = &.{ null, null, null } };
    const calls = [_]Call{
        .{ .func = .row_number },
        .{ .func = .rank },
        .{ .func = .dense_rank },
        .{ .func = .lag, .arg = 2, .default = .{ .int = -1 } },
        .{ .func = .sum, .arg = 2 },
        .{ .func = .sum, .arg = 2, .frame = .{ .rows = true, .start = .{ .preceding = 1 }, .end = .current_row } },
        .{ .func = .max, .arg = 2, .frame = .{ .rows = true, .start = .current_row, .end = .unbounded_following } },
        .{ .func = .count_star, .frame = .{ .rows = true, .start = .{ .following = 5 }, .end = .{ .following = 9 } } },
        .{ .func = .percent_rank },
        .{ .func = .cume_dist },
    };
    const out = try columns(a, t, .{ .partition = &.{0}, .order = &.{1}, .calls = &calls });
    try T.expectEqualSlices(i64, &.{ 1, 2, 3, 1, 2, 3 }, out[0].col.i64.values);
    try T.expectEqualSlices(i64, &.{ 1, 1, 3, 1, 2, 2 }, out[1].col.i64.values);
    try T.expectEqualSlices(i64, &.{ 1, 1, 2, 1, 2, 2 }, out[2].col.i64.values);
    try T.expectEqualSlices(i64, &.{ -1, 10, 20, -1, 40, 50 }, out[3].col.i64.values);
    // The default frame ends at the last peer, so rows 0 and 1 share their sum, and 4 and 5.
    try T.expectEqualSlices(i64, &.{ 30, 30, 60, 40, 150, 150 }, out[4].col.i64.values);
    try T.expectEqualSlices(i64, &.{ 10, 30, 50, 40, 90, 110 }, out[5].col.i64.values);
    try T.expectEqualSlices(i64, &.{ 30, 30, 30, 60, 60, 60 }, out[6].col.i64.values);
    // A frame wholly past the partition's end is empty.
    try T.expectEqualSlices(i64, &.{ 0, 0, 0, 0, 0, 0 }, out[7].col.i64.values);
    // Rows before the row's peers over the other rows of the partition (0 for a partition of one); the rows up to its last peer over all.
    try T.expectEqualSlices(f64, &.{ 0, 0, 1, 0, 0.5, 0.5 }, out[8].col.f64.values);
    const third: f64 = 1.0 / 3.0;
    try T.expectEqualSlices(f64, &.{ 2.0 / 3.0, 2.0 / 3.0, 1, third, 1, 1 }, out[9].col.f64.values);
}

fn intElem() schema.SchemaElement {
    return .{ .type = .INT64, .type_length = null, .repetition_type = .REQUIRED, .name = "c", .num_children = null, .scale = null, .precision = null, .field_id = null };
}

test "window: frames are clamped into the partition" {
    const T = std.testing;
    var pstart = [_]u32{ 2, 2, 2 };
    var pend = [_]u32{ 5, 5, 5 };
    var gstart = [_]u32{ 2, 3, 4 };
    var gend = [_]u32{ 3, 4, 5 };
    var dense = [_]u32{ 1, 2, 3 };
    const r: Runs = .{ .pstart = &pstart, .pend = &pend, .gstart = &gstart, .gend = &gend, .dense = &dense };
    const far = frameOf(.{ .rows = true, .start = .{ .following = 40 }, .end = .{ .following = 50 } }, r, 1);
    try T.expectEqual(@as(u32, 5), far.lo);
    try T.expectEqual(@as(u32, 5), far.hi);
    const behind = frameOf(.{ .rows = true, .start = .{ .preceding = 50 }, .end = .{ .preceding = 40 } }, r, 1);
    try T.expectEqual(@as(u32, 2), behind.lo);
    try T.expectEqual(@as(u32, 2), behind.hi);
    const default = frameOf(.{}, r, 1);
    try T.expectEqual(@as(u32, 2), default.lo);
    try T.expectEqual(@as(u32, 4), default.hi);
    const whole = frameOf(.{ .end = .unbounded_following }, r, 1);
    try T.expectEqual(@as(u32, 5), whole.hi);
    // Peers on both sides.
    const peers = frameOf(.{ .start = .current_row }, r, 1);
    try T.expectEqual(@as(u32, 3), peers.lo);
    try T.expectEqual(@as(u32, 4), peers.hi);
}

fn offset(m: i128) plan_types.Reach {
    return .{ .number = .{ .mant = m, .scale = 0, .bits = 32 } };
}

fn reachFrame(start: Bound, end: Bound) Frame {
    return .{ .start = start, .end = end };
}

/// The frames of `f` over one partition whose ORDER BY key is `keys`, NULL where `nulls` says.
fn rangeSpans(a: std.mem.Allocator, keys: []const i64, nulls: []const bool, elem: schema.SchemaElement, descending: bool, f: Frame) ![]const Span {
    const p = try a.alloc(i64, keys.len);
    @memset(p, 1);
    const def = try a.alloc(u32, keys.len);
    for (def, nulls) |*d, is_null| d.* = @intFromBool(!is_null);
    const cols = try a.dupe(Column, &.{
        .{ .i64 = .{ .values = p } },
        .{ .i64 = .{ .values = keys, .def_levels = def, .max_def = 1, .has_nulls = true } },
    });
    const elems = try a.dupe(schema.SchemaElement, &.{ intElem(), elem });
    const t: Table = .{ .rows = keys.len, .cols = cols, .elems = elems, .decs = &.{ null, null } };
    const spec: Spec = .{ .partition = &.{0}, .order = &.{1}, .descending = descending, .calls = &.{} };
    const r = try runs(a, t, spec);
    const out = try a.alloc(Span, keys.len);
    try rangeFrames(t, r, spec, f, out);
    return out;
}

fn expectSpans(want: []const [2]u32, got: []const Span) !void {
    try std.testing.expectEqual(want.len, got.len);
    for (want, got) |w, g| {
        try std.testing.expectEqual(w[0], g.lo);
        try std.testing.expectEqual(w[1], g.hi);
    }
}

test "window: a RANGE offset frames the rows whose key is within reach, a NULL key its peers" {
    const T = std.testing;
    var arena_state = std.heap.ArenaAllocator.init(T.allocator);
    defer arena_state.deinit();
    const a = arena_state.allocator();
    // Ascending, NULLs last: keys 1, 2, 2, 4, 7 and two NULLs.
    const keys = [_]i64{ 1, 2, 2, 4, 7, 0, 0 };
    const nulls = [_]bool{ false, false, false, false, false, true, true };
    try expectSpans(&.{ .{ 0, 3 }, .{ 0, 3 }, .{ 0, 3 }, .{ 3, 4 }, .{ 4, 5 }, .{ 5, 7 }, .{ 5, 7 } }, try rangeSpans(a, &keys, &nulls, intElem(), false, reachFrame(.{ .reach_preceding = offset(1) }, .{ .reach_following = offset(1) })));
    // The end 1 PRECEDING can leave a frame empty; a NULL key's frame runs to the end of its peers.
    try expectSpans(&.{ .{ 0, 0 }, .{ 0, 1 }, .{ 0, 1 }, .{ 0, 3 }, .{ 0, 4 }, .{ 0, 7 }, .{ 0, 7 } }, try rangeSpans(a, &keys, &nulls, intElem(), false, reachFrame(.unbounded_preceding, .{ .reach_preceding = offset(1) })));
    // 1 FOLLOWING to the end takes in the NULLs, which sort after every key.
    try expectSpans(&.{ .{ 1, 7 }, .{ 3, 7 }, .{ 3, 7 }, .{ 4, 7 }, .{ 5, 7 }, .{ 5, 7 }, .{ 5, 7 } }, try rangeSpans(a, &keys, &nulls, intElem(), false, reachFrame(.{ .reach_following = offset(1) }, .unbounded_following)));
    // Descending, NULLs first: 7, 4, 2, 2, 1 after two NULLs; before and after follow the order of the sort.
    const down = [_]i64{ 0, 0, 7, 4, 2, 2, 1 };
    const down_nulls = [_]bool{ true, true, false, false, false, false, false };
    try expectSpans(&.{ .{ 0, 2 }, .{ 0, 2 }, .{ 2, 3 }, .{ 3, 4 }, .{ 4, 6 }, .{ 4, 6 }, .{ 4, 7 } }, try rangeSpans(a, &down, &down_nulls, intElem(), true, reachFrame(.{ .reach_preceding = offset(1) }, .current_row)));
    // From the row's first peer to the last row with its key.
    try expectSpans(&.{ .{ 0, 1 }, .{ 1, 3 }, .{ 1, 3 }, .{ 3, 4 }, .{ 4, 5 }, .{ 5, 7 }, .{ 5, 7 } }, try rangeSpans(a, &keys, &nulls, intElem(), false, reachFrame(.current_row, .{ .reach_following = offset(0) })));
}

test "window: a RANGE offset past the key's type or the wrong way is an error, and the key's type decides" {
    const T = std.testing;
    var arena_state = std.heap.ArenaAllocator.init(T.allocator);
    defer arena_state.deinit();
    const a = arena_state.allocator();
    const keys = [_]i64{ 100, 126, 127 };
    const none = [_]bool{ false, false, false };
    var tiny = intElem();
    tiny.converted_type = .INT_8;
    // 127 + 1 leaves a TINYINT; 200 is an INTEGER offset, and the sum is computed in that.
    try T.expectError(error.WindowRangeOverflow, rangeSpans(a, &keys, &none, tiny, false, reachFrame(.current_row, .{ .reach_following = offset(1) })));
    try expectSpans(&.{ .{ 0, 3 }, .{ 0, 3 }, .{ 0, 3 } }, try rangeSpans(a, &keys, &none, tiny, false, reachFrame(.{ .reach_preceding = offset(200) }, .unbounded_following)));
    try T.expectError(error.WindowRangeOffset, rangeSpans(a, &keys, &none, intElem(), false, reachFrame(.{ .reach_preceding = offset(-1) }, .current_row)));
    try T.expectError(error.WindowRangeOffset, rangeSpans(a, &keys, &none, intElem(), true, reachFrame(.current_row, .{ .reach_following = offset(-1) })));
    // An INTERVAL needs a DATE or TIMESTAMP key.
    try T.expectError(error.WindowRangeKey, rangeSpans(a, &keys, &none, intElem(), false, reachFrame(.{ .reach_preceding = .{ .interval = .{ .months = 0, .days = 1, .micros = 0 } } }, .current_row)));
}

/// The frames of `f` over one partition whose ORDER BY key is `key`, in the lane, element and DECIMAL it is given.
fn keySpans(a: std.mem.Allocator, key: Column, elem: schema.SchemaElement, dec: ?expr_ast.Dec, rows: usize, f: Frame) ![]const Span {
    const p = try a.alloc(i64, rows);
    @memset(p, 1);
    const cols = try a.dupe(Column, &.{ .{ .i64 = .{ .values = p } }, key });
    const elems = try a.dupe(schema.SchemaElement, &.{ intElem(), elem });
    const t: Table = .{ .rows = rows, .cols = cols, .elems = elems, .decs = &.{ null, dec } };
    const spec: Spec = .{ .partition = &.{0}, .order = &.{1}, .calls = &.{} };
    const r = try runs(a, t, spec);
    const out = try a.alloc(Span, rows);
    try rangeFrames(t, r, spec, f, out);
    return out;
}

test "window: an INTERVAL offset on a DATE moves by calendar months, the day clamped to the month" {
    const T = std.testing;
    var arena_state = std.heap.ArenaAllocator.init(T.allocator);
    defer arena_state.deinit();
    const a = arena_state.allocator();
    // 2000-01-31, 2000-02-29, 2000-03-31.
    const days = [_]i64{ 10987, 11016, 11047 };
    const none = [_]bool{ false, false, false };
    var date = intElem();
    date.logical_type = .{ .DATE = .{} };
    const month: plan_types.Reach = .{ .interval = .{ .months = 1, .days = 0, .micros = 0 } };
    try expectSpans(&.{ .{ 0, 1 }, .{ 0, 2 }, .{ 1, 3 } }, try rangeSpans(a, &days, &none, date, false, reachFrame(.{ .reach_preceding = month }, .current_row)));
    // A whole number on a DATE is days.
    try expectSpans(&.{ .{ 0, 1 }, .{ 1, 2 }, .{ 2, 3 } }, try rangeSpans(a, &days, &none, date, false, reachFrame(.{ .reach_preceding = offset(1) }, .current_row)));
}

test "window: a RANGE offset's bound is an error where DuckDB's 38 digits or DATE's range runs out" {
    const T = std.testing;
    var arena_state = std.heap.ArenaAllocator.init(T.allocator);
    defer arena_state.deinit();
    const a = arena_state.allocator();
    const big: i128 = std.math.pow(i128, 10, 38) - 1;
    const zero = [_]i128{big};
    const low = [_]i128{-big};
    const dec38_0: ?expr_ast.Dec = .{ .precision = 38, .scale = 0 };
    const dec38_37: ?expr_ast.Dec = .{ .precision = 38, .scale = 37 };
    const dec10_2: ?expr_ast.Dec = .{ .precision = 10, .scale = 2 };
    // DECIMAL(38, 0) at either limit: one more digit is an Out of Range error, DuckDB's too.
    try T.expectError(error.WindowRangeOverflow, keySpans(a, .{ .i128 = .{ .values = &zero } }, intElem(), dec38_0, 1, reachFrame(.current_row, .{ .reach_following = offset(1) })));
    try T.expectError(error.WindowRangeOverflow, keySpans(a, .{ .i128 = .{ .values = &low } }, intElem(), dec38_0, 1, reachFrame(.{ .reach_preceding = offset(1) }, .current_row)));
    try expectSpans(&.{.{ 0, 1 }}, try keySpans(a, .{ .i128 = .{ .values = &zero } }, intElem(), dec38_0, 1, reachFrame(.{ .reach_preceding = offset(1) }, .current_row)));
    // DECIMAL(38, 37) at its limit with the smallest offset: the same, one place to the right of the point.
    const least: plan_types.Reach = .{ .number = .{ .mant = 1, .scale = 37, .bits = 32 } };
    try T.expectError(error.WindowRangeOverflow, keySpans(a, .{ .i128 = .{ .values = &zero } }, intElem(), dec38_37, 1, reachFrame(.current_row, .{ .reach_following = least })));
    // An offset with more decimals than the key casts the key column up: a key too big for it is a Conversion Error.
    const milli: plan_types.Reach = .{ .number = .{ .mant = 1, .scale = 3, .bits = 32 } };
    try T.expectError(error.WindowRangeCast, keySpans(a, .{ .i128 = .{ .values = &zero } }, intElem(), dec10_2, 1, reachFrame(.current_row, .{ .reach_following = milli })));
    const small = [_]i128{500};
    try expectSpans(&.{.{ 0, 1 }}, try keySpans(a, .{ .i128 = .{ .values = &small } }, intElem(), dec10_2, 1, reachFrame(.current_row, .{ .reach_following = milli })));
    // And an offset with fewer decimals is the one cast up: 10^36 at scale 0 is 38 digits at the key's scale 2.
    const huge: plan_types.Reach = .{ .number = .{ .mant = std.math.pow(i128, 10, 36), .scale = 0, .bits = 32 } };
    try T.expectError(error.WindowRangeCast, keySpans(a, .{ .i128 = .{ .values = &small } }, intElem(), dec10_2, 1, reachFrame(.current_row, .{ .reach_following = huge })));
    // A fractional offset on an integer key: the BIGINT cast to DECIMAL(38, 20) fails, to DECIMAL(38, 19) it does not.
    const wide = [_]i64{1_000_000_000_000_000_000};
    const scale20: plan_types.Reach = .{ .number = .{ .mant = 1, .scale = 20, .bits = 32 } };
    const scale19: plan_types.Reach = .{ .number = .{ .mant = 1, .scale = 19, .bits = 32 } };
    try T.expectError(error.WindowRangeCast, keySpans(a, .{ .i64 = .{ .values = &wide } }, intElem(), null, 1, reachFrame(.{ .reach_preceding = scale20 }, .current_row)));
    try expectSpans(&.{.{ 0, 1 }}, try keySpans(a, .{ .i64 = .{ .values = &wide } }, intElem(), null, 1, reachFrame(.{ .reach_preceding = scale19 }, .current_row)));
    // The sum a fractional offset makes is a DECIMAL's: 10 + (10^38 - 1) leaves it.
    const one = [_]i64{1};
    const largest: plan_types.Reach = .{ .number = .{ .mant = big, .scale = 1, .bits = 32 } };
    try T.expectError(error.WindowRangeOverflow, keySpans(a, .{ .i64 = .{ .values = &one } }, intElem(), null, 1, reachFrame(.current_row, .{ .reach_following = largest })));
    // A DATE takes whole days that fit an INTEGER, and a sum the days a DATE holds: 2147483647 days past the epoch is
    // DuckDB's "Date out of range", while 3000000000 is a BIGINT and DuckDB binds no -(DATE, BIGINT).
    var date = intElem();
    date.logical_type = .{ .DATE = .{} };
    const epoch = [_]i64{0};
    const past: plan_types.Reach = .{ .number = .{ .mant = std.math.maxInt(i32), .scale = 0, .bits = 32 } };
    const fits: plan_types.Reach = .{ .number = .{ .mant = std.math.maxInt(i32) - 1, .scale = 0, .bits = 32 } };
    const wide_day: plan_types.Reach = .{ .number = .{ .mant = 3_000_000_000, .scale = 0, .bits = 64 } };
    try T.expectError(error.WindowRangeOverflow, keySpans(a, .{ .i64 = .{ .values = &epoch } }, date, null, 1, reachFrame(.current_row, .{ .reach_following = past })));
    try expectSpans(&.{.{ 0, 1 }}, try keySpans(a, .{ .i64 = .{ .values = &epoch } }, date, null, 1, reachFrame(.current_row, .{ .reach_following = fits })));
    try T.expectError(error.WindowRangeKey, keySpans(a, .{ .i64 = .{ .values = &epoch } }, date, null, 1, reachFrame(.{ .reach_preceding = wide_day }, .current_row)));
}

test "window: AVG finalizes as DuckDB does, short types in doubles and the rest in long doubles" {
    const T = std.testing;
    // 4-digit DECIMALs and 16-bit integers divide in doubles by count * scale; wider ones in long doubles.
    try T.expectEqual(@as(f64, 2.5), averageOf(25, 1, 8, .{ .precision = 4, .scale = 1 }));
    try T.expectEqual(@as(f64, 1.5), averageOf(3, 2, 16, null));
    try T.expectEqual(@as(f64, 12.3456), averageOf(123456, 1, 128, .{ .precision = 12, .scale = 4 }));
    try T.expectEqual(@as(f64, 3.5), averageOf(7, 2, 64, null));
}

test "window: the allocator refuses past its ledger and counts what it hands out" {
    const T = std.testing;
    var ledger: core_memory.SharedBudget = .{ .limit = 1000 };
    var counted: Ledgered = .{ .inner = T.allocator, .ledger = &ledger };
    const a = counted.allocator();
    const first = try a.alloc(u8, 600);
    defer a.free(first);
    try T.expectError(error.OutOfMemory, a.alloc(u8, 600));
    const second = try a.alloc(u8, 400);
    defer a.free(second);
    try T.expectEqual(@as(usize, 1000), ledger.used.load(.monotonic));
}

test "window: rows sort stably by key, descending, NULLs where asked" {
    const T = std.testing;
    var arena_state = std.heap.ArenaAllocator.init(T.allocator);
    defer arena_state.deinit();
    const a = arena_state.allocator();
    const k = [_]i64{ 2, 1, 2, 3, 1, 2 };
    const defs = [_]u32{ 1, 1, 0, 1, 1, 1 };
    const cols = [_]Column{.{ .i64 = .{ .values = &k, .def_levels = &defs, .max_def = 1, .has_nulls = true } }};
    // Ascending, NULL last: 1 1 2 2 2 NULL (the equal keys keep their order).
    const asc = try sortedRows(a, &cols, &.{.{ .col = 0, .descending = false, .nulls_first = false }}, 6);
    try T.expectEqualSlices(u32, &.{ 1, 4, 0, 5, 3, 2 }, asc);
    // Descending, NULL first.
    const desc = try sortedRows(a, &cols, &.{.{ .col = 0, .descending = true, .nulls_first = true }}, 6);
    try T.expectEqualSlices(u32, &.{ 2, 3, 0, 5, 1, 4 }, desc);
    // No keys: the file's order.
    const none = try sortedRows(a, &cols, &.{}, 6);
    try T.expectEqualSlices(u32, &.{ 0, 1, 2, 3, 4, 5 }, none);
}

test "window: sums slide per frame and do not overflow across partitions or steps" {
    const T = std.testing;
    var arena_state = std.heap.ArenaAllocator.init(T.allocator);
    defer arena_state.deinit();
    const a = arena_state.allocator();
    const big: i128 = std.math.pow(i128, 10, 38) - 1;
    // Two partitions of two DECIMAL(38, 0) values at the limit; a frame of one row fits, a frame of two does not.
    const v = [_]i128{ big, big, big, big };
    const p = [_]i64{ 1, 1, 2, 2 };
    const cols = [_]Column{ .{ .i64 = .{ .values = &p } }, .{ .i128 = .{ .values = &v } } };
    const elems = [_]schema.SchemaElement{ intElem(), intElem() };
    const t: Table = .{ .rows = 4, .cols = &cols, .elems = &elems, .decs = &.{ null, .{ .precision = 38, .scale = 0 } } };
    const one_row: Call = .{ .func = .sum, .arg = 1, .frame = .{ .rows = true, .start = .current_row, .end = .current_row } };
    const out = try columns(a, t, .{ .partition = &.{0}, .order = &.{}, .calls = &.{one_row} });
    try T.expectEqualSlices(i128, &.{ big, big, big, big }, out[0].col.i128.values);
    const two_rows: Call = .{ .func = .sum, .arg = 1 };
    try T.expectError(error.WindowSumOverflow, columns(a, t, .{ .partition = &.{0}, .order = &.{}, .calls = &.{two_rows} }));
    // BIGINT sums are exact per frame and refused past 64 bits only where a frame is.
    const w = [_]i64{ std.math.maxInt(i64), 1, 5 };
    const q = [_]i64{ 1, 1, 2 };
    const wcols = [_]Column{ .{ .i64 = .{ .values = &q } }, .{ .i64 = .{ .values = &w } } };
    const wt: Table = .{ .rows = 3, .cols = &wcols, .elems = &elems, .decs = &.{ null, null } };
    const current: Call = .{ .func = .sum, .arg = 1, .frame = .{ .rows = true, .start = .current_row, .end = .current_row } };
    const fits = try columns(a, wt, .{ .partition = &.{0}, .order = &.{}, .calls = &.{current} });
    try T.expectEqualSlices(i64, &.{ std.math.maxInt(i64), 1, 5 }, fits[0].col.i64.values);
    try T.expectError(error.WindowSumOverflow, columns(a, wt, .{ .partition = &.{0}, .order = &.{}, .calls = &.{.{ .func = .sum, .arg = 1 }} }));
}

test "window: unsigned integers order as the unsigned numbers their raw bits are" {
    const T = std.testing;
    var arena_state = std.heap.ArenaAllocator.init(T.allocator);
    defer arena_state.deinit();
    const a = arena_state.allocator();
    // UINT64 values 1, 2^63 and 2^64 - 1 sit in the i64 lane as 1, minInt and -1; UINT32 2^31 as minInt of i32.
    const wide = [_]i64{ 1, std.math.minInt(i64), -1 };
    const narrow = [_]i32{ 1, std.math.minInt(i32), -1 };
    const cols = [_]Column{ .{ .i64 = .{ .values = &wide } }, .{ .i32 = .{ .values = &narrow } } };
    try T.expectEqual(std.math.Order.lt, orderAt(cols[0], 0, 1, true));
    try T.expectEqual(std.math.Order.lt, orderAt(cols[0], 1, 2, true));
    try T.expectEqual(std.math.Order.gt, orderAt(cols[0], 0, 1, false));
    try T.expectEqual(std.math.Order.lt, orderAt(cols[1], 0, 1, true));
    const keys64 = [_]SortIdx{.{ .col = 0, .descending = false, .nulls_first = false, .unsigned = true }};
    try T.expectEqualSlices(u32, &.{ 0, 1, 2 }, try sortedRows(a, &cols, &keys64, 3));
    const desc32 = [_]SortIdx{.{ .col = 1, .descending = true, .nulls_first = false, .unsigned = true }};
    try T.expectEqualSlices(u32, &.{ 2, 1, 0 }, try sortedRows(a, &cols, &desc32, 3));
    // Signed, the same bits sort the other way round.
    const signed = [_]SortIdx{.{ .col = 0, .descending = false, .nulls_first = false }};
    try T.expectEqualSlices(u32, &.{ 1, 2, 0 }, try sortedRows(a, &cols, &signed, 3));
}

test "window: ntile spreads the remainder over the first buckets, and the value functions read the frame" {
    const T = std.testing;
    var arena_state = std.heap.ArenaAllocator.init(T.allocator);
    defer arena_state.deinit();
    const a = arena_state.allocator();
    // Partition 1 has five rows, partition 2 two.
    const p = [_]i64{ 1, 1, 1, 1, 1, 2, 2 };
    const v = [_]i64{ 10, 20, 30, 40, 50, 60, 70 };
    const cols = [_]Column{ .{ .i64 = .{ .values = &p } }, .{ .i64 = .{ .values = &v } } };
    const elems = [_]schema.SchemaElement{ intElem(), intElem() };
    const t: Table = .{ .rows = 7, .cols = &cols, .elems = &elems, .decs = &.{ null, null } };
    const calls = [_]Call{
        .{ .func = .ntile, .offset = 2 },
        .{ .func = .ntile, .offset = 9 },
        .{ .func = .first_value, .arg = 1 },
        .{ .func = .last_value, .arg = 1, .frame = .{ .rows = true, .start = .{ .preceding = 1 }, .end = .{ .following = 1 } } },
        .{ .func = .nth_value, .arg = 1, .offset = 3 },
        .{ .func = .nth_value, .arg = 1, .offset = 2, .frame = .{ .rows = true, .start = .current_row, .end = .unbounded_following } },
    };
    const out = try columns(a, t, .{ .partition = &.{0}, .order = &.{1}, .calls = &calls });
    // Five rows in two buckets: three and two; two rows in two: one each; nine buckets of five rows: one row each.
    try T.expectEqualSlices(i64, &.{ 1, 1, 1, 2, 2, 1, 2 }, out[0].col.i64.values);
    try T.expectEqualSlices(i64, &.{ 1, 2, 3, 4, 5, 1, 2 }, out[1].col.i64.values);
    try T.expectEqualSlices(i64, &.{ 10, 10, 10, 10, 10, 60, 60 }, out[2].col.i64.values);
    // The frame is a row each side; the last row of each ends the partition.
    try T.expectEqualSlices(i64, &.{ 20, 30, 40, 50, 50, 70, 70 }, out[3].col.i64.values);
    // The default frame ends at the row's last peer: the third row of the partition exists from the third row on.
    const third = out[4].col.i64;
    try T.expectEqualSlices(u32, &.{ 0, 0, 1, 1, 1, 0, 0 }, third.def_levels.?);
    try T.expectEqual(@as(i64, 30), third.values[2]);
    // Two rows on from the current row: the next row, none for the last of a partition.
    const second = out[5].col.i64;
    try T.expectEqualSlices(u32, &.{ 1, 1, 1, 1, 0, 1, 0 }, second.def_levels.?);
    try T.expectEqual(@as(i64, 20), second.values[0]);
}
