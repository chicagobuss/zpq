//! Dense GROUP BY over a retained exact key domain.
//!
//! The planner's dictionary union can retain value-equal axes and their per-row-group code translations. Workers
//! fold an admitted domain by global ID, so keys are not serialized or resolved again for every row group.
//!
//! Every dictionary is checked against its translation before consumption. A missing or mismatched dictionary uses
//! the ordinary worker table; those partials join the lanes before final ranking. Unused entries stay untouched,
//! and each reached ID retains its least selected ordinal. String values are owned independently of reader windows.

const std = @import("std");
const agg = @import("agg.zig");
const agg_coded = @import("agg_coded.zig");
const expr_ast = @import("ast.zig");
const filter_eval = @import("../filter/eval.zig");
const filter_selection = @import("../filter/selection.zig");
const spawn = @import("../spawn.zig");
const hw = @import("../hw.zig");
const SelectedBatch = @import("../selected_batch.zig").SelectedBatch;
const key_pack = @import("key_pack.zig");
const expr_eval = @import("eval.zig");

const Accumulator = agg.Accumulator;
const AggCall = agg.AggCall;
const Column = filter_eval.Batch.Column;
const Sel = filter_selection.SelectionVector;

/// Largest retained axis and largest product folded globally, including NULL slots: lanes are walked at merge.
pub const max_ids: usize = 1 << 20;
const unseen = std.math.maxInt(u64);

/// One exact value domain. Dictionary codes are local; these owned values and translations establish identity.
pub const Axis = struct {
    values: union(enum) { int32: []u64, int64: []u64, bytes: [][]u8 },
    trans: [][]u32,
    file_base: []usize,
    bytes: usize,

    pub fn len(d: *const Axis) usize {
        return switch (d.values) {
            inline else => |v| v.len,
        };
    }

    pub fn ids(d: *const Axis) usize {
        return d.len() + 1;
    }

    pub fn deinit(d: *Axis, gpa: std.mem.Allocator) void {
        for (d.trans) |t| gpa.free(t);
        gpa.free(d.trans);
        gpa.free(d.file_base);
        switch (d.values) {
            .bytes => |v| {
                for (v) |key| gpa.free(key);
                gpa.free(v);
            },
            inline else => |v| gpa.free(v),
        }
        d.* = undefined;
    }

    pub fn forRowGroup(d: *const Axis, file: usize, rg: usize) ?[]const u32 {
        if (file >= d.file_base.len) return null;
        const i = d.file_base[file] + rg;
        const end = if (file + 1 < d.file_base.len) d.file_base[file + 1] else d.trans.len;
        if (i >= end) return null;
        return if (d.trans[i].len == 0) null else d.trans[i];
    }

    pub fn sameDictionary(d: *const Axis, col: Column, trans: []const u32) bool {
        if (trans.len != col.dictLen() + 1 or trans[trans.len - 1] != d.len()) return false;
        switch (d.values) {
            .int64 => |values| {
                if (col != .i64) return false;
                for (col.i64.dict, trans[0..col.dictLen()]) |v, id|
                    if (id >= values.len or values[id] != @as(u64, @bitCast(v))) return false;
            },
            .int32 => |values| {
                if (col != .i32) return false;
                for (col.i32.dict, trans[0..col.dictLen()]) |v, id|
                    if (id >= values.len or values[id] != @as(u64, @bitCast(@as(i64, v)))) return false;
            },
            .bytes => |values| {
                if (col != .string) return false;
                for (col.string.dict, trans[0..col.dictLen()]) |v, id|
                    if (id >= values.len or !std.mem.eql(u8, values[id], v)) return false;
            },
        }
        return true;
    }

    // Whole UINT32 code decode zero-extends the dictionary to i64; slices retain physical i32 bits.
    pub fn sameUnsigned32Dictionary(d: *const Axis, col: Column, trans: []const u32) bool {
        if (col != .i64 or d.values != .int32) return d.sameDictionary(col, trans);
        if (trans.len != col.dictLen() + 1 or trans[trans.len - 1] != d.len()) return false;
        const values = d.values.int32;
        for (col.i64.dict, trans[0..col.dictLen()]) |value, id| {
            if (id >= values.len or value < 0 or value > std.math.maxInt(u32)) return false;
            if (@as(u32, @truncate(values[id])) != @as(u64, @intCast(value))) return false;
        }
        return true;
    }

    fn dictionary(d: *const Axis, arena: std.mem.Allocator) !Column {
        return switch (d.values) {
            .int64 => |values| .{ .i64 = .{ .values = &.{}, .dict = @ptrCast(values) } },
            .int32 => |values| blk: {
                const out = try arena.alloc(i32, values.len);
                for (out, values) |*v, raw| v.* = @truncate(@as(i64, @bitCast(raw)));
                break :blk .{ .i32 = .{ .values = &.{}, .dict = out } };
            },
            .bytes => |values| blk: {
                const out = try arena.alloc([]const u8, values.len);
                for (out, values) |*v, key| v.* = key;
                break :blk .{ .string = .{ .values = &.{}, .dict = out } };
            },
        };
    }
};

pub const CountAxis = struct {
    codes: []const u32,
    trans: []const u32,
    ni: @TypeOf(@as(Column, undefined).nullInfo()),
    stride: usize,
};

/// Exact tuple identity; NULL occupies the trailing slot of each translated axis.
pub inline fn productId(axes: anytype, r: usize, comptime nullable: bool) !usize {
    var id: usize = 0;
    for (axes) |axis| {
        const is_null = if (nullable) (if (axis.ni.def_levels) |dl| dl[r] < axis.ni.max_def else false) else false;
        const code = if (is_null) axis.trans.len - 1 else axis.codes[r];
        if (!is_null and code >= axis.trans.len - 1) return error.BadColumn;
        id += axis.trans[code] * axis.stride;
    }
    return id;
}

/// Mixed-radix IDs over value-equal axes. Unused dictionary entries only enlarge the possible product.
pub const Domain = struct {
    axes: []Axis,
    bytes: usize,

    pub fn deinit(d: *Domain, gpa: std.mem.Allocator) void {
        for (d.axes) |*axis| axis.deinit(gpa);
        gpa.free(d.axes);
        d.* = undefined;
    }

    pub fn ids(d: *const Domain) usize {
        var n: usize = 1;
        for (d.axes) |*axis| n *|= axis.ids();
        return n;
    }

    /// A packed identity is a value-equal tuple, not a hash. Reject products outside one machine word.
    pub fn packedIds(d: *const Domain) ?usize {
        var n: usize = 1;
        for (d.axes) |*axis| n = std.math.mul(usize, n, axis.ids()) catch return null;
        return n;
    }

    /// Every page must still carry the dictionary the union translated. Refuse before any row changes state.
    pub fn bind(
        d: *const Domain,
        keys: []const expr_ast.Expr,
        file: usize,
        rg: usize,
        batch: *const filter_eval.Batch,
        lookup: []const ?usize,
        bindings: []CountAxis,
    ) !bool {
        if (keys.len != d.axes.len or bindings.len != keys.len) return false;
        var stride: usize = 1;
        for (keys, d.axes, bindings) |key, *axis, *b| {
            if (key != .col_ref or key.col_ref.col_idx >= lookup.len) return false;
            const trans = axis.forRowGroup(file, rg) orelse return false;
            const pos = lookup[key.col_ref.col_idx] orelse return false;
            const col = batch.cols[pos];
            const same = if (key.col_ref.unsigned_32)
                axis.sameUnsigned32Dictionary(col, trans)
            else
                axis.sameDictionary(col, trans);
            if (col.nullInfo().max_rep != 0 or !same) return false;
            const codes = col.codes() orelse return false;
            const ni = col.nullInfo();
            if (codes.len < batch.num_rows) return error.BadColumn;
            if (ni.def_levels) |dl| if (dl.len < batch.num_rows) return error.BadColumn;
            b.* = .{ .codes = codes, .trans = trans, .ni = ni, .stride = stride };
            stride = std.math.mul(usize, stride, axis.ids()) catch return false;
        }
        return true;
    }

    pub fn globalFits(d: *const Domain, workers: usize) bool {
        return d.ids() <= max_ids and workers *| d.ids() <= d.entries();
    }

    pub fn maxKeyBytes(d: *const Domain) usize {
        var n: usize = 0;
        for (d.axes) |axis| switch (axis.values) {
            .bytes => |values| {
                var widest: usize = 0;
                for (values) |value| widest = @max(widest, value.len);
                n +|= 5 +| widest;
            },
            else => n +|= 9,
        };
        return n;
    }

    /// Reconstruct a single axis from the exact product identity. Returned strings borrow the retained domain.
    pub const KeyValue = union(enum) { null_val, i: i128, s: []const u8 };

    pub fn keyValue(d: *const Domain, product: usize, ki: usize, key: expr_ast.ColRef) KeyValue {
        var rem = product;
        for (d.axes[0..ki]) |*axis| rem /= axis.ids();
        const axis = &d.axes[ki];
        const id = rem % axis.ids();
        return d.valueAt(ki, id, key);
    }

    pub fn valueAt(d: *const Domain, ki: usize, id: usize, key: expr_ast.ColRef) KeyValue {
        const axis = &d.axes[ki];
        if (id == axis.len()) return .null_val;
        return switch (axis.values) {
            .bytes => |values| .{ .s = values[id] },
            .int64 => |values| if (key.unsigned_64)
                .{ .i = values[id] }
            else
                .{ .i = @as(i64, @bitCast(values[id])) },
            .int32 => |values| if (key.unsigned_32)
                .{ .i = @as(u32, @truncate(values[id])) }
            else
                .{ .i = @as(i64, @bitCast(values[id])) },
        };
    }

    pub fn forRowGroup(d: *const Domain, file: usize, rg: usize) ?[]const u32 {
        if (d.axes.len != 1) return null;
        return d.axes[0].forRowGroup(file, rg);
    }

    /// Possible local composites the established coded fold resolves, summed across row groups.
    pub fn entries(d: *const Domain) usize {
        if (d.axes.len == 0) return 0;
        var n: usize = 0;
        for (d.axes[0].trans, 0..) |_, rg| {
            var product: usize = 1;
            for (d.axes) |*axis| product *|= axis.trans[rg].len;
            n +|= product;
        }
        return n;
    }
};

/// Lone integer calls, composite COUNT(*), or a composite signed SUM with optional COUNT(*), over retained keys.
/// Floats keep their ordered paths, and expressions, DECIMAL, strings and count(DISTINCT) the general ones.
pub fn shapeOk(calls: []const AggCall, keys: []const expr_ast.Expr) bool {
    if (countOnly(calls) or (keys.len > 1 and (sumCall(calls) != null or numericCalls(calls)))) {
        if (keys.len == 0) return false;
        for (keys) |key| {
            if (key != .col_ref) return false;
            const c = key.col_ref;
            if (!(c.expr_type == .str and c.physical_type == .BYTE_ARRAY) and
                !(c.expr_type == .i64 and (c.physical_type == .INT32 or c.physical_type == .INT64))) return false;
        }
        return true;
    }
    if (keys.len != 1 or keys[0] != .col_ref) return false;
    const k = keys[0].col_ref;
    if (k.expr_type != .i64 or (k.physical_type != .INT32 and k.physical_type != .INT64)) return false;
    for (calls) |call| {
        switch (call.func) {
            .count => {},
            .sum, .min, .max => if (call.result != .i64) return false,
            .avg => if (call.result != .avg_f64) return false,
            .count_distinct => return false,
        }
        if (call.arg) |arg| {
            if (arg != .col_ref or arg.col_ref.expr_type != .i64) return false;
        } else if (call.func != .count) return false;
    }
    return true;
}

/// One bare signed SUM, optionally accompanied by COUNT(*). Other calls retain their established folds.
pub fn sumCall(calls: []const AggCall) ?usize {
    if (calls.len == 0 or calls.len > 2) return null;
    var sum: ?usize = null;
    for (calls, 0..) |call, i| {
        if (call.where != null or call.state or call.offset != 0 or call.result != .i64) return null;
        switch (call.func) {
            .count => if (call.arg != null) return null,
            .sum => {
                if (sum != null) return null;
                const arg = call.arg orelse return null;
                if (arg != .col_ref or arg.col_ref.expr_type != .i64 or arg.col_ref.unsigned_64 or
                    arg.col_ref.unsigned_32 or (arg.col_ref.physical_type != .INT32 and
                    arg.col_ref.physical_type != .INT64)) return null;
                sum = i;
            },
            else => return null,
        }
    }
    return sum;
}

pub fn countOnly(calls: []const AggCall) bool {
    if (calls.len != 1) return false;
    const c = calls[0];
    return c.func == .count and c.arg == null and c.where == null and !c.state and c.offset == 0;
}

/// Numeric multi-call states use the established exact/float folds. Float extrema retain their ordinary path.
pub fn numericCalls(calls: []const AggCall) bool {
    if (calls.len == 0) return false;
    for (calls) |call| {
        if (call.where != null or call.offset != 0 or call.state or call.result == .bytes) return false;
        switch (call.func) {
            .count => if (call.arg != null) return false,
            .sum, .avg => if (call.arg == null or call.arg.?.typeOf() == .str) return false,
            .min, .max => if (call.arg == null or call.arg.?.typeOf() == .f64 or
                call.arg.?.typeOf() == .str) return false,
            .count_distinct => return false,
        }
    }
    return true;
}

pub fn orderSensitive(calls: []const AggCall) bool {
    for (calls) |call| {
        if (call.func == .count) continue;
        if (call.result == .f64 or (call.result == .avg_f64 and call.arg.?.typeOf() != .i64)) return true;
    }
    return false;
}

/// Generic states are useful only while the complete id domain fits the worker's cache.
pub fn numericFits(ids: usize, calls: []const AggCall) bool {
    return ids <= max_ids and numericCalls(calls) and
        ids *| (@sizeOf(u64) +| calls.len *| @sizeOf(Accumulator)) <= hw.l2BytesPerWorkerOr(256 << 10);
}

/// Per-id row-group scratch; decoded columns, selections and expression DAGs use the ordinary batch reservation.
pub fn numericScratchBytes(ids: usize, calls: []const AggCall) usize {
    return ids *| (@sizeOf(u32) +| calls.len *| (@sizeOf(agg.DecSum) + @sizeOf(i64) + @sizeOf(u64)));
}

pub fn countBytes(rows: u64) usize {
    return if (rows <= std.math.maxInt(u32)) @sizeOf(u32) else @sizeOf(u64);
}

/// The same row bound chooses storage and its prepaid quote. Selection can only lower a lane's count.
pub fn laneBytes(ids: usize, calls: []const AggCall, rows: u64, product_sum: bool) usize {
    if (product_sum and sumCall(calls) != null)
        return ids *| (if (rows < compact_present) @as(usize, @sizeOf(CompactSum)) else @sizeOf(Sum) + @sizeOf(u64));
    const state = if (countOnly(calls)) countBytes(rows) else @sizeOf(Accumulator) *| calls.len;
    return ids *| (@sizeOf(u64) +| state);
}

fn partialRows(rows: u64) usize {
    return @intCast(@min(rows, @max(1, hw.l2BytesPerWorkerOr(256 << 10) / (@sizeOf(i64) + @sizeOf(u32)))));
}

pub fn partialBytes(ids: usize, rows: u64) usize {
    if (rows >= compact_present) return 0;
    return ids *| @sizeOf(i64) +| @min(ids, partialRows(rows)) *| @sizeOf(u32);
}

/// Bytes at most the table `intoTable` fills holds for `ids` groups: what `GroupTable.reserveFixed` charges.
pub fn tableBytes(ids: usize, stride: usize, n_calls: usize) usize {
    const slots = std.math.ceilPowerOfTwo(usize, @max(16, ids *| 4 / 3 + 1)) catch std.math.maxInt(usize);
    return ids *| (stride + @sizeOf(u64) + @sizeOf(Accumulator) *| n_calls) +| slots *| (@sizeOf(u64) + @sizeOf(u32) * 2);
}

/// Variable-key tables use the same per-entry charge as ordinary insertion, including capacity slack.
pub fn domainTableBytes(d: *const Domain, ids: usize, stride: usize, n_calls: usize) usize {
    if (stride != 0) return tableBytes(ids, stride, n_calls);
    return ids *| agg.GroupTable.variableEntryBytes(d.maxKeyBytes(), n_calls);
}

/// The reservation includes the retained domain, every worker lane, the merge table and ranking scratch.
/// Planning and admission share this quote; the table credit transfers to GroupTable at merge.
pub fn charge(
    d: *const Domain,
    workers: usize,
    calls: []const AggCall,
    rows: u64,
    stride: usize,
    cut: ?usize,
) struct { total: usize, table: usize } {
    const ids = d.ids();
    const table = domainTableBytes(d, ids, stride, calls.len);
    const heap = if (cut) |h| @min(h, ids) *| @sizeOf(u32) else 0;
    const product_sum = d.axes.len > 1 and sumCall(calls) != null;
    const product_numeric = d.axes.len > 1 and !countOnly(calls) and !product_sum;
    const positions = if (product_sum and rows < compact_present)
        (d.axes[0].trans.len +| 1) *| @sizeOf(u32)
    else
        0;
    const lanes = laneBytes(ids, calls, rows, product_sum);
    const partials = if (product_sum) partialBytes(ids, rows) else if (product_numeric)
        numericScratchBytes(ids, calls)
    else
        0;
    const raw_keys = for (d.axes) |axis| {
        if (axis.values == .bytes) break false;
    } else true;
    const identity = if (product_sum or product_numeric) key_pack.planBytes(d.axes.len, true, raw_keys) else 0;
    return .{
        .total = d.bytes +| identity +| workers *| (lanes +| partials) +| table +| heap +|
            ids *| @sizeOf(u32) +| positions,
        .table = table,
    };
}

const present: u64 = 1 << 63;
// Keep the full firstPosition separately: row-group ordinals cannot be narrowed from a total-row bound.
const Sum = extern struct { value: i128 align(8) = 0, count_present: u64 = 0 };

const compact_present: u32 = 1 << 31;

/// Prefix rows in the scan's original work-item order, including empty items. Borrowed, prepaid query storage.
pub const Ordinals = struct {
    starts: []const u32,

    pub fn init(starts: []const u32) ?Ordinals {
        if (starts.len == 0 or starts[0] != 0 or starts[starts.len - 1] >= compact_present) return null;
        for (starts[1..], starts[0 .. starts.len - 1]) |next, previous| if (next < previous) return null;
        return .{ .starts = starts };
    }

    pub fn rows(o: Ordinals) u32 {
        return o.starts[o.starts.len - 1];
    }

    pub fn pack(o: Ordinals, position: u64) !u32 {
        const item = position >> 32;
        const row: u32 = @truncate(position);
        if (item >= o.starts.len - 1 or row >= o.starts[item + 1] - o.starts[item]) return error.BadColumn;
        return o.starts[item] + row;
    }

    pub fn unpack(o: Ordinals, ordinal: u32) !u64 {
        if (ordinal >= o.rows()) return error.BadColumn;
        const item = std.sort.upperBound(u32, o.starts, ordinal, struct {
            fn order(value: u32, start: u32) std.math.Order {
                return std.math.order(value, start);
            }
        }.order) - 1;
        return agg.firstPosition(item, ordinal - o.starts[item]);
    }
};

/// Exact SUM with first selected input ordinal; the row proof leaves the count's high bit for SUM-present.
pub const CompactSum = extern struct {
    value: i128 align(8) = 0,
    count_present: u32 = 0,
    first: u32 = std.math.maxInt(u32),

    pub fn count(s: CompactSum) u32 {
        return s.count_present & ~compact_present;
    }

    pub fn sum(s: CompactSum) ?i128 {
        return if (s.count_present & compact_present != 0) s.value else null;
    }

    pub fn add(s: *CompactSum, value: ?i64, ordinal: u32) void {
        s.count_present += 1;
        if (value) |v| {
            s.count_present |= compact_present;
            s.value += v;
        }
        s.first = @min(s.first, ordinal);
    }

    pub fn merge(s: *CompactSum, other: CompactSum) void {
        const n = s.count() + other.count();
        s.count_present = n | ((s.count_present | other.count_present) & compact_present);
        s.value += other.value;
        s.first = @min(s.first, other.first);
    }
};

const RowOrdinals = struct {
    base: u64,
    input_rows: ?[]const u32 = null,
    limit: u64 = std.math.maxInt(u64),

    fn at(o: RowOrdinals, row: usize) !u64 {
        const offset: u64 = if (o.input_rows) |rows| rows[row] else @intCast(row);
        const ordinal = try std.math.add(u64, o.base, offset);
        if (ordinal >= o.limit) return error.BadColumn;
        return ordinal;
    }
};

fn sumValue(values: anytype, row: usize) !?i64 {
    if (values.def_levels) |dl| if (dl[row] < values.max_def) return null;
    const at = if (values.codes) |codes| codes[row] else row;
    const vs = if (values.codes != null) values.dict else values.values;
    if (at >= vs.len) return error.BadColumn;
    return vs[at];
}

fn maxAbs(values: []const i64) u64 {
    const width = std.simd.suggestVectorLength(i64) orelse 1;
    const V = @Vector(width, i64);
    var maximum: @Vector(width, u64) = @splat(0);
    var at: usize = 0;
    while (values.len - at >= width) : (at += width) {
        const v: V = values[at..][0..width].*;
        maximum = @max(maximum, @abs(v));
    }
    var out = @reduce(.Max, maximum);
    for (values[at..]) |v| out = @max(out, @abs(v));
    return out;
}

/// Actual decoded values are a proof; unverified footer extrema and uninitialized NULL slots are not.
fn magnitude(values: anytype, rows: usize) ?u64 {
    if (@TypeOf(values.values[0]) == i32) return @as(u64, 1) << 31;
    if (values.codes != null) return maxAbs(values.dict);
    if (values.values.len < rows) return null;
    if (values.def_levels) |defs| {
        var out: u64 = 0;
        for (defs[0..rows], 0..) |dl, row| {
            if (dl >= values.max_def) out = @max(out, @abs(values.values[row]));
        }
        return out;
    }
    return maxAbs(values.values[0..rows]);
}

fn activeRange(sel: ?*const Sel, lo: usize, hi: usize, comptime row: anytype, ctx: anytype) void {
    const selected = sel orelse {
        for (lo..hi) |r| row(ctx, r);
        return;
    };
    var at = lo;
    while (at < hi) {
        const n = @min(64 - at % 64, hi - at);
        const mask = @as(u64, std.math.maxInt(u64)) >> @as(u6, @intCast(64 - n));
        var bits = (selected.mask[at / 64] >> @as(u6, @intCast(at % 64))) & mask;
        while (bits != 0) {
            row(ctx, at + @ctz(bits));
            bits &= bits - 1;
        }
        at += n;
    }
}

fn exactDelta(
    ra: std.mem.Allocator,
    values: anytype,
    comp: []const u32,
    ids: usize,
    sel: *const Sel,
) ![]agg.DecSum {
    if (values.values.len < comp.len) return error.BadColumn;
    if (values.def_levels) |dl| if (dl.len < comp.len) return error.BadColumn;
    const delta = try ra.alloc(agg.DecSum, ids);
    @memset(delta, .{});
    var maximum: u128 = 0;
    activeRange(sel, 0, comp.len, struct {
        fn row(c: anytype, r: usize) void {
            if (c.values.def_levels) |dl| if (dl[r] < c.values.max_def) return;
            c.maximum.* = @max(c.maximum.*, @as(u128, @abs(c.values.values[r])));
        }
    }.row, .{ .values = values, .maximum = &maximum });
    const safe_rows: usize = if (maximum == 0) comp.len else @intCast(@min(comp.len, std.math.maxInt(i64) / maximum));
    const mini = @min(partialRows(comp.len), safe_rows);
    if (mini <= 1) {
        activeRange(sel, 0, comp.len, struct {
            fn row(c: anytype, r: usize) void {
                if (c.values.def_levels) |dl| if (dl[r] < c.values.max_def) return;
                c.delta[c.comp[r]].add(c.values.values[r], 1);
            }
        }.row, .{ .values = values, .comp = comp, .delta = delta });
        return delta;
    }
    const partials = try ra.alloc(i64, ids);
    const counts = try ra.alloc(u64, ids);
    var off: usize = 0;
    while (off < comp.len) {
        const end = off + @min(mini, comp.len - off);
        @memset(partials, 0);
        @memset(counts, 0);
        activeRange(sel, off, end, struct {
            fn row(c: anytype, r: usize) void {
                if (c.values.def_levels) |dl| if (dl[r] < c.values.max_def) return;
                const id = c.comp[r];
                c.partials[id] += @intCast(c.values.values[r]);
                c.counts[id] += 1;
            }
        }.row, .{ .values = values, .comp = comp, .partials = partials, .counts = counts });
        for (delta, partials, counts) |*d, sum, n| if (n != 0) d.add(sum, n);
        off = end;
    }
    return delta;
}

/// Root evaluation stays call-major, including checked DECIMAL errors. SUM and AVG share each exact root's delta.
fn foldExactNumeric(
    ra: std.mem.Allocator,
    gpa: std.mem.Allocator,
    accs: []Accumulator,
    gids: []const u32,
    comp: []const u32,
    calls: []const AggCall,
    batch: *const filter_eval.Batch,
    lookup: []const ?usize,
    sel: *const Sel,
) !bool {
    for (calls) |call| {
        if (call.func == .count and call.arg == null) continue;
        if ((call.func != .sum and call.func != .avg) or call.arg == null or call.arg.?.typeOf() != .dec)
            return false;
    }
    var plan: expr_eval.ExactPlan = .{};
    const roots = try ra.alloc(?usize, calls.len);
    for (calls, roots) |call, *root| {
        root.* = if (call.arg) |arg| (try plan.add(ra, batch, lookup, arg)) orelse return false else null;
    }
    const deltas = try ra.alloc(?[]agg.DecSum, plan.nodes.items.len);
    @memset(deltas, null);
    const restricted = batch.restrictedTo(sel);
    for (calls, roots, 0..) |call, root, ai| {
        if (root) |r| {
            if (deltas[r] == null) {
                const values = try plan.evaluate(ra, &restricted, lookup, r);
                deltas[r] = switch (values) {
                    inline .i32, .i64, .i128 => |col| try exactDelta(ra, col, comp, gids.len, sel),
                    else => return error.TypeMismatch,
                };
            }
            for (deltas[r].?, gids) |delta, gid| {
                if (delta.n == 0) continue;
                switch (accs[gid * calls.len + ai]) {
                    .sum_d, .avg_d => |*d| d.add(delta.get(), delta.n),
                    else => return error.UnsupportedAggType,
                }
            }
        } else try agg_coded.foldGroupedCall(
            ra,
            gpa,
            accs,
            gids,
            comp,
            gids.len,
            ai,
            calls.len,
            call,
            batch,
            lookup,
            sel,
            null,
        );
    }
    return true;
}

/// One worker's least positions and states: COUNT counters, exact composite SUM lanes, or generic accumulators.
/// Allocated on its first row group, so the worker first touches the pages it fills.
pub const Lanes = struct {
    domain: *const Domain,
    calls: []const AggCall,
    accs: []Accumulator = &.{},
    first: []u64 = &.{},
    rows: u64 = std.math.maxInt(u64),
    counts32: []u32 = &.{},
    counts64: []u64 = &.{},
    sums: []Sum = &.{},
    compact_sums: []CompactSum = &.{},
    positions: ?Ordinals = null,
    global_ordinals: bool = false,
    partials: []i64 = &.{},
    touched: []u32 = &.{},

    fn allocated(l: *const Lanes) bool {
        return l.first.len != 0 or l.compact_sums.len != 0;
    }

    fn seen(l: *const Lanes, id: usize) bool {
        return if (l.compact_sums.len != 0) l.compact_sums[id].count() != 0 else l.first[id] != unseen;
    }

    pub fn firstOrdinal(l: *const Lanes, id: usize) u64 {
        return if (l.compact_sums.len != 0) l.compact_sums[id].first else l.first[id];
    }

    pub fn position(l: *const Lanes, id: usize) u64 {
        if (l.global_ordinals) return l.firstOrdinal(id);
        if (l.compact_sums.len == 0) return l.first[id];
        if (!l.seen(id)) return unseen;
        return l.positions.?.unpack(l.compact_sums[id].first) catch unreachable;
    }

    fn ensure(l: *Lanes, gpa: std.mem.Allocator) !void {
        if (l.allocated()) return;
        const n = l.domain.ids();
        if (n > max_ids) return error.UnsupportedAggType;
        if (l.domain.axes.len > 1 and !countOnly(l.calls) and sumCall(l.calls) == null and
            !numericFits(n, l.calls)) return error.UnsupportedAggType;
        if (l.domain.axes.len > 1 and sumCall(l.calls) != null and l.rows >= present)
            return error.RowCountTooLarge;
        if (l.domain.axes.len > 1 and sumCall(l.calls) != null and l.rows < compact_present) {
            if (!l.global_ordinals) {
                const positions = l.positions orelse return error.UnsupportedAggType;
                if (positions.rows() != l.rows) return error.UnsupportedAggType;
            }
            l.compact_sums = try gpa.alloc(CompactSum, n);
            @memset(l.compact_sums, .{});
            return;
        }
        const first = try gpa.alloc(u64, n);
        errdefer gpa.free(first);
        if (l.domain.axes.len > 1 and sumCall(l.calls) != null) {
            l.sums = try gpa.alloc(Sum, n);
            @memset(l.sums, .{});
            @memset(first, unseen);
            l.first = first;
            return;
        }
        if (countOnly(l.calls)) {
            if (countBytes(l.rows) == @sizeOf(u32)) {
                l.counts32 = try gpa.alloc(u32, n);
                @memset(l.counts32, 0);
            } else {
                l.counts64 = try gpa.alloc(u64, n);
                @memset(l.counts64, 0);
            }
            @memset(first, unseen);
            l.first = first;
            return;
        }
        const accs = try gpa.alloc(Accumulator, n * l.calls.len);
        @memset(first, unseen);
        for (0..n) |i| for (l.calls, accs[i * l.calls.len ..][0..l.calls.len]) |call, *acc| {
            acc.* = .init(call);
        };
        l.first = first;
        l.accs = accs;
    }

    pub fn deinit(l: *Lanes, gpa: std.mem.Allocator) void {
        gpa.free(l.accs);
        gpa.free(l.first);
        gpa.free(l.counts32);
        gpa.free(l.counts64);
        gpa.free(l.sums);
        gpa.free(l.compact_sums);
        gpa.free(l.partials);
        gpa.free(l.touched);
        l.accs = &.{};
        l.first = &.{};
        l.counts32 = &.{};
        l.counts64 = &.{};
        l.sums = &.{};
        l.compact_sums = &.{};
        l.partials = &.{};
        l.touched = &.{};
    }

    fn ensurePartials(l: *Lanes, gpa: std.mem.Allocator) !void {
        if (l.partials.len != 0) return;
        const n = l.domain.ids();
        if (n > max_ids) return error.UnsupportedAggType;
        const partials = try gpa.alloc(i64, n);
        errdefer gpa.free(partials);
        l.touched = try gpa.alloc(u32, @min(n, partialRows(l.rows)));
        l.partials = partials;
    }

    fn count(l: *const Lanes, id: usize) u64 {
        if (l.compact_sums.len != 0) return l.compact_sums[id].count();
        if (l.sums.len != 0) return l.sums[id].count_present & ~present;
        return if (l.counts32.len != 0) l.counts32[id] else l.counts64[id];
    }

    fn addCount(l: *Lanes, id: usize, n: u64) void {
        if (l.counts32.len != 0) l.counts32[id] += @intCast(n) else l.counts64[id] += n;
    }

    /// Validate every axis before changing a lane. PLAIN/mismatched chunks join through the ordinary table.
    pub fn foldKeys(
        l: *Lanes,
        ra: std.mem.Allocator,
        gpa: std.mem.Allocator,
        keys: []const expr_ast.Expr,
        file: usize,
        rg: usize,
        pos_base: u64,
        batch: *const filter_eval.Batch,
        lookup: []const ?usize,
        sel: *const Sel,
    ) !bool {
        if (keys.len != l.domain.axes.len or !shapeOk(l.calls, keys)) return false;
        if (keys.len == 1) return l.fold(ra, gpa, keys[0].col_ref, file, rg, pos_base, batch, lookup, sel);
        if (l.domain.ids() > max_ids or (!countOnly(l.calls) and l.rows >= present)) return false;
        if (!countOnly(l.calls) and sumCall(l.calls) == null and !numericFits(l.domain.ids(), l.calls)) return false;
        const bindings = try ra.alloc(CountAxis, keys.len);
        if (!try l.domain.bind(keys, file, rg, batch, lookup, bindings)) {
            // Ordered blocks cannot mix dense and table partials without changing float addition order.
            if (orderSensitive(l.calls)) return error.ExceededMemoryBudget;
            return false;
        }
        if (!countOnly(l.calls) and sumCall(l.calls) == null) {
            try l.foldNumeric(ra, gpa, bindings, .{ .base = pos_base }, batch, lookup, sel);
            return true;
        }
        if (sumCall(l.calls)) |ai| {
            if (l.rows < compact_present) {
                const positions = l.positions orelse return false;
                if (positions.rows() != l.rows) return false;
            }
            if (batch.num_rows == 0) return true;
            const ref = l.calls[ai].arg.?.col_ref;
            const pos = lookup[ref.col_idx] orelse return false;
            const col = batch.cols[pos];
            if (col != .i32 and col != .i64) return false;
            if (col.nullInfo().max_rep != 0) return false;
            switch (col) {
                inline .i32, .i64 => |c| {
                    if (c.rowCount() < batch.num_rows) return error.BadColumn;
                    if (c.def_levels) |dl| if (dl.len < batch.num_rows) return error.BadColumn;
                    try l.ensure(gpa);
                    if (bindings.len == 2 and bindings[0].ni.def_levels == null and
                        bindings[1].ni.def_levels == null)
                    {
                        try l.foldSum(gpa, bindings[0..2].*, false, c, pos_base, batch.num_rows, sel);
                    } else try l.foldSum(gpa, bindings, true, c, pos_base, batch.num_rows, sel);
                },
                else => unreachable,
            }
            return true;
        }
        try l.ensure(gpa);
        if (l.counts32.len != 0) {
            try l.foldProduct(u32, l.counts32, bindings, pos_base, batch.num_rows, sel);
        } else try l.foldProduct(u64, l.counts64, bindings, pos_base, batch.num_rows, sel);
        return true;
    }

    /// The shared adapter carries global input ordinals, including gaps left by selection and gathering.
    pub fn foldSelected(
        l: *Lanes,
        ra: std.mem.Allocator,
        gpa: std.mem.Allocator,
        keys: []const expr_ast.Expr,
        file: usize,
        rg: usize,
        source: SelectedBatch,
        lookup: []const ?usize,
    ) !bool {
        if (!l.global_ordinals or keys.len != l.domain.axes.len or !shapeOk(l.calls, keys)) return false;
        if (keys.len == 1 and !countOnly(l.calls)) return false;
        if (l.domain.ids() > max_ids or (!countOnly(l.calls) and l.rows >= present)) return false;
        if (!countOnly(l.calls) and sumCall(l.calls) == null and !numericFits(l.domain.ids(), l.calls)) return false;
        try source.validate();
        const rows = source.batch.num_rows;
        const ordinals: RowOrdinals = .{
            .base = source.input_base,
            .input_rows = source.input_rows,
            .limit = l.rows,
        };
        if (source.input_rows != null) {
            for (0..rows) |r| _ = try ordinals.at(r);
        } else if (rows != 0) _ = try ordinals.at(rows - 1);
        const bindings = try ra.alloc(CountAxis, keys.len);
        if (!try l.domain.bind(keys, file, rg, source.batch, lookup, bindings)) {
            if (orderSensitive(l.calls)) return error.ExceededMemoryBudget;
            return false;
        }
        if (!countOnly(l.calls) and sumCall(l.calls) == null) {
            var all: Sel = undefined;
            const sel = source.selected orelse blk: {
                all = try Sel.init(ra, rows);
                break :blk &all;
            };
            try l.foldNumeric(ra, gpa, bindings, ordinals, source.batch, lookup, sel);
            return true;
        }
        if (sumCall(l.calls)) |ai| {
            if (rows == 0) return true;
            const ref = l.calls[ai].arg.?.col_ref;
            if (ref.col_idx >= lookup.len) return false;
            const pos = lookup[ref.col_idx] orelse return false;
            if (pos >= source.batch.cols.len) return error.BadColumn;
            const col = source.batch.cols[pos];
            if ((col != .i32 and col != .i64) or col.nullInfo().max_rep != 0) return false;
            switch (col) {
                inline .i32, .i64 => |values| {
                    if (values.rowCount() < rows) return error.BadColumn;
                    if (values.def_levels) |dl| if (dl.len < rows) return error.BadColumn;
                    try l.ensure(gpa);
                    if (l.compact_sums.len != 0) {
                        try l.foldSumOrdinals(gpa, bindings, true, values, ordinals, rows, source.selected, true);
                    } else try l.foldSumOrdinals(gpa, bindings, true, values, ordinals, rows, source.selected, false);
                },
                else => unreachable,
            }
        } else {
            try l.ensure(gpa);
            if (l.counts32.len != 0) {
                try l.foldProductOrdinals(u32, l.counts32, bindings, ordinals, rows, source.selected);
            } else try l.foldProductOrdinals(u64, l.counts64, bindings, ordinals, rows, source.selected);
        }
        return true;
    }

    fn foldNumeric(
        l: *Lanes,
        ra: std.mem.Allocator,
        gpa: std.mem.Allocator,
        bindings: []const CountAxis,
        ordinals: RowOrdinals,
        batch: *const filter_eval.Batch,
        lookup: []const ?usize,
        sel: *const Sel,
    ) !void {
        try (SelectedBatch{ .batch = batch, .selected = sel }).validate();
        try l.ensure(gpa);
        const comp = try ra.alloc(u32, batch.num_rows);
        @memset(comp, 0);
        var failure: ?anyerror = null;
        activeRange(sel, 0, batch.num_rows, struct {
            fn row(c: anytype, r: usize) void {
                const id = productId(c.bindings, r, true) catch |e| {
                    c.failure.* = e;
                    return;
                };
                const ordinal = c.ordinals.at(r) catch |e| {
                    c.failure.* = e;
                    return;
                };
                c.comp[r] = @intCast(id);
                c.first[id] = @min(c.first[id], ordinal);
            }
        }.row, .{ .bindings = bindings, .ordinals = ordinals, .comp = comp, .first = l.first, .failure = &failure });
        if (failure) |e| return e;
        const gids = try ra.alloc(u32, l.domain.ids());
        for (gids, 0..) |*gid, id| gid.* = @intCast(id);
        if (try foldExactNumeric(ra, gpa, l.accs, gids, comp, l.calls, batch, lookup, sel)) return;
        try agg_coded.foldDenseCalls(ra, gpa, l.accs, gids, comp, l.calls, batch, lookup, sel);
    }

    fn foldSum(
        l: *Lanes,
        gpa: std.mem.Allocator,
        axes: anytype,
        comptime nullable: bool,
        values: anytype,
        base: u64,
        rows: usize,
        sel: *const Sel,
    ) !void {
        if (l.compact_sums.len != 0) {
            const base_ordinal = try l.positions.?.pack(base);
            if (try l.positions.?.pack(base + rows - 1) != base_ordinal + rows - 1) return error.BadColumn;
            return l.foldSumOrdinals(gpa, axes, nullable, values, .{
                .base = base_ordinal,
                .limit = l.rows,
            }, rows, sel, true);
        }
        return l.foldSumOrdinals(gpa, axes, nullable, values, .{ .base = base }, rows, sel, false);
    }

    fn foldSumOrdinals(
        l: *Lanes,
        gpa: std.mem.Allocator,
        axes: anytype,
        comptime nullable: bool,
        values: anytype,
        ordinals: RowOrdinals,
        rows: usize,
        sel: ?*const Sel,
        comptime compact: bool,
    ) !void {
        if (compact) if (magnitude(values, rows)) |max_abs| {
            const bound = if (max_abs == 0) rows else (present - 1) / max_abs;
            const mini: usize = @intCast(@min(rows, @min(partialRows(l.rows), bound)));
            if (mini > 1) {
                try l.ensurePartials(gpa);
                return l.foldSumPartials(axes, nullable, values, ordinals, rows, sel, mini);
            }
        };
        return l.foldSumRows(axes, nullable, values, ordinals, rows, sel, compact);
    }

    fn foldSumPartials(
        l: *Lanes,
        axes: anytype,
        comptime nullable: bool,
        values: anytype,
        ordinals: RowOrdinals,
        rows: usize,
        sel: ?*const Sel,
        mini: usize,
    ) !void {
        var lo: usize = 0;
        while (lo < rows) {
            const hi = lo + @min(rows - lo, mini);
            var n_touched: usize = 0;
            var bad = false;
            activeRange(sel, lo, hi, struct {
                fn row(c: anytype, r: usize) void {
                    const id = productId(c.axes, r, nullable) catch {
                        c.bad.* = true;
                        return;
                    };
                    const value = sumValue(c.values, r) catch {
                        c.bad.* = true;
                        return;
                    };
                    const ordinal = c.ordinals.at(r) catch {
                        c.bad.* = true;
                        return;
                    };
                    const s = &c.states[id];
                    // Original ordinals are <2^31. Their spare bit marks only this worker's pending mini-batch.
                    if (s.count() == 0 or s.first & compact_present == 0) {
                        c.touched[c.n_touched.*] = @intCast(id);
                        c.n_touched.* += 1;
                        c.partials[id] = 0;
                    }
                    s.first = compact_present | @min(s.first & ~compact_present, @as(u32, @intCast(ordinal)));
                    s.count_present += 1;
                    if (value) |v| {
                        c.partials[id] += v;
                        s.count_present |= compact_present;
                    }
                }
            }.row, .{
                .axes = axes,
                .values = values,
                .ordinals = ordinals,
                .states = l.compact_sums,
                .partials = l.partials,
                .touched = l.touched,
                .n_touched = &n_touched,
                .bad = &bad,
            });
            for (l.touched[0..n_touched]) |id| {
                l.compact_sums[id].value += l.partials[id];
                l.compact_sums[id].first &= ~compact_present;
            }
            if (bad) return error.BadColumn;
            lo = hi;
        }
    }

    fn foldSumRows(
        l: *Lanes,
        axes: anytype,
        comptime nullable: bool,
        values: anytype,
        ordinals: RowOrdinals,
        rows: usize,
        sel: ?*const Sel,
        comptime compact: bool,
    ) !void {
        var bad = false;
        activeRange(sel, 0, rows, struct {
            fn row(c: anytype, r: usize) void {
                const id = productId(c.axes, r, nullable) catch {
                    c.bad.* = true;
                    return;
                };
                const value = sumValue(c.values, r) catch {
                    c.bad.* = true;
                    return;
                };
                const ordinal = c.ordinals.at(r) catch {
                    c.bad.* = true;
                    return;
                };
                if (compact) {
                    c.compact[id].add(value, @intCast(ordinal));
                } else {
                    const s = &c.sums[id];
                    s.count_present += 1;
                    s.count_present |= if (value == null) 0 else present;
                    s.value += value orelse 0;
                    c.first[id] = @min(c.first[id], ordinal);
                }
            }
        }.row, .{
            .axes = axes,
            .values = values,
            .sums = l.sums,
            .compact = l.compact_sums,
            .first = l.first,
            .ordinals = ordinals,
            .bad = &bad,
        });
        if (bad) return error.BadColumn;
    }

    pub fn integer(l: *const Lanes, id: usize, ai: usize) ?i128 {
        if (l.accs.len != 0) return switch (l.accs[id * l.calls.len + ai]) {
            .count => |n| n,
            .sum_i, .min_i, .max_i => |n| n,
            else => null,
        };
        if (l.calls[ai].func == .count) return l.count(id);
        if (l.compact_sums.len != 0) return l.compact_sums[id].sum();
        const s = l.sums[id];
        return if (s.count_present & present != 0) s.value else null;
    }

    pub fn accumulator(l: *const Lanes, id: usize, ai: usize) Accumulator {
        if (l.counts32.len != 0 or l.counts64.len != 0) return .{ .count = l.count(id) };
        if (l.sums.len == 0 and l.compact_sums.len == 0) return l.accs[id * l.calls.len + ai];
        return if (l.calls[ai].func == .count) .{ .count = l.count(id) } else .{ .sum_i = l.integer(id, ai) };
    }

    pub fn groupCount(l: *const Lanes) usize {
        var n: usize = 0;
        if (!l.allocated()) return 0;
        for (0..l.domain.ids()) |id| n += @intFromBool(l.seen(id));
        return n;
    }

    fn foldProduct(
        l: *Lanes,
        comptime T: type,
        counts: []T,
        axes: []const CountAxis,
        base: u64,
        rows: usize,
        sel: *const Sel,
    ) !void {
        return l.foldProductOrdinals(T, counts, axes, .{ .base = base }, rows, sel);
    }

    fn foldProductOrdinals(
        l: *Lanes,
        comptime T: type,
        counts: []T,
        axes: []const CountAxis,
        ordinals: RowOrdinals,
        rows: usize,
        sel: ?*const Sel,
    ) !void {
        if (axes.len == 2 and axes[0].ni.def_levels == null and axes[1].ni.def_levels == null)
            return l.foldProductRows(T, counts, axes[0..2].*, false, ordinals, rows, sel);
        return l.foldProductRows(T, counts, axes, true, ordinals, rows, sel);
    }

    fn foldProductRows(
        l: *Lanes,
        comptime T: type,
        counts: []T,
        axes: anytype,
        comptime nullable: bool,
        ordinals: RowOrdinals,
        rows: usize,
        sel: ?*const Sel,
    ) !void {
        var bad = false;
        activeRange(sel, 0, rows, struct {
            fn row(c: anytype, r: usize) void {
                const id = productId(c.axes, r, nullable) catch {
                    c.bad.* = true;
                    return;
                };
                const ordinal = c.ordinals.at(r) catch {
                    c.bad.* = true;
                    return;
                };
                c.counts[id] += 1;
                c.first[id] = @min(c.first[id], ordinal);
            }
        }.row, .{ .axes = axes, .counts = counts, .first = l.first, .ordinals = ordinals, .bad = &bad });
        if (bad) return error.BadColumn;
    }

    /// Fold a row group whose lone key is column `key` of `batch`. False, having touched nothing, when its
    /// dictionary is not the one the domain translated: the caller folds it the usual way.
    pub fn fold(
        l: *Lanes,
        ra: std.mem.Allocator,
        gpa: std.mem.Allocator,
        key: expr_ast.ColRef,
        file: usize,
        rg: usize,
        pos_base: u64,
        batch: *const filter_eval.Batch,
        lookup: []const ?usize,
        sel: *const Sel,
    ) !bool {
        const trans = l.domain.forRowGroup(file, rg) orelse return false;
        const pos = lookup[key.col_idx] orelse return false;
        const col = batch.cols[pos];
        if (col.nullInfo().max_rep > 0 or col.dictLen() + 1 != trans.len) return false;
        if (!l.sameDictionary(col, trans)) return false;
        try l.ensure(gpa);
        const n_calls = l.calls.len;
        if (col.hist()) |hist| {
            // Count-only lone key decoded straight into a histogram with first rows (`agg_coded.histGroupKey`).
            const first = col.histFirst() orelse return error.UnsupportedAggType;
            if (first.len != hist.len or hist.len != col.dictLen()) return error.UnsupportedAggType;
            for (hist, first, 0..) |n, fr, code| {
                if (n == 0) continue;
                if (fr == std.math.maxInt(u32)) return error.UnsupportedAggType;
                const id = trans[code];
                l.first[id] = @min(l.first[id], pos_base + fr);
                if (countOnly(l.calls)) {
                    l.addCount(id, n);
                } else for (l.accs[id * n_calls ..][0..n_calls]) |*acc| acc.count += n;
            }
            return true;
        }
        const codes = col.codes() orelse return false;
        const ni = col.nullInfo();
        if (countOnly(l.calls)) {
            if (codes.len < batch.num_rows) return error.BadColumn;
            if (ni.def_levels) |dl| if (dl.len < batch.num_rows) return error.BadColumn;
            if (l.counts32.len != 0) {
                try l.foldCounts(u32, l.counts32, codes, trans, ni, pos_base, batch.num_rows, sel);
            } else try l.foldCounts(u64, l.counts64, codes, trans, ni, pos_base, batch.num_rows, sel);
            return true;
        }
        const space = trans.len;
        const comp: []const u32 = if (ni.def_levels) |dl| comp: {
            const out = try ra.alloc(u32, batch.num_rows);
            const null_slot: u32 = @intCast(col.dictLen());
            for (out, codes[0..batch.num_rows], dl) |*cp, code, d| cp.* = if (d < ni.max_def) null_slot else code;
            break :comp out;
        } else codes[0..batch.num_rows];
        const reachable = col.dictLen() + @intFromBool(ni.def_levels != null);
        const first_row = try agg_coded.compositeFirstRows(ra, comp, space, reachable, sel);
        for (first_row, trans) |fr, id| if (fr != agg_coded.no_row) {
            l.first[id] = @min(l.first[id], pos_base + fr);
        };
        for (l.calls, 0..) |call, ai| {
            try @call(.never_inline, agg_coded.foldGroupedCall, .{
                ra, gpa, l.accs, trans, comp, space, ai, n_calls, call, batch, lookup, sel, null,
            });
        }
        return true;
    }

    fn foldCounts(
        l: *Lanes,
        comptime T: type,
        counts: []T,
        codes: []const u32,
        trans: []const u32,
        ni: @TypeOf(@as(Column, undefined).nullInfo()),
        pos_base: u64,
        rows: usize,
        sel: *const Sel,
    ) !void {
        var bad_code = false;
        agg_coded.forEachActive(sel, rows, struct {
            fn row(c: anytype, r: usize) void {
                const is_null = if (c.ni.def_levels) |dl| dl[r] < c.ni.max_def else false;
                const code = if (is_null) c.trans.len - 1 else c.codes[r];
                if (!is_null and code >= c.trans.len - 1) {
                    c.bad.* = true;
                    return;
                }
                const id = c.trans[code];
                c.counts[id] += 1;
                c.first[id] = @min(c.first[id], c.base + r);
            }
        }.row, .{ .counts = counts, .first = l.first, .codes = codes, .trans = trans, .ni = ni, .base = pos_base, .bad = &bad_code });
        if (bad_code) return error.BadColumn;
    }

    fn sameDictionary(l: *const Lanes, col: Column, trans: []const u32) bool {
        return l.domain.axes.len == 1 and l.domain.axes[0].sameDictionary(col, trans);
    }
};

/// Merge every worker's lanes by id into `table`, one group per id some row reached, at the least position any worker
/// saw it. `table` must be empty and use the scan's key image (`fixed_stride`), so its groups are the ones the merged
/// worker tables would hold, and every later stage reads it as a single table. Groups go in by id, not by position:
/// every reader of a table orders its groups by `first` (as one worker's table, filled in code order, needs).
pub fn intoTable(
    gpa: std.mem.Allocator,
    arena: std.mem.Allocator,
    lanes: []Lanes,
    keys: []const expr_ast.Expr,
    table: *agg.GroupTable,
    lookup_len: usize,
    n_threads: usize,
    cut: ?usize,
) !void {
    std.debug.assert(table.groupCount() == 0);
    const into = (try merge(gpa, arena, lanes, n_threads)) orelse return;
    const domain = into.domain;
    const calls = into.calls;
    const selected = try reached(gpa, into, cut);
    defer gpa.free(selected);
    const groups = selected.len;
    if (table.fixed_stride != 0) try table.reserveFixed(groups, calls.len);

    // The domain's values as the key column the scan decodes, so each key's image is the one the row path packs.
    const kd = try arena.alloc(Column, keys.len);
    for (domain.axes, keys, kd) |*axis, key, *out|
        out.* = try agg_coded.keyDictColumn(arena, try axis.dictionary(arena), key.col_ref, lookup_len);
    const indices = try arena.alloc(usize, keys.len);
    var key_scratch: std.ArrayList(u8) = .empty;
    var image: [32]u8 = undefined;
    for (selected) |id| {
        const f = into.position(id);
        var rem: usize = id;
        for (domain.axes, indices) |*axis, *row| {
            row.* = rem % axis.ids();
            rem /= axis.ids();
        }
        try agg_coded.serializeKeyAt(&key_scratch, arena, kd, indices);
        const key = if (table.fixed_stride != 0)
            agg.fixedImage(table.fixed_stride, key_scratch.items, &image) orelse return error.BadGroupKey
        else
            key_scratch.items;
        const g = try table.getOrInsert(key, calls, f);
        var scratch: [16]agg.Accumulator = undefined;
        const dst = table.readGroup(g, calls.len, &scratch);
        if (countOnly(calls)) dst[0] = .{ .count = into.count(id) } else {
            for (dst, 0..) |*acc, ai| acc.* = into.accumulator(id, ai);
        }
        table.writeGroup(g, dst);
    }
}

/// Completed lanes stay owned by their worker; callers can consume them without serializing or hashing keys.
pub fn merge(gpa: std.mem.Allocator, arena: std.mem.Allocator, lanes: []Lanes, n_threads: usize) !?*Lanes {
    var live: std.ArrayList(*Lanes) = .empty;
    for (lanes) |*l| if (l.allocated()) try live.append(arena, l);
    if (live.items.len == 0) return null;
    const domain = live.items[0].domain;
    const n = domain.ids();

    var ctx: MergeCtx = .{ .lanes = live.items, .n = n, .gpa = gpa };
    const tasks = try arena.alloc(MergeTask, @max(1, @min(n_threads, n / 16384)));
    for (tasks, 0..) |*t, i| t.* = .{ .ctx = &ctx, .lo = i * n / tasks.len, .hi = (i + 1) * n / tasks.len };
    if (tasks.len != 1) _ = spawn.runTree(MergeTask, tasks, .{ .stack_size = 1 << 20 }, MergeTask.run);
    // Partial pool startup can succeed while leaving exclusive ranges unrun.
    for (tasks) |*t| if (!t.ran) t.run();
    for (tasks) |t| if (t.err) |e| return e;
    return live.items[0];
}

/// Complete ranks only: SUM losers are never omitted before all selected rows have folded.
pub fn reached(gpa: std.mem.Allocator, into: *const Lanes, cut: ?usize) ![]u32 {
    var heap: RankHeap = .initContext(into);
    defer heap.deinit(gpa);
    var groups = into.groupCount();
    if (cut) |h| {
        if (!countOnly(into.calls) and into.sums.len == 0 and into.compact_sums.len == 0)
            return error.UnsupportedAggType;
        if (h == 0) return gpa.alloc(u32, 0);
        try heap.ensureTotalCapacityPrecise(gpa, @min(h, groups));
        for (0..into.domain.ids()) |id| if (into.seen(id)) {
            const candidate: u32 = @intCast(id);
            if (heap.count() == h) {
                if (rankOrder(into, candidate, heap.peek().?) != .gt) continue;
                _ = heap.pop();
            }
            try heap.push(gpa, candidate);
        };
        groups = heap.count();
    }
    const ids = try gpa.alloc(u32, groups);
    if (cut != null) @memcpy(ids, heap.items) else {
        var i: usize = 0;
        for (0..into.domain.ids()) |id| if (into.seen(id)) {
            ids[i] = @intCast(id);
            i += 1;
        };
    }
    return ids;
}

const RankHeap = std.PriorityQueue(u32, *const Lanes, rankOrder);

fn rankOrder(lanes: *const Lanes, a: u32, b: u32) std.math.Order {
    const by_value = if (lanes.sums.len != 0 or lanes.compact_sums.len != 0) blk: {
        const ai = sumCall(lanes.calls).?;
        const x = lanes.integer(a, ai);
        const y = lanes.integer(b, ai);
        if (x == null or y == null) break :blk std.math.order(@intFromBool(x != null), @intFromBool(y != null));
        break :blk std.math.order(x.?, y.?);
    } else std.math.order(lanes.count(a), lanes.count(b));
    if (by_value != .eq) return by_value;
    return std.math.order(lanes.firstOrdinal(b), lanes.firstOrdinal(a));
}

const MergeCtx = struct { lanes: []const *Lanes, n: usize, gpa: std.mem.Allocator };

/// Folds ids `lo..hi` of every worker's lanes into the first worker's.
const MergeTask = struct {
    ctx: *MergeCtx,
    lo: usize,
    hi: usize,
    ran: bool = false,
    err: ?anyerror = null,

    fn run(t: *MergeTask) void {
        t.ran = true;
        t.runErr() catch |e| {
            t.err = e;
        };
    }

    fn runErr(t: *MergeTask) !void {
        const into = t.ctx.lanes[0];
        const n_calls = into.calls.len;
        // The ordinary ordered block merge starts from fresh states, including its first floating partial.
        if (into.accs.len != 0 and orderSensitive(into.calls)) {
            for (t.lo..t.hi) |id| {
                if (!into.seen(id)) continue;
                for (into.calls, into.accs[id * n_calls ..][0..n_calls]) |call, *acc| {
                    var initial: Accumulator = .init(call);
                    try initial.merge(acc.*, t.ctx.gpa);
                    acc.* = initial;
                }
            }
        }
        for (t.ctx.lanes[1..]) |src| for (t.lo..t.hi) |id| {
            if (!src.seen(id)) continue;
            if (into.compact_sums.len != 0) {
                into.compact_sums[id].merge(src.compact_sums[id]);
                continue;
            }
            into.first[id] = @min(into.first[id], src.first[id]);
            if (into.sums.len != 0) {
                const s = src.sums[id];
                into.sums[id].value += s.value;
                const count_sum = into.count(id) + src.count(id);
                into.sums[id].count_present = count_sum | ((into.sums[id].count_present | s.count_present) & present);
                continue;
            }
            if (countOnly(into.calls)) {
                into.addCount(id, src.count(id));
                continue;
            }
            const dst = into.accs[id * n_calls ..][0..n_calls];
            for (dst, src.accs[id * n_calls ..][0..n_calls]) |*d, s| try d.merge(s, t.ctx.gpa);
        };
    }
};

const testing = std.testing;

test "dense compact SUM is 24 bytes and preserves NULL, wide cancellation and selected ordinal ties" {
    try testing.expectEqual(@as(usize, 24), @sizeOf(CompactSum));
    var a: CompactSum = .{};
    var b: CompactSum = .{};
    a.add(null, 10);
    try testing.expectEqual(@as(?i128, null), a.sum());
    a.add(std.math.maxInt(i64), 12);
    a.add(std.math.maxInt(i64), 11);
    b.add(-std.math.maxInt(i64), 2);
    a.merge(b);
    try testing.expectEqual(@as(?i128, std.math.maxInt(i64)), a.sum());
    try testing.expectEqual(@as(u32, 4), a.count());
    try testing.expectEqual(@as(u32, 2), a.first);
    var all_null: CompactSum = .{};
    all_null.add(null, 0);
    var zero: CompactSum = .{};
    zero.add(0, 0);
    try testing.expectEqual(@as(?i128, null), all_null.sum());
    try testing.expectEqual(@as(?i128, 0), zero.sum());
}

test "dense ordinal proof maps original positions across empty items and refuses truncation" {
    const o = Ordinals.init(&.{ 0, 0, 3, 3, 7 }).?;
    for (0..7) |r| {
        const item: usize = if (r < 3) 1 else 3;
        const row = if (r < 3) r else r - 3;
        const position = agg.firstPosition(item, row);
        try testing.expectEqual(@as(u32, @intCast(r)), try o.pack(position));
        try testing.expectEqual(position, try o.unpack(@intCast(r)));
    }
    try testing.expectError(error.BadColumn, o.pack(agg.firstPosition(0, 0)));
    try testing.expectError(error.BadColumn, o.pack(agg.firstPosition(3, 4)));
    try testing.expectError(error.BadColumn, o.unpack(7));
    try testing.expectEqual(null, Ordinals.init(&.{ 0, compact_present }));
    try testing.expectEqual(null, Ordinals.init(&.{ 0, 2, 1 }));
    try testing.expectEqual(null, Ordinals.init(&.{1}));
}

test "dense SUM quote matches compact and widened storage and refuses a missing ordinal proof before allocation" {
    const a = testing.allocator;
    const fixture = Fixture.domain();
    var axes = [_]Axis{ fixture.axes[0], fixture.axes[0] };
    const domain: Domain = .{ .axes = &axes, .bytes = 0 };
    const calls = Fixture.calls[0..2];
    const compact_bytes = domain.ids() * 24;
    try testing.expectEqual(compact_bytes, laneBytes(domain.ids(), calls, 258, true));
    try testing.expectEqual(domain.ids() * 32, laneBytes(domain.ids(), calls, compact_present, true));
    var lanes: Lanes = .{ .domain = &domain, .calls = calls, .rows = 258 };
    defer lanes.deinit(a);
    var no_alloc = testing.FailingAllocator.init(a, .{ .fail_index = 0 });
    try testing.expectError(error.UnsupportedAggType, lanes.ensure(no_alloc.allocator()));
    try testing.expectEqual(@as(usize, 0), no_alloc.alloc_index);
    lanes.positions = Ordinals.init(&.{ 0, 129, 258 });
    try lanes.ensure(a);
    try testing.expectEqual(compact_bytes, lanes.compact_sums.len * @sizeOf(CompactSum));
    try testing.expectEqual(@as(usize, 0), lanes.first.len);
    const quote = charge(&domain, 3, calls, 258, 0, 2);
    const scratch = domain.ids() * @sizeOf(u32) + (axes[0].trans.len + 1) * @sizeOf(u32);
    try testing.expectEqual(
        key_pack.planBytes(2, true, true) +
            3 * (compact_bytes + partialBytes(domain.ids(), 258)) + quote.table + 2 * @sizeOf(u32) + scratch,
        quote.total,
    );
    var ledger: agg.SharedBudget = .{ .limit = quote.total - 1 };
    try testing.expectEqual(null, ledger.drawExact(quote.total));
    ledger.limit = quote.total;
    const paid = ledger.drawExact(quote.total).?;
    ledger.release(paid);
    var wide: Lanes = .{ .domain = &domain, .calls = calls, .rows = compact_present };
    defer wide.deinit(a);
    try wide.ensure(a);
    try testing.expectEqual(domain.ids() * 32, wide.sums.len * @sizeOf(Sum) + wide.first.len * @sizeOf(u64));
}

test "dense selected SUM mini-batches equal exact i128 folding across cache sizes, gather gaps and NULLs" {
    const a = testing.allocator;
    const old_cpu = hw.test_cpu;
    defer hw.test_cpu = old_cpu;
    const fixture = Fixture.domain();
    var axes = [_]Axis{ fixture.axes[0], fixture.axes[0] };
    const domain: Domain = .{ .axes = &axes, .bytes = 0 };
    const keys = [_]expr_ast.Expr{ .{ .col_ref = Fixture.key }, .{ .col_ref = Fixture.key } };
    const rows = [_]u32{ 1, 4, 6, 7, 10, 11, 16, 19 };
    for ([_]usize{ 12, 36, 1 << 20 }) |cache_bytes| {
        hw.test_cpu = .{ .l2 = .{ .bytes = cache_bytes } };
        var got: Lanes = .{ .domain = &domain, .calls = Fixture.calls[0..2], .rows = 1000, .global_ordinals = true };
        defer got.deinit(a);
        var want: Lanes = .{ .domain = &domain, .calls = Fixture.calls[0..2], .rows = 1000, .global_ordinals = true };
        defer want.deinit(a);
        var arena: std.heap.ArenaAllocator = .init(a);
        defer arena.deinit();
        for ([_]usize{ 1, 0 }) |rg| {
            const batch = Fixture.batch(rg);
            var sel = try Fixture.selection(a, rg);
            defer sel.deinit();
            const source: SelectedBatch = .{
                .batch = &batch,
                .selected = if (rg == 1) &sel else null,
                .input_base = if (rg == 1) 700 else 100,
                .input_rows = &rows,
            };
            try testing.expect(try got.foldSelected(arena.allocator(), a, &keys, 0, rg, source, &.{ 0, 1 }));
            var bindings: [2]CountAxis = undefined;
            try testing.expect(try domain.bind(&keys, 0, rg, &batch, &.{ 0, 1 }, &bindings));
            try want.ensure(a);
            try want.foldSumRows(&bindings, true, batch.cols[1].i64, .{
                .base = source.input_base,
                .input_rows = &rows,
                .limit = 1000,
            }, 8, source.selected, true);
        }
        try testing.expectEqualDeep(want.compact_sums, got.compact_sums);
        try testing.expectEqual(cache_bytes > 12, got.partials.len != 0);
        const owned = got.partials.len * @sizeOf(i64) + got.touched.len * @sizeOf(u32);
        if (owned != 0) try testing.expectEqual(partialBytes(domain.ids(), 1000), owned);
        for (0..domain.ids()) |id| if (got.seen(id)) {
            try testing.expectEqual(want.firstOrdinal(id), got.position(id));
            try testing.expect(got.firstOrdinal(id) < 1000);
        };
    }
}

test "dense mini-batch magnitude proofs include negative extremes and reject unsafe single-value bounds" {
    try testing.expectEqual(@as(u64, 1) << 63, maxAbs(&.{ 0, -7, std.math.minInt(i64), std.math.maxInt(i64) }));
    try testing.expectEqual(@as(u64, 0), maxAbs(&.{}));
    const col = Column{ .i64 = .{ .values = &.{ -9, undefined, 7 }, .def_levels = &.{ 1, 0, 1 }, .max_def = 1 } };
    try testing.expectEqual(@as(?u64, 9), magnitude(col.i64, 3));
    try testing.expectEqual(@as(?u64, @as(u64, 1) << 31), magnitude((Column{ .i32 = .{ .values = &.{} } }).i32, 8));
}

test "dense selected SUM refuses magnitude 2^63 at the fold and admits zero without changing exact sums" {
    const a = testing.allocator;
    var arena: std.heap.ArenaAllocator = .init(a);
    defer arena.deinit();
    const fixture = Fixture.domain();
    var axes = [_]Axis{ fixture.axes[0], fixture.axes[0] };
    const domain: Domain = .{ .axes = &axes, .bytes = 0 };
    const keys = [_]expr_ast.Expr{ .{ .col_ref = Fixture.key }, .{ .col_ref = Fixture.key } };
    const lo = std.math.minInt(i64);
    for ([_][2]i64{ .{ lo, lo }, .{ lo, 0 }, .{ 0, 0 }, .{ std.math.maxInt(i64), 0 } }, 0..) |values, i| {
        var lanes: Lanes = .{
            .domain = &domain,
            .calls = Fixture.calls[0..2],
            .rows = 2,
            .global_ordinals = true,
        };
        defer lanes.deinit(a);
        const batch: filter_eval.Batch = .{ .num_rows = 2, .cols = &.{
            .{ .i64 = .{ .values = &.{}, .codes = &.{ 0, 0 }, .dict = &Fixture.dict0 } },
            .{ .i64 = .{ .values = &values } },
        } };
        try testing.expect(try lanes.foldSelected(arena.allocator(), a, &keys, 0, 0, .{ .batch = &batch }, &.{ 0, 1 }));
        const id = Fixture.t0[0] * (axes[0].ids() + 1);
        const sum = @as(i128, values[0]) + values[1];
        try testing.expectEqual(@as(?i128, sum), lanes.integer(id, 1));
        if (i == 0) try testing.expectEqual(-(@as(i128, 1) << 64), sum);
        try testing.expectEqual(i == 2, lanes.partials.len != 0);
    }
}

test "dense numeric calls match canonical coded blocks for DECIMAL and float expressions with allocation failures" {
    try testing.checkAllAllocationFailures(testing.allocator, testNumericLanes, .{false});
    try testNumericLanes(testing.allocator, true);
}

fn testNumericLanes(a: std.mem.Allocator, floating: bool) !void {
    var arena: std.heap.ArenaAllocator = .init(a);
    defer arena.deinit();
    const ra = arena.allocator();
    const old_cpu = hw.test_cpu;
    defer hw.test_cpu = old_cpu;
    hw.test_cpu = .{ .l2 = .{ .bytes = 32 << 10 } };
    const old_batch = expr_eval.forced_batch_rows;
    defer expr_eval.forced_batch_rows = old_batch;
    expr_eval.forced_batch_rows = 64;
    const fixture = Fixture.domain();
    var axes = [_]Axis{ fixture.axes[0], fixture.axes[0] };
    const domain: Domain = .{ .axes = &axes, .bytes = 0 };
    const keys = [_]expr_ast.Expr{ .{ .col_ref = Fixture.key }, .{ .col_ref = Fixture.key } };
    var x: expr_ast.Expr = .{ .col_ref = .{
        .col_idx = 1,
        .physical_type = if (floating) .DOUBLE else .INT64,
        .expr_type = if (floating) .f64 else .dec,
        .dec = if (floating) null else .{ .precision = 15, .scale = 2 },
    } };
    var two: expr_ast.Expr = .{ .literal = .{ .i64 = 2 } };
    const twice: expr_ast.Expr = .{ .binop = .{
        .op = .mul,
        .left = &x,
        .right = &two,
        .result_type = if (floating) .f64 else .dec,
        .dec = if (floating) null else .{ .precision = 35, .scale = 2 },
        .depth = 2,
    } };
    const calls = [_]AggCall{
        .{ .func = .sum, .arg = x, .result = if (floating) .f64 else .dec, .where = null, .alias = "s" },
        .{ .func = .avg, .arg = x, .result = if (floating) .avg_f64 else .avg_dec, .where = null, .alias = "a" },
        .{ .func = .sum, .arg = twice, .result = if (floating) .f64 else .dec, .where = null, .alias = "d" },
        .{ .func = .avg, .arg = twice, .result = if (floating) .avg_f64 else .avg_dec, .where = null, .alias = "b" },
        Fixture.calls[0],
    };
    try testing.expect(shapeOk(&calls, &keys));
    try testing.expect(numericFits(domain.ids(), &calls));
    const n = 257;
    var codes: [n]u32 = undefined;
    var defs: [n]u32 = undefined;
    var key_defs: [n]u32 = undefined;
    var exact: [n]i64 = undefined;
    var floats: [n]f64 = undefined;
    for (&codes, &defs, &key_defs, &exact, &floats, 0..) |*code, *def, *kd, *v, *f, r| {
        code.* = @intCast(r % 3);
        def.* = @intFromBool(r % 19 != 0);
        kd.* = @intFromBool(r % 23 != 0);
        v.* = if (def.* == 0) undefined else @as(i64, @intCast(r)) - 100;
        f.* = if (def.* == 0) undefined else switch (r % 4) {
            0 => 1e16,
            1 => 1,
            2 => -1e16,
            else => -0.0,
        };
    }
    const batch: filter_eval.Batch = .{ .num_rows = n, .cols = &.{
        .{ .i64 = .{
            .values = &.{},
            .codes = &codes,
            .dict = &Fixture.dict0,
            .def_levels = &key_defs,
            .max_def = 1,
            .has_nulls = true,
        } },
        if (floating) .{ .f64 = .{
            .values = &floats,
            .def_levels = &defs,
            .max_def = 1,
            .has_nulls = true,
        } } else .{ .i64 = .{
            .values = &exact,
            .unscaled = .{ .i64 = &exact },
            .def_levels = &defs,
            .max_def = 1,
            .has_nulls = true,
        } },
    } };
    var sel = try Sel.init(ra, n);
    sel.clearRange(64, 128);
    sel.set(256, false);
    var lanes: [3]Lanes = @splat(.{ .domain = &domain, .calls = &calls, .rows = n * 6 });
    defer for (&lanes) |*lane| lane.deinit(a);
    var want = agg.GroupTable.init(a, 1 << 24);
    defer want.deinit();
    for (&lanes, 0..) |*lane, block| {
        var ordinary = agg.GroupTable.init(a, 1 << 24);
        defer ordinary.deinit();
        for (0..2) |window| {
            const base = agg.firstPosition(block * 2 + window, 0);
            try testing.expect(try agg_coded.groupedCoded(
                ra,
                a,
                &calls,
                &keys,
                &ordinary,
                base,
                &batch,
                &.{ 0, 1 },
                &sel,
                null,
                null,
            ));
            try testing.expect(try lane.foldKeys(ra, a, &keys, 0, 0, base, &batch, &.{ 0, 1 }, &sel));
        }
        for (0..ordinary.groupCount()) |g| {
            const dst = try want.getOrInsert(ordinary.keyAt(g), &calls, ordinary.first.items[g]);
            for (0..calls.len) |ai| try want.accumulators.items[dst * calls.len + ai].merge(
                ordinary.readAcc(g, ai, calls.len),
                a,
            );
        }
    }
    var got = agg.GroupTable.init(a, 1 << 24);
    defer got.deinit();
    try intoTable(a, ra, &lanes, &keys, &got, 2, 4, null);
    try testing.expectEqual(want.groupCount(), got.groupCount());
    for (0..want.groupCount()) |g| {
        const id = got.findKey(want.keyAt(g)) orelse return error.TestExpectedEqual;
        try testing.expectEqual(want.first.items[g], got.first.items[id]);
        for (0..calls.len) |ai| try expectNumericAcc(want.readAcc(g, ai, calls.len), got.readAcc(id, ai, calls.len));
    }
    var plain = batch;
    var plain_cols = [_]Column{ .{ .i64 = .{ .values = &exact } }, batch.cols[1] };
    plain.cols = &plain_cols;
    if (floating) {
        try testing.expectError(error.ExceededMemoryBudget, lanes[0].foldKeys(
            ra,
            a,
            &keys,
            0,
            0,
            0,
            &plain,
            &.{ 0, 1 },
            &sel,
        ));
    } else try testing.expect(!try lanes[0].foldKeys(ra, a, &keys, 0, 0, 0, &plain, &.{ 0, 1 }, &sel));
}

fn expectNumericAcc(want: Accumulator, got: Accumulator) !void {
    try testing.expectEqual(std.meta.activeTag(want), std.meta.activeTag(got));
    switch (want) {
        .sum_f => |v| {
            try testing.expectEqual(v != null, got.sum_f != null);
            if (v) |x| try testing.expectEqual(@as(u64, @bitCast(x)), @as(u64, @bitCast(got.sum_f.?)));
        },
        .avg => |v| {
            try testing.expectEqual(v.count, got.avg.count);
            try testing.expectEqual(v.isum, got.avg.isum);
            try testing.expectEqual(@as(u64, @bitCast(v.sum)), @as(u64, @bitCast(got.avg.sum)));
        },
        else => try testing.expectEqualDeep(want, got),
    }
}

test "dense numeric selected DECIMAL keeps 192-bit cancellation and gathered original ordinals" {
    const a = testing.allocator;
    var arena: std.heap.ArenaAllocator = .init(a);
    defer arena.deinit();
    const fixture = Fixture.domain();
    var axes = [_]Axis{ fixture.axes[0], fixture.axes[0] };
    const domain: Domain = .{ .axes = &axes, .bytes = 0 };
    const keys = [_]expr_ast.Expr{ .{ .col_ref = Fixture.key }, .{ .col_ref = Fixture.key } };
    const arg: expr_ast.Expr = .{ .col_ref = .{
        .col_idx = 1,
        .physical_type = .FIXED_LEN_BYTE_ARRAY,
        .expr_type = .dec,
        .dec = .{ .precision = 38, .scale = 0 },
    } };
    const calls = [_]AggCall{
        .{ .func = .sum, .arg = arg, .result = .dec, .where = null, .alias = "s" },
        .{ .func = .avg, .arg = arg, .result = .avg_dec, .where = null, .alias = "a" },
        Fixture.calls[0],
    };
    const h: i128 = @as(i128, 1) << 125;
    const values = [_]i128{ h, h, h, h, h, -h, -h, -h, undefined };
    const codes: [values.len]u32 = @splat(0);
    const batch: filter_eval.Batch = .{ .num_rows = values.len, .cols = &.{
        .{ .i64 = .{ .values = &.{}, .codes = &codes, .dict = &Fixture.dict0 } },
        .{ .i128 = .{
            .values = &values,
            .def_levels = &.{ 1, 1, 1, 1, 1, 1, 1, 1, 0 },
            .max_def = 1,
            .has_nulls = true,
        } },
    } };
    var lane: Lanes = .{ .domain = &domain, .calls = &calls, .rows = 2000, .global_ordinals = true };
    defer lane.deinit(a);
    try testing.expect(try lane.foldSelected(arena.allocator(), a, &keys, 0, 0, .{
        .batch = &batch,
        .input_base = 1000,
        .input_rows = &.{ 99, 4, 40, 22, 200, 5, 3, 9, 0 },
    }, &.{ 0, 1 }));
    const id = Fixture.t0[0] * (axes[0].ids() + 1);
    try testing.expectEqual(@as(i192, 2 * h), lane.accumulator(id, 0).sum_d.get());
    try testing.expectEqual(@as(u64, 8), lane.accumulator(id, 0).sum_d.n);
    try testing.expectEqualDeep(lane.accumulator(id, 0).sum_d, lane.accumulator(id, 1).avg_d);
    try testing.expectEqual(@as(?i128, 9), lane.integer(id, 2));
    try testing.expectEqual(@as(u64, 1000), lane.firstOrdinal(id));
}

test "dense numeric cache refusal allocates no lanes and quotes all per-id scratch" {
    const old_cpu = hw.test_cpu;
    defer hw.test_cpu = old_cpu;
    const fixture = Fixture.domain();
    var axes = [_]Axis{ fixture.axes[0], fixture.axes[0] };
    const domain: Domain = .{ .axes = &axes, .bytes = 0 };
    const calls = Fixture.calls[0..3];
    const bytes = laneBytes(domain.ids(), calls, 8, false);
    hw.test_cpu = .{ .l2 = .{ .bytes = bytes - 1 } };
    var lane: Lanes = .{ .domain = &domain, .calls = calls, .rows = 8 };
    var no_alloc = testing.FailingAllocator.init(testing.allocator, .{ .fail_index = 0 });
    try testing.expectError(error.UnsupportedAggType, lane.ensure(no_alloc.allocator()));
    try testing.expect(!no_alloc.has_induced_failure);
    hw.test_cpu = .{ .l2 = .{ .bytes = bytes } };
    try testing.expect(numericFits(domain.ids(), calls));
    const quote = charge(&domain, 3, calls, 8, 0, null);
    try testing.expectEqual(
        key_pack.planBytes(2, true, true) + 3 * (bytes + numericScratchBytes(domain.ids(), calls)) +
            quote.table + domain.ids() * @sizeOf(u32),
        quote.total,
    );
}

test "dense ordered merge includes fresh identity and retains block order for signed zero and NaN payloads" {
    const a = testing.allocator;
    var arena: std.heap.ArenaAllocator = .init(a);
    defer arena.deinit();
    const fixture = Fixture.domain();
    var axes = [_]Axis{ fixture.axes[0], fixture.axes[0] };
    const domain: Domain = .{ .axes = &axes, .bytes = 0 };
    const arg: expr_ast.Expr = .{ .col_ref = .{ .col_idx = 1, .physical_type = .DOUBLE, .expr_type = .f64 } };
    const calls = [_]AggCall{
        .{ .func = .sum, .arg = arg, .result = .f64, .where = null, .alias = "s" },
        .{ .func = .avg, .arg = arg, .result = .avg_f64, .where = null, .alias = "a" },
    };
    for ([_]usize{ 1, 4, 16 }) |threads| {
        var lanes: [4]Lanes = @splat(.{ .domain = &domain, .calls = &calls, .rows = 16 });
        defer for (&lanes) |*lane| lane.deinit(a);
        for (&lanes, 0..) |*lane, block| {
            try lane.ensure(a);
            for (0..3) |id| {
                const v: f64 = switch (id) {
                    0 => -0.0,
                    1 => switch (block) {
                        0 => 1e16,
                        1 => 1,
                        2 => -1e16,
                        else => 2,
                    },
                    else => @bitCast(@as(u64, 0x7ff8000000000011) + block),
                };
                lane.first[id] = agg.firstPosition(block, id);
                lane.accs[id * 2] = .{ .sum_f = v };
                lane.accs[id * 2 + 1] = .{ .avg = .{ .sum = v, .count = 1 } };
            }
        }
        var expected: [3][2]Accumulator = undefined;
        for (&expected, 0..) |*group, id| {
            for (calls, group) |call, *acc| acc.* = .init(call);
            for (&lanes) |*lane| for (group, 0..) |*acc, ai| try acc.merge(lane.accumulator(id, ai), a);
        }
        const merged = (try merge(a, arena.allocator(), &lanes, threads)).?;
        for (expected, 0..) |group, id| for (group, 0..) |want, ai| {
            try expectNumericAcc(want, merged.accumulator(id, ai));
        };
    }
}

test "dense selected adapters validate ordinal metadata before allocation and free failed partial scratch" {
    const a = testing.allocator;
    const old_cpu = hw.test_cpu;
    defer hw.test_cpu = old_cpu;
    hw.test_cpu = .{ .l2 = .{ .bytes = 96 } };
    const fixture = Fixture.domain();
    var axes = [_]Axis{ fixture.axes[0], fixture.axes[0] };
    const domain: Domain = .{ .axes = &axes, .bytes = 0 };
    const keys = [_]expr_ast.Expr{ .{ .col_ref = Fixture.key }, .{ .col_ref = Fixture.key } };
    const batch = Fixture.batch(0);
    var arena: std.heap.ArenaAllocator = .init(a);
    defer arena.deinit();
    var no_alloc = testing.FailingAllocator.init(a, .{ .fail_index = 0 });
    var invalid: Lanes = .{ .domain = &domain, .calls = Fixture.calls[0..2], .rows = 1000, .global_ordinals = true };
    defer invalid.deinit(a);
    try testing.expectError(error.BadColumn, invalid.foldSelected(
        arena.allocator(),
        no_alloc.allocator(),
        &keys,
        0,
        0,
        .{ .batch = &batch, .input_base = 999 },
        &.{ 0, 1 },
    ));
    try testing.expectEqual(@as(usize, 0), no_alloc.alloc_index);
    for (0..3) |failure| {
        var failing = testing.FailingAllocator.init(a, .{ .fail_index = failure });
        var lanes: Lanes = .{ .domain = &domain, .calls = Fixture.calls[0..2], .rows = 1000, .global_ordinals = true };
        defer lanes.deinit(failing.allocator());
        try testing.expectError(error.OutOfMemory, lanes.foldSelected(
            arena.allocator(),
            failing.allocator(),
            &keys,
            0,
            0,
            .{ .batch = &batch, .input_base = 100 },
            &.{ 0, 1 },
        ));
        try testing.expectEqual(@as(usize, 0), lanes.partials.len);
    }
}

/// Two row groups over the same values with differently ordered dictionaries (the second repeats a value under two
/// codes and holds nulls), folded densely and through `groupedCoded`, must make the same groups with the same state.
const Fixture = struct {
    const v_arg: expr_ast.Expr = .{ .col_ref = .{ .col_idx = 1, .physical_type = .INT64, .expr_type = .i64 } };
    const key: expr_ast.ColRef = .{ .col_idx = 0, .physical_type = .INT64, .expr_type = .i64 };
    const calls = [_]AggCall{
        .{ .func = .count, .arg = null, .result = .i64, .where = null, .alias = "c" },
        .{ .func = .sum, .arg = v_arg, .result = .i64, .where = null, .alias = "s" },
        .{ .func = .min, .arg = v_arg, .result = .i64, .where = null, .alias = "lo" },
        .{ .func = .max, .arg = v_arg, .result = .i64, .where = null, .alias = "hi" },
        .{ .func = .avg, .arg = v_arg, .result = .avg_f64, .where = null, .alias = "av" },
        .{ .func = .count, .arg = v_arg, .result = .i64, .where = null, .alias = "cv" },
    };
    // Domain: 10, 20, 30, 40 (40 in no dictionary's selected rows), then null.
    var values = [_]u64{ 30, 10, 40, 20 };
    var t0 = [_]u32{ 1, 3, 0, 4 }; // dict 10, 20, 30
    var t1 = [_]u32{ 0, 3, 0, 2, 4 }; // dict 30, 20, 30, 40
    var trans = [_][]u32{ &t0, &t1 };
    var file_base = [_]usize{0};
    const dict0 = [_]i64{ 10, 20, 30 };
    const codes0 = [_]u32{ 2, 0, 0, 1, 2, 0, 1, 1 };
    const vals0 = [_]i64{ 5, -3, 8, 100, 7, 1, -50, 2 };
    const dict1 = [_]i64{ 30, 20, 30, 40 };
    const codes1 = [_]u32{ 1, 0, 0, 2, 3, 2, 0, 1 };
    const kdef1 = [_]u32{ 1, 0, 1, 1, 1, 1, 0, 1 };
    const vals1 = [_]i64{ 4, 9, 0, 6, -1, 11, 13, 2 };
    const vdef1 = [_]u32{ 1, 1, 0, 1, 1, 1, 1, 1 };

    var axes = [_]Axis{.{ .values = .{ .int64 = &values }, .trans = &trans, .file_base = &file_base, .bytes = 0 }};

    fn domain() Domain {
        return .{ .axes = &axes, .bytes = 0 };
    }

    fn batch(rg: usize) filter_eval.Batch {
        const cols = if (rg == 0) &[_]Column{
            .{ .i64 = .{ .values = &.{}, .codes = &codes0, .dict = &dict0 } },
            .{ .i64 = .{ .values = &vals0 } },
        } else &[_]Column{
            .{ .i64 = .{ .values = &.{}, .codes = &codes1, .dict = &dict1, .def_levels = &kdef1, .max_def = 1, .has_nulls = true } },
            .{ .i64 = .{ .values = &vals1, .def_levels = &vdef1, .max_def = 1, .has_nulls = true } },
        };
        return .{ .cols = cols, .num_rows = 8 };
    }

    /// Rows 3 and 4 of the second row group filtered out: the last row of 40 is row 4, so 40 never becomes a group.
    fn selection(a: std.mem.Allocator, rg: usize) !Sel {
        var sel = try Sel.init(a, 8);
        if (rg == 1) {
            sel.set(3, false);
            sel.set(4, false);
        }
        return sel;
    }
};

fn expectSameGroups(want: *const agg.GroupTable, got: *const agg.GroupTable, n_calls: usize) !void {
    try testing.expectEqual(want.groupCount(), got.groupCount());
    for (0..want.groupCount()) |g| {
        const h = got.findKey(want.keyAt(g)) orelse return error.TestExpectedEqual;
        try testing.expectEqual(want.first.items[g], got.first.items[h]);
        for (0..n_calls) |i| try testing.expect(std.meta.eql(
            want.readAcc(g, i, n_calls),
            got.readAcc(h, i, n_calls),
        ));
    }
}

test "dense count lanes share the row-width boundary with their quote" {
    const calls = [_]AggCall{Fixture.calls[0]};
    const last = std.math.maxInt(u32);
    try testing.expectEqual(@as(usize, 4), countBytes(last));
    try testing.expectEqual(@as(usize, 8), countBytes(@as(u64, last) + 1));
    try testing.expectEqual(@as(usize, 60), laneBytes(5, &calls, last, false));
    try testing.expectEqual(@as(usize, 80), laneBytes(5, &calls, @as(u64, last) + 1, false));
    const d = Fixture.domain();
    for ([_]u64{ last, @as(u64, last) + 1 }) |rows| {
        var l: Lanes = .{ .domain = &d, .calls = &calls, .rows = rows };
        defer l.deinit(testing.allocator);
        try l.ensure(testing.allocator);
        l.addCount(0, rows);
        try testing.expectEqual(rows, l.count(0));
        try testing.expectEqualDeep(Accumulator{ .count = rows }, l.accumulator(0, 0));
        const owned_lanes = l.first.len * 8 + l.counts32.len * 4 + l.counts64.len * 8;
        try testing.expectEqual(laneBytes(d.ids(), &calls, rows, false), owned_lanes);
        try testing.expectEqual(@as(usize, 0), l.accs.len);
        for ([_]usize{ 0, 9 }) |stride| {
            const quoted = charge(&d, 3, &calls, rows, stride, 2);
            const owned = d.bytes + 3 * (l.first.len * 8 + l.counts32.len * 4 + l.counts64.len * 8);
            try testing.expectEqual(owned + quoted.table + (2 + d.ids()) * @sizeOf(u32), quoted.total);
            var ledger: agg.SharedBudget = .{ .limit = quoted.total - 1 };
            try testing.expectEqual(null, ledger.drawExact(quoted.total));
            ledger.limit = quoted.total;
            const held = ledger.drawExact(quoted.total).?;
            ledger.release(held);
            try testing.expectEqual(@as(usize, 0), ledger.used.load(.monotonic));
        }
    }
}

test "dense merge recovers every exclusive range after partial spawn startup for SUM and both COUNT widths" {
    const a = testing.allocator;
    var arena: std.heap.ArenaAllocator = .init(a);
    defer arena.deinit();
    const values = try arena.allocator().alloc(u64, 255);
    for (values, 0..) |*value, i| value.* = i;
    var axes: [2]Axis = @splat(.{
        .values = .{ .int64 = values },
        .trans = &.{},
        .file_base = &.{},
        .bytes = 0,
    });
    const domain: Domain = .{ .axes = &axes, .bytes = 0 };
    try testing.expectEqual(@as(usize, 4), domain.ids() / 16384);
    const Layout = enum { count32, count64, sum };
    for ([_]Layout{ .count32, .count64, .sum }) |layout| {
        for (0..4) |allowed| {
            const rows: u64 = if (layout == .count64) @as(u64, 1) << 40 else 64;
            const calls = Fixture.calls[0..if (layout == .sum) 2 else 1];
            var lanes: [2]Lanes = @splat(.{
                .domain = &domain,
                .calls = calls,
                .rows = rows,
                .positions = Ordinals.init(&.{ 0, 64 }),
            });
            defer for (&lanes) |*lane| lane.deinit(a);
            for (&lanes) |*lane| try lane.ensure(a);
            const weight: u64 = if (layout == .count64) @as(u64, 1) << 32 else 1;
            for (0..4) |range| {
                const id = range * 16384 + 123;
                if (layout == .sum) {
                    lanes[0].compact_sums[id].add(null, @intCast(40 + range));
                    lanes[1].compact_sums[id].add(-17 * @as(i64, @intCast(range + 1)), @intCast(10 + range));
                } else {
                    lanes[0].first[id] = 40 + range;
                    lanes[1].first[id] = 10 + range;
                    lanes[0].addCount(id, weight);
                    lanes[1].addCount(id, 2 * weight);
                }
            }
            spawn.injectFailureAfter(allowed);
            defer spawn.resetFailure();
            const merged = (try merge(a, arena.allocator(), &lanes, 4)).?;
            try testing.expect(spawn.failuresInjected() > 0);
            try testing.expectEqual(@as(usize, 4), merged.groupCount());
            for (0..4) |range| {
                const id = range * 16384 + 123;
                try testing.expectEqual(@as(u64, 10 + range), merged.firstOrdinal(id));
                const count: u64 = if (layout == .sum) 2 else 3 * weight;
                try testing.expectEqualDeep(Accumulator{ .count = count }, merged.accumulator(id, 0));
                if (layout == .sum) try testing.expectEqualDeep(
                    Accumulator{ .sum_i = -17 * @as(i128, @intCast(range + 1)) },
                    merged.accumulator(id, 1),
                );
            }
        }
    }
}

test "dense count lanes preserve selected first rows, NULLs and allocation failures" {
    try testing.checkAllAllocationFailures(testing.allocator, testCountLanes, .{});
}

test "dense count lanes consume full, empty and tail words and propagate a late bad code" {
    const a = testing.allocator;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const ra = arena.allocator();
    const calls = [_]AggCall{Fixture.calls[0]};
    const d = Fixture.domain();
    var l: Lanes = .{ .domain = &d, .calls = &calls, .rows = 129 };
    defer l.deinit(a);
    var codes: [129]u32 = undefined;
    for (&codes, 0..) |*c, r| c.* = @intCast(r % 3);
    const cols = [_]Column{.{ .i64 = .{ .values = &.{}, .codes = &codes, .dict = &Fixture.dict0 } }};
    const batch: filter_eval.Batch = .{ .cols = &cols, .num_rows = codes.len };
    var sel = try Sel.init(ra, codes.len);
    sel.clearRange(64, 128);
    try testing.expect(try l.fold(ra, a, Fixture.key, 0, 0, 0, &batch, &.{0}, &sel));
    try testing.expectEqual(@as(u64, 22), l.count(0));
    try testing.expectEqual(@as(u64, 22), l.count(1));
    try testing.expectEqual(@as(u64, 21), l.count(3));
    try testing.expectEqual(@as(u64, 2), l.first[0]);
    try testing.expectEqual(unseen, l.first[2]);
    var bad: Lanes = .{ .domain = &d, .calls = &calls, .rows = 129 };
    defer bad.deinit(a);
    codes[128] = 99;
    try testing.expectError(error.BadColumn, bad.fold(ra, a, Fixture.key, 0, 0, 0, &batch, &.{0}, &sel));
}

fn testCountLanes(a: std.mem.Allocator) !void {
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const ra = arena.allocator();
    const calls = [_]AggCall{Fixture.calls[0]};
    const lookup = [_]?usize{ 0, 1 };
    const d = Fixture.domain();
    for ([_]u64{ 16, std.math.maxInt(u64) }) |rows| {
        var want = agg.GroupTable.init(a, 1 << 20);
        defer want.deinit();
        want.fixed_stride = 9;
        var got = agg.GroupTable.init(a, 1 << 20);
        defer got.deinit();
        got.fixed_stride = 9;
        var lanes = [_]Lanes{
            .{ .domain = &d, .calls = &calls, .rows = rows },
            .{ .domain = &d, .calls = &calls, .rows = rows },
        };
        defer for (&lanes) |*l| l.deinit(a);
        for ([_]usize{ 1, 0 }, 0..) |rg, wi| {
            const b = Fixture.batch(rg);
            var sel = try Fixture.selection(ra, rg);
            const pos = agg.firstPosition(rg, 0);
            try testing.expect(try agg_coded.groupedCoded(ra, a, &calls, &.{.{ .col_ref = Fixture.key }}, &want, pos, &b, &lookup, &sel, null, null));
            try testing.expect(try lanes[wi].fold(ra, a, Fixture.key, 0, rg, pos, &b, &lookup, &sel));
        }
        try intoTable(a, ra, &lanes, &.{.{ .col_ref = Fixture.key }}, &got, lookup.len, 2, null);
        try expectSameGroups(&want, &got, calls.len);
        try testing.expectEqual(@as(usize, 4), got.groupCount());
    }
}

test "dense fold makes the groups and states grouping by code makes, by value across dictionaries" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const calls = &Fixture.calls;
    const lookup = [_]?usize{ 0, 1 };
    var want = agg.GroupTable.init(testing.allocator, 1 << 20);
    defer want.deinit();
    want.fixed_stride = 9;
    var got = agg.GroupTable.init(testing.allocator, 1 << 20);
    defer got.deinit();
    got.fixed_stride = 9;
    const d = Fixture.domain();
    // Each row group on a worker of its own, the second first, so the merge has to take the least position.
    var lanes = [_]Lanes{ .{ .domain = &d, .calls = calls }, .{ .domain = &d, .calls = calls } };
    defer for (&lanes) |*l| l.deinit(testing.allocator);
    for ([_]usize{ 1, 0 }, 0..) |rg, wi| {
        const b = Fixture.batch(rg);
        var sel = try Fixture.selection(a, rg);
        const pos = agg.firstPosition(rg, 0);
        try testing.expect(try agg_coded.groupedCoded(a, testing.allocator, calls, &.{.{ .col_ref = Fixture.key }}, &want, pos, &b, &lookup, &sel, null, null));
        try testing.expect(try lanes[wi].fold(a, testing.allocator, Fixture.key, 0, rg, pos, &b, &lookup, &sel));
    }
    try intoTable(testing.allocator, a, &lanes, &.{.{ .col_ref = Fixture.key }}, &got, lookup.len, 2, null);
    try expectSameGroups(&want, &got, calls.len);
    // Admission reserves the table beside the lanes (`tableBytes`): it must cover what the table then holds.
    try testing.expect(got.allocated_bytes <= tableBytes(d.ids(), 9, calls.len));
    // 10, 20, 30 and null: 40 sits only in a filtered row.
    try testing.expectEqual(@as(usize, 4), got.groupCount());
}

test "dense fold declines a dictionary the domain did not translate, touching nothing" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const lookup = [_]?usize{ 0, 1 };
    const d = Fixture.domain();
    var l: Lanes = .{ .domain = &d, .calls = &Fixture.calls };
    defer l.deinit(testing.allocator);
    var sel = try Sel.init(a, 8);
    // The first row group's codes over a dictionary whose second entry differs.
    const other = [_]i64{ 10, 21, 30 };
    const cols = [_]Column{
        .{ .i64 = .{ .values = &.{}, .codes = &Fixture.codes0, .dict = &other } },
        .{ .i64 = .{ .values = &Fixture.vals0 } },
    };
    const b: filter_eval.Batch = .{ .cols = &cols, .num_rows = 8 };
    try testing.expect(!try l.fold(a, testing.allocator, Fixture.key, 0, 0, 0, &b, &lookup, &sel));
    // No translation for a third row group, nor for a second file.
    const b0 = Fixture.batch(0);
    try testing.expect(!try l.fold(a, testing.allocator, Fixture.key, 0, 2, 0, &b0, &lookup, &sel));
    try testing.expect(!try l.fold(a, testing.allocator, Fixture.key, 1, 0, 0, &b0, &lookup, &sel));
    try testing.expectEqual(@as(usize, 0), l.first.len);
}

test "dense fold and merge release everything on allocation failure" {
    try testing.checkAllAllocationFailures(testing.allocator, struct {
        fn run(gpa: std.mem.Allocator) !void {
            var arena = std.heap.ArenaAllocator.init(gpa);
            defer arena.deinit();
            const a = arena.allocator();
            const lookup = [_]?usize{ 0, 1 };
            const d = Fixture.domain();
            var lanes = [_]Lanes{ .{ .domain = &d, .calls = &Fixture.calls }, .{ .domain = &d, .calls = &Fixture.calls } };
            defer for (&lanes) |*l| l.deinit(gpa);
            for ([_]usize{ 0, 1 }, &lanes) |rg, *l| {
                const b = Fixture.batch(rg);
                var sel = try Fixture.selection(a, rg);
                try testing.expect(try l.fold(a, gpa, Fixture.key, 0, rg, agg.firstPosition(rg, 0), &b, &lookup, &sel));
            }
            var got = agg.GroupTable.init(gpa, 1 << 20);
            defer got.deinit();
            got.fixed_stride = 9;
            try intoTable(gpa, a, &lanes, &.{.{ .col_ref = Fixture.key }}, &got, lookup.len, 1, null);
            try testing.expectEqual(@as(usize, 4), got.groupCount());
        }
    }.run, .{});
}

test "dense shape: lone integer calls and composite signed SUM with optional COUNT" {
    const k: expr_ast.Expr = .{ .col_ref = Fixture.key };
    const f: expr_ast.Expr = .{ .col_ref = .{ .col_idx = 2, .physical_type = .DOUBLE, .expr_type = .f64 } };
    try testing.expect(shapeOk(&Fixture.calls, &.{k}));
    try testing.expect(shapeOk(Fixture.calls[0..2], &.{ k, k }));
    try testing.expect(!shapeOk(&Fixture.calls, &.{f}));
    const fsum = [_]AggCall{.{ .func = .sum, .arg = f, .result = .f64, .where = null, .alias = "s" }};
    try testing.expect(!shapeOk(&fsum, &.{k}));
    const distinct = [_]AggCall{.{ .func = .count_distinct, .arg = Fixture.v_arg, .result = .i64, .where = null, .alias = "d" }};
    try testing.expect(!shapeOk(&distinct, &.{k}));
}

test "dense products preserve composite identity, NULLs, selected first ties and exact top-H with failures" {
    try testing.checkAllAllocationFailures(testing.allocator, testCountProduct, .{});
}

fn testCountProduct(a: std.mem.Allocator) !void {
    try testCountProductMode(a, true);
}

test "dense present pairs preserve selected first ties and top-H across dictionary permutations" {
    try testCountProductMode(testing.allocator, false);
}

test "dense present pairs validate selected codes at tail words for both count widths" {
    const a = testing.allocator;
    var codes: [129]u32 = undefined;
    for (&codes, 0..) |*code, r| code.* = @intCast(r % 2);
    codes[128] = std.math.maxInt(u32);
    const trans = [_]u32{ 0, 1, 2 };
    const left: CountAxis = .{
        .codes = &codes,
        .trans = &trans,
        .ni = .{ .def_levels = null, .max_def = 0, .max_rep = 0 },
        .stride = 1,
    };
    var right = left;
    right.stride = 3;
    const axes = [_]CountAxis{ left, right };
    var sel = try Sel.init(a, codes.len);
    defer sel.deinit();
    sel.set(128, false);
    inline for (.{ u32, u64 }) |T| {
        var counts: [9]T = @splat(0);
        var first: [9]u64 = @splat(unseen);
        var lanes: Lanes = .{ .domain = undefined, .calls = &.{}, .first = &first };
        try lanes.foldProduct(T, &counts, &axes, 17, codes.len, &sel);
        try testing.expectEqual(@as(T, 64), counts[0]);
        try testing.expectEqual(@as(T, 64), counts[4]);
        try testing.expectEqual(@as(u64, 17), first[0]);
        try testing.expectEqual(@as(u64, 18), first[4]);
        sel.set(128, true);
        try testing.expectError(error.BadColumn, lanes.foldProduct(T, &counts, &axes, 17, codes.len, &sel));
        sel.set(128, false);
    }
}

fn testCountProductMode(a: std.mem.Allocator, nullable: bool) !void {
    return testProductMode(a, nullable, false);
}

test "dense composite SUM keeps exact wide signed states, NULLs, selected first and fallback under allocation failure" {
    // Arena growth must allocate every time, so preceding tests cannot change the failure indices through remaps.
    const NoResize = struct {
        fn alloc(ctx: *anyopaque, len: usize, alignment: std.mem.Alignment, ret_addr: usize) ?[*]u8 {
            const child: *std.mem.Allocator = @ptrCast(@alignCast(ctx));
            return child.rawAlloc(len, alignment, ret_addr);
        }
        fn free(ctx: *anyopaque, memory: []u8, alignment: std.mem.Alignment, ret_addr: usize) void {
            const child: *std.mem.Allocator = @ptrCast(@alignCast(ctx));
            child.rawFree(memory, alignment, ret_addr);
        }
    };
    var child = testing.allocator;
    const fixed: std.mem.Allocator = .{ .ptr = &child, .vtable = &.{
        .alloc = NoResize.alloc,
        .resize = std.mem.Allocator.noResize,
        .remap = std.mem.Allocator.noRemap,
        .free = NoResize.free,
    } };
    try testing.checkAllAllocationFailures(fixed, testSumProduct, .{});
}

fn testSumProduct(a: std.mem.Allocator) !void {
    try testProductMode(a, true, true);
}

test "dense present composite SUM preserves wide signed states and exact top-H" {
    try testProductMode(testing.allocator, false, true);
}

fn testProductMode(a: std.mem.Allocator, nullable: bool, with_sum: bool) !void {
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const ra = arena.allocator();
    const sum: AggCall = .{ .func = .sum, .arg = .{ .col_ref = .{
        .col_idx = 2,
        .physical_type = .INT64,
        .expr_type = .i64,
    } }, .result = .i64, .where = null, .alias = "s" };
    const all_calls = [_]AggCall{ Fixture.calls[0], sum };
    const calls = all_calls[0..if (with_sum) 2 else 1];
    const keys = [_]expr_ast.Expr{
        .{ .col_ref = .{ .col_idx = 0, .physical_type = .INT32, .expr_type = .i64 } },
        .{ .col_ref = .{ .col_idx = 1, .physical_type = .BYTE_ARRAY, .expr_type = .str } },
    };
    var ints = [_]u64{ @bitCast(@as(i64, -7)), 11, 99 };
    var empty: [0]u8 = .{};
    var embedded = [_]u8{ 'a', 0, 'b' };
    var z = [_]u8{'z'};
    var unused = [_]u8{ 'u', 'n', 'u', 's', 'e', 'd' };
    var strings = [_][]u8{ &empty, &embedded, &z, &unused };
    var a0 = [_]u32{ 1, 0, 0, 2, 3 };
    var a1 = [_]u32{ 2, 1, 0, 3 };
    var b0 = [_]u32{ 2, 0, 1, 1, 3, 4 };
    var b1 = [_]u32{ 3, 1, 0, 2, 4 };
    var at = [_][]u32{ &a0, &a1 };
    var bt = [_][]u32{ &b0, &b1 };
    var file_base = [_]usize{0};
    var axes = [_]Axis{
        .{ .values = .{ .int32 = &ints }, .trans = &at, .file_base = &file_base, .bytes = 0 },
        .{ .values = .{ .bytes = &strings }, .trans = &bt, .file_base = &file_base, .bytes = 0 },
    };
    const domain: Domain = .{ .axes = &axes, .bytes = 0 };
    try testing.expect(domain.globalFits(2));
    const ad0 = [_]i32{ 11, -7, -7, 99 };
    const ad1 = [_]i32{ 99, 11, -7 };
    const bd0 = [_][]const u8{ "z", "", "a\x00b", "a\x00b", "unused" };
    const bd1 = [_][]const u8{ "unused", "a\x00b", "", "z" };
    var ac: [2][129]u32 = undefined;
    var bc: [2][129]u32 = undefined;
    var adef: [129]u32 = undefined;
    var bdef: [129]u32 = undefined;
    for (0..129) |r| {
        ac[0][r] = @intCast(r % 3);
        ac[1][r] = @intCast(1 + r % 2);
        bc[0][r] = @intCast(r % 4);
        bc[1][r] = @intCast(1 + r % 3);
        adef[r] = @intFromBool(r % 11 != 0);
        bdef[r] = @intFromBool(r % 17 != 0);
    }
    ac[1][128] = 0;
    bc[1][128] = 0; // Both unused dictionary values occur only in a filtered row.
    var values: [129]i64 = undefined;
    var value_defs: [129]u32 = undefined;
    for (&values, &value_defs, 0..) |*v, *d, r| {
        v.* = if (r % 3 == 0) std.math.maxInt(i64) else if (r % 3 == 1) -std.math.maxInt(i64) else -17;
        d.* = @intFromBool(r % 5 != 0);
    }
    const value_col: Column = .{ .i64 = .{
        .values = &values,
        .def_levels = if (nullable) &value_defs else null,
        .max_def = 1,
        .has_nulls = nullable,
    } };
    var cols = [_][3]Column{
        .{
            .{ .i32 = .{
                .values = &.{},
                .codes = &ac[0],
                .dict = &ad0,
                .def_levels = &adef,
                .max_def = 1,
                .has_nulls = true,
            } },
            .{ .string = .{
                .values = &.{},
                .codes = &bc[0],
                .dict = &bd0,
                .def_levels = &bdef,
                .max_def = 1,
                .has_nulls = true,
            } },
            value_col,
        },
        .{
            .{ .i32 = .{
                .values = &.{},
                .codes = &ac[1],
                .dict = &ad1,
                .def_levels = &adef,
                .max_def = 1,
                .has_nulls = true,
            } },
            .{ .string = .{
                .values = &.{},
                .codes = &bc[1],
                .dict = &bd1,
                .def_levels = &bdef,
                .max_def = 1,
                .has_nulls = true,
            } },
            value_col,
        },
    };
    if (!nullable) for (&cols) |*rg| for (rg) |*col| switch (col.*) {
        inline else => |*c| {
            c.def_levels = null;
            c.has_nulls = false;
        },
    };
    for ([_]?usize{ null, 3 }) |cut| {
        var want = agg.GroupTable.init(a, 1 << 20);
        defer want.deinit();
        var got = agg.GroupTable.init(a, 1 << 20);
        defer got.deinit();
        if (!with_sum) got.compact.configure(calls);
        var lanes = [_]Lanes{
            .{ .domain = &domain, .calls = calls, .rows = 258, .positions = Ordinals.init(&.{ 0, 129, 258 }) },
            .{ .domain = &domain, .calls = calls, .rows = 258, .positions = Ordinals.init(&.{ 0, 129, 258 }) },
        };
        defer for (&lanes) |*l| l.deinit(a);
        for ([_]usize{ 1, 0 }, 0..) |rg, wi| {
            const batch: filter_eval.Batch = .{ .cols = &cols[rg], .num_rows = 129 };
            var sel = try Sel.init(ra, 129);
            if (rg == 0) sel.clearRange(64, 128) else {
                for (0..129) |r| if (r % 3 != 0) sel.set(r, false);
            }
            sel.set(128, false);
            const pos = agg.firstPosition(rg, 0);
            try testing.expect(try agg_coded.groupedCoded(
                ra,
                a,
                calls,
                &keys,
                &want,
                pos,
                &batch,
                &.{ 0, 1, 2 },
                &sel,
                null,
                null,
            ));
            try testing.expect(try lanes[wi].foldKeys(ra, a, &keys, 0, rg, pos, &batch, &.{ 0, 1, 2 }, &sel));
        }
        try intoTable(a, ra, &lanes, &keys, &got, 2, 2, cut);
        try testing.expect(got.allocated_bytes <= domainTableBytes(&domain, domain.ids(), 0, calls.len));
        if (cut) |h| {
            const ids = try ra.alloc(u32, want.groupCount());
            for (ids, 0..) |*id, i| id.* = @intCast(i);
            const Rank = struct { table: *const agg.GroupTable, sum: bool };
            std.mem.sortUnstable(u32, ids, Rank{ .table = &want, .sum = with_sum }, struct {
                fn better(ctx: Rank, x: u32, y: u32) bool {
                    const t = ctx.table;
                    const ca: ?i128 = if (ctx.sum) t.readAcc(x, 1, 2).sum_i else t.readAcc(x, 0, 1).count;
                    const cb: ?i128 = if (ctx.sum) t.readAcc(y, 1, 2).sum_i else t.readAcc(y, 0, 1).count;
                    if (ca == null or cb == null) {
                        if ((ca == null) != (cb == null)) return ca != null;
                    } else if (ca.? != cb.?) return ca.? > cb.?;
                    return t.first.items[x] < t.first.items[y];
                }
            }.better);
            try testing.expectEqual(@min(h, ids.len), got.groupCount());
            for (ids[0..@min(h, ids.len)]) |id| {
                const g = got.findKey(want.keyAt(id)) orelse return error.TestExpectedEqual;
                for (0..calls.len) |ai|
                    try testing.expect(std.meta.eql(want.readAcc(id, ai, calls.len), got.readAcc(g, ai, calls.len)));
                try testing.expectEqual(want.first.items[id], got.first.items[g]);
            }
        } else try expectSameGroups(&want, &got, calls.len);
    }
    var declined: Lanes = .{
        .domain = &domain,
        .calls = calls,
        .rows = 258,
        .positions = Ordinals.init(&.{ 0, 129, 258 }),
    };
    defer declined.deinit(a);
    var mismatch = cols[1];
    const changed = [_][]const u8{ "unused", "a\x00b", "", "late" };
    mismatch[1].string.dict = &changed;
    const batch: filter_eval.Batch = .{ .cols = &mismatch, .num_rows = 129 };
    var all = try Sel.init(ra, 129);
    try testing.expect(!try declined.foldKeys(ra, a, &keys, 0, 1, 0, &batch, &.{ 0, 1, 2 }, &all));
    try testing.expectEqual(@as(usize, 0), declined.first.len);
}

test "packed identities check product overflow and distinguish NULL from every code" {
    var values = [_]u64{ 0, 1 };
    const axis: Axis = .{ .values = .{ .int64 = &values }, .trans = &.{}, .file_base = &.{}, .bytes = 0 };
    var axes: [41]Axis = @splat(axis);
    var domain: Domain = .{ .axes = &axes, .bytes = 0 };
    try std.testing.expect(domain.packedIds() == null);
    var refused: Lanes = .{ .domain = &domain, .calls = Fixture.calls[0..2], .rows = 12 };
    var no_alloc = testing.FailingAllocator.init(testing.allocator, .{ .fail_index = 0 });
    try testing.expectError(error.UnsupportedAggType, refused.ensure(no_alloc.allocator()));
    try testing.expectEqual(@as(usize, 0), no_alloc.alloc_index);
    try testing.expectEqual(@as(usize, 0), refused.first.len);
    domain.axes = axes[0..2];
    try std.testing.expectEqual(@as(?usize, 9), domain.packedIds());
    const trans = [_]u32{ 1, 0, 2 };
    const codes = [_]u32{ 0, 1, std.math.maxInt(u32) };
    const defs = [_]u32{ 1, 1, 0 };
    const bindings = [_]CountAxis{.{
        .codes = &codes,
        .trans = &trans,
        .ni = .{ .def_levels = &defs, .max_def = 1, .max_rep = 0 },
        .stride = 1,
    }};
    try std.testing.expectEqual(@as(usize, 1), try productId(bindings, 0, true));
    try std.testing.expectEqual(@as(usize, 0), try productId(bindings, 1, true));
    try std.testing.expectEqual(@as(usize, 2), try productId(bindings, 2, true));
    try std.testing.expectError(error.BadColumn, productId(bindings, 2, false));
}

test "packed unsigned32 axes bind exact widened and physical dictionary codes" {
    // The retained union stores sign-extended physical i32 bits, not the zero-extended logical value.
    var values = [_]u64{ @bitCast(@as(i64, std.math.minInt(i32))), @bitCast(@as(i64, -1)), 0 };
    var translation = [_]u32{ 2, 0, 1, 3 };
    var translations = [_][]u32{&translation};
    var file_base = [_]usize{0};
    var axes = [_]Axis{.{
        .values = .{ .int32 = &values },
        .trans = &translations,
        .file_base = &file_base,
        .bytes = 0,
    }};
    var domain: Domain = .{ .axes = &axes, .bytes = 0 };
    var key: expr_ast.Expr = .{ .col_ref = .{
        .col_idx = 0,
        .physical_type = .INT32,
        .expr_type = .i64,
        .unsigned_32 = true,
    } };
    const codes = [_]u32{ 0, 1, 2, 0 };
    const defs = [_]u32{ 1, 1, 1, 0 };
    var wide = [_]i64{ 0, 1 << 31, std.math.maxInt(u32) };
    const physical = [_]i32{ 0, std.math.minInt(i32), -1 };
    var bindings: [1]CountAxis = undefined;
    var batch: filter_eval.Batch = .{ .num_rows = codes.len, .cols = &.{.{ .i64 = .{
        .values = &.{},
        .dict = &wide,
        .codes = &codes,
        .def_levels = &defs,
        .max_def = 1,
    } }} };
    try std.testing.expect(try domain.bind((&key)[0..1], 0, 0, &batch, &.{0}, &bindings));
    for ([_]usize{ 2, 0, 1, 3 }, 0..) |id, r| try std.testing.expectEqual(id, try productId(bindings, r, true));
    wide[1] += 1;
    try std.testing.expect(!try domain.bind((&key)[0..1], 0, 0, &batch, &.{0}, &bindings));
    wide[1] = @as(i64, 1) << 32;
    try std.testing.expect(!try domain.bind((&key)[0..1], 0, 0, &batch, &.{0}, &bindings));
    wide[1] = @as(i64, 1) << 31;
    key.col_ref.unsigned_32 = false;
    try std.testing.expect(!try domain.bind((&key)[0..1], 0, 0, &batch, &.{0}, &bindings));
    key.col_ref.unsigned_32 = true;
    batch.cols = &.{.{ .i32 = .{
        .values = &.{},
        .dict = &physical,
        .codes = &codes,
        .def_levels = &defs,
        .max_def = 1,
    } }};
    try std.testing.expect(try domain.bind((&key)[0..1], 0, 0, &batch, &.{0}, &bindings));
}
