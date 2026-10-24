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

/// Whether a lone bare integer key and these calls fold densely: count, and integer sum/min/max/avg of a bare column.
/// Floats keep their ordered paths, and expressions, DECIMAL, strings and count(DISTINCT) the general ones.
pub fn shapeOk(calls: []const AggCall, keys: []const expr_ast.Expr) bool {
    if (countOnly(calls)) {
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

pub fn countOnly(calls: []const AggCall) bool {
    if (calls.len != 1) return false;
    const c = calls[0];
    return c.func == .count and c.arg == null and c.where == null and !c.state and c.offset == 0;
}

pub fn countBytes(rows: u64) usize {
    return if (rows <= std.math.maxInt(u32)) @sizeOf(u32) else @sizeOf(u64);
}

/// The same row bound chooses storage and its prepaid quote. Selection can only lower a lane's count.
pub fn laneBytes(ids: usize, calls: []const AggCall, rows: u64) usize {
    const state = if (countOnly(calls)) countBytes(rows) else @sizeOf(Accumulator) *| calls.len;
    return ids *| (@sizeOf(u64) +| state);
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

/// One worker's least positions and states: counters for COUNT(*), otherwise `calls.len` accumulators per id.
/// Allocated on its first row group, so the worker first touches the pages it fills.
pub const Lanes = struct {
    domain: *const Domain,
    calls: []const AggCall,
    accs: []Accumulator = &.{},
    first: []u64 = &.{},
    rows: u64 = std.math.maxInt(u64),
    counts32: []u32 = &.{},
    counts64: []u64 = &.{},

    fn ensure(l: *Lanes, gpa: std.mem.Allocator) !void {
        if (l.first.len != 0) return;
        const n = l.domain.ids();
        const first = try gpa.alloc(u64, n);
        errdefer gpa.free(first);
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
        l.accs = &.{};
        l.first = &.{};
        l.counts32 = &.{};
        l.counts64 = &.{};
    }

    fn count(l: *const Lanes, id: usize) u64 {
        return if (l.counts32.len != 0) l.counts32[id] else l.counts64[id];
    }

    fn addCount(l: *Lanes, id: usize, n: u64) void {
        if (l.counts32.len != 0) l.counts32[id] += @intCast(n) else l.counts64[id] += n;
    }

    const CountAxis = struct {
        codes: []const u32,
        trans: []const u32,
        ni: @TypeOf(@as(Column, undefined).nullInfo()),
        stride: usize,
    };

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
        if (!countOnly(l.calls) or l.domain.ids() > max_ids) return false;
        const bindings = try ra.alloc(CountAxis, keys.len);
        var stride: usize = 1;
        for (keys, l.domain.axes, bindings) |key, *axis, *b| {
            const trans = axis.forRowGroup(file, rg) orelse return false;
            const pos = lookup[key.col_ref.col_idx] orelse return false;
            const col = batch.cols[pos];
            if (col.nullInfo().max_rep != 0 or !axis.sameDictionary(col, trans)) return false;
            const codes = col.codes() orelse return false;
            const ni = col.nullInfo();
            if (codes.len < batch.num_rows) return error.BadColumn;
            if (ni.def_levels) |dl| if (dl.len < batch.num_rows) return error.BadColumn;
            b.* = .{ .codes = codes, .trans = trans, .ni = ni, .stride = stride };
            stride *= axis.ids();
        }
        try l.ensure(gpa);
        if (l.counts32.len != 0) {
            try l.foldProduct(u32, l.counts32, bindings, pos_base, batch.num_rows, sel);
        } else try l.foldProduct(u64, l.counts64, bindings, pos_base, batch.num_rows, sel);
        return true;
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
        var bad = false;
        agg_coded.forEachActive(sel, rows, struct {
            fn row(c: anytype, r: usize) void {
                var id: usize = 0;
                for (c.axes) |axis| {
                    const is_null = if (axis.ni.def_levels) |dl| dl[r] < axis.ni.max_def else false;
                    const code = if (is_null) axis.trans.len - 1 else axis.codes[r];
                    if (!is_null and code >= axis.trans.len - 1) {
                        c.bad.* = true;
                        return;
                    }
                    id += axis.trans[code] * axis.stride;
                }
                c.counts[id] += 1;
                c.first[id] = @min(c.first[id], c.base + r);
            }
        }.row, .{ .axes = axes, .counts = counts, .first = l.first, .base = base, .bad = &bad });
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
    var live: std.ArrayList(*Lanes) = .empty;
    for (lanes) |*l| if (l.first.len != 0) try live.append(arena, l);
    if (live.items.len == 0) return;
    const domain = live.items[0].domain;
    const calls = live.items[0].calls;
    const n = domain.ids();

    var ctx: MergeCtx = .{ .lanes = live.items, .n = n, .gpa = gpa };
    const tasks = try arena.alloc(MergeTask, @max(1, @min(n_threads, n / 16384)));
    for (tasks, 0..) |*t, i| t.* = .{ .ctx = &ctx, .lo = i * n / tasks.len, .hi = (i + 1) * n / tasks.len };
    if (tasks.len == 1 or !spawn.runTree(MergeTask, tasks, .{ .stack_size = 1 << 20 }, MergeTask.run)) {
        for (tasks) |*t| if (!t.ran) t.run();
    }
    for (tasks) |t| if (t.err) |e| return e;

    const into = live.items[0];
    var heap: CountHeap = .initContext(into);
    defer heap.deinit(gpa);
    var groups: usize = 0;
    for (into.first) |f| groups += @intFromBool(f != unseen);
    if (cut) |h| {
        if (!countOnly(calls)) return error.UnsupportedAggType;
        if (h == 0) return;
        try heap.ensureTotalCapacityPrecise(gpa, @min(h, groups));
        for (into.first, 0..) |f, id| if (f != unseen) {
            const candidate: u32 = @intCast(id);
            if (heap.count() == h) {
                if (countOrder(into, candidate, heap.peek().?) != .gt) continue;
                _ = heap.pop();
            }
            try heap.push(gpa, candidate);
        };
        groups = heap.count();
    }
    if (table.fixed_stride != 0) try table.reserveFixed(groups, calls.len);

    // The domain's values as the key column the scan decodes, so each key's image is the one the row path packs.
    const kd = try arena.alloc(Column, keys.len);
    for (domain.axes, keys, kd) |*axis, key, *out|
        out.* = try agg_coded.keyDictColumn(arena, try axis.dictionary(arena), key.col_ref, lookup_len);
    const indices = try arena.alloc(usize, keys.len);
    var key_scratch: std.ArrayList(u8) = .empty;
    var image: [32]u8 = undefined;
    var i: usize = 0;
    while (i < n) : (i += 1) {
        const id: usize = if (cut != null) (if (i < heap.items.len) heap.items[i] else break) else i;
        const f = into.first[id];
        if (f == unseen) continue;
        var rem = id;
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
        if (countOnly(calls)) dst[0] = .{ .count = into.count(id) } else @memcpy(dst, into.accs[id * calls.len ..][0..calls.len]);
        table.writeGroup(g, dst);
    }
}

const CountHeap = std.PriorityQueue(u32, *const Lanes, countOrder);

fn countOrder(lanes: *const Lanes, a: u32, b: u32) std.math.Order {
    const by_count = std.math.order(lanes.count(a), lanes.count(b));
    if (by_count != .eq) return by_count;
    return std.math.order(lanes.first[b], lanes.first[a]);
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
        for (t.ctx.lanes[1..]) |src| for (t.lo..t.hi) |id| {
            if (src.first[id] == unseen) continue;
            into.first[id] = @min(into.first[id], src.first[id]);
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
    try testing.expectEqual(@as(usize, 60), laneBytes(5, &calls, last));
    try testing.expectEqual(@as(usize, 80), laneBytes(5, &calls, @as(u64, last) + 1));
    const d = Fixture.domain();
    for ([_]u64{ last, @as(u64, last) + 1 }) |rows| {
        var l: Lanes = .{ .domain = &d, .calls = &calls, .rows = rows };
        defer l.deinit(testing.allocator);
        try l.ensure(testing.allocator);
        l.addCount(0, rows);
        try testing.expectEqual(rows, l.count(0));
        try testing.expectEqual(laneBytes(d.ids(), &calls, rows), l.first.len * 8 + l.counts32.len * 4 + l.counts64.len * 8);
        try testing.expectEqual(@as(usize, 0), l.accs.len);
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

test "dense shape: integer count/sum/min/max/avg of bare columns over a lone integer key only" {
    const k: expr_ast.Expr = .{ .col_ref = Fixture.key };
    const f: expr_ast.Expr = .{ .col_ref = .{ .col_idx = 2, .physical_type = .DOUBLE, .expr_type = .f64 } };
    try testing.expect(shapeOk(&Fixture.calls, &.{k}));
    try testing.expect(!shapeOk(&Fixture.calls, &.{ k, k }));
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
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const ra = arena.allocator();
    const calls = [_]AggCall{Fixture.calls[0]};
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
    const cols = [_][2]Column{
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
        },
    };
    for ([_]?usize{ null, 3 }) |cut| {
        var want = agg.GroupTable.init(a, 1 << 20);
        defer want.deinit();
        var got = agg.GroupTable.init(a, 1 << 20);
        defer got.deinit();
        got.compact.configure(&calls);
        var lanes = [_]Lanes{
            .{ .domain = &domain, .calls = &calls, .rows = 258 },
            .{ .domain = &domain, .calls = &calls, .rows = 258 },
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
                &calls,
                &keys,
                &want,
                pos,
                &batch,
                &.{ 0, 1 },
                &sel,
                null,
                null,
            ));
            try testing.expect(try lanes[wi].foldKeys(ra, a, &keys, 0, rg, pos, &batch, &.{ 0, 1 }, &sel));
        }
        try intoTable(a, ra, &lanes, &keys, &got, 2, 2, cut);
        try testing.expect(got.allocated_bytes <= domainTableBytes(&domain, domain.ids(), 0, calls.len));
        if (cut) |h| {
            const ids = try ra.alloc(u32, want.groupCount());
            for (ids, 0..) |*id, i| id.* = @intCast(i);
            std.mem.sortUnstable(u32, ids, &want, struct {
                fn better(t: *const agg.GroupTable, x: u32, y: u32) bool {
                    const ca = t.accumulators.items[x].count;
                    const cb = t.accumulators.items[y].count;
                    return ca > cb or (ca == cb and t.first.items[x] < t.first.items[y]);
                }
            }.better);
            try testing.expectEqual(@min(h, ids.len), got.groupCount());
            for (ids[0..@min(h, ids.len)]) |id| {
                const g = got.findKey(want.keyAt(id)) orelse return error.TestExpectedEqual;
                try testing.expectEqual(want.accumulators.items[id].count, got.readAcc(g, 0, 1).count);
                try testing.expectEqual(want.first.items[id], got.first.items[g]);
            }
        } else try expectSameGroups(&want, &got, calls.len);
    }
    var declined: Lanes = .{ .domain = &domain, .calls = &calls, .rows = 129 };
    defer declined.deinit(a);
    var mismatch = cols[1];
    const changed = [_][]const u8{ "unused", "a\x00b", "", "late" };
    mismatch[1].string.dict = &changed;
    const batch: filter_eval.Batch = .{ .cols = &mismatch, .num_rows = 129 };
    var all = try Sel.init(ra, 129);
    try testing.expect(!try declined.foldKeys(ra, a, &keys, 0, 1, 0, &batch, &.{ 0, 1 }, &all));
    try testing.expectEqual(@as(usize, 0), declined.first.len);
}
