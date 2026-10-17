//! Dense GROUP BY over a retained exact key domain.
//!
//! The planner's dictionary union (`ndv.unionBound`) decodes every dictionary of a lone integer key to bound its
//! distinct values. When asked, it also keeps what it found: each distinct value once, under a global id, and for
//! every row group a translation from its dictionary codes to those ids. A scan worker then folds a row group into
//! accumulators indexed by global id (`Lanes`) through the same per-composite folds `agg_coded.groupedCoded` runs, with
//! the translation standing where the composite-to-group map would be: no key is serialized or looked up per row
//! group, and the workers' lanes merge by index instead of through a hash merge.
//!
//! Exact because identity is by value: a row group's dictionary is checked entry for entry against the domain before
//! its codes are trusted, and one that differs (or lacks a translation) folds through the worker's table as before.
//! Unused dictionary entries leave their lanes untouched, so only ids some selected row reached become groups, at the
//! least position any worker saw them, exactly as a merged table would hold them.

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

/// Largest domain kept, nulls' id included: the lanes are walked whole at the merge.
pub const max_ids: usize = 1 << 20;
const unseen = std.math.maxInt(u64);

/// The distinct values of a lone INT32/INT64 key over every row group, as raw 64-bit patterns (INT32 sign-extended,
/// as the union reads it), and each row group's code-to-id translation. Id `values.len` is the null key.
pub const Domain = struct {
    values: []u64,
    int32: bool,
    /// Per row group, in (file, row group) order: one id per dictionary entry, then the null id.
    trans: [][]u32,
    /// Index into `trans` of each file's first row group.
    file_base: []usize,
    /// Bytes `values` and `trans` hold.
    bytes: usize,

    pub fn deinit(d: *Domain, gpa: std.mem.Allocator) void {
        for (d.trans) |t| gpa.free(t);
        gpa.free(d.trans);
        gpa.free(d.file_base);
        gpa.free(d.values);
        d.* = undefined;
    }

    pub fn ids(d: *const Domain) usize {
        return d.values.len + 1;
    }

    pub fn forRowGroup(d: *const Domain, file: usize, rg: usize) ?[]const u32 {
        if (file >= d.file_base.len) return null;
        const i = d.file_base[file] + rg;
        const end = if (file + 1 < d.file_base.len) d.file_base[file + 1] else d.trans.len;
        if (i >= end) return null;
        return if (d.trans[i].len == 0) null else d.trans[i];
    }

    /// Entries the translations hold over all row groups: what grouping by code resolves, one group lookup each.
    pub fn entries(d: *const Domain) usize {
        var n: usize = 0;
        for (d.trans) |t| n += t.len;
        return n;
    }
};

/// Whether a lone bare integer key and these calls fold densely: count, and integer sum/min/max/avg of a bare column.
/// Floats keep their ordered paths, and expressions, DECIMAL, strings and count(DISTINCT) the general ones.
pub fn shapeOk(calls: []const AggCall, keys: []const expr_ast.Expr) bool {
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

/// Bytes one worker's lanes take for `ids` ids and `n_calls` calls.
pub fn laneBytes(ids: usize, n_calls: usize) usize {
    return ids *| (@sizeOf(u64) + @sizeOf(Accumulator) *| n_calls);
}

/// Bytes at most the table `intoTable` fills holds for `ids` groups: what `GroupTable.reserveFixed` charges.
pub fn tableBytes(ids: usize, stride: usize, n_calls: usize) usize {
    const slots = std.math.ceilPowerOfTwo(usize, @max(16, ids *| 4 / 3 + 1)) catch std.math.maxInt(usize);
    return ids *| (stride + @sizeOf(u64) + @sizeOf(Accumulator) *| n_calls) +| slots *| (@sizeOf(u64) + @sizeOf(u32) * 2);
}

/// One worker's accumulators, `calls.len` per id, and each id's least position. Allocated on the worker's first row
/// group, so the pages are first touched by the thread that fills them.
pub const Lanes = struct {
    domain: *const Domain,
    calls: []const AggCall,
    accs: []Accumulator = &.{},
    first: []u64 = &.{},

    fn ensure(l: *Lanes, gpa: std.mem.Allocator) !void {
        if (l.first.len != 0) return;
        const n = l.domain.ids();
        const first = try gpa.alloc(u64, n);
        errdefer gpa.free(first);
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
        l.accs = &.{};
        l.first = &.{};
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
                for (l.accs[id * n_calls ..][0..n_calls]) |*acc| acc.count += n;
            }
            return true;
        }
        const codes = col.codes() orelse return false;
        const ni = col.nullInfo();
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

    fn sameDictionary(l: *const Lanes, col: Column, trans: []const u32) bool {
        const values = l.domain.values;
        switch (col) {
            .i64 => |c| {
                if (l.domain.int32) return false;
                for (c.dict, trans[0..c.dict.len]) |v, id| if (id >= values.len or values[id] != @as(u64, @bitCast(v))) return false;
            },
            .i32 => |c| {
                if (!l.domain.int32) return false;
                for (c.dict, trans[0..c.dict.len]) |v, id| {
                    if (id >= values.len or values[id] != @as(u64, @bitCast(@as(i64, v)))) return false;
                }
            },
            else => return false,
        }
        return trans[trans.len - 1] == values.len;
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
    key: expr_ast.ColRef,
    table: *agg.GroupTable,
    lookup_len: usize,
    n_threads: usize,
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
    var groups: usize = 0;
    for (into.first) |f| groups += @intFromBool(f != unseen);
    try table.reserveFixed(groups, calls.len);

    // The domain's values as the key column the scan decodes, so each key's image is the one the row path packs.
    const dict_col: Column = if (domain.int32) blk: {
        const d = try arena.alloc(i32, domain.values.len);
        for (d, domain.values) |*o, v| o.* = @truncate(@as(i64, @bitCast(v)));
        break :blk .{ .i32 = .{ .values = &.{}, .dict = d } };
    } else .{ .i64 = .{ .values = &.{}, .dict = @ptrCast(domain.values) } };
    const kd = [_]Column{try agg_coded.keyDictColumn(arena, dict_col, key, lookup_len)};
    var image: [32]u8 = undefined;
    for (into.first, 0..) |f, id| if (f != unseen) {
        const len = agg.packFixedKey(&kd, id, &image) orelse return error.BadGroupKey;
        const g = try table.getOrInsert(image[0..len], calls, f);
        var scratch: [16]agg.Accumulator = undefined;
        const dst = table.readGroup(g, calls.len, &scratch);
        @memcpy(dst, into.accs[id * calls.len ..][0..calls.len]);
        table.writeGroup(g, dst);
    };
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

    fn domain() Domain {
        return .{ .values = &values, .int32 = false, .trans = &trans, .file_base = &file_base, .bytes = 0 };
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
        for (want.accumulators.items[g * n_calls ..][0..n_calls], got.accumulators.items[h * n_calls ..][0..n_calls]) |w, x|
            try testing.expect(std.meta.eql(w, x));
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
    try intoTable(testing.allocator, a, &lanes, Fixture.key, &got, lookup.len, 2);
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
            try intoTable(gpa, a, &lanes, Fixture.key, &got, lookup.len, 1);
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
