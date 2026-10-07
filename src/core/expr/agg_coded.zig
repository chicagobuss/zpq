//! Encoded execution: aggregates folded in the dictionary-code domain.
//!
//! A column decoded as codes (`ColumnT.codes` + `dict`) never materializes its values. Aggregates over it
//! histogram the selected rows' codes, then fold each dictionary entry once, weighted by its count:
//!   sum = Σ count[k]·dict[k], count = Σ count[k], min/max = extremum over entries with count[k] > 0.
//! Grouping by coded keys maps each row's tuple of codes to a dense composite index, resolves each composite that
//! occurs to a group id once (through the same serialized key the row path builds, so groups are identical), and
//! folds per composite rather than per row.
//!
//! Integer results are exact. Float sums are reassociated (count·value instead of value added count times), which
//! differs from the row-order sum only in rounding.

const std = @import("std");
const filter_eval = @import("../filter/eval.zig");
const filter_selection = @import("../filter/selection.zig");
const expr_ast = @import("ast.zig");
const expr_eval = @import("eval.zig");
const agg = @import("agg.zig");

const Column = filter_eval.Batch.Column;
const Sel = filter_selection.SelectionVector;
const Accumulator = agg.Accumulator;
const AggCall = agg.AggCall;
const Error = agg.Error;

/// The coded column a bare col_ref argument names, or null when the call can't take the coded path.
pub fn codedArg(call: AggCall, batch: *const filter_eval.Batch, lookup: []const ?usize) ?Column {
    const arg = call.arg orelse return null;
    if (arg != .col_ref) return null;
    const pos = lookup[arg.col_ref.col_idx] orelse return null;
    const col = batch.cols[pos];
    if (col.codes() == null) return null;
    if (col.nullInfo().max_rep > 0) return null;
    return col;
}

/// Count selected rows per code. Four interleaved tables break the store-to-load chain that runs of one code
/// would otherwise serialize on.
fn histogram(arena: std.mem.Allocator, codes: []const u32, n_dict: usize, sel: *const Sel) ![]u64 {
    const h = try arena.alloc(u64, n_dict * 4);
    defer arena.free(h);
    @memset(h, 0);
    const out = try arena.alloc(u64, n_dict);
    @memset(out, 0);
    const n = codes.len;
    var w: usize = 0;
    while ((w + 1) * 64 <= n) : (w += 1) {
        const word = sel.mask[w];
        const base = w * 64;
        if (word == std.math.maxInt(u64)) {
            var r: usize = base;
            while (r < base + 64) : (r += 4) {
                h[codes[r] * 4 + 0] += 1;
                h[codes[r + 1] * 4 + 1] += 1;
                h[codes[r + 2] * 4 + 2] += 1;
                h[codes[r + 3] * 4 + 3] += 1;
            }
        } else if (word != 0) {
            var bits = word;
            while (bits != 0) {
                const i: usize = @ctz(bits);
                h[codes[base + i] * 4] += 1;
                bits &= bits - 1;
            }
        }
    }
    for (w * 64..n) |r| if (sel.isActive(r)) {
        h[codes[r] * 4] += 1;
    };
    for (out, 0..) |*o, k| o.* = h[k * 4] + h[k * 4 + 1] + h[k * 4 + 2] + h[k * 4 + 3];
    return out;
}

/// The dictionary widened to the call's lane, as `PreparedAggInput.bindColRef` widens row values.
fn widenDict(arena: std.mem.Allocator, col: Column, c: expr_ast.ColRef) Error!Column {
    const d = col.dictColumn();
    return switch (c.expr_type) {
        .i64 => .{ .i64 = .{ .values = try expr_eval.widenColRefToI64(arena, d, c) } },
        .f64 => .{ .f64 = .{ .values = try expr_eval.widenToF64(arena, d) } },
        .str => d,
    };
}

fn selectWithNulls(
    arena: std.mem.Allocator,
    call: AggCall,
    col: Column,
    batch: *const filter_eval.Batch,
    lookup: []const ?usize,
    outer: *const Sel,
) Error!?Sel {
    var own: ?Sel = null;
    if (call.where) |pred| {
        own = try outer.cloneAlloc(arena);
        try filter_eval.evaluate(pred, batch, &own.?, lookup, arena);
    }
    const has_nulls = switch (col) {
        inline else => |c| c.has_nulls,
    };
    if (has_nulls) {
        if (own == null) own = try outer.cloneAlloc(arena);
        agg.intersectPresentMask(&own.?, col);
    }
    return own;
}

/// Scalar `updateOne` for a coded argument. Returns false (nothing folded) when the call doesn't qualify.
pub fn updateOneCoded(
    arena: std.mem.Allocator,
    persist: std.mem.Allocator,
    state: *Accumulator,
    call: AggCall,
    batch: *const filter_eval.Batch,
    lookup: []const ?usize,
    outer_sel: *const Sel,
) Error!bool {
    if (call.func == .count) if (call.arg) |arg| if (arg == .col_ref) {
        const pos = lookup[arg.col_ref.col_idx] orelse return error.BadColumn;
        const lcol = batch.cols[pos];
        if (lcol.levelsOnly()) {
            var own = try selectWithNulls(arena, call, lcol, batch, lookup, outer_sel);
            defer if (own) |*s| s.deinit();
            const sel: *const Sel = if (own) |*s| s else outer_sel;
            state.count += sel.count();
            return true;
        }
    };
    if (call.arg) |arg| if (arg == .col_ref) {
        const pos = lookup[arg.col_ref.col_idx] orelse return error.BadColumn;
        const hcol = batch.cols[pos];
        // Planned only for unfiltered scalar calls without FILTER, so every present row is selected.
        if (hcol.hist()) |h| {
            std.debug.assert(call.where == null);
            const dict = try widenDict(arena, hcol, arg.col_ref);
            try foldHist(persist, state, call, dict, h, arg.col_ref.unsigned_64);
            return true;
        }
    };
    const col = codedArg(call, batch, lookup) orelse return false;
    const c = call.arg.?.col_ref;
    var own = try selectWithNulls(arena, call, col, batch, lookup, outer_sel);
    defer if (own) |*s| s.deinit();
    const sel: *const Sel = if (own) |*s| s else outer_sel;
    const hist = try histogram(arena, col.codes().?, col.dictLen(), sel);
    const dict = try widenDict(arena, col, c);
    try foldHist(persist, state, call, dict, hist, c.unsigned_64);
    return true;
}

/// Fold one histogram over a (widened) dictionary into `state`, matching the row folds' semantics.
fn foldHist(
    persist: std.mem.Allocator,
    state: *Accumulator,
    call: AggCall,
    dict: Column,
    hist: []const u64,
    unsigned_64: bool,
) Error!void {
    var total: u64 = 0;
    for (hist) |h| total += h;
    switch (call.func) {
        .count => {
            state.count += total;
            return;
        },
        else => {},
    }
    if (total == 0) return;
    switch (state.*) {
        .sum_i => |*s| {
            var acc: i128 = 0;
            for (hist, dict.i64.values) |h, v| {
                if (h == 0) continue;
                const x: i128 = if (unsigned_64) @as(u64, @bitCast(v)) else v;
                acc += x * @as(i128, h);
            }
            s.* = (s.* orelse 0) + acc;
        },
        .sum_f => |*s| {
            var acc: f64 = 0;
            for (hist, dict.f64.values) |h, v| {
                if (h == 0) continue;
                acc += @as(f64, @floatFromInt(h)) * v;
            }
            s.* = (s.* orelse 0) + acc;
        },
        .min_i, .max_i => |*m| {
            const is_min = state.* == .min_i;
            for (hist, dict.i64.values) |h, v| {
                if (h == 0) continue;
                const x: i128 = if (unsigned_64) @as(u64, @bitCast(v)) else v;
                if (m.* == null or (is_min and x < m.*.?) or (!is_min and x > m.*.?)) m.* = x;
            }
        },
        .min_f, .max_f => |*m| {
            const is_min = state.* == .min_f;
            // Row folds reduce with @min/@max, under which a NaN entry never wins against a number.
            var best: ?f64 = null;
            for (hist, dict.f64.values) |h, v| {
                if (h == 0) continue;
                best = if (best) |b| (if (is_min) @min(b, v) else @max(b, v)) else v;
            }
            if (best) |b| m.* = if (m.*) |cur| (if (is_min) @min(cur, b) else @max(cur, b)) else b;
        },
        .min_bytes, .max_bytes => |*m| {
            const is_min = state.* == .min_bytes;
            var best: ?[]const u8 = null;
            for (hist, dict.string.values) |h, v| {
                if (h == 0) continue;
                if (best) |b| {
                    const ord = std.mem.order(u8, v, b);
                    if ((is_min and ord == .lt) or (!is_min and ord == .gt)) best = v;
                } else best = v;
            }
            const cand = best.?;
            if (m.*) |cur| {
                const ord = std.mem.order(u8, cand, cur);
                if (!((is_min and ord == .lt) or (!is_min and ord == .gt))) return;
                const owned = try persist.dupe(u8, cand);
                persist.free(cur);
                m.* = owned;
                return;
            }
            m.* = try persist.dupe(u8, cand);
        },
        .avg => |*a| {
            const vals: []const f64 = switch (dict) {
                .f64 => |d| d.values,
                .i64 => |d| blk: {
                    // Built per call; dictionaries are small.
                    var sum: f64 = 0;
                    for (hist, d.values) |h, v| {
                        if (h == 0) continue;
                        sum += @as(f64, @floatFromInt(h)) * @as(f64, @floatFromInt(v));
                    }
                    a.sum += sum;
                    a.count += total;
                    break :blk &.{};
                },
                else => return error.UnsupportedAggType,
            };
            if (dict == .f64) {
                var sum: f64 = 0;
                for (hist, vals) |h, v| {
                    if (h == 0) continue;
                    sum += @as(f64, @floatFromInt(h)) * v;
                }
                a.sum += sum;
                a.count += total;
            }
        },
        .count => unreachable,
    }
}

// ------------------------------------------------------------------ grouped

/// Composite spaces above this fall back to the row path.
const MAX_COMPOSITE: usize = 1 << 16;
/// Per-call (composite × code) histograms above this fall back to the per-row typed fold.
const MAX_HIST: usize = 1 << 20;

/// Grouped scan with every GROUP BY key a bare coded column. Fills `group_id_arr` for selected rows and folds every
/// call. Returns false, having done nothing, when the keys don't qualify.
pub fn groupedCoded(
    ra: std.mem.Allocator,
    gpa: std.mem.Allocator,
    calls: []const AggCall,
    keys: []const expr_ast.Expr,
    table: *agg.GroupTable,
    /// `agg.firstPosition` of row 0.
    pos_base: u64,
    batch: *const filter_eval.Batch,
    lookup: []const ?usize,
    sel: *const Sel,
) !bool {
    const num_rows = batch.num_rows;
    const key_cols = try ra.alloc(Column, keys.len);
    const strides = try ra.alloc(usize, keys.len);
    var space: usize = 1;
    for (keys, 0..) |k, i| {
        if (k != .col_ref) return false;
        const pos = lookup[k.col_ref.col_idx] orelse return false;
        const col = batch.cols[pos];
        if (col.codes() == null or col.nullInfo().max_rep > 0) return false;
        key_cols[i] = col;
        strides[i] = space;
        space *= col.dictLen() + 1; // + the null slot
        if (space > MAX_COMPOSITE) return false;
    }

    // Composite code per selected row.
    const comp = try ra.alloc(u32, num_rows);
    @memset(comp, 0);
    for (key_cols, strides) |col, stride| {
        const codes = col.codes().?;
        const ni = col.nullInfo();
        const null_slot: u32 = @intCast(col.dictLen());
        const st: u32 = @intCast(stride);
        if (ni.def_levels) |dl| {
            for (comp, codes, dl) |*cp, code, d| cp.* += (if (d < ni.max_def) null_slot else code) * st;
        } else {
            for (comp, codes) |*cp, code| cp.* += code * st;
        }
    }

    // Resolve each composite that occurs among selected rows, once.
    const gid_of = try ra.alloc(u32, space);
    const unresolved = std.math.maxInt(u32);
    @memset(gid_of, unresolved);
    // Each key's dictionary plus a trailing null entry, widened exactly as the row path widens key values.
    const kd_cols = try ra.alloc(Column, keys.len);
    for (keys, key_cols, 0..) |k, col, i| {
        kd_cols[i] = try keyDictColumn(ra, col, k.col_ref, lookup.len);
    }
    var scratch: std.ArrayList(u8) = .empty;
    const rows = try ra.alloc(usize, keys.len);
    // One pass finds each composite's first selected row; only composites that occur get resolved, in the order they
    // first occur, so a new group's position is where it first appears.
    const first_row = try compositeFirstRows(ra, comp, space, sel);
    const order = try ra.alloc(u32, space);
    var n_occur: usize = 0;
    for (first_row, 0..) |fr, cp| {
        if (fr == no_row) continue;
        order[n_occur] = @intCast(cp);
        n_occur += 1;
    }
    std.mem.sort(u32, order[0..n_occur], first_row, struct {
        fn lt(fr: []const u32, a: u32, b: u32) bool {
            return fr[a] < fr[b];
        }
    }.lt);
    for (order[0..n_occur]) |cp| {
        var rem: usize = cp;
        var i: usize = keys.len;
        while (i > 0) {
            i -= 1;
            rows[i] = rem / strides[i];
            rem %= strides[i];
        }
        try serializeKeyAt(&scratch, ra, kd_cols, rows);
        gid_of[cp] = try table.getOrInsert(scratch.items, calls, pos_base | first_row[cp]);
    }

    const agg_len = calls.len;
    for (calls, 0..) |call, ai| {
        const accs = table.accumulators.items;
        try foldGroupedCall(ra, gpa, accs, gid_of, comp, space, ai, agg_len, call, batch, lookup, sel);
    }
    return true;
}

/// The dictionary of a key column as a group-key column: entries 0..n-1, then a null at n.
fn keyDictColumn(ra: std.mem.Allocator, col: Column, c: expr_ast.ColRef, lookup_len: usize) !Column {
    const n = col.dictLen();
    const dl = try ra.alloc(u32, n + 1);
    @memset(dl[0..n], 1);
    dl[n] = 0;
    const ext: Column = switch (col) {
        inline else => |cc, tag| blk: {
            const T = @TypeOf(cc.dict[0]);
            const vals = try ra.alloc(T, n + 1);
            @memcpy(vals[0..n], cc.dict);
            vals[n] = std.mem.zeroes(T);
            const ext_col: @TypeOf(cc) = .{ .values = vals, .def_levels = dl, .max_def = 1, .has_nulls = true };
            break :blk @unionInit(Column, @tagName(tag), ext_col);
        },
    };
    const cols = [_]Column{ext};
    const fake: filter_eval.Batch = .{ .cols = &cols, .num_rows = n + 1 };
    const lk = try ra.alloc(?usize, lookup_len);
    @memset(lk, null);
    lk[c.col_idx] = 0;
    return expr_eval.evalGroupKeyExpr(ra, &fake, lk, .{ .col_ref = c });
}

/// `agg.serializeRowKey` with each key column at its own row.
fn serializeKeyAt(
    list: *std.ArrayList(u8),
    allocator: std.mem.Allocator,
    cols: []const Column,
    rows: []const usize,
) !void {
    list.clearRetainingCapacity();
    var one: std.ArrayList(u8) = .empty;
    defer one.deinit(allocator);
    for (cols, rows) |col, row| {
        try agg.serializeRowKey(&one, allocator, &.{col}, row);
        try list.appendSlice(allocator, one.items);
    }
}

fn foldGroupedCall(
    ra: std.mem.Allocator,
    gpa: std.mem.Allocator,
    accs: []Accumulator,
    gid_of: []const u32,
    comp: []const u32,
    space: usize,
    ai: usize,
    agg_len: usize,
    call: AggCall,
    batch: *const filter_eval.Batch,
    lookup: []const ?usize,
    outer: *const Sel,
) !void {
    // Selection for this call: outer, narrowed by FILTER and the argument's nulls.
    var own: ?Sel = null;
    defer if (own) |*s| s.deinit();
    if (call.where) |pred| {
        own = try outer.cloneAlloc(ra);
        try filter_eval.evaluate(pred, batch, &own.?, lookup, ra);
    }
    var arg_col: ?Column = null;
    if (call.arg) |arg| if (arg == .col_ref) {
        const pos = lookup[arg.col_ref.col_idx] orelse return error.BadColumn;
        arg_col = batch.cols[pos];
        if (arg_col.?.nullInfo().max_rep > 0) return error.UnsupportedAggType;
        const has_nulls = switch (arg_col.?) {
            inline else => |c| c.has_nulls,
        };
        if (has_nulls) {
            if (own == null) own = try outer.cloneAlloc(ra);
            agg.intersectPresentMask(&own.?, arg_col.?);
        }
    };
    const sel: *const Sel = if (own) |*s| s else outer;

    // count(*) / count(col): rows per composite.
    if (call.func == .count) {
        const cnt = try compositeCounts(ra, comp, space, sel);
        for (cnt, 0..) |n, cp| if (n != 0) {
            accs[gid_of[cp] * agg_len + ai].count += n;
        };
        return;
    }

    if (call.arg.? == .col_ref and arg_col.?.codes() != null and space * arg_col.?.dictLen() <= MAX_HIST) {
        // (composite, code) histogram, then fold each entry once per group.
        const col = arg_col.?;
        const c = call.arg.?.col_ref;
        const card = col.dictLen();
        const codes = col.codes().?;
        const h = try ra.alloc(u64, space * card);
        @memset(h, 0);
        forEachActive(sel, comp.len, struct {
            fn f(ctx: anytype, r: usize) void {
                ctx.h[@as(usize, ctx.comp[r]) * ctx.card + ctx.codes[r]] += 1;
            }
        }.f, .{ .h = h, .comp = comp, .card = card, .codes = codes });
        const dict = try widenDict(ra, col, c);
        for (0..space) |cp| {
            const hist = h[cp * card ..][0..card];
            var any = false;
            for (hist) |x| if (x != 0) {
                any = true;
                break;
            };
            if (!any) continue;
            try foldHist(gpa, &accs[gid_of[cp] * agg_len + ai], call, dict, hist, c.unsigned_64);
        }
        return;
    }

    // Values argument (flat column or computed expression): typed per-row fold into per-composite partials.
    var vals_col: Column = undefined;
    var unsigned_64 = false;
    switch (call.arg.?) {
        .col_ref => |c| {
            const raw = try materialize(ra, arg_col.?);
            unsigned_64 = c.unsigned_64;
            vals_col = switch (c.expr_type) {
                .i64 => .{ .i64 = .{ .values = try expr_eval.widenColRefToI64(ra, raw, c) } },
                .f64 => .{ .f64 = .{ .values = try expr_eval.widenToF64(ra, raw) } },
                .str => raw,
            };
        },
        else => |e| vals_col = try expr_eval.evalExpr(ra, batch, lookup, e),
    }
    // Strings and anything unusual: the row fold, with group ids expanded from composites.
    const is_numeric_sum_like = (vals_col == .f64 or vals_col == .i64) and call.result != .bytes;
    if (!is_numeric_sum_like) {
        const gids = try ra.alloc(u32, comp.len);
        for (gids, comp) |*g, cp| g.* = gid_of[cp];
        try agg.updateOneGroupedValues(ra, gpa, accs, gids, ai, agg_len, call, vals_col, sel, unsigned_64);
        return;
    }
    const seen = try ra.alloc(u64, space);
    @memset(seen, 0);
    switch (vals_col) {
        .f64 => |vc| {
            const v = vc.values;
            const is_min = call.func == .min;
            const part = try ra.alloc(f64, space);
            switch (call.func) {
                .sum, .avg => @memset(part, 0),
                .min => @memset(part, std.math.inf(f64)),
                .max => @memset(part, -std.math.inf(f64)),
                .count => unreachable,
            }
            switch (call.func) {
                .sum, .avg => forEachActive(sel, comp.len, struct {
                    fn f(ctx: anytype, r: usize) void {
                        const cp = ctx.comp[r];
                        ctx.seen[cp] += 1;
                        ctx.part[cp] += ctx.v[r];
                    }
                }.f, .{ .comp = comp, .seen = seen, .part = part, .v = v }),
                .min, .max => {
                    var r: usize = 0;
                    while (r < comp.len) : (r += 1) {
                        if (!sel.isActive(r)) continue;
                        const cp = comp[r];
                        seen[cp] += 1;
                        part[cp] = if (is_min) @min(part[cp], v[r]) else @max(part[cp], v[r]);
                    }
                },
                .count => unreachable,
            }
            for (0..space) |cp| {
                if (seen[cp] == 0) continue;
                const st = &accs[gid_of[cp] * agg_len + ai];
                switch (st.*) {
                    .sum_f => |*s| s.* = (s.* orelse 0) + part[cp],
                    .avg => |*a| {
                        a.sum += part[cp];
                        a.count += seen[cp];
                    },
                    .min_f => |*m| m.* = if (m.*) |cur| @min(cur, part[cp]) else part[cp],
                    .max_f => |*m| m.* = if (m.*) |cur| @max(cur, part[cp]) else part[cp],
                    else => return error.UnsupportedAggType,
                }
            }
        },
        .i64 => |vc| {
            const v = vc.values;
            const part = try ra.alloc(i128, space);
            const fpart = try ra.alloc(f64, space);
            @memset(fpart, 0);
            switch (call.func) {
                .sum, .avg => @memset(part, 0),
                .min => @memset(part, std.math.maxInt(i128)),
                .max => @memset(part, std.math.minInt(i128)),
                .count => unreachable,
            }
            var r: usize = 0;
            while (r < comp.len) : (r += 1) {
                if (!sel.isActive(r)) continue;
                const cp = comp[r];
                seen[cp] += 1;
                const x: i128 = if (unsigned_64) @as(u64, @bitCast(v[r])) else v[r];
                switch (call.func) {
                    .sum => part[cp] += x,
                    .avg => fpart[cp] += @floatFromInt(v[r]),
                    .min => part[cp] = @min(part[cp], x),
                    .max => part[cp] = @max(part[cp], x),
                    .count => unreachable,
                }
            }
            for (0..space) |cp| {
                if (seen[cp] == 0) continue;
                const st = &accs[gid_of[cp] * agg_len + ai];
                switch (st.*) {
                    .sum_i => |*s| s.* = (s.* orelse 0) + part[cp],
                    .avg => |*a| {
                        a.sum += fpart[cp];
                        a.count += seen[cp];
                    },
                    .min_i => |*m| m.* = if (m.*) |cur| @min(cur, part[cp]) else part[cp],
                    .max_i => |*m| m.* = if (m.*) |cur| @max(cur, part[cp]) else part[cp],
                    else => return error.UnsupportedAggType,
                }
            }
        },
        else => unreachable,
    }
}

const no_row = std.math.maxInt(u32);

/// Each composite's first row `sel` keeps, or `no_row`.
fn compositeFirstRows(ra: std.mem.Allocator, comp: []const u32, space: usize, sel: *const Sel) ![]u32 {
    const first = try ra.alloc(u32, space);
    @memset(first, no_row);
    forEachActive(sel, comp.len, struct {
        fn f(ctx: anytype, r: usize) void {
            const slot = &ctx.first[ctx.comp[r]];
            if (slot.* == no_row) slot.* = @intCast(r);
        }
    }.f, .{ .first = first, .comp = comp });
    return first;
}

fn compositeCounts(ra: std.mem.Allocator, comp: []const u32, space: usize, sel: *const Sel) ![]u64 {
    const cnt = try ra.alloc(u64, space);
    @memset(cnt, 0);
    forEachActive(sel, comp.len, struct {
        fn f(ctx: anytype, r: usize) void {
            ctx.cnt[ctx.comp[r]] += 1;
        }
    }.f, .{ .cnt = cnt, .comp = comp });
    return cnt;
}

inline fn forEachActive(sel: *const Sel, n: usize, comptime f: anytype, ctx: anytype) void {
    var w: usize = 0;
    while (w * 64 < n) : (w += 1) {
        const word = sel.mask[w];
        if (word == 0) continue;
        const base = w * 64;
        if (word == std.math.maxInt(u64) and base + 64 <= n) {
            for (base..base + 64) |r| f(ctx, r);
        } else {
            var bits = word;
            while (bits != 0) {
                const i: usize = @ctz(bits);
                f(ctx, base + i);
                bits &= bits - 1;
            }
        }
    }
}

/// The rows `sel` keeps of a flat column, `active` of them, as a column of their own: values (looked up in the
/// dictionary for a coded column) and levels compacted. Nested columns are not gathered.
pub fn gatherSelected(ra: std.mem.Allocator, col: Column, sel: *const Sel, active: usize) !Column {
    return switch (col) {
        inline else => |c, tag| blk: {
            if (c.max_rep > 0) return error.UnsupportedAggType;
            const T = @TypeOf(c.dict[0]);
            const vals = try ra.alloc(T, active);
            const dl: ?[]u32 = if (c.def_levels != null) try ra.alloc(u32, active) else null;
            const Ctx = struct { c: @TypeOf(c), vals: []T, dl: ?[]u32, j: usize = 0 };
            var ctx: Ctx = .{ .c = c, .vals = vals, .dl = dl };
            forEachActive(sel, sel.len, struct {
                fn f(x: *Ctx, r: usize) void {
                    const present = if (x.c.def_levels) |d| d[r] == x.c.max_def else true;
                    x.vals[x.j] = if (x.c.codes) |k|
                        (if (present) x.c.dict[k[r]] else std.mem.zeroes(T))
                    else
                        x.c.values[r];
                    if (x.dl) |d| d[x.j] = x.c.def_levels.?[r];
                    x.j += 1;
                }
            }.f, &ctx);
            std.debug.assert(ctx.j == active);
            break :blk @unionInit(Column, @tagName(tag), .{
                .values = vals,
                .def_levels = dl,
                .max_def = c.max_def,
                .has_nulls = c.has_nulls,
            });
        },
    };
}

/// Values for a column that may hold only codes: gathered from the dictionary when they were never materialized.
pub fn materialize(ra: std.mem.Allocator, col: Column) !Column {
    return switch (col) {
        inline else => |c, tag| blk: {
            if (!c.codesOnly()) break :blk col;
            const T = @TypeOf(c.dict[0]);
            const codes = c.codes.?;
            const vals = try ra.alloc(T, codes.len);
            const d = c.dict;
            if (d.len == 0) {
                @memset(vals, std.mem.zeroes(T));
            } else for (vals, codes) |*v, k| {
                v.* = d[k];
            }
            if (c.def_levels) |dl| for (vals, dl) |*v, dv| {
                if (dv < c.max_def) v.* = std.mem.zeroes(T);
            };
            var out = c;
            out.values = vals;
            break :blk @unionInit(Column, @tagName(tag), out);
        },
    };
}

test "aggregates over a coded column match the row folds over its values, histogram form included" {
    const testing = std.testing;
    var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_state.deinit();
    const a = arena_state.allocator();

    // 300 rows over a 5-entry dictionary with nulls; the selection drops every third row.
    const dict = [_]i64{ -7, 0, 3, 1_000_000_007, -2 };
    const n = 300;
    var codes: [n]u32 = undefined;
    var defs: [n]u32 = undefined;
    var vals: [n]i64 = undefined;
    for (0..n) |i| {
        defs[i] = @intFromBool(i % 11 != 4);
        codes[i] = if (defs[i] == 1) @intCast((i * 7 + i / 13) % dict.len) else 0;
        vals[i] = if (defs[i] == 1) dict[codes[i]] else 0;
    }
    var sel = try Sel.init(a, n);
    for (0..n) |i| if (i % 3 == 0) sel.set(i, false);
    var all = try Sel.init(a, n);
    _ = &all;

    const coded: Column = .{ .i64 = .{
        .values = &.{},
        .codes = &codes,
        .dict = &dict,
        .def_levels = &defs,
        .max_def = 1,
        .has_nulls = true,
    } };
    var hist: [dict.len]u64 = @splat(0);
    var present: usize = 0;
    for (codes, defs) |k, d| if (d == 1) {
        hist[k] += 1;
        present += 1;
    };
    const histogram_col: Column = .{ .i64 = .{
        .values = &.{},
        .dict = &dict,
        .hist = &hist,
        .levels_only_rows = n,
        .has_nulls = present != n,
    } };
    const plain: Column = .{ .i64 = .{ .values = &vals, .def_levels = &defs, .max_def = 1, .has_nulls = true } };
    const lookup = [_]?usize{0};
    const ref: expr_ast.Expr = .{ .col_ref = .{ .col_idx = 0, .physical_type = .INT64, .expr_type = .i64 } };

    const Case = struct { func: agg.AggFunc, result: agg.ResultShape };
    for ([_]Case{
        .{ .func = .sum, .result = .i64 },
        .{ .func = .min, .result = .i64 },
        .{ .func = .max, .result = .i64 },
        .{ .func = .count, .result = .i64 },
        .{ .func = .avg, .result = .avg_f64 },
    }) |c| {
        const call: AggCall = .{ .func = c.func, .arg = ref, .where = null, .alias = "a", .result = c.result };
        for ([_]*const Sel{ &sel, &all }) |s| {
            var want = Accumulator.init(call);
            const plain_cols = [_]Column{plain};
            try agg.updateOne(a, a, &want, call, &.{ .cols = &plain_cols, .num_rows = n }, &lookup, s);
            var got = Accumulator.init(call);
            const coded_cols = [_]Column{coded};
            try agg.updateOne(a, a, &got, call, &.{ .cols = &coded_cols, .num_rows = n }, &lookup, s);
            try testing.expectEqualDeep(want, got);
        }
        // The histogram form stands for an unfiltered scan: every row selected.
        var want = Accumulator.init(call);
        const plain_cols = [_]Column{plain};
        try agg.updateOne(a, a, &want, call, &.{ .cols = &plain_cols, .num_rows = n }, &lookup, &all);
        var got = Accumulator.init(call);
        const hist_cols = [_]Column{histogram_col};
        try agg.updateOne(a, a, &got, call, &.{ .cols = &hist_cols, .num_rows = n }, &lookup, &all);
        try testing.expectEqualDeep(want, got);
    }
}

test "gatherSelected keeps the selected rows of a coded or plain column, in order, with their levels" {
    const testing = std.testing;
    var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_state.deinit();
    const a = arena_state.allocator();
    const dict = [_]f64{ 1.5, -2.0, 8.25 };
    const n = 130;
    var codes: [n]u32 = undefined;
    var defs: [n]u32 = undefined;
    var vals: [n]f64 = undefined;
    var sel = try Sel.init(a, n);
    var want: std.ArrayList(f64) = .empty;
    var want_defs: std.ArrayList(u32) = .empty;
    for (0..n) |i| {
        defs[i] = @intFromBool(i % 5 != 2);
        codes[i] = if (defs[i] == 1) @intCast(i % 3) else 0;
        vals[i] = if (defs[i] == 1) dict[codes[i]] else 0;
        const keep = i % 7 == 0 or i == n - 1;
        sel.set(i, keep);
        if (keep) {
            try want.append(a, vals[i]);
            try want_defs.append(a, defs[i]);
        }
    }
    const active = sel.count();
    for ([_]Column{
        .{ .f64 = .{ .values = &.{}, .codes = &codes, .dict = &dict, .def_levels = &defs, .max_def = 1 } },
        .{ .f64 = .{ .values = &vals, .def_levels = &defs, .max_def = 1 } },
    }) |col| {
        const got = try gatherSelected(a, col, &sel, active);
        try testing.expectEqualSlices(f64, want.items, got.f64.values);
        try testing.expectEqualSlices(u32, want_defs.items, got.f64.def_levels.?);
    }
}
