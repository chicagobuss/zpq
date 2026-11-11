const std = @import("std");
const filter_eval = @import("../filter/eval.zig");
const filter_selection = @import("../filter/selection.zig");
const expr_ast = @import("ast.zig");
const expr_eval = @import("eval.zig");
const agg = @import("agg.zig");
const DecodeBudget = @import("../consumer.zig").DecodeBudget;
const huge_pages = @import("../huge_pages.zig");
const Column = filter_eval.Batch.Column;
const Sel = filter_selection.SelectionVector;
const Accumulator = agg.Accumulator;
const AggCall = agg.AggCall;
const ac = @import("agg_coded.zig");
const codePlainKeys = ac.codePlainKeys;
const foldExactBatch = ac.foldExactBatch;
const foldExactWindows = ac.foldExactWindows;
const foldGroupedCall = ac.foldGroupedCall;
const foldGroupedCallWithContext = ac.foldGroupedCallWithContext;
const fuse_probe = ac.fuse_probe;
const gatherSelected = ac.gatherSelected;
const groupedCoded = ac.groupedCoded;
const histGroupKey = ac.histGroupKey;
const no_row = ac.no_row;
const unresolved = ac.unresolved;
const compositeFirstRows = ac.compositeFirstRows;
const foldFloatBatches = ac.foldFloatBatches;
const max_coded_plain = ac.max_coded_plain;
const max_int_range = ac.max_int_range;
const seedable = ac.seedable;

test "float batches preserve grouped sum bits, NULLs, selections, CSE and allocation failures" {
    try std.testing.checkAllAllocationFailures(std.testing.allocator, testFloatBatches, .{});
}

fn testFloatBatches(a: std.mem.Allocator) !void {
    const old = expr_eval.forced_batch_rows;
    expr_eval.forced_batch_rows = 4096;
    defer expr_eval.forced_batch_rows = old;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const ra = arena.allocator();
    const n = 32769;
    const vals = try ra.alloc(f64, n);
    const defs = try ra.alloc(u32, n);
    const comp = try ra.alloc(u32, n);
    for (vals, defs, comp, 0..) |*v, *d, *c, r| {
        v.* = switch (r % 3) {
            0 => 1e16,
            1 => 1,
            else => -1e16,
        };
        d.* = @intFromBool(r % 19 != 0);
        c.* = @intCast(r % 4);
    }
    const cols = [_]Column{.{ .f64 = .{ .values = vals, .def_levels = defs, .max_def = 1, .has_nulls = true } }};
    const batch: filter_eval.Batch = .{ .cols = &cols, .num_rows = n };
    const lookup = [_]?usize{0};
    var x: expr_ast.Expr = .{ .col_ref = .{ .col_idx = 0, .physical_type = .DOUBLE, .expr_type = .f64 } };
    var one: expr_ast.Expr = .{ .literal = .{ .i64 = 1 } };
    var sum: expr_ast.Expr = .{ .binop = .{ .op = .add, .left = &x, .right = &one, .result_type = .f64, .depth = 2 } };
    // A second occurrence has its own AST address but shares its computed result.
    var same_sum = sum;
    const product: expr_ast.Expr = .{ .binop = .{
        .op = .mul,
        .left = &same_sum,
        .right = &sum,
        .result_type = .f64,
        .depth = 3,
    } };
    const calls = [_]AggCall{
        .{ .func = .sum, .arg = sum, .result = .f64, .where = null, .alias = "s" },
        .{ .func = .avg, .arg = product, .result = .avg_f64, .where = null, .alias = "a" },
        .{ .func = .count, .arg = null, .result = .i64, .where = null, .alias = "n" },
    };
    var plan: expr_eval.FloatPlan = .{};
    _ = try plan.add(ra, &batch, &lookup, sum);
    _ = try plan.add(ra, &batch, &lookup, product);
    try std.testing.expectEqual(@as(usize, 4), plan.nodes.items.len);
    var sel = try Sel.init(ra, n);
    for (0..n) |r| if (r % 7 == 0 or r / 64 == 80) sel.set(r, false);
    var want = agg.GroupTable.init(a, 1 << 24);
    defer want.deinit();
    var got = agg.GroupTable.init(a, 1 << 24);
    defer got.deinit();
    const keys = [_][]const u8{ "a", "b", "c", "d" };
    for (keys) |key| {
        _ = try want.getOrInsert(key, &calls, 0);
        _ = try got.getOrInsert(key, &calls, 0);
    }
    const gids = [_]u32{ 0, 1, 2, 3 };
    for (calls, 0..) |call, ai| try foldGroupedCall(
        ra,
        a,
        want.accumulators.items,
        &gids,
        comp,
        4,
        ai,
        calls.len,
        call,
        &batch,
        &lookup,
        &sel,
        &want,
    );
    try std.testing.expect(try foldFloatBatches(ra, a, &got, &gids, comp, 4, &calls, &batch, &lookup, &sel));
    try std.testing.expectEqualDeep(want.accumulators.items, got.accumulators.items);
}

test "exact batches sum 64-bit DECIMAL lanes in i128 at their extremes, with NULLs, selections and i128 inputs" {
    const old = expr_eval.forced_batch_rows;
    defer expr_eval.forced_batch_rows = old;
    // Few composites take interleaved partials; past a quarter of the rows each composite takes one. The whole
    // batch at once, then windows of one selection word and of several.
    for ([_]usize{ 5, 9000 }) |space| for ([_]usize{ 0, 64, 4096 }) |rows| {
        expr_eval.forced_batch_rows = rows;
        try testExactBatches(std.testing.allocator, space);
    };
}

fn testExactBatches(a: std.mem.Allocator, space: usize) !void {
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const ra = arena.allocator();
    // Not a multiple of 64, so the last selection word is partial.
    const n = 32769;
    const max18: i64 = 999_999_999_999_999_999;
    const wide: i128 = std.math.pow(i128, 10, 37);
    const big = try ra.alloc(i64, n);
    const half = try ra.alloc(i64, n);
    const small = try ra.alloc(i32, n);
    const huge = try ra.alloc(i128, n);
    const shadow = try ra.alloc(f64, n);
    const times = try ra.alloc(i64, n);
    const defs = try ra.alloc(u32, n);
    const comp = try ra.alloc(u32, n);
    for (big, half, small, huge, shadow, times, defs, comp, 0..) |*b, *h, *sm, *hg, *f, *t, *d, *c, r| {
        // Runs of one sign at DECIMAL(18)'s extremes: any 64-bit running sum would overflow.
        b.* = if (r % 6 < 3) max18 else -max18 + @as(i64, @intCast(r % 5));
        h.* = if (r % 2 == 0) @divTrunc(max18, 2) else -@as(i64, @intCast(r));
        sm.* = if (r % 3 == 0) -999_999_999 else @intCast(r);
        hg.* = if (r % 4 == 0) -wide else wide - @as(i128, @intCast(r));
        f.* = 0;
        // Every third selection word's products leave 64 bits: their windows take the i128 lane, the rest do not.
        // Those products sum past 2^127, beyond what an i128 partial could carry.
        t.* = if ((r / 64) % 3 != 1) 1 else if (b.* > 0) 100_000_000_000_000_000 else 10;
        d.* = @intFromBool(r % 19 != 0);
        // A null row's slot holds garbage the fold must not read as a value.
        if (d.* == 0) {
            h.* = std.math.minInt(i64);
            sm.* = std.math.minInt(i32);
        }
        // Half the rows in one composite, so even one slot per composite runs past 64 bits.
        c.* = if (r % 2 == 0) 0 else @intCast((r * 7 + r / 3) % space);
    }
    const cols = [_]Column{
        .{ .i64 = .{ .values = big, .unscaled = .{ .i64 = big } } },
        .{ .i64 = .{
            .values = half,
            .unscaled = .{ .i64 = half },
            .def_levels = defs,
            .max_def = 1,
            .has_nulls = true,
        } },
        .{ .i32 = .{
            .values = small,
            .unscaled = .{ .i32 = small },
            .def_levels = defs,
            .max_def = 1,
            .has_nulls = true,
        } },
        .{ .f64 = .{ .values = shadow, .unscaled = .{ .i128 = huge } } },
        .{ .i64 = .{ .values = times, .unscaled = .{ .i64 = times } } },
    };
    const batch: filter_eval.Batch = .{ .cols = &cols, .num_rows = n };
    const lookup = [_]?usize{ 0, 1, 2, 3, 4 };
    const d18: expr_ast.Dec = .{ .precision = 18, .scale = 2 };
    var c0: expr_ast.Expr = .{ .col_ref = .{ .col_idx = 0, .physical_type = .INT64, .expr_type = .dec, .dec = d18 } };
    var c1: expr_ast.Expr = .{ .col_ref = .{ .col_idx = 1, .physical_type = .INT64, .expr_type = .dec, .dec = d18 } };
    const c2: expr_ast.Expr = .{ .col_ref = .{
        .col_idx = 2,
        .physical_type = .INT32,
        .expr_type = .dec,
        .dec = .{ .precision = 9, .scale = 2 },
    } };
    const c3: expr_ast.Expr = .{ .col_ref = .{
        .col_idx = 3,
        .physical_type = .FIXED_LEN_BYTE_ARRAY,
        .expr_type = .dec,
        .dec = .{ .precision = 38, .scale = 2 },
    } };
    // DECIMAL(19, 2): past 18 digits, computed in the checked 64-bit lane while its values fit one.
    const plus: expr_ast.Expr = .{ .binop = .{
        .op = .add,
        .left = &c1,
        .right = &c0,
        .result_type = .dec,
        .depth = 2,
        .dec = .{ .precision = 19, .scale = 2 },
    } };
    var c4: expr_ast.Expr = .{ .col_ref = .{
        .col_idx = 4,
        .physical_type = .INT64,
        .expr_type = .dec,
        .dec = .{ .precision = 18, .scale = 0 },
    } };
    // DECIMAL(36, 2), in the checked 64-bit lane in a window whose products fit, else in i128.
    const product: expr_ast.Expr = .{ .binop = .{
        .op = .mul,
        .left = &c0,
        .right = &c4,
        .result_type = .dec,
        .depth = 2,
        .dec = .{ .precision = 36, .scale = 2 },
    } };
    const calls = [_]AggCall{
        .{ .func = .sum, .arg = c0, .result = .dec, .where = null, .alias = "a" },
        .{ .func = .avg, .arg = c1, .result = .avg_dec, .where = null, .alias = "b" },
        .{ .func = .sum, .arg = plus, .result = .dec, .where = null, .alias = "c" },
        .{ .func = .avg, .arg = c2, .result = .avg_dec, .where = null, .alias = "d" },
        .{ .func = .sum, .arg = c3, .result = .dec, .where = null, .alias = "e" },
        .{ .func = .sum, .arg = c1, .result = .dec, .where = null, .alias = "f" },
        .{ .func = .sum, .arg = product, .result = .dec, .where = null, .alias = "g" },
        .{ .func = .count, .arg = null, .result = .i64, .where = null, .alias = "n" },
    };
    var sel = try Sel.init(ra, n);
    for (0..n) |r| if (r % 7 == 0 or r / 64 == 80) sel.set(r, false);
    var got = agg.GroupTable.init(a, 1 << 24);
    defer got.deinit();
    const gids = try ra.alloc(u32, space);
    var key: [8]u8 = undefined;
    for (gids, 0..) |*g, cp| {
        std.mem.writeInt(u64, &key, cp, .little);
        g.* = try got.getOrInsert(&key, &calls, 0);
    }
    try std.testing.expect(try foldExactBatch(ra, a, &got, gids, comp, space, &calls, &batch, &lookup, &sel));
    // Forced windows fold every root themselves, rather than leaving them to the whole batch.
    if (expr_eval.forced_batch_rows != 0) {
        var plan: expr_eval.ExactPlan = .{};
        const roots = try ra.alloc(?usize, calls.len);
        for (calls, roots) |call, *root| {
            root.* = if (call.arg) |arg| (try plan.add(ra, &batch, &lookup, arg)).? else null;
        }
        const partials = try ra.alloc(?[]agg.DecSum, plan.nodes.items.len);
        @memset(partials, null);
        foldExactWindows(ra, &plan, &calls, roots, partials, space, comp, &batch, &lookup, &sel);
        for (roots) |root| if (root) |r| try std.testing.expect(partials[r] != null);
    }
    // The per-row fold this replaced, at 192 bits, as the oracle.
    const want_sum = try ra.alloc(i192, space * calls.len);
    const want_n = try ra.alloc(u64, space * calls.len);
    @memset(want_sum, 0);
    @memset(want_n, 0);
    const want_rows = try ra.alloc(u64, space);
    @memset(want_rows, 0);
    for (0..n) |r| {
        if (!sel.isActive(r)) continue;
        want_rows[comp[r]] += 1;
        const present = defs[r] == 1;
        const row = [_]?i192{
            big[r],
            if (present) half[r] else null,
            if (present) @as(i192, half[r]) + big[r] else null,
            if (present) small[r] else null,
            huge[r],
            if (present) half[r] else null,
            @as(i192, big[r]) * times[r],
        };
        for (row, 0..) |v, ai| if (v) |x| {
            want_sum[comp[r] * calls.len + ai] += x;
            want_n[comp[r] * calls.len + ai] += 1;
        };
    }
    var sizable = false;
    for (gids, 0..) |g, cp| for (calls[0 .. calls.len - 1], 0..) |_, ai| {
        const d = switch (got.accumulators.items[g * calls.len + ai]) {
            .sum_d, .avg_d => |d| d,
            else => return error.TestUnexpectedResult,
        };
        try std.testing.expectEqual(want_sum[cp * calls.len + ai], d.get());
        try std.testing.expectEqual(want_n[cp * calls.len + ai], d.n);
        if (ai != 4 and @abs(d.get()) > std.math.maxInt(i64)) sizable = true;
    };
    for (gids, 0..) |g, cp| {
        try std.testing.expectEqual(want_rows[cp], got.accumulators.items[g * calls.len + calls.len - 1].count);
    }
    // The 64-bit lanes reach sums no 64-bit accumulator holds, and the windowed product one no i128 holds.
    try std.testing.expect(sizable);
    try std.testing.expect(for (gids) |g| {
        if (@abs(got.accumulators.items[g * calls.len + 6].sum_d.get()) > std.math.maxInt(i128)) break true;
    } else false);
}

test "exact windows fail as the whole batch does, leaving the table as it leaves it" {
    const old = expr_eval.forced_batch_rows;
    defer expr_eval.forced_batch_rows = old;
    const a = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const ra = arena.allocator();
    const n = 1000;
    const vals = try ra.alloc(i64, n);
    const late = try ra.alloc(i128, n);
    const early = try ra.alloc(i128, n);
    const shadow = try ra.alloc(f64, n);
    const comp = try ra.alloc(u32, n);
    const near38: i128 = 9 * std.math.pow(i128, 10, 37);
    for (vals, late, early, shadow, comp, 0..) |*v, *l, *e, *f, *c, r| {
        v.* = @intCast(r);
        // Doubled, each overflows 38 digits in one row: the second call's in the first window, the first's in the
        // last, so windows meet the calls' errors in the opposite order to the whole batch.
        l.* = if (r == n - 3) near38 else 1;
        e.* = if (r == 5) near38 else 1;
        f.* = 0;
        c.* = @intCast(r % 3);
    }
    const cols = [_]Column{
        .{ .i64 = .{ .values = vals, .unscaled = .{ .i64 = vals } } },
        .{ .f64 = .{ .values = shadow, .unscaled = .{ .i128 = late } } },
        .{ .f64 = .{ .values = shadow, .unscaled = .{ .i128 = early } } },
    };
    const batch: filter_eval.Batch = .{ .cols = &cols, .num_rows = n };
    const lookup = [_]?usize{ 0, 1, 2 };
    const d38: expr_ast.Dec = .{ .precision = 38, .scale = 0 };
    const c0: expr_ast.Expr = .{ .col_ref = .{
        .col_idx = 0,
        .physical_type = .INT64,
        .expr_type = .dec,
        .dec = .{ .precision = 18, .scale = 0 },
    } };
    var c1: expr_ast.Expr = .{ .col_ref = .{
        .col_idx = 1,
        .physical_type = .FIXED_LEN_BYTE_ARRAY,
        .expr_type = .dec,
        .dec = d38,
    } };
    var c2: expr_ast.Expr = .{ .col_ref = .{
        .col_idx = 2,
        .physical_type = .FIXED_LEN_BYTE_ARRAY,
        .expr_type = .dec,
        .dec = d38,
    } };
    const twice_late: expr_ast.Expr = .{ .binop = .{
        .op = .add,
        .left = &c1,
        .right = &c1,
        .result_type = .dec,
        .depth = 2,
        .dec = d38,
    } };
    const twice_early: expr_ast.Expr = .{ .binop = .{
        .op = .add,
        .left = &c2,
        .right = &c2,
        .result_type = .dec,
        .depth = 2,
        .dec = d38,
    } };
    const calls = [_]AggCall{
        .{ .func = .sum, .arg = c0, .result = .dec, .where = null, .alias = "a" },
        .{ .func = .sum, .arg = twice_late, .result = .dec, .where = null, .alias = "b" },
        .{ .func = .avg, .arg = twice_early, .result = .avg_dec, .where = null, .alias = "c" },
        .{ .func = .count, .arg = null, .result = .i64, .where = null, .alias = "n" },
    };
    const sel = try Sel.init(ra, n);
    const gids = [_]u32{ 0, 1, 2 };
    var tables: [2]agg.GroupTable = undefined;
    for (&tables, [_]usize{ 0, 64 }) |*table, rows| {
        expr_eval.forced_batch_rows = rows;
        table.* = agg.GroupTable.init(a, 1 << 24);
        for ([_][]const u8{ "a", "b", "c" }) |key| _ = try table.getOrInsert(key, &calls, 0);
        try std.testing.expectError(
            error.DecimalOverflow,
            foldExactBatch(ra, a, table, &gids, comp, 3, &calls, &batch, &lookup, &sel),
        );
    }
    defer for (&tables) |*table| table.deinit();
    try std.testing.expectEqualDeep(tables[0].accumulators.items, tables[1].accumulators.items);
    // The whole batch folds the first call before the second fails.
    try std.testing.expect(tables[1].accumulators.items[0].sum_d.n != 0);
}

test "a failed windowed pass gives its scratch back to a worker's region before the whole batch reruns" {
    const old = expr_eval.forced_batch_rows;
    defer expr_eval.forced_batch_rows = old;
    expr_eval.forced_batch_rows = 64;
    var region = huge_pages.Region.init(std.testing.allocator, 4 * huge_pages.huge_page_size);
    defer region.deinit();
    if (region.buf.len == 0) return error.SkipZigTest;
    var worker = @import("../row_group_arena.zig").RowGroupArena.initOn(&region);
    defer worker.deinit();
    var fixture = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer fixture.deinit();
    const fa = fixture.allocator();
    const n = 4096;
    const vals = try fa.alloc(i64, n);
    const wide = try fa.alloc(i128, n);
    const shadow = try fa.alloc(f64, n);
    const comp = try fa.alloc(u32, n);
    for (vals, wide, shadow, comp, 0..) |*v, *w, *f, *c, r| {
        v.* = @intCast(r);
        // Doubled, the last window's value overflows 38 digits, after every other window has folded.
        w.* = if (r == n - 1) 9 * std.math.pow(i128, 10, 37) else @intCast(r);
        f.* = 0;
        c.* = @intCast(r % 3);
    }
    const cols = [_]Column{
        .{ .i64 = .{ .values = vals, .unscaled = .{ .i64 = vals } } },
        .{ .f64 = .{ .values = shadow, .unscaled = .{ .i128 = wide } } },
    };
    const batch: filter_eval.Batch = .{ .cols = &cols, .num_rows = n };
    const lookup = [_]?usize{ 0, 1 };
    const d38: expr_ast.Dec = .{ .precision = 38, .scale = 0 };
    var c0: expr_ast.Expr = .{ .col_ref = .{
        .col_idx = 0,
        .physical_type = .INT64,
        .expr_type = .dec,
        .dec = .{ .precision = 18, .scale = 0 },
    } };
    var c1: expr_ast.Expr = .{ .col_ref = .{
        .col_idx = 1,
        .physical_type = .FIXED_LEN_BYTE_ARRAY,
        .expr_type = .dec,
        .dec = d38,
    } };
    const product: expr_ast.Expr = .{ .binop = .{
        .op = .mul,
        .left = &c0,
        .right = &c0,
        .result_type = .dec,
        .depth = 2,
        .dec = .{ .precision = 36, .scale = 0 },
    } };
    const twice: expr_ast.Expr = .{ .binop = .{
        .op = .add,
        .left = &c1,
        .right = &c1,
        .result_type = .dec,
        .depth = 2,
        .dec = d38,
    } };
    const calls = [_]AggCall{
        .{ .func = .sum, .arg = product, .result = .dec, .where = null, .alias = "a" },
        .{ .func = .sum, .arg = twice, .result = .dec, .where = null, .alias = "b" },
    };
    const ra = worker.allocator();
    var plan: expr_eval.ExactPlan = .{};
    var roots: [calls.len]?usize = undefined;
    for (calls, &roots) |call, *root| root.* = (try plan.add(ra, &batch, &lookup, call.arg.?)).?;
    const partials = try ra.alloc(?[]agg.DecSum, plan.nodes.items.len);
    @memset(partials, null);
    const sel = try Sel.init(ra, n);
    const mark = region.end;
    foldExactWindows(ra, &plan, &calls, &roots, partials, 3, comp, &batch, &lookup, &sel);
    for (partials) |p| try std.testing.expect(p == null);
    // All the pass keeps is the block its sums would have been published in, two roots over three composites.
    try std.testing.expect(region.end - mark <= 2 * 3 * @sizeOf(agg.DecSum) + @alignOf(agg.DecSum));
}

test "fused float sums match call-major row order" {
    try testFusedFloatSums(std.testing.allocator);
}

fn testFusedFloatSums(a: std.mem.Allocator) !void {
    const old = expr_eval.forced_batch_rows;
    expr_eval.forced_batch_rows = 4096;
    defer expr_eval.forced_batch_rows = old;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const ra = arena.allocator();
    const n = 32769;
    const xs = try ra.alloc(f64, n);
    const ys = try ra.alloc(f64, n);
    const comp = try ra.alloc(u32, n);
    for (xs, ys, comp, 0..) |*x, *y, *c, r| {
        x.* = switch (r % 3) {
            0 => 1e16,
            1 => 1,
            else => -1e16,
        };
        y.* = switch (r % 5) {
            0 => 1e16,
            1 => -1,
            2 => 1e16,
            3 => 0.5,
            else => -1e16,
        };
        c.* = @intCast(r % 4);
    }
    const cols = [_]Column{
        .{ .f64 = .{ .values = xs } },
        .{ .f64 = .{ .values = ys } },
    };
    const batch: filter_eval.Batch = .{ .cols = &cols, .num_rows = n };
    const lookup = [_]?usize{ 0, 1 };
    var x: expr_ast.Expr = .{ .col_ref = .{ .col_idx = 0, .physical_type = .DOUBLE, .expr_type = .f64 } };
    var y: expr_ast.Expr = .{ .col_ref = .{ .col_idx = 1, .physical_type = .DOUBLE, .expr_type = .f64 } };
    var one: expr_ast.Expr = .{ .literal = .{ .i64 = 1 } };
    const sum_xy: expr_ast.Expr = .{ .binop = .{
        .op = .add,
        .left = &x,
        .right = &y,
        .result_type = .f64,
        .depth = 2,
    } };
    const x_plus: expr_ast.Expr = .{ .binop = .{
        .op = .add,
        .left = &x,
        .right = &one,
        .result_type = .f64,
        .depth = 2,
    } };
    const calls = [_]AggCall{
        .{ .func = .sum, .arg = x, .result = .f64, .where = null, .alias = "sx" },
        .{ .func = .sum, .arg = y, .result = .f64, .where = null, .alias = "sy" },
        .{ .func = .sum, .arg = sum_xy, .result = .f64, .where = null, .alias = "sxy" },
        .{ .func = .avg, .arg = x_plus, .result = .avg_f64, .where = null, .alias = "ax" },
        .{ .func = .count, .arg = null, .result = .i64, .where = null, .alias = "n" },
    };
    var sel = try Sel.init(ra, n);
    for (0..n) |r| if (r % 11 == 0) sel.set(r, false);
    var want = agg.GroupTable.init(a, 1 << 24);
    defer want.deinit();
    var got = agg.GroupTable.init(a, 1 << 24);
    defer got.deinit();
    const keys = [_][]const u8{ "a", "b", "c", "d" };
    for (keys) |key| {
        _ = try want.getOrInsert(key, &calls, 0);
        _ = try got.getOrInsert(key, &calls, 0);
    }
    const gids = [_]u32{ 0, 1, 2, 3 };
    for (calls, 0..) |call, ai| try foldGroupedCall(
        ra,
        a,
        want.accumulators.items,
        &gids,
        comp,
        4,
        ai,
        calls.len,
        call,
        &batch,
        &lookup,
        &sel,
        &want,
    );
    fuse_probe.passes = 0;
    try std.testing.expect(try foldFloatBatches(ra, a, &got, &gids, comp, 4, &calls, &batch, &lookup, &sel));
    try std.testing.expect(fuse_probe.passes >= 2);
    try std.testing.expectEqualDeep(want.accumulators.items, got.accumulators.items);
}

// Scalar `+=` in selected-row order. Not `foldFloatPartials` and not `foldGroupedCall`.
// Sharing stands in for `-j`: L2 is split by `max_sharing`. Forced 0 is batching off, a zero L2 refuses a batch,
// and forced 64 admits one. Only the admitted path fuses two or more roots.
test "mixed float roots match a scalar oracle across batch admission" {
    const hw = @import("../hw.zig");
    defer hw.test_cpu = null;
    defer expr_eval.forced_batch_rows = null;
    const sharing = [_]usize{ 1, 3, 8, 16 };
    const modes = [_]struct { forced: ?usize, l2: usize, admit: bool }{
        .{ .forced = 0, .l2 = 1 << 20, .admit = false },
        .{ .forced = null, .l2 = 0, .admit = false },
        .{ .forced = 64, .l2 = 1 << 20, .admit = true },
    };
    var first: ?[7]u64 = null;
    var first_count: ?u64 = null;
    for (sharing) |workers| {
        for (modes) |mode| {
            expr_eval.forced_batch_rows = mode.forced;
            hw.test_cpu = .{ .l2 = .{ .bytes = mode.l2, .max_sharing = workers } };
            const got = try mixedFloatOracle(std.testing.allocator);
            try std.testing.expectEqual(got.oracle, got.bits);
            try std.testing.expectEqual(got.oracle_count, got.count);
            try std.testing.expectEqual(@as(u64, @bitCast(@as(f64, -0.0))), got.bits[5]);
            // Bare roots share one pass on every mode. A second pass is the per-batch expression fusion.
            if (mode.admit) {
                try std.testing.expect(fuse_probe.passes > 1);
            } else {
                try std.testing.expectEqual(@as(usize, 1), fuse_probe.passes);
            }
            if (first) |prev| {
                try std.testing.expectEqual(prev, got.bits);
                try std.testing.expectEqual(first_count.?, got.count);
            } else {
                first = got.bits;
                first_count = got.count;
            }
        }
    }
}

fn mixedFloatOracle(a: std.mem.Allocator) !struct { bits: [7]u64, count: u64, oracle: [7]u64, oracle_count: u64 } {
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const ra = arena.allocator();
    const n: usize = 192;
    const nan_a: f64 = @bitCast(@as(u64, 0x7ff0_0000_0000_0001));
    const nan_b: f64 = @bitCast(@as(u64, 0xfff0_0000_0000_0002));
    try std.testing.expect(@as(u64, @bitCast(nan_a)) != @as(u64, @bitCast(nan_b)));
    const keys = [_][]const u8{"g"};
    const key_codes = try ra.alloc(u32, n);
    @memset(key_codes, 0);
    const xs = try ra.alloc(f64, n);
    const ys = try ra.alloc(f64, n);
    const zs = try ra.alloc(f64, n);
    const ws = try ra.alloc(f64, n);
    for (xs, ys, zs, ws, 0..) |*x, *y, *z, *w, r| {
        x.* = switch (r % 3) {
            0 => 1e16,
            1 => 1,
            else => -1e16,
        };
        y.* = switch (r % 3) {
            0 => 0.5,
            1 => -0.25,
            else => 0.25,
        };
        z.* = -0.0;
        w.* = if (r == 1) nan_a else if (r == 2) nan_b else 0;
    }
    const cols = [_]Column{
        .{ .string = .{ .values = &.{}, .codes = key_codes, .dict = &keys } },
        .{ .f64 = .{ .values = xs } },
        .{ .f64 = .{ .values = ys } },
        .{ .f64 = .{ .values = zs } },
        .{ .f64 = .{ .values = ws } },
    };
    const lookup = [_]?usize{ 0, 1, 2, 3, 4 };
    const key: expr_ast.Expr = .{ .col_ref = .{ .col_idx = 0, .physical_type = .BYTE_ARRAY, .expr_type = .str } };
    var xref: expr_ast.Expr = .{ .col_ref = .{ .col_idx = 1, .physical_type = .DOUBLE, .expr_type = .f64 } };
    var yref: expr_ast.Expr = .{ .col_ref = .{ .col_idx = 2, .physical_type = .DOUBLE, .expr_type = .f64 } };
    const zref: expr_ast.Expr = .{ .col_ref = .{ .col_idx = 3, .physical_type = .DOUBLE, .expr_type = .f64 } };
    const wref: expr_ast.Expr = .{ .col_ref = .{ .col_idx = 4, .physical_type = .DOUBLE, .expr_type = .f64 } };
    const sum_xy: expr_ast.Expr = .{ .binop = .{
        .op = .add,
        .left = &xref,
        .right = &yref,
        .result_type = .f64,
        .depth = 2,
    } };
    const sum_xmy: expr_ast.Expr = .{ .binop = .{
        .op = .sub,
        .left = &xref,
        .right = &yref,
        .result_type = .f64,
        .depth = 2,
    } };
    const calls = [_]AggCall{
        .{ .func = .sum, .arg = xref, .result = .f64, .where = null, .alias = "sx" },
        .{ .func = .sum, .arg = yref, .result = .f64, .where = null, .alias = "sy" },
        .{ .func = .avg, .arg = xref, .result = .avg_f64, .where = null, .alias = "ax" },
        .{ .func = .sum, .arg = sum_xy, .result = .f64, .where = null, .alias = "sxy" },
        .{ .func = .sum, .arg = sum_xmy, .result = .f64, .where = null, .alias = "sxmy" },
        .{ .func = .sum, .arg = zref, .result = .f64, .where = null, .alias = "sz" },
        .{ .func = .sum, .arg = wref, .result = .f64, .where = null, .alias = "sw" },
    };
    var table = agg.GroupTable.init(a, 1 << 20);
    defer table.deinit();
    const stub_codes = [_]u32{0};
    const stub_x = [_]f64{0};
    const stub_cols = [_]Column{
        .{ .string = .{ .values = &.{}, .codes = &stub_codes, .dict = &keys } },
        .{ .f64 = .{ .values = &stub_x } },
        .{ .f64 = .{ .values = &stub_x } },
        .{ .f64 = .{ .values = &stub_x } },
        .{ .f64 = .{ .values = &stub_x } },
    };
    var stub_sel = try Sel.init(ra, 1);
    const stub_batch: filter_eval.Batch = .{ .cols = &stub_cols, .num_rows = 1 };
    try std.testing.expect(try groupedCoded(
        ra,
        a,
        &calls,
        &.{key},
        &table,
        0,
        &stub_batch,
        &lookup,
        &stub_sel,
        null,
        null,
    ));
    const seed_z: f64 = -0.0;
    table.accumulators.items[0] = .{ .sum_f = 0 };
    table.accumulators.items[1] = .{ .sum_f = 0 };
    table.accumulators.items[2] = .{ .avg = .{ .sum = 0, .count = 0 } };
    table.accumulators.items[3] = .{ .sum_f = 0 };
    table.accumulators.items[4] = .{ .sum_f = 0 };
    table.accumulators.items[5] = .{ .sum_f = seed_z };
    table.accumulators.items[6] = .{ .sum_f = 0 };
    if (comptime @import("builtin").is_test) fuse_probe.passes = 0;

    var sel = try Sel.init(ra, n);
    for (0..n) |r| if (r % 17 == 0) sel.set(r, false);
    const batch: filter_eval.Batch = .{ .cols = &cols, .num_rows = n };
    try std.testing.expect(try groupedCoded(
        ra,
        a,
        &calls,
        &.{key},
        &table,
        0,
        &batch,
        &lookup,
        &sel,
        null,
        null,
    ));

    var ox: f64 = 0;
    var oy: f64 = 0;
    var oavg: f64 = 0;
    var ocount: u64 = 0;
    var oxy: f64 = 0;
    var oxmy: f64 = 0;
    var oz: f64 = seed_z;
    var ow: f64 = 0;
    for (0..n) |r| if (sel.isActive(r)) {
        ox += xs[r];
        oy += ys[r];
        oavg += xs[r];
        ocount += 1;
        oxy += xs[r] + ys[r];
        oxmy += xs[r] - ys[r];
        oz += zs[r];
        ow += ws[r];
    };
    const acc = table.accumulators.items;
    return .{
        .bits = .{
            @bitCast(acc[0].sum_f.?),
            @bitCast(acc[1].sum_f.?),
            @bitCast(acc[2].avg.sum),
            @bitCast(acc[3].sum_f.?),
            @bitCast(acc[4].sum_f.?),
            @bitCast(acc[5].sum_f.?),
            @bitCast(acc[6].sum_f.?),
        },
        .count = acc[2].avg.count,
        .oracle = .{
            @bitCast(ox),
            @bitCast(oy),
            @bitCast(oavg),
            @bitCast(oxy),
            @bitCast(oxmy),
            @bitCast(oz),
            @bitCast(ow),
        },
        .oracle_count = ocount,
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

test "regression: aggregates over a coded column that no row reaches stay NULL, as the row folds leave them" {
    const testing = std.testing;
    const filter_ast = @import("../filter/ast.zig");
    var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_state.deinit();
    const a = arena_state.allocator();

    // 4096 rows over 8 entries, every fifth null. Selections: none, only null rows, and some present rows.
    const n = 4096;
    const fdict = [_]f64{ 0.5, -1.25, 3, 8, 1e10, -0.0, 7.5, 2 };
    const idict = [_]i64{ 5, -9, 3, 8, 100, 0, 7, 2 };
    var codes: [n]u32 = undefined;
    var defs: [n]u32 = undefined;
    var fvals: [n]f64 = undefined;
    var ivals: [n]i64 = undefined;
    for (0..n) |i| {
        defs[i] = @intFromBool(i % 5 != 2);
        codes[i] = if (defs[i] == 1) @intCast((i * 3 + i / 7) % fdict.len) else 0;
        fvals[i] = if (defs[i] == 1) fdict[codes[i]] else 0;
        ivals[i] = if (defs[i] == 1) idict[codes[i]] else 0;
    }
    var none = try Sel.init(a, n);
    @memset(none.mask, 0);
    var nulls = try Sel.init(a, n);
    for (0..n) |i| nulls.set(i, defs[i] == 0);
    var some = try Sel.init(a, n);
    for (0..n) |i| some.set(i, i % 3 == 0);
    const lookup = [_]?usize{0};

    const Lane = struct { coded: Column, plain: Column, ref: expr_ast.Expr, scalar: agg.ResultShape, never: filter_ast.Filter };
    const lanes = [_]Lane{
        .{
            .coded = .{ .f64 = .{ .values = &.{}, .codes = &codes, .dict = &fdict, .def_levels = &defs, .max_def = 1, .has_nulls = true } },
            .plain = .{ .f64 = .{ .values = &fvals, .def_levels = &defs, .max_def = 1, .has_nulls = true } },
            .ref = .{ .col_ref = .{ .col_idx = 0, .physical_type = .DOUBLE, .expr_type = .f64 } },
            .scalar = .f64,
            .never = .{ .double = .{ .col_idx = 0, .op = .Lt, .value = -1e300 } },
        },
        .{
            .coded = .{ .i64 = .{ .values = &.{}, .codes = &codes, .dict = &idict, .def_levels = &defs, .max_def = 1, .has_nulls = true } },
            .plain = .{ .i64 = .{ .values = &ivals, .def_levels = &defs, .max_def = 1, .has_nulls = true } },
            .ref = .{ .col_ref = .{ .col_idx = 0, .physical_type = .INT64, .expr_type = .i64 } },
            .scalar = .i64,
            .never = .{ .int64 = .{ .col_idx = 0, .op = .Lt, .value = -1_000_000 } },
        },
    };
    for (lanes) |lane| {
        for ([_]agg.AggFunc{ .sum, .min, .max, .avg, .count }) |func| {
            const result: agg.ResultShape = switch (func) {
                .avg => .avg_f64,
                .count => .i64,
                else => lane.scalar,
            };
            // Plain, and through a FILTER no row passes.
            for ([_]?filter_ast.Filter{ null, lane.never }) |where| {
                const call: AggCall = .{ .func = func, .arg = lane.ref, .where = where, .alias = "a", .result = result };
                for ([_]*const Sel{ &none, &nulls, &some }) |s| {
                    var want = Accumulator.init(call);
                    try agg.updateOne(a, a, &want, call, &.{ .cols = &.{lane.plain}, .num_rows = n }, &lookup, s);
                    var got = Accumulator.init(call);
                    try agg.updateOne(a, a, &got, call, &.{ .cols = &.{lane.coded}, .num_rows = n }, &lookup, s);
                    try testing.expectEqualDeep(want, got);
                }
            }
        }
        // The fuzz repro's shape: a float sum no row reaches is NULL.
        const call: AggCall = .{ .func = .sum, .arg = lane.ref, .where = null, .alias = "a", .result = lane.scalar };
        var got = Accumulator.init(call);
        try agg.updateOne(a, a, &got, call, &.{ .cols = &.{lane.coded}, .num_rows = n }, &lookup, &none);
        switch (got) {
            .sum_f => |s| try testing.expectEqual(@as(?f64, null), s),
            .sum_i => |s| try testing.expectEqual(@as(?i128, null), s),
            else => return error.TestUnexpectedResult,
        }
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

test "regression: grouped coded keys do not invent infinities for all-NaN values" {
    const testing = std.testing;
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const dict = [_][]const u8{"key"};
    const codes: [256]u32 = @splat(0);
    const vals: [256]f64 = @splat(std.math.nan(f64));
    const cols = [_]Column{
        .{ .string = .{ .values = &.{}, .codes = &codes, .dict = &dict } },
        .{ .f64 = .{ .values = &vals } },
    };
    const lookup = [_]?usize{ 0, 1 };
    const key: expr_ast.Expr = .{ .col_ref = .{ .col_idx = 0, .physical_type = .BYTE_ARRAY, .expr_type = .str } };
    const arg: expr_ast.Expr = .{ .col_ref = .{ .col_idx = 1, .physical_type = .DOUBLE, .expr_type = .f64 } };
    const calls = [_]AggCall{
        .{ .func = .min, .arg = arg, .result = .f64, .where = null, .alias = "lo" },
        .{ .func = .max, .arg = arg, .result = .f64, .where = null, .alias = "hi" },
    };
    var table = agg.GroupTable.init(testing.allocator, 1 << 20);
    defer table.deinit();
    var sel = try Sel.init(a, vals.len);
    const batch: filter_eval.Batch = .{ .cols = &cols, .num_rows = vals.len };
    try testing.expect(try groupedCoded(a, testing.allocator, &calls, &.{key}, &table, 0, &batch, &lookup, &sel, null, null));
    try testing.expect(std.math.isNan(table.accumulators.items[0].min_f.?));
    try testing.expect(std.math.isNan(table.accumulators.items[1].max_f.?));
}

test "regression: grouped float min/max over coded keys and coded values order NaN above every number" {
    const testing = std.testing;
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const keys = [_][]const u8{"key"};
    const key_codes: [256]u32 = @splat(0);
    // The values are themselves coded: row 0 holds NaN, every later row 1.0. Min skips the NaN; max is NaN.
    const vdict = [_]f64{ std.math.nan(f64), 1.0 };
    var vcodes: [256]u32 = @splat(1);
    vcodes[0] = 0;
    const cols = [_]Column{
        .{ .string = .{ .values = &.{}, .codes = &key_codes, .dict = &keys } },
        .{ .f64 = .{ .values = &.{}, .codes = &vcodes, .dict = &vdict } },
    };
    const lookup = [_]?usize{ 0, 1 };
    const key: expr_ast.Expr = .{ .col_ref = .{ .col_idx = 0, .physical_type = .BYTE_ARRAY, .expr_type = .str } };
    const arg: expr_ast.Expr = .{ .col_ref = .{ .col_idx = 1, .physical_type = .DOUBLE, .expr_type = .f64 } };
    const calls = [_]AggCall{
        .{ .func = .min, .arg = arg, .result = .f64, .where = null, .alias = "lo" },
        .{ .func = .max, .arg = arg, .result = .f64, .where = null, .alias = "hi" },
    };
    var table = agg.GroupTable.init(testing.allocator, 1 << 20);
    defer table.deinit();
    var sel = try Sel.init(a, vcodes.len);
    const batch: filter_eval.Batch = .{ .cols = &cols, .num_rows = vcodes.len };
    try testing.expect(try groupedCoded(a, testing.allocator, &calls, &.{key}, &table, 0, &batch, &lookup, &sel, null, null));
    try testing.expectEqual(@as(f64, 1.0), table.accumulators.items[0].min_f.?);
    try testing.expect(std.math.isNan(table.accumulators.items[1].max_f.?));
}

test "codePlainKeys: codes map back to the values, nulls aside, and a key with too many values stays plain" {
    var arena_state = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena_state.deinit();
    const a = arena_state.allocator();
    const vals = [_][]const u8{ "R", "", "A", "R", "N", "A", "long value past a few bytes", "A" };
    const defs = [_]u32{ 1, 0, 1, 1, 1, 1, 1, 1 }; // row 1 is null
    var cols = [_]Column{.{ .string = .{ .values = &vals, .def_levels = &defs, .max_def = 1, .has_nulls = true } }};
    const keys = [_]expr_ast.Expr{.{ .col_ref = .{ .col_idx = 0, .physical_type = .BYTE_ARRAY, .expr_type = .str } }};
    const lookup = [_]?usize{0};
    try codePlainKeys(a, &keys, &cols, &lookup);
    const sc = cols[0].string;
    for (vals, defs, sc.codes.?) |v, d, c| {
        if (d == 0) continue;
        try std.testing.expectEqualStrings(v, sc.dict[c]);
    }
    try std.testing.expectEqual(@as(usize, 4), sc.dict.len);

    // max_coded_plain + 1 distinct values: left plain.
    const many = try a.alloc([]const u8, max_coded_plain + 1);
    for (many, 0..) |*m, i| m.* = try std.fmt.allocPrint(a, "v{d}", .{i});
    var wide = [_]Column{.{ .string = .{ .values = many } }};
    try codePlainKeys(a, &keys, &wide, &lookup);
    try std.testing.expect(wide[0].string.codes == null);
}

test "codePlainKeys: a narrow integer key is coded by its offset from the least value, nulls aside" {
    var arena_state = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena_state.deinit();
    const a = arena_state.allocator();
    const keys = [_]expr_ast.Expr{.{ .col_ref = .{ .col_idx = 0, .physical_type = .INT64, .expr_type = .i64 } }};
    const lookup = [_]?usize{0};
    const vals = [_]i64{ -3, 7, 99, -3, 12, 7 };
    const defs = [_]u32{ 1, 1, 0, 1, 1, 1 }; // row 2 (99) is null and must not widen the range
    var cols = [_]Column{.{ .i64 = .{ .values = &vals, .def_levels = &defs, .max_def = 1, .has_nulls = true } }};
    try codePlainKeys(a, &keys, &cols, &lookup);
    const ic = cols[0].i64;
    try std.testing.expectEqual(@as(usize, 16), ic.dict.len); // -3 ..= 12
    for (vals, defs, ic.codes.?) |v, d, c| if (d == 1) try std.testing.expectEqual(v, ic.dict[c]);

    const wide_vals = [_]i64{ 0, max_int_range };
    var wide = [_]Column{.{ .i64 = .{ .values = &wide_vals } }};
    try codePlainKeys(a, &keys, &wide, &lookup);
    try std.testing.expect(wide[0].i64.codes == null);
}

test "a composite space past 2^16 groups by code only when its scratch is reserved, float sums in row order" {
    const testing = std.testing;
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const n_keys = 70_000;
    const n_rows = 2 * n_keys;
    const dict = try a.alloc(i64, n_keys);
    for (dict, 0..) |*d, i| d.* = @intCast(i * 3);
    const codes = try a.alloc(u32, n_rows);
    const vals = try a.alloc(f64, n_rows);
    for (codes, vals, 0..) |*c, *v, i| {
        c.* = @intCast(i % n_keys);
        v.* = 0.1 * @as(f64, @floatFromInt(i)) + 1e-9;
    }
    const cols = [_]Column{
        .{ .i64 = .{ .values = &.{}, .codes = codes, .dict = dict } },
        .{ .f64 = .{ .values = vals } },
    };
    const lookup = [_]?usize{ 0, 1 };
    const key: expr_ast.Expr = .{ .col_ref = .{ .col_idx = 0, .physical_type = .INT64, .expr_type = .i64 } };
    const arg: expr_ast.Expr = .{ .col_ref = .{ .col_idx = 1, .physical_type = .DOUBLE, .expr_type = .f64 } };
    const calls = [_]AggCall{
        .{ .func = .count, .arg = null, .result = .i64, .where = null, .alias = "n" },
        .{ .func = .sum, .arg = arg, .result = .f64, .where = null, .alias = "s" },
    };
    const batch: filter_eval.Batch = .{ .cols = &cols, .num_rows = n_rows };
    var sel = try Sel.init(a, n_rows);

    // Refused (no budget, or one too small): nothing is grouped, so the row path can run in its place.
    for ([_]?usize{ null, 1024 }) |limit| {
        var table = agg.GroupTable.init(testing.allocator, 1 << 28);
        defer table.deinit();
        var budget: DecodeBudget = .{ .limit = limit orelse 0 };
        try testing.expect(!try groupedCoded(a, testing.allocator, &calls, &.{key}, &table, 0, &batch, &lookup, &sel, if (limit != null) &budget else null, null));
        try testing.expectEqual(@as(usize, 0), table.groupCount());
        try testing.expectEqual(@as(usize, 0), budget.used.load(.monotonic));
    }

    var table = agg.GroupTable.init(testing.allocator, 1 << 28);
    defer table.deinit();
    var budget: DecodeBudget = .{ .limit = 1 << 30 };
    try testing.expect(try groupedCoded(a, testing.allocator, &calls, &.{key}, &table, 0, &batch, &lookup, &sel, &budget, null));
    // Released once grouped.
    try testing.expectEqual(@as(usize, 0), budget.used.load(.monotonic));
    try testing.expectEqual(@as(usize, n_keys), table.groupCount());
    for ([_]usize{ 0, 1, 12_345, n_keys - 1 }) |g| {
        const accs = table.accumulators.items[g * calls.len ..][0..calls.len];
        try testing.expectEqual(@as(u64, 2), accs[0].count);
        // Exactly the two values added in row order, as the row path adds them.
        try testing.expectEqual(vals[g] + vals[g + n_keys], accs[1].sum_f.?);
    }
}

test "compositeFirstRows matches a full scan, stopping early only once every reachable composite is found" {
    const testing = std.testing;
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var prng = std.Random.DefaultPrng.init(0x7f1a);
    const r = prng.random();
    for ([_]usize{ 1, 63, 64, 65, 300, 4096 }) |n| for ([_]u32{ 1, 3, 17 }) |space| {
        const comp = try a.alloc(u32, n);
        for (comp) |*c| c.* = r.uintLessThan(u32, space);
        var sel = try Sel.init(a, n);
        // Whole words kept and dropped, and words kept in part.
        for (0..n) |i| if ((i / 64) % 3 == 1 or (i % 7 == 0 and (i / 64) % 3 == 2)) sel.set(i, false);
        const want = try a.alloc(u32, space);
        @memset(want, no_row);
        var present: usize = 0;
        for (comp, 0..) |c, i| if (sel.isActive(i) and want[c] == no_row) {
            want[c] = @intCast(i);
            present += 1;
        };
        // Exactly the composites present (early stop), all of them (some absent: no stop), and the whole space.
        for ([_]usize{ present, space, space + 1 }) |reachable| {
            if (reachable < present) continue;
            try testing.expectEqualSlices(u32, want, try compositeFirstRows(a, comp, space, reachable, &sel));
        }
    };
}

test "groupedCoded: a lone null-free key groups as when a constant second key is composed with it" {
    const testing = std.testing;
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const n = 1000;
    const dict = [_]i64{ 40, 10, 30, 20 };
    const one = [_]i64{7};
    const codes = try a.alloc(u32, n);
    const zeros = try a.alloc(u32, n);
    @memset(zeros, 0);
    const vals = try a.alloc(i64, n);
    for (codes, vals, 0..) |*c, *v, i| {
        // Code 3 first appears late, so the early stop must still reach it; code 2 never does.
        c.* = if (i == 777) 3 else @intCast((i * 7 / 5) % 2);
        v.* = @intCast(i);
    }
    const cols = [_]Column{
        .{ .i64 = .{ .values = &.{}, .codes = codes, .dict = &dict } },
        .{ .i64 = .{ .values = &.{}, .codes = zeros, .dict = &one } },
        .{ .i64 = .{ .values = vals } },
    };
    const lookup = [_]?usize{ 0, 1, 2 };
    const k0: expr_ast.Expr = .{ .col_ref = .{ .col_idx = 0, .physical_type = .INT64, .expr_type = .i64 } };
    const k1: expr_ast.Expr = .{ .col_ref = .{ .col_idx = 1, .physical_type = .INT64, .expr_type = .i64 } };
    const arg: expr_ast.Expr = .{ .col_ref = .{ .col_idx = 2, .physical_type = .INT64, .expr_type = .i64 } };
    const calls = [_]AggCall{
        .{ .func = .count, .arg = null, .result = .i64, .where = null, .alias = "n" },
        .{ .func = .sum, .arg = arg, .result = .i64, .where = null, .alias = "s" },
    };
    const batch: filter_eval.Batch = .{ .cols = &cols, .num_rows = n };
    var sel = try Sel.init(a, n);
    for (0..n) |i| if (i % 5 == 0) sel.set(i, false);

    var lone = agg.GroupTable.init(testing.allocator, 1 << 20);
    defer lone.deinit();
    try testing.expect(try groupedCoded(a, testing.allocator, &calls, &.{k0}, &lone, 0, &batch, &lookup, &sel, null, null));
    var pair = agg.GroupTable.init(testing.allocator, 1 << 20);
    defer pair.deinit();
    const both = [_]expr_ast.Expr{ k0, k1 };
    try testing.expect(try groupedCoded(a, testing.allocator, &calls, &both, &pair, 0, &batch, &lookup, &sel, null, null));

    try testing.expectEqual(@as(usize, 3), lone.groupCount());
    try testing.expectEqual(lone.groupCount(), pair.groupCount());
    try testing.expectEqualSlices(u64, pair.first.items, lone.first.items);
    for (0..lone.groupCount()) |g| {
        const x = lone.accumulators.items[g * calls.len ..][0..calls.len];
        const y = pair.accumulators.items[g * calls.len ..][0..calls.len];
        try testing.expectEqual(y[0].count, x[0].count);
        try testing.expectEqual(y[1].sum_i, x[1].sum_i);
    }
}

test "groupedCoded over a key histogram with first rows makes the groups, positions and counts its codes make" {
    const testing = std.testing;
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const n = 500;
    const dict = [_][]const u8{ "d", "a", "c", "b", "never" };
    const codes = try a.alloc(u32, n);
    for (codes, 0..) |*c, i| c.* = if (i == 431) 0 else @intCast(1 + (i * 7 / 3) % 3);
    var hist: [5]u64 = @splat(0);
    var first: [5]u32 = @splat(std.math.maxInt(u32));
    for (codes, 0..) |c, i| {
        hist[c] += 1;
        if (first[c] == std.math.maxInt(u32)) first[c] = @intCast(i);
    }
    const coded = [_]Column{.{ .string = .{ .values = &.{}, .codes = codes, .dict = &dict } }};
    const histed = [_]Column{.{ .string = .{
        .values = &.{},
        .dict = &dict,
        .hist = &hist,
        .hist_first = &first,
        .levels_only_rows = n,
    } }};
    const lookup = [_]?usize{0};
    const key: expr_ast.Expr = .{ .col_ref = .{ .col_idx = 0, .physical_type = .BYTE_ARRAY, .expr_type = .str } };
    const calls = [_]AggCall{
        .{ .func = .count, .arg = null, .result = .i64, .where = null, .alias = "n" },
        .{ .func = .count, .arg = key, .result = .i64, .where = null, .alias = "nk" },
    };
    try testing.expectEqual(@as(?usize, 0), histGroupKey(&calls, &.{key}));
    const sum_call = [_]AggCall{.{ .func = .sum, .arg = key, .result = .i64, .where = null, .alias = "s" }};
    try testing.expectEqual(@as(?usize, null), histGroupKey(&sum_call, &.{key}));
    var sel = try Sel.init(a, n);
    var tables: [2]agg.GroupTable = undefined;
    for (&tables, [_][]const Column{ &coded, &histed }) |*t, cols| {
        t.* = agg.GroupTable.init(testing.allocator, 1 << 20);
        const batch: filter_eval.Batch = .{ .cols = cols, .num_rows = n };
        try testing.expect(try groupedCoded(a, testing.allocator, &calls, &.{key}, t, 7, &batch, &lookup, &sel, null, null));
    }
    defer for (&tables) |*t| t.deinit();
    try testing.expectEqual(@as(usize, 4), tables[1].groupCount());
    try testing.expectEqual(tables[0].groupCount(), tables[1].groupCount());
    try testing.expectEqualSlices(u64, tables[0].first.items, tables[1].first.items);
    for (0..tables[0].groupCount()) |g| {
        try testing.expectEqualStrings(tables[0].keyAt(g), tables[1].keyAt(g));
        for (0..calls.len) |ai| try testing.expectEqual(
            tables[0].accumulators.items[g * calls.len + ai].count,
            tables[1].accumulators.items[g * calls.len + ai].count,
        );
    }
}

test "distinct coded bitmaps fold only selected present identities across words and free failed allocations" {
    try std.testing.checkAllAllocationFailures(std.testing.allocator, distinctCodesTrial, .{false});
}

test "distinct coded sets coexist with active pairs and drain complete identities with failures" {
    try std.testing.checkAllAllocationFailures(std.testing.allocator, distinctCodesTrial, .{true});
}

fn distinctCodesTrial(a: std.mem.Allocator, active_pairs: bool) !void {
    const testing = std.testing;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const ra = arena.allocator();
    var dictionary: [129]i64 = undefined;
    var codes: [129]u32 = undefined;
    var defs: [129]u32 = undefined;
    var comp: [129]u32 = undefined;
    for (&dictionary, &codes, &defs, &comp, 0..) |*v, *c, *d, *cp, i| {
        v.* = @intCast(i);
        c.* = @intCast(i);
        d.* = @intFromBool(i % 11 != 0);
        cp.* = @intCast(i % 3);
    }
    dictionary[1] = -7;
    dictionary[64] = 63;
    const call: AggCall = .{
        .func = .count_distinct,
        .arg = .{ .col_ref = .{ .col_idx = 0, .physical_type = .INT64, .expr_type = .i64 } },
        .result = .i64,
        .where = null,
        .alias = "d",
    };
    const column: Column = .{ .i64 = .{
        .values = &.{},
        .dict = &dictionary,
        .codes = &codes,
        .def_levels = &defs,
        .max_def = 1,
        .has_nulls = true,
    } };
    const batch: filter_eval.Batch = .{ .cols = &.{column}, .num_rows = codes.len };
    for (0..3) |shape| {
        var table = agg.GroupTable.init(a, 1 << 20);
        defer table.deinit();
        var budget: agg.SharedBudget = .{ .limit = 1 << 20 };
        var pairs: agg.DistinctPairs = .{
            .gpa = a,
            .budget = &budget,
            .table = &table,
            .agg_idx = 0,
            .active = active_pairs,
            .allow_coded_sets = true,
        };
        defer pairs.deinit();
        const calls = [_]AggCall{call};
        for (0..3) |g| {
            const key = [_]u8{@intCast(g)};
            _ = try table.getOrInsert(&key, &calls, g);
        }
        var selected = try Sel.init(ra, codes.len);
        var expected: [3][137]bool = @splat(@splat(false));
        for (0..codes.len) |r| {
            const keep = shape == 0 or (shape == 1 and r % 5 == 0);
            selected.set(r, keep);
            if (keep and defs[r] != 0) expected[comp[r]][@intCast(dictionary[r] + 7)] = true;
        }
        try foldGroupedCallWithContext(
            ra,
            a,
            table.accumulators.items,
            &.{ 0, 1, 2 },
            &comp,
            3,
            0,
            1,
            call,
            &batch,
            &.{0},
            &selected,
            &table,
            .{ .distinct_pairs = &pairs },
        );
        for (expected, 0..) |bits, g| {
            var n: usize = 0;
            for (bits) |bit| n += @intFromBool(bit);
            try testing.expectEqual(n, table.readAcc(g, 0, 1).distinct.count());
        }
        codes[128] = 129;
        selected.set(128, true);
        if (foldGroupedCallWithContext(
            ra,
            a,
            table.accumulators.items,
            &.{ 0, 1, 2 },
            &comp,
            3,
            0,
            1,
            call,
            &batch,
            &.{0},
            &selected,
            &table,
            .{ .distinct_pairs = &pairs },
        )) return error.TestExpectedError else |e| {
            if (e == error.OutOfMemory) return e;
            try testing.expectEqual(error.BadColumn, e);
        }
        codes[128] = 128;
        if (active_pairs) {
            try testing.expect(pairs.pending_sets);
            try testing.expectEqual(@as(usize, 0), pairs.charged);
            var one = try Sel.init(ra, 1);
            one.set(0, true);
            try agg.updateOneGroupedValuesWithContext(
                ra,
                a,
                table.accumulators.items,
                &.{0},
                0,
                1,
                call,
                .{ .i64 = .{ .values = &.{5000} } },
                &one,
                false,
                .{ .distinct_pairs = &pairs },
            );
            try testing.expect(pairs.charged != 0);
            try pairs.drainTable(&table);
            var found: [3][137]bool = @splat(@splat(false));
            var found_plain = false;
            for (pairs.parts) |part| for (part.items) |pair| {
                const group: usize = @intCast(pair.k0);
                try testing.expect(group < 3);
                const v: i64 = @bitCast(pair.x);
                if (v == 5000) {
                    try testing.expectEqual(@as(usize, 0), group);
                    found_plain = true;
                } else {
                    try testing.expect(v >= -7 and v <= 128);
                    found[group][@intCast(v + 7)] = true;
                }
            };
            try testing.expect(found_plain);
            try testing.expectEqualDeep(expected, found);
        }
    }
}

test "seedable: distinct resolved group ids, through a bitset or, past it, a sort" {
    const a = std.testing.allocator;
    const u = unresolved;
    try std.testing.expect(try seedable(a, &.{ 3, u, 7, 1 }));
    try std.testing.expect(!try seedable(a, &.{ 3, u, 7, 3 }));
    try std.testing.expect(try seedable(a, &.{ u, 5 }));
    try std.testing.expect(try seedable(a, &.{ 64 * 4096 + 9, 2, 64 * 4096 }));
    try std.testing.expect(!try seedable(a, &.{ 64 * 4096 + 9, 2, 64 * 4096 + 9 }));
}
