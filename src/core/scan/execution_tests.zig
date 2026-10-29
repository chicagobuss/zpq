const std = @import("std");
const thrift_mod = @import("../thrift.zig");
const spawn_util = @import("../spawn.zig");
const schema = @import("../schema.zig");
const consumer = @import("../consumer.zig");
const metadata = @import("../parquet/metadata.zig");
const expr_ast = @import("../expr/ast.zig");
const expr_agg = @import("../expr/agg.zig");
const partagg = @import("../partagg.zig");
const filter_parser = @import("../filter/parser.zig");
const filter_prune = @import("../filter/prune.zig");
const Diag = @import("../diag.zig").Diag;
const worker_contracts = @import("worker_contracts.zig");
const Worker = worker_contracts.Worker;
const loadFixture = @import("test_support.zig").loadFixture;
const scan_materialize = @import("materialize.zig");
const materializeOne = scan_materialize.materializeOne;
const plan_types = @import("plan_types.zig");
const PlanInputs = plan_types.PlanInputs;
const ScanPlan = plan_types.ScanPlan;
const full_match_fixture = @import("test_support.zig").full_match_fixture;
const loadFullMatchFixture = @import("test_support.zig").loadFullMatchFixture;
const scan_plan_impl = @import("plan.zig");
const planAggregate = scan_plan_impl.planAggregate;
const applyCostRules = scan_plan_impl.applyCostRules;
const scan_execute = @import("execute.zig");
const runMultiAggregate = scan_execute.runMultiAggregate;
const explainAggregate = scan_execute.explainAggregate;
const integration_support = @import("integration_support.zig");
const SPAWN_FIXTURE_AGG = integration_support.SPAWN_FIXTURE_AGG;
const sameValue = integration_support.sameValue;
const runSketchedAndFree = integration_support.runSketchedAndFree;
const runFirstCutAndFree = integration_support.runFirstCutAndFree;
const runCensusAndFree = integration_support.runCensusAndFree;
const runDistinctCensusAndFree = integration_support.runDistinctCensusAndFree;
const nested_key_fixture = integration_support.nested_key_fixture;
const loadNestedKeyFixture = integration_support.loadNestedKeyFixture;
const freeResult = integration_support.freeResult;
const FullMatchQuery = integration_support.FullMatchQuery;
const runFullMatchQuery = integration_support.runFullMatchQuery;
const expectSameAnswer = integration_support.expectSameAnswer;
const checkFullMatchQuery = integration_support.checkFullMatchQuery;
const poisonChunks = integration_support.poisonChunks;
const unsigned_order_fixture = integration_support.unsigned_order_fixture;
const expectCountSum = integration_support.expectCountSum;
const deprecated_stats_fixture = integration_support.deprecated_stats_fixture;
const column_order_fixture = integration_support.column_order_fixture;
const nan_stats_fixture = integration_support.nan_stats_fixture;
const rewritten = integration_support.rewritten;
const expectPoisonCases = integration_support.expectPoisonCases;
const rg1Stats = integration_support.rg1Stats;
const selectList = integration_support.selectList;
const NoInPlaceResize = integration_support.NoInPlaceResize;
const scan_types = @import("types.zig");
const MultiAggArgs = scan_types.MultiAggArgs;
const SketchTopK = scan_types.SketchTopK;
const Input = scan_types.Input;
const output_values = @import("output_values.zig");
const DecValue = output_values.DecValue;
const testing = std.testing;
const test_fixtures = @import("../parquet/test_fixtures.zig");

// ============================================================
// Compile-time API check. Behavioral coverage is via the CLI
// regression suite + lambda integration tests.
// ============================================================
test "scan: API is well-typed" {
    _ = runMultiAggregate;
    _ = materializeOne;
}

// ============================================================ Partial thread-spawn failure.
//
// `Thread.spawn` can fail partway through a loop (EAGAIN); leftover work must still run and started threads must still
// be joined. Driven through the seam in spawn.zig, since real spawn failure is not reproducible.
// ============================================================


test "runMultiAggregate: injected spawn failures don't change the answer" {
    // The scan workers, unlike the footer parsers, are the loop whose old `try spawn` returned while earlier threads
    // still read the arena the caller frees. Sizing is by decode volume, so a small fixture would test nothing.
    const fixture = "data/bench_types.parquet";
    const bytes = metadata.readFileSlice(fixture, testing.allocator) catch |err| {
        if (err == error.FileNotFound) {
            std.debug.print("skipping: {s} not present\n", .{fixture});
            return error.SkipZigTest;
        }
        return err;
    };
    defer testing.allocator.free(bytes);

    const parallelism = 4;
    const inputs = [_]Input{.{ .name = fixture, .bytes = bytes }};

    const Answer = struct { sum: i128, rows: i64 };
    var baseline: ?Answer = null;
    for ([_]?usize{ null, 0, 1, parallelism - 1 }) |fail_after| {
        var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
        defer arena_state.deinit();

        if (fail_after) |n| spawn_util.injectFailureAfter(n);
        defer spawn_util.resetFailure();

        const res = try runMultiAggregate(testing.allocator, arena_state.allocator(), .{
            .inputs = &inputs,
            .aggregate = "sum(id) AS s",
            .parallelism = parallelism,
            // Force a real decode; otherwise the sum is answered from statistics and no worker ever runs.
            .scan_all = true,
        });
        defer freeResult(res);

        try testing.expectEqual(@as(usize, 1), res.output.aggs.len);
        const got: Answer = .{ .sum = res.output.aggs[0].value.i, .rows = res.rows_in };
        // Work dropped by a failed spawn and never picked up inline would show as a short row count.
        try testing.expect(got.rows > 0);

        if (baseline) |b| {
            try testing.expectEqual(b.sum, got.sum);
            try testing.expectEqual(b.rows, got.rows);
            // Prove the run took the failure path; one worker means nothing was tested.
            if (spawn_util.failuresInjected() == 0) {
                std.debug.print(
                    "spawn injection never fired (fail_after={?d}) — test is vacuous\n",
                    .{fail_after},
                );
                return error.TestUnexpectedResult;
            }
        } else {
            baseline = got;
        }
    }
}

test "partagg: same GROUP BY answer as the per-worker tables at every -j" {
    const bytes = (try loadFixture(full_match_fixture)) orelse return error.SkipZigTest;
    defer testing.allocator.free(bytes);
    const inputs = [_]Input{.{ .name = full_match_fixture, .bytes = bytes }};
    const Q = struct {
        aggregate: []const u8,
        group_by: []const u8,
        /// False: the query must stay on the per-worker tables (float merge, or count distinct).
        part: bool,
        cap: usize = 0,
        probe: u64 = 0,
    };
    const queries = [_]Q{
        .{ .aggregate = "count(*) AS c, sum(ts) AS sm, min(ts) AS mn, max(ts) AS mx, avg(ts) AS a", .group_by = "b", .part = true },
        .{ .aggregate = "count(*) AS c, min(s) AS mn, max(s) AS mx", .group_by = "b", .part = true, .cap = 40, .probe = 80 },
        .{ .aggregate = "count(*) AS c, sum(ts) AS sm", .group_by = "s", .part = true, .cap = 30, .probe = 50 },
        .{ .aggregate = "count(*) AS c, sum(ts) AS sm", .group_by = "n", .part = true, .cap = 40, .probe = 80 },
        .{ .aggregate = "sum(x) AS sx", .group_by = "b", .part = false },
        .{ .aggregate = "count(DISTINCT ts) AS d", .group_by = "b", .part = false },
    };
    for (queries) |q| for ([_]usize{ 1, 3, 8 }) |j| {
        var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
        defer arena_state.deinit();
        const base: MultiAggArgs = .{
            .inputs = &inputs,
            .aggregate = q.aggregate,
            .group_by = q.group_by,
            .parallelism = j,
            .min_bytes_per_worker = 1,
            .scan_all = true,
        };
        const want = try runMultiAggregate(testing.allocator, arena_state.allocator(), base);
        defer freeResult(want);
        var args = base;
        args.partagg = true;
        args.part_cap = q.cap;
        args.part_probe = q.probe;
        const got = try runMultiAggregate(testing.allocator, arena_state.allocator(), args);
        defer freeResult(got);
        try testing.expectEqual(q.part, got.part_agg);
        expectSameAnswer(want, got) catch |err| {
            std.debug.print("partagg -j{d} {s} GROUP BY {s}\n", .{ j, q.aggregate, q.group_by });
            return err;
        };
    };
}

test "partagg: shared partitions fit the small GROUP BY budget" {
    // Round 6 repro: one worker plus 32 shared tables. Worker-only block sizing strands quota during the flush.
    const path = "ci/fixtures/parquet/partagg_budget.parquet";
    const bytes = try metadata.readFileSlice(path, testing.allocator);
    defer testing.allocator.free(bytes);
    const inputs = [_]Input{.{ .name = path, .bytes = bytes }};
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const res = try runMultiAggregate(testing.allocator, arena.allocator(), .{
        .inputs = &inputs,
        .group_by = "k",
        .aggregate = "count(*) AS c",
        .partagg = true,
        .parallelism = 1,
        .max_memory = 256000,
    });
    defer freeResult(res);
    try testing.expect(res.part_agg);
    const rows = res.output.group_rows.?;
    try testing.expectEqual(@as(usize, 1000), rows.len);
    for (rows, 0..) |row, i| {
        try testing.expectEqual(@as(i64, @intCast(i)), row[0].i);
        try testing.expectEqual(@as(i64, 1), row[1].i);
    }
}

test "partagg: a spawn failure does not change the answer" {
    const bytes = (try loadFixture(full_match_fixture)) orelse return error.SkipZigTest;
    defer testing.allocator.free(bytes);
    const inputs = [_]Input{.{ .name = full_match_fixture, .bytes = bytes }};
    // The baseline's accumulators borrow its arena, so that arena outlives the comparisons.
    var base_arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer base_arena.deinit();
    const baseline = try runMultiAggregate(testing.allocator, base_arena.allocator(), .{
        .inputs = &inputs,
        .aggregate = "count(*) AS c, sum(ts) AS sm, min(s) AS mn",
        .group_by = "n",
        .parallelism = 4,
        .min_bytes_per_worker = 1,
        .partagg = true,
        .part_cap = 40,
        .part_probe = 80,
        .scan_all = true,
    });
    defer freeResult(baseline);
    try testing.expect(baseline.part_agg);
    for ([_]usize{ 0, 1, 3 }) |fail_after| {
        var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
        defer arena_state.deinit();
        spawn_util.injectFailureAfter(fail_after);
        defer spawn_util.resetFailure();
        const got = try runMultiAggregate(testing.allocator, arena_state.allocator(), .{
            .inputs = &inputs,
            .aggregate = "count(*) AS c, sum(ts) AS sm, min(s) AS mn",
            .group_by = "n",
            .parallelism = 4,
            .min_bytes_per_worker = 1,
            .partagg = true,
            .part_cap = 40,
            .part_probe = 80,
            .scan_all = true,
        });
        defer freeResult(got);
        expectSameAnswer(baseline, got) catch |err| {
            std.debug.print("partagg spawn fail_after={d}\n", .{fail_after});
            return err;
        };
        if (spawn_util.failuresInjected() == 0) {
            std.debug.print("partagg spawn injection never fired (fail_after={d})\n", .{fail_after});
            return error.TestUnexpectedResult;
        }
    }
}

test "runMultiAggregate: skewed work does not let idle workers strand the budget" {
    // Blocks sized as a fraction of the whole budget rather than of each worker's share let fifteen barely-grouping
    // workers strand most of the pool at -j16. Admission cannot depend on worker count when live state fits the budget.
    const heavy = "ci/fixtures/parquet/spawn_budget_a.parquet";
    const light = "ci/fixtures/parquet/spawn_budget_b.parquet";
    var bytes: [2][]u8 = undefined;
    var loaded: usize = 0;
    defer for (bytes[0..loaded]) |b| testing.allocator.free(b);
    for ([_][]const u8{ heavy, light }, 0..) |f, i| {
        bytes[i] = metadata.readFileSlice(f, testing.allocator) catch |err| {
            if (err == error.FileNotFound) {
                std.debug.print("skipping: {s} not present\n", .{f});
                return error.SkipZigTest;
            }
            return err;
        };
        loaded += 1;
    }

    var inputs: [16]Input = undefined;
    inputs[0] = .{ .name = heavy, .bytes = bytes[0] };
    for (inputs[1..]) |*in| in.* = .{ .name = light, .bytes = bytes[1] };

    // The fixed budget includes modest headroom for the surviving groups and must be enough at any -j.
    for ([_]usize{ 1, 4, 16 }) |parallelism| {
        var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
        defer arena_state.deinit();

        const res = runMultiAggregate(testing.allocator, arena_state.allocator(), .{
            .inputs = &inputs,
            .filter = "group_key < 1001",
            .aggregate = SPAWN_FIXTURE_AGG,
            .group_by = "group_key",
            .parallelism = parallelism,
            .max_memory = 710000,
            // Per-worker tables: ordered float results need room for a block's partials too (the next test).
            .unordered = true,
        }) catch |err| {
            std.debug.print(
                "-j{d}: skewed work rejected a query with 10% headroom: {s}\n",
                .{ parallelism, @errorName(err) },
            );
            return err;
        };
        defer freeResult(res);
        try testing.expectEqual(@as(usize, 1001), (res.output.group_rows orelse return error.TestUnexpectedResult).len);
    }
}

test "runMultiAggregate: --max-memory means the same thing at every -j" {
    // One integer group in the fixed table: 16 slots, the 9-byte key, and the accumulator and first arrays at
    // their first growth step. That footprint has to be enough at -j1 and, because the tables share one budget,
    // at every larger -j too.
    const one_group = 1361;
    const files = [_][]const u8{
        "ci/fixtures/parquet/spawn_budget_a.parquet",
        "ci/fixtures/parquet/spawn_budget_b.parquet",
    };
    var bytes: [files.len][]u8 = undefined;
    var loaded: usize = 0;
    defer for (bytes[0..loaded]) |b| testing.allocator.free(b);
    for (files, 0..) |f, i| {
        bytes[i] = metadata.readFileSlice(f, testing.allocator) catch |err| {
            if (err == error.FileNotFound) {
                std.debug.print("skipping: {s} not present\n", .{f});
                return error.SkipZigTest;
            }
            return err;
        };
        loaded += 1;
    }
    var inputs: [files.len]Input = undefined;
    for (files, 0..) |f, i| inputs[i] = .{ .name = f, .bytes = bytes[i] };

    for ([_]usize{ 1, 2, 4 }) |parallelism| {
        var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
        defer arena_state.deinit();

        const res = runMultiAggregate(testing.allocator, arena_state.allocator(), .{
            .inputs = &inputs,
            .filter = "group_key = 0",
            .aggregate = SPAWN_FIXTURE_AGG,
            .group_by = "group_key",
            .parallelism = parallelism,
            .max_memory = one_group,
            .unordered = true,
        }) catch |err| {
            std.debug.print(
                "-j{d}: a GROUP BY that fits {d} bytes at -j1 was rejected: {s}\n",
                .{ parallelism, one_group, @errorName(err) },
            );
            return err;
        };
        defer freeResult(res);
        try testing.expectEqual(@as(usize, 1), (res.output.group_rows orelse return error.TestUnexpectedResult).len);
    }
    // Ordered float sums keep a block's partials beside the merged sums: twice one group's room is enough at every
    // -j. Integer results merge in any order, so they need no more than unordered (the min/max query below).
    for ([_]usize{ 1, 2, 4 }) |parallelism| {
        for ([_]struct { agg: []const u8, budget: usize }{
            .{ .agg = SPAWN_FIXTURE_AGG, .budget = one_group * 2 },
            .{ .agg = "count(*) AS n, min(group_key) AS lo, max(group_key) AS hi", .budget = 800 },
        }) |c| {
            var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
            defer arena_state.deinit();
            const res = runMultiAggregate(testing.allocator, arena_state.allocator(), .{
                .inputs = &inputs,
                .filter = "group_key = 0",
                .aggregate = c.agg,
                .group_by = "group_key",
                .parallelism = parallelism,
                .max_memory = c.budget,
            }) catch |err| {
                std.debug.print("-j{d}: ordered {s} rejected at {d} bytes: {s}\n", .{
                    parallelism, c.agg[0..@min(c.agg.len, 24)], c.budget, @errorName(err),
                });
                return err;
            };
            defer freeResult(res);
            try testing.expectEqual(@as(usize, 1), (res.output.group_rows orelse return error.TestUnexpectedResult).len);
        }
    }
}



test "runMultiAggregate: a COUNT-descending cut merges only groups that can make it, and answers the same" {
    const path = "ci/fixtures/parquet/topk_skew.parquet";
    const bytes = metadata.readFileSlice(path, testing.allocator) catch |err| {
        if (err == error.FileNotFound) return error.SkipZigTest;
        return err;
    };
    defer testing.allocator.free(bytes);
    const inputs = [_]Input{.{ .name = path, .bytes = bytes }};
    const Cut = struct { order: expr_ast.ResultOrder, prunes: bool };
    const cuts = [_]Cut{
        .{ .order = .{ .keys = &.{.{ .column = 1, .descending = true }}, .limit = 10 }, .prunes = true },
        .{ .order = .{ .keys = &.{ .{ .column = 1, .descending = true }, .{ .column = 0, .descending = true } }, .limit = 3, .offset = 5 }, .prunes = true },
        .{ .order = .{ .keys = &.{.{ .column = 1, .descending = true }}, .limit = 1 }, .prunes = true },
        .{ .order = .{ .keys = &.{.{ .column = 1, .descending = true }}, .limit = 0 }, .prunes = true },
        // No ORDER BY: the first groups to appear.
        .{ .order = .{ .limit = 10 }, .prunes = true },
        .{ .order = .{ .limit = 4, .offset = 7 }, .prunes = true },
        // Not a COUNT-descending cut: merged in full.
        .{ .order = .{ .keys = &.{.{ .column = 1, .descending = false }}, .limit = 10 }, .prunes = false },
        .{ .order = .{ .keys = &.{.{ .column = 2, .descending = true }}, .limit = 10 }, .prunes = false },
        .{ .order = .{ .keys = &.{.{ .column = 1, .descending = true }} }, .prunes = false },
    };
    // `v` is unique per row: every count ties at 1, the threshold cannot prune, and the cut falls to the merge's
    // per-partition count heap (or the one-table heap), which must break the ties as the full sort does.
    for ([_][]const u8{ "k", "n", "v" }) |key| {
        for ([_]usize{ 1, 2, 4, 8 }) |parallelism| {
            for (cuts) |cut| {
                var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
                defer arena_state.deinit();
                const arena = arena_state.allocator();
                const select_cols = try selectList(arena, &.{ key, "count(*) AS c", "min(v) AS lo", "max(v) AS hi" });
                var args: MultiAggArgs = .{
                    .inputs = &inputs,
                    .aggregate = "count(*) AS c, min(v) AS lo, max(v) AS hi",
                    .group_by = key,
                    .select_cols = select_cols,
                    .parallelism = parallelism,
                };
                var whole = try runMultiAggregate(testing.allocator, arena, args);
                defer whole.deinit(testing.allocator);
                args.result_order = cut.order;
                var cut_res = try runMultiAggregate(testing.allocator, arena, args);
                defer cut_res.deinit(testing.allocator);

                const all_groups = whole.output.group_rows.?.len;
                const merged = cut_res.output.group_rows.?.len;
                // On unique keys only a cut by the COUNT alone shrinks the merge: a second key needs every tie.
                const prunes = cut.prunes and !(std.mem.eql(u8, key, "v") and cut.order.keys.len > 1);
                if (prunes) try testing.expect(merged < all_groups / 10) else try testing.expectEqual(all_groups, merged);
                try whole.output.applyResultOrder(testing.allocator, cut.order);
                try cut_res.output.applyResultOrder(testing.allocator, cut.order);
                const want = whole.output.group_rows.?;
                const got = cut_res.output.group_rows.?;
                try testing.expectEqual(want.len, got.len);
                for (want, got) |w, g| for (w, g) |a, b| try testing.expect(sameValue(a, b));
            }
        }
    }
}

test "runMultiAggregate: a sketched COUNT cut answers as the full scan, certified or not" {
    const path = "ci/fixtures/parquet/topk_skew.parquet";
    const bytes = (try loadFixture(path)) orelse return error.SkipZigTest;
    defer testing.allocator.free(bytes);
    const inputs = [_]Input{.{ .name = path, .bytes = bytes }};
    const Outcome = SketchTopK.Outcome;
    const cuts = [_]expr_ast.ResultOrder{
        .{ .keys = &.{.{ .column = 1, .descending = true }}, .limit = 3 },
        // Ties at the cut broken by the key, past an offset.
        .{ .keys = &.{ .{ .column = 1, .descending = true }, .{ .column = 0, .descending = false } }, .limit = 4, .offset = 2 },
        // Not a COUNT-descending cut: never sketched.
        .{ .keys = &.{.{ .column = 1, .descending = false }}, .limit = 3 },
    };
    const configs = [_]SketchTopK{
        .{},
        // Few cells: collisions inflate the bound.
        .{ .cell_bits = 8, .admit_per_row = 1 },
        .{ .certify = false },
        // When the cells bail: the census riding along the count pass, then dropped early by a worker's cells, then
        // counting in a pass of its own, and none.
        .{ .cell_bits = 8, .admit_per_row = 1, .fuse_census = true },
        .{ .cell_bits = 8, .admit_per_row = 1, .fuse_census = true, .census_check_rows = 64 },
        .{ .cell_bits = 8, .admit_per_row = 1, .fuse_census = false },
        .{ .cell_bits = 8, .admit_per_row = 1, .census = false },
    };
    var seen = std.EnumSet(Outcome){};
    // `k` is skewed and has nulls, `n` is a fixed-width key, `v` is unique (every count ties at 1).
    for ([_][]const u8{ "k", "n", "v", "k, n" }) |key| for ([_]?[]const u8{ null, "n < 12" }) |filter| {
        for ([_]usize{ 1, 3 }) |parallelism| for (cuts) |cut| for (configs) |cfg_in| {
            var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
            defer arena_state.deinit();
            const arena = arena_state.allocator();
            const first_key = key[0 .. std.mem.indexOfScalar(u8, key, ',') orelse key.len];
            const select_cols = try selectList(arena, &.{ first_key, "count(*) AS c", "min(v) AS lo" });
            var args: MultiAggArgs = .{
                .inputs = &inputs,
                .ndv_policy = false,
                .filter = filter,
                .aggregate = "count(*) AS c, min(v) AS lo",
                .group_by = key,
                .select_cols = if (std.mem.eql(u8, key, first_key)) select_cols else null,
                .parallelism = parallelism,
                .min_bytes_per_worker = 1,
                .result_order = cut,
            };
            var want = try runMultiAggregate(testing.allocator, arena, args);
            defer want.deinit(testing.allocator);
            var outcome: Outcome = .ineligible;
            var cfg = cfg_in;
            cfg.outcome = &outcome;
            args.sketch_topk = cfg;
            var got = try runMultiAggregate(testing.allocator, arena, args);
            defer got.deinit(testing.allocator);
            seen.insert(outcome);
            try testing.expectEqual(want.rows_in, got.rows_in);
            try testing.expectEqual(want.rows_scanned, got.rows_scanned);
            try testing.expectEqual(want.rows_matched, got.rows_matched);
            try want.output.applyResultOrder(testing.allocator, cut);
            try got.output.applyResultOrder(testing.allocator, cut);
            const w = want.output.group_rows.?;
            const g = got.output.group_rows.?;
            try testing.expectEqual(w.len, g.len);
            for (w, g) |wr, gr| for (wr, gr) |a, b| try testing.expect(sameValue(a, b));
        };
    };
    for ([_]Outcome{ .ineligible, .bailed, .certified, .fell_back, .census }) |o| try testing.expect(seen.contains(o));
}

test "runMultiAggregate: a census cut answers as the full scan, certified or not" {
    // Near-unique keys in shuffled order (tools/zpq-gen census): the cells bail, and the census decides.
    const path = "ci/fixtures/parquet/census_topk.parquet";
    const bytes = (try loadFixture(path)) orelse return error.SkipZigTest;
    defer testing.allocator.free(bytes);
    const inputs = [_]Input{.{ .name = path, .bytes = bytes }};
    const Outcome = SketchTopK.Outcome;
    const cuts = [_]expr_ast.ResultOrder{
        // `u` and `s` tie at 1 inside the cut, `t` at 3.
        .{ .keys = &.{.{ .column = 1, .descending = true }}, .limit = 10 },
        .{ .keys = &.{.{ .column = 1, .descending = true }}, .limit = 3, .offset = 2 },
        // Ties broken by the key, which first appearance does not follow.
        .{ .keys = &.{ .{ .column = 1, .descending = true }, .{ .column = 0, .descending = false } }, .limit = 10 },
        .{ .keys = &.{.{ .column = 1, .descending = true }}, .limit = 30 },
    };
    const configs = [_]SketchTopK{
        .{ .fuse_census = true },
        .{ .fuse_census = false },
        // Colliding fingerprints inflate every total: the census must not trust a cut they make up.
        .{ .fuse_census = true, .census_fp_bits = 3 },
        .{ .fuse_census = false, .census_fp_bits = 0 },
        .{ .census = false },
        // Each partition keeps only k fingerprints: the census must certify against the largest count it dropped, or
        // refuse, and collisions on top only make it refuse more.
        .{ .fuse_census = false, .admit_per_row = 1 },
        .{ .fuse_census = true, .admit_per_row = 1, .census_fp_bits = 6 },
    };
    var seen = std.EnumSet(Outcome){};
    // A count of a column, or under FILTER, can give the first keys to appear 0 (`n` is NULL through the first row
    // group): the census counts rows, an upper bound only.
    const counts = [_][]const u8{ "count(*) AS c", "count(n) AS c", "count(*) FILTER (WHERE n = 1) AS c" };
    for ([_][]const u8{ "u", "t", "s" }) |key| for ([_]?[]const u8{ null, "u > 300000000000" }) |filter| for (counts) |count| {
        for ([_]usize{ 1, 3 }) |parallelism| for (cuts) |cut| for (configs) |cfg_in| {
            var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
            defer arena_state.deinit();
            const arena = arena_state.allocator();
            const select_cols = try selectList(arena, &.{ key, count, "min(s) AS lo", "sum(t) AS st" });
            const aggregate = try std.fmt.allocPrint(arena, "{s}, min(s) AS lo, sum(t) AS st", .{count});
            var args: MultiAggArgs = .{
                .inputs = &inputs,
                .ndv_policy = false,
                .filter = if (filter != null and std.mem.eql(u8, key, "u")) filter else null,
                .aggregate = aggregate,
                .group_by = key,
                .select_cols = select_cols,
                .parallelism = parallelism,
                .min_bytes_per_worker = 1,
                .result_order = cut,
            };
            var want = try runMultiAggregate(testing.allocator, arena, args);
            defer want.deinit(testing.allocator);
            var outcome: Outcome = .ineligible;
            var cfg = cfg_in;
            cfg.outcome = &outcome;
            args.sketch_topk = cfg;
            var got = try runMultiAggregate(testing.allocator, arena, args);
            defer got.deinit(testing.allocator);
            seen.insert(outcome);
            try testing.expectEqual(want.rows_in, got.rows_in);
            try testing.expectEqual(want.rows_scanned, got.rows_scanned);
            try testing.expectEqual(want.rows_matched, got.rows_matched);
            try want.output.applyResultOrder(testing.allocator, cut);
            try got.output.applyResultOrder(testing.allocator, cut);
            const w = want.output.group_rows.?;
            const g = got.output.group_rows.?;
            try testing.expectEqual(w.len, g.len);
            for (w, g) |wr, gr| for (wr, gr) |a, b| try testing.expect(sameValue(a, b));
        };
    };
    for ([_]Outcome{ .bailed, .fell_back, .census }) |o| try testing.expect(seen.contains(o));
}

test "runMultiAggregate: a count(DISTINCT) census cut answers as the full scan, certified or not" {
    // tools/zpq-gen census. By `t` four keys hold five rows and twelve hold three, so a cut of 10 ties at 3 and
    // each key's distinct `u` is its rows; by `u` three keys hold two rows, so every distinct count ties at the
    // census bound and nothing certifies; `s` is `u` as a string, hashed whole; `n` is NULL through the first row
    // group, so keys read only there count 0 distinct.
    const path = "ci/fixtures/parquet/census_topk.parquet";
    const bytes = (try loadFixture(path)) orelse return error.SkipZigTest;
    defer testing.allocator.free(bytes);
    const inputs = [_]Input{.{ .name = path, .bytes = bytes }};
    const Outcome = SketchTopK.Outcome;
    const cuts = [_]expr_ast.ResultOrder{
        .{ .keys = &.{.{ .column = 1, .descending = true }}, .limit = 10 },
        .{ .keys = &.{.{ .column = 1, .descending = true }}, .limit = 3, .offset = 2 },
        // Ties broken by the key, which first appearance does not follow.
        .{ .keys = &.{ .{ .column = 1, .descending = true }, .{ .column = 0, .descending = false } }, .limit = 10 },
        .{ .keys = &.{.{ .column = 1, .descending = true }}, .limit = 30 },
    };
    const configs = [_]SketchTopK{
        .{},
        // Ten fingerprints admitted tie at the bound; four times as many certify.
        .{ .admit_per_row = 1 },
        // Colliding fingerprints inflate every bound, and with none left the admitted rows are most of the input.
        .{ .census_fp_bits = 3 },
        .{ .census_fp_bits = 0 },
        // Workers judge after 64 rows that the fullest cells hold too much, and abandon mid row group.
        .{ .census_check_rows = 64 },
        .{ .certify = false },
        .{ .census = false },
    };
    var seen = std.EnumSet(Outcome){};
    const distincts = [_][]const u8{ "count(DISTINCT u) AS d", "count(DISTINCT n) AS d", "count(DISTINCT t) AS d" };
    for ([_][]const u8{ "t", "u", "s" }) |key| for (distincts) |distinct| {
        if (std.mem.eql(u8, key, "t") == std.mem.eql(u8, distinct, "count(DISTINCT t) AS d")) continue;
        for ([_]usize{ 1, 3, 16 }) |parallelism| for (cuts) |cut| for (configs) |cfg_in| {
            var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
            defer arena_state.deinit();
            const arena = arena_state.allocator();
            const select_cols = try selectList(arena, &.{ key, distinct, "count(*) AS c", "min(s) AS lo" });
            const aggregate = try std.fmt.allocPrint(arena, "{s}, count(*) AS c, min(s) AS lo", .{distinct});
            var args: MultiAggArgs = .{
                .inputs = &inputs,
                .ndv_policy = false,
                .aggregate = aggregate,
                .group_by = key,
                .select_cols = select_cols,
                .parallelism = parallelism,
                .min_bytes_per_worker = 1,
                .result_order = cut,
            };
            var want = try runMultiAggregate(testing.allocator, arena, args);
            defer want.deinit(testing.allocator);
            var outcome: Outcome = .ineligible;
            var cfg = cfg_in;
            cfg.outcome = &outcome;
            args.sketch_topk = cfg;
            var got = try runMultiAggregate(testing.allocator, arena, args);
            defer got.deinit(testing.allocator);
            seen.insert(outcome);
            try testing.expectEqual(want.rows_in, got.rows_in);
            try want.output.applyResultOrder(testing.allocator, cut);
            try got.output.applyResultOrder(testing.allocator, cut);
            const w = want.output.group_rows.?;
            const g = got.output.group_rows.?;
            try testing.expectEqual(w.len, g.len);
            for (w, g) |wr, gr| for (wr, gr) |a, b| try testing.expect(sameValue(a, b));
        };
    };
    for ([_]Outcome{ .census, .fell_back, .bailed, .ineligible }) |o| try testing.expect(seen.contains(o));
}

test "runMultiAggregate: a count(DISTINCT) census never drops the error a computed argument raises" {
    // u = 368403641522 is the first row's, under a key of `t` seen once: a key the cut leaves out.
    const path = "ci/fixtures/parquet/census_topk.parquet";
    const bytes = (try loadFixture(path)) orelse return error.SkipZigTest;
    defer testing.allocator.free(bytes);
    const inputs = [_]Input{.{ .name = path, .bytes = bytes }};
    const aggregates = [_][]const u8{
        "count(DISTINCT 100 / (u - 368403641522)) AS d",
        "count(DISTINCT u) AS d, sum(100 / (u - 368403641522)) AS s",
    };
    for (aggregates) |aggregate| for ([_]usize{ 1, 3, 16 }) |parallelism| {
        var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
        defer arena_state.deinit();
        const arena = arena_state.allocator();
        const args: MultiAggArgs = .{
            .inputs = &inputs,
            .ndv_policy = false,
            .aggregate = aggregate,
            .group_by = "t",
            .parallelism = parallelism,
            .min_bytes_per_worker = 1,
            .result_order = .{ .keys = &.{.{ .column = 1, .descending = true }}, .limit = 10 },
            .sketch_topk = .{},
        };
        const ex = try explainAggregate(testing.allocator, arena, args);
        try testing.expect(!ex.plan.dispatch.distinct_census);
        try testing.expectError(error.DivisionByZero, runMultiAggregate(testing.allocator, arena, args));
    };
}

test "runMultiAggregate: count and first-keys cuts never drop the error a computed argument raises" {
    // u = 573290335422 is the last row's, under a key of `t` seen once: neither a key the count cut admits nor one in
    // the first keys' prefix.
    const path = "ci/fixtures/parquet/census_topk.parquet";
    const bytes = (try loadFixture(path)) orelse return error.SkipZigTest;
    defer testing.allocator.free(bytes);
    const inputs = [_]Input{.{ .name = path, .bytes = bytes }};
    const top: expr_ast.ResultOrder = .{ .keys = &.{.{ .column = 1, .descending = true }}, .limit = 10 };
    const Case = struct { aggregate: []const u8, order: expr_ast.ResultOrder };
    const cases = [_]Case{
        .{ .aggregate = "count(*) AS c, sum(100 / (u - 573290335422)) AS s", .order = top },
        .{ .aggregate = "sum(100 / (u - 573290335422)) AS s", .order = .{ .limit = 3 } },
    };
    for (cases) |case| for ([_]?bool{ true, false }) |fuse| for ([_]usize{ 1, 3, 16 }) |parallelism| {
        var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
        defer arena_state.deinit();
        const arena = arena_state.allocator();
        const args: MultiAggArgs = .{
            .inputs = &inputs,
            .ndv_policy = false,
            .aggregate = case.aggregate,
            .group_by = "t",
            .parallelism = parallelism,
            .min_bytes_per_worker = 1,
            .result_order = case.order,
            .sketch_topk = .{ .fuse_census = fuse },
        };
        const ex = try explainAggregate(testing.allocator, arena, args);
        try testing.expectEqual(.plain_hash, ex.plan.dispatch.strategy);
        try testing.expectError(error.DivisionByZero, runMultiAggregate(testing.allocator, arena, args));
    };
}

test "planAggregate: a count(DISTINCT) census needs a LIMIT HAVING leaves, a census that fits and a large domain" {
    const path = "ci/fixtures/parquet/census_topk.parquet";
    const bytes = (try loadFixture(path)) orelse return error.SkipZigTest;
    defer testing.allocator.free(bytes);
    const inputs = [_]Input{.{ .name = path, .bytes = bytes }};
    var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_state.deinit();
    const arena = arena_state.allocator();
    const top: expr_ast.ResultOrder = .{ .keys = &.{.{ .column = 1, .descending = true }}, .limit = 10 };
    const having: expr_ast.Having = .{ .cmp = .{ .column = 1, .op = .gt, .literal = .{ .number = "1" } } };
    var with_having = top;
    with_having.having = &having;
    const Case = struct { order: expr_ast.ResultOrder, max_memory: usize = 512 << 20, census: bool };
    const cases = [_]Case{
        .{ .order = top, .census = true },
        // The engine hands the scan HAVING's cut order (`ResultProgram.cutOrder`): no limit, so no cut.
        .{ .order = @import("../result_program.zig").ResultProgram.cutOrder(with_having), .census = false },
        // A census of every row must fit a quarter of the budget, or the scan runs as usual.
        .{ .order = top, .max_memory = 40 << 10, .census = false },
    };
    for (cases) |case| {
        const args: MultiAggArgs = .{
            .inputs = &inputs,
            .ndv_policy = false,
            .aggregate = "count(DISTINCT u) AS d",
            .group_by = "t",
            .select_cols = try selectList(arena, &.{ "t", "count(DISTINCT u) AS d" }),
            .result_order = case.order,
            .max_memory = case.max_memory,
            .sketch_topk = .{},
        };
        const ex = try explainAggregate(testing.allocator, arena, args);
        try testing.expectEqual(case.census, ex.plan.dispatch.distinct_census);
        try testing.expectEqual(case.census, ex.plan.reasons.contains(.census_distinct));
    }
    // A proven domain too small for the census to leave most rows out keeps the plain scan.
    const plan: ScanPlan = .{ .dispatch = .{
        .strategy = .sketch,
        .use_blocks = false,
        .tables = 4,
        .count_columns = true,
        .sketch_fits = true,
        .keys_unique = false,
        .filter_small = true,
        .count_agg = 0,
        .census_allowed = true,
        .census_fused = true,
        .distinct_census = true,
    } };
    const calls = [_]expr_agg.AggCall{.{ .func = .count_distinct, .alias = "d", .result = .i64, .arg = null, .where = null }};
    const in: PlanInputs = .{
        .args = .{ .inputs = &inputs, .aggregate = "", .result_order = top, .sketch_topk = .{} },
        .metas = &.{},
        .work = &.{},
        .calls = &calls,
        .keys = &.{},
        .outputs = null,
        .filter_cols = &.{},
        .workers = 4,
    };
    for ([_]u64{ 10_239, 10_240 }, [_]bool{ false, true }) |hi, kept| {
        var p = plan;
        applyCostRules(in, .{ .source = .dictionary_union, .lo = 0, .hi = hi, .rows = 1 << 24, .covered_rows = 1 << 24, .confidence = .upper_bound }, &p);
        try testing.expectEqual(kept, p.dispatch.distinct_census);
        try testing.expectEqual(kept, p.dispatch.strategy == .sketch);
    }
}

test "runMultiAggregate: behind heavy keys a census cut certifies on a short prefix, and answers as the full scan" {
    // tools/zpq-gen census: three keys hold 4500 of 8000 rows, the rest are unique, so a cut of 10 ties at 1.
    const path = "ci/fixtures/parquet/census_heavy.parquet";
    const bytes = (try loadFixture(path)) orelse return error.SkipZigTest;
    defer testing.allocator.free(bytes);
    const inputs = [_]Input{.{ .name = path, .bytes = bytes }};
    const Outcome = SketchTopK.Outcome;
    var seen = std.EnumSet(Outcome){};
    const cuts = [_]expr_ast.ResultOrder{
        .{ .keys = &.{.{ .column = 1, .descending = true }}, .limit = 10 },
        .{ .keys = &.{.{ .column = 1, .descending = true }}, .limit = 4, .offset = 3 },
        // More than the short prefix can hold: it must widen to the sure one, not certify short.
        .{ .keys = &.{.{ .column = 1, .descending = true }}, .limit = 400 },
    };
    for ([_][]const u8{ "h", "g" }) |key| for ([_]usize{ 1, 3 }) |parallelism| for (cuts) |cut| {
        for ([_]bool{ true, false }) |fuse| {
            var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
            defer arena_state.deinit();
            const arena = arena_state.allocator();
            var args: MultiAggArgs = .{
                .inputs = &inputs,
                .ndv_policy = false,
                .aggregate = "count(*) AS c, min(v) AS lo, sum(v) AS sv",
                .group_by = key,
                .select_cols = try selectList(arena, &.{ key, "count(*) AS c", "min(v) AS lo", "sum(v) AS sv" }),
                .parallelism = parallelism,
                .min_bytes_per_worker = 1,
                .result_order = cut,
            };
            var want = try runMultiAggregate(testing.allocator, arena, args);
            defer want.deinit(testing.allocator);
            var outcome: Outcome = .ineligible;
            args.sketch_topk = .{ .fuse_census = fuse, .outcome = &outcome };
            var got = try runMultiAggregate(testing.allocator, arena, args);
            defer got.deinit(testing.allocator);
            seen.insert(outcome);
            try want.output.applyResultOrder(testing.allocator, cut);
            try got.output.applyResultOrder(testing.allocator, cut);
            const w = want.output.group_rows.?;
            const g = got.output.group_rows.?;
            try testing.expectEqual(w.len, g.len);
            for (w, g) |wr, gr| for (wr, gr) |a, b| try testing.expect(sameValue(a, b));
        }
    };
    try testing.expect(seen.contains(.census_short));
    try testing.expect(seen.contains(.census));
}



test "runMultiAggregate: a sketched COUNT cut frees everything on any allocation failure" {
    // One row group, string keys that own memory, one past the 16 bytes an unframed key needs.
    const path = "ci/fixtures/parquet/json_escape.parquet";
    const bytes = (try loadFixture(path)) orelse return error.SkipZigTest;
    defer testing.allocator.free(bytes);
    var meta_arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer meta_arena.deinit();
    const meta = try metadata.open(meta_arena.allocator(), bytes);
    const inputs = [_]Input{.{ .name = path, .bytes = bytes }};
    var fixed: NoInPlaceResize = .{ .child = testing.allocator };
    for ([_]bool{ true, false }) |certify| {
        try testing.checkAllAllocationFailures(fixed.allocator(), runSketchedAndFree, .{ &inputs, &meta, certify });
    }
}

test "runMultiAggregate: a LIMIT with no ORDER BY answers as the full scan through the first-keys cut" {
    for ([_][]const u8{ "ci/fixtures/parquet/topk_skew.parquet", "ci/fixtures/parquet/census_topk.parquet" }) |path| {
        const bytes = (try loadFixture(path)) orelse return error.SkipZigTest;
        defer testing.allocator.free(bytes);
        const inputs = [_]Input{.{ .name = path, .bytes = bytes }};
        const census = std.mem.indexOf(u8, path, "census") != null;
        const keys: []const []const u8 = if (census) &.{ "u", "t", "s", "t, u" } else &.{ "k", "n", "v", "k, n" };
        const filter: []const u8 = if (census) "u > 300000000000" else "n < 12";
        for (keys) |key| for ([_]?[]const u8{ null, filter }) |f| for ([_]usize{ 1, 3 }) |parallelism| {
            // Within one row group, past it (the discovery doubles), past every key, and none.
            for ([_]expr_ast.ResultOrder{
                .{ .limit = 10 },
                .{ .limit = 4, .offset = 7 },
                .{ .limit = 1500 },
                .{ .limit = 30000 },
                .{ .limit = 0 },
            }) |cut| {
                var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
                defer arena_state.deinit();
                const arena = arena_state.allocator();
                const agg = if (census) "count(*) AS c, min(s) AS lo, sum(t) AS st" else "count(*) AS c, min(v) AS lo";
                var args: MultiAggArgs = .{
                    .inputs = &inputs,
                    .filter = f,
                    .aggregate = agg,
                    .group_by = key,
                    .parallelism = parallelism,
                    .min_bytes_per_worker = 1,
                    .result_order = cut,
                };
                var want = try runMultiAggregate(testing.allocator, arena, args);
                defer want.deinit(testing.allocator);
                args.sketch_topk = .{};
                var got = try runMultiAggregate(testing.allocator, arena, args);
                defer got.deinit(testing.allocator);
                try testing.expectEqual(want.rows_in, got.rows_in);
                try testing.expectEqual(want.rows_scanned, got.rows_scanned);
                try testing.expectEqual(want.rows_matched, got.rows_matched);
                try want.output.applyResultOrder(testing.allocator, cut);
                try got.output.applyResultOrder(testing.allocator, cut);
                const w = want.output.group_rows.?;
                const g = got.output.group_rows.?;
                try testing.expectEqual(w.len, g.len);
                for (w, g) |wr, gr| for (wr, gr) |a, b| try testing.expect(sameValue(a, b));
            }
        };
    }
}



test "runMultiAggregate: a first-keys cut frees everything on any allocation failure" {
    const path = "ci/fixtures/parquet/census_topk.parquet";
    const bytes = (try loadFixture(path)) orelse return error.SkipZigTest;
    defer testing.allocator.free(bytes);
    var meta_arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer meta_arena.deinit();
    var meta = try metadata.open(meta_arena.allocator(), bytes);
    // Three row groups of key `n`: NULL through the first, 1 after. A cut of 1 ends inside the first; one of 2 makes
    // the discovery read two, then the admit pass reads the third; one of 3 has the discovery read everything.
    meta.row_groups.items.len = 3;
    const inputs = [_]Input{.{ .name = path, .bytes = bytes }};
    var fixed: NoInPlaceResize = .{ .child = testing.allocator };
    for ([_]u64{ 1, 2, 3 }) |limit| {
        try testing.checkAllAllocationFailures(fixed.allocator(), runFirstCutAndFree, .{ &inputs, &meta, limit });
    }
}





test "runMultiAggregate: a count(DISTINCT) census cut frees everything on any allocation failure" {
    const path = "ci/fixtures/parquet/census_topk.parquet";
    const bytes = (try loadFixture(path)) orelse return error.SkipZigTest;
    defer testing.allocator.free(bytes);
    var meta_arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer meta_arena.deinit();
    var meta = try metadata.open(meta_arena.allocator(), bytes);
    meta.row_groups.items.len = 1;
    const inputs = [_]Input{.{ .name = path, .bytes = bytes }};
    var fixed: NoInPlaceResize = .{ .child = testing.allocator };
    // Certified, widened once, abandoned mid row group, and falling back after the admit passes.
    for ([_]SketchTopK{ .{}, .{ .admit_per_row = 1 }, .{ .census_check_rows = 64 }, .{ .certify = false } }) |cfg| {
        try testing.checkAllAllocationFailures(fixed.allocator(), runDistinctCensusAndFree, .{ &inputs, &meta, cfg });
    }
}

test "runMultiAggregate: a census cut frees everything on any allocation failure" {
    const path = "ci/fixtures/parquet/census_topk.parquet";
    const bytes = (try loadFixture(path)) orelse return error.SkipZigTest;
    defer testing.allocator.free(bytes);
    var meta_arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer meta_arena.deinit();
    var meta = try metadata.open(meta_arena.allocator(), bytes);
    // The first row group alone: every failure point runs the query again.
    meta.row_groups.items.len = 1;
    const inputs = [_]Input{.{ .name = path, .bytes = bytes }};
    var fixed: NoInPlaceResize = .{ .child = testing.allocator };
    // Riding along the count pass and dropped early, riding along, alone, and falling back after the admit pass.
    for ([_]SketchTopK{
        .{ .fuse_census = true, .census_check_rows = 64 },
        .{ .fuse_census = true },
        .{ .fuse_census = false },
        .{ .fuse_census = false, .certify = false },
    }) |cfg| {
        try testing.checkAllAllocationFailures(fixed.allocator(), runCensusAndFree, .{ &inputs, &meta, cfg });
    }
}

test "runMultiAggregate: a GROUP BY that fits its budget still fits when spawns fail" {
    // A spawn failure must not turn a query that fits `max_memory` into ExceededMemoryBudget. Reaching that path
    // needs four things at once:
    //
    //  1. GROUP BY, or there is no group table and no budget to get wrong.
    //  2. n_workers >= 2, hence twelve fetched columns to clear the 2 MiB-per-worker sizing threshold.
    //  3. Group keys disjoint per work item, so one worker holding the union really needs ~2x; shared keys hide it.
    //  4. A budget that fits only when the fallback worker gets the full shared allowance, not one worker's share.
    //
    // Metadata is passed in so every injected failure is necessarily a scan-worker spawn; otherwise footer-parse spawns
    // satisfy the "injection fired" check while the scan loop quietly runs single-threaded.
    //
    // Tracked fixtures, not data/ (gitignored, so a test pointed there would skip in a clean clone): regenerate with
    // tools/zpq-gen spawn_budget.
    const files = [_][]const u8{
        "ci/fixtures/parquet/spawn_budget_a.parquet",
        "ci/fixtures/parquet/spawn_budget_b.parquet",
    };
    const group_by = "group_key";
    const aggregate = SPAWN_FIXTURE_AGG;
    // A finished block stays live while it folds into the merge. Two thousand groups, with that overlap, sit a
    // little over 2 MiB once keys are a slot array instead of a per-entry charge.
    const budget = 2300 * 1024;
    const parallelism = 2;

    var bytes: [files.len][]u8 = undefined;
    var loaded: usize = 0;
    defer for (bytes[0..loaded]) |b| testing.allocator.free(b);
    for (files, 0..) |f, i| {
        bytes[i] = metadata.readFileSlice(f, testing.allocator) catch |err| {
            if (err == error.FileNotFound) {
                std.debug.print("skipping: {s} not present\n", .{f});
                return error.SkipZigTest;
            }
            return err;
        };
        loaded += 1;
    }

    var inputs: [files.len]Input = undefined;
    for (files, 0..) |f, i| inputs[i] = .{ .name = f, .bytes = bytes[i] };

    var baseline_groups: ?usize = null;
    for ([_]?usize{ null, 0, 1 }) |fail_after| {
        var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
        defer arena_state.deinit();
        const arena = arena_state.allocator();

        var metas: [files.len]schema.FileMetaData = undefined;
        for (bytes[0..], 0..) |b, i| metas[i] = try metadata.open(arena, b);

        if (fail_after) |n| spawn_util.injectFailureAfter(n);
        defer spawn_util.resetFailure();

        const res = runMultiAggregate(testing.allocator, arena, .{
            .inputs = &inputs,
            .metas = &metas,
            .aggregate = aggregate,
            .group_by = group_by,
            .parallelism = parallelism,
            .max_memory = budget,
        }) catch |err| {
            std.debug.print(
                "fail_after={?d}: a GROUP BY that fits {d} KiB was rejected: {s}\n",
                .{ fail_after, budget / 1024, @errorName(err) },
            );
            return err;
        };
        defer freeResult(res);

        const groups = (res.output.group_rows orelse return error.TestUnexpectedResult).len;
        if (baseline_groups) |b| {
            try testing.expectEqual(b, groups);
            // Metadata was supplied, so this can only be a scan-worker spawn.
            if (spawn_util.failuresInjected() == 0) {
                std.debug.print(
                    "fail_after={?d}: no scan-worker spawn was refused — test is vacuous\n",
                    .{fail_after},
                );
                return error.TestUnexpectedResult;
            }
        } else {
            baseline_groups = groups;
        }
    }
}

// ============================================================ Group-key framing.
//
// Leaf-indexed framing read the wrong schema element for every column after the first nested one (see
// `leafLabel`). The fixture puts a MAP ahead of ordinary scalars to reproduce that shape.
// ============================================================







test "group key: a scalar column after a MAP is framed by its own type" {
    // `ts` is INT64 but leaf-indexed lookup resolved a BYTE_ARRAY element, so deserialization read the timestamp bytes
    // as a string length: "index out of bounds: index 2690342917, len 9".
    const bytes = (try loadNestedKeyFixture()) orelse return error.SkipZigTest;
    defer testing.allocator.free(bytes);

    var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_state.deinit();

    const inputs = [_]Input{.{ .name = nested_key_fixture, .bytes = bytes }};
    const res = try runMultiAggregate(testing.allocator, arena_state.allocator(), .{
        .inputs = &inputs,
        .aggregate = "count(*) AS n",
        .group_by = "ts",
        .parallelism = 1,
    });
    defer freeResult(res);

    const rows = res.output.group_rows orelse return error.TestUnexpectedResult;
    try testing.expectEqual(@as(usize, 5), rows.len);
    var total: i128 = 0;
    for (rows) |r| total += r[1].i;
    try testing.expectEqual(@as(i128, 60), total);
    for (rows) |r| try testing.expect(r[0] == .i);

    // The output column name also depends on resolving the right leaf.
    const cols = res.output.group_cols orelse return error.TestUnexpectedResult;
    try testing.expectEqualStrings("ts", cols[0]);
}

test "group key: a BOOLEAN first column does not truncate the rest" {
    // BOOLEAN evaluates to the i64 lane (8 bytes) but the physical type said `boolean` and deserialization consumed 1,
    // so every later key column was read from the wrong offset: `name` came back null while counts looked fine.
    const bytes = (try loadNestedKeyFixture()) orelse return error.SkipZigTest;
    defer testing.allocator.free(bytes);

    var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_state.deinit();

    const inputs = [_]Input{.{ .name = nested_key_fixture, .bytes = bytes }};
    const res = try runMultiAggregate(testing.allocator, arena_state.allocator(), .{
        .inputs = &inputs,
        .aggregate = "count(*) AS n",
        .group_by = "flag, name",
        .parallelism = 1,
    });
    defer freeResult(res);

    const rows = res.output.group_rows orelse return error.TestUnexpectedResult;
    // 2 flags x 4 names, and every name must survive.
    try testing.expectEqual(@as(usize, 8), rows.len);
    for (rows) |r| {
        try testing.expect(r[0] == .i);
        try testing.expect(r[1] == .s);
        try testing.expect(std.mem.startsWith(u8, r[1].s, "name"));
    }
}

// ============================================================ Full-match row groups.
//
// Where statistics prove every row of a row group passes the filter, the scan neither evaluates the filter there nor
// fetches/decodes columns only the filter reads. Every answer is checked against `--scan-all`, which disables all
// statistics use and evaluates the filter on every row, so the two paths share nothing but the decoders and folds.
// Fixture layout (tools/zpq-gen full_match): 8 row groups x 200 rows, `ts` = 0..1599 sorted, so `ts >= 500`
// skips row groups 0-1, leaves 2 partial and proves 3-7.
// ============================================================











test "min-fill: a column the filter also reads under an OR decodes the pages its conjunct is proven on" {
    // ci/fixtures/parquet/min_fill_or.parquet (tools/zpq-gen min_fill): c alternates 1, 2 with no nulls, in
    // pages of 50 with a page index; b is 1 on every seventh row. Each page proves `c IS NOT NULL` (and, trusting
    // statistics, `c >= 0`); standing the page minimum in for c would drop every c = 2 row the OR keeps.
    const path = "ci/fixtures/parquet/min_fill_or.parquet";
    const bytes = metadata.readFileSlice(path, testing.allocator) catch |err| {
        if (err == error.FileNotFound) return error.SkipZigTest;
        return err;
    };
    defer testing.allocator.free(bytes);
    const inputs = [_]Input{.{ .name = path, .bytes = bytes }};
    const Case = struct { filter: []const u8, trust: bool, n: i128, sx: i128 };
    const cases = [_]Case{
        .{ .filter = "c IS NOT NULL AND (c = 2 OR b = 1)", .trust = false, .n = 572, .sx = 285784 },
        .{ .filter = "c >= 0 AND (c = 2 OR b = 1)", .trust = true, .n = 572, .sx = 285784 },
        .{ .filter = "(c = 2 OR b = 1) AND c IS NOT NULL", .trust = true, .n = 572, .sx = 285784 },
        // A column only mandatory conjuncts read may still be filled: the answer is the same either way.
        .{ .filter = "c IS NOT NULL AND b = 1", .trust = false, .n = 143, .sx = 71071 },
    };
    for (cases) |case| for ([_]usize{ 1, 3 }) |j| {
        var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
        defer arena_state.deinit();
        var r = try runMultiAggregate(testing.allocator, arena_state.allocator(), .{
            .inputs = &inputs,
            .filter = case.filter,
            .aggregate = "count(*) AS n, sum(x) AS sx",
            .parallelism = j,
            .trust_stats = case.trust,
            .min_bytes_per_worker = 1,
        });
        defer r.deinit(testing.allocator);
        testing.expectEqual(case.n, r.output.aggs[0].value.i) catch |err| {
            std.debug.print("min-fill: {s} at -j {d}\n", .{ case.filter, j });
            return err;
        };
        try testing.expectEqual(case.sx, r.output.aggs[1].value.i);
    };
}

test "SQL GROUP BY binds SELECT items to keys as bound expressions, not text" {
    const bytes = (try loadFullMatchFixture()) orelse return error.SkipZigTest;
    defer testing.allocator.free(bytes);
    const inputs = [_]Input{.{ .name = full_match_fixture, .bytes = bytes }};
    const Col = expr_ast.SelectColumn;
    const Case = struct { group_by: []const u8, select: []const Col, names: []const []const u8, groups: usize };
    const cases = [_]Case{
        // An unaliased expression key, named as written (ClickBench Q35's shape).
        .{
            .group_by = "b, b - 1",
            .select = &.{ .{ .expr = "b - 1", .role = .key }, .{ .expr = "\"b\"", .role = .key }, .{ .expr = "count(*)", .role = .agg } },
            .names = &.{ "b - 1", "b", "count(*)" },
            .groups = 16,
        },
        // A key the SELECT list names by its alias (`SELECT b * 2 AS k ... GROUP BY k`), and a constant key.
        .{
            .group_by = "k, 1",
            .select = &.{ .{ .expr = "1", .role = .key }, .{ .expr = "b * 2", .alias = "k", .role = .key }, .{ .expr = "count(*)", .alias = "n", .role = .agg } },
            .names = &.{ "1", "k", "n" },
            .groups = 16,
        },
    };
    for (cases) |case| {
        var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
        defer arena_state.deinit();
        var r = try runMultiAggregate(testing.allocator, arena_state.allocator(), .{
            .inputs = &inputs,
            .aggregate = "count(*)",
            .group_by = case.group_by,
            .select_cols = case.select,
        });
        defer r.deinit(testing.allocator);
        const cols = r.output.group_cols.?;
        try testing.expectEqual(case.names.len, cols.len);
        for (case.names, cols) |want, got| try testing.expectEqualStrings(want, got);
        try testing.expectEqual(case.groups, r.output.group_rows.?.len);
        var total: i128 = 0;
        for (r.output.group_rows.?) |row| total += row[2].i;
        try testing.expectEqual(@as(i128, 1600), total);
    }
    // `b + 1` is no key of `GROUP BY b - 1`, however alike the text.
    var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_state.deinit();
    try testing.expectError(error.ColumnMustBeGrouped, runMultiAggregate(testing.allocator, arena_state.allocator(), .{
        .inputs = &inputs,
        .aggregate = "count(*)",
        .group_by = "b - 1",
        .select_cols = &.{ .{ .expr = "b + 1", .role = .key }, .{ .expr = "count(*)", .role = .agg } },
    }));
}

test "full match: proven row groups answer like --scan-all" {
    const bytes = (try loadFullMatchFixture()) orelse return error.SkipZigTest;
    defer testing.allocator.free(bytes);
    const inputs = [_]Input{.{ .name = full_match_fixture, .bytes = bytes }};

    const queries = [_]FullMatchQuery{
        // count(*) alone: proven row groups need no column at all.
        .{ .filter = "ts >= 500", .aggregate = "count(*) AS n", .full_match = 5 },
        .{ .filter = "ts >= 500", .aggregate = "count(*) AS n, sum(x) AS sx, min(x) AS mn", .full_match = 5 },
        // The filter column is also an aggregate argument / per-agg FILTER input, so it must still be decoded.
        .{ .filter = "ts >= 500", .aggregate = "sum(ts) AS st, max(ts) AS mt, count(*) AS n", .full_match = 5 },
        .{
            .filter = "ts >= 500",
            .aggregate = "count(*) AS n, sum(x) FILTER (WHERE ts < 1000) AS sx",
            .full_match = 5,
        },
        .{ .filter = "ts >= 500", .aggregate = "avg(x) AS a, count(x) AS cx", .full_match = 5 },
        // Strings: a min-side proof needs no exactness flag; a max-side one relies on is_max_value_exact.
        .{ .filter = "s >= 'k000500'", .aggregate = "count(*) AS n, sum(x) AS sx", .full_match = 5 },
        .{ .filter = "s < 'k000500'", .aggregate = "count(*) AS n, sum(x) AS sx", .full_match = 2 },
        // Nullable: group 1 is all null (skip), 0/2/3 have nulls (partial), 4-7 have none (proven).
        .{ .filter = "n IS NOT NULL", .aggregate = "count(*) AS c, sum(x) AS sx, count(n) AS cn", .full_match = 4 },
        .{ .filter = "n IS NULL", .aggregate = "count(*) AS c, sum(x) AS sx", .full_match = 1 },
        .{ .filter = "n >= 0", .aggregate = "count(*) AS c, sum(x) AS sx", .full_match = 4 },
        .{ .filter = "ts >= 500 AND n IS NOT NULL", .aggregate = "count(*) AS c, sum(n) AS sn", .full_match = 4 },
        .{ .filter = "ts >= 1400 OR x > 1000", .aggregate = "count(*) AS c, sum(x) AS sx", .full_match = 1 },
        .{ .filter = "NOT ts < 500", .aggregate = "count(*) AS c, sum(x) AS sx", .full_match = 5 },
        // Floats: NaN could hide outside [min, max].
        .{ .filter = "x >= -3", .aggregate = "count(*) AS n", .full_match = 0 },
    };
    for (queries) |q| {
        const res = try checkFullMatchQuery(&inputs, q);
        freeResult(res);
    }

    // Hand-known values, so agreement with --scan-all is not agreement on a shared mistake.
    const res = try checkFullMatchQuery(&inputs, queries[2]);
    defer freeResult(res);
    try testing.expectEqual(@as(i128, (500 + 1599) * 1100 / 2), res.output.aggs[0].value.i);
    try testing.expectEqual(@as(i128, 1599), res.output.aggs[1].value.i);
    try testing.expectEqual(@as(i128, 1100), res.output.aggs[2].value.i);
    try testing.expectEqual(@as(usize, 2), res.row_groups_pruned);
}

test "a file with no row groups aggregates to the empty answer" {
    // A valid empty table (pyarrow's ParquetWriter closed without writing). Splitting aggregates across workers
    // divided by the row-group count, and the leaf count was read from row group 0.
    const bytes = try metadata.readFileSlice("ci/fixtures/parquet/no_row_groups.parquet", testing.allocator);
    defer testing.allocator.free(bytes);
    var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_state.deinit();
    const inputs = [_]Input{.{ .name = "no_row_groups.parquet", .bytes = bytes }};
    for ([_]usize{ 1, 4 }) |parallelism| {
        const res = try runMultiAggregate(testing.allocator, arena_state.allocator(), .{
            .inputs = &inputs,
            .filter = "a > 1",
            .aggregate = "count(*) AS n",
            .parallelism = parallelism,
        });
        defer freeResult(res);
        try testing.expectEqual(@as(i128, 0), res.output.aggs[0].value.i);
    }
}

test "full match: per-row-group decisions on a real footer" {
    // Unsigned columns prune in unsigned order but are never proven fully matching.
    const bytes = (try loadFullMatchFixture()) orelse return error.SkipZigTest;
    defer testing.allocator.free(bytes);
    var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_state.deinit();
    const arena = arena_state.allocator();
    const meta = try metadata.open(arena, bytes);

    // pyarrow declares TYPE_DEFINED_ORDER for every leaf and marks untruncated string bounds exact.
    const orders = meta.column_orders orelse return error.TestUnexpectedResult;
    try testing.expectEqual(@as(usize, 6), orders.items.len);
    for (orders.items) |o| try testing.expectEqual(schema.COLUMN_ORDER_TYPE_DEFINED, o);
    const s_stats = meta.row_groups.items[0].columns.items[2].meta_data.?.statistics.?;
    try testing.expectEqual(@as(?bool, true), s_stats.is_max_value_exact);
    try testing.expectEqual(@as(?bool, true), s_stats.is_min_value_exact);

    const D = filter_prune.Decision;
    const all: D = .always_match;
    const cases = [_]struct { filter: []const u8, want: [8]D }{
        .{ .filter = "ts >= 600", .want = .{ .skip, .skip, .skip, all, all, all, all, all } },
        .{ .filter = "u >= 600", .want = .{ .skip, .skip, .skip, .keep, .keep, .keep, .keep, .keep } },
        .{ .filter = "x >= -3", .want = .{ .keep, .keep, .keep, .keep, .keep, .keep, .keep, .keep } },
        .{ .filter = "n IS NOT NULL", .want = .{ .keep, .skip, .keep, .keep, all, all, all, all } },
    };
    for (cases) |c| {
        const f = try filter_parser.parse(arena, c.filter, &meta, null);
        for (meta.row_groups.items, c.want, 0..) |*rg, want, i| {
            const got = filter_prune.pruneRowGroup(rg, f, &meta);
            testing.expectEqual(want, got) catch |err| {
                std.debug.print("{s}: row group {d} decided {s}, want {s}\n", .{
                    c.filter, i, @tagName(got), @tagName(want),
                });
                return err;
            };
        }
    }
}

test "full match: GROUP BY on the filter column" {
    const bytes = (try loadFullMatchFixture()) orelse return error.SkipZigTest;
    defer testing.allocator.free(bytes);
    const inputs = [_]Input{.{ .name = full_match_fixture, .bytes = bytes }};

    // b = ts / 100: group 2 holds b = 4, 5 (partial); groups 3-7 hold b = 6..15 (proven).
    const res = try checkFullMatchQuery(&inputs, .{
        .filter = "b >= 5",
        .aggregate = "count(*) AS n, sum(x) AS sx",
        .group_by = "b",
        .full_match = 5,
    });
    defer freeResult(res);
    const rows = res.output.group_rows orelse return error.TestUnexpectedResult;
    try testing.expectEqual(@as(usize, 11), rows.len);
    for (rows) |r| try testing.expectEqual(@as(i128, 100), r[1].i);

    const keyed = try checkFullMatchQuery(&inputs, .{
        .filter = "ts >= 500",
        .aggregate = "count(*) AS n, sum(x) AS sx",
        .group_by = "b",
        .full_match = 5,
    });
    freeResult(keyed);
}

test "full match: several files keep their own decisions" {
    const bytes = (try loadFullMatchFixture()) orelse return error.SkipZigTest;
    defer testing.allocator.free(bytes);
    const inputs = [_]Input{
        .{ .name = full_match_fixture, .bytes = bytes },
        .{ .name = full_match_fixture, .bytes = bytes },
        .{ .name = full_match_fixture, .bytes = bytes },
    };
    const res = try checkFullMatchQuery(&inputs, .{
        .filter = "ts >= 500",
        .aggregate = "count(*) AS n, sum(ts) AS st",
        .full_match = 15,
    });
    defer freeResult(res);
    try testing.expectEqual(@as(i128, 3 * 1100), res.output.aggs[0].value.i);
}



test "full match: filter-only chunks of proven row groups are never read" {
    const clean = (try loadFullMatchFixture()) orelse return error.SkipZigTest;
    defer testing.allocator.free(clean);

    var meta_arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer meta_arena.deinit();
    const meta = try metadata.open(meta_arena.allocator(), clean);

    const cases = [_]struct { col: usize, rgs: []const usize, q: FullMatchQuery }{
        .{ .col = 0, .rgs = &.{ 3, 4, 5, 6, 7 }, .q = .{
            .filter = "ts >= 500",
            .aggregate = "count(*) AS n, sum(x) AS sx",
            .full_match = 5,
        } },
        .{ .col = 2, .rgs = &.{ 3, 4, 5, 6, 7 }, .q = .{
            .filter = "s >= 'k000500'",
            .aggregate = "count(*) AS n, sum(x) AS sx",
            .full_match = 5,
        } },
        .{ .col = 4, .rgs = &.{ 4, 5, 6, 7 }, .q = .{
            .filter = "n IS NOT NULL",
            .aggregate = "count(*) AS n, sum(x) AS sx",
            .full_match = 4,
        } },
    };
    for (cases) |c| {
        const poisoned = try testing.allocator.dupe(u8, clean);
        defer testing.allocator.free(poisoned);
        poisonChunks(poisoned, &meta, c.col, c.rgs);
        const poisoned_in = [_]Input{.{ .name = "poisoned", .bytes = poisoned }};

        var want_arena = std.heap.ArenaAllocator.init(testing.allocator);
        defer want_arena.deinit();
        const clean_in = [_]Input{.{ .name = "clean", .bytes = clean }};
        const want = try runFullMatchQuery(want_arena.allocator(), &clean_in, c.q, 1, false);
        defer freeResult(want);

        for ([_]usize{ 1, 16 }) |parallelism| {
            var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
            defer arena_state.deinit();
            const got = try runFullMatchQuery(arena_state.allocator(), &poisoned_in, c.q, parallelism, false);
            defer freeResult(got);
            try expectSameAnswer(want, got);
            try testing.expectEqual(c.q.full_match, got.row_groups_full_match);
        }

        // Control: the poison is detectable once the column is read, or the checks above prove nothing.
        var ctl_arena = std.heap.ArenaAllocator.init(testing.allocator);
        defer ctl_arena.deinit();
        var ctl_q = c.q;
        ctl_q.aggregate = "count(*) AS n";
        if (runFullMatchQuery(ctl_arena.allocator(), &poisoned_in, ctl_q, 1, true)) |r| {
            freeResult(r);
            std.debug.print("full match: poisoned column {d} decoded cleanly under --scan-all\n", .{c.col});
            return error.TestUnexpectedResult;
        } else |_| {}
    }
}

// ------------------------------------------------------------
// Statistics order: filters and pruning on columns whose order is not the signed one their bytes suggest.
// Fixtures from tools/zpq-gen stats_order; expected answers are hand-computed from the generator's formulas.
// ------------------------------------------------------------







test "unsigned columns filter and prune in unsigned order" {
    try expectCountSum(unsigned_order_fixture, &.{
        .{ .filter = "u32 >= 2147483648", .n = 640, .sum_i = 450240 },
        .{ .filter = "u32 = 2147483648", .n = 1, .sum_i = 384 },
        .{ .filter = "u32 < 2147483648", .n = 384, .sum_i = 73536 },
        .{ .filter = "u32 != 3000000000", .n = 1023, .sum_i = 523264 },
        .{ .filter = "u32 IN (0, 4294967295)", .n = 2, .sum_i = 1023 },
        .{ .filter = "u64 > 9223372036854775807", .n = 640, .sum_i = 450240 },
        .{ .filter = "u64 = 18446744073709551615", .n = 1, .sum_i = 1023 },
        .{ .filter = "u8 >= 128", .n = 512, .sum_i = 294656 },
        .{ .filter = "u16 BETWEEN 32768 AND 65535", .n = 512, .sum_i = 293888 },
        .{ .filter = "n32 > 2147483647", .n = 256, .sum_i = 147456 },
        // Literals outside the column's range have a constant answer.
        .{ .filter = "u32 > -1", .n = 1024, .sum_i = 523776 },
        .{ .filter = "u32 = -5", .n = 0, .sum_i = 0 },
        .{ .filter = "u64 < 18446744073709551616", .n = 1024, .sum_i = 523776 },
        .{ .filter = "NOT u64 <= 18446744073709551615", .n = 0, .sum_i = 0 },
    });
}

test "unsigned columns: row-group decisions read bounds unsigned" {
    const bytes = (try loadFixture(unsigned_order_fixture)) orelse return error.SkipZigTest;
    defer testing.allocator.free(bytes);
    var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_state.deinit();
    const arena = arena_state.allocator();
    const meta = try metadata.open(arena, bytes);

    const D = filter_prune.Decision;
    const cases = [_]struct { filter: []const u8, want: [4]D }{
        .{ .filter = "u32 >= 3000000000", .want = .{ .skip, .skip, .keep, .keep } },
        .{ .filter = "u32 < 256", .want = .{ .keep, .skip, .skip, .skip } },
        .{ .filter = "u64 >= 18446744073709551360", .want = .{ .skip, .skip, .skip, .keep } },
        .{ .filter = "u64 = 9223372036854775808", .want = .{ .skip, .keep, .skip, .skip } },
        .{ .filter = "n32 IS NULL", .want = .{ .keep, .keep, .keep, .always_match } },
    };
    for (cases) |c| {
        const f = try filter_parser.parse(arena, c.filter, &meta, null);
        for (meta.row_groups.items, c.want, 0..) |*rg, want, i| {
            const got = filter_prune.pruneRowGroup(rg, f, &meta);
            testing.expectEqual(want, got) catch |err| {
                std.debug.print("{s}: row group {d} decided {s}, want {s}\n", .{
                    c.filter, i, @tagName(got), @tagName(want),
                });
                return err;
            };
        }
    }
}



test "deprecated min/max prune only where their signed order is the column's order" {
    try expectCountSum(deprecated_stats_fixture, &.{
        // Byte arrays, DECIMAL and unsigned: the signed pair excludes a stored value from its own row group.
        .{ .filter = "s = 'a'", .n = 1, .sum_i = 3 },
        .{ .filter = "s < 'b'", .n = 1, .sum_i = 3 },
        .{ .filter = "dec = 1.00", .n = 1, .sum_i = 1 },
        .{ .filter = "dec < 1.28", .n = 1, .sum_i = 1 },
        .{ .filter = "u = 1", .n = 1, .sum_i = 1 },
        .{ .filter = "u = 3000000000", .n = 1, .sum_i = 3 },
        // Signed integers and doubles: the pair is in their order and still prunes.
        .{ .filter = "i = 2", .n = 1, .sum_i = 2 },
        .{ .filter = "d >= 2.5", .n = 4, .sum_i = 309 },
        .{ .filter = "idec = 1.28", .n = 1, .sum_i = 2 },
    });

    const bytes = (try loadFixture(deprecated_stats_fixture)) orelse return error.SkipZigTest;
    defer testing.allocator.free(bytes);
    var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_state.deinit();
    const arena = arena_state.allocator();
    const meta = try metadata.open(arena, bytes);
    const D = filter_prune.Decision;
    const cases = [_]struct { filter: []const u8, want: [2]D }{
        .{ .filter = "s = 'a'", .want = .{ .unknown, .unknown } },
        .{ .filter = "dec = 6.00", .want = .{ .unknown, .unknown } },
        .{ .filter = "u = 11", .want = .{ .unknown, .unknown } },
        .{ .filter = "i > 100", .want = .{ .skip, .keep } },
        .{ .filter = "d < 1", .want = .{ .keep, .skip } },
        // DECIMAL over INT32 compares as its signed unscaled integer: the pair is in numeric order.
        .{ .filter = "idec > 6.50", .want = .{ .skip, .keep } },
        .{ .filter = "idec < 3", .want = .{ .keep, .skip } },
    };
    for (cases) |c| {
        const f = try filter_parser.parse(arena, c.filter, &meta, null);
        for (meta.row_groups.items, c.want, 0..) |*rg, want, i| {
            const got = filter_prune.pruneRowGroup(rg, f, &meta);
            testing.expectEqual(want, got) catch |err| {
                std.debug.print("{s}: row group {d} decided {s}, want {s}\n", .{
                    c.filter, i, @tagName(got), @tagName(want),
                });
                return err;
            };
        }
    }
}

test "deprecated min/max never answer a DECIMAL aggregate under --trust-stats" {
    const bytes = (try loadFixture(deprecated_stats_fixture)) orelse return error.SkipZigTest;
    defer testing.allocator.free(bytes);
    const inputs = [_]Input{.{ .name = deprecated_stats_fixture, .bytes = bytes }};
    var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_state.deinit();
    const res = try runMultiAggregate(testing.allocator, arena_state.allocator(), .{
        .inputs = &inputs,
        .aggregate = "min(dec) AS lo, max(dec) AS hi, max(i) AS mi",
        .parallelism = 1,
        .trust_stats = true,
    });
    defer freeResult(res);
    // DECIMAL(9,2): exact unscaled values.
    const d92: expr_ast.Dec = .{ .precision = 9, .scale = 2 };
    try testing.expectEqual(DecValue{ .v = 100, .spec = d92 }, res.output.aggs[0].value.dec);
    try testing.expectEqual(DecValue{ .v = 700, .spec = d92 }, res.output.aggs[1].value.dec);
    try testing.expectEqual(@as(i128, 103), res.output.aggs[2].value.i);
}



test "bounds in a declared column order zpq does not implement never prune" {
    // `s` declares an unknown order whose bounds, read bytewise, exclude 'B' and 'Y' from their row groups and pages.
    try expectCountSum(column_order_fixture, &.{
        .{ .filter = "s = 'B'", .n = 1, .sum_i = 1 },
        .{ .filter = "s = 'Y'", .n = 1, .sum_i = 102 },
        .{ .filter = "s < 'a'", .n = 2, .sum_i = 103 },
        .{ .filter = "s >= 'a'", .n = 4, .sum_i = 209 },
        .{ .filter = "i > 100", .n = 3, .sum_i = 306 },
    });

    const bytes = (try loadFixture(column_order_fixture)) orelse return error.SkipZigTest;
    defer testing.allocator.free(bytes);
    var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_state.deinit();
    const arena = arena_state.allocator();
    const meta = try metadata.open(arena, bytes);
    const D = filter_prune.Decision;
    const cases = [_]struct { filter: []const u8, want: [2]D }{
        .{ .filter = "s = 'B'", .want = .{ .unknown, .unknown } },
        .{ .filter = "s >= 'zz'", .want = .{ .unknown, .unknown } },
        // `i` keeps TYPE_DEFINED_ORDER.
        .{ .filter = "i > 100", .want = .{ .skip, .always_match } },
    };
    for (cases) |c| {
        const f = try filter_parser.parse(arena, c.filter, &meta, null);
        for (meta.row_groups.items, c.want, 0..) |*rg, want, i| {
            const got = filter_prune.pruneRowGroup(rg, f, &meta);
            testing.expectEqual(want, got) catch |err| {
                std.debug.print("{s}: row group {d} decided {s}, want {s}\n", .{
                    c.filter, i, @tagName(got), @tagName(want),
                });
                return err;
            };
        }
    }
}



test "!= on a float chunk whose bounds are the literal skips only when nan_count proves no NaN" {
    // Every row group records [-0, 0]; row groups 0 and 2 hold a NaN, which `!= 0` keeps.
    try expectCountSum(nan_stats_fixture, &.{
        .{ .filter = "d != 0", .n = 2, .sum_i = 8 },
        .{ .filter = "NOT d = 0", .n = 2, .sum_i = 8 },
        .{ .filter = "d = 0", .n = 7, .sum_i = 28 },
    });

    const bytes = (try loadFixture(nan_stats_fixture)) orelse return error.SkipZigTest;
    defer testing.allocator.free(bytes);
    var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_state.deinit();
    const arena = arena_state.allocator();
    const meta = try metadata.open(arena, bytes);
    const rgs = meta.row_groups.items;
    try testing.expectEqual(@as(?i64, null), rgs[0].columns.items[0].meta_data.?.statistics.?.nan_count);
    try testing.expectEqual(@as(?i64, 0), rgs[1].columns.items[0].meta_data.?.statistics.?.nan_count);
    const f = try filter_parser.parse(arena, "d != 0", &meta, null);
    const D = filter_prune.Decision;
    for (meta.row_groups.items, [_]D{ .keep, .skip, .keep }, 0..) |*rg, want, i| {
        const got = filter_prune.pruneRowGroup(rg, f, &meta);
        testing.expectEqual(want, got) catch |err| {
            std.debug.print("d != 0: row group {d} decided {s}, want {s}\n", .{ i, @tagName(got), @tagName(want) });
            return err;
        };
    }
}

test "clashing output column names are rejected and named" {
    const bytes = (try loadFullMatchFixture()) orelse return error.SkipZigTest;
    defer testing.allocator.free(bytes);
    const inputs = [_]Input{.{ .name = full_match_fixture, .bytes = bytes }};

    const Case = struct {
        group_by: ?[]const u8 = null,
        aggregate: []const u8,
        select_cols: ?[]const []const u8 = null,
        want: anyerror,
        column: []const u8,
    };
    const cases = [_]Case{
        // A key renamed away from its source column and an aggregate named after it: `s` used to show the key.
        .{ .group_by = "s AS k", .aggregate = "sum(x) AS s", .want = error.AmbiguousOutputColumn, .column = "s" },
        .{ .group_by = "b", .aggregate = "count(*) AS b", .want = error.AmbiguousOutputColumn, .column = "b" },
        .{
            .group_by = "b",
            .aggregate = "sum(x) AS t, min(x) AS T",
            .want = error.DuplicateOutputColumn,
            .column = "T",
        },
        .{ .aggregate = "sum(x) AS t, min(x) AS t", .want = error.DuplicateOutputColumn, .column = "t" },
        .{
            .group_by = "b AS k, ts AS k",
            .aggregate = "count(*) AS c",
            .want = error.DuplicateOutputColumn,
            .column = "k",
        },
        // The SQL front end's select list: `SELECT s AS k, sum(x) AS s ... GROUP BY s`.
        .{
            .group_by = "s",
            .aggregate = "sum(x) AS s",
            .select_cols = &.{ "s AS k", "sum(x) AS s" },
            .want = error.AmbiguousOutputColumn,
            .column = "s",
        },
    };
    for (cases) |c| {
        var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
        defer arena_state.deinit();
        var diag: Diag = .{};
        const res = runMultiAggregate(testing.allocator, arena_state.allocator(), .{
            .inputs = &inputs,
            .aggregate = c.aggregate,
            .group_by = c.group_by,
            .select_cols = try selectList(arena_state.allocator(), c.select_cols),
            .parallelism = 1,
            .diag = &diag,
        });
        try testing.expectError(c.want, res);
        try testing.expectEqualStrings(c.column, diag.column.get());
    }
}

test "unaliased aggregates keep the function name unless it clashes, then take the call text" {
    const bytes = (try loadFullMatchFixture()) orelse return error.SkipZigTest;
    defer testing.allocator.free(bytes);
    const inputs = [_]Input{.{ .name = full_match_fixture, .bytes = bytes }};

    const Case = struct {
        group_by: ?[]const u8 = null,
        aggregate: []const u8,
        select_cols: ?[]const []const u8 = null,
        want: []const []const u8,
    };
    const cases = [_]Case{
        .{ .aggregate = "sum(x)", .want = &.{"sum"} },
        .{ .aggregate = "sum(x), sum(ts), count(*)", .want = &.{ "sum(x)", "sum(ts)", "count" } },
        .{ .aggregate = "count(*) AS count, count(n)", .want = &.{ "count", "count(n)" } },
        .{
            .aggregate = "sum(x) FILTER (WHERE ts > 5), sum(x)",
            .want = &.{ "sum(x) FILTER (WHERE ts > 5)", "sum(x)" },
        },
        .{ .group_by = "b", .aggregate = "min(x), min(ts)", .want = &.{ "b", "min(x)", "min(ts)" } },
        // The SQL front end lists unaliased aggregates by call text, which used to resolve to nothing.
        .{
            .group_by = "b",
            .aggregate = "count(*), sum(x)",
            .select_cols = &.{ "b", "count(*)", "sum(x)" },
            .want = &.{ "b", "count(*)", "sum(x)" },
        },
    };
    for (cases) |c| {
        var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
        defer arena_state.deinit();
        const res = try runMultiAggregate(testing.allocator, arena_state.allocator(), .{
            .inputs = &inputs,
            .aggregate = c.aggregate,
            .group_by = c.group_by,
            .select_cols = try selectList(arena_state.allocator(), c.select_cols),
            .parallelism = 1,
        });
        defer freeResult(res);
        if (res.output.group_cols) |cols| {
            try testing.expectEqual(c.want.len, cols.len);
            for (c.want, cols) |w, g| try testing.expectEqualStrings(w, g);
        } else {
            try testing.expectEqual(c.want.len, res.output.aggs.len);
            for (c.want, res.output.aggs) |w, g| try testing.expectEqualStrings(w, g.alias);
        }
    }
}

test "page-pruned row groups decode just their kept rows, with the answers of whole columns" {
    var compaction: consumer.CompactionControl = .{};
    // Tiny pages with a page index, dictionary and plain encoded, nullable columns of every common type.
    const files = [_][]const u8{
        "data/parquet-testing/data/alltypes_tiny_pages.parquet",
        "data/parquet-testing/data/alltypes_tiny_pages_plain.parquet",
    };
    // Only conjunctive leaves prune pages: the OR is a control that decodes whole columns.
    const filters = [_]struct { sql: []const u8, compacts: bool }{
        .{ .sql = "id >= 3000 AND id < 3100", .compacts = true },
        .{ .sql = "id < 50", .compacts = true },
        .{ .sql = "id >= 7000 OR id < 10", .compacts = false },
        .{ .sql = "id = 4242", .compacts = true },
        .{ .sql = "id >= 1000 AND id < 1400 AND int_col > 3", .compacts = true },
    };
    const shapes = [_]struct { agg: []const u8, group: ?[]const u8 }{
        .{ .agg = "count(*) AS n, sum(bigint_col) AS sb, count(float_col) AS cf, min(double_col) AS md, " ++
            "max(string_col) AS ms, min(date_string_col) AS mdate, sum(tinyint_col) AS st, count(bool_col) AS cb", .group = null },
        .{ .agg = "count(*) AS n, sum(int_col) AS si, max(id) AS hi", .group = "string_col" },
        .{ .agg = "count(*) AS n, min(id) AS lo", .group = "month, bool_col" },
    };
    for (files) |fixture| {
        const bytes = metadata.readFileSlice(fixture, testing.allocator) catch |err| {
            if (err == error.FileNotFound) return error.SkipZigTest;
            return err;
        };
        defer testing.allocator.free(bytes);
        const inputs = [_]Input{.{ .name = fixture, .bytes = bytes }};
        for (filters) |f| for (shapes) |shape| for ([_]usize{ 1, 4 }) |j| {
            var arena = std.heap.ArenaAllocator.init(testing.allocator);
            defer arena.deinit();
            const a = arena.allocator();
            const args: MultiAggArgs = .{
                .inputs = &inputs,
                .filter = f.sql,
                .aggregate = shape.agg,
                .group_by = shape.group,
                .parallelism = j,
                .min_bytes_per_worker = 1,
                .compaction = &compaction,
            };
            compaction.enabled = false;
            var want = try runMultiAggregate(testing.allocator, a, args);
            defer want.deinit(testing.allocator);
            compaction.enabled = true;
            const before = compaction.row_groups.load(.monotonic);
            var got = try runMultiAggregate(testing.allocator, a, args);
            defer got.deinit(testing.allocator);
            testing.expectEqual(f.compacts, compaction.row_groups.load(.monotonic) > before) catch |e| {
                std.debug.print("compaction: {s} {s}\n", .{ fixture, f.sql });
                return e;
            };
            testing.expectEqualDeep(want.output.aggs, got.output.aggs) catch |e| {
                std.debug.print("{s} filter {s} j{d}\n", .{ fixture, f.sql, j });
                return e;
            };
            testing.expectEqualDeep(want.output.group_rows, got.output.group_rows) catch |e| {
                std.debug.print("{s} grouped filter {s} j{d}\n", .{ fixture, f.sql, j });
                return e;
            };
            try testing.expectEqual(want.rows_matched, got.rows_matched);
        };
    }
}

test "page-pruned DECIMAL and INT96 columns decode whole and answer as with compaction off" {
    var compaction: consumer.CompactionControl = .{};
    // INT32/INT64/FLBA-backed DECIMAL(9/18/38) and INT96, required and optional, behind a page index: their exact
    // decoders read whole chunks, so a row group reading them decodes whole columns rather than compacting.
    const fixture = "ci/fixtures/parquet/decimal_int96_page_index.parquet";
    const bytes = metadata.readFileSlice(fixture, testing.allocator) catch |err| {
        if (err == error.FileNotFound) return error.SkipZigTest;
        return err;
    };
    defer testing.allocator.free(bytes);
    const inputs = [_]Input{.{ .name = fixture, .bytes = bytes }};
    const shapes = [_]struct { agg: []const u8, compacts: ?bool }{
        .{ .agg = "count(*) AS n, sum(d9) AS a, sum(d18) AS b, max(d38) AS c", .compacts = false },
        .{ .agg = "count(*) AS n, sum(d9o) AS a, min(d18o) AS b, max(d38o) AS c, count(d38o) AS cc", .compacts = false },
        .{ .agg = "count(*) AS n, min(ts) AS t, max(tso) AS u, count(tso) AS cu", .compacts = false },
        // A control reading neither: whether it compacts is not this test's subject, only that answers agree.
        .{ .agg = "count(*) AS n, max(id) AS hi", .compacts = null },
    };
    for ([_][]const u8{ "id >= 100 AND id < 140", "id >= 1490 AND id < 1510", "id = 2999" }) |f| for (shapes) |shape|
        for ([_]usize{ 1, 4 }) |j| {
            var arena = std.heap.ArenaAllocator.init(testing.allocator);
            defer arena.deinit();
            const a = arena.allocator();
            const args: MultiAggArgs = .{
                .inputs = &inputs,
                .filter = f,
                .aggregate = shape.agg,
                .parallelism = j,
                .min_bytes_per_worker = 1,
                .compaction = &compaction,
            };
            compaction.enabled = false;
            var want = try runMultiAggregate(testing.allocator, a, args);
            defer want.deinit(testing.allocator);
            compaction.enabled = true;
            const before = compaction.row_groups.load(.monotonic);
            var got = try runMultiAggregate(testing.allocator, a, args);
            defer got.deinit(testing.allocator);
            if (shape.compacts) |c| try testing.expectEqual(c, compaction.row_groups.load(.monotonic) > before);
            try testing.expectEqualDeep(want.output.aggs, got.output.aggs);
        };
}

test "a payload page whose rows the filter rejects is not decoded; one with a selected row reports its corruption" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const streaming = @import("../writer/streaming.zig");
    var sink: streaming.MemorySink = .{ .arena = a };
    const n = 100000;
    const k = try a.alloc(i64, n);
    const v = try a.alloc(i64, n);
    const v_def = try a.alloc(u32, n);
    for (0..n) |i| {
        k[i] = @intCast(i);
        v[i] = @intCast(i % 97);
        v_def[i] = @intFromBool(i % 5 != 0);
    }
    const cols = [_]expr_agg.OutputCol{
        .{ .name = "k", .parquet_type = .INT64, .nullable = false, .col = .{ .i64 = .{ .values = k } } },
        .{ .name = "v", .parquet_type = .INT64, .nullable = true, .col = .{ .i64 = .{ .values = v, .def_levels = v_def, .max_def = 1 } } },
    };
    _ = try consumer.writeAggregateRows(a, sink.sink(), &cols, n, .SNAPPY, .{}, .{});
    const bytes = sink.buffer.items;
    // Corrupt the third data page of `v`: its snappy stream claims an impossible length.
    const meta = try metadata.open(a, bytes);
    const cm = meta.row_groups.items[0].columns.items[1].meta_data.?;
    var pos: usize = @intCast(cm.data_page_offset);
    var first_row: usize = 0;
    var page: usize = 0;
    var rows: usize = 0;
    while (true) : (page += 1) {
        var rdr = thrift_mod.Reader.init(bytes[pos..]);
        const header = try schema.PageHeader.read(&rdr);
        rows = @intCast(header.data_page_header.?.num_values);
        if (page == 2) {
            @memset(bytes[pos + rdr.pos ..][0..4], 0xff);
            break;
        }
        first_row += rows;
        pos += rdr.pos + @as(usize, @intCast(header.compressed_page_size));
    }
    const inputs = [_]Input{.{ .name = "corrupt-payload.parquet", .bytes = bytes }};
    // The canonical site: what an unfiltered scan, which decodes every page, reports.
    var whole: Diag = .{};
    try testing.expectError(error.DecompressionFailed, runMultiAggregate(testing.allocator, a, .{
        .inputs = &inputs,
        .aggregate = "count(*) AS n, sum(v) AS s",
        .parallelism = 1,
        .diag = &whole,
    }));
    var select_one_buf: [64]u8 = undefined;
    const select_one = try std.fmt.bufPrint(&select_one_buf, "k = {d}", .{first_row + rows / 2});
    for ([_]usize{ 1, 4 }) |j| for ([_]usize{ 1 << 20, 512 << 20 }) |budget| {
        // Every row of the corrupt page rejected (with others kept): the answer, from the other pages.
        for ([_][]const u8{ "k < 1000", "k >= 99000 OR k < 10" }) |rejecting| {
            var ok = try runMultiAggregate(testing.allocator, a, .{
                .inputs = &inputs,
                .filter = rejecting,
                .aggregate = "count(*) AS n, sum(v) AS s",
                .parallelism = j,
                .max_memory = budget,
            });
            defer ok.deinit(testing.allocator);
            try testing.expect(ok.output.aggs[0].value.i > 0);
        }
        // A filter keeping one of its rows reads the page, and reports the canonical decode error at that page.
        var diag: Diag = .{};
        const failed = runMultiAggregate(testing.allocator, a, .{
            .inputs = &inputs,
            .filter = select_one,
            .aggregate = "count(*) AS n, sum(v) AS s",
            .parallelism = j,
            .max_memory = budget,
            .diag = &diag,
        });
        if (failed) |res| {
            var r = res;
            r.deinit(testing.allocator);
            return error.TestExpectedError;
        } else |e| try testing.expectEqual(error.DecompressionFailed, e);
        try testing.expectEqual(@as(usize, 0), diag.detail.decode.row_group);
        try testing.expectEqualStrings("v", diag.detail.decode.column.get());
        try testing.expectEqual(whole.detail.decode.page, diag.detail.decode.page);
    };
}

test "page index: null pages a footer contradicts do not drop rows" {
    // These files flag every page of their two REQUIRED columns all-null in the ColumnIndex, while every page holds
    // values. Pruning on that claim answered 0 for filters every other row satisfies.
    const files = [_][]const u8{
        "data/parquet-testing/data/datapage_v1-snappy-compressed-checksum.parquet",
        "data/parquet-testing/data/datapage_v1-uncompressed-checksum.parquet",
        "data/parquet-testing/data/datapage_v1-corrupt-checksum.parquet",
    };
    const Query = struct { filter: []const u8, want: i128 };
    const queries = [_]Query{
        .{ .filter = "a > -66052", .want = 2560 },
        .{ .filter = "a IS NOT NULL", .want = 5120 },
        .{ .filter = "b < 0 OR b >= 0", .want = 5120 },
    };
    for (files) |fixture| {
        const bytes = metadata.readFileSlice(fixture, testing.allocator) catch |err| {
            if (err == error.FileNotFound) {
                std.debug.print("skipping: {s} not present\n", .{fixture});
                return error.SkipZigTest;
            }
            return err;
        };
        defer testing.allocator.free(bytes);
        const inputs = [_]Input{.{ .name = fixture, .bytes = bytes }};
        for (queries) |q| {
            var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
            defer arena_state.deinit();
            const res = try runMultiAggregate(testing.allocator, arena_state.allocator(), .{
                .inputs = &inputs,
                .filter = q.filter,
                .aggregate = "count(*) AS n",
                .parallelism = 1,
            });
            defer freeResult(res);
            testing.expectEqual(q.want, res.output.aggs[0].value.i) catch |err| {
                std.debug.print("{s}: filter {s}\n", .{ fixture, q.filter });
                return err;
            };
        }
    }
}

test "runMultiAggregate: inputs whose nesting differs are a schema mismatch" {
    // Columns bind by leaf index against file 0. Here file 1 has the same element names in the same order, but
    // `name` is a top-level column rather than a field of `r`, so leaf 1 means different columns in the two files.
    var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_state.deinit();
    const a = arena_state.allocator();
    const m0 = try test_fixtures.sharedLeafNameMeta(a);
    var m1 = try test_fixtures.sharedLeafNameMeta(a);
    m1.schema.items[0].num_children = 6;
    m1.schema.items[1].num_children = 1;

    const inputs = [_]Input{ .{ .name = "f0", .bytes = "" }, .{ .name = "f1", .bytes = "" } };
    const metas = [_]schema.FileMetaData{ m0, m1 };
    var diag: Diag = .{};
    try testing.expectError(error.SchemaMismatch, runMultiAggregate(testing.allocator, a, .{
        .inputs = &inputs,
        .metas = &metas,
        .aggregate = "sum(key) AS s",
        .parallelism = 1,
        .diag = &diag,
    }));
    try testing.expectEqualStrings("f1", diag.input.?);
    const mismatch = diag.detail.schema_mismatch;
    try testing.expectEqual(@as(usize, 1), mismatch.file);
    try testing.expectEqualStrings("f0", mismatch.first_input);
    try testing.expectEqualStrings(m0.schema.items[0].name, diag.column.get());
    try testing.expectEqualStrings(m1.schema.items[0].name, mismatch.other.get());
}

test "runMultiAggregate: an input that is not Parquet is named with the reason" {
    var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_state.deinit();
    const inputs = [_]Input{.{ .name = "notes.txt", .bytes = "not a parquet file at all" }};
    var diag: Diag = .{};
    try testing.expectError(error.NotParquet, runMultiAggregate(testing.allocator, arena_state.allocator(), .{
        .inputs = &inputs,
        .aggregate = "count(*) AS n",
        .parallelism = 1,
        .diag = &diag,
    }));
    try testing.expectEqualStrings("notes.txt", diag.input.?);
    try testing.expect(diag.cause != null);
}

// ============================================================ Poisoned statistics.
//
// Checked-in fixtures with their footer, ColumnIndex or OffsetIndex rewritten in memory so they lie in the ways
// `statistics.chunkStats` and `statistics.pageIndexIsPlausible` check for. By default every answer is the decoded
// one. Under --trust-stats impossible claims are still refused, while contradictory or uncorroborated ones are taken
// as written, lie and all.
// ============================================================











test "poisoned stats: chunk bounds with min above max are refused, by default and under --trust-stats" {
    const edit = struct {
        fn f(meta: *schema.FileMetaData) void {
            for ([_]usize{ 0, 2, 3, 5 }) |ci| {
                const st = rg1Stats(meta, ci);
                std.mem.swap(?[]const u8, &st.min_value, &st.max_value);
                std.mem.swap(?[]const u8, &st.min, &st.max);
            }
        }
    }.f;
    try expectPoisonCases(full_match_fixture, .{ .footer = edit }, &.{
        .{ .filter = "ts = 300", .n = 1, .trusted = 1 },
        .{ .filter = "ts >= 250 AND ts <= 260", .n = 11, .trusted = 11 },
        .{ .filter = "s = 'k000300-pppppppppppppppppppppppp'", .n = 1, .trusted = 1 },
        .{ .filter = "u = 300", .n = 1, .trusted = 1 },
        .{ .filter = "x > 5 AND ts >= 200 AND ts < 400", .n = 29, .trusted = 29 },
    });
}

test "poisoned stats: an all-null count beside bounds decodes by default and is trusted under --trust-stats" {
    const edit = struct {
        fn f(meta: *schema.FileMetaData) void {
            rg1Stats(meta, 0).null_count = 200;
        }
    }.f;
    try expectPoisonCases(full_match_fixture, .{ .footer = edit }, &.{
        .{ .filter = "ts IS NULL", .n = 0, .trusted = 200 },
        .{ .filter = "ts IS NOT NULL", .n = 1600, .trusted = 1400 },
        .{ .filter = "ts = 300", .n = 1, .trusted = 1 },
    });
}

test "poisoned stats: a zero null count with no bounds decodes by default and is trusted under --trust-stats" {
    // Row group 1's n is all null, so its writer kept no bounds; the footer now claims no nulls either.
    const edit = struct {
        fn f(meta: *schema.FileMetaData) void {
            rg1Stats(meta, 4).null_count = 0;
        }
    }.f;
    try expectPoisonCases(full_match_fixture, .{ .footer = edit }, &.{
        .{ .filter = "n IS NULL", .n = 320, .trusted = 120 },
        .{ .filter = "n IS NOT NULL", .n = 1280, .trusted = 1480 },
    });
}

