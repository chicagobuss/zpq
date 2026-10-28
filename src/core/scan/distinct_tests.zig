const std = @import("std");
const thrift_mod = @import("../thrift.zig");
const spawn_util = @import("../spawn.zig");
const schema = @import("../schema.zig");
const consumer = @import("../consumer.zig");
const agg_plan = @import("../agg_plan.zig");
const metadata = @import("../parquet/metadata.zig");
const expr_ast = @import("../expr/ast.zig");
const expr_parser = @import("../expr/parser.zig");
const expr_agg = @import("../expr/agg.zig");
const core_memory = @import("../memory.zig");
const partagg = @import("../partagg.zig");
const grouped_bitmap = @import("../grouped_distinct_bitmap.zig");
const filter_parser = @import("../filter/parser.zig");
const Diag = @import("../diag.zig").Diag;
const blocking = @import("../blocking.zig");
const owned_merge = @import("owned_merge.zig");
const freeOwnedAccumulatorStrings = owned_merge.freeOwnedAccumulatorStrings;
const worker_contracts = @import("worker_contracts.zig");
const Worker = worker_contracts.Worker;
const FailureProbe = worker_contracts.FailureProbe;
const loadFixture = @import("test_support.zig").loadFixture;
const scan_worker = @import("worker.zig");
const canonicalFailurePrefix = scan_worker.canonicalFailurePrefix;
const firstWorkerFailure = scan_worker.firstWorkerFailure;
const plan_types = @import("plan_types.zig");
const Reason = plan_types.Reason;
const scan_count_cut = @import("count_cut.zig");
const MergedSlot = scan_count_cut.MergedSlot;
const scan_rank_cut = @import("rank_cut.zig");
const ResultCut = scan_rank_cut.ResultCut;
const CutScratch = scan_rank_cut.CutScratch;
const resultLimitGroups = scan_rank_cut.resultLimitGroups;
const keepResultTop = scan_rank_cut.keepResultTop;
const scan_result = @import("result.zig");
const MultiAggResult = scan_result.MultiAggResult;
const full_match_fixture = @import("test_support.zig").full_match_fixture;
const scan_plan_impl = @import("plan.zig");
const sampledPassShare = scan_plan_impl.sampledPassShare;
const slot_sort = @import("slot_sort.zig");
const sortSlotsByFirst = slot_sort.sortSlotsByFirst;
const scan_phase = @import("phase.zig");
const clearWorkerErrors = scan_phase.clearWorkerErrors;
const phaseFailure = scan_phase.phaseFailure;
const scan_census = @import("census.zig");
const Repeat = scan_census.Repeat;
const scan_merge = @import("merge.zig");
const MergeGroupRef = scan_merge.MergeGroupRef;
const hashMergeGroupRefs = scan_merge.hashMergeGroupRefs;
const mergeCountCut = scan_merge.mergeCountCut;
const merge_part_bytes = scan_merge.merge_part_bytes;
const merge_parts_max = scan_merge.merge_parts_max;
const mergePartitionCount = scan_merge.mergePartitionCount;
const mergeEntryBytes = scan_merge.mergeEntryBytes;
const mergeFoldedRefs = scan_merge.mergeFoldedRefs;
const scan_execute = @import("execute.zig");
const runMultiAggregate = scan_execute.runMultiAggregate;
const Chain = scan_execute.Chain;
const narrowed = scan_execute.narrowed;
const explainAggregate = scan_execute.explainAggregate;
const prepareAggregate = scan_execute.prepareAggregate;
const integration_support = @import("integration_support.zig");
const sameValue = integration_support.sameValue;
const freeResult = integration_support.freeResult;
const expectSameAggValue = integration_support.expectSameAggValue;
const expectSameAnswer = integration_support.expectSameAnswer;
const selectList = integration_support.selectList;
const setMergeTestString = integration_support.setMergeTestString;
const decimal_exact_fixture = integration_support.decimal_exact_fixture;
const outcomes = integration_support.outcomes;
const expectSameOutcomes = integration_support.expectSameOutcomes;
const BitmapTestFixture = integration_support.BitmapTestFixture;
const LivePeak = integration_support.LivePeak;
const checkRecordDecodeModes = integration_support.checkRecordDecodeModes;
const scan_types = @import("types.zig");
const MultiAggArgs = scan_types.MultiAggArgs;
const Input = scan_types.Input;
const FastPath = scan_types.FastPath;
const WorkItem = scan_types.WorkItem;
const BoundOutputColumn = scan_types.BoundOutputColumn;
const byte_stream = @import("byte_stream.zig");
const integer_stream = @import("integer_stream.zig");
const typed_answer = @import("typed_answer.zig");
const testing = std.testing;
const test_fixtures = @import("../parquet/test_fixtures.zig");

test "grouped DISTINCT bitmap memory refusal keeps a budget the exact fallback answers" {
    const gpa = testing.allocator;
    var arena = std.heap.ArenaAllocator.init(gpa);
    defer arena.deinit();
    const a = arena.allocator();
    const fixture = try BitmapTestFixture.init(a);
    var args = fixture.args();
    args.max_memory = 16 << 20;
    args.grouped_distinct_bitmap = .off;
    var want = try runMultiAggregate(gpa, a, args);
    defer want.deinit(gpa);
    var hi: [8]u8 = undefined;
    std.mem.writeInt(i64, &hi, 1 << 24, .little);
    for (fixture.metas[0].row_groups.items) |*rg| {
        const st = &rg.columns.items[1].meta_data.?.statistics.?;
        st.max_value = &hi;
        st.max = &hi;
    }
    args.grouped_distinct_bitmap = .private;
    var taken: std.EnumSet(FastPath) = .{};
    args.taken = &taken;
    var got = try prepareAggregate(gpa, a, args, false);
    defer got.deinit(gpa);
    try testing.expect(!taken.contains(.grouped_distinct_bitmap));
    try testing.expect(!got.grouped_distinct_bitmap);
    try testing.expectEqualDeep(want.output.group_rows, got.output.group_rows);
}

test "runMultiAggregate: grouped count(DISTINCT) through pairs answers as through sets" {
    const path = "ci/fixtures/parquet/census_dictionary_review.parquet";
    const bytes = (try loadFixture(path)) orelse return error.SkipZigTest;
    defer testing.allocator.free(bytes);
    const inputs = [_]Input{.{ .name = path, .bytes = bytes }};
    // `t` holds 7 keys, `u` is near unique, `n` is NULL through the first row group: a key's distinct values, ones
    // only NULL counts, a FILTER, other aggregates beside it, and a cut on the distinct count.
    const aggregates = [_][]const u8{
        "count(DISTINCT u) AS d, count(*) AS c",
        "count(DISTINCT n) AS d, sum(u) AS s",
        "count(DISTINCT u) FILTER (WHERE n = 1) AS d, min(u) AS lo",
    };
    const cuts = [_]expr_ast.ResultOrder{
        .{},
        .{ .keys = &.{.{ .column = 1, .descending = true }}, .limit = 3 },
    };
    for (aggregates) |aggregate| for ([_][]const u8{ "t", "u" }) |key| for ([_]usize{ 1, 3 }) |j| for (cuts) |cut| {
        for ([_]bool{ false, true }) |encoded| {
            var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
            defer arena_state.deinit();
            const arena = arena_state.allocator();
            var args: MultiAggArgs = .{
                .inputs = &inputs,
                .aggregate = aggregate,
                .group_by = key,
                .parallelism = j,
                .min_bytes_per_worker = 1,
                .encoded = encoded,
                .result_order = cut,
                .distinct_pairs = .off,
            };
            var want = try runMultiAggregate(testing.allocator, arena, args);
            defer want.deinit(testing.allocator);
            args.distinct_pairs = .on;
            var got = try runMultiAggregate(testing.allocator, arena, args);
            defer got.deinit(testing.allocator);
            try testing.expect(!want.distinct_pairs);
            try testing.expect(got.distinct_pairs);
            try want.output.applyResultOrder(testing.allocator, cut);
            try got.output.applyResultOrder(testing.allocator, cut);
            const wr = want.output.group_rows.?;
            const gr = got.output.group_rows.?;
            try testing.expectEqual(wr.len, gr.len);
            for (wr, gr) |a, b| for (a, b) |av, bv| try testing.expect(sameValue(av, bv));
        }
    };
}

test "runMultiAggregate: workers judge their sets after a pilot and move them to pairs only where pairs pay" {
    const path = "ci/fixtures/parquet/census_dictionary_review.parquet";
    const bytes = (try loadFixture(path)) orelse return error.SkipZigTest;
    defer testing.allocator.free(bytes);
    const inputs = [_]Input{.{ .name = path, .bytes = bytes }};
    // By `t` (7 keys) every group gains new values: its sets move, at -j 3 alongside workers still on sets. By `u`
    // (near unique) a group is a row or two: the sets stay.
    for ([_][]const u8{ "t", "u" }, [_]bool{ true, false }) |key, moves| for ([_]usize{ 1, 3 }) |j| {
        var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
        defer arena_state.deinit();
        const arena = arena_state.allocator();
        var args: MultiAggArgs = .{
            .inputs = &inputs,
            .ndv_policy = false,
            .aggregate = "count(DISTINCT u) AS d, count(*) AS c",
            .group_by = key,
            .parallelism = j,
            .min_bytes_per_worker = 1,
            .distinct_pairs = .off,
        };
        var want = try runMultiAggregate(testing.allocator, arena, args);
        defer want.deinit(testing.allocator);
        args.distinct_pairs = .auto;
        args.pairs_pilot_rows = 1000;
        var got = try runMultiAggregate(testing.allocator, arena, args);
        defer got.deinit(testing.allocator);
        try testing.expectEqual(moves, got.distinct_pairs);
        const wr = want.output.group_rows.?;
        const gr = got.output.group_rows.?;
        try testing.expectEqual(wr.len, gr.len);
        for (wr, gr) |a, b| for (a, b) |av, bv| try testing.expect(sameValue(av, bv));
    };
}

test "mergePartitionCount sizes a full merge's partitions by their state, never below one a worker" {
    const entry = mergeEntryBytes(2);
    // Few references keep one partition a worker; near-unique millions split until a partition's state fits.
    try testing.expectEqual(@as(usize, 1), mergePartitionCount(16, 4000, false, entry));
    try testing.expectEqual(@as(usize, 16), mergePartitionCount(16, 16_000, false, entry));
    const many = mergePartitionCount(16, 27_000_000, false, entry);
    try testing.expect(many > 16 and many <= merge_parts_max);
    try testing.expect(27_000_000 * entry / many <= merge_part_bytes);
    try testing.expectEqual(@as(usize, merge_parts_max), mergePartitionCount(16, 1 << 40, false, entry));
    // A cut keeps its own rule.
    try testing.expectEqual(@as(usize, 512), mergePartitionCount(16, 27_000_000, true, entry));
}

test "runMultiAggregate: a pilot short of rows a group judges again at twice the rows" {
    const path = "ci/fixtures/parquet/census_dictionary_review.parquet";
    const bytes = (try loadFixture(path)) orelse return error.SkipZigTest;
    defer testing.allocator.free(bytes);
    const inputs = [_]Input{.{ .name = path, .bytes = bytes }};
    // 200 groups of nearly unique values over 1000-row groups: five rows a group after the first row group, ten after
    // the second. Without re-checks the sets stay; with them every worker reading two row groups or more moves, and at
    // -j 3 some worker reads three of the eight while another may read one.
    for ([_]u8{ 0, 3 }) |rechecks| for ([_]usize{ 1, 3 }) |j| {
        var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
        defer arena_state.deinit();
        const arena = arena_state.allocator();
        var args: MultiAggArgs = .{
            .inputs = &inputs,
            .ndv_policy = false,
            .aggregate = "count(DISTINCT u) AS d, count(*) AS c",
            .group_by = "u / 5000000000 AS b",
            .parallelism = j,
            .min_bytes_per_worker = 1,
            .distinct_pairs = .off,
        };
        var want = try runMultiAggregate(testing.allocator, arena, args);
        defer want.deinit(testing.allocator);
        args.distinct_pairs = .auto;
        args.pairs_pilot_rows = 1000;
        args.pairs_rechecks = rechecks;
        var got = try runMultiAggregate(testing.allocator, arena, args);
        defer got.deinit(testing.allocator);
        try testing.expectEqual(rechecks > 0, got.distinct_pairs);
        const wr = want.output.group_rows.?;
        const gr = got.output.group_rows.?;
        try testing.expectEqual(wr.len, gr.len);
        for (wr, gr) |a, b| for (a, b) |av, bv| try testing.expect(sameValue(av, bv));
    };
}

test "regression: count(DISTINCT) pairs answer every budget the set path answers, as the set path does" {
    const path = "ci/fixtures/parquet/census_dictionary_review.parquet";
    const bytes = (try loadFixture(path)) orelse return error.SkipZigTest;
    defer testing.allocator.free(bytes);
    const inputs = [_]Input{.{ .name = path, .bytes = bytes }};
    var answered: usize = 0;
    for ([_]usize{ 64 << 10, 256 << 10, 1 << 20, 512 << 20 }) |budget| {
        var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
        defer arena_state.deinit();
        var results: [2]?MultiAggResult = .{ null, null };
        defer for (results) |r| if (r) |res| freeResult(res);
        var errs: [2]?anyerror = .{ null, null };
        for ([_]@FieldType(MultiAggArgs, "distinct_pairs"){ .off, .on }, 0..) |pairs, i| {
            results[i] = runMultiAggregate(testing.allocator, arena_state.allocator(), .{
                .inputs = &inputs,
                .aggregate = "count(DISTINCT u) AS d",
                .group_by = "t",
                .parallelism = 3,
                .min_bytes_per_worker = 1,
                .max_memory = budget,
                .distinct_pairs = pairs,
            }) catch |e| blk: {
                errs[i] = e;
                break :blk null;
            };
        }
        // Pairs refused budgets the set path answers: the refusal now runs the set path.
        try testing.expectEqual(errs[0], errs[1]);
        const off = results[0] orelse continue;
        answered += 1;
        const rows = off.output.group_rows.?;
        try testing.expectEqual(rows.len, results[1].?.output.group_rows.?.len);
        for (rows, results[1].?.output.group_rows.?) |r, p_row| for (r, p_row) |x, y| try expectSameAggValue(x, y);
    }
    try testing.expect(answered > 0);
}

test "count-only: charged bytes equals keys and filter estimate alone" {
    // Under a sketch count-only pass, depth-2 sum(t + t) must not be charged.
    // keys+filter estimate is 16,189; uncorrected calls charged 48,189, which forced slicing under limit 20,000.
    const path = "ci/fixtures/parquet/census_topk.parquet";
    const bytes = (try loadFixture(path)) orelse return error.SkipZigTest;
    defer testing.allocator.free(bytes);
    const inputs = [_]Input{.{ .name = path, .bytes = bytes }};
    var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_state.deinit();
    const arena = arena_state.allocator();
    const args: MultiAggArgs = .{
        .inputs = &inputs,
        .aggregate = "count(*) AS c, sum(t + t) AS st, min(s) AS lo, count(DISTINCT t) AS d, count(*) FILTER (WHERE n = 1) AS f",
        .group_by = "u",
        .parallelism = 1,
        .decode_limit = 20000,
        .sketch_topk = .{},
        .result_order = .{ .keys = &.{.{ .column = 1, .descending = true }}, .limit = 3, .offset = 2 },
    };
    var res = try runMultiAggregate(testing.allocator, arena, args);
    defer res.deinit(testing.allocator);
    try res.output.applyResultOrder(testing.allocator, args.result_order);
    const rows = res.output.group_rows.?;
    try testing.expect(rows.len > 0);
}

test "count-only: no filter-only column materialized" {
    // Under a sketch count-only pass on dictionary-encoded column t with filter t > 3,
    // t is not consumed by aggregates and must remain coded without value materialization.
    const path = "ci/fixtures/parquet/census_dictionary_review.parquet";
    const bytes = (try loadFixture(path)) orelse return error.SkipZigTest;
    defer testing.allocator.free(bytes);
    const inputs = [_]Input{.{ .name = path, .bytes = bytes }};
    var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_state.deinit();
    const arena = arena_state.allocator();
    const args: MultiAggArgs = .{
        .inputs = &inputs,
        .filter = "t > 3",
        .aggregate = "count(*) AS c, sum(t + t) AS st, min(s) AS lo, count(DISTINCT t) AS d, count(*) FILTER (WHERE n = 1) AS f",
        .group_by = "u",
        .parallelism = 1,
        .encoded = true,
        .decode_limit = null,
        .sketch_topk = .{},
        .result_order = .{ .keys = &.{.{ .column = 1, .descending = true }}, .limit = 3, .offset = 2 },
    };
    var res = try runMultiAggregate(testing.allocator, arena, args);
    defer res.deinit(testing.allocator);
    try res.output.applyResultOrder(testing.allocator, args.result_order);
    const rows = res.output.group_rows.?;
    try testing.expect(rows.len > 0);
}



test "result cut SUM: complete signed ranks, NULLs, ties, offsets and owned payload failures" {
    spawn_util.injectFailureAfter(0);
    defer spawn_util.resetFailure();
    try testing.checkAllAllocationFailures(testing.allocator, struct {
        fn run(gpa: std.mem.Allocator) !void {
            const calls = [_]expr_agg.AggCall{
                .{ .func = .sum, .arg = null, .where = null, .alias = "s", .result = .i64 },
                .{ .func = .min, .arg = null, .where = null, .alias = "m", .result = .bytes },
            };
            var tables: [2]expr_agg.GroupTable = undefined;
            for (&tables) |*t| t.* = .init(gpa, 1 << 20);
            defer for (&tables) |*t| t.deinit();
            var refs: std.ArrayList(MergeGroupRef) = .empty;
            defer refs.deinit(gpa);
            var buf: [16]u8 = undefined;
            for (0..40) |g| for (&tables, 0..) |*t, ti| {
                const key = try std.fmt.bufPrint(&buf, "k{d}", .{g});
                const id = try t.getOrInsert(key, &calls, g * 2 + 1 - ti);
                t.accumulators.items[id * 2].sum_i = if (g == 39)
                    null
                else if (ti == 0) 1000 else @as(i128, @intCast(g % 9)) - 1004;
                try setMergeTestString(gpa, &t.accumulators.items[id * 2 + 1], if (ti == 0) "z" else "a");
                try refs.append(gpa, .{ .table = @intCast(ti), .group = id });
            };
            var arena_state = std.heap.ArenaAllocator.init(gpa);
            defer arena_state.deinit();
            var slots: std.ArrayList(MergedSlot) = .empty;
            defer slots.deinit(gpa);
            var accs: std.ArrayList(expr_agg.Accumulator) = .empty;
            defer {
                freeOwnedAccumulatorStrings(gpa, accs.items);
                accs.deinit(gpa);
            }
            const cut: ResultCut = .{ .a_idx = 0, .k = 3, .descending = true, .ranking_first = true };
            try mergeFoldedRefs(gpa, gpa, &tables, refs.items, &calls, 3, cut, false, &slots, &accs);
            try keepResultTop(gpa, gpa, &calls, cut, &slots, &accs);
            try sortSlotsByFirst(gpa, slots.items, 1);
            try testing.expectEqual(@as(usize, 3), slots.items.len);
            for (slots.items, [_]u64{ 16, 34, 52 }) |s, first| {
                try testing.expectEqual(first, s.first);
                try testing.expectEqual(@as(?i128, 4), accs.items[s.idx].sum_i);
                try testing.expectEqualStrings("a", accs.items[s.idx + 1].min_bytes.?);
            }
        }
    }.run, .{});
}

test "result cut SUM: scan and shared partitions match full ordered answers at j1 j3 j8 and spawn failure" {
    const path = "ci/fixtures/parquet/census_topk.parquet";
    const bytes = (try loadFixture(path)) orelse return error.SkipZigTest;
    defer testing.allocator.free(bytes);
    const inputs = [_]Input{.{ .name = path, .bytes = bytes }};
    for ([_]usize{ 1, 3, 8 }) |jobs| for ([_]bool{ false, true }) |part| {
        for ([_]bool{ false, true }) |descending| {
            for ([_]u64{ 0, 1, 5, 500 }) |limit| for ([_]bool{ false, true }) |fail_spawn| {
                var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
                defer arena_state.deinit();
                const arena = arena_state.allocator();
                const distinct = if (part) "count(*) AS d" else "count(DISTINCT t) AS d";
                const aggregate = try std.fmt.allocPrint(
                    arena,
                    "sum(t - 30) FILTER (WHERE n = 1) AS total, min(s) AS lo, {s}",
                    .{distinct},
                );
                var args: MultiAggArgs = .{
                    .inputs = &inputs,
                    .aggregate = aggregate,
                    .group_by = "s",
                    .parallelism = jobs,
                    .partagg = part,
                    .min_bytes_per_worker = 1,
                    .sketch_topk = null,
                    .select_cols = try selectList(arena, &.{
                        "s", "sum(t - 30) FILTER (WHERE n = 1) AS total", "min(s) AS lo", distinct,
                    }),
                };
                var want = try runMultiAggregate(testing.allocator, arena, args);
                defer want.deinit(testing.allocator);
                args.result_order = .{
                    .keys = &.{.{ .column = 1, .descending = descending }},
                    .limit = limit,
                    .offset = 2,
                };
                if (fail_spawn) spawn_util.injectFailureAfter(0);
                var got = runMultiAggregate(testing.allocator, arena, args) catch |e| {
                    spawn_util.resetFailure();
                    return e;
                };
                spawn_util.resetFailure();
                defer got.deinit(testing.allocator);
                try want.output.applyResultOrder(testing.allocator, args.result_order);
                try got.output.applyResultOrder(testing.allocator, args.result_order);
                const wr = want.output.group_rows.?;
                const gr = got.output.group_rows.?;
                try testing.expectEqual(wr.len, gr.len);
                for (wr, gr) |w, g| for (w, g) |a, b| try testing.expect(sameValue(a, b));
            };
        }
    };
}

test "result cut SUM: the merge's scratch is uncharged, as the full merge's is" {
    const calls = [_]expr_agg.AggCall{.{ .func = .sum, .arg = null, .where = null, .alias = "s", .result = .i64 }};
    var table = expr_agg.GroupTable.init(testing.allocator, 1 << 20);
    defer table.deinit();
    const id = try table.getOrInsert("a", &calls, 0);
    table.accumulators.items[id].sum_i = 4;
    var budget: expr_agg.SharedBudget = .{ .limit = 1, .block = 1 };
    table.shared = &budget;
    defer table.shared = null; // This test only charges merge scratch, not the previously private source table.
    var slots: std.ArrayList(MergedSlot) = .empty;
    defer slots.deinit(testing.allocator);
    var accs: std.ArrayList(expr_agg.Accumulator) = .empty;
    defer accs.deinit(testing.allocator);
    // A budget the full merge ignores no longer refuses the cut that stands in for it.
    try hashMergeGroupRefs(
        testing.allocator,
        testing.allocator,
        (&table)[0..1],
        &.{.{ .table = 0, .group = id }},
        null,
        &calls,
        .{ .a_idx = 0, .k = 1, .ranking_first = true },
        false,
        &slots,
        &accs,
    );
    try testing.expectEqual(@as(usize, 1), slots.items.len);
    try testing.expectEqual(@as(usize, 0), budget.used.load(.monotonic));
}

test "result cut SUM: the one-table cut holds no charge; retained winner capacities share and return the budget" {
    const calls = [_]expr_agg.AggCall{.{ .func = .sum, .arg = null, .where = null, .alias = "s", .result = .i64 }};
    const outputs = [_]BoundOutputColumn{.{ .source = .{ .agg = 0 }, .name = "s" }};
    var table = expr_agg.GroupTable.init(testing.allocator, 1 << 20);
    defer table.deinit();
    for ([_][]const u8{ "a", "b" }, 0..) |key, i| {
        const id = try table.getOrInsert(key, &calls, i);
        table.accumulators.items[id].sum_i = @intCast(i);
    }
    var budget: expr_agg.SharedBudget = .{ .limit = 1, .block = 1 };
    table.shared = &budget;
    defer table.shared = null;
    var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_state.deinit();
    const order: expr_ast.ResultOrder = .{ .keys = &.{.{ .column = 0, .descending = true }}, .limit = 1 };
    // The full merge it stands in for answers whatever the budget, so the cut does too (it refused before).
    const top = (try resultLimitGroups(
        arena_state.allocator(),
        &table,
        &outputs,
        &calls,
        order,
        false,
        null,
    )).?;
    try testing.expectEqualSlices(u32, &.{1}, top);
    try testing.expectEqual(@as(usize, 0), budget.used.load(.monotonic));
    budget.limit = 1 << 20;
    var memory: CutScratch = .{ .parent = testing.allocator, .shared = &budget };
    defer memory.finish();
    var slots: std.ArrayList(MergedSlot) = .empty;
    defer slots.deinit(testing.allocator);
    var accs: std.ArrayList(expr_agg.Accumulator) = .empty;
    defer accs.deinit(testing.allocator);
    try hashMergeGroupRefs(
        testing.allocator,
        testing.allocator,
        (&table)[0..1],
        &.{ .{ .table = 0, .group = 0 }, .{ .table = 0, .group = 1 } },
        null,
        &calls,
        .{ .a_idx = 0, .k = 1, .ranking_first = true, .memory = memory.allocator() },
        false,
        &slots,
        &accs,
    );
    try testing.expectEqual(
        slots.capacity * @sizeOf(MergedSlot) + accs.capacity * @sizeOf(expr_agg.Accumulator),
        budget.used.load(.monotonic),
    );
}

test "result cut extrema AVG DECIMAL: scans match full stable order at j1 j3 j8" {
    const fixtures = [_]struct { path: []const u8, keys: []const []const u8, aggs: []const []const u8 }{
        .{
            .path = "ci/fixtures/parquet/census_topk.parquet",
            .keys = &.{"s"},
            .aggs = &.{
                "min(t - 30) FILTER (WHERE n = 1) AS rank", "max(t - 30) FILTER (WHERE n = 1) AS rank",
                "avg(t - 30) FILTER (WHERE n = 1) AS rank", "avg_state(t - 30) FILTER (WHERE n = 1) AS rank",
            },
        },
        .{
            .path = decimal_exact_fixture,
            .keys = &.{ "k", "i" },
            .aggs = &.{
                "sum(d38) AS rank", "min(dn) AS rank", "max(dn) AS rank", "avg(dn) AS rank", "avg_state(dn) AS rank",
            },
        },
    };
    for (fixtures) |fixture| {
        const bytes = (try loadFixture(fixture.path)) orelse return error.SkipZigTest;
        defer testing.allocator.free(bytes);
        const inputs = [_]Input{.{ .name = fixture.path, .bytes = bytes }};
        for (fixture.keys) |key| for (fixture.aggs) |ranking| for ([_]usize{ 1, 3, 8 }) |jobs| {
            for ([_]bool{ false, true }) |part| for ([_]bool{ false, true }) |desc| {
                var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
                defer arena_state.deinit();
                const arena = arena_state.allocator();
                const aggregate = try std.fmt.allocPrint(arena, "{s}, count(*) AS c", .{ranking});
                var args: MultiAggArgs = .{
                    .inputs = &inputs,
                    .aggregate = aggregate,
                    .group_by = key,
                    .parallelism = jobs,
                    .partagg = part,
                    .min_bytes_per_worker = 1,
                    .sketch_topk = null,
                    .select_cols = try selectList(arena, &.{ key, ranking, "count(*) AS c" }),
                };
                var want = try runMultiAggregate(testing.allocator, arena, args);
                defer want.deinit(testing.allocator);
                args.result_order = .{ .keys = &.{.{ .column = 1, .descending = desc }}, .limit = 1, .offset = 1 };
                var got = try runMultiAggregate(testing.allocator, arena, args);
                defer got.deinit(testing.allocator);
                try want.output.applyResultOrder(testing.allocator, args.result_order);
                try got.output.applyResultOrder(testing.allocator, args.result_order);
                try testing.expectEqual(want.output.group_rows.?.len, got.output.group_rows.?.len);
                for (want.output.group_rows.?, got.output.group_rows.?) |w, g|
                    for (w, g) |a, b| try testing.expect(sameValue(a, b));
            };
        };
    }
}

test "result cut DECIMAL: loser and zero-limit overflow stays observable, payload falls back" {
    const bytes = (try loadFixture(decimal_exact_fixture)) orelse return error.SkipZigTest;
    defer testing.allocator.free(bytes);
    const inputs = [_]Input{.{ .name = decimal_exact_fixture, .bytes = bytes }};
    for ([_]usize{ 1, 3, 8 }) |jobs| for ([_]u64{ 0, 1 }) |limit| {
        var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
        defer arena_state.deinit();
        const arena = arena_state.allocator();
        // c's sum fits and ranks first ASC; a and b overflow but still have to be finalized.
        try testing.expectError(error.DecimalOverflow, runMultiAggregate(testing.allocator, arena, .{
            .inputs = &inputs,
            .aggregate = "sum(big) AS rank",
            .group_by = "k",
            .parallelism = jobs,
            .min_bytes_per_worker = 1,
            .select_cols = try selectList(arena, &.{ "k", "sum(big) AS rank" }),
            .result_order = .{ .keys = &.{.{ .column = 1, .descending = false }}, .limit = limit },
        }));
        // c also ranks first by min(i) DESC; the losing SUM(big) payload must keep the full-path error.
        try testing.expectError(error.DecimalOverflow, runMultiAggregate(testing.allocator, arena, .{
            .inputs = &inputs,
            .aggregate = "min(i) AS rank, sum(big) AS payload",
            .group_by = "k",
            .parallelism = jobs,
            .min_bytes_per_worker = 1,
            .select_cols = try selectList(arena, &.{ "k", "min(i) AS rank", "sum(big) AS payload" }),
            .result_order = .{ .keys = &.{.{ .column = 1, .descending = true }}, .limit = limit },
        }));
    };
}

test "runMultiAggregate: a gated input plans from the dictionaries its probe reads first, as a mapped one does" {
    const gpa = testing.allocator;
    const bytes = try metadata.readFileSlice("ci/fixtures/parquet/topk_skew.parquet", gpa);
    defer gpa.free(bytes);
    var meta_arena = std.heap.ArenaAllocator.init(gpa);
    defer meta_arena.deinit();
    const meta = try metadata.open(meta_arena.allocator(), bytes);
    // A reader's buffer: zero until the probe copies what it asks for, the rest once the first gate is waited on.
    const Fake = struct {
        src: []const u8,
        buf: []u8,
        probes: usize = 0,
        started: bool = false,
        filled: bool = false,
        fn add(ctx: *anyopaque, _: usize, _: usize, lo: u64, hi: u64) anyerror!void {
            const self: *@This() = @ptrCast(@alignCast(ctx));
            try testing.expect(!self.started);
            @memcpy(self.buf[lo..hi], self.src[lo..hi]);
            self.probes += 1;
        }
        fn start(ctx: *anyopaque) anyerror!void {
            const self: *@This() = @ptrCast(@alignCast(ctx));
            self.started = true;
        }
        fn probed(ctx: *anyopaque, _: usize) anyerror!void {
            const self: *@This() = @ptrCast(@alignCast(ctx));
            try testing.expect(self.started);
        }
        fn wait(ctx: *anyopaque, _: usize) anyerror!void {
            const self: *@This() = @ptrCast(@alignCast(ctx));
            if (!self.filled) @memcpy(self.buf, self.src);
            self.filled = true;
        }
        fn done(_: *anyopaque, _: usize, _: usize) void {}
    };
    const buf = try gpa.alloc(u8, bytes.len);
    defer gpa.free(buf);
    // A lone integer key, whose union the dense fold may use: a reader's input reads it only where a choice can turn on
    // it. Past the probe's cap a reader's input plans from its footer, as it did with no probe.
    for ([_]?u64{ null, 4096 }) |cap| for ([_][]const u8{ "count(*) AS c", "count(*) AS c, sum(n) AS s" }) |aggregate| {
        @memset(buf, 0);
        var fake: Fake = .{ .src = bytes, .buf = buf };
        var a1 = std.heap.ArenaAllocator.init(gpa);
        defer a1.deinit();
        var mapped = try runMultiAggregate(gpa, a1.allocator(), .{
            .inputs = &.{.{ .name = "m", .bytes = bytes }},
            .metas = &.{meta},
            .aggregate = aggregate,
            .group_by = "n",
            .parallelism = 1,
        });
        defer mapped.deinit(gpa);
        var a2 = std.heap.ArenaAllocator.init(gpa);
        defer a2.deinit();
        var gated = try runMultiAggregate(gpa, a2.allocator(), .{
            .inputs = &.{.{
                .name = "g",
                .bytes = buf,
                .gate = .{ .ctx = &fake, .wait = Fake.wait, .done = Fake.done },
                .plan_bytes = bytes,
                .dict_wait = .{ .ctx = &fake, .wait = Fake.probed },
            }},
            .metas = &.{meta},
            .aggregate = aggregate,
            .group_by = "n",
            .parallelism = 1,
            .probe_cap = cap,
            .start_reads = .{ .ctx = &fake, .start = Fake.start },
            .dict_probe = .{ .ctx = &fake, .add = Fake.add },
        });
        defer gated.deinit(gpa);
        try testing.expect(mapped.reasons.contains(.dictionary_union));
        if (cap == null) {
            try testing.expect(fake.probes > 0);
            try testing.expectEqual(mapped.reasons, gated.reasons);
        } else {
            try testing.expectEqual(@as(usize, 0), fake.probes);
            try testing.expect(!gated.reasons.contains(.dictionary_union) and gated.reasons.contains(.unknown_ndv));
        }
        try expectSameAnswer(mapped, gated);
    };
}

test "runMultiAggregate: a LIMIT with no ORDER BY over few dictionary entries groups in one plain pass, same answer" {
    const gpa = testing.allocator;
    const bytes = try metadata.readFileSlice("ci/fixtures/parquet/topk_skew.parquet", gpa);
    defer gpa.free(bytes);
    for ([_]usize{ 1, 4 }) |j| {
        var arenas = [2]std.heap.ArenaAllocator{ .init(gpa), .init(gpa) };
        defer for (&arenas) |*a| a.deinit();
        var answers: [2]MultiAggResult = undefined;
        for (&answers, &arenas, [_]bool{ false, true }) |*ans, *arena, policy| {
            ans.* = try runMultiAggregate(gpa, arena.allocator(), .{
                .inputs = &.{.{ .name = "t", .bytes = bytes }},
                .aggregate = "count(*) AS c, sum(n) AS s",
                .group_by = "k",
                .parallelism = j,
                .min_bytes_per_worker = 1,
                .sketch_topk = .{},
                .result_order = .{ .limit = 5 },
                // Without the cost rules the first keys are cut as before.
                .ndv_policy = policy,
            });
        }
        defer for (&answers) |*a| a.deinit(gpa);
        try testing.expect(answers[0].reasons.contains(.first_keys_cut));
        try testing.expect(answers[1].reasons.contains(.few_dictionary_entries));
        // Rows arrive uncut, in first-appearance order: the first five are the cut's.
        const cut = answers[0].output.group_rows.?;
        const all = answers[1].output.group_rows.?;
        try testing.expect(all.len >= cut.len and cut.len >= 5);
        for (cut[0..5], all[0..5]) |w, g| for (w, g) |a, b| try expectSameAggValue(a, b);
    }
}

test "runMultiAggregate: whether a budget answers does not depend on the workers or their timing" {
    const gpa = testing.allocator;
    const bytes = try metadata.readFileSlice("ci/fixtures/parquet/repeat_keys.parquet", gpa);
    defer gpa.free(bytes);
    // Every key is in every row group: each of four workers' tables holds every key whichever row groups it claims,
    // one table each key once. Partitions answer sum; count(DISTINCT) has none, so it runs on one worker.
    for ([_][]const u8{ "sum(v) AS s", "count(DISTINCT v) AS s" }) |aggregate| {
        var args: MultiAggArgs = .{
            .inputs = &.{.{ .name = "r", .bytes = bytes }},
            .aggregate = aggregate,
            .group_by = "k",
            .result_order = .{ .keys = &.{.{ .column = 1, .descending = true }}, .limit = 5 },
            .min_bytes_per_worker = 1,
            .dense = false,
            .parallelism = 1,
        };
        var budgets: [40]usize = undefined;
        for (&budgets, 0..) |*b, i| b.* = (256 << 10) + i * (96 << 10);
        var want: [budgets.len](anyerror!usize) = undefined;
        outcomes(args, &budgets, &want);
        args.parallelism = 4;
        var rescued = false;
        for (0..8) |_| {
            var got: [budgets.len](anyerror!usize) = undefined;
            outcomes(args, &budgets, &got);
            try expectSameOutcomes(&want, &got);
        }
        // Some budget the four workers' own tables refuse, which the narrower reruns answer.
        for (budgets, want) |b, w| {
            _ = w catch continue;
            var arena = std.heap.ArenaAllocator.init(gpa);
            defer arena.deinit();
            var first = args;
            first.max_memory = b;
            var taken: std.EnumSet(FastPath) = .{};
            first.taken = &taken;
            if (prepareAggregate(gpa, arena.allocator(), first, false)) |r| freeResult(r) else |e| {
                try testing.expectEqual(error.ExceededMemoryBudget, e);
                rescued = true;
            }
        }
        try testing.expect(rescued);
    }
}

test "regression: a plain grouping a cost rule chose over a cut answers every budget the cut answers" {
    const gpa = testing.allocator;
    const Case = struct {
        path: []const u8,
        aggregate: []const u8,
        filter: ?[]const u8 = null,
        order: expr_ast.ResultOrder,
        /// The rule that groups plainly; null where the facts cannot bound its table and the cut stands.
        rule: ?Reason,
        /// Some budget refuses the rule's plain scan, which the cut answers.
        rescues: bool,
        budget0: usize,
        step: usize,
    };
    const top10: expr_ast.ResultOrder = .{ .keys = &.{.{ .column = 1, .descending = true }}, .limit = 10 };
    const skew = "ci/fixtures/parquet/skew_filter.parquet";
    const cases = [_]Case{
        // Few dictionary entries: plain grouping in place of the first keys' cut. Its small domain fits wherever the
        // union runs, so nothing refuses.
        .{
            .path = "ci/fixtures/parquet/topk_skew.parquet",
            .aggregate = "count(*) AS c, sum(n) AS s",
            .order = .{ .limit = 5 },
            .rule = .few_dictionary_entries,
            .rescues = false,
            .budget0 = 512 << 10,
            .step = 512 << 10,
        },
        // The bounds say `s = 5` passes a thousandth of the rows; nine in ten pass, over near-unique keys. Nothing
        // proves the filter selective or the groups few, and the first row group read passes nine in ten: the count
        // cut stays.
        .{
            .path = skew,
            .aggregate = "count(*) AS c",
            .filter = "s = 5",
            .order = top10,
            .rule = null,
            .rescues = false,
            .budget0 = 64 << 10,
            .step = 32 << 10,
        },
        // The bounds put `f > 994` at under 1%, and the first row group the scan reads passes that few, counted.
        .{
            .path = skew,
            .aggregate = "count(*) AS c",
            .filter = "f > 994",
            .order = top10,
            .rule = .selective_filter,
            .rescues = false,
            .budget0 = 32 << 10,
            .step = 8 << 10,
        },
        // The bounds prove `f = 0` passes one row group in 24, whose near-unique keys a tight budget refuses plainly.
        .{
            .path = skew,
            .aggregate = "count(*) AS c",
            .filter = "f = 0",
            .order = top10,
            .rule = .selective_filter,
            .rescues = true,
            .budget0 = 32 << 10,
            .step = 8 << 10,
        },
    };
    for (cases) |case| {
        const bytes = (try loadFixture(case.path)) orelse return error.SkipZigTest;
        defer gpa.free(bytes);
        var budgets: [24]usize = undefined;
        for (&budgets, 0..) |*b, i| b.* = case.budget0 + i * case.step;
        for ([_]usize{ 1, 4 }) |j| {
            const args: MultiAggArgs = .{
                .inputs = &.{.{ .name = case.path, .bytes = bytes }},
                .aggregate = case.aggregate,
                .group_by = "k",
                .filter = case.filter,
                .result_order = case.order,
                .parallelism = j,
                .min_bytes_per_worker = 1,
                .sketch_topk = .{ .cell_bits = 8, .admit_per_row = 1 },
            };
            {
                var arena = std.heap.ArenaAllocator.init(gpa);
                defer arena.deinit();
                var at = args;
                at.max_memory = budgets[budgets.len - 1];
                const plan = (try explainAggregate(gpa, arena.allocator(), at)).plan;
                if (case.rule) |r| try testing.expect(plan.reasons.contains(r)) else {
                    try testing.expect(!plan.reasons.contains(.selective_filter));
                }
            }
            var cut = args;
            cut.plain_rules = false;
            var want: [budgets.len]anyerror!usize = undefined;
            outcomes(cut, &budgets, &want);
            // The plain grouping may also answer budgets the cut refuses. On several workers whether the cut itself
            // answers depends on which worker claimed which row groups, and its refusal is final, so the outcomes are
            // compared on one; there both are the same every run.
            if (j == 1) {
                var got: [budgets.len]anyerror!usize = undefined;
                outcomes(args, &budgets, &got);
                for (want, got) |w, g| if (w) |n| try testing.expectEqual(n, try g) else |_| {};
            }
            if (!case.rescues) continue;
            var rescued = false;
            for (budgets, want) |b, w| {
                _ = w catch continue;
                var arena = std.heap.ArenaAllocator.init(gpa);
                defer arena.deinit();
                var first = args;
                first.max_memory = b;
                var taken: std.EnumSet(FastPath) = .{};
                first.taken = &taken;
                if (prepareAggregate(gpa, arena.allocator(), first, false)) |r| freeResult(r) else |e| {
                    try testing.expectEqual(error.ExceededMemoryBudget, e);
                    try testing.expect(taken.contains(.plain_rule));
                    rescued = true;
                }
            }
            try testing.expect(rescued);
        }
    }
}

test "runMultiAggregate: every refusal narrows, admitted runs retry shared partitions, within seven scans" {
    const S = std.EnumSet(FastPath);
    const Walk = struct {
        /// `plain`: a per-worker plain scan has run. `scans`: how many have.
        const State = struct { a: MultiAggArgs, chain: Chain, may_fit: bool, cut: bool, plain: bool, scans: usize };

        /// Whether a scan of `st` can arm `t`, as `prepareAggregate` arms paths: a cut arms none; shared partitions
        /// (the cost rules' or forced) arm no per-worker path; the dense fold excludes spill and pairs. A plan's
        /// facts are the same at every rerun (they read the mappings) but may differ from the first scan's (a
        /// reader's), so the cost rules' partitions come only in the first two scans, and a per-worker path armable
        /// at all was armed by the first per-worker plain scan.
        fn armable(st: State, t: S) bool {
            const a = st.a;
            if (t.contains(.integer_records)) return a.integer_records and a.typed_answer and t.count() == 1;
            if (st.cut) return t.count() == 0;
            if (t.contains(.partitioned) and st.scans > 1) return false;
            if (st.plain and (t.contains(.dense) or t.contains(.spill) or t.contains(.pairs))) return false;
            if (t.contains(.plain_rule) and !a.plain_rules) return false;
            if (t.contains(.partitioned) and (!a.partition_cost or a.partagg)) return false;
            if (t.contains(.private_runs) and (!a.private_runs or !t.contains(.partitioned))) return false;
            const shared = t.contains(.partitioned) or a.partagg;
            if (shared and (t.contains(.dense) or t.contains(.spill) or t.contains(.pairs) or
                t.contains(.partitions_fit))) return false;
            if (t.contains(.dense) and (!a.dense or t.contains(.spill) or t.contains(.pairs))) return false;
            if (t.contains(.spill) and !a.spill) return false;
            if (t.contains(.pairs) and a.distinct_pairs == .off) return false;
            if (t.contains(.worker_tables) and a.parallelism == 1) return false;
            if (t.contains(.merge_memory) and a.parallelism == 1) return false;
            return true;
        }

        /// What `narrowed` reads and changes of a state, as bits: equal keys walk the same.
        fn key(st: State) u16 {
            const a = st.a;
            var k: u16 = 0;
            for ([_]bool{
                a.dense,             a.spill,            a.distinct_pairs != .off, a.partition_cost,
                a.partagg,           a.parallelism == 1, a.plain_rules,            st.chain.parts_tried,
                st.may_fit,          st.cut,             st.plain,                 st.scans > 1,
                st.chain.lean_tried, a.private_runs,     a.integer_records,
            }, 0..) |b, i| k |= @as(u16, @intFromBool(b)) << @intCast(i);
            return k;
        }

        /// The most scans any sequence of refusals from `st` runs; every one of them ends.
        fn longest(args: MultiAggArgs, st: State, depth: usize) !usize {
            try testing.expect(depth < 9);
            var seen: [1 << 15]bool = @splat(false);
            var most: usize = 1;
            var candidates: [std.enums.values(FastPath).len]FastPath = undefined;
            var count: usize = 0;
            for (std.enums.values(FastPath)) |path| {
                // Independent executor prefixes are checked below this general-chain walk.
                if (path == .msd_records or path == .shared_strings or path == .grouped_distinct_bitmap) continue;
                if (path == .integer_records and (!st.a.integer_records or !st.a.typed_answer)) continue;
                candidates[count] = path;
                count += 1;
            }
            const all = candidates[0..count];
            for (0..@as(usize, 1) << @intCast(all.len)) |bits| {
                var t: S = .{};
                for (all, 0..) |path, i| if (bits >> @intCast(i) & 1 != 0) t.insert(path);
                if (!armable(st, t)) continue;
                var chain = st.chain;
                const may_fit = st.may_fit or t.contains(.one_table_may_fit);
                const next = narrowed(args, st.a, t, &chain, may_fit) orelse continue;
                const after: State = .{
                    .a = next,
                    .chain = chain,
                    .may_fit = may_fit,
                    .cut = st.cut or (t.contains(.plain_rule) and !t.contains(.private_runs)),
                    .plain = st.plain or !(t.contains(.integer_records) or t.contains(.partitioned) or
                        st.a.partagg or t.contains(.plain_rule)),
                    .scans = st.scans + 1,
                };
                if (seen[key(after)]) continue;
                seen[key(after)] = true;
                most = @max(most, 1 + try longest(args, after, depth + 1));
            }
            return most;
        }
    };
    const args: MultiAggArgs = .{ .inputs = &.{}, .aggregate = "", .parallelism = 4, .decode_limit = 256 << 20 };
    const start: Walk.State = .{
        .a = args,
        .chain = .{ .parts_tried = false },
        .may_fit = false,
        .cut = false,
        .plain = false,
        .scans = 0,
    };
    // The integer path is a single prefix to this general chain: it arms no other path and switches itself off.
    var integer_chain: Chain = .{ .parts_tried = false };
    var integer_args = args;
    integer_args.typed_answer = true;
    const declined = narrowed(integer_args, integer_args, S.initMany(&.{.integer_records}), &integer_chain, false).?;
    try testing.expect(!declined.integer_records and declined.parallelism == args.parallelism);
    try testing.expect(!integer_chain.parts_tried);
    var strings_chain: Chain = .{ .parts_tried = false };
    const strings_declined = narrowed(args, args, S.initMany(&.{.shared_strings}), &strings_chain, false).?;
    try testing.expect(!strings_declined.shared_strings);
    try testing.expectEqual(args.parallelism, strings_declined.parallelism);
    try testing.expectEqual(args.integer_records, strings_declined.integer_records);
    try testing.expectEqual(args.dense, strings_declined.dense);
    try testing.expect(!strings_chain.parts_tried);
    var msd_chain: Chain = .{ .parts_tried = false };
    const msd_declined = narrowed(args, args, S.initMany(&.{ .msd_records, .decode_memory }), &msd_chain, false).?;
    try testing.expect(!msd_declined.msd_records);
    try testing.expectEqual(args.parallelism, msd_declined.parallelism);
    try testing.expectEqual(args.integer_records, msd_declined.integer_records);
    try testing.expectEqual(args.byte_records, msd_declined.byte_records);
    var bitmap_chain: Chain = .{ .parts_tried = false };
    const bitmap_declined = narrowed(args, args, S.initMany(&.{ .grouped_distinct_bitmap, .decode_memory }), &bitmap_chain, false).?;
    try testing.expectEqual(grouped_bitmap.Mode.off, bitmap_declined.grouped_distinct_bitmap);
    try testing.expectEqual(args.parallelism, bitmap_declined.parallelism);
    try testing.expectEqual(args.integer_records, bitmap_declined.integer_records);
    try testing.expect(!bitmap_chain.parts_tried);
    // The cost rules' shared partitions, a fast path's mask, the plain tables, the forced partitions, one worker, and
    // one worker on a small decode budget.
    try testing.expectEqual(@as(usize, 7), try Walk.longest(args, start, 0));
    var forced = args;
    forced.partagg = true;
    // Forced partitions, one worker, one worker on a small decode budget.
    try testing.expectEqual(@as(usize, 3), try Walk.longest(forced, .{
        .a = forced,
        .chain = .{ .parts_tried = true },
        .may_fit = false,
        .cut = false,
        .plain = false,
        .scans = 0,
    }, 0));
    // Refused shared partitions leave the forced ones still to try, before one worker.
    var chain: Chain = .{ .parts_tried = false };
    const shared_retry = narrowed(args, args, S.initMany(&.{ .private_runs, .partitioned, .plain_rule }), &chain, true).?;
    try testing.expect(!shared_retry.private_runs and shared_retry.plain_rules and shared_retry.partition_cost);
    try testing.expectEqual(args.parallelism, shared_retry.parallelism);
    try testing.expect(!chain.parts_tried);
    const plain = narrowed(args, args, S.initMany(&.{ .partitioned, .worker_tables }), &chain, true).?;
    try testing.expect(!plain.partition_cost and !plain.partagg and plain.parallelism == 4);
    const parts = narrowed(args, plain, S.initMany(&.{ .worker_tables, .partitions_fit }), &chain, true).?;
    try testing.expect(parts.partagg and parts.parallelism == 4);
    const one = narrowed(args, parts, S.initMany(&.{.worker_tables}), &chain, true).?;
    try testing.expect(!one.partagg and one.parallelism == 1);
    try testing.expect(narrowed(args, one, .{}, &chain, true) == null);
    // Without copies of the same keys, one worker is not tried.
    chain = .{ .parts_tried = true };
    try testing.expect(narrowed(args, args, S.initMany(&.{.worker_tables}), &chain, false) == null);
    // A cost rule's plain grouping gives way to its cut, and nothing else changes.
    chain = .{ .parts_tried = false };
    const cut = narrowed(args, args, S.initMany(&.{ .plain_rule, .dense, .worker_tables }), &chain, false).?;
    try testing.expect(!cut.plain_rules and cut.dense and cut.parallelism == 4 and !chain.parts_tried);
    // Refused decode memory: one worker, without copies of the keys or any table at all; then a small decode budget
    // there; then the refusal is the query's.
    chain = .{ .parts_tried = true };
    const decoder = narrowed(args, args, S.initMany(&.{.decode_memory}), &chain, false).?;
    try testing.expect(decoder.parallelism == 1 and decoder.decode_limit.? == args.decode_limit.?);
    const lean = narrowed(args, decoder, S.initMany(&.{.decode_memory}), &chain, false).?;
    try testing.expect(lean.parallelism == 1 and !lean.charge_decode);
    try testing.expectEqual(core_memory.lean_decode / 2, lean.decode_limit.?);
    try testing.expect(chain.lean_tried);
    try testing.expect(narrowed(args, lean, S.initMany(&.{.decode_memory}), &chain, false) == null);
    // A refused merge: one worker, whose one table needs none; its own refusal is the query's.
    chain = .{ .parts_tried = true };
    const merged = narrowed(args, args, S.initMany(&.{.merge_memory}), &chain, false).?;
    try testing.expect(merged.parallelism == 1 and merged.charge_decode);
    try testing.expect(narrowed(args, merged, .{}, &chain, false) == null);
}

test "runMultiAggregate: a refusal says one worker may fit only where tables held copies or one worker plans a cut" {
    const gpa = testing.allocator;
    const top10: expr_ast.ResultOrder = .{ .keys = &.{.{ .column = 1, .descending = true }}, .limit = 10 };
    for ([_]struct { path: []const u8, order: expr_ast.ResultOrder = .{}, may_fit: bool }{
        // Every key in every row group.
        .{ .path = "ci/fixtures/parquet/repeat_keys.parquet", .may_fit = true },
        // Near-unique keys.
        .{ .path = "ci/fixtures/parquet/skew_filter.parquet", .may_fit = false },
        // The same, cut by their count: one worker's sketch cells fit where four workers' did not.
        .{ .path = "ci/fixtures/parquet/skew_filter.parquet", .order = top10, .may_fit = true },
    }) |case| {
        const bytes = (try loadFixture(case.path)) orelse return error.SkipZigTest;
        defer gpa.free(bytes);
        var arena = std.heap.ArenaAllocator.init(gpa);
        defer arena.deinit();
        var taken: std.EnumSet(FastPath) = .{};
        const refused = prepareAggregate(gpa, arena.allocator(), .{
            .inputs = &.{.{ .name = case.path, .bytes = bytes }},
            .aggregate = "count(*) AS c",
            .group_by = "k",
            .result_order = case.order,
            .sketch_topk = .{ .cell_bits = 12 },
            .parallelism = 4,
            .min_bytes_per_worker = 1,
            .max_memory = 256 << 10,
            .dense = false,
            .spill = false,
            .partition_cost = false,
            .taken = &taken,
        }, false);
        if (refused) |r| freeResult(r) else |e| try testing.expectEqual(error.ExceededMemoryBudget, e);
        try testing.expect(taken.contains(.worker_tables));
        try testing.expectEqual(case.may_fit, taken.contains(.one_table_may_fit));
    }
}

test "regression: counts pairs folded merge in place or under a count cut, as the sets' merge leaves them" {
    const path = "ci/fixtures/parquet/census_dictionary_review.parquet";
    const bytes = (try loadFixture(path)) orelse return error.SkipZigTest;
    defer testing.allocator.free(bytes);
    const inputs = [_]Input{.{ .name = path, .bytes = bytes }};
    // A full merge (in place), a cut by the count(DISTINCT) (`mergeCountCut`), and payloads beside it.
    const top: expr_ast.ResultOrder = .{ .keys = &.{.{ .column = 1, .descending = true }}, .limit = 7 };
    for ([_]struct { aggregate: []const u8, order: expr_ast.ResultOrder }{
        .{ .aggregate = "count(DISTINCT u) AS d", .order = .{} },
        .{ .aggregate = "count(DISTINCT u) AS d, count(*) AS c, sum(u) AS s", .order = .{} },
        .{ .aggregate = "count(DISTINCT u) AS d", .order = top },
        .{ .aggregate = "count(DISTINCT u) AS d, min(u) AS m", .order = top },
    }) |case| for ([_]usize{ 1, 3, 8 }) |j| {
        var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
        defer arena_state.deinit();
        var results: [2]MultiAggResult = undefined;
        for ([_]@FieldType(MultiAggArgs, "distinct_pairs"){ .off, .on }, &results) |pairs, *res| {
            res.* = try runMultiAggregate(testing.allocator, arena_state.allocator(), .{
                .inputs = &inputs,
                .aggregate = case.aggregate,
                .group_by = "t",
                .result_order = case.order,
                .parallelism = j,
                .min_bytes_per_worker = 1,
                .distinct_pairs = pairs,
            });
        }
        defer for (results) |r| freeResult(r);
        try testing.expect(!results[0].distinct_pairs and results[1].distinct_pairs);
        const sets = results[0].output.group_rows.?;
        const counted = results[1].output.group_rows.?;
        try testing.expectEqual(sets.len, counted.len);
        for (sets, counted) |r, p_row| for (r, p_row) |x, y| try expectSameAggValue(x, y);
    };
}



test "regression: count(DISTINCT) pairs pay by the values the sets took, a filter's dropped rows left out" {
    const path = "ci/fixtures/parquet/census_dictionary_review.parquet";
    const bytes = (try loadFixture(path)) orelse return error.SkipZigTest;
    defer testing.allocator.free(bytes);
    const inputs = [_]Input{.{ .name = path, .bytes = bytes }};
    var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_state.deinit();
    // Seven groups of near-unique values; the filter drops one row in eight, which the rows read counted.
    var results: [2]MultiAggResult = undefined;
    for ([_]@FieldType(MultiAggArgs, "distinct_pairs"){ .off, .auto }, &results) |pairs, *res| {
        res.* = try runMultiAggregate(testing.allocator, arena_state.allocator(), .{
            .inputs = &inputs,
            .aggregate = "count(DISTINCT u) AS d",
            .group_by = "t",
            .filter = "n = 1",
            .parallelism = 1,
            .min_bytes_per_worker = 1,
            .distinct_pairs = pairs,
            .pairs_pilot_rows = 1000,
            .ndv_policy = false,
        });
    }
    defer for (results) |r| freeResult(r);
    try testing.expect(results[1].distinct_pairs);
    const sets = results[0].output.group_rows.?;
    try testing.expectEqual(sets.len, results[1].output.group_rows.?.len);
    for (sets, results[1].output.group_rows.?) |r, p_row| for (r, p_row) |x, y| try expectSameAggValue(x, y);
}



test "a scalar query's string winner copy is drawn before it is made, so a refusal holds no copy past the ledger" {
    const path = "ci/fixtures/parquet/scalar_strings.parquet";
    const bytes = (try loadFixture(path)) orelse return error.SkipZigTest;
    defer testing.allocator.free(bytes);
    const inputs = [_]Input{.{ .name = path, .bytes = bytes }};
    // Each row group's 1 MiB value is a new winner of all three aggregates: three copies, one fold. Drawn only after
    // the fold, the third copy was made before the ledger refused it (and at 40 MiB a value, past the host's memory).
    for ([_]usize{ 1, 3 }) |j| {
        var ledger: core_memory.SharedBudget = .{ .limit = (5 << 20) / 2 };
        var gpa: LivePeak = .{ .child = testing.allocator };
        var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
        defer arena_state.deinit();
        const args: MultiAggArgs = .{
            .inputs = &inputs,
            .aggregate = "min(s) AS a, max(s) AS b, min(s) AS c",
            .parallelism = j,
            .max_memory = ledger.limit,
            .ledger = &ledger,
            .decode_limit = ledger.limit / 2,
            .min_bytes_per_worker = 1,
        };
        const refused = runMultiAggregate(gpa.allocator(), arena_state.allocator(), args);
        try testing.expectError(error.ExceededMemoryBudget, refused);
        try testing.expectEqual(@as(usize, 0), ledger.used.load(.monotonic));
        try testing.expectEqual(@as(usize, 0), gpa.live.load(.monotonic));
        if (j == 1) try testing.expect(gpa.peak.load(.monotonic) < 3 << 20);
    }
}

test "a scalar query's string winners are drawn from the query's ledger: past it the query refuses, holding nothing" {
    const path = "ci/fixtures/parquet/scalar_strings.parquet";
    const bytes = (try loadFixture(path)) orelse return error.SkipZigTest;
    defer testing.allocator.free(bytes);
    const inputs = [_]Input{.{ .name = path, .bytes = bytes }};
    for ([_]usize{ 1, 3 }) |j| {
        // Four row groups' winners of 1 MiB each, held until the merge, do not fit 3 MiB beside each other.
        var tight: core_memory.SharedBudget = .{ .limit = 3 << 20 };
        var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
        defer arena_state.deinit();
        const args: MultiAggArgs = .{
            .inputs = &inputs,
            .aggregate = "min(s) AS m",
            .parallelism = j,
            .max_memory = tight.limit,
            .ledger = &tight,
            .decode_limit = tight.limit / 2,
            .min_bytes_per_worker = 1,
        };
        const refused = runMultiAggregate(testing.allocator, arena_state.allocator(), args);
        try testing.expectError(error.ExceededMemoryBudget, refused);
        try testing.expectEqual(@as(usize, 0), tight.used.load(.monotonic));
        // With room for them the answer is the least string, and what stays drawn is its winner and its copy.
        var roomy: core_memory.SharedBudget = .{ .limit = 64 << 20 };
        var wide = args;
        wide.max_memory = roomy.limit;
        wide.ledger = &roomy;
        wide.decode_limit = roomy.limit / 2;
        const res = try runMultiAggregate(testing.allocator, arena_state.allocator(), wide);
        defer freeResult(res);
        const got = res.output.aggs[0].value.s;
        try testing.expectEqual(@as(usize, 1 << 20), got.len);
        try testing.expectEqual(@as(u8, 'a'), got[0]);
        try testing.expect(roomy.used.load(.monotonic) >= 2 << 20);
    }
}

test "partagg: admitted long composite counts match shared partitions after a spawn failure" {
    const bytes = (try loadFixture(full_match_fixture)) orelse return error.SkipZigTest;
    defer testing.allocator.free(bytes);
    // Repeat a small fixture to cross the planning row gate; a short probe makes the first local window unique.
    var inputs: [48]Input = undefined;
    for (&inputs) |*in| in.* = .{ .name = full_match_fixture, .bytes = bytes };
    for ([_]usize{ 3, 16 }) |j| {
        var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
        defer arena_state.deinit();
        var taken: std.EnumSet(FastPath) = .{};
        var args: MultiAggArgs = .{
            .inputs = &inputs,
            .aggregate = "count(*) AS c",
            .group_by = "s, ts",
            .parallelism = j,
            .min_bytes_per_worker = 1,
            .max_memory = 128 << 20,
            .part_cap = 8,
            .part_probe = 8,
            .private_runs = false,
        };
        const want = try prepareAggregate(testing.allocator, arena_state.allocator(), args, false);
        defer freeResult(want);
        args.private_runs = true;
        args.taken = &taken;
        const got = try prepareAggregate(testing.allocator, arena_state.allocator(), args, false);
        defer freeResult(got);
        try testing.expect(taken.contains(.private_runs));
        try expectSameAnswer(want, got);
        spawn_util.injectFailureAfter(1);
        defer spawn_util.resetFailure();
        const failed = try prepareAggregate(testing.allocator, arena_state.allocator(), args, false);
        defer freeResult(failed);
        try testing.expect(spawn_util.failuresInjected() > 0);
        try expectSameAnswer(want, failed);
    }
}

test "stream decode probes release failed arenas and finish before the source gate returns its bytes" {
    const path = "ci/fixtures/parquet/plain_values_corrupt.parquet";
    const original = (try loadFixture(path)) orelse return error.SkipZigTest;
    defer testing.allocator.free(original);
    var meta = try metadata.open(testing.allocator, original);
    defer meta.deinit(testing.allocator);
    const metas = [_]schema.FileMetaData{meta};
    const Gate = struct {
        data: []u8,
        returned: std.atomic.Value(usize) = .init(0),
        fn wait(_: *anyopaque, _: usize) anyerror!void {}
        fn done(ctx: *anyopaque, _: usize, items: usize) void {
            const g: *@This() = @ptrCast(@alignCast(ctx));
            if (g.returned.fetchAdd(1, .acq_rel) + 1 == items) @memset(g.data, 0);
        }
    };
    for ([_]usize{ 1, 2, 4, 16 }) |j| for ([_]usize{ 64 << 10, 1 << 20 }) |limit| for ([_]bool{ false, true }) |provided| {
        const data = try testing.allocator.dupe(u8, original);
        defer testing.allocator.free(data);
        var gate: Gate = .{ .data = data };
        const inputs = [_]Input{.{
            .name = "s3://fixture/corrupt.parquet",
            .bytes = data,
            .gate = .{ .ctx = &gate, .wait = Gate.wait, .done = Gate.done },
        }};
        var arena = std.heap.ArenaAllocator.init(testing.allocator);
        defer arena.deinit();
        var ledger: core_memory.SharedBudget = .{ .limit = limit };
        var diag: Diag = .{};
        const result = runMultiAggregate(testing.allocator, arena.allocator(), .{
            .inputs = &inputs,
            .metas = &metas,
            .aggregate = "min(s) AS lo,max(s) AS hi",
            .parallelism = j,
            .min_bytes_per_worker = 1,
            .max_memory = limit,
            .decode_limit = limit / 2,
            .ledger = if (provided) &ledger else null,
            .diag = &diag,
            .scan_all = true,
        });
        try testing.expectError(error.UnexpectedPage, result);
        try testing.expectEqual(@as(usize, 0), ledger.used.load(.monotonic));
        try testing.expect(ledger.peak.load(.monotonic) <= limit);
        try testing.expect(gate.returned.load(.monotonic) > 0);
        try testing.expect(std.mem.allEqual(u8, data, 0));
        try testing.expectEqualStrings("s", diag.detail.decode.column.get());
        try testing.expectEqual(@as(?u32, 0), diag.detail.decode.page);
    };
}

test "optional filter sampling declines corrupt values so the full projection chooses the failure" {
    const path = "ci/fixtures/parquet/plain_values_corrupt.parquet";
    const bytes = (try loadFixture(path)) orelse return error.SkipZigTest;
    defer testing.allocator.free(bytes);
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const metas = [_]schema.FileMetaData{try metadata.open(a, bytes)};
    const calls = [_]expr_agg.AggCall{.{ .func = .count, .arg = null, .where = null, .alias = "n", .result = .i64 }};
    const filter = try filter_parser.parse(a, "s = 'a'", &metas[0], null);
    const plan: agg_plan.Plan = try .init(a, .{ .filter = filter, .calls = &calls, .group_keys = null }, &metas, .fromFlags(true, false));
    const inputs = [_]Input{.{ .name = path, .bytes = bytes }};
    const work = [_]WorkItem{.{
        .file = 0,
        .rg = 0,
        .agg_start = 0,
        .agg_len = 1,
        .columns = plan.columns,
        .accumulators = &.{},
    }};
    try testing.expectEqual(@as(?f64, 1), try sampledPassShare(testing.allocator, a, &plan, &metas, &inputs, &work, .{ .fast_levels = true }, null));
}

test "worker refusal races replay an earlier corrupt row group in either completion order" {
    const bytes = (try loadFixture("ci/fixtures/parquet/plain_values_corrupt.parquet")) orelse return error.SkipZigTest;
    defer testing.allocator.free(bytes);
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var meta = try metadata.open(a, bytes);
    const rgs = try a.alloc(schema.RowGroup, 2);
    @memset(rgs, meta.row_groups.items[0]);
    meta.row_groups.items = rgs;
    meta.num_rows *= 2;
    const metas = [_]schema.FileMetaData{meta};
    const inputs = [_]Input{.{ .name = "race.parquet", .bytes = bytes }};
    const work = [_]WorkItem{
        .{ .file = 0, .rg = 0, .agg_start = 0, .agg_len = 1, .columns = undefined, .accumulators = &.{} },
        .{ .file = 0, .rg = 1, .agg_start = 0, .agg_len = 1, .columns = undefined, .accumulators = &.{} },
    };
    const Publisher = struct {
        fn run(w: *Worker, turn: *std.atomic.Value(u32), rank: u32, err: anyerror) void {
            while (true) {
                const seen = turn.load(.acquire);
                if (seen > rank) return; // A failed thread spawn cancels the other publisher.
                if (seen == rank) break;
                blocking.io.futexWaitUncancelable(u32, &turn.raw, seen);
            }
            w.err = err;
            turn.store(rank + 1, .release);
            blocking.io.futexWake(u32, &turn.raw, std.math.maxInt(u32));
        }
    };
    for (0..2) |refusal_worker| for (0..2) |first_completion| {
        var cursor: std.atomic.Value(usize) = .init(0);
        var workers: [2]Worker = undefined;
        for (&workers, 0..) |*w, i| w.* = .{
            .gpa = testing.allocator,
            .inputs = &inputs,
            .metas = &metas,
            .work = &work,
            .cursor = &cursor,
            .agg_calls = &.{},
            .plan = undefined,
            .decode_options = .{},
            .failure_work = if (i == refusal_worker) 0 else 1,
            // A refusal may retain the previous decoder's marker. Its work item is authoritative.
            .failure = .{ .rg = &rgs[1], .column = 1, .first_row = 0, .projection = 0 },
        };
        var turn: std.atomic.Value(u32) = .init(0);
        const t0 = try std.Thread.spawn(.{}, Publisher.run, .{
            &workers[0],                                                                                  &turn, @as(u32, @intCast(first_completion)),
            @as(anyerror, if (refusal_worker == 0) error.ExceededMemoryBudget else error.UnexpectedPage),
        });
        const t1 = std.Thread.spawn(.{}, Publisher.run, .{
            &workers[1],                                                                                  &turn, @as(u32, @intCast(1 - first_completion)),
            @as(anyerror, if (refusal_worker == 1) error.ExceededMemoryBudget else error.UnexpectedPage),
        }) catch |err| {
            turn.store(2, .release);
            blocking.io.futexWake(u32, &turn.raw, std.math.maxInt(u32));
            t0.join();
            return err;
        };
        t0.join();
        t1.join();
        const refusal = firstWorkerFailure(&workers, null).?;
        try testing.expectEqual(error.ExceededMemoryBudget, refusal.err);
        try testing.expect(refusal.site == null);
        try clearWorkerErrors(&workers, error.ExceededMemoryBudget);
        try testing.expect(phaseFailure(&workers) == null);
        workers[refusal_worker].err = error.CensusAbandoned;
        workers[1 - refusal_worker].err = error.UnexpectedPage;
        try testing.expectEqual(error.CensusAbandoned, phaseFailure(&workers).?);
        try clearWorkerErrors(&workers, error.CensusAbandoned);
        try testing.expect(phaseFailure(&workers) == null);
        workers[refusal_worker].err = error.ExceededMemoryBudget;
        workers[1 - refusal_worker].err = error.UnexpectedPage;
        // An unlocated refusal is equally conservative, including a refusal before any decoder starts.
        workers[refusal_worker].failure_work = null;
        try testing.expectEqual(error.ExceededMemoryBudget, firstWorkerFailure(&workers, null).?.err);
        // A known later refusal must not suppress an earlier confirmed decode error.
        workers[refusal_worker].failure_work = 1;
        workers[1 - refusal_worker].failure_work = 0;
        workers[1 - refusal_worker].failure.?.rg = &rgs[0];
        try testing.expectEqual(error.UnexpectedPage, firstWorkerFailure(&workers, null).?.err);

        workers[refusal_worker].err = error.EarlierFailure;
        workers[refusal_worker].failure_work = 0;
        workers[1 - refusal_worker].err = error.LaterFailure;
        workers[1 - refusal_worker].failure_work = 1;
        try testing.expectError(error.EarlierFailure, clearWorkerErrors(&workers, error.CensusAbandoned));

        var chain: Chain = .{ .parts_tried = false };
        var diag: Diag = .{};
        const args: MultiAggArgs = .{
            .inputs = &inputs,
            .metas = &metas,
            .aggregate = "min(s)",
            .parallelism = 2,
            .min_bytes_per_worker = 1,
            .decode_limit = 1 << 20,
            .max_memory = 2 << 20,
            .scan_all = true,
            .diag = &diag,
        };
        const replay = narrowed(args, args, std.EnumSet(FastPath).initMany(&.{.decode_memory}), &chain, false).?;
        try testing.expectEqual(@as(usize, 1), replay.parallelism);
        try testing.expectError(error.UnexpectedPage, runMultiAggregate(testing.allocator, a, replay));
        try testing.expectEqual(@as(usize, 0), diag.detail.decode.row_group);
        try testing.expectEqual(@as(?u32, 0), diag.detail.decode.page);
        try testing.expectEqualStrings("s", diag.detail.decode.column.get());
    };
}

test "packed byte SUM selection matches the established rows across ties, offsets, windows and workers" {
    const gpa = testing.allocator;
    var arena = std.heap.ArenaAllocator.init(gpa);
    defer arena.deinit();
    const a = arena.allocator();
    const n = 4000;
    const keys = try a.alloc([]const u8, n);
    const defs = try a.alloc(u32, n);
    const values = try a.alloc(i64, n);
    const vdefs = try a.alloc(u32, n);
    var dictionary: [103][]const u8 = undefined;
    for (&dictionary, 0..) |*key, i| key.* = switch (i) {
        0 => "ignored",
        1 => "",
        2 => "\x00",
        3 => "a\x00",
        4 => "0123456789abcde",
        else => try std.fmt.allocPrint(a, "key-{d:0>6}", .{i}),
    };
    const Capture = struct {
        bytes: std.ArrayList(u8) = .empty,
        a: std.mem.Allocator,
        fn write(ctx: *anyopaque, bytes: []const u8) anyerror!void {
            const c: *@This() = @ptrCast(@alignCast(ctx));
            try c.bytes.appendSlice(c.a, bytes);
        }
    };
    for (0..3) |shape| {
        for (keys, defs, values, vdefs, 0..) |*key, *def, *v, *vd, i| {
            const g = (i * 37) % dictionary.len;
            key.* = dictionary[g];
            def.* = @intFromBool(g != 0);
            vd.* = @intFromBool(g != 6 and i % 19 != 0);
            v.* = switch (shape) {
                0 => @as(i64, @intCast((i * 31) % 101)) - 50,
                1 => 1,
                else => if (i % 2 == 0) std.math.maxInt(i64) else -1234567,
            };
        }
        const columns = [_]expr_agg.OutputCol{
            .{
                .name = "k",
                .col = .{ .string = .{ .values = keys, .def_levels = defs, .max_def = 1 } },
                .parquet_type = .BYTE_ARRAY,
                .nullable = true,
            },
            .{
                .name = "v",
                .col = .{ .i64 = .{ .values = values, .def_levels = vdefs, .max_def = 1 } },
                .parquet_type = .INT64,
                .nullable = true,
            },
        };
        var capture: Capture = .{ .a = a };
        _ = try consumer.writeAggregateRows(
            a,
            .{ .ctx = &capture, .write_fn = Capture.write },
            &columns,
            n,
            .SNAPPY,
            .{},
            .{},
        );
        var meta = try metadata.open(a, capture.bytes.items);
        const groups = try a.alloc(schema.RowGroup, 16);
        @memset(groups, meta.row_groups.items[0]);
        meta.row_groups.items = groups;
        meta.num_rows *= 16;
        const inputs = [_]Input{.{ .name = "packed-keys.parquet", .bytes = capture.bytes.items }};
        const base: MultiAggArgs = .{
            .inputs = &inputs,
            .metas = &.{meta},
            .aggregate = "sum(v) AS s",
            .group_by = "k",
            .parallelism = 1,
            .min_bytes_per_worker = 1,
            .max_memory = 512 << 20,
            .integer_records = false,
            .msd_records = false,
            .result_order = .{
                .keys = &.{.{ .column = 1, .descending = true }},
                .limit = 7,
                .offset = 3,
            },
        };
        if (shape == 0) for ([_]usize{ 1, 16 }) |j| for ([_]u64{ 0, 7 }) |offset| {
            var args = base;
            var taken: std.EnumSet(FastPath) = .{};
            args.parallelism = j;
            args.byte_records = true;
            args.byte_records_min_rows = 0;
            args.taken = &taken;
            args.result_order.limit = 0;
            args.result_order.offset = offset;
            const explanation = try explainAggregate(gpa, a, args);
            try testing.expect(explanation.record_path == null);
            var got = try prepareAggregate(gpa, a, args, false);
            defer got.deinit(gpa);
            try testing.expect(!taken.contains(.integer_records));
            try got.output.applyResultOrder(gpa, args.result_order);
            try testing.expectEqual(@as(usize, 0), got.output.group_rows.?.len);
        };
        for ([_]bool{ false, true }) |descending| {
            var ordinary = base;
            ordinary.result_order.keys = &.{.{ .column = 1, .descending = descending }};
            var want = try runMultiAggregate(gpa, a, ordinary);
            defer want.deinit(gpa);
            try want.output.applyResultOrder(gpa, ordinary.result_order);
            for ([_]usize{ 1, 2, 4, 8, 16 }) |j| for ([_]bool{ false, true }) |encoded|
                for ([_]usize{ 0, 64 }) |batch_rows| {
                    var budget: core_memory.SharedBudget = .{ .limit = base.max_memory };
                    var taken: std.EnumSet(FastPath) = .{};
                    var args = ordinary;
                    args.parallelism = j;
                    args.encoded = encoded;
                    args.byte_records = true;
                    args.byte_records_min_rows = 0;
                    args.byte_top_partitions = 256;
                    args.byte_top_batch_rows = batch_rows;
                    args.unordered = j > 1;
                    args.ledger = &budget;
                    args.taken = &taken;
                    const explanation = try explainAggregate(gpa, a, args);
                    try testing.expectEqual(.byte_sum_top_h, explanation.record_path.?.layout);
                    var got = try prepareAggregate(gpa, a, args, false);
                    defer got.deinit(gpa);
                    try testing.expect(taken.contains(.integer_records));
                    try got.output.applyResultOrder(gpa, args.result_order);
                    try testing.expectEqualDeep(want.output.group_cols.?, got.output.group_cols.?);
                    try testing.expectEqualDeep(want.output.group_rows.?, got.output.group_rows.?);
                    try testing.expect(budget.used.load(.monotonic) < 8192);
                };
        }
        if (shape == 0) {
            const corrupt = try a.dupe(u8, capture.bytes.items);
            for (meta.row_groups.items[0].columns.items) |column| {
                const cm = column.meta_data.?;
                const offset: usize = @intCast(cm.data_page_offset);
                var reader = thrift_mod.Reader.init(corrupt[offset..]);
                const header = try schema.PageHeader.read(&reader);
                const body = offset + reader.pos;
                @memset(corrupt[body..][0..@min(12, @as(usize, @intCast(header.compressed_page_size)))], 0xff);
            }
            const bad_inputs = [_]Input{.{ .name = "packed-errors.parquet", .bytes = corrupt }};
            var bad = base;
            bad.inputs = &bad_inputs;
            bad.byte_records = true;
            bad.byte_records_min_rows = 0;
            try checkRecordDecodeModes(gpa, a, bad, .integer_records, 0);
            bad.column_order = "s,k";
            bad.result_order.keys = &.{.{ .column = 0, .descending = true }};
            try checkRecordDecodeModes(gpa, a, bad, .integer_records, 1);
        }
    }
}

test "retained MSD composite count matches ordered rows across workers, encodings and budgets" {
    try testing.expect(!consumer.isDecodeError(error.RetainedOwnershipViolation));
    const gpa = testing.allocator;
    var arena = std.heap.ArenaAllocator.init(gpa);
    defer arena.deinit();
    const a = arena.allocator();
    const n = 40000;
    const keys = try a.alloc([]const u8, n);
    const defs = try a.alloc(u32, n);
    const ints = try a.alloc(i32, n);
    const fdefs = try a.alloc(u32, n);
    var dictionary: [200][]const u8 = undefined;
    for (&dictionary, 0..) |*key, i| {
        const len = if (i == 1) 0 else (i * 37) % 400;
        const b = try a.alloc(u8, len);
        for (b, 0..) |*v, j| v.* = @truncate(i * 73 + j * 13);
        if (len >= 20) @memcpy(b[0..20], "https://example.org/");
        key.* = b;
    }
    for (keys, defs, ints, fdefs, 0..) |*key, *d, *v, *fd, i| {
        const group = (i * 37) % dictionary.len;
        key.* = dictionary[group];
        d.* = @intFromBool(group % 19 != 0);
        v.* = @as(i32, @intCast(group)) * 100003 - 7000000;
        fd.* = @intFromBool(group % 23 != 0);
    }
    const cols = [_]expr_agg.OutputCol{
        .{ .name = "k", .col = .{ .string = .{ .values = keys, .def_levels = defs, .max_def = 1 } }, .parquet_type = .BYTE_ARRAY, .nullable = true },
        .{ .name = "f", .col = .{ .i32 = .{ .values = ints, .def_levels = fdefs, .max_def = 1 } }, .parquet_type = .INT32, .nullable = true },
    };
    const Capture = struct {
        a: std.mem.Allocator,
        bytes: std.ArrayList(u8) = .empty,
        fn write(ctx: *anyopaque, data: []const u8) anyerror!void {
            const c: *@This() = @ptrCast(@alignCast(ctx));
            try c.bytes.appendSlice(c.a, data);
        }
    };
    for ([_]schema.CompressionCodec{ .SNAPPY, .UNCOMPRESSED }) |codec| {
        var capture: Capture = .{ .a = a };
        _ = try consumer.writeAggregateRows(a, .{ .ctx = &capture, .write_fn = Capture.write }, &cols, n, codec, .{}, .{});
        var meta = try metadata.open(a, capture.bytes.items);
        const rgs = try a.alloc(schema.RowGroup, 16);
        @memset(rgs, meta.row_groups.items[0]);
        // Reusing an immutable chunk gives sixteen independent logical row groups without a large fixture file.
        meta.row_groups.items = rgs;
        meta.num_rows = 16 * n;
        const inputs = [_]Input{.{ .name = "composite.parquet", .bytes = capture.bytes.items }};
        const base: MultiAggArgs = .{ .inputs = &inputs, .metas = &.{meta}, .aggregate = "count(*) AS c", .group_by = "k,f", .result_order = .{ .keys = &.{.{ .column = 2, .descending = true }}, .limit = 10 }, .max_memory = 512 << 20, .parallelism = 1, .min_bytes_per_worker = 1, .msd_records = false };
        var want = try runMultiAggregate(gpa, a, base);
        defer want.deinit(gpa);
        try want.output.applyResultOrder(gpa, base.result_order);
        for ([_]usize{ 1, 2, 4, 8, 16 }) |j| for ([_]usize{ 96 << 20, 512 << 20 }) |limit| for ([_]bool{ false, true }) |encoded| {
            var budget: core_memory.SharedBudget = .{ .limit = limit };
            defer std.debug.assert(budget.used.load(.monotonic) < 1 << 20);
            var args = base;
            args.parallelism = j;
            args.max_memory = limit;
            args.ledger = &budget;
            args.msd_records = true;
            args.msd_records_min_rows = 0;
            args.encoded = encoded;
            const explanation = try explainAggregate(gpa, a, args);
            if (limit == 512 << 20) {
                try testing.expectEqual(@as(usize, j), explanation.workers);
                try testing.expectEqual(.msd_count_top_h, explanation.record_path.?.layout);
            } else try testing.expect(explanation.record_path == null);
            var got = try runMultiAggregate(gpa, a, args);
            defer got.deinit(gpa);
            try got.output.applyResultOrder(gpa, args.result_order);
            try testing.expectEqualDeep(want.output.group_cols.?, got.output.group_cols.?);
            try testing.expectEqualDeep(want.output.group_rows.?, got.output.group_rows.?);
        };
        {
            var relaxed = base;
            relaxed.msd_records = true;
            relaxed.msd_records_min_rows = 0;
            relaxed.parallelism = 16;
            relaxed.unordered = true;
            const explanation = try explainAggregate(gpa, a, relaxed);
            try testing.expectEqual(.msd_count_top_h, explanation.record_path.?.layout);
            var got = try runMultiAggregate(gpa, a, relaxed);
            defer got.deinit(gpa);
            try got.output.applyResultOrder(gpa, relaxed.result_order);
            try testing.expectEqualDeep(want.output.group_rows.?, got.output.group_rows.?);
        }
        if (codec == .SNAPPY) {
            // An earlier secondary-column page wins over the late key page in every decode/admission mode.
            // Keep its header valid so planning admits MSD and the failure actually reaches record workers.
            var plain_capture: Capture = .{ .a = a };
            try plain_capture.bytes.appendSlice(a, capture.bytes.items);
            var plain_meta = try metadata.open(a, capture.bytes.items);
            const encoder = @import("../writer/encoder.zig");
            var key_plan = try encoder.plan(a, .{ .values = cols[0].col, .schema_elem = &plain_meta.schema.items[1], .path_in_schema = &.{"k"}, .codec = codec, .distinct = true });
            key_plan.values_encoding = .PLAIN;
            const key_pages = try a.alloc(encoder.BuiltPage, key_plan.pageCount());
            for (key_pages, 0..) |*pg, i| pg.* = try key_plan.page(a, i);
            var encoded_key = try key_plan.finish(a, key_pages);
            encoded_key.meta.data_page_offset += @intCast(plain_capture.bytes.items.len);
            try plain_capture.bytes.appendSlice(a, encoded_key.bytes);
            // Supplied metadata permits a compact synthetic input: the replacement chunk follows the old footer.
            plain_meta.row_groups.items[0].columns.items[0].meta_data = encoded_key.meta;
            const key_cm = plain_meta.row_groups.items[0].columns.items[0].meta_data.?;
            const f_cm = plain_meta.row_groups.items[0].columns.items[1].meta_data.?;
            try testing.expect(key_cm.dictionary_page_offset == null);
            const key_end: usize = @intCast(key_cm.data_page_offset + key_cm.total_compressed_size);
            @memset(plain_capture.bytes.items[key_end - 32 .. key_end], 0xff);
            const f_start: usize = @intCast(f_cm.data_page_offset);
            var f_header = thrift_mod.Reader.init(plain_capture.bytes.items[f_start..]);
            const fh = try schema.PageHeader.read(&f_header);
            const f_body = f_start + f_header.pos;
            const damaged = @min(12, @as(usize, @intCast(fh.compressed_page_size)));
            @memset(plain_capture.bytes.items[f_body..][0..damaged], 0xff);
            const plain_rgs = try a.alloc(schema.RowGroup, 16);
            @memset(plain_rgs, plain_meta.row_groups.items[0]);
            plain_meta.row_groups.items = plain_rgs;
            plain_meta.num_rows = n * 16;
            const bad_inputs = [_]Input{.{ .name = "late-key-error.parquet", .bytes = plain_capture.bytes.items }};
            var bad = base;
            bad.inputs = &bad_inputs;
            bad.metas = &.{plain_meta};
            try checkRecordDecodeModes(gpa, a, bad, .msd_records, 1);
        }
        {
            // A large LIMIT can require more owned output than the fixed floor. Reject its conservative bound
            // before consumption, even when the actual answer later turns out small enough for the old path.
            var large = base;
            large.result_order.limit = 1000;
            var expected = try runMultiAggregate(gpa, a, large);
            defer expected.deinit(gpa);
            try expected.output.applyResultOrder(gpa, large.result_order);
            large.msd_records = true;
            large.msd_records_min_rows = 0;
            const explanation = try explainAggregate(gpa, a, large);
            try testing.expect(explanation.record_path == null);
            var got = try runMultiAggregate(gpa, a, large);
            defer got.deinit(gpa);
            try got.output.applyResultOrder(gpa, large.result_order);
            try testing.expectEqualDeep(expected.output.group_rows.?, got.output.group_rows.?);
        }
        if (codec == .SNAPPY) {
            // The footer lies about a late chunk: the established reader follows its DELTA page headers. MSD
            // must refuse before copying expansion, then replay after earlier groups have already been emitted.
            const encoder = @import("../writer/encoder.zig");
            var late: Capture = .{ .a = a };
            try late.bytes.appendSlice(a, capture.bytes.items);
            var lied = try metadata.open(a, capture.bytes.items);
            var delta = try encoder.plan(a, .{ .values = cols[0].col, .schema_elem = &lied.schema.items[1], .path_in_schema = &.{"k"}, .codec = codec, .distinct = true });
            delta.values_encoding = .DELTA_BYTE_ARRAY;
            const pages = try a.alloc(encoder.BuiltPage, delta.pageCount());
            for (pages, 0..) |*p, i| p.* = try delta.page(a, i);
            var encoded = try delta.finish(a, pages);
            encoded.meta.data_page_offset += @intCast(late.bytes.items.len);
            encoded.meta.encodings.items = try a.dupe(schema.Encoding, &.{ .PLAIN, .RLE });
            encoded.meta.encoding_stats = null;
            try late.bytes.appendSlice(a, encoded.bytes);
            const late_rgs = try a.alloc(schema.RowGroup, 16);
            @memset(late_rgs, lied.row_groups.items[0]);
            late_rgs[15].columns.items = try a.dupe(schema.ColumnChunk, lied.row_groups.items[0].columns.items);
            late_rgs[15].columns.items[0].meta_data = encoded.meta;
            lied.row_groups.items = late_rgs;
            lied.num_rows = n * 16;
            const late_inputs = [_]Input{.{ .name = "late-encoding.parquet", .bytes = late.bytes.items }};
            for ([_]usize{ 1, 2, 4, 8, 16 }) |j| {
                var budget: core_memory.SharedBudget = .{ .limit = 1 << 30 };
                var args = base;
                args.inputs = &late_inputs;
                args.metas = &.{lied};
                args.max_memory = budget.limit;
                args.ledger = &budget;
                args.parallelism = j;
                args.msd_records = true;
                args.msd_records_min_rows = 0;
                const explanation = try explainAggregate(gpa, a, args);
                try testing.expectEqual(.msd_count_top_h, explanation.record_path.?.layout);
                var taken: std.EnumSet(FastPath) = .{};
                args.taken = &taken;
                try testing.expectError(error.ExceededMemoryBudget, prepareAggregate(gpa, a, args, false));
                try testing.expect(taken.contains(.msd_records));
                try testing.expectEqual(@as(usize, 0), budget.used.load(.monotonic));
                var got = try runMultiAggregate(gpa, a, args);
                defer got.deinit(gpa);
                try got.output.applyResultOrder(gpa, args.result_order);
                try testing.expectEqualDeep(want.output.group_rows.?, got.output.group_rows.?);
            }
        }
        const Coordinator = struct {
            started: bool = false,
            planned: usize = 0,
            loan: usize = 0,
            forced: std.atomic.Value(bool) = .init(false),
            budget: *core_memory.SharedBudget,
            remote: bool,
            abandoned: usize = 0,
            fn start(ctx: *anyopaque) anyerror!void {
                const c: *@This() = @ptrCast(@alignCast(ctx));
                c.started = true;
            }
            fn plannedRead(ctx: *anyopaque) void {
                const c: *@This() = @ptrCast(@alignCast(ctx));
                c.planned += 1;
            }
            fn probe(ctx: *anyopaque, _: usize, _: usize, _: u64, _: u64) anyerror!void {
                const c: *@This() = @ptrCast(@alignCast(ctx));
                try testing.expect(!c.started);
            }
            fn waitProbe(ctx: *anyopaque, _: usize) anyerror!void {
                const c: *@This() = @ptrCast(@alignCast(ctx));
                try testing.expect(c.started);
            }
            fn abandon(ctx: *anyopaque) void {
                const c: *@This() = @ptrCast(@alignCast(ctx));
                c.abandoned += 1;
                c.budget.release(c.loan);
                c.loan = 0;
            }
        };
        const Window = struct {
            bytes: []u8,
            coordinator: *Coordinator,
            waits: usize = 0,
            done: usize = 0,
            refuse_at: ?usize = null,
            fn wait(ctx: *anyopaque, _: usize) anyerror!void {
                const w: *@This() = @ptrCast(@alignCast(ctx));
                const c = w.coordinator;
                try testing.expect(c.started);
                w.waits += 1;
                if (w.refuse_at) |at| if (w.waits == at) return error.ExceededMemoryBudget;
                if (!c.remote and c.forced.cmpxchgStrong(false, true, .monotonic, .monotonic) == null) {
                    const used = c.budget.used.load(.monotonic);
                    try testing.expect(used > 64 << 20);
                    c.loan = c.budget.limit - used;
                    c.budget.force(c.loan);
                }
            }
            fn release(ctx: *anyopaque, _: usize, _: usize) void {
                const w: *@This() = @ptrCast(@alignCast(ctx));
                w.done += 1;
                @memset(w.bytes, 0);
            }
        };
        var single = meta;
        single.row_groups.items = rgs[0..1];
        single.num_rows = n;
        for ([_]bool{ false, true }) |remote| {
            var budget: core_memory.SharedBudget = .{ .limit = 512 << 20 };
            var coordinator: Coordinator = .{ .budget = &budget, .remote = remote };
            var windows = [_]Window{
                .{ .bytes = try a.dupe(u8, capture.bytes.items), .coordinator = &coordinator },
                .{ .bytes = try a.dupe(u8, capture.bytes.items), .coordinator = &coordinator },
            };
            var window_inputs: [2]Input = undefined;
            for (&window_inputs, &windows) |*input, *window| input.* = .{
                .name = if (remote) "s3://bucket/window.parquet" else "window.parquet",
                .bytes = window.bytes,
                .plan_bytes = if (remote) null else capture.bytes.items,
                .gate = .{ .ctx = window, .wait = Window.wait, .done = Window.release },
                .dict_wait = .{ .ctx = &coordinator, .wait = Coordinator.waitProbe },
            };
            var args = base;
            args.inputs = &window_inputs;
            args.metas = &.{ single, single };
            args.parallelism = 2;
            args.msd_records = true;
            args.msd_records_min_rows = 0;
            args.ledger = &budget;
            args.start_reads = .{ .ctx = &coordinator, .start = Coordinator.start, .planned = Coordinator.plannedRead };
            args.dict_probe = .{ .ctx = &coordinator, .add = Coordinator.probe };
            var got = try runMultiAggregate(gpa, a, args);
            defer got.deinit(gpa);
            budget.release(coordinator.loan);
            try got.output.applyResultOrder(gpa, args.result_order);
            try testing.expectEqual(@as(usize, 1), coordinator.planned);
            for (windows) |window| {
                try testing.expectEqual(@as(usize, 1), window.waits);
                try testing.expectEqual(@as(usize, 1), window.done);
            }
            for (want.output.group_rows.?, got.output.group_rows.?) |expected, actual| {
                try testing.expectEqualDeep(expected[0..2], actual[0..2]);
                try testing.expectEqual(expected[2].i, actual[2].i * 8);
            }
            try testing.expect(budget.used.load(.monotonic) < 1 << 20);
        }
        {
            // The first row group has emitted retained records before the next worker admission refuses.
            // The retry must abandon the old window, release every charge, and read the original mapping.
            var budget: core_memory.SharedBudget = .{ .limit = 512 << 20 };
            var coordinator: Coordinator = .{ .budget = &budget, .remote = false };
            var window: Window = .{ .bytes = try a.dupe(u8, capture.bytes.items), .coordinator = &coordinator, .refuse_at = 2 };
            const replay = [_]Input{.{ .name = "replay.parquet", .bytes = window.bytes, .plan_bytes = capture.bytes.items, .gate = .{ .ctx = &window, .wait = Window.wait, .done = Window.release }, .dict_wait = .{ .ctx = &coordinator, .wait = Coordinator.waitProbe } }};
            var args = base;
            args.inputs = &replay;
            args.msd_records = true;
            args.msd_records_min_rows = 0;
            args.ledger = &budget;
            args.start_reads = .{ .ctx = &coordinator, .start = Coordinator.start, .planned = Coordinator.plannedRead, .abandon = Coordinator.abandon };
            args.dict_probe = .{ .ctx = &coordinator, .add = Coordinator.probe };
            var got = try runMultiAggregate(gpa, a, args);
            defer got.deinit(gpa);
            try got.output.applyResultOrder(gpa, args.result_order);
            try testing.expectEqual(@as(usize, 1), coordinator.abandoned);
            try testing.expectEqualDeep(want.output.group_rows.?, got.output.group_rows.?);
            try testing.expect(budget.used.load(.monotonic) < 1 << 20);
        }
    }
}



test "inline integer decode errors follow projection ties across windows, decline and mapped replay" {
    const gpa = testing.allocator;
    var arena = std.heap.ArenaAllocator.init(gpa);
    defer arena.deinit();
    const a = arena.allocator();
    const values = try a.alloc(i64, 40000);
    const defs = try a.alloc(u32, values.len);
    for (values, defs, 0..) |*v, *d, i| {
        v.* = @intCast(i % 503);
        d.* = @intFromBool(i % 11 != 0);
    }
    const cols = [_]expr_agg.OutputCol{
        .{
            .name = "n",
            .col = .{ .i64 = .{ .values = values, .def_levels = defs, .max_def = 1 } },
            .parquet_type = .INT64,
            .nullable = true,
        },
        .{
            .name = "v",
            .col = .{ .i64 = .{ .values = values, .def_levels = defs, .max_def = 1 } },
            .parquet_type = .INT64,
            .nullable = true,
        },
    };
    const Capture = struct {
        a: std.mem.Allocator,
        bytes: std.ArrayList(u8) = .empty,
        fn write(ctx: *anyopaque, data: []const u8) anyerror!void {
            const c: *@This() = @ptrCast(@alignCast(ctx));
            try c.bytes.appendSlice(c.a, data);
        }
    };
    var capture: Capture = .{ .a = a };
    _ = try consumer.writeAggregateRows(
        a,
        .{ .ctx = &capture, .write_fn = Capture.write },
        &cols,
        @intCast(values.len),
        .UNCOMPRESSED,
        .{},
        .{},
    );
    const bad = capture.bytes.items;
    var meta = try metadata.open(a, bad);
    var key: usize = 0;
    var value: usize = 0;
    for (meta.row_groups.items[0].columns.items, 0..) |chunk, ci| {
        const cm = chunk.meta_data.?;
        const name = cm.path_in_schema.items[0];
        if (std.mem.eql(u8, name, "n")) key = ci else if (std.mem.eql(u8, name, "v")) value = ci else continue;
        var header = thrift_mod.Reader.init(bad[@intCast(cm.data_page_offset)..]);
        const page = try schema.PageHeader.read(&header);
        const body = @as(usize, @intCast(cm.data_page_offset)) + header.pos;
        @memset(bad[body..][0..@min(12, @as(usize, @intCast(page.compressed_page_size)))], 0xff);
    }
    const rgs = try a.alloc(schema.RowGroup, 16);
    @memset(rgs, meta.row_groups.items[0]);
    meta.row_groups.items = rgs;
    meta.num_rows = rgs[0].num_rows * 16;
    const inputs = [_]Input{.{ .name = "integer-projection-errors.parquet", .bytes = bad }};
    var args: MultiAggArgs = .{
        .inputs = &inputs,
        .metas = &.{meta},
        .aggregate = "count(*) AS c, sum(v) AS s",
        .group_by = "n",
        .typed_answer = true,
        .min_bytes_per_worker = 1,
        .scan_all = true,
    };
    try checkRecordDecodeModes(gpa, a, args, .integer_records, key);
    args.column_order = "s,n,c";
    try checkRecordDecodeModes(gpa, a, args, .integer_records, value);
}

test "unordered keeps exact inline integer and byte answers and bounded SUM selection" {
    const bytes = (try loadFixture("ci/fixtures/parquet/topk_skew.parquet")) orelse return error.SkipZigTest;
    defer testing.allocator.free(bytes);
    const inputs = [_]Input{.{ .name = "topk_skew.parquet", .bytes = bytes }};
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const order_keys = [_]expr_ast.OrderKey{.{ .column = 1, .descending = true }};
    for (0..3) |shape| for ([_]usize{ 1, 2, 4, 8, 16 }) |j| {
        var args: MultiAggArgs = .{
            .inputs = &inputs,
            .aggregate = if (shape == 2) "sum(n) AS s" else "count(*) AS c,sum(n) AS s",
            .group_by = if (shape == 1) "k" else "n",
            .result_order = if (shape == 2) .{ .keys = &order_keys, .limit = 7, .offset = 3 } else .{},
            .max_memory = 512 << 20,
            .parallelism = j,
            .min_bytes_per_worker = 1,
            .typed_answer = true,
            .integer_records = true,
            .integer_records_min_rows = 0,
            .byte_records = true,
            .byte_records_min_rows = 0,
        };
        const ordered = try explainAggregate(testing.allocator, a, args);
        try testing.expect(ordered.record_path != null);
        var want = try prepareAggregate(testing.allocator, a, args, false);
        defer want.deinit(testing.allocator);
        args.unordered = true;
        const relaxed = try explainAggregate(testing.allocator, a, args);
        try testing.expectEqual(ordered.record_path.?.layout, relaxed.record_path.?.layout);
        var taken: std.EnumSet(FastPath) = .{};
        args.taken = &taken;
        var got = try prepareAggregate(testing.allocator, a, args, false);
        defer got.deinit(testing.allocator);
        try testing.expect(taken.contains(.integer_records));
        if (shape == 0) {
            const expected = want.output.integer_stream.?;
            const actual = got.output.integer_stream.?;
            try testing.expectEqual(expected.state.group_count, actual.state.group_count);
            const hi = @min(expected.state.group_count, 137);
            var x = try expected.window(testing.allocator, want.state.group_col_types.?, 0, hi, j);
            defer x.deinit(testing.allocator);
            var y = try actual.window(testing.allocator, got.state.group_col_types.?, 0, hi, j);
            defer y.deinit(testing.allocator);
            try testing.expectEqualDeep(x.columns, y.columns);
        } else if (shape == 1) {
            const expected = want.output.byte_stream.?;
            const actual = got.output.byte_stream.?;
            try testing.expectEqual(expected.state.group_count, actual.state.group_count);
            const hi = @min(expected.state.group_count, 137);
            var x = try expected.window(testing.allocator, 0, hi, j);
            defer x.deinit(testing.allocator);
            var y = try actual.window(testing.allocator, 0, hi, j);
            defer y.deinit(testing.allocator);
            try testing.expectEqualDeep(x.columns, y.columns);
        } else {
            try testing.expectEqualDeep(want.output.group_rows.?, got.output.group_rows.?);
        }
    };
}

test "canonical full projection prefix includes count arguments and skipped admit items" {
    const bytes = (try loadFixture("ci/fixtures/parquet/plain_values_corrupt.parquet")) orelse return error.SkipZigTest;
    defer testing.allocator.free(bytes);
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var meta = try metadata.open(a, bytes);
    const groups = try a.alloc(schema.RowGroup, 3);
    @memset(groups, meta.row_groups.items[0]);
    meta.row_groups.items = groups;
    const metas = [_]schema.FileMetaData{meta};
    const inputs = [_]Input{.{ .name = "omitted.parquet", .bytes = bytes }};
    const key = try expr_parser.parseExprOnly(a, "b", &meta, null);
    const arg = try expr_parser.parseExprOnly(a, "s", &meta, null);
    const calls = [_]expr_agg.AggCall{.{ .func = .min, .arg = arg, .where = null, .alias = "lo", .result = .bytes }};
    const keys = [_]expr_ast.Expr{key};
    const plan = try agg_plan.Plan.init(a, .{ .calls = &calls, .group_keys = &keys, .filter = null }, &metas, .ignore);
    const count_columns = try plan.forCalls(a, &.{});
    try testing.expect(!count_columns.read[1]);
    const work = [_]WorkItem{.{ .file = 0, .rg = 2, .agg_start = 0, .agg_len = 1, .columns = count_columns, .accumulators = &.{} }};
    var w: Worker = undefined;
    w.plan = &plan;
    w.metas = &metas;
    w.inputs = &inputs;
    w.work = &work;
    w.failed_item = 0;
    w.failure = .{ .rg = &groups[2], .column = 1, .page = 0 };
    w.err = error.UnexpectedPage;
    w.decode_options = .{};
    w.agg_calls = &calls;
    w.group_by_keys = &keys;
    w.error_group_keys = null;
    w.error_outputs = null;
    // The worker sees only RG2, with a count-only projection. Neither controls the canonical replay.
    const found = canonicalFailurePrefix(testing.allocator, FailureProbe.contextOf(&w));
    try testing.expectEqual(error.UnexpectedPage, found.err);
    try testing.expectEqual(&groups[0], found.site.rg);
    try testing.expectEqual(@as(usize, 1), found.site.column);
    // Force actual count passes to encounter a later corrupt key after skipping the earlier corrupt argument.
    const last_columns = try a.dupe(schema.ColumnChunk, groups[2].columns.items);
    const old_b = last_columns[0].meta_data.?;
    last_columns[0].meta_data = last_columns[1].meta_data;
    last_columns[0].meta_data.?.type = .INT32;
    last_columns[0].meta_data.?.path_in_schema = old_b.path_in_schema;
    groups[2].columns.items = last_columns;
    const selected = try selectList(a, &.{ "b", "count(*) AS n", "min(s) AS lo" });
    for ([_]usize{ 1, 2, 4, 16 }) |j| for ([_]usize{ 64 << 10, 1 << 20 }) |limit| {
        for ([_]?bool{ null, false, true }) |fuse| {
            var diag: Diag = .{};
            const args: MultiAggArgs = .{
                .inputs = &inputs,
                .metas = &metas,
                .aggregate = "count(*) AS n,min(s) AS lo",
                .group_by = "b",
                .select_cols = selected,
                .scan_all = true,
                .ndv_policy = false,
                .plain_rules = false,
                .parallelism = j,
                .min_bytes_per_worker = 1,
                .max_memory = limit,
                .decode_limit = limit / 2,
                .result_order = .{ .keys = &.{.{ .column = 1, .descending = true }}, .limit = 1 },
                .sketch_topk = .{ .cell_bits = 8, .admit_per_row = 1, .fuse_census = fuse },
                .diag = &diag,
            };
            const ex = try explainAggregate(testing.allocator, a, args);
            try testing.expectEqual(.sketch, ex.plan.dispatch.strategy);
            try testing.expectError(error.UnexpectedPage, runMultiAggregate(testing.allocator, a, args));
            try testing.expectEqual(@as(usize, 0), diag.detail.decode.row_group);
            try testing.expectEqualStrings("s", diag.detail.decode.column.get());
        }
    };
    // An earlier valid RG must contribute no invented marker. The next omitted RG still wins.
    const first_columns = try a.dupe(schema.ColumnChunk, groups[0].columns.items);
    first_columns[1].meta_data.?.num_values = 0;
    groups[0].columns.items = first_columns;
    const next = canonicalFailurePrefix(testing.allocator, FailureProbe.contextOf(&w));
    try testing.expectEqual(&groups[1], next.site.rg);
}

