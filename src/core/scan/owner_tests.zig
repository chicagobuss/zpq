const std = @import("std");
const thrift_mod = @import("../thrift.zig");
const spawn_util = @import("../spawn.zig");
const huge_pages = @import("../huge_pages.zig");
const schema = @import("../schema.zig");
const consumer = @import("../consumer.zig");
const agg_plan = @import("../agg_plan.zig");
const metadata = @import("../parquet/metadata.zig");
const expr_ast = @import("../expr/ast.zig");
const expr_agg = @import("../expr/agg.zig");
const core_memory = @import("../memory.zig");
const partagg = @import("../partagg.zig");
const string_table = @import("../string_table.zig");
const hw = @import("../hw.zig");
const eval = @import("../filter/eval.zig");
const ndv = @import("../ndv.zig");
const agg_dense = @import("../expr/agg_dense.zig");
const owner_groups = @import("../owner_groups.zig");
const filter_parser = @import("../filter/parser.zig");
const worker_contracts = @import("worker_contracts.zig");
const Worker = worker_contracts.Worker;
const loadFixture = @import("test_support.zig").loadFixture;
const scan_worker = @import("worker.zig");
const WORKER_REGION_RESERVE = scan_worker.WORKER_REGION_RESERVE;
const releaseRegionsParallel = scan_worker.releaseRegionsParallel;
const plan_types = @import("plan_types.zig");
const RecordAdmission = plan_types.RecordAdmission;
const scan_count_cut = @import("count_cut.zig");
const MergedSlot = scan_count_cut.MergedSlot;
const keepCountTop = scan_count_cut.keepCountTop;
const scan_rank_cut = @import("rank_cut.zig");
const ResultCut = scan_rank_cut.ResultCut;
const scan_result = @import("result.zig");
const MultiAggResult = scan_result.MultiAggResult;
const scan_plan_impl = @import("plan.zig");
const sampledPassShare = scan_plan_impl.sampledPassShare;
const scan_rows = @import("rows.zig");
const answerBytes = scan_rows.answerBytes;
const answerBytesParallel = scan_rows.answerBytesParallel;
const slot_sort = @import("slot_sort.zig");
const sortSlotsByFirst = slot_sort.sortSlotsByFirst;
const pair_fold = @import("pair_fold.zig");
const foldDistinctPairs = pair_fold.foldDistinctPairs;
const scan_merge = @import("merge.zig");
const MergeGroupRef = scan_merge.MergeGroupRef;
const mergeCandidates = scan_merge.mergeCandidates;
const finishGroupRanges = scan_merge.finishGroupRanges;
const freeAccumulatorRanges = scan_merge.freeAccumulatorRanges;
const mergeFoldedRefs = scan_merge.mergeFoldedRefs;
const firstCandidatesParallel = scan_merge.firstCandidatesParallel;
const scan_execute = @import("execute.zig");
const runMultiAggregate = scan_execute.runMultiAggregate;
const Chain = scan_execute.Chain;
const narrowed = scan_execute.narrowed;
const explainAggregate = scan_execute.explainAggregate;
const prepareAggregate = scan_execute.prepareAggregate;
const integration_support = @import("integration_support.zig");
const rewritten = integration_support.rewritten;
const setMergeTestString = integration_support.setMergeTestString;
const LivePeak = integration_support.LivePeak;
const ReleasingScanWindow = integration_support.ReleasingScanWindow;
const ownedGroupOutputBytes = integration_support.ownedGroupOutputBytes;
const selectedIdentityReplayTrial = integration_support.selectedIdentityReplayTrial;
const scan_types = @import("types.zig");
const MultiAggArgs = scan_types.MultiAggArgs;
const SketchTopK = scan_types.SketchTopK;
const Input = scan_types.Input;
const FastPath = scan_types.FastPath;
const GroupColType = scan_types.GroupColType;
const WorkItem = scan_types.WorkItem;
const integer_stream = @import("integer_stream.zig");
const typed_answer = @import("typed_answer.zig");
const scan_output = @import("output.zig");
const AggOutput = scan_output.AggOutput;
const output_values = @import("output_values.zig");
const AggValue = output_values.AggValue;
const testing = std.testing;
const test_fixtures = @import("../parquet/test_fixtures.zig");

test "COUNT-only composite top-k keeps requested workers outside DISTINCT owners" {
    const profile = @import("../../profile.zig");
    const gpa = testing.allocator;
    var arena = std.heap.ArenaAllocator.init(gpa);
    defer arena.deinit();
    const a = arena.allocator();
    var users: [256]i64 = undefined;
    var terms: [256][]const u8 = undefined;
    var user_defs: [256]u32 = undefined;
    var term_defs: [256]u32 = undefined;
    for (&users, &terms, &user_defs, &term_defs, 0..) |*user, *term, *ud, *td, i| {
        user.* = if (i < 16) 7 else @intCast(i % 17);
        term.* = if (i < 16) "" else try std.fmt.allocPrint(a, "term-{d}", .{i});
        ud.* = @intFromBool(i % 37 != 0);
        td.* = @intFromBool(i % 41 != 0);
    }
    const columns = [_]expr_agg.OutputCol{
        .{ .name = "UserID", .parquet_type = .INT64, .nullable = true, .col = .{ .i64 = .{ .values = &users, .def_levels = &user_defs, .max_def = 1 } } },
        .{ .name = "SearchPhrase", .parquet_type = .BYTE_ARRAY, .nullable = true, .col = .{ .string = .{ .values = &terms, .def_levels = &term_defs, .max_def = 1 } } },
    };
    var sink: @import("../writer/streaming.zig").MemorySink = .{ .arena = a };
    _ = try consumer.writeAggregateRows(a, sink.sink(), &columns, users.len, .SNAPPY, .{}, .{});
    var meta = try metadata.open(a, sink.buffer.items);
    const groups = try a.alloc(schema.RowGroup, 64);
    @memset(groups, meta.row_groups.items[0]);
    meta.row_groups.items = groups;
    meta.num_rows *= @intCast(groups.len);
    var args: MultiAggArgs = .{
        .inputs = &.{.{ .name = "count-composite", .bytes = sink.buffer.items }},
        .metas = &.{meta},
        .aggregate = "count(*) AS c",
        .group_by = "UserID,SearchPhrase",
        .parallelism = 1,
        .min_bytes_per_worker = 1,
        .max_memory = 256 << 20,
        .integer_records_min_rows = 0,
        .ndv_policy = false,
        .plain_rules = false,
        .distinct_pairs = .on,
        .sketch_topk = .{ .cell_bits = 12, .fuse_census = true },
        .result_order = .{ .keys = &.{.{ .column = 2, .descending = true }}, .limit = 10 },
    };
    var old = args;
    old.integer_records = false;
    old.distinct_pairs = .off;
    old.sketch_topk = null;
    var want = try runMultiAggregate(gpa, a, old);
    defer want.deinit(gpa);
    try want.output.applyResultOrder(gpa, args.result_order);
    for ([_]usize{ 1, 16, 32 }) |workers| {
        const profiler = try profile.Profiler.create(gpa, .{ .sample_interval_ns = 0 }, .{});
        defer profiler.destroy();
        args.profiler = profiler;
        args.parallelism = workers;
        const explained = try explainAggregate(gpa, a, args);
        try testing.expectEqual(workers, explained.workers);
        try testing.expectEqual(.sketch, explained.plan.dispatch.strategy);
        if (explained.record_path) |record| try testing.expect(record.layout != .pair_distinct);
        var got = try runMultiAggregate(gpa, a, args);
        defer got.deinit(gpa);
        try got.output.applyResultOrder(gpa, args.result_order);
        try testing.expectEqualDeep(want.output.group_cols, got.output.group_cols);
        try testing.expectEqualDeep(want.output.group_rows, got.output.group_rows);
        profiler.finish();
        try testing.expectEqual(@as(u64, 0), profiler.dropped());
        var seen: u64 = 0;
        var thread = profiler.head.load(.acquire);
        while (thread) |t| : (thread = t.next) {
            var events: profile.EventIterator = .{ .t = t };
            while (events.next()) |batch| for (batch) |event| if (event.kind == .scan_worker) {
                try testing.expect(event.args[0] < workers);
                seen |= @as(u64, 1) << @as(u6, @intCast(event.args[0]));
            };
        }
        try testing.expectEqual(workers, @as(usize, @popCount(seen)));
    }
}

test "integer owners prepay and retain duplicated output names in the query ledger" {
    const gpa = testing.allocator;
    var arena = std.heap.ArenaAllocator.init(gpa);
    defer arena.deinit();
    const a = arena.allocator();
    var sink: @import("../writer/streaming.zig").MemorySink = .{ .arena = a };
    var file = try consumer.AggregateFile.init(a, sink.sink());
    defer file.deinit();
    const columns = [_]expr_agg.OutputCol{
        .{
            .name = "k",
            .parquet_type = .INT64,
            .nullable = false,
            .col = .{ .i64 = .{ .values = &.{ 1, 1, 2, 2, 3, 3, 4, 4 } } },
        },
        .{
            .name = "v",
            .parquet_type = .INT64,
            .nullable = false,
            .col = .{ .i64 = .{ .values = &.{ 2, -1, 4, 3, -5, 1, 7, 6 } } },
        },
    };
    try file.append(&columns, 8, .SNAPPY, .{}, .{});
    _ = try file.finish(&columns);
    const meta = try metadata.open(a, sink.buffer.items);
    const input = [_]Input{.{ .name = "owner-names.parquet", .bytes = sink.buffer.items }};
    const long_name: [4096]u8 = @splat('x');
    for (0..3) |mode| {
        const aggregate = try std.fmt.allocPrint(a, "{s} AS {s}{s}", .{
            if (mode == 1) "count(*)" else "sum(v)", &long_name, if (mode == 2) ",count(*) AS n" else "",
        });
        var budget: core_memory.SharedBudget = .{ .limit = 512 << 20 };
        var taken: std.EnumSet(FastPath) = .{};
        const args: MultiAggArgs = .{
            .inputs = &input,
            .metas = (&meta)[0..1],
            .aggregate = aggregate,
            .group_by = "k",
            .integer_records = true,
            .integer_records_min_rows = 0,
            .typed_answer = true,
            .ndv_policy = false,
            .byte_records = false,
            .msd_records = false,
            .plain_rules = false,
            .scan_all = true,
            .ledger = &budget,
            .max_memory = budget.limit,
            .taken = &taken,
            .result_order = if (mode == 2) .{} else .{
                .keys = &.{.{ .column = 1, .descending = true }},
                .limit = 2,
            },
        };
        const explanation = try explainAggregate(gpa, a, args);
        try testing.expect(explanation.record_path != null);
        var got = try prepareAggregate(gpa, a, args, false);
        try testing.expect(taken.contains(.integer_records));
        if (mode == 2) {
            const state = got.output.integer_stream.?.state;
            var name_bytes = got.output.group_cols.?.len * @sizeOf([]const u8);
            for (got.output.group_cols.?) |name| name_bytes += name.len;
            try testing.expectEqual(name_bytes, state.output_credit);
            try testing.expectEqual(explanation.record_path.?.reservation, state.charged);
            try testing.expectEqual(state.charged, budget.used.load(.monotonic));
            got.deinit(gpa);
            try testing.expectEqual(@as(usize, 0), budget.used.load(.monotonic));
        } else {
            defer got.deinit(gpa);
            try testing.expectEqual(ownedGroupOutputBytes(got.output), budget.used.load(.monotonic));
        }
    }
}

test "filtered COUNT record owners consume only survivors on indexed mapped and releasing reads" {
    const gpa = testing.allocator;
    var held = std.heap.ArenaAllocator.init(gpa);
    defer held.deinit();
    const a = held.allocator();
    const bytes = (try loadFixture("ci/fixtures/parquet/filtered_records_indexed.parquet")) orelse
        return error.SkipZigTest;
    defer gpa.free(bytes);
    const meta = try metadata.open(a, bytes);
    const mapped = [_]Input{.{ .name = "selected-indexed.parquet", .bytes = bytes }};
    const Ready = struct {
        fn wait(_: *anyopaque, _: usize) anyerror!void {}
        fn probe(_: *anyopaque, _: usize, _: usize, _: u64, _: u64) anyerror!void {}
    };
    for ([_][]const u8{ "f < 160 AND (b IS NULL OR b != '')", "f > 1 AND f < 1" }, 0..) |filter, empty|
        for ([_][]const u8{ "k", "b", "b,f", "k,f" }, 0..) |keys, mode| {
            const base: MultiAggArgs = .{
                .inputs = &mapped,
                .metas = &.{meta},
                .aggregate = "count(*) AS c",
                .group_by = keys,
                .filter = filter,
                .scan_all = empty != 0,
                .integer_records = false,
                .byte_records = false,
                .msd_records = false,
                .dense = false,
                .ndv_policy = false,
                .plain_rules = false,
                .sketch_topk = null,
                .min_bytes_per_worker = 1,
                .max_memory = 512 << 20,
                .result_order = .{
                    .keys = &.{.{ .column = if (mode < 2) 1 else 2, .descending = true }},
                    .limit = 3,
                    .offset = 1,
                },
            };
            var want = try runMultiAggregate(gpa, a, base);
            defer want.deinit(gpa);
            try want.output.applyResultOrder(gpa, base.result_order);
            if (empty == 0)
                try testing.expect(want.rows_matched > 0 and want.rows_matched < want.rows_in)
            else
                try testing.expectEqual(@as(i64, 0), want.rows_matched);
            for (1..17) |j| for ([_]usize{ 0, 64 }) |batch| for ([_]bool{ false, true }) |gated| {
                if (empty != 0 and j != 1 and j != 16) continue;
                var trial = std.heap.ArenaAllocator.init(gpa);
                defer trial.deinit();
                const ta = trial.allocator();
                var window: ReleasingScanWindow = .{ .bytes = try ta.dupe(u8, bytes), .meta = &meta };
                var input = window.input(bytes);
                input.dict_wait = .{ .ctx = &window, .wait = Ready.wait };
                var budget: core_memory.SharedBudget = .{ .limit = base.max_memory };
                var taken: std.EnumSet(FastPath) = .{};
                var args = base;
                args.inputs = if (gated) (&input)[0..1] else &mapped;
                args.parallelism = j;
                args.decode_batch_rows = batch;
                args.integer_records = mode == 0 or mode == 3;
                args.integer_records_min_rows = 0;
                args.byte_records = mode == 1;
                args.byte_records_min_rows = 0;
                args.msd_records = mode == 2;
                args.msd_records_min_rows = 0;
                args.dense = true;
                args.ndv_policy = true;
                args.plain_rules = false;
                args.sketch_topk = .{ .cell_bits = 8, .admit_per_row = 1 };
                args.ledger = &budget;
                args.taken = &taken;
                if (gated) {
                    args.dict_probe = .{ .ctx = &window, .add = Ready.probe };
                    args.start_reads = .{
                        .ctx = &window,
                        .start = ReleasingScanWindow.start,
                        .abandon = ReleasingScanWindow.abandon,
                    };
                }
                const explanation = try explainAggregate(gpa, ta, args);
                const selected_owner = mode == 3 and explanation.record_path.?.layout == .packed_count_top_h;
                try testing.expectEqual(
                    @as(@FieldType(RecordAdmission, "layout"), if (mode == 1) .byte_count_top_h else if (mode == 2)
                        .msd_count_top_h
                    else if (selected_owner)
                        .packed_count_top_h
                    else
                        .integer_count_top_h),
                    explanation.record_path.?.layout,
                );
                var got = try prepareAggregate(gpa, ta, args, false);
                defer got.deinit(gpa);
                try testing.expect(taken.contains(if (mode == 2) .msd_records else .integer_records));
                try testing.expectEqual(want.rows_in, got.rows_in);
                try testing.expectEqual(want.rows_scanned, got.rows_scanned);
                try testing.expectEqual(want.rows_matched, got.rows_matched);
                try testing.expectEqual(want.row_groups_pruned, got.row_groups_pruned);
                try testing.expectEqual(want.row_groups_full_match, got.row_groups_full_match);
                const schema_credit = if (selected_owner)
                    got.state.group_col_types.?.len * @sizeOf(GroupColType) +
                        explanation.workers * (@sizeOf(Worker) + @sizeOf(expr_agg.GroupTable))
                else
                    0;
                try testing.expectEqual(ownedGroupOutputBytes(got.output) + schema_credit, budget.used.load(.monotonic));
                try got.output.applyResultOrder(gpa, args.result_order);
                try testing.expectEqualDeep(want.output.group_rows orelse &.{}, got.output.group_rows orelse &.{});
                if (gated) {
                    try testing.expectEqual(meta.row_groups.items.len, window.releases.load(.monotonic));
                    try testing.expectEqual(@as(usize, 0), window.abandoned);
                }
            };
        };
}

test "unfiltered integer COUNT owns raw nullable keys without a dictionary union across readers and workers" {
    const gpa = testing.allocator;
    var held = std.heap.ArenaAllocator.init(gpa);
    defer held.deinit();
    const a = held.allocator();
    var sink: @import("../writer/streaming.zig").MemorySink = .{ .arena = a };
    var file = try consumer.AggregateFile.init(a, sink.sink());
    defer file.deinit();
    var columns = [_]expr_agg.OutputCol{.{
        .name = "k",
        .parquet_type = .INT64,
        .nullable = true,
        .col = .{ .i64 = .{ .values = &.{} } },
    }};
    for (0..16) |rg| {
        var values: [257]i64 = undefined;
        var defs: [257]u32 = undefined;
        for (&values, &defs, 0..) |*v, *d, r| {
            v.* = @as(i64, @intCast((r * 37 + rg) % 193)) * (1 << 40) - (1 << 50);
            d.* = @intFromBool((r + rg) % 17 != 0);
        }
        columns[0].col = .{ .i64 = .{ .values = &values, .def_levels = &defs, .max_def = 1 } };
        try file.append(&columns, values.len, .SNAPPY, .{}, .{});
    }
    _ = try file.finish(&columns);
    const meta = try metadata.open(a, sink.buffer.items);
    const mapped = [_]Input{.{ .name = "raw-count.parquet", .bytes = sink.buffer.items }};
    const base: MultiAggArgs = .{
        .inputs = &mapped,
        .metas = &.{meta},
        .aggregate = "count(*) AS c",
        .group_by = "k",
        .integer_records = false,
        .integer_records_min_rows = 1,
        .byte_records = false,
        .msd_records = false,
        .ndv_policy = false,
        .dense = false,
        .sketch_topk = null,
        .min_bytes_per_worker = 1,
        .max_memory = 128 << 20,
        .result_order = .{ .keys = &.{.{ .column = 1, .descending = true }}, .limit = 10, .offset = 2 },
    };
    var want = try runMultiAggregate(gpa, a, base);
    defer want.deinit(gpa);
    try want.output.applyResultOrder(gpa, base.result_order);
    for (1..17) |j| for ([_]usize{ 0, 64 }) |batch| for ([_]bool{ false, true }) |gated| {
        var trial = std.heap.ArenaAllocator.init(gpa);
        defer trial.deinit();
        const ta = trial.allocator();
        var window: ReleasingScanWindow = .{ .bytes = try ta.dupe(u8, sink.buffer.items), .meta = &meta };
        const inputs = [_]Input{window.input(sink.buffer.items)};
        var args = base;
        args.inputs = if (gated) &inputs else &mapped;
        args.start_reads = if (gated) .{
            .ctx = &window,
            .start = ReleasingScanWindow.start,
            .abandon = ReleasingScanWindow.abandon,
        } else null;
        args.integer_records = true;
        args.parallelism = j;
        args.decode_batch_rows = batch;
        var budget: core_memory.SharedBudget = .{ .limit = base.max_memory };
        args.ledger = &budget;
        var taken: std.EnumSet(FastPath) = .{};
        args.taken = &taken;
        const explanation = try explainAggregate(gpa, ta, args);
        try testing.expectEqual(.integer_count_top_h, explanation.record_path.?.layout);
        var got = try prepareAggregate(gpa, ta, args, false);
        defer got.deinit(gpa);
        try testing.expect(taken.contains(.integer_records));
        try testing.expectEqual(ownedGroupOutputBytes(got.output), budget.used.load(.monotonic));
        try got.output.applyResultOrder(gpa, args.result_order);
        try testing.expectEqualDeep(want.output.group_rows, got.output.group_rows);
        try testing.expectEqual(want.rows_matched, got.rows_matched);
        if (gated) {
            try testing.expectEqual(meta.row_groups.items.len, window.waits.load(.monotonic));
            try testing.expectEqual(@as(usize, 0), window.abandoned);
        }
    };
}

test "unfiltered record COUNT retains row identities instead of a dictionary histogram" {
    const gpa = testing.allocator;
    var held = std.heap.ArenaAllocator.init(gpa);
    defer held.deinit();
    const a = held.allocator();
    var sink: @import("../writer/streaming.zig").MemorySink = .{ .arena = a };
    var file = try consumer.AggregateFile.init(a, sink.sink());
    defer file.deinit();
    var values: [4096]i64 = undefined;
    for (&values, 0..) |*v, i| v.* = @as(i64, @intCast((i * 7) % 16)) * (1 << 40);
    const columns = [_]expr_agg.OutputCol{
        .{ .name = "unused", .parquet_type = .INT64, .nullable = false, .col = .{ .i64 = .{ .values = &values } } },
        .{ .name = "k", .parquet_type = .INT64, .nullable = false, .col = .{ .i64 = .{ .values = &values } } },
    };
    for (0..2) |_| try file.append(&columns, values.len, .SNAPPY, .{}, .{});
    _ = try file.finish(&columns);
    const meta = try metadata.open(a, sink.buffer.items);
    const base: MultiAggArgs = .{
        .inputs = &.{.{ .name = "hist-count.parquet", .bytes = sink.buffer.items }},
        .metas = &.{meta},
        .aggregate = "count(*) AS c",
        .group_by = "k",
        .integer_records_min_rows = 0,
        .ndv_policy = false,
        .sketch_topk = null,
        .max_memory = 128 << 20,
        .scan_all = true,
        .result_order = .{ .keys = &.{.{ .column = 1, .descending = true }}, .limit = 10 },
    };
    for ([_]usize{ 0, 64 }) |batch| for ([_]bool{ false, true }) |encoded| {
        var args = base;
        args.decode_batch_rows = batch;
        args.encoded = encoded;
        var taken: std.EnumSet(FastPath) = .{};
        args.taken = &taken;
        var got = try prepareAggregate(gpa, a, args, false);
        defer got.deinit(gpa);
        try testing.expect(taken.contains(.integer_records));
        try testing.expectEqual(@as(i64, 8192), got.rows_matched);
        try got.output.applyResultOrder(gpa, args.result_order);
        try testing.expectEqual(@as(usize, 10), got.output.group_rows.?.len);
        for (got.output.group_rows.?, 0..) |row, i| {
            try testing.expectEqual(@as(i128, @intCast((i * 7) % 16)) * (1 << 40), row[0].i);
            try testing.expectEqual(@as(i128, 512), row[1].i);
        }
    };
}

test "filtered hot COUNT certificate proves selected counts and first appearances" {
    const gpa = testing.allocator;
    var held = std.heap.ArenaAllocator.init(gpa);
    defer held.deinit();
    const a = held.allocator();
    const bytes = (try loadFixture("ci/fixtures/parquet/count_certificate_dict.parquet")) orelse
        return error.SkipZigTest;
    defer gpa.free(bytes);
    const inputs = [_]Input{.{ .name = "filtered-certificate.parquet", .bytes = bytes }};
    var certified = false;
    for ([_][]const u8{ "k", "k,f" }) |keys| {
        const base: MultiAggArgs = .{
            .inputs = &inputs,
            .aggregate = "count(*) AS c",
            .group_by = keys,
            .filter = "f >= 0",
            .integer_records = false,
            .byte_records = false,
            .msd_records = false,
            .dense = false,
            .ndv_policy = false,
            .plain_rules = false,
            .sketch_topk = null,
            .max_memory = 512 << 20,
            .min_bytes_per_worker = 1,
            .result_order = .{
                .keys = &.{.{ .column = if (keys.len == 1) 1 else 2, .descending = true }},
                .limit = 3,
            },
        };
        var want = try runMultiAggregate(gpa, a, base);
        defer want.deinit(gpa);
        try want.output.applyResultOrder(gpa, base.result_order);
        for (1..17) |j| for ([_]usize{ 0, 64 }) |batch| {
            var trial = std.heap.ArenaAllocator.init(gpa);
            defer trial.deinit();
            var args = base;
            var outcome: SketchTopK.HotOutcome = .declined;
            args.sketch_topk = .{ .cell_bits = 8, .admit_per_row = 1, .hot_outcome = &outcome };
            args.parallelism = j;
            args.decode_batch_rows = batch;
            var got = try runMultiAggregate(gpa, trial.allocator(), args);
            defer got.deinit(gpa);
            certified = certified or outcome == .certified or outcome == .residual;
            try testing.expectEqual(want.rows_in, got.rows_in);
            try testing.expectEqual(want.rows_scanned, got.rows_scanned);
            try testing.expectEqual(want.rows_matched, got.rows_matched);
            try got.output.applyResultOrder(gpa, args.result_order);
            try testing.expectEqualDeep(want.output.group_rows, got.output.group_rows);
        };
    }
    try testing.expect(certified);
}

test "filtered COUNT record ties start at the first selected row across mask words" {
    const gpa = testing.allocator;
    var arena = std.heap.ArenaAllocator.init(gpa);
    defer arena.deinit();
    const a = arena.allocator();
    var sink: @import("../writer/streaming.zig").MemorySink = .{ .arena = a };
    var file = try consumer.AggregateFile.init(a, sink.sink());
    defer file.deinit();
    var ints: [70]i64 = @splat(40);
    var strings: [70][]const u8 = @splat("");
    var defs: [70]u32 = @splat(1);
    var flags: [70]i32 = @splat(0);
    ints[1] = 50;
    ints[2] = 60;
    strings[1] = "a\x00b";
    strings[2] = "z";
    defs[64] = 0;
    for ([_]usize{ 1, 2, 63, 64 }) |i| flags[i] = 1;
    const cols = [_]expr_agg.OutputCol{
        .{
            .name = "k",
            .parquet_type = .INT64,
            .nullable = true,
            .col = .{ .i64 = .{ .values = &ints, .def_levels = &defs, .max_def = 1 } },
        },
        .{
            .name = "b",
            .parquet_type = .BYTE_ARRAY,
            .nullable = true,
            .col = .{ .string = .{ .values = &strings, .def_levels = &defs, .max_def = 1 } },
        },
        .{ .name = "f", .parquet_type = .INT32, .nullable = false, .col = .{ .i32 = .{ .values = &flags } } },
    };
    for (0..2) |_| try file.append(&cols, flags.len, .SNAPPY, .{}, .{});
    _ = try file.finish(&cols);
    const inputs = [_]Input{.{ .name = "first-selected.parquet", .bytes = sink.buffer.items }};
    for ([_][]const u8{ "k", "b" }) |key| for ([_]usize{ 0, 64 }) |batch| {
        var taken: std.EnumSet(FastPath) = .{};
        const args: MultiAggArgs = .{
            .inputs = &inputs,
            .aggregate = "count(*) AS c",
            .group_by = key,
            .filter = "f = 1",
            .integer_records = key[0] == 'k',
            .integer_records_min_rows = 0,
            .byte_records = key[0] == 'b',
            .byte_records_min_rows = 0,
            .msd_records = false,
            .decode_batch_rows = batch,
            .min_bytes_per_worker = 1,
            .parallelism = 2,
            .max_memory = 512 << 20,
            .taken = &taken,
            .result_order = .{ .keys = &.{.{ .column = 1, .descending = true }}, .limit = 3, .offset = 1 },
        };
        var got = try prepareAggregate(gpa, a, args, false);
        defer got.deinit(gpa);
        try testing.expect(taken.contains(.integer_records));
        try testing.expectEqual(@as(i64, 8), got.rows_matched);
        try got.output.applyResultOrder(gpa, args.result_order);
        const rows = got.output.group_rows.?;
        try testing.expectEqual(@as(usize, 3), rows.len);
        if (key[0] == 'k') {
            try testing.expectEqual(@as(i128, 60), rows[0][0].i);
            try testing.expectEqual(@as(i128, 40), rows[1][0].i);
        } else {
            try testing.expectEqualStrings("z", rows[0][0].s);
            try testing.expectEqualStrings("", rows[1][0].s);
        }
        try testing.expectEqual(.null_val, std.meta.activeTag(rows[2][0]));
        for (rows) |row| try testing.expectEqual(@as(i128, 2), row[1].i);
    };
}

test "filtered record pruning preserves canonical failures and stable replay of skipped corrupt pages" {
    const gpa = testing.allocator;
    var held = std.heap.ArenaAllocator.init(gpa);
    defer held.deinit();
    const a = held.allocator();
    const bytes = (try loadFixture("ci/fixtures/parquet/filtered_records_indexed.parquet")) orelse
        return error.SkipZigTest;
    defer gpa.free(bytes);
    const meta = try metadata.open(a, bytes);
    const Ready = struct {
        fn wait(_: *anyopaque, _: usize) anyerror!void {}
        fn probe(_: *anyopaque, _: usize, _: usize, _: u64, _: u64) anyerror!void {}
    };
    for ([_][]const u8{ "b,f", "k,f" }, 0..) |keys, mode| {
        const ci: usize = if (mode == 0) 1 else 0;
        const chunk = meta.row_groups.items[0].columns.items[ci];
        var index_reader = thrift_mod.Reader.init(bytes[@intCast(chunk.offset_index_offset.?)..]);
        const index = try schema.OffsetIndex.read(a, &index_reader);
        try testing.expect(index.page_locations.items.len > 2);
        const last = index.page_locations.items.len - 1;
        try testing.expect(index.page_locations.items[last].first_row_index > 160);
        for ([_]usize{ 0, last }) |page_index| {
            const bad = try a.dupe(u8, bytes);
            const offset: usize = @intCast(index.page_locations.items[page_index].offset);
            var header_reader = thrift_mod.Reader.init(bad[offset..]);
            const header = try schema.PageHeader.read(&header_reader);
            const body = offset + header_reader.pos;
            @memset(bad[body..][0..@min(12, @as(usize, @intCast(header.compressed_page_size)))], 0xff);
            const mapped = [_]Input{.{ .name = "corrupt-selected.parquet", .bytes = bad }};
            for ([_]usize{ 0, 64 }) |batch| {
                const base: MultiAggArgs = .{
                    .inputs = &mapped,
                    .metas = &.{meta},
                    .aggregate = "count(*) AS c",
                    .group_by = keys,
                    .filter = "f < 160",
                    .integer_records = false,
                    .msd_records = false,
                    .byte_records = false,
                    .dense = false,
                    .ndv_policy = false,
                    .plain_rules = false,
                    .sketch_topk = null,
                    .decode_batch_rows = batch,
                    .min_bytes_per_worker = 1,
                    .max_memory = 512 << 20,
                    .result_order = .{ .keys = &.{.{ .column = 2, .descending = true }}, .limit = 3 },
                };
                var expected_error: ?anyerror = null;
                var expected_column: usize = 0;
                var expected_page: ?u32 = null;
                var want: ?MultiAggResult = null;
                if (runMultiAggregate(gpa, a, base)) |result| {
                    want = result;
                    try want.?.output.applyResultOrder(gpa, base.result_order);
                } else |err| {
                    try testing.expect(consumer.isDecodeError(err));
                    expected_error = err;
                    expected_column = consumer.decodeFailure().?.column;
                    expected_page = consumer.decodeFailure().?.page;
                }
                defer if (want) |*result| result.deinit(gpa);
                if (page_index == 0) try testing.expect(expected_error != null);
                if (page_index == last and batch == 0) try testing.expect(expected_error == null);
                for (1..17) |j| for ([_]bool{ false, true }) |gated| {
                    var trial = std.heap.ArenaAllocator.init(gpa);
                    defer trial.deinit();
                    const ta = trial.allocator();
                    var window: ReleasingScanWindow = .{ .bytes = try ta.dupe(u8, bad), .meta = &meta };
                    var input = window.input(bad);
                    input.dict_wait = .{ .ctx = &window, .wait = Ready.wait };
                    var budget: core_memory.SharedBudget = .{ .limit = base.max_memory };
                    var args = base;
                    args.inputs = if (gated) (&input)[0..1] else &mapped;
                    args.integer_records = mode == 1;
                    args.integer_records_min_rows = 0;
                    args.msd_records = mode == 0;
                    args.msd_records_min_rows = 0;
                    args.dense = true;
                    args.ndv_policy = true;
                    args.sketch_topk = .{ .cell_bits = 8, .admit_per_row = 1 };
                    args.parallelism = j;
                    args.ledger = &budget;
                    if (gated) {
                        args.dict_probe = .{ .ctx = &window, .add = Ready.probe };
                        args.start_reads = .{
                            .ctx = &window,
                            .start = ReleasingScanWindow.start,
                            .abandon = ReleasingScanWindow.abandon,
                        };
                    }
                    if (expected_error) |expected| {
                        const err = failed: {
                            var unexpected = runMultiAggregate(gpa, ta, args) catch |e| break :failed e;
                            unexpected.deinit(gpa);
                            return error.TestUnexpectedResult;
                        };
                        try testing.expectEqual(expected, err);
                        try testing.expectEqual(expected_column, consumer.decodeFailure().?.column);
                        try testing.expectEqual(expected_page, consumer.decodeFailure().?.page);
                        try testing.expectEqual(@as(usize, 0), budget.used.load(.monotonic));
                    } else {
                        var got = runMultiAggregate(gpa, ta, args) catch |err| {
                            std.debug.print(
                                "filtered pruning mode={d} page={d} batch={d} j={d} gated={} error={s}\n",
                                .{ mode, page_index, batch, j, gated, @errorName(err) },
                            );
                            return err;
                        };
                        defer got.deinit(gpa);
                        try got.output.applyResultOrder(gpa, args.result_order);
                        try testing.expectEqualDeep(want.?.output.group_rows, got.output.group_rows);
                        if (gated) try testing.expect(window.abandoned > 0);
                    }
                };
            }
        }
    }
}

test "filtered COUNT sorted NULLs and known seams use one exact record pass with honest admission" {
    const gpa = testing.allocator;
    var held = std.heap.ArenaAllocator.init(gpa);
    defer held.deinit();
    const a = held.allocator();
    for ([_][]const u8{ "ordered", "seams" }) |shape| {
        const path = try std.fmt.allocPrint(a, "ci/fixtures/parquet/filtered_records_{s}.parquet", .{shape});
        const bytes = (try loadFixture(path)) orelse return error.SkipZigTest;
        defer gpa.free(bytes);
        const meta = try metadata.open(a, bytes);
        const mapped = [_]Input{.{ .name = path, .bytes = bytes }};
        const base: MultiAggArgs = .{
            .inputs = &mapped,
            .metas = &.{meta},
            .aggregate = "count(*) AS c",
            .group_by = "k",
            .filter = "f = 1",
            .integer_records = false,
            .byte_records = false,
            .msd_records = false,
            .ndv_policy = false,
            .dense = false,
            .plain_rules = false,
            .sketch_topk = null,
            .max_memory = 128 << 20,
            .min_bytes_per_worker = 1,
            .result_order = .{ .keys = &.{.{ .column = 1, .descending = true }}, .limit = 10, .offset = 2 },
        };
        var want = try runMultiAggregate(gpa, a, base);
        defer want.deinit(gpa);
        try want.output.applyResultOrder(gpa, base.result_order);
        for (1..17) |j| for ([_]usize{ 0, 64 }) |batch| for ([_]bool{ false, true }) |gated| {
            var trial = std.heap.ArenaAllocator.init(gpa);
            defer trial.deinit();
            const ta = trial.allocator();
            var window: ReleasingScanWindow = .{ .bytes = try ta.dupe(u8, bytes), .meta = &meta };
            const inputs = [_]Input{window.input(bytes)};
            var args = base;
            args.inputs = if (gated) &inputs else &mapped;
            args.start_reads = if (gated) .{
                .ctx = &window,
                .start = ReleasingScanWindow.start,
                .abandon = ReleasingScanWindow.abandon,
            } else null;
            args.integer_records = true;
            args.integer_records_min_rows = 0;
            args.parallelism = j;
            args.decode_batch_rows = batch;
            var taken: std.EnumSet(FastPath) = .{};
            args.taken = &taken;
            const explanation = try explainAggregate(gpa, ta, args);
            try testing.expectEqual(.integer_count_top_h, explanation.record_path.?.layout);
            var got = try prepareAggregate(gpa, ta, args, false);
            defer got.deinit(gpa);
            try testing.expect(taken.contains(.integer_records));
            try got.output.applyResultOrder(gpa, args.result_order);
            try testing.expectEqualDeep(want.output.group_rows, got.output.group_rows);
            try testing.expectEqual(want.rows_matched, got.rows_matched);
            try testing.expectEqual(@as(usize, 0), window.abandoned);
            if (gated) try testing.expectEqual(meta.row_groups.items.len, window.waits.load(.monotonic));
        };
        // The unknown PLAIN domain has at most a few estimated survivors, despite thousands of raw rows.
        var selective = base;
        selective.filter = "rid < 2";
        selective.integer_records = true;
        selective.integer_records_min_rows = 100;
        const explanation = try explainAggregate(gpa, a, selective);
        try testing.expect(explanation.record_path == null);
        var taken: std.EnumSet(FastPath) = .{};
        selective.taken = &taken;
        var got = try prepareAggregate(gpa, a, selective, false);
        defer got.deinit(gpa);
        try testing.expect(!taken.contains(.integer_records));
        try testing.expectEqual(@as(i64, 2), got.rows_matched);
    }
}

test "dense quote refusal restores displaced partitions before consuming rows" {
    const gpa = testing.allocator;
    var arena = std.heap.ArenaAllocator.init(gpa);
    defer arena.deinit();
    const a = arena.allocator();
    var names: [4096][]const u8 = undefined;
    for (&names, 0..) |*name, i| name.* = try std.fmt.allocPrint(
        a,
        "a-long-dictionary-value-with-a-stable-shared-prefix-and-identity-{d}",
        .{i % 256},
    );
    const columns = [_]expr_agg.OutputCol{.{
        .name = "k",
        .col = .{ .string = .{ .values = &names } },
        .parquet_type = .BYTE_ARRAY,
        .nullable = false,
    }};
    const Capture = struct {
        bytes: std.ArrayList(u8) = .empty,
        a: std.mem.Allocator,
        fn write(ctx: *anyopaque, bytes: []const u8) anyerror!void {
            const self: *@This() = @ptrCast(@alignCast(ctx));
            try self.bytes.appendSlice(self.a, bytes);
        }
    };
    var capture: Capture = .{ .a = a };
    _ = try consumer.writeAggregateRows(
        a,
        .{ .ctx = &capture, .write_fn = Capture.write },
        &columns,
        names.len,
        .SNAPPY,
        .{},
        .{},
    );
    var meta = try metadata.open(a, capture.bytes.items);
    const groups = try a.alloc(schema.RowGroup, 64);
    @memset(groups, meta.row_groups.items[0]);
    meta.row_groups.items = groups;
    meta.num_rows *= @intCast(groups.len);
    var args: MultiAggArgs = .{
        .inputs = &.{.{ .name = "long-keys", .bytes = capture.bytes.items }},
        .metas = &.{meta},
        .aggregate = "count(*) AS c",
        .group_by = "k",
        .parallelism = 64,
        .min_bytes_per_worker = 1,
        .max_memory = 4 << 20,
        .dense = true,
        .plain_rules = false,
    };
    const ordinary = try explainAggregate(gpa, a, args);
    try testing.expectEqual(.partitioned_hash, ordinary.plan.dispatch.strategy);
    args.plain_rules = true;
    const dense = try explainAggregate(gpa, a, args);
    try testing.expectEqualStrings("dense", dense.strategyName());
    try testing.expect(dense.plan.group_bytes.? <= args.max_memory / 8);
    try testing.expect(dense.plan.plain_bytes.? > args.max_memory / 8);
    var ledger: core_memory.SharedBudget = .{ .limit = args.max_memory };
    const held = ledger.drawExact(args.max_memory - 1).?;
    args.ledger = &ledger;
    var taken: std.EnumSet(FastPath) = .{};
    args.taken = &taken;
    try testing.expectError(error.ExceededMemoryBudget, prepareAggregate(gpa, a, args, false));
    try testing.expect(taken.contains(.plain_rule));
    try testing.expect(!taken.contains(.dense) and !taken.contains(.worker_tables));
    try testing.expectEqual(held, ledger.used.load(.monotonic));
    var chain: Chain = .{ .parts_tried = false };
    const fallback = narrowed(args, args, taken, &chain, false).?;
    const restored = try explainAggregate(gpa, a, fallback);
    try testing.expectEqual(.partitioned_hash, restored.plan.dispatch.strategy);
    ledger.release(held);
    {
        var want = try runMultiAggregate(gpa, a, args);
        defer want.deinit(gpa);
        var got = try runMultiAggregate(gpa, a, fallback);
        defer got.deinit(gpa);
        try testing.expect(got.part_agg);
        try testing.expectEqualDeep(want.output.group_rows.?, got.output.group_rows.?);
    }
}

test "integer and packed COUNT owner caps agree with EXPLAIN beyond sixty four workers" {
    const gpa = testing.allocator;
    var held = std.heap.ArenaAllocator.init(gpa);
    defer held.deinit();
    const a = held.allocator();
    var sink: @import("../writer/streaming.zig").MemorySink = .{ .arena = a };
    var file = try consumer.AggregateFile.init(a, sink.sink());
    defer file.deinit();
    var keys: [1025]i64 = undefined;
    var flags: [1025]i64 = undefined;
    for (&keys, &flags, 0..) |*key, *flag, i| {
        key.* = @as(i64, @intCast((i * 17) % 61)) * (1 << 40);
        flag.* = @intCast(i % 3);
    }
    const columns = [_]expr_agg.OutputCol{
        .{ .name = "k", .parquet_type = .INT64, .nullable = false, .col = .{ .i64 = .{ .values = &keys } } },
        .{ .name = "f", .parquet_type = .INT64, .nullable = false, .col = .{ .i64 = .{ .values = &flags } } },
    };
    try file.append(&columns, keys.len, .SNAPPY, .{}, .{});
    _ = try file.finish(&columns);
    const meta = try metadata.open(a, sink.buffer.items);
    for (meta.row_groups.items[0].columns.items) |column|
        try testing.expect(column.meta_data.?.dictionary_page_offset != null);
    const inputs: [128]Input = @splat(.{ .name = "owner-cap.parquet", .bytes = sink.buffer.items });
    const metas: [128]schema.FileMetaData = @splat(meta);
    for ([_][]const u8{ "k", "k,f" }, 0..) |group_by, shape| {
        const base: MultiAggArgs = .{
            .inputs = &inputs,
            .metas = &metas,
            .aggregate = "count(*) AS c",
            .group_by = group_by,
            .integer_records = false,
            .byte_records = false,
            .msd_records = false,
            .ndv_policy = false,
            .dense = false,
            .plain_rules = false,
            .sketch_topk = null,
            .min_bytes_per_worker = 1,
            .max_memory = 256 << 20,
            .result_order = .{ .keys = &.{.{ .column = shape + 1, .descending = true }}, .limit = 10, .offset = 2 },
        };
        var want = try runMultiAggregate(gpa, a, base);
        defer want.deinit(gpa);
        try want.output.applyResultOrder(gpa, base.result_order);
        for ([_]usize{ 64, 65, 96, 128 }) |j| {
            var trial = std.heap.ArenaAllocator.init(gpa);
            defer trial.deinit();
            var args = base;
            args.parallelism = j;
            args.integer_records = true;
            args.integer_records_min_rows = 0;
            var ready: u8 = 0;
            const Ready = struct {
                fn wait(_: *anyopaque, _: usize) anyerror!void {}
                fn done(_: *anyopaque, _: usize, _: usize) void {}
            };
            var readers = inputs;
            if (shape == 1) {
                for (&readers) |*input| {
                    input.plan_bytes = input.bytes;
                    input.gate = .{ .ctx = &ready, .wait = Ready.wait, .done = Ready.done };
                    input.dict_wait = .{ .ctx = &ready, .wait = Ready.wait };
                }
                args.inputs = &readers;
                args.ndv_policy = true;
                args.dense = true;
            }
            var taken: std.EnumSet(FastPath) = .{};
            args.taken = &taken;
            const explanation = try explainAggregate(gpa, trial.allocator(), args);
            try testing.expectEqual(@as(usize, owner_groups.max_workers), explanation.workers);
            if (explanation.record_path == null) std.debug.print(
                "owner cap shape={d} j={d} has no record admission\n",
                .{ shape, j },
            );
            try testing.expect(explanation.record_path != null);
            try testing.expectEqual(.integer_count_top_h, explanation.record_path.?.layout);
            var got = try prepareAggregate(gpa, trial.allocator(), args, false);
            defer got.deinit(gpa);
            try testing.expect(taken.contains(.integer_records));
            try got.output.applyResultOrder(gpa, args.result_order);
            try testing.expectEqualDeep(want.output.group_rows, got.output.group_rows);
        }
    }
}

test "skewed single-key COUNT keeps dense and a sampled demotion can retry it" {
    const gpa = testing.allocator;
    var arena = std.heap.ArenaAllocator.init(gpa);
    defer arena.deinit();
    const a = arena.allocator();
    var keys: [8192]i32 = undefined;
    var values: [8192]i32 = @splat(0);
    for (&keys, 0..) |*key, i| key.* = @intCast(i % 256);
    values[values.len - 1] = 1_000_000;
    const columns = [_]expr_agg.OutputCol{
        .{ .name = "k", .col = .{ .i32 = .{ .values = &keys } }, .parquet_type = .INT32, .nullable = false },
        .{ .name = "f", .col = .{ .i32 = .{ .values = &values } }, .parquet_type = .INT32, .nullable = false },
    };
    const Capture = struct {
        bytes: std.ArrayList(u8) = .empty,
        a: std.mem.Allocator,
        fn write(ctx: *anyopaque, bytes: []const u8) anyerror!void {
            const c: *@This() = @ptrCast(@alignCast(ctx));
            try c.bytes.appendSlice(c.a, bytes);
        }
    };
    var capture: Capture = .{ .a = a };
    _ = try consumer.writeAggregateRows(
        a,
        .{ .ctx = &capture, .write_fn = Capture.write },
        &columns,
        keys.len,
        .SNAPPY,
        .{},
        .{},
    );
    var meta = try metadata.open(a, capture.bytes.items);
    const groups = try a.alloc(schema.RowGroup, 16);
    @memset(groups, meta.row_groups.items[0]);
    meta.row_groups.items = groups;
    meta.num_rows *= 16;
    var args: MultiAggArgs = .{
        .inputs = &.{.{ .name = "skewed-count", .bytes = capture.bytes.items }},
        .metas = &.{meta},
        .aggregate = "count(*) AS c",
        .group_by = "k",
        .filter = "f < 1",
        .parallelism = 1,
        .min_bytes_per_worker = 1,
        .max_memory = 64 << 20,
        .result_order = .{ .keys = &.{.{ .column = 1, .descending = true }}, .limit = 10 },
        .dense = false,
    };
    var want = try runMultiAggregate(gpa, a, args);
    defer want.deinit(gpa);
    try want.output.applyResultOrder(gpa, args.result_order);
    for ([_]usize{ 1, 4, 16 }) |j| for ([_]usize{ 0, 64 }) |window| {
        var taken: std.EnumSet(FastPath) = .{};
        args.parallelism = j;
        args.decode_batch_rows = window;
        args.dense = true;
        args.taken = &taken;
        const explanation = try explainAggregate(gpa, a, args);
        try testing.expect(explanation.dense);
        var got = try prepareAggregate(gpa, a, args, false);
        defer got.deinit(gpa);
        try testing.expect(taken.contains(.dense));
        try got.output.applyResultOrder(gpa, args.result_order);
        try testing.expectEqualDeep(want.output.group_rows.?, got.output.group_rows.?);
    };
    args.filter = "f > 999999";
    var taken: std.EnumSet(FastPath) = .{};
    args.taken = &taken;
    var rare = try prepareAggregate(gpa, a, args, false);
    defer rare.deinit(gpa);
    try testing.expect(!taken.contains(.dense) and taken.contains(.plain_rule));
    var chain: Chain = .{ .parts_tried = false };
    var retry = narrowed(args, args, taken, &chain, false).?;
    try testing.expect(!retry.plain_rules and retry.dense);
    retry.taken = null;
    try testing.expect((try explainAggregate(gpa, a, retry)).dense);
    var rescued = try prepareAggregate(gpa, a, retry, false);
    defer rescued.deinit(gpa);
    try rare.output.applyResultOrder(gpa, args.result_order);
    try rescued.output.applyResultOrder(gpa, args.result_order);
    try testing.expectEqualDeep(rare.output.group_rows.?, rescued.output.group_rows.?);

    const calls = [_]expr_agg.AggCall{.{ .func = .count, .arg = null, .where = null, .alias = "n", .result = .i64 }};
    const filter = try filter_parser.parse(a, "f < 1", &meta, null);
    const plan: agg_plan.Plan = try .init(a, .{
        .filter = filter,
        .calls = &calls,
        .group_keys = null,
    }, &.{meta}, .fromFlags(true, false));
    const mapped = [_]Input{.{
        .name = "retained-skew",
        .bytes = &.{},
        .plan_bytes = capture.bytes.items,
        .logical_size = capture.bytes.items.len,
    }};
    const work = [_]WorkItem{.{
        .file = 0,
        .rg = 0,
        .agg_start = 0,
        .agg_len = 1,
        .columns = plan.columns,
        .accumulators = &.{},
    }};
    const sampled = try sampledPassShare(gpa, a, &plan, &.{meta}, &mapped, &work, .{ .fast_levels = true }, null);
    try testing.expectEqual(@as(?f64, 8191.0 / 8192.0), sampled);
}

test "selective composite COUNT keeps its packed owner while wider filters retain dense lanes" {
    const gpa = testing.allocator;
    var arena = std.heap.ArenaAllocator.init(gpa);
    defer arena.deinit();
    const a = arena.allocator();
    var ints: [8192]i32 = undefined;
    var names: [8192][]const u8 = undefined;
    var filter: [8192]i32 = undefined;
    for (&ints, &names, &filter, 0..) |*v, *s, *f, i| {
        v.* = @intCast(i % 3);
        s.* = try std.fmt.allocPrint(a, "key-{d}", .{i % 1024});
        f.* = @intCast(i);
    }
    const columns = [_]expr_agg.OutputCol{
        .{ .name = "a", .col = .{ .i32 = .{ .values = &ints } }, .parquet_type = .INT32, .nullable = false },
        .{ .name = "b", .col = .{ .string = .{ .values = &names } }, .parquet_type = .BYTE_ARRAY, .nullable = false },
        .{ .name = "f", .col = .{ .i32 = .{ .values = &filter } }, .parquet_type = .INT32, .nullable = false },
    };
    const Capture = struct {
        bytes: std.ArrayList(u8) = .empty,
        a: std.mem.Allocator,
        fn write(ctx: *anyopaque, bytes: []const u8) anyerror!void {
            const c: *@This() = @ptrCast(@alignCast(ctx));
            try c.bytes.appendSlice(c.a, bytes);
        }
    };
    var capture: Capture = .{ .a = a };
    _ = try consumer.writeAggregateRows(
        a,
        .{ .ctx = &capture, .write_fn = Capture.write },
        &columns,
        ints.len,
        .SNAPPY,
        .{},
        .{},
    );
    var meta = try metadata.open(a, capture.bytes.items);
    const groups = try a.alloc(schema.RowGroup, 16);
    @memset(groups, meta.row_groups.items[0]);
    meta.row_groups.items = groups;
    meta.num_rows *= 16;
    var args: MultiAggArgs = .{
        .inputs = &.{.{ .name = "selective-product", .bytes = capture.bytes.items }},
        .metas = &.{meta},
        .aggregate = "count(*) AS c",
        .group_by = "a,b",
        .filter = "f < 1",
        .parallelism = 1,
        .min_bytes_per_worker = 1,
        .max_memory = 64 << 20,
        .integer_records_min_rows = 0,
        .result_order = .{ .keys = &.{.{ .column = 2, .descending = true }}, .limit = 3 },
        .dense = false,
        .integer_records = false,
        .sketch_topk = .{ .cell_bits = 12 },
    };
    var want = try runMultiAggregate(gpa, a, args);
    defer want.deinit(gpa);
    try want.output.applyResultOrder(gpa, args.result_order);
    for ([_]usize{ 1, 4, 16 }) |j| for ([_]usize{ 0, 64 }) |window| {
        var taken: std.EnumSet(FastPath) = .{};
        args.parallelism = j;
        args.decode_batch_rows = window;
        args.dense = true;
        args.integer_records = true;
        args.taken = &taken;
        const explanation = try explainAggregate(gpa, a, args);
        try testing.expect(explanation.record_path != null);
        try testing.expectEqual(.packed_count_top_h, explanation.record_path.?.layout);
        try testing.expect(!explanation.dense);
        var got = try prepareAggregate(gpa, a, args, false);
        defer got.deinit(gpa);
        try testing.expect(taken.contains(.integer_records));
        try testing.expect(!taken.contains(.dense));
        try got.output.applyResultOrder(gpa, args.result_order);
        try testing.expectEqualDeep(want.output.group_rows.?, got.output.group_rows.?);
    };
    args.filter = "f < 1000";
    args.taken = null;
    const wider = try explainAggregate(gpa, a, args);
    try testing.expect(wider.dense);
}

test "selected owner coded identity replays a late dictionary mismatch after earlier records" {
    try selectedIdentityReplayTrial(false);
}

test "M1 finish DISTINCT replays a late argument dictionary mismatch after completed groups and NULL presence" {
    try selectedIdentityReplayTrial(true);
}

test "selected COUNT setup refusal preserves an occupied ledger and falls back to ordinary grouping" {
    const gpa = testing.allocator;
    var fixture = std.heap.ArenaAllocator.init(gpa);
    defer fixture.deinit();
    const fa = fixture.allocator();
    const values = try fa.alloc(i32, 1 << 20);
    for (values, 0..) |*value, i| value.* = @intCast(i % 17);
    const columns = [_]expr_agg.OutputCol{.{
        .name = "k",
        .col = .{ .i32 = .{ .values = values } },
        .parquet_type = .INT32,
        .nullable = false,
    }};
    var sink: @import("../writer/streaming.zig").MemorySink = .{ .arena = fa };
    _ = try consumer.writeAggregateRows(fa, sink.sink(), &columns, @intCast(values.len), .SNAPPY, .{}, .{});
    const meta = try metadata.open(fa, sink.buffer.items);
    const base: MultiAggArgs = .{
        .inputs = &.{.{ .name = "occupied", .bytes = sink.buffer.items }},
        .metas = &.{meta},
        .aggregate = "count(*) AS c",
        .group_by = "k",
        .parallelism = 1,
        .max_memory = 64 << 20,
        .charge_decode = false,
        .dense = false,
        .plain_rules = false,
        .msd_records = false,
        .result_order = .{ .keys = &.{.{ .column = 1, .descending = true }}, .limit = 10 },
    };
    const occupied = 60 << 20;
    var baseline_rows: ?[][]const AggValue = null;
    var baseline_types: ?[]const GroupColType = null;
    var retained: [2]usize = undefined;
    for ([_]bool{ false, true }, 0..) |enabled, mode| {
        var arena = std.heap.ArenaAllocator.init(gpa);
        defer arena.deinit();
        const a = arena.allocator();
        var ledger: core_memory.SharedBudget = .{ .limit = base.max_memory };
        _ = ledger.drawExact(occupied).?;
        var args = base;
        args.ledger = &ledger;
        args.integer_records = enabled;
        if (enabled) {
            const explanation = try explainAggregate(gpa, a, args);
            try testing.expectEqual(.packed_count_top_h, explanation.record_path.?.layout);
            try testing.expect(explanation.record_path.?.reservation <= args.max_memory);
            const O = owner_groups.Packed(u64, &.{}, u32);
            try testing.expect(O.State.charge(values.len, 1, 64) > args.max_memory - occupied);
        }
        var got = runMultiAggregate(gpa, a, args) catch |err| {
            std.debug.print("owner enabled={any}: {s}\n", .{ enabled, @errorName(err) });
            return err;
        };
        defer got.deinit(gpa);
        try testing.expect(!got.part_agg);
        try got.output.applyResultOrder(gpa, args.result_order);
        const rows = got.output.group_rows.?;
        try testing.expectEqual(@as(usize, 10), rows.len);
        if (!enabled) {
            const saved = try fa.alloc([]const AggValue, rows.len);
            for (saved, rows) |*row, original| row.* = try fa.dupe(AggValue, original);
            baseline_rows = saved;
            baseline_types = try fa.dupe(GroupColType, got.state.group_col_types.?);
        } else {
            try testing.expectEqualDeep(baseline_rows.?, rows);
            try testing.expectEqualDeep(baseline_types.?, got.state.group_col_types.?);
        }
        for (rows, 0..) |row, i| {
            try testing.expectEqual(@as(i64, @intCast(i)), row[0].i);
            try testing.expectEqual(@as(i64, 61681), row[1].i);
        }
        // Ordinary output retains its small query-lifetime credit. Setup refusal must add nothing to that credit.
        retained[mode] = ledger.used.load(.monotonic);
        try testing.expect(retained[mode] >= occupied);
        ledger.release(occupied);
        try testing.expectEqual(retained[mode] - occupied, ledger.used.load(.monotonic));
    }
    try testing.expectEqual(retained[0], retained[1]);
}

test "selected packed COUNT runs the typed owner for INT32 keys with original NULL ties and sliced files" {
    const gpa = testing.allocator;
    var arena_state = std.heap.ArenaAllocator.init(gpa);
    defer arena_state.deinit();
    const a = arena_state.allocator();
    var keys: [256]i32 = undefined;
    var defs: [256]u32 = undefined;
    var filter_values: [256]i32 = undefined;
    for (&keys, &defs, &filter_values, 0..) |*key, *def, *f, i| {
        key.* = @as(i32, @intCast(i % 17)) - 8;
        def.* = @intFromBool(i % 23 != 0);
        f.* = @intCast(i % 3);
    }
    const columns = [_]expr_agg.OutputCol{
        .{
            .name = "k",
            .col = .{ .i32 = .{ .values = &keys, .def_levels = &defs, .max_def = 1 } },
            .parquet_type = .INT32,
            .nullable = true,
        },
        .{ .name = "f", .col = .{ .i32 = .{ .values = &filter_values } }, .parquet_type = .INT32, .nullable = false },
    };
    const Capture = struct {
        bytes: std.ArrayList(u8) = .empty,
        allocator: std.mem.Allocator,
        fn write(ctx: *anyopaque, bytes: []const u8) anyerror!void {
            const c: *@This() = @ptrCast(@alignCast(ctx));
            try c.bytes.appendSlice(c.allocator, bytes);
        }
    };
    var capture: Capture = .{ .allocator = a };
    _ = try consumer.writeAggregateRows(
        a,
        .{ .ctx = &capture, .write_fn = Capture.write },
        &columns,
        keys.len,
        .SNAPPY,
        .{},
        .{},
    );
    var meta = try metadata.open(a, capture.bytes.items);
    const groups = try a.alloc(schema.RowGroup, 32);
    @memset(groups, meta.row_groups.items[0]);
    meta.row_groups.items = groups;
    meta.num_rows *= @intCast(groups.len);
    var args: MultiAggArgs = .{
        .inputs = &.{
            .{ .name = "first", .bytes = capture.bytes.items },
            .{ .name = "second", .bytes = capture.bytes.items },
        },
        .metas = &.{ meta, meta },
        .aggregate = "count(*) AS c,count(*) AS d",
        .group_by = "k",
        .filter = "f < 1",
        .parallelism = 1,
        .min_bytes_per_worker = 1,
        .max_memory = 512 << 20,
        .result_order = .{ .keys = &.{.{ .column = 1, .descending = true }}, .limit = 7, .offset = 2 },
        .dense = false,
        .encoded = false,
        .plain_rules = false,
        .integer_records = false,
        .integer_records_min_rows = 0,
    };
    var want = try runMultiAggregate(gpa, a, args);
    defer want.deinit(gpa);
    try want.output.applyResultOrder(gpa, args.result_order);
    for ([_]usize{ 1, 4, 16 }) |jobs| for ([_]usize{ 0, 64 }) |window| {
        var taken: std.EnumSet(FastPath) = .{};
        args.integer_records = true;
        args.parallelism = jobs;
        args.decode_batch_rows = window;
        args.taken = &taken;
        const explanation = try explainAggregate(gpa, a, args);
        try testing.expectEqual(.packed_count_top_h, explanation.record_path.?.layout);
        var got = try prepareAggregate(gpa, a, args, false);
        defer got.deinit(gpa);
        try testing.expect(taken.contains(.integer_records));
        try got.output.applyResultOrder(gpa, args.result_order);
        try testing.expectEqualDeep(want.output.group_rows.?, got.output.group_rows.?);
        try testing.expect(!got.state.group_col_types.?[1].nullable);
    };
    args.parallelism = 96;
    try testing.expectEqual(@as(usize, 64), (try explainAggregate(gpa, a, args)).workers);
}

test "integer COUNT repeated RG dictionaries stay coded after complete union refinement" {
    const gpa = testing.allocator;
    var arena_state = std.heap.ArenaAllocator.init(gpa);
    defer arena_state.deinit();
    const a = arena_state.allocator();
    const values = try a.alloc(i64, 65536);
    for (values, 0..) |*value, i| value.* = @intCast((i % 4096) * 7919);
    const columns = [_]expr_agg.OutputCol{.{
        .name = "k",
        .col = .{ .i64 = .{ .values = values } },
        .parquet_type = .INT64,
        .nullable = false,
    }};
    var sink: @import("../writer/streaming.zig").MemorySink = .{ .arena = a };
    _ = try consumer.writeAggregateRows(a, sink.sink(), &columns, @intCast(values.len), .SNAPPY, .{}, .{});
    var meta = try metadata.open(a, sink.buffer.items);
    const key: expr_ast.Expr = .{ .col_ref = .{
        .col_idx = 0,
        .physical_type = .INT64,
        .expr_type = .i64,
    } };
    const single = ndv.footerEntries(&.{meta}, &.{key}).?;
    try testing.expect(single != 0);
    const copies: usize = @intCast(agg_dense.max_ids / single + 1);
    const groups = try a.alloc(schema.RowGroup, copies);
    @memset(groups, meta.row_groups.items[0]);
    meta.row_groups.items = groups;
    meta.num_rows *= @intCast(copies);
    const summed = ndv.footerEntries(&.{meta}, &.{key}).?;
    try testing.expect(summed > agg_dense.max_ids and summed < @as(u64, @intCast(meta.num_rows)) / 4);
    var args: MultiAggArgs = .{
        .inputs = &.{.{ .name = "repeated", .bytes = sink.buffer.items }},
        .metas = &.{meta},
        .aggregate = "count(*) AS c",
        .group_by = "k",
        .parallelism = 4,
        .min_bytes_per_worker = 1,
        .max_memory = 8 << 30,
        .result_order = .{ .keys = &.{.{ .column = 1, .descending = true }}, .limit = 10 },
    };
    const explanation = try explainAggregate(gpa, a, args);
    try testing.expect(explanation.plan.ndv_stat.?.union_complete);
    try testing.expectEqual(@as(u64, 4096), explanation.plan.ndv_stat.?.hi);
    try testing.expect(explanation.record_path == null);
    var taken: std.EnumSet(FastPath) = .{};
    args.taken = &taken;
    var got = try prepareAggregate(gpa, a, args, false);
    defer got.deinit(gpa);
    try testing.expect(!taken.contains(.integer_records));
    try got.output.applyResultOrder(gpa, args.result_order);
    const rows = got.output.group_rows.?;
    try testing.expectEqual(@as(usize, 10), rows.len);
    for (rows, 0..) |row, i| {
        try testing.expectEqual(@as(i64, @intCast(i * 7919)), row[0].i);
        try testing.expectEqual(@as(i64, @intCast(copies * 16)), row[1].i);
    }
    args.ndv_union = false;
    const no_union = try explainAggregate(gpa, a, args);
    try testing.expectEqual(.integer_count_top_h, no_union.record_path.?.layout);
}

test "M1 finish DISTINCT selected owners match sets for skew NULL groups arguments and the empty global group" {
    const gpa = testing.allocator;
    var arena = std.heap.ArenaAllocator.init(gpa);
    defer arena.deinit();
    const a = arena.allocator();
    var k: [4096]i32 = undefined;
    var x: [4096]i64 = undefined;
    var s: [4096][]const u8 = undefined;
    var kd: [4096]u32 = undefined;
    var xd: [4096]u32 = undefined;
    for (&k, &x, &s, &kd, &xd, 0..) |*key, *arg, *word, *key_def, *arg_def, i| {
        key.* = if (i % 8 == 0) 0 else if (i % 4 != 0) 1 else @intCast(i % 17);
        arg.* = @intCast(i % 13);
        word.* = try std.fmt.allocPrint(a, "dictionary-string-with-shared-prefix-{d}", .{i % 7});
        key_def.* = @intFromBool(i % 97 != 0);
        arg_def.* = @intFromBool(key.* != 0 and i % 11 != 0);
    }
    const cols = [_]expr_agg.OutputCol{
        .{
            .name = "k",
            .col = .{ .i32 = .{ .values = &k, .def_levels = &kd, .max_def = 1 } },
            .parquet_type = .INT32,
            .nullable = true,
        },
        .{
            .name = "x",
            .col = .{ .i64 = .{ .values = &x, .def_levels = &xd, .max_def = 1 } },
            .parquet_type = .INT64,
            .nullable = true,
        },
        .{ .name = "s", .col = .{ .string = .{ .values = &s } }, .parquet_type = .BYTE_ARRAY, .nullable = false },
    };
    var bytes: @import("../writer/streaming.zig").MemorySink = .{ .arena = a };
    _ = try consumer.writeAggregateRows(a, bytes.sink(), &cols, k.len, .SNAPPY, .{}, .{});
    var meta = try metadata.open(a, bytes.buffer.items);
    const rgs = try a.alloc(schema.RowGroup, 16);
    @memset(rgs, meta.row_groups.items[0]);
    meta.row_groups.items = rgs;
    meta.num_rows *= 16;
    const Case = struct { group: ?[]const u8, agg: []const u8, filter: ?[]const u8 = null, top: bool = false, rank: usize = 2, pair: bool = true };
    const cases = [_]Case{
        .{ .group = "k", .agg = "count(DISTINCT x) AS d", .top = true, .rank = 1 },
        .{ .group = "k", .agg = "count(DISTINCT x) AS d,count(*) AS c" },
        .{ .group = "k", .agg = "count(DISTINCT x) AS d,count(*) AS c,sum(x) AS s,avg(x) AS a,min(x) AS lo,max(x) AS hi,count(x) AS n", .pair = false },
        .{ .group = "k", .agg = "count(DISTINCT x) AS d,count(*) AS c,sum(x) AS s,avg(x) AS a", .top = true },
        .{ .group = "s", .agg = "count(*) AS c,count(DISTINCT x) AS d", .top = true },
        .{ .group = "k,s", .agg = "count(DISTINCT x) AS d" },
        .{ .group = "k", .agg = "count(DISTINCT x) AS d", .filter = "k = 0" },
        .{ .group = null, .agg = "count(DISTINCT s) AS d,count(*) AS c" },
        .{ .group = null, .agg = "count(DISTINCT s) AS d", .filter = "k < 0" },
        .{ .group = "k", .agg = "count(DISTINCT x) AS d", .filter = "k < 0" },
    };
    for (cases) |c| {
        var args: MultiAggArgs = .{
            .inputs = &.{.{ .name = "M1-distinct", .bytes = bytes.buffer.items }},
            .grouped_distinct_bitmap = .off,
            .metas = &.{meta},
            .aggregate = c.agg,
            .group_by = c.group,
            .filter = c.filter,
            .max_memory = 512 << 20,
            .min_bytes_per_worker = 1,
            .integer_records_min_rows = 0,
            .integer_records = false,
            .byte_records = false,
            .msd_records = false,
            .dense = false,
            .distinct_pairs = .off,
            .plain_rules = false,
            .result_order = if (c.top) .{ .keys = &.{.{ .column = c.rank, .descending = true }}, .limit = 3 } else .{},
        };
        var want = try runMultiAggregate(gpa, a, args);
        defer want.deinit(gpa);
        try want.output.applyResultOrder(gpa, args.result_order);
        for ([_]bool{ false, true }) |raw| for ([_]usize{ 1, 16 }) |j| for ([_]usize{ 0, 64 }) |window| for ([_]bool{ false, true }) |unordered| {
            if (unordered and (!c.top or c.rank != 1)) continue;
            args.unordered = unordered;
            if (raw and (c.group == null or std.mem.indexOf(u8, c.group.?, "s") != null)) continue;
            args.ndv_union = !raw;
            var taken: std.EnumSet(FastPath) = .{};
            args.integer_records = true;
            args.distinct_pairs = .on;
            args.parallelism = j;
            args.decode_batch_rows = window;
            args.taken = &taken;
            if (!c.pair) {
                const explanation = try explainAggregate(gpa, a, args);
                try testing.expect(explanation.record_path == null or explanation.record_path.?.layout != .pair_distinct);
                var legacy = try runMultiAggregate(gpa, a, args);
                defer legacy.deinit(gpa);
                try testing.expect(!taken.contains(.integer_records));
                try legacy.output.applyResultOrder(gpa, args.result_order);
                try testing.expectEqualDeep(want.output.group_rows, legacy.output.group_rows);
                continue;
            }
            if (c.group != null and (std.mem.eql(u8, c.group.?, "k") or std.mem.eql(u8, c.group.?, "s"))) {
                args.distinct_pairs = .auto;
                const auto_path = (try explainAggregate(gpa, a, args)).record_path;
                if (std.mem.eql(u8, c.group.?, "k")) {
                    try testing.expectEqual(.pair_distinct, auto_path.?.layout);
                } else try testing.expect(auto_path == null);
                var automatic = try runMultiAggregate(gpa, a, args);
                defer automatic.deinit(gpa);
                try automatic.output.applyResultOrder(gpa, args.result_order);
                try testing.expectEqualDeep(want.output.group_rows, automatic.output.group_rows);
                try testing.expectEqualDeep(want.output.aggs, automatic.output.aggs);
                args.distinct_pairs = .on;
                taken = .{};
            }
            const explanation = try explainAggregate(gpa, a, args);
            try testing.expectEqual(.pair_distinct, explanation.record_path.?.layout);
            var got = try prepareAggregate(gpa, a, args, false);
            defer got.deinit(gpa);
            try testing.expect(taken.contains(.integer_records));
            try testing.expect(got.distinct_pairs);
            try got.output.applyResultOrder(gpa, args.result_order);
            try testing.expectEqualDeep(want.output.group_rows, got.output.group_rows);
            try testing.expectEqualDeep(want.output.aggs, got.output.aggs);
            try testing.expectEqualDeep(want.state.group_col_types, got.state.group_col_types);
        };
    }
}

test "M1 finish DISTINCT poisoned NULL record bounds replay with and without count star" {
    const gpa = testing.allocator;
    var arena = std.heap.ArenaAllocator.init(gpa);
    defer arena.deinit();
    const a = arena.allocator();
    const P = @import("../owner_distinct.zig").Pipeline(u64);
    const cache_slots = @import("../owner_cache.zig").capacity(@sizeOf(P.Groups.Record));
    // Cycle a domain well beyond the producer cache. NULL presence records cannot be bounded by its NDV:
    // eviction and the low-hit bypass can emit one record per input row, even for the same group.
    // The writer requires at least 64 present values and at most 10% distinct integer keys for dictionaries, and
    // the 64 present values close the file: all of it is one row group, however large the host's cache.
    const groups: usize = @min(@max(4096, cache_slots * 8), (consumer.answer_rg_rows - 64) / 16);
    try testing.expect(groups >= cache_slots * 2);
    const rows = groups * 16 + 64;
    const k = try a.alloc(i32, rows);
    const x = try a.alloc(i32, rows);
    const defs = try a.alloc(u32, rows);
    for (k, 0..) |*key, i| key.* = @intCast(i % groups);
    @memset(x, 7);
    @memset(defs, 0);
    @memset(defs[rows - 64 ..], 1);
    const cols = [_]expr_agg.OutputCol{
        .{ .name = "k", .col = .{ .i32 = .{ .values = k } }, .parquet_type = .INT32, .nullable = false },
        .{
            .name = "x",
            .col = .{ .i32 = .{ .values = x, .def_levels = defs, .max_def = 1 } },
            .parquet_type = .INT32,
            .nullable = true,
        },
    };
    var bytes: @import("../writer/streaming.zig").MemorySink = .{ .arena = a };
    _ = try consumer.writeAggregateRows(a, bytes.sink(), &cols, @intCast(rows), .SNAPPY, .{}, .{});
    const poisoned = try rewritten(bytes.buffer.items, .{ .footer = struct {
        fn edit(meta: *schema.FileMetaData) void {
            for (meta.row_groups.items) |*rg| rg.columns.items[1].meta_data.?.statistics.?.null_count = 0;
        }
    }.edit });
    defer gpa.free(poisoned);
    const meta = try metadata.open(a, poisoned);
    try testing.expectEqual(@as(?i64, 0), meta.row_groups.items[0].columns.items[1].meta_data.?.statistics.?.null_count);
    for (meta.row_groups.items[0].columns.items) |col|
        try testing.expect(col.meta_data.?.dictionary_page_offset != null);
    for ([_][]const u8{ "count(DISTINCT x) AS d", "count(DISTINCT x) AS d,count(*) AS c" }) |aggregate| {
        var ledger: core_memory.SharedBudget = .{ .limit = 1 << 30 };
        var args: MultiAggArgs = .{
            .inputs = &.{.{ .name = "M1-poisoned-NULL", .bytes = poisoned }},
            .aggregate = aggregate,
            .group_by = "k",
            .parallelism = 1,
            .max_memory = ledger.limit,
            .ledger = &ledger,
            .integer_records_min_rows = 0,
            .integer_records = false,
            .distinct_pairs = .off,
            .byte_records = false,
            .msd_records = false,
            .dense = false,
            .plain_rules = false,
            .trust_stats = false,
            // Ensure the underestimated stream reaches the physical-record check instead of exhausting
            // scatter capacity first; both refusals must take the same established replay.
            .byte_top_partitions = 16384,
        };
        var want = try runMultiAggregate(gpa, a, args);
        defer want.deinit(gpa);
        try testing.expectEqual(groups, want.output.group_rows.?.len);
        for (want.output.group_rows.?, 0..) |row, i| {
            try testing.expectEqual(@as(i64, @intFromBool(i < 64)), row[1].i);
            if (row.len == 3) try testing.expectEqual(@as(i64, if (i < 64) 17 else 16), row[2].i);
        }
        args.integer_records = true;
        args.distinct_pairs = .on;
        var taken: std.EnumSet(FastPath) = .{};
        args.taken = &taken;
        try testing.expectEqual(.pair_distinct, (try explainAggregate(gpa, a, args)).record_path.?.layout);
        const ledger_before = ledger.used.load(.monotonic);
        const refusal: anyerror = if (prepareAggregate(gpa, a, args, false)) |result| blk: {
            var unexpected = result;
            unexpected.deinit(gpa);
            break :blk error.TestUnexpectedResult;
        } else |err| err;
        try testing.expectEqual(error.ExceededMemoryBudget, refusal);
        try testing.expect(taken.contains(.integer_records));
        try testing.expectEqual(ledger_before, ledger.used.load(.monotonic));
        var got = try runMultiAggregate(gpa, a, args);
        defer got.deinit(gpa);
        // The canonical fallback may itself use foldDistinctPairs; its result flag does not identify the owner.
        // The armed refusal above and successful runMultiAggregate prove the whole-attempt retry.
        try testing.expectEqualDeep(want.output.group_rows, got.output.group_rows);
        try testing.expectEqualDeep(want.state.group_col_types, got.state.group_col_types);
    }
}

test "M1 finish partitions full and top-H states including preparation cleanup ties and failed starts" {
    const a = testing.allocator;
    const calls = [_]expr_agg.AggCall{
        .{ .func = .count, .alias = "c", .result = .i64, .arg = null, .where = null },
        .{ .func = .min, .alias = "lo", .result = .bytes, .arg = null, .where = null },
    };
    for ([_]usize{ 1, 16 }) |workers| for ([_]enum { full, top, first }{ .full, .top, .first }) |mode| {
        const top = mode == .top;
        var tables: [3]expr_agg.GroupTable = undefined;
        for (&tables) |*t| t.* = .init(a, 256 << 20);
        defer {
            finishGroupRanges(&tables, null, null, workers, true);
            for (&tables) |*t| t.deinitStorage();
        }
        const n = 16391;
        for (&tables, 0..) |*t, ti| for (0..n) |i| {
            var key: [32]u8 = undefined;
            const bytes = try std.fmt.bufPrint(&key, "g-{d}", .{i});
            const id = try t.getOrInsert(bytes, &calls, i * 5 + 2 - ti);
            const at = @as(usize, id) * calls.len;
            t.accumulators.items[at] = .{ .count = i % 7 + 1 };
            try setMergeTestString(a, &t.accumulators.items[at + 1], if (ti == 1) "best" else "other");
        };
        const refs = try a.alloc(MergeGroupRef, n * tables.len);
        defer a.free(refs);
        finishGroupRanges(&tables, refs, null, workers, false);
        for (refs, 0..) |ref, i| {
            try testing.expectEqual(@as(u32, @intCast(i / n)), ref.table);
            try testing.expectEqual(@as(u32, @intCast(i % n)), ref.group);
        }
        var slots: std.ArrayList(MergedSlot) = .empty;
        defer slots.deinit(a);
        var accs: std.ArrayList(expr_agg.Accumulator) = .empty;
        defer {
            freeAccumulatorRanges(a, accs.items, workers);
            accs.deinit(a);
        }
        const cut: ?ResultCut = if (top) .{ .a_idx = 0, .k = 7 } else null;
        if (workers == 16) spawn_util.injectFailureAfter(1);
        defer spawn_util.resetFailure();
        if (mode == .first) {
            const keys = (try firstCandidatesParallel(a, &tables, 7, workers)).?;
            defer a.free(keys);
            try testing.expectEqual(@as(usize, 7), keys.len);
            try mergeCandidates(a, a, &tables, keys, &calls, workers, &slots, &accs);
        } else try mergeFoldedRefs(a, a, &tables, refs, &calls, workers, cut, false, &slots, &accs);
        if (top) try keepCountTop(a, a, &calls, .{ .a_idx = 0, .k = 7 }, &slots, &accs);
        try sortSlotsByFirst(a, slots.items, workers);
        for ([_]bool{ false, true }) |typed| try testing.expectEqual(
            answerBytes(slots.items, &.{.{ .lane = .string, .nullable = true }}, &.{.string}, typed),
            answerBytesParallel(slots.items, &.{.{ .lane = .string, .nullable = true }}, &.{.string}, typed, workers),
        );
        try testing.expectEqual(@as(usize, if (mode != .full) 7 else n), slots.items.len);
        for (slots.items, 0..) |slot, i| {
            const id = if (top) 6 + i * 7 else i;
            try testing.expectEqual(@as(u64, id * 5), slot.first);
            try testing.expectEqual(@as(u64, 3 * (id % 7 + 1)), accs.items[slot.idx].count);
            try testing.expectEqualStrings("best", accs.items[slot.idx + 1].min_bytes.?);
        }
    };
}

test "R11 partition COUNT cut builds only limit plus offset when no partition reaches the cut" {
    const gpa = testing.allocator;
    var arena = std.heap.ArenaAllocator.init(gpa);
    defer arena.deinit();
    const a = arena.allocator();
    var keys: [4096][]const u8 = undefined;
    var values: [4096][]const u8 = undefined;
    for (&keys, &values, 0..) |*k, *v, i| {
        k.* = try std.fmt.allocPrint(a, "source-with-a-long-shared-prefix-and-independent-group-{d}", .{i % 128});
        v.* = try std.fmt.allocPrint(a, "destination-with-another-long-shared-prefix-{d}", .{i % 8});
    }
    const columns = [_]expr_agg.OutputCol{
        .{ .name = "k", .col = .{ .string = .{ .values = &keys } }, .parquet_type = .BYTE_ARRAY, .nullable = false },
        .{ .name = "v", .col = .{ .string = .{ .values = &values } }, .parquet_type = .BYTE_ARRAY, .nullable = false },
    };
    var sink: @import("../writer/streaming.zig").MemorySink = .{ .arena = a };
    _ = try consumer.writeAggregateRows(a, sink.sink(), &columns, keys.len, .SNAPPY, .{}, .{});
    var meta = try metadata.open(a, sink.buffer.items);
    const groups = try a.alloc(schema.RowGroup, 16);
    @memset(groups, meta.row_groups.items[0]);
    meta.row_groups.items = groups;
    meta.num_rows *= @intCast(groups.len);
    var args: MultiAggArgs = .{
        .inputs = &.{.{ .name = "partition-cut", .bytes = sink.buffer.items }},
        .metas = &.{meta},
        .aggregate = "count(*) AS c",
        .group_by = "k,v",
        .max_memory = 256 << 20,
        .min_bytes_per_worker = 1,
        .integer_records = false,
        .byte_records = false,
        .msd_records = false,
        .dense = false,
        .plain_rules = false,
        .private_runs = false,
        .ndv_union = false,
        .partition_cost = false,
        .spill = false,
    };
    var want = try runMultiAggregate(gpa, a, args);
    defer want.deinit(gpa);
    try testing.expectEqual(@as(usize, 128), want.output.group_rows.?.len);
    const order: expr_ast.ResultOrder = .{
        .keys = &.{.{ .column = 2, .descending = true }},
        .offset = 12,
        .limit = 5,
    };
    try want.output.applyResultOrder(gpa, order);
    args.result_order = order;
    args.spill = true;
    args.spill_groups = 64;
    args.spill_rows = 128;
    var image: std.ArrayList(u8) = .empty;
    const key_cols = [_]@import("../filter/eval.zig").Batch.Column{
        .{ .string = .{ .values = &keys } },
        .{ .string = .{ .values = &values } },
    };
    var in_partition: [32]usize = @splat(0);
    for (0..128) |row| {
        try expr_agg.serializeRowKey(&image, a, &key_cols, row);
        in_partition[std.hash.Wyhash.hash(0, image.items) >> 59] += 1;
    }
    for (in_partition) |n| try testing.expect(n < 17);
    for ([_]usize{ 1, 16 }) |workers| {
        args.parallelism = workers;
        var got = try runMultiAggregate(gpa, a, args);
        defer got.deinit(gpa);
        try testing.expect(got.part_agg);
        // Complete partitions together contain 128 groups, but each is below 17. The global cut must run before
        // building either string output, not merely when the engine later applies LIMIT/OFFSET.
        try testing.expectEqual(@as(usize, 17), got.output.group_rows.?.len);
        try got.output.applyResultOrder(gpa, order);
        try testing.expectEqualDeep(want.output.group_rows, got.output.group_rows);
    }
}

test "STRGROUP grouped strings survive local partition and typed output ownership" {
    const gpa = testing.allocator;
    var arena = std.heap.ArenaAllocator.init(gpa);
    defer arena.deinit();
    const a = arena.allocator();
    const n = 768;
    var keys: [n][]const u8 = undefined;
    var vals: [n][]const u8 = undefined;
    var kd: [n]u32 = undefined;
    var vd: [n]u32 = undefined;
    const long = try a.alloc(u8, 70001);
    @memset(long, 'q');
    for (&keys, &vals, &kd, &vd, 0..) |*k, *v, *dk, *dv, i| {
        k.* = switch (i % 193) {
            0 => "",
            1 => long,
            else => try std.fmt.allocPrint(a, "key-{d}", .{i % 193}),
        };
        v.* = try std.fmt.allocPrint(a, "value-{d:0>4}", .{n - i});
        dk.* = @intFromBool(i % 31 != 0);
        dv.* = @intFromBool(i % 17 != 0);
    }
    const cols = [_]expr_agg.OutputCol{
        .{ .name = "k", .col = .{ .string = .{ .values = &keys, .def_levels = &kd, .max_def = 1 } }, .parquet_type = .BYTE_ARRAY, .nullable = true },
        .{ .name = "v", .col = .{ .string = .{ .values = &vals, .def_levels = &vd, .max_def = 1 } }, .parquet_type = .BYTE_ARRAY, .nullable = true },
    };
    var bytes: @import("../writer/streaming.zig").MemorySink = .{ .arena = a };
    _ = try consumer.writeAggregateRows(a, bytes.sink(), &cols, n, .SNAPPY, .{}, .{});
    var meta = try metadata.open(a, bytes.buffer.items);
    const rgs = try a.alloc(schema.RowGroup, 16);
    @memset(rgs, meta.row_groups.items[0]);
    meta.row_groups.items = rgs;
    meta.num_rows *= @intCast(rgs.len);
    var args: MultiAggArgs = .{
        .inputs = &.{.{ .name = "STRGROUP", .bytes = bytes.buffer.items }},
        .metas = &.{meta},
        .aggregate = "count(*) AS c,min(k) AS lk,max(k) AS hk,min(v) AS lv,max(v) AS hv",
        .group_by = "k",
        .parallelism = 1,
        .min_bytes_per_worker = 1,
        .scan_all = true,
        .integer_records = false,
        .byte_records = false,
        .msd_records = false,
        .dense = false,
        .part_cap = 8,
        .part_probe = 16,
    };
    var want = try runMultiAggregate(gpa, a, args);
    defer want.deinit(gpa);
    const rows = want.output.group_rows.?;
    try testing.expectEqual(@as(usize, 194), rows.len);
    for (rows) |row| {
        var count: i64 = 0;
        var lo: ?[]const u8 = null;
        var hi: ?[]const u8 = null;
        for (keys, vals, kd, vd) |k, v, dk, dv| {
            const same = if (row[0] == .null_val) dk == 0 else dk == 1 and std.mem.eql(u8, row[0].s, k);
            if (!same) continue;
            count += 16;
            if (dv == 0) continue;
            if (lo == null or std.mem.order(u8, v, lo.?) == .lt) lo = v;
            if (hi == null or std.mem.order(u8, v, hi.?) == .gt) hi = v;
        }
        try testing.expectEqual(count, row[1].i);
        try testing.expectEqualDeep(row[0], row[2]);
        try testing.expectEqualDeep(row[0], row[3]);
        if (lo) |v| try testing.expectEqualStrings(v, row[4].s) else try testing.expect(row[4] == .null_val);
        if (hi) |v| try testing.expectEqualStrings(v, row[5].s) else try testing.expect(row[5] == .null_val);
    }
    for ([_]bool{ false, true }) |parts| for ([_]usize{ 1, 16 }) |j| for ([_]usize{ 0, 64 }) |window| for ([_]bool{ false, true }) |typed| {
        args.partagg = parts;
        args.parallelism = j;
        args.decode_batch_rows = window;
        args.typed_answer = typed;
        var got = try runMultiAggregate(gpa, a, args);
        defer got.deinit(gpa);
        try testing.expect(got.output.string_arenas.len > 0);
        if (!typed) {
            try testing.expectEqualDeep(rows, got.output.group_rows.?);
        } else {
            const out = got.output.typed.?;
            try testing.expectEqual(rows.len, out.rows);
            for (rows, 0..) |row, r| for (row, out.columns) |v, col| switch (col) {
                .i64 => |lane| try testing.expectEqual(v.i, lane.values[r]),
                .string => |lane| {
                    try testing.expectEqual(@as(u32, @intFromBool(v != .null_val)), lane.defs[r]);
                    if (v == .s) try testing.expectEqualStrings(v.s, lane.values[r]);
                },
                else => return error.TestUnexpectedResult,
            };
        }
    };
}

test "STRGROUP returned output detaches a query-local arena ledger exactly once" {
    var budget: core_memory.SharedBudget = .{ .limit = 1024 };
    _ = budget.drawExact(128).?;
    var output: AggOutput = .{ .arena_ledger = &budget, .arena_credit = 128 };
    output.detachArenaLedger();
    try testing.expectEqual(@as(usize, 0), budget.used.load(.monotonic));
    _ = budget.drawExact(32).?;
    output.deinit(testing.allocator);
    try testing.expectEqual(@as(usize, 32), budget.used.load(.monotonic));
    budget.release(32);
}

test "grouped string MIN MAX winners refuse at the live ledger without leaking" {
    const gpa = testing.allocator;
    var arena = std.heap.ArenaAllocator.init(gpa);
    defer arena.deinit();
    const a = arena.allocator();
    var keys: [128]i32 = undefined;
    var values: [128][]const u8 = undefined;
    var words: [8][]const u8 = undefined;
    for (&words, 0..) |*word, i| {
        const bytes = try a.alloc(u8, 65536);
        @memset(bytes, @as(u8, @intCast('a' + i)));
        word.* = bytes;
    }
    for (&keys, &values, 0..) |*key, *value, i| {
        key.* = @intCast(i % 16);
        value.* = words[7 - i / 16];
    }
    const cols = [_]expr_agg.OutputCol{
        .{ .name = "k", .col = .{ .i32 = .{ .values = &keys } }, .parquet_type = .INT32, .nullable = false },
        .{ .name = "s", .col = .{ .string = .{ .values = &values } }, .parquet_type = .BYTE_ARRAY, .nullable = false },
    };
    var bytes: @import("../writer/streaming.zig").MemorySink = .{ .arena = a };
    _ = try consumer.writeAggregateRows(a, bytes.sink(), &cols, keys.len, .SNAPPY, .{}, .{});
    for ([_]bool{ false, true }) |encoded| for ([_]usize{ 1, 4 }) |j| {
        var ledger: core_memory.SharedBudget = .{ .limit = 512 << 10 };
        var live: LivePeak = .{ .child = gpa };
        const args: MultiAggArgs = .{
            .inputs = &.{.{ .name = "grouped-winner-ledger", .bytes = bytes.buffer.items }},
            .aggregate = "min(s) AS lo,max(s) AS hi,count(*) AS c",
            .group_by = "k",
            .scan_all = true,
            .max_memory = ledger.limit,
            .ledger = &ledger,
            .decode_limit = ledger.limit / 2,
            .parallelism = j,
            .encoded = encoded,
            .min_bytes_per_worker = 1,
            .dense = false,
            .integer_records = false,
        };
        try testing.expectError(error.ExceededMemoryBudget, runMultiAggregate(live.allocator(), a, args));
        try testing.expectEqual(@as(usize, 0), ledger.used.load(.monotonic));
        try testing.expectEqual(@as(usize, 0), live.live.load(.monotonic));
        // Old grouped winners were outside the ledger, and could grow past this bound before admission noticed.
        try testing.expect(live.peak.load(.monotonic) < 1 << 20);
        var roomy: core_memory.SharedBudget = .{ .limit = 64 << 20 };
        var wide = args;
        wide.ledger = &roomy;
        wide.max_memory = roomy.limit;
        wide.decode_limit = roomy.limit / 2;
        var got = try runMultiAggregate(gpa, a, wide);
        defer got.deinit(gpa);
        try testing.expectEqual(@as(usize, 16), got.output.group_rows.?.len);
        for (got.output.group_rows.?) |row| {
            try testing.expectEqualStrings(words[0], row[1].s);
            try testing.expectEqualStrings(words[7], row[2].s);
            try testing.expectEqual(@as(i128, 8), row[3].i);
        }
    };
}

test "final multipass region retirement releases every charge after partial spawn refusal" {
    const gpa = testing.allocator;
    for ([_]usize{ 0, 1, 2, 3, 4 }) |allowed| {
        var budget: core_memory.SharedBudget = .{ .limit = 64 << 20 };
        var workers: [4]Worker = undefined;
        for (&workers) |*w| {
            var pair = huge_pages.takePair(gpa, WORKER_REGION_RESERVE);
            pair.arena.budget = &budget;
            pair.scratch.budget = &budget;
            const pages = try pair.arena.allocator().alloc(u8, 4 << 20);
            @memset(pages, 7);
            w.* = .{ .gpa = gpa, .inputs = &.{}, .metas = &.{}, .work = &.{}, .cursor = undefined, .agg_calls = &.{}, .plan = undefined, .decode_options = .{}, .region = pair.arena, .scratch_region = pair.scratch };
        }
        try testing.expect(budget.used.load(.monotonic) > 0);
        spawn_util.injectFailureAfter(allowed);
        defer spawn_util.resetFailure();
        releaseRegionsParallel(&workers);
        for (workers) |w| {
            try testing.expect(w.region == null and w.scratch_region == null);
        }
        try testing.expectEqual(@as(usize, 0), budget.used.load(.monotonic));
    }
}

test "packed payload top H preserves integer SUM AVG MIN MAX NULL ties and filtered windows" {
    const gpa = testing.allocator;
    var arena = std.heap.ArenaAllocator.init(gpa);
    defer arena.deinit();
    const a = arena.allocator();
    var k: [2048]i32 = undefined;
    var text: [2048][]const u8 = undefined;
    var value: [2048]i64 = undefined;
    var defs: [2048]u32 = undefined;
    for (&k, &text, &value, &defs, 0..) |*ki, *t, *v, *d, i| {
        ki.* = @intCast(i % 17);
        t.* = if (i % 2 == 0) "first" else "second";
        v.* = @as(i64, @intCast(i % 19)) - 9;
        d.* = @intFromBool(i % 17 != 0);
    }
    const columns = [_]expr_agg.OutputCol{
        .{ .name = "k", .col = .{ .i32 = .{ .values = &k } }, .parquet_type = .INT32, .nullable = false },
        .{ .name = "t", .col = .{ .string = .{ .values = &text } }, .parquet_type = .BYTE_ARRAY, .nullable = false },
        .{ .name = "v", .col = .{ .i64 = .{ .values = &value, .def_levels = &defs, .max_def = 1 } }, .parquet_type = .INT64, .nullable = true },
    };
    var sink: @import("../writer/streaming.zig").MemorySink = .{ .arena = a };
    _ = try consumer.writeAggregateRows(a, sink.sink(), &columns, k.len, .SNAPPY, .{}, .{});
    var meta = try metadata.open(a, sink.buffer.items);
    const rgs = try a.alloc(schema.RowGroup, 16);
    @memset(rgs, meta.row_groups.items[0]);
    meta.row_groups.items = rgs;
    meta.num_rows *= @intCast(rgs.len);
    for ([_]?[]const u8{ null, "k < 12" }) |filter| {
        var args: MultiAggArgs = .{
            .inputs = &.{.{ .name = "packed-payload", .bytes = sink.buffer.items }},
            .metas = &.{meta},
            .aggregate = "count(*) AS c,sum(v) AS s,avg(v) AS a,min(v) AS lo,max(v) AS hi",
            .group_by = "k,t",
            .filter = filter,
            .max_memory = 256 << 20,
            .parallelism = 1,
            .min_bytes_per_worker = 1,
            .integer_records_min_rows = 0,
            .integer_records = false,
            .dense = true,
            .plain_rules = false,
            .result_order = .{ .keys = &.{.{ .column = 2, .descending = true }}, .limit = 9, .offset = 2 },
        };
        var want = try runMultiAggregate(gpa, a, args);
        defer want.deinit(gpa);
        try want.output.applyResultOrder(gpa, args.result_order);
        for ([_]usize{ 1, 4, 16 }) |j| for ([_]usize{ 0, 64 }) |window| {
            args.parallelism = j;
            args.decode_batch_rows = window;
            args.integer_records = true;
            var taken: std.EnumSet(FastPath) = .{};
            args.taken = &taken;
            const ex = try explainAggregate(gpa, a, args);
            try testing.expectEqual(.packed_count_top_h, ex.record_path.?.layout);
            var got = try prepareAggregate(gpa, a, args, false);
            defer got.deinit(gpa);
            try testing.expect(taken.contains(.integer_records));
            try got.output.applyResultOrder(gpa, args.result_order);
            try testing.expectEqualDeep(want.output.group_rows, got.output.group_rows);
            try testing.expectEqualDeep(want.state.group_col_types, got.state.group_col_types);
        };
    }
}

test "single INT64 payload top H owns ordered keys and matches grouping for skewed uniform NULL keys and ties" {
    const gpa = testing.allocator;
    var arena = std.heap.ArenaAllocator.init(gpa);
    defer arena.deinit();
    const a = arena.allocator();
    const n = 4096;
    var k: [n]i64 = undefined;
    var key_defs: [n]u32 = undefined;
    var value: [n]i64 = undefined;
    var defs: [n]u32 = undefined;
    for (0..3) |shape| {
        for (&k, &key_defs, &value, &defs, 0..) |*ki, *kd, *v, *d, i| {
            const at: i64 = @intCast(i);
            ki.* = switch (shape) {
                // Sorted runs: five long ones, then runs of four that tie across the cutoff.
                0 => if (i < 100) @divTrunc(at, 20) else @divTrunc(at, 4),
                // Skewed: a few heavy keys among a wide tail.
                1 => if (i % 3 == 0) @mod(at, 13) else @mod(at * 2654435761, 200000) - 100000,
                // Near-uniform: every key ties.
                else => @mod(at * 40503, 1024),
            };
            kd.* = @intFromBool(shape == 0 or i % 11 != 0);
            v.* = @mod(at * 7919, 2001) - 1000;
            d.* = @intFromBool(i % 17 != 0);
        }
        const columns = [_]expr_agg.OutputCol{
            .{ .name = "k", .col = .{ .i64 = .{ .values = &k, .def_levels = &key_defs, .max_def = 1 } }, .parquet_type = .INT64, .nullable = true },
            .{ .name = "v", .col = .{ .i64 = .{ .values = &value, .def_levels = &defs, .max_def = 1 } }, .parquet_type = .INT64, .nullable = true },
        };
        var sink: @import("../writer/streaming.zig").MemorySink = .{ .arena = a };
        _ = try consumer.writeAggregateRows(a, sink.sink(), &columns, k.len, .SNAPPY, .{}, .{});
        var meta = try metadata.open(a, sink.buffer.items);
        // Copies of one row group overlap, so only the single sorted group proves an ordered key to the owner.
        const rgs = try a.alloc(schema.RowGroup, if (shape == 0) 1 else 8);
        @memset(rgs, meta.row_groups.items[0]);
        meta.row_groups.items = rgs;
        meta.num_rows *= @intCast(rgs.len);
        for ([_]expr_ast.ResultOrder{
            .{ .keys = &.{.{ .column = 1, .descending = true }}, .limit = 10 },
            .{ .keys = &.{.{ .column = 1, .descending = true }}, .limit = 7, .offset = 3 },
        }) |order| {
            var args: MultiAggArgs = .{
                .inputs = &.{.{ .name = "int64-payload", .bytes = sink.buffer.items }},
                .metas = &.{meta},
                .aggregate = "count(*) AS c,sum(v) AS s,avg(v) AS a,min(v) AS lo,max(v) AS hi",
                .group_by = "k",
                .max_memory = 256 << 20,
                .parallelism = 1,
                .min_bytes_per_worker = 1,
                .integer_records_min_rows = 0,
                .integer_records = false,
                .plain_rules = false,
                .result_order = order,
            };
            var want = try runMultiAggregate(gpa, a, args);
            defer want.deinit(gpa);
            try want.output.applyResultOrder(gpa, args.result_order);
            for ([_]usize{ 1, 4, 16 }) |j| for ([_]usize{ 0, 64 }) |window| {
                args.parallelism = j;
                args.decode_batch_rows = window;
                args.integer_records = true;
                var taken: std.EnumSet(FastPath) = .{};
                args.taken = &taken;
                const ex = try explainAggregate(gpa, a, args);
                const owned = ex.record_path != null and ex.record_path.?.layout == .packed_count_top_h;
                // Skewed and NULL keys have no ordered footers and keep the sketch.
                try testing.expectEqual(shape == 0, owned);
                var got = try prepareAggregate(gpa, a, args, false);
                defer got.deinit(gpa);
                if (owned) try testing.expect(taken.contains(.integer_records));
                try got.output.applyResultOrder(gpa, args.result_order);
                try testing.expectEqualDeep(want.output.group_rows, got.output.group_rows);
                try testing.expectEqualDeep(want.state.group_col_types, got.state.group_col_types);
            };
            if (shape != 0) continue;
            // A budget just below the owner's quote declines it before any row is read; grouping answers the same.
            args.parallelism = 4;
            args.decode_batch_rows = 0;
            args.taken = null;
            args.max_memory = (try explainAggregate(gpa, a, args)).record_path.?.reservation - 1;
            const ex = try explainAggregate(gpa, a, args);
            if (ex.record_path) |path| try testing.expect(path.layout != .packed_count_top_h);
            var got = try prepareAggregate(gpa, a, args, false);
            defer got.deinit(gpa);
            try got.output.applyResultOrder(gpa, args.result_order);
            try testing.expectEqualDeep(want.output.group_rows, got.output.group_rows);
        }
    }
}

test "raw DISTINCT batch owners preserve NULLs and replay speculative footer range violations" {
    const gpa = testing.allocator;
    var arena = std.heap.ArenaAllocator.init(gpa);
    defer arena.deinit();
    const a = arena.allocator();
    var k: [2048]i64 = undefined;
    var x: [2048]i64 = undefined;
    var kd: [2048]u32 = undefined;
    var xd: [2048]u32 = undefined;
    for (&k, &x, &kd, &xd, 0..) |*key, *value, *dk, *dx, i| {
        key.* = (1 << 40) + @as(i64, @intCast(i % 33));
        value.* = @intCast(i % 95);
        dk.* = @intFromBool(i % 17 != 0);
        dx.* = @intFromBool(i % 11 != 0);
    }
    const columns = [_]expr_agg.OutputCol{
        .{ .name = "k", .col = .{ .i64 = .{ .values = &k, .def_levels = &kd, .max_def = 1 } }, .parquet_type = .INT64, .nullable = true },
        .{ .name = "x", .col = .{ .i64 = .{ .values = &x, .def_levels = &xd, .max_def = 1 } }, .parquet_type = .INT64, .nullable = true },
    };
    var sink: @import("../writer/streaming.zig").MemorySink = .{ .arena = a };
    _ = try consumer.writeAggregateRows(a, sink.sink(), &columns, k.len, .SNAPPY, .{}, .{});
    var meta = try metadata.open(a, sink.buffer.items);
    const rgs = try a.alloc(schema.RowGroup, 16);
    @memset(rgs, meta.row_groups.items[0]);
    meta.row_groups.items = rgs;
    meta.num_rows *= @intCast(rgs.len);
    for ([_][]const u8{ "count(DISTINCT x) AS d", "count(DISTINCT x) AS d,count(*) AS c" }) |calls| {
        var args: MultiAggArgs = .{
            .inputs = &.{.{ .name = "raw-distinct", .bytes = sink.buffer.items }},
            .metas = &.{meta},
            .aggregate = calls,
            .group_by = "k",
            .parallelism = 1,
            .min_bytes_per_worker = 1,
            .max_memory = 512 << 20,
            .integer_records = false,
            .integer_records_min_rows = 0,
            .distinct_pairs = .off,
            .ndv_union = false,
            .trust_stats = false,
            .result_order = .{ .keys = &.{.{ .column = 1, .descending = true }}, .limit = 10 },
        };
        var want = try runMultiAggregate(gpa, a, args);
        defer want.deinit(gpa);
        try want.output.applyResultOrder(gpa, args.result_order);
        for ([_]bool{ false, true }) |poison| {
            var footer = meta;
            const groups = try a.dupe(schema.RowGroup, meta.row_groups.items);
            footer.row_groups.items = groups;
            var zero: [8]u8 = @splat(0);
            if (poison) for (groups) |*rg| {
                rg.columns.items = try a.dupe(schema.ColumnChunk, rg.columns.items);
                var st = &rg.columns.items[0].meta_data.?.statistics.?;
                st.min_value = &zero;
                st.max_value = &zero;
                st.min = &zero;
                st.max = &zero;
            };
            args.metas = &.{footer};
            args.integer_records = true;
            args.distinct_pairs = .auto;
            for ([_]usize{ 1, 16 }) |j| for ([_]usize{ 0, 64 }) |window| {
                args.parallelism = j;
                args.decode_batch_rows = window;
                var taken: std.EnumSet(FastPath) = .{};
                args.taken = &taken;
                const ex = try explainAggregate(gpa, a, args);
                try testing.expectEqual(.pair_distinct, ex.record_path.?.layout);
                if (poison) {
                    try testing.expectError(error.ExceededMemoryBudget, prepareAggregate(gpa, a, args, false));
                    try testing.expect(taken.contains(.integer_records));
                }
                var got = try runMultiAggregate(gpa, a, args);
                defer got.deinit(gpa);
                try got.output.applyResultOrder(gpa, args.result_order);
                try testing.expectEqualDeep(want.output.group_rows, got.output.group_rows);
            };
        }
    }
}

test "scalar long string DISTINCT keeps NULL empty and windowed answers across the cached partition fanout" {
    const gpa = testing.allocator;
    var arena = std.heap.ArenaAllocator.init(gpa);
    defer arena.deinit();
    const a = arena.allocator();
    var text: [2048][]const u8 = undefined;
    var defs: [2048]u32 = undefined;
    const long = try a.alloc(u8, 70001);
    @memset(long, 'x');
    for (&text, &defs, 0..) |*t, *d, i| {
        t.* = switch (i % 33) {
            0 => "",
            1 => long,
            else => try std.fmt.allocPrint(a, "key-{d}", .{i % 33}),
        };
        d.* = @intFromBool(i % 17 != 0);
    }
    const columns = [_]expr_agg.OutputCol{.{
        .name = "k",
        .col = .{ .string = .{ .values = &text, .def_levels = &defs, .max_def = 1 } },
        .parquet_type = .BYTE_ARRAY,
        .nullable = true,
    }};
    var sink: @import("../writer/streaming.zig").MemorySink = .{ .arena = a };
    _ = try consumer.writeAggregateRows(a, sink.sink(), &columns, text.len, .SNAPPY, .{}, .{});
    var meta = try metadata.open(a, sink.buffer.items);
    const groups = try a.alloc(schema.RowGroup, 16);
    @memset(groups, meta.row_groups.items[0]);
    meta.row_groups.items = groups;
    meta.num_rows *= @intCast(groups.len);
    for ([_]bool{ false, true }) |encoded| for ([_]usize{ 1, 16 }) |j| for ([_]usize{ 0, 64 }) |window| {
        const args: MultiAggArgs = .{
            .inputs = &.{.{ .name = "long-distinct", .bytes = sink.buffer.items }},
            .metas = &.{meta},
            .aggregate = "count(DISTINCT k) AS d",
            .max_memory = 256 << 20,
            .parallelism = j,
            .min_bytes_per_worker = 1,
            .encoded = encoded,
            .decode_batch_rows = window,
        };
        try testing.expect((try explainAggregate(gpa, a, args)).record_path == null);
        var got = try runMultiAggregate(gpa, a, args);
        defer got.deinit(gpa);
        try testing.expectEqual(@as(i128, 33), got.output.aggs[0].value.i);
    };
}

test "shared string scan preserves full grouped and scalar answers NULLs original order and both readers" {
    const gpa = testing.allocator;
    var arena = std.heap.ArenaAllocator.init(gpa);
    defer arena.deinit();
    const a = arena.allocator();
    var text: [512][]const u8 = undefined;
    var key_defs: [512]u32 = undefined;
    var values: [512]i64 = undefined;
    var defs: [512]u32 = undefined;
    const big = try a.alloc(u8, 70001);
    @memset(big, 'x');
    for (&text, &key_defs, &values, &defs, 0..) |*key, *kd, *value, *d, i| {
        key.* = switch (i % 128) {
            0 => "",
            1 => big,
            2 => "a\x00b",
            else => try std.fmt.allocPrint(a, "a wide plain key for the shared string table {d}", .{i % 128}),
        };
        kd.* = @intFromBool(i % 19 != 0);
        value.* = @as(i64, @intCast(i % 13)) - 6;
        d.* = @intFromBool(i % 7 != 0);
    }
    const columns = [_]expr_agg.OutputCol{
        .{ .name = "k", .parquet_type = .BYTE_ARRAY, .nullable = true, .col = .{ .string = .{ .values = &text, .def_levels = &key_defs, .max_def = 1 } } },
        .{ .name = "v", .parquet_type = .INT64, .nullable = true, .col = .{ .i64 = .{ .values = &values, .def_levels = &defs, .max_def = 1 } } },
    };
    var bytes: @import("../writer/streaming.zig").MemorySink = .{ .arena = a };
    _ = try consumer.writeAggregateRows(a, bytes.sink(), &columns, text.len, .SNAPPY, .{}, .{});
    var meta = try metadata.open(a, bytes.buffer.items);
    const groups = try a.alloc(schema.RowGroup, 16);
    @memset(groups, meta.row_groups.items[0]);
    meta.row_groups.items = groups;
    meta.num_rows *= @intCast(groups.len);
    for ([_]bool{ false, true }) |scalar| for ([_]?[]const u8{ null, "v >= -4", "k <> ''" }) |filter| {
        var args: MultiAggArgs = .{
            .inputs = &.{.{ .name = "shared-string", .bytes = bytes.buffer.items }},
            .metas = &.{meta},
            .aggregate = if (scalar) "count(DISTINCT k) AS d" else "count(*) AS c,sum(v) AS s,avg(v) AS av,min(v) AS lo,max(v) AS hi,count(v) AS n",
            .group_by = if (scalar) null else "k",
            .filter = filter,
            .max_memory = 256 << 20,
            .parallelism = 1,
            .min_bytes_per_worker = 1,
            .shared_strings = false,
        };
        var want = try runMultiAggregate(gpa, a, args);
        defer want.deinit(gpa);
        for ([_]bool{ false, true }) |encoded| for ([_]usize{ 1, 16 }) |j| for ([_]usize{ 0, 64 }) |window| {
            args.shared_strings = true;
            args.encoded = encoded;
            args.parallelism = j;
            args.decode_batch_rows = window;
            var taken: std.EnumSet(FastPath) = .{};
            args.taken = &taken;
            const ex = try explainAggregate(gpa, a, args);
            try testing.expectEqual(.shared_string_table, ex.record_path.?.layout);
            try testing.expectEqualStrings("shared_string_table", ex.strategyName());
            var got = try prepareAggregate(gpa, a, args, false);
            defer got.deinit(gpa);
            try testing.expect(taken.contains(.shared_strings));
            if (scalar) {
                try testing.expectEqualDeep(want.output.aggs, got.output.aggs);
            } else {
                try testing.expectEqualDeep(want.output.group_cols, got.output.group_cols);
                try testing.expectEqualDeep(want.output.group_rows, got.output.group_rows);
                try testing.expectEqualDeep(want.state.group_col_types, got.state.group_col_types);
            }
        };
    };
    // A setup decline leaves the same occupied ledger, even when the canonical decoder also cannot fit.
    var ledger: core_memory.SharedBudget = .{ .limit = 16 << 20 };
    const held = ledger.drawExact(ledger.limit - 1).?;
    defer ledger.release(held);
    var taken: std.EnumSet(FastPath) = .{};
    const tight: MultiAggArgs = .{
        .inputs = &.{.{ .name = "shared-string", .bytes = bytes.buffer.items }},
        .metas = &.{meta},
        .aggregate = "count(DISTINCT k) AS d",
        .max_memory = ledger.limit,
        .ledger = &ledger,
        .parallelism = 1,
        .taken = &taken,
    };
    try testing.expectError(error.ExceededMemoryBudget, prepareAggregate(gpa, a, tight, false));
    try testing.expect(!taken.contains(.shared_strings));
    try testing.expect((try explainAggregate(gpa, a, tight)).record_path == null);
    try testing.expectEqual(held, ledger.used.load(.monotonic));
}

test "shared string ranked LIMIT keeps exact winners NULL keys cutoff ties and refusal replay" {
    const gpa = testing.allocator;
    var arena = std.heap.ArenaAllocator.init(gpa);
    defer arena.deinit();
    const a = arena.allocator();
    var text: [1024][]const u8 = undefined;
    var key_defs: [1024]u32 = undefined;
    var values: [1024]i64 = undefined;
    var defs: [1024]u32 = undefined;
    const big = try a.alloc(u8, 70001);
    @memset(big, 'x');
    for (&text, &key_defs, &values, &defs, 0..) |*key, *kd, *value, *d, i| {
        // Groups 3..8 lead by 10-11 rows; the rest tie at one or two across every cutoff. NULL keys form a group.
        // Half the rows distinct keeps the writer off its dictionary, so chunks are PLAIN as the owner requires.
        const g = if (i < 64) 3 + i % 6 else i % 512;
        key.* = switch (g) {
            0 => "",
            1 => big,
            2 => "a\x00b",
            else => try std.fmt.allocPrint(a, "a wide plain key for the shared string table {d}", .{g}),
        };
        kd.* = @intFromBool(i % 23 != 5);
        value.* = @as(i64, @intCast((i * 7919) % 101)) - 50;
        d.* = @intFromBool(i % 11 != 0);
    }
    const columns = [_]expr_agg.OutputCol{
        .{ .name = "k", .parquet_type = .BYTE_ARRAY, .nullable = true, .col = .{ .string = .{ .values = &text, .def_levels = &key_defs, .max_def = 1 } } },
        .{ .name = "v", .parquet_type = .INT64, .nullable = true, .col = .{ .i64 = .{ .values = &values, .def_levels = &defs, .max_def = 1 } } },
    };
    var bytes: @import("../writer/streaming.zig").MemorySink = .{ .arena = a };
    _ = try consumer.writeAggregateRows(a, bytes.sink(), &columns, text.len, .SNAPPY, .{}, .{});
    var meta = try metadata.open(a, bytes.buffer.items);
    const groups = try a.alloc(schema.RowGroup, 8);
    @memset(groups, meta.row_groups.items[0]);
    meta.row_groups.items = groups;
    meta.num_rows *= @intCast(groups.len);
    const Shape = struct { aggregate: []const u8, order: expr_ast.ResultOrder };
    const shapes = [_]Shape{
        .{ .aggregate = "count(*) AS c", .order = .{ .keys = &.{.{ .column = 1, .descending = true }}, .limit = 5 } },
        .{ .aggregate = "count(*) AS c", .order = .{ .keys = &.{.{ .column = 1, .descending = true }}, .limit = 3, .offset = 4 } },
        .{ .aggregate = "sum(v) AS s", .order = .{ .keys = &.{.{ .column = 1, .descending = true }}, .limit = 10 } },
        .{ .aggregate = "sum(v) AS s", .order = .{ .keys = &.{.{ .column = 1, .descending = false }}, .limit = 7 } },
        .{
            .aggregate = "count(*) AS c,sum(v) AS s,avg(v) AS av,min(v) AS lo,max(v) AS hi",
            .order = .{ .keys = &.{.{ .column = 1, .descending = true }}, .limit = 10 },
        },
        .{
            .aggregate = "count(*) AS c,sum(v) AS s,avg(v) AS av,min(v) AS lo,max(v) AS hi",
            .order = .{ .keys = &.{.{ .column = 3, .descending = true }}, .limit = 4 },
        },
        .{ .aggregate = "count(*) AS c", .order = .{ .keys = &.{.{ .column = 1, .descending = true }}, .limit = 0 } },
    };
    for (shapes) |shape| for ([_]?[]const u8{ null, "v >= -20" }) |filter| {
        var args: MultiAggArgs = .{
            .inputs = &.{.{ .name = "ranked-string", .bytes = bytes.buffer.items }},
            .metas = &.{meta},
            .aggregate = shape.aggregate,
            .group_by = "k",
            .filter = filter,
            .max_memory = 256 << 20,
            .parallelism = 1,
            .min_bytes_per_worker = 1,
            .shared_strings = false,
            .result_order = shape.order,
        };
        var want = try runMultiAggregate(gpa, a, args);
        defer want.deinit(gpa);
        try want.output.applyResultOrder(gpa, args.result_order);
        for ([_]usize{ 1, 4, 16 }) |j| for ([_]usize{ 0, 64 }) |window| {
            args.shared_strings = true;
            args.parallelism = j;
            args.decode_batch_rows = window;
            var taken: std.EnumSet(FastPath) = .{};
            args.taken = &taken;
            const ex = try explainAggregate(gpa, a, args);
            try testing.expectEqual(.shared_string_table, ex.record_path.?.layout);
            var got = try prepareAggregate(gpa, a, args, false);
            defer got.deinit(gpa);
            try testing.expect(taken.contains(.shared_strings));
            try got.output.applyResultOrder(gpa, args.result_order);
            try testing.expectEqualDeep(want.output.group_cols, got.output.group_cols);
            try testing.expectEqualDeep(want.output.group_rows, got.output.group_rows);
        };
    };
    // Orders the cut cannot rank stay on the generic path.
    var unranked: MultiAggArgs = .{
        .inputs = &.{.{ .name = "ranked-string", .bytes = bytes.buffer.items }},
        .metas = &.{meta},
        .aggregate = "count(*) AS c",
        .group_by = "k",
        .max_memory = 256 << 20,
        .parallelism = 1,
        .result_order = .{ .keys = &.{.{ .column = 0, .descending = false }}, .limit = 3 },
    };
    try testing.expect((try explainAggregate(gpa, a, unranked)).record_path == null);
    unranked.result_order = .{ .keys = &.{.{ .column = 1, .descending = true }} };
    try testing.expect((try explainAggregate(gpa, a, unranked)).record_path == null);
    // Under a shared ledger mostly held elsewhere the owner answers exactly or replays on the generic path.
    var ledger: core_memory.SharedBudget = .{ .limit = 24 << 20 };
    var taken: std.EnumSet(FastPath) = .{};
    var tight: MultiAggArgs = .{
        .inputs = &.{.{ .name = "ranked-string", .bytes = bytes.buffer.items }},
        .metas = &.{meta},
        .aggregate = "count(*) AS c,sum(v) AS s",
        .group_by = "k",
        .max_memory = ledger.limit,
        .parallelism = 4,
        .min_bytes_per_worker = 1,
        .shared_strings = false,
        .result_order = .{ .keys = &.{.{ .column = 1, .descending = true }}, .limit = 6 },
    };
    var want = try runMultiAggregate(gpa, a, tight);
    defer want.deinit(gpa);
    try want.output.applyResultOrder(gpa, tight.result_order);
    const held = ledger.drawExact(ledger.limit - (ledger.limit / 4 + (1 << 20))).?;
    tight.shared_strings = true;
    tight.ledger = &ledger;
    tight.taken = &taken;
    {
        var got = try prepareAggregate(gpa, a, tight, false);
        defer got.deinit(gpa);
        try got.output.applyResultOrder(gpa, tight.result_order);
        try testing.expectEqualDeep(want.output.group_rows, got.output.group_rows);
        try testing.expect(ledger.used.load(.monotonic) <= ledger.limit);
    }
    ledger.release(held);
}

/// Wide PLAIN string keys with NULL and empty keys and nullable INT64 values, in row groups of `rg_rows`.
fn rankedStringFile(a: std.mem.Allocator, rows: usize, rg_rows: usize, key_bytes: usize, distinct: usize) !struct { bytes: []const u8, meta: schema.FileMetaData } {
    const text = try a.alloc([]const u8, rows);
    const key_defs = try a.alloc(u32, rows);
    const values = try a.alloc(i64, rows);
    const defs = try a.alloc(u32, rows);
    const pad = try a.alloc(u8, key_bytes);
    @memset(pad, 'k');
    for (text, key_defs, values, defs, 0..) |*key, *kd, *value, *d, i| {
        // The varying part leads, so prefix (delta) encoding keeps each key's full width on the page.
        key.* = if (i % 67 == 0) "" else try std.fmt.allocPrint(a, "{d}{s}", .{ (i * 7907) % distinct, pad });
        kd.* = @intFromBool(i % 41 != 0);
        value.* = @as(i64, @intCast((i * 71) % 61)) - 30;
        d.* = @intFromBool(i % 7 != 0);
    }
    var sink: @import("../writer/streaming.zig").MemorySink = .{ .arena = a };
    var file = try consumer.AggregateFile.init(a, sink.sink());
    defer file.deinit();
    var columns: [2]expr_agg.OutputCol = undefined;
    var at: usize = 0;
    while (at < rows) : (at += rg_rows) {
        const n = @min(rg_rows, rows - at);
        columns = .{
            .{ .name = "k", .parquet_type = .BYTE_ARRAY, .nullable = true, .col = .{ .string = .{ .values = text[at..][0..n], .def_levels = key_defs[at..][0..n], .max_def = 1 } } },
            .{ .name = "v", .parquet_type = .INT64, .nullable = true, .col = .{ .i64 = .{ .values = values[at..][0..n], .def_levels = defs[at..][0..n], .max_def = 1 } } },
        };
        try file.append(&columns, @intCast(n), .SNAPPY, .{}, .{});
    }
    _ = try file.finish(&columns);
    return .{ .bytes = sink.buffer.items, .meta = try metadata.open(a, sink.buffer.items) };
}

test "filtered long-string top-k keeps tied cutoff groups by first occurrence at every thread count" {
    const gpa = testing.allocator;
    var arena = std.heap.ArenaAllocator.init(gpa);
    defer arena.deinit();
    const a = arena.allocator();
    const rows = 2048;
    const copies = 8;
    const text = try a.alloc([]const u8, rows);
    const key_defs = try a.alloc(u32, rows);
    const flags = try a.alloc(i64, rows);
    const pad = try a.alloc(u8, 72);
    @memset(pad, 'k');
    const tied = [_][]const u8{ "alpha", "bravo", "charlie", "delta" };
    // alpha leads. bravo, charlie and delta tie under it; echo is present and loses. Unique keys the filter
    // drops keep the writer off a dictionary, so the chunks stay PLAIN and wide enough for this owner.
    for (text, key_defs, flags, 0..) |*key, *kd, *flag, i| {
        kd.* = 1;
        if (i < 4) {
            key.* = try std.fmt.allocPrint(a, "{s}-{s}", .{ tied[i], pad });
            flag.* = 1;
        } else if (i == 4) {
            key.* = try std.fmt.allocPrint(a, "echo-{s}", .{pad});
            flag.* = 1;
        } else if (i < 16) {
            key.* = text[0];
            flag.* = 1;
        } else if (i < 22) {
            key.* = text[1];
            flag.* = 1;
        } else if (i < 28) {
            key.* = text[2];
            flag.* = 1;
        } else if (i < 34) {
            key.* = text[3];
            flag.* = 1;
        } else if (i % 2 == 0) {
            key.* = try std.fmt.allocPrint(a, "{d}-{s}", .{ i, pad });
            flag.* = 0;
        } else {
            key.* = try std.fmt.allocPrint(a, "late-{d}-{s}", .{ i, pad });
            flag.* = 1;
        }
    }
    const columns = [_]expr_agg.OutputCol{
        .{ .name = "k", .parquet_type = .BYTE_ARRAY, .nullable = true, .col = .{ .string = .{ .values = text, .def_levels = key_defs, .max_def = 1 } } },
        .{ .name = "f", .parquet_type = .INT64, .nullable = false, .col = .{ .i64 = .{ .values = flags, .def_levels = null, .max_def = 0 } } },
    };
    var bytes: @import("../writer/streaming.zig").MemorySink = .{ .arena = a };
    _ = try consumer.writeAggregateRows(a, bytes.sink(), &columns, rows, .SNAPPY, .{}, .{});
    var meta = try metadata.open(a, bytes.buffer.items);
    const groups = try a.alloc(schema.RowGroup, copies);
    @memset(groups, meta.row_groups.items[0]);
    meta.row_groups.items = groups;
    meta.num_rows *= @intCast(groups.len);
    const order: expr_ast.ResultOrder = .{ .keys = &.{.{ .column = 1, .descending = true }}, .limit = 3 };
    var base: MultiAggArgs = .{
        .inputs = &.{.{ .name = "filtered-tie", .bytes = bytes.buffer.items }},
        .metas = &.{meta},
        .aggregate = "count(*) AS c",
        .group_by = "k",
        .filter = "f > 0",
        .max_memory = 256 << 20,
        .parallelism = 1,
        .min_bytes_per_worker = 1,
        .shared_strings = false,
        .result_order = order,
    };
    var want = try runMultiAggregate(gpa, a, base);
    defer want.deinit(gpa);
    try want.output.applyResultOrder(gpa, order);
    const want_rows = want.output.group_rows.?;
    try testing.expectEqual(@as(usize, 3), want_rows.len);
    // 12, then three keys at 7; the cut keeps the two whose first kept row is earlier.
    try testing.expectEqual(@as(i64, 12 * copies), want_rows[0][1].i);
    try testing.expectEqual(@as(i64, 7 * copies), want_rows[1][1].i);
    try testing.expectEqual(@as(i64, 7 * copies), want_rows[2][1].i);
    try testing.expectEqualStrings(text[0], want_rows[0][0].s);
    try testing.expectEqualStrings(text[1], want_rows[1][0].s);
    try testing.expectEqualStrings(text[2], want_rows[2][0].s);
    var key_bytes: u128 = 0;
    var key_rows: u128 = 0;
    for (meta.row_groups.items) |rg| {
        const cm = rg.columns.items[0].meta_data.?;
        key_bytes += @intCast(@max(0, cm.total_uncompressed_size));
        key_rows += @intCast(@max(0, rg.num_rows));
    }
    const per_row: usize = @intCast(@max(key_bytes / key_rows, 1));
    const saved_cpu = hw.test_cpu;
    defer hw.test_cpu = saved_cpu;
    // The engine sets this when no decode-window env is forced. The default leaves the owner's block unreached.
    base.auto_decode_batches = true;
    // null is this host's L2; 64 KiB and 8 KiB (shared by two) are a mid window and the 64-row floor.
    for ([_]?usize{ null, 64 << 10, 8 << 10 }) |l2_bytes| {
        hw.test_cpu = if (l2_bytes) |share|
            .{ .l2 = .{ .bytes = share, .line_bytes = 64, .max_sharing = 2 } }
        else
            null;
        for ([_]usize{ 1, 4, 16 }) |j| for ([_]usize{ 0, 64 }) |window| {
            base.shared_strings = true;
            base.parallelism = j;
            base.decode_batch_rows = window;
            string_table.test_fit = 0;
            var taken: std.EnumSet(FastPath) = .{};
            base.taken = &taken;
            const ex = try explainAggregate(gpa, a, base);
            try testing.expectEqual(.shared_string_table, ex.record_path.?.layout);
            var got = try prepareAggregate(gpa, a, base, false);
            defer got.deinit(gpa);
            // Snapshot before the expectation calls rowsThatFit, which would overwrite the owner's window.
            const got_fit = string_table.test_fit;
            const want_fit: usize = if (window == 0) string_table.rowsThatFit(per_row) else 0;
            try testing.expectEqual(want_fit, got_fit);
            try testing.expect(taken.contains(.shared_strings));
            try got.output.applyResultOrder(gpa, order);
            try testing.expectEqualDeep(want.output.group_cols, got.output.group_cols);
            try testing.expectEqualDeep(want.output.group_rows, got.output.group_rows);
        };
    }
}

test "filtered shared-string window sizes PLAIN chunks without dictionary-covered ones" {
    const gpa = testing.allocator;
    var arena = std.heap.ArenaAllocator.init(gpa);
    defer arena.deinit();
    const a = arena.allocator();
    const plain_n = 512;
    const dict_n = 512;
    const pad = try a.alloc(u8, 72);
    @memset(pad, 'k');
    const plain_text = try a.alloc([]const u8, plain_n);
    const plain_defs = try a.alloc(u32, plain_n);
    const plain_flags = try a.alloc(i64, plain_n);
    for (plain_text, plain_defs, plain_flags, 0..) |*key, *kd, *flag, i| {
        key.* = try std.fmt.allocPrint(a, "{d}-{s}", .{ i, pad });
        kd.* = 1;
        flag.* = 1;
    }
    const dict_text = try a.alloc([]const u8, dict_n);
    const dict_defs = try a.alloc(u32, dict_n);
    const dict_flags = try a.alloc(i64, dict_n);
    const short = [_][]const u8{ "aa", "bb", "cc", "dd" };
    for (dict_text, dict_defs, dict_flags, 0..) |*key, *kd, *flag, i| {
        key.* = short[i % short.len];
        kd.* = 1;
        flag.* = 1;
    }
    var plain_sink: @import("../writer/streaming.zig").MemorySink = .{ .arena = a };
    var dict_sink: @import("../writer/streaming.zig").MemorySink = .{ .arena = a };
    const plain_cols = [_]expr_agg.OutputCol{
        .{
            .name = "k",
            .parquet_type = .BYTE_ARRAY,
            .nullable = true,
            .col = .{ .string = .{ .values = plain_text, .def_levels = plain_defs, .max_def = 1 } },
        },
        .{
            .name = "f",
            .parquet_type = .INT64,
            .nullable = false,
            .col = .{ .i64 = .{ .values = plain_flags, .def_levels = null, .max_def = 0 } },
        },
    };
    const dict_cols = [_]expr_agg.OutputCol{
        .{
            .name = "k",
            .parquet_type = .BYTE_ARRAY,
            .nullable = true,
            .col = .{ .string = .{ .values = dict_text, .def_levels = dict_defs, .max_def = 1 } },
        },
        .{
            .name = "f",
            .parquet_type = .INT64,
            .nullable = false,
            .col = .{ .i64 = .{ .values = dict_flags, .def_levels = null, .max_def = 0 } },
        },
    };
    _ = try consumer.writeAggregateRows(a, plain_sink.sink(), &plain_cols, plain_n, .SNAPPY, .{}, .{});
    _ = try consumer.writeAggregateRows(a, dict_sink.sink(), &dict_cols, dict_n, .SNAPPY, .{}, .{});
    const plain_meta = try metadata.open(a, plain_sink.buffer.items);
    const dict_meta = try metadata.open(a, dict_sink.buffer.items);
    try testing.expectEqual(@as(usize, 1), plain_meta.row_groups.items.len);
    try testing.expectEqual(@as(usize, 1), dict_meta.row_groups.items.len);
    const plain_cm = &plain_meta.row_groups.items[0].columns.items[0].meta_data.?;
    const dict_cm = &dict_meta.row_groups.items[0].columns.items[0].meta_data.?;
    try testing.expect(!ndv.fullCoverage(plain_cm));
    try testing.expect(ndv.fullCoverage(dict_cm));
    const plain_per: usize = @intCast(@max(@as(u128, @intCast(@max(0, plain_cm.total_uncompressed_size))) /
        @as(u128, @intCast(plain_meta.row_groups.items[0].num_rows)), 1));
    var all_bytes: u128 = @intCast(@max(0, plain_cm.total_uncompressed_size));
    all_bytes += @intCast(@max(0, dict_cm.total_uncompressed_size));
    const all_rows: u128 = @intCast(plain_meta.num_rows + dict_meta.num_rows);
    const all_per: usize = @intCast(@max(all_bytes / all_rows, 1));
    var base: MultiAggArgs = .{
        .inputs = &.{
            .{ .name = "plain-wide", .bytes = plain_sink.buffer.items },
            .{ .name = "dict-narrow", .bytes = dict_sink.buffer.items },
        },
        .metas = &.{ plain_meta, dict_meta },
        .aggregate = "count(*) AS c",
        .group_by = "k",
        .filter = "f > 0",
        .max_memory = 256 << 20,
        .parallelism = 1,
        .min_bytes_per_worker = 1,
        .shared_strings = false,
        .auto_decode_batches = true,
    };
    var want = try runMultiAggregate(gpa, a, base);
    defer want.deinit(gpa);
    const saved_cpu = hw.test_cpu;
    defer hw.test_cpu = saved_cpu;
    // 1 MiB per worker keeps both widths inside the clamps and apart, so a diluted average cannot pass.
    hw.test_cpu = .{ .l2 = .{ .bytes = 1 << 20, .line_bytes = 64, .max_sharing = 1 } };
    base.shared_strings = true;
    string_table.test_fit = 0;
    var taken: std.EnumSet(FastPath) = .{};
    base.taken = &taken;
    var got = try prepareAggregate(gpa, a, base, false);
    defer got.deinit(gpa);
    const got_fit = string_table.test_fit;
    const plain_fit = string_table.rowsThatFit(plain_per);
    const all_fit = string_table.rowsThatFit(all_per);
    try testing.expect(plain_fit != all_fit);
    try testing.expectEqual(plain_fit, got_fit);
    try testing.expect(taken.contains(.shared_strings));
    const want_rows = want.output.group_rows.?;
    const got_rows = got.output.group_rows.?;
    try testing.expectEqual(want_rows.len, got_rows.len);
    try testing.expectEqual(@as(usize, plain_n + short.len), want_rows.len);
    var want_n: i128 = 0;
    var got_n: i128 = 0;
    for (want_rows) |row| want_n += row[1].i;
    for (got_rows) |row| got_n += row[1].i;
    try testing.expectEqual(want_n, got_n);
    try testing.expectEqual(@as(i128, plain_n + dict_n), want_n);
}

test "shared string top-k cut refusals replay at every budget the old path answers" {
    const gpa = testing.allocator;
    var arena = std.heap.ArenaAllocator.init(gpa);
    defer arena.deinit();
    const a = arena.allocator();
    // The reviewer's shape: small row groups keep the scan inside a few MiB, so a cut past it can still refuse.
    const file = try rankedStringFile(a, 180000, 2048, 60, 13007);
    const Shape = struct { aggregate: []const u8, order: expr_ast.ResultOrder };
    const shapes = [_]Shape{
        .{ .aggregate = "count(*) AS c,sum(v) AS s", .order = .{ .keys = &.{.{ .column = 1, .descending = true }}, .limit = 8000 } },
        .{ .aggregate = "sum(v) AS s", .order = .{ .keys = &.{.{ .column = 1, .descending = true }}, .limit = 5000 } },
        .{ .aggregate = "max(v) AS m,count(*) AS c", .order = .{ .keys = &.{.{ .column = 1, .descending = false }}, .limit = 6000, .offset = 100 } },
    };
    var answered: usize = 0;
    for (shapes) |shape| {
        const roomy: MultiAggArgs = .{
            .inputs = &.{.{ .name = "ranked-budget", .bytes = file.bytes }},
            .metas = &.{file.meta},
            .aggregate = shape.aggregate,
            .group_by = "k",
            .max_memory = 256 << 20,
            .parallelism = 1,
            .result_order = shape.order,
        };
        try testing.expectEqual(.shared_string_table, (try explainAggregate(gpa, a, roomy)).record_path.?.layout);
    }
    for (shapes) |shape| for ([_]usize{ 1, 4 }) |j| {
        var half_mib: usize = 2;
        while (half_mib <= 24) : (half_mib += 1) {
            var run_arena = std.heap.ArenaAllocator.init(gpa);
            defer run_arena.deinit();
            const ra = run_arena.allocator();
            var outcomes: [2]?MultiAggResult = .{ null, null };
            defer for (&outcomes) |*o| if (o.*) |*r| r.deinit(gpa);
            for ([_]bool{ false, true }, &outcomes) |owner, *outcome| {
                var ledger: core_memory.SharedBudget = .{ .limit = half_mib << 19 };
                const args: MultiAggArgs = .{
                    .inputs = &.{.{ .name = "ranked-budget", .bytes = file.bytes }},
                    .metas = &.{file.meta},
                    .aggregate = shape.aggregate,
                    .group_by = "k",
                    .max_memory = ledger.limit,
                    .ledger = &ledger,
                    .parallelism = j,
                    .min_bytes_per_worker = 1,
                    .shared_strings = owner,
                    .result_order = shape.order,
                    // As the CLI runs it: half the ledger for decode, bounded batches, a streamed answer.
                    .decode_limit = ledger.limit / 2,
                    .auto_decode_batches = true,
                    .record_answer = true,
                };
                // Only a budget refusal may end either run; an out-of-memory escape fails the test here.
                outcome.* = runMultiAggregate(gpa, ra, args) catch |e| switch (e) {
                    error.ExceededMemoryBudget => null,
                    else => return e,
                };
                if (outcome.*) |*r| try r.output.applyResultOrder(gpa, shape.order);
            }
            const want = outcomes[0] orelse continue;
            const got = outcomes[1] orelse return error.TestExpectedEqual;
            try testing.expectEqualDeep(want.output.group_rows, got.output.group_rows);
            answered += 1;
        }
    };
    try testing.expect(answered > 0);
}

test "shared string partition rank heaps draw the cut's charged allocator" {
    const gpa = testing.allocator;
    var arena = std.heap.ArenaAllocator.init(gpa);
    defer arena.deinit();
    const a = arena.allocator();
    const col = expr_ast.Expr{ .col_ref = .{ .col_idx = 1, .physical_type = .INT64, .expr_type = .i64 } };
    for ([_]expr_agg.AggFunc{ .sum, .max }) |func| {
        const calls = [_]expr_agg.AggCall{.{ .func = func, .alias = "r", .result = .i64, .arg = col, .where = null }};
        const outputs = [_]scan_types.BoundOutputColumn{ .{ .source = .{ .key = 0 }, .name = "k" }, .{ .source = .{ .agg = 0 }, .name = "r" } };
        var ledger: core_memory.SharedBudget = .{ .limit = 64 << 20 };
        var state = (try string_table.State.init(gpa, &ledger, &calls, 0, false, 1)).?;
        defer state.deinit();
        state.bind();
        const n = 1 << 15;
        const keys = try a.alloc([]const u8, n);
        const values = try a.alloc(i64, n);
        for (keys, values, 0..) |*key, *value, i| {
            key.* = try std.fmt.allocPrint(a, "rank-heap-key-{d}", .{i});
            value.* = @intCast(i % 977);
        }
        const batch: eval.Batch = .{
            .cols = &.{ .{ .string = .{ .values = keys } }, .{ .i64 = .{ .values = values } } },
            .num_rows = n,
        };
        try state.locals[0].batch(.{ .batch = &batch }, &.{ 0, 1 });
        var part = &state.parts[0];
        for (state.parts) |*p| if (p.table.groupCount() > part.table.groupCount()) {
            part = p;
        };
        // One group short of the partition, so the cut must rank them all.
        const k = part.table.groupCount() - 1;
        try testing.expect(k >= 64);
        const order: expr_ast.ResultOrder = .{ .keys = &.{.{ .column = 1, .descending = true }}, .limit = k };
        // Room for the k ids, not for a heap of k ranked items beside them.
        const room = k * @sizeOf(u32) + 64;
        const held = ledger.drawExact(ledger.limit - ledger.used.load(.monotonic) - room).?;
        var charged: core_memory.Charged = .{ .child = gpa, .budget = &ledger };
        const before = ledger.used.load(.monotonic);
        if (scan_rank_cut.resultLimitGroups(a, &part.table, &outputs, &calls, order, false, charged.allocator())) |ids| {
            if (ids) |kept| charged.allocator().free(kept);
            return error.TestUnexpectedResult;
        } else |e| try testing.expectEqual(error.ExceededMemoryBudget, charged.refusal(e));
        try testing.expectEqual(before, ledger.used.load(.monotonic));
        ledger.release(held);
        var fits: core_memory.Charged = .{ .child = gpa, .budget = &ledger };
        const ids = (try scan_rank_cut.resultLimitGroups(a, &part.table, &outputs, &calls, order, false, fits.allocator())).?;
        try testing.expectEqual(k, ids.len);
        fits.allocator().free(ids);
        // Both cuts returned every byte they drew.
        try testing.expectEqual(before - held, ledger.used.load(.monotonic));
    }
}

test "shared string top-k charges every projected copy of a winning key" {
    const gpa = testing.allocator;
    var arena = std.heap.ArenaAllocator.init(gpa);
    defer arena.deinit();
    const a = arena.allocator();
    const file = try rankedStringFile(a, 2048, 1024, 20000, 300);
    for ([_]usize{ 1, 2, 3 }) |copies| {
        var ledger: core_memory.SharedBudget = .{ .limit = 1 << 30 };
        var taken: std.EnumSet(FastPath) = .{};
        const select: []const []const u8 = switch (copies) {
            1 => &.{ "k AS a", "count(*) AS c" },
            2 => &.{ "k AS a", "k AS b", "count(*) AS c" },
            else => &.{ "k AS a", "count(*) AS c", "k AS b", "k AS z" },
        };
        const args: MultiAggArgs = .{
            .inputs = &.{.{ .name = "ranked-copies", .bytes = file.bytes }},
            .metas = &.{file.meta},
            .aggregate = "count(*) AS c",
            .group_by = "k",
            .select_cols = try integration_support.selectList(a, select),
            .max_memory = ledger.limit,
            .ledger = &ledger,
            .parallelism = 4,
            .min_bytes_per_worker = 1,
            .taken = &taken,
            .result_order = .{ .keys = &.{.{ .column = if (copies == 3) 1 else copies, .descending = true }}, .limit = 10 },
        };
        var got = try prepareAggregate(gpa, a, args, false);
        defer got.deinit(gpa);
        try testing.expect(taken.contains(.shared_strings));
        // NULL and empty keys win too; the long winners carry the bytes, once for each projection.
        var one_copy: usize = 0;
        var key_bytes: usize = 0;
        for (got.output.group_rows.?) |row| {
            if (row[0] == .s) one_copy += row[0].s.len;
            for (row) |cell| if (cell == .s) {
                key_bytes += cell.s.len;
            };
        }
        try testing.expect(one_copy >= 5 * 20000);
        try testing.expectEqual(copies * one_copy, key_bytes);
        // The ledger still holds the answer it was charged for: every owned cell and key copy.
        try testing.expect(ledger.used.load(.monotonic) >= ownedGroupOutputBytes(got.output));
    }
}

test "shared string top-k leaves coded chunks and selective filters to the established plans" {
    const gpa = testing.allocator;
    var arena = std.heap.ArenaAllocator.init(gpa);
    defer arena.deinit();
    const a = arena.allocator();
    const plain = try rankedStringFile(a, 8192, 1024, 60, 13007);
    // The same shape, but the first row group repeats a few keys: wholly dictionary-coded, as ClickBench's URL is
    // in part. Its later row groups stay PLAIN.
    const mixed = blk: {
        const rows = 8192;
        const text = try a.alloc([]const u8, rows);
        const values = try a.alloc(i64, rows);
        for (text, values, 0..) |*key, *value, i| {
            key.* = try std.fmt.allocPrint(a, "{d} is a wide key repeated in the coded row group", .{if (i < 1024) i % 16 else (i * 7907) % 13007});
            value.* = @intCast(i % 61);
        }
        var sink: @import("../writer/streaming.zig").MemorySink = .{ .arena = a };
        var file = try consumer.AggregateFile.init(a, sink.sink());
        defer file.deinit();
        var columns: [2]expr_agg.OutputCol = undefined;
        var at: usize = 0;
        while (at < rows) : (at += 1024) {
            columns = .{
                .{ .name = "k", .parquet_type = .BYTE_ARRAY, .nullable = false, .col = .{ .string = .{ .values = text[at..][0..1024] } } },
                .{ .name = "v", .parquet_type = .INT64, .nullable = false, .col = .{ .i64 = .{ .values = values[at..][0..1024] } } },
            };
            try file.append(&columns, 1024, .SNAPPY, .{}, .{});
        }
        _ = try file.finish(&columns);
        break :blk .{ .bytes = sink.buffer.items, .meta = try metadata.open(a, sink.buffer.items) };
    };
    try testing.expect(ndv.fullCoverage(&mixed.meta.row_groups.items[0].columns.items[0].meta_data.?));
    try testing.expect(!ndv.fullCoverage(&mixed.meta.row_groups.items[1].columns.items[0].meta_data.?));
    const Case = struct { bytes: []const u8, meta: schema.FileMetaData, filter: ?[]const u8, limit: ?u64, owned: bool };
    const cases = [_]Case{
        .{ .bytes = plain.bytes, .meta = plain.meta, .filter = null, .limit = 10, .owned = true },
        .{ .bytes = plain.bytes, .meta = plain.meta, .filter = "v >= -20", .limit = 10, .owned = true },
        // Statistics say about one row in sixty passes.
        .{ .bytes = plain.bytes, .meta = plain.meta, .filter = "v > 29", .limit = 10, .owned = false },
        .{ .bytes = mixed.bytes, .meta = mixed.meta, .filter = null, .limit = 10, .owned = false },
        // A full GROUP BY over the same mixed chunks keeps the owner it had.
        .{ .bytes = mixed.bytes, .meta = mixed.meta, .filter = null, .limit = null, .owned = true },
    };
    for (cases) |c| for ([_]bool{ false, true }) |owner| {
        const args: MultiAggArgs = .{
            .inputs = &.{.{ .name = "ranked-admission", .bytes = c.bytes }},
            .metas = &.{c.meta},
            .aggregate = "count(*) AS c,sum(v) AS s",
            .group_by = "k",
            .filter = c.filter,
            .max_memory = 256 << 20,
            .parallelism = 4,
            .min_bytes_per_worker = 1,
            .shared_strings = owner,
            .result_order = if (c.limit) |n| .{ .keys = &.{.{ .column = 1, .descending = true }}, .limit = n } else .{},
        };
        const ex = try explainAggregate(gpa, a, args);
        const owned = ex.record_path != null and ex.record_path.?.layout == .shared_string_table;
        try testing.expectEqual(owner and c.owned, owned);
        if (!owner) continue;
        var want_args = args;
        want_args.shared_strings = false;
        var want = try runMultiAggregate(gpa, a, want_args);
        defer want.deinit(gpa);
        try want.output.applyResultOrder(gpa, args.result_order);
        var got = try runMultiAggregate(gpa, a, args);
        defer got.deinit(gpa);
        try got.output.applyResultOrder(gpa, args.result_order);
        try testing.expectEqualDeep(want.output.group_rows, got.output.group_rows);
    };
}

test "DISTINCT second passes read retained mappings after gated pages are released" {
    const gpa = testing.allocator;
    const bytes = (try loadFixture("ci/fixtures/parquet/packed_count_indexed.parquet")) orelse
        return error.SkipZigTest;
    defer gpa.free(bytes);
    var held = std.heap.ArenaAllocator.init(gpa);
    defer held.deinit();
    const a = held.allocator();
    const meta = try metadata.open(a, bytes);
    for ([_][]const u8{
        "count(DISTINCT k) AS d",
        "count(DISTINCT k) AS d,sum(f) AS s,avg(f) AS a,min(f) AS lo,max(f) AS hi,count(f) AS n",
        "count(*) AS c,sum(f) AS s,avg(f) AS a,count(DISTINCT k) AS d",
    }) |aggregate| for ([_]usize{ 1, 4 }) |workers| {
        var trial = std.heap.ArenaAllocator.init(gpa);
        defer trial.deinit();
        const ta = trial.allocator();
        var args: MultiAggArgs = .{
            .inputs = &.{.{ .name = "retained", .bytes = bytes }},
            .metas = &.{meta},
            .group_by = "f",
            .aggregate = aggregate,
            .integer_records = false,
            .distinct_pairs = .off,
            .grouped_distinct_bitmap = .off,
            .plain_rules = false,
            .byte_records = false,
            .msd_records = false,
            .dense = false,
            .integer_records_min_rows = 0,
            .min_bytes_per_worker = 1,
            .parallelism = workers,
            .decode_batch_rows = 64,
            .max_memory = 512 << 20,
            .result_order = .{ .keys = &.{.{ .column = 1, .descending = true }}, .limit = 10 },
        };
        var want = try runMultiAggregate(gpa, ta, args);
        defer want.deinit(gpa);
        try want.output.applyResultOrder(gpa, args.result_order);
        var window: ReleasingScanWindow = .{ .bytes = try ta.dupe(u8, bytes), .meta = &meta };
        window.completed = try ta.alloc(std.atomic.Value(usize), meta.row_groups.items.len);
        for (window.completed.?) |*n| n.* = .init(0);
        const input = window.input(bytes);
        args.inputs = (&input)[0..1];
        args.start_reads = .{ .ctx = &window, .start = ReleasingScanWindow.start, .abandon = ReleasingScanWindow.abandon };
        args.integer_records = true;
        args.distinct_pairs = .auto;
        const explanation = try explainAggregate(gpa, ta, args);
        try testing.expectEqual(.pair_distinct, explanation.record_path.?.layout);
        var got = try runMultiAggregate(gpa, ta, args);
        defer got.deinit(gpa);
        try got.output.applyResultOrder(gpa, args.result_order);
        try testing.expect(got.distinct_pairs);
        try testing.expect(window.releases.load(.monotonic) != 0);
        try testing.expect(window.abandoned != 0);
        try testing.expectEqualDeep(want.output.group_rows, got.output.group_rows);
    };
}

test "DISTINCT compact explain covers measured allocations including repeat caches and tie recovery" {
    const gpa = testing.allocator;
    const bytes = (try loadFixture("ci/fixtures/parquet/packed_count_indexed.parquet")) orelse
        return error.SkipZigTest;
    defer gpa.free(bytes);
    var held = std.heap.ArenaAllocator.init(gpa);
    defer held.deinit();
    const meta = try metadata.open(held.allocator(), bytes);
    for ([_]usize{ 1, 4 }) |workers| {
        const args: MultiAggArgs = .{
            .inputs = &.{.{ .name = "compact-peak", .bytes = bytes }},
            .metas = &.{meta},
            .group_by = "f",
            .aggregate = "count(DISTINCT k) AS d",
            .integer_records_min_rows = 0,
            .min_bytes_per_worker = 1,
            .parallelism = workers,
            .grouped_distinct_bitmap = .off,
            .plain_rules = false,
            .max_memory = 512 << 20,
            .result_order = .{ .keys = &.{.{ .column = 1, .descending = true }}, .limit = 10 },
        };
        const explanation = try explainAggregate(gpa, held.allocator(), args);
        try testing.expectEqual(.pair_distinct, explanation.record_path.?.layout);
        var peak: LivePeak = .{ .child = gpa };
        {
            var trial = std.heap.ArenaAllocator.init(peak.allocator());
            defer trial.deinit();
            var got = try prepareAggregate(peak.allocator(), trial.allocator(), args, false);
            defer got.deinit(peak.allocator());
            try testing.expect(got.distinct_pairs);
            try testing.expect(peak.peak.load(.monotonic) <= explanation.record_path.?.reservation);
        }
        try testing.expectEqual(@as(usize, 0), peak.live.load(.monotonic));
    }
}

test "DISTINCT companion output refusal releases rescan metadata credit" {
    const gpa = testing.allocator;
    const bytes = (try loadFixture("ci/fixtures/parquet/packed_count_indexed.parquet")) orelse
        return error.SkipZigTest;
    defer gpa.free(bytes);
    var held = std.heap.ArenaAllocator.init(gpa);
    defer held.deinit();
    const a = held.allocator();
    const meta = try metadata.open(a, bytes);
    var window: ReleasingScanWindow = .{ .bytes = try a.dupe(u8, bytes), .meta = &meta };
    window.completed = try a.alloc(std.atomic.Value(usize), meta.row_groups.items.len);
    for (window.completed.?) |*n| n.* = .init(0);
    const alias = "terminal_companion_output_alias_unique";
    const RefuseOutput = struct {
        child: std.mem.Allocator,
        window: *ReleasingScanWindow,
        hit: bool = false,
        fn alloc(ctx: *anyopaque, n: usize, alignment: std.mem.Alignment, ret: usize) ?[*]u8 {
            const self: *@This() = @ptrCast(@alignCast(ctx));
            if (self.window.abandoned != 0 and n == alias.len) {
                self.hit = true;
                return null;
            }
            return self.child.rawAlloc(n, alignment, ret);
        }
        fn resize(ctx: *anyopaque, m: []u8, alignment: std.mem.Alignment, n: usize, ret: usize) bool {
            const self: *@This() = @ptrCast(@alignCast(ctx));
            return self.child.rawResize(m, alignment, n, ret);
        }
        fn remap(ctx: *anyopaque, m: []u8, alignment: std.mem.Alignment, n: usize, ret: usize) ?[*]u8 {
            const self: *@This() = @ptrCast(@alignCast(ctx));
            return self.child.rawRemap(m, alignment, n, ret);
        }
        fn free(ctx: *anyopaque, m: []u8, alignment: std.mem.Alignment, ret: usize) void {
            const self: *@This() = @ptrCast(@alignCast(ctx));
            self.child.rawFree(m, alignment, ret);
        }
    };
    var fault: RefuseOutput = .{ .child = gpa, .window = &window };
    const allocator: std.mem.Allocator = .{ .ptr = &fault, .vtable = &.{
        .alloc = RefuseOutput.alloc,
        .resize = RefuseOutput.resize,
        .remap = RefuseOutput.remap,
        .free = RefuseOutput.free,
    } };
    var budget: core_memory.SharedBudget = .{ .limit = 512 << 20 };
    const input = window.input(bytes);
    const args: MultiAggArgs = .{
        .inputs = (&input)[0..1],
        .metas = &.{meta},
        .group_by = "f",
        .aggregate = "count(DISTINCT k) AS " ++ alias ++ ",sum(f) AS s",
        .integer_records_min_rows = 0,
        .min_bytes_per_worker = 1,
        .parallelism = 4,
        .grouped_distinct_bitmap = .off,
        .plain_rules = false,
        .max_memory = budget.limit,
        .ledger = &budget,
        .start_reads = .{ .ctx = &window, .start = ReleasingScanWindow.start, .abandon = ReleasingScanWindow.abandon },
        .result_order = .{ .keys = &.{.{ .column = 1, .descending = true }}, .limit = 10 },
    };
    try testing.expectError(error.OutOfMemory, prepareAggregate(allocator, a, args, false));
    try testing.expect(fault.hit);
    try testing.expectEqual(@as(usize, 0), budget.used.load(.monotonic));
}
