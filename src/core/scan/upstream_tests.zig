const std = @import("std");
const testing = std.testing;
const schema = @import("../schema.zig");
const consumer = @import("../consumer.zig");
const metadata = @import("../parquet/metadata.zig");
const expr_agg = @import("../expr/agg.zig");
const profile = @import("../../profile.zig");
const row_subgroups = @import("../row_subgroups.zig");
const spawn_util = @import("../spawn.zig");
const Input = @import("types.zig").Input;
const MultiAggArgs = @import("types.zig").MultiAggArgs;
const execute = @import("execute.zig");
const runMultiAggregate = execute.runMultiAggregate;
const explainAggregate = execute.explainAggregate;
const ReleasingScanWindow = @import("integration_support.zig").ReleasingScanWindow;

test "row subgroups decline sparse unknown predicates before walking payload pages" {
    const gpa = testing.allocator;
    var arena = std.heap.ArenaAllocator.init(gpa);
    defer arena.deinit();
    const a = arena.allocator();
    const fixture = try row_subgroups.testFixture(a);
    var predicate: std.ArrayList(u8) = .empty;
    for (0..36) |i| try predicate.print(a, "{s}k != {d}", .{ if (i == 0) "" else " AND ", i });
    var args: MultiAggArgs = .{
        .inputs = &.{.{ .name = "sparse", .bytes = fixture.bytes }},
        .metas = &.{fixture.meta},
        .aggregate = "count(*) AS c",
        .group_by = "k,s",
        .filter = predicate.items,
        .parallelism = 1,
        .min_bytes_per_worker = 2 << 10,
        .max_memory = 64 << 20,
        .dense = false,
        .ndv_policy = false,
        .plain_rules = false,
        .spill = false,
        .sketch_topk = null,
    };
    var want = try runMultiAggregate(gpa, a, args);
    defer want.deinit(gpa);
    args.parallelism = 8;
    row_subgroups.test_ranges.store(0, .monotonic);
    var got = try runMultiAggregate(gpa, a, args);
    defer got.deinit(gpa);
    try testing.expectEqual(@as(usize, 0), row_subgroups.test_ranges.load(.monotonic));
    try testing.expectEqual(want.rows_matched, got.rows_matched);
    try testing.expectEqualDeep(want.output.group_rows, got.output.group_rows);
}

test "row subgroups preserve grouped counts, first ordinals, ties and LIMIT OFFSET under pool failures" {
    const gpa = testing.allocator;
    var arena = std.heap.ArenaAllocator.init(gpa);
    defer arena.deinit();
    const a = arena.allocator();
    const fixture = try row_subgroups.testFixture(a);
    const inputs = [_]Input{.{ .name = "subgroups", .bytes = fixture.bytes }};
    var args: MultiAggArgs = .{
        .inputs = &inputs,
        .metas = (&fixture.meta)[0..1],
        .aggregate = "count(*) AS c",
        .group_by = "k,s",
        .filter = "v >= 10 AND v < 4000",
        .parallelism = 1,
        .min_bytes_per_worker = 1,
        .max_memory = 64 << 20,
        .dense = false,
        .ndv_policy = false,
        .shared_strings = false,
        .integer_records = false,
        .byte_records = false,
        .msd_records = false,
        .plain_rules = false,
        .spill = false,
        .sketch_topk = null,
        .result_order = .{ .keys = &.{.{ .column = 2, .descending = true }}, .limit = 13, .offset = 37 },
    };
    var want = try runMultiAggregate(gpa, a, args);
    defer want.deinit(gpa);
    try want.output.applyResultOrder(gpa, args.result_order);
    args.parallelism = 8;
    const explanation = try explainAggregate(gpa, a, args);
    try testing.expect(explanation.workers > 1);
    try testing.expect(explanation.work_items > fixture.meta.row_groups.items.len);
    for ([_]?usize{ null, 0, 1 }) |fail_after| {
        if (fail_after) |k| spawn_util.injectFailureAfter(k);
        defer spawn_util.resetFailure();
        args.parallelism = 8;
        row_subgroups.test_ranges.store(0, .monotonic);
        var got = try runMultiAggregate(gpa, a, args);
        defer got.deinit(gpa);
        try testing.expect(row_subgroups.test_ranges.load(.monotonic) > 1);
        try got.output.applyResultOrder(gpa, args.result_order);
        try testing.expectEqual(want.rows_in, got.rows_in);
        try testing.expectEqual(want.rows_scanned, got.rows_scanned);
        try testing.expectEqual(want.rows_matched, got.rows_matched);
        try testing.expectEqual(want.row_groups_in, got.row_groups_in);
        try testing.expectEqualDeep(want.output.group_rows, got.output.group_rows);
    }
    // The producer poisons its chunks at done(): an early release corrupts another range's next page.
    var window: ReleasingScanWindow = .{ .bytes = try a.dupe(u8, fixture.bytes), .meta = &fixture.meta };
    const streamed = [_]Input{window.input(fixture.bytes)};
    args.inputs = &streamed;
    row_subgroups.test_ranges.store(0, .monotonic);
    var streamed_result = try runMultiAggregate(gpa, a, args);
    defer streamed_result.deinit(gpa);
    try streamed_result.output.applyResultOrder(gpa, args.result_order);
    const ranges = row_subgroups.test_ranges.load(.monotonic);
    try testing.expect(ranges > 1);
    try testing.expectEqual(ranges, window.waits.load(.monotonic));
    try testing.expectEqual(@as(usize, 1), window.releases.load(.monotonic));
    try testing.expectEqualDeep(want.output.group_rows, streamed_result.output.group_rows);
    args.inputs = &inputs;
    const indexed = try a.dupe(schema.RowGroup, fixture.meta.row_groups.items);
    indexed[0].columns.items = try a.dupe(schema.ColumnChunk, indexed[0].columns.items);
    indexed[0].columns.items[2].column_index_offset = 0;
    var indexed_meta = fixture.meta;
    indexed_meta.row_groups.items = indexed;
    args.metas = (&indexed_meta)[0..1];
    const indexed_plan = try explainAggregate(gpa, a, args);
    try testing.expectEqual(@as(usize, 1), indexed_plan.work_items);
    args.metas = (&fixture.meta)[0..1];
    const profiler = try profile.Profiler.create(gpa, .{ .sample_interval_ns = 0 }, .{});
    defer profiler.destroy();
    args.profiler = profiler;
    var profiled = try runMultiAggregate(gpa, a, args);
    defer profiled.deinit(gpa);
    profiler.finish();
    var scans: usize = 0;
    var range_plans: usize = 0;
    var thread = profiler.head.load(.acquire);
    while (thread) |t| : (thread = t.next) {
        var events: profile.EventIterator = .{ .t = t };
        while (events.next()) |batch| for (batch) |event| {
            scans += @intFromBool(event.kind == .plan);
            range_plans += @intFromBool(event.kind == .plan_ranges);
        };
    }
    try testing.expectEqual(@as(usize, 1), scans);
    try testing.expectEqual(@as(usize, 1), range_plans);
}

const RankedFixture = struct { bytes: []const u8, meta: schema.FileMetaData };

fn rankedFixture(a: std.mem.Allocator) !RankedFixture {
    return rankedFixtureColumns(a, .{ "k", "x", "y" }, false);
}

fn rankedFixtureColumns(a: std.mem.Allocator, names: [3][]const u8, even_y: bool) !RankedFixture {
    const k = try a.alloc(i32, 2048);
    const x = try a.alloc(i64, k.len);
    const y = try a.alloc(i32, k.len);
    const kd = try a.alloc(u32, k.len);
    const xd = try a.alloc(u32, k.len);
    for (k, x, y, kd, xd, 0..) |*key, *value, *payload, *dk, *dx, i| {
        key.* = @as(i32, @intCast(i % 13)) - 6;
        value.* = @as(i64, @intCast(i % 67)) - 33;
        payload.* = if (even_y) @as(i32, @intCast(2 * (i % 2))) else @as(i32, @intCast(i % 37)) - 18;
        dk.* = @intFromBool(i % 13 != 0);
        dx.* = @intFromBool(i % 13 != 1 and i % 7 != 0);
    }
    const columns = [_]expr_agg.OutputCol{
        .{ .name = names[0], .col = .{ .i32 = .{ .values = k, .def_levels = kd, .max_def = 1 } }, .parquet_type = .INT32, .nullable = true },
        .{ .name = names[1], .col = .{ .i64 = .{ .values = x, .def_levels = xd, .max_def = 1 } }, .parquet_type = .INT64, .nullable = true },
        .{ .name = names[2], .col = .{ .i32 = .{ .values = y } }, .parquet_type = .INT32, .nullable = false },
    };
    var sink: @import("../writer/streaming.zig").MemorySink = .{ .arena = a };
    _ = try consumer.writeAggregateRows(a, sink.sink(), &columns, @intCast(k.len), .SNAPPY, .{}, .{});
    var meta = try metadata.open(a, sink.buffer.items);
    const rgs = try a.alloc(schema.RowGroup, 8);
    @memset(rgs, meta.row_groups.items[0]);
    meta.row_groups.items = rgs;
    meta.num_rows *= @intCast(rgs.len);
    return .{ .bytes = sink.buffer.items, .meta = meta };
}

test "count-ranked DISTINCT keeps repeated aggregate aliases and bound quoted identifiers" {
    const gpa = testing.allocator;
    var arena = std.heap.ArenaAllocator.init(gpa);
    defer arena.deinit();
    const a = arena.allocator();
    for ([_]bool{ false, true }) |quoted| {
        const fixture = try rankedFixtureColumns(a, if (quoted) .{ "group key", "user x", "select" } else .{ "k", "x", "y" }, false);
        const sql = if (quoted)
            "SELECT \"group key\" AS g,sum(\"select\") AS a,sum(\"select\") AS b,count(*) AS c,count(DISTINCT \"user x\") AS d FROM 'ranked' GROUP BY \"group key\" ORDER BY c DESC LIMIT 5"
        else
            "SELECT k,sum(y) AS a,sum(y) AS b,count(*) AS c,count(DISTINCT x) AS d FROM 'ranked' GROUP BY k ORDER BY c DESC LIMIT 5";
        const q = try @import("../expr/sql_parser.zig").parseSqlQuery(a, sql);
        var args: MultiAggArgs = .{
            .inputs = &.{.{ .name = "ranked", .bytes = fixture.bytes }},
            .metas = &.{fixture.meta},
            .aggregate = q.aggregate_str.?,
            .group_by = q.group_by_str,
            .select_cols = q.select_cols,
            .result_order = q.result_order,
            .parallelism = 4,
            .min_bytes_per_worker = 1,
            .max_memory = 128 << 20,
            .count_ranked_distinct = .off,
        };
        var want = try runMultiAggregate(gpa, a, args);
        defer want.deinit(gpa);
        try want.output.applyResultOrder(gpa, args.result_order);
        args.count_ranked_distinct = .on;
        var got = try runMultiAggregate(gpa, a, args);
        defer got.deinit(gpa);
        try testing.expect(got.count_ranked_distinct);
        try got.output.applyResultOrder(gpa, args.result_order);
        try testing.expectEqualDeep(want.output.group_rows, got.output.group_rows);
        try testing.expectEqualDeep(want.output.group_cols, got.output.group_cols);
        try testing.expectEqualDeep(want.state.group_col_types, got.state.group_col_types);
    }
}

test "count-ranked DISTINCT returns an empty filtered result without rebuilding a false predicate" {
    const gpa = testing.allocator;
    var arena = std.heap.ArenaAllocator.init(gpa);
    defer arena.deinit();
    const a = arena.allocator();
    const fixture = try rankedFixtureColumns(a, .{ "k", "x", "y" }, true);
    var args: MultiAggArgs = .{
        .inputs = &.{.{ .name = "ranked-empty", .bytes = fixture.bytes }},
        .metas = &.{fixture.meta},
        .aggregate = "sum(y) AS a,count(*) AS c,count(DISTINCT x) AS d",
        .group_by = "k",
        .filter = "y = 1",
        .result_order = .{ .keys = &.{.{ .column = 2, .descending = true }}, .limit = 5 },
        .parallelism = 4,
        .min_bytes_per_worker = 1,
        .max_memory = 128 << 20,
        .count_ranked_distinct = .off,
    };
    var want = try runMultiAggregate(gpa, a, args);
    defer want.deinit(gpa);
    args.count_ranked_distinct = .on;
    var got = try runMultiAggregate(gpa, a, args);
    defer got.deinit(gpa);
    try got.output.applyResultOrder(gpa, args.result_order);
    try testing.expect(got.count_ranked_distinct);
    try testing.expectEqual(@as(usize, 0), got.output.group_rows.?.len);
    try testing.expectEqualDeep(want.output.group_rows, got.output.group_rows);
    try testing.expectEqualDeep(want.output.group_cols, got.output.group_cols);
    try testing.expectEqualDeep(want.state.group_col_types, got.state.group_col_types);
}

test "count-ranked DISTINCT keeps expression HAVING and alternate alias ordering on exact fallback" {
    const gpa = testing.allocator;
    var arena = std.heap.ArenaAllocator.init(gpa);
    defer arena.deinit();
    const a = arena.allocator();
    const fixture = try rankedFixture(a);
    const queries = [_][]const u8{
        "SELECT k,sum(CAST(y AS BIGINT)) AS s,count(*) AS c,count(DISTINCT x) AS d FROM 'ranked' GROUP BY k ORDER BY c DESC LIMIT 5",
        "SELECT k,sum(y+1) AS s,count(*) AS c,count(DISTINCT x) AS d FROM 'ranked' GROUP BY k ORDER BY c DESC LIMIT 5",
        "SELECT k,sum(y) AS s,count(*) AS c,count(DISTINCT x+1) AS d FROM 'ranked' GROUP BY k ORDER BY c DESC LIMIT 5",
        "SELECT k,sum(y) AS s,count(*) AS c,count(DISTINCT x) AS d FROM 'ranked' GROUP BY k HAVING s > 0 ORDER BY c DESC LIMIT 5",
        "SELECT k,sum(y) AS s,count(*) AS c,count(DISTINCT x) AS d FROM 'ranked' GROUP BY k ORDER BY s DESC LIMIT 5",
    };
    for (queries) |sql| {
        const q = try @import("../expr/sql_parser.zig").parseSqlQuery(a, sql);
        var args: MultiAggArgs = .{
            .inputs = &.{.{ .name = "ranked", .bytes = fixture.bytes }},
            .metas = &.{fixture.meta},
            .aggregate = q.aggregate_str.?,
            .group_by = q.group_by_str,
            .select_cols = q.select_cols,
            .result_order = q.result_order,
            .parallelism = 4,
            .min_bytes_per_worker = 1,
            .max_memory = 128 << 20,
            .count_ranked_distinct = .off,
        };
        var want = runMultiAggregate(gpa, a, args) catch |e| {
            args.count_ranked_distinct = .on;
            try testing.expectError(e, runMultiAggregate(gpa, a, args));
            continue;
        };
        defer want.deinit(gpa);
        try want.output.applyResultOrder(gpa, args.result_order);
        args.count_ranked_distinct = .on;
        var got = try runMultiAggregate(gpa, a, args);
        defer got.deinit(gpa);
        try testing.expect(!got.count_ranked_distinct);
        try got.output.applyResultOrder(gpa, args.result_order);
        try testing.expectEqualDeep(want.output.group_rows, got.output.group_rows);
        try testing.expectEqualDeep(want.output.group_cols, got.output.group_cols);
        try testing.expectEqualDeep(want.state.group_col_types, got.state.group_col_types);
    }
}

test "count-ranked DISTINCT preserves NULLs exact sums averages ties and forced controls" {
    const gpa = testing.allocator;
    var arena = std.heap.ArenaAllocator.init(gpa);
    defer arena.deinit();
    const a = arena.allocator();
    const fixture = try rankedFixture(a);
    for ([_]?[]const u8{ null, "y >= 0" }) |filter| {
        var args: MultiAggArgs = .{
            .inputs = &.{.{ .name = "ranked-fixture", .bytes = fixture.bytes }},
            .metas = &.{fixture.meta},
            .aggregate = "sum(y) AS s,count(*) AS c,avg(x) AS a,count(DISTINCT x) AS d",
            .group_by = "k",
            .filter = filter,
            .parallelism = 1,
            .min_bytes_per_worker = 1,
            .max_memory = 128 << 20,
            .count_ranked_distinct = .off,
            .result_order = .{ .keys = &.{.{ .column = 2, .descending = true }}, .limit = 5, .offset = 1 },
        };
        var want = try runMultiAggregate(gpa, a, args);
        defer want.deinit(gpa);
        try want.output.applyResultOrder(gpa, args.result_order);
        for ([_]usize{ 1, 4 }) |j| for ([_]usize{ 0, 64 }) |window| {
            for ([_]@FieldType(MultiAggArgs, "count_ranked_distinct"){ .auto, .on, .off }) |mode| {
                args.count_ranked_distinct = mode;
                args.parallelism = j;
                args.decode_batch_rows = window;
                var got = try runMultiAggregate(gpa, a, args);
                defer got.deinit(gpa);
                try testing.expectEqual(mode != .off, got.count_ranked_distinct);
                try got.output.applyResultOrder(gpa, args.result_order);
                try testing.expectEqualDeep(want.output.group_rows, got.output.group_rows);
                try testing.expectEqualDeep(want.output.group_cols, got.output.group_cols);
                try testing.expectEqualDeep(want.state.group_col_types, got.state.group_col_types);
            }
        };
        args.count_ranked_distinct = .on;
        args.result_order.offset = 0;
        args.result_order.limit = 32;
        var whole = try runMultiAggregate(gpa, a, args);
        defer whole.deinit(gpa);
        try testing.expect(whole.count_ranked_distinct);
        var null_key = false;
        var empty_argument = false;
        for (whole.output.group_rows.?) |row| {
            null_key = null_key or row[0] == .null_val;
            if (row[0] == .i and row[0].i == -5) {
                try testing.expect(row[3] == .null_val);
                try testing.expectEqual(@as(i128, 0), row[4].i);
                empty_argument = true;
            }
        }
        try testing.expect(null_key and empty_argument);
    }
}

test "count-ranked DISTINCT replays a runtime dictionary-domain violation" {
    const gpa = testing.allocator;
    var arena = std.heap.ArenaAllocator.init(gpa);
    defer arena.deinit();
    const a = arena.allocator();
    const bytes = @embedFile("../testdata/count-ranked-fallback.parquet");
    var meta = try metadata.open(a, bytes);
    // The page headers still declare a real PLAIN fallback with new keys. Only the planning hint lies.
    const stats = try a.dupe(schema.PageEncodingStats, &.{
        .{ .page_type = @backingInt(schema.PageType.DICTIONARY_PAGE), .encoding = @backingInt(schema.Encoding.PLAIN), .count = 1 },
        .{ .page_type = @backingInt(schema.PageType.DATA_PAGE), .encoding = @backingInt(schema.Encoding.RLE_DICTIONARY), .count = 1 },
    });
    meta.row_groups.items[0].columns.items[0].meta_data.?.encoding_stats = std.ArrayListUnmanaged(schema.PageEncodingStats).fromOwnedSlice(stats);
    var args: MultiAggArgs = .{ .inputs = &.{.{ .name = "runtime-domain-refusal", .bytes = bytes }}, .metas = &.{meta}, .aggregate = "sum(y) AS s,count(*) AS c,avg(x) AS a,count(DISTINCT x) AS d", .group_by = "k", .parallelism = 1, .max_memory = 128 << 20, .result_order = .{ .keys = &.{.{ .column = 2, .descending = true }}, .limit = 5 }, .count_ranked_distinct = .off };
    var ordinary = args;
    ordinary.aggregate = "sum(y),count(*),avg(x)";
    ordinary.plain_rules = false;
    ordinary.partition_cost = false;
    try testing.expect((try explainAggregate(gpa, a, ordinary)).dense);
    var want = try runMultiAggregate(gpa, a, args);
    defer want.deinit(gpa);
    try want.output.applyResultOrder(gpa, args.result_order);
    args.count_ranked_distinct = .on;
    var got = try runMultiAggregate(gpa, a, args);
    defer got.deinit(gpa);
    try testing.expect(!got.count_ranked_distinct);
    try got.output.applyResultOrder(gpa, args.result_order);
    try testing.expectEqualDeep(want.output.group_rows, got.output.group_rows);
}

test "count-ranked DISTINCT refuses an unverified domain and keeps exact fallback" {
    const gpa = testing.allocator;
    var arena = std.heap.ArenaAllocator.init(gpa);
    defer arena.deinit();
    const a = arena.allocator();
    const fixture = try rankedFixture(a);
    var args: MultiAggArgs = .{ .inputs = &.{.{ .name = "domain-refusal", .bytes = fixture.bytes }}, .metas = &.{fixture.meta}, .aggregate = "count(DISTINCT x) AS d,avg(y) AS a,count(*) AS c,sum(x) AS s", .group_by = "k", .parallelism = 2, .min_bytes_per_worker = 1, .max_memory = 128 << 20, .result_order = .{ .keys = &.{.{ .column = 3, .descending = true }}, .limit = 4 }, .count_ranked_distinct = .off };
    var want = try runMultiAggregate(gpa, a, args);
    defer want.deinit(gpa);
    try want.output.applyResultOrder(gpa, args.result_order);
    args.count_ranked_distinct = .on;
    args.ndv_union = false;
    var got = try runMultiAggregate(gpa, a, args);
    defer got.deinit(gpa);
    try testing.expect(!got.count_ranked_distinct);
    try got.output.applyResultOrder(gpa, args.result_order);
    try testing.expectEqualDeep(want.output.group_rows, got.output.group_rows);
}

test "count-ranked DISTINCT memory refusal replays the exact fitting query" {
    const gpa = testing.allocator;
    var arena = std.heap.ArenaAllocator.init(gpa);
    defer arena.deinit();
    const a = arena.allocator();
    const fixture = try rankedFixture(a);
    var refused = false;
    for ([_]usize{ 128 << 10, 256 << 10, 512 << 10, 1 << 20, 2 << 20 }) |budget| {
        var args: MultiAggArgs = .{ .inputs = &.{.{ .name = "budget-refusal", .bytes = fixture.bytes }}, .metas = &.{fixture.meta}, .aggregate = "sum(y) AS s,count(*) AS c,avg(x) AS a,count(DISTINCT x) AS d", .group_by = "k", .parallelism = 1, .decode_batch_rows = 64, .max_memory = budget, .result_order = .{ .keys = &.{.{ .column = 2, .descending = true }}, .limit = 5 }, .count_ranked_distinct = .off };
        var want = runMultiAggregate(gpa, a, args) catch |e| {
            if (e == error.ExceededMemoryBudget) continue;
            return e;
        };
        defer want.deinit(gpa);
        try want.output.applyResultOrder(gpa, args.result_order);
        args.count_ranked_distinct = .on;
        var got = try runMultiAggregate(gpa, a, args);
        defer got.deinit(gpa);
        refused = refused or !got.count_ranked_distinct;
        try got.output.applyResultOrder(gpa, args.result_order);
        try testing.expectEqualDeep(want.output.group_rows, got.output.group_rows);
    }
    try testing.expect(refused);
}
