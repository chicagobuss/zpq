const std = @import("std");
const spawn_util = @import("../spawn.zig");
const schema = @import("../schema.zig");
const consumer = @import("../consumer.zig");
const metadata = @import("../parquet/metadata.zig");
const expr_ast = @import("../expr/ast.zig");
const expr_agg = @import("../expr/agg.zig");
const core_memory = @import("../memory.zig");
const owner_groups = @import("../owner_groups.zig");
const grouped_bitmap = @import("../grouped_distinct_bitmap.zig");
const Diag = @import("../diag.zig").Diag;
const owned_merge = @import("owned_merge.zig");
const freeOwnedAccumulatorStrings = owned_merge.freeOwnedAccumulatorStrings;
const loadFixture = @import("test_support.zig").loadFixture;
const plan_types = @import("plan_types.zig");
const AggregateDispatch = plan_types.AggregateDispatch;
const RecordAdmission = plan_types.RecordAdmission;
const scan_count_cut = @import("count_cut.zig");
const MergedSlot = scan_count_cut.MergedSlot;
const scan_rank_cut = @import("rank_cut.zig");
const ResultCut = scan_rank_cut.ResultCut;
const scan_result = @import("result.zig");
const MultiAggResult = scan_result.MultiAggResult;
const folded_acc = @import("folded_acc.zig");
const sameAccBits = folded_acc.sameAccBits;
const full_match_fixture = @import("test_support.zig").full_match_fixture;
const loadFullMatchFixture = @import("test_support.zig").loadFullMatchFixture;
const scan_plan_impl = @import("plan.zig");
const planAggregate = scan_plan_impl.planAggregate;
const owner_support = @import("owner_support.zig");
const outputNamesBytes = owner_support.outputNamesBytes;
const owner_integer = @import("owner_integer.zig");
const integerQuote = owner_integer.integerQuote;
const owner_bytes = @import("owner_bytes.zig");
const byteSampleMayFit = owner_bytes.byteSampleMayFit;
const scan_merge = @import("merge.zig");
const MergeGroupRef = scan_merge.MergeGroupRef;
const mergePartitionCount = scan_merge.mergePartitionCount;
const mergeEntryBytes = scan_merge.mergeEntryBytes;
const mergeFoldedRefs = scan_merge.mergeFoldedRefs;
const firstCandidates = scan_merge.firstCandidates;
const scan_execute = @import("execute.zig");
const runMultiAggregate = scan_execute.runMultiAggregate;
const Chain = scan_execute.Chain;
const narrowed = scan_execute.narrowed;
const explainAggregate = scan_execute.explainAggregate;
const prepareAggregate = scan_execute.prepareAggregate;
const refuseRepeated = scan_execute.refuseRepeated;
const integration_support = @import("integration_support.zig");
const sameValue = integration_support.sameValue;
const freeResult = integration_support.freeResult;
const expectSameAnswer = integration_support.expectSameAnswer;
const rewritten = integration_support.rewritten;
const PoisonCase = integration_support.PoisonCase;
const expectPoisonCases = integration_support.expectPoisonCases;
const rg1Stats = integration_support.rg1Stats;
const null_pages_fixture = integration_support.null_pages_fixture;
const selectList = integration_support.selectList;
const runAndFree = integration_support.runAndFree;
const NoInPlaceResize = integration_support.NoInPlaceResize;
const expectSameBits = integration_support.expectSameBits;
const setMergeTestString = integration_support.setMergeTestString;
const mergeOverlapAndFree = integration_support.mergeOverlapAndFree;
const fillCountTables = integration_support.fillCountTables;
const countSum = integration_support.countSum;
const mergeFixedAsBytes = integration_support.mergeFixedAsBytes;
const mergeAndBuildRows = integration_support.mergeAndBuildRows;
const byteWindowAllocTrial = integration_support.byteWindowAllocTrial;
const decimal_exact_fixture = integration_support.decimal_exact_fixture;
const DecWant = integration_support.DecWant;
const expectDecimalAnswers = integration_support.expectDecimalAnswers;
const NoGate = integration_support.NoGate;
const outcomes = integration_support.outcomes;
const expectSameOutcomes = integration_support.expectSameOutcomes;
const BitmapTestFixture = integration_support.BitmapTestFixture;
const scan_types = @import("types.zig");
const MultiAggArgs = scan_types.MultiAggArgs;
const SketchTopK = scan_types.SketchTopK;
const Input = scan_types.Input;
const FastPath = scan_types.FastPath;
const WorkItem = scan_types.WorkItem;
const BoundOutputColumn = scan_types.BoundOutputColumn;
const byte_stream = @import("byte_stream.zig");
const integer_stream = @import("integer_stream.zig");
const typed_answer = @import("typed_answer.zig");
const output_values = @import("output_values.zig");
const AggValue = output_values.AggValue;
const orderValues = output_values.orderValues;
const testing = std.testing;
const test_fixtures = @import("../parquet/test_fixtures.zig");

test "poisoned stats: a null count outside the chunk's values is refused, by default and under --trust-stats" {
    const cases = [_]PoisonCase{
        .{ .filter = "ts IS NULL", .n = 0, .trusted = 0 },
        .{ .filter = "ts IS NOT NULL", .n = 1600, .trusted = 1600 },
    };
    inline for ([_]i64{ -1, 201, 1000 }) |nc| {
        const edit = struct {
            fn f(meta: *schema.FileMetaData) void {
                rg1Stats(meta, 0).null_count = nc;
            }
        }.f;
        try expectPoisonCases(full_match_fixture, .{ .footer = edit }, &cases);
    }
}

test "cut bounds prune but never answer: equality and ranges at the cut, min and max decoded" {
    // full_match.parquet's s holds 'k%06d-' then 24 'p's, row ids 200..399 in row group 1. Its bounds there become what
    // a writer cutting to 9 bytes writes: the min a prefix, the max a prefix grown by one, both marked inexact. ts's max
    // there becomes an inexact 10^6 (no row holds it).
    const edit = struct {
        fn f(meta: *schema.FileMetaData) void {
            const st = rg1Stats(meta, 2);
            st.min_value = "k000200-p";
            st.max_value = "k000399-q";
            st.min = null;
            st.max = null;
            st.is_min_value_exact = false;
            st.is_max_value_exact = false;
            const ts = rg1Stats(meta, 0);
            ts.max_value = &big_ts;
            ts.max = null;
            ts.is_max_value_exact = false;
        }
        const big_ts = [_]u8{ 0x40, 0x42, 0x0f, 0, 0, 0, 0, 0 }; // 1_000_000
    }.f;
    const p = "pppppppppppppppppppppppp";
    try expectPoisonCases(full_match_fixture, .{ .footer = edit }, &.{
        // Equal to the cut min, and to the cut max: no row holds either.
        .{ .filter = "s = 'k000200-p'", .n = 0, .trusted = 0 },
        .{ .filter = "s = 'k000399-q'", .n = 0, .trusted = 0 },
        // The values the cut bounds stand for, just above the cut min and just below the cut max.
        .{ .filter = "s = 'k000200-" ++ p ++ "'", .n = 1, .trusted = 1 },
        .{ .filter = "s = 'k000399-" ++ p ++ "'", .n = 1, .trusted = 1 },
        // Ranges with the cut bounds and their neighbours as edges.
        .{ .filter = "s < 'k000200-p'", .n = 200, .trusted = 200 },
        .{ .filter = "s <= 'k000200-pa'", .n = 200, .trusted = 200 },
        .{ .filter = "s < 'k000200-pq'", .n = 201, .trusted = 201 },
        .{ .filter = "s >= 'k000200-p'", .n = 1400, .trusted = 1400 },
        .{ .filter = "s > 'k000399-q'", .n = 1200, .trusted = 1200 },
        .{ .filter = "s < 'k000399-q'", .n = 400, .trusted = 400 },
        .{ .filter = "s >= 'k000399-q'", .n = 1200, .trusted = 1200 },
        .{ .filter = "ts <= 399", .n = 400, .trusted = 400 },
    });

    // The answers come from decoding, with and without --trust-stats.
    const clean = try loadFullMatchFixture() orelse return error.SkipZigTest;
    defer testing.allocator.free(clean);
    const bytes = try rewritten(clean, .{ .footer = edit });
    defer testing.allocator.free(bytes);
    const inputs = [_]Input{.{ .name = full_match_fixture, .bytes = bytes }};
    for ([_]bool{ false, true }) |trust| {
        var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
        defer arena_state.deinit();
        const args: MultiAggArgs = .{
            .inputs = &inputs,
            .aggregate = "min(s) AS lo, max(s) AS hi, max(ts) AS t, min(ts) AS u",
            .parallelism = 1,
            .trust_stats = trust,
        };
        var oracle_args = args;
        oracle_args.scan_all = true;
        const oracle = try runMultiAggregate(testing.allocator, arena_state.allocator(), oracle_args);
        defer freeResult(oracle);
        const got = try runMultiAggregate(testing.allocator, arena_state.allocator(), args);
        defer freeResult(got);
        expectSameAnswer(oracle, got) catch |err| {
            std.debug.print("cut bounds: min/max answered from statistics, trust_stats={}\n", .{trust});
            return err;
        };
        try testing.expectEqual(@as(i128, 1599), got.output.aggs[2].value.i);
    }
}

test "poisoned stats: a page whose min is above its max drops the page index, by default and under --trust-stats" {
    // null_pages.parquet: id = row, ten pages of 100 rows in row group 0; page 1 holds 100..199.
    const edit = struct {
        fn f(ci: *schema.ColumnIndex) void {
            std.mem.swap([]const u8, &ci.min_values.items[1], &ci.max_values.items[1]);
        }
    }.f;
    try expectPoisonCases(null_pages_fixture, .{ .column_index = edit }, &.{
        .{ .filter = "id = 130", .n = 1, .trusted = 1 },
        .{ .filter = "id >= 126 AND id <= 128", .n = 3, .trusted = 3 },
    });
}

test "poisoned stats: page locations out of page order drop the page index, by default and under --trust-stats" {
    // Pages 1 and 2 of id swap places while their row starts stay in order: skipping to page 1 would decode page 2.
    const edit = struct {
        fn f(oi: *schema.OffsetIndex) void {
            const locs = oi.page_locations.items;
            std.mem.swap(i64, &locs[1].offset, &locs[2].offset);
            std.mem.swap(i32, &locs[1].compressed_page_size, &locs[2].compressed_page_size);
        }
    }.f;
    try expectPoisonCases(null_pages_fixture, .{ .offset_index = edit }, &.{
        .{ .filter = "id = 130", .n = 1, .trusted = 1 },
        .{ .filter = "id = 260", .n = 1, .trusted = 1 },
    });
}

// ============================================================ All-null pages.
//
// The fixture's ColumnIndex flags whole pages of `v` and `s.b` all-null (see tools/zpq-gen null_pages). A
// null check proves such a page always-match; its rows must still read as null wherever the filter is evaluated.
// ============================================================



test "all-null pages: null checks and their combinations answer like --scan-all" {
    const bytes = metadata.readFileSlice(null_pages_fixture, testing.allocator) catch |err| {
        if (err == error.FileNotFound) {
            std.debug.print("skipping: {s} not present\n", .{null_pages_fixture});
            return error.SkipZigTest;
        }
        return err;
    };
    defer testing.allocator.free(bytes);
    const inputs = [_]Input{.{ .name = null_pages_fixture, .bytes = bytes }};

    const Q = struct { filter: []const u8, aggregate: []const u8 = "count(*) AS n, sum(id) AS si", group_by: ?[]const u8 = null };
    const queries = [_]Q{
        .{ .filter = "v IS NULL" },
        .{ .filter = "v IS NOT NULL" },
        .{ .filter = "NOT v IS NULL" },
        .{ .filter = "NOT v IS NOT NULL" },
        .{ .filter = "s.b IS NULL" },
        .{ .filter = "s.b IS NOT NULL" },
        .{ .filter = "s.a IS NULL" },
        .{ .filter = "v IS NULL AND id >= 700" },
        .{ .filter = "id >= 700 AND v IS NULL" },
        .{ .filter = "s.b IS NULL AND id < 1500" },
        .{ .filter = "v IS NULL OR v > 1500" },
        .{ .filter = "v IS NOT NULL AND v > 1200" },
        .{ .filter = "v IS NULL", .aggregate = "count(*) AS n, count(v) AS c, sum(v) AS s, min(v) AS mn, max(v) AS mx" },
        .{ .filter = "v IS NOT NULL", .aggregate = "count(v) AS c, sum(v) AS s, min(v) AS mn, max(v) AS mx" },
        .{ .filter = "s.b IS NULL", .aggregate = "count(s.b) AS c, count(s.a) AS ca, sum(s.a) AS sa" },
        .{ .filter = "v IS NULL", .aggregate = "count(*) AS n", .group_by = "v" },
        .{ .filter = "v IS NOT NULL AND v < 50", .aggregate = "count(*) AS n", .group_by = "v" },
    };
    for ([_]bool{ false, true }) |trust_stats| for (queries) |q| {
        var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
        defer arena_state.deinit();
        const args: MultiAggArgs = .{
            .inputs = &inputs,
            .filter = q.filter,
            .aggregate = q.aggregate,
            .group_by = q.group_by,
            .parallelism = 1,
            .trust_stats = trust_stats,
        };
        var oracle_args = args;
        oracle_args.scan_all = true;
        const oracle = try runMultiAggregate(testing.allocator, arena_state.allocator(), oracle_args);
        defer freeResult(oracle);
        const got = try runMultiAggregate(testing.allocator, arena_state.allocator(), args);
        defer freeResult(got);
        expectSameAnswer(oracle, got) catch |err| {
            std.debug.print("all-null pages: filter={s} agg={s} trust_stats={} disagrees with --scan-all\n", .{
                q.filter, q.aggregate, trust_stats,
            });
            return err;
        };
        // Hand-known counts, so agreement is not agreement on a shared mistake.
        if (q.group_by == null and std.mem.eql(u8, q.aggregate, "count(*) AS n, sum(id) AS si")) {
            const want: ?i128 = if (std.mem.eql(u8, q.filter, "v IS NULL")) 650 else if (std.mem.eql(u8, q.filter, "s.b IS NULL")) 997 else null;
            if (want) |w| try testing.expectEqual(w, got.output.aggs[0].value.i);
        }
    };
}

test "GROUP BY labels a nested leaf whose dotted path a top-level column takes by its quoted path" {
    // Top-level `a.b` (1, null, 3) beside the group `a`'s field `b` (10, 20, null).
    const fixture = "ci/fixtures/parquet/dotted_twin.parquet";
    const bytes = (try loadFixture(fixture)) orelse return error.SkipZigTest;
    defer testing.allocator.free(bytes);
    const inputs = [_]Input{.{ .name = fixture, .bytes = bytes }};

    const Case = struct {
        group_by: []const u8,
        aggregate: []const u8 = "count(*) AS n",
        select_cols: ?[]const []const u8 = null,
        want: []const []const u8,
    };
    const cases = [_]Case{
        .{ .group_by = "b", .want = &.{ "\"a\".\"b\"", "n" } },
        .{ .group_by = "\"a\".\"b\"", .want = &.{ "\"a\".\"b\"", "n" } },
        .{ .group_by = "\"a.b\"", .want = &.{ "a.b", "n" } },
        // Both keys at once used to be one name twice.
        .{ .group_by = "b, \"a.b\"", .want = &.{ "\"a\".\"b\"", "a.b", "n" } },
        // The label binds back as an output column name, and is not mistaken for a quoted `a"."b`.
        .{
            .group_by = "b, a.b",
            .select_cols = &.{ "n", "\"a\".\"b\"", "a.b" },
            .want = &.{ "n", "\"a\".\"b\"", "a.b" },
        },
        .{ .group_by = "b AS k", .want = &.{ "k", "n" } },
        // A select list (SQL) names a bare key by any name that binds its column; the output keeps the label
        // unless the select list renames it.
        .{ .group_by = "b", .select_cols = &.{ "b", "n" }, .want = &.{ "\"a\".\"b\"", "n" } },
        .{ .group_by = "b", .select_cols = &.{ "b AS k", "n" }, .want = &.{ "k", "n" } },
        .{
            .group_by = "\"a\".\"b\", \"a.b\"",
            .select_cols = &.{ "\"a.b\"", "b" },
            .want = &.{ "a.b", "\"a\".\"b\"" },
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
        const cols = res.output.group_cols.?;
        try testing.expectEqual(c.want.len, cols.len);
        for (c.want, cols) |w, g| try testing.expectEqualStrings(w, g);
    }

    // The duplicate-name check sees the label, not the dotted path both columns share.
    var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_state.deinit();
    var diag: Diag = .{};
    const res = runMultiAggregate(testing.allocator, arena_state.allocator(), .{
        .inputs = &inputs,
        .aggregate = "count(*) AS n",
        .group_by = "b, \"a\".\"b\" AS \"a\".\"b\"",
        .parallelism = 1,
        .diag = &diag,
    });
    try testing.expectError(error.DuplicateOutputColumn, res);
    try testing.expectEqualStrings("\"a\".\"b\"", diag.column.get());
}







test "runMultiAggregate: the result destructors free a partly built answer" {
    // Strings are what own memory: string min/max winners, string group keys, output names. Every allocation is
    // failed in turn, so each partial state the answer passes through must be one `deinit` frees.
    const path = "ci/fixtures/parquet/json_escape.parquet";
    const bytes = (try loadFixture(path)) orelse return error.SkipZigTest;
    defer testing.allocator.free(bytes);
    var meta_arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer meta_arena.deinit();
    const meta = try metadata.open(meta_arena.allocator(), bytes);
    const inputs = [_]Input{.{ .name = path, .bytes = bytes }};
    var fixed: NoInPlaceResize = .{ .child = testing.allocator };
    const args = .{ &inputs, &meta, "min(s) AS m, max(s) AS x, count(*) AS n", null };
    try testing.checkAllAllocationFailures(fixed.allocator(), runAndFree, args);
    const grouped = .{ &inputs, &meta, "min(s) AS m, count(*) AS n", @as(?[]const u8, "s") };
    try testing.checkAllAllocationFailures(fixed.allocator(), runAndFree, grouped);
}

test "ordered GROUP BY: groups as they first appear, and the same float bits at every -j" {
    const path = "ci/fixtures/parquet/group_order.parquet";
    const bytes = metadata.readFileSlice(path, testing.allocator) catch |err| {
        if (err == error.FileNotFound) return error.SkipZigTest;
        return err;
    };
    defer testing.allocator.free(bytes);
    const inputs = [_]Input{ .{ .name = path, .bytes = bytes }, .{ .name = path, .bytes = bytes } };
    // Keys first appear zeta, mu, alpha, omega, beta, kappa (24 row groups, mixed in as they go); `x` mixes
    // magnitudes from 1e-3 to 1e16, so where partial sums meet changes the bits of every float sum.
    const want_order = [_][]const u8{ "zeta", "mu", "alpha", "omega", "beta", "kappa" };
    for ([_]bool{ false, true }) |encoded| for ([_][]const u8{ "k", "i" }) |key| {
        // Results keep state in the arena, so it outlives every one of them.
        var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
        defer arena_state.deinit();
        var first: ?MultiAggResult = null;
        defer if (first) |f| freeResult(f);
        for ([_]usize{ 1, 2, 3, 8, 32, 32, 7 }) |j| {
            const res = try runMultiAggregate(testing.allocator, arena_state.allocator(), .{
                .inputs = &inputs,
                .aggregate = "sum(x) AS s, avg(x) AS a, count(*) AS n, min(k) AS m",
                .group_by = key,
                .parallelism = j,
                .encoded = encoded,
                .scan_all = true,
                .min_bytes_per_worker = 1,
            });
            const rows = res.output.group_rows.?;
            try testing.expectEqual(want_order.len, rows.len);
            for (rows, want_order) |r, k| try testing.expectEqualStrings(k, r[4].s);
            if (first) |f| {
                for (rows, f.output.group_rows.?) |r, fr| {
                    try testing.expectEqual(@as(u64, @bitCast(fr[1].f)), @as(u64, @bitCast(r[1].f)));
                    try testing.expectEqual(@as(u64, @bitCast(fr[2].f)), @as(u64, @bitCast(r[2].f)));
                    try testing.expectEqual(fr[3].i, r[3].i);
                }
                freeResult(res);
            } else first = res;
        }
    };
}

test "regression: float sums and avgs keep their bits at every -j, budget, decoder and filter" {
    const path = "ci/fixtures/parquet/group_order.parquet";
    const bytes = metadata.readFileSlice(path, testing.allocator) catch |err| {
        if (err == error.FileNotFound) return error.SkipZigTest;
        return err;
    };
    defer testing.allocator.free(bytes);
    const inputs = [_]Input{ .{ .name = path, .bytes = bytes }, .{ .name = path, .bytes = bytes } };
    // 24 row groups of 500 rows, `x` dictionary-encoded and mixing magnitudes from 1e-3 to 1e16: where partial sums
    // meet changes the bits. A small budget reads each row group in slices; the coded path folds over the
    // dictionary; a filter leaves blocks partly selected.
    const Shape = struct { group_by: ?[]const u8, filter: ?[]const u8 };
    for ([_]Shape{
        .{ .group_by = null, .filter = null },
        .{ .group_by = null, .filter = "i > 7" },
        .{ .group_by = null, .filter = "k = 'zeta'" },
        .{ .group_by = "k", .filter = null },
        .{ .group_by = "k", .filter = "i > 7" },
    }) |shape| {
        var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
        defer arena_state.deinit();
        var first: ?MultiAggResult = null;
        defer if (first) |f| freeResult(f);
        for ([_]usize{ 1, 3, 8 }) |j| for ([_]?usize{ null, 8 << 10, 32 << 10, 128 << 10 }) |budget| {
            for ([_]bool{ true, false }) |encoded| for ([_]bool{ true, false }) |scan_all| {
                const res = try runMultiAggregate(testing.allocator, arena_state.allocator(), .{
                    .inputs = &inputs,
                    // Computed arguments over the coded column: one gathered under a sparse filter, one that may be
                    // NULL and so evaluates in place.
                    .aggregate = "sum(x) AS s, avg(x) AS a, sum(x * 2) AS t, sum(CASE WHEN i > 3 THEN x END) AS u, " ++
                        "avg_state(x) AS st",
                    .group_by = shape.group_by,
                    .filter = shape.filter,
                    .parallelism = j,
                    .decode_limit = budget,
                    .encoded = encoded,
                    .scan_all = scan_all,
                    .min_bytes_per_worker = 1,
                });
                const f = first orelse {
                    first = res;
                    continue;
                };
                defer freeResult(res);
                if (shape.group_by == null) {
                    for (res.output.aggs, f.output.aggs) |g, w| try expectSameBits(w.value, g.value);
                } else for (res.output.group_rows.?, f.output.group_rows.?) |r, w| {
                    for (r, w) |g, x| try expectSameBits(x, g);
                }
            };
        };
    }
}



test "regression: count(col) answers, or fails, the same at every budget and -j" {
    const path = "ci/fixtures/parquet/plain_values_corrupt.parquet";
    const bytes = metadata.readFileSlice(path, testing.allocator) catch |err| {
        if (err == error.FileNotFound) return error.SkipZigTest;
        return err;
    };
    defer testing.allocator.free(bytes);
    const inputs = [_]Input{.{ .name = path, .bytes = bytes }};
    // One PLAIN length prefix in `s` runs past its page: count(s) reads only its levels when the row group decodes
    // whole, and read the values (and failed) when a small budget sliced it.
    var want: ?anyerror!i128 = null;
    for ([_]usize{ 1, 3 }) |j| for ([_]?usize{ null, 64 << 10, 4 << 10, 512 }) |budget| {
        var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
        defer arena_state.deinit();
        const got: anyerror!i128 = if (runMultiAggregate(testing.allocator, arena_state.allocator(), .{
            .inputs = &inputs,
            .aggregate = "count(s) AS n",
            .parallelism = j,
            .decode_limit = budget,
            .min_bytes_per_worker = 1,
        })) |res| blk: {
            defer freeResult(res);
            break :blk res.output.aggs[0].value.i;
        } else |err| err;
        const w = want orelse {
            want = got;
            continue;
        };
        if (w) |v| try testing.expectEqual(v, try got) else |e| try testing.expectError(e, got);
    };
    try testing.expectEqual(@as(i128, 216), try want.?);
}

test "GROUP BY hash merge: same groups, counts and string mins at every -j" {
    const path = "ci/fixtures/parquet/group_order.parquet";
    const bytes = metadata.readFileSlice(path, testing.allocator) catch |err| {
        if (err == error.FileNotFound) return error.SkipZigTest;
        return err;
    };
    defer testing.allocator.free(bytes);
    const inputs = [_]Input{ .{ .name = path, .bytes = bytes }, .{ .name = path, .bytes = bytes } };
    // count and string min are not order-sensitive, so workers keep separate tables and the final merge hashes.
    for ([_][]const u8{ "k", "i" }) |key| {
        var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
        defer arena_state.deinit();
        var base: ?MultiAggResult = null;
        defer if (base) |b| freeResult(b);
        for ([_]usize{ 1, 2, 8, 32 }) |j| {
            const res = try runMultiAggregate(testing.allocator, arena_state.allocator(), .{
                .inputs = &inputs,
                .aggregate = "count(*) AS n, min(k) AS mk",
                .group_by = key,
                .parallelism = j,
                .scan_all = true,
                .min_bytes_per_worker = 1,
            });
            const rows = res.output.group_rows.?;
            if (base) |b| {
                const want = b.output.group_rows.?;
                try testing.expectEqual(want.len, rows.len);
                for (rows, want) |row, w| {
                    try testing.expectEqual(w[1].i, row[1].i);
                    try testing.expectEqualStrings(w[2].s, row[2].s);
                    switch (w[0]) {
                        .s => |s| try testing.expectEqualStrings(s, row[0].s),
                        .i => |v| try testing.expectEqual(v, row[0].i),
                        else => return error.TestUnexpectedResult,
                    }
                }
                freeResult(res);
            } else base = res;
        }
    }
}



test "hash merge of worker tables: owned-string setup preserves arena state on refusal" {
    var arena: expr_agg.KeyArena = .{};
    // A nonempty payload makes the old tag/payload mismatch visible without relying on allocator fill or layout.
    var acc: expr_agg.Accumulator = .{ .min_arena = .{
        .store = &arena,
        .ref = 0xaaaaaaaa,
        .key = 0xbbbbbbbb,
        .skip = 5,
    } };
    const before = acc;
    var failing = testing.FailingAllocator.init(testing.allocator, .{ .fail_index = 0 });
    try testing.expectError(error.OutOfMemory, setMergeTestString(failing.allocator(), &acc, "winner"));
    try testing.expect(sameAccBits(before, acc));
    try setMergeTestString(testing.allocator, &acc, "winner");
    defer freeOwnedAccumulatorStrings(testing.allocator, (&acc)[0..1]);
    try testing.expectEqualStrings("winner", acc.min_bytes.?);
}







test "parallel hash merge matches one thread, including a failed spawn" {
    const calls = [_]expr_agg.AggCall{
        .{ .func = .count, .alias = "n", .result = .i64, .arg = null, .where = null },
    };
    const n: usize = 10_000;
    var a = expr_agg.GroupTable.init(testing.allocator, 64 << 20);
    defer a.deinit();
    var b = expr_agg.GroupTable.init(testing.allocator, 64 << 20);
    defer b.deinit();
    try fillCountTables(&a, &b, &calls, n);
    const tables = [_]expr_agg.GroupTable{ a, b };
    const refs = try testing.allocator.alloc(MergeGroupRef, a.refs.items.len + b.refs.items.len);
    defer testing.allocator.free(refs);
    for (a.refs.items, 0..) |_, i| refs[i] = .{ .table = 0, .group = @intCast(i) };
    for (b.refs.items, 0..) |_, i| refs[a.refs.items.len + i] = .{ .table = 1, .group = @intCast(i) };

    var serial_slots: std.ArrayList(MergedSlot) = .empty;
    defer serial_slots.deinit(testing.allocator);
    var serial_accs: std.ArrayList(expr_agg.Accumulator) = .empty;
    defer serial_accs.deinit(testing.allocator);
    // Counts are copied by value and the source is left intact, so a second merge sees the same partials.
    const ta = testing.allocator;
    try mergeFoldedRefs(ta, ta, &tables, refs, &calls, 1, null, false, &serial_slots, &serial_accs);
    try testing.expectEqual(n + n / 2, serial_slots.items.len);
    const half: u64 = @intCast(n / 2);
    try testing.expectEqual(half + 11 * half + 10 * half, countSum(serial_accs.items, serial_slots.items));

    const Less = struct {
        fn lt(_: void, x: MergedSlot, y: MergedSlot) bool {
            return std.mem.order(u8, x.key, y.key) == .lt;
        }
    };
    std.sort.pdq(MergedSlot, serial_slots.items, {}, Less.lt);

    for ([_]usize{ 0, 1, 2 }) |fail_after| {
        var slots: std.ArrayList(MergedSlot) = .empty;
        defer slots.deinit(testing.allocator);
        var accs: std.ArrayList(expr_agg.Accumulator) = .empty;
        defer accs.deinit(testing.allocator);
        spawn_util.injectFailureAfter(fail_after);
        defer spawn_util.resetFailure();
        try mergeFoldedRefs(ta, ta, &tables, refs, &calls, 8, null, false, &slots, &accs);
        spawn_util.resetFailure();
        try testing.expectEqual(serial_slots.items.len, slots.items.len);
        std.sort.pdq(MergedSlot, slots.items, {}, Less.lt);
        for (slots.items, serial_slots.items) |got, want| {
            try testing.expectEqualStrings(want.key, got.key);
            try testing.expectEqual(want.first, got.first);
            try testing.expectEqual(serial_accs.items[want.idx].count, accs.items[got.idx].count);
        }
    }
}

test "a merge drawn from the query's ledger gives all of it back, and past it is refused, holding nothing" {
    const calls = [_]expr_agg.AggCall{
        .{ .func = .count, .alias = "n", .result = .i64, .arg = null, .where = null },
    };
    const n: usize = 10_000;
    var a = expr_agg.GroupTable.init(testing.allocator, 64 << 20);
    defer a.deinit();
    var b = expr_agg.GroupTable.init(testing.allocator, 64 << 20);
    defer b.deinit();
    try fillCountTables(&a, &b, &calls, n);
    const tables = [_]expr_agg.GroupTable{ a, b };
    const refs = try testing.allocator.alloc(MergeGroupRef, a.refs.items.len + b.refs.items.len);
    defer testing.allocator.free(refs);
    for (a.refs.items, 0..) |_, i| refs[i] = .{ .table = 0, .group = @intCast(i) };
    for (b.refs.items, 0..) |_, i| refs[a.refs.items.len + i] = .{ .table = 1, .group = @intCast(i) };
    for ([_]usize{ 1, 8 }) |workers| for ([_]bool{ false, true }) |in_place| {
        var budget: core_memory.SharedBudget = .{ .limit = 64 << 20 };
        var charged: core_memory.Charged = .{ .child = testing.allocator, .budget = &budget };
        const lists = charged.allocator();
        {
            var slots: std.ArrayList(MergedSlot) = .empty;
            defer slots.deinit(lists);
            var accs: std.ArrayList(expr_agg.Accumulator) = .empty;
            defer accs.deinit(lists);
            const ta = testing.allocator;
            try mergeFoldedRefs(ta, lists, &tables, refs, &calls, workers, null, in_place, &slots, &accs);
            try testing.expectEqual(n + n / 2, slots.items.len);
            try testing.expect(budget.used.load(.monotonic) >= slots.capacity * @sizeOf(MergedSlot));
        }
        try testing.expectEqual(@as(usize, 0), budget.used.load(.monotonic));
        budget.limit = 64 << 10;
        var slots: std.ArrayList(MergedSlot) = .empty;
        defer slots.deinit(lists);
        var accs: std.ArrayList(expr_agg.Accumulator) = .empty;
        defer accs.deinit(lists);
        const ta = testing.allocator;
        const refused = mergeFoldedRefs(ta, lists, &tables, refs, &calls, workers, null, in_place, &slots, &accs);
        try testing.expectError(error.OutOfMemory, refused);
        try testing.expectEqual(error.ExceededMemoryBudget, charged.refusal(error.OutOfMemory));
        slots.clearAndFree(lists);
        accs.clearAndFree(lists);
        try testing.expectEqual(@as(usize, 0), budget.used.load(.monotonic));
    };
}

test "hash merge of worker tables: overlapping keys, first appearance, allocation failure" {
    try mergeOverlapAndFree(testing.allocator);
    var fixed: NoInPlaceResize = .{ .child = testing.allocator };
    try testing.checkAllAllocationFailures(fixed.allocator(), mergeOverlapAndFree, .{});
}







test "hash merge of fixed-width tables: the byte map's groups, firsts, order and state, cut or not, at every -j" {
    for ([_]u8{ 9, 18, 27 }) |stride| for ([_]usize{ 1, 3, 8 }) |j| {
        try mergeFixedAsBytes(testing.allocator, 6000, stride, j);
    };
    spawn_util.injectFailureAfter(1);
    defer spawn_util.resetFailure();
    try mergeFixedAsBytes(testing.allocator, 6000, 9, 8);
}









test "in-place merge: the copying merge's rows, bit for bit, at every -j and when spawns fail" {
    const gpa = testing.allocator;
    // Over 8192 refs, so several workers split the merge into partitions.
    const n = 6000;
    for ([_]bool{ false, true }) |fixed| {
        var want = try mergeAndBuildRows(gpa, fixed, n, 1, false);
        defer want.deinit(gpa);
        const Run = struct { j: usize, fail_after: ?usize = null };
        for ([_]Run{ .{ .j = 1 }, .{ .j = 3 }, .{ .j = 8 }, .{ .j = 8, .fail_after = 1 } }) |run| {
            if (run.fail_after) |k| spawn_util.injectFailureAfter(k);
            defer spawn_util.resetFailure();
            var got = try mergeAndBuildRows(gpa, fixed, n, run.j, true);
            defer got.deinit(gpa);
            const w = want.group_rows.?;
            const g = got.group_rows.?;
            try testing.expectEqual(w.len, g.len);
            var neg_zero: usize = 0;
            for (w, g) |wr, gr| for (wr, gr) |wv, gv| {
                try testing.expectEqual(std.meta.activeTag(wv), std.meta.activeTag(gv));
                switch (wv) {
                    .s => |s| try testing.expectEqualStrings(s, gv.s),
                    .f => |f| {
                        try testing.expectEqual(@as(u64, @bitCast(f)), @as(u64, @bitCast(gv.f)));
                        if (std.math.signbit(f) and f == 0) neg_zero += 1;
                    },
                    .avg => |a| {
                        try testing.expectEqual(a.sum == null, gv.avg.sum == null);
                        if (a.sum) |sum|
                            try testing.expectEqual(@as(u64, @bitCast(sum)), @as(u64, @bitCast(gv.avg.sum.?)));
                        try testing.expectEqual(a.count, gv.avg.count);
                    },
                    else => try testing.expect(std.meta.eql(wv, gv)),
                }
            };
            // A float sum of -0.0 is never an answer: the copying merge adds every one into +0.0.
            try testing.expectEqual(@as(usize, 0), neg_zero);
        }
    }
}

test "in-place merge: an allocation failure frees everything" {
    var fixed: NoInPlaceResize = .{ .child = testing.allocator };
    try testing.checkAllAllocationFailures(fixed.allocator(), struct {
        fn run(a: std.mem.Allocator) !void {
            var out = try mergeAndBuildRows(a, true, 12, 1, true);
            out.deinit(a);
        }
    }.run, .{});
}

test "hash merge of fixed-width tables: an allocation failure frees everything" {
    var fixed: NoInPlaceResize = .{ .child = testing.allocator };
    try testing.checkAllAllocationFailures(fixed.allocator(), struct {
        fn run(a: std.mem.Allocator) !void {
            try mergeFixedAsBytes(a, 24, 18, 1);
        }
    }.run, .{});
}

test "runMultiAggregate: a big GROUP BY answers the same rows in the same order at every -j" {
    const path = "ci/fixtures/parquet/topk_skew.parquet";
    const bytes = (try loadFixture(path)) orelse return error.SkipZigTest;
    defer testing.allocator.free(bytes);
    const inputs = [_]Input{ .{ .name = path, .bytes = bytes }, .{ .name = path, .bytes = bytes } };
    // `v` is unique per row (20000 groups, so several blocks of rows), `n` a fixed-width key, `k` skewed with nulls.
    for ([_][]const u8{ "v", "n", "k, v" }) |key| for ([_]bool{ false, true }) |unordered| {
        var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
        defer arena_state.deinit();
        var base: ?MultiAggResult = null;
        defer if (base) |b| freeResult(b);
        const Run = struct { j: usize, fail_after: ?usize = null };
        for ([_]Run{ .{ .j = 1 }, .{ .j = 3 }, .{ .j = 8 }, .{ .j = 8, .fail_after = 1 }, .{ .j = 8, .fail_after = 5 } }) |run| {
            if (run.fail_after) |n| spawn_util.injectFailureAfter(n);
            defer spawn_util.resetFailure();
            var res = try runMultiAggregate(testing.allocator, arena_state.allocator(), .{
                .inputs = &inputs,
                .aggregate = "count(*) AS c, min(v) AS lo, sum(n) AS s",
                .group_by = key,
                .parallelism = run.j,
                .min_bytes_per_worker = 1,
                .unordered = unordered,
            });
            if (run.fail_after != null) try testing.expect(spawn_util.failuresInjected() > 0);
            const rows = res.output.group_rows.?;
            if (std.mem.eql(u8, key, "v")) try testing.expectEqual(@as(usize, 20_000), rows.len);
            const b = base orelse {
                base = res;
                continue;
            };
            defer res.deinit(testing.allocator);
            const want = b.output.group_rows.?;
            try testing.expectEqual(want.len, rows.len);
            if (unordered) {
                // Unordered rows follow the merge's partitions, which change with -j: compare as sets.
                const Less = struct {
                    fn lt(_: void, x: []const AggValue, y: []const AggValue) bool {
                        for (x, y) |xv, yv| {
                            const o = orderValues(xv, yv, false);
                            if (o != .eq) return o == .lt;
                        }
                        return false;
                    }
                };
                const w = try testing.allocator.dupe([]const AggValue, want);
                defer testing.allocator.free(w);
                const g = try testing.allocator.dupe([]const AggValue, rows);
                defer testing.allocator.free(g);
                std.sort.pdq([]const AggValue, w, {}, Less.lt);
                std.sort.pdq([]const AggValue, g, {}, Less.lt);
                for (w, g) |wr, gr| for (wr, gr) |x, y| try testing.expect(sameValue(x, y));
            } else for (want, rows) |wr, gr| for (wr, gr) |x, y| try testing.expect(sameValue(x, y));
        }
    };
}

test "inline integer stream matches the row answer across workers, spawn failure and a tight fallback" {
    const path = "ci/fixtures/parquet/topk_skew.parquet";
    const bytes = (try loadFixture(path)) orelse return error.SkipZigTest;
    defer testing.allocator.free(bytes);
    const inputs = [_]Input{.{ .name = path, .bytes = bytes }};
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const base: MultiAggArgs = .{
        .inputs = &inputs,
        .aggregate = "count(*) AS c, sum(n) AS s",
        .group_by = "n",
        .max_memory = 512 << 20,
        .parallelism = 1,
        .min_bytes_per_worker = 1,
        .integer_records = false,
    };
    var want = try runMultiAggregate(testing.allocator, arena.allocator(), base);
    defer want.deinit(testing.allocator);
    for ([_]usize{ 1, 2, 4, 8, 16 }) |j| for ([_]?usize{ null, 0, 2 }) |fail_after| {
        var args = base;
        args.parallelism = j;
        args.typed_answer = true;
        args.integer_records = true;
        args.integer_records_min_rows = 0;
        if (fail_after) |n| spawn_util.injectFailureAfter(n);
        defer spawn_util.resetFailure();
        var got = try runMultiAggregate(testing.allocator, arena.allocator(), args);
        defer got.deinit(testing.allocator);
        const stream = got.output.integer_stream.?;
        const rows = want.output.group_rows.?;
        try testing.expectEqual(rows.len, stream.state.group_count);
        var lo: usize = 0;
        while (lo < rows.len) {
            const hi = @min(rows.len, lo + 137);
            var window = try stream.window(testing.allocator, got.state.group_col_types.?, lo, hi, j);
            defer window.deinit(testing.allocator);
            for (rows[lo..hi], 0..) |row, i| for (row, window.columns) |value, col| {
                const lane = col.i64;
                try testing.expectEqual(value == .null_val, lane.defs.len != 0 and lane.defs[i] == 0);
                if (value != .null_val) try testing.expectEqual(value.i, lane.values[i]);
            };
            lo = hi;
        }
    };
    var tight = base;
    tight.integer_records = true;
    tight.integer_records_min_rows = 0;
    tight.typed_answer = true;
    tight.max_memory = 16 << 20;
    var fallback = try runMultiAggregate(testing.allocator, arena.allocator(), tight);
    defer fallback.deinit(testing.allocator);
    try testing.expect(fallback.output.integer_stream == null);
    try testing.expectEqual(want.output.group_rows.?.len, fallback.output.typed.?.rows);
}

test "inline records decline released remote windows and retry only through a retained mapping" {
    const gpa = testing.allocator;
    var arena = std.heap.ArenaAllocator.init(gpa);
    defer arena.deinit();
    const a = arena.allocator();
    const n = consumer.answer_rg_rows + 1;
    const strings = try a.alloc([]const u8, n);
    @memset(strings, "a");
    strings[n - 1] = "a long interior key";
    const ints = try a.alloc(i64, n);
    @memset(ints, 1);
    const cols = [_]expr_agg.OutputCol{
        .{
            .name = "k",
            .col = .{ .string = .{ .values = strings, .max_def = 0 } },
            .parquet_type = .BYTE_ARRAY,
            .nullable = false,
        },
        .{
            .name = "n",
            .col = .{ .i64 = .{ .values = ints, .max_def = 0 } },
            .parquet_type = .INT64,
            .nullable = false,
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
    var captured: Capture = .{ .a = a };
    _ = try consumer.writeAggregateRows(
        a,
        .{ .ctx = &captured, .write_fn = Capture.write },
        &cols,
        n,
        .SNAPPY,
        .{},
        .{},
    );
    const meta = try metadata.open(a, captured.bytes.items);
    // No width evidence in the footer: a mapped byte scan must discover the last key after releasing the first RG.
    for (meta.row_groups.items) |*rg| rg.columns.items[0].meta_data.?.statistics = null;
    const Probe = struct {
        fn trial(alloc: std.mem.Allocator, m: schema.FileMetaData, data: []const u8) !void {
            var budget: core_memory.SharedBudget = .{ .limit = 8 << 20 };
            defer std.debug.assert(budget.used.load(.monotonic) == 0);
            const inputs = [_]Input{.{ .name = "sample.parquet", .bytes = data }};
            const work = [_]WorkItem{.{
                .file = 0,
                .rg = 0,
                .agg_start = 0,
                .agg_len = 0,
                .columns = undefined,
                .accumulators = &.{},
            }};
            if (!try byteSampleMayFit(alloc, .{
                .inputs = &inputs,
                .aggregate = "",
                .ledger = &budget,
            }, &.{m}, &work, 0)) return error.OutOfMemory;
        }
    };
    try Probe.trial(gpa, meta, captured.bytes.items);
    try testing.checkAllAllocationFailures(gpa, Probe.trial, .{ meta, captured.bytes.items });
    var last = meta;
    last.row_groups.items = meta.row_groups.items[1..];
    const sample_inputs = [_]Input{.{ .name = "sample.parquet", .bytes = captured.bytes.items }};
    const sample_work = [_]WorkItem{.{
        .file = 0,
        .rg = 0,
        .agg_start = 0,
        .agg_len = 0,
        .columns = undefined,
        .accumulators = &.{},
    }};
    try testing.expect(!try byteSampleMayFit(
        gpa,
        .{ .inputs = &sample_inputs, .aggregate = "" },
        &.{last},
        &sample_work,
        0,
    ));
    var failing_probe = testing.FailingAllocator.init(gpa, .{ .fail_index = 0 });
    var probe_budget: core_memory.SharedBudget = .{ .limit = 8 << 20 };
    try testing.expect(!try byteSampleMayFit(
        failing_probe.allocator(),
        .{ .inputs = &sample_inputs, .aggregate = "", .ledger = &probe_budget },
        &.{meta},
        &sample_work,
        0,
    ));
    try testing.expect(failing_probe.has_induced_failure);
    try testing.expectEqual(@as(usize, 0), probe_budget.used.load(.monotonic));
    const Unstarted = struct {
        fn wait(_: *anyopaque, _: usize) anyerror!void {
            return error.ReaderNotStarted;
        }
        fn done(_: *anyopaque, _: usize, _: usize) void {
            @panic("reader not started");
        }
    };
    // Neither buffered bytes nor the mapping may be read before a reader starts.
    var unstarted: u8 = 0;
    const gated_inputs = [_]Input{.{
        .name = "unstarted.parquet",
        .bytes = &.{},
        .plan_bytes = &.{},
        .gate = .{ .ctx = &unstarted, .wait = Unstarted.wait, .done = Unstarted.done },
    }};
    try testing.expect(try byteSampleMayFit(
        failing_probe.allocator(),
        .{ .inputs = &gated_inputs, .aggregate = "", .ledger = &probe_budget },
        &.{meta},
        &sample_work,
        0,
    ));
    try testing.expectEqual(@as(usize, 0), probe_budget.used.load(.monotonic));
    const Window = struct {
        bytes: []u8,
        meta: *const schema.FileMetaData,
        budget: *core_memory.SharedBudget,
        refuse_at: usize,
        force_decode: bool,
        refused: bool = false,
        waits: [2]usize = @splat(0),
        done: [2]usize = @splat(0),
        fn wait(ctx: *anyopaque, rg: usize) anyerror!void {
            const w: *@This() = @ptrCast(@alignCast(ctx));
            w.waits[rg] += 1;
            if (rg != 0 and w.force_decode and !w.refused and w.budget.used.load(.monotonic) >= w.refuse_at) {
                w.refused = true;
                return error.ExceededMemoryBudget;
            }
        }
        fn release(ctx: *anyopaque, rg: usize, _: usize) void {
            const w: *@This() = @ptrCast(@alignCast(ctx));
            w.done[rg] += 1;
            for (w.meta.row_groups.items[rg].columns.items) |col| {
                const cm = col.meta_data.?;
                const lo: usize = @intCast(cm.dictionary_page_offset orelse cm.data_page_offset);
                @memset(w.bytes[lo..][0..@intCast(cm.total_compressed_size)], 0);
            }
        }
    };
    const integer_outputs = [_]BoundOutputColumn{
        .{ .source = .{ .key = 0 }, .name = "n" },
        .{ .source = .{ .agg = 0 }, .name = "c" },
        .{ .source = .{ .agg = 1 }, .name = "s" },
    };
    const names_credit = outputNamesBytes(&integer_outputs);
    for ([_]usize{ 0, 4, 8 }) |requested| for ([_]bool{ false, true }) |mapped| for (0..3) |shape|
        for ([_]bool{ false, true }) |text| {
            const bytes_key = shape != 0;
            const parallel_retry = requested == 4;
            var budget: core_memory.SharedBudget = .{ .limit = 512 << 20 };
            // The established answer retains its small materialization charge until the query ledger dies.
            defer std.debug.assert(budget.used.load(.monotonic) < 1 << 20);
            const buffer = try a.dupe(u8, captured.bytes.items);
            var window: Window = .{
                .bytes = buffer,
                .meta = &meta,
                .budget = &budget,
                .refuse_at = owner_groups.IntFull.State.reservation(n, 1, owner_groups.full_parts, 0, names_credit),
                .force_decode = !bytes_key,
            };
            const in = [_]Input{.{
                .name = if (mapped) "window.parquet" else "s3://bucket/window.parquet",
                .bytes = buffer,
                .gate = .{ .ctx = &window, .wait = Window.wait, .done = Window.release },
                .plan_bytes = if (mapped) captured.bytes.items else null,
            }};
            const args: MultiAggArgs = .{
                .inputs = &in,
                .metas = &.{meta},
                .aggregate = if (shape == 2) "sum(n) AS s" else "count(*) AS c,sum(n) AS s",
                .group_by = if (bytes_key) "k" else "n",
                .result_order = if (shape == 2)
                    .{ .keys = &.{.{ .column = 1, .descending = true }}, .limit = 2 }
                else
                    .{},
                .parallelism = if (mapped and !bytes_key) requested else 1,
                .typed_answer = shape != 2 and !text,
                .record_answer = text,
                .integer_records_min_rows = 0,
                .byte_records = true,
                .byte_records_min_rows = 0,
                .max_memory = budget.limit,
                .ledger = &budget,
                .min_bytes_per_worker = if (parallel_retry) 1 else std.math.maxInt(usize),
            };
            if (shape == 2) {
                const path = (try explainAggregate(gpa, a, args)).record_path;
                if (mapped)
                    try testing.expectEqual(.byte_sum_top_h, path.?.layout)
                else
                    try testing.expect(path == null);
            }
            var got = try runMultiAggregate(gpa, a, args);
            defer got.deinit(gpa);
            try testing.expectEqualSlices(usize, &.{ 1, 1 }, &window.waits);
            try testing.expectEqualSlices(usize, &.{ 1, 1 }, &window.done);
            try testing.expect(got.output.byte_stream == null);
            if (mapped and !bytes_key and parallel_retry) {
                // The injected parallel decode refusal must keep the inline layout for its deterministic serial retry.
                const stream = got.output.integer_stream.?;
                try testing.expectEqual(@as(usize, 1), stream.state.locals.len);
                try testing.expectEqual(@as(usize, 1), stream.state.group_count);
            } else if (shape == 2) {
                try got.output.applyResultOrder(gpa, args.result_order);
                const rows = got.output.group_rows.?;
                try testing.expectEqual(@as(usize, 2), rows.len);
                try testing.expectEqualStrings("a", rows[0][0].s);
                try testing.expectEqual(@as(i128, n - 1), rows[0][1].i);
                try testing.expectEqualStrings(strings[n - 1], rows[1][0].s);
                try testing.expectEqual(@as(i128, 1), rows[1][1].i);
            } else {
                try testing.expect(got.output.integer_stream == null);
                try testing.expectEqual(@as(usize, if (bytes_key) 2 else 1), got.output.rowCount());
                if (text) try testing.expect(got.output.group_rows != null);
            }
            try testing.expectEqual(mapped and !bytes_key, window.refused);
        };
    for ([_]bool{ false, true }) |bytes_key| {
        const inputs = [_]Input{.{ .name = "mapped.parquet", .bytes = captured.bytes.items }};
        const args: MultiAggArgs = .{
            .inputs = &inputs,
            .metas = &.{meta},
            .aggregate = "count(*) AS c,sum(n) AS s",
            .group_by = if (bytes_key) "k" else "n",
            .typed_answer = true,
            .integer_records_min_rows = 0,
            .byte_records = true,
            .byte_records_min_rows = 0,
            .max_memory = 512 << 20,
        };
        const explanation = try explainAggregate(gpa, a, args);
        const expected: RecordAdmission = .{
            .layout = if (bytes_key) .byte_group_stream else .integer_group_stream,
            .reservation = if (bytes_key)
                owner_groups.Full.State.reservation(n, 1, owner_groups.full_parts, 0, 0)
            else
                integerQuote(n, 1, null, args, false, false, 0, names_credit),
        };
        try testing.expectEqual(expected, explanation.record_path.?);
    }
}

test "inline record decode refusals retry serially without disabling the layout" {
    const S = std.EnumSet(FastPath);
    var chain: Chain = .{ .parts_tried = false };
    const args: MultiAggArgs = .{ .inputs = &.{}, .aggregate = "", .parallelism = 8, .byte_records = true };
    const serial = narrowed(args, args, S.initMany(&.{ .integer_records, .decode_memory }), &chain, false).?;
    try testing.expectEqual(@as(usize, 1), serial.parallelism);
    try testing.expect(serial.integer_records and serial.byte_records);
    const fallback = narrowed(args, serial, S.initMany(&.{ .integer_records, .decode_memory }), &chain, false).?;
    try testing.expect(!fallback.integer_records and !fallback.byte_records);
    try testing.expectEqual(args.parallelism, fallback.parallelism);
    const wide = narrowed(args, args, S.initMany(&.{.integer_records}), &chain, false).?;
    try testing.expect(!wide.integer_records and !wide.byte_records);
}



test "inline byte stream matches full rows across workers, windows, allocation and spawn failures" {
    const path = "ci/fixtures/parquet/topk_skew.parquet";
    const bytes = (try loadFixture(path)) orelse return error.SkipZigTest;
    defer testing.allocator.free(bytes);
    const inputs = [_]Input{.{ .name = path, .bytes = bytes }};
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    for ([_]?[]const u8{ null, "s,k,c" }) |column_order| {
        const base: MultiAggArgs = .{
            .inputs = &inputs,
            .aggregate = "count(*) AS c,sum(n) AS s",
            .group_by = "k",
            .column_order = column_order,
            .max_memory = 512 << 20,
            .parallelism = 1,
            .min_bytes_per_worker = 1,
        };
        var want = try runMultiAggregate(testing.allocator, arena.allocator(), base);
        defer want.deinit(testing.allocator);
        for ([_]usize{ 1, 2, 4, 8, 16 }) |j| for ([_]?usize{ null, 0, 2 }) |fail_after| {
            var args = base;
            args.parallelism = j;
            args.typed_answer = true;
            args.byte_records = true;
            args.byte_records_min_rows = 0;
            if (fail_after) |n| spawn_util.injectFailureAfter(n);
            defer spawn_util.resetFailure();
            var got = try runMultiAggregate(testing.allocator, arena.allocator(), args);
            defer got.deinit(testing.allocator);
            const stream = got.output.byte_stream.?;
            try testing.expectEqual(want.output.group_rows.?.len, stream.state.group_count);
            const rows = want.output.group_rows.?;
            var lo: usize = 0;
            while (lo < rows.len) {
                const hi = @min(rows.len, lo + 137);
                var w = try stream.window(testing.allocator, lo, hi, j);
                defer w.deinit(testing.allocator);
                for (rows[lo..hi], 0..) |row, i| for (row, w.columns) |v, col| {
                    switch (col.col) {
                        .string => |c| {
                            const missing = if (c.def_levels) |d| d[i] == 0 else false;
                            try testing.expectEqual(v == .null_val, missing);
                            if (!missing) try testing.expectEqualStrings(v.s, c.values[i]);
                        },
                        .i64 => |c| {
                            const missing = if (c.def_levels) |d| d[i] == 0 else false;
                            try testing.expectEqual(v == .null_val, missing);
                            if (!missing) try testing.expectEqual(v.i, c.values[i]);
                        },
                        else => unreachable,
                    }
                };
                lo = hi;
            }
            if (j == 1 and fail_after == null and column_order == null)
                try testing.checkAllAllocationFailures(testing.allocator, byteWindowAllocTrial, .{stream});
        };
        var tight = base;
        tight.byte_records = true;
        tight.byte_records_min_rows = 0;
        tight.typed_answer = true;
        tight.max_memory = 16 << 20;
        var fallback = try runMultiAggregate(testing.allocator, arena.allocator(), tight);
        defer fallback.deinit(testing.allocator);
        try testing.expect(fallback.output.byte_stream == null);
        try testing.expectEqual(want.output.group_rows.?.len, fallback.output.typed.?.rows);
    }
    var args: MultiAggArgs = .{ .inputs = &inputs, .aggregate = "count(*) AS c", .byte_records = true };
    var chain: Chain = .{ .parts_tried = false };
    args = narrowed(args, args, std.EnumSet(FastPath).initMany(&.{.integer_records}), &chain, false).?;
    try testing.expect(!args.byte_records and !args.integer_records);
}

test "inline integer SUM cut matches the established partition cut across workers and spawn failures" {
    const path = "ci/fixtures/parquet/topk_skew.parquet";
    const bytes = (try loadFixture(path)) orelse return error.SkipZigTest;
    defer testing.allocator.free(bytes);
    const inputs = [_]Input{.{ .name = path, .bytes = bytes }};
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    for ([_]bool{ false, true }) |descending| {
        const order_keys = [_]expr_ast.OrderKey{.{ .column = 1, .descending = descending }};
        const base: MultiAggArgs = .{
            .inputs = &inputs,
            .aggregate = "sum(n) AS s",
            .group_by = "n",
            .result_order = .{ .keys = &order_keys, .limit = 7, .offset = 3 },
            .max_memory = 512 << 20,
            .parallelism = 1,
            .min_bytes_per_worker = 1,
            .integer_records = false,
        };
        var want = try runMultiAggregate(testing.allocator, arena.allocator(), base);
        defer want.deinit(testing.allocator);
        for ([_]usize{ 1, 2, 4, 8, 16 }) |j| for ([_]?usize{ null, 0, 2 }) |fail_after| {
            var args = base;
            args.parallelism = j;
            args.integer_records = true;
            args.integer_records_min_rows = 0;
            var taken: std.EnumSet(FastPath) = .{};
            args.taken = &taken;
            if (fail_after) |n| spawn_util.injectFailureAfter(n);
            defer spawn_util.resetFailure();
            var got = try prepareAggregate(testing.allocator, arena.allocator(), args, false);
            defer got.deinit(testing.allocator);
            try testing.expect(taken.contains(.integer_records));
            const rows = got.output.group_rows.?;
            try testing.expectEqual(want.output.group_rows.?.len, rows.len);
            for (want.output.group_rows.?, rows) |a, b| for (a, b) |x, y| {
                try testing.expectEqual(std.meta.activeTag(x), std.meta.activeTag(y));
                if (x == .i) try testing.expectEqual(x.i, y.i);
            };
        };
    }
}

test "runMultiAggregate: a NULL first key of two fixed-width keys still reads the second back" {
    // A fixed-width key image pads a NULL column's payload with zeros; read as framing, the pad would be taken for
    // the next column's flag and `id` would come back NULL.
    const bytes = (try loadFixture(null_pages_fixture)) orelse return error.SkipZigTest;
    defer testing.allocator.free(bytes);
    const inputs = [_]Input{.{ .name = null_pages_fixture, .bytes = bytes }};
    for ([_]usize{ 1, 3 }) |j| {
        var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
        defer arena_state.deinit();
        var res = try runMultiAggregate(testing.allocator, arena_state.allocator(), .{
            .inputs = &inputs,
            .aggregate = "count(*) AS c",
            .group_by = "v, id",
            .parallelism = j,
            .min_bytes_per_worker = 1,
        });
        defer res.deinit(testing.allocator);
        // `id` is unique and never NULL; `v` is NULL in 650 of the 2000 rows.
        const rows = res.output.group_rows.?;
        try testing.expectEqual(@as(usize, 2000), rows.len);
        var null_v: usize = 0;
        var id_sum: i128 = 0;
        for (rows) |row| {
            if (row[0] == .null_val) null_v += 1;
            id_sum += row[1].i;
        }
        try testing.expectEqual(@as(usize, 650), null_v);
        try testing.expectEqual(@as(i128, 1999 * 2000 / 2), id_sum);
    }
}

test "regression: ordered grouping retains the small-query memory budget at one worker" {
    const path = "ci/fixtures/parquet/group_order.parquet";
    const bytes = try metadata.readFileSlice(path, testing.allocator);
    defer testing.allocator.free(bytes);
    const inputs = [_]Input{ .{ .name = path, .bytes = bytes }, .{ .name = path, .bytes = bytes } };
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const res = try runMultiAggregate(testing.allocator, arena.allocator(), .{
        .inputs = &inputs,
        .aggregate = "count(*) AS n",
        .group_by = "k",
        .parallelism = 1,
        .max_memory = 4096,
        .scan_all = true,
    });
    defer freeResult(res);
    try testing.expectEqual(@as(usize, 6), res.output.group_rows.?.len);
}

test "statistics never invent a zero of the other sign: --trust-stats answers zero extrema as decoding does" {
    // Arrow records -0 / +0 bounds for any chunk holding a zero, all +0 or all -0 alike.
    const path = "ci/fixtures/parquet/signed_zero.parquet";
    const bytes = metadata.readFileSlice(path, testing.allocator) catch |err| {
        if (err == error.FileNotFound) return error.SkipZigTest;
        return err;
    };
    defer testing.allocator.free(bytes);
    const inputs = [_]Input{.{ .name = path, .bytes = bytes }};
    var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_state.deinit();
    for ([_]?[]const u8{ null, "k = 0", "k = 1", "k = 2", "k >= 2" }) |filter| {
        var answers: [2][2]u64 = undefined;
        for ([_]bool{ false, true }, 0..) |trust, t| {
            const res = try runMultiAggregate(testing.allocator, arena_state.allocator(), .{
                .inputs = &inputs,
                .filter = filter,
                .aggregate = "min(v) AS lo, max(v) AS hi",
                .trust_stats = trust,
                .scan_all = !trust,
            });
            defer freeResult(res);
            answers[t] = .{ @bitCast(res.output.aggs[0].value.f), @bitCast(res.output.aggs[1].value.f) };
        }
        try testing.expectEqual(answers[0], answers[1]);
    }
}

// ============================================================ Exact DECIMAL.
//
// The fixture (tools/zpq-gen decimal_exact) stores decimals as INT32, INT64 and FLBA(16), dictionary-encoded,
// in three row groups. Every expected value is DuckDB's answer for the same query, as unscaled integer and type.







test "DECIMAL aggregates and arithmetic are exact, on every decode path and worker count" {
    const bytes = (try loadFixture(decimal_exact_fixture)) orelse return error.SkipZigTest;
    defer testing.allocator.free(bytes);
    const inputs = [_]Input{.{ .name = decimal_exact_fixture, .bytes = bytes }};
    const want = [_]DecWant{
        .{ .alias = "s9", .v = -15150, .p = 38, .s = 2 },
        .{ .alias = "mn9", .v = -15150, .p = 9, .s = 2 },
        .{ .alias = "mx9", .v = 15049, .p = 9, .s = 2 },
        .{ .alias = "s18", .v = 344850, .p = 38, .s = 4 },
        .{ .alias = "s38", .v = 448500000000000, .p = 38, .s = 10 },
        .{ .alias = "mn38", .v = -123456789012345678901234561234567891, .p = 38, .s = 10 },
        .{ .alias = "mx38", .v = 123456789012345678901237551234567891, .p = 38, .s = 10 },
        .{ .alias = "sdn", .v = 3670000, .p = 38, .s = 2 },
        .{ .alias = "sq", .v = 45000, .p = 38, .s = 2 },
        // DECIMAL(9,2) * DECIMAL(18,4) is DECIMAL(27,6); a sum is DECIMAL(38, s).
        .{ .alias = "p", .v = 209832550, .p = 38, .s = 6 },
        // Mixed with an INT32 column (DECIMAL(10,0)) and with literals, as SQL types them.
        .{ .alias = "di", .v = -30150, .p = 38, .s = 2 },
        .{ .alias = "dti", .v = 227255050, .p = 38, .s = 2 },
        .{ .alias = "dd", .v = 450015000000000, .p = 38, .s = 10 },
        .{ .alias = "dlit", .v = -227250, .p = 38, .s = 3 },
        .{ .alias = "ilit", .v = -2250, .p = 38, .s = 1 },
        .{ .alias = "neg", .v = 15150, .p = 38, .s = 2 },
        .{ .alias = "co", .v = 3670000, .p = 38, .s = 2 },
        .{ .alias = "cc", .v = 3649800, .p = 38, .s = 2 },
        // avg keeps the argument's type, rounded half away from zero: 36700.00 / 133 = 275.939...
        // avg over DECIMAL(15,2) is DECIMAL(19,6); over DECIMAL(38,10) its own type. avg_state ranks by the mean
        // at the argument's scale.
        .{ .alias = "adn", .v = 275939850, .p = 19, .s = 6 },
        .{ .alias = "a38", .v = 1495000000000, .p = 38, .s = 10 },
        .{ .alias = "sadn", .v = 27594, .p = 15, .s = 2 },
    };
    const aggregate =
        "sum(d9) AS s9, min(d9) AS mn9, max(d9) AS mx9, sum(d18) AS s18, sum(d38) AS s38, min(d38) AS mn38, " ++
        "max(d38) AS mx38, sum(dn) AS sdn, sum(q) AS sq, sum(d9 * d18) AS p, sum(d9 + i) AS di, " ++
        "sum(d9 * i) AS dti, sum(d38 - d9) AS dd, sum(d9 * 1.5) AS dlit, sum(i * 1.5) AS ilit, sum(-d9) AS neg, " ++
        "sum(coalesce(dn, 0)) AS co, sum(coalesce(dn, d9)) AS cc, avg(dn) AS adn, avg(d38) AS a38, " ++
        "avg_state(dn) AS sadn";
    for ([_]bool{ true, false }) |encoded| for ([_]usize{ 1, 3 }) |j| {
        try expectDecimalAnswers(.{
            .inputs = &inputs,
            .aggregate = aggregate,
            .parallelism = j,
            .min_bytes_per_worker = 1,
            .encoded = encoded,
        }, &want);
    };
    // A filter: Q6's shape over the decimals (a sparse selection gathers the exact values with the rows).
    try expectDecimalAnswers(.{
        .inputs = &inputs,
        .aggregate = "sum(d9 * d18) AS p, sum(q) AS sq",
        .filter = "q >= 1.00 AND q <= 2.00 AND i < 100",
        .parallelism = 1,
    }, &.{
        .{ .alias = "p", .v = -296209063, .p = 38, .s = 6 },
        .{ .alias = "sq", .v = 18700, .p = 38, .s = 2 },
    });
}

test "a DECIMAL result past its precision is an error, never a wrapped or rounded value" {
    const bytes = (try loadFixture(decimal_exact_fixture)) orelse return error.SkipZigTest;
    defer testing.allocator.free(bytes);
    const inputs = [_]Input{.{ .name = decimal_exact_fixture, .bytes = bytes }};
    // Two values of 10^38 - 1 sum past 38 digits; each row of `big * 10` and `big + big` is past them already; and
    // `big + 0.1` first brings `big` to the common DECIMAL(38,1), where 10^38 - 1 has no room (as the SQL engines
    // refuse that cast).
    for ([_][]const u8{ "sum(big) AS s", "sum(big * 10) AS s", "sum(big + big) AS s", "sum(big + 0.1) AS s" }) |agg| {
        var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
        defer arena_state.deinit();
        try testing.expectError(error.DecimalOverflow, runMultiAggregate(testing.allocator, arena_state.allocator(), .{
            .inputs = &inputs,
            .aggregate = agg,
            .parallelism = 1,
        }));
    }
    // The same column without its two wide rows fits.
    try expectDecimalAnswers(.{
        .inputs = &inputs,
        .aggregate = "sum(big) AS s",
        .filter = "i >= -148",
        .parallelism = 1,
    }, &.{.{ .alias = "s", .v = 298, .p = 38, .s = 0 }});
    try expectDecimalAnswers(.{
        .inputs = &inputs,
        .aggregate = "sum(big + 0.1) AS s",
        .filter = "i >= -148",
        .parallelism = 1,
    }, &.{.{ .alias = "s", .v = 3278, .p = 38, .s = 1 }});
    // A mean fits wherever its values do, though their sum does not: avg divides the wide sum. (2 × (10^38 - 1) +
    // 298) / 300, rounded half away from zero.
    try expectDecimalAnswers(.{
        .inputs = &inputs,
        .aggregate = "avg(big) AS a",
        .parallelism = 1,
    }, &.{.{ .alias = "a", .v = 666666666666666666666666666666666668, .p = 38, .s = 0 }});
}

test "an aggregate or GROUP BY key over a repeated leaf is refused while planning, before any page decodes" {
    // `l` is LIST<INT64>; a filter on its elements is refused by the filter parser, these by `refuseRepeated`.
    const path = "ci/fixtures/parquet/list_int64.parquet";
    const bytes = (try loadFixture(path)) orelse return error.SkipZigTest;
    defer testing.allocator.free(bytes);
    const inputs = [_]Input{.{ .name = path, .bytes = bytes }};
    const Case = struct { aggregate: []const u8, group_by: ?[]const u8 = null, err: anyerror };
    for ([_]Case{
        .{ .aggregate = "count(l.list.element) AS c", .err = error.NestedAggregateNotSupported },
        .{ .aggregate = "sum(l.list.element + 1) AS s", .err = error.NestedAggregateNotSupported },
        .{ .aggregate = "count(*) AS c", .group_by = "l.list.element", .err = error.NestedGroupKeyNotSupported },
    }) |c| {
        var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
        defer arena_state.deinit();
        var diag: Diag = .{};
        try testing.expectError(c.err, runMultiAggregate(testing.allocator, arena_state.allocator(), .{
            .inputs = &inputs,
            .aggregate = c.aggregate,
            .group_by = c.group_by,
            .diag = &diag,
        }));
        try testing.expect(!diag.execution_started);
        try testing.expectEqualStrings("l.list.element", diag.column.get());
    }
    // Its sibling top-level column still aggregates.
    var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_state.deinit();
    const ok = try runMultiAggregate(testing.allocator, arena_state.allocator(), .{
        .inputs = &inputs,
        .aggregate = "sum(id) AS s",
    });
    defer freeResult(ok);
    try testing.expectEqual(@as(i128, 15), ok.output.aggs[0].value.i);
}

test "DECIMAL aggregates over no values are NULL, and GROUP BY a DECIMAL key is exact" {
    const bytes = (try loadFixture(decimal_exact_fixture)) orelse return error.SkipZigTest;
    defer testing.allocator.free(bytes);
    const inputs = [_]Input{.{ .name = decimal_exact_fixture, .bytes = bytes }};
    var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_state.deinit();
    // Row group 1 holds only NULL `dn`.
    const empty = try runMultiAggregate(testing.allocator, arena_state.allocator(), .{
        .inputs = &inputs,
        .aggregate = "sum(dn) AS s, min(dn) AS m, avg(dn) AS a, avg_state(dn) AS st",
        .filter = "i >= -50 AND i < 50",
        .parallelism = 1,
    });
    defer freeResult(empty);
    try testing.expect(empty.output.aggs[0].value == .null_val);
    try testing.expect(empty.output.aggs[1].value == .null_val);
    try testing.expect(empty.output.aggs[2].value == .null_val);
    const a = empty.output.aggs[3].value.avg_dec;
    try testing.expect(a.sum == null and a.mean == null and a.count == 0);

    const grouped = try runMultiAggregate(testing.allocator, arena_state.allocator(), .{
        .inputs = &inputs,
        .aggregate = "count(*) AS n, sum(d18 * q) AS p",
        .group_by = "dn",
        .filter = "i >= -60 AND i <= 60",
        .parallelism = 1,
    });
    defer freeResult(grouped);
    const rows = grouped.output.group_rows.?;
    // 121 rows: 108 have a NULL `dn`, the other 13 a distinct value each.
    try testing.expectEqual(@as(usize, 14), rows.len);
    var nulls: usize = 0;
    for (rows) |row| switch (row[0]) {
        .null_val => {
            nulls += 1;
            try testing.expectEqual(@as(i128, 108), row[1].i);
        },
        .dec => |k| {
            try testing.expectEqual(expr_ast.Dec{ .precision = 15, .scale = 2 }, k.spec);
            try testing.expectEqual(@as(i128, 1), row[1].i);
            try testing.expectEqual(@as(u8, 6), row[2].dec.spec.scale);
        },
        else => return error.TestUnexpectedResult,
    };
    try testing.expectEqual(@as(usize, 1), nulls);
}

test "--trust-stats answers DECIMAL min/max from statistics exactly, as decoding does" {
    const bytes = (try loadFixture(decimal_exact_fixture)) orelse return error.SkipZigTest;
    defer testing.allocator.free(bytes);
    const inputs = [_]Input{.{ .name = decimal_exact_fixture, .bytes = bytes }};
    const want = [_]DecWant{
        .{ .alias = "mn9", .v = -15150, .p = 9, .s = 2 },
        .{ .alias = "mx18", .v = 1299, .p = 18, .s = 4 },
        .{ .alias = "mn38", .v = -123456789012345678901234561234567891, .p = 38, .s = 10 },
        .{ .alias = "mx38", .v = 123456789012345678901237551234567891, .p = 38, .s = 10 },
    };
    for ([_]bool{ false, true }) |trust| try expectDecimalAnswers(.{
        .inputs = &inputs,
        .aggregate = "min(d9) AS mn9, max(d18) AS mx18, min(d38) AS mn38, max(d38) AS mx38",
        .parallelism = 1,
        .trust_stats = trust,
    }, &want);
}

test "a decode budget slices wide row groups, FIXED_LEN_BYTE_ARRAY DECIMAL included, with the same answer" {
    const bytes = (try loadFixture(decimal_exact_fixture)) orelse return error.SkipZigTest;
    defer testing.allocator.free(bytes);
    const inputs = [_]Input{.{ .name = decimal_exact_fixture, .bytes = bytes }};
    const want = [_]DecWant{
        .{ .alias = "s9", .v = -15150, .p = 38, .s = 2 },
        .{ .alias = "s38", .v = 448500000000000, .p = 38, .s = 10 },
        .{ .alias = "p", .v = 209832550, .p = 38, .s = 6 },
        .{ .alias = "adn", .v = 275939850, .p = 19, .s = 6 },
    };
    const aggregate = "sum(d9) AS s9, sum(d38) AS s38, sum(d9 * d18) AS p, avg(dn) AS adn";
    // Unbounded; then budgets too small for a whole row group per worker, down to a few rows a slice.
    for ([_]?usize{ null, 10_000, 1000 }) |limit| for ([_]usize{ 1, 3 }) |j| {
        try expectDecimalAnswers(.{
            .inputs = &inputs,
            .aggregate = aggregate,
            .parallelism = j,
            .min_bytes_per_worker = 1,
            .decode_limit = limit,
        }, &want);
    };
    // GROUP BY slices too.
    for ([_]?usize{ null, 16_000, 1000 }) |limit| {
        var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
        defer arena_state.deinit();
        const res = try runMultiAggregate(testing.allocator, arena_state.allocator(), .{
            .inputs = &inputs,
            .aggregate = "sum(d18 * q) AS p, max(dn) AS mx",
            .group_by = "k",
            .parallelism = 1,
            .decode_limit = limit,
        });
        defer freeResult(res);
        const rows = res.output.group_rows.?;
        try testing.expectEqual(@as(usize, 3), rows.len);
        try testing.expectEqual(@as(i128, 17235000), rows[0][1].dec.v);
    }
}

test "a decode budget that slices row groups keeps where each group first appeared" {
    // Unique keys in shuffled order: a LIMIT with no ORDER BY, or a count tied at 1, returns the first to appear. A
    // cut of 300 ends past the first slices of the first row group.
    const path = "ci/fixtures/parquet/census_topk.parquet";
    const bytes = (try loadFixture(path)) orelse return error.SkipZigTest;
    defer testing.allocator.free(bytes);
    const inputs = [_]Input{.{ .name = path, .bytes = bytes }};
    for ([_]expr_ast.ResultOrder{
        .{ .limit = 300 },
        .{ .keys = &.{.{ .column = 1, .descending = true }}, .limit = 300 },
    }) |cut| for ([_][]const u8{ "u", "s" }) |key| {
        var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
        defer arena_state.deinit();
        const arena = arena_state.allocator();
        var args: MultiAggArgs = .{
            .inputs = &inputs,
            .aggregate = "count(*) AS c, sum(t) AS st",
            .group_by = key,
            .select_cols = try selectList(arena, &.{ key, "count(*) AS c", "sum(t) AS st" }),
            .parallelism = 1,
            .result_order = cut,
        };
        var want = try runMultiAggregate(testing.allocator, arena, args);
        defer want.deinit(testing.allocator);
        args.decode_limit = 3000;
        var got = try runMultiAggregate(testing.allocator, arena, args);
        defer got.deinit(testing.allocator);
        try want.output.applyResultOrder(testing.allocator, cut);
        try got.output.applyResultOrder(testing.allocator, cut);
        const w = want.output.group_rows.?;
        const g = got.output.group_rows.?;
        try testing.expectEqual(w.len, g.len);
        for (w, g) |wr, gr| for (wr, gr) |a, b| try testing.expect(sameValue(a, b));
    };
}

test "a first-keys cut under a decode budget ends inside a later slice where the full scan does" {
    // Keys 3-9 count 7; the ninth group to appear is key 8 (tools/zpq-gen census).
    const path = "ci/fixtures/parquet/census_sliced.parquet";
    const bytes = (try loadFixture(path)) orelse return error.SkipZigTest;
    defer testing.allocator.free(bytes);
    const inputs = [_]Input{.{ .name = path, .bytes = bytes }};
    for ([_]?usize{ 100, 150, null }) |limit| {
        var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
        defer arena_state.deinit();
        const arena = arena_state.allocator();
        var args: MultiAggArgs = .{
            .inputs = &inputs,
            .group_by = "k",
            .aggregate = "count(*) AS c",
            .parallelism = 1,
            .decode_limit = limit,
            .result_order = .{ .limit = 1, .offset = 8 },
        };
        for ([_]?SketchTopK{ null, .{} }) |sketch| {
            args.sketch_topk = sketch;
            var res = try runMultiAggregate(testing.allocator, arena, args);
            defer res.deinit(testing.allocator);
            try res.output.applyResultOrder(testing.allocator, args.result_order);
            const rows = res.output.group_rows.?;
            try testing.expectEqual(@as(usize, 1), rows.len);
            try testing.expectEqual(@as(i128, 8), rows[0][0].i);
            try testing.expectEqual(@as(i128, 7), rows[0][1].i);
        }
    }
}

test "a count cut inside the merge keeps exactly the groups the full sort would, ties by first appearance" {
    // Four tables over 40,000 keys, 80,000 refs: enough for the cut to split the merge into partitions. Most keys
    // count 2 (seen by two tables); every 9973rd counts 3, so the top 25 holds the five count-3 keys and a run of the
    // earliest count-2 keys, decided by first appearance.
    const calls = [_]expr_agg.AggCall{.{ .func = .count, .alias = "n", .result = .i64, .arg = null, .where = null }};
    var tables: [4]expr_agg.GroupTable = undefined;
    for (&tables) |*t| t.* = .init(testing.allocator, 1 << 30);
    defer for (&tables) |*t| t.deinit();
    var buf: [32]u8 = undefined;
    const n_keys: usize = 40_000;
    for (0..n_keys) |i| {
        const key = std.fmt.bufPrint(&buf, "k{d}", .{(i * 7919) % n_keys}) catch unreachable;
        for ([_]usize{ i % 4, (i + 1) % 4 }) |ti| {
            const id = try tables[ti].getOrInsert(key, &calls, i * 4 + ti);
            tables[ti].accumulators.items[id].count += if (i % 9973 == 0 and ti == i % 4) 2 else 1;
        }
    }
    var refs: std.ArrayList(MergeGroupRef) = .empty;
    defer refs.deinit(testing.allocator);
    for (&tables, 0..) |*t, ti| for (0..t.groupCount()) |g| try refs.append(testing.allocator, .{ .table = @intCast(ti), .group = @intCast(g) });
    try testing.expect(mergePartitionCount(8, refs.items.len, true, mergeEntryBytes(calls.len)) > 1);

    var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_state.deinit();
    const arena = arena_state.allocator();
    const Ranked = struct { c: u64, first: u64, key: []const u8 };
    const rank = struct {
        fn lt(_: void, a: Ranked, b: Ranked) bool {
            return a.c > b.c or (a.c == b.c and a.first < b.first);
        }
    }.lt;
    var lists: [2][]Ranked = undefined;
    for ([_]?ResultCut{ null, .{ .a_idx = 0, .k = 25 } }, &lists) |cut, *out| {
        var slots: std.ArrayList(MergedSlot) = .empty;
        defer slots.deinit(testing.allocator);
        var accs: std.ArrayList(expr_agg.Accumulator) = .empty;
        defer accs.deinit(testing.allocator);
        try mergeFoldedRefs(testing.allocator, testing.allocator, &tables, refs.items, &calls, 8, cut, false, &slots, &accs);
        if (cut != null) try testing.expectEqual(@as(usize, 25) * mergePartitionCount(8, refs.items.len, true, mergeEntryBytes(calls.len)), slots.items.len);
        const ranked = try arena.alloc(Ranked, slots.items.len);
        for (slots.items, ranked) |s, *r| r.* = .{ .c = accs.items[s.idx].count, .first = s.first, .key = s.key };
        std.sort.pdq(Ranked, ranked, {}, rank);
        out.* = ranked[0..25];
    }
    for (lists[0], lists[1]) |want, got| {
        try testing.expectEqual(want.c, got.c);
        try testing.expectEqual(want.first, got.first);
        try testing.expectEqualStrings(want.key, got.key);
    }
}

test "firstCandidates: every table's earliest groups hold the groups that appeared first overall" {
    // Table 0 saw a..h at 10..17; table 1 saw d at 0 and its own keys at 100.. . The two earliest overall are d (first
    // seen by table 1) and a (table 0), each among the two earliest of the table that first saw it.
    const calls = [_]expr_agg.AggCall{.{ .func = .count, .arg = null, .where = null, .alias = "c", .result = .i64 }};
    var tables: [2]expr_agg.GroupTable = .{ .init(testing.allocator, 1 << 20), .init(testing.allocator, 1 << 20) };
    defer for (&tables) |*t| t.deinit();
    for ([_][]const u8{ "a", "b", "c", "d", "e", "f", "g", "h" }, 10..) |key, pos| _ = try tables[0].getOrInsert(key, &calls, pos);
    _ = try tables[1].getOrInsert("d", &calls, 0);
    for ([_][]const u8{ "x", "y", "z", "w", "v", "u", "t" }, 100..) |key, pos| _ = try tables[1].getOrInsert(key, &calls, pos);
    var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_state.deinit();
    const kept = (try firstCandidates(arena_state.allocator(), &tables, 2)).?;
    try testing.expect(kept.len <= 4);
    for ([_][]const u8{ "d", "a" }) |want| {
        for (kept) |k| {
            if (std.mem.eql(u8, k, want)) break;
        } else return error.TestExpectedKey;
    }
}



test "runMultiAggregate: returned count(DISTINCT) sets no longer point at the scan's budget" {
    const path = "ci/fixtures/parquet/topk_skew.parquet";
    const bytes = metadata.readFileSlice(path, testing.allocator) catch |err| {
        if (err == error.FileNotFound) return error.SkipZigTest;
        return err;
    };
    defer testing.allocator.free(bytes);
    const inputs = [_]Input{.{ .name = path, .bytes = bytes }};
    for ([_]usize{ 1, 4 }) |parallelism| {
        var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
        defer arena_state.deinit();
        var res = try runMultiAggregate(testing.allocator, arena_state.allocator(), .{
            .inputs = &inputs,
            .aggregate = "count(DISTINCT k) AS u, count(DISTINCT n) AS w",
            .parallelism = parallelism,
            .min_bytes_per_worker = 1,
        });
        defer res.deinit(testing.allocator);
        for (res.state.accumulators) |acc| {
            const d = acc.distinct.heap() orelse continue;
            try testing.expect(d.shared == null);
            if (d.parts) |parts| for (parts) |p| try testing.expect(p.shared == null);
        }
    }
}

test "planAggregate: frozen dispatch agrees across fixtures, knobs, filters and cuts" {
    // The shared binder compares the frozen dispatch and planner; empty input bytes also forbid reading any pages.
    spawn_util.injectFailureAfter(0);
    defer spawn_util.resetFailure();
    const fixtures = [_]struct { path: []const u8, keys: []const []const u8, filters: []const ?[]const u8 }{
        .{
            .path = "ci/fixtures/parquet/topk_skew.parquet",
            .keys = &.{ "k", "n", "v", "k, n", "n + 1 AS shifted" },
            .filters = &.{ null, "n < 12", "v > '10'", "k LIKE '%a%'" },
        },
        .{
            .path = "ci/fixtures/parquet/census_topk.parquet",
            .keys = &.{ "u", "t", "s", "u, s" },
            .filters = &.{ null, "u > 300000000000", "s LIKE '%x%'" },
        },
    };
    const configs = [_]?SketchTopK{
        null,
        .{},
        .{ .cell_bits = 3 },
        .{ .cell_bits = 8, .admit_per_row = 1 },
        .{ .census = false },
        .{ .fuse_census = true },
        .{ .fuse_census = false },
        .{ .certify = false, .census_fp_bits = 3, .census_check_rows = 64 },
    };
    const orders = [_]expr_ast.ResultOrder{
        .{},
        .{ .limit = 3 },
        .{ .limit = 0 },
        .{ .keys = &.{.{ .column = 1, .descending = true }}, .limit = 3, .offset = 2 },
        .{ .keys = &.{.{ .column = 1, .descending = false }}, .limit = 3 },
        .{ .keys = &.{.{ .column = 0, .descending = true }}, .limit = 3 },
    };
    var seen = std.EnumSet(@FieldType(AggregateDispatch, "strategy")){};
    for (fixtures) |fixture| {
        const bytes = (try loadFixture(fixture.path)) orelse return error.SkipZigTest;
        defer testing.allocator.free(bytes);
        var meta_arena = std.heap.ArenaAllocator.init(testing.allocator);
        defer meta_arena.deinit();
        const metas = [_]schema.FileMetaData{try metadata.open(meta_arena.allocator(), bytes)};
        const inputs = [_]Input{.{ .name = fixture.path, .bytes = &.{} }};
        for (fixture.keys) |key| for (fixture.filters) |filter| for (configs) |cfg| for (orders) |order| {
            for ([_]usize{ 1, 3 }) |parallelism| for ([_]usize{ 1024, 512 << 20 }) |max_memory| {
                var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
                defer arena_state.deinit();
                const explained = try explainAggregate(testing.allocator, arena_state.allocator(), .{
                    .inputs = &inputs,
                    .ndv_policy = false,
                    .metas = &metas,
                    .aggregate = "count(*) AS c",
                    .group_by = key,
                    .filter = filter,
                    .parallelism = parallelism,
                    .min_bytes_per_worker = 1,
                    .sketch_topk = cfg,
                    .result_order = order,
                    .max_memory = max_memory,
                });
                seen.insert(explained.plan.dispatch.strategy);
            };
        };
    }
    for ([_]@FieldType(AggregateDispatch, "strategy"){ .plain_hash, .sketch, .first_keys }) |strategy|
        try testing.expect(seen.contains(strategy));
    try testing.expectEqual(@as(usize, 0), spawn_util.failuresInjected());
}

test "planAggregate: scalar, ordered blocks, empty work and gated input stay unchanged" {
    const Gate = struct {
        var marker: u8 = 0;
        fn wait(_: *anyopaque, _: usize) anyerror!void {
            return error.UnexpectedPageRead;
        }
        fn done(_: *anyopaque, _: usize, _: usize) void {
            unreachable;
        }
    };
    for ([_][]const u8{ "group_order", "no_row_groups" }) |fixture| {
        var meta_arena = std.heap.ArenaAllocator.init(testing.allocator);
        defer meta_arena.deinit();
        const path = try std.fmt.allocPrint(meta_arena.allocator(), "ci/fixtures/parquet/{s}.parquet", .{fixture});
        const bytes = (try loadFixture(path)) orelse return error.SkipZigTest;
        defer testing.allocator.free(bytes);
        const metas = [_]schema.FileMetaData{try metadata.open(meta_arena.allocator(), bytes)};
        for ([_]bool{ false, true }) |gated| for ([_]bool{ false, true }) |unordered| {
            for ([_]bool{ false, true }) |grouped| for ([_]bool{ false, true }) |scan_all| {
                var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
                defer arena_state.deinit();
                const inputs = [_]Input{.{ .name = path, .bytes = &.{}, .gate = if (gated)
                    .{ .ctx = &Gate.marker, .wait = Gate.wait, .done = Gate.done }
                else
                    null }};
                const has_float = std.mem.eql(u8, fixture, "group_order");
                const explained = try explainAggregate(testing.allocator, arena_state.allocator(), .{
                    .inputs = &inputs,
                    .metas = &metas,
                    .aggregate = if (has_float) "count(*) AS c, sum(x) AS s" else "count(*) AS c",
                    .group_by = if (grouped) (if (std.mem.eql(u8, fixture, "group_order")) "k" else "s") else null,
                    .parallelism = 3,
                    .min_bytes_per_worker = 1,
                    .sketch_topk = .{},
                    .result_order = .{ .limit = 3 },
                    .unordered = unordered,
                    .scan_all = scan_all,
                });
                if (!grouped) try testing.expectEqual(.scalar, explained.plan.dispatch.strategy);
                if (gated) try testing.expect(explained.plan.dispatch.count_agg == null);
                if (grouped and !unordered and std.mem.eql(u8, fixture, "group_order"))
                    try testing.expect(explained.plan.dispatch.use_blocks);
            };
        };
    }
}
test "review round3 count budget and path matrix" {
    for ([_][]const u8{ "ci/fixtures/parquet/census_topk.parquet", "ci/fixtures/parquet/census_dictionary_review.parquet" }) |path| {
        const bytes = (try loadFixture(path)) orelse return error.SkipZigTest;
        defer testing.allocator.free(bytes);
        const inputs = [_]Input{.{ .name = path, .bytes = bytes }};
        for ([_]?usize{ null, 3000, 20000 }) |limit| for ([_]bool{ false, true }) |encoded|
            for ([_][]const u8{ "u", "s" }) |key| for ([_]?[]const u8{ null, "n = 1", "t > 50000000", "t > 3" }) |filter|
                for ([_]SketchTopK{ .{ .fuse_census = true }, .{ .fuse_census = false }, .{ .certify = false }, .{ .census_fp_bits = 0 } }) |cfg| {
                    var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
                    defer arena_state.deinit();
                    const arena = arena_state.allocator();
                    var args: MultiAggArgs = .{
                        .inputs = &inputs,
                        .filter = filter,
                        .aggregate = "count(*) AS c, sum(t + t) AS st, min(s) AS lo, count(DISTINCT t) AS d, count(*) FILTER (WHERE n = 1) AS f",
                        .group_by = key,
                        .parallelism = 1,
                        .decode_limit = limit,
                        .encoded = encoded,
                        .result_order = .{ .keys = &.{.{ .column = 1, .descending = true }}, .limit = 3, .offset = 2 },
                    };
                    var want = try runMultiAggregate(testing.allocator, arena, args);
                    defer want.deinit(testing.allocator);
                    args.sketch_topk = cfg;
                    var got = try runMultiAggregate(testing.allocator, arena, args);
                    defer got.deinit(testing.allocator);
                    try want.output.applyResultOrder(testing.allocator, args.result_order);
                    try got.output.applyResultOrder(testing.allocator, args.result_order);
                    try testing.expectEqual(want.rows_in, got.rows_in);
                    try testing.expectEqual(want.rows_scanned, got.rows_scanned);
                    try testing.expectEqual(want.rows_matched, got.rows_matched);
                    const wr = want.output.group_rows.?;
                    const gr = got.output.group_rows.?;
                    try testing.expectEqual(wr.len, gr.len);
                    for (wr, gr) |a, b| for (a, b) |av, bv| try testing.expect(sameValue(av, bv));
                };
    }
}

test "runMultiAggregate: workers that move to partition runs mid-scan answer as the plain fold" {
    // census_topk's `s`: near-unique 20-byte strings, 8 row groups of 1000. A threshold of 64 groups moves a worker
    // after its first window; at -j 3 some workers may never move and still join the runs.
    const path = "ci/fixtures/parquet/census_topk.parquet";
    const bytes = (try loadFixture(path)) orelse return error.SkipZigTest;
    defer testing.allocator.free(bytes);
    const inputs = [_]Input{.{ .name = path, .bytes = bytes }};
    const aggregates = [_][]const u8{
        "count(*) AS c, sum(t) AS st, min(s) AS lo, max(u) AS hi",
        "count(n) AS c, avg(t) AS av",
    };
    const cuts = [_]expr_ast.ResultOrder{
        .{},
        .{ .keys = &.{.{ .column = 1, .descending = true }}, .limit = 7 },
    };
    for (aggregates) |aggregate| for ([_][]const u8{ "s", "s, n" }) |key| for ([_]usize{ 1, 3 }) |j| for (cuts) |cut| {
        var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
        defer arena_state.deinit();
        const arena = arena_state.allocator();
        var args: MultiAggArgs = .{
            .inputs = &inputs,
            .aggregate = aggregate,
            .group_by = key,
            .parallelism = j,
            .min_bytes_per_worker = 1,
            .result_order = cut,
            .spill = false,
        };
        var want = try runMultiAggregate(testing.allocator, arena, args);
        defer want.deinit(testing.allocator);
        args.spill = true;
        args.spill_groups = 64;
        var got = try runMultiAggregate(testing.allocator, arena, args);
        defer got.deinit(testing.allocator);
        try testing.expect(!want.part_agg);
        try testing.expect(got.part_agg);
        try want.output.applyResultOrder(testing.allocator, cut);
        try got.output.applyResultOrder(testing.allocator, cut);
        const wr = want.output.group_rows.?;
        const gr = got.output.group_rows.?;
        try testing.expectEqual(wr.len, gr.len);
        for (wr, gr) |a, b| for (a, b) |av, bv| try testing.expect(sameValue(av, bv));
    };
}

test "runMultiAggregate: a budget the plain fold fits in holds every worker's partition runs" {
    // Each of thousands of runs drawing a shared-budget block once failed a budget the plain fold uses a sliver of.
    const path = "ci/fixtures/parquet/census_topk.parquet";
    const bytes = (try loadFixture(path)) orelse return error.SkipZigTest;
    defer testing.allocator.free(bytes);
    const inputs = [_]Input{.{ .name = path, .bytes = bytes }};
    for ([_]bool{ false, true }) |spill| {
        var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
        defer arena_state.deinit();
        var res = try runMultiAggregate(testing.allocator, arena_state.allocator(), .{
            .inputs = &inputs,
            .aggregate = "count(*) AS c, sum(t) AS st",
            .group_by = "s",
            .parallelism = 3,
            .min_bytes_per_worker = 1,
            .max_memory = 4 << 20,
            .spill = spill,
            .spill_groups = 64,
        });
        defer res.deinit(testing.allocator);
        try testing.expectEqual(spill, res.part_agg);
        try testing.expectEqual(@as(usize, 7997), res.output.group_rows.?.len);
    }
}







test "regression: string-key spill answers every budget the plain fold answers, gated inputs included" {
    const path = "ci/fixtures/parquet/census_topk.parquet";
    const bytes = (try loadFixture(path)) orelse return error.SkipZigTest;
    defer testing.allocator.free(bytes);
    // A gate that never waits: with the mapped file beside it the input reads again, without it a spill is not armed.
    const plain_in = [_]Input{.{ .name = path, .bytes = bytes }};
    const gated_in = [_]Input{.{ .name = path, .bytes = bytes, .gate = NoGate.gate, .plan_bytes = bytes }};
    const unmapped_in = [_]Input{.{ .name = path, .bytes = bytes, .gate = NoGate.gate }};
    for ([_]usize{ 1, 3 }) |j| {
        // Dynamic RG assignment changes private-table growth slack at 768 KiB. Keep that boundary serial;
        // concurrent parity uses a refusal margin, beside the unchanged successful-budget cases.
        const budgets = [_]usize{
            96 << 10,                                     192 << 10,  384 << 10,
            (if (j == 1) @as(usize, 768) else 512) << 10, 1536 << 10, 4 << 20,
        };
        const base: MultiAggArgs = .{
            .inputs = &plain_in,
            .aggregate = "count(*) AS c, sum(t) AS st",
            .group_by = "s",
            .parallelism = j,
            .min_bytes_per_worker = 1,
            .spill_groups = 64,
        };
        var want: [budgets.len]anyerror!usize = undefined;
        var no_spill = base;
        no_spill.spill = false;
        outcomes(no_spill, &budgets, &want);
        for ([_][]const Input{ &plain_in, &gated_in, &unmapped_in }) |inputs| {
            var got: [budgets.len]anyerror!usize = undefined;
            var a = base;
            a.inputs = inputs;
            outcomes(a, &budgets, &got);
            try expectSameOutcomes(&want, &got);
        }
    }
}

test "regression: a ranking result cut answers every budget the uncut merge answers" {
    const path = "ci/fixtures/parquet/census_topk.parquet";
    const bytes = (try loadFixture(path)) orelse return error.SkipZigTest;
    defer testing.allocator.free(bytes);
    const inputs = [_]Input{.{ .name = path, .bytes = bytes }};
    for ([_]usize{ 1, 3 }) |j| {
        // Match the spill parity test: a serial boundary, concurrent refusal/success margins.
        const budgets = [_]usize{
            96 << 10,                                     192 << 10, 384 << 10,
            (if (j == 1) @as(usize, 768) else 512) << 10, 4 << 20,
        };
        const uncut: MultiAggArgs = .{
            .inputs = &inputs,
            .aggregate = "sum(t) AS st, count(*) AS c",
            .group_by = "s",
            .parallelism = j,
            .min_bytes_per_worker = 1,
        };
        var cut = uncut;
        cut.result_order = .{ .keys = &.{.{ .column = 1, .descending = true }}, .limit = 10 };
        var want: [budgets.len]anyerror!usize = undefined;
        var got: [budgets.len]anyerror!usize = undefined;
        outcomes(uncut, &budgets, &want);
        outcomes(cut, &budgets, &got);
        for (want, got) |w, g| {
            if (w) |_| try testing.expectEqual(@as(usize, 10), try g) else |e| try testing.expectError(e, g);
        }
    }
}

test "grouped DISTINCT bitmap forced layouts match exact fallback with NULLs filters ordering and typed output" {
    const gpa = testing.allocator;
    var arena = std.heap.ArenaAllocator.init(gpa);
    defer arena.deinit();
    const a = arena.allocator();
    const Fixture = BitmapTestFixture;
    const fixture = try Fixture.init(a);
    for ([_]?[]const u8{ null, "x >= -4", "k >= 2" }) |filter| {
        for ([_]bool{ false, true }) |typed| {
            const cut: expr_ast.ResultOrder = if (typed) .{} else .{ .keys = &.{.{ .column = 1, .descending = true }}, .limit = 8 };
            var args = fixture.args();
            args.filter = filter;
            args.typed_answer = typed;
            args.result_order = cut;
            args.grouped_distinct_bitmap = .off;
            var want = try runMultiAggregate(gpa, a, args);
            defer want.deinit(gpa);
            try want.output.applyResultOrder(gpa, cut);
            for ([_]grouped_bitmap.Mode{ .private, .partitioned, .auto }) |mode| {
                args.grouped_distinct_bitmap = mode;
                var got = try runMultiAggregate(gpa, a, args);
                defer got.deinit(gpa);
                try testing.expect(got.grouped_distinct_bitmap);
                try got.output.applyResultOrder(gpa, cut);
                try testing.expectEqualDeep(want.output.group_cols, got.output.group_cols);
                try testing.expectEqualDeep(want.output.group_rows, got.output.group_rows);
                try testing.expectEqualDeep(want.output.typed, got.output.typed);
                try testing.expectEqualDeep(want.state.group_col_types, got.state.group_col_types);
                const before = try std.json.Stringify.valueAlloc(a, want.output.group_rows, .{});
                const after = try std.json.Stringify.valueAlloc(a, got.output.group_rows, .{});
                try testing.expectEqualStrings(before, after);
            }
        }
    }
    const args = fixture.args();
    var got = try runMultiAggregate(gpa, a, args);
    defer got.deinit(gpa);
    var null_key = false;
    var null_values = false;
    for (got.output.group_rows.?) |row| {
        if (row[0] == .null_val) null_key = true;
        if (row[0] == .i and row[0].i == 7) {
            null_values = true;
            try testing.expectEqual(@as(i128, 0), row[1].i);
        }
    }
    try testing.expect(null_key and null_values);
}



test "grouped DISTINCT bitmap domain violation discards partial state and replays exact fallback" {
    const gpa = testing.allocator;
    var arena = std.heap.ArenaAllocator.init(gpa);
    defer arena.deinit();
    const a = arena.allocator();
    const fixture = try BitmapTestFixture.init(a);
    var args = fixture.args();
    args.grouped_distinct_bitmap = .off;
    var want = try runMultiAggregate(gpa, a, args);
    defer want.deinit(gpa);
    const zero: [8]u8 = @splat(0);
    for (fixture.metas[0].row_groups.items) |*rg| {
        const st = &rg.columns.items[1].meta_data.?.statistics.?;
        st.min_value = &zero;
        st.max_value = &zero;
        st.min = &zero;
        st.max = &zero;
    }
    for ([_]grouped_bitmap.Mode{ .private, .partitioned }) |mode| {
        args.grouped_distinct_bitmap = mode;
        var taken: std.EnumSet(FastPath) = .{};
        args.taken = &taken;
        var ledger: core_memory.SharedBudget = .{ .limit = args.max_memory };
        args.ledger = &ledger;
        try testing.expectError(error.ExceededMemoryBudget, prepareAggregate(gpa, a, args, false));
        try testing.expect(taken.contains(.grouped_distinct_bitmap));
        try testing.expectEqual(@as(usize, 0), ledger.used.load(.monotonic));
        args.ledger = null;
        var got = try runMultiAggregate(gpa, a, args);
        defer got.deinit(gpa);
        try testing.expect(!got.grouped_distinct_bitmap);
        try testing.expectEqualDeep(want.output.group_rows, got.output.group_rows);
    }
}

