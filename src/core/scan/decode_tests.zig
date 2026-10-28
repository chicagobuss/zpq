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
const owner_groups = @import("../owner_groups.zig");
const worker_contracts = @import("worker_contracts.zig");
const FailureProbe = worker_contracts.FailureProbe;
const loadFixture = @import("test_support.zig").loadFixture;
const plan_types = @import("plan_types.zig");
const AggregateDispatch = plan_types.AggregateDispatch;
const RecordAdmission = plan_types.RecordAdmission;
const scan_census = @import("census.zig");
const streamProbeBytes = scan_census.streamProbeBytes;
const input_rewind = @import("input_rewind.zig");
const rewindInputs = input_rewind.rewindInputs;
const owner_bytes = @import("owner_bytes.zig");
const inlineByteTopAnswer = owner_bytes.inlineByteTopAnswer;
const scan_execute = @import("execute.zig");
const runMultiAggregate = scan_execute.runMultiAggregate;
const Chain = scan_execute.Chain;
const narrowed = scan_execute.narrowed;
const explainAggregate = scan_execute.explainAggregate;
const prepareAggregate = scan_execute.prepareAggregate;
const integration_support = @import("integration_support.zig");
const freeResult = integration_support.freeResult;
const rewritten = integration_support.rewritten;
const selectList = integration_support.selectList;
const NoInPlaceResize = integration_support.NoInPlaceResize;
const expectSameBits = integration_support.expectSameBits;
const runHotCertificateAndFree = integration_support.runHotCertificateAndFree;
const countResidualBudgetTrial = integration_support.countResidualBudgetTrial;
const ReleasingScanWindow = integration_support.ReleasingScanWindow;
const releasingOrderedTrial = integration_support.releasingOrderedTrial;
const denseProductScan = integration_support.denseProductScan;
const ownedGroupOutputBytes = integration_support.ownedGroupOutputBytes;
const packedAllocationTrial = integration_support.packedAllocationTrial;
const scan_types = @import("types.zig");
const MultiAggArgs = scan_types.MultiAggArgs;
const SketchTopK = scan_types.SketchTopK;
const Input = scan_types.Input;
const FastPath = scan_types.FastPath;
const RowGroupGate = scan_types.RowGroupGate;
const BoundOutputColumn = scan_types.BoundOutputColumn;
const integer_stream = @import("integer_stream.zig");
const typed_answer = @import("typed_answer.zig");
const testing = std.testing;
const test_fixtures = @import("../parquet/test_fixtures.zig");

test "false but corroborated footer bounds pin the default and scan-all trust boundary" {
    const path = "ci/fixtures/parquet/false_bounds.parquet";
    const bytes = (try loadFixture(path)) orelse return error.SkipZigTest;
    defer testing.allocator.free(bytes);
    const inputs = [_]Input{.{ .name = path, .bytes = bytes }};
    for ([_]usize{ 1, 3, 16 }) |j| for ([_]bool{ false, true }) |scan_all| {
        var arena = std.heap.ArenaAllocator.init(testing.allocator);
        defer arena.deinit();
        var result = try runMultiAggregate(testing.allocator, arena.allocator(), .{
            .inputs = &inputs,
            .aggregate = "sum(v) AS s",
            .filter = "x = 100001",
            .scan_all = scan_all,
            .parallelism = j,
            .min_bytes_per_worker = 1,
        });
        defer result.deinit(testing.allocator);
        const got = result.output.aggs[0].value;
        if (scan_all) try testing.expectEqual(@as(i128, 20), got.i) else try testing.expect(got == .null_val);
    };
}

test "sketch census and first-key plans reject all gated inputs including mapped readers" {
    const path = "ci/fixtures/parquet/topk_skew.parquet";
    const bytes = (try loadFixture(path)) orelse return error.SkipZigTest;
    defer testing.allocator.free(bytes);
    const Gate = struct {
        fn wait(_: *anyopaque, _: usize) anyerror!void {
            unreachable;
        }
        fn done(_: *anyopaque, _: usize, _: usize) void {
            unreachable;
        }
    };
    var marker: u8 = 0;
    var inputs = [_]Input{.{ .name = path, .bytes = bytes }};
    for ([_][]const u8{ "count(*) AS c", "count(DISTINCT v) AS c" }) |aggregate| {
        for ([_]bool{ false, true }) |first| {
            var arena = std.heap.ArenaAllocator.init(testing.allocator);
            defer arena.deinit();
            const a = arena.allocator();
            const args: MultiAggArgs = .{
                .inputs = &inputs,
                .aggregate = aggregate,
                .group_by = "k",
                .select_cols = try selectList(a, &.{ "k", aggregate }),
                .ndv_policy = false,
                .result_order = .{
                    .keys = if (first) &.{} else &.{.{ .column = 1, .descending = true }},
                    .limit = 1,
                },
                .sketch_topk = .{ .fuse_census = true },
            };
            inputs[0].gate = null;
            inputs[0].plan_bytes = null;
            const ordinary = try explainAggregate(testing.allocator, a, args);
            const expected: @FieldType(AggregateDispatch, "strategy") = if (first) .first_keys else .sketch;
            try testing.expectEqual(expected, ordinary.plan.dispatch.strategy);
            if (!first) try testing.expect(ordinary.plan.dispatch.census_allowed);
            for ([_]bool{ false, true }) |mapped| {
                inputs[0].gate = .{ .ctx = &marker, .wait = Gate.wait, .done = Gate.done };
                inputs[0].plan_bytes = if (mapped) bytes else null;
                const gated = try explainAggregate(testing.allocator, a, args);
                try testing.expectEqual(.plain_hash, gated.plan.dispatch.strategy);
                try testing.expect(!gated.plan.dispatch.census_allowed);
                try testing.expect(!gated.plan.dispatch.census_fused);
                try testing.expect(!gated.plan.dispatch.distinct_census);
            }
        }
    }
}

test "stream probe funding follows projected chunks and retains the fixed option cap" {
    const bytes = (try loadFixture("ci/fixtures/parquet/plain_values_corrupt.parquet")) orelse return error.SkipZigTest;
    defer testing.allocator.free(bytes);
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const metas = [_]schema.FileMetaData{try metadata.open(a, bytes)};
    const Gate = struct {
        fn wait(_: *anyopaque, _: usize) anyerror!void {
            unreachable;
        }
        fn done(_: *anyopaque, _: usize, _: usize) void {
            unreachable;
        }
    };
    var marker: u8 = 0;
    var inputs = [_]Input{.{
        .name = "stream",
        .bytes = &.{},
        .gate = .{ .ctx = &marker, .wait = Gate.wait, .done = Gate.done },
    }};
    const leaf = try expr_parser.parseExprOnly(a, "s", &metas[0], null);
    const calls = [_]expr_agg.AggCall{.{
        .func = .min,
        .arg = leaf,
        .alias = "m",
        .result = .bytes,
        .where = null,
    }};
    const plan = try agg_plan.Plan.init(a, .{ .filter = null, .calls = &calls, .group_keys = null }, &metas, .ignore);
    const quote = streamProbeBytes(&inputs, &metas, &plan);
    try testing.expect(quote > 0 and quote < core_memory.lean_decode / 2);
    var budget: core_memory.SharedBudget = .{ .limit = 1 << 30 };
    var probe: FailureProbe = .{ .limit = budget.limit };
    try probe.reserveStream(&budget, quote);
    try testing.expectEqual(quote, budget.used.load(.monotonic));
    probe.releaseStream();
    // No projected payload and no non-rescannable source require no funding.
    const empty = try agg_plan.Plan.init(a, .{ .filter = null, .calls = &.{}, .group_keys = null }, &metas, .ignore);
    try testing.expectEqual(@as(usize, 0), streamProbeBytes(&inputs, &metas, &empty));
    inputs[0].plan_bytes = bytes;
    try testing.expectEqual(@as(usize, 0), streamProbeBytes(&inputs, &metas, &plan));
    // The plan may request arbitrarily much; the quota is still independent of workers and live ledger room.
    try probe.reserveStream(&budget, std.math.maxInt(usize));
    try testing.expectEqual(core_memory.lean_decode / 2, budget.used.load(.monotonic));
    probe.releaseStream();
    try testing.expectEqual(@as(usize, 0), budget.used.load(.monotonic));
}

test "writer statistics: rewritten footers suppress pruning page indexes and trusted aggregate answers" {
    const path = "ci/fixtures/parquet/writer_stats.parquet";
    const original = (try loadFixture(path)) orelse return error.SkipZigTest;
    defer testing.allocator.free(original);
    const cases = [_]struct { writer: []const u8, binary: bool, unsigned: bool }{
        .{ .writer = "parquet-mr version 1.7.0 (build synthetic)", .binary = false, .unsigned = false },
        .{ .writer = "parquet-mr version 1.8.0", .binary = true, .unsigned = false },
        .{ .writer = "parquet-mr version 1.10.0", .binary = true, .unsigned = true },
        .{ .writer = "parquet-cpp version 1.2.9", .binary = true, .unsigned = false },
        .{ .writer = "parquet-cpp version 1.3.0", .binary = true, .unsigned = true },
        .{ .writer = "parquet-mr version 1.5.0-cdh5.4.999", .binary = false, .unsigned = false },
        .{ .writer = "parquet-mr version 1.5.0-cdh5.5.0", .binary = false, .unsigned = false },
        .{ .writer = "parquet-mr version malformed", .binary = false, .unsigned = false },
    };
    for (cases) |c| {
        const bytes = try rewritten(original, .{ .created_by = c.writer });
        defer testing.allocator.free(bytes);
        const inputs = [_]Input{.{ .name = path, .bytes = bytes }};
        // The fixture's false but plausible chunk AND page claims exclude real rows and say there are no nulls.
        for ([_][]const u8{ "s = 'a'", "s IS NULL", "d = 1.00", "d IS NULL", "u = 1", "u IS NULL" }, 0..) |filter, fi| {
            for ([_]bool{ false, true }) |trust| for ([_]bool{ false, true }) |scan_all| {
                var arena = std.heap.ArenaAllocator.init(testing.allocator);
                defer arena.deinit();
                const result = try runMultiAggregate(testing.allocator, arena.allocator(), .{
                    .inputs = &inputs,
                    .aggregate = "count(*) AS n",
                    .filter = filter,
                    .trust_stats = trust,
                    .scan_all = scan_all,
                    .parallelism = 3,
                    .max_memory = 64 << 10,
                    .decode_batch_rows = 64,
                });
                defer freeResult(result);
                const allowed = if (fi < 4) c.binary else c.unsigned;
                const want: i128 = if (scan_all or !allowed) 1 else 0;
                try testing.expectEqual(want, result.output.aggs[0].value.i);
            };
        }
        for ([_]usize{ 1, 3, 16 }) |j| for ([_]bool{ false, true }) |scan_all| {
            var arena = std.heap.ArenaAllocator.init(testing.allocator);
            defer arena.deinit();
            const result = try runMultiAggregate(testing.allocator, arena.allocator(), .{
                .inputs = &inputs,
                .aggregate = "count(s) AS s, count(d) AS d, count(u) AS u, min(d) AS m",
                .trust_stats = true,
                .scan_all = scan_all,
                .parallelism = j,
            });
            defer freeResult(result);
            for (0..3) |ci| {
                const allowed = if (ci < 2) c.binary else c.unsigned;
                const want: i128 = if (allowed and !scan_all) 3 else 2;
                try testing.expectEqual(want, result.output.aggs[ci].value.i);
            }
            const minimum: i128 = if (c.binary and !scan_all) 900 else 100;
            try testing.expectEqual(minimum, result.output.aggs[3].value.dec.v);
        };
    }
}

test "dependent group keys: nullable multi-key trees and canonical float blocks match legacy" {
    var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_state.deinit();
    const a = arena_state.allocator();
    const streaming = @import("../writer/streaming.zig");
    var sink: streaming.MemorySink = .{ .arena = a };
    const k = [_]i32{ std.math.minInt(i32), -1, 0, 1, std.math.maxInt(i32), -1, 1, 0 };
    const b = [_]i32{ 0, 1, 1, -1, 0, 1, -1, 1 };
    const defs = [_]u32{ 1, 1, 0, 1, 1, 1, 1, 0 };
    const f = [_]f64{ 1e20, 1, -1e20, -0.0, 7, 1e-20, 11, -0.0 };
    const cols = [_]expr_agg.OutputCol{
        .{ .name = "k", .parquet_type = .INT32, .nullable = true, .col = .{ .i32 = .{
            .values = &k,
            .def_levels = &defs,
            .max_def = 1,
        } } },
        .{ .name = "b", .parquet_type = .INT32, .nullable = false, .col = .{ .i32 = .{ .values = &b } } },
        .{ .name = "f", .parquet_type = .DOUBLE, .nullable = false, .col = .{ .f64 = .{ .values = &f } } },
    };
    _ = try consumer.writeAggregateRows(a, sink.sink(), &cols, k.len, .SNAPPY, .{}, .{});
    var meta = try metadata.open(a, sink.buffer.items);
    const rgs = try a.alloc(schema.RowGroup, 16);
    @memset(rgs, meta.row_groups.items[0]);
    meta.row_groups.items = rgs;
    meta.num_rows *= 16;
    const inputs = [_]Input{.{ .name = "dependent.parquet", .bytes = sink.buffer.items }};
    for ([_][]const u8{ "k, k-1 AS km, k*k AS sq", "b, k-1 AS km, k, k+b AS kb, k*0 AS z" }) |keys| {
        const args: MultiAggArgs = .{
            .inputs = &inputs,
            .metas = &.{meta},
            .aggregate = "count(*) AS c, sum(f) AS s, avg(f) AS a",
            .group_by = keys,
            .dependent_keys = false,
            .parallelism = 1,
            .min_bytes_per_worker = 1,
            .scan_all = true,
        };
        var want = try runMultiAggregate(testing.allocator, a, args);
        defer want.deinit(testing.allocator);
        for ([_]usize{ 1, 3, 16 }) |j| for ([_]usize{ 0, 64 }) |rows| {
            var trial = args;
            trial.parallelism = j;
            trial.dependent_keys = true;
            trial.decode_batch_rows = rows;
            var got = try runMultiAggregate(testing.allocator, a, trial);
            defer got.deinit(testing.allocator);
            const wr = want.output.group_rows.?;
            const gr = got.output.group_rows.?;
            try testing.expectEqual(wr.len, gr.len);
            for (wr, gr) |w, g| for (w, g) |x, y| try expectSameBits(x, y);
            for (want.output.group_cols.?, got.output.group_cols.?) |w, g| try testing.expectEqualStrings(w, g);
        };
        var typed_args = args;
        typed_args.aggregate = "count(*) AS c";
        var original = try runMultiAggregate(testing.allocator, a, typed_args);
        defer original.deinit(testing.allocator);
        typed_args.dependent_keys = true;
        typed_args.typed_answer = true;
        var typed = try runMultiAggregate(testing.allocator, a, typed_args);
        defer typed.deinit(testing.allocator);
        for (original.output.group_rows.?, 0..) |row, i| {
            for (row, typed.output.typed.?.columns) |v, col| {
                try testing.expectEqual(v == .null_val, col.i64.defs[i] == 0);
                if (v != .null_val) try testing.expectEqual(v.i, col.i64.values[i]);
            }
        }
        // Derived arithmetic is INT64, even when the retained base is INT32.
        try testing.expect(typed.state.group_col_types.?[1].source == null);
    }
}

test "packed byte SUM declines reconstructed outputs before admission" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const key: expr_ast.Expr = .{ .col_ref = .{ .col_idx = 0, .physical_type = .BYTE_ARRAY, .expr_type = .str } };
    const value: expr_ast.Expr = .{ .col_ref = .{ .col_idx = 1, .physical_type = .INT64, .expr_type = .i64 } };
    const calls = [_]expr_agg.AggCall{.{ .func = .sum, .arg = value, .where = null, .alias = "s", .result = .i64 }};
    var outputs = [_]BoundOutputColumn{
        .{ .source = .{ .key = 0 }, .name = "k" },
        .{ .source = .{ .agg = 0 }, .name = "s" },
    };
    const args: MultiAggArgs = .{
        .inputs = &.{},
        .aggregate = "sum(v) AS s",
        .byte_records = true,
        .byte_records_min_rows = 0,
        .max_memory = 512 << 20,
        .result_order = .{ .keys = &.{.{ .column = 1, .descending = true }}, .limit = 10 },
    };
    const plan = try agg_plan.Plan.init(a, .{ .filter = null, .calls = &.{}, .group_keys = null }, &.{}, .ignore);
    // Keep the other admission conditions satisfied: a multi-key SQL fixture alone could mask a missing guard.
    const admitted = try inlineByteTopAnswer(
        testing.allocator,
        a,
        args,
        &.{},
        &plan,
        &.{},
        &calls,
        &.{key},
        &outputs,
        1,
        .{},
        true,
    );
    try testing.expect(admitted != null);
    for (&outputs) |*out| {
        out.derived = key;
        const refused = try inlineByteTopAnswer(
            testing.allocator,
            a,
            args,
            &.{},
            &plan,
            &.{},
            &calls,
            &.{key},
            &outputs,
            1,
            .{},
            true,
        );
        try testing.expect(refused == null);
        out.derived = null;
    }
}

test "packed byte COUNT top-H answers as the general path, with NULL, empty and embedded-zero keys and ties" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const streaming = @import("../writer/streaming.zig");
    var sink: streaming.MemorySink = .{ .arena = a };
    const words = [_][]const u8{ "b", "a", "", "a\x00", "zz", "a", "q", "b", "", "r" };
    const n = 4000;
    const k = try a.alloc([]const u8, n);
    const k_def = try a.alloc(u32, n);
    for (0..n) |i| {
        // Skewed, with many equal counts at the cut: ties go to first appearance.
        k[i] = words[(i * 7 + i / 13) % words.len];
        k_def[i] = @intFromBool(i % 17 != 4);
    }
    const cols = [_]expr_agg.OutputCol{
        .{ .name = "k", .parquet_type = .BYTE_ARRAY, .nullable = true, .col = .{ .string = .{ .values = k, .def_levels = k_def, .max_def = 1 } } },
    };
    _ = try consumer.writeAggregateRows(a, sink.sink(), &cols, n, .SNAPPY, .{}, .{});
    var meta = try metadata.open(a, sink.buffer.items);
    const rgs = try a.alloc(schema.RowGroup, 8);
    @memset(rgs, meta.row_groups.items[0]);
    meta.row_groups.items = rgs;
    meta.num_rows *= 8;
    const inputs = [_]Input{.{ .name = "count-keys.parquet", .bytes = sink.buffer.items }};
    for ([_]bool{ true, false }) |descending| for ([_]u64{ 0, 2 }) |offset| {
        const base: MultiAggArgs = .{
            .inputs = &inputs,
            .metas = &.{meta},
            .aggregate = "count(*) AS c",
            .group_by = "k",
            .parallelism = 1,
            .min_bytes_per_worker = 1,
            .result_order = .{ .keys = &.{.{ .column = 1, .descending = descending }}, .limit = 3, .offset = offset },
        };
        var want = try runMultiAggregate(testing.allocator, a, base);
        defer want.deinit(testing.allocator);
        try want.output.applyResultOrder(testing.allocator, base.result_order);
        for ([_]usize{ 1, 2, 4, 8, 16 }) |j| for ([_]usize{ 4 << 20, 512 << 20 }) |limit| {
            var trial = base;
            trial.byte_records = true;
            trial.byte_records_min_rows = 0;
            trial.parallelism = j;
            trial.max_memory = limit;
            const explanation = try explainAggregate(testing.allocator, a, trial);
            const admitted = explanation.record_path != null and explanation.record_path.?.layout == .byte_count_top_h;
            var got = try runMultiAggregate(testing.allocator, a, trial);
            defer got.deinit(testing.allocator);
            try got.output.applyResultOrder(testing.allocator, trial.result_order);
            // A tight budget declines the layout and answers through the general path.
            try testing.expectEqual(limit == 512 << 20, admitted);
            try testing.expectEqualDeep(want.output.group_cols.?, got.output.group_cols.?);
            try testing.expectEqualDeep(want.output.group_rows.?, got.output.group_rows.?);
        };
    };
}

test "packed byte SUM with a dependent integer output uses general grouping" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const streaming = @import("../writer/streaming.zig");
    var sink: streaming.MemorySink = .{ .arena = a };
    const k = [_][]const u8{ "b", "a", "b", "a", "", "b" };
    const f = [_]i32{ 1, -2, 1, -2, 0, 3 };
    const v = [_]i64{ 3, 5, -1, 5, 10, 10 };
    const cols = [_]expr_agg.OutputCol{
        .{ .name = "k", .parquet_type = .BYTE_ARRAY, .nullable = false, .col = .{ .string = .{ .values = &k } } },
        .{ .name = "f", .parquet_type = .INT32, .nullable = false, .col = .{ .i32 = .{ .values = &f } } },
        .{ .name = "v", .parquet_type = .INT64, .nullable = false, .col = .{ .i64 = .{ .values = &v } } },
    };
    _ = try consumer.writeAggregateRows(a, sink.sink(), &cols, k.len, .SNAPPY, .{}, .{});
    var meta = try metadata.open(a, sink.buffer.items);
    const rgs = try a.alloc(schema.RowGroup, 16);
    @memset(rgs, meta.row_groups.items[0]);
    meta.row_groups.items = rgs;
    meta.num_rows *= 16;
    const inputs = [_]Input{.{ .name = "dependent-string.parquet", .bytes = sink.buffer.items }};
    const base: MultiAggArgs = .{
        .inputs = &inputs,
        .metas = &.{meta},
        .aggregate = "sum(v) AS s",
        .group_by = "k, f, f-1 AS fm",
        .dependent_keys = false,
        .parallelism = 1,
        .min_bytes_per_worker = 1,
        .scan_all = true,
        .result_order = .{ .keys = &.{.{ .column = 3, .descending = true }}, .limit = 3 },
    };
    var want = try runMultiAggregate(testing.allocator, a, base);
    defer want.deinit(testing.allocator);
    try want.output.applyResultOrder(testing.allocator, base.result_order);
    for ([_]usize{ 1, 2, 4, 8, 16 }) |j| for ([_]usize{ 64 << 10, 512 << 20 }) |limit| {
        var taken: std.EnumSet(FastPath) = .{};
        var trial = base;
        trial.dependent_keys = true;
        trial.byte_records = true;
        trial.byte_records_min_rows = 0;
        trial.parallelism = j;
        trial.max_memory = limit;
        trial.taken = &taken;
        const explanation = try explainAggregate(testing.allocator, a, trial);
        try testing.expect(explanation.record_path == null);
        var got = try runMultiAggregate(testing.allocator, a, trial);
        defer got.deinit(testing.allocator);
        try got.output.applyResultOrder(testing.allocator, trial.result_order);
        try testing.expect(!taken.contains(.integer_records));
        try testing.expectEqualDeep(want.output.group_cols.?, got.output.group_cols.?);
        try testing.expectEqualDeep(want.output.group_rows.?, got.output.group_rows.?);
    };
}

test "integer owner EXPLAIN admits synthetic rows through the shared 31-bit boundary" {
    const path = "ci/fixtures/parquet/topk_skew.parquet";
    const bytes = (try loadFixture(path)) orelse return error.SkipZigTest;
    defer testing.allocator.free(bytes);
    var held = std.heap.ArenaAllocator.init(testing.allocator);
    defer held.deinit();
    var meta = try metadata.open(held.allocator(), bytes);
    meta.row_groups.items.len = 1;
    const inputs = [_]Input{.{ .name = path, .bytes = bytes }};
    for ([_]usize{ (1 << 27) - 1, 1 << 27, 180_000_000, (1 << 31) - 1, 1 << 31 }) |rows| {
        meta.num_rows = @intCast(rows);
        meta.row_groups.items[0].num_rows = @intCast(rows);
        for ([_]bool{ false, true }) |top| {
            var arena = std.heap.ArenaAllocator.init(testing.allocator);
            defer arena.deinit();
            const args: MultiAggArgs = .{
                .inputs = &inputs,
                .metas = (&meta)[0..1],
                .aggregate = if (top) "sum(n) AS s" else "count(*) AS c, sum(n) AS s",
                .group_by = "n",
                .result_order = if (top) .{ .keys = &.{.{ .column = 1, .descending = true }}, .limit = 7 } else .{},
                .typed_answer = true,
                .integer_records = true,
                .integer_records_min_rows = 0,
                .max_memory = std.math.maxInt(usize),
                .parallelism = 1,
                .scan_all = true,
            };
            const ex = try explainAggregate(testing.allocator, arena.allocator(), args);
            try testing.expectEqual(owner_groups.admitsRows(.int64, rows), ex.record_path != null);
            if (ex.record_path) |record| {
                const expected: @FieldType(RecordAdmission, "layout") = if (top)
                    .integer_sum_top_h
                else
                    .integer_group_stream;
                try testing.expectEqual(expected, record.layout);
            }
        }
    }
}

test "COUNT certificate sample reads real pages across row groups and repeated input files" {
    const cert = @import("../count_certificate.zig");
    const path = "ci/fixtures/parquet/topk_skew.parquet";
    const bytes = (try loadFixture(path)) orelse return error.SkipZigTest;
    defer testing.allocator.free(bytes);
    var state = std.heap.ArenaAllocator.init(testing.allocator);
    defer state.deinit();
    const meta = try metadata.open(state.allocator(), bytes);
    const source: cert.Source = .{
        .rg = &meta.row_groups.items[0],
        .meta = &meta,
        .src = .{ .bytes = bytes, .byte_origin = 0 },
    };
    const sources = [_]cert.Source{ source, source, source };
    var sample = try cert.sample(testing.allocator, &sources, 1, 1, 4, 4, .{});
    defer sample.deinit(testing.allocator);
    try testing.expectEqual(@as(u64, @intCast(source.rg.num_rows * 3)), sample.rows);
    try testing.expect(sample.counts.count() > 0);
    try testing.expect(sample.fmax >= sample.fmin);
}

test "hot COUNT certificate matches canonical clustered keys, NULLs, ties, offsets and late cold values" {
    var certified = false;
    var residual = false;
    for ([_][]const u8{ "plain", "dict" }) |encoding| {
        var file_arena = std.heap.ArenaAllocator.init(testing.allocator);
        defer file_arena.deinit();
        const path = try std.fmt.allocPrint(
            file_arena.allocator(),
            "ci/fixtures/parquet/count_certificate_{s}.parquet",
            .{encoding},
        );
        const bytes = (try loadFixture(path)) orelse return error.SkipZigTest;
        defer testing.allocator.free(bytes);
        const inputs = [_]Input{.{ .name = path, .bytes = bytes }};
        for ([_][]const u8{ "k", "k, f" }) |key| for ([_]usize{ 1, 2, 4, 8, 16 }) |j| {
            for ([_]usize{ 1 << 20, 512 << 20 }) |budget| for ([_]bool{ false, true }) |force_replay| {
                var arena = std.heap.ArenaAllocator.init(testing.allocator);
                defer arena.deinit();
                const args: MultiAggArgs = .{
                    .inputs = &inputs,
                    .aggregate = "count(*) AS c",
                    .group_by = key,
                    .parallelism = j,
                    .min_bytes_per_worker = 1,
                    .max_memory = budget,
                    .decode_limit = budget,
                    .result_order = .{
                        .keys = &.{.{ .column = if (key.len == 1) 1 else 2, .descending = true }},
                        .limit = 3,
                        .offset = if (key.len == 1) 0 else 2,
                    },
                    .ndv_policy = false,
                    .plain_rules = false,
                    .dense = false,
                };
                var want = try runMultiAggregate(testing.allocator, arena.allocator(), args);
                defer want.deinit(testing.allocator);
                var outcome: SketchTopK.HotOutcome = .declined;
                var trial = args;
                trial.sketch_topk = .{
                    .cell_bits = 8,
                    .admit_per_row = 1,
                    .hot_outcome = &outcome,
                    .certify = !force_replay,
                };
                var got = try runMultiAggregate(testing.allocator, arena.allocator(), trial);
                defer got.deinit(testing.allocator);
                certified = certified or outcome == .certified;
                residual = residual or outcome == .residual;
                try testing.expectEqual(want.rows_in, got.rows_in);
                try testing.expectEqual(want.rows_scanned, got.rows_scanned);
                try testing.expectEqual(want.rows_matched, got.rows_matched);
                try want.output.applyResultOrder(testing.allocator, args.result_order);
                try got.output.applyResultOrder(testing.allocator, args.result_order);
                try testing.expectEqualDeep(want.output.group_rows.?, got.output.group_rows.?);
            };
        };
    }
    try testing.expect(certified);
    try testing.expect(residual);
}



test "hot COUNT certificate releases every trial allocation and its ledger charge on failure" {
    const path = "ci/fixtures/parquet/count_certificate_plain.parquet";
    const bytes = (try loadFixture(path)) orelse return error.SkipZigTest;
    defer testing.allocator.free(bytes);
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    var meta = try metadata.open(arena.allocator(), bytes);
    meta.row_groups.items = meta.row_groups.items[meta.row_groups.items.len - 1 ..];
    const inputs = [_]Input{.{ .name = path, .bytes = bytes }};
    var fixed: NoInPlaceResize = .{ .child = testing.allocator };
    try testing.checkAllAllocationFailures(fixed.allocator(), runHotCertificateAndFree, .{ &inputs, &meta });
}



test "COUNT-certified payloads preserve signed SUM, exact integer AVG, NULLs, extrema and complete counts" {
    var used_certificate = false;
    var used_payload_policy = false;
    for ([_][]const u8{ "plain", "dict" }) |encoding| {
        var held = std.heap.ArenaAllocator.init(testing.allocator);
        defer held.deinit();
        const path = try std.fmt.allocPrint(
            held.allocator(),
            "ci/fixtures/parquet/count_certificate_{s}.parquet",
            .{encoding},
        );
        const bytes = (try loadFixture(path)) orelse return error.SkipZigTest;
        defer testing.allocator.free(bytes);
        const inputs = [_]Input{.{ .name = path, .bytes = bytes }};
        for ([_]usize{ 1, 2, 4, 8, 16 }) |j| for ([_]usize{ 8 << 20, 512 << 20 }) |budget| {
            var arena = std.heap.ArenaAllocator.init(testing.allocator);
            defer arena.deinit();
            const args: MultiAggArgs = .{
                .inputs = &inputs,
                .aggregate = "count(*) AS c, sum(v) AS total, avg(v) AS av, min(v) AS mn, max(v) AS mx",
                .group_by = "s",
                .parallelism = j,
                .min_bytes_per_worker = 1,
                .max_memory = budget,
                .decode_limit = budget,
                .result_order = .{ .keys = &.{.{ .column = 1, .descending = true }}, .limit = 3, .offset = 2 },
                .dense = false,
                .ndv_union = false,
            };
            var want = try runMultiAggregate(testing.allocator, arena.allocator(), args);
            defer want.deinit(testing.allocator);
            var outcome: SketchTopK.Outcome = .ineligible;
            var trial = args;
            trial.sketch_topk = .{ .cell_bits = 8, .admit_per_row = 1, .outcome = &outcome };
            const explained = try explainAggregate(testing.allocator, arena.allocator(), trial);
            var got = try runMultiAggregate(testing.allocator, arena.allocator(), trial);
            defer got.deinit(testing.allocator);
            used_certificate = used_certificate or outcome == .census or outcome == .certified;
            if (explained.plan.reasons.contains(.census_short_string_payload)) {
                used_payload_policy = true;
                try testing.expect(explained.plan.dispatch.census_allowed and explained.plan.dispatch.census_fused);
            }
            try want.output.applyResultOrder(testing.allocator, args.result_order);
            try got.output.applyResultOrder(testing.allocator, args.result_order);
            try testing.expectEqualDeep(want.output.group_rows.?, got.output.group_rows.?);
            try testing.expectEqual(want.rows_matched, got.rows_matched);
        };
    }
    try testing.expect(used_certificate);
    try testing.expect(used_payload_policy);
}

test "hot COUNT footer range hints cannot exclude NULL or out-of-range winners" {
    const path = "ci/fixtures/parquet/count_certificate_plain.parquet";
    const bytes = (try loadFixture(path)) orelse return error.SkipZigTest;
    defer testing.allocator.free(bytes);
    const inputs = [_]Input{.{ .name = path, .bytes = bytes }};
    for ([_][2]i64{ .{ -1, 1 }, .{ 2, 2 } }) |range| for ([_]usize{ 1, 4 }) |j| {
        var arena = std.heap.ArenaAllocator.init(testing.allocator);
        defer arena.deinit();
        const actual = try metadata.open(arena.allocator(), bytes);
        const hint = try metadata.open(arena.allocator(), bytes);
        var lo: [8]u8 = undefined;
        var hi: [8]u8 = undefined;
        std.mem.writeInt(i64, &lo, range[0], .little);
        std.mem.writeInt(i64, &hi, range[1], .little);
        for (hint.row_groups.items) |*rg| {
            const cm = &rg.columns.items[1].meta_data.?;
            try testing.expectEqual(schema.Type.INT64, cm.type);
            cm.statistics.?.min_value = &lo;
            cm.statistics.?.max_value = &hi;
        }
        var args: MultiAggArgs = .{
            .inputs = &inputs,
            .metas = (&actual)[0..1],
            .aggregate = "count(*) AS c",
            .group_by = "k,f",
            .parallelism = j,
            .min_bytes_per_worker = 1,
            .ndv_policy = false,
            .plain_rules = false,
            .dense = false,
            .result_order = .{ .keys = &.{.{ .column = 2, .descending = true }}, .limit = 3 },
        };
        var want = try runMultiAggregate(testing.allocator, arena.allocator(), args);
        defer want.deinit(testing.allocator);
        args.metas = (&hint)[0..1];
        args.sketch_topk = .{ .cell_bits = 8, .admit_per_row = 1 };
        var got = try runMultiAggregate(testing.allocator, arena.allocator(), args);
        defer got.deinit(testing.allocator);
        try want.output.applyResultOrder(testing.allocator, args.result_order);
        try got.output.applyResultOrder(testing.allocator, args.result_order);
        try testing.expectEqualDeep(want.output.group_rows.?, got.output.group_rows.?);
    };
}

test "COUNT input-wide sample survives every partial worker start" {
    const cert = @import("../count_certificate.zig");
    const path = "ci/fixtures/parquet/topk_skew.parquet";
    const bytes = (try loadFixture(path)) orelse return error.SkipZigTest;
    defer testing.allocator.free(bytes);
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const meta = try metadata.open(arena.allocator(), bytes);
    const source: cert.Source = .{
        .rg = &meta.row_groups.items[0],
        .meta = &meta,
        .src = .{ .bytes = bytes, .byte_origin = 0 },
    };
    const sources = [_]cert.Source{ source, source, source };
    var want = try cert.sample(testing.allocator, &sources, 1, 1, 4, 4, .{});
    defer want.deinit(testing.allocator);
    for (0..3) |started| {
        spawn_util.injectFailureAfter(started);
        defer spawn_util.resetFailure();
        var got = try cert.sample(testing.allocator, &sources, 1, 1, 4, 4, .{});
        defer got.deinit(testing.allocator);
        try testing.expect(spawn_util.failuresInjected() > 0);
        try testing.expectEqual(want.rows, got.rows);
        try testing.expectEqual(want.fmin, got.fmin);
        try testing.expectEqual(want.fmax, got.fmax);
        try testing.expectEqual(want.counts.count(), got.counts.count());
        var it = want.counts.iterator();
        while (it.next()) |entry| try testing.expectEqual(entry.value_ptr.*, got.counts.get(entry.key_ptr.*).?);
    }
}



test "COUNT residual budget refusal releases trial groups and restores counts before the fitting plain scan" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var sink: @import("../writer/streaming.zig").MemorySink = .{ .arena = a };
    const keys = [_]i64{ 7, 3, 7, 9, 3, 7, 11, 11, 12, 13, 14, 15, 16, 17, 18, 19 };
    const cols = [_]expr_agg.OutputCol{.{
        .name = "k",
        .parquet_type = .INT64,
        .nullable = false,
        .col = .{ .i64 = .{ .values = &keys } },
    }};
    _ = try consumer.writeAggregateRows(a, sink.sink(), &cols, keys.len, .SNAPPY, .{}, .{});
    const meta = try metadata.open(a, sink.buffer.items);
    try countResidualBudgetTrial(testing.allocator, sink.buffer.items, &meta, false);
    var fixed: NoInPlaceResize = .{ .child = testing.allocator };
    try testing.checkAllAllocationFailures(
        fixed.allocator(),
        countResidualBudgetTrial,
        .{ sink.buffer.items, &meta, false },
    );
}

test "COUNT residual never turns a real decode failure into a budget fallback" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var sink: @import("../writer/streaming.zig").MemorySink = .{ .arena = a };
    const keys = [_]i64{ 1, 2, 1, 3 };
    const cols = [_]expr_agg.OutputCol{.{
        .name = "k",
        .parquet_type = .INT64,
        .nullable = false,
        .col = .{ .i64 = .{ .values = &keys } },
    }};
    _ = try consumer.writeAggregateRows(a, sink.sink(), &cols, keys.len, .SNAPPY, .{}, .{});
    const meta = try metadata.open(a, sink.buffer.items);
    const cm = meta.row_groups.items[0].columns.items[0].meta_data.?;
    const offset: usize = @intCast(cm.dictionary_page_offset orelse cm.data_page_offset);
    sink.buffer.items[offset] = 0xaf;
    try countResidualBudgetTrial(testing.allocator, sink.buffer.items, &meta, true);
}

test "integer owner decoded runs and false sorted hints match the canonical multi-file answer" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const streaming = @import("../writer/streaming.zig");
    for ([_]bool{ false, true }) |descent| {
        var sink: streaming.MemorySink = .{ .arena = a };
        var file = try consumer.AggregateFile.init(a, sink.sink());
        defer file.deinit();
        for (0..4) |item| {
            var keys = [_]i64{ 2, 2, 2, 2 };
            if (item == 0) keys[0] = 1;
            if (item == 3) keys[3] = 3;
            if (descent and item == 2) keys[2] = -1;
            const values = [_]i64{ 3, -4, 6, 5 };
            const cols = [_]expr_agg.OutputCol{
                .{ .name = "k", .parquet_type = .INT64, .nullable = false, .col = .{ .i64 = .{ .values = &keys } } },
                .{
                    .name = "v",
                    .parquet_type = .INT64,
                    .nullable = true,
                    .col = .{ .i64 = .{ .values = &values, .def_levels = &.{ 0, 1, 1, 0 }, .max_def = 1 } },
                },
            };
            try file.append(&cols, keys.len, .SNAPPY, .{}, .{});
        }
        _ = try file.finish(&.{
            .{ .name = "k", .parquet_type = .INT64, .nullable = false, .col = .{ .i64 = .{ .values = &.{} } } },
            .{ .name = "v", .parquet_type = .INT64, .nullable = true, .col = .{ .i64 = .{ .values = &.{} } } },
        });
        const meta = try metadata.open(a, sink.buffer.items);
        if (descent) for (meta.row_groups.items) |*rg| {
            var zero: [8]u8 = @splat(0);
            const st = &rg.columns.items[0].meta_data.?.statistics.?;
            st.min_value = try a.dupe(u8, &zero);
            st.max_value = try a.dupe(u8, &zero);
        };
        // Two files force a source seam that descends; one file exercises a three-row-group equality chain.
        const input: Input = .{ .name = "runs.parquet", .bytes = sink.buffer.items };
        for ([_]usize{ 1, 2 }) |files| {
            const inputs = [_]Input{ input, input };
            const metas = [_]schema.FileMetaData{ meta, meta };
            const base: MultiAggArgs = .{
                .inputs = inputs[0..files],
                .metas = metas[0..files],
                .aggregate = "count(*) AS c, sum(v) AS s",
                .group_by = "k",
                .max_memory = 512 << 20,
                .parallelism = 1,
                .min_bytes_per_worker = 1,
                .integer_records = false,
                .scan_all = true,
            };
            var want = try runMultiAggregate(testing.allocator, a, base);
            defer want.deinit(testing.allocator);
            for ([_]usize{ 1, 2, 4, 8, 16 }) |j| {
                var args = base;
                args.integer_records = true;
                args.integer_records_min_rows = 0;
                args.typed_answer = true;
                args.parallelism = j;
                args.decode_batch_rows = 2;
                var got = try runMultiAggregate(testing.allocator, a, args);
                defer got.deinit(testing.allocator);
                const stream = got.output.integer_stream.?;
                const rows = want.output.group_rows.?;
                try testing.expectEqual(!descent and files == 1, stream.state.runs.len != 0);
                try testing.expectEqual(rows.len, stream.state.group_count);
                var window = try stream.window(testing.allocator, got.state.group_col_types.?, 0, rows.len, j);
                defer window.deinit(testing.allocator);
                for (rows, 0..) |row, i| for (row, window.columns) |v, col| {
                    const lane = col.i64;
                    try testing.expectEqual(v == .null_val, lane.defs.len != 0 and lane.defs[i] == 0);
                    if (v != .null_val) try testing.expectEqual(v.i, lane.values[i]);
                };
                for ([_][]const u8{ "sum(v) AS s", "count(*) AS c" }) |aggregate| for ([_]bool{ false, true }) |desc| {
                    var top_args = base;
                    top_args.aggregate = aggregate;
                    top_args.parallelism = j;
                    top_args.result_order = .{ .keys = &.{.{ .column = 1, .descending = desc }}, .limit = 2, .offset = 1 };
                    var top_want = try runMultiAggregate(testing.allocator, a, top_args);
                    defer top_want.deinit(testing.allocator);
                    top_args.integer_records = true;
                    top_args.integer_records_min_rows = 0;
                    top_args.typed_answer = true;
                    var taken: std.EnumSet(FastPath) = .{};
                    top_args.taken = &taken;
                    if (!descent and files == 1 and aggregate[0] == 'c') {
                        const explanation = try explainAggregate(testing.allocator, a, top_args);
                        try testing.expectEqual(.integer_count_top_h, explanation.record_path.?.layout);
                    }
                    var top_got = try prepareAggregate(testing.allocator, a, top_args, false);
                    defer top_got.deinit(testing.allocator);
                    if (!descent and files == 1) try testing.expect(taken.contains(.integer_records));
                    try top_want.output.applyResultOrder(testing.allocator, top_args.result_order);
                    try top_got.output.applyResultOrder(testing.allocator, top_args.result_order);
                    try testing.expectEqualDeep(top_want.output.group_rows.?, top_got.output.group_rows.?);
                };
            }
        }
    }
}





test "ordered decline replays releasing gates for NULL descent and cross-row-group overlap" {
    var held = std.heap.ArenaAllocator.init(testing.allocator);
    defer held.deinit();
    const a = held.allocator();
    for (0..3) |fault| {
        var sink: @import("../writer/streaming.zig").MemorySink = .{ .arena = a };
        var file = try consumer.AggregateFile.init(a, sink.sink());
        defer file.deinit();
        for (0..3) |rg| {
            var keys = [_]i64{ 1, 2, 2, 3 };
            var defs = [_]u32{ 1, 1, 1, 1 };
            if (fault != 2) for (&keys) |*key| {
                key.* += @intCast(rg * 2);
            };
            if (fault == 0 and rg == 1) defs[2] = 0;
            if (fault == 1 and rg == 1) keys[2] = 3;
            const values = [_]i64{ 3, -4, 6, 5 };
            const cols = [_]expr_agg.OutputCol{
                .{
                    .name = "k",
                    .parquet_type = .INT64,
                    .nullable = true,
                    .col = .{ .i64 = .{ .values = &keys, .def_levels = &defs, .max_def = 1 } },
                },
                .{ .name = "v", .parquet_type = .INT64, .nullable = false, .col = .{ .i64 = .{ .values = &values } } },
            };
            try file.append(&cols, keys.len, .SNAPPY, .{}, .{});
        }
        _ = try file.finish(&.{
            .{ .name = "k", .parquet_type = .INT64, .nullable = true, .col = .{ .i64 = .{ .values = &.{} } } },
            .{ .name = "v", .parquet_type = .INT64, .nullable = false, .col = .{ .i64 = .{ .values = &.{} } } },
        });
        const meta = try metadata.open(a, sink.buffer.items);
        // Bounds select the attempt, but decoded NULLs, descents and seams must all be checked.
        if (fault == 2) for (meta.row_groups.items) |*rg| {
            const stats = &rg.columns.items[0].meta_data.?.statistics.?;
            stats.min_value = &@as([8]u8, @splat(0));
            stats.max_value = stats.min_value;
        };
        const inputs = [_]Input{.{ .name = "scan-all.parquet", .bytes = sink.buffer.items }};
        var want = try runMultiAggregate(testing.allocator, a, .{
            .inputs = &inputs,
            .metas = (&meta)[0..1],
            .aggregate = "count(*) AS c, sum(v) AS s",
            .group_by = "k",
            .scan_all = true,
            .integer_records = false,
        });
        defer want.deinit(testing.allocator);
        for ([_]usize{ 1, 2, 4, 8, 16 }) |j|
            try releasingOrderedTrial(testing.allocator, sink.buffer.items, &meta, want.output.group_rows.?, j);
        if (fault == 0) {
            var fixed: NoInPlaceResize = .{ .child = testing.allocator };
            try testing.checkAllAllocationFailures(
                fixed.allocator(),
                releasingOrderedTrial,
                .{ sink.buffer.items, &meta, want.output.group_rows.?, 1 },
            );
        }
    }
}

test "phase-four replay policies decline releasing gates before certificate residual or budget trials" {
    const path = "ci/fixtures/parquet/count_certificate_plain.parquet";
    const bytes = (try loadFixture(path)) orelse return error.SkipZigTest;
    defer testing.allocator.free(bytes);
    var held = std.heap.ArenaAllocator.init(testing.allocator);
    defer held.deinit();
    const meta = try metadata.open(held.allocator(), bytes);
    const mapped = [_]Input{.{ .name = path, .bytes = bytes }};
    for ([_][]const u8{ "count(*) AS c", "count(DISTINCT v) AS c" }) |aggregate|
        for ([_]bool{ false, true }) |first| {
            var arena = std.heap.ArenaAllocator.init(testing.allocator);
            defer arena.deinit();
            const a = arena.allocator();
            var args: MultiAggArgs = .{
                .inputs = &mapped,
                .metas = (&meta)[0..1],
                .aggregate = aggregate,
                .group_by = "k",
                .integer_records = false,
                .ndv_policy = false,
                .plain_rules = false,
                .dense = false,
                .scan_all = true,
                .min_bytes_per_worker = 1,
                .result_order = .{ .keys = if (first) &.{} else &.{.{ .column = 1, .descending = true }}, .limit = 3 },
            };
            var want = try runMultiAggregate(testing.allocator, a, args);
            defer want.deinit(testing.allocator);
            try want.output.applyResultOrder(testing.allocator, args.result_order);
            for ([_]usize{ 1, 4, 16 }) |j| for ([_]usize{ 1 << 20, 512 << 20 }) |budget|
                for ([_]bool{ false, true }) |certify| {
                    var window: ReleasingScanWindow = .{ .bytes = try a.dupe(u8, bytes), .meta = &meta };
                    const inputs = [_]Input{window.input(bytes)};
                    var outcome: SketchTopK.Outcome = .certified;
                    args.inputs = &inputs;
                    args.parallelism = j;
                    args.max_memory = budget;
                    args.decode_limit = budget;
                    args.sketch_topk = .{
                        .cell_bits = 8,
                        .admit_per_row = 1,
                        .fuse_census = true,
                        .certify = certify,
                        .outcome = &outcome,
                    };
                    const explanation = try explainAggregate(testing.allocator, a, args);
                    try testing.expectEqual(.plain_hash, explanation.plan.dispatch.strategy);
                    var got = try runMultiAggregate(testing.allocator, a, args);
                    defer got.deinit(testing.allocator);
                    try got.output.applyResultOrder(testing.allocator, args.result_order);
                    try testing.expectEqualDeep(want.output.group_rows.?, got.output.group_rows.?);
                    try testing.expectEqual(meta.row_groups.items.len, window.waits.load(.monotonic));
                    try testing.expectEqual(meta.row_groups.items.len, window.releases.load(.monotonic));
                    try testing.expectEqual(.ineligible, outcome);
                };
        };
}

test "windowed S3 merge refusal never retries consumed non-rescannable inputs" {
    const s3_window = @import("../../io/s3_window.zig");
    if (!s3_window.supported) return error.SkipZigTest;
    const gpa = testing.allocator;
    var arena = std.heap.ArenaAllocator.init(gpa);
    defer arena.deinit();
    const a = arena.allocator();
    var sink: @import("../writer/streaming.zig").MemorySink = .{ .arena = a };
    const keys = [_]i64{ 7, 3, 7, 9, 3, 7, 11, 11, 12, 13, 14, 15, 16, 17, 18, 19 };
    const cols = [_]expr_agg.OutputCol{.{
        .name = "k",
        .parquet_type = .INT64,
        .nullable = false,
        .col = .{ .i64 = .{ .values = &keys } },
    }};
    _ = try consumer.writeAggregateRows(a, sink.sink(), &cols, keys.len, .SNAPPY, .{}, .{});
    const meta = try metadata.open(a, sink.buffer.items);
    const Refusal = struct {
        gate: RowGroupGate,
        total: *std.atomic.Value(usize),
        budget: *core_memory.SharedBudget,
        held: *usize,
        waits: std.atomic.Value(usize) = .init(0),

        fn wait(ctx: *anyopaque, rg: usize) anyerror!void {
            const self: *@This() = @ptrCast(@alignCast(ctx));
            _ = self.waits.fetchAdd(1, .monotonic);
            try self.gate.wait(self.gate.ctx, rg);
        }

        fn done(ctx: *anyopaque, rg: usize, items: usize) void {
            const self: *@This() = @ptrCast(@alignCast(ctx));
            self.gate.done(self.gate.ctx, rg, items);
            if (self.total.fetchAdd(1, .acq_rel) + 1 != 2) return;
            // All work is finished. Hold the entire merge ceiling, even if other scan charges return before
            // merging. This injects a merge-only refusal independently of which worker claimed each item.
            self.held.* = self.budget.limit + core_memory.finalize_allowance;
            self.budget.force(self.held.*);
        }
    };
    for ([_]usize{ 2, 4, 16 }) |j| for ([_]bool{ false, true }) |mixed| {
        var budget: core_memory.SharedBudget = .{ .limit = 64 << 20 };
        var held: usize = 0;
        defer budget.release(held);
        var total: std.atomic.Value(usize) = .init(0);
        var groups: [2]s3_window.RowGroup = undefined;
        var order = [_]*s3_window.RowGroup{ &groups[0], &groups[1] };
        var window: s3_window.Window = .{
            .gpa = gpa,
            .order = &order,
            .limit = 2 * s3_window.page,
            .batch = s3_window.page,
        };
        const first = try s3_window.mapBuffer(sink.buffer.items.len);
        defer s3_window.unmapBuffer(first);
        const second = try s3_window.mapBuffer(sink.buffer.items.len);
        defer s3_window.unmapBuffer(second);
        const buffers = [_][]align(s3_window.page) u8{ first, second };
        var rgs = [2][1]?*s3_window.RowGroup{ .{&groups[0]}, .{&groups[1]} };
        var files: [2]s3_window.File = undefined;
        var refusals: [2]Refusal = undefined;
        var inputs: [2]Input = undefined;
        for (0..2) |i| {
            @memcpy(buffers[i][0..sink.buffer.items.len], sink.buffer.items);
            // Already fetched: s3_window's ready state. Its real gate releases these anonymous pages.
            groups[i] = .{
                .jobs = &.{},
                .bytes = buffers[i].len,
                .seg = buffers[i],
                .admitted = true,
                .window = &window,
                .state = .init(1),
            };
            files[i] = .{ .rgs = &rgs[i] };
            refusals[i] = .{ .gate = files[i].gate(), .total = &total, .budget = &budget, .held = &held };
            inputs[i] = .{
                .name = if (mixed and i == 0) "local.parquet" else "s3://bucket/window.parquet",
                .bytes = buffers[i][0..sink.buffer.items.len],
                .plan_bytes = if (mixed and i == 0) sink.buffer.items else null,
                .gate = .{ .ctx = &refusals[i], .wait = Refusal.wait, .done = Refusal.done },
            };
        }
        if (runMultiAggregate(gpa, a, .{
            .inputs = &inputs,
            .metas = &.{ meta, meta },
            .aggregate = "count(*) AS c",
            .group_by = "k",
            .parallelism = j,
            .min_bytes_per_worker = 1,
            .scan_all = true,
            .max_memory = budget.limit,
            .ledger = &budget,
            .charge_decode = false,
            .ndv_policy = false,
            .plain_rules = false,
            .dense = false,
            .partition_cost = false,
            .spill = false,
            .distinct_pairs = .off,
        })) |got| {
            var unexpected = got;
            unexpected.deinit(gpa);
            return error.TestExpectedError;
        } else |err| try testing.expectEqual(error.ExceededMemoryBudget, err);
        try testing.expect(held != 0);
        try testing.expectEqual(held, budget.used.load(.monotonic));
        try testing.expectEqual(@as(usize, 2), total.load(.monotonic));
        for (&groups, &refusals, buffers) |*rg, *refusal, buffer| {
            try testing.expectEqual(@as(usize, 1), refusal.waits.load(.monotonic));
            try testing.expectEqual(@as(u32, 1), rg.items_done.load(.monotonic));
            try testing.expectEqual(@as(u8, 0), buffer[0]); // Actual MADV_DONTNEED, not a retained test buffer.
        }
    };
}

test "budget narrowing requires rescannable inputs for every restart reason" {
    const Gate = struct {
        fn wait(_: *anyopaque, _: usize) anyerror!void {
            unreachable;
        }
        fn done(_: *anyopaque, _: usize, _: usize) void {
            unreachable;
        }
    };
    var marker: u8 = 0;
    var inputs = [_]Input{.{
        .name = "s3://bucket/window.parquet",
        .bytes = &.{},
        .gate = .{ .ctx = &marker, .wait = Gate.wait, .done = Gate.done },
    }};
    var args: MultiAggArgs = .{ .inputs = &inputs, .aggregate = "", .parallelism = 4 };
    for (std.enums.values(FastPath)) |path| {
        var chain: Chain = .{ .parts_tried = false };
        try testing.expect(narrowed(args, args, std.EnumSet(FastPath).initOne(path), &chain, true) == null);
    }
    // The helper also declines at runtime, before allocating a view or abandoning a reader.
    try testing.expectError(error.ExceededMemoryBudget, rewindInputs(testing.allocator, &args));
    try testing.expectEqual(inputs[0..].ptr, args.inputs.ptr);
    for ([_]bool{ false, true }) |mapped| {
        inputs[0].gate = if (mapped) .{ .ctx = &marker, .wait = Gate.wait, .done = Gate.done } else null;
        inputs[0].plan_bytes = if (mapped) &.{} else null;
        var chain: Chain = .{ .parts_tried = false };
        const retry = narrowed(args, args, std.EnumSet(FastPath).initOne(.merge_memory), &chain, false).?;
        try testing.expectEqual(@as(usize, 1), retry.parallelism);
    }
}

test "dense composite counts arm globally and preserve selected first through windows" {
    try denseProductScan(false);
}

test "dense composite SUM arms globally and preserves selected first through windows" {
    try denseProductScan(true);
}



test "distinct bitmaps retain identities after releasing windows and late wide fallback" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var sink: @import("../writer/streaming.zig").MemorySink = .{ .arena = a };
    var file = try consumer.AggregateFile.init(a, sink.sink());
    defer file.deinit();
    for (0..4) |rg| {
        var keys: [512]i64 = undefined;
        var values: [512]i64 = undefined;
        var defs: [512]u32 = undefined;
        for (&keys, &values, &defs, 0..) |*k, *v, *d, r| {
            k.* = @intCast(r % 3);
            v.* = @intCast((r + rg * 31) % 128);
            d.* = @intFromBool(r % 17 != 0);
        }
        if (rg == 3) values[511] = 1 << 48;
        const cols = [_]expr_agg.OutputCol{
            .{ .name = "k", .parquet_type = .INT64, .nullable = false, .col = .{ .i64 = .{ .values = &keys } } },
            .{
                .name = "v",
                .parquet_type = .INT64,
                .nullable = true,
                .col = .{ .i64 = .{ .values = &values, .def_levels = &defs, .max_def = 1 } },
            },
        };
        try file.append(&cols, keys.len, .SNAPPY, .{}, .{});
    }
    _ = try file.finish(&.{
        .{ .name = "k", .parquet_type = .INT64, .nullable = false, .col = .{ .i64 = .{ .values = &.{} } } },
        .{ .name = "v", .parquet_type = .INT64, .nullable = true, .col = .{ .i64 = .{ .values = &.{} } } },
    });
    const bytes = sink.buffer.items;
    const meta = try metadata.open(a, bytes);
    const mapped = [_]Input{.{ .name = "distinct.parquet", .bytes = bytes }};
    for ([_]?[]const u8{ null, "k" }) |group_by| {
        var args: MultiAggArgs = .{
            .inputs = &mapped,
            .metas = (&meta)[0..1],
            .aggregate = "count(DISTINCT v) AS d, count(*) AS c",
            .group_by = group_by,
            .integer_records = false,
            .ndv_policy = false,
            .plain_rules = false,
            .distinct_pairs = .off,
            .dense = false,
            .scan_all = true,
            .min_bytes_per_worker = 1,
        };
        var want = try runMultiAggregate(testing.allocator, a, args);
        defer want.deinit(testing.allocator);
        for ([_]usize{ 1, 2, 4, 8, 16 }) |j| for ([_]usize{ 0, 64 }) |batch_rows| {
            var completed: [4]std.atomic.Value(usize) = @splat(.init(0));
            var window: ReleasingScanWindow = .{
                .bytes = try a.dupe(u8, bytes),
                .meta = &meta,
                .completed = &completed,
            };
            // No mapped planning view: a retry would have to read already released bytes.
            const inputs = [_]Input{.{
                .name = "distinct.parquet",
                .bytes = window.bytes,
                .gate = .{ .ctx = &window, .wait = ReleasingScanWindow.wait, .done = ReleasingScanWindow.release },
            }};
            args.inputs = &inputs;
            args.parallelism = j;
            args.decode_batch_rows = batch_rows;
            var got = try runMultiAggregate(testing.allocator, a, args);
            defer got.deinit(testing.allocator);
            try testing.expectEqualDeep(want.output.group_rows, got.output.group_rows);
            try testing.expectEqualDeep(want.output.aggs, got.output.aggs);
            try testing.expect(window.waits.load(.monotonic) >= meta.row_groups.items.len);
            try testing.expectEqual(meta.row_groups.items.len, window.releases.load(.monotonic));
        };
    }
}

test "distinct coded sets share automatic pairs across releasing windows and refused final drains" {
    const gpa = testing.allocator;
    var arena = std.heap.ArenaAllocator.init(gpa);
    defer arena.deinit();
    const a = arena.allocator();
    for ([_]bool{ false, true }) |mixed| {
        var sink: @import("../writer/streaming.zig").MemorySink = .{ .arena = a };
        var file = try consumer.AggregateFile.init(a, sink.sink());
        defer file.deinit();
        for (0..16) |rg| {
            var keys: [2048]i64 = undefined;
            var values: [2048]i64 = undefined;
            var kd: [2048]u32 = undefined;
            var vd: [2048]u32 = undefined;
            for (&keys, &values, &kd, &vd, 0..) |*k, *v, *kn, *vn, r| {
                k.* = @intCast(r % 3);
                v.* = @intCast(if (mixed and rg == 7) 5000 + r else (r / 3) % 64 + rg * 128);
                kn.* = @intFromBool(r % 23 != 0);
                vn.* = @intFromBool(r % 17 != 0);
            }
            const cols = [_]expr_agg.OutputCol{
                .{
                    .name = "k",
                    .parquet_type = .INT64,
                    .nullable = true,
                    .col = .{ .i64 = .{ .values = &keys, .def_levels = &kd, .max_def = 1 } },
                },
                .{
                    .name = "v",
                    .parquet_type = .INT64,
                    .nullable = true,
                    .col = .{ .i64 = .{ .values = &values, .def_levels = &vd, .max_def = 1 } },
                },
            };
            try file.append(&cols, keys.len, .SNAPPY, .{}, .{});
        }
        _ = try file.finish(&.{
            .{ .name = "k", .parquet_type = .INT64, .nullable = true, .col = .{ .i64 = .{ .values = &.{} } } },
            .{ .name = "v", .parquet_type = .INT64, .nullable = true, .col = .{ .i64 = .{ .values = &.{} } } },
        });
        const bytes = sink.buffer.items;
        const meta = try metadata.open(a, bytes);
        const mapped = [_]Input{.{ .name = "mixed-distinct.parquet", .bytes = bytes }};
        var args: MultiAggArgs = .{
            .inputs = &mapped,
            .metas = (&meta)[0..1],
            .aggregate = "count(DISTINCT v) AS d",
            .group_by = "k",
            .scan_all = true,
            .plain_rules = false,
            .distinct_pairs = .off,
            .pairs_pilot_rows = 1,
            .min_bytes_per_worker = 1,
        };
        var want = try runMultiAggregate(gpa, a, args);
        defer want.deinit(gpa);
        args.distinct_pairs = .auto;
        var mapped_auto = try runMultiAggregate(gpa, a, args);
        defer mapped_auto.deinit(gpa);
        try testing.expectEqualDeep(want.output.group_rows, mapped_auto.output.group_rows);
        try testing.expectEqual(mixed, mapped_auto.distinct_pairs);
        for ([_]usize{ 1, 4, 16 }) |j| for ([_]usize{ 0, 64 }) |batch|
            for ([_]@FieldType(MultiAggArgs, "distinct_pairs"){ .on, .auto }) |mode| {
                var window: ReleasingScanWindow = .{ .bytes = try a.dupe(u8, bytes), .meta = &meta };
                const inputs = [_]Input{window.input(bytes)};
                args.inputs = &inputs;
                args.parallelism = j;
                args.decode_batch_rows = batch;
                args.distinct_pairs = mode;
                var got = try runMultiAggregate(gpa, a, args);
                defer got.deinit(gpa);
                try testing.expectEqualDeep(want.output.group_rows, got.output.group_rows);
                try testing.expectEqual(mode == .on or mixed, got.distinct_pairs);
                try testing.expectEqual(meta.row_groups.items.len, window.releases.load(.monotonic));
            };
        if (mixed) {
            const Refusal = struct {
                window: ReleasingScanWindow,
                budget: *core_memory.SharedBudget,
                held: usize = 0,
                denied: std.atomic.Value(bool) = .init(false),
                fn wait(ctx: *anyopaque, rg: usize) anyerror!void {
                    const self: *@This() = @ptrCast(@alignCast(ctx));
                    try ReleasingScanWindow.wait(&self.window, rg);
                }
                fn done(ctx: *anyopaque, rg: usize, items: usize) void {
                    const self: *@This() = @ptrCast(@alignCast(ctx));
                    ReleasingScanWindow.release(&self.window, rg, items);
                    if (self.window.releases.load(.acquire) == self.window.meta.row_groups.items.len and
                        self.denied.cmpxchgStrong(false, true, .acq_rel, .acquire) == null)
                    {
                        self.held = self.budget.limit + core_memory.finalize_allowance;
                        self.budget.force(self.held);
                    }
                }
                fn start(_: *anyopaque) anyerror!void {}
                fn abandon(ctx: *anyopaque) void {
                    const self: *@This() = @ptrCast(@alignCast(ctx));
                    self.budget.release(self.held);
                    self.held = 0;
                    ReleasingScanWindow.abandon(&self.window);
                }
            };
            var budget: core_memory.SharedBudget = .{ .limit = 64 << 20 };
            var refused: Refusal = .{
                .window = .{ .bytes = try a.dupe(u8, bytes), .meta = &meta },
                .budget = &budget,
            };
            defer budget.release(refused.held);
            const inputs = [_]Input{.{
                .name = "mixed-distinct.parquet",
                .bytes = refused.window.bytes,
                .plan_bytes = bytes,
                .gate = .{ .ctx = &refused, .wait = Refusal.wait, .done = Refusal.done },
            }};
            args.inputs = &inputs;
            args.parallelism = 4;
            args.decode_batch_rows = 0;
            args.distinct_pairs = .auto;
            args.max_memory = 64 << 20;
            args.decode_limit = 64 << 20;
            args.ledger = &budget;
            args.start_reads = .{ .ctx = &refused, .start = Refusal.start, .abandon = Refusal.abandon };
            var got = try runMultiAggregate(gpa, a, args);
            defer got.deinit(gpa);
            try testing.expect(refused.denied.load(.monotonic));
            try testing.expectEqual(@as(usize, 1), refused.window.abandoned);
            try testing.expectEqualDeep(want.output.group_rows, got.output.group_rows);
        }
    }
}



test "scalar owner cardinality survives releasing windows and late width refusal through mapped rewind" {
    const gpa = testing.allocator;
    var arena = std.heap.ArenaAllocator.init(gpa);
    defer arena.deinit();
    const a = arena.allocator();
    for ([_]bool{ false, true }) |late| {
        var sink: @import("../writer/streaming.zig").MemorySink = .{ .arena = a };
        var file = try consumer.AggregateFile.init(a, sink.sink());
        defer file.deinit();
        for (0..16) |rg| {
            var values: [512][]const u8 = undefined;
            var defs: [512]u32 = undefined;
            for (&values, &defs, 0..) |*v, *d, r| {
                const k = (r + rg * 17) % 128;
                v.* = if (k == 0) "" else if (k == 1) "a\x00" else try std.fmt.allocPrint(a, "key-{d}", .{k});
                d.* = @intFromBool(r % 17 != 0);
            }
            if (late and rg == 15) values[511] = "late key beyond fifteen bytes";
            const cols = [_]expr_agg.OutputCol{.{
                .name = "k",
                .parquet_type = .BYTE_ARRAY,
                .nullable = true,
                .col = .{ .string = .{ .values = &values, .def_levels = &defs, .max_def = 1 } },
            }};
            try file.append(&cols, values.len, .SNAPPY, .{}, .{});
        }
        _ = try file.finish(&.{.{
            .name = "k",
            .parquet_type = .BYTE_ARRAY,
            .nullable = true,
            .col = .{ .string = .{ .values = &.{} } },
        }});
        const bytes = sink.buffer.items;
        const meta = try metadata.open(a, bytes);
        for (meta.row_groups.items) |*rg| rg.columns.items[0].meta_data.?.statistics = null;
        const mapped = [_]Input{.{ .name = "scalar-distinct.parquet", .bytes = bytes }};
        var args: MultiAggArgs = .{
            .inputs = &mapped,
            .metas = (&meta)[0..1],
            .aggregate = "count(DISTINCT k) AS cardinality",
            .integer_records = false,
            .byte_records = false,
            .dense = false,
            .ndv_policy = false,
            .plain_rules = false,
            .scan_all = true,
            .min_bytes_per_worker = 1,
            .max_memory = 64 << 20,
        };
        var want = try runMultiAggregate(gpa, a, args);
        defer want.deinit(gpa);
        for ([_]usize{ 1, 2, 4, 8, 16 }) |j| for ([_]usize{ 0, 64 }) |batch| {
            var window: ReleasingScanWindow = .{ .bytes = try a.dupe(u8, bytes), .meta = &meta };
            const inputs = [_]Input{window.input(bytes)};
            args.inputs = &inputs;
            args.byte_records = true;
            args.byte_records_min_rows = 0;
            args.parallelism = j;
            args.decode_batch_rows = batch;
            args.start_reads = .{
                .ctx = &window,
                .start = ReleasingScanWindow.start,
                .abandon = ReleasingScanWindow.abandon,
            };
            const explanation = try explainAggregate(gpa, a, args);
            try testing.expectEqual(.byte_distinct, explanation.record_path.?.layout);
            var got = try runMultiAggregate(gpa, a, args);
            defer got.deinit(gpa);
            try testing.expectEqualDeep(want.output.aggs, got.output.aggs);
            if (!late) {
                try testing.expectEqual(.count, got.state.agg_calls[0].func);
                try testing.expectEqual(@as(usize, 1), got.state.accumulators.len);
                try testing.expectEqual(@as(usize, 1), got.state.agg_sources.len);
                const cols = try expr_agg.finalize(a, got.state.agg_calls[0], got.state.accumulators[0]);
                try testing.expectEqual(@as(i64, @intCast(want.output.aggs[0].value.i)), cols[0].col.i64.values[0]);
            }
            try testing.expectEqual(@as(usize, 16), window.releases.load(.monotonic));
            try testing.expectEqual(@as(usize, @intFromBool(late)), window.abandoned);
        };
        // The same remote input without a retained mapping must decline before consuming any records.
        var window: ReleasingScanWindow = .{ .bytes = try a.dupe(u8, bytes), .meta = &meta };
        var input = window.input(bytes);
        input.plan_bytes = null;
        args.inputs = (&input)[0..1];
        args.start_reads = null;
        const remote = try explainAggregate(gpa, a, args);
        try testing.expect(remote.record_path == null);
    }
}

test "distinct owner radix completes full pairs after releasing every reader window" {
    const gpa = testing.allocator;
    var arena = std.heap.ArenaAllocator.init(gpa);
    defer arena.deinit();
    const a = arena.allocator();
    var sink: @import("../writer/streaming.zig").MemorySink = .{ .arena = a };
    var file = try consumer.AggregateFile.init(a, sink.sink());
    defer file.deinit();
    for (0..16) |rg| {
        var keys: [512]i64 = undefined;
        var vals: [512]i64 = undefined;
        var kd: [512]u32 = undefined;
        var vd: [512]u32 = undefined;
        for (&keys, &vals, &kd, &vd, 0..) |*k, *v, *kn, *vn, r| {
            k.* = @as(i64, @intCast((r + rg * 7) % 13)) * (1 << 48) - (1 << 49);
            v.* = @as(i64, @intCast((r * 3 + rg * 11) % 17)) * (1 << 41) - (1 << 45);
            kn.* = @intFromBool(r % 23 != 0);
            vn.* = @intFromBool(r % 17 != 0 and k.* != 0);
        }
        keys[1] = std.math.minInt(i64);
        vals[1] = std.math.maxInt(i64);
        vals[2] = std.math.minInt(i64);
        const cols = [_]expr_agg.OutputCol{
            .{
                .name = "k",
                .parquet_type = .INT64,
                .nullable = true,
                .col = .{ .i64 = .{ .values = &keys, .def_levels = &kd, .max_def = 1 } },
            },
            .{
                .name = "x",
                .parquet_type = .INT64,
                .nullable = true,
                .col = .{ .i64 = .{ .values = &vals, .def_levels = &vd, .max_def = 1 } },
            },
        };
        try file.append(&cols, keys.len, .SNAPPY, .{}, .{});
    }
    _ = try file.finish(&.{
        .{ .name = "k", .parquet_type = .INT64, .nullable = true, .col = .{ .i64 = .{ .values = &.{} } } },
        .{ .name = "x", .parquet_type = .INT64, .nullable = true, .col = .{ .i64 = .{ .values = &.{} } } },
    });
    const bytes = sink.buffer.items;
    const meta = try metadata.open(a, bytes);
    const mapped = [_]Input{.{ .name = "pair-radix.parquet", .bytes = bytes }};
    var args: MultiAggArgs = .{
        .inputs = &mapped,
        .metas = (&meta)[0..1],
        .aggregate = "count(DISTINCT x) AS d",
        .group_by = "k",
        .integer_records = false,
        .byte_records = false,
        .dense = false,
        .distinct_pairs = .off,
        .ndv_policy = false,
        .plain_rules = false,
        .scan_all = true,
        .min_bytes_per_worker = 1,
        .max_memory = 64 << 20,
        .result_order = .{ .keys = &.{.{ .column = 1, .descending = true }}, .limit = 4, .offset = 2 },
    };
    var want = try runMultiAggregate(gpa, a, args);
    defer want.deinit(gpa);
    try want.output.applyResultOrder(gpa, args.result_order);
    for ([_]usize{ 1, 2, 4, 8, 16 }) |j| for ([_]usize{ 0, 64 }) |batch| {
        var window: ReleasingScanWindow = .{ .bytes = try a.dupe(u8, bytes), .meta = &meta };
        const inputs = [_]Input{window.input(bytes)};
        var taken: std.EnumSet(FastPath) = .{};
        args.inputs = &inputs;
        args.integer_records = true;
        args.integer_records_min_rows = 0;
        args.parallelism = j;
        args.decode_batch_rows = batch;
        args.taken = &taken;
        args.start_reads = .{
            .ctx = &window,
            .start = ReleasingScanWindow.start,
            .abandon = ReleasingScanWindow.abandon,
        };
        const explanation = try explainAggregate(gpa, a, args);
        try testing.expectEqual(.integer_distinct_top_h, explanation.record_path.?.layout);
        var got = try prepareAggregate(gpa, a, args, false);
        defer got.deinit(gpa);
        try testing.expect(taken.contains(.integer_records));
        try got.output.applyResultOrder(gpa, args.result_order);
        try testing.expectEqualDeep(want.output.group_rows.?, got.output.group_rows.?);
        try testing.expectEqual(want.state.group_col_types.?[1].nullable, got.state.group_col_types.?[1].nullable);
        try testing.expectEqual(@as(usize, 16), window.releases.load(.monotonic));
        try testing.expectEqual(@as(usize, 0), window.abandoned);
    };
    args.start_reads = null;
    args.inputs = &mapped;
    args.distinct_pairs = .on;
    args.taken = null;
    const forced = try explainAggregate(gpa, a, args);
    try testing.expectEqual(.pair_distinct, forced.record_path.?.layout);
    var shared_pairs = try prepareAggregate(gpa, a, args, false);
    defer shared_pairs.deinit(gpa);
    try testing.expect(shared_pairs.distinct_pairs);
    try shared_pairs.output.applyResultOrder(gpa, args.result_order);
    try testing.expectEqualDeep(want.output.group_rows, shared_pairs.output.group_rows);
    var window: ReleasingScanWindow = .{ .bytes = try a.dupe(u8, bytes), .meta = &meta };
    var input = window.input(bytes);
    input.plan_bytes = null;
    args.inputs = (&input)[0..1];
    args.distinct_pairs = .off;
    const remote = try explainAggregate(gpa, a, args);
    try testing.expect(remote.record_path == null);
}

test "shared integer scalar consumer owns answers after releasing each full or sliced reader window" {
    const gpa = testing.allocator;
    var arena = std.heap.ArenaAllocator.init(gpa);
    defer arena.deinit();
    const a = arena.allocator();
    var sink: @import("../writer/streaming.zig").MemorySink = .{ .arena = a };
    var file = try consumer.AggregateFile.init(a, sink.sink());
    defer file.deinit();
    var sum: i128 = 0;
    var present: u64 = 0;
    var all: u64 = 0;
    var lo: i64 = std.math.maxInt(i64);
    var hi: i64 = std.math.minInt(i64);
    for (0..16) |rg| {
        var values: [193]i64 = undefined;
        var defs: [193]u32 = undefined;
        for (&values, &defs, 0..) |*v, *d, r| {
            v.* = @as(i64, @intCast(r * 137 + rg * 73)) - 15000;
            d.* = @intFromBool(r % 11 != 0);
            all += 1;
            if (d.* != 0) {
                sum += v.*;
                present += 1;
                lo = @min(lo, v.*);
                hi = @max(hi, v.*);
            }
        }
        try file.append(&.{.{
            .name = "v",
            .parquet_type = .INT64,
            .nullable = true,
            .col = .{ .i64 = .{ .values = &values, .def_levels = &defs, .max_def = 1 } },
        }}, values.len, .SNAPPY, .{}, .{});
    }
    _ = try file.finish(&.{.{
        .name = "v",
        .parquet_type = .INT64,
        .nullable = true,
        .col = .{ .i64 = .{ .values = &.{} } },
    }});
    const bytes = sink.buffer.items;
    const meta = try metadata.open(a, bytes);
    for (meta.row_groups.items) |*rg| rg.columns.items[0].meta_data.?.statistics = null;
    for ([_]usize{ 1, 2, 4, 8, 16 }) |j| for ([_]usize{ 0, 64 }) |batch| {
        var window: ReleasingScanWindow = .{ .bytes = try a.dupe(u8, bytes), .meta = &meta };
        const inputs = [_]Input{window.input(bytes)};
        var got = try runMultiAggregate(gpa, a, .{
            .inputs = &inputs,
            .metas = (&meta)[0..1],
            .aggregate = "count(*) AS c,sum(v) AS s,min(v) AS lo,max(v) AS hi,avg(v) AS av,count(v) AS n",
            .parallelism = j,
            .decode_batch_rows = batch,
            .encoded = false,
            .scan_all = true,
            .min_bytes_per_worker = 1,
            .start_reads = .{
                .ctx = &window,
                .start = ReleasingScanWindow.start,
                .abandon = ReleasingScanWindow.abandon,
            },
        });
        defer got.deinit(gpa);
        const acc = got.state.accumulators;
        try testing.expectEqual(all, acc[0].count);
        try testing.expectEqual(sum, acc[1].sum_i.?);
        try testing.expectEqual(lo, acc[2].min_i.?);
        try testing.expectEqual(hi, acc[3].max_i.?);
        try testing.expectEqual(sum, acc[4].avg.isum);
        try testing.expectEqual(present, acc[4].avg.count);
        try testing.expectEqual(present, acc[5].count);
        try testing.expectEqual(@as(usize, 16), window.releases.load(.monotonic));
        try testing.expectEqual(@as(usize, 0), window.abandoned);
    };
}

test "sliced consumer reuses payload survivors across releasing reader windows" {
    const gpa = testing.allocator;
    var arena = std.heap.ArenaAllocator.init(gpa);
    defer arena.deinit();
    const a = arena.allocator();
    var sink: @import("../writer/streaming.zig").MemorySink = .{ .arena = a };
    var file = try consumer.AggregateFile.init(a, sink.sink());
    defer file.deinit();
    const empty = [_]expr_agg.OutputCol{
        .{ .name = "d", .parquet_type = .INT64, .nullable = true, .col = .{ .i64 = .{ .values = &.{} } } },
        .{ .name = "q", .parquet_type = .DOUBLE, .nullable = true, .col = .{ .f64 = .{ .values = &.{} } } },
        .{ .name = "discount", .parquet_type = .DOUBLE, .nullable = true, .col = .{ .f64 = .{ .values = &.{} } } },
        .{ .name = "price", .parquet_type = .DOUBLE, .nullable = true, .col = .{ .f64 = .{ .values = &.{} } } },
    };
    for (0..16) |rg| {
        var dates: [257]i64 = undefined;
        var quantity: [257]f64 = undefined;
        var discount: [257]f64 = undefined;
        var price: [257]f64 = undefined;
        var defs: [257]u32 = undefined;
        for (&dates, &quantity, &discount, &price, &defs, 0..) |*d, *q, *disc, *p, *dl, r| {
            d.* = @intCast((r + rg * 3) % 23);
            q.* = @floatFromInt(r % 51);
            disc.* = @as(f64, @floatFromInt(r % 11)) / 100;
            p.* = switch (r % 5) {
                0 => 1e16,
                1 => 1,
                2 => -1e16,
                3 => -0.0,
                else => 0.1,
            };
            dl.* = @intFromBool(r % 17 != 0);
        }
        const cols = [_]expr_agg.OutputCol{
            .{
                .name = "d",
                .parquet_type = .INT64,
                .nullable = true,
                .col = .{ .i64 = .{ .values = &dates, .def_levels = &defs, .max_def = 1 } },
            },
            .{
                .name = "q",
                .parquet_type = .DOUBLE,
                .nullable = true,
                .col = .{ .f64 = .{ .values = &quantity, .def_levels = &defs, .max_def = 1 } },
            },
            .{
                .name = "discount",
                .parquet_type = .DOUBLE,
                .nullable = true,
                .col = .{ .f64 = .{ .values = &discount, .def_levels = &defs, .max_def = 1 } },
            },
            .{
                .name = "price",
                .parquet_type = .DOUBLE,
                .nullable = true,
                .col = .{ .f64 = .{ .values = &price, .def_levels = &defs, .max_def = 1 } },
            },
        };
        try file.append(&cols, dates.len, .SNAPPY, .{}, .{});
    }
    _ = try file.finish(&empty);
    const bytes = sink.buffer.items;
    const meta = try metadata.open(a, bytes);
    for (meta.row_groups.items) |*rg| {
        for (rg.columns.items) |*col| col.meta_data.?.statistics = null;
    }
    const mapped = [_]Input{.{ .name = "survivors.parquet", .bytes = bytes }};
    for ([_][]const u8{
        "d>=3 AND d<17 AND discount>=0.05 AND discount<=0.07 AND q<24",
        "d<0 AND q<24",
        "d>=0 AND q>=0",
    }) |filter| {
        var args: MultiAggArgs = .{
            .inputs = &mapped,
            .metas = (&meta)[0..1],
            .aggregate = "sum(price*discount) AS revenue,avg(price*discount) AS av,count(*) AS n",
            .filter = filter,
            .scan_all = true,
            .min_bytes_per_worker = 1,
        };
        var want = try runMultiAggregate(gpa, a, args);
        defer want.deinit(gpa);
        for ([_]usize{ 1, 2, 4, 8, 16 }) |j| {
            var window: ReleasingScanWindow = .{ .bytes = try a.dupe(u8, bytes), .meta = &meta };
            const inputs = [_]Input{window.input(bytes)};
            args.inputs = &inputs;
            args.parallelism = j;
            args.decode_batch_rows = 64;
            args.start_reads = .{
                .ctx = &window,
                .start = ReleasingScanWindow.start,
                .abandon = ReleasingScanWindow.abandon,
            };
            var got = try runMultiAggregate(gpa, a, args);
            defer got.deinit(gpa);
            for (want.state.accumulators, got.state.accumulators) |w, g| switch (w) {
                .sum_f => |value| {
                    try testing.expectEqual(value != null, g.sum_f != null);
                    if (value) |v| try testing.expectEqual(@as(u64, @bitCast(v)), @as(u64, @bitCast(g.sum_f.?)));
                },
                .avg => |avg| {
                    try testing.expectEqual(avg.count, g.avg.count);
                    try testing.expectEqual(@as(u64, @bitCast(avg.sum)), @as(u64, @bitCast(g.avg.sum)));
                },
                .count => |count| try testing.expectEqual(count, g.count),
                else => unreachable,
            };
            try testing.expectEqual(@as(usize, 16), window.releases.load(.monotonic));
            try testing.expectEqual(@as(usize, 0), window.abandoned);
        }
    }
}

test "multi aggregate dictionary keys survive releasing slices and materialized fallback" {
    const gpa = testing.allocator;
    var arena = std.heap.ArenaAllocator.init(gpa);
    defer arena.deinit();
    const a = arena.allocator();
    var sink: @import("../writer/streaming.zig").MemorySink = .{ .arena = a };
    var file = try consumer.AggregateFile.init(a, sink.sink());
    defer file.deinit();
    const empty = [_]expr_agg.OutputCol{
        .{ .name = "k", .parquet_type = .BYTE_ARRAY, .nullable = true, .col = .{ .string = .{ .values = &.{} } } },
        .{ .name = "b", .parquet_type = .BYTE_ARRAY, .nullable = true, .col = .{ .string = .{ .values = &.{} } } },
        .{ .name = "x", .parquet_type = .DOUBLE, .nullable = true, .col = .{ .f64 = .{ .values = &.{} } } },
        .{ .name = "y", .parquet_type = .DOUBLE, .nullable = true, .col = .{ .f64 = .{ .values = &.{} } } },
        .{ .name = "f", .parquet_type = .INT64, .nullable = false, .col = .{ .i64 = .{ .values = &.{} } } },
    };
    for (0..16) |rg| {
        var k: [257][]const u8 = undefined;
        var b: [257][]const u8 = undefined;
        var x: [257]f64 = undefined;
        var y: [257]f64 = undefined;
        var f: [257]i64 = undefined;
        var d: [257]u32 = undefined;
        var bd: [257]u32 = undefined;
        for (0..257) |r| {
            k[r] = if (r % 2 == 0) "A" else "B";
            b[r] = try std.fmt.allocPrint(a, "dictionary-{d}", .{if (rg == 14) r else (r + rg) % 64});
            x[r] = switch (r % 5) {
                0 => 1e16,
                1 => 1,
                2 => -1e16,
                3 => -0.0,
                else => 0.1,
            };
            y[r] = @as(f64, @floatFromInt(r % 11)) / 100;
            f[r] = @intCast(r % 13);
            d[r] = @intFromBool(r % 17 != 0);
            bd[r] = @intFromBool(rg != 15 and r % 19 != 0);
        }
        var cols = empty;
        cols[0].col = .{ .string = .{ .values = &k, .def_levels = &d, .max_def = 1 } };
        cols[1].col = .{ .string = .{ .values = &b, .def_levels = &bd, .max_def = 1 } };
        cols[2].col = .{ .f64 = .{ .values = &x, .def_levels = &d, .max_def = 1 } };
        cols[3].col = .{ .f64 = .{ .values = &y, .def_levels = &bd, .max_def = 1 } };
        cols[4].col = .{ .i64 = .{ .values = &f } };
        try file.append(&cols, 257, .SNAPPY, .{}, .{});
    }
    _ = try file.finish(&empty);
    const bytes = sink.buffer.items;
    const meta = try metadata.open(a, bytes);
    for (meta.row_groups.items) |*rg| for (rg.columns.items) |*c| {
        c.meta_data.?.statistics = null;
    };
    const mapped = [_]Input{.{ .name = "coded-multi.parquet", .bytes = bytes }};
    for ([_][]const u8{ "f<7", "f<0", "f>=0" }) |filter| {
        var args: MultiAggArgs = .{
            .inputs = &mapped,
            .metas = (&meta)[0..1],
            .filter = filter,
            .aggregate = "sum(x) AS s,avg(x) AS av,sum(x*(1-y)) AS discounted,sum(x*(1-y)*(1+y)) AS charge,avg(y) AS ay,count(*) AS n,count(y) AS ny",
            .group_by = "k,b",
            .encoded = false,
            .dense = false,
            .integer_records = false,
            .byte_records = false,
            .ndv_policy = false,
            .scan_all = true,
            .min_bytes_per_worker = 1,
        };
        var want = try runMultiAggregate(gpa, a, args);
        defer want.deinit(gpa);
        for ([_]usize{ 1, 2, 4, 8, 16 }) |j| for ([_]usize{ 0, 64, 4096 }) |batch| {
            var window: ReleasingScanWindow = .{ .bytes = try a.dupe(u8, bytes), .meta = &meta };
            const inputs = [_]Input{window.input(bytes)};
            args.inputs = &inputs;
            args.parallelism = j;
            args.decode_batch_rows = batch;
            args.encoded = true;
            args.start_reads = .{ .ctx = &window, .start = ReleasingScanWindow.start, .abandon = ReleasingScanWindow.abandon };
            var got = try runMultiAggregate(gpa, a, args);
            defer got.deinit(gpa);
            try testing.expectEqualDeep(want.output.group_rows, got.output.group_rows);
            try testing.expectEqual(@as(usize, 16), window.releases.load(.monotonic));
            try testing.expectEqual(@as(usize, 0), window.abandoned);
        };
        args.inputs = &mapped;
    }
}

test "exact multi aggregate roots preserve state through releasing reader windows" {
    const gpa = testing.allocator;
    var arena = std.heap.ArenaAllocator.init(gpa);
    defer arena.deinit();
    const a = arena.allocator();
    var sink: @import("../writer/streaming.zig").MemorySink = .{ .arena = a };
    var file = try consumer.AggregateFile.init(a, sink.sink());
    defer file.deinit();
    const empty = [_]expr_agg.OutputCol{
        .{ .name = "k", .parquet_type = .BYTE_ARRAY, .nullable = true, .col = .{ .string = .{ .values = &.{} } } },
        .{ .name = "b", .parquet_type = .BYTE_ARRAY, .nullable = true, .col = .{ .string = .{ .values = &.{} } } },
        .{
            .name = "x",
            .parquet_type = .INT64,
            .nullable = true,
            .dec = .{ .precision = 15, .scale = 2 },
            .col = .{ .i128 = .{ .values = &.{} } },
        },
        .{
            .name = "y",
            .parquet_type = .INT64,
            .nullable = true,
            .dec = .{ .precision = 15, .scale = 2 },
            .col = .{ .i128 = .{ .values = &.{} } },
        },
        .{ .name = "f", .parquet_type = .INT64, .nullable = false, .col = .{ .i64 = .{ .values = &.{} } } },
    };
    for (0..16) |rg| {
        var k: [257][]const u8 = undefined;
        var b: [257][]const u8 = undefined;
        var x: [257]i128 = undefined;
        var y: [257]i128 = undefined;
        var f: [257]i64 = undefined;
        var d: [257]u32 = undefined;
        var bd: [257]u32 = undefined;
        for (0..257) |r| {
            k[r] = if (r % 2 == 0) "A" else "B";
            b[r] = try std.fmt.allocPrint(a, "dictionary-{d}", .{if (rg == 14) r else (r + rg) % 64});
            x[r] = if (r % 13 == 0) 10000000000000 else @as(i128, @intCast(r * 137)) - 12000;
            y[r] = if (r % 13 == 0) 100000 else @intCast(r % 11);
            f[r] = @intCast(r % 13);
            d[r] = @intFromBool(r % 17 != 0);
            bd[r] = @intFromBool(rg != 15 and r % 19 != 0);
        }
        var cols = empty;
        cols[0].col = .{ .string = .{ .values = &k, .def_levels = &d, .max_def = 1 } };
        cols[1].col = .{ .string = .{ .values = &b, .def_levels = &bd, .max_def = 1 } };
        cols[2].col = .{ .i128 = .{ .values = &x, .def_levels = &d, .max_def = 1 } };
        cols[3].col = .{ .i128 = .{ .values = &y, .def_levels = &bd, .max_def = 1 } };
        cols[4].col = .{ .i64 = .{ .values = &f } };
        try file.append(&cols, 257, .SNAPPY, .{}, .{});
    }
    _ = try file.finish(&empty);
    const bytes = sink.buffer.items;
    const meta = try metadata.open(a, bytes);
    for (meta.row_groups.items) |*rg| for (rg.columns.items) |*c| {
        c.meta_data.?.statistics = null;
    };
    const mapped = [_]Input{.{ .name = "exact-multi.parquet", .bytes = bytes }};
    for ([_][]const u8{ "f<7", "f<0", "f>=0" }) |filter| {
        var args: MultiAggArgs = .{
            .inputs = &mapped,
            .metas = (&meta)[0..1],
            .filter = filter,
            .aggregate = "sum(x) AS s,avg(x) AS av,sum(x*(1-y)) AS discounted,sum(x*(1-y)*(1+y)) AS charge," ++
                "avg(y) AS ay," ++
                "sum(x*(1-y)) AS repeated,avg(x*(1-y)) AS repeated_avg,count(*) AS n,count(y) AS ny",
            .group_by = "k,b",
            .encoded = false,
            .dense = false,
            .integer_records = false,
            .byte_records = false,
            .ndv_policy = false,
            .scan_all = true,
            .min_bytes_per_worker = 1,
        };
        var want = try runMultiAggregate(gpa, a, args);
        defer want.deinit(gpa);
        for ([_]usize{ 1, 2, 4, 8, 16 }) |j| for ([_]usize{ 0, 64, 4096 }) |batch| {
            var window: ReleasingScanWindow = .{ .bytes = try a.dupe(u8, bytes), .meta = &meta };
            const inputs = [_]Input{window.input(bytes)};
            args.inputs = &inputs;
            args.parallelism = j;
            args.decode_batch_rows = batch;
            args.encoded = true;
            args.start_reads = .{
                .ctx = &window,
                .start = ReleasingScanWindow.start,
                .abandon = ReleasingScanWindow.abandon,
            };
            var got = try runMultiAggregate(gpa, a, args);
            defer got.deinit(gpa);
            try testing.expectEqualDeep(want.output.group_rows, got.output.group_rows);
            try testing.expectEqual(@as(usize, 16), window.releases.load(.monotonic));
            try testing.expectEqual(@as(usize, 0), window.abandoned);
        };
        args.inputs = &mapped;
    }
}

test "duplicate integer counts keep levels-only rows through releasing readers" {
    const Batch = @import("../filter/eval.zig").Batch;
    const gpa = testing.allocator;
    var arena = std.heap.ArenaAllocator.init(gpa);
    defer arena.deinit();
    const a = arena.allocator();
    inline for (.{ i32, i64 }) |T| {
        const phys: schema.Type = if (T == i32) .INT32 else .INT64;
        var sink: @import("../writer/streaming.zig").MemorySink = .{ .arena = a };
        var file = try consumer.AggregateFile.init(a, sink.sink());
        defer file.deinit();
        var expected: [3]u64 = @splat(0);
        for (0..16) |rg| {
            var values: [193]T = undefined;
            var f: [193]i64 = undefined;
            var defs: [193]u32 = undefined;
            for (&values, &f, &defs, 0..) |*v, *fv, *d, r| {
                v.* = @intCast(@as(i64, @intCast(r * 137 + rg * 73)) - 15000);
                fv.* = @intCast((r + rg) % 13);
                d.* = @intFromBool(r % 11 != 0 and rg != 15);
                if (d.* != 0) {
                    expected[0] += 1;
                    if (fv.* < 7) expected[1] += 1;
                }
            }
            try file.append(&.{
                .{ .name = "v", .parquet_type = phys, .nullable = true, .col = @unionInit(Batch.Column, @typeName(T), .{
                    .values = &values,
                    .def_levels = &defs,
                    .max_def = 1,
                }) },
                .{ .name = "f", .parquet_type = .INT64, .nullable = false, .col = .{ .i64 = .{ .values = &f } } },
            }, values.len, .SNAPPY, .{}, .{});
        }
        _ = try file.finish(&.{
            .{
                .name = "v",
                .parquet_type = phys,
                .nullable = true,
                .col = @unionInit(Batch.Column, @typeName(T), .{ .values = &.{} }),
            },
            .{ .name = "f", .parquet_type = .INT64, .nullable = false, .col = .{ .i64 = .{ .values = &.{} } } },
        });
        const bytes = sink.buffer.items;
        const meta = try metadata.open(a, bytes);
        for (meta.row_groups.items) |*rg| for (rg.columns.items) |*c| {
            c.meta_data.?.statistics = null;
        };
        for ([_]?[]const u8{ null, "f<7", "f<0" }, expected) |filter, want| {
            for ([_]usize{ 1, 2, 4, 8, 16 }) |j| for ([_]usize{ 0, 64 }) |batch| {
                var window: ReleasingScanWindow = .{ .bytes = try a.dupe(u8, bytes), .meta = &meta };
                const inputs = [_]Input{window.input(bytes)};
                var got = try runMultiAggregate(gpa, a, .{
                    .inputs = &inputs,
                    .metas = (&meta)[0..1],
                    .filter = filter,
                    .aggregate = "count(v) AS a,count(v) AS b",
                    .parallelism = j,
                    .decode_batch_rows = batch,
                    .scan_all = true,
                    .min_bytes_per_worker = 1,
                    .start_reads = .{
                        .ctx = &window,
                        .start = ReleasingScanWindow.start,
                        .abandon = ReleasingScanWindow.abandon,
                    },
                });
                defer got.deinit(gpa);
                try testing.expectEqual(want, got.state.accumulators[0].count);
                try testing.expectEqual(want, got.state.accumulators[1].count);
                try testing.expectEqual(@as(usize, 16), window.releases.load(.monotonic));
                try testing.expectEqual(@as(usize, 0), window.abandoned);
            };
        }
    }
}





test "packed composite count survives releasing windows and late dictionary refusal" {
    const gpa = testing.allocator;
    var held = std.heap.ArenaAllocator.init(gpa);
    defer held.deinit();
    const a = held.allocator();
    const path = "ci/fixtures/parquet/packed_count_indexed.parquet";
    const bytes = (try loadFixture(path)) orelse return error.SkipZigTest;
    defer gpa.free(bytes);
    const meta = try metadata.open(a, bytes);
    for (meta.row_groups.items) |rg| for (rg.columns.items) |col| {
        try testing.expect(col.offset_index_offset != null and col.column_index_offset != null);
    };
    const mapped = [_]Input{.{ .name = "packed.parquet", .bytes = bytes }};
    const Reader = struct {
        window: ReleasingScanWindow,
        fault: bool,
        changed: bool = false,
        fn probe(_: *anyopaque, _: usize, _: usize, _: u64, _: u64) anyerror!void {}
        fn ready(_: *anyopaque, _: usize) anyerror!void {}
        fn start(_: *anyopaque) anyerror!void {}
        fn planned(ctx: *anyopaque) void {
            const self: *@This() = @ptrCast(@alignCast(ctx));
            if (!self.fault or self.changed) return;
            self.changed = true;
            // The union owns the original dictionary. A late reader page now has another valid value.
            const cm = self.window.meta.row_groups.items[15].columns.items[0].meta_data.?;
            const offset: usize = @intCast(cm.dictionary_page_offset.?);
            var reader = thrift_mod.Reader.init(self.window.bytes[offset..]);
            _ = schema.PageHeader.read(&reader) catch unreachable;
            std.mem.writeInt(i64, self.window.bytes[offset + reader.pos ..][0..8], 987654321, .little);
        }
        fn abandon(ctx: *anyopaque) void {
            const self: *@This() = @ptrCast(@alignCast(ctx));
            ReleasingScanWindow.abandon(&self.window);
        }
    };
    for ([_]?[]const u8{ null, "f < 0" }) |filter| for ([_][]const u8{ "k,f", "k,b,f" }, 0..) |keys, shape| {
        var args: MultiAggArgs = .{
            .inputs = &mapped,
            .metas = (&meta)[0..1],
            .aggregate = "count(*) AS c",
            .group_by = keys,
            .filter = filter,
            .integer_records = false,
            .msd_records = false,
            .byte_records = false,
            .typed_answer = false,
            .plain_rules = false,
            .scan_all = false,
            .min_bytes_per_worker = 1,
            .max_memory = 256 << 20,
            .sketch_topk = .{ .cell_bits = 10, .admit_per_row = 1 },
            .result_order = .{ .keys = &.{.{ .column = shape + 2, .descending = true }}, .limit = 3, .offset = 2 },
        };
        var want = try runMultiAggregate(gpa, a, args);
        defer want.deinit(gpa);
        try want.output.applyResultOrder(gpa, args.result_order);
        // Direct preparation cannot silently retry: matching EXPLAIN must execute the packed owner.
        if (shape == 0) for (1..17) |j| for ([_]usize{ 0, 64 }) |batch| {
            var trial = std.heap.ArenaAllocator.init(gpa);
            defer trial.deinit();
            var mapped_args = args;
            mapped_args.parallelism = j;
            mapped_args.decode_batch_rows = batch;
            mapped_args.integer_records = true;
            mapped_args.integer_records_min_rows = 0;
            var budget: core_memory.SharedBudget = .{ .limit = args.max_memory };
            mapped_args.ledger = &budget;
            var taken: std.EnumSet(FastPath) = .{};
            mapped_args.taken = &taken;
            const explanation = try explainAggregate(gpa, trial.allocator(), mapped_args);
            try testing.expectEqual(.integer_count_top_h, explanation.record_path.?.layout);
            var got = try prepareAggregate(gpa, trial.allocator(), mapped_args, false);
            defer got.deinit(gpa);
            try testing.expect(taken.contains(.integer_records));
            try testing.expectEqual(ownedGroupOutputBytes(got.output), budget.used.load(.monotonic));
            try got.output.applyResultOrder(gpa, args.result_order);
            try testing.expectEqualDeep(want.output.group_rows, got.output.group_rows);
        };
        for (1..17) |j| for ([_]usize{ 0, 64 }) |batch| for ([_]bool{ false, true }) |fault| {
            var trial = std.heap.ArenaAllocator.init(gpa);
            defer trial.deinit();
            const ta = trial.allocator();
            var reader: Reader = .{ .window = .{ .bytes = try ta.dupe(u8, bytes), .meta = &meta }, .fault = fault };
            var input = reader.window.input(bytes);
            input.dict_wait = .{ .ctx = &reader, .wait = Reader.ready };
            var budget: core_memory.SharedBudget = .{ .limit = args.max_memory };
            args.ledger = &budget;
            args.inputs = (&input)[0..1];
            args.parallelism = j;
            args.decode_batch_rows = batch;
            args.integer_records = true;
            args.integer_records_min_rows = 0;
            args.start_reads = .{
                .ctx = &reader,
                .start = Reader.start,
                .planned = Reader.planned,
                .abandon = Reader.abandon,
            };
            args.dict_probe = .{ .ctx = &reader, .add = Reader.probe };
            const explanation = try explainAggregate(gpa, ta, args);
            try testing.expect(explanation.record_path != null);
            try testing.expectEqual(.integer_count_top_h, explanation.record_path.?.layout);
            var taken: std.EnumSet(FastPath) = .{};
            args.taken = &taken;
            if (fault) {
                var got = try runMultiAggregate(gpa, ta, args);
                defer got.deinit(gpa);
                try got.output.applyResultOrder(gpa, args.result_order);
                try testing.expectEqualDeep(want.output.group_rows, got.output.group_rows);
            } else {
                if (j == 16) spawn_util.injectFailureAfter(if (batch == 0) 0 else 3);
                defer spawn_util.resetFailure();
                var got = try prepareAggregate(gpa, ta, args, false);
                defer got.deinit(gpa);
                try testing.expect(taken.contains(.integer_records));
                try testing.expectEqual(ownedGroupOutputBytes(got.output), budget.used.load(.monotonic));
                if (j == 16) try testing.expect(spawn_util.failuresInjected() > 0);
                try got.output.applyResultOrder(gpa, args.result_order);
                try testing.expectEqualDeep(want.output.group_rows, got.output.group_rows);
            }
            try testing.expectEqual(@as(usize, 16), reader.window.releases.load(.monotonic));
            try testing.expectEqual(@as(usize, @intFromBool(fault)), reader.window.abandoned);
            try testing.expectEqual(fault, reader.changed);
            try testing.expect(budget.used.load(.monotonic) < 1 << 20);
        };
        if (shape == 1) {
            args.inputs = &mapped;
            args.start_reads = null;
            args.dict_probe = null;
            args.taken = null;
            args.ledger = null;
            args.parallelism = 1;
            args.decode_batch_rows = 64;
            const bad = try a.dupe(u8, bytes);
            for ([_]usize{ 0, 2 }) |rg| for ([_]usize{ 0, 2 }) |ci| {
                const cm = meta.row_groups.items[rg].columns.items[ci].meta_data.?;
                var header = thrift_mod.Reader.init(bad[@intCast(cm.data_page_offset)..]);
                const page = try schema.PageHeader.read(&header);
                const body = @as(usize, @intCast(cm.data_page_offset)) + header.pos;
                @memset(bad[body..][0..@min(12, @as(usize, @intCast(page.compressed_page_size)))], 0xff);
            };
            var failure_args = args;
            var expected_error: ?anyerror = null;
            var expected_page: ?u32 = null;
            for (1..17) |j| for ([_]usize{ 0, 64 }) |batch| for ([_]bool{ false, true }) |records| {
                var trial = std.heap.ArenaAllocator.init(gpa);
                defer trial.deinit();
                var reader: Reader = .{
                    .window = .{ .bytes = try trial.allocator().dupe(u8, bad), .meta = &meta },
                    .fault = false,
                };
                var input = reader.window.input(bad);
                input.dict_wait = .{ .ctx = &reader, .wait = Reader.ready };
                failure_args.inputs = (&input)[0..1];
                failure_args.dict_probe = .{ .ctx = &reader, .add = Reader.probe };
                failure_args.start_reads = .{ .ctx = &reader, .start = Reader.start, .abandon = Reader.abandon };
                var budget: core_memory.SharedBudget = .{ .limit = args.max_memory };
                failure_args.ledger = &budget;
                failure_args.parallelism = j;
                failure_args.decode_batch_rows = batch;
                failure_args.integer_records = records;
                var taken: std.EnumSet(FastPath) = .{};
                failure_args.taken = &taken;
                const err = failure: {
                    var unexpected = (if (filter == null)
                        prepareAggregate(gpa, trial.allocator(), failure_args, false)
                    else
                        runMultiAggregate(gpa, trial.allocator(), failure_args)) catch |e| break :failure e;
                    unexpected.deinit(gpa);
                    return error.TestUnexpectedResult;
                };
                try testing.expect(consumer.isDecodeError(err));
                if (records and filter == null) try testing.expect(taken.contains(.integer_records));
                const site = consumer.decodeFailure().?;
                try testing.expectEqual(@as(usize, 0), site.column);
                try testing.expect(site.rg == &meta.row_groups.items[0]);
                if (expected_error) |want_error| {
                    try testing.expectEqual(want_error, err);
                    try testing.expectEqual(expected_page, site.page);
                } else {
                    expected_error = err;
                    expected_page = site.page;
                }
                try testing.expectEqual(@as(usize, 0), budget.used.load(.monotonic));
            };
            var fixed: NoInPlaceResize = .{ .child = gpa };
            if (filter == null) try testing.checkAllAllocationFailures(fixed.allocator(), packedAllocationTrial, .{
                args, want.output.group_rows.?,
            });
        }
    };
}

