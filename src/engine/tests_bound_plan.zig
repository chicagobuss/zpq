const std = @import("std");
const types = @import("types.zig");
const plans = @import("bound_plan.zig");
const aggregate = @import("aggregate.zig");
const printing = @import("print.zig");
const projection = @import("projection.zig");
const fetch = @import("s3_fetch.zig");
const schema = @import("../core/schema.zig");
const metadata = @import("../core/parquet/metadata.zig");
const expr_parser = @import("../core/expr/parser.zig");
const filter_parser = @import("../core/filter/parser.zig");
const scan = @import("../core/scan.zig");
const expr = @import("../core/expr/ast.zig");
const Diag = @import("../core/diag.zig").Diag;
const fixture = "ci/fixtures/parquet/full_match.parquet";

// This test frontend binds original fragments at its boundary; poisoned engine fields prove no consumer rebinds.
const Frontend = struct {
    count: *usize,
    text: []const u8,
    filter: ?[]const u8 = null,
    group: ?[]const u8 = null,
    rows: bool = false,

    fn bind(raw: *const anyopaque, a: std.mem.Allocator, meta: *const schema.FileMetaData, diag: ?*Diag, _: plans.Options) !plans.Query {
        const self: *const Frontend = @ptrCast(@alignCast(raw));
        self.count.* += 1;
        const text = try a.dupe(u8, self.text);
        const f = if (self.filter) |s| try filter_parser.parse(a, try a.dupe(u8, s), meta, diag) else null;
        if (self.rows) {
            const items = try expr_parser.parseSelect(a, text, meta, diag);
            const e = try expr_parser.parseExprOnly(a, "x", meta, diag);
            const keys = try a.alloc(plans.RowSortKey, 1);
            keys[0] = try projection.rowSortKey(a, meta, e, false, 0, "x", diag);
            return .{ .projection = .{ .select_items = items, .filter = f, .row_keys = keys } };
        }
        const calls = try expr_parser.parseAggList(a, text, meta, diag);
        if (self.group) |g| {
            const items = try expr_parser.parseGroupByOf(a, try a.dupe(u8, g), meta, diag, &.{});
            const keys = try a.alloc(expr.Expr, items.len);
            for (items, keys) |item, *key| key.* = item.expr;
            var names = try scan.scan_binding.KeyNames.init(a, meta, items);
            try scan.scan_binding.nameUnaliasedAggs(a, calls, &names);
            const outputs = try scan.scan_binding.bindGroupOutputs(a, &names, calls, meta, .{
                .inputs = &.{},
                .aggregate = "",
                .diag = diag,
            });
            return .{ .aggregate = .{ .calls = calls, .keys = keys, .outputs = outputs, .filter = f } };
        }
        try scan.scan_binding.checkAggAliases(calls, diag);
        return .{ .aggregate = .{ .calls = calls, .filter = f } };
    }

    fn source(self: *const Frontend) plans.Source {
        return .{ .context = self, .kind = if (self.rows) .rows else if (self.group != null) .grouped else .scalar, .bind_fn = bind };
    }
};

test "bound frontend: each execution and EXPLAIN gets one invocation-local binding" {
    const a = std.testing.allocator;
    const ctx: types.Context = .{ .gpa = a, .env = .empty, .io = std.testing.io };
    var count: usize = 0;
    const frontend: Frontend = .{ .count = &count, .text = "count(*) AS n", .filter = "x > 0" };
    const args: types.QueryArgs = .{
        .inputs = &.{fixture},
        .plan_source = frontend.source(),
        .aggregate = "invalid aggregate",
        .filter = "invalid filter",
        .parallelism = 1,
    };
    for (1..3) |n| {
        var result = try aggregate.runQuery(ctx, args);
        defer result.deinit(a);
        try std.testing.expectEqual(n, count);
        try std.testing.expectEqualStrings("n", result.aggregate.output.aggs[0].alias);
    }
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const explanation = try aggregate.explainAggregate(ctx, arena.allocator(), args);
    try std.testing.expectEqual(@as(usize, 3), count);
    try std.testing.expectEqual(@as(usize, 1), explanation.conjuncts.len);
    try std.testing.expectEqualStrings("x", explanation.conjuncts[0].column);
}

test "bound frontend: projection and row ordering ignore legacy expression fields" {
    const a = std.testing.allocator;
    const ctx: types.Context = .{ .gpa = a, .env = .empty, .io = std.testing.io };
    var count: usize = 0;
    const frontend: Frontend = .{ .count = &count, .text = "x AS a", .filter = "x > 0", .rows = true };
    var expected_buf: [4096]u8 = undefined;
    var expected: std.Io.Writer = .fixed(&expected_buf);
    _ = try printing.runPrintSink(ctx, .{
        .inputs = &.{fixture},
        .select = frontend.text,
        .filter = frontend.filter,
        .result_order = .{ .rows = &.{.{ .expr = "x", .descending = false }} },
    }, .jsonl, 3, 1, 1, &expected);
    var got_buf: [4096]u8 = undefined;
    var got: std.Io.Writer = .fixed(&got_buf);
    _ = try printing.runPrintSink(ctx, .{
        .inputs = &.{fixture},
        .plan_source = frontend.source(),
        .select = "invalid select",
        .filter = "invalid filter",
        .result_order = .{ .rows = &.{.{ .expr = "invalid order", .descending = false }} },
    }, .jsonl, 3, 1, 1, &got);
    try std.testing.expectEqual(@as(usize, 1), count);
    try std.testing.expectEqualStrings(expected.buffered(), got.buffered());
}

test "bound frontend: S3 range planning and read cuts share one checked binding" {
    const a = std.testing.allocator;
    const bytes = try metadata.readFileSlice(fixture, a);
    defer a.free(bytes);
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const ar = arena.allocator();
    const meta = try metadata.open(ar, bytes);
    var count: usize = 0;
    const frontend: Frontend = .{ .count = &count, .text = "count(*) AS n", .filter = "x > 0", .group = "b" };
    var cache: plans.Cache = .{};
    const args = (types.QueryArgs{
        .inputs = &.{fixture},
        .plan_source = frontend.source(),
        .aggregate = "invalid aggregate",
        .group_by = "invalid keys",
        .filter = "invalid filter",
    }).withPlanCache(&cache);
    _ = try fetch.planRanges(ar, args, &.{meta});
    const bound = (try args.boundAggregate(ar, &meta)).?;
    try std.testing.expectEqual(@as(usize, 1), count);
    try std.testing.expectEqual(scan.CutShape.count, scan.cutShape(ar, &meta, .{
        .inputs = &.{},
        .aggregate = "invalid aggregate",
        .bound_aggregate = bound,
        .result_order = .{ .keys = &.{.{ .column = 1, .descending = true }}, .limit = 2 },
    }));
    var changed = meta;
    changed.schema.items = try ar.dupe(schema.SchemaElement, meta.schema.items);
    changed.schema.items[1].name = "different_column";
    changed.leaf_index = null;
    _ = try args.bound(ar, &changed);
    try std.testing.expectEqual(@as(usize, 2), count);
    try std.testing.expectError(error.SchemaMismatch, scan.explainAggregate(a, ar, .{
        .inputs = &.{ .{ .bytes = bytes, .name = fixture }, .{ .bytes = bytes, .name = fixture } },
        .metas = &.{ meta, changed },
        .aggregate = "invalid aggregate",
        .bound_aggregate = bound,
        .parallelism = 1,
    }));
}

test "bound frontend: result names outlive frontend storage and allocation failures release query state" {
    const Lifetime = struct {
        fn run(a: std.mem.Allocator) !void {
            var result: types.QueryResult = undefined;
            var count: usize = 0;
            {
                const text = try a.dupe(u8, "count(*) AS kept_name");
                defer a.free(text);
                const frontend: Frontend = .{ .count = &count, .text = text };
                result = try aggregate.runQuery(.{ .gpa = a, .env = .empty, .io = std.testing.io }, .{
                    .inputs = &.{fixture},
                    .plan_source = frontend.source(),
                    .parallelism = 1,
                });
            }
            defer result.deinit(a);
            try std.testing.expectEqualStrings("kept_name", result.aggregate.output.aggs[0].alias);
            try std.testing.expectEqual(@as(usize, 1), count);
        }
    };
    try Lifetime.run(std.testing.allocator);
    const bytes = try metadata.readFileSlice(fixture, std.testing.allocator);
    defer std.testing.allocator.free(bytes);
    var metadata_arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer metadata_arena.deinit();
    const meta = try metadata.open(metadata_arena.allocator(), bytes);
    const Check = struct {
        fn run(a: std.mem.Allocator, m: *const schema.FileMetaData) !void {
            var arena = std.heap.ArenaAllocator.init(a);
            defer arena.deinit();
            var count: usize = 0;
            const frontend: Frontend = .{ .count = &count, .text = "count(*) AS kept_name" };
            var cache: plans.Cache = .{};
            const query = try cache.get(frontend.source(), arena.allocator(), m, null, .{ .trust_stats = false, .nested_output = false });
            try std.testing.expectEqualStrings("kept_name", query.aggregate.calls[0].alias);
            try std.testing.expectEqual(@as(usize, 1), count);
        }
    };
    // Arena growth may resize in place depending on allocator addresses. Disable that optional path so every
    // child-allocation failure is reproducible, then verify both propagation and complete arena cleanup.
    var baseline = std.testing.FailingAllocator.init(std.testing.allocator, .{ .resize_fail_index = 0 });
    try Check.run(baseline.allocator(), &meta);
    try std.testing.expectEqual(baseline.allocated_bytes, baseline.freed_bytes);
    for (0..baseline.alloc_index) |fail_index| {
        var failing = std.testing.FailingAllocator.init(std.testing.allocator, .{
            .fail_index = fail_index,
            .resize_fail_index = 0,
        });
        try std.testing.expectError(error.OutOfMemory, Check.run(failing.allocator(), &meta));
        try std.testing.expect(failing.has_induced_failure);
        try std.testing.expectEqual(failing.allocated_bytes, failing.freed_bytes);
    }
}

test "bound frontend: compatible physical drift binds a new view without reusing column proofs" {
    const a = std.testing.allocator;
    const bytes = try metadata.readFileSlice(fixture, a);
    defer a.free(bytes);
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const ar = arena.allocator();
    const meta = try metadata.open(ar, bytes);
    var changed = meta;
    changed.schema.items = try ar.dupe(schema.SchemaElement, meta.schema.items);
    changed.leaf_index = null;
    const x = try metadata.resolveColumn(&meta, "x");
    var leaf_index: usize = 0;
    for (changed.schema.items) |*element| if (element.type != null) {
        if (leaf_index == x) {
            element.type = .INT64;
            break;
        }
        leaf_index += 1;
    };
    var count: usize = 0;
    const frontend: Frontend = .{ .count = &count, .text = "sum(x) AS s" };
    var cache: plans.Cache = .{};
    const options: plans.Options = .{ .trust_stats = false, .nested_output = false };
    const first = try cache.get(frontend.source(), ar, &meta, null, options);
    const second = try cache.get(frontend.source(), ar, &changed, null, options);
    try std.testing.expect(first != second);
    try std.testing.expectEqual(@as(usize, 2), count);
    try std.testing.expectEqual(second, try cache.get(frontend.source(), ar, &changed, null, options));
    try std.testing.expectEqual(first, try cache.get(frontend.source(), ar, &meta, null, options));
    try std.testing.expectEqual(@as(usize, 2), count);
}

test "syntax: SQL hands the engine owned nodes and separate aliases and output slots" {
    const sql = @import("../core/expr/sql_parser.zig");
    if (comptime !@hasDecl(sql, "parseSqlPlan")) return;
    const a = std.testing.allocator;
    const ctx: types.Context = .{ .gpa = a, .env = .empty, .io = std.testing.io };
    for ([_][]const u8{
        "SELECT sum(x) AS \"a a\", sum(x) AS \"b b\" FROM 'ci/fixtures/parquet/full_match.parquet'",
        "SELECT b AS flag, count(*) AS n FROM 'ci/fixtures/parquet/full_match.parquet' GROUP BY b HAVING sum(x) > 0 ORDER BY count(*) DESC LIMIT 2",
        "SELECT x AS value FROM 'ci/fixtures/parquet/full_match.parquet' WHERE NOT (x IN (1, NULL)) ORDER BY value DESC LIMIT 3",
        "SELECT b AS k, count(*) AS n FROM 'ci/fixtures/parquet/full_match.parquet' GROUP BY k ORDER BY n DESC",
        "SELECT b AS k, count(*) AS c, sum(n) AS a, sum(n) AS b, count(DISTINCT s) AS d FROM 'ci/fixtures/parquet/full_match.parquet' WHERE b < 0 GROUP BY b ORDER BY c DESC LIMIT 2",
        "SELECT CASE WHEN b > 0 THEN b ELSE NULL END AS k, count(n) AS n FROM 'ci/fixtures/parquet/full_match.parquet' GROUP BY 1 ORDER BY k",
        "SELECT count(*) AS n FROM 'ci/fixtures/parquet/full_match.parquet' HAVING sum(n) < 0",
    }) |text| {
        const original = try sql.parseSqlQuery(a, text);
        defer original.deinit(a);
        const parsed = try sql.parseSqlPlan(a, text);
        defer parsed.deinit(a);
        var want_bytes: [4096]u8 = undefined;
        var got_bytes: [4096]u8 = undefined;
        var want = std.Io.Writer.fixed(&want_bytes);
        var got = std.Io.Writer.fixed(&got_bytes);
        const args: types.QueryArgs = .{
            .inputs = &.{fixture},
            .aggregate = original.aggregate_str,
            .group_by = original.group_by_str,
            .filter = original.filter_str,
            .select = original.select_str,
            .select_cols = original.select_cols,
            .result_order = original.result_order,
            .parallelism = 1,
        };
        var bound = args;
        bound.plan_source = @import("syntax_plan.zig").source(parsed.syntax.?);
        bound.aggregate = if (original.is_aggregate) "poisoned aggregate" else null;
        bound.group_by = if (original.group_by_str != null) "poisoned keys" else null;
        bound.filter = "poisoned predicate";
        bound.select = if (original.is_aggregate) null else "poisoned projection";
        if (original.is_aggregate) {
            var expected = try aggregate.runQuery(ctx, args);
            defer expected.deinit(a);
            var actual = try aggregate.runQuery(ctx, bound);
            defer actual.deinit(a);
            try @import("text.zig").writeAggregateJsonValue(&want, expected.aggregate.output, expected.aggregate.text_kinds, expected.aggregate.output.rowCount());
            try @import("text.zig").writeAggregateJsonValue(&got, actual.aggregate.output, actual.aggregate.text_kinds, actual.aggregate.output.rowCount());
        } else {
            _ = try printing.runPrintSink(ctx, args, .jsonl, 3, 1, 1, &want);
            _ = try printing.runPrintSink(ctx, bound, .jsonl, 3, 1, 1, &got);
        }
        try std.testing.expectEqualStrings(want.buffered(), got.buffered());
    }
}

test "syntax: SQL result storage survives releasing the frontend syntax owner" {
    const sql = @import("../core/expr/sql_parser.zig");
    if (comptime !@hasDecl(sql, "parseSqlPlan")) return;
    const a = std.testing.allocator;
    var result: types.QueryResult = undefined;
    {
        const parsed = try sql.parseSqlPlan(a, "SELECT b AS key, min(s) AS value FROM 'ci/fixtures/parquet/full_match.parquet' GROUP BY b");
        defer parsed.deinit(a);
        result = try aggregate.runQuery(.{ .gpa = a, .io = std.testing.io, .env = .empty }, .{
            .inputs = &.{fixture},
            .parallelism = 1,
            .plan_source = @import("syntax_plan.zig").source(parsed.syntax.?),
        });
    }
    defer result.deinit(a);
    try std.testing.expectEqualStrings("key", result.aggregate.output.group_cols.?[0]);
    try std.testing.expectEqualStrings("value", result.aggregate.output.group_cols.?[1]);
    var bytes: [16384]u8 = undefined;
    var writer = std.Io.Writer.fixed(&bytes);
    try @import("text.zig").writeAggregateJsonValue(&writer, result.aggregate.output, result.aggregate.text_kinds, result.aggregate.output.rowCount());
    try std.testing.expect(writer.buffered().len > 0);
}

test "syntax: SQL flat predicate chains retain their length without consuming grouping depth" {
    const sql = @import("../core/expr/sql_parser.zig");
    if (comptime !@hasDecl(sql, "parseSqlPlan")) return;
    const a = std.testing.allocator;
    var text: std.Io.Writer.Allocating = .init(a);
    defer text.deinit();
    try text.writer.writeAll("SELECT count(*) AS n FROM 'ci/fixtures/parquet/full_match.parquet' WHERE b >= 0");
    for (0..128) |_| try text.writer.writeAll(" AND b >= 0");
    const parsed = try sql.parseSqlPlan(a, text.written());
    defer parsed.deinit(a);
    const ctx: types.Context = .{ .gpa = a, .io = std.testing.io, .env = .empty };
    var expected = try aggregate.runQuery(ctx, .{ .inputs = &.{fixture}, .aggregate = "count(*) AS n", .filter = "b >= 0", .parallelism = 1 });
    defer expected.deinit(a);
    var got = try aggregate.runQuery(ctx, .{ .inputs = &.{fixture}, .plan_source = @import("syntax_plan.zig").source(parsed.syntax.?), .parallelism = 1 });
    defer got.deinit(a);
    try std.testing.expectEqual(expected.aggregate.output.aggs[0].value.i, got.aggregate.output.aggs[0].value.i);
}

test "syntax: unsupported SQL modifiers and casts retain refusal instead of dropping clauses" {
    const sql = @import("../core/expr/sql_parser.zig");
    if (comptime !@hasDecl(sql, "parseSqlPlan")) return;
    const a = std.testing.allocator;
    try std.testing.expectError(error.TrailingTokens, sql.parseSqlPlan(a, "SELECT strlen(s) FILTER (WHERE b > 0) AS n FROM 'ci/fixtures/parquet/full_match.parquet'"));
    try std.testing.expectError(error.ExpectedRParen, sql.parseSqlPlan(a, "SELECT sum(n ORDER BY n) AS n FROM 'ci/fixtures/parquet/full_match.parquet'"));
    const parsed = try sql.parseSqlPlan(a, "SELECT sum(CAST(b AS BIGINT)) AS n FROM 'ci/fixtures/parquet/full_match.parquet'");
    defer parsed.deinit(a);
    try std.testing.expectError(error.UnknownFunction, aggregate.runQuery(.{ .gpa = a, .io = std.testing.io, .env = .empty }, .{
        .inputs = &.{fixture},
        .parallelism = 1,
        .plan_source = @import("syntax_plan.zig").source(parsed.syntax.?),
    }));
}
