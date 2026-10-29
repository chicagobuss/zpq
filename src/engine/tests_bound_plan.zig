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
    try std.testing.expectError(error.SchemaMismatch, args.bound(ar, &changed));
    try std.testing.expectEqual(@as(usize, 1), count);
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
