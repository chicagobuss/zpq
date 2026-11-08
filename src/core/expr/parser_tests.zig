const std = @import("std");
const ast = @import("ast.zig");
const syntax = @import("syntax.zig");
const agg = @import("agg.zig");
const schema = @import("../schema.zig");
const filter_ast = @import("../filter/ast.zig");
const filter_parser = @import("../filter/parser.zig");
const Diag = @import("../diag.zig").Diag;
const ac = @import("parser.zig");
const parseSelect = ac.parseSelect;
const testing = std.testing;
const MAX_EXPR_DEPTH = ac.MAX_EXPR_DEPTH;
const bindSyntax = ac.bindSyntax;
const filter_eval_mod = @import("../filter/eval.zig");
const parseAggList = ac.parseAggList;
const parseExprOnly = ac.parseExprOnly;
const parseGroupBy = ac.parseGroupBy;
const parseGroupByOf = ac.parseGroupByOf;
const parseSelectSyntax = ac.parseSelectSyntax;
const test_fixtures = @import("../parquet/test_fixtures.zig");
const parseAggSyntax = ac.parseAggSyntax;
const unaryNode = ac.unaryNode;
const bindAggSyntax = ac.bindAggSyntax;

fn fakeFile(arena: std.mem.Allocator, names: []const []const u8, types: []const schema.Type) !schema.FileMetaData {
    // Build a minimal FileMetaData with a flat schema: one root group +
    // one leaf per name. Enough for findColumnIndex / getColumnSchema.
    var elems: std.ArrayList(schema.SchemaElement) = .empty;
    try elems.append(arena, .{
        .type = null,
        .type_length = null,
        .repetition_type = null,
        .name = "schema",
        .num_children = @intCast(names.len),
        .scale = null,
        .precision = null,
        .field_id = null,
    });
    for (names, types) |n, t| {
        try elems.append(arena, .{
            .type = t,
            .type_length = null,
            .repetition_type = .REQUIRED,
            .name = n,
            .num_children = 0,
            .scale = null,
            .precision = null,
            .field_id = null,
        });
    }
    return .{
        .version = 1,
        .schema = elems,
        .num_rows = 0,
        .row_groups = .empty,
        .created_by = null,
    };
}

test "parse: nesting past MAX_EXPR_DEPTH is rejected" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const file = try fakeFile(a, &.{}, &.{});

    // Parenthesis nesting trips the parser-recursion guard, before any AST node exists to measure.
    const n = MAX_EXPR_DEPTH + 1;
    const parens = try a.alloc(u8, n * 2 + 1);
    @memset(parens[0..n], '(');
    parens[n] = '1';
    @memset(parens[n + 1 ..], ')');
    try testing.expectError(error.ExpressionTooDeep, parseExprOnly(a, parens, &file, null));

    // A left-associative chain is parsed by a loop, not recursion, so only the tree-height check catches it.
    var chain: std.ArrayList(u8) = .empty;
    try chain.appendSlice(a, "1");
    for (0..n) |_| try chain.appendSlice(a, " + 1");
    try testing.expectError(error.ExpressionTooDeep, parseExprOnly(a, chain.items, &file, null));

    // A run of unary minus is self-recursive and hits the same guard.
    const minuses = try a.alloc(u8, n + 1);
    @memset(minuses[0..n], '-');
    minuses[n] = '1';
    try testing.expectError(error.ExpressionTooDeep, parseExprOnly(a, minuses, &file, null));

    // Just inside the cap still parses.
    const ok_parens = try a.alloc(u8, (MAX_EXPR_DEPTH - 1) * 2 + 1);
    @memset(ok_parens[0 .. MAX_EXPR_DEPTH - 1], '(');
    ok_parens[MAX_EXPR_DEPTH - 1] = '1';
    @memset(ok_parens[MAX_EXPR_DEPTH..], ')');
    _ = try parseExprOnly(a, ok_parens, &file, null);
}

test "parse: literal int" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const file = try fakeFile(a, &.{}, &.{});
    const e = try parseExprOnly(a, "42", &file, null);
    try testing.expectEqual(ast.Type.i64, e.typeOf());
    try testing.expectEqual(@as(i64, 42), e.literal.i64);
}

test "parse: literal float" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const file = try fakeFile(a, &.{}, &.{});
    // A literal with a decimal point is an exact DECIMAL, as SQL types it, with its double alongside.
    const e = try parseExprOnly(a, "3.14", &file, null);
    try testing.expectEqual(ast.Type.dec, e.typeOf());
    try testing.expectEqual(@as(i128, 314), e.literal.dec.unscaled);
    try testing.expectEqual(ast.Dec{ .precision = 3, .scale = 2 }, e.literal.dec.spec);
    try testing.expectEqual(@as(f64, 3.14), e.literal.dec.f);
}

test "parse: column ref resolves" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const file = try fakeFile(a, &.{ "x", "y" }, &.{ .INT32, .DOUBLE });
    const ex = try parseExprOnly(a, "x", &file, null);
    try testing.expectEqual(ast.Type.i64, ex.typeOf()); // INT32 promoted
    try testing.expectEqual(@as(usize, 0), ex.col_ref.col_idx);
    const ey = try parseExprOnly(a, "y", &file, null);
    try testing.expectEqual(ast.Type.f64, ey.typeOf());
    try testing.expectEqual(@as(usize, 1), ey.col_ref.col_idx);
}

test "parse: precedence — mul before add" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const file = try fakeFile(a, &.{ "x", "y", "z" }, &.{ .INT64, .INT64, .INT64 });
    const e = try parseExprOnly(a, "x + y * z", &file, null);
    // Should parse as `x + (y * z)`.
    try testing.expectEqual(ast.Op.add, e.binop.op);
    try testing.expectEqual(ast.Op.mul, e.binop.right.binop.op);
}

test "parse: parens override precedence" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const file = try fakeFile(a, &.{ "x", "y", "z" }, &.{ .INT64, .INT64, .INT64 });
    const e = try parseExprOnly(a, "(x + y) * z", &file, null);
    try testing.expectEqual(ast.Op.mul, e.binop.op);
    try testing.expectEqual(ast.Op.add, e.binop.left.binop.op);
}

test "parse: select list with alias" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const file = try fakeFile(a, &.{ "x", "y" }, &.{ .INT64, .INT64 });
    const items = try parseSelect(a, "x AS first, x + y AS sum", &file, null);
    try testing.expectEqual(@as(usize, 2), items.len);
    try testing.expectEqualStrings("first", items[0].alias.?);
    try testing.expectEqualStrings("sum", items[1].alias.?);
    try testing.expectEqual(ast.Op.add, items[1].expr.binop.op);
}

test "parse: type promotion in binop" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const file = try fakeFile(a, &.{ "i", "f" }, &.{ .INT64, .DOUBLE });
    const e = try parseExprOnly(a, "i + f", &file, null);
    try testing.expectEqual(ast.Type.f64, e.typeOf());
}

test "parse: unknown column errors" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const file = try fakeFile(a, &.{"x"}, &.{.INT64});
    try testing.expectError(error.UnknownColumn, parseExprOnly(a, "y + 1", &file, null));
}

test "parse: string literal" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const file = try fakeFile(a, &.{}, &.{});
    const e = try parseExprOnly(a, "'hello'", &file, null);
    try testing.expectEqual(ast.Type.str, e.typeOf());
    try testing.expectEqualStrings("hello", e.literal.str);
}

test "parse: string column ref" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const file = try fakeFile(a, &.{"name"}, &.{.BYTE_ARRAY});
    const e = try parseExprOnly(a, "name", &file, null);
    try testing.expectEqual(ast.Type.str, e.typeOf());
}

test "parse: || string concat" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const file = try fakeFile(a, &.{ "first", "last" }, &.{ .BYTE_ARRAY, .BYTE_ARRAY });
    const e = try parseExprOnly(a, "first || ' ' || last", &file, null);
    try testing.expectEqual(ast.Type.str, e.typeOf());
    try testing.expectEqual(ast.Op.concat, e.binop.op);
}

test "parse: type mismatch — string + number errors" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const file = try fakeFile(a, &.{"name"}, &.{.BYTE_ARRAY});
    try testing.expectError(error.TypeMismatch, parseExprOnly(a, "name + 1", &file, null));
}

test "parse: `||` over numbers, DECIMAL ones included, is a type error" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const file = try fakeFile(a, &.{"name"}, &.{.BYTE_ARRAY});
    // A decimal literal makes the operands DECIMAL; this reached `unreachable` in the DECIMAL result spec.
    try testing.expectError(error.TypeMismatch, parseExprOnly(a, "1.5 || 2", &file, null));
    try testing.expectError(error.TypeMismatch, parseExprOnly(a, "1 || 2.25", &file, null));
    try testing.expectError(error.TypeMismatch, parseExprOnly(a, "1 || 2", &file, null));
}

test "parse and evaluate: searched CASE takes the first TRUE WHEN and evaluates branches only where taken" {
    const expr_eval = @import("eval.zig");
    var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_state.deinit();
    const a = arena_state.allocator();
    const file = try fakeFile(a, &.{ "a", "s" }, &.{ .INT64, .BYTE_ARRAY });
    const av = [_]i64{ 1, 3, 4, 5 };
    const sv = [_][]const u8{ "xa", "b", "c", "xd" };
    const batch: filter_eval_mod.Batch = .{ .cols = &.{ .{ .i64 = .{ .values = &av } }, .{ .string = .{ .values = &sv } } }, .num_rows = 4 };
    const lookup = [_]?usize{ 0, 1 };

    // The row a = 3 divides by zero in the second branch, which the first WHEN takes it from.
    const num = try parseExprOnly(a, "CASE WHEN a = 3 THEN 0 WHEN a > 2 THEN 10 / (a - 3) ELSE -1 END", &file, null);
    const got = try expr_eval.evalExpr(a, &batch, &lookup, num);
    try testing.expectEqualSlices(i64, &.{ -1, 0, 10, 5 }, got.i64.values);
    try testing.expect(got.i64.def_levels == null);
    // Without ELSE a row no WHEN takes is NULL; a string literal joins a string column.
    const str = try parseExprOnly(a, "CASE WHEN s LIKE 'x%' AND (a < 2 OR a > 4) THEN s WHEN a = 3 THEN 'three' END", &file, null);
    const sg = try expr_eval.evalExpr(a, &batch, &lookup, str);
    try testing.expectEqualStrings("xa", sg.string.values[0]);
    try testing.expectEqualStrings("three", sg.string.values[1]);
    try testing.expectEqualSlices(u32, &.{ 1, 1, 0, 1 }, sg.string.def_levels.?);
    try testing.expect(str.mayBeNull());
    try testing.expect(!num.mayBeNull());
    // A DECIMAL and an integer branch meet in the DECIMAL holding both, the integer rescaled exactly; the same CASE
    // parsed twice is one bound expression.
    const mixed = try parseExprOnly(a, "CASE WHEN a > 3 THEN a * 1.5 ELSE 2 END", &file, null);
    try testing.expectEqual(ast.Type.dec, mixed.typeOf());
    const mg = try expr_eval.evalExpr(a, &batch, &lookup, mixed);
    try testing.expectEqualSlices(i128, &.{ 20, 20, 60, 75 }, mg.i128.values);
    try testing.expect(str.eql(try parseExprOnly(a, "case when s like 'x%' and (a < 2 or a > 4) then s when a = 3 then 'three' end", &file, null)));
    try testing.expect(!str.eql(mixed));

    try testing.expectError(error.UnsupportedCase, parseExprOnly(a, "CASE a WHEN 1 THEN 2 END", &file, null));
    try testing.expectError(error.UnsupportedCase, parseExprOnly(a, "CASE WHEN a > 1 THEN s ELSE 1 END", &file, null));
    try testing.expectError(error.UnsupportedCase, parseExprOnly(a, "CASE WHEN a > 1 THEN NULL END", &file, null));
}

test "parse: coalesce(col, default)" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const file = try fakeFile(a, &.{"x"}, &.{.INT64});
    const e = try parseExprOnly(a, "coalesce(x, 0)", &file, null);
    try testing.expectEqual(ast.Type.i64, e.typeOf());
    try testing.expectEqual(ast.Func.coalesce, e.call.func);
    try testing.expectEqual(@as(usize, 2), e.call.args.len);
}

test "parse: coalesce is case-insensitive" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const file = try fakeFile(a, &.{"x"}, &.{.INT64});
    const e = try parseExprOnly(a, "COALESCE(x, 0)", &file, null);
    try testing.expectEqual(ast.Func.coalesce, e.call.func);
}

test "parse: coalesce promotes int+float arg types to f64" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const file = try fakeFile(a, &.{ "i", "f" }, &.{ .INT64, .DOUBLE });
    const e = try parseExprOnly(a, "coalesce(i, f)", &file, null);
    try testing.expectEqual(ast.Type.f64, e.typeOf());
}

test "parse: coalesce arity error" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const file = try fakeFile(a, &.{"x"}, &.{.INT64});
    try testing.expectError(error.WrongArity, parseExprOnly(a, "coalesce(x)", &file, null));
}

test "parse: unknown function errors" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const file = try fakeFile(a, &.{"x"}, &.{.INT64});
    try testing.expectError(error.UnknownFunction, parseExprOnly(a, "abs(x)", &file, null));
}

test "parse: incomplete trailing operator fails" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const file = try fakeFile(a, &.{"x"}, &.{.INT64});
    try testing.expectError(error.UnexpectedEnd, parseExprOnly(a, "x +", &file, null));
}

test "parse: extra trailing token fails" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const file = try fakeFile(a, &.{"x"}, &.{.INT64});
    try testing.expectError(error.TrailingTokens, parseExprOnly(a, "x 1", &file, null));
}

test "agg: count(*) with default alias" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const file = try fakeFile(a, &.{}, &.{});
    const items = try parseAggList(a, "count(*)", &file, null);
    try testing.expectEqual(@as(usize, 1), items.len);
    try testing.expectEqual(agg.AggFunc.count, items[0].func);
    try testing.expectEqualStrings("count", items[0].alias);
    try testing.expect(items[0].arg == null);
    try testing.expect(items[0].where == null);
}

test "agg: count(DISTINCT col) and count_distinct(col)" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const file = try fakeFile(a, &.{"uid"}, &.{.INT64});
    const items1 = try parseAggList(a, "count(DISTINCT uid) AS u", &file, null);
    try testing.expectEqual(@as(usize, 1), items1.len);
    try testing.expectEqual(agg.AggFunc.count_distinct, items1[0].func);
    try testing.expectEqualStrings("u", items1[0].alias);

    const items2 = try parseAggList(a, "count_distinct(uid)", &file, null);
    try testing.expectEqual(@as(usize, 1), items2.len);
    try testing.expectEqual(agg.AggFunc.count_distinct, items2[0].func);
    try testing.expectEqualStrings("count_distinct", items2[0].alias);
}

test "agg: sum with column ref + alias" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const file = try fakeFile(a, &.{"cost"}, &.{.DOUBLE});
    const items = try parseAggList(a, "sum(cost) AS total", &file, null);
    try testing.expectEqual(@as(usize, 1), items.len);
    try testing.expectEqual(agg.AggFunc.sum, items[0].func);
    try testing.expectEqualStrings("total", items[0].alias);
    try testing.expectEqual(agg.ResultShape.f64, items[0].result);
}

test "agg: avg result shape is avg_f64 (split-output)" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const file = try fakeFile(a, &.{"cost"}, &.{.DOUBLE});
    const items = try parseAggList(a, "avg(cost) AS mean", &file, null);
    try testing.expectEqual(agg.ResultShape.avg_f64, items[0].result);
}

test "agg: FILTER (WHERE pred) attaches a predicate" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const file = try fakeFile(a, &.{ "service", "cost" }, &.{ .BYTE_ARRAY, .DOUBLE });
    const items = try parseAggList(a, "sum(cost) FILTER (WHERE service = ec2) AS ec2_cost", &file, null);
    try testing.expectEqual(@as(usize, 1), items.len);
    try testing.expect(items[0].where != null);
    try testing.expect(items[0].where.? == .string);
    try testing.expectEqualStrings("ec2_cost", items[0].alias);
}

test "agg: multiple FILTER aggs in one list" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const file = try fakeFile(a, &.{ "svc", "cost" }, &.{ .BYTE_ARRAY, .DOUBLE });
    const items = try parseAggList(
        a,
        "sum(cost) FILTER (WHERE svc = ec2) AS ec2, sum(cost) FILTER (WHERE svc = s3) AS s3, count(*) AS rows",
        &file,
        null,
    );
    try testing.expectEqual(@as(usize, 3), items.len);
    try testing.expectEqualStrings("ec2", items[0].alias);
    try testing.expectEqualStrings("s3", items[1].alias);
    try testing.expectEqualStrings("rows", items[2].alias);
    try testing.expect(items[0].where != null);
    try testing.expect(items[1].where != null);
    try testing.expect(items[2].where == null);
}

test "agg: star only valid in count" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const file = try fakeFile(a, &.{}, &.{});
    try testing.expectError(error.StarOnlyValidInCount, parseAggList(a, "sum(*)", &file, null));
}

test "agg: unknown agg function errors" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const file = try fakeFile(a, &.{"x"}, &.{.INT64});
    try testing.expectError(error.UnknownAggFunc, parseAggList(a, "median(x)", &file, null));
}

test "agg: case-insensitive keywords" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const file = try fakeFile(a, &.{ "service", "cost" }, &.{ .BYTE_ARRAY, .DOUBLE });
    const items = try parseAggList(a, "SUM(cost) filter (where service = ec2) as total", &file, null);
    try testing.expectEqual(@as(usize, 1), items.len);
    try testing.expectEqualStrings("total", items[0].alias);
    try testing.expect(items[0].where != null);
}

test "agg: double-quoted column identifier resolves like bare" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const file = try fakeFile(a, &.{ "service", "cost" }, &.{ .BYTE_ARRAY, .DOUBLE });
    // `"cost"` (SQL-standard quoting) must parse identically to bare `cost`.
    const items = try parseAggList(a, "sum(\"cost\") AS c, max(\"cost\") AS m", &file, null);
    try testing.expectEqual(@as(usize, 2), items.len);
    try testing.expectEqualStrings("c", items[0].alias);
    try testing.expectEqualStrings("m", items[1].alias);
}

test "group-by: bare column and AS alias" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const file = try fakeFile(a, &.{ "region", "kind" }, &.{ .INT32, .BYTE_ARRAY });
    const items = try parseGroupBy(a, "region, kind AS k", &file, null);
    try testing.expectEqual(@as(usize, 2), items.len);
    try testing.expectEqual(@as(?[]const u8, null), items[0].alias);
    try testing.expectEqualStrings("k", items[1].alias.?);
}

test "parse: a UINT64 column may stand alone but is refused as an operand" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var file = try fakeFile(a, &.{ "u", "w" }, &.{ .INT64, .INT32 });
    file.schema.items[1].logical_type = .{ .INTEGER = .{ .bitWidth = 64, .isSigned = false } };
    file.schema.items[2].converted_type = .UINT_32;

    const bare = try parseSelect(a, "u AS b", &file, null);
    try testing.expect(bare[0].expr.col_ref.unsigned_64);
    _ = try parseAggList(a, "sum(u) AS s, max(u) AS m", &file, null);
    _ = try parseExprOnly(a, "w + 1", &file, null); // UINT32 zero-extends into i64 exactly

    for ([_][]const u8{ "u + 1", "1 * u", "-u", "coalesce(u, 0)", "(u) - 2" }) |src| {
        try testing.expectError(error.Unsigned64Operand, parseExprOnly(a, src, &file, null));
    }
    try testing.expectError(error.Unsigned64Operand, parseAggList(a, "sum(u + 1) AS s", &file, null));
    try testing.expectError(error.Unsigned64Operand, parseGroupBy(a, "u * 2 AS k", &file, null));
}

test "parse: bare names bind top-level columns, dotted paths nested leaves" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const file = try test_fixtures.sharedLeafNameMeta(a);

    const aggs = try parseAggList(
        a,
        "sum(key) AS s, sum(r.key) AS t, count(*) FILTER (WHERE key > 1005) AS n",
        &file,
        null,
    );
    try testing.expectEqual(@as(usize, 2), aggs[0].arg.?.col_ref.col_idx);
    try testing.expectEqual(@as(usize, 0), aggs[1].arg.?.col_ref.col_idx);

    const sel = try parseSelect(a, "key + r.key AS both, \"r.name\" AS n", &file, null);
    try testing.expectEqual(@as(usize, 2), sel[0].expr.binop.left.col_ref.col_idx);
    try testing.expectEqual(@as(usize, 0), sel[0].expr.binop.right.col_ref.col_idx);
    try testing.expectEqual(ast.Type.str, sel[1].expr.typeOf());

    const keys = try parseGroupBy(a, "key, r.key", &file, null);
    try testing.expectEqual(@as(usize, 2), keys[0].expr.col_ref.col_idx);
    try testing.expectEqual(@as(usize, 0), keys[1].expr.col_ref.col_idx);

    try testing.expectError(error.AmbiguousColumn, parseExprOnly(a, "x", &file, null));
    var diag: Diag = .{};
    try testing.expectError(error.AmbiguousColumn, parseAggList(a, "sum(x) AS s", &file, &diag));
    try testing.expectEqualStrings("x", diag.column.get());
    try testing.expect(diag.detail.ambiguous_column.parser == .expression);
    // A per-aggregate FILTER is the filter parser's.
    try testing.expectError(error.AmbiguousColumn, parseAggList(a, "count(*) FILTER (WHERE x = 1) AS n", &file, &diag));
    try testing.expect(diag.detail.ambiguous_column.parser == .filter);
    try testing.expectEqual(@as(usize, 5), (try parseExprOnly(a, "b.x", &file, null)).col_ref.col_idx);
    // A dot that does not continue an identifier is still an error, not part of the name.
    try testing.expectError(error.UnexpectedChar, parseExprOnly(a, "key.", &file, null));
}

test "parse: a quoted path binds the nested leaf a top-level column's dotted name hides" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var file = try test_fixtures.sharedLeafNameMeta(a);
    file.schema.items[4].name = "r.key"; // the top-level `key` becomes `r.key`, beside the group r's field key

    try testing.expectEqual(@as(usize, 2), (try parseExprOnly(a, "r.key", &file, null)).col_ref.col_idx);
    try testing.expectEqual(@as(usize, 2), (try parseExprOnly(a, "\"r.key\"", &file, null)).col_ref.col_idx);
    try testing.expectEqual(@as(usize, 0), (try parseExprOnly(a, "\"r\".\"key\"", &file, null)).col_ref.col_idx);
    const sel = try parseSelect(a, "\"r\".\"key\" + \"r.key\" AS s, \"r\".\"name\"", &file, null);
    try testing.expectEqual(@as(usize, 0), sel[0].expr.binop.left.col_ref.col_idx);
    try testing.expectEqual(@as(usize, 2), sel[0].expr.binop.right.col_ref.col_idx);
    try testing.expectEqual(@as(usize, 1), sel[1].expr.col_ref.col_idx);
    const keys = try parseGroupBy(a, "\"r\".\"key\", \"r.key\"", &file, null);
    try testing.expectEqual(@as(usize, 0), keys[0].expr.col_ref.col_idx);
    try testing.expectEqual(@as(usize, 2), keys[1].expr.col_ref.col_idx);
    try testing.expectError(error.UnknownColumn, parseExprOnly(a, "\"r\".\"nope\"", &file, null));
    try testing.expectError(error.UnterminatedString, parseExprOnly(a, "\"r\".\"key", &file, null));
}

test "parse: DECIMAL columns are exact, typed as DuckDB and Trino type their arithmetic" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var file = try fakeFile(a, &.{ "p", "d", "i", "b", "f" }, &.{ .INT64, .INT64, .INT32, .INT64, .DOUBLE });
    file.schema.items[1].logical_type = .{ .DECIMAL = .{ .precision = 15, .scale = 2 } };
    file.schema.items[2].logical_type = .{ .DECIMAL = .{ .precision = 15, .scale = 2 } };
    const Case = struct { src: []const u8, t: ast.Type, dec: ?ast.Dec = null };
    const cases = [_]Case{
        .{ .src = "p", .t = .dec, .dec = .{ .precision = 15, .scale = 2 } },
        .{ .src = "p * d", .t = .dec, .dec = .{ .precision = 30, .scale = 4 } },
        .{ .src = "1 - d", .t = .dec, .dec = .{ .precision = 16, .scale = 2 } },
        .{ .src = "p * (1 - d) * (1 + d)", .t = .dec, .dec = .{ .precision = 38, .scale = 6 } },
        .{ .src = "p + i", .t = .dec, .dec = .{ .precision = 16, .scale = 2 } },
        .{ .src = "p + b", .t = .dec, .dec = .{ .precision = 22, .scale = 2 } },
        .{ .src = "p * b", .t = .dec, .dec = .{ .precision = 34, .scale = 2 } },
        .{ .src = "p * 0.5", .t = .dec, .dec = .{ .precision = 17, .scale = 3 } },
        .{ .src = "-p", .t = .dec, .dec = .{ .precision = 15, .scale = 2 } },
        .{ .src = "coalesce(p, i)", .t = .dec, .dec = .{ .precision = 15, .scale = 2 } },
        .{ .src = "i * 1.5", .t = .dec, .dec = .{ .precision = 12, .scale = 1 } },
        // Division and DOUBLE operands leave exact arithmetic; integers alone stay integers.
        .{ .src = "p / d", .t = .f64 },
        .{ .src = "i / 1.0", .t = .f64 },
        .{ .src = "p + f", .t = .f64 },
        .{ .src = "i + b", .t = .i64 },
    };
    for (cases) |c| {
        const e = try parseExprOnly(a, c.src, &file, null);
        testing.expectEqual(c.t, e.typeOf()) catch |err| {
            std.debug.print("{s}: {s}\n", .{ c.src, @tagName(e.typeOf()) });
            return err;
        };
        if (c.dec) |want| try testing.expectEqual(want, e.decSpec().?);
    }
}

test "parse: DECIMAL limits — declared types, literal width, product scale" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var file = try fakeFile(a, &.{ "neg", "wide", "s30" }, &.{ .INT64, .FIXED_LEN_BYTE_ARRAY, .FIXED_LEN_BYTE_ARRAY });
    // Parquet allows no negative scale and no precision past 38: such a column is refused, not approximated.
    file.schema.items[1].logical_type = .{ .DECIMAL = .{ .precision = 10, .scale = -2 } };
    file.schema.items[2].logical_type = .{ .DECIMAL = .{ .precision = 39, .scale = 0 } };
    file.schema.items[3].logical_type = .{ .DECIMAL = .{ .precision = 38, .scale = 30 } };
    try testing.expectError(error.InvalidDecimalType, parseExprOnly(a, "neg", &file, null));
    try testing.expectError(error.InvalidDecimalType, parseExprOnly(a, "wide + 1", &file, null));
    // Scales add under `*`: 30 + 30 has no DECIMAL.
    try testing.expectError(error.DecimalScaleTooLarge, parseExprOnly(a, "s30 * s30", &file, null));
    _ = try parseExprOnly(a, "s30 * 1.25", &file, null);
    // 38 digits is still exact; a 39th makes the literal DOUBLE, as in DuckDB.
    const d38 = try parseExprOnly(a, "1234567890123456789012345678.9012345678", &file, null);
    try testing.expectEqual(ast.Dec{ .precision = 38, .scale = 10 }, d38.literal.dec.spec);
    const d39 = try parseExprOnly(a, "1234567890123456789012345678.90123456789", &file, null);
    try testing.expectEqual(ast.Type.f64, d39.typeOf());
    const lit = try parseExprOnly(a, "-0.05", &file, null);
    try testing.expectEqual(@as(i128, -5), lit.literal.dec.unscaled);
    try testing.expectEqual(ast.Dec{ .precision = 3, .scale = 2 }, lit.literal.dec.spec);
}

/// Fuzz target: arbitrary text as select, aggregate, group-by, a lone expression and a filter, against a schema of
/// every lane: each parses or fails with an error.
fn fuzzParsers(_: void, smith: *std.testing.Smith) anyerror!void {
    var buf: [1024]u8 = undefined;
    const text = buf[0..smith.slice(&buf)];
    var arena_state = std.heap.ArenaAllocator.init(std.heap.page_allocator);
    defer arena_state.deinit();
    const a = arena_state.allocator();
    const file = try fakeFile(
        a,
        &.{ "a", "b", "c", "s", "f", "fl", "u" },
        &.{ .INT32, .INT64, .DOUBLE, .BYTE_ARRAY, .BOOLEAN, .FLOAT, .FIXED_LEN_BYTE_ARRAY },
    );
    _ = parseSelect(a, text, &file, null) catch {};
    _ = parseAggList(a, text, &file, null) catch {};
    _ = parseGroupBy(a, text, &file, null) catch {};
    _ = parseExprOnly(a, text, &file, null) catch {};
    _ = filter_parser.parse(a, text, &file, null) catch {};
}

test "fuzz: select, aggregate, group-by and filter text parse or fail with an error, never panic" {
    try std.testing.fuzz({}, fuzzParsers, .{ .corpus = &.{
        "a + 1 AS x, s || 'z' AS t",
        "count(*) AS n, sum(b * 2) AS s, avg(c) FILTER (WHERE a > 3) AS v",
        "a, s AS k",
        "a > 5 AND s = 'x' OR c BETWEEN 1.5 AND 2 AND NOT f = true",
        "a IN (1, 2, 3) AND s NOT IN ('p', 'q') AND s LIKE 'ab%' AND b IS NOT NULL",
        "coalesce(a, 0) * -(b - 3) / 2",
    } });
}

test "regression: `%` in an expression is named as an operator zpq does not have" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const file = try fakeFile(a, &.{"id"}, &.{.INT64});
    try testing.expectError(error.ModuloNotSupported, parseExprOnly(a, "id % 2", &file, null));
    try testing.expectError(error.ModuloNotSupported, parseSelect(a, "id, id%2 AS m", &file, null));
    // In a string literal it is a character.
    _ = try parseExprOnly(a, "coalesce('a%', 'b')", &file, null);
}

test "regression: a STRUCT named where a value is wanted is refused, naming a leaf to select instead" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const file = try test_fixtures.sharedLeafNameMeta(a);
    var diag: Diag = .{};
    try testing.expectError(error.NestedColumnSelected, parseExprOnly(a, "r", &file, &diag));
    try testing.expectEqualStrings("r", diag.column.get());
    try testing.expectEqualStrings("r.key", diag.detail.nested_selected.example.get());
    try testing.expect(!diag.detail.nested_selected.repeated);
    try testing.expectError(error.NestedColumnSelected, parseSelect(a, "key, r AS q", &file, null));
    // Its leaves, and a name that matches nothing, keep their answers.
    try testing.expectEqual(@as(usize, 0), (try parseExprOnly(a, "r.key", &file, null)).col_ref.col_idx);
    try testing.expectError(error.UnknownColumn, parseExprOnly(a, "nope", &file, null));
}

test "regression: a doubled quote in a quoted alias is one quote of the output name" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const file = try fakeFile(a, &.{"id"}, &.{.INT64});
    const sel = try parseSelect(a, "id AS \"a\"\"b\", id AS \"plain\", id AS bare, id AS \"\"\"\"", &file, null);
    try testing.expectEqualStrings("a\"b", sel[0].alias.?);
    try testing.expectEqualStrings("plain", sel[1].alias.?);
    try testing.expectEqualStrings("bare", sel[2].alias.?);
    try testing.expectEqualStrings("\"", sel[3].alias.?);
    const aggs = try parseAggList(a, "count(*) AS \"c\"\"d\"", &file, null);
    try testing.expectEqualStrings("c\"d", aggs[0].alias);
    const keys = try parseGroupBy(a, "id AS \"k\"\"k\"", &file, null);
    try testing.expectEqualStrings("k\"k", keys[0].alias.?);
}

test "regression: GROUP BY binds an alias by its exact text, not one that differs by surrounding spaces" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const file = try fakeFile(a, &.{"id"}, &.{.INT64});
    const spaced = [_]ast.SelectColumn{.{ .expr = "id + 1", .alias = " x ", .role = .key }};
    try testing.expectError(error.UnknownColumn, parseGroupByOf(a, "x", &file, null, &spaced));
    const exact = [_]ast.SelectColumn{.{ .expr = "id + 1", .alias = "x", .role = .key }};
    const keys = try parseGroupByOf(a, "x", &file, null, &exact);
    try testing.expect(keys[0].expr == .binop);
}

test "parse: date/time functions bind DuckDB's argument types and carry their unit" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var file = try fakeFile(a, &.{ "i", "b", "tms", "s" }, &.{ .INT32, .INT64, .INT64, .BYTE_ARRAY });
    file.schema.items[3].logical_type = .{ .TIMESTAMP = .{ .isAdjustedToUTC = true, .unit = .{ .MILLIS = .{} } } };

    const d = try parseExprOnly(a, "make_date(i)", &file, null);
    try testing.expectEqual(ast.Temporal.date, d.temporal().?);
    const m = try parseExprOnly(a, "date_part('minute', epoch_ms(b * 1000))", &file, null);
    try testing.expectEqual(ast.DatePart.minute, m.call.part.?);
    try testing.expectEqual(@as(?ast.Temporal, null), m.temporal());
    try testing.expectEqual(ast.Temporal.ts_us, m.call.args[1].temporal().?);
    // A TIMESTAMP column carries its own unit; a plural part names the same part.
    const t = try parseExprOnly(a, "DATE_TRUNC('Hours', tms)", &file, null);
    try testing.expectEqual(ast.DatePart.hour, t.call.part.?);
    try testing.expectEqual(ast.Temporal.ts_ms, t.call.args[1].temporal().?);
    try testing.expect(!t.eql(try parseExprOnly(a, "date_trunc('minute', tms)", &file, null)));

    _ = try parseExprOnly(a, "make_date(coalesce(-i * 2 + 1, 0))", &file, null);
    for ([_][]const u8{
        "make_date(b)",                      "make_date(2147483648)", "make_date(make_date(i))", "make_date(i + b)",
        "make_date(date_part('year', tms))", "epoch_ms(tms)",         "date_trunc('minute', b)", "date_part(s, tms)",
    }) |src|
        try testing.expectError(error.TypeMismatch, parseExprOnly(a, src, &file, null));
    try testing.expectError(error.UnsupportedDatePart, parseExprOnly(a, "date_trunc('dow', tms)", &file, null));
    try testing.expectError(error.UnsupportedDatePart, parseExprOnly(a, "date_part('fortnight', tms)", &file, null));
    try testing.expectError(error.WrongArity, parseExprOnly(a, "date_part('minute')", &file, null));
}

test "parse: regexp_replace compiles its constant pattern, replacement and options once" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const file = try fakeFile(a, &.{ "s", "i" }, &.{ .BYTE_ARRAY, .INT64 });
    const e = try parseExprOnly(a, "REGEXP_REPLACE(s, '^https?://([^/]+)/', '\\1', 'g')", &file, null);
    try testing.expectEqual(ast.Type.str, e.typeOf());
    try testing.expect(e.call.replace.?.global);
    try testing.expectEqual(@as(u32, 2), e.call.replace.?.re.groups);
    try testing.expectError(error.TypeMismatch, parseExprOnly(a, "regexp_replace(i, 'a', 'b')", &file, null));
    try testing.expectError(error.TypeMismatch, parseExprOnly(a, "regexp_replace(s, s, 'b')", &file, null));
    try testing.expectError(error.WrongArity, parseExprOnly(a, "regexp_replace(s, 'a')", &file, null));
    try testing.expectError(error.InvalidRegex, parseExprOnly(a, "regexp_replace(s, 'a', '\\2')", &file, null));
}

test "parse: min/max of make_date(col) fold the column itself and answer as a DATE" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const file = try fakeFile(a, &.{ "i", "b" }, &.{ .INT32, .INT64 });
    const calls = try parseAggList(a, "MIN(make_date(i)), max(make_date(i)) AS hi, count(make_date(i))", &file, null);
    for (calls[0..2]) |c| {
        try testing.expect(c.as_date);
        try testing.expect(c.arg.? == .col_ref);
        try testing.expectEqual(@as(usize, 0), c.arg.?.col_ref.col_idx);
    }
    // Only min and max pass a value through; count still counts the expression's values.
    try testing.expect(!calls[2].as_date);
    try testing.expect(calls[2].arg.? == .call);
    // An expression under make_date is no column to fold.
    const shifted = try parseAggList(a, "min(make_date(i + 1))", &file, null);
    try testing.expect(!shifted[0].as_date and shifted[0].arg.? == .call);
}

test "syntax: original fragment grammar and shared expression semantics agree" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const file = try fakeFile(a, &.{ "x", "s" }, &.{ .INT32, .BYTE_ARRAY });
    for ([_][]const u8{
        "x + 2 * 3 AS n",                                  "-x AS n",       "coalesce(x, 0) AS n", "strlen(s) AS n",
        "CASE WHEN x IN (1, NULL) THEN x ELSE 0 END AS n", "s || 'q' AS n",
    }) |text| {
        const direct = try parseSelect(a, text, &file, null);
        const unbound = try parseSelectSyntax(a, text);
        try testing.expectEqual(direct.len, unbound.len);
        const got = try bindSyntax(a, unbound[0].expr, &file, null);
        try testing.expect(direct[0].expr.eql(got));
        try testing.expectEqualStrings(direct[0].alias.?, unbound[0].alias.?);
    }
    for ([_][]const u8{
        "sum(x + 2) AS a, sum(x + 2) AS b",                      "avg_state(x) AS a",
        "count(DISTINCT x) FILTER (WHERE s = 'O''Reilly') AS a", "min(make_date(x)) AS d",
    }) |text| {
        const direct = try parseAggList(a, text, &file, null);
        const unbound = try parseAggSyntax(a, text);
        for (direct, unbound) |want, node| {
            const got = try bindAggSyntax(a, node, &file, null);
            try testing.expectEqual(want.func, got.func);
            try testing.expectEqual(want.result, got.result);
            try testing.expectEqual(want.offset, got.offset);
            try testing.expectEqual(want.as_date, got.as_date);
            try testing.expectEqual(want.state, got.state);
            try testing.expectEqualStrings(want.alias, got.alias);
            if (want.arg) |arg| try testing.expect(arg.eql(got.arg.?));
            if (want.where) |f| try testing.expect(filter_ast.eql(f, got.where.?));
        }
    }
}

test "syntax: structured binding keeps type and depth refusals" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const file = try fakeFile(a, &.{ "x", "s" }, &.{ .INT32, .BYTE_ARRAY });
    try testing.expectError(error.BadNumber, bindSyntax(a, .{ .number = .{ .token = "-9223372036854775808", .floating = false } }, &file, null));
    var boundary: syntax.Expr = .{ .number = .{ .token = "1", .floating = false } };
    for (0..MAX_EXPR_DEPTH) |_| boundary = try unaryNode(a, boundary);
    // Literal negations fold, so the grammar's permitted unary depth need not produce a deep evaluation tree.
    _ = try bindSyntax(a, boundary, &file, null);
    const bad = try parseSelectSyntax(a, "s + 1");
    try testing.expectError(error.TypeMismatch, bindSyntax(a, bad[0].expr, &file, null));
    var deep: syntax.Expr = .{ .column = "x" };
    for (0..MAX_EXPR_DEPTH + 1) |_| deep = try unaryNode(a, deep);
    try testing.expectError(error.ExpressionTooDeep, bindSyntax(a, deep, &file, null));
}
