const std = @import("std");
const testing = std.testing;
const schema = @import("../schema.zig");
const metadata = @import("../parquet/metadata.zig");
const expr_agg = @import("../expr/agg.zig");
const expr_ast = @import("../expr/ast.zig");
const expr_parser = @import("../expr/parser.zig");
const group_dependencies = @import("../expr/group_dependencies.zig");
const Diag = @import("../diag.zig").Diag;
const scan_types = @import("types.zig");
const Error = scan_types.Error;
const MultiAggArgs = scan_types.MultiAggArgs;
const BoundOutputColumn = scan_types.BoundOutputColumn;
const ColSource = scan_types.ColSource;
const keyTypeFromExpr = @import("key_decode.zig").keyTypeFromExpr;
const worker_contracts = @import("worker_contracts.zig");
const Worker = worker_contracts.Worker;
const decodeProjectionOrder = worker_contracts.decodeProjectionOrder;
const workerErrorRanks = worker_contracts.workerErrorRanks;
const loadFullMatchFixture = @import("test_support.zig").loadFullMatchFixture;

/// The names a query's GROUP BY keys go by, each worked out once on first use: a column key's label walks the
/// schema, and binding output columns asks for every key's names once per output column.
pub const KeyNames = struct {
    arena: std.mem.Allocator,
    meta: *const schema.FileMetaData,
    items: []const expr_ast.SelectItem,
    labels: []?[]const u8,
    sources: []?[]const u8,
    /// SQL names its result columns from the SELECT list; an unaliased expression key it does not select has no name
    /// to give and needs none.
    sql: bool = false,

    pub fn init(arena: std.mem.Allocator, meta: *const schema.FileMetaData, items: []const expr_ast.SelectItem) !KeyNames {
        const labels = try arena.alloc(?[]const u8, items.len);
        @memset(labels, null);
        const sources = try arena.alloc(?[]const u8, items.len);
        @memset(sources, null);
        return .{ .arena = arena, .meta = meta, .items = items, .labels = labels, .sources = sources };
    }

    /// Key `k`'s output label: its alias, else its column's label. A key that is neither needs an alias.
    fn label(self: *KeyNames, k: usize) Error![]const u8 {
        if (self.labels[k]) |l| return l;
        const item = self.items[k];
        const l = item.alias orelse switch (item.expr) {
            .col_ref => |ref| try leafLabel(self.arena, self.meta, ref.col_idx),
            else => if (self.sql) "" else return error.GroupKeyAliasRequired,
        };
        self.labels[k] = l;
        return l;
    }

    /// The label of the column a bare column key reads; `label(k)` for any other key, and for an unaliased column key,
    /// whose label already is its column's.
    fn source(self: *KeyNames, k: usize) Error![]const u8 {
        if (self.sources[k]) |src| return src;
        const item = self.items[k];
        const src = if (item.expr != .col_ref or item.alias == null)
            try self.label(k)
        else
            try leafLabel(self.arena, self.meta, item.expr.col_ref.col_idx);
        self.sources[k] = src;
        return src;
    }
};

fn splitColumnList(arena: std.mem.Allocator, csv: []const u8) ![]const []const u8 {
    var names: std.ArrayList([]const u8) = .empty;
    var it = std.mem.splitScalar(u8, csv, ',');
    while (it.next()) |part| {
        const name = std.mem.trim(u8, part, " \t\r\n");
        if (name.len > 0) try names.append(arena, name);
    }
    return names.items;
}

/// The GROUP BY output columns as written: the select list (SQL), else `column_order`, else every key then every
/// aggregate, each under its name.
fn groupOutputSpecs(
    arena: std.mem.Allocator,
    keys: *KeyNames,
    agg_calls: []const expr_agg.AggCall,
    select_cols: ?[]const expr_ast.SelectColumn,
    column_order: ?[]const u8,
) ![]const expr_ast.SelectColumn {
    if (select_cols) |cols| {
        if (cols.len > 0) return cols;
    }
    if (column_order) |order| {
        const names = try splitColumnList(arena, order);
        if (names.len == 0) return error.UnknownColumn;
        const specs = try arena.alloc(expr_ast.SelectColumn, names.len);
        for (names, specs) |name, *spec| spec.* = .parse(name);
        return specs;
    }
    const specs = try arena.alloc(expr_ast.SelectColumn, keys.items.len + agg_calls.len);
    for (specs[0..keys.items.len], 0..) |*spec, k| spec.* = .{ .expr = try keys.label(k) };
    for (specs[keys.items.len..], agg_calls) |*spec, call| spec.* = .{ .expr = call.alias };
    return specs;
}

pub fn dependentGroupKeys(
    arena: std.mem.Allocator,
    keys: []const expr_ast.Expr,
    outputs: []const BoundOutputColumn,
) !struct { keys: []const expr_ast.Expr, outputs: []const BoundOutputColumn } {
    var count: usize = 0;
    for (keys) |e| if (group_dependencies.dependency(e, keys) != null) {
        count += 1;
    };
    if (count == 0) return .{ .keys = keys, .outputs = outputs };
    const kept = try arena.alloc(expr_ast.Expr, keys.len - count);
    const map = try arena.alloc(usize, keys.len);
    const bound = try arena.dupe(BoundOutputColumn, outputs);
    var n: usize = 0;
    for (keys, 0..) |e, i| if (group_dependencies.dependency(e, keys) == null) {
        map[i] = n;
        kept[n] = e;
        n += 1;
    };
    for (bound) |*out| if (out.source == .key) {
        const old = out.source.key;
        if (group_dependencies.dependency(keys[old], keys)) |base| {
            out.derived = keys[old];
            out.source.key = map[base];
        } else out.source.key = map[old];
    };
    return .{ .keys = kept, .outputs = bound };
}

/// An unaliased aggregate is named after its function (`sum`), as it always has been, unless that name is taken by
/// another aggregate or a GROUP BY key; then it takes its call text (`sum(x)`), as DuckDB names it. So
/// `sum(x), sum(f)` gives `sum(x)` and `sum(f)`, while a lone `sum(x)` still prints as `sum`.
pub fn nameUnaliasedAggs(arena: std.mem.Allocator, agg_calls: []expr_agg.AggCall, keys: *KeyNames) Error!void {
    const taken = try arena.alloc(bool, agg_calls.len);
    for (agg_calls, taken, 0..) |call, *t, i| {
        t.* = false;
        if (call.default_name == null) continue;
        for (agg_calls, 0..) |other, j| {
            if (i != j and std.ascii.eqlIgnoreCase(call.alias, other.alias)) t.* = true;
        }
        for (0..keys.items.len) |k| {
            const label = try keys.label(k);
            const source = try keys.source(k);
            if (std.ascii.eqlIgnoreCase(call.alias, label) or std.ascii.eqlIgnoreCase(call.alias, source)) t.* = true;
        }
    }
    // Decided before renaming any, so one rename can't make another name look unique.
    for (agg_calls, taken) |*call, t| if (t) {
        call.alias = call.default_name.?;
    };
}

/// Reject two output columns sharing a name, compared case-insensitively: both would land under one output name.
/// Names the later of the pair. Every output that names its columns (aggregates, GROUP BY rows, `--select`) checks
/// through this one rule.
pub fn checkDistinctOutputNames(
    comptime T: type,
    items: []const T,
    comptime nameOf: fn (T) []const u8,
    diag: ?*Diag,
) error{DuplicateOutputColumn}!void {
    for (items, 0..) |item, i| for (items[0..i]) |prev| {
        if (std.ascii.eqlIgnoreCase(nameOf(item), nameOf(prev))) {
            Diag.setColumn(diag, nameOf(item));
            return error.DuplicateOutputColumn;
        }
    };
}

fn aggAlias(call: expr_agg.AggCall) []const u8 {
    return call.alias;
}

fn boundName(col: BoundOutputColumn) []const u8 {
    return col.name;
}

/// Reject two aggregates sharing an alias: both would land under one output name.
pub fn checkAggAliases(agg_calls: []const expr_agg.AggCall, diag: ?*Diag) Error!void {
    return checkDistinctOutputNames(expr_agg.AggCall, agg_calls, aggAlias, diag);
}

/// Bind the GROUP BY output columns and check their names up front, before any scanning: no aggregate alias twice,
/// no output name twice, and no name that matches both a key and an aggregate. A clash would otherwise surface as one
/// column silently showing another's values.
pub fn bindGroupOutputs(
    arena: std.mem.Allocator,
    keys: *KeyNames,
    agg_calls: []const expr_agg.AggCall,
    meta: *const schema.FileMetaData,
    args: MultiAggArgs,
) ![]const BoundOutputColumn {
    try checkAggAliases(agg_calls, args.diag);
    for (keys.items, 0..) |item, k| if (item.alias == null and item.expr == .col_ref) {
        _ = keys.label(k) catch |err| {
            if (err == error.DuplicateOutputColumn) {
                const segments = (try metadata.leafPathSegments(arena, meta, item.expr.col_ref.col_idx)).?;
                Diag.setColumn(args.diag, try metadata.quotePath(arena, segments));
            }
            return err;
        };
    };
    if (sqlBound(args.select_cols)) return bindSqlOutputs(arena, keys, agg_calls, args);
    const specs = try groupOutputSpecs(arena, keys, agg_calls, args.select_cols, args.column_order);
    const bound = try arena.alloc(BoundOutputColumn, specs.len);
    for (specs, bound) |spec, *out| {
        const src = try resolveOutputColumn(keys, agg_calls, spec, args.diag);
        out.* = .{ .source = src, .name = try groupOutputName(keys, spec, src) };
    }
    try checkDistinctOutputNames(BoundOutputColumn, bound, boundName, args.diag);
    return bound;
}

/// Refuse a HAVING literal its result column cannot compare with, before any row is read.
pub fn checkHavingLanes(
    arena: std.mem.Allocator,
    having: expr_ast.Having,
    outputs: ?[]const BoundOutputColumn,
    agg_calls: []const expr_agg.AggCall,
    keys: ?[]const expr_ast.Expr,
) !void {
    const result_program = @import("../result_program.zig");
    const Lane = result_program.Lane;
    const aggLane = struct {
        fn of(call: expr_agg.AggCall) Lane {
            return if (call.result == .bytes) .string else .number;
        }
    }.of;
    const lanes = if (outputs) |outs| blk: {
        const l = try arena.alloc(Lane, outs.len);
        for (outs, l) |o, *lane| lane.* = switch (o.source) {
            .key => |k| if (keyTypeFromExpr(keys.?[k]) == .string) .string else .number,
            .agg => |a| aggLane(agg_calls[a]),
        };
        break :blk l;
    } else blk: {
        const l = try arena.alloc(Lane, agg_calls.len);
        for (agg_calls, l) |call, *lane| lane.* = aggLane(call);
        break :blk l;
    };
    try result_program.checkHaving(having, lanes);
}

/// The cut a GROUP BY's order allows that reads its input twice, so a read policy can keep the input mapped for it:
/// a plain COUNT ranking first, descending (the sketch and census), a count(DISTINCT) likewise (its census), or a
/// LIMIT with no ORDER BY (first keys). Decided
/// from the query bound to `meta` exactly as the scan binds it, never from its text. A query that does not bind is
/// `.none` here; the scan reports why.
pub const CutShape = enum { none, count, count_distinct, first_keys };

pub fn cutShape(arena: std.mem.Allocator, meta: *const schema.FileMetaData, args: MultiAggArgs) CutShape {
    const order = args.result_order;
    const limit = order.limit orelse return .none;
    if (args.bound_aggregate) |bound| {
        if (bound.keys == null) return .none;
    } else if (args.group_by == null) return .none;
    if (order.keys.len == 0) return if (order.offset +| limit > 0) .first_keys else .none;
    if (!order.keys[0].descending) return .none;
    return countFirst(arena, meta, args) catch .none;
}

fn countFirst(arena: std.mem.Allocator, meta: *const schema.FileMetaData, args: MultiAggArgs) !CutShape {
    if (args.bound_aggregate) |bound| return firstAggregateCut(bound.calls, bound.outputs orelse return .none, args);
    const calls: []expr_agg.AggCall = if (args.aggregate.len == 0)
        &.{}
    else
        try expr_parser.parseAggList(arena, args.aggregate, meta, null);
    const items = try expr_parser.parseGroupByOf(arena, args.group_by.?, meta, null, args.select_cols orelse &.{});
    var keys = try KeyNames.init(arena, meta, items);
    keys.sql = sqlBound(args.select_cols);
    try nameUnaliasedAggs(arena, calls, &keys);
    var quiet = args;
    quiet.diag = null;
    const outputs = try bindGroupOutputs(arena, &keys, calls, meta, quiet);
    return firstAggregateCut(calls, outputs, args);
}

fn firstAggregateCut(calls: []const expr_agg.AggCall, outputs: []const BoundOutputColumn, args: MultiAggArgs) CutShape {
    const first = args.result_order.keys[0].column;
    if (first >= outputs.len) return .none;
    return switch (outputs[first].source) {
        .agg => |a| switch (calls[a].func) {
            .count => .count,
            .count_distinct => .count_distinct,
            else => .none,
        },
        .key => .none,
    };
}

/// Whether `select_cols` are a SQL select list, which binds by role (`SelectColumn.Role`) rather than by name.
pub fn sqlBound(select_cols: ?[]const expr_ast.SelectColumn) bool {
    const cols = select_cols orelse return false;
    for (cols) |col| if (col.role != .named) return true;
    return false;
}

/// Bind a SQL select list, then the hidden ORDER BY terms (`ResultOrder.hidden`) after it, to slots: each aggregate
/// item is the next aggregate call, in order (the hidden ones are the calls after the SELECT list's); any other item
/// is the GROUP BY key it equals as a bound expression (`Expr.eql`: `"r"."v"` and `r.v` are one key exactly when
/// they bind one column). Visible names: an alias, else a column key's label, else the item as written. Hidden
/// columns have no name and are dropped before any output sees them.
fn bindSqlOutputs(
    arena: std.mem.Allocator,
    keys: *KeyNames,
    agg_calls: []const expr_agg.AggCall,
    args: MultiAggArgs,
) ![]const BoundOutputColumn {
    const visible = args.select_cols.?;
    const hidden = args.result_order.hidden;
    const bound = try arena.alloc(BoundOutputColumn, visible.len + hidden.len);
    var next_agg: usize = 0;
    for (bound, 0..) |*out, i| {
        const spec = if (i < visible.len) visible[i] else hidden[i - visible.len];
        const source: ColSource = switch (spec.role) {
            .agg => blk: {
                if (next_agg >= agg_calls.len) return error.EmptyAggregate;
                next_agg += 1;
                break :blk .{ .agg = next_agg - 1 };
            },
            .key, .named => blk: {
                const e = try expr_parser.parseExprOnly(arena, spec.expr, keys.meta, args.diag);
                for (keys.items, 0..) |item, k| if (item.expr.eql(e)) break :blk .{ .key = k };
                Diag.setColumn(args.diag, exprName(spec));
                return error.ColumnMustBeGrouped;
            },
        };
        const name: []const u8 = if (i >= visible.len) "" else if (spec.alias != null) aliasName(spec) else switch (source) {
            .key => |k| if (keys.items[k].expr == .col_ref) try keys.label(k) else exprName(spec),
            .agg => exprName(spec),
        };
        out.* = .{ .source = source, .name = name };
    }
    try checkDistinctOutputNames(BoundOutputColumn, bound[0..visible.len], boundName, args.diag);
    return bound;
}

/// The name output column `spec` prints under: an `AS` alias it gives; else a GROUP BY key's label, however `spec`
/// spells the key (`SELECT "r"."v"` and `--group-by v` both print `r.v`, see `metadata.leafLabel`); else the
/// aggregate's name as `spec` gives it.
fn groupOutputName(keys: *KeyNames, spec: expr_ast.SelectColumn, src: ColSource) Error![]const u8 {
    return switch (src) {
        .key => |k| if (spec.alias != null) aliasName(spec) else keys.label(k),
        .agg => aliasName(spec),
    };
}

/// Which GROUP BY key or aggregate an output column names. A key matches by its output label or, for a bare column
/// key, its source column path (`r.key` for a nested leaf); an aggregate by its alias. A name matching both is an
/// error rather than a guess: with `--group-by 's AS k'` and `... AS s`, the key used to win and `s` silently showed
/// the key's values.
fn resolveOutputColumn(
    keys: *KeyNames,
    agg_calls: []const expr_agg.AggCall,
    spec: expr_ast.SelectColumn,
    diag: ?*Diag,
) Error!ColSource {
    const expr_name = exprName(spec);
    const alias_name = aliasName(spec);
    // A key by the item's expression first, then by its alias: in `g AS k ... GROUP BY k, g` the column is `g`,
    // named `k`, and a match on the alias alone would show the key `k` instead.
    var key: ?usize = null;
    for ([_][]const u8{ expr_name, alias_name }) |name| {
        for (0..keys.items.len) |k_idx| {
            if (std.ascii.eqlIgnoreCase(name, try keys.label(k_idx)) or std.ascii.eqlIgnoreCase(name, try keys.source(k_idx))) {
                key = k_idx;
                break;
            }
        }
        if (key != null) break;
    }
    var agg: ?usize = null;
    for (agg_calls, 0..) |call, a_idx| {
        if (std.ascii.eqlIgnoreCase(alias_name, call.alias) or std.ascii.eqlIgnoreCase(expr_name, call.alias)) {
            agg = a_idx;
            break;
        }
        // An unaliased SQL select item (`count(*)`) names its aggregate by call text, whatever the alias became.
        if (call.default_name) |text| if (std.ascii.eqlIgnoreCase(expr_name, text)) {
            agg = a_idx;
            break;
        };
    }
    if (key == null and agg == null) {
        // A select-list column (SQL `SELECT b ... GROUP BY b`) names a bare key by any name that binds its column, as
        // the key itself was bound; its label may be a path (`r.b`) or quoted path the select list does not spell.
        if (metadata.resolveColumn(keys.meta, expr_name) catch null) |leaf| {
            for (keys.items, 0..) |item, k_idx| if (item.expr == .col_ref and item.expr.col_ref.col_idx == leaf) {
                key = k_idx;
                break;
            };
        }
    }
    if (key != null and agg != null) {
        // Name the aggregate alias: it is the clashing name whichever side of `AS` matched, and the one to rename.
        Diag.setColumn(diag, agg_calls[agg.?].alias);
        return error.AmbiguousOutputColumn;
    }
    if (key) |k| return .{ .key = k };
    if (agg) |a| return .{ .agg = a };
    Diag.setColumn(diag, alias_name);
    return error.UnknownColumn;
}

/// The output column name for the `leaf_idx`-th primitive column: `metadata.leafLabel`, the name the row formats
/// print it under, so a nested leaf (`r.key`) is not labelled like a top-level column that shares its leaf name
/// (`key`) or its dotted path (a top-level `r.key`; the nested one is then `"r"."key"`).
///
/// `col_idx` counts LEAVES, but `meta.schema` is a flattened DFS including group nodes, so `leaf_idx + 1` is correct
/// only for files with no nested types. Used for output column NAMES; key framing must not depend on it.
fn leafLabel(arena: std.mem.Allocator, meta: *const schema.FileMetaData, leaf_idx: usize) Error![]const u8 {
    // Callers resolved `leaf_idx` from this same schema. Falling back to
    // another element would just print the wrong column name.
    return (try metadata.leafLabel(arena, meta, leaf_idx)) orelse
        std.debug.panic("leaf_idx {d} not in schema", .{leaf_idx});
}

/// The schema element for the `leaf_idx`-th primitive column, for the type an output column carries over from its
/// source. Names come from `leafLabel`; key framing must not depend on this.
pub fn leafSchemaElem(meta: *const schema.FileMetaData, leaf_idx: usize) schema.SchemaElement {
    // Callers resolved `leaf_idx` from this same schema.
    return metadata.leafSchemaElement(meta, leaf_idx) orelse
        std.debug.panic("leaf_idx {d} not in schema", .{leaf_idx});
}

/// The name an output column's expression gives: its text, unquoted unless it is a quoted path.
fn exprName(spec: expr_ast.SelectColumn) []const u8 {
    return stripQuotes(std.mem.trim(u8, spec.expr, " "));
}

/// The name an output column asks to print under: its alias, else its expression's name. An alias is the name as the
/// parser decoded it, spaces inside a quoted one included (`AS " n "` names ` n `); the flag form trims its own text.
fn aliasName(spec: expr_ast.SelectColumn) []const u8 {
    return spec.alias orelse exprName(spec);
}

/// A quoted path (`"a"."b"`, a nested leaf's label) is a name in its own right and keeps its quotes.
fn stripQuotes(name: []const u8) []const u8 {
    if (name.len >= 2 and name[0] == '"' and metadata.unquoteIdent(name).len == name.len) return name;
    if (name.len >= 2 and ((name[0] == '\'' and name[name.len - 1] == '\'') or (name[0] == '"' and name[name.len - 1] == '"'))) {
        return name[1 .. name.len - 1];
    }
    return name;
}

test "cutShape names the cut a GROUP BY order allows from the bound query, never its text" {
    const bytes = (try loadFullMatchFixture()) orelse return error.SkipZigTest;
    defer testing.allocator.free(bytes);
    var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_state.deinit();
    const a = arena_state.allocator();
    const meta = try metadata.open(a, bytes);
    const Col = expr_ast.SelectColumn;
    const desc1: []const expr_ast.OrderKey = &.{.{ .column = 1, .descending = true }};
    const Case = struct { aggregate: []const u8, select: []const Col, order: expr_ast.ResultOrder, want: CutShape };
    const cases = [_]Case{
        .{ .aggregate = "COUNT(*) AS n", .select = &.{ .{ .expr = "b", .role = .key }, .{ .expr = "COUNT(*)", .alias = "n", .role = .agg } }, .order = .{ .keys = desc1, .limit = 5 }, .want = .count },
        // A hidden ORDER BY count binds like a selected one.
        .{ .aggregate = "count(*) AS __h0", .select = &.{.{ .expr = "b", .role = .key }}, .order = .{ .keys = desc1, .limit = 5, .hidden = &.{.{ .expr = "count(*)", .role = .agg }} }, .want = .count },
        .{ .aggregate = "count(DISTINCT x) AS d", .select = &.{ .{ .expr = "b", .role = .key }, .{ .expr = "count(DISTINCT x)", .alias = "d", .role = .agg } }, .order = .{ .keys = desc1, .limit = 5 }, .want = .count_distinct },
        .{ .aggregate = "sum(x) AS s", .select = &.{ .{ .expr = "b", .role = .key }, .{ .expr = "sum(x)", .alias = "s", .role = .agg } }, .order = .{ .keys = desc1, .limit = 5 }, .want = .none },
        .{ .aggregate = "count(*) AS n", .select = &.{ .{ .expr = "b", .role = .key }, .{ .expr = "count(*)", .alias = "n", .role = .agg } }, .order = .{ .keys = &.{.{ .column = 1, .descending = false }}, .limit = 5 }, .want = .none },
        .{ .aggregate = "count(*) AS n", .select = &.{ .{ .expr = "b", .role = .key }, .{ .expr = "count(*)", .alias = "n", .role = .agg } }, .order = .{ .limit = 5 }, .want = .first_keys },
        .{ .aggregate = "count(*) AS n", .select = &.{ .{ .expr = "b", .role = .key }, .{ .expr = "count(*)", .alias = "n", .role = .agg } }, .order = .{ .limit = 0 }, .want = .none },
        // A query that does not bind steers nothing.
        .{ .aggregate = "count(*) AS n", .select = &.{ .{ .expr = "nope", .role = .key }, .{ .expr = "count(*)", .alias = "n", .role = .agg } }, .order = .{ .keys = desc1, .limit = 5 }, .want = .none },
    };
    for (cases) |case| {
        const got = cutShape(a, &meta, .{ .inputs = &.{}, .aggregate = case.aggregate, .group_by = "b", .select_cols = case.select, .result_order = case.order });
        try testing.expectEqual(case.want, got);
    }
}

test "regression: a decoded alias keeps its surrounding spaces; an expression's name is trimmed" {
    try std.testing.expectEqualStrings(" n ", aliasName(.{ .expr = "count(*)", .alias = " n " }));
    try std.testing.expectEqualStrings("x", aliasName(.{ .expr = "x", .alias = "x" }));
    try std.testing.expectEqualStrings("k", aliasName(.{ .expr = " k " }));
    // The flag form's `--column-order "k AS  n "` trims what it was given, as before.
    try std.testing.expectEqualStrings("n", aliasName(.parse(" k AS  n ")));
}

test "dependent group keys: allocation failures and original projection ranks" {
    var x: expr_ast.Expr = .{ .col_ref = .{ .col_idx = 5, .physical_type = .INT32, .expr_type = .i64 } };
    var y: expr_ast.Expr = .{ .col_ref = .{ .col_idx = 3, .physical_type = .INT32, .expr_type = .i64 } };
    const sum: expr_ast.Expr = .{ .binop = .{
        .op = .add,
        .left = &x,
        .right = &y,
        .result_type = .i64,
        .depth = 2,
    } };
    const keys = [_]expr_ast.Expr{ sum, x, y };
    const outputs = [_]BoundOutputColumn{
        .{ .source = .{ .key = 0 }, .name = "sum" },
        .{ .source = .{ .key = 1 }, .name = "x" },
        .{ .source = .{ .key = 2 }, .name = "y" },
    };
    const Check = struct {
        fn run(gpa: std.mem.Allocator, ks: []const expr_ast.Expr, outs: []const BoundOutputColumn) !void {
            var arena_state = std.heap.ArenaAllocator.init(gpa);
            defer arena_state.deinit();
            const a = arena_state.allocator();
            const result = try dependentGroupKeys(a, ks, outs);
            try testing.expectEqual(@as(usize, 2), result.keys.len);
            try testing.expect(result.outputs[0].derived != null);
            const before = try decodeProjectionOrder(a, 6, &.{}, ks, outs);
            const after = try decodeProjectionOrder(a, 6, &.{}, result.keys, result.outputs);
            try testing.expectEqualSlices(usize, before, after);
            // With no key output, a removed expression may have established a different order from bare keys.
            var w: Worker = undefined;
            w.agg_calls = &.{};
            w.group_by_keys = result.keys;
            w.error_group_keys = ks;
            w.error_outputs = &.{};
            const hidden_before = try decodeProjectionOrder(a, 6, &.{}, ks, &.{});
            const hidden_after = try workerErrorRanks(&w, a, 6);
            try testing.expectEqualSlices(usize, hidden_before, hidden_after);
        }
    };
    try testing.checkAllAllocationFailures(testing.allocator, Check.run, .{ &keys, &outputs });
}

test "bound plans steer the read cut without aggregate or group SQL" {
    const bytes = (try loadFullMatchFixture()) orelse return error.SkipZigTest;
    defer testing.allocator.free(bytes);
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const meta = try metadata.open(a, bytes);
    const calls = try expr_parser.parseAggList(a, "count(*) AS n", &meta, null);
    var bound: scan_types.BoundAggregate = .{
        .calls = calls,
        .keys = &.{},
        .outputs = &.{.{ .source = .{ .agg = 0 }, .name = "n" }},
        .filter = null,
    };
    var args: MultiAggArgs = .{
        .inputs = &.{},
        .aggregate = "this must never be parsed",
        .bound_aggregate = &bound,
        .result_order = .{ .keys = &.{.{ .column = 0, .descending = true }}, .limit = 2 },
    };
    try testing.expectEqual(CutShape.count, cutShape(a, &meta, args));
    bound.keys = null;
    try testing.expectEqual(CutShape.none, cutShape(a, &meta, args));
    args.result_order.keys = &.{};
    try testing.expectEqual(CutShape.none, cutShape(a, &meta, args));
    bound.keys = &.{};
    try testing.expectEqual(CutShape.first_keys, cutShape(a, &meta, args));
}
