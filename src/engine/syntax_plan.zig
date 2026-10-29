//! Bind schema-independent frontend syntax once for all engine consumers.
const std = @import("std");
const syntax = @import("../core/expr/syntax.zig");
const expr = @import("../core/expr/ast.zig");
const parser = @import("../core/expr/parser.zig");
const agg = @import("../core/expr/agg.zig");
const filters = @import("../core/filter/parser.zig");
const schema = @import("../core/schema.zig");
const metadata = @import("../core/parquet/metadata.zig");
const scan = @import("../core/scan.zig");
const plans = @import("bound_plan.zig");
const projection = @import("projection.zig");
const Diag = @import("../core/diag.zig").Diag;

pub fn source(query: *const syntax.Query) plans.Source {
    return .{ .context = query, .kind = if (!query.aggregate) .rows else if (query.keys != null) .grouped else .scalar, .bind_fn = bind };
}

fn columns(arena: std.mem.Allocator, specs: []const syntax.Column, meta: *const schema.FileMetaData, diag: ?*Diag) ![]expr.SelectColumn {
    const out = try arena.alloc(expr.SelectColumn, specs.len);
    for (specs, out) |spec, *col| col.* = .{
        .expr = spec.display,
        .alias = spec.alias,
        .role = spec.role,
        .bound_expr = if (spec.expr) |node| try parser.bindSyntax(arena, node, meta, diag) else null,
    };
    return out;
}

fn bind(raw: *const anyopaque, arena: std.mem.Allocator, meta: *const schema.FileMetaData, diag: ?*Diag, options: plans.Options) !plans.Query {
    const query: *const syntax.Query = @ptrCast(@alignCast(raw));
    if (!query.aggregate) {
        const cols = try arena.alloc(expr.SelectColumn, query.columns.len);
        for (query.columns, cols) |spec, *col| col.* = .{ .expr = spec.display, .alias = spec.alias };
        const kept_set = if (options.nested_output)
            try projection.nestedProjection(arena, meta, cols, metadata.leafCount(meta))
        else
            null;
        const items = if (kept_set == null) if (query.select) |select| blk: {
            const out = try arena.alloc(expr.SelectItem, select.len);
            for (select, out) |item, *o| o.* = .{ .expr = try parser.bindSyntax(arena, item.expr, meta, diag), .alias = item.alias };
            break :blk out;
        } else null else null;
        const filter = if (query.filter) |node| try filters.bindSyntax(arena, node, meta, diag) else null;
        const row_keys = try arena.alloc(plans.RowSortKey, query.row_keys.len);
        for (query.row_keys, row_keys, 0..) |key, *out, n|
            out.* = try projection.rowSortKey(arena, meta, try parser.bindSyntax(arena, key.expr, meta, diag), key.descending, n, key.display, diag);
        return .{ .projection = .{ .select_items = items, .filter = filter, .row_keys = row_keys, .kept_set = kept_set } };
    }
    const calls = try arena.alloc(agg.AggCall, query.calls.len);
    for (query.calls, calls) |call, *out| out.* = try parser.bindAggSyntax(arena, call, meta, diag);
    const filter = if (query.filter) |node| try filters.bindSyntax(arena, node, meta, diag) else null;
    if (query.keys) |key_syntax| {
        const items = try arena.alloc(expr.SelectItem, key_syntax.len);
        for (key_syntax, items) |key, *out| {
            // A real column wins over a SELECT alias, exactly as for original GROUP BY fragments.
            var node = key.expr;
            if (node == .column and std.mem.indexOfScalar(u8, node.column, '.') == null) {
                if (metadata.resolveColumn(meta, node.column)) |_| {} else |err| if (err == error.UnknownColumn) {
                    for (query.columns) |col| if (col.alias) |alias| {
                        if (!std.ascii.eqlIgnoreCase(alias, metadata.unquoteIdent(node.column))) continue;
                        if (col.role == .agg) return error.AggregateInGroupBy;
                        node = col.expr orelse return error.EmptyExpr;
                        break;
                    };
                }
            }
            out.* = .{ .expr = try parser.bindSyntax(arena, node, meta, diag), .alias = key.alias };
        }
        const keys = try arena.alloc(expr.Expr, items.len);
        for (items, keys) |item, *out| out.* = item.expr;
        var names = try scan.scan_binding.KeyNames.init(arena, meta, items);
        const cols = try columns(arena, query.columns, meta, diag);
        const hidden = try columns(arena, query.hidden, meta, diag);
        names.sql = scan.scan_binding.sqlBound(cols);
        try scan.scan_binding.nameUnaliasedAggs(arena, calls, &names);
        const outputs = try scan.scan_binding.bindGroupOutputs(arena, &names, calls, meta, .{
            .inputs = &.{},
            .aggregate = "",
            .select_cols = if (cols.len == 0) null else cols,
            .result_order = .{ .hidden = hidden },
            .diag = diag,
        });
        return .{ .aggregate = .{ .calls = calls, .keys = keys, .outputs = outputs, .filter = filter } };
    }
    try scan.scan_binding.checkAggAliases(calls, diag);
    return .{ .aggregate = .{ .calls = calls, .filter = filter } };
}
