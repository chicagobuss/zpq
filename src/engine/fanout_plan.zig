//! Native fanout preparation: aliases and AVG partial states are values, never rebuilt SQL.
const std = @import("std");
const types = @import("types.zig");
const plans = @import("bound_plan.zig");
const syntax_plan = @import("syntax_plan.zig");
const syntax = @import("../core/expr/syntax.zig");
const parser = @import("../core/expr/parser.zig");
const filters = @import("../core/filter/parser.zig");
const scan = @import("../core/scan.zig");
const memory = @import("../core/memory.zig");
const input = @import("input.zig");

pub const Query = syntax.Query;
pub const MergeColumn = struct { function: []const u8, name: []const u8 };

pub fn prepare(ctx: types.Context, args: types.QueryArgs, result_limit: usize, version: u32) ![]u8 {
    var ledger: memory.SharedBudget = .{ .limit = args.max_memory };
    var charged: memory.Charged = .{ .child = ctx.gpa, .budget = &ledger };
    return prepareOver(ctx, args, result_limit, version, charged.allocator(), &ledger) catch |err| return charged.refusal(err);
}

fn prepareOver(ctx: types.Context, args: types.QueryArgs, result_limit: usize, version: u32, scratch: std.mem.Allocator, ledger: *memory.SharedBudget) ![]u8 {
    var state = std.heap.ArenaAllocator.init(scratch);
    defer state.deinit();
    const a = state.allocator();
    const original = args.aggregate orelse return error.EmptyAggregate;
    const calls = try parser.parseAggSyntax(a, original);
    const keys = if (args.group_by) |group| try parser.parseSelectSyntax(a, group) else null;
    const merge_columns = try a.alloc(MergeColumn, calls.len);
    for (calls, merge_columns, 0..) |*call, *out, i| {
        if (call.state) return error.UnmergeableAggregate;
        switch (call.func) {
            .count, .sum, .min, .max, .avg => {},
            else => return error.UnmergeableAggregate,
        }
        out.* = .{ .function = @tagName(call.func), .name = call.default_name orelse call.alias };
        call.state = call.func == .avg;
        call.alias = try std.fmt.allocPrint(a, "_a{d}", .{i});
        call.default_name = null;
    }
    const groups = try a.alloc([]const u8, if (keys) |k| k.len else 0);
    if (keys) |items| for (items, groups, 0..) |*key, *out, i| {
        out.* = key.alias orelse key.display;
        key.alias = try std.fmt.allocPrint(a, "_g{d}", .{i});
    };
    const query: syntax.Query = .{
        .aggregate = true,
        .calls = calls,
        .keys = keys,
        .filter = if (args.filter) |f| (if (f.len == 0) null else try filters.parseSyntax(a, f)) else null,
    };
    var cache: plans.Cache = .{};
    var bound_args = args;
    bound_args.plan_source = syntax_plan.source(&query);
    bound_args = bound_args.withPlanCache(&cache);
    var opened = try input.openInputs(ctx, a, bound_args, .{ .limit = args.max_memory, .footer_only = true });
    defer opened.deinit();
    const metas = try input.materializeMetas(a, opened, args.diag, null);
    if (metas.len == 0) return error.NoInputs;
    const bound = (try bound_args.boundAggregate(a, &metas[0])).?;
    // Binding/refusals share the execution planner, while footer-only opening starts no column fetch or scan.
    _ = try scan.explainAggregate(ctx.gpa, a, .{
        .inputs = opened.inputs,
        .metas = metas,
        .aggregate = "",
        .bound_aggregate = bound,
        .max_memory = args.max_memory,
        .scan_all = args.scan_all,
        .trust_stats = args.trust_stats,
        .encoded = args.encoded,
        .unordered = args.unordered,
        .ledger = ledger,
        .parallelism = args.parallelism,
        .diag = args.diag,
    });
    const counts = try a.alloc(u32, metas.len);
    for (metas, counts) |meta, *count| count.* = std.math.cast(u32, meta.row_groups.items.len) orelse return error.RowGroupOutOfRange;
    try opened.intact();
    const json = try std.json.Stringify.valueAlloc(a, .{
        .plan_version = version,
        .plan = query,
        .inputs = args.inputs,
        .row_group_counts = counts,
        .aggregates = merge_columns,
        .groups = groups,
    }, .{});
    if (json.len > result_limit) return error.ResultTooLarge;
    return ctx.gpa.dupe(u8, json);
}

/// The JSON request codec keeps an opaque value; only the engine owns native syntax types across Zig modules.
pub fn decode(arena: std.mem.Allocator, value: std.json.Value) !syntax.Query {
    try checkDepth(value, 0);
    const query = std.json.parseFromValueLeaky(syntax.Query, arena, value, .{}) catch |err| {
        if (err == error.OutOfMemory) return err;
        return error.InvalidBoundPlan;
    };
    if (!query.aggregate or query.columns.len > 0 or query.hidden.len > 0 or
        query.row_keys.len > 0 or query.select != null) return error.InvalidBoundPlan;
    return query;
}

fn checkDepth(value: std.json.Value, depth: usize) error{PlanTooDeep}!void {
    // Tagged nodes add object/array wrappers around the bounded expression and predicate trees.
    if (depth > 3 * filters.MAX_GROUP_DEPTH * 3 + 16) return error.PlanTooDeep;
    switch (value) {
        .array => |array| for (array.items) |v| try checkDepth(v, depth + 1),
        .object => |object| for (object.values()) |v| try checkDepth(v, depth + 1),
        else => {},
    }
}
