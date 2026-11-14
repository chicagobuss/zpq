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
const metadata = @import("../core/parquet/metadata.zig");
const statistics = @import("../core/parquet/statistics.zig");
const schema = @import("../core/schema.zig");
const lookup = @import("../core/join/lookup.zig");

pub const Query = syntax.Query;
pub const MergeColumn = struct { function: []const u8, name: []const u8 };

pub fn prepare(ctx: types.Context, args: types.QueryArgs, result_limit: usize, version: u32) ![]u8 {
    var ledger: memory.SharedBudget = .{ .limit = args.max_memory };
    var charged: memory.Charged = .{ .child = ctx.gpa, .budget = &ledger };
    return prepareOver(ctx, args, result_limit, version, charged.allocator(), &ledger) catch |err| return charged.refusal(err);
}

/// The query a fan-out worker runs, and what a coordinator merges its partial answers by.
pub const Mergeable = struct {
    query: syntax.Query,
    /// Per aggregate, in order: its merge function and the caller's output name. Aggregate `i` answers as `_a{i}`.
    aggregates: []const MergeColumn,
    /// The caller's group output names, in key order. Group key `i` answers as `_g{i}`.
    groups: []const []const u8,
};

/// The caller's aggregate, GROUP BY and filter as worker syntax: aggregates aliased `_aN` (AVG as its partial state),
/// group keys `_gN`. COUNT, SUM, MIN, MAX and AVG merge; anything else is `UnmergeableAggregate`.
pub fn mergeable(a: std.mem.Allocator, aggregate: []const u8, group_by: ?[]const u8, filter: ?[]const u8) !Mergeable {
    const calls = try parser.parseAggSyntax(a, aggregate);
    const keys = if (group_by) |group| try parser.parseSelectSyntax(a, group) else null;
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
    return .{
        .query = .{
            .aggregate = true,
            .calls = calls,
            .keys = keys,
            .filter = if (filter) |f| (if (f.len == 0) null else try filters.parseSyntax(a, f)) else null,
        },
        .aggregates = merge_columns,
        .groups = groups,
    };
}

/// The members a `partial_aggregate` answer adds, without braces: `plan_digest` (the first 16 bytes of SHA-256 over
/// the plan's JSON, in hex; workers that planned the query apart answer different digests), then `aggregates` and
/// `groups` as `Mergeable` holds them.
pub fn partialMembers(a: std.mem.Allocator, m: Mergeable) ![]const u8 {
    const plan = try std.json.Stringify.valueAlloc(a, m.query, .{});
    var digest: [32]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash(plan, &digest, .{});
    const json = try std.json.Stringify.valueAlloc(a, .{
        .plan_digest = std.fmt.bytesToHex(digest[0..16].*, .lower),
        .aggregates = m.aggregates,
        .groups = m.groups,
    }, .{});
    return json[1 .. json.len - 1];
}

fn prepareOver(ctx: types.Context, args: types.QueryArgs, result_limit: usize, version: u32, scratch: std.mem.Allocator, ledger: *memory.SharedBudget) ![]u8 {
    var state = std.heap.ArenaAllocator.init(scratch);
    defer state.deinit();
    const a = state.allocator();
    const m = try mergeable(a, args.aggregate orelse return error.EmptyAggregate, args.group_by, args.filter);
    const query = m.query;
    const merge_columns = m.aggregates;
    const groups = m.groups;
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

/// Only the join engine binds the name-based build/probe description.
pub fn decodeJoin(a: std.mem.Allocator, value: std.json.Value) !syntax.Join {
    try checkDepth(value, 0);
    return std.json.parseFromValueLeaky(syntax.Join, a, value, .{}) catch |err| {
        if (err == error.OutOfMemory) return err;
        return error.InvalidBoundPlan;
    };
}

pub const KeySpan = struct { low: i64, high: i64 };

pub fn decodeAlignment(a: std.mem.Allocator, value: std.json.Value) !types.AlignedJoin {
    try checkDepth(value, 0);
    const s = std.json.parseFromValueLeaky(types.AlignedJoin, a, value, .{}) catch |err| {
        if (err == error.OutOfMemory) return err;
        return error.AlignedJoinBadRange;
    };
    if (s.low == null or s.high == null or s.low.? > s.high.? or s.build.of != null or s.build.start > s.build.end)
        return error.AlignedJoinBadRange;
    return s;
}

/// Equality at a row-group boundary is common for an N:1 probe. Wider overlaps cannot prove alignment.
pub fn ordered(spans: []const KeySpan) !void {
    for (spans, 0..) |s, i| {
        if (s.low > s.high) return error.AlignedJoinUnordered;
        if (i > 0 and (s.low < spans[i - 1].high or s.high < spans[i - 1].high)) return error.AlignedJoinUnordered;
    }
}

pub fn keyIntervals(a: std.mem.Allocator, spans: []const KeySpan, workers: usize) ![]const KeySpan {
    try ordered(spans);
    if (workers == 0 or spans.len == 0) return error.AlignedJoinNoKeys;
    const n = @min(workers, spans.len);
    var out: std.ArrayList(KeySpan) = .empty;
    var low = spans[0].low;
    for (0..n) |i| {
        const high = spans[(i + 1) * spans.len / n - 1].high;
        if (high < low) continue;
        try out.append(a, .{ .low = low, .high = high });
        if (high == std.math.maxInt(i64)) break;
        low = high + 1;
    }
    return out.toOwnedSlice(a);
}

/// Includes both groups when a key is shared at their boundary. The row predicate owns the boundary exactly once.
pub fn overlapping(spans: []const KeySpan, interval: KeySpan) types.RowGroupRange {
    var start: usize = 0;
    var end = spans.len;
    while (start < end) {
        const mid = start + (end - start) / 2;
        if (spans[mid].high < interval.low) start = mid + 1 else end = mid;
    }
    const first = start;
    end = spans.len;
    while (start < end) {
        const mid = start + (end - start) / 2;
        if (spans[mid].low <= interval.high) start = mid + 1 else end = mid;
    }
    return .{ .start = @intCast(first), .end = @intCast(start) };
}

fn spansOf(a: std.mem.Allocator, meta: *const schema.FileMetaData, key: []const u8) ![]const KeySpan {
    const leaf = try metadata.resolveColumn(meta, key);
    const element = metadata.leafSchemaElement(meta, leaf) orelse return error.AlignedJoinKeyType;
    if ((lookup.KeyDomain.of(element) orelse return error.AlignedJoinKeyType) != .signed) return error.AlignedJoinKeyType;
    const spans = try a.alloc(KeySpan, meta.row_groups.items.len);
    for (meta.row_groups.items, spans) |*rg, *out| {
        const stats = statistics.chunkStats(rg, leaf, meta, .corroborated);
        if (stats.null_count == null or stats.null_count.? != 0) return error.AlignedJoinMissingStats;
        const bounds = stats.bounds orelse return error.AlignedJoinMissingStats;
        out.* = switch (element.type.?) {
            .INT32 => if (bounds.min.len == 4 and bounds.max.len == 4) .{ .low = std.mem.readInt(i32, bounds.min[0..4], .little), .high = std.mem.readInt(i32, bounds.max[0..4], .little) } else return error.AlignedJoinMissingStats,
            .INT64 => if (bounds.min.len == 8 and bounds.max.len == 8) .{ .low = std.mem.readInt(i64, bounds.min[0..8], .little), .high = std.mem.readInt(i64, bounds.max[0..8], .little) } else return error.AlignedJoinMissingStats,
            else => return error.AlignedJoinKeyType,
        };
    }
    try ordered(spans);
    return spans;
}

const KeyFile = struct { spans: []const KeySpan, snapshot: types.SourceSnapshot };
fn keyFile(ctx: types.Context, a: std.mem.Allocator, args: types.QueryArgs, path: []const u8, key: []const u8) !KeyFile {
    if (!std.mem.startsWith(u8, path, "s3://")) return error.AlignedJoinNeedsS3;
    var footer_args = args;
    footer_args.inputs = &.{path};
    footer_args.aggregate = null;
    footer_args.group_by = null;
    footer_args.filter = null;
    footer_args.plan_source = null;
    footer_args.join = null;
    footer_args.row_groups = null;
    footer_args.pin_input_generation = true;
    var opened = try input.openInputs(ctx, a, footer_args, .{ .limit = args.max_memory, .footer_only = true });
    defer opened.deinit();
    const metas = try input.materializeMetas(a, opened, args.diag, null);
    const snapshot = opened.snapshots[0] orelse return error.JoinBuildNoSnapshot;
    if (snapshot.etag.len == 0 or std.mem.startsWith(u8, snapshot.etag, "W/")) return error.JoinBuildNoSnapshot;
    return .{ .spans = try spansOf(a, &metas[0], key), .snapshot = snapshot };
}

/// The existing Lambda preparation seam carries footer-derived physical selections, never exchanged rows.
pub fn prepareAligned(ctx: types.Context, args: types.QueryArgs, join: syntax.Join, workers: usize, result_limit: usize) ![]u8 {
    var ledger: memory.SharedBudget = .{ .limit = args.max_memory };
    var charged: memory.Charged = .{ .child = ctx.gpa, .budget = &ledger };
    var state = std.heap.ArenaAllocator.init(charged.allocator());
    defer state.deinit();
    const a = state.allocator();
    var read_ctx = ctx;
    read_ctx.pool_gpa = ctx.pool_gpa orelse ctx.gpa;
    read_ctx.gpa = charged.allocator();
    _ = try mergeable(a, args.aggregate orelse return error.EmptyAggregate, args.group_by, args.filter);
    const facts = try a.alloc(KeyFile, args.inputs.len);
    var all: std.ArrayList(KeySpan) = .empty;
    const snapshots = try a.alloc(?types.SourceSnapshot, args.inputs.len);
    for (args.inputs, facts, snapshots) |path, *fact, *snapshot| {
        fact.* = try keyFile(read_ctx, a, args, path, join.fk);
        snapshot.* = fact.snapshot;
        try all.appendSlice(a, fact.spans);
    }
    std.mem.sort(KeySpan, all.items, {}, struct {
        fn less(_: void, x: KeySpan, y: KeySpan) bool {
            return x.low < y.low or (x.low == y.low and x.high < y.high);
        }
    }.less);
    const intervals = try keyIntervals(a, all.items, workers);
    const build_file = try keyFile(read_ctx, a, args, join.build.path, join.build.key);
    const Selection = struct { alignment: types.AlignedJoin, row_groups: []const [2]u32 };
    const selections = try a.alloc(Selection, intervals.len);
    for (intervals, selections) |interval, *selection| {
        const ranges = try a.alloc([2]u32, facts.len);
        for (facts, ranges) |fact, *range| {
            const r = overlapping(fact.spans, interval);
            range.* = .{ r.start, r.end };
        }
        selection.* = .{
            .alignment = .{ .low = interval.low, .high = interval.high, .build = overlapping(build_file.spans, interval), .build_snapshot = build_file.snapshot, .probes = snapshots },
            .row_groups = ranges,
        };
    }
    const json = try std.json.Stringify.valueAlloc(a, .{ .aligned_join_version = 1, .selections = selections }, .{});
    if (json.len > result_limit) return error.ResultTooLarge;
    return ctx.gpa.dupe(u8, json);
}

/// Adds a physical key restriction without rebuilding user SQL or changing the common merge contract.
pub fn intervalFilter(a: std.mem.Allocator, base: ?syntax.Predicate, key: []const u8, low: ?i64, high: ?i64) !?syntax.Predicate {
    if (low != null and high != null and low.? > high.?) return error.AlignedJoinBadRange;
    var items: std.ArrayList(syntax.Predicate) = .empty;
    if (base) |p| try items.append(a, p);
    if (low) |n| try items.append(a, .{ .comparison = .{ .operand = .{ .column = key }, .op = .GtEq, .value = try std.fmt.allocPrint(a, "{d}", .{n}) } });
    if (high) |n| try items.append(a, .{ .comparison = .{ .operand = .{ .column = key }, .op = .LtEq, .value = try std.fmt.allocPrint(a, "{d}", .{n}) } });
    return if (items.items.len == 0) null else .{ .and_ = try items.toOwnedSlice(a) };
}

test "fanout aligned: equal-key boundaries have one owner; overlap refuses; INT64 edge does not wrap" {
    const a = std.testing.allocator;
    const spans = [_]KeySpan{ .{ .low = -9, .high = 1 }, .{ .low = 1, .high = 4 }, .{ .low = 8, .high = std.math.maxInt(i64) } };
    const intervals = try keyIntervals(a, &spans, 3);
    defer a.free(intervals);
    try std.testing.expectEqual(@as(usize, 3), intervals.len);
    try std.testing.expectEqual(@as(i64, 2), intervals[1].low);
    try std.testing.expectEqual(@as(u32, 1), overlapping(&spans, intervals[0]).end - 1);
    try std.testing.expectEqual(@as(u32, 1), overlapping(&spans, intervals[1]).start);
    try std.testing.expectError(error.AlignedJoinUnordered, ordered(&.{ .{ .low = 0, .high = 4 }, .{ .low = 3, .high = 9 } }));
    const tied = try keyIntervals(a, &.{ .{ .low = 1, .high = 1 }, .{ .low = 1, .high = 1 } }, 2);
    defer a.free(tied);
    try std.testing.expectEqual(@as(usize, 1), tied.len);
}
