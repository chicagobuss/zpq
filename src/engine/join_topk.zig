//! The top-K of a join's rows (0.6.2). A join's plain rows run as their groups (`sql_parser.rowsAsGroups`), so
//! `SELECT ... FROM a JOIN b ... ORDER BY k LIMIT n` groups every matched row before it cuts to n. Here the cut is
//! found first: the `n`-th best `k` among the rows the join keeps, ranked by `k` alone (`rank.kthKey`), which row
//! groups whose statistics cannot beat it need never be read for. No row with a worse `k` can be in the answer, so the
//! plan gains `k <= cut` (or `>=`) as a conjunct and the group scan runs under it as it always does: ties, duplicate
//! rows and their order are the scan's own, not a second implementation's, and so are its refusals. Anything that
//! stops the ranking (a shape it does not take, an error, the budget) leaves the plan as bound.
const std = @import("std");
const schema = @import("../core/schema.zig");
const metadata = @import("../core/parquet/metadata.zig");
const statistics = @import("../core/parquet/statistics.zig");
const consumer = @import("../core/consumer.zig");
const scan = @import("../core/scan.zig");
const filter_ast = @import("../core/filter/ast.zig");
const core_memory = @import("../core/memory.zig");
const engine_rank = @import("rank.zig");
const engine_types = @import("types.zig");
const envDisables = @import("env.zig").envDisables;

const Context = engine_types.Context;
const QueryArgs = engine_types.QueryArgs;
const Timings = engine_types.Timings;
const RowSortKey = engine_rank.RowSortKey;
const SortCell = engine_rank.SortCell;

/// The most rows (OFFSET + LIMIT) a cut is found for: what each ranking worker's heap holds.
const max_rows = 1 << 16;

/// Narrow `args`' bound plan to the rows that can reach the answer, when the query is a join's rows cut by one
/// integer probe column. `inputs` are the probe's mapped files, `metas` their footers extended with the build columns.
pub fn narrow(
    ctx: Context,
    arena: std.mem.Allocator,
    args: QueryArgs,
    inputs: []const scan.Input,
    metas: []const schema.FileMetaData,
    ledger: *core_memory.SharedBudget,
) void {
    narrowTo(ctx, arena, args, inputs, metas, ledger) catch consumer.setDecodeFailure(null);
}

fn narrowTo(
    ctx: Context,
    arena: std.mem.Allocator,
    args: QueryArgs,
    inputs: []const scan.Input,
    metas: []const schema.FileMetaData,
    ledger: *core_memory.SharedBudget,
) !void {
    const order = args.result_order;
    const limit = order.limit orelse return;
    if (envDisables(ctx.env, "ZPQ_JOIN_TOPK") or args.scan_all or args.row_groups != null) return;
    if (order.repeat == null or order.post or order.having != null or order.keys.len != 1 or limit == 0) return;
    const h = std.math.add(u64, order.offset, limit) catch return;
    if (h > max_rows or metas.len == 0 or inputs.len != metas.len) return;
    // Mapped whole: a gated input releases a row group's bytes as it is read.
    for (inputs) |in| if (in.gate != null) return;
    // One binding serves every file, as the scan's does.
    for (metas[1..]) |*m| if (metadata.schemaMismatch(metas[0].schema.items, m.schema.items, .exact) != null) return;

    const j = (args.join orelse return).main.join;
    const bound = (try args.boundAggregate(arena, &metas[0])) orelse return;
    // A group-join folds each build row's probe rows without hashing them by value: there is nothing for a cut to save.
    if (j.group != null) return;
    const filter = bound.filter orelse return;
    const keys = bound.keys orelse return;
    // A computed key or filter can fail on a row, and a row left unread would answer where the scan raises.
    for (keys) |k| if (k != .col_ref) return;
    if (hasExpr(filter)) return;
    const by = order.keys[0];
    if (by.column >= keys.len) return;
    const col = keys[by.column].col_ref;
    // A bare INTEGER, DATE, TIME or TIMESTAMP column of the probe itself: the one kind whose cut is a plain bound.
    if (col.col_idx >= j.virtual_base or col.expr_type != .i64 or col.dec != null) return;
    const elem = metadata.leafSchemaElement(&metas[0], col.col_idx) orelse return;
    const physical = elem.type orelse return;
    const unsigned = schema.isUnsignedInt(elem);
    if ((physical != .INT32 and physical != .INT64) or !(unsigned or statistics.signedIntOrder(elem))) return;
    var walk: schema.LeafIterator = undefined;
    if ((metas[0].leafAt(col.col_idx, &walk) orelse return).max_rep != 0) return;

    const key: RowSortKey = .{
        .spec = .{ .passthrough = .{ .ci = col.col_idx } },
        .descending = by.descending,
        .unsigned = unsigned,
        .nulls_first = by.nulls_first,
    };
    // What the filter and the key read; the probe's own leaves are the ones with chunks.
    const fetch = try arena.alloc(bool, j.virtual_base);
    @memset(fetch, false);
    var read: std.ArrayList(usize) = .empty;
    try filter.collectColumns(&read, arena);
    for (read.items) |ci| if (ci < fetch.len) {
        fetch[ci] = true;
    };
    fetch[col.col_idx] = true;

    const items = try engine_rank.rankRowGroups(arena, inputs, metas, key, true);
    // One row group has nothing to be passed over for.
    if (items.len < 2) return;
    var t: Timings = .{};
    const room = @min(engine_rank.topnLimit(ctx, args, inputs), args.max_memory -| ledger.used.load(.monotonic));
    const kth = try engine_rank.kthKey(
        ctx,
        arena,
        args,
        inputs,
        metas,
        filter,
        fetch,
        key,
        @intCast(h),
        items,
        room,
        true,
        &t,
    );
    const cut = (try cutFilter(arena, col.col_idx, physical, unsigned, key, kth orelse return)) orelse return;
    try args.plan_cache.?.narrow(arena, cut);
}

/// Whether some leaf of `f` compares expressions, which can fail on a row's values.
fn hasExpr(f: filter_ast.Filter) bool {
    return switch (f) {
        .expr => true,
        .and_filter, .or_filter => |c| for (c.children) |k| {
            if (hasExpr(k)) break true;
        } else false,
        .lookup => |l| if (l.residual) |r| hasExpr(r.filter) else false,
        else => false,
    };
}

/// The rows that rank no later than `kth` under `key`: `<=` ascending, `>=` descending, and a NULL where NULLs rank
/// first. Null when that is every row (`kth` NULL, which ranks last).
fn cutFilter(
    arena: std.mem.Allocator,
    ci: usize,
    physical: schema.Type,
    unsigned: bool,
    key: RowSortKey,
    kth: SortCell,
) !?filter_ast.Filter {
    const is_null: filter_ast.Filter = .{ .null_check = .{ .col_idx = ci, .is_not = false } };
    const at = switch (kth) {
        .int => |v| v,
        .null => return if (key.nulls_first) is_null else null,
        else => return null,
    };
    const op: filter_ast.Operator = if (key.descending) .GtEq else .LtEq;
    const bound: filter_ast.Filter = if (unsigned)
        .{ .uint64 = .{ .col_idx = ci, .op = op, .value = @intCast(at) } }
    else if (physical == .INT32)
        .{ .int32 = .{ .col_idx = ci, .op = op, .value = @intCast(at) } }
    else
        .{ .int64 = .{ .col_idx = ci, .op = op, .value = @intCast(at) } };
    if (!key.nulls_first) return bound;
    const kids = try arena.alloc(filter_ast.Filter, 2);
    kids[0] = is_null;
    kids[1] = bound;
    return .{ .or_filter = .{ .children = kids } };
}

test "the cut keeps the rows that rank no later than the k-th, NULLs where they rank first" {
    var arena_state = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena_state.deinit();
    const a = arena_state.allocator();
    const asc: RowSortKey = .{ .spec = .{ .passthrough = .{ .ci = 3 } }, .descending = false, .unsigned = false };
    const f = (try cutFilter(a, 3, .INT64, false, asc, .{ .int = -7 })).?;
    try std.testing.expectEqual(filter_ast.Operator.LtEq, f.int64.op);
    try std.testing.expectEqual(@as(i64, -7), f.int64.value);
    var desc = asc;
    desc.descending = true;
    const g = (try cutFilter(a, 3, .INT32, false, desc, .{ .int = 5 })).?;
    try std.testing.expectEqual(filter_ast.Operator.GtEq, g.int32.op);
    // An unsigned column compares as unsigned, past 2^63.
    var big = asc;
    big.unsigned = true;
    const u = (try cutFilter(a, 3, .INT64, true, big, .{ .int = std.math.maxInt(u64) })).?;
    try std.testing.expectEqual(@as(u64, std.math.maxInt(u64)), u.uint64.value);
    // NULLs last: a NULL k-th leaves every row in. First: it is the NULLs alone, or them and the rows up to it.
    try std.testing.expect((try cutFilter(a, 3, .INT64, false, asc, .null)) == null);
    var first = asc;
    first.nulls_first = true;
    const nulls = (try cutFilter(a, 3, .INT64, false, first, .null)).?;
    try std.testing.expectEqual(@as(usize, 3), nulls.null_check.col_idx);
    const both = (try cutFilter(a, 3, .INT64, false, first, .{ .int = 9 })).?;
    try std.testing.expectEqual(@as(usize, 2), both.or_filter.children.len);
    try std.testing.expect(both.or_filter.children[0] == .null_check);
}

test "a filter that compares expressions is found through its AND and its lookup's residual" {
    const one: @import("../core/expr/ast.zig").Expr = .{ .literal = .{ .i64 = 1 } };
    const cmp: filter_ast.Filter = .{ .expr = .{ .op = .Eq, .left = &one, .right = &one } };
    const plain: filter_ast.Filter = .{ .int64 = .{ .col_idx = 0, .op = .Eq, .value = 1 } };
    try std.testing.expect(hasExpr(cmp));
    try std.testing.expect(!hasExpr(plain));
    const kids = [_]filter_ast.Filter{ plain, cmp };
    try std.testing.expect(hasExpr(.{ .and_filter = .{ .children = &kids } }));
    const two = [_]filter_ast.Filter{ plain, plain };
    try std.testing.expect(!hasExpr(.{ .or_filter = .{ .children = &two } }));
}
