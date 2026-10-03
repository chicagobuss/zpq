//! Which columns an aggregate query reads, decided from the parsed query and the footers.
//!
//! Two consumers act on the same decisions and must never disagree: the S3 planner in `engine.zig` fetches exactly
//! the chunks a plan names into a compact buffer, and the scan workers in `scan.zig` decode exactly those chunks. Local
//! mmap hands workers the whole file, so a divergence there passes every local run and fails only over the network;
//! hence one policy, here, for both. A chunk the S3 planner leaves out on purpose has its offset poisoned, so a reader
//! that disagrees anyway fails with MissingChunkBytes instead of decoding stray bytes.

const std = @import("std");
const schema = @import("schema.zig");
const metadata = @import("parquet/metadata.zig");
const expr_ast = @import("expr/ast.zig");
const expr_agg = @import("expr/agg.zig");
const filter_ast = @import("filter/ast.zig");
const filter_prune = @import("filter/prune.zig");

/// How far a query may rely on file statistics. One mode rather than `--scan-all` and `--trust-stats` as two flags,
/// whose combination would otherwise need its own rule at every use: `--scan-all` wins.
pub const StatsUse = enum {
    /// `--scan-all`: no row-group or page pruning, no answer from statistics; every byte decodes.
    ignore,
    /// The default: prune row groups and pages, which only ever skips rows provably excluded, and answer `count(*)`
    /// from row counts. Values themselves always decode, since writers do emit wrong min/max.
    prune,
    /// `--trust-stats`: also answer min/max/sum/count(col) from statistics where they suffice.
    trust,

    pub fn fromFlags(scan_all: bool, trust_stats: bool) StatsUse {
        if (scan_all) return .ignore;
        return if (trust_stats) .trust else .prune;
    }
};

/// The parts of a parsed aggregate query that decide what it reads. Column indexes are leaf indexes of the footer
/// the query was parsed against.
pub const Query = struct {
    filter: ?filter_ast.Filter,
    calls: []const expr_agg.AggCall,
    group_keys: ?[]const expr_ast.Expr,
};

/// Columns a set of aggregates reads from one row group.
pub const Columns = struct {
    /// Read where the outer filter is evaluated.
    read: []const bool,
    /// The part of `read` whose values feed an aggregate argument, a per-aggregate FILTER or a GROUP BY key; the rest
    /// only the outer filter reads. It is all a row group proven fully matching reads, and the set whose pages the
    /// page-level always-match shortcut must still decode for real.
    consumed: []const bool,

    /// The columns to read under `decision`, which must not be `.skip`.
    pub fn forRowGroup(self: Columns, decision: Decision) []const bool {
        return switch (decision) {
            .skip => unreachable,
            .full_match => self.consumed,
            .filter => self.read,
        };
    }
};

/// What statistics say about one row group under the outer filter.
pub const Decision = enum {
    /// No row can pass: the row group is not read at all.
    skip,
    /// Every row passes: the row group is scanned unfiltered, reading only `Columns.consumed`.
    full_match,
    /// Rows are filtered as they decode (or there is no filter), reading `Columns.read`.
    filter,
};

pub const Plan = struct {
    query: Query,
    stats: StatsUse,
    /// Columns the outer filter references, whether or not an aggregate consumes them too.
    filter_cols: []const bool,
    /// The whole query's columns.
    columns: Columns,
    /// Columns statistics answer in every row group of every input, left out of `columns.read`.
    cols_stat_pruned: usize,

    /// Plan `query` over `metas`, the footers its bytes will come from (every input's for a scan; the fetched ones'
    /// for the S3 planner). All must share the schema `query` was parsed against.
    pub fn init(arena: std.mem.Allocator, query: Query, metas: []const schema.FileMetaData, stats: StatsUse) !Plan {
        const num_leaves = if (metas.len == 0) 0 else metadata.leafCount(&metas[0]);

        const filter_cols = try arena.alloc(bool, num_leaves);
        @memset(filter_cols, false);
        if (query.filter) |f| try markFilterColumns(arena, f, filter_cols);

        const consumed = try consumedColumns(arena, query.calls, query.group_keys, num_leaves);
        const read = try arena.alloc(bool, num_leaves);
        for (read, filter_cols, consumed) |*r, f, c| r.* = f or c;

        // A column every aggregate reading it can answer from statistics, in every row group of every input, never
        // decodes, so it is never fetched. Not under an outer filter (statistics describe all rows, not the
        // survivors) nor under GROUP BY (a chunk statistic cannot answer a per-group aggregate). Per-aggregate FILTER
        // makes a call ineligible, which `statsCoverageComplete` enforces.
        var cols_stat_pruned: usize = 0;
        if (query.filter == null and query.group_keys == null and stats != .ignore) {
            const call_cols = try arena.alloc([]bool, query.calls.len);
            for (query.calls, call_cols) |call, *cols| cols.* = try consumedColumns(arena, &.{call}, null, num_leaves);
            for (read, 0..) |*needed, ci| {
                if (!needed.*) continue;
                const answerable = for (query.calls, call_cols) |call, cols| {
                    if (!cols[ci]) continue;
                    if (!expr_agg.statsCoverageComplete(call, metas, ci, stats == .trust)) break false;
                } else true;
                if (answerable) {
                    needed.* = false;
                    cols_stat_pruned += 1;
                }
            }
        }
        for (consumed, read) |*c, r| c.* = c.* and r;

        return .{
            .query = query,
            .stats = stats,
            .filter_cols = filter_cols,
            .columns = .{ .read = read, .consumed = consumed },
            .cols_stat_pruned = cols_stat_pruned,
        };
    }

    /// The columns `calls`, a subset of the query's aggregates, read: the whole query's minus what only the other
    /// calls need. The GROUP BY keys and the outer filter belong to every subset.
    pub fn forCalls(self: *const Plan, arena: std.mem.Allocator, calls: []const expr_agg.AggCall) !Columns {
        const consumed = try consumedColumns(arena, calls, self.query.group_keys, self.filter_cols.len);
        const read = try arena.alloc(bool, self.filter_cols.len);
        for (read, consumed, self.filter_cols, self.columns.read) |*r, *c, f, whole| {
            r.* = whole and (f or c.*);
            c.* = whole and c.*;
        }
        return .{ .read = read, .consumed = consumed };
    }

    /// Prune one row group against the outer filter. The S3 planner and the scan workers both call this on the same
    /// footer: it is what makes a chunk the planner left unfetched one no worker reads.
    pub fn decide(self: *const Plan, rg: *const schema.RowGroup, meta: *const schema.FileMetaData) Decision {
        const f = self.query.filter orelse return .filter;
        if (self.stats == .ignore) return .filter;
        return switch (filter_prune.pruneRowGroup(rg, f, meta)) {
            .skip => .skip,
            .always_match => .full_match,
            .keep, .unknown => .filter,
        };
    }

    /// The outer filter a row group under `decision` is scanned with.
    pub fn filterFor(self: *const Plan, decision: Decision) ?filter_ast.Filter {
        return if (decision == .full_match) null else self.query.filter;
    }
};

/// Columns `calls`' arguments and FILTERs, and the GROUP BY keys, read.
fn consumedColumns(
    arena: std.mem.Allocator,
    calls: []const expr_agg.AggCall,
    group_keys: ?[]const expr_ast.Expr,
    num_leaves: usize,
) ![]bool {
    const cols = try arena.alloc(bool, num_leaves);
    @memset(cols, false);
    for (calls) |call| {
        if (call.arg) |arg_expr| arg_expr.collectColumns(cols);
        if (call.where) |w| try markFilterColumns(arena, w, cols);
    }
    if (group_keys) |keys| for (keys) |key| key.collectColumns(cols);
    return cols;
}

fn markFilterColumns(arena: std.mem.Allocator, f: filter_ast.Filter, cols: []bool) !void {
    var list: std.ArrayList(usize) = .empty;
    defer list.deinit(arena);
    try f.collectColumns(&list, arena);
    for (list.items) |ci| if (ci < cols.len) {
        cols[ci] = true;
    };
}

const testing = std.testing;
const expr_parser = @import("expr/parser.zig");
const filter_parser = @import("filter/parser.zig");

/// Columns of ci/fixtures/parquet/full_match.parquet (see tools/gen_full_match_fixture.py).
const TS = 0;
const B = 1;
const X = 3;

fn planFixture(arena: std.mem.Allocator, meta: *const schema.FileMetaData, filter: ?[]const u8, aggregate: []const u8, group_by: ?[]const u8, stats: StatsUse) !Plan {
    const keys: ?[]const expr_ast.Expr = if (group_by) |gb| blk: {
        const items = try expr_parser.parseGroupBy(arena, gb, meta, null);
        const exprs = try arena.alloc(expr_ast.Expr, items.len);
        for (items, exprs) |item, *e| e.* = item.expr;
        break :blk exprs;
    } else null;
    return .init(arena, .{
        .filter = if (filter) |f| try filter_parser.parse(arena, f, meta, null) else null,
        .calls = try expr_parser.parseAggList(arena, aggregate, meta, null),
        .group_keys = keys,
    }, &.{meta.*}, stats);
}

fn expectColumns(want: []const usize, got: []const bool) !void {
    for (got, 0..) |g, ci| {
        const w = std.mem.indexOfScalar(usize, want, ci) != null;
        testing.expect(w == g) catch |err| {
            std.debug.print("column {d}: want {}, got {}\n", .{ ci, w, g });
            return err;
        };
    }
}

test "a proven row group reads only the columns an aggregate consumes" {
    const bytes = metadata.readFileSlice("ci/fixtures/parquet/full_match.parquet", testing.allocator) catch |err| {
        if (err == error.FileNotFound) return error.SkipZigTest;
        return err;
    };
    defer testing.allocator.free(bytes);
    var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_state.deinit();
    const arena = arena_state.allocator();
    const meta = try metadata.open(arena, bytes);

    const plan = try planFixture(arena, &meta, "ts >= 500", "count(*) AS n, sum(x) AS sx", null, .trust);
    try expectColumns(&.{ TS, X }, plan.columns.read);
    try expectColumns(&.{X}, plan.columns.consumed);
    const want = [_]Decision{ .skip, .skip, .filter, .full_match, .full_match, .full_match, .full_match, .full_match };
    for (meta.row_groups.items, want) |*rg, d| try testing.expectEqual(d, plan.decide(rg, &meta));
    try testing.expectEqual(@as(?filter_ast.Filter, null), plan.filterFor(.full_match));

    // A subset keeps the filter's columns but not the other calls' inputs.
    const count_only = try plan.forCalls(arena, plan.query.calls[0..1]);
    try expectColumns(&.{TS}, count_only.read);
    try expectColumns(&.{}, count_only.consumed);

    // --scan-all proves nothing.
    const scan_all = try planFixture(arena, &meta, "ts >= 500", "count(*) AS n, sum(x) AS sx", null, .ignore);
    for (meta.row_groups.items) |*rg| try testing.expectEqual(Decision.filter, scan_all.decide(rg, &meta));
}

test "only columns statistics answer everywhere, unfiltered and ungrouped, leave the read set" {
    const bytes = metadata.readFileSlice("ci/fixtures/parquet/full_match.parquet", testing.allocator) catch |err| {
        if (err == error.FileNotFound) return error.SkipZigTest;
        return err;
    };
    defer testing.allocator.free(bytes);
    var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_state.deinit();
    const arena = arena_state.allocator();
    const meta = try metadata.open(arena, bytes);

    // max(ts) folds from every row group's bounds; sum(x) is not constant anywhere, so x still decodes.
    const trusted = try planFixture(arena, &meta, null, "max(ts) AS m, sum(x) AS sx", null, .trust);
    try expectColumns(&.{X}, trusted.columns.read);
    try testing.expectEqual(@as(usize, 1), trusted.cols_stat_pruned);

    // Without --trust-stats min/max decode.
    const untrusted = try planFixture(arena, &meta, null, "max(ts) AS m, sum(x) AS sx", null, .prune);
    try expectColumns(&.{ TS, X }, untrusted.columns.read);
    try testing.expectEqual(@as(usize, 0), untrusted.cols_stat_pruned);

    // Nor under a filter or GROUP BY: the statistic covers rows the query does not aggregate together.
    const filtered = try planFixture(arena, &meta, "x > 0", "max(ts) AS m", null, .trust);
    try expectColumns(&.{ TS, X }, filtered.columns.read);
    const grouped = try planFixture(arena, &meta, null, "max(ts) AS m", "b", .trust);
    try expectColumns(&.{ TS, B }, grouped.columns.read);
    try expectColumns(&.{ TS, B }, grouped.columns.consumed);
}
