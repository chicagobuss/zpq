const std = @import("std");
const thrift_mod = @import("../thrift.zig");
const schema = @import("../schema.zig");
const consumer = @import("../consumer.zig");
const agg_plan = @import("../agg_plan.zig");
const metadata = @import("../parquet/metadata.zig");
const expr_ast = @import("../expr/ast.zig");
const expr_agg = @import("../expr/agg.zig");
const core_memory = @import("../memory.zig");
const owned_merge = @import("owned_merge.zig");
const freeOwnedAccumulatorStrings = owned_merge.freeOwnedAccumulatorStrings;
const worker_contracts = @import("worker_contracts.zig");
const Worker = worker_contracts.Worker;
const loadFixture = @import("test_support.zig").loadFixture;
const scan_worker = @import("worker.zig");
const releaseRegions = scan_worker.releaseRegions;
const plan_types = @import("plan_types.zig");
const Planning = plan_types.Planning;
const RecordAdmission = plan_types.RecordAdmission;
const scan_count_cut = @import("count_cut.zig");
const MergedSlot = scan_count_cut.MergedSlot;
const scan_rank_cut = @import("rank_cut.zig");
const ResultCut = scan_rank_cut.ResultCut;
const scan_result = @import("result.zig");
const MultiAggResult = scan_result.MultiAggResult;
const scan_rows = @import("rows.zig");
const buildGroupRows = scan_rows.buildGroupRows;
const slot_sort = @import("slot_sort.zig");
const sortSlotsByFirst = slot_sort.sortSlotsByFirst;
const scan_phase = @import("phase.zig");
const runPhase = scan_phase.runPhase;
const scan_census = @import("census.zig");
const CutBoundPlan = scan_census.CutBoundPlan;
const certifiedResidual = scan_census.certifiedResidual;
const scan_merge = @import("merge.zig");
const MergeGroupRef = scan_merge.MergeGroupRef;
const hashMergeGroupRefs = scan_merge.hashMergeGroupRefs;
const mergeFoldedRefs = scan_merge.mergeFoldedRefs;
const scan_execute = @import("execute.zig");
const runMultiAggregate = scan_execute.runMultiAggregate;
const Chain = scan_execute.Chain;
const narrowed = scan_execute.narrowed;
const explainAggregate = scan_execute.explainAggregate;
const prepareAggregate = scan_execute.prepareAggregate;
const scan_types = @import("types.zig");
const MultiAggArgs = scan_types.MultiAggArgs;
const SketchTopK = scan_types.SketchTopK;
const Input = scan_types.Input;
const FastPath = scan_types.FastPath;
const RowGroupGate = scan_types.RowGroupGate;
const WorkItem = scan_types.WorkItem;
const BoundOutputColumn = scan_types.BoundOutputColumn;
const byte_stream = @import("byte_stream.zig");
const ByteStream = byte_stream.ByteStream;
const integer_stream = @import("integer_stream.zig");
const typed_answer = @import("typed_answer.zig");
const scan_output = @import("output.zig");
const AggOutput = scan_output.AggOutput;
const output_values = @import("output_values.zig");
const AggValue = output_values.AggValue;
const DecValue = output_values.DecValue;
const testing = std.testing;
const test_fixtures = @import("../parquet/test_fixtures.zig");

/// The padding columns exist only to push fetch volume past the 2 MiB-per-worker threshold, so workers really spawn.
pub const SPAWN_FIXTURE_AGG =
    "count(*) AS n" ++
    ", sum(pad0) AS s0" ++ ", sum(pad1) AS s1" ++ ", sum(pad2) AS s2" ++
    ", sum(pad3) AS s3" ++ ", sum(pad4) AS s4" ++ ", sum(pad5) AS s5" ++
    ", sum(pad6) AS s6" ++ ", sum(pad7) AS s7" ++ ", sum(pad8) AS s8" ++
    ", sum(pad9) AS s9" ++ ", sum(pad10) AS s10";

pub fn sameValue(a: AggValue, b: AggValue) bool {
    if (std.meta.activeTag(a) != std.meta.activeTag(b)) return false;
    return switch (a) {
        .s => |x| std.mem.eql(u8, x, b.s),
        else => std.meta.eql(a, b),
    };
}

pub fn runSketchedAndFree(gpa: std.mem.Allocator, inputs: []const Input, meta: *const schema.FileMetaData, certify: bool) !void {
    var arena_state = std.heap.ArenaAllocator.init(gpa);
    defer arena_state.deinit();
    var res = try runMultiAggregate(gpa, arena_state.allocator(), .{
        .inputs = inputs,
        // Parsed up front: a footer parse reports running out of memory as a file that is not Parquet.
        .metas = meta[0..1],
        .aggregate = "count(*) AS c, min(s) AS lo",
        .group_by = "s",
        .parallelism = 1,
        .result_order = .{ .keys = &.{.{ .column = 1, .descending = true }}, .limit = 1 },
        .sketch_topk = .{ .cell_bits = 10, .admit_per_row = 4, .certify = certify },
    });
    res.deinit(gpa);
}

pub fn runFirstCutAndFree(gpa: std.mem.Allocator, inputs: []const Input, meta: *const schema.FileMetaData, limit: u64) !void {
    var arena_state = std.heap.ArenaAllocator.init(gpa);
    defer arena_state.deinit();
    var res = try runMultiAggregate(gpa, arena_state.allocator(), .{
        .inputs = inputs,
        .metas = meta[0..1],
        .aggregate = "count(*) AS c, min(s) AS lo",
        .group_by = "n",
        .parallelism = 1,
        .result_order = .{ .limit = limit },
        .sketch_topk = .{},
    });
    res.deinit(gpa);
}

pub fn runCensusAndFree(gpa: std.mem.Allocator, inputs: []const Input, meta: *const schema.FileMetaData, cfg: SketchTopK) !void {
    var arena_state = std.heap.ArenaAllocator.init(gpa);
    defer arena_state.deinit();
    var res = try runMultiAggregate(gpa, arena_state.allocator(), .{
        .inputs = inputs,
        .metas = meta[0..1],
        .aggregate = "count(*) AS c, min(s) AS lo",
        .group_by = "s",
        .parallelism = 1,
        .result_order = .{ .keys = &.{.{ .column = 1, .descending = true }}, .limit = 4 },
        .sketch_topk = cfg,
    });
    res.deinit(gpa);
}

pub fn runDistinctCensusAndFree(gpa: std.mem.Allocator, inputs: []const Input, meta: *const schema.FileMetaData, cfg: SketchTopK) !void {
    var arena_state = std.heap.ArenaAllocator.init(gpa);
    defer arena_state.deinit();
    var res = try runMultiAggregate(gpa, arena_state.allocator(), .{
        .inputs = inputs,
        .metas = meta[0..1],
        .aggregate = "count(DISTINCT u) AS d, min(s) AS lo",
        .group_by = "t",
        .parallelism = 1,
        .result_order = .{ .keys = &.{.{ .column = 1, .descending = true }}, .limit = 4 },
        .sketch_topk = cfg,
    });
    res.deinit(gpa);
}

pub const nested_key_fixture = "ci/fixtures/parquet/nested_key_shape.parquet";

pub fn loadNestedKeyFixture() !?[]u8 {
    return metadata.readFileSlice(nested_key_fixture, testing.allocator) catch |err| {
        if (err == error.FileNotFound) {
            std.debug.print("skipping: {s} not present\n", .{nested_key_fixture});
            return null;
        }
        return err;
    };
}

pub fn freeResult(res: MultiAggResult) void {
    var owned = res;
    owned.deinit(testing.allocator);
}

pub const FullMatchQuery = struct {
    filter: ?[]const u8,
    aggregate: []const u8,
    group_by: ?[]const u8 = null,
    /// Row groups the proof must claim; checked so a silently disabled optimization cannot pass as a correct answer.
    full_match: usize,
};

pub fn runFullMatchQuery(
    arena: std.mem.Allocator,
    inputs: []const Input,
    q: FullMatchQuery,
    parallelism: usize,
    scan_all: bool,
) !MultiAggResult {
    return runMultiAggregate(testing.allocator, arena, .{
        .inputs = inputs,
        .filter = q.filter,
        .aggregate = q.aggregate,
        .group_by = q.group_by,
        .parallelism = parallelism,
        .scan_all = scan_all,
    });
}

pub fn expectSameAggValue(want: AggValue, got: AggValue) !void {
    try testing.expectEqual(std.meta.activeTag(want), std.meta.activeTag(got));
    switch (want) {
        .i => |v| try testing.expectEqual(v, got.i),
        // Fixture doubles are multiples of 0.5 with small sums, so every summation order is exact.
        .f => |v| try testing.expectEqual(v, got.f),
        .avg => |v| {
            try testing.expectEqual(v.sum, got.avg.sum);
            try testing.expectEqual(v.count, got.avg.count);
        },
        .s => |v| try testing.expectEqualStrings(v, got.s),
        .dec => |v| try testing.expectEqual(v, got.dec),
        .avg_dec => |v| try testing.expectEqual(v, got.avg_dec),
        .null_val => {},
    }
}

pub fn expectSameAnswer(want: MultiAggResult, got: MultiAggResult) !void {
    try testing.expectEqual(want.output.aggs.len, got.output.aggs.len);
    for (want.output.aggs, got.output.aggs) |w, g| try expectSameAggValue(w.value, g.value);
    const want_rows = want.output.group_rows orelse &.{};
    const got_rows = got.output.group_rows orelse &.{};
    try testing.expectEqual(want_rows.len, got_rows.len);
    for (want_rows, got_rows) |wr, gr| {
        try testing.expectEqual(wr.len, gr.len);
        for (wr, gr) |w, g| try expectSameAggValue(w, g);
    }
}

/// Runs `q` with and without statistics at several -j (16 splits each row group's aggregates across work items, each
/// with its own proven fetch set) and requires identical answers plus the expected number of proven row groups.
pub fn checkFullMatchQuery(inputs: []const Input, q: FullMatchQuery) !MultiAggResult {
    var oracle_arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer oracle_arena.deinit();
    const oracle = runFullMatchQuery(oracle_arena.allocator(), inputs, q, 1, true) catch |err| {
        std.debug.print("full match: --scan-all failed for filter={?s} agg={s}: {s}\n", .{
            q.filter, q.aggregate, @errorName(err),
        });
        return err;
    };
    defer freeResult(oracle);
    try testing.expectEqual(@as(usize, 0), oracle.row_groups_full_match);

    var last: ?MultiAggResult = null;
    errdefer if (last) |r| freeResult(r);
    for ([_]usize{ 1, 4, 16 }) |parallelism| {
        var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
        defer arena_state.deinit();
        var got = try runFullMatchQuery(arena_state.allocator(), inputs, q, parallelism, false);
        // Only the answer outlives this arena.
        got.state.deinit(testing.allocator);
        if (last) |r| freeResult(r);
        last = got;
        expectSameAnswer(oracle, got) catch |err| {
            std.debug.print("full match: -j{d} disagrees with --scan-all for filter={?s} agg={s} group_by={?s}\n", .{
                parallelism, q.filter, q.aggregate, q.group_by,
            });
            return err;
        };
        testing.expectEqual(q.full_match, got.row_groups_full_match) catch |err| {
            std.debug.print("full match: filter={?s} proved {d} row groups, want {d}\n", .{
                q.filter, got.row_groups_full_match, q.full_match,
            });
            return err;
        };
    }
    return last.?;
}

/// Overwrite column `col`'s chunk in each listed row group with bytes no decoder accepts.
pub fn poisonChunks(bytes: []u8, meta: *const schema.FileMetaData, col: usize, rgs: []const usize) void {
    for (rgs) |rg_i| {
        const cm = meta.row_groups.items[rg_i].columns.items[col].meta_data.?;
        const start: usize = @intCast(cm.dictionary_page_offset orelse cm.data_page_offset);
        const len: usize = @intCast(cm.total_compressed_size);
        @memset(bytes[start .. start + len], 0xA5);
    }
}

pub const unsigned_order_fixture = "ci/fixtures/parquet/unsigned_order.parquet";

pub const CountSumCase = struct { filter: []const u8, n: i128, sum_i: i128 };

/// `count(*)` and `sum(i)` for each filter, with statistics (pruning, page index) and under --scan-all, at -j1 and -j4.
pub fn expectCountSum(path: []const u8, cases: []const CountSumCase) !void {
    const bytes = (try loadFixture(path)) orelse return error.SkipZigTest;
    defer testing.allocator.free(bytes);
    const inputs = [_]Input{.{ .name = path, .bytes = bytes }};
    for (cases) |c| {
        for ([_]bool{ false, true }) |scan_all| for ([_]usize{ 1, 4 }) |parallelism| {
            var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
            defer arena_state.deinit();
            const res = runMultiAggregate(testing.allocator, arena_state.allocator(), .{
                .inputs = &inputs,
                .filter = c.filter,
                .aggregate = "count(*) AS n, sum(i) AS s",
                .parallelism = parallelism,
                .scan_all = scan_all,
            }) catch |err| {
                std.debug.print("{s}: filter {s} failed: {s}\n", .{ path, c.filter, @errorName(err) });
                return err;
            };
            defer freeResult(res);
            const got_sum: i128 = switch (res.output.aggs[1].value) {
                .i => |v| v,
                else => 0, // sum over no rows
            };
            if (res.output.aggs[0].value.i != c.n or got_sum != c.sum_i) {
                std.debug.print("{s}: filter {s} (scan_all={}, -j{d}) gave n={d} sum={d}, want n={d} sum={d}\n", .{
                    path, c.filter, scan_all, parallelism, res.output.aggs[0].value.i, got_sum, c.n, c.sum_i,
                });
                return error.TestUnexpectedResult;
            }
        };
    }
}

pub const deprecated_stats_fixture = "ci/fixtures/parquet/deprecated_stats.parquet";

pub const column_order_fixture = "ci/fixtures/parquet/column_order.parquet";

pub const nan_stats_fixture = "ci/fixtures/parquet/nan_stats.parquet";

/// What `rewritten` changes: the footer, and one chunk's ColumnIndex or OffsetIndex.
pub const Rewrite = struct {
    created_by: ?[]const u8 = null,
    footer: ?*const fn (*schema.FileMetaData) void = null,
    rg: usize = 0,
    col: usize = 0,
    column_index: ?*const fn (*schema.ColumnIndex) void = null,
    offset_index: ?*const fn (*schema.OffsetIndex) void = null,
};

/// `bytes` rewritten by `rw`: an edited index is written after the pages and the footer pointed at it. Caller frees.
pub fn rewritten(bytes: []const u8, rw: Rewrite) ![]u8 {
    const thrift = @import("../thrift.zig");
    var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_state.deinit();
    const arena = arena_state.allocator();
    var meta = try metadata.open(arena, bytes);
    const old_len = std.mem.readInt(u32, bytes[bytes.len - 8 ..][0..4], .little);
    var out: std.ArrayList(u8) = .empty;
    errdefer out.deinit(testing.allocator);
    try out.appendSlice(testing.allocator, bytes[0 .. bytes.len - 8 - old_len]);

    const chunk = &meta.row_groups.items[rw.rg].columns.items[rw.col];
    if (rw.column_index) |edit| {
        const at: usize = @intCast(chunk.column_index_offset.?);
        var r = thrift.Reader.init(bytes[at..][0..@intCast(chunk.column_index_length.?)]);
        var ci = try schema.ColumnIndex.read(arena, &r);
        edit(&ci);
        var w = thrift.Writer.init(arena);
        try ci.write(&w);
        chunk.column_index_offset = @intCast(out.items.len);
        chunk.column_index_length = @intCast(w.bytes().len);
        try out.appendSlice(testing.allocator, w.bytes());
    }
    if (rw.offset_index) |edit| {
        const at: usize = @intCast(chunk.offset_index_offset.?);
        var r = thrift.Reader.init(bytes[at..][0..@intCast(chunk.offset_index_length.?)]);
        var oi = try schema.OffsetIndex.read(arena, &r);
        edit(&oi);
        var w = thrift.Writer.init(arena);
        try oi.write(&w);
        chunk.offset_index_offset = @intCast(out.items.len);
        chunk.offset_index_length = @intCast(w.bytes().len);
        try out.appendSlice(testing.allocator, w.bytes());
    }
    if (rw.footer) |edit| edit(&meta);
    if (rw.created_by) |writer| meta.created_by = writer;

    var w = thrift.Writer.init(arena);
    try meta.write(&w);
    try out.appendSlice(testing.allocator, w.bytes());
    var len: [4]u8 = undefined;
    std.mem.writeInt(u32, &len, @intCast(w.bytes().len), .little);
    try out.appendSlice(testing.allocator, &len);
    try out.appendSlice(testing.allocator, "PAR1");
    return out.toOwnedSlice(testing.allocator);
}

pub const PoisonCase = struct {
    filter: []const u8,
    /// count(*) by default (the decoded truth, hand-known) and under --trust-stats.
    n: i128,
    trusted: i128,
};

pub fn expectPoisonCases(path: []const u8, rw: Rewrite, cases: []const PoisonCase) !void {
    const clean = try loadFixture(path) orelse return error.SkipZigTest;
    defer testing.allocator.free(clean);
    const bytes = try rewritten(clean, rw);
    defer testing.allocator.free(bytes);
    const inputs = [_]Input{.{ .name = path, .bytes = bytes }};
    for (cases) |c| for ([_]bool{ false, true }) |trust| for ([_]bool{ true, false }) |scan_all| {
        var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
        defer arena_state.deinit();
        const got = try runMultiAggregate(testing.allocator, arena_state.allocator(), .{
            .inputs = &inputs,
            .filter = c.filter,
            .aggregate = "count(*) AS n",
            .parallelism = 1,
            .trust_stats = trust,
            .scan_all = scan_all,
        });
        defer freeResult(got);
        const want = if (trust and !scan_all) c.trusted else c.n;
        testing.expectEqual(want, got.output.aggs[0].value.i) catch |err| {
            const fmt = "poisoned stats: {s}: {s} trust_stats={} scan_all={}\n";
            std.debug.print(fmt, .{ path, c.filter, trust, scan_all });
            return err;
        };
    };
}

/// full_match.parquet's row group 1 chunk statistics for leaf `ci`: ts, b, s, x, n, u.
pub fn rg1Stats(meta: *schema.FileMetaData, ci: usize) *schema.Statistics {
    return &meta.row_groups.items[1].columns.items[ci].meta_data.?.statistics.?;
}

pub const null_pages_fixture = "ci/fixtures/parquet/null_pages.parquet";

/// A select list given as the SQL front end writes one (`expr AS alias`), split as `SelectColumn.parse` splits it.
pub fn selectList(arena: std.mem.Allocator, cols: ?[]const []const u8) !?[]const expr_ast.SelectColumn {
    const texts = cols orelse return null;
    const out = try arena.alloc(expr_ast.SelectColumn, texts.len);
    for (texts, out) |text, *col| col.* = .parse(text);
    return out;
}

/// One query against `meta`, freeing its result through the destructors alone.
pub fn runAndFree(
    gpa: std.mem.Allocator,
    inputs: []const Input,
    meta: *const schema.FileMetaData,
    aggregate: []const u8,
    group_by: ?[]const u8,
) !void {
    var arena_state = std.heap.ArenaAllocator.init(gpa);
    defer arena_state.deinit();
    var res = try runMultiAggregate(gpa, arena_state.allocator(), .{
        .inputs = inputs,
        .metas = meta[0..1],
        .aggregate = aggregate,
        .group_by = group_by,
        .parallelism = 1,
        .scan_all = true,
    });
    res.deinit(gpa);
}

/// Forwards to `child` but never grows, shrinks or moves an allocation in place. `testing.allocator` resizes in place
/// only while the allocation is the newest in its bucket and the bucket has room, which depends on everything the
/// thread allocated before; a refused resize makes the caller allocate instead, so the allocation count
/// `checkAllAllocationFailures` replays would change from run to run (on Apple silicon's 16 KiB pages, every time).
/// Refusing them all makes every growth an allocation of its own, and so one more failure the check injects.
pub const NoInPlaceResize = struct {
    child: std.mem.Allocator,

    pub fn allocator(self: *NoInPlaceResize) std.mem.Allocator {
        return .{ .ptr = self, .vtable = &.{
            .alloc = alloc,
            .resize = std.mem.Allocator.noResize,
            .remap = std.mem.Allocator.noRemap,
            .free = free,
        } };
    }

    pub fn alloc(ctx: *anyopaque, len: usize, alignment: std.mem.Alignment, ret_addr: usize) ?[*]u8 {
        const self: *NoInPlaceResize = @ptrCast(@alignCast(ctx));
        return self.child.rawAlloc(len, alignment, ret_addr);
    }

    pub fn free(ctx: *anyopaque, memory: []u8, alignment: std.mem.Alignment, ret_addr: usize) void {
        const self: *NoInPlaceResize = @ptrCast(@alignCast(ctx));
        self.child.rawFree(memory, alignment, ret_addr);
    }
};

/// `expectSameAggValue`, floats compared bit for bit (a -0 is no 0, and every rounding shows).
pub fn expectSameBits(want: AggValue, got: AggValue) !void {
    switch (want) {
        .f => |v| try testing.expectEqual(@as(u64, @bitCast(v)), @as(u64, @bitCast(got.f))),
        .avg => |v| {
            const want_sum: ?u64 = if (v.sum) |x| @bitCast(x) else null;
            const got_sum: ?u64 = if (got.avg.sum) |x| @bitCast(x) else null;
            try testing.expectEqual(want_sum, got_sum);
            try testing.expectEqual(v.count, got.avg.count);
        },
        else => try expectSameAggValue(want, got),
    }
}

pub fn setMergeTestString(allocator: std.mem.Allocator, acc: *expr_agg.Accumulator, value: []const u8) !void {
    // A union initializer writes its tag before evaluating the payload. Keep the arena variant valid on OOM.
    const owned = try allocator.dupe(u8, value);
    acc.* = .{ .min_bytes = owned };
}

pub fn mergeOverlapAndFree(allocator: std.mem.Allocator) !void {
    const calls = [_]expr_agg.AggCall{
        .{ .func = .count, .alias = "n", .result = .i64, .arg = null, .where = null },
        .{ .func = .min, .alias = "m", .result = .bytes, .arg = null, .where = null },
    };
    var a = expr_agg.GroupTable.init(allocator, 1 << 20);
    defer a.deinit();
    var b = expr_agg.GroupTable.init(allocator, 1 << 20);
    defer b.deinit();
    _ = try a.getOrInsert("alpha", &calls, 10);
    a.accumulators.items[0].count = 2;
    try setMergeTestString(allocator, &a.accumulators.items[1], "mango");
    _ = try a.getOrInsert("beta", &calls, 40);
    a.accumulators.items[2].count = 1;
    try setMergeTestString(allocator, &a.accumulators.items[3], "pear");
    _ = try b.getOrInsert("alpha", &calls, 3);
    b.accumulators.items[0].count = 5;
    try setMergeTestString(allocator, &b.accumulators.items[1], "apple");
    _ = try b.getOrInsert("gamma", &calls, 20);
    b.accumulators.items[2].count = 7;
    try setMergeTestString(allocator, &b.accumulators.items[3], "fig");

    const tables = [_]expr_agg.GroupTable{ a, b };
    const refs = [_]MergeGroupRef{
        .{ .table = 0, .group = 0 },
        .{ .table = 0, .group = 1 },
        .{ .table = 1, .group = 0 },
        .{ .table = 1, .group = 1 },
    };
    var slots: std.ArrayList(MergedSlot) = .empty;
    defer slots.deinit(allocator);
    var accs: std.ArrayList(expr_agg.Accumulator) = .empty;
    defer {
        freeOwnedAccumulatorStrings(allocator, accs.items);
        accs.deinit(allocator);
    }
    try hashMergeGroupRefs(allocator, allocator, &tables, &refs, null, &calls, null, false, &slots, &accs);
    std.sort.pdq(MergedSlot, slots.items, {}, struct {
        fn lt(_: void, x: MergedSlot, y: MergedSlot) bool {
            return x.first < y.first;
        }
    }.lt);
    try testing.expectEqual(@as(usize, 3), slots.items.len);
    const want_key = [_][]const u8{ "alpha", "gamma", "beta" };
    const want_first = [_]u64{ 3, 20, 40 };
    const want_count = [_]u64{ 7, 7, 1 };
    const want_min = [_][]const u8{ "apple", "fig", "pear" };
    for (slots.items, want_key, want_first, want_count, want_min) |slot, key, first, count, min| {
        try testing.expectEqualStrings(key, slot.key);
        try testing.expectEqual(first, slot.first);
        try testing.expectEqual(count, accs.items[slot.idx].count);
        try testing.expectEqualStrings(min, accs.items[slot.idx + 1].min_bytes.?);
    }
}

pub fn fillCountTables(
    a: *expr_agg.GroupTable,
    b: *expr_agg.GroupTable,
    calls: []const expr_agg.AggCall,
    n: usize,
) !void {
    var buf: [32]u8 = undefined;
    for (0..n) |i| {
        const key = std.fmt.bufPrint(&buf, "k{d}", .{i}) catch unreachable;
        const id = try a.getOrInsert(key, calls, i);
        a.accumulators.items[@as(usize, id) * calls.len].count = 1;
    }
    for (0..n) |i| {
        const key = std.fmt.bufPrint(&buf, "k{d}", .{i + n / 2}) catch unreachable;
        const id = try b.getOrInsert(key, calls, 1_000_000 + i);
        b.accumulators.items[@as(usize, id) * calls.len].count = 10;
    }
}

pub fn countSum(accs: []const expr_agg.Accumulator, slots: []const MergedSlot) u64 {
    var sum: u64 = 0;
    for (slots) |s| sum += accs[s.idx].count;
    return sum;
}

pub fn fillFixedPair(
    allocator: std.mem.Allocator,
    fixed: *[2]expr_agg.GroupTable,
    bytes: *[2]expr_agg.GroupTable,
    stride: u8,
    calls: []const expr_agg.AggCall,
    n: usize,
) !void {
    for (fixed, bytes, 0..) |*f, *b, ti| {
        f.* = expr_agg.GroupTable.init(allocator, 1 << 30);
        f.fixed_stride = stride;
        b.* = expr_agg.GroupTable.init(allocator, 1 << 30);
        // Table 1 repeats the upper half of table 0's keys, asked from earlier positions for odd keys.
        for (0..n) |r| {
            const i = r + ti * n / 2;
            var image: [27]u8 = @splat(0);
            var c: usize = 0;
            while (c < stride) : (c += 9) {
                image[c] = 1;
                std.mem.writeInt(u64, image[c + 1 ..][0..8], (i *% 0x9E3779B97F4A7C15) >> @intCast(c), .little);
            }
            const pos: u64 = if (ti == 1 and i % 2 == 1) i / 4 else 1000 * ti + i;
            const fid = try f.getOrInsertFixed(image[0..stride], calls, pos);
            const bid = try b.getOrInsert(image[0..stride], calls, pos);
            for ([_]*expr_agg.GroupTable{ f, b }, [_]u32{ fid, bid }) |t, id| {
                const accs = t.accumulators.items[@as(usize, id) * calls.len ..][0..calls.len];
                accs[0].count = i % 7 + 1;
                accs[1] = .{ .sum_i = @as(i128, @intCast(i)) - 300 };
            }
        }
    }
}

pub fn expectSameMerge(
    allocator: std.mem.Allocator,
    fixed: []const expr_agg.GroupTable,
    bytes: []const expr_agg.GroupTable,
    refs: []const MergeGroupRef,
    calls: []const expr_agg.AggCall,
    n_workers: usize,
    cut: ?ResultCut,
) !void {
    var arena_state = std.heap.ArenaAllocator.init(allocator);
    defer arena_state.deinit();
    const Lists = struct {
        slots: std.ArrayList(MergedSlot) = .empty,
        accs: std.ArrayList(expr_agg.Accumulator) = .empty,
    };
    var lists: [2]Lists = .{ .{}, .{} };
    defer for (&lists) |*l| {
        l.slots.deinit(allocator);
        l.accs.deinit(allocator);
    };
    for ([_][]const expr_agg.GroupTable{ fixed, bytes }, &lists) |tables, *l|
        try mergeFoldedRefs(allocator, allocator, tables, refs, calls, n_workers, cut, false, &l.slots, &l.accs);
    try testing.expectEqual(lists[1].slots.items.len, lists[0].slots.items.len);
    for (lists[0].slots.items, lists[1].slots.items) |f, b| {
        try testing.expectEqualSlices(u8, b.key, f.key);
        try testing.expectEqual(b.first, f.first);
        try testing.expectEqual(lists[1].accs.items[b.idx].count, lists[0].accs.items[f.idx].count);
        try testing.expectEqual(lists[1].accs.items[b.idx + 1].sum_i, lists[0].accs.items[f.idx + 1].sum_i);
    }
}

pub fn mergeFixedAsBytes(allocator: std.mem.Allocator, comptime n: usize, stride: u8, n_workers: usize) !void {
    const calls = [_]expr_agg.AggCall{
        .{ .func = .count, .alias = "n", .result = .i64, .arg = null, .where = null },
        .{ .func = .sum, .alias = "s", .result = .i64, .arg = null, .where = null },
    };
    var fixed: [2]expr_agg.GroupTable = .{ .init(allocator, 0), .init(allocator, 0) };
    var bytes: [2]expr_agg.GroupTable = .{ .init(allocator, 0), .init(allocator, 0) };
    defer for (&fixed, &bytes) |*f, *b| {
        f.deinit();
        b.deinit();
    };
    try fillFixedPair(allocator, &fixed, &bytes, stride, &calls, n);
    var refs: [2 * n]MergeGroupRef = undefined;
    for (&refs, 0..) |*r, i| r.* = .{ .table = @intCast(i / n), .group = @intCast(i % n) };
    // Whole merges, a count cut and a ranking cut, each in one partition and in several.
    try expectSameMerge(allocator, &fixed, &bytes, &refs, &calls, n_workers, null);
    try expectSameMerge(allocator, &fixed, &bytes, &refs, &calls, n_workers, .{ .a_idx = 0, .k = 25 });
    try expectSameMerge(allocator, &fixed, &bytes, &refs, &calls, n_workers, .{
        .a_idx = 1,
        .k = 25,
        .ranking_first = true,
    });
}

pub const in_place_calls = [_]expr_agg.AggCall{
    .{ .func = .count, .alias = "n", .result = .i64, .arg = null, .where = null },
    .{ .func = .sum, .alias = "si", .result = .i64, .arg = null, .where = null },
    .{ .func = .sum, .alias = "sf", .result = .f64, .arg = null, .where = null },
    .{ .func = .min, .alias = "mb", .result = .bytes, .arg = null, .where = null },
    .{ .func = .avg, .alias = "av", .result = .avg_f64, .arg = null, .where = null },
    .{ .func = .max, .alias = "mf", .result = .f64, .arg = null, .where = null },
};

/// Three tables over `n` keys, each holding about two thirds of them, with float sums of -0.0 alone, -0.0 meeting
/// -0.0 or +0.0, NULL sums and extrema, and owned string minima.
pub fn fillInPlaceTables(gpa: std.mem.Allocator, tables: *[3]expr_agg.GroupTable, fixed: bool, n: usize) !void {
    const calls = &in_place_calls;
    for (tables, 0..) |*t, ti| {
        t.* = expr_agg.GroupTable.init(gpa, 1 << 30);
        if (fixed) t.fixed_stride = 9;
        for (0..n) |r| {
            const i = (r * 7 + ti * 3) % n;
            if ((i + ti) % 3 == 0) continue;
            var image: [9]u8 = @splat(0);
            image[0] = 1;
            std.mem.writeInt(u64, image[1..9], i, .little);
            const id = try t.getOrInsert(&image, calls, ti * n + r);
            const accs = t.accumulators.items[@as(usize, id) * calls.len ..][0..calls.len];
            const v: f64 = @floatFromInt(@as(i64, @intCast(i % 11)) - 5);
            accs[0].count = i % 4 + 1;
            accs[1] = .{ .sum_i = if (i % 13 == 0) null else @as(i128, @intCast(i)) * 3 - 700 };
            accs[2] = .{ .sum_f = switch (i % 5) {
                0 => -0.0,
                1 => if (ti == 0) 0.0 else -0.0,
                2 => null,
                else => v * 0.1,
            } };
            const tag = (i * 31 + ti * 5) % 97;
            const min: ?[]const u8 = if (i % 6 == 0) null else try std.fmt.allocPrint(gpa, "s{d}", .{tag});
            accs[3] = .{ .min_bytes = min };
            accs[4] = .{ .avg = .{ .sum = v, .count = 1, .isum = @as(i128, @intCast(i)) + ti } };
            accs[5] = .{ .max_f = if (i % 4 == 0) null else v };
        }
    }
}

pub const in_place_outputs = blk: {
    var outs: [in_place_calls.len]BoundOutputColumn = undefined;
    for (&outs, 0..) |*o, i| o.* = .{ .source = .{ .agg = i }, .name = in_place_calls[i].alias };
    break :blk outs;
};

/// Merge, then build rows, from fresh tables, copying the state or in place.
pub fn mergeAndBuildRows(gpa: std.mem.Allocator, fixed: bool, n: usize, n_workers: usize, in_place: bool) !AggOutput {
    var tables: [3]expr_agg.GroupTable = .{ .init(gpa, 0), .init(gpa, 0), .init(gpa, 0) };
    defer for (&tables) |*t| t.deinit();
    try fillInPlaceTables(gpa, &tables, fixed, n);
    var refs: std.ArrayList(MergeGroupRef) = .empty;
    defer refs.deinit(gpa);
    for (tables, 0..) |t, ti| for (0..t.groupCount()) |g| {
        try refs.append(gpa, .{ .table = @intCast(ti), .group = @intCast(g) });
    };
    var arena_state = std.heap.ArenaAllocator.init(gpa);
    defer arena_state.deinit();
    var slots: std.ArrayList(MergedSlot) = .empty;
    defer slots.deinit(gpa);
    var accs: std.ArrayList(expr_agg.Accumulator) = .empty;
    defer {
        freeOwnedAccumulatorStrings(gpa, accs.items);
        accs.deinit(gpa);
    }
    const calls = &in_place_calls;
    try mergeFoldedRefs(gpa, gpa, &tables, refs.items, calls, n_workers, null, in_place, &slots, &accs);
    if (in_place) try testing.expectEqual(@as(usize, 0), accs.items.len);
    try sortSlotsByFirst(gpa, slots.items, n_workers);
    var out: AggOutput = .{};
    errdefer out.deinit(gpa);
    const rows = try gpa.alloc([]const AggValue, slots.items.len);
    @memset(rows, &.{});
    out.group_rows = rows;
    try buildGroupRows(gpa, &out.slab, .{
        .slots = slots.items,
        .outputs = &in_place_outputs,
        .calls = calls,
        .key_types = &.{.i64},
        .accs = accs.items,
        .folded = true,
        .in_place = if (in_place) &tables else &.{},
        .fixed_keys = fixed,
    }, rows, n_workers);
    return out;
}

pub fn byteWindowAllocTrial(a: std.mem.Allocator, stream: ByteStream) !void {
    var w = try stream.window(a, 0, @min(stream.state.group_count, 137), 1);
    defer w.deinit(a);
    try testing.expectEqual(@as(usize, 3), w.columns.len);
}

pub const decimal_exact_fixture = "ci/fixtures/parquet/decimal_exact.parquet";

pub const DecWant = struct { alias: []const u8, v: i128, p: u8, s: u8 };

pub fn expectDecimalAnswers(args: MultiAggArgs, want: []const DecWant) !void {
    var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_state.deinit();
    const res = try runMultiAggregate(testing.allocator, arena_state.allocator(), args);
    defer freeResult(res);
    for (want) |w| {
        const item = for (res.output.aggs) |it| {
            if (std.mem.eql(u8, it.alias, w.alias)) break it;
        } else return error.TestUnexpectedResult;
        const got: DecValue = switch (item.value) {
            .dec => |d| d,
            .avg_dec => |a| .{ .v = a.mean.?, .spec = a.spec },
            else => {
                std.debug.print("{s}: not a DECIMAL answer ({s})\n", .{ w.alias, @tagName(item.value) });
                return error.TestUnexpectedResult;
            },
        };
        testing.expectEqual(DecValue{ .v = w.v, .spec = .{ .precision = w.p, .scale = w.s } }, got) catch |err| {
            std.debug.print("{s}: got {d} DECIMAL({d},{d})\n", .{ w.alias, got.v, got.spec.precision, got.spec.scale });
            return err;
        };
    }
}

pub const NoGate = struct {
    var ctx: u8 = 0;
    pub fn wait(_: *anyopaque, _: usize) anyerror!void {}
    pub fn done(_: *anyopaque, _: usize, _: usize) void {}
    pub const gate: RowGroupGate = .{ .ctx = &ctx, .wait = wait, .done = done };
};

/// The query's outcome at each budget, an error or the group count, for comparing fast paths with their fallback.
pub fn outcomes(args: MultiAggArgs, budgets: []const usize, out: []anyerror!usize) void {
    for (budgets, out) |budget, *o| {
        var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
        defer arena_state.deinit();
        var a = args;
        a.max_memory = budget;
        o.* = if (runMultiAggregate(testing.allocator, arena_state.allocator(), a)) |res| blk: {
            var r = res;
            defer freeResult(r);
            r.output.applyResultOrder(testing.allocator, args.result_order) catch |e| break :blk e;
            break :blk r.output.group_rows.?.len;
        } else |e| e;
    }
}

pub fn expectSameOutcomes(want: []const (anyerror!usize), got: []const (anyerror!usize)) !void {
    for (want, got) |w, g| {
        if (w) |n| try testing.expectEqual(n, try g) else |e| try testing.expectError(e, g);
    }
}

pub const BitmapTestFixture = struct {
    inputs: []const Input,
    metas: []schema.FileMetaData,

    pub fn init(a: std.mem.Allocator) !BitmapTestFixture {
        const n = 8192;
        const keys = try a.alloc(i64, n);
        const key_defs = try a.alloc(u32, n);
        const values = try a.alloc(i64, n);
        const defs = try a.alloc(u32, n);
        for (keys, key_defs, values, defs, 0..) |*key, *kd, *value, *d, i| {
            key.* = @intCast(i % 8);
            kd.* = @intFromBool(i % 19 != 0);
            value.* = @as(i64, @intCast(i % 13)) - 6;
            d.* = @intFromBool(i % 8 != 7 and i % 11 != 0);
        }
        const columns = [_]expr_agg.OutputCol{
            .{ .name = "k", .parquet_type = .INT64, .nullable = true, .col = .{ .i64 = .{ .values = keys, .def_levels = key_defs, .max_def = 1 } } },
            .{ .name = "x", .parquet_type = .INT64, .nullable = true, .col = .{ .i64 = .{ .values = values, .def_levels = defs, .max_def = 1 } } },
        };
        var bytes: @import("../writer/streaming.zig").MemorySink = .{ .arena = a };
        _ = try consumer.writeAggregateRows(a, bytes.sink(), &columns, n, .SNAPPY, .{}, .{});
        const metas = try a.alloc(schema.FileMetaData, 1);
        metas[0] = try metadata.open(a, bytes.buffer.items);
        const groups = try a.alloc(schema.RowGroup, 4);
        @memset(groups, metas[0].row_groups.items[0]);
        metas[0].row_groups.items = groups;
        metas[0].num_rows *= @intCast(groups.len);
        const inputs = try a.alloc(Input, 1);
        inputs[0] = .{ .name = "bitmap-fixture", .bytes = bytes.buffer.items };
        return .{ .inputs = inputs, .metas = metas };
    }

    pub fn args(f: BitmapTestFixture) MultiAggArgs {
        return .{ .inputs = f.inputs, .metas = f.metas, .aggregate = "count(DISTINCT x) AS d", .group_by = "k", .parallelism = 4, .min_bytes_per_worker = 1, .integer_records_min_rows = 0, .grouped_distinct_bitmap = .private, .max_memory = 128 << 20 };
    }
};

/// Counts the bytes live through it, and their high-water mark.
pub const LivePeak = struct {
    child: std.mem.Allocator,
    live: std.atomic.Value(usize) = .init(0),
    peak: std.atomic.Value(usize) = .init(0),

    pub fn allocator(self: *LivePeak) std.mem.Allocator {
        return .{ .ptr = self, .vtable = &.{ .alloc = alloc, .resize = resize, .remap = remap, .free = free } };
    }

    pub fn grew(self: *LivePeak, n: usize) void {
        const now = self.live.fetchAdd(n, .monotonic) + n;
        _ = self.peak.fetchMax(now, .monotonic);
    }

    pub fn alloc(ctx: *anyopaque, len: usize, a: std.mem.Alignment, ret: usize) ?[*]u8 {
        const self: *LivePeak = @ptrCast(@alignCast(ctx));
        const p = self.child.rawAlloc(len, a, ret) orelse return null;
        self.grew(len);
        return p;
    }

    pub fn resize(ctx: *anyopaque, m: []u8, a: std.mem.Alignment, new_len: usize, ret: usize) bool {
        const self: *LivePeak = @ptrCast(@alignCast(ctx));
        if (!self.child.rawResize(m, a, new_len, ret)) return false;
        if (new_len > m.len) self.grew(new_len - m.len) else _ = self.live.fetchSub(m.len - new_len, .monotonic);
        return true;
    }

    pub fn remap(ctx: *anyopaque, m: []u8, a: std.mem.Alignment, new_len: usize, ret: usize) ?[*]u8 {
        const self: *LivePeak = @ptrCast(@alignCast(ctx));
        const p = self.child.rawRemap(m, a, new_len, ret) orelse return null;
        if (new_len > m.len) self.grew(new_len - m.len) else _ = self.live.fetchSub(m.len - new_len, .monotonic);
        return p;
    }

    pub fn free(ctx: *anyopaque, m: []u8, a: std.mem.Alignment, ret: usize) void {
        const self: *LivePeak = @ptrCast(@alignCast(ctx));
        self.child.rawFree(m, a, ret);
        _ = self.live.fetchSub(m.len, .monotonic);
    }
};

pub fn checkRecordDecodeModes(
    gpa: std.mem.Allocator,
    arena: std.mem.Allocator,
    base: MultiAggArgs,
    path: FastPath,
    column: usize,
) !void {
    const Mode = enum { general, records, declined, replay, mixed_replay };
    const Reader = struct {
        waits: std.atomic.Value(usize) = .init(0),
        abandoned: usize = 0,
        mixed: bool = false,
        fn start(_: *anyopaque) anyerror!void {}
        fn probe(_: *anyopaque, _: usize, _: usize, _: u64, _: u64) anyerror!void {}
        fn ready(_: *anyopaque, _: usize) anyerror!void {}
        fn refuse(ctx: *anyopaque, rg: usize) anyerror!void {
            const self: *@This() = @ptrCast(@alignCast(ctx));
            _ = self.waits.fetchAdd(1, .monotonic);
            if (self.mixed and rg != 0) return;
            return error.ExceededMemoryBudget;
        }
        fn done(_: *anyopaque, _: usize, _: usize) void {}
        fn abandon(ctx: *anyopaque) void {
            const self: *@This() = @ptrCast(@alignCast(ctx));
            self.abandoned += 1;
        }
    };
    var expected: ?anyerror = null;
    var expected_page: ?u32 = null;
    for ([_]usize{ 1, 2, 4, 8, 16 }) |j| for ([_]usize{ 0, 64, 256 }) |window| for (std.enums.values(Mode)) |mode| {
        const replay = mode == .replay or mode == .mixed_replay;
        var budget: core_memory.SharedBudget = .{ .limit = if (mode == .declined)
            (if (base.byte_records) @as(usize, 4 << 20) else 96 << 20)
        else
            1 << 30 };
        var args = base;
        args.parallelism = j;
        args.decode_batch_rows = window;
        args.max_memory = budget.limit;
        args.ledger = &budget;
        args.integer_records = mode != .general and path == .integer_records;
        args.msd_records = mode != .general and path == .msd_records;
        args.byte_records = mode != .general and base.byte_records;
        args.integer_records_min_rows = 0;
        args.msd_records_min_rows = 0;
        var reader: Reader = .{ .mixed = mode == .mixed_replay };
        if (replay) {
            const inputs = try arena.dupe(Input, base.inputs);
            for (inputs) |*input| {
                input.plan_bytes = input.bytes;
                input.gate = .{ .ctx = &reader, .wait = Reader.refuse, .done = Reader.done };
                input.dict_wait = .{ .ctx = &reader, .wait = Reader.ready };
            }
            args.inputs = inputs;
            args.start_reads = .{ .ctx = &reader, .start = Reader.start, .abandon = Reader.abandon };
            args.dict_probe = .{ .ctx = &reader, .add = Reader.probe };
        }
        var taken: std.EnumSet(FastPath) = .{};
        args.taken = &taken;
        if (mode == .records or replay) {
            const explanation = try explainAggregate(gpa, arena, args);
            try testing.expect(explanation.record_path != null);
            try testing.expectEqual(j, explanation.workers);
        } else {
            try testing.expect((try explainAggregate(gpa, arena, args)).record_path == null);
        }
        const err = failure: {
            const attempt = if (mode == .records)
                prepareAggregate(gpa, arena, args, false)
            else
                runMultiAggregate(gpa, arena, args);
            var unexpected = attempt catch |e| break :failure e;
            unexpected.deinit(gpa);
            return error.TestUnexpectedResult;
        };
        try testing.expect(consumer.isDecodeError(err));
        if (mode == .records) try testing.expect(taken.contains(path));
        if (replay) {
            try testing.expect(reader.waits.load(.monotonic) != 0);
            try testing.expectEqual(@as(usize, 1), reader.abandoned);
        }
        const site = consumer.decodeFailure().?;
        try testing.expectEqual(column, site.column);
        try testing.expectEqual(@as(usize, 0), site.first_row);
        try testing.expect(site.rg == &base.metas.?[0].row_groups.items[0]);
        try testing.expect(site.page != null);
        if (expected) |want| {
            try testing.expectEqual(want, err);
            try testing.expectEqual(expected_page, site.page);
        } else {
            expected = err;
            expected_page = site.page;
        }
        try testing.expectEqual(@as(usize, 0), budget.used.load(.monotonic));
    };
}

pub fn runHotCertificateAndFree(gpa: std.mem.Allocator, inputs: []const Input, meta: *const schema.FileMetaData) !void {
    var arena = std.heap.ArenaAllocator.init(gpa);
    defer arena.deinit();
    var result = try runMultiAggregate(gpa, arena.allocator(), .{
        .inputs = inputs,
        .metas = meta[0..1],
        .aggregate = "count(*) AS c",
        .group_by = "k",
        .parallelism = 1,
        .result_order = .{ .keys = &.{.{ .column = 1, .descending = true }}, .limit = 3 },
        .ndv_policy = false,
        .plain_rules = false,
        .dense = false,
        .sketch_topk = .{ .cell_bits = 8, .admit_per_row = 1, .census = false },
    });
    result.deinit(gpa);
}

pub fn countResidualBudgetTrial(
    gpa: std.mem.Allocator,
    bytes: []const u8,
    meta: *const schema.FileMetaData,
    corrupt: bool,
) !void {
    defer consumer.setDecodeFailure(null);
    var arena = std.heap.ArenaAllocator.init(gpa);
    defer arena.deinit();
    const a = arena.allocator();
    const inputs = [_]Input{.{ .name = "count-budget.parquet", .bytes = bytes }};
    const calls = [_]expr_agg.AggCall{.{ .func = .count, .arg = null, .where = null, .alias = "c", .result = .i64 }};
    const keys = [_]expr_ast.Expr{.{ .col_ref = .{ .col_idx = 0, .physical_type = .INT64, .expr_type = .i64 } }};
    const order: expr_ast.ResultOrder = .{ .keys = &.{.{ .column = 1, .descending = true }}, .limit = 2 };
    const plan = try agg_plan.Plan.init(
        a,
        .{ .filter = null, .calls = &calls, .group_keys = &keys },
        meta[0..1],
        .ignore,
    );
    const columns: agg_plan.Columns = .{ .read = &.{true}, .consumed = &.{true}, .preserve = &.{true} };
    const work = [_]WorkItem{.{
        .file = 0,
        .rg = 0,
        .agg_start = 0,
        .agg_len = 1,
        .columns = columns,
        .accumulators = &.{},
    }};
    var budget: core_memory.SharedBudget = .{ .limit = 4096, .block = 1 };
    defer std.debug.assert(budget.used.load(.monotonic) == 0);
    var tables = [_]expr_agg.GroupTable{expr_agg.GroupTable.init(gpa, 0)};
    tables[0].shared = &budget;
    tables[0].fixed_stride = 9;
    defer tables[0].deinit();
    var cursor: std.atomic.Value(usize) = .init(0);
    var workers = [_]Worker{.{
        .gpa = gpa,
        .inputs = &inputs,
        .metas = meta[0..1],
        .work = &work,
        .cursor = &cursor,
        .agg_calls = &calls,
        .plan = &plan,
        .decode_options = .{},
        .group_by_keys = &keys,
        .group_tables = &tables,
        .index = 0,
    }};
    defer releaseRegions(&workers);
    var cells = [_]u32{ 0, 0 };
    var admitted = [_]u64{3};
    var sketches = [_]expr_agg.CountSketch{.{ .cells = &cells, .shift = 63, .admit = &admitted }};
    tables[0].sketch = &sketches[0];
    defer tables[0].sketch = null;
    var bound: CutBoundPlan = .{ .bound = 0, .cells = &admitted };
    var outcome: SketchTopK.Outcome = .ineligible;
    // Retained prefix state and a still-live certificate leave room for a few residual groups, not their growth.
    // The complete plain table fits this same ledger after the trial releases its reservation.
    var prefix: [9]u8 = undefined;
    prefix[0] = 1;
    std.mem.writeInt(i64, prefix[1..9], 7, .little);
    tables[0].sketch = null;
    _ = try tables[0].getOrInsertFixed(&prefix, &calls, 0);
    tables[0].sketch = &sketches[0];
    const held = budget.drawExact(budget.available() - 64).?;
    var retained = true;
    defer if (retained) budget.release(held);
    const accepted = certifiedResidual(
        gpa,
        a,
        &workers,
        &cursor,
        &tables,
        &sketches,
        &.{},
        &calls,
        order,
        0,
        .{ .outcome = &outcome },
        &bound,
    ) catch |err| {
        if (!corrupt) return err;
        try testing.expect(consumer.isDecodeError(err));
        return;
    };
    if (corrupt) return error.TestExpectedError;
    try testing.expect(!accepted);
    try testing.expectEqual(.fell_back, outcome);
    try testing.expectEqual(@as(usize, 0), tables[0].groupCount());
    try testing.expectEqual(held, budget.used.load(.monotonic));
    try testing.expect(workers[0].err == null);
    try testing.expectEqual(@as(i64, 0), workers[0].rows_matched);
    budget.release(held);
    retained = false;
    tables[0].sketch = null;
    try runPhase(&workers, &cursor);
    var want = try runMultiAggregate(gpa, a, .{
        .inputs = &inputs,
        .metas = meta[0..1],
        .aggregate = "count(*) AS c",
        .group_by = "k",
        .max_memory = budget.limit,
        .parallelism = 1,
        .scan_all = true,
        .dense = false,
        .ndv_policy = false,
        .plain_rules = false,
    });
    defer want.deinit(gpa);
    const rows = want.output.group_rows.?;
    try testing.expectEqual(rows.len, tables[0].groupCount());
    var previous: ?u64 = null;
    for (rows) |row| {
        var image: [9]u8 = undefined;
        image[0] = 1;
        std.mem.writeInt(i64, image[1..9], @intCast(row[0].i), .little);
        const id = tables[0].findKey(&image) orelse return error.TestExpectedEqual;
        const first = tables[0].first.items[id];
        if (previous) |p| try testing.expect(first > p);
        previous = first;
        try testing.expectEqual(row[1].i, @as(i128, tables[0].readAcc(id, 0, 1).count));
    }
    try testing.expectEqual(want.rows_matched, workers[0].rows_matched);
}

/// Returning an item really destroys its encoded pages; waiting twice does not refill them.
pub const ReleasingScanWindow = struct {
    bytes: []u8,
    meta: *const schema.FileMetaData,
    waits: std.atomic.Value(usize) = .init(0),
    releases: std.atomic.Value(usize) = .init(0),
    abandoned: usize = 0,
    completed: ?[]std.atomic.Value(usize) = null,

    pub fn wait(ctx: *anyopaque, _: usize) anyerror!void {
        const self: *@This() = @ptrCast(@alignCast(ctx));
        _ = self.waits.fetchAdd(1, .monotonic);
    }

    pub fn release(ctx: *anyopaque, rg: usize, items: usize) void {
        const self: *@This() = @ptrCast(@alignCast(ctx));
        if (self.completed) |counts| {
            if (counts[rg].fetchAdd(1, .acq_rel) + 1 != items) return;
        } else std.debug.assert(items == 1);
        for (self.meta.row_groups.items[rg].columns.items) |chunk| {
            const cm = chunk.meta_data.?;
            const lo: usize = @intCast(cm.dictionary_page_offset orelse cm.data_page_offset);
            @memset(self.bytes[lo..][0..@intCast(cm.total_compressed_size)], 0);
        }
        _ = self.releases.fetchAdd(1, .monotonic);
    }

    pub fn start(_: *anyopaque) anyerror!void {}

    pub fn abandon(ctx: *anyopaque) void {
        const self: *@This() = @ptrCast(@alignCast(ctx));
        self.abandoned += 1;
        @memset(self.bytes, 0);
    }

    pub fn input(self: *@This(), mapped: []const u8) Input {
        return .{
            .name = "releasing.parquet",
            .bytes = self.bytes,
            .plan_bytes = mapped,
            .gate = .{ .ctx = self, .wait = wait, .done = release },
        };
    }
};

pub fn releasingOrderedTrial(
    gpa: std.mem.Allocator,
    bytes: []const u8,
    meta: *const schema.FileMetaData,
    full_rows: []const []const AggValue,
    j: usize,
) !void {
    var arena = std.heap.ArenaAllocator.init(gpa);
    defer arena.deinit();
    const a = arena.allocator();
    for ([_][]const u8{ "count(*) AS c, sum(v) AS s", "sum(v) AS s", "count(*) AS c" }, 0..) |agg, mode| {
        var window: ReleasingScanWindow = .{ .bytes = try a.dupe(u8, bytes), .meta = meta };
        const inputs = [_]Input{window.input(bytes)};
        const order: expr_ast.ResultOrder = if (mode == 0) .{} else .{
            .keys = &.{.{ .column = 1, .descending = true }},
            .limit = 2,
            .offset = 1,
        };
        var args: MultiAggArgs = .{
            .inputs = &inputs,
            .metas = meta[0..1],
            .aggregate = agg,
            .group_by = "k",
            .parallelism = j,
            .min_bytes_per_worker = 1,
            .decode_batch_rows = 2,
            .integer_records = true,
            .integer_records_min_rows = 0,
            .typed_answer = true,
            .scan_all = true,
            .ndv_policy = false,
            .plain_rules = false,
            .dense = false,
            .result_order = order,
            .start_reads = .{
                .ctx = &window,
                .start = ReleasingScanWindow.start,
                .abandon = ReleasingScanWindow.abandon,
            },
        };
        const explanation = try explainAggregate(gpa, a, args);
        const known_null = for (meta.row_groups.items) |rg| {
            if ((rg.columns.items[0].meta_data.?.statistics.?.null_count orelse 0) > 0) break true;
        } else false;
        // NULLs decline the ordered run, but COUNT can now use the same raw owner as SUM and full grouping.
        try testing.expect(explanation.record_path != null);
        var got = try runMultiAggregate(gpa, a, args);
        defer got.deinit(gpa);
        try testing.expectEqual(meta.row_groups.items.len, window.waits.load(.monotonic));
        try testing.expectEqual(meta.row_groups.items.len, window.releases.load(.monotonic));
        try testing.expectEqual(@as(usize, if (known_null) 0 else 1), window.abandoned);
        if (mode == 0) {
            const stream = got.output.integer_stream.?;
            try testing.expectEqual(@as(usize, 0), stream.state.runs.len);
            try testing.expectEqual(full_rows.len, stream.state.group_count);
            var typed = try stream.window(gpa, got.state.group_col_types.?, 0, full_rows.len, j);
            defer typed.deinit(gpa);
            for (full_rows, 0..) |row, i| for (row, typed.columns) |v, col| {
                try testing.expectEqual(v == .null_val, col.i64.defs.len != 0 and col.i64.defs[i] == 0);
                if (v != .null_val) try testing.expectEqual(v.i, col.i64.values[i]);
            };
        } else {
            const mapped = [_]Input{.{ .name = "scan-all.parquet", .bytes = bytes }};
            args.inputs = &mapped;
            args.start_reads = null;
            args.integer_records = false;
            var want = try runMultiAggregate(gpa, a, args);
            defer want.deinit(gpa);
            try got.output.applyResultOrder(gpa, order);
            try want.output.applyResultOrder(gpa, order);
            try testing.expectEqualDeep(want.output.group_rows.?, got.output.group_rows.?);
        }
    }
}

pub fn denseProductScan(with_sum: bool) !void {
    const gpa = testing.allocator;
    var arena = std.heap.ArenaAllocator.init(gpa);
    defer arena.deinit();
    const a = arena.allocator();
    var ints: [256]i32 = undefined;
    var names: [256][]const u8 = undefined;
    var values: [256]i64 = undefined;
    for (&ints, &names, 0..) |*v, *s, i| {
        v.* = @intCast(i % 3);
        values[i] = @as(i64, @intCast(i % 7)) - 3;
        s.* = try std.fmt.allocPrint(a, "name\x00-{d}", .{i % 64});
    }
    const columns = [_]expr_agg.OutputCol{
        .{ .name = "a", .col = .{ .i32 = .{ .values = &ints } }, .parquet_type = .INT32, .nullable = false },
        .{
            .name = "b",
            .col = .{ .string = .{ .values = &names } },
            .parquet_type = .BYTE_ARRAY,
            .nullable = false,
        },
        .{ .name = "v", .col = .{ .i64 = .{ .values = &values } }, .parquet_type = .INT64, .nullable = false },
    };
    const Capture = struct {
        bytes: std.ArrayList(u8) = .empty,
        a: std.mem.Allocator,
        fn write(ctx: *anyopaque, bytes: []const u8) anyerror!void {
            const c: *@This() = @ptrCast(@alignCast(ctx));
            try c.bytes.appendSlice(c.a, bytes);
        }
    };
    var capture: Capture = .{ .a = a };
    _ = try consumer.writeAggregateRows(
        a,
        .{ .ctx = &capture, .write_fn = Capture.write },
        &columns,
        ints.len,
        .SNAPPY,
        .{},
        .{},
    );
    var meta = try metadata.open(a, capture.bytes.items);
    const groups = try a.alloc(schema.RowGroup, 16);
    @memset(groups, meta.row_groups.items[0]);
    meta.row_groups.items = groups;
    meta.num_rows *= 16;
    var args: MultiAggArgs = .{
        .inputs = &.{.{ .name = "product", .bytes = capture.bytes.items }},
        .metas = &.{meta},
        .aggregate = if (with_sum) "count(*) AS c,sum(v) AS s" else "count(*) AS c",
        .group_by = "a,b",
        .filter = "a < 2",
        .parallelism = 1,
        .min_bytes_per_worker = 1,
        .result_order = .{
            .keys = &.{.{ .column = if (with_sum) 3 else 2, .descending = true }},
            .limit = 3,
            .offset = 1,
        },
        .dense = false,
        .max_memory = 4 << 20,
        .sketch_topk = .{ .cell_bits = 12 },
    };
    var want = try runMultiAggregate(gpa, a, args);
    defer want.deinit(gpa);
    try want.output.applyResultOrder(gpa, args.result_order);
    for ([_]usize{ 1, 4, 16 }) |j| for ([_]usize{ 0, 64 }) |window| {
        var taken: std.EnumSet(FastPath) = .{};
        args.parallelism = j;
        args.decode_batch_rows = window;
        args.dense = true;
        args.taken = &taken;
        const explanation = try explainAggregate(gpa, a, args);
        try testing.expectEqual(.plain_hash, explanation.plan.dispatch.strategy);
        try testing.expectEqualStrings("dense", explanation.strategyName());
        try testing.expectEqual(j, explanation.workers);
        if (j == 16 and !with_sum) try testing.expect(explanation.plan.plain_bytes.? > args.max_memory / 8);
        var got = try prepareAggregate(gpa, a, args, false);
        defer got.deinit(gpa);
        try testing.expect(taken.contains(.dense));
        try got.output.applyResultOrder(gpa, args.result_order);
        try testing.expectEqualDeep(want.output.group_rows.?, got.output.group_rows.?);
    };
    if (with_sum) {
        args.result_order = .{};
        args.dense = false;
        args.taken = null;
        var full = try runMultiAggregate(gpa, a, args);
        defer full.deinit(gpa);
        for ([_]usize{ 1, 4, 16 }) |j| for ([_]usize{ 0, 64 }) |window| {
            var taken: std.EnumSet(FastPath) = .{};
            args.parallelism = j;
            args.decode_batch_rows = window;
            args.dense = true;
            args.taken = &taken;
            var got = try prepareAggregate(gpa, a, args, false);
            defer got.deinit(gpa);
            try testing.expect(taken.contains(.dense));
            try testing.expectEqualDeep(full.output.group_rows.?, got.output.group_rows.?);
        };
        return;
    }
    args.encoded = false;
    const legacy = try explainAggregate(gpa, a, args);
    try testing.expectEqual(.sketch, legacy.plan.dispatch.strategy);
    try testing.expectEqualStrings("sketch", legacy.strategyName());
    args.encoded = true;
    var ledger: core_memory.SharedBudget = .{ .limit = args.max_memory };
    const held = ledger.drawExact(args.max_memory - 1).?;
    defer ledger.release(held);
    args.ledger = &ledger;
    var refused: std.EnumSet(FastPath) = .{};
    args.taken = &refused;
    try testing.expectError(error.ExceededMemoryBudget, prepareAggregate(gpa, a, args, false));
    try testing.expect(refused.contains(.plain_rule));
    try testing.expect(!refused.contains(.dense));
    try testing.expectEqual(held, ledger.used.load(.monotonic));
    var chain: Chain = .{ .parts_tried = false };
    const fallback = narrowed(args, args, refused, &chain, false).?;
    try testing.expect(!fallback.plain_rules);
    const after = try explainAggregate(gpa, a, fallback);
    try testing.expectEqual(.sketch, after.plan.dispatch.strategy);
}

pub fn ownedGroupOutputBytes(output: AggOutput) usize {
    var bytes: usize = 0;
    if (output.group_cols) |names| {
        bytes += names.len * @sizeOf([]const u8);
        for (names) |name| bytes += name.len;
    }
    if (output.group_rows) |rows| bytes += rows.len * @sizeOf([]const AggValue);
    bytes += output.slab.len * @sizeOf(AggValue);
    for (output.slab) |cell| if (cell == .s) {
        bytes += cell.s.len;
    };
    return bytes;
}

pub fn packedAllocationTrial(gpa: std.mem.Allocator, base: MultiAggArgs, expected: []const []const AggValue) !void {
    var arena = std.heap.ArenaAllocator.init(gpa);
    defer arena.deinit();
    const bytes = base.inputs[0].bytes;
    var window: ReleasingScanWindow = .{
        .bytes = try arena.allocator().dupe(u8, bytes),
        .meta = &base.metas.?[0],
    };
    const Probe = struct {
        fn add(_: *anyopaque, _: usize, _: usize, _: u64, _: u64) anyerror!void {}
    };
    var input = window.input(bytes);
    input.dict_wait = .{ .ctx = &window, .wait = ReleasingScanWindow.wait };
    var args = base;
    args.inputs = (&input)[0..1];
    args.dict_probe = .{ .ctx = &window, .add = Probe.add };
    args.start_reads = .{ .ctx = &window, .start = ReleasingScanWindow.start, .abandon = ReleasingScanWindow.abandon };
    var got = try runMultiAggregate(gpa, arena.allocator(), args);
    defer got.deinit(gpa);
    try got.output.applyResultOrder(gpa, args.result_order);
    try testing.expectEqualDeep(expected, got.output.group_rows.?);
}

pub fn selectedIdentityReplayTrial(distinct: bool) !void {
    const gpa = testing.allocator;
    var held = std.heap.ArenaAllocator.init(gpa);
    defer held.deinit();
    const a = held.allocator();
    const bytes = (try loadFixture("ci/fixtures/parquet/packed_count_indexed.parquet")) orelse
        return error.SkipZigTest;
    defer gpa.free(bytes);
    const meta = try metadata.open(a, bytes);
    const base: MultiAggArgs = .{
        .inputs = &.{.{ .name = "selected-replay", .bytes = bytes }},
        .metas = &.{meta},
        .aggregate = if (distinct) "count(DISTINCT k) AS d,count(*) AS c" else "count(*) AS c,count(*) AS d",
        .group_by = if (distinct) "b" else "k,b,f",
        .distinct_pairs = if (distinct) .on else .auto,
        .integer_records = false,
        .integer_records_min_rows = 0,
        .byte_records = false,
        .msd_records = false,
        .plain_rules = false,
        .min_bytes_per_worker = 1,
        .max_memory = 512 << 20,
        .result_order = .{
            .keys = &.{.{ .column = if (distinct) 1 else 3, .descending = true }},
            .limit = 3,
            .offset = 2,
        },
    };
    var want = try runMultiAggregate(gpa, a, base);
    defer want.deinit(gpa);
    try want.output.applyResultOrder(gpa, base.result_order);
    const Reader = struct {
        window: ReleasingScanWindow,
        fault: bool,
        changed: bool = false,
        earlier: usize = 0,
        fn probe(_: *anyopaque, _: usize, _: usize, _: u64, _: u64) anyerror!void {}
        fn ready(_: *anyopaque, _: usize) anyerror!void {}
        fn start(_: *anyopaque) anyerror!void {}
        fn wait(ctx: *anyopaque, rg: usize) anyerror!void {
            const r: *@This() = @ptrCast(@alignCast(ctx));
            try ReleasingScanWindow.wait(&r.window, rg);
            if (!r.fault or rg != 15 or r.changed) return;
            r.earlier = r.window.releases.load(.acquire);
            // Planning retained the original dictionary; change only the late streamed copy, after other RGs
            // emitted records. Replay must abandon this view and read the unchanged retained mapping.
            const cm = r.window.meta.row_groups.items[rg].columns.items[0].meta_data.?;
            try testing.expectEqual(.UNCOMPRESSED, cm.codec);
            const offset: usize = @intCast(cm.dictionary_page_offset.?);
            var header = thrift_mod.Reader.init(r.window.bytes[offset..]);
            _ = try schema.PageHeader.read(&header);
            std.mem.writeInt(i64, r.window.bytes[offset + header.pos ..][0..8], 987654321, .little);
            r.changed = true;
        }
        fn done(ctx: *anyopaque, rg: usize, items: usize) void {
            const r: *@This() = @ptrCast(@alignCast(ctx));
            ReleasingScanWindow.release(&r.window, rg, items);
        }
        fn abandon(ctx: *anyopaque) void {
            const r: *@This() = @ptrCast(@alignCast(ctx));
            ReleasingScanWindow.abandon(&r.window);
        }
    };
    for ([_]usize{ 1, 4 }) |jobs| for ([_]usize{ 0, 64 }) |window| for ([_]bool{ false, true }) |fault| {
        var trial = std.heap.ArenaAllocator.init(gpa);
        defer trial.deinit();
        const ta = trial.allocator();
        var reader: Reader = .{
            .window = .{ .bytes = try ta.dupe(u8, bytes), .meta = &meta },
            .fault = fault,
        };
        var input = reader.window.input(bytes);
        input.gate = .{ .ctx = &reader, .wait = Reader.wait, .done = Reader.done };
        input.dict_wait = .{ .ctx = &reader, .wait = Reader.ready };
        var ledger: core_memory.SharedBudget = .{ .limit = base.max_memory };
        var taken: std.EnumSet(FastPath) = .{};
        var args = base;
        args.inputs = (&input)[0..1];
        args.integer_records = true;
        args.parallelism = jobs;
        args.decode_batch_rows = window;
        args.ledger = &ledger;
        args.taken = &taken;
        args.start_reads = .{ .ctx = &reader, .start = Reader.start, .abandon = Reader.abandon };
        args.dict_probe = .{ .ctx = &reader, .add = Reader.probe };
        const explanation = try explainAggregate(gpa, ta, args);
        try testing.expectEqual(
            @as(@FieldType(RecordAdmission, "layout"), if (distinct) .pair_distinct else .packed_count_top_h),
            explanation.record_path.?.layout,
        );
        var got = if (fault)
            try runMultiAggregate(gpa, ta, args)
        else
            try prepareAggregate(gpa, ta, args, false);
        defer got.deinit(gpa);
        if (!fault) try testing.expect(taken.contains(.integer_records));
        try got.output.applyResultOrder(gpa, args.result_order);
        try testing.expectEqualDeep(want.output.group_cols, got.output.group_cols);
        try testing.expectEqualDeep(want.state.group_col_types, got.state.group_col_types);
        try testing.expectEqualDeep(want.output.group_rows, got.output.group_rows);
        try testing.expectEqual(fault, reader.changed);
        if (fault) try testing.expect(reader.earlier > 0);
        try testing.expectEqual(@as(usize, @intFromBool(fault)), reader.window.abandoned);
        try testing.expectEqual(@as(usize, 16), reader.window.releases.load(.monotonic));
        try testing.expect(ledger.used.load(.monotonic) < 1 << 20);
    };
}
