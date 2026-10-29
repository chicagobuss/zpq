const std = @import("std");
const testing = std.testing;
const metadata = @import("../parquet/metadata.zig");
const schema = @import("../schema.zig");
const agg_plan = @import("../agg_plan.zig");
const consumer = @import("../consumer.zig");
const expr_agg = @import("../expr/agg.zig");
const expr_ast = @import("../expr/ast.zig");
const core_memory = @import("../memory.zig");
const huge_pages = @import("../huge_pages.zig");
const partagg = @import("../partagg.zig");
const agg_dense = @import("../expr/agg_dense.zig");
const profile = @import("../../profile.zig");
const scan_types = @import("contracts.zig");
const Input = scan_types.Input;
const WorkItem = scan_types.WorkItem;
const RowGroupGate = scan_types.RowGroupGate;
const BoundOutputColumn = scan_types.BoundOutputColumn;
const BlockMerge = @import("block_merge.zig").BlockMerge;
const WinnerCharge = expr_agg.WinnerCharge;
const loadFixture = @import("test_support.zig").loadFixture;

/// One worker thread's slice of work.
pub fn decodeProjectionOrder(
    a: std.mem.Allocator,
    n: usize,
    calls: []const expr_agg.AggCall,
    keys: ?[]const expr_ast.Expr,
    outputs: ?[]const BoundOutputColumn,
) ![]const usize {
    const ranks = try a.alloc(usize, n);
    @memset(ranks, std.math.maxInt(usize));
    const Builder = struct {
        ranks: []usize,
        mask: []bool,
        next: usize = 0,

        fn add(b: *@This(), e: expr_ast.Expr) void {
            @memset(b.mask, false);
            e.collectColumns(b.mask);
            for (b.mask, 0..) |used, ci| if (used and b.ranks[ci] == std.math.maxInt(usize)) {
                b.ranks[ci] = b.next;
                b.next += 1;
            };
        }
    };
    var b: Builder = .{ .ranks = ranks, .mask = try a.alloc(bool, n) };
    if (outputs) |cols| for (cols) |out| switch (out.source) {
        .key => |ki| b.add(out.derived orelse keys.?[ki]),
        .agg => |ai| if (calls[ai].arg) |arg| b.add(arg),
    };
    if (keys) |ks| for (ks) |key| b.add(key);
    for (calls) |call| if (call.arg) |arg| b.add(arg);
    for (ranks) |*rank| if (rank.* == std.math.maxInt(usize)) {
        rank.* = b.next;
        b.next += 1;
    };
    return ranks;
}

pub fn workerErrorRanks(ctx: *const anyopaque, a: std.mem.Allocator, n: usize) anyerror![]const usize {
    const w: *const Worker = @ptrCast(@alignCast(ctx));
    return decodeProjectionOrder(a, n, w.agg_calls, w.error_group_keys orelse w.group_by_keys, w.error_outputs);
}

pub const FailureProbe = struct {
    pub const Context = struct {
        // Copy the plan: its stack frame has unwound before mapped-input prefix replay.
        plan: agg_plan.Plan,
        metas: []const schema.FileMetaData,
        inputs: []const Input,
        file: usize,
        rg: usize,
        in: consumer.AggRowGroup,
        options: consumer.DecodeOptions,
        calls: []const expr_agg.AggCall,
        scalar: bool,
        original: @import("../decode_order.zig").Failure,
    };
    lock: std.Io.Mutex = .init,
    limit: usize = 0,
    workers: []const Worker = &.{},
    context: ?Context = null,
    /// Prepaid before stream workers start; probes never borrow a live worker's headroom.
    stream_budget: core_memory.SharedBudget = .{ .limit = 0 },

    pub fn reserveStream(p: *FailureProbe, parent: *core_memory.SharedBudget, planned: usize) !void {
        const quota = @min(planned, @min(p.limit / 8, core_memory.lean_decode / 2));
        if (parent.drawExact(quota) == null) return error.ExceededMemoryBudget;
        p.stream_budget = .{ .limit = quota, .parent = parent, .funded = quota };
    }

    pub fn releaseStream(p: *FailureProbe) void {
        std.debug.assert(p.stream_budget.used.load(.monotonic) == 0);
        p.stream_budget.releaseFloor();
        p.stream_budget.parent = null;
    }

    pub fn releaseGates(p: *FailureProbe) void {
        for (p.workers) |w| if (w.failed_gate) |gate| gate.done(gate.ctx, w.failed_rg, w.items_per_rg);
        p.workers = &.{};
    }

    pub fn capture(p: *FailureProbe, workers: []const Worker, wi: usize) void {
        const w = &workers[wi];
        // An unrescanable stream must probe before giving its failed row group back to the producer.
        if (w.inputs[w.work[w.failed_item].file].gate != null and
            w.inputs[w.work[w.failed_item].file].plan_bytes == null) return;
        p.context = contextOf(w);
    }

    pub fn contextOf(w: *const Worker) Context {
        const site = w.failure.?;
        const item = w.work[w.failed_item];
        const decision = w.plan.decide(site.rg, &w.metas[item.file]);
        return .{
            .plan = w.plan.*,
            .metas = w.metas,
            .inputs = w.inputs,
            .file = item.file,
            .rg = item.rg,
            .in = .{
                .rg = site.rg,
                .meta = &w.metas[item.file],
                .src = .{ .bytes = w.inputs[item.file].plan_bytes orelse w.inputs[item.file].bytes, .byte_origin = 0 },
                .filter = w.plan.filterFor(decision),
                .read = w.plan.columns.forRowGroup(decision),
                .consumed = w.plan.columns.consumed,
                .error_ranks = .{ .ctx = w, .make = workerErrorRanks },
            },
            .options = w.decode_options,
            .calls = w.agg_calls,
            .scalar = w.group_by_keys == null,
            .original = .{ .err = w.err.?, .site = site },
        };
    }
};

pub const Worker = struct {
    gpa: std.mem.Allocator,
    inputs: []const Input,
    metas: []const schema.FileMetaData,
    /// The FULL work list, shared by every worker; items are claimed through `cursor`. Row-group cost varies too much
    /// — size, encoding, whether a filter prunes them — for a static split not to leave workers idle.
    work: []const WorkItem,
    /// Relaxed ordering suffices: this only hands out disjoint indices, and the post-join merge is the real
    /// synchronization edge.
    cursor: *std.atomic.Value(usize),
    /// Set once a pass is abandoned (`Census.distinct`): the worker claims no more items.
    stop: ?*const std.atomic.Value(bool) = null,
    /// What a sketch's count pass reads: the outer filter's columns and the keys, no aggregate's (`Plan.forCalls`).
    count_columns: ?agg_plan.Columns = null,
    agg_calls: []const expr_agg.AggCall,
    plan: *const agg_plan.Plan,
    decode_options: consumer.DecodeOptions,
    expr_batch_rows: ?usize = null,
    decode_batch_rows: usize = 0,
    auto_decode_batches: bool = false,
    cache_hints: ?*consumer.CacheHints = null,
    compaction: ?*consumer.CompactionControl = null,
    error_outputs: ?[]const BoundOutputColumn = null,
    /// Original bindings, before dependent-key removal, including keys absent from SELECT.
    error_group_keys: ?[]const expr_ast.Expr = null,
    scan_workers: usize = 1,
    failure_probe: ?*FailureProbe = null,
    failed_gate: ?RowGroupGate = null,
    failed_rg: usize = 0,
    failed_item: usize = 0,
    /// The failing work item, also for refusals before a decoder installs its site. Null means unlocated.
    failure_work: ?usize = null,
    timings: consumer.Timings = .{},
    rows_in: i64 = 0,
    rows_scanned: i64 = 0,
    rows_matched: i64 = 0,
    /// Where this worker's decode error was raised (`consumer.decodeFailure`), handed to the reporting thread.
    failure: ?consumer.DecodeSite = null,
    rgs_in: usize = 0,
    rgs_pruned: usize = 0,
    rgs_full_match: usize = 0,
    group_by_keys: ?[]const expr_ast.Expr = null,
    /// GROUP BY tables, shared by every worker: one per worker when unordered (this worker's is `index`), else one per
    /// block of `blocks`.
    group_tables: []expr_agg.GroupTable = &.{},
    /// Shared partitioned tables, with this worker's pre-aggregation state. Null keeps today's per-worker table.
    part: ?*partagg.PartAgg = null,
    local: partagg.Local = .{},

    /// Grouped count(DISTINCT) as pairs instead of sets (`expr_agg.DistinctPairs`), for this worker's table.
    pairs: ?*expr_agg.DistinctPairs = null,
    /// This worker's lanes over a retained key domain (`agg_dense`); row groups it cannot translate use the table.
    dense: ?*agg_dense.Lanes = null,
    records: ?*const consumer.RecordSink = null,
    record_starts: []const u32 = &.{},
    /// Whether this worker has decided if its sets move to pairs.
    pairs_decided: bool = false,
    /// Values its sets took (`DistinctPairs.seen`) at which they are judged next, and how many more judgments a
    /// `.wait` may buy.
    pairs_pilot_rows: u64 = 0,
    pairs_rechecks: u8 = 0,
    /// Ordered GROUP BY: the work list split into fixed blocks, block `b` = `work[blocks[b]..blocks[b + 1]]`, claimed
    /// whole through `block_cursor` and folded in order into `group_tables[b]`.
    blocks: ?[]const usize = null,
    block_cursor: ?*std.atomic.Value(usize) = null,
    /// With `blocks`: where finished blocks merge, in block order.
    block_merge: ?*BlockMerge = null,
    /// Work items per row group (every row group gets the same number), for `Input.gate`.
    items_per_rg: usize = 1,
    /// This worker's decode memory, mapped by its first pass and kept across the passes over `workers` (a sketch's
    /// count and admit passes, a census), so a later pass reuses faulted pages instead of zeroing fresh ones. Only the
    /// owning thread touches it during a pass; `prepareAggregate` unmaps it after the last.
    region: ?huge_pages.Region = null,
    /// The decode scratch's (page and dictionary buffers, kept across row groups), beside `region`, which the row
    /// group arena has to itself (`consumer.RowGroupArena.initOn`). Drawn from `ledger` as `region` is.
    scratch_region: ?huge_pages.Region = null,
    /// A single final pass can retire its regions on the owning worker instead of a serial post-join tail.
    retire_after_run: bool = false,
    retired_decode_held: bool = false,
    decode_budget: ?*consumer.DecodeBudget = null,
    /// The query's ledger, which the region and `charged` draw from; null draws nothing.
    ledger: ?*expr_agg.SharedBudget = null,
    /// Memory a row group decodes on beside the region (staged, sliced), drawn from `ledger`.
    charged: ?core_memory.Charged = null,
    /// A scalar query's string min/max winners, each copy drawn from the query's ledger before it is made.
    winners: ?*WinnerCharge = null,
    err: ?anyerror = null,
    profiler: ?*profile.Profiler = null,
    index: usize = 0,
};

test "stream probe funding is independent of peer ledger pressure and unwinds every refusal" {
    const bytes = (try loadFixture("ci/fixtures/parquet/plain_values_corrupt.parquet")) orelse return error.SkipZigTest;
    defer testing.allocator.free(bytes);
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const meta = try metadata.open(arena.allocator(), bytes);
    const in: consumer.AggRowGroup = .{
        .rg = &meta.row_groups.items[0],
        .meta = &meta,
        .src = .{ .bytes = bytes, .byte_origin = 0 },
        .filter = null,
        .read = &.{ true, true },
        .consumed = &.{ true, true },
    };
    const original: @import("../decode_order.zig").Failure = .{
        .err = error.UnexpectedPage,
        .site = .{ .rg = in.rg, .column = 1, .page = 0 },
    };
    for ([_]usize{ 64 << 10, 1 << 20 }) |limit| {
        var expected: ?consumer.DecodeSite = null;
        // Both orders: an idle peer, then one holding every unreserved byte, then idle again.
        for ([_]bool{ false, true, false }) |pressure| {
            var root: core_memory.SharedBudget = .{ .limit = limit };
            var probe: FailureProbe = .{ .limit = limit };
            try probe.reserveStream(&root, 128 << 10);
            const held = if (pressure) root.available() else 0;
            _ = root.drawExact(held).?;
            var charge: core_memory.Charged = .{ .child = testing.allocator, .budget = &probe.stream_budget };
            const storage = try charge.allocator().alloc(u8, 4096);
            try testing.expectEqual(@as(usize, 4096), probe.stream_budget.used.load(.monotonic));
            charge.allocator().free(storage);
            const found = consumer.canonicalDecodeFailure(charge.allocator(), in, .{}, &.{}, false, original);
            try testing.expectEqual(error.UnexpectedPage, found.err);
            if (expected) |want| try testing.expectEqualDeep(want, found.site) else expected = found.site;
            try testing.expectEqual(@as(usize, 0), probe.stream_budget.used.load(.monotonic));
            root.release(held);
            probe.releaseStream();
            try testing.expectEqual(@as(usize, 0), root.used.load(.monotonic));
            try testing.expect(root.peak.load(.monotonic) <= limit);
        }
    }
    var small: core_memory.SharedBudget = .{ .limit = 1 };
    var probe: FailureProbe = .{ .limit = 1 << 20 };
    try testing.expectError(error.ExceededMemoryBudget, probe.reserveStream(&small, 128 << 10));
    try testing.expectEqual(@as(usize, 0), small.used.load(.monotonic));
}

