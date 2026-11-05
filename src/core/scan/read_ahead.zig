//! Reads a mapped scan's planned column chunks ahead of its workers.
//!
//! A scan over the plain mapping (the cut-steered shapes, which make several passes over the same pages, or any
//! input the local-I/O choice leaves mapped) otherwise reads each chunk when a worker decodes it: one synchronous
//! fault and its read-around at a time per worker, so a cold scan runs at a queue depth of about its worker count
//! and decoders wait on the disk. Here a worker that claims a work item advises the chunks the next items of the
//! same pass will decode (`MADV_WILLNEED`), so their pages arrive while it decodes.
//!
//! Bounded on every side, from run-time facts only:
//! - what: exactly the chunks of the columns this pass decodes for each item, nothing between them;
//! - how far: a few items per worker ahead of the latest claim, and never more than `window` bytes, which the caller
//!   sizes from the query's budget (`Config`);
//! - how: in blocks of the device's read-ahead (`Config.block`): one advice call reads no more than that;
//! - who: one caller at a time (the others go on decoding): several advising callers contend on the page cache;
//! - when: only for pages not already resident (`mincore`), so a hot scan issues no reads and makes no advice.
//!
//! Advice changes when pages arrive, never what is decoded: answers and refusals are those of the mapping.
const std = @import("std");
const builtin = @import("builtin");
const schema = @import("../schema.zig");
const agg_plan = @import("../agg_plan.zig");
const contracts = @import("contracts.zig");
const WorkItem = contracts.WorkItem;

/// What the local-I/O choice hands the scan (`input.chooseLocalIo`).
pub const Config = struct {
    /// Bytes advised and not yet claimed, at most: a share of the query's budget, never past the host's memory.
    window: u64,
    /// The device's read-ahead in bytes: the most one advice call moves.
    block: usize,
    /// Advise resident pages too (`ZPQ_IO_ADVISE=always`): coverage for the advice path on inputs a test leaves
    /// resident. Never chosen by the engine.
    force: bool = false,
};

/// Tests: the most bytes any pass held advised ahead of its claims.
pub var test_peak_ahead: std.atomic.Value(u64) = .init(0);

/// Items advised ahead of the latest claim, per worker: the one each worker decodes next, and one more in flight.
const ahead_per_worker = 2;

pub const ReadAhead = struct {
    config: Config,
    work: []const WorkItem,
    metas: []const schema.FileMetaData,
    plan: *const agg_plan.Plan,
    /// Each input's mapping, indexed like the scan's inputs: what the advice and residency checks address.
    sources: []const []const u8,
    lookahead: usize,
    /// Held by the one caller advising; the others skip.
    busy: std.atomic.Value(bool) = .init(false),
    /// The next item to consider. Only the holder of `busy` touches it, and `reset` between passes.
    next: usize = 0,
    /// The row group advised last, so the items a row group was split into advise it once.
    last: ?struct { file: usize, rg: usize } = null,
    /// Bytes this scan advised, and the item chunks it found resident already (profile counts).
    advised: std.atomic.Value(u64) = .init(0),
    resident: std.atomic.Value(u64) = .init(0),

    pub fn init(
        config: Config,
        work: []const WorkItem,
        metas: []const schema.FileMetaData,
        plan: *const agg_plan.Plan,
        sources: []const []const u8,
        workers: usize,
    ) ReadAhead {
        return .{
            .config = config,
            .work = work,
            .metas = metas,
            .plan = plan,
            .sources = sources,
            .lookahead = @max(1, workers) * ahead_per_worker,
        };
    }

    /// A scan's read-ahead over `inputs`, or null when it has none: no `config`, or an input read through a gate (the
    /// local reader, an S3 window), whose reads are planned already.
    pub fn forScan(
        arena: std.mem.Allocator,
        config: ?Config,
        inputs: []const contracts.Input,
        work: []const WorkItem,
        metas: []const schema.FileMetaData,
        plan: *const agg_plan.Plan,
        workers: usize,
    ) !?ReadAhead {
        const c = config orelse return null;
        const sources = try arena.alloc([]const u8, inputs.len);
        for (inputs, sources) |in, *src| {
            if (in.gate != null) return null;
            src.* = in.plan_bytes orelse in.bytes;
        }
        return init(c, work, metas, plan, sources, workers);
    }

    /// A pass over the work list begins, decoding `columns` for every item (null: each item's own): advise its first
    /// items now, so the workers about to claim them at once do not each fault theirs in. Before its workers start.
    pub fn start(self: *ReadAhead, columns: ?agg_plan.Columns) void {
        self.next = 0;
        self.last = null;
        self.advance(0, columns);
    }

    /// Item `idx` was claimed. `columns`: what this pass decodes for every item (a sketch's count pass reads only its
    /// keys), or null for each item's own columns.
    pub fn claimed(self: *ReadAhead, idx: usize, columns: ?agg_plan.Columns) void {
        self.advance(idx + 1, columns);
    }

    /// Advise items from `from` (the first not yet claimed) up to `lookahead` of them and `window` bytes.
    fn advance(self: *ReadAhead, from: usize, columns: ?agg_plan.Columns) void {
        if (builtin.os.tag != .linux) return;
        if (self.busy.swap(true, .acquire)) return;
        defer self.busy.store(false, .release);
        if (self.next < from) self.next = from;
        const stop = @min(self.work.len, from +| self.lookahead);
        // What earlier calls already advised past the claims counts against the window.
        var ahead: u64 = 0;
        if (from < stop) for (self.work[from..@min(self.next, stop)]) |item| {
            ahead +|= self.itemBytes(item, columns);
        };
        while (self.next < stop) : (self.next += 1) {
            const item = self.work[self.next];
            const bytes = self.itemBytes(item, columns);
            if (ahead > 0 and ahead +| bytes > self.config.window) break;
            self.advise(item, columns);
            ahead +|= bytes;
            if (builtin.is_test) _ = test_peak_ahead.fetchMax(ahead, .monotonic);
        }
    }

    fn readColumns(self: *const ReadAhead, item: WorkItem, columns: ?agg_plan.Columns) ?[]const bool {
        const meta = &self.metas[item.file];
        const rg = &meta.row_groups.items[item.rg];
        const decision = self.plan.decide(rg, meta);
        if (decision == .skip or rg.num_rows == 0) return null;
        return (columns orelse item.columns).forRowGroup(decision);
    }

    fn itemBytes(self: *const ReadAhead, item: WorkItem, columns: ?agg_plan.Columns) u64 {
        const read = self.readColumns(item, columns) orelse return 0;
        const rg = &self.metas[item.file].row_groups.items[item.rg];
        var total: u64 = 0;
        for (read, 0..) |needed, ci| {
            if (!needed or ci >= rg.columns.items.len) continue;
            const cm = rg.columns.items[ci].meta_data orelse continue;
            total +|= std.math.cast(u64, cm.total_compressed_size) orelse 0;
        }
        return total;
    }

    fn advise(self: *ReadAhead, item: WorkItem, columns: ?agg_plan.Columns) void {
        if (self.last) |l| if (l.file == item.file and l.rg == item.rg) return;
        self.last = .{ .file = item.file, .rg = item.rg };
        const read = self.readColumns(item, columns) orelse return;
        const source = self.sources[item.file];
        if (source.len == 0) return;
        const rg = &self.metas[item.file].row_groups.items[item.rg];
        for (read, 0..) |needed, ci| {
            if (!needed or ci >= rg.columns.items.len) continue;
            const cm = rg.columns.items[ci].meta_data orelse continue;
            const at = std.math.cast(usize, cm.dictionary_page_offset orelse cm.data_page_offset) orelse continue;
            const len = std.math.cast(usize, cm.total_compressed_size) orelse continue;
            // A chunk outside the mapping fails its own decode; advice has nothing to add.
            if (at > source.len or len > source.len - at) continue;
            const moved = adviseRange(source[at..][0..len], self.config.block, self.config.force);
            if (moved == 0) _ = self.resident.fetchAdd(len, .monotonic) else _ = self.advised.fetchAdd(moved, .monotonic);
        }
    }
};

/// `MADV_WILLNEED` over the pages of `bytes` that are not resident (all of them with `force`), in `block`-sized calls;
/// the bytes advised. Advisory only: a refusal or an unsupported mapping keeps the ordinary fault path.
pub fn adviseRange(bytes: []const u8, block: usize, force: bool) u64 {
    if (builtin.os.tag != .linux or bytes.len == 0) return 0;
    const page = std.heap.pageSize();
    const lo = std.mem.alignBackward(usize, @intFromPtr(bytes.ptr), page);
    const hi = std.mem.alignForward(usize, @intFromPtr(bytes.ptr) + bytes.len, page);
    const step = std.mem.alignForward(usize, @max(block, page), page);
    var moved: u64 = 0;
    var at = lo;
    while (at < hi) {
        const len = @min(step, hi - at);
        if (force or !allResident(at, len, page)) {
            _ = std.os.linux.madvise(@ptrFromInt(at), len, std.os.linux.MADV.WILLNEED);
            moved += len;
        }
        at += len;
    }
    return moved;
}

/// Whether every page of the `len` bytes at `addr` (page-aligned) is in the page cache. A failed `mincore` says no.
fn allResident(addr: usize, len: usize, page: usize) bool {
    var vec: [256]u8 = undefined;
    var at = addr;
    const end = addr + len;
    while (at < end) {
        const n = @min(vec.len, (end - at + page - 1) / page);
        const rc = std.os.linux.mincore(@ptrFromInt(at), n * page, &vec);
        if (std.os.linux.errno(rc) != .SUCCESS) return false;
        for (vec[0..n]) |v| if (v & 1 == 0) return false;
        at += n * page;
    }
    return true;
}

test "adviseRange: a resident range is left alone, and a range is advised in blocks over its pages" {
    if (builtin.os.tag != .linux) return error.SkipZigTest;
    const page = std.heap.pageSize();
    // Page-aligned anonymous memory: the page allocator maps it.
    const buf = try std.heap.page_allocator.alloc(u8, 8 * page);
    defer std.heap.page_allocator.free(buf);
    @memset(buf, 1);
    // Touched anonymous pages are resident: nothing to read.
    try std.testing.expectEqual(@as(u64, 0), adviseRange(buf[page / 2 .. 5 * page], page, false));
    // Forced, the same range advises its five pages, two to a call.
    try std.testing.expectEqual(@as(u64, 5 * page), adviseRange(buf[page / 2 .. 5 * page], 2 * page, true));
}

test "a pass advises its first items before any claim, then keeps a lookahead of items within the window" {
    const testing = std.testing;
    const metadata = @import("../parquet/metadata.zig");
    const expr_parser = @import("../expr/parser.zig");
    const expr_ast = @import("../expr/ast.zig");
    const bytes = (try @import("test_support.zig").loadFixture("ci/fixtures/parquet/topk_skew.parquet")) orelse
        return error.SkipZigTest;
    defer testing.allocator.free(bytes);
    var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_state.deinit();
    const a = arena_state.allocator();
    const meta = try metadata.open(a, bytes);
    const metas = [_]schema.FileMetaData{meta};
    const keys = [_]expr_ast.Expr{try expr_parser.parseExprOnly(a, "k", &meta, null)};
    const plan = try agg_plan.Plan.init(a, .{ .calls = &.{}, .group_keys = &keys, .filter = null }, &metas, .ignore);
    const columns = try plan.forCalls(a, &.{});
    const n = meta.row_groups.items.len;
    const work = try a.alloc(WorkItem, n);
    for (work, 0..) |*w, rg| w.* = .{ .file = 0, .rg = rg, .agg_start = 0, .agg_len = 0, .columns = columns, .accumulators = &.{} };
    const sources = [_][]const u8{bytes};
    var smallest: u64 = std.math.maxInt(u64);
    var largest: u64 = 0;
    for (meta.row_groups.items) |rg| {
        const size: u64 = @intCast(rg.columns.items[0].meta_data.?.total_compressed_size);
        smallest = @min(smallest, size);
        largest = @max(largest, size);
    }
    const workers = 2;
    // A window wider than the lookahead: the lookahead binds. One no two items fit in: the window does.
    for ([_]u64{ 1 << 30, smallest + 1 }) |window| {
        var ra: ReadAhead = .init(.{ .window = window, .block = std.heap.pageSize(), .force = true }, work, &metas, &plan, &sources, workers);
        test_peak_ahead.store(0, .monotonic);
        ra.start(null);
        const first = ra.next;
        if (builtin.os.tag != .linux) return;
        if (window > n * largest) {
            try testing.expectEqual(@min(n, workers * ahead_per_worker), first);
        } else {
            try testing.expectEqual(@as(usize, 1), first);
        }
        for (0..n) |idx| {
            ra.claimed(idx, null);
            // Never advised past the lookahead, and never so far that the window is exceeded past one item.
            try testing.expect(ra.next <= idx + 1 + ra.lookahead);
            try testing.expect(ra.next >= @min(n, idx + 2));
        }
        try testing.expectEqual(n, ra.next);
        try testing.expect(test_peak_ahead.load(.monotonic) <= @max(window, largest));
        try testing.expect(ra.advised.load(.monotonic) > 0);
    }
}
