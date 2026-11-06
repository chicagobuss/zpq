//! Reads a mapped scan's planned column chunks ahead of its workers.
//!
//! A scan over the plain mapping (the cut-steered shapes, which make several passes over the same pages, or any
//! input the local-I/O choice leaves mapped) otherwise reads each chunk when a worker decodes it: one synchronous
//! fault and its read-around at a time per worker, so a cold scan runs at a queue depth of about its worker count
//! and decoders wait on the disk. Here a worker that claims a work item advises the chunks the next items of the
//! same pass will decode (`MADV_WILLNEED`), so their pages arrive while it decodes.
//!
//! Bounded on every side, from run-time facts only:
//! - what: the chunks of the columns this pass decodes for each item, nothing between them but the pages a chunk's
//!   first and last bytes share with its neighbours (advice is page-granular, and a fault there reads the same page);
//! - how far: a few items per worker ahead of the latest claim, and a hard cap on the bytes advised and not yet
//!   claimed: the caller's window (`Config`), or what the query's ledger has free when it is less, whichever is
//!   smaller each time it advances. The cap holds within an item too: a cursor advises an item block by block and
//!   stops at the cap mid-chunk, the rest left to the next claim or to demand faults. A cap below one page advises
//!   nothing. Advice never waits for room: it is optional, and a fault reads what it declined;
//! - how: in blocks of the device's read-ahead (`Config.block`): one advice call reads no more than that;
//! - who: one caller at a time (the others go on decoding): several advising callers contend on the page cache;
//! - when: only for pages not already resident (`mincore`), so a hot scan issues no reads and makes no advice.
//!
//! What a claim releases: the bytes advised for every item up to it, which a worker is decoding or has decoded.
//! Advice changes when pages arrive, never what is decoded: answers and refusals are those of the mapping.
const std = @import("std");
const builtin = @import("builtin");
const schema = @import("../schema.zig");
const agg_plan = @import("../agg_plan.zig");
const core_memory = @import("../memory.zig");
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

/// Tests: the most bytes any pass held advised ahead of its claims, page-rounded.
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
    /// The query's ledger: the cap never passes what it has free (`SharedBudget.available`). Null: the window alone.
    ledger: ?*core_memory.SharedBudget = null,
    /// Page-rounded bytes advised for each item, given back to the cap when the item is claimed.
    item_bytes: []u64,
    /// Held by the one caller advising; the others skip. Everything below it is the holder's.
    busy: std.atomic.Value(bool) = .init(false),
    /// One past the latest claimed item: claims arrive while another caller advises, and are released at its next turn.
    claimed_hi: std.atomic.Value(usize) = .init(0),
    /// The cursor: the next bytes to advise are chunk `col` of item `item`, from `offset` into it.
    item: usize = 0,
    col: usize = 0,
    offset: usize = 0,
    /// Items before `released` are claimed, and their advice no longer counts.
    released: usize = 0,
    /// Page-rounded bytes advised for items not yet claimed: never past the cap.
    ahead: u64 = 0,
    /// The row group advised last and the item that advised it, so the items a row group was split into advise it
    /// once (and an item the cap stopped before its first block is not taken for one of them on return).
    last: ?struct { file: usize, rg: usize, item: usize } = null,
    /// Page-rounded bytes this scan advised, and those it found resident already (profile counts).
    advised: std.atomic.Value(u64) = .init(0),
    resident: std.atomic.Value(u64) = .init(0),

    pub fn init(
        config: Config,
        work: []const WorkItem,
        metas: []const schema.FileMetaData,
        plan: *const agg_plan.Plan,
        sources: []const []const u8,
        workers: usize,
        ledger: ?*core_memory.SharedBudget,
        item_bytes: []u64,
    ) ReadAhead {
        std.debug.assert(item_bytes.len == work.len);
        @memset(item_bytes, 0);
        return .{
            .config = config,
            .work = work,
            .metas = metas,
            .plan = plan,
            .sources = sources,
            .lookahead = @max(1, workers) * ahead_per_worker,
            .ledger = ledger,
            .item_bytes = item_bytes,
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
        ledger: ?*core_memory.SharedBudget,
    ) !?ReadAhead {
        const c = config orelse return null;
        const sources = try arena.alloc([]const u8, inputs.len);
        for (inputs, sources) |in, *src| {
            if (in.gate != null) return null;
            src.* = in.plan_bytes orelse in.bytes;
        }
        return init(c, work, metas, plan, sources, workers, ledger, try arena.alloc(u64, work.len));
    }

    /// A pass over the work list begins, decoding `columns` for every item (null: each item's own): advise its first
    /// items now, so the workers about to claim them at once do not each fault theirs in. Before its workers start.
    pub fn start(self: *ReadAhead, columns: ?agg_plan.Columns) void {
        self.item = 0;
        self.col = 0;
        self.offset = 0;
        self.released = 0;
        self.ahead = 0;
        self.last = null;
        self.claimed_hi.store(0, .monotonic);
        @memset(self.item_bytes, 0);
        self.advance(columns);
    }

    /// Item `idx` was claimed. `columns`: what this pass decodes for every item (a sketch's count pass reads only its
    /// keys), or null for each item's own columns.
    pub fn claimed(self: *ReadAhead, idx: usize, columns: ?agg_plan.Columns) void {
        _ = self.claimed_hi.fetchMax(idx + 1, .monotonic);
        self.advance(columns);
    }

    /// The most bytes advised and not yet claimed, now: the window, or what the ledger has free when that is less.
    fn cap(self: *const ReadAhead) u64 {
        const free: u64 = if (self.ledger) |l| l.available() else std.math.maxInt(u64);
        return @min(self.config.window, free);
    }

    /// Release what claims have taken, then advise block by block up to the lookahead and the cap.
    fn advance(self: *ReadAhead, columns: ?agg_plan.Columns) void {
        if (builtin.os.tag != .linux) return;
        if (self.busy.swap(true, .acquire)) return;
        defer self.busy.store(false, .release);
        const hi = @min(self.claimed_hi.load(.monotonic), self.work.len);
        while (self.released < hi) : (self.released += 1) self.ahead -|= self.item_bytes[self.released];
        // Never advise what a worker already claimed: its own decode faults whatever is missing.
        if (self.item < self.released) {
            self.item = self.released;
            self.col = 0;
            self.offset = 0;
        }
        const limit = self.cap();
        const page = std.heap.pageSize();
        // A block no larger than the cap, so the cap can be met exactly; below one page there is nothing to advise.
        const step = std.mem.alignBackward(u64, @min(@as(u64, @max(self.config.block, page)), limit), page);
        if (step == 0) return;
        const stop = @min(self.work.len, self.released +| self.lookahead);
        while (self.item < stop) {
            const range = self.nextRange(columns) orelse continue;
            // Page-rounded: the pages a block touches are what advice reads and what the cap counts.
            const lo = std.mem.alignBackward(usize, @intFromPtr(range.ptr), page);
            const end = std.mem.alignForward(usize, @intFromPtr(range.ptr) + range.len, page);
            const len: usize = @intCast(@min(step, end - lo));
            if (!self.config.force and allResident(lo, len, page)) {
                _ = self.resident.fetchAdd(len, .monotonic);
            } else {
                if (self.ahead + len > limit) break;
                _ = std.os.linux.madvise(@ptrFromInt(lo), len, std.os.linux.MADV.WILLNEED);
                self.ahead += len;
                self.item_bytes[self.item] += len;
                _ = self.advised.fetchAdd(len, .monotonic);
                if (builtin.is_test) _ = test_peak_ahead.fetchMax(self.ahead, .monotonic);
            }
            // Past this block: the chunk's next block, or its next chunk.
            const chunk_lo = @intFromPtr(range.ptr) - self.offset;
            const done = lo + len - chunk_lo;
            if (done >= range.len + self.offset) {
                self.col += 1;
                self.offset = 0;
            } else self.offset = done;
        }
    }

    /// The rest of the chunk at the cursor (from `offset`), moving the cursor past items and columns with nothing to
    /// advise. Null when it moved without finding one this call; the caller checks the item bound again.
    fn nextRange(self: *ReadAhead, columns: ?agg_plan.Columns) ?[]const u8 {
        const item = self.work[self.item];
        const read = blk: {
            if (self.col == 0 and self.offset == 0) {
                // The items a row group was split into advise it once, with the first of them.
                if (self.last) |l| if (l.file == item.file and l.rg == item.rg and l.item != self.item) break :blk null;
                self.last = .{ .file = item.file, .rg = item.rg, .item = self.item };
            }
            break :blk self.readColumns(item, columns);
        };
        const source = self.sources[item.file];
        const rg = &self.metas[item.file].row_groups.items[item.rg];
        if (read) |r| if (source.len != 0) while (self.col < r.len and self.col < rg.columns.items.len) : ({
            self.col += 1;
            self.offset = 0;
        }) {
            if (!r[self.col]) continue;
            const cm = rg.columns.items[self.col].meta_data orelse continue;
            const at = std.math.cast(usize, cm.dictionary_page_offset orelse cm.data_page_offset) orelse continue;
            const len = std.math.cast(usize, cm.total_compressed_size) orelse continue;
            // A chunk outside the mapping fails its own decode; advice has nothing to add.
            if (at > source.len or len > source.len - at or self.offset >= len) continue;
            return source[at + self.offset .. at + len];
        };
        self.item += 1;
        self.col = 0;
        self.offset = 0;
        return null;
    }

    fn readColumns(self: *const ReadAhead, item: WorkItem, columns: ?agg_plan.Columns) ?[]const bool {
        const meta = &self.metas[item.file];
        const rg = &meta.row_groups.items[item.rg];
        const decision = self.plan.decide(rg, meta);
        if (decision == .skip or rg.num_rows == 0) return null;
        return (columns orelse item.columns).forRowGroup(decision);
    }
};

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

/// Tests: one row group of one chunk, `len` bytes at `offset` of `bytes`, read by every item.
const OneChunk = struct {
    chunks: [1]schema.ColumnChunk,
    groups: [1]schema.RowGroup,
    metas: [1]schema.FileMetaData,
    mask: [1]bool = .{true},
    plan: agg_plan.Plan,
    work: [1]WorkItem,
    item_bytes: [1]u64 = .{0},

    fn init(self: *OneChunk, offset: usize, len: usize) void {
        self.mask = .{true};
        self.chunks = .{.{
            .file_path = null,
            .file_offset = 0,
            .meta_data = .{
                .type = .INT64,
                .encodings = .empty,
                .path_in_schema = .empty,
                .codec = .UNCOMPRESSED,
                .num_values = 1,
                .total_uncompressed_size = @intCast(len),
                .total_compressed_size = @intCast(len),
                .data_page_offset = @intCast(offset),
                .index_page_offset = null,
                .dictionary_page_offset = null,
            },
        }};
        self.groups = .{.{ .columns = .fromOwnedSlice(&self.chunks), .total_byte_size = @intCast(len), .num_rows = 1 }};
        self.metas = .{.{ .version = 2, .schema = .empty, .num_rows = 1, .created_by = null, .row_groups = .fromOwnedSlice(&self.groups) }};
        const columns: agg_plan.Columns = .{ .read = &self.mask, .consumed = &self.mask, .preserve = &self.mask };
        self.plan = .{
            .query = .{ .filter = null, .calls = &.{}, .group_keys = null },
            .stats = .ignore,
            .filter_cols = &self.mask,
            .columns = columns,
            .cols_stat_pruned = 0,
        };
        self.work = .{.{ .file = 0, .rg = 0, .agg_start = 0, .agg_len = 0, .columns = columns, .accumulators = &.{} }};
    }

    fn readAhead(self: *OneChunk, sources: []const []const u8, window: u64, workers: usize, ledger: ?*core_memory.SharedBudget) ReadAhead {
        const page = std.heap.pageSize();
        return .init(.{ .window = window, .block = page, .force = true }, &self.work, &self.metas, &self.plan, sources, workers, ledger, &self.item_bytes);
    }
};

test "the cap holds within an oversized first item: advice stops mid-chunk, before any claim" {
    if (builtin.os.tag != .linux) return error.SkipZigTest;
    const page = std.heap.pageSize();
    const bytes = try std.heap.page_allocator.alloc(u8, 64 * page);
    defer std.heap.page_allocator.free(bytes);
    @memset(bytes, 1);
    const sources = [_][]const u8{bytes};
    var one: OneChunk = undefined;
    one.init(0, bytes.len);
    for ([_]usize{ 1, 4, 16 }) |workers| for ([_]u64{ page, 3 * page + page / 2 }) |window| {
        var ra = one.readAhead(&sources, window, workers, null);
        ra.start(null);
        // Whole pages only, never past the window: one page, or three of the three and a half.
        try std.testing.expectEqual(window / page * page, ra.advised.load(.monotonic));
        try std.testing.expect(ra.ahead <= window);
        // Claimed, the item is a worker's: nothing more is advised for it, and its advice no longer counts.
        ra.claimed(0, null);
        try std.testing.expectEqual(window / page * page, ra.advised.load(.monotonic));
        try std.testing.expectEqual(@as(u64, 0), ra.ahead);
    };
}

test "a chunk's boundary page counts whole: one byte inside a page advises and counts that page" {
    if (builtin.os.tag != .linux) return error.SkipZigTest;
    const page = std.heap.pageSize();
    const bytes = try std.heap.page_allocator.alloc(u8, 2 * page);
    defer std.heap.page_allocator.free(bytes);
    @memset(bytes, 1);
    const sources = [_][]const u8{bytes};
    var one: OneChunk = undefined;
    one.init(page + 17, 1);
    var ra = one.readAhead(&sources, 1 << 30, 4, null);
    ra.start(null);
    // The accepted contract: advice is page-granular, so the page the byte sits on is advised and counted.
    try std.testing.expectEqual(@as(u64, page), ra.advised.load(.monotonic));
    try std.testing.expectEqual(@as(u64, page), ra.ahead);
    // A window below one page advises nothing.
    var narrow = one.readAhead(&sources, page - 1, 4, null);
    narrow.start(null);
    try std.testing.expectEqual(@as(u64, 0), narrow.advised.load(.monotonic));
}

test "the cap never passes what the query's ledger has free" {
    if (builtin.os.tag != .linux) return error.SkipZigTest;
    const page = std.heap.pageSize();
    const bytes = try std.heap.page_allocator.alloc(u8, 64 * page);
    defer std.heap.page_allocator.free(bytes);
    @memset(bytes, 1);
    const sources = [_][]const u8{bytes};
    var one: OneChunk = undefined;
    one.init(0, bytes.len);
    var ledger: core_memory.SharedBudget = .{ .limit = 1 << 30 };
    // Nearly exhausted: two pages free of a window far wider.
    ledger.used.store(ledger.limit - 2 * page, .monotonic);
    var ra = one.readAhead(&sources, 1 << 30, 4, &ledger);
    ra.start(null);
    try std.testing.expectEqual(@as(u64, 2 * page), ra.advised.load(.monotonic));
    // Exhausted: no advice at all, and nothing waits for room.
    ledger.used.store(ledger.limit, .monotonic);
    var none = one.readAhead(&sources, 1 << 30, 4, &ledger);
    none.start(null);
    try std.testing.expectEqual(@as(u64, 0), none.advised.load(.monotonic));
}

test "a pass advises its first items before any claim, then keeps a lookahead of items within the window" {
    const testing = std.testing;
    const metadata = @import("../parquet/metadata.zig");
    const expr_parser = @import("../expr/parser.zig");
    const expr_ast = @import("../expr/ast.zig");
    const bytes = (try @import("test_support.zig").loadFixture("ci/fixtures/parquet/topk_skew.parquet")) orelse
        return error.SkipZigTest;
    defer testing.allocator.free(bytes);
    if (builtin.os.tag != .linux) return;
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
    const page = std.heap.pageSize();
    const workers = 2;
    // A window wider than the lookahead: the lookahead binds. One page and a half: the window does, mid-chunk.
    for ([_]u64{ 1 << 30, page + page / 2 }) |window| {
        var ra: ReadAhead = .init(.{ .window = window, .block = page, .force = true }, work, &metas, &plan, &sources, workers, null, try a.alloc(u64, n));
        test_peak_ahead.store(0, .monotonic);
        ra.start(null);
        if (window > bytes.len) try testing.expectEqual(@min(n, workers * ahead_per_worker), ra.item);
        for (0..n) |idx| {
            ra.claimed(idx, null);
            try testing.expect(ra.item <= idx + 1 + ra.lookahead);
            try testing.expect(ra.ahead <= window);
        }
        // Hard: never more than the window advised ahead of the claims, page-rounded.
        try testing.expect(test_peak_ahead.load(.monotonic) <= window);
        try testing.expect(ra.advised.load(.monotonic) > 0);
    }
}
