//! Reads a mapped scan's planned column chunks ahead of its workers.
//!
//! A scan over the plain mapping (the cut-steered shapes, which make several passes over the same pages, or any
//! input the local-I/O choice leaves mapped) otherwise reads each chunk when a worker decodes it: one synchronous
//! fault and its read-around at a time per worker, so a cold scan runs at a queue depth of about its worker count
//! and decoders wait on the disk. Here a worker that claims a work item advises the chunks the next items of the
//! same pass will decode (`POSIX_FADV_WILLNEED` through the file's descriptor), so their pages arrive while it
//! decodes.
//!
//! Bounded on every side, from run-time facts only:
//! - what: the chunks of the columns this pass decodes for each item, nothing between them but the pages a chunk's
//!   first and last bytes share with its neighbours (advice is page-granular, and a fault there reads the same page).
//!   A column the decode reads behind the filter's gate (`gated`: a payload column, or a filter column read only at
//!   the survivors of the conjuncts before it) follows what those conjuncts keep: its own reader advises just the
//!   pages its gate keeps, once that gate is decided (`consumer/plan.zig` `adviseGated`), and here it is advised whole only
//!   while the latest such reader found its gate keeping every page (`kept`). A gated column whose pages are skipped
//!   is never advised ahead of the survivors that skip them;
//! - how far: a few items per worker ahead of the latest claim, paced by the claims (each moves the cursor by about
//!   one item), and a hard cap on the bytes advised and not yet claimed: the caller's window (`Config`: within what
//!   one of the device's hardware queues holds), or what the query's ledger has free when it is less, whichever is
//!   smaller each time it advances. The cap holds within an item too: a cursor advises an item block by block and
//!   stops at the cap mid-chunk, the rest left to the next claim or to demand faults. A cap below one page advises
//!   nothing. Advice never waits for room: it is optional, and a fault reads what it declined;
//! - how: in blocks of the device's read-ahead (`Config.block`): one advice call reads no more than that;
//! - who: one caller at a time (the others go on decoding): several advising callers contend on the page cache;
//! - when: only once workers run, from their claims, and only for pages not already resident (`cachestat`): a hot
//!   scan issues no reads and makes no advice, and pages a worker's fault already has in flight count as resident.
//!   Nothing is advised before the workers start: the first claims fault their own items in together, as many reads
//!   in flight as there are workers, and a serial walk over the first items ahead of them only delayed every
//!   worker's start (by about 10 ms at 32 workers) for little: where the workers already keep the device busy, the
//!   delay is the whole cost.
//!
//! Residency and advice go through each file's descriptor (`Config.fds`): `cachestat` and `fadvise`, where the
//! mapping's `mincore` and `madvise` also take the address space's lock, which the workers' region maps write. On
//! granite that was 4-6% of a 3 GB cold scan (ClickBench Q21) and even elsewhere; per call, the two cost about the
//! same. A mapping kept without a descriptor uses the mapping's calls.
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
const consumer_workspace = @import("../consumer/workspace.zig");

/// What the local-I/O choice hands the scan (`input.chooseLocalIo`).
pub const Config = struct {
    /// Bytes advised and not yet claimed, at most: a share of the query's budget, never past the host's memory.
    window: u64,
    /// The device's read-ahead in bytes: the most one advice call moves.
    block: usize,
    /// Advise resident pages too (`ZPQ_IO_ADVISE=always`): coverage for the advice path on inputs a test leaves
    /// resident. Never chosen by the engine.
    force: bool = false,
    /// Each input's open descriptor, indexed like the scan's inputs, or -1: residency and advice through it, else
    /// through the mapping (`mincore`, `madvise`).
    fds: []const i32 = &.{},
};

/// Tests: the most bytes any pass held advised ahead of its claims, page-rounded.
pub var test_peak_ahead: std.atomic.Value(u64) = .init(0);
/// Tests: the bytes passes advised or found resident already, page-rounded: what the read-ahead looked at.
pub var test_seen: std.atomic.Value(u64) = .init(0);

/// How far ahead of the latest claim advice reaches, in items per worker.
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
    /// `claimed_hi` as the last advance saw it: each advance moves the cursor by at most the items claimed since, and
    /// one, so advice keeps pace with the claims instead of falling on one worker in a burst.
    paced: usize = 0,
    workers: usize,
    /// The cursor: the next bytes to advise are chunk `col` of item `item`, from `offset` into it.
    item: usize = 0,
    col: usize = 0,
    offset: usize = 0,
    /// Per leaf column, whether the latest row group to plan its decode reads it behind the filter's gate
    /// (`AggWorkspace.gated`, set by the decode): advice here leaves such a column to the gated reader, which advises
    /// only the pages its gate keeps (`adviseGated`). Empty: no column is.
    gated: []std.atomic.Value(bool) = &.{},
    /// Per leaf column, whether the latest gated reader of it found the gate keeping every page of its chunk
    /// (`AggWorkspace.kept`). A gated column is advised here again while it is, read whole as an ungated one is.
    /// Until a reader says so, it is not.
    kept: []std.atomic.Value(bool) = &.{},
    /// Items before `released` are claimed, and their advice no longer counts.
    released: usize = 0,
    /// Page-rounded bytes advised and not yet decoded, never past the cap: the cursor's for items not yet claimed, and a
    /// gated column's for a claimed item its worker is decoding (`adviseClaimed`), until that worker's next claim.
    ahead: std.atomic.Value(u64) = .init(0),
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
            .workers = @max(1, workers),
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
        var ra = init(c, work, metas, plan, sources, workers, ledger, try arena.alloc(u64, work.len));
        var leaves: usize = 0;
        for (metas) |m| for (m.row_groups.items) |rg| {
            leaves = @max(leaves, rg.columns.items.len);
        };
        ra.gated = try arena.alloc(std.atomic.Value(bool), leaves);
        @memset(ra.gated, .init(false));
        ra.kept = try arena.alloc(std.atomic.Value(bool), leaves);
        @memset(ra.kept, .init(false));
        return ra;
    }

    /// A pass over the work list begins, before its workers start: nothing of it is advised yet, and nothing is
    /// advised here (the first claims do).
    pub fn start(self: *ReadAhead) void {
        self.item = 0;
        self.col = 0;
        self.offset = 0;
        self.released = 0;
        self.ahead.store(0, .monotonic);
        self.paced = 0;
        self.last = null;
        self.claimed_hi.store(0, .monotonic);
        @memset(self.item_bytes, 0);
    }

    /// Item `idx` was claimed. `columns`: what this pass decodes for every item (a sketch's count pass reads only its
    /// keys), or null for each item's own columns.
    pub fn claimed(self: *ReadAhead, idx: usize, columns: ?agg_plan.Columns) void {
        _ = self.claimed_hi.fetchMax(idx + 1, .monotonic);
        self.advance(columns);
    }

    /// Advise `range` of input `file`'s mapping for a claimed item about to decode it: the pages a gated column's gate
    /// keeps (`consumer/plan.zig` `adviseGated`). The cursor's bounds hold: page-rounded calls of at most a block,
    /// resident blocks skipped, each block admitted only while the bytes advised and not yet decoded stay within the
    /// cap (the window, or what the ledger has free), and past it the rest is left to demand faults. Never waits. What
    /// it admits is added to `held`, the worker's own, which it gives back at its next claim (`releaseHeld`).
    pub fn adviseClaimed(self: *ReadAhead, file: usize, range: []const u8, held: *u64) void {
        if (builtin.target.os.tag != .linux or file >= self.sources.len or range.len == 0) return;
        const page = std.heap.pageSize();
        const src = self.sources[file];
        const src_lo = std.mem.alignBackward(usize, @intFromPtr(src.ptr), page);
        const src_hi = std.mem.alignForward(usize, @intFromPtr(src.ptr) + src.len, page);
        var lo = std.mem.alignBackward(usize, @intFromPtr(range.ptr), page);
        const end = std.mem.alignForward(usize, @intFromPtr(range.ptr) + range.len, page);
        // Outside the mapping there is nothing of this input to advise.
        if (lo < src_lo or end > src_hi) return;
        const fd: i32 = if (file < self.config.fds.len) self.config.fds[file] else -1;
        while (lo < end) {
            const limit = self.cap();
            const step = std.mem.alignBackward(u64, @min(@as(u64, @max(self.config.block, page)), limit), page);
            if (step == 0) return;
            const len: usize = @intCast(@min(step, end - lo));
            const file_off = lo -| @intFromPtr(src.ptr);
            if (!self.config.force and cached(fd, file_off, lo, len, page)) {
                _ = self.resident.fetchAdd(len, .monotonic);
                if (builtin.is_test) _ = test_seen.fetchAdd(len, .monotonic);
            } else {
                if (!self.take(len, limit)) return;
                willNeed(fd, file_off, lo, len);
                held.* += len;
                _ = self.advised.fetchAdd(len, .monotonic);
                if (builtin.is_test) _ = test_seen.fetchAdd(len, .monotonic);
            }
            lo += len;
        }
    }

    /// `adviseClaimed` for the decode (`AggWorkspace.gate_advice`), counting into the worker's `held`.
    pub fn gateAdvice(self: *ReadAhead, held: *u64) consumer_workspace.GateAdvice {
        return .{ .ctx = self, .held = held, .advise = adviseOpaque };
    }

    fn adviseOpaque(ctx: *anyopaque, file: usize, range: []const u8, held: *u64) void {
        const self: *ReadAhead = @ptrCast(@alignCast(ctx));
        self.adviseClaimed(file, range, held);
    }

    /// Give back what `adviseClaimed` held for a worker's item, once that item's decode is done.
    pub fn releaseHeld(self: *ReadAhead, held: *u64) void {
        self.give(held.*);
        held.* = 0;
    }

    /// Count `len` more bytes advised and not yet decoded, if that keeps them within `limit`.
    fn take(self: *ReadAhead, len: u64, limit: u64) bool {
        var cur = self.ahead.load(.monotonic);
        while (cur + len <= limit) {
            cur = self.ahead.cmpxchgWeak(cur, cur + len, .monotonic, .monotonic) orelse {
                if (builtin.is_test) _ = test_peak_ahead.fetchMax(cur + len, .monotonic);
                return true;
            };
        }
        return false;
    }

    fn give(self: *ReadAhead, len: u64) void {
        if (len == 0) return;
        var cur = self.ahead.load(.monotonic);
        while (self.ahead.cmpxchgWeak(cur, cur -| len, .monotonic, .monotonic)) |actual| cur = actual;
    }

    /// The most bytes advised and not yet claimed, now: the window, or what the ledger has free when that is less.
    fn cap(self: *const ReadAhead) u64 {
        const free: u64 = if (self.ledger) |l| l.available() else std.math.maxInt(u64);
        return @min(self.config.window, free);
    }

    /// Release what claims have taken, then advise block by block up to the lookahead and the cap.
    fn advance(self: *ReadAhead, columns: ?agg_plan.Columns) void {
        if (builtin.target.os.tag != .linux) return;
        if (self.busy.swap(true, .acquire)) return;
        defer self.busy.store(false, .release);
        const hi = @min(self.claimed_hi.load(.monotonic), self.work.len);
        while (self.released < hi) : (self.released += 1) self.give(self.item_bytes[self.released]);
        const claims = hi -| self.paced;
        self.paced = hi;
        // Never advise what a worker already claimed.
        if (self.item < hi) {
            self.item = hi;
            self.col = 0;
            self.offset = 0;
        }
        const limit = self.cap();
        const page = std.heap.pageSize();
        // A block no larger than the cap, so the cap can be met exactly; below one page there is nothing to advise.
        const step = std.mem.alignBackward(u64, @min(@as(u64, @max(self.config.block, page)), limit), page);
        if (step == 0) return;
        const stop = @min(@min(self.work.len, self.released +| self.lookahead), self.item +| claims +| 1);
        while (self.item < stop) {
            const range = self.nextRange(columns) orelse continue;
            // Page-rounded: the pages a block touches are what advice reads and what the cap counts.
            const lo = std.mem.alignBackward(usize, @intFromPtr(range.ptr), page);
            const end = std.mem.alignForward(usize, @intFromPtr(range.ptr) + range.len, page);
            const len: usize = @intCast(@min(step, end - lo));
            const fd: i32 = if (self.work[self.item].file < self.config.fds.len) self.config.fds[self.work[self.item].file] else -1;
            const file_off = lo - @intFromPtr(self.sources[self.work[self.item].file].ptr);
            if (!self.config.force and cached(fd, file_off, lo, len, page)) {
                _ = self.resident.fetchAdd(len, .monotonic);
                if (builtin.is_test) _ = test_seen.fetchAdd(len, .monotonic);
            } else {
                if (!self.take(len, limit)) break;
                willNeed(fd, file_off, lo, len);
                self.item_bytes[self.item] += len;
                _ = self.advised.fetchAdd(len, .monotonic);
                if (builtin.is_test) _ = test_seen.fetchAdd(len, .monotonic);
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
            if (self.col < self.gated.len and self.gated[self.col].load(.monotonic) and
                !(self.col < self.kept.len and self.kept[self.col].load(.monotonic))) continue;
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

/// Whether every page of the `len` bytes at file offset `off` (mapped at page-aligned `addr`) is in the page cache:
/// `cachestat` through `fd`, else `mincore` on the mapping (no descriptor, or a kernel before 6.5).
fn cached(fd: i32, off: usize, addr: usize, len: usize, page: usize) bool {
    if (fd >= 0) {
        const linux = std.os.linux;
        const range: linux.cache_stat_range = .{ .off = off, .len = len };
        var cs: linux.cache_stat = undefined;
        if (linux.errno(linux.cachestat(fd, &range, &cs, 0)) == .SUCCESS) return cs.cache * page >= len;
    }
    return allResident(addr, len, page);
}

/// Advise the `len` bytes at file offset `off` (mapped at `addr`): `fadvise` through `fd`, else `madvise`. Advisory
/// only: a refusal keeps the ordinary fault path.
fn willNeed(fd: i32, off: usize, addr: usize, len: usize) void {
    const linux = std.os.linux;
    if (fd >= 0 and linux.errno(linux.fadvise(fd, @intCast(off), @intCast(len), linux.POSIX_FADV.WILLNEED)) == .SUCCESS)
        return;
    _ = linux.madvise(@ptrFromInt(addr), len, linux.MADV.WILLNEED);
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

/// Tests: `n` row groups of one chunk each, `len` bytes at `offset + i * stride` of the source, read by every item.
const Chunks = struct {
    chunks: []schema.ColumnChunk,
    groups: []schema.RowGroup,
    metas: [1]schema.FileMetaData,
    mask: [1]bool = .{true},
    plan: agg_plan.Plan,
    work: []WorkItem,
    item_bytes: []u64,

    fn init(self: *Chunks, a: std.mem.Allocator, n: usize, offset: usize, stride: usize, len: usize) !void {
        self.mask = .{true};
        self.chunks = try a.alloc(schema.ColumnChunk, n);
        self.groups = try a.alloc(schema.RowGroup, n);
        self.work = try a.alloc(WorkItem, n);
        self.item_bytes = try a.alloc(u64, n);
        const columns: agg_plan.Columns = .{ .read = &self.mask, .consumed = &self.mask, .preserve = &self.mask };
        for (self.chunks, self.groups, self.work, 0..) |*c, *g, *w, i| {
            c.* = .{
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
                    .data_page_offset = @intCast(offset + i * stride),
                    .index_page_offset = null,
                    .dictionary_page_offset = null,
                },
            };
            g.* = .{ .columns = .fromOwnedSlice(c[0..1]), .total_byte_size = @intCast(len), .num_rows = 1 };
            w.* = .{ .file = 0, .rg = i, .agg_start = 0, .agg_len = 0, .columns = columns, .accumulators = &.{} };
        }
        self.metas = .{.{ .version = 2, .schema = .empty, .num_rows = @intCast(n), .created_by = null, .row_groups = .fromOwnedSlice(self.groups) }};
        self.plan = .{
            .query = .{ .filter = null, .calls = &.{}, .group_keys = null },
            .stats = .ignore,
            .filter_cols = &self.mask,
            .columns = columns,
            .cols_stat_pruned = 0,
        };
    }

    fn readAhead(self: *Chunks, sources: []const []const u8, window: u64, workers: usize, ledger: ?*core_memory.SharedBudget) ReadAhead {
        const page = std.heap.pageSize();
        return .init(.{ .window = window, .block = page, .force = true }, self.work, &self.metas, &self.plan, sources, workers, ledger, self.item_bytes);
    }
};

test "a pass's start advises nothing, and each claim advises about one item past it" {
    if (builtin.target.os.tag != .linux) return error.SkipZigTest;
    const page = std.heap.pageSize();
    var arena_state = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena_state.deinit();
    const bytes = try std.heap.page_allocator.alloc(u8, 16 * page);
    defer std.heap.page_allocator.free(bytes);
    @memset(bytes, 1);
    const sources = [_][]const u8{bytes};
    var set: Chunks = undefined;
    try set.init(arena_state.allocator(), 16, 0, page, page);
    const workers = 4;
    var ra = set.readAhead(&sources, 1 << 30, workers, null);
    ra.start();
    try std.testing.expectEqual(@as(u64, 0), ra.advised.load(.monotonic));
    // Item 0 claimed: never item 0 itself, and the next item (and one for the claim) advised.
    ra.claimed(0, null);
    try std.testing.expectEqual(@as(u64, 0), set.item_bytes[0]);
    try std.testing.expectEqual(@as(u64, 2 * page), ra.advised.load(.monotonic));
    try std.testing.expect(set.item_bytes[1] == page and set.item_bytes[2] == page);
    // Paced: each later claim advises about one more item, never the whole lookahead at once.
    for (1..16) |idx| {
        const before = ra.advised.load(.monotonic);
        ra.claimed(idx, null);
        try std.testing.expect(ra.advised.load(.monotonic) - before <= 2 * page);
        try std.testing.expect(ra.item <= idx + 1 + ra.lookahead);
    }
}

test "the cap holds within an oversized item: advice stops mid-chunk" {
    if (builtin.target.os.tag != .linux) return error.SkipZigTest;
    const page = std.heap.pageSize();
    var arena_state = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena_state.deinit();
    for ([_]usize{ 1, 4, 16 }) |workers| for ([_]u64{ page, 3 * page + page / 2 }) |window| {
        const n = 2 * workers + 2;
        const bytes = try std.heap.page_allocator.alloc(u8, n * 64 * page);
        defer std.heap.page_allocator.free(bytes);
        @memset(bytes, 1);
        const sources = [_][]const u8{bytes};
        var set: Chunks = undefined;
        try set.init(arena_state.allocator(), n, 0, 64 * page, 64 * page);
        var ra = set.readAhead(&sources, window, workers, null);
        ra.start();
        ra.claimed(0, null);
        // Whole pages only, never past the window: one page, or three of the three and a half.
        try std.testing.expectEqual(window / page * page, ra.advised.load(.monotonic));
        try std.testing.expect(ra.ahead.load(.monotonic) <= window);
        // A later claim of the advised item gives its advice back; the cap holds again for what follows.
        ra.claimed(1, null);
        try std.testing.expect(ra.ahead.load(.monotonic) <= window);
    };
}

test "a chunk's boundary page counts whole: one byte inside a page advises and counts that page" {
    if (builtin.target.os.tag != .linux) return error.SkipZigTest;
    const page = std.heap.pageSize();
    var arena_state = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena_state.deinit();
    const bytes = try std.heap.page_allocator.alloc(u8, 8 * page);
    defer std.heap.page_allocator.free(bytes);
    @memset(bytes, 1);
    const sources = [_][]const u8{bytes};
    var set: Chunks = undefined;
    // Four one-byte chunks, each 17 bytes into its own page.
    try set.init(arena_state.allocator(), 4, page + 17, 2 * page, 1);
    var ra = set.readAhead(&sources, 1 << 30, 1, null);
    ra.start();
    ra.claimed(0, null);
    // The accepted contract: advice is page-granular, so the page each advised byte (items 1 and 2's) sits on is
    // advised and counted whole.
    try std.testing.expectEqual(@as(u64, 2 * page), ra.advised.load(.monotonic));
    try std.testing.expect(set.item_bytes[1] == page and set.item_bytes[2] == page);
    try std.testing.expectEqual(@as(u64, 2 * page), ra.ahead.load(.monotonic));
    // A window below one page advises nothing.
    var narrow = set.readAhead(&sources, page - 1, 1, null);
    narrow.start();
    narrow.claimed(0, null);
    try std.testing.expectEqual(@as(u64, 0), narrow.advised.load(.monotonic));
}

test "through a file's descriptor: resident pages are left alone, and the rest advised" {
    if (builtin.target.os.tag != .linux) return error.SkipZigTest;
    const local_fs = @import("../../local_fs.zig");
    const page = std.heap.pageSize();
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var arena_state = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena_state.deinit();
    const a = arena_state.allocator();
    const path = try std.fmt.allocPrint(a, ".zig-cache/tmp/{s}/chunks.bin", .{tmp.sub_path});
    const fd = try local_fs.createFile(path);
    const body = try a.alloc(u8, 8 * page);
    @memset(body, 1);
    try local_fs.writeAll(fd, body);
    local_fs.close(fd);
    const m = try local_fs.mapFile(path);
    defer m.unmap();
    const held = m.fd orelse return error.SkipZigTest;
    const sources = [_][]const u8{m.bytes};
    const fds = [_]i32{held};
    var set: Chunks = undefined;
    try set.init(a, 8, 0, page, page);
    // Just written, so resident: `cachestat` says so and nothing is advised.
    var ra: ReadAhead = .init(.{ .window = 1 << 30, .block = page, .fds = &fds }, set.work, &set.metas, &set.plan, &sources, 1, null, set.item_bytes);
    ra.start();
    ra.claimed(0, null);
    try std.testing.expectEqual(@as(u64, 0), ra.advised.load(.monotonic));
    try std.testing.expectEqual(@as(u64, 2 * page), ra.resident.load(.monotonic));
    // Forced: the same pages advised through the descriptor, and counted as for the mapping.
    var forced: ReadAhead = .init(.{ .window = 1 << 30, .block = page, .force = true, .fds = &fds }, set.work, &set.metas, &set.plan, &sources, 1, null, set.item_bytes);
    forced.start();
    forced.claimed(0, null);
    try std.testing.expectEqual(@as(u64, 2 * page), forced.advised.load(.monotonic));
    try std.testing.expectEqual(@as(u64, 0), forced.resident.load(.monotonic));
}

test "a column read behind the filter's gate is left to its reader until a reader says the gate kept all of it" {
    if (builtin.target.os.tag != .linux) return error.SkipZigTest;
    const page = std.heap.pageSize();
    var arena_state = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena_state.deinit();
    const bytes = try std.heap.page_allocator.alloc(u8, 8 * page);
    defer std.heap.page_allocator.free(bytes);
    @memset(bytes, 1);
    const sources = [_][]const u8{bytes};
    var set: Chunks = undefined;
    try set.init(arena_state.allocator(), 8, 0, page, page);
    var gated = [_]std.atomic.Value(bool){.init(true)};
    var kept = [_]std.atomic.Value(bool){.init(false)};
    var ra = set.readAhead(&sources, 1 << 30, 1, null);
    ra.gated = &gated;
    ra.kept = &kept;
    ra.start();
    // Gated, and no reader has said its gate kept every page: nothing is advised, though the cursor moves on.
    ra.claimed(0, null);
    try std.testing.expectEqual(@as(u64, 0), ra.advised.load(.monotonic));
    // A reader found its gate keeping every page: the column is advised as an ungated one is.
    kept[0].store(true, .monotonic);
    ra.claimed(3, null);
    try std.testing.expect(ra.advised.load(.monotonic) > 0);
    // Not gated in the latest row group: advised whatever the readers said.
    kept[0].store(false, .monotonic);
    gated[0].store(false, .monotonic);
    const before = ra.advised.load(.monotonic);
    ra.claimed(5, null);
    try std.testing.expect(ra.advised.load(.monotonic) > before);
}

test "a gated column's kept pages are advised within the window and the ledger, as the cursor's are (review R3)" {
    if (builtin.target.os.tag != .linux) return error.SkipZigTest;
    const thrift = @import("../thrift.zig");
    const local_fs = @import("../../local_fs.zig");
    const consumer_plan = @import("../consumer/plan.zig");
    const consumer_types = @import("../consumer/types.zig");
    const selection = @import("../filter/selection.zig");
    var arena_state = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena_state.deinit();
    const a = arena_state.allocator();
    const page = std.heap.pageSize();
    // The reviewer's chunk: two PLAIN INT32 data pages of 256 OS pages (1 MiB) each, the gate keeping every row of
    // the first and none of the second.
    const payload_len = 256 * page;
    const rows = payload_len / @sizeOf(i32);
    var header = thrift.Writer.init(a);
    header.writeStructBegin();
    try header.writeFieldI32(1, 0);
    try header.writeFieldI32(2, @intCast(payload_len));
    try header.writeFieldI32(3, @intCast(payload_len));
    try header.writeFieldBegin(.Struct, 5);
    header.writeStructBegin();
    try header.writeFieldI32(1, @intCast(rows));
    try header.writeFieldI32(2, 0);
    try header.writeFieldI32(3, 3);
    try header.writeFieldI32(4, 3);
    try header.writeStructEnd();
    try header.writeStructEnd();
    const h = header.bytes();
    const one_len = h.len + payload_len;
    const body = try a.alloc(u8, 2 * one_len);
    @memset(body, 0);
    @memcpy(body[0..h.len], h);
    @memcpy(body[one_len..][0..h.len], h);
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try std.fmt.allocPrint(a, ".zig-cache/tmp/{s}/gated.bin", .{tmp.sub_path});
    const fd = try local_fs.createFile(path);
    try local_fs.writeAll(fd, body);
    local_fs.close(fd);
    const mapped = try local_fs.mapFile(path);
    defer mapped.unmap();
    var chunks = [_]schema.ColumnChunk{.{
        .file_path = null,
        .file_offset = 0,
        .meta_data = .{
            .type = .INT32,
            .encodings = .empty,
            .path_in_schema = .empty,
            .codec = .UNCOMPRESSED,
            .num_values = @intCast(2 * rows),
            .total_uncompressed_size = @intCast(body.len),
            .total_compressed_size = @intCast(body.len),
            .data_page_offset = 0,
            .dictionary_page_offset = null,
            .index_page_offset = null,
        },
    }};
    var groups = [_]schema.RowGroup{.{ .columns = .fromOwnedSlice(&chunks), .total_byte_size = @intCast(body.len), .num_rows = @intCast(2 * rows) }};
    const metas = [_]schema.FileMetaData{.{ .version = 2, .schema = .empty, .num_rows = @intCast(2 * rows), .created_by = null, .row_groups = .fromOwnedSlice(&groups) }};
    const in: consumer_types.AggRowGroup = .{
        .rg = &groups[0],
        .meta = &metas[0],
        .src = .{ .bytes = mapped.bytes, .byte_origin = 0 },
        .filter = null,
        .read = &.{true},
        .consumed = &.{true},
    };
    var gate = try selection.SelectionVector.init(a, 2 * rows);
    gate.clearRange(rows, 2 * rows);
    const mask = [_]bool{true};
    const columns: agg_plan.Columns = .{ .read = &mask, .consumed = &mask, .preserve = &mask };
    const plan: agg_plan.Plan = .{
        .query = .{ .filter = null, .calls = &.{}, .group_keys = null },
        .stats = .ignore,
        .filter_cols = &mask,
        .columns = columns,
        .cols_stat_pruned = 0,
    };
    const work = [_]WorkItem{.{ .file = 0, .rg = 0, .agg_start = 0, .agg_len = 0, .columns = columns, .accumulators = &.{} }};
    const sources = [_][]const u8{mapped.bytes};
    var item_bytes = [_]u64{0};
    for ([_]usize{ 1, 4, 16 }) |workers| {
        // A one-page window and block, and a ledger with nothing free: nothing is advised, and nothing counted.
        var ledger: core_memory.SharedBudget = .{ .limit = page };
        _ = ledger.drawExact(page).?;
        var kept = [_]std.atomic.Value(bool){.init(true)};
        var gated = [_]std.atomic.Value(bool){.init(true)};
        var ra: ReadAhead = .init(.{ .window = page, .block = page, .force = true }, &work, &metas, &plan, &sources, workers, &ledger, &item_bytes);
        ra.gated = &gated;
        ra.kept = &kept;
        ra.start();
        ra.claimed(0, null);
        var held: u64 = 0;
        const ws: consumer_workspace.AggWorkspace = .{ .scratch = undefined, .rg_arena = undefined, .timings = undefined, .gated = &gated, .kept = &kept, .gate_advice = ra.gateAdvice(&held) };
        consumer_plan.adviseGated(in, 0, &gate, ws);
        try std.testing.expect(!kept[0].load(.monotonic));
        try std.testing.expectEqual(@as(u64, 0), ra.advised.load(.monotonic));
        try std.testing.expectEqual(@as(u64, 0), ra.ahead.load(.monotonic));
        try std.testing.expectEqual(@as(u64, 0), held);
        ledger.release(page);
        // Room in the ledger, a window of three and a half pages: whole pages up to the window and no further,
        // counted as the cursor's, held for the worker's item until its next claim gives them back.
        test_peak_ahead.store(0, .monotonic);
        var wide: core_memory.SharedBudget = .{ .limit = 1 << 30 };
        var roomy: ReadAhead = .init(.{ .window = 3 * page + page / 2, .block = page, .force = true }, &work, &metas, &plan, &sources, workers, &wide, &item_bytes);
        roomy.gated = &gated;
        roomy.kept = &kept;
        roomy.start();
        roomy.claimed(0, null);
        const room: consumer_workspace.AggWorkspace = .{ .scratch = undefined, .rg_arena = undefined, .timings = undefined, .gated = &gated, .kept = &kept, .gate_advice = roomy.gateAdvice(&held) };
        consumer_plan.adviseGated(in, 0, &gate, room);
        try std.testing.expectEqual(@as(u64, 3 * page), roomy.advised.load(.monotonic));
        try std.testing.expectEqual(@as(u64, 3 * page), held);
        try std.testing.expect(test_peak_ahead.load(.monotonic) <= roomy.config.window);
        roomy.releaseHeld(&held);
        try std.testing.expectEqual(@as(u64, 0), roomy.ahead.load(.monotonic));
    }
}

test "the cap never passes what the query's ledger has free" {
    if (builtin.target.os.tag != .linux) return error.SkipZigTest;
    const page = std.heap.pageSize();
    var arena_state = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena_state.deinit();
    const bytes = try std.heap.page_allocator.alloc(u8, 4 * 64 * page);
    defer std.heap.page_allocator.free(bytes);
    @memset(bytes, 1);
    const sources = [_][]const u8{bytes};
    var set: Chunks = undefined;
    try set.init(arena_state.allocator(), 4, 0, 64 * page, 64 * page);
    var ledger: core_memory.SharedBudget = .{ .limit = 1 << 30 };
    // Nearly exhausted: two pages free of a window far wider.
    ledger.used.store(ledger.limit - 2 * page, .monotonic);
    var ra = set.readAhead(&sources, 1 << 30, 1, &ledger);
    ra.start();
    ra.claimed(0, null);
    try std.testing.expectEqual(@as(u64, 2 * page), ra.advised.load(.monotonic));
    // Exhausted: no advice at all, and nothing waits for room.
    ledger.used.store(ledger.limit, .monotonic);
    var none = set.readAhead(&sources, 1 << 30, 1, &ledger);
    none.start();
    none.claimed(0, null);
    try std.testing.expectEqual(@as(u64, 0), none.advised.load(.monotonic));
}

test "a pass keeps a lookahead of items within the window" {
    const testing = std.testing;
    const metadata = @import("../parquet/metadata.zig");
    const expr_parser = @import("../expr/parser.zig");
    const expr_ast = @import("../expr/ast.zig");
    const bytes = (try @import("test_support.zig").loadFixture("ci/fixtures/parquet/topk_skew.parquet")) orelse
        return error.SkipZigTest;
    defer testing.allocator.free(bytes);
    if (builtin.target.os.tag != .linux) return;
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
        ra.start();
        for (0..n) |idx| {
            ra.claimed(idx, null);
            try testing.expect(ra.item <= idx + 1 + ra.lookahead);
            try testing.expect(ra.ahead.load(.monotonic) <= window);
        }
        // Hard: never more than the window advised ahead of the claims, page-rounded.
        try testing.expect(test_peak_ahead.load(.monotonic) <= window);
        try testing.expect(ra.advised.load(.monotonic) > 0);
    }
}
