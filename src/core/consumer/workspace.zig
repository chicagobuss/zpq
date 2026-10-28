const std = @import("std");
const schema = @import("../schema.zig");
const expr_agg = @import("../expr/agg.zig");
const huge_pages = @import("../huge_pages.zig");
const page_mod = @import("../parquet/page.zig");
const DecodeScratch = @import("../parquet/column.zig").DecodeScratch;
const RowGroupArena = @import("../row_group_arena.zig").RowGroupArena;
const Timings = @import("types.zig").Timings;
const AggRowGroup = @import("types.zig").AggRowGroup;
const DecodeBudget = @import("budget.zig").DecodeBudget;

/// A scan worker's state, reused from one row group to the next.
pub const AggWorkspace = struct {
    /// The scan coordinator revalidates after all workers and their arenas have unwound.
    defer_decode_errors: bool = false,
    decode_batch_rows: usize = 0,
    auto_decode_batches: bool = false,
    cache_hints: ?*CacheHints = null,
    scan_workers: usize = 1,
    expr_batch_rows: ?usize = null,
    scratch: *DecodeScratch,
    /// Rewound at each row group that decodes: everything decoded lives here until the next one.
    rg_arena: *RowGroupArena,
    timings: *Timings,
    /// Shared by the query's scan workers; null decodes without a bound (tests, callers that budget elsewhere).
    budget: ?*DecodeBudget = null,
    /// The worker's region under `scratch` and `rg_arena`, which keeps an earlier pass's pages: trimmed before a row
    /// group decodes sliced or staged on memory of its own, so those pages are not held beside its reservation.
    region: ?*huge_pages.Region = null,
    /// Where a row group decodes staged or sliced, beside the region: drawn from the query's ledger, as the region is.
    /// Null allocates from the scan's `gpa`.
    charged: ?std.mem.Allocator = null,
    /// Optional cost hints use their own refusal flags, while charging the same ledger.
    planning_charged: ?std.mem.Allocator = null,
    /// The ledger `charged` draws from, which a sliced row group sizes its slices to fit.
    ledger: ?*const expr_agg.SharedBudget = null,
};

/// Whether a row group's whole decode of `bytes` may go on the worker's region: the query's ledger has room now for
/// what the region does not hold already (`Region.fitsRowGroup`). Without a region (or a ledger) only the decode budget
/// decides.
pub fn admitWhole(ws: AggWorkspace, bytes: usize) bool {
    const r = ws.region orelse return true;
    return r.fitsRowGroup(bytes);
}

/// Give back everything the worker's region holds, the row-group arena's nodes and the decode scratch, with what
/// they drew from the ledger: a row group the ledger refused there then decodes in slices on memory of its own, not
/// beside the pages it was refused for. Nothing allocated on either may be live.
pub fn dropRegion(ws: AggWorkspace) void {
    const r = ws.region orelse return;
    if (ws.rg_arena.region == null) {
        const prof = ws.scratch.prof;
        // The slice pool is drawn from the ledger, beside the region. Dropping the region rewinds the scratch
        // the pool hangs off; the pool itself stays mapped until the worker releases it.
        const kept = ws.scratch.slice;
        ws.scratch.slice = null;
        ws.scratch.deinit();
        ws.scratch.* = .init(r.allocator());
        ws.scratch.prof = prof;
        ws.scratch.slice = kept;
        ws.rg_arena.deinit();
        ws.rg_arena.* = .init(r.allocator());
    }
    r.rewind();
    r.trim();
}

/// `err` was the region's or `ws.charged`'s refusal of a draw, not the host's out-of-memory: the row group may still
/// decode in slices, on less.
pub fn refusedDraw(ws: AggWorkspace, err: anyerror) bool {
    if (err != error.OutOfMemory) return false;
    if (ws.region) |r| if (r.refused) {
        r.refused = false;
        return true;
    };
    return false;
}


const CacheDictionary = struct { entries: usize, bytes: usize };

/// Query-local header facts, shared across workers/passes. No page or source-buffer pointers are retained.
/// Optional allocation refusal leaves the cost hint uncached; it never affects decode diagnostics.
pub const CacheHints = struct {
    const Key = struct { rg: *const schema.RowGroup, column: usize };
    const Shard = struct {
        lock: std.Io.Mutex = .init,
        facts: std.AutoHashMapUnmanaged(Key, ?CacheDictionary) = .empty,
    };
    inline_shard: Shard = .{},
    shards: []Shard = &.{},
    allocator: std.mem.Allocator,

    /// Initialized before workers start. Refusal retains the inline memo, without changing decoder flags.
    pub fn initShards(h: *CacheHints, workers: usize) !void {
        if (workers <= 1) return;
        const shards = try h.allocator.alloc(Shard, workers);
        @memset(shards, .{});
        h.shards = shards;
    }

    pub fn deinit(h: *CacheHints) void {
        h.inline_shard.facts.deinit(h.allocator);
        h.inline_shard.facts = .empty;
        for (h.shards) |*shard| shard.facts.deinit(h.allocator);
        if (h.shards.len != 0) h.allocator.free(h.shards);
        h.shards = &.{};
    }

    pub fn count(h: *const CacheHints) usize {
        var n = h.inline_shard.facts.count();
        for (h.shards) |shard| n += shard.facts.count();
        return n;
    }

    pub fn get(h: *CacheHints, in: AggRowGroup, ci: usize) ?CacheDictionary {
        const key: Key = .{ .rg = in.rg, .column = ci };
        const hash = (std.hash_map.AutoContext(Key){}).hash(key);
        const shard = if (h.shards.len == 0) &h.inline_shard else &h.shards[hash % h.shards.len];
        shard.lock.lockUncancelable(@import("../blocking.zig").io);
        defer shard.lock.unlock(@import("../blocking.zig").io);
        if (shard.facts.getPtr(key)) |fact| return fact.*;
        const fact = cacheDictionary(in, ci);
        shard.facts.put(h.allocator, key, fact) catch {};
        return fact;
    }
};

pub fn cacheHintsWanted(calls: []const expr_agg.AggCall) bool {
    for (calls) |call| if (call.arg) |arg| if (arg == .binop and arg.typeOf() == .f64) return true;
    return false;
}

/// Header-only proof of a column's finite dictionary domain. It is a cost hint: malformed/incomplete bytes decline
/// admission and leave all errors to ordinary execution. No statistics or decompression result authorizes a path.
pub fn cacheDictionary(in: AggRowGroup, ci: usize) ?CacheDictionary {
    if (ci >= in.rg.columns.items.len) return null;
    const cm = in.rg.columns.items[ci].meta_data orelse return null;
    if (cm.total_compressed_size < 0) return null;
    const offset = cm.dictionary_page_offset orelse return null;
    const absolute = std.math.cast(usize, offset) orelse return null;
    const origin = std.math.cast(usize, in.src.byte_origin) orelse return null;
    if (absolute < origin) return null;
    const start = absolute - origin;
    const len: usize = @intCast(cm.total_compressed_size);
    if (start > in.src.bytes.len or len > in.src.bytes.len - start) return null;
    var pages = page_mod.PageReader.init(in.src.bytes[start..][0..len], cm.codec, std.heap.page_allocator);
    var dict: ?CacheDictionary = null;
    var data = false;
    while (pages.peek() catch return null) |p| {
        switch (p.header.type) {
            .DICTIONARY_PAGE => {
                if (dict != null or data) return null;
                const h = p.header.dictionary_page_header orelse return null;
                if (h.num_values <= 0) return null;
                dict = .{ .entries = @intCast(h.num_values), .bytes = @intCast(p.header.uncompressed_page_size) };
            },
            .DATA_PAGE, .DATA_PAGE_V2 => {
                if (dict == null) return null;
                const encoding = if (p.header.data_page_header) |h|
                    h.encoding
                else if (p.header.data_page_header_v2) |h| h.encoding else return null;
                if (encoding != .RLE_DICTIONARY and encoding != .PLAIN_DICTIONARY) return null;
                data = true;
            },
            .INDEX_PAGE => {},
        }
        pages.pass(p);
    }
    return if (data) dict else null;
}

