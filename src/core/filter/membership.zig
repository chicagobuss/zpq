//! Row-group pruning by membership: a chunk's Bloom filter, or its dictionary, proving an `=` literal absent.
//!
//! Statistics bound a chunk's values; for `=` on a column whose values span the domain in every row group (keys,
//! UUIDs, trace ids) they prune nothing. A split-block Bloom filter records which values a chunk holds, and so does
//! the dictionary of a chunk whose every page is dictionary-encoded, so either can prove a literal absent where the
//! bounds cannot.
//!
//! Two halves. `load` runs once per query, before any row group is planned: it picks the chunks worth probing (row
//! groups statistics keep, `=` leaves statistics leave open), reads their filters, or the dictionaries of chunks
//! without one, through a `Source`, and records on each chunk the literals proven absent (`ColumnChunk.absent`).
//! `provenAbsent` is what `prune.zig` asks at an `=` leaf. Pruning stays a function of the footer alone, so every
//! caller of `pruneRowGroup` (the S3 planner, the scan workers, the write path) reaches the same answer.
//!
//! I/O stays behind `Source`: a mapping for local files, the S3 range fetcher, later an io_uring reader. Which bytes
//! to read, how to merge them and when to give up is decided here, from the source's `Cost`. Bloom ranges that
//! remain after the retained suffix is dropped go through the same dispatch planner as column data.

const std = @import("std");
const builtin = @import("builtin");
const ast = @import("ast.zig");
const prune = @import("prune.zig");
const schema = @import("../schema.zig");
const bloom = @import("../parquet/bloom.zig");
const compression = @import("../parquet/compression.zig");
const thrift = @import("../thrift.zig");
const coalescer = @import("../../io/coalescer.zig");
const nowMonoNs = @import("../../clock.zig").monoNs;

// ============================================================
// Literal keys: the one place a bound literal becomes stored bytes
// ============================================================

/// A literal's stored form: the bytes PLAIN encodes it as, which is what a Bloom filter hashed.
pub const Key = struct {
    fixed: [2][8]u8 = undefined,
    /// Bytes of a fixed-width form; 0 for byte arrays, held in `str`.
    width: u8 = 0,
    str: []const u8 = &.{},
    /// Forms to rule out: 2 for a float zero, which equals the other signed zero under `=` though its bits differ.
    n: u8 = 1,

    /// Stored form `i`; form 0 is the literal's own.
    pub fn form(self: *const Key, i: usize) []const u8 {
        return if (self.width == 0) self.str else self.fixed[i][0..self.width];
    }

    fn put(self: *Key, i: usize, comptime T: type, v: T) void {
        std.mem.writeInt(T, self.fixed[i][0..@sizeOf(T)], v, .little);
        self.width = @sizeOf(T);
    }
};

/// The column an `=` leaf on a stored literal tests, else null.
pub fn eqColumn(f: ast.Filter) ?usize {
    return switch (f) {
        inline .int32, .int64, .uint64, .float, .double, .string => |l| {
            return if (l.op == .Eq and l.stored) l.col_idx else null;
        },
        else => null,
    };
}

/// The stored form of `f`'s literal in `rg`'s chunk of its column, or null when membership cannot decide it: no `=`
/// on a stored literal (the binder decides that), a lane that is not the chunk's physical type, a fixed-length literal
/// of another width, NaN, a repeated or encrypted column, or a chunk kept in another file.
pub fn keyOf(f: ast.Filter, rg: *const schema.RowGroup, fm: *const schema.FileMetaData) ?Key {
    const slot = chunkSlot(rg, eqColumn(f) orelse return null, fm) orelse return null;
    return storedForm(f, slot.physical, slot.type_length);
}

/// What membership needs of a chunk it can decide `=` on.
const Slot = struct { physical: schema.Type, type_length: ?i32 };

/// The chunk of column `col` in `rg`, when membership can decide `=` on it at all: flat, unencrypted, kept in this
/// file, and of the physical type its schema declares.
fn chunkSlot(rg: *const schema.RowGroup, col: usize, fm: *const schema.FileMetaData) ?Slot {
    if (col >= rg.columns.items.len) return null;
    const chunk = &rg.columns.items[col];
    if (chunk.encrypted or chunk.file_path != null) return null;
    const cm = &(chunk.meta_data orelse return null);
    const elem = fm.getColumnSchema(cm.path_in_schema.items) orelse return null;
    if (elem.type == null or elem.type.? != cm.type) return null;
    if (fm.getColumnLevels(cm.path_in_schema.items).max_rep != 0) return null;
    return .{ .physical = cm.type, .type_length = elem.type_length };
}

/// The PLAIN bytes of `f`'s literal as a value of physical type `physical`, or null when it has none. A null
/// `type_length` skips the width check of fixed-length literals.
fn storedForm(f: ast.Filter, physical: schema.Type, type_length: ?i32) ?Key {
    if (eqColumn(f) == null) return null;
    var k: Key = .{};
    switch (f) {
        .int32 => |l| {
            if (physical != .INT32) return null;
            k.put(0, i32, l.value);
        },
        .int64 => |l| {
            if (physical != .INT64) return null;
            k.put(0, i64, l.value);
        },
        // UINT_8/16/32 are stored as the INT32 with the same low 32 bits.
        .uint64 => |l| switch (physical) {
            .INT32 => k.put(0, u32, std.math.cast(u32, l.value) orelse return null),
            .INT64 => k.put(0, u64, l.value),
            else => return null,
        },
        .float => |l| {
            if (physical != .FLOAT or std.math.isNan(l.value)) return null;
            k.put(0, u32, @bitCast(l.value));
            if (l.value == 0) {
                k.put(1, u32, @bitCast(-l.value));
                k.n = 2;
            }
        },
        .double => |l| {
            if (physical != .DOUBLE or std.math.isNan(l.value)) return null;
            k.put(0, u64, @bitCast(l.value));
            if (l.value == 0) {
                k.put(1, u64, @bitCast(-l.value));
                k.n = 2;
            }
        },
        .string => |l| {
            switch (physical) {
                .BYTE_ARRAY => {},
                .FIXED_LEN_BYTE_ARRAY => if (type_length) |tl| if (l.value.len != tl) return null,
                else => return null,
            }
            k.str = l.value;
        },
        else => return null,
    }
    return k;
}

fn bytesLess(_: void, a: []const u8, b: []const u8) bool {
    return std.mem.lessThan(u8, a, b);
}

/// Whether `load` proved the `=` leaf `f` matches no row of `rg`. What `prune.zig` asks at an `=` leaf; cheap enough
/// for every leaf of every row group, since a chunk carries absent forms only once `load` has vetted it: equal stored
/// bytes are the same value, so no schema check is repeated here.
pub fn provenAbsent(rg: *const schema.RowGroup, f: ast.Filter) bool {
    const col = eqColumn(f) orelse return false;
    if (col >= rg.columns.items.len) return false;
    const chunk = &rg.columns.items[col];
    if (chunk.absent.len == 0) return false;
    const cm = chunk.meta_data orelse return false;
    const k = storedForm(f, cm.type, null) orelse return false;
    return std.sort.binarySearch([]const u8, chunk.absent, k.form(0), struct {
        fn cmp(want: []const u8, have: []const u8) std.math.Order {
            return std.mem.order(u8, want, have);
        }
    }.cmp) != null;
}

// ============================================================
// Sources and their cost
// ============================================================

/// What reading costs on a medium. The planner merges nearby ranges when the bytes in between cost less than the
/// requests they save, and balances the merged spans over the requests in flight.
pub const Cost = struct {
    /// A request's fixed cost, as the bytes the medium moves in that time. 0 for memory already mapped: nothing is
    /// merged, and probing everything costs too little to need an adaptive first wave.
    request_bytes: u64,
    /// Requests in flight at once.
    concurrency: usize = 1,
    /// The longest request the medium serves whole; longer spans are planned as several.
    max_request: u64 = std.math.maxInt(u64),
    /// Latency cap for `coalesceDispatch`. 0 keeps the cost-model planner.
    gap_cap: u64 = 0,
    /// In-flight cap for that planner. 0 uses `concurrency`.
    in_flight: usize = 0,
    /// Split a dispatch span longer than this. 0 leaves the planner's spans whole.
    split: u64 = 0,
    /// Window charged to one file's bloom spans. Null does not cap a gap.
    window: ?u64 = null,

    pub const mapped: Cost = .{ .request_bytes = 0 };

    fn remote(self: Cost) bool {
        return self.request_bytes > 0;
    }
};

/// A byte range of input file `file` (an index into the `metas` given to `load`).
pub const Span = struct {
    file: u32,
    start: u64,
    end: u64,

    fn len(self: Span) u64 {
        return self.end - self.start;
    }
};

pub const Source = struct {
    ctx: *anyopaque,
    vtable: *const VTable,
    cost: Cost,

    pub const VTable = struct {
        /// Fill `out[i]` with the bytes of `spans[i]`, valid until the query ends. A span reaching past its file comes
        /// back short or empty, and what it does not cover reads as "no filter", never as an error.
        read: *const fn (
            ctx: *anyopaque,
            arena: std.mem.Allocator,
            spans: []const Span,
            out: [][]const u8,
        ) anyerror!void,
        /// A whole filter already in memory (a warm cache), or null.
        cached: ?*const fn (ctx: *anyopaque, arena: std.mem.Allocator, span: Span) ?[]const u8 = null,
        /// A whole filter this query read, offered for reuse by later queries.
        retain: ?*const fn (ctx: *anyopaque, span: Span, bytes: []const u8) void = null,
        /// Start fetching `spans` in the background, so the reads after it overlap instead of waiting in turn: on a
        /// mapping, one cold page fault per probe would otherwise be one disk read at a time.
        prefetch: ?*const fn (ctx: *anyopaque, spans: []const Span) void = null,
        /// The cost as this query has measured it so far, once the source has timed enough of its reads; `cost`
        /// stands until then.
        measured: ?*const fn (ctx: *anyopaque) ?Cost = null,
        /// The retained tail of `file`, when this source already holds one. Bytes inside it are not fetched.
        suffix: ?*const fn (ctx: *anyopaque, file: u32) ?Span = null,
        /// Write `spans[i]` into `into[i]` (`into[i].len` is the span). Null falls back to `read` plus a copy.
        read_into: ?*const fn (
            ctx: *anyopaque,
            arena: std.mem.Allocator,
            spans: []const Span,
            into: [][]u8,
        ) anyerror!void = null,
        /// Copy a cached span into `dest` (`dest.len` is the span). False when this source has no such blob.
        /// The copy is made under the cache lock: a borrowed blob can be evicted by the next insert.
        copy_cached: ?*const fn (ctx: *anyopaque, span: Span, dest: []u8) bool = null,
        /// Whether `copy_cached` would succeed for `span`. A hit is still copied into the wave buffer and charged.
        has_cached: ?*const fn (ctx: *anyopaque, span: Span) bool = null,
    };

    /// What planning should assume now.
    pub fn currentCost(self: Source) Cost {
        if (self.vtable.measured) |m| if (m(self.ctx)) |c| return c;
        return self.cost;
    }

    fn prefetch(self: Source, spans: []const Span) void {
        if (spans.len > 0) if (self.vtable.prefetch) |f| f(self.ctx, spans);
    }

    fn read(self: Source, arena: std.mem.Allocator, spans: []const Span, out: [][]const u8) !void {
        return self.vtable.read(self.ctx, arena, spans, out);
    }

    fn suffix(self: Source, file: u32) ?Span {
        const f = self.vtable.suffix orelse return null;
        return f(self.ctx, file);
    }

    fn readInto(self: Source, arena: std.mem.Allocator, spans: []const Span, into: [][]u8) !void {
        if (self.vtable.read_into) |f| return f(self.ctx, arena, spans, into);
        const got = try arena.alloc([]const u8, spans.len);
        try self.read(arena, spans, got);
        for (into, got) |dest, g| {
            const n = @min(dest.len, g.len);
            @memcpy(dest[0..n], g[0..n]);
        }
    }

    fn hasCached(self: Source, span: Span) bool {
        const f = self.vtable.has_cached orelse return false;
        return f(self.ctx, span);
    }

    fn copyCached(self: Source, span: Span, dest: []u8) bool {
        const f = self.vtable.copy_cached orelse return false;
        if (dest.len != span.len()) return false;
        return f(self.ctx, span, dest);
    }
};

/// Files already in memory, whole (mapped local files): reads are slices, so they cost nothing to plan around.
pub const MappedSource = struct {
    /// Indexed like `metas`; an empty entry is a file not held here, whose reads come back empty.
    files: []const []const u8,

    pub fn source(self: *const MappedSource) Source {
        return .{ .ctx = @ptrCast(@constCast(self)), .vtable = &vtable, .cost = .mapped };
    }

    const vtable: Source.VTable = .{ .read = readFn, .prefetch = prefetchFn };

    /// MADV_WILLNEED on the pages under each span: the kernel queues every read at once.
    fn prefetchFn(ctx: *anyopaque, spans: []const Span) void {
        if (builtin.os.tag != .linux) return;
        const self: *const MappedSource = @ptrCast(@alignCast(ctx));
        const page = std.heap.pageSize();
        for (spans) |s| {
            const bytes = if (s.file < self.files.len) self.files[s.file] else continue;
            if (s.start >= bytes.len) continue;
            const end: usize = @intCast(@min(s.end, bytes.len));
            const lo = std.mem.alignBackward(usize, @intFromPtr(bytes.ptr) + @as(usize, @intCast(s.start)), page);
            const hi = std.mem.alignForward(usize, @intFromPtr(bytes.ptr) + end, page);
            // Advice only: a failure leaves the pages to fault in on demand.
            _ = std.os.linux.madvise(@ptrFromInt(lo), hi - lo, std.os.linux.MADV.WILLNEED);
        }
    }

    fn readFn(ctx: *anyopaque, _: std.mem.Allocator, spans: []const Span, out: [][]const u8) anyerror!void {
        const self: *const MappedSource = @ptrCast(@alignCast(ctx));
        for (spans, out) |s, *o| {
            const bytes = if (s.file < self.files.len) self.files[s.file] else &.{};
            const start = @min(s.start, bytes.len);
            o.* = bytes[@intCast(start)..@intCast(@min(s.end, bytes.len))];
        }
    }
};

// ============================================================
// Read planning: merge by cost, balance over the requests in flight
// ============================================================

/// Gaps the planner tries merging across. Every one is costed; none is a tuning knob.
const candidate_gaps = [_]u64{
    0,       16 << 10, 64 << 10, 256 << 10,            1 << 20,
    4 << 20, 16 << 20, 64 << 20, std.math.maxInt(u64),
};

/// The spans to read for `ranges` (sorted by file, then start): merged across the gap that minimises the estimated
/// time, then split so no request is longer than the medium takes and the bytes spread over every request in flight.
pub fn planSpans(arena: std.mem.Allocator, ranges: []const Span, cost: Cost) ![]Span {
    if (ranges.len == 0) return &.{};
    // In memory, adjacent and overlapping ranges still merge (it is free), so the spans come out disjoint.
    if (!cost.remote()) return merge(arena, ranges, 0);
    var best: []Span = &.{};
    var best_t: u64 = std.math.maxInt(u64);
    for (candidate_gaps) |gap| {
        const spans = try split(arena, try merge(arena, ranges, gap), cost);
        const t = estimate(arena, spans, cost) catch std.math.maxInt(u64);
        if (t < best_t) {
            best = spans;
            best_t = t;
        }
    }
    return best;
}

/// The gap bytes one merged span may carry: a mebibyte, or twice the bytes it is read for, whichever is more. A plan
/// that reads several times what it wants bets on bandwidth the medium may not have, and cold reads vary too much to
/// take that bet on an estimate.
pub const max_gap_floor: u64 = 1 << 20;

fn merge(arena: std.mem.Allocator, ranges: []const Span, gap: u64) ![]Span {
    var out: std.ArrayList(Span) = .empty;
    var cur = ranges[0];
    var wanted = cur.len();
    var gaps: u64 = 0;
    for (ranges[1..]) |r| {
        const g = if (r.start > cur.end) r.start - cur.end else 0;
        const adds = if (r.end > cur.end) r.end - @max(r.start, cur.end) else 0;
        if (r.file == cur.file and g <= gap and gaps + g <= @max(max_gap_floor, 2 * (wanted + adds))) {
            cur.end = @max(cur.end, r.end);
            wanted += adds;
            gaps += g;
        } else {
            try out.append(arena, cur);
            cur = r;
            wanted = r.len();
            gaps = 0;
        }
    }
    try out.append(arena, cur);
    return out.items;
}

fn splitAt(arena: std.mem.Allocator, spans: []const Span, split_len: u64) ![]Span {
    if (split_len == 0) return arena.dupe(Span, spans);
    var out: std.ArrayList(Span) = .empty;
    for (spans) |s| {
        var at = s.start;
        while (at < s.end) {
            const n = @min(split_len, s.end - at);
            try out.append(arena, .{ .file = s.file, .start = at, .end = at + n });
            at += n;
        }
    }
    return out.items;
}

fn split(arena: std.mem.Allocator, spans: []const Span, cost: Cost) ![]Span {
    var total: u64 = 0;
    for (spans) |s| total += s.len();
    // Pieces long enough to repay their request, short enough to keep every request in flight busy.
    const want = std.math.divCeil(u64, total, @max(cost.concurrency, 1)) catch unreachable;
    const piece = @max(@min(@max(want, cost.request_bytes), cost.max_request), 1);
    var out: std.ArrayList(Span) = .empty;
    for (spans) |s| {
        const n = std.math.divCeil(u64, s.len(), piece) catch unreachable;
        if (n <= 1) {
            try out.append(arena, s);
            continue;
        }
        const each = std.math.divCeil(u64, s.len(), n) catch unreachable;
        var at = s.start;
        while (at < s.end) : (at += each) {
            try out.append(arena, .{ .file = s.file, .start = at, .end = @min(at + each, s.end) });
        }
    }
    return out.items;
}

/// Estimated wall time of reading `spans`, in bytes-equivalent: each request costs its overhead plus its bytes, and
/// requests go longest first to whichever of the `concurrency` lanes frees up first.
pub fn estimate(arena: std.mem.Allocator, spans: []const Span, cost: Cost) !u64 {
    const lanes = try arena.alloc(u64, @max(cost.concurrency, 1));
    defer arena.free(lanes);
    @memset(lanes, 0);
    const costs = try arena.alloc(u64, spans.len);
    defer arena.free(costs);
    for (spans, costs) |s, *c| c.* = cost.request_bytes + s.len();
    std.mem.sort(u64, costs, {}, std.sort.desc(u64));
    for (costs) |c| {
        const i = std.mem.indexOfMin(u64, lanes);
        lanes[i] += c;
    }
    return std.mem.max(u64, lanes);
}

/// One range to read and, once read, its bytes.
const Req = struct {
    span: Span,
    out: []const u8 = &.{},
};

fn spanLess(_: void, a: Span, b: Span) bool {
    return if (a.file != b.file) a.file < b.file else a.start < b.start;
}

fn sortedSpans(arena: std.mem.Allocator, reqs: []const Req) ![]Span {
    const ranges = try arena.alloc(Span, reqs.len);
    for (reqs, ranges) |r, *s| s.* = r.span;
    std.mem.sort(Span, ranges, {}, spanLess);
    return ranges;
}

/// How `readAll` turns requests into spans. Header probes stay on the cost model. Bloom bodies use the dispatch
/// planner when the source has a latency cap. `exact` does not merge: a header probe is 64 bytes, and merging the
/// gap between filters would allocate the bodies the probe exists to size.
const ReadKind = enum { cost, dispatch, exact };

/// The part of `span` the suffix does not already hold, and the part it does.
fn clipSuffix(span: Span, suf: ?Span) struct { http: ?Span, tail: ?Span } {
    if (span.end <= span.start) return .{ .http = null, .tail = null };
    const s = suf orelse return .{ .http = span, .tail = null };
    if (s.file != span.file or s.end <= span.start or s.start >= span.end) return .{ .http = span, .tail = null };
    const http: ?Span = if (span.start < s.start) .{
        .file = span.file,
        .start = span.start,
        .end = @min(span.end, s.start),
    } else null;
    const tail_start = @max(span.start, s.start);
    const tail_end = @min(span.end, s.end);
    const tail: ?Span = if (tail_end > tail_start) .{
        .file = span.file,
        .start = tail_start,
        .end = tail_end,
    } else null;
    return .{ .http = http, .tail = tail };
}

/// Bloom ranges through `coalesceDispatch`, before the split. Every piece of one file shares that file's group, so a
/// contiguous column joins; the window still charges the gaps and refuses one that would push the span past the limit.
fn dispatchCovers(arena: std.mem.Allocator, ranges: []const Span, cost: Cost) ![]Span {
    if (ranges.len == 0) return &.{};
    var selected: u64 = 0;
    const pieces = try arena.alloc(coalescer.Piece, ranges.len);
    for (ranges, pieces) |r, *p| {
        selected += r.len();
        p.* = .{
            .object = r.file,
            .group = r.file,
            .start = r.start,
            .end = r.end,
            .gap_cap = cost.gap_cap,
        };
    }
    const flight = if (cost.in_flight > 0) cost.in_flight else @max(cost.concurrency, 1);
    const planned = try coalescer.coalesceDispatch(arena, pieces, flight, selected *| 2, cost.window);
    var out: std.ArrayList(Span) = .empty;
    for (planned) |sp| try out.append(arena, .{ .file = sp.object, .start = sp.start, .end = sp.end });
    std.mem.sort(Span, out.items, {}, spanLess);
    return out.items;
}

/// Bloom ranges through `coalesceDispatch`. Planned spans then split at `cost.split`.
fn planDispatch(arena: std.mem.Allocator, ranges: []const Span, cost: Cost) ![]Span {
    const covers = try dispatchCovers(arena, ranges, cost);
    var out: std.ArrayList(Span) = .empty;
    for (covers) |sp| {
        var at = sp.start;
        while (at < sp.end) {
            const room = sp.end - at;
            const n = if (cost.split == 0) room else @min(cost.split, room);
            try out.append(arena, .{ .file = sp.file, .start = at, .end = at + n });
            at += n;
        }
    }
    std.mem.sort(Span, out.items, {}, spanLess);
    return out.items;
}

/// The buffers `readPlaced` allocates for `ranges`: one per merged span, before any split. The same function the
/// admission charge uses, so the bytes reserved and the bytes allocated cannot drift. `plans` counts one bloom-body
/// plan; header probes are exact and are not plans.
fn coverSpans(arena: std.mem.Allocator, ranges: []const Span, cost: Cost, kind: ReadKind, plans: ?*usize) ![]Span {
    if (ranges.len == 0) return &.{};
    if (kind == .exact) return arena.dupe(Span, ranges);
    if (plans) |p| p.* += 1;
    if (kind == .dispatch and cost.gap_cap > 0) return dispatchCovers(arena, ranges, cost);
    const sorted = try arena.dupe(Span, ranges);
    std.mem.sort(Span, sorted, {}, spanLess);
    if (!cost.remote()) return merge(arena, sorted, 0);
    var best: []Span = &.{};
    var best_t: u64 = std.math.maxInt(u64);
    for (candidate_gaps) |gap| {
        const merged = try merge(arena, sorted, gap);
        const spans = try split(arena, merged, cost);
        const t = estimate(arena, spans, cost) catch std.math.maxInt(u64);
        if (t < best_t) {
            best = merged;
            best_t = t;
        }
    }
    return best;
}

fn joinBytes(arena: std.mem.Allocator, head: []const u8, tail: []const u8) ![]const u8 {
    if (tail.len == 0) return head;
    if (head.len == 0) return tail;
    const out = try arena.alloc(u8, head.len + tail.len);
    @memcpy(out[0..head.len], head);
    @memcpy(out[head.len..], tail);
    return out;
}

/// One round trip of bandwidth. A contiguous run larger than this is waved, even when the fetch window would hold it.
const one_round_bytes: u64 = 8 << 20;

/// Bytes a probe of `span` allocates. One buffer of the span: split requests and a suffix tail write into it. A span
/// the retained tail already holds is a slice of that tail, and allocates nothing.
fn heldBytes(span: Span, suf: ?Span) u64 {
    if (span.end <= span.start) return 0;
    const clip = clipSuffix(span, suf);
    if (clip.http == null) return 0;
    return span.len();
}

/// Read every request through `src`. On a remote source each merged span is one buffer: HTTP pieces and the suffix
/// tail write into it, and a cache hit is copied into it. A mapping still returns slices of the file.
fn readAll(arena: std.mem.Allocator, src: Source, reqs: []Req, stats: *Stats, kind: ReadKind, planned: ?[]const Span) !void {
    if (reqs.len == 0) return;
    if (src.cost.remote()) return readPlaced(arena, src, reqs, stats, kind, planned);
    const cost = src.currentCost();
    const Part = struct { http: ?Span, tail: ?usize };
    const parts = try arena.alloc(Part, reqs.len);
    var http: std.ArrayList(Span) = .empty;
    var tails: std.ArrayList(Span) = .empty;
    for (reqs, parts) |r, *p| {
        const clip = clipSuffix(r.span, src.suffix(r.span.file));
        p.http = clip.http;
        if (clip.http) |h| try http.append(arena, h);
        if (clip.tail) |t| {
            p.tail = tails.items.len;
            try tails.append(arena, t);
        } else p.tail = null;
    }
    const http_plan: []Span = if (http.items.len == 0)
        &.{}
    else if (kind == .dispatch and cost.gap_cap > 0) blk: {
        break :blk try planDispatch(arena, http.items, cost);
    } else blk: {
        std.mem.sort(Span, http.items, {}, spanLess);
        break :blk try planSpans(arena, http.items, cost);
    };
    const got = try arena.alloc([]const u8, http_plan.len);
    if (http_plan.len > 0) try src.read(arena, http_plan, got);
    const tail_got = try arena.alloc([]const u8, tails.items.len);
    if (tails.items.len > 0) try src.read(arena, tails.items, tail_got);
    // A mapping reads nothing up front; the pages a probe touches fault in on their own.
    if (src.cost.remote()) {
        stats.requests += http_plan.len;
        for (got) |g| stats.bytes += g.len;
        for (tail_got) |g| stats.suffix_bytes += g.len;
    }
    for (reqs, parts) |*r, p| {
        const head = if (p.http) |h| try assemble(arena, http_plan, got, h) else &.{};
        const tail = if (p.tail) |i| tail_got[i] else &.{};
        r.out = try joinBytes(arena, head, tail);
    }
}

/// `holes` with `cut` removed. Spans stay inside one file.
fn punch(arena: std.mem.Allocator, holes: []const Span, cut: Span) ![]Span {
    var out: std.ArrayList(Span) = .empty;
    for (holes) |h| {
        if (h.file != cut.file or cut.end <= h.start or cut.start >= h.end) {
            try out.append(arena, h);
            continue;
        }
        if (h.start < cut.start) try out.append(arena, .{ .file = h.file, .start = h.start, .end = cut.start });
        if (cut.end < h.end) try out.append(arena, .{ .file = h.file, .start = cut.end, .end = h.end });
    }
    return out.items;
}

fn coverOf(covers: []const Span, s: Span) ?usize {
    for (covers, 0..) |c, i| {
        if (c.file == s.file and s.start >= c.start and s.end <= c.end) return i;
    }
    return null;
}

/// One buffer per merged span. Split HTTP writes and the suffix tail land at their offsets. A cache hit is copied
/// into that buffer and is not fetched. `stats.bytes` counts only HTTP. `planned` is the wave's one plan; a null
/// plans the requests itself.
fn readPlaced(arena: std.mem.Allocator, src: Source, reqs: []Req, stats: *Stats, kind: ReadKind, planned: ?[]const Span) !void {
    const cost = src.currentCost();
    const covers = if (planned) |p| p else blk: {
        const ranges = try arena.alloc(Span, reqs.len);
        for (reqs, ranges) |r, *s| s.* = r.span;
        break :blk try coverSpans(arena, ranges, cost, kind, null);
    };
    const Buf = struct {
        bytes: []u8,
        borrowed: ?[]const u8,
        start: u64,
        file: u32,
    };
    const bufs = try arena.alloc(Buf, covers.len);
    var http_spans: std.ArrayList(Span) = .empty;
    var http_dests: std.ArrayList([]u8) = .empty;

    for (covers, bufs) |c, *b| {
        const clip = clipSuffix(c, src.suffix(c.file));
        if (clip.http == null) {
            const only = [_]Span{c};
            var got: [1][]const u8 = undefined;
            try src.read(arena, &only, &got);
            stats.suffix_bytes += got[0].len;
            b.* = .{ .bytes = &.{}, .borrowed = got[0], .start = c.start, .file = c.file };
            continue;
        }
        const dest = try arena.alloc(u8, @intCast(c.len()));
        b.* = .{ .bytes = dest, .borrowed = null, .start = c.start, .file = c.file };
        var http_holes: []const Span = &.{};
        if (clip.http) |h| {
            const hole = try arena.alloc(Span, 1);
            hole[0] = h;
            http_holes = hole;
        }
        var tail_holes: []const Span = &.{};
        if (clip.tail) |t| {
            const hole = try arena.alloc(Span, 1);
            hole[0] = t;
            tail_holes = hole;
        }
        for (reqs) |r| {
            if (r.span.file != c.file or r.span.start < c.start or r.span.end > c.end) continue;
            const off: usize = @intCast(r.span.start - c.start);
            const n: usize = @intCast(r.span.len());
            if (n > dest.len - off) continue;
            if (!src.copyCached(r.span, dest[off..][0..n])) continue;
            http_holes = try punch(arena, http_holes, r.span);
            tail_holes = try punch(arena, tail_holes, r.span);
        }
        if (tail_holes.len > 0) {
            const got = try arena.alloc([]const u8, tail_holes.len);
            try src.read(arena, tail_holes, got);
            for (tail_holes, got) |h, g| {
                const off: usize = @intCast(h.start - c.start);
                const n = @min(g.len, @as(usize, @intCast(h.len())));
                if (off + n <= dest.len) @memcpy(dest[off..][0..n], g[0..n]);
                stats.suffix_bytes += n;
            }
        }
        if (http_holes.len > 0) {
            const pieces: []const Span = if (kind == .exact)
                http_holes
            else if (kind == .dispatch)
                try splitAt(arena, http_holes, cost.split)
            else
                try split(arena, http_holes, cost);
            for (pieces) |h| {
                const off: usize = @intCast(h.start - c.start);
                const n: usize = @intCast(h.len());
                if (off + n > dest.len) continue;
                try http_spans.append(arena, h);
                try http_dests.append(arena, dest[off..][0..n]);
            }
        }
    }

    if (http_spans.items.len > 0) {
        try src.readInto(arena, http_spans.items, http_dests.items);
        stats.requests += http_spans.items.len;
        for (http_dests.items) |d| stats.bytes += d.len;
    }
    for (reqs) |*r| {
        const i = coverOf(covers, r.span) orelse {
            r.out = &.{};
            continue;
        };
        const b = bufs[i];
        const off: usize = @intCast(r.span.start - b.start);
        const n: usize = @intCast(r.span.len());
        if (b.borrowed) |br| {
            r.out = if (off >= br.len) &.{} else br[off..@min(br.len, off + n)];
        } else if (off >= b.bytes.len) {
            r.out = &.{};
        } else {
            r.out = b.bytes[off..@min(b.bytes.len, off + n)];
        }
    }
}

fn estimateReqs(arena: std.mem.Allocator, reqs: []const Req, cost: Cost) !u64 {
    if (reqs.len == 0) return 0;
    return estimate(arena, try planSpans(arena, try sortedSpans(arena, reqs), cost), cost);
}

/// The bytes of `want` out of the read `spans` (sorted, disjoint), as far as they cover it from its start.
fn assemble(arena: std.mem.Allocator, spans: []const Span, got: []const []const u8, want: Span) ![]const u8 {
    var i = std.sort.lowerBound(Span, spans, want, struct {
        fn cmp(w: Span, s: Span) std.math.Order {
            if (w.file != s.file) return std.math.order(w.file, s.file);
            return if (s.end <= w.start) .gt else .lt;
        }
    }.cmp);
    if (i >= spans.len or spans[i].file != want.file or spans[i].start > want.start) return &.{};
    const first = got[i][@intCast(@min(want.start - spans[i].start, got[i].len))..];
    if (want.end <= spans[i].end or first.len < spans[i].end - want.start)
        return first[0..@intCast(@min(first.len, want.len()))];
    var buf: std.ArrayList(u8) = .empty;
    try buf.appendSlice(arena, first);
    i += 1;
    while (buf.items.len < want.len() and i < spans.len and spans[i].file == want.file and
        spans[i].start == want.start + buf.items.len) : (i += 1)
    {
        try buf.appendSlice(arena, got[i][0..@intCast(@min(got[i].len, want.len() - buf.items.len))]);
        if (got[i].len < spans[i].len()) break;
    }
    return buf.items;
}

// ============================================================
// load
// ============================================================

pub const Options = struct {
    /// On a remote source, the row groups whose filters are read first. If under `min_wave_prune` of them prune, the
    /// rest are left unread: that bounds what filters cost a query they cannot help.
    wave: usize = 8,
    min_wave_prune: f64 = 0.25,
};

pub const Stats = struct {
    /// Row groups statistics kept that a Bloom filter, or failing that a dictionary, then ruled out.
    row_groups_bloom: usize = 0,
    row_groups_dict: usize = 0,
    /// Filters and dictionaries probed.
    filters: usize = 0,
    dictionaries: usize = 0,
    /// HTTP requests made, and bytes fetched by them. Copies from a retained suffix are `suffix_bytes`.
    requests: usize = 0,
    bytes: u64 = 0,
    suffix_bytes: u64 = 0,
    /// A first wave pruned too little to pay for reading the rest of its kind.
    bloom_stopped: bool = false,
    dict_stopped: bool = false,
    /// Filters not read because one of them would not fit in the charged window. The row group stays unpruned.
    bloom_skipped_oversize: usize = 0,
    /// Dispatch plans of bloom bodies. One per wave: a prefix loop would count once per prefix.
    planner_calls: usize = 0,
    /// A request's fixed cost, in bytes, that the last plan assumed (remote sources only).
    request_bytes: u64 = 0,
    ns: u64 = 0,

    pub fn add(self: *Stats, o: Stats) void {
        self.request_bytes = @max(self.request_bytes, o.request_bytes);
        self.row_groups_bloom += o.row_groups_bloom;
        self.row_groups_dict += o.row_groups_dict;
        self.filters += o.filters;
        self.dictionaries += o.dictionaries;
        self.requests += o.requests;
        self.bytes += o.bytes;
        self.suffix_bytes += o.suffix_bytes;
        self.bloom_stopped = self.bloom_stopped or o.bloom_stopped;
        self.dict_stopped = self.dict_stopped or o.dict_stopped;
        self.bloom_skipped_oversize += o.bloom_skipped_oversize;
        self.planner_calls += o.planner_calls;
        self.ns += o.ns;
    }
};

/// One chunk to probe: in a row group statistics keep, of a column with `=` literals they leave open.
const Target = struct {
    file: u32,
    rg: u32,
    col: u32,
    keys: std.ArrayList(Key) = .empty,
    /// Forms 0 of the keys proven absent.
    absent: std.ArrayList([]const u8) = .empty,
    /// Length learned from a header probe when the footer omits `bloom_filter_length`. Not written back to the footer.
    learned: ?u64 = null,

    fn chunk(self: *const Target, metas: []const schema.FileMetaData) *schema.ColumnChunk {
        return &metas[self.file].row_groups.items[self.rg].columns.items[self.col];
    }

    fn rowGroup(self: *const Target, metas: []const schema.FileMetaData) *const schema.RowGroup {
        return &metas[self.file].row_groups.items[self.rg];
    }

    fn sameRowGroup(self: *const Target, other: *const Target) bool {
        return self.file == other.file and self.rg == other.rg;
    }
};

/// Whether `k` is among `proven`, the keys earlier waves proved absent (sorted as each wave ended).
fn isAbsent(proven: []const []const u8, k: *const Key) bool {
    return std.sort.binarySearch([]const u8, proven, k.form(0), orderBytes) != null;
}

fn orderBytes(want: []const u8, have: []const u8) std.math.Order {
    return std.mem.order(u8, want, have);
}

/// Probes hoisted per literal form: the hash and its 8-lane mask, computed once however many chunks they test.
const ProbeCache = struct {
    forms: std.StringHashMapUnmanaged(bloom.Probe) = .empty,

    fn get(self: *ProbeCache, arena: std.mem.Allocator, bytes: []const u8) !bloom.Probe {
        const gop = try self.forms.getOrPut(arena, bytes);
        if (!gop.found_existing) {
            gop.key_ptr.* = try arena.dupe(u8, bytes);
            gop.value_ptr.* = bloom.Probe.init(bloom.hashBytes(bytes));
        }
        return gop.value_ptr.*;
    }
};

/// Prove `=` literals of `filter` absent from chunks of `metas` wherever their Bloom filters, then their dictionaries,
/// allow, and record each on its chunk for `prune.zig`. `metas` are this query's footers, all parsed against the
/// schema `filter` was bound to, and file `i` of `src` is `metas[i]`. What is recorded lives in `arena`; the filter
/// and dictionary bytes read on the way only in a scratch arena over `gpa`, freed on return. Nothing is recorded under
/// any doubt.
pub fn load(
    arena: std.mem.Allocator,
    gpa: std.mem.Allocator,
    filter: ast.Filter,
    metas: []const schema.FileMetaData,
    src: Source,
    opts: Options,
) !Stats {
    const t0 = nowMonoNs();
    var scratch = std.heap.ArenaAllocator.init(gpa);
    defer scratch.deinit();
    var wave = std.heap.ArenaAllocator.init(gpa);
    defer wave.deinit();
    var stats: Stats = .{};
    var p: Probing = .{
        .arena = arena,
        .scratch = scratch.allocator(),
        .wave = &wave,
        .metas = metas,
        .src = src,
        .stats = &stats,
        .filter = filter,
        .opts = opts,
    };
    try p.run();
    if (src.cost.remote()) stats.request_bytes = src.currentCost().request_bytes;
    stats.ns = @intCast(nowMonoNs() - t0);
    return stats;
}

/// Every `=` leaf on a stored literal, anywhere in `f`: AND, OR and IN compose through `pruneRowGroup`.
fn collectLeaves(arena: std.mem.Allocator, f: ast.Filter, out: *std.ArrayList(ast.Filter)) !void {
    switch (f) {
        .and_filter, .or_filter => |c| {
            try collectLeaves(arena, c.left.*, out);
            try collectLeaves(arena, c.right.*, out);
        },
        else => if (eqColumn(f) != null) try out.append(arena, f),
    }
}

/// The chunks worth probing: in row groups statistics keep, for `=` leaves their own statistics leave open. One they
/// skip prunes already; one they prove matches every row holds its literal.
fn findTargets(
    arena: std.mem.Allocator,
    filter: ast.Filter,
    leaves: []const ast.Filter,
    metas: []const schema.FileMetaData,
) !std.ArrayList(Target) {
    // Leaves grouped by column, so a chunk is vetted once however long an IN list is.
    const sorted = try arena.dupe(ast.Filter, leaves);
    std.mem.sort(ast.Filter, sorted, {}, struct {
        fn lt(_: void, x: ast.Filter, y: ast.Filter) bool {
            return eqColumn(x).? < eqColumn(y).?;
        }
    }.lt);
    var targets: std.ArrayList(Target) = .empty;
    for (metas, 0..) |*fm, fi| for (fm.row_groups.items, 0..) |*rg, ri| {
        if (prune.pruneRowGroup(rg, filter, fm) == .skip) continue;
        var at: usize = 0;
        while (at < sorted.len) {
            const col = eqColumn(sorted[at]).?;
            var end = at + 1;
            while (end < sorted.len and eqColumn(sorted[end]).? == col) end += 1;
            defer at = end;
            const slot = chunkSlot(rg, col, fm) orelse continue;
            var t: Target = .{ .file = @intCast(fi), .rg = @intCast(ri), .col = @intCast(col) };
            for (sorted[at..end]) |leaf| {
                switch (prune.pruneRowGroup(rg, leaf, fm)) {
                    .keep, .unknown => {},
                    .skip, .always_match => continue,
                }
                try t.keys.append(arena, storedForm(leaf, slot.physical, slot.type_length) orelse continue);
            }
            if (t.keys.items.len > 0) try targets.append(arena, t);
        }
    };
    return targets;
}

/// Where a target's filter lies, as far as the footer says: the whole filter when it gives the length, else an empty
/// span at the offset.
fn filterSpan(t: *const Target, metas: []const schema.FileMetaData) ?Span {
    const cm = t.chunk(metas).meta_data orelse return null;
    const off = cm.bloom_filter_offset orelse return null;
    // Only the magic precedes offset 4.
    if (off < 4) return null;
    const start: u64 = @intCast(off);
    if (cm.bloom_filter_length) |l| {
        // No reader accepts a bitset past the cap, so a longer length is corrupt, not a filter.
        if (l <= 0 or l > bloom.max_filter_bytes) return null;
        return .{ .file = t.file, .start = start, .end = start + @as(u64, @intCast(l)) };
    }
    return .{ .file = t.file, .start = start, .end = start };
}

/// One past the last target of the first `n` row groups.
fn waveEnd(ts: []const *Target, n: usize) usize {
    var rgs: usize = 0;
    for (ts, 0..) |t, i| {
        if (i == 0 or !ts[i - 1].sameRowGroup(t)) {
            if (rgs == n) return i;
            rgs += 1;
        }
    }
    return ts.len;
}

/// Whether a chunk's dictionary page holds every value it stores, so a literal missing from it is missing from the
/// chunk. A writer may start a chunk dictionary-encoded and fall back to PLAIN pages once the dictionary grows, and
/// then the dictionary covers only part of it. With `encoding_stats`: a dictionary page, data pages, and every data
/// page dictionary-encoded (the rule Hardwood applies). Without them, only parquet-java's v1 signature tells:
/// PLAIN_DICTIONARY with nothing but level encodings beside it; a v2 RLE_DICTIONARY chunk could have fallen back to
/// PLAIN without saying so.
pub fn dictionaryCoversChunk(cm: *const schema.ColumnMetaData) bool {
    const DICTIONARY_PAGE = @backingInt(schema.PageType.DICTIONARY_PAGE);
    const plain_dictionary = @backingInt(schema.Encoding.PLAIN_DICTIONARY);
    const rle_dictionary = @backingInt(schema.Encoding.RLE_DICTIONARY);
    if (cm.encoding_stats) |es| {
        var dict_page = false;
        var data_page = false;
        for (es.items) |e| {
            if (e.count <= 0) continue;
            switch (e.page_type) {
                DICTIONARY_PAGE => dict_page = true,
                @backingInt(schema.PageType.INDEX_PAGE) => {},
                @backingInt(schema.PageType.DATA_PAGE), @backingInt(schema.PageType.DATA_PAGE_V2) => {
                    if (e.encoding != plain_dictionary and e.encoding != rle_dictionary) return false;
                    data_page = true;
                },
                else => return false,
            }
        }
        return dict_page and data_page;
    }
    var v1_dictionary = false;
    for (cm.encodings.items) |e| switch (e) {
        .PLAIN_DICTIONARY => v1_dictionary = true,
        .RLE, .BIT_PACKED => {},
        else => return false,
    };
    return v1_dictionary;
}

/// Where a target's dictionary page lies, when it covers the chunk: from `dictionary_page_offset` to the first data
/// page, or, from writers that leave the former out and start the chunk at the dictionary, an empty span at the
/// chunk's start whose length the page header gives.
fn dictSpan(t: *const Target, metas: []const schema.FileMetaData) ?Span {
    const cm = &(t.chunk(metas).meta_data orelse return null);
    if (!dictionaryCoversChunk(cm)) return null;
    if (cm.data_page_offset < 4) return null;
    const data: u64 = @intCast(cm.data_page_offset);
    if (cm.dictionary_page_offset) |d| {
        if (d < 4 or d >= cm.data_page_offset) return null;
        return .{ .file = t.file, .start = @intCast(d), .end = data };
    }
    return .{ .file = t.file, .start = data, .end = data };
}

/// Bytes read to learn a dictionary page's length from its header.
const page_header_probe = 256;

/// Header plus payload length of the dictionary page `bytes` starts with, or null when it is no dictionary page.
fn dictionaryPageLen(bytes: []const u8) ?u64 {
    var r = thrift.Reader.init(bytes);
    const h = schema.PageHeader.read(&r) catch return null;
    if (h.type != .DICTIONARY_PAGE or h.compressed_page_size < 0) return null;
    return r.pos + @as(u64, @intCast(h.compressed_page_size));
}

/// The PLAIN values of the dictionary page `page` holds, decompressed, or null for anything else (another page,
/// another encoding, a truncated or corrupt page).
fn dictionaryValues(
    scratch: std.mem.Allocator,
    page: []const u8,
    codec: schema.CompressionCodec,
) ?struct { bytes: []const u8, count: usize } {
    var r = thrift.Reader.init(page);
    const h = schema.PageHeader.read(&r) catch return null;
    if (h.type != .DICTIONARY_PAGE) return null;
    const dh = h.dictionary_page_header orelse return null;
    if (dh.encoding != .PLAIN and dh.encoding != .PLAIN_DICTIONARY) return null;
    if (dh.num_values < 0 or h.compressed_page_size < 0 or h.uncompressed_page_size < 0) return null;
    const csize: usize = @intCast(h.compressed_page_size);
    if (csize > page.len - r.pos) return null;
    const payload = page[r.pos..][0..csize];
    const size: usize = @intCast(h.uncompressed_page_size);
    const values = compression.decompress(scratch, payload, codec, size) catch return null;
    return .{ .bytes = values, .count = @intCast(dh.num_values) };
}

/// Mark `found[i]` for every `want[i]` (sorted, distinct stored forms) among the `count` PLAIN values `values` holds.
/// False when the values do not parse as `count` entries of the slot's type: then nothing can be concluded.
fn scanDictionary(values: []const u8, count: usize, slot: Slot, want: []const []const u8, found: []bool) bool {
    const width: usize = switch (slot.physical) {
        .INT32, .FLOAT => 4,
        .INT64, .DOUBLE => 8,
        .FIXED_LEN_BYTE_ARRAY => std.math.cast(usize, slot.type_length orelse return false) orelse return false,
        .BYTE_ARRAY => 0,
        else => return false,
    };
    var pos: usize = 0;
    for (0..count) |_| {
        var len = width;
        if (width == 0) {
            if (values.len - pos < 4) return false;
            len = std.mem.readInt(u32, values[pos..][0..4], .little);
            pos += 4;
        }
        if (values.len - pos < len) return false;
        const entry = values[pos..][0..len];
        pos += len;
        if (std.sort.binarySearch([]const u8, want, entry, orderBytes)) |i| found[i] = true;
    }
    // Bytes left over mean the page holds more than it says: it is corrupt, and proves nothing.
    return pos == values.len;
}

/// Whether a file's writer emits the canonical header, which is what lets a block be located without its header.
const Verdict = enum { unknown, canonical, other };

/// Bytes read to learn a filter's size when the footer leaves its length out. The header is at most 19.
const header_probe_bytes = 64;

fn bloomFilterTotal(bytes: []const u8) ?u64 {
    const h = bloom.parseHeader(bytes) orelse return null;
    return h.len + h.num_bytes;
}

/// How one target's filter comes back from a read.
const Fetch = union(enum) {
    none,
    /// Header and bitset, already in hand.
    bytes: []const u8,
    /// Header and bitset, read by request `req`.
    whole: usize,
    /// Only the blocks the keys select: requests `first..end`, block `blocks[i - first]` each (ascending), and this
    /// filter's own header in request `header`, which must match the canonical one the blocks were located by.
    blocks: struct { first: usize, end: usize, num_blocks: u64, blocks: []const u64, header: usize, total_len: u64 },
};

const ReadPlan = struct {
    reqs: std.ArrayList(Req) = .empty,
    fetch: []Fetch,
};

const WaveResult = struct { row_groups: usize = 0, pruned: usize = 0 };

const Stage = enum { bloom, dictionary };

const Probing = struct {
    /// Where what is proven is recorded: lives as long as the footers.
    arena: std.mem.Allocator,
    /// Bytes read and decoded along the way.
    scratch: std.mem.Allocator,
    /// Filter bodies of the wave in hand. Reset after the wave is probed, so the next wave does not keep them.
    wave: *std.heap.ArenaAllocator,
    metas: []const schema.FileMetaData,
    src: Source,
    stats: *Stats,
    filter: ast.Filter,
    opts: Options,
    verdicts: []Verdict = &.{},
    probes: ProbeCache = .{},
    /// Covers of the wave in hand, allocated in `wave`. The read fills these instead of planning again.
    body_covers: ?[]const Span = null,
    /// (file, row group) pairs already counted in `row_groups_bloom`.
    bloom_counted: std.AutoHashMapUnmanaged(u64, void) = .empty,

    fn run(self: *Probing) !void {
        var leaves: std.ArrayList(ast.Filter) = .empty;
        try collectLeaves(self.scratch, self.filter, &leaves);
        if (leaves.items.len == 0) return;
        const targets = try findTargets(self.arena, self.filter, leaves.items, self.metas);
        self.verdicts = try self.scratch.alloc(Verdict, self.metas.len);
        @memset(self.verdicts, .unknown);
        try self.stage(.bloom, targets.items);
        try self.stage(.dictionary, targets.items);
    }

    /// Probe every target `stage` can still help, in file order. On a remote source the first `opts.wave` row groups
    /// go first, and unless they prune often enough the rest are left unread.
    fn stage(self: *Probing, comptime which: Stage, targets: []Target) !void {
        var queue: std.ArrayList(*Target) = .empty;
        for (targets) |*t| {
            const ok = switch (which) {
                .bloom => filterSpan(t, self.metas) != null,
                // A dictionary is read only for a chunk with no filter, in a row group still kept: where a filter
                // said "maybe" the literal is almost always there; its false positives are too rare to pay a read.
                .dictionary => filterSpan(t, self.metas) == null and !self.rowGroupSkips(t) and
                    dictSpan(t, self.metas) != null,
            };
            if (ok) try queue.append(self.scratch, t);
        }
        const all = queue.items;
        if (all.len == 0) return;
        // Remote bloom batches every file and column. A contiguous run is one round only when it is cheap; a larger
        // one, and a scattered one, keeps the adaptive wave.
        if (which == .bloom and self.src.cost.remote()) {
            try self.stageBloomRemote(all);
            return;
        }
        const first_end = if (self.src.cost.remote() or which == .dictionary) waveEnd(all, self.opts.wave) else all.len;
        const first = try self.probeWave(which, all[0..first_end]);
        if (first_end == all.len) return;
        const want = self.opts.min_wave_prune * @as(f64, @floatFromInt(first.row_groups));
        if (@as(f64, @floatFromInt(first.pruned)) < want) {
            switch (which) {
                .bloom => self.stats.bloom_stopped = true,
                .dictionary => self.stats.dict_stopped = true,
            }
            return;
        }
        _ = try self.probeWave(which, all[first_end..]);
    }

    /// Remote bloom. Lengths the footer omits are learned first, in window-sized header batches that are released
    /// before any filter body is admitted. Cheap contiguous runs from every file and column then share one dispatch.
    /// A run is cheap when the one buffer of its union fits the charged window and one round trip (8 MiB). Anything
    /// larger, and every scattered column, takes the adaptive first wave and then window-sized waves. Each wave plans
    /// its candidates once, admits a prefix of that plan, and releases the plan with the bodies. A filter that does
    /// not fit the window on its own is not read.
    fn stageBloomRemote(self: *Probing, all: []const *Target) !void {
        try self.resolveOmitted(all);
        var used = try self.scratch.alloc(bool, all.len);
        @memset(used, false);
        var cheap: std.ArrayList(*Target) = .empty;
        var rest: std.ArrayList(*Target) = .empty;
        var i: usize = 0;
        while (i < all.len) : (i += 1) {
            if (used[i]) continue;
            var col: std.ArrayList(*Target) = .empty;
            var j: usize = i;
            while (j < all.len) : (j += 1) {
                if (used[j]) continue;
                if (all[j].file != all[i].file or all[j].col != all[i].col) continue;
                used[j] = true;
                try col.append(self.scratch, all[j]);
            }
            if (try self.columnContiguous(col.items) and self.contiguousCharge(col.items) <= self.oneRoundCap()) {
                try cheap.appendSlice(self.scratch, col.items);
            } else try rest.appendSlice(self.scratch, col.items);
        }
        self.keepReadable(&cheap);
        self.keepReadable(&rest);
        try self.probeSequence(cheap.items, self.oneRoundCap());
        if (rest.items.len == 0) return;
        std.mem.sort(*Target, rest.items, {}, targetLess);
        const at = self.skipOversize(rest.items, 0);
        if (at >= rest.items.len) return;
        const first_n = waveEnd(rest.items[at..], self.opts.wave);
        const first_slice = rest.items[at..][0..first_n];
        const cut = try self.cutWave(first_slice, self.windowCap());
        if (cut.admitted.len == 0) return;
        const tail = rest.items[at + first_n ..];
        const cont = try self.scratch.alloc(*Target, cut.rest.len + tail.len);
        @memcpy(cont[0..cut.rest.len], cut.rest);
        @memcpy(cont[cut.rest.len..], tail);
        self.body_covers = cut.covers;
        const first = try self.probeWave(.bloom, cut.admitted);
        self.releaseWave();
        if (cont.len == 0) return;
        const want = self.opts.min_wave_prune * @as(f64, @floatFromInt(first.row_groups));
        if (@as(f64, @floatFromInt(first.pruned)) < want) {
            self.stats.bloom_stopped = true;
            return;
        }
        try self.probeSequence(cont, self.windowCap());
    }

    /// Header probes for every omitted length, batched under the window and released before bodies are admitted.
    fn resolveOmitted(self: *Probing, all: []const *Target) !void {
        var pending: std.ArrayList(*Target) = .empty;
        for (all) |t| {
            const s = filterSpan(t, self.metas) orelse continue;
            if (s.end == s.start) try pending.append(self.scratch, t);
        }
        var at: usize = 0;
        const cap = self.windowCap();
        while (at < pending.items.len) {
            if (header_probe_bytes > cap) break;
            var n: usize = 0;
            var bytes: u64 = 0;
            while (at + n < pending.items.len) {
                if (n > 0 and bytes + header_probe_bytes > cap) break;
                bytes += header_probe_bytes;
                n += 1;
            }
            if (n == 0) break;
            try self.probeHeaders(pending.items[at..][0..n]);
            self.releaseWave();
            at += n;
        }
    }

    fn probeHeaders(self: *Probing, targets: []const *Target) !void {
        const a = self.bodyAllocator();
        var reqs: std.ArrayList(Req) = .empty;
        for (targets) |t| {
            const s = filterSpan(t, self.metas).?;
            try reqs.append(a, .{ .span = .{ .file = s.file, .start = s.start, .end = s.start + header_probe_bytes } });
        }
        try readAll(a, self.src, reqs.items, self.stats, .exact, null);
        for (targets, reqs.items) |t, r| {
            const total = bloomFilterTotal(r.out) orelse continue;
            if (total == 0 or total > @as(u64, @intCast(bloom.max_filter_bytes))) continue;
            t.learned = total;
        }
    }

    /// Window-sized waves over `targets`. Each wave plans once; the unadmitted tail is planned again as the next wave.
    fn probeSequence(self: *Probing, targets: []const *Target, cap: u64) !void {
        if (targets.len == 0) return;
        const buf_a = try self.scratch.alloc(*Target, targets.len);
        const buf_b = try self.scratch.alloc(*Target, targets.len);
        @memcpy(buf_a, targets);
        var cur: []*Target = buf_a[0..targets.len];
        var spare: []*Target = buf_b;
        var left: usize = targets.len + 1;
        while (cur.len > 0) {
            if (left == 0) return;
            left -= 1;
            const skip = self.skipOversize(cur, 0);
            if (skip >= cur.len) return;
            if (skip > 0) {
                cur = cur[skip..];
                continue;
            }
            const cut = try self.cutWave(cur, cap);
            if (cut.admitted.len == 0) {
                self.stats.bloom_skipped_oversize += 1;
                cur = cur[1..];
                continue;
            }
            const nrest = cut.rest.len;
            @memcpy(spare[0..nrest], cut.rest);
            self.body_covers = cut.covers;
            _ = try self.probeWave(.bloom, cut.admitted);
            self.releaseWave();
            cur = spare[0..nrest];
            spare = if (spare.ptr == buf_a.ptr) buf_b else buf_a;
        }
    }

    const WaveCut = struct {
        admitted: []const *Target,
        rest: []const *Target,
        covers: []const Span,
    };

    /// One plan of `targets` on the wave arena. Spans are admitted in order until the next buffer would pass `cap`.
    /// A span that is itself larger than `cap` (touching filters join before the window is consulted) contributes the
    /// longest prefix of its filters whose one buffer still fits; the rest of it waits for the next wave.
    fn cutWave(self: *Probing, targets: []const *Target, cap: u64) !WaveCut {
        const a = self.wave.allocator();
        if (targets.len == 0) return .{ .admitted = &.{}, .rest = &.{}, .covers = &.{} };
        const planned = try self.planBodies(targets);
        var held: u64 = 0;
        var cover_n: usize = 0;
        for (planned) |c| {
            const ch = heldBytes(c, self.src.suffix(c.file));
            if (held + ch > cap) break;
            held += ch;
            cover_n += 1;
        }
        var partial: []const *Target = &.{};
        var partial_span: ?Span = null;
        if (cover_n == 0 and planned.len > 0) {
            const inside = try self.orderedInside(planned[0], targets);
            const npre = self.prefixCount(inside, cap);
            if (npre > 0) {
                partial = inside[0..npre];
                const s0 = self.bodySpan(inside[0]).?;
                const s1 = self.bodySpan(inside[npre - 1]).?;
                partial_span = .{ .file = s0.file, .start = s0.start, .end = s1.end };
            }
        }
        const mark = try a.alloc(bool, targets.len);
        @memset(mark, false);
        for (targets, mark) |t, *m| {
            const s = self.bodySpan(t) orelse continue;
            for (planned[0..cover_n]) |c| {
                if (c.file == s.file and s.start >= c.start and s.end <= c.end) {
                    m.* = true;
                    break;
                }
            }
        }
        for (partial) |p| for (targets, mark) |t, *m| {
            if (t == p) m.* = true;
        };
        var admitted: std.ArrayList(*Target) = .empty;
        var rest: std.ArrayList(*Target) = .empty;
        for (targets, mark) |t, m| {
            if (m) try admitted.append(a, t) else try rest.append(a, t);
        }
        var covers: std.ArrayList(Span) = .empty;
        for (planned[0..cover_n]) |c| try covers.append(a, c);
        if (partial_span) |s| try covers.append(a, s);
        return .{ .admitted = admitted.items, .rest = rest.items, .covers = covers.items };
    }

    /// One dispatch plan of these filters. The arrays live in the wave arena and die at `releaseWave`.
    fn planBodies(self: *Probing, targets: []const *Target) ![]Span {
        const a = self.wave.allocator();
        var spans: std.ArrayList(Span) = .empty;
        for (targets) |t| if (self.bodySpan(t)) |s| try spans.append(a, s);
        return coverSpans(a, spans.items, self.src.currentCost(), .dispatch, &self.stats.planner_calls);
    }

    fn orderedInside(self: *Probing, cover: Span, targets: []const *Target) ![]*Target {
        const a = self.wave.allocator();
        const Item = struct { t: *Target, start: u64 };
        var items: std.ArrayList(Item) = .empty;
        for (targets) |t| {
            const s = self.bodySpan(t) orelse continue;
            if (s.file == cover.file and s.start >= cover.start and s.end <= cover.end)
                try items.append(a, .{ .t = t, .start = s.start });
        }
        std.mem.sort(Item, items.items, {}, struct {
            fn less(_: void, x: Item, y: Item) bool {
                return x.start < y.start;
            }
        }.less);
        const out = try a.alloc(*Target, items.items.len);
        for (items.items, out) |it, *slot| slot.* = it.t;
        return out;
    }

    /// How many of `inside`, already in offset order, fit in one buffer of `cap`.
    fn prefixCount(self: *const Probing, inside: []const *Target, cap: u64) usize {
        if (inside.len == 0) return 0;
        const s0 = self.bodySpan(inside[0]) orelse return 0;
        var n: usize = 0;
        for (inside, 0..) |t, i| {
            const s = self.bodySpan(t) orelse break;
            const grown: Span = .{ .file = s0.file, .start = s0.start, .end = s.end };
            if (heldBytes(grown, self.src.suffix(grown.file)) > cap) break;
            n = i + 1;
        }
        return n;
    }

    fn keepReadable(self: *const Probing, list: *std.ArrayList(*Target)) void {
        var n: usize = 0;
        for (list.items) |t| {
            if (self.bodySpan(t) != null) {
                list.items[n] = t;
                n += 1;
            }
        }
        list.shrinkRetainingCapacity(n);
    }

    /// Count and step over filters that cannot fit in `windowCap` alone. Returns the next index that might.
    fn skipOversize(self: *Probing, ts: []const *Target, start: usize) usize {
        var at = start;
        const cap = self.windowCap();
        while (at < ts.len and self.heldTarget(ts[at]) > cap) : (at += 1) {
            self.stats.bloom_skipped_oversize += 1;
        }
        return at;
    }

    fn releaseWave(self: *Probing) void {
        self.body_covers = null;
        _ = self.wave.reset(.free_all);
    }

    /// Filter bodies. A remote wave allocates them in `wave`, which is reset afterwards. A mapping keeps the scratch.
    fn bodyAllocator(self: *Probing) std.mem.Allocator {
        return if (self.src.cost.remote()) self.wave.allocator() else self.scratch;
    }

    fn windowCap(self: *const Probing) u64 {
        return self.src.cost.window orelse std.math.maxInt(u64);
    }

    fn oneRoundCap(self: *const Probing) u64 {
        return @min(self.windowCap(), one_round_bytes);
    }

    /// The filter span once its length is known: the footer length, or the length a header probe learned.
    fn bodySpan(self: *const Probing, t: *const Target) ?Span {
        const s = filterSpan(t, self.metas) orelse return null;
        if (s.end > s.start) return s;
        const n = t.learned orelse return null;
        if (n == 0 or n > @as(u64, @intCast(bloom.max_filter_bytes))) return null;
        return .{ .file = s.file, .start = s.start, .end = s.start + n };
    }

    fn heldTarget(self: *const Probing, t: *const Target) u64 {
        const s = self.bodySpan(t) orelse return 0;
        return heldBytes(s, self.src.suffix(s.file));
    }

    /// Charge of a contiguous column: one buffer from the first byte to the last. Touching filters join into that
    /// span, so this is the plan's charge without planning.
    fn contiguousCharge(self: *const Probing, col: []const *Target) u64 {
        var lo: u64 = std.math.maxInt(u64);
        var hi: u64 = 0;
        var file: u32 = 0;
        for (col) |t| {
            const s = self.bodySpan(t) orelse return std.math.maxInt(u64);
            file = s.file;
            lo = @min(lo, s.start);
            hi = @max(hi, s.end);
        }
        if (hi <= lo) return 0;
        return heldBytes(.{ .file = file, .start = lo, .end = hi }, self.src.suffix(file));
    }

    fn targetLess(_: void, a: *Target, b: *Target) bool {
        if (a.file != b.file) return a.file < b.file;
        if (a.rg != b.rg) return a.rg < b.rg;
        return a.col < b.col;
    }

    /// Known lengths, and each next filter starts where the previous one ends. An omitted length is not a run.
    fn columnContiguous(self: *Probing, col: []const *Target) !bool {
        if (col.len == 0) return true;
        var spans = try self.scratch.alloc(Span, col.len);
        for (col, spans) |t, *s| s.* = self.bodySpan(t) orelse return false;
        if (spans.len == 1) return true;
        std.mem.sort(Span, spans, {}, spanLess);
        for (spans[0 .. spans.len - 1], spans[1..]) |a, b| {
            if (b.file != a.file or b.start > a.end) return false;
        }
        return true;
    }

    fn probeWave(self: *Probing, comptime which: Stage, targets: []const *Target) !WaveResult {
        switch (which) {
            .bloom => try self.probeBloom(targets),
            .dictionary => try self.probeDictionaries(targets),
        }
        for (targets) |t| if (t.absent.items.len > 0) {
            std.mem.sort([]const u8, t.absent.items, {}, bytesLess);
            t.chunk(self.metas).absent = t.absent.items;
        };
        var res: WaveResult = .{};
        for (targets, 0..) |t, i| {
            if (i > 0 and targets[i - 1].sameRowGroup(t)) continue;
            res.row_groups += 1;
            if (self.rowGroupSkips(t)) res.pruned += 1;
        }
        switch (which) {
            .bloom => try self.noteBloomPruned(targets),
            .dictionary => self.stats.row_groups_dict += res.pruned,
        }
        return res;
    }

    /// Each (file, row group) counts once, across columns and across waves.
    fn noteBloomPruned(self: *Probing, targets: []const *Target) !void {
        for (targets) |t| {
            if (!self.rowGroupSkips(t)) continue;
            const key = (@as(u64, t.file) << 32) | t.rg;
            const gop = try self.bloom_counted.getOrPut(self.scratch, key);
            if (!gop.found_existing) self.stats.row_groups_bloom += 1;
        }
    }

    fn rowGroupSkips(self: *const Probing, t: *const Target) bool {
        const fm = &self.metas[t.file];
        return prune.pruneRowGroup(&fm.row_groups.items[t.rg], self.filter, fm) == .skip;
    }

    /// Read and probe the Bloom filters of `wave`, recording every key they prove absent.
    fn probeBloom(self: *Probing, wave: []const *Target) !void {
        const a = self.scratch;
        const fetch = try a.alloc(Fetch, wave.len);
        @memset(fetch, .none);

        // Every filter's header first, all at once: where reads are cheap enough to skip planning, the headers then
        // the blocks they locate are the only bytes touched (see `prefetchBlocks`).
        if (!self.src.cost.remote()) {
            const firsts = try a.alloc(Span, wave.len);
            for (wave, firsts) |t, *f| {
                const s = filterSpan(t, self.metas).?;
                f.* = .{ .file = s.file, .start = s.start, .end = s.start + bloom.max_header_bytes };
            }
            self.src.prefetch(firsts);
        }

        // A filter whose length the footer leaves out (older parquet-java) costs a header read first.
        const spans = try a.alloc(?Span, wave.len);
        for (wave, spans) |t, *s| {
            // Remote lengths were learned, and released, before admission. A mapping still learns an omitted
            // length here: `bodySpan` would hide the empty span the header probe is looking for.
            s.* = if (self.src.cost.remote()) self.bodySpan(t) else filterSpan(t, self.metas);
        }
        const omitted = try self.fillOmittedLengths(spans, header_probe_bytes, bloomFilterTotal);
        for (omitted.reqs, omitted.index) |r, i| {
            const sp = spans[i] orelse continue;
            if (sp.len() <= r.out.len) {
                fetch[i] = .{ .bytes = r.out[0..sp.len()] };
                spans[i] = null;
            }
        }
        if (!self.src.cost.remote()) {
            // A mapping's hit is a slice of the file. A remote hit is copied into the wave buffer in `readPlaced`.
            for (wave, spans, fetch) |_, s, *f| if (s) |sp| if (self.src.vtable.cached) |cached| {
                if (cached(self.src.ctx, a, sp)) |bytes| f.* = .{ .bytes = bytes };
            };
        }

        // Whole filters, or just the blocks the keys select, by the source's cost model. A whole filter can serve later
        // queries from the warm cache, blocks only this one, so blocks must save a quarter of the time to be chosen.
        var plan = try self.wholePlan(wave, spans, fetch);
        var planned_covers: ?[]const Span = if (self.src.cost.remote()) self.body_covers else null;
        if (self.src.cost.remote()) {
            if (try self.sparsePlan(wave, spans, fetch)) |sparse| {
                const cost = self.src.currentCost();
                const t_sparse = try estimateReqs(a, sparse.reqs.items, cost);
                if (t_sparse * 4 < try estimateReqs(a, plan.reqs.items, cost) * 3) {
                    plan = sparse;
                    planned_covers = null;
                }
            }
        }
        try readAll(self.bodyAllocator(), self.src, plan.reqs.items, self.stats, .dispatch, planned_covers);

        if (!self.src.cost.remote()) try self.prefetchBlocks(wave, plan, spans);

        for (wave, plan.fetch) |t, f| {
            switch (f) {
                .none => {},
                .bytes => |bytes| try self.probeWhole(t, bytes),
                .whole => |ri| {
                    const r = plan.reqs.items[ri];
                    if (self.src.vtable.retain) |retain| {
                        if (r.out.len == r.span.len()) retain(self.src.ctx, r.span, r.out);
                    }
                    try self.probeWhole(t, r.out);
                },
                .blocks => |b| {
                    // Blocks count only if this filter's own header is the canonical one that located them; one
                    // writer can emit headers of several shapes in one file.
                    var buf: [bloom.max_header_bytes]u8 = undefined;
                    const want = bloom.canonicalHeader(b.total_len, &buf);
                    if (want == null or !std.mem.eql(u8, want.?, plan.reqs.items[b.header].out)) {
                        self.verdicts[t.file] = .other;
                        continue;
                    }
                    self.stats.filters += 1;
                    const set: BlockSet = .{
                        .reqs = plan.reqs.items[b.first..b.end],
                        .blocks = b.blocks,
                        .num_blocks = b.num_blocks,
                    };
                    for (t.keys.items) |*k| if (try self.keyAbsent(k, set)) try t.absent.append(self.arena, k.form(0));
                },
            }
        }
    }

    /// Prefetch the block every key selects in each whole filter of `plan`, located through its (prefetched) header.
    fn prefetchBlocks(self: *Probing, wave: []const *Target, plan: ReadPlan, spans: []const ?Span) !void {
        if (self.src.vtable.prefetch == null) return;
        var blocks: std.ArrayList(Span) = .empty;
        for (wave, plan.fetch, spans) |t, f, s| {
            const bytes = switch (f) {
                .whole => |ri| plan.reqs.items[ri].out,
                else => continue,
            };
            const start = (s orelse continue).start;
            const h = bloom.parseHeader(bytes) orelse continue;
            const n = h.num_bytes / bloom.block_bytes;
            for (t.keys.items) |*k| for (0..k.n) |i| {
                const at = start + h.len + (try self.probes.get(self.scratch, k.form(i))).block(n) * bloom.block_bytes;
                try blocks.append(self.scratch, .{ .file = t.file, .start = at, .end = at + bloom.block_bytes });
            };
        }
        self.src.prefetch(blocks.items);
    }

    fn probeWhole(self: *Probing, t: *Target, bytes: []const u8) !void {
        const h = bloom.parseHeader(bytes) orelse return;
        // A header whose size disagrees with the footer's length leaves no telling which is right: no filter.
        if (t.chunk(self.metas).meta_data.?.bloom_filter_length) |l| {
            if (h.len + h.num_bytes != l) return;
        }
        const bits = bloom.bitset(bytes) orelse return;
        self.noteHeader(t.file, bytes);
        self.stats.filters += 1;
        for (t.keys.items) |*k| {
            if (try self.keyAbsent(k, WholeSet{ .bits = bits })) try t.absent.append(self.arena, k.form(0));
        }
    }

    /// Whether no stored form of `k` can be in the filter `set` reads.
    fn keyAbsent(self: *Probing, k: *const Key, set: anytype) !bool {
        for (0..k.n) |i| if (set.has(try self.probes.get(self.scratch, k.form(i)))) return false;
        return true;
    }

    /// Learn from a whole filter whether its file's writer emits the canonical header.
    fn noteHeader(self: *Probing, file: u32, bytes: []const u8) void {
        if (self.verdicts[file] != .unknown) return;
        const h = bloom.parseHeader(bytes) orelse return;
        var buf: [bloom.max_header_bytes]u8 = undefined;
        const want = bloom.canonicalHeader(h.len + h.num_bytes, &buf);
        self.verdicts[file] = if (want != null and std.mem.eql(u8, want.?, bytes[0..h.len])) .canonical else .other;
    }

    fn wholePlan(self: *Probing, wave: []const *Target, spans: []const ?Span, fetch: []const Fetch) !ReadPlan {
        var plan: ReadPlan = .{ .fetch = try self.scratch.dupe(Fetch, fetch) };
        for (wave, spans, plan.fetch) |_, s, *f| {
            if (f.* != .none) continue;
            const sp = s orelse continue;
            f.* = .{ .whole = plan.reqs.items.len };
            try plan.reqs.append(self.scratch, .{ .span = sp });
        }
        return plan;
    }

    /// A footer that omits a length is an empty span. Read `probe_len` bytes at each such span and, when `parse`
    /// names a total, replace it with `[start, start + total)`. The header bytes stay on the returned requests.
    fn fillOmittedLengths(
        self: *Probing,
        spans: []?Span,
        probe_len: u64,
        parse: *const fn ([]const u8) ?u64,
    ) !struct { reqs: []Req, index: []usize } {
        const a = self.scratch;
        var heads: std.ArrayList(Req) = .empty;
        var head_of: std.ArrayList(usize) = .empty;
        for (spans, 0..) |s, i| {
            const sp = s orelse continue;
            if (sp.end != sp.start) continue;
            try heads.append(a, .{ .span = .{ .file = sp.file, .start = sp.start, .end = sp.start + probe_len } });
            try head_of.append(a, i);
        }
        try readAll(a, self.src, heads.items, self.stats, .cost, null);
        for (heads.items, head_of.items) |r, i| {
            const total = parse(r.out) orelse {
                spans[i] = null;
                continue;
            };
            spans[i] = .{ .file = r.span.file, .start = r.span.start, .end = r.span.start + total };
        }
        return .{ .reqs = heads.items, .index = head_of.items };
    }

    /// Read the dictionary pages of `wave` and record every key none of them holds.
    fn probeDictionaries(self: *Probing, wave: []const *Target) !void {
        const a = self.scratch;
        const spans = try a.alloc(?Span, wave.len);
        for (wave, spans) |t, *s| s.* = dictSpan(t, self.metas).?;
        _ = try self.fillOmittedLengths(spans, page_header_probe, dictionaryPageLen);
        var reqs: std.ArrayList(Req) = .empty;
        const req_of = try a.alloc(?usize, wave.len);
        for (spans, req_of) |s, *ri| {
            ri.* = null;
            const sp = s orelse continue;
            ri.* = reqs.items.len;
            try reqs.append(a, .{ .span = sp });
        }
        const pages = try a.alloc(Span, reqs.items.len);
        for (reqs.items, pages) |r, *p| p.* = r.span;
        self.src.prefetch(pages);
        try readAll(a, self.src, reqs.items, self.stats, .cost, null);

        var want: std.ArrayList([]const u8) = .empty;
        for (wave, req_of) |t, ri| {
            const page = reqs.items[ri orelse continue].out;
            const cm = t.chunk(self.metas).meta_data.?;
            const values = dictionaryValues(a, page, cm.codec) orelse continue;
            self.stats.dictionaries += 1;
            // Every form of every key still open, sorted so each entry costs one binary search. Only what earlier
            // waves proved is sorted; this one appends below.
            const proven = t.absent.items;
            want.clearRetainingCapacity();
            for (t.keys.items) |*k| {
                if (isAbsent(proven, k)) continue;
                for (0..k.n) |fi| try want.append(a, k.form(fi));
            }
            std.mem.sort([]const u8, want.items, {}, bytesLess);
            // Distinct, so the entry a form is found at is the one it is looked up at.
            var n: usize = 0;
            for (want.items) |w| {
                if (n > 0 and std.mem.eql(u8, want.items[n - 1], w)) continue;
                want.items[n] = w;
                n += 1;
            }
            want.shrinkRetainingCapacity(n);
            const found = try a.alloc(bool, want.items.len);
            @memset(found, false);
            const slot = chunkSlot(t.rowGroup(self.metas), t.col, &self.metas[t.file]) orelse continue;
            if (!scanDictionary(values.bytes, values.count, slot, want.items, found)) continue;
            for (t.keys.items) |*k| {
                if (isAbsent(proven, k)) continue;
                const held = for (0..k.n) |fi| {
                    const at = std.sort.binarySearch([]const u8, want.items, k.form(fi), orderBytes).?;
                    if (found[at]) break true;
                } else false;
                if (!held) try t.absent.append(self.arena, k.form(0));
            }
        }
    }

    /// The blocks the keys select in each filter of known length, located through the canonical header, plus that
    /// filter's real header to check the location by. Filters it cannot locate stay whole; null when it locates none.
    fn sparsePlan(self: *Probing, wave: []const *Target, spans: []const ?Span, fetch: []const Fetch) !?ReadPlan {
        const a = self.scratch;
        var plan: ReadPlan = .{ .fetch = try a.dupe(Fetch, fetch) };
        var any = false;
        for (wave, spans, plan.fetch) |t, s, *f| {
            if (f.* != .none) continue;
            const sp = s orelse continue;
            var buf: [bloom.max_header_bytes]u8 = undefined;
            const hdr = if (self.verdicts[t.file] == .other) null else bloom.canonicalHeader(sp.len(), &buf);
            const h = hdr orelse {
                f.* = .{ .whole = plan.reqs.items.len };
                try plan.reqs.append(a, .{ .span = sp });
                continue;
            };
            const num_blocks = (sp.len() - h.len) / bloom.block_bytes;
            var blocks: std.ArrayList(u64) = .empty;
            for (t.keys.items) |*k| for (0..k.n) |i| try blocks.append(a, (try self.probes.get(a, k.form(i))).block(num_blocks));
            std.mem.sort(u64, blocks.items, {}, std.sort.asc(u64));
            var distinct: usize = 0;
            for (blocks.items) |b| {
                if (distinct > 0 and blocks.items[distinct - 1] == b) continue;
                blocks.items[distinct] = b;
                distinct += 1;
            }
            blocks.shrinkRetainingCapacity(distinct);
            const header = plan.reqs.items.len;
            try plan.reqs.append(a, .{ .span = .{ .file = sp.file, .start = sp.start, .end = sp.start + h.len } });
            const first = plan.reqs.items.len;
            for (blocks.items) |b| {
                const at = sp.start + h.len + b * bloom.block_bytes;
                try plan.reqs.append(a, .{ .span = .{ .file = sp.file, .start = at, .end = at + bloom.block_bytes } });
            }
            f.* = .{ .blocks = .{
                .first = first,
                .end = plan.reqs.items.len,
                .num_blocks = num_blocks,
                .blocks = blocks.items,
                .header = header,
                .total_len = sp.len(),
            } };
            any = true;
        }
        return if (any) plan else null;
    }
};

const WholeSet = struct {
    bits: []const u8,

    fn has(self: WholeSet, p: bloom.Probe) bool {
        return p.mightContain(self.bits);
    }
};

/// Some blocks of one filter: `reqs[i]` read block `blocks[i]` of `num_blocks`.
const BlockSet = struct {
    reqs: []const Req,
    blocks: []const u64,
    num_blocks: u64,

    fn has(self: BlockSet, p: bloom.Probe) bool {
        const want = p.block(self.num_blocks);
        const i = std.sort.binarySearch(u64, self.blocks, want, struct {
            fn cmp(w: u64, b: u64) std.math.Order {
                return std.math.order(w, b);
            }
        }.cmp) orelse return true;
        // A block that did not come back whole answers "maybe".
        const out = self.reqs[i].out;
        if (out.len != bloom.block_bytes) return true;
        return bloom.blockHas(out[0..bloom.block_bytes], p.m);
    }
};

// ============================================================
// Tests
// ============================================================

const testing = std.testing;
const parser = @import("parser.zig");
const metadata = @import("../parquet/metadata.zig");
const test_fixtures = @import("../parquet/test_fixtures.zig");

fn le(comptime T: type, v: T) [@sizeOf(T)]u8 {
    var b: [@sizeOf(T)]u8 = undefined;
    std.mem.writeInt(T, &b, v, .little);
    return b;
}

fn plain(t: schema.Type) schema.SchemaElement {
    return test_fixtures.statsLeaf(t);
}

fn conv(t: schema.Type, c: schema.ConvertedType) schema.SchemaElement {
    var e = test_fixtures.statsLeaf(t);
    e.converted_type = c;
    if (c == .DECIMAL) {
        e.precision = 9;
        e.scale = 2;
    }
    return e;
}

fn logical(t: schema.Type, l: schema.LogicalType) schema.SchemaElement {
    var e = test_fixtures.statsLeaf(t);
    e.logical_type = l;
    return e;
}

fn flba(len: i32, l: ?schema.LogicalType) schema.SchemaElement {
    var e = test_fixtures.statsLeaf(.FIXED_LEN_BYTE_ARRAY);
    e.type_length = len;
    e.logical_type = l;
    return e;
}

fn tsOf(unit: schema.TimeUnit) schema.LogicalType {
    return .{ .TIMESTAMP = .{ .isAdjustedToUTC = true, .unit = unit } };
}

fn one(a: []const u8) Want {
    return .{ .forms = &.{a} };
}

fn two(a: []const u8, b: []const u8) Want {
    return .{ .forms = &.{ a, b } };
}

const Want = union(enum) {
    /// Membership must not decide this literal.
    none,
    /// The binder rejects the filter outright.
    parse_error,
    /// The stored forms membership must probe, the literal's own first.
    forms: []const []const u8,
};

// The binder decides what a literal means (`stored`), membership turns that into the bytes a writer hashed. Each row
// pins one type's answer to bytes worked out by hand from the PLAIN encoding, so neither side can drift alone.
test "membership keys match what the binder bound, across the type matrix" {
    var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_state.deinit();
    const a = arena_state.allocator();
    const ms: schema.TimeUnit = .{ .MILLIS = .{} };
    const us: schema.TimeUnit = .{ .MICROS = .{} };
    const ns: schema.TimeUnit = .{ .NANOS = .{} };
    const u16_type: schema.LogicalType = .{ .INTEGER = .{ .bitWidth = 16, .isSigned = false } };
    const time_ns: schema.LogicalType = .{ .TIME = .{ .isAdjustedToUTC = true, .unit = ns } };
    const us20: i64 = 1_577_836_800_000_000;
    const ms20: i64 = 1_577_836_800_000;
    const Case = struct { elem: schema.SchemaElement, expr: []const u8, want: Want };
    // Comptime, so the expected bytes `le` builds outlive the initializer.
    const cases = comptime [_]Case{
        .{ .elem = plain(.INT32), .expr = "c = 42", .want = one(&le(i32, 42)) },
        .{ .elem = conv(.INT32, .INT_8), .expr = "c = -5", .want = one(&le(i32, -5)) },
        .{ .elem = logical(.INT32, .{ .DATE = .{} }), .expr = "c = '2020-01-02'", .want = one(&le(i32, 18263)) },
        .{ .elem = conv(.INT32, .DATE), .expr = "c = 18263", .want = one(&le(i32, 18263)) },
        .{ .elem = conv(.INT32, .TIME_MILLIS), .expr = "c = '01:02:03.004'", .want = one(&le(i32, 3_723_004)) },
        .{ .elem = conv(.INT32, .UINT_32), .expr = "c = 4294967295", .want = one(&le(u32, 0xffff_ffff)) },
        .{ .elem = conv(.INT32, .UINT_8), .expr = "c = 200", .want = one(&le(u32, 200)) },
        .{ .elem = logical(.INT32, u16_type), .expr = "c = 65535", .want = one(&le(u32, 65535)) },
        // No INT32 holds 2^32: the binder keeps it, but there is no stored form to look up.
        .{ .elem = conv(.INT32, .UINT_32), .expr = "c = 4294967296", .want = .none },
        .{ .elem = conv(.INT32, .DECIMAL), .expr = "c = 1.50", .want = .none },
        .{ .elem = plain(.INT64), .expr = "c = -1", .want = one(&le(i64, -1)) },
        .{
            .elem = logical(.INT64, tsOf(us)),
            .expr = "c = '2020-01-01 00:00:00.000001'",
            .want = one(&le(i64, us20 + 1)),
        },
        .{
            .elem = logical(.INT64, tsOf(ns)),
            .expr = "c = '1970-01-01 00:00:01'",
            .want = one(&le(i64, 1_000_000_000)),
        },
        .{
            .elem = conv(.INT64, .TIMESTAMP_MILLIS),
            .expr = "c = '2020-01-01 00:00:00.001'",
            .want = one(&le(i64, ms20 + 1)),
        },
        // Finer than the unit: no stored value equals it, and the binder says so with a never-true leaf.
        .{ .elem = logical(.INT64, tsOf(ms)), .expr = "c = '2020-01-01 00:00:00.0005'", .want = .none },
        .{ .elem = logical(.INT64, time_ns), .expr = "c = '00:00:01'", .want = one(&le(i64, 1_000_000_000)) },
        .{ .elem = conv(.INT64, .UINT_64), .expr = "c = 18446744073709551615", .want = one(&le(i64, -1)) },
        .{ .elem = conv(.INT64, .DECIMAL), .expr = "c = 1.5", .want = .none },
        .{ .elem = plain(.INT96), .expr = "c = '2020-01-01'", .want = .none },
        .{ .elem = plain(.FLOAT), .expr = "c = 1.5", .want = one(&le(u32, 0x3fc00000)) },
        .{ .elem = plain(.FLOAT), .expr = "c = 0", .want = two(&le(u32, 0), &le(u32, 0x80000000)) },
        .{ .elem = plain(.FLOAT), .expr = "c = -0.0", .want = two(&le(u32, 0x80000000), &le(u32, 0)) },
        .{ .elem = plain(.FLOAT), .expr = "c = nan", .want = .none },
        .{ .elem = plain(.DOUBLE), .expr = "c = 0.1", .want = one(&le(u64, 0x3fb999999999999a)) },
        .{ .elem = plain(.DOUBLE), .expr = "c = -0.0", .want = two(&le(u64, 0x8000000000000000), &le(u64, 0)) },
        .{ .elem = conv(.BYTE_ARRAY, .UTF8), .expr = "c = 'h\xc3\xa9llo'", .want = one("h\xc3\xa9llo") },
        .{ .elem = plain(.BYTE_ARRAY), .expr = "c = abc", .want = one("abc") },
        .{ .elem = plain(.BYTE_ARRAY), .expr = "c = ''", .want = one("") },
        .{ .elem = conv(.BYTE_ARRAY, .DECIMAL), .expr = "c = 1.5", .want = .none },
        .{ .elem = flba(4, null), .expr = "c = 'abcd'", .want = one("abcd") },
        .{ .elem = flba(4, null), .expr = "c = 'abc'", .want = .none },
        .{ .elem = flba(2, .{ .FLOAT16 = .{} }), .expr = "c = 1.0", .want = .parse_error },
        .{ .elem = plain(.BOOLEAN), .expr = "c = true", .want = .none },
        .{ .elem = plain(.INT32), .expr = "c != 42", .want = .none },
        .{ .elem = plain(.INT32), .expr = "c >= 42", .want = .none },
        .{ .elem = plain(.INT32), .expr = "NOT c != 42", .want = one(&le(i32, 42)) },
    };
    for (cases) |c| {
        const sc = try test_fixtures.StatsColumn.init(a, c.elem, .{});
        const f = parser.parse(a, c.expr, &sc.fm, null) catch |err| {
            if (c.want == .parse_error) continue;
            std.debug.print("{s}: {}\n", .{ c.expr, err });
            return err;
        };
        const got = keyOf(f, &sc.rg, &sc.fm);
        switch (c.want) {
            .parse_error => return error.TestExpectedParseError,
            .none => if (got != null) {
                std.debug.print("{s}: membership decides a literal the binder did not bind as stored\n", .{c.expr});
                return error.TestUnexpectedResult;
            },
            .forms => |forms| {
                const k = got orelse {
                    std.debug.print("{s}: no stored form\n", .{c.expr});
                    return error.TestUnexpectedResult;
                };
                try testing.expectEqual(forms.len, k.n);
                for (forms, 0..) |want, i| try testing.expectEqualSlices(u8, want, k.form(i));
            },
        }
    }
}

test "IN lists bind every element as a stored literal; NOT IN binds none" {
    var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_state.deinit();
    const a = arena_state.allocator();
    const sc = try test_fixtures.StatsColumn.init(a, plain(.INT64), .{});
    var leaves: std.ArrayList(ast.Filter) = .empty;
    try collectLeaves(a, try parser.parse(a, "c IN (1, 2, 3)", &sc.fm, null), &leaves);
    try testing.expectEqual(@as(usize, 3), leaves.items.len);
    for (leaves.items) |l| try testing.expect(keyOf(l, &sc.rg, &sc.fm) != null);
    leaves.clearRetainingCapacity();
    try collectLeaves(a, try parser.parse(a, "c NOT IN (1, 2, 3)", &sc.fm, null), &leaves);
    try testing.expectEqual(@as(usize, 0), leaves.items.len);
}

test "planSpans: memory merges only what touches; remote merges where requests cost more than gaps" {
    var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_state.deinit();
    const a = arena_state.allocator();

    const mapped = try planSpans(a, &.{
        .{ .file = 0, .start = 10, .end = 20 },
        .{ .file = 0, .start = 15, .end = 30 },
        .{ .file = 0, .start = 30, .end = 40 },
        .{ .file = 0, .start = 50, .end = 60 },
        .{ .file = 1, .start = 0, .end = 5 },
    }, .mapped);
    try testing.expectEqual(@as(usize, 3), mapped.len);
    try testing.expectEqual(Span{ .file = 0, .start = 10, .end = 40 }, mapped[0]);

    const s3ish: Cost = .{ .request_bytes = 1 << 20, .concurrency = 8, .max_request = 24 << 20 };
    // 100 blocks 300 KB apart: one request each would queue ~13 deep per lane, so neighbours merge, but no merged span
    // carries more than a mebibyte of bytes nobody asked for.
    var close: [100]Span = undefined;
    for (&close, 0..) |*s, i| s.* = .{ .file = 0, .start = i * 300_000, .end = i * 300_000 + 32 };
    const merged = try planSpans(a, &close, s3ish);
    try testing.expect(merged.len < 50);
    try testing.expect(try estimate(a, merged, s3ish) < try estimate(a, &close, s3ish));
    for (merged) |s| try testing.expect(s.len() <= max_gap_floor + 32 * 5);
    // Wanted bytes earn their gap: 100 ranges of 200 KB 300 KB apart may merge whole (gaps under twice the wanted).
    var wide: [100]Span = undefined;
    for (&wide, 0..) |*s, i| s.* = .{ .file = 0, .start = i * 500_000, .end = i * 500_000 + 200_000 };
    try testing.expect((try merge(a, &wide, std.math.maxInt(u64))).len == 1);
    // At 100 KB wanted per 1.4 MB apart they cannot: each merge would add more gap than twice what it wants.
    for (&wide, 0..) |*s, i| s.* = .{ .file = 0, .start = i * 1_500_000, .end = i * 1_500_000 + 100_000 };
    const capped = try merge(a, &wide, std.math.maxInt(u64));
    try testing.expect(capped.len > 1);
    for (capped) |s| {
        var want: u64 = 0;
        for (wide) |b| if (b.start >= s.start and b.end <= s.end) {
            want += b.len();
        };
        try testing.expect(s.len() - want <= @max(max_gap_floor, 2 * want));
    }
    // The same blocks 300 MB apart: merging would read gigabytes, so each stays its own request.
    var far: [100]Span = undefined;
    for (&far, 0..) |*s, i| s.* = .{ .file = 0, .start = i * 300_000_000, .end = i * 300_000_000 + 32 };
    try testing.expectEqual(@as(usize, 100), (try planSpans(a, &far, s3ish)).len);
    // One big range is split to keep every lane busy.
    const big = try planSpans(a, &.{.{ .file = 0, .start = 0, .end = 64 << 20 }}, s3ish);
    try testing.expectEqual(@as(usize, 8), big.len);
}

test "assemble reconstructs a request split across spans and stops at a short read" {
    var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_state.deinit();
    const a = arena_state.allocator();
    const data = "0123456789abcdefghij";
    const spans = [_]Span{
        .{ .file = 0, .start = 0, .end = 8 },
        .{ .file = 0, .start = 8, .end = 16 },
        .{ .file = 0, .start = 16, .end = 20 },
    };
    const got = [_][]const u8{ data[0..8], data[8..16], data[16..18] };
    try testing.expectEqualStrings("3456789abc", try assemble(a, &spans, &got, .{ .file = 0, .start = 3, .end = 13 }));
    try testing.expectEqualStrings("89ab", try assemble(a, &spans, &got, .{ .file = 0, .start = 8, .end = 12 }));
    try testing.expectEqualStrings("efgh", try assemble(a, &spans, &got, .{ .file = 0, .start = 14, .end = 20 }));
    try testing.expectEqualStrings("", try assemble(a, &spans, &got, .{ .file = 1, .start = 0, .end = 4 }));
}

fn openFixture(a: std.mem.Allocator, path: []const u8) !struct { bytes: []u8, meta: schema.FileMetaData } {
    const bytes = metadata.readFileSlice(path, a) catch |e| {
        if (e == error.FileNotFound) return error.SkipZigTest;
        return e;
    };
    return .{ .bytes = bytes, .meta = try metadata.open(a, bytes) };
}

test "parquet-testing bloom fixtures: present strings keep the row group, absent ones prune it" {
    // Column `String` holds "Hello", "This is", "a", "test", "How", "are you", "doing ", "today", "the quick",
    // "brown fox", "jumps", "over", "the lazy", "dog". parquet-java wrote the first file without
    // bloom_filter_length, parquet-rs the second with it.
    for ([_][]const u8{
        "data/parquet-testing/data/data_index_bloom_encoding_stats.parquet",
        "data/parquet-testing/data/data_index_bloom_encoding_with_length.parquet",
    }) |path| {
        var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
        defer arena_state.deinit();
        const a = arena_state.allocator();
        const fx = try openFixture(a, path);
        const metas = [_]schema.FileMetaData{fx.meta};
        const mapped: MappedSource = .{ .files = &.{fx.bytes} };
        const rg = &metas[0].row_groups.items[0];

        for ([_][]const u8{ "'Hello'", "'doing '", "'dog'", "'the lazy'" }) |v| {
            for (rg.columns.items) |*c| c.absent = &.{};
            const f = try parser.parse(a, try std.fmt.allocPrint(a, "String = {s}", .{v}), &metas[0], null);
            const st = try load(a, testing.allocator, f, &metas, mapped.source(), .{});
            try testing.expectEqual(@as(usize, 1), st.filters);
            try testing.expect(prune.pruneRowGroup(rg, f, &metas[0]) != .skip);
        }
        var pruned: usize = 0;
        for ([_][]const u8{ "'hello'", "'doing'", "'cat'", "'zzz'", "''", "'Hello '", "'brown'", "'fox'" }) |v| {
            for (rg.columns.items) |*c| c.absent = &.{};
            const f = try parser.parse(a, try std.fmt.allocPrint(a, "String = {s}", .{v}), &metas[0], null);
            const st = try load(a, testing.allocator, f, &metas, mapped.source(), .{});
            if (prune.pruneRowGroup(rg, f, &metas[0]) == .skip) pruned += 1;
            try testing.expect(st.row_groups_bloom <= 1);
        }
        try testing.expect(pruned >= 6);

        // Composition through pruneRowGroup: IN and OR prune only when every value is absent, AND when any is; NOT IN
        // never prunes on membership.
        const cases = [_]struct { e: []const u8, skip: bool }{
            .{ .e = "String IN ('cat', 'dog')", .skip = false },
            .{ .e = "String IN ('cat', 'fox')", .skip = true },
            .{ .e = "String = 'cat' OR String = 'dog'", .skip = false },
            .{ .e = "String = 'dog' AND String = 'cat'", .skip = true },
            .{ .e = "String NOT IN ('cat')", .skip = false },
            .{ .e = "NOT String != 'cat'", .skip = true },
        };
        for (cases) |c| {
            for (rg.columns.items) |*ch| ch.absent = &.{};
            const f = try parser.parse(a, c.e, &metas[0], null);
            _ = try load(a, testing.allocator, f, &metas, mapped.source(), .{});
            testing.expectEqual(c.skip, prune.pruneRowGroup(rg, f, &metas[0]) == .skip) catch |err| {
                std.debug.print("{s}: {s}\n", .{ path, c.e });
                return err;
            };
        }
    }
}

/// A file of `n_rgs` row groups of one BYTE_ARRAY column `c` with no statistics, row group `i` holding
/// "k<i>-<j>" for j < `per_rg`, each with a Bloom filter at 1% laid out after the row groups as writers do.
const SynthFile = struct {
    bytes: []u8,
    meta: schema.FileMetaData,

    fn init(a: std.mem.Allocator, n_rgs: usize, per_rg: usize) !SynthFile {
        return initOpts(a, n_rgs, per_rg, null, 0);
    }

    /// `gap` bytes between filters. Zero is the column-major run zpq writes.
    fn initGap(a: std.mem.Allocator, n_rgs: usize, per_rg: usize, gap: usize) !SynthFile {
        return initOpts(a, n_rgs, per_rg, null, gap);
    }

    /// `forge_from`: the headers of filters from that row group on carry an extra 32-byte field after the three
    /// unions. Still valid (readers skip unknown fields), but the bitset no longer sits where a header of the canonical
    /// shape would put it.
    fn initOpts(a: std.mem.Allocator, n_rgs: usize, per_rg: usize, forge_from: ?usize, gap: usize) !SynthFile {
        var meta: schema.FileMetaData = .{
            .version = 1,
            .schema = .empty,
            .num_rows = 0,
            .created_by = null,
            .row_groups = .empty,
        };
        try meta.schema.appendSlice(a, &.{ test_fixtures.group("schema", 1), test_fixtures.leaf("c", .BYTE_ARRAY) });
        var bytes: std.ArrayList(u8) = .empty;
        // Stand-in data pages: only the filters are ever read here.
        try bytes.appendNTimes(a, 'x', 4 + n_rgs * 1000);
        for (0..n_rgs) |i| {
            var b = try bloom.Builder.init(a, per_rg, 0.01);
            for (0..per_rg) |j| b.insert(bloom.hashBytes(try std.fmt.allocPrint(a, "k{d}-{d}", .{ i, j })));
            b.foldToFpp(0.01);
            var filt = try b.serialize(a);
            if (forge_from != null and i >= forge_from.?) {
                const h = bloom.parseHeader(filt).?;
                var forged: std.ArrayList(u8) = .empty;
                try forged.appendSlice(a, filt[0 .. h.len - 1]);
                try forged.appendSlice(a, &.{ 0x18, 30 });
                try forged.appendNTimes(a, 'z', 30);
                try forged.append(a, 0);
                try forged.appendSlice(a, filt[h.len..]);
                filt = forged.items;
            }
            var path: schema.StringList = .empty;
            try path.append(a, "c");
            var rg: schema.RowGroup = .{ .columns = .empty, .total_byte_size = 1000, .num_rows = @intCast(per_rg) };
            try rg.columns.append(a, .{ .file_path = null, .file_offset = 4, .meta_data = .{
                .type = .BYTE_ARRAY,
                .encodings = .empty,
                .path_in_schema = path,
                .codec = .UNCOMPRESSED,
                .num_values = @intCast(per_rg),
                .total_uncompressed_size = 1000,
                .total_compressed_size = 1000,
                .data_page_offset = @intCast(4 + i * 1000),
                .index_page_offset = null,
                .dictionary_page_offset = null,
                .bloom_filter_offset = @intCast(bytes.items.len),
                .bloom_filter_length = @intCast(filt.len),
            } });
            try bytes.appendSlice(a, filt);
            if (gap > 0 and i + 1 < n_rgs) try bytes.appendNTimes(a, 'g', gap);
            try meta.row_groups.append(a, rg);
        }
        return .{ .bytes = bytes.items, .meta = meta };
    }

    /// `n_rgs` copies of one filter that contains the literal `keep`. Building it once keeps the large-run tests cheap.
    fn initSolid(a: std.mem.Allocator, n_rgs: usize, per_rg: usize) !SynthFile {
        var b = try bloom.Builder.init(a, per_rg, 0.01);
        b.insert(bloom.hashBytes("keep"));
        // Leave it at the size `per_rg` asked for. Folding one inserted key would shrink it to a block.
        const filt = try b.serialize(a);
        var meta: schema.FileMetaData = .{
            .version = 1,
            .schema = .empty,
            .num_rows = 0,
            .created_by = null,
            .row_groups = .empty,
        };
        try meta.schema.appendSlice(a, &.{ test_fixtures.group("schema", 1), test_fixtures.leaf("c", .BYTE_ARRAY) });
        var bytes: std.ArrayList(u8) = .empty;
        try bytes.appendNTimes(a, 'x', 4 + n_rgs * 64);
        for (0..n_rgs) |i| {
            var path: schema.StringList = .empty;
            try path.append(a, "c");
            var rg: schema.RowGroup = .{ .columns = .empty, .total_byte_size = 64, .num_rows = 1 };
            try rg.columns.append(a, .{ .file_path = null, .file_offset = 4, .meta_data = .{
                .type = .BYTE_ARRAY,
                .encodings = .empty,
                .path_in_schema = path,
                .codec = .UNCOMPRESSED,
                .num_values = 1,
                .total_uncompressed_size = 64,
                .total_compressed_size = 64,
                .data_page_offset = @intCast(4 + i * 64),
                .index_page_offset = null,
                .dictionary_page_offset = null,
                .bloom_filter_offset = @intCast(bytes.items.len),
                .bloom_filter_length = @intCast(filt.len),
            } });
            try bytes.appendSlice(a, filt);
            try meta.row_groups.append(a, rg);
        }
        return .{ .bytes = bytes.items, .meta = meta };
    }

    /// Two contiguous columns, `c` then `d`, each a copy of one empty filter. Both prune a key they never saw.
    fn initPair(a: std.mem.Allocator, n_rgs: usize) !SynthFile {
        var b = try bloom.Builder.init(a, 32, 0.01);
        b.insert(bloom.hashBytes("present"));
        b.foldToFpp(0.01);
        const filt = try b.serialize(a);
        const h = bloom.parseHeader(filt).?;
        @memset(filt[h.len..], 0);
        var meta: schema.FileMetaData = .{
            .version = 1,
            .schema = .empty,
            .num_rows = 0,
            .created_by = null,
            .row_groups = .empty,
        };
        try meta.schema.appendSlice(a, &.{
            test_fixtures.group("schema", 2),
            test_fixtures.leaf("c", .BYTE_ARRAY),
            test_fixtures.leaf("d", .BYTE_ARRAY),
        });
        var bytes: std.ArrayList(u8) = .empty;
        try bytes.appendNTimes(a, 'x', 4);
        const off_c = try a.alloc(u64, n_rgs);
        const off_d = try a.alloc(u64, n_rgs);
        for (0..n_rgs) |i| {
            off_c[i] = @intCast(bytes.items.len);
            try bytes.appendSlice(a, filt);
        }
        for (0..n_rgs) |i| {
            off_d[i] = @intCast(bytes.items.len);
            try bytes.appendSlice(a, filt);
        }
        for (0..n_rgs) |i| {
            var rg: schema.RowGroup = .{ .columns = .empty, .total_byte_size = 64, .num_rows = 1 };
            for ([_][]const u8{ "c", "d" }, [_]u64{ off_c[i], off_d[i] }) |name, off| {
                var path: schema.StringList = .empty;
                try path.append(a, name);
                try rg.columns.append(a, .{ .file_path = null, .file_offset = 4, .meta_data = .{
                    .type = .BYTE_ARRAY,
                    .encodings = .empty,
                    .path_in_schema = path,
                    .codec = .UNCOMPRESSED,
                    .num_values = 1,
                    .total_uncompressed_size = 64,
                    .total_compressed_size = 64,
                    .data_page_offset = 4,
                    .index_page_offset = null,
                    .dictionary_page_offset = null,
                    .bloom_filter_offset = @intCast(off),
                    .bloom_filter_length = @intCast(filt.len),
                } });
            }
            try meta.row_groups.append(a, rg);
        }
        return .{ .bytes = bytes.items, .meta = meta };
    }
};

/// A remote stand-in: S3-like costs, and a log of what was asked for.
const FakeRemote = struct {
    files: []const []const u8,
    cost: Cost = .{ .request_bytes = 1 << 20, .concurrency = 8, .max_request = 24 << 20 },
    requests: usize = 0,
    bytes: u64 = 0,
    block_reads: usize = 0,
    /// One increment per `read` that fetched something over HTTP. A suffix copy is not a round.
    http_rounds: usize = 0,
    http_end_max: u64 = 0,
    http_span_max: u64 = 0,
    /// Largest HTTP byte sum of one `read` or `readInto` call.
    max_call_bytes: u64 = 0,
    /// `[suffix_start, file end)` is already in hand, the way a retained S3 tail is.
    suffix_start: ?u64 = null,
    /// Spans a warm cache already holds. A hit is copied into the caller's buffer and is not fetched.
    cached: []const Span = &.{},
    cache_copies: usize = 0,

    fn source(self: *FakeRemote) Source {
        return .{ .ctx = self, .vtable = &.{
            .read = readFn,
            .suffix = suffixFn,
            .read_into = readInto,
            .has_cached = hasCached,
            .copy_cached = copyCached,
        }, .cost = self.cost };
    }

    fn suffixFn(ctx: *anyopaque, file: u32) ?Span {
        const self: *FakeRemote = @ptrCast(@alignCast(ctx));
        const start = self.suffix_start orelse return null;
        const f = self.files[file];
        if (start >= f.len) return null;
        return .{ .file = file, .start = start, .end = f.len };
    }

    fn sliceOf(self: *FakeRemote, s: Span) []const u8 {
        const f = self.files[s.file];
        return f[@intCast(@min(s.start, f.len))..@intCast(@min(s.end, f.len))];
    }

    fn noteSpan(self: *FakeRemote, s: Span, n: usize) bool {
        if (s.len() == bloom.block_bytes) self.block_reads += 1;
        const covered = if (self.suffix_start) |suf| s.start >= suf else false;
        if (covered) return false;
        self.requests += 1;
        self.bytes += n;
        self.http_end_max = @max(self.http_end_max, s.end);
        self.http_span_max = @max(self.http_span_max, s.len());
        return true;
    }

    fn finishCall(self: *FakeRemote, http: bool, call: u64) void {
        if (http) self.http_rounds += 1;
        self.max_call_bytes = @max(self.max_call_bytes, call);
    }

    fn readFn(ctx: *anyopaque, _: std.mem.Allocator, spans: []const Span, out: [][]const u8) anyerror!void {
        const self: *FakeRemote = @ptrCast(@alignCast(ctx));
        var http = false;
        var call: u64 = 0;
        for (spans, out) |s, *o| {
            o.* = self.sliceOf(s);
            if (self.noteSpan(s, o.len)) {
                http = true;
                call += o.len;
            }
        }
        self.finishCall(http, call);
    }

    fn readInto(ctx: *anyopaque, _: std.mem.Allocator, spans: []const Span, into: [][]u8) anyerror!void {
        const self: *FakeRemote = @ptrCast(@alignCast(ctx));
        var http = false;
        var call: u64 = 0;
        for (spans, into) |s, dest| {
            const bytes = self.sliceOf(s);
            const n = @min(dest.len, bytes.len);
            @memcpy(dest[0..n], bytes[0..n]);
            if (self.noteSpan(s, n)) {
                http = true;
                call += n;
            }
        }
        self.finishCall(http, call);
    }

    fn hasCached(ctx: *anyopaque, span: Span) bool {
        const self: *FakeRemote = @ptrCast(@alignCast(ctx));
        for (self.cached) |c| {
            if (c.file == span.file and c.start == span.start and c.end == span.end) return true;
        }
        return false;
    }

    fn copyCached(ctx: *anyopaque, span: Span, dest: []u8) bool {
        const self: *FakeRemote = @ptrCast(@alignCast(ctx));
        if (!hasCached(ctx, span)) return false;
        const bytes = self.sliceOf(span);
        if (bytes.len != dest.len) return false;
        @memcpy(dest, bytes);
        self.cache_copies += 1;
        return true;
    }
};

const dispatch_cost: Cost = .{
    .request_bytes = 1 << 20,
    .concurrency = 8,
    .max_request = 24 << 20,
    .gap_cap = 1 << 20,
    .in_flight = 16,
    .split = 8 << 20,
    .window = 128 << 20,
};

fn filterOff(sf: SynthFile, rg: usize) u64 {
    return @intCast(sf.meta.row_groups.items[rg].columns.items[0].meta_data.?.bloom_filter_offset.?);
}

fn filterLen(sf: SynthFile, rg: usize) u64 {
    return @intCast(sf.meta.row_groups.items[rg].columns.items[0].meta_data.?.bloom_filter_length.?);
}

fn clearAbsent(metas: []const schema.FileMetaData) void {
    for (metas) |m| for (m.row_groups.items) |rg| for (rg.columns.items) |*c| {
        c.absent = &.{};
    };
}

test "remote: big filters are read by block, each file's layout verified, and every present key keeps its group" {
    var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_state.deinit();
    const a = arena_state.allocator();
    // 40 row groups x 200k values: ~256 KiB filters, spread far enough apart that blocks beat whole filters.
    const sf = try SynthFile.init(a, 40, 200_000);
    const metas = [_]schema.FileMetaData{sf.meta};
    // A medium where requests are cheap next to bytes (think local NVMe through io_uring).
    var remote: FakeRemote = .{ .files = &.{sf.bytes}, .cost = .{ .request_bytes = 4 << 10, .concurrency = 8 } };
    const f = try parser.parse(a, "c = 'k7-123'", &metas[0], null);
    const st = try load(a, testing.allocator, f, &metas, remote.source(), .{});
    try testing.expect(!st.bloom_stopped);
    // One read per filter: its header and the block the key selects (2 KiB in, so merged with the header), ~2 KiB each
    // where whole filters would be 10 MiB.
    try testing.expectEqual(@as(usize, 40), st.filters);
    try testing.expect(remote.bytes < 128 << 10 and remote.requests <= 41);
    var kept: usize = 0;
    for (metas[0].row_groups.items, 0..) |*rg, i| {
        const d = prune.pruneRowGroup(rg, f, &metas[0]);
        if (i == 7) try testing.expect(d != .skip);
        if (d != .skip) kept += 1;
    }
    try testing.expect(kept <= 3);
    try testing.expect(st.row_groups_bloom >= 37);

    // S3-like costs on the same file: its filters sit back to back, so reading them whole in a few large requests
    // beats a block request per filter. Same answers.
    for (metas[0].row_groups.items) |rg| for (rg.columns.items) |*c| {
        c.absent = &.{};
    };
    var s3ish: FakeRemote = .{ .files = &.{sf.bytes} };
    _ = try load(a, testing.allocator, f, &metas, s3ish.source(), .{});
    try testing.expect(s3ish.requests <= 20 and s3ish.block_reads < 32);

    // The same through a mapped source: whole filters, the same answers.
    var arena2 = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena2.deinit();
    const sf2 = try SynthFile.init(arena2.allocator(), 40, 200_000);
    const metas2 = [_]schema.FileMetaData{sf2.meta};
    const mapped: MappedSource = .{ .files = &.{sf2.bytes} };
    _ = try load(arena2.allocator(), testing.allocator, f, &metas2, mapped.source(), .{});
    for (metas[0].row_groups.items, metas2[0].row_groups.items) |*x, *y|
        try testing.expectEqual(prune.pruneRowGroup(x, f, &metas[0]), prune.pruneRowGroup(y, f, &metas2[0]));
}

test "remote: a forged header layout voids the blocks it located" {
    var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_state.deinit();
    const a = arena_state.allocator();
    const sf = try SynthFile.initOpts(a, 20, 200_000, 0, 0);
    const off0: usize = @intCast(sf.meta.row_groups.items[0].columns.items[0].meta_data.?.bloom_filter_offset.?);
    try testing.expect(bloom.bitset(sf.bytes[off0..]) != null);
    const metas = [_]schema.FileMetaData{sf.meta};
    // Cheap requests, so the blocks are what gets read. A whole filter would still parse: the extra field is skipped.
    var remote: FakeRemote = .{ .files = &.{sf.bytes}, .cost = .{ .request_bytes = 4 << 10, .concurrency = 8 } };
    const f = try parser.parse(a, "c = 'zz-not-there'", &metas[0], null);
    _ = try load(a, testing.allocator, f, &metas, remote.source(), .{});
    try testing.expect(remote.block_reads > 0);
    for (metas[0].row_groups.items) |*rg| try testing.expect(prune.pruneRowGroup(rg, f, &metas[0]) != .skip);
}

test "remote: a first wave that prunes nothing leaves the other filters unread" {
    var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_state.deinit();
    const a = arena_state.allocator();
    // One byte between filters: a row-group-major run, so the wave still stops. Gap 0 would be one contiguous column.
    const sf = try SynthFile.initGap(a, 64, 2000, 1);
    // Every row group's filter says "maybe" to a key inserted in all of them.
    for (sf.meta.row_groups.items) |rg| {
        const cm = rg.columns.items[0].meta_data.?;
        const off: usize = @intCast(cm.bloom_filter_offset.?);
        const bits = sf.bytes[off..][0..@intCast(cm.bloom_filter_length.?)];
        const h = bloom.parseHeader(bits).?;
        @memset(bits[h.len..], 0xff);
    }
    const metas = [_]schema.FileMetaData{sf.meta};
    var remote: FakeRemote = .{ .files = &.{sf.bytes} };
    const f = try parser.parse(a, "c = 'k1-1'", &metas[0], null);
    const st = try load(a, testing.allocator, f, &metas, remote.source(), .{});
    try testing.expect(st.bloom_stopped);
    try testing.expectEqual(@as(usize, 8), st.filters);
    // In memory, every filter is probed: there is nothing to save.
    var arena2 = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena2.deinit();
    const mapped: MappedSource = .{ .files = &.{sf.bytes} };
    for (metas[0].row_groups.items) |rg| for (rg.columns.items) |*c| {
        c.absent = &.{};
    };
    const st2 = try load(arena2.allocator(), testing.allocator, f, &metas, mapped.source(), .{});
    try testing.expect(!st2.bloom_stopped);
    try testing.expectEqual(@as(usize, 64), st2.filters);
}

fn paintMaybe(sf: SynthFile) void {
    for (sf.meta.row_groups.items) |rg| {
        const cm = rg.columns.items[0].meta_data.?;
        const off: usize = @intCast(cm.bloom_filter_offset.?);
        const bits = sf.bytes[off..][0..@intCast(cm.bloom_filter_length.?)];
        const h = bloom.parseHeader(bits).?;
        @memset(bits[h.len..], 0xff);
    }
}

test "remote: census a contiguous rare key is one bloom round" {
    var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_state.deinit();
    const a = arena_state.allocator();
    // B6 zpq int64_random: one column, gap 0, the key only in the last row group, the tail already in the suffix.
    const sf = try SynthFile.init(a, 16, 200_000);
    const metas = [_]schema.FileMetaData{sf.meta};
    const cut = filterOff(sf, 13);
    var remote: FakeRemote = .{ .files = &.{sf.bytes}, .cost = dispatch_cost, .suffix_start = cut };
    const f = try parser.parse(a, "c = 'k15-0'", &metas[0], null);
    const st = try load(a, testing.allocator, f, &metas, remote.source(), .{});
    try testing.expect(!st.bloom_stopped);
    try testing.expectEqual(@as(usize, 16), st.filters);
    try testing.expectEqual(@as(usize, 1), remote.http_rounds);
    try testing.expectEqual(@as(usize, 1), st.requests);
    try testing.expectEqual(cut - filterOff(sf, 0), st.bytes);
    try testing.expectEqual(cut, remote.http_end_max);
    try testing.expect(st.suffix_bytes > 0);
    try testing.expect(st.bytes + st.suffix_bytes == filterOff(sf, 15) + filterLen(sf, 15) - filterOff(sf, 0));
    try testing.expect(prune.pruneRowGroup(&metas[0].row_groups.items[15], f, &metas[0]) != .skip);

    // The same run, split at 1 MiB, is still one round. The pieces stay at most that long.
    clearAbsent(&metas);
    var split_cost = dispatch_cost;
    split_cost.split = 1 << 20;
    var remote2: FakeRemote = .{ .files = &.{sf.bytes}, .cost = split_cost, .suffix_start = cut };
    const st2 = try load(a, testing.allocator, f, &metas, remote2.source(), .{});
    try testing.expectEqual(@as(usize, 1), remote2.http_rounds);
    try testing.expect(st2.requests > 1);
    try testing.expect(remote2.http_span_max <= 1 << 20);
    try testing.expectEqual(st.bytes, st2.bytes);
}

test "remote: census filters inside the suffix make no bloom GET" {
    var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_state.deinit();
    const a = arena_state.allocator();
    // Wide k/mix and zpq l_returnflag: the column's run is already in the tail.
    const sf = try SynthFile.init(a, 16, 64);
    paintMaybe(sf);
    const metas = [_]schema.FileMetaData{sf.meta};
    var remote: FakeRemote = .{ .files = &.{sf.bytes}, .cost = dispatch_cost, .suffix_start = filterOff(sf, 0) };
    const f = try parser.parse(a, "c = 'k1-1'", &metas[0], null);
    const st = try load(a, testing.allocator, f, &metas, remote.source(), .{});
    try testing.expectEqual(@as(usize, 0), remote.http_rounds);
    try testing.expectEqual(@as(usize, 0), st.requests);
    try testing.expectEqual(@as(u64, 0), st.bytes);
    try testing.expectEqual(@as(usize, 16), st.filters);
    var want: u64 = 0;
    for (0..16) |i| want += filterLen(sf, i);
    try testing.expectEqual(want, st.suffix_bytes);
}

test "remote: census a straddling run plans only its prefix" {
    var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_state.deinit();
    const a = arena_state.allocator();
    // parquet-mr l_partkey: one contiguous column, most of it already in the suffix.
    const sf = try SynthFile.init(a, 16, 2000);
    const metas = [_]schema.FileMetaData{sf.meta};
    const cut = filterOff(sf, 8);
    var remote: FakeRemote = .{ .files = &.{sf.bytes}, .cost = dispatch_cost, .suffix_start = cut };
    const f = try parser.parse(a, "c = 'k15-0'", &metas[0], null);
    const st = try load(a, testing.allocator, f, &metas, remote.source(), .{});
    try testing.expectEqual(@as(usize, 1), remote.http_rounds);
    try testing.expectEqual(@as(usize, 1), st.requests);
    try testing.expectEqual(cut, remote.http_end_max);
    try testing.expectEqual(cut - filterOff(sf, 0), st.bytes);
    try testing.expectEqual(@as(usize, 16), st.filters);
    try testing.expect(st.suffix_bytes == filterOff(sf, 15) + filterLen(sf, 15) - cut);
}

test "remote: census a scattered common key stops after eight" {
    var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_state.deinit();
    const a = arena_state.allocator();
    // pyarrow and DuckDB: row-group-major, same-column gap over the latency cap, the key in every row group.
    const sf = try SynthFile.initGap(a, 64, 64, (1 << 20) + 64);
    paintMaybe(sf);
    const metas = [_]schema.FileMetaData{sf.meta};
    var remote: FakeRemote = .{ .files = &.{sf.bytes}, .cost = dispatch_cost };
    const f = try parser.parse(a, "c = 'k1-1'", &metas[0], null);
    const st = try load(a, testing.allocator, f, &metas, remote.source(), .{});
    try testing.expect(st.bloom_stopped);
    try testing.expectEqual(@as(usize, 8), st.filters);
    try testing.expectEqual(@as(usize, 1), remote.http_rounds);
    try testing.expectEqual(@as(usize, 8), st.requests);
}

test "remote: census a window refuses a bloom gap past its limit" {
    var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_state.deinit();
    const a = arena_state.allocator();
    const sf = try SynthFile.initGap(a, 2, 64, 64);
    const metas = [_]schema.FileMetaData{sf.meta};
    const len = filterLen(sf, 0) + filterLen(sf, 1);
    var cost = dispatch_cost;
    cost.in_flight = 1;
    cost.window = len + 32;
    var remote: FakeRemote = .{ .files = &.{sf.bytes}, .cost = cost };
    const f = try parser.parse(a, "c = 'k1-0'", &metas[0], null);
    const st = try load(a, testing.allocator, f, &metas, remote.source(), .{});
    try testing.expectEqual(@as(usize, 2), st.requests);
    try testing.expectEqual(@as(usize, 1), remote.http_rounds);

    clearAbsent(&metas);
    cost.window = 128 << 20;
    var wide: FakeRemote = .{ .files = &.{sf.bytes}, .cost = cost };
    const st2 = try load(a, testing.allocator, f, &metas, wide.source(), .{});
    try testing.expectEqual(@as(usize, 1), st2.requests);
}

test "remote: an omitted filter length is sized by the shared header probe" {
    var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_state.deinit();
    const a = arena_state.allocator();
    const sf = try SynthFile.init(a, 4, 8);
    for (sf.meta.row_groups.items) |*rg| {
        if (rg.columns.items[0].meta_data) |*cm| cm.bloom_filter_length = null;
    }
    const metas = [_]schema.FileMetaData{sf.meta};
    var remote: FakeRemote = .{ .files = &.{sf.bytes}, .cost = dispatch_cost };
    const f = try parser.parse(a, "c = 'k3-0'", &metas[0], null);
    const st = try load(a, testing.allocator, f, &metas, remote.source(), .{});
    try testing.expectEqual(@as(usize, 4), st.filters);
    try testing.expect(remote.http_rounds >= 1);
    try testing.expect(prune.pruneRowGroup(&metas[0].row_groups.items[3], f, &metas[0]) != .skip);
    try testing.expect(prune.pruneRowGroup(&metas[0].row_groups.items[0], f, &metas[0]) == .skip);
}

fn zeroBits(sf: SynthFile, rg: usize) void {
    const off: usize = @intCast(filterOff(sf, rg));
    const bits = sf.bytes[off..][0..@intCast(filterLen(sf, rg))];
    const h = bloom.parseHeader(bits).?;
    @memset(bits[h.len..], 0);
}

test "remote: a contiguous run over one round is waved, and a saturated one stops" {
    var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_state.deinit();
    const a = arena_state.allocator();
    const n = 64;
    const sf = try SynthFile.initSolid(a, n, 200_000);
    var total: u64 = 0;
    for (0..n) |i| total += filterLen(sf, i);
    try testing.expect(total > one_round_bytes);
    // The key is only in the last filter. The first wave prunes, so the rest is read in window-sized waves.
    for (0..n - 1) |i| zeroBits(sf, i);
    var cost = dispatch_cost;
    cost.window = 1 << 20;
    const metas = [_]schema.FileMetaData{sf.meta};
    var remote: FakeRemote = .{ .files = &.{sf.bytes}, .cost = cost };
    const rare = try parser.parse(a, "c = 'keep'", &metas[0], null);
    const st = try load(a, testing.allocator, rare, &metas, remote.source(), .{});
    try testing.expect(!st.bloom_stopped);
    try testing.expectEqual(n, st.filters);
    try testing.expect(remote.http_rounds > 1);
    try testing.expect(prune.pruneRowGroup(&metas[0].row_groups.items[n - 1], rare, &metas[0]) != .skip);
    try testing.expect(prune.pruneRowGroup(&metas[0].row_groups.items[0], rare, &metas[0]) == .skip);

    // Saturated bits: the first wave prunes nothing, and the rest of the run stays unread.
    paintMaybe(sf);
    clearAbsent(&metas);
    var stopped: FakeRemote = .{ .files = &.{sf.bytes}, .cost = dispatch_cost };
    const common = try parser.parse(a, "c = 'keep'", &metas[0], null);
    const st2 = try load(a, testing.allocator, common, &metas, stopped.source(), .{});
    try testing.expect(st2.bloom_stopped);
    try testing.expectEqual(@as(usize, 8), st2.filters);
    try testing.expect(stopped.bytes * 2 < total);
}

test "remote: cheap runs from every file are one dispatch" {
    var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_state.deinit();
    const a = arena_state.allocator();
    const sf0 = try SynthFile.init(a, 1, 64);
    const sf1 = try SynthFile.init(a, 1, 64);
    const metas = [_]schema.FileMetaData{ sf0.meta, sf1.meta };
    var remote: FakeRemote = .{ .files = &.{ sf0.bytes, sf1.bytes }, .cost = dispatch_cost };
    const f = try parser.parse(a, "c = 'k0-0'", &metas[0], null);
    const st = try load(a, testing.allocator, f, &metas, remote.source(), .{});
    try testing.expectEqual(@as(usize, 1), remote.http_rounds);
    try testing.expectEqual(@as(usize, 2), st.filters);
    try testing.expect(!st.bloom_stopped);
}

test "remote: a straddling filter is one buffer of its length" {
    var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_state.deinit();
    const a = arena_state.allocator();
    const sf = try SynthFile.initSolid(a, 2, 200_000);
    zeroBits(sf, 0);
    const metas = [_]schema.FileMetaData{sf.meta};
    const cut = filterOff(sf, 1) + filterLen(sf, 1) / 2;
    const span0: Span = .{ .file = 0, .start = filterOff(sf, 0), .end = filterOff(sf, 0) + filterLen(sf, 0) };
    const span1: Span = .{ .file = 0, .start = filterOff(sf, 1), .end = filterOff(sf, 1) + filterLen(sf, 1) };
    const suf: Span = .{ .file = 0, .start = cut, .end = sf.bytes.len };
    const charged = heldBytes(span0, suf) + heldBytes(span1, suf);
    try testing.expectEqual(span0.len() + span1.len(), charged);
    var cost = dispatch_cost;
    cost.window = charged;
    var remote: FakeRemote = .{ .files = &.{sf.bytes}, .cost = cost, .suffix_start = cut };
    const f = try parser.parse(a, "c = 'keep'", &metas[0], null);
    const st = try load(a, testing.allocator, f, &metas, remote.source(), .{});
    try testing.expectEqual(@as(usize, 2), st.filters);
    try testing.expectEqual(@as(usize, 1), remote.http_rounds);
    // The wave is charged the filter lengths. What comes back is at most that: a block plan reads less, and neither
    // plan allocates a second joined copy.
    try testing.expect(st.bytes + st.suffix_bytes > 0);
    try testing.expect(st.bytes + st.suffix_bytes <= span0.len() + span1.len());
    try testing.expect(prune.pruneRowGroup(&metas[0].row_groups.items[1], f, &metas[0]) != .skip);

    clearAbsent(&metas);
    cost.window = span1.len() / 2;
    var small: FakeRemote = .{ .files = &.{sf.bytes}, .cost = cost, .suffix_start = cut };
    const st2 = try load(a, testing.allocator, f, &metas, small.source(), .{});
    try testing.expectEqual(@as(usize, 0), st2.filters);
    try testing.expectEqual(@as(usize, 2), st2.bloom_skipped_oversize);
    try testing.expectEqual(@as(usize, 0), small.requests);
    try testing.expect(prune.pruneRowGroup(&metas[0].row_groups.items[1], f, &metas[0]) != .skip);
}

test "remote: a filter larger than the window is not read" {
    var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_state.deinit();
    const a = arena_state.allocator();
    const sf = try SynthFile.initSolid(a, 1, 200_000);
    const metas = [_]schema.FileMetaData{sf.meta};
    var cost = dispatch_cost;
    cost.window = filterLen(sf, 0) / 2;
    var remote: FakeRemote = .{ .files = &.{sf.bytes}, .cost = cost };
    const f = try parser.parse(a, "c = 'keep'", &metas[0], null);
    const st = try load(a, testing.allocator, f, &metas, remote.source(), .{});
    try testing.expectEqual(@as(usize, 0), st.filters);
    try testing.expectEqual(@as(usize, 1), st.bloom_skipped_oversize);
    try testing.expectEqual(@as(usize, 0), remote.requests);
    try testing.expect(prune.pruneRowGroup(&metas[0].row_groups.items[0], f, &metas[0]) != .skip);
}

test "remote: omitted lengths are sized before a wave is admitted" {
    var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_state.deinit();
    const a = arena_state.allocator();
    const n = 4;
    const sf = try SynthFile.initSolid(a, n, 200_000);
    const flen = filterLen(sf, 0);
    for (0..n - 1) |i| zeroBits(sf, i);
    for (sf.meta.row_groups.items) |*rg| {
        if (rg.columns.items[0].meta_data) |*cm| cm.bloom_filter_length = null;
    }
    const metas = [_]schema.FileMetaData{sf.meta};
    var cost = dispatch_cost;
    cost.window = flen;
    var remote: FakeRemote = .{ .files = &.{sf.bytes}, .cost = cost };
    const f = try parser.parse(a, "c = 'keep'", &metas[0], null);
    const st = try load(a, testing.allocator, f, &metas, remote.source(), .{});
    try testing.expectEqual(n, st.filters);
    try testing.expectEqual(@as(usize, 0), st.bloom_skipped_oversize);
    try testing.expect(remote.max_call_bytes <= flen);
    try testing.expect(remote.http_rounds > 1);
    try testing.expect(prune.pruneRowGroup(&metas[0].row_groups.items[n - 1], f, &metas[0]) != .skip);
}

test "remote: a warm cache hit is copied into the wave and charged" {
    var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_state.deinit();
    const a = arena_state.allocator();
    const n = 3;
    const sf = try SynthFile.initSolid(a, n, 200_000);
    for (0..n - 1) |i| zeroBits(sf, i);
    const metas = [_]schema.FileMetaData{sf.meta};
    const cached = try a.alloc(Span, n);
    for (0..n) |i| cached[i] = .{ .file = 0, .start = filterOff(sf, i), .end = filterOff(sf, i) + filterLen(sf, i) };
    var cost = dispatch_cost;
    cost.window = filterLen(sf, 0);
    var remote: FakeRemote = .{ .files = &.{sf.bytes}, .cost = cost, .cached = cached };
    const f = try parser.parse(a, "c = 'keep'", &metas[0], null);
    const st = try load(a, testing.allocator, f, &metas, remote.source(), .{});
    try testing.expectEqual(n, st.filters);
    try testing.expectEqual(n, remote.cache_copies);
    try testing.expectEqual(@as(usize, 0), remote.requests);
    try testing.expectEqual(@as(u64, 0), st.bytes);
    try testing.expectEqual(@as(usize, 0), remote.http_rounds);
    try testing.expect(prune.pruneRowGroup(&metas[0].row_groups.items[n - 1], f, &metas[0]) != .skip);
}

test "remote: a suffix-served column is planned once per wave" {
    var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_state.deinit();
    const a = arena_state.allocator();
    const n: usize = 64;
    const sf = try SynthFile.init(a, n, 8);
    const metas = [_]schema.FileMetaData{sf.meta};
    var remote: FakeRemote = .{ .files = &.{sf.bytes}, .cost = dispatch_cost, .suffix_start = filterOff(sf, 0) };
    const f = try parser.parse(a, "c = 'k1-1'", &metas[0], null);
    const st = try load(a, testing.allocator, f, &metas, remote.source(), .{});
    try testing.expectEqual(n, st.filters);
    try testing.expectEqual(@as(usize, 0), st.requests);
    try testing.expectEqual(@as(usize, 0), remote.http_rounds);
    try testing.expectEqual(@as(usize, 1), st.planner_calls);

    // A window of one filter, and the key only in the last: each wave plans the remainder once, not every prefix.
    clearAbsent(&metas);
    const wide = try SynthFile.initSolid(a, 4, 200_000);
    for (0..3) |i| zeroBits(wide, i);
    const metas2 = [_]schema.FileMetaData{wide.meta};
    var one_cost = dispatch_cost;
    one_cost.window = filterLen(wide, 0);
    var remote2: FakeRemote = .{ .files = &.{wide.bytes}, .cost = one_cost };
    const f2 = try parser.parse(a, "c = 'keep'", &metas2[0], null);
    const st2 = try load(a, testing.allocator, f2, &metas2, remote2.source(), .{});
    try testing.expectEqual(@as(usize, 4), st2.filters);
    try testing.expectEqual(@as(usize, 4), st2.planner_calls);
    try testing.expect(!st2.bloom_stopped);
}

test "remote: an AND counts each row group once" {
    var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_state.deinit();
    const a = arena_state.allocator();
    const n = 4;
    const sf = try SynthFile.initPair(a, n);
    const metas = [_]schema.FileMetaData{sf.meta};
    var col_c: u64 = 0;
    for (0..n) |i| col_c += @intCast(sf.meta.row_groups.items[i].columns.items[0].meta_data.?.bloom_filter_length.?);
    var cost = dispatch_cost;
    // Each column fits a round. Together they do not, so they are two waves, and the count is their union.
    cost.window = col_c;
    var remote: FakeRemote = .{ .files = &.{sf.bytes}, .cost = cost };
    const f = try parser.parse(a, "c = 'zz' AND d = 'zz'", &metas[0], null);
    const st = try load(a, testing.allocator, f, &metas, remote.source(), .{});
    try testing.expectEqual(@as(usize, 2), remote.http_rounds);
    try testing.expectEqual(n, st.row_groups_bloom);
    try testing.expectEqual(n * 2, st.filters);
}

fn pageStat(page: schema.PageType, enc: schema.Encoding, count: i32) schema.PageEncodingStats {
    return .{ .page_type = @backingInt(page), .encoding = @backingInt(enc), .count = count };
}

test "dictionaryCoversChunk: encoding_stats decide; without them only the v1 signature does" {
    var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_state.deinit();
    const a = arena_state.allocator();
    const S = schema.PageEncodingStats;
    const dict = pageStat(.DICTIONARY_PAGE, .PLAIN, 1);
    const Case = struct { stats: ?[]const S, encodings: []const schema.Encoding, covers: bool };
    const cases = [_]Case{
        .{ .stats = &.{ dict, pageStat(.DATA_PAGE, .RLE_DICTIONARY, 3) }, .encodings = &.{}, .covers = true },
        .{ .stats = &.{ dict, pageStat(.DATA_PAGE_V2, .PLAIN_DICTIONARY, 3) }, .encodings = &.{}, .covers = true },
        // Fell back to PLAIN partway: the dictionary covers only the first pages.
        .{
            .stats = &.{ dict, pageStat(.DATA_PAGE, .RLE_DICTIONARY, 3), pageStat(.DATA_PAGE, .PLAIN, 2) },
            .encodings = &.{},
            .covers = false,
        },
        // No dictionary page, no data page, or a page type from the future.
        .{ .stats = &.{pageStat(.DATA_PAGE, .RLE_DICTIONARY, 3)}, .encodings = &.{}, .covers = false },
        .{ .stats = &.{dict}, .encodings = &.{}, .covers = false },
        .{
            .stats = &.{ dict, .{ .page_type = 9, .encoding = 8, .count = 1 } },
            .encodings = &.{},
            .covers = false,
        },
        // No stats: parquet-java's v1 signature says it all, a v2 chunk cannot.
        .{ .stats = null, .encodings = &.{ .PLAIN_DICTIONARY, .RLE, .BIT_PACKED }, .covers = true },
        .{ .stats = null, .encodings = &.{ .PLAIN_DICTIONARY, .PLAIN, .RLE }, .covers = false },
        .{ .stats = null, .encodings = &.{ .PLAIN, .RLE, .RLE_DICTIONARY }, .covers = false },
    };
    for (cases) |c| {
        var cm = (try test_fixtures.StatsColumn.init(a, plain(.INT32), .{})).rg.columns.items[0].meta_data.?;
        try cm.encodings.appendSlice(a, c.encodings);
        if (c.stats) |st| {
            var list: std.ArrayListUnmanaged(S) = .empty;
            try list.appendSlice(a, st);
            cm.encoding_stats = list;
        }
        try testing.expectEqual(c.covers, dictionaryCoversChunk(&cm));
    }
}

test "dictionaries prune what statistics and Bloom filters cannot, and compose with them" {
    var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_state.deinit();
    const a = arena_state.allocator();
    // tools/gen_bloom_fixture.py: group g's `cat` holds "a-g", "m-g", "z-g" (dictionary-encoded, no Bloom filter,
    // bounds "a-g".."z-g" so statistics keep every group); `id` holds even numbers, with Bloom filters.
    const fx = try openFixture(a, "ci/fixtures/parquet/bloom_keys.parquet");
    const metas = [_]schema.FileMetaData{fx.meta};
    const mapped: MappedSource = .{ .files = &.{fx.bytes} };
    const Case = struct { e: []const u8, kept: []const usize, dict: usize };
    const cases = [_]Case{
        .{ .e = "cat = 'm-3'", .kept = &.{3}, .dict = 7 },
        .{ .e = "cat = 'q-3'", .kept = &.{}, .dict = 8 },
        .{ .e = "cat IN ('m-3', 'z-5')", .kept = &.{ 3, 5 }, .dict = 6 },
        .{ .e = "cat = 'm-3' OR cat = 'a-6'", .kept = &.{ 3, 6 }, .dict = 6 },
        // AND: the Bloom filter rules out every group for the odd id first; the dictionaries are never needed.
        .{ .e = "cat = 'm-3' AND id = 1001", .kept = &.{}, .dict = 0 },
        .{ .e = "cat != 'm-3'", .kept = &.{ 0, 1, 2, 3, 4, 5, 6, 7 }, .dict = 0 },
    };
    for (cases) |c| {
        for (metas[0].row_groups.items) |rg| for (rg.columns.items) |*ch| {
            ch.absent = &.{};
        };
        const f = try parser.parse(a, c.e, &metas[0], null);
        const st = try load(a, testing.allocator, f, &metas, mapped.source(), .{});
        for (metas[0].row_groups.items, 0..) |*rg, i| {
            const keep = std.mem.indexOfScalar(usize, c.kept, i) != null;
            testing.expectEqual(keep, prune.pruneRowGroup(rg, f, &metas[0]) != .skip) catch |err| {
                std.debug.print("{s}: row group {d}\n", .{ c.e, i });
                return err;
            };
        }
        try testing.expectEqual(c.dict, st.row_groups_dict);
    }
}

test "zpq's own dictionary chunks say they cover their values" {
    var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_state.deinit();
    const a = arena_state.allocator();
    const encoder = @import("../writer/encoder.zig");
    var vals: [1000][]const u8 = undefined;
    for (&vals, 0..) |*v, i| v.* = ([_][]const u8{ "red", "green", "blue" })[i % 3];
    const elem = plain(.BYTE_ARRAY);
    const enc = try encoder.encodeColumn(a, .{
        .values = .{ .string = .{ .values = &vals } },
        .schema_elem = &elem,
        .path_in_schema = &.{"c"},
        .codec = .SNAPPY,
    });
    try testing.expect(enc.meta.dictionary_page_offset != null);
    try testing.expect(dictionaryCoversChunk(&enc.meta));
    // And the page decodes back to exactly those three entries.
    const values = dictionaryValues(a, enc.bytes, .SNAPPY) orelse return error.TestUnexpectedResult;
    const want = [_][]const u8{ "blue", "green", "purple", "red" };
    var found: [4]bool = @splat(false);
    const slot: Slot = .{ .physical = .BYTE_ARRAY, .type_length = null };
    try testing.expect(scanDictionary(values.bytes, values.count, slot, &want, &found));
    try testing.expectEqualSlices(bool, &.{ true, true, false, true }, &found);
}

fn eqLeaf(comptime tag: std.meta.Tag(ast.Filter), v: anytype) ast.Filter {
    return @unionInit(ast.Filter, @tagName(tag), .{ .col_idx = 0, .op = .Eq, .value = v, .stored = true });
}

test "fuzz: random values of every filterable type, written with filters, read back, never a false negative" {
    const encoder = @import("../writer/encoder.zig");
    const filter_eval = @import("eval.zig");
    var prng = std.Random.DefaultPrng.init(0xb100f);
    const r = prng.random();
    for (0..60) |round| {
        var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
        defer arena_state.deinit();
        const a = arena_state.allocator();
        const n = 1 + r.uintLessThan(usize, 3000);
        // Low cardinality some rounds (the encoder then writes a dictionary), unique values others.
        const card = if (r.boolean()) 1 + r.uintLessThan(usize, 40) else n;
        const nullable = r.boolean();
        const def = try a.alloc(u32, n);
        for (def) |*d| d.* = if (nullable and r.uintLessThan(u8, 5) == 0) 0 else 1;
        const fpp = ([_]f64{ 0.001, 0.01, 0.1, 0.3 })[r.uintLessThan(usize, 4)];
        const width = 1 + r.uintLessThan(usize, 24);
        const kind = round % 6;
        const physical: schema.Type = switch (kind) {
            0 => .INT32,
            1 => .INT64,
            2 => .FLOAT,
            3 => .DOUBLE,
            4 => .BYTE_ARRAY,
            else => .FIXED_LEN_BYTE_ARRAY,
        };
        var elem = plain(physical);
        if (physical == .FIXED_LEN_BYTE_ARRAY) elem.type_length = @intCast(width);
        const pool = try a.alloc(u64, card);
        for (pool) |*p| p.* = r.int(u64);
        const pick = struct {
            fn f(rr: std.Random, ps: []const u64) u64 {
                return ps[rr.uintLessThan(usize, ps.len)];
            }
        }.f;
        var col: filter_eval.Batch.Column = undefined;
        var leaves: std.ArrayList(ast.Filter) = .empty;
        switch (physical) {
            .INT32 => {
                const v = try a.alloc(i32, n);
                for (v) |*x| x.* = @truncate(@as(i64, @bitCast(pick(r, pool))));
                col = .{ .i32 = .{ .values = v, .def_levels = def, .max_def = 1 } };
                for (v, def) |x, d| if (d == 1) try leaves.append(a, eqLeaf(.int32, x));
            },
            .INT64 => {
                const v = try a.alloc(i64, n);
                for (v) |*x| x.* = @bitCast(pick(r, pool));
                col = .{ .i64 = .{ .values = v, .def_levels = def, .max_def = 1 } };
                for (v, def) |x, d| if (d == 1) try leaves.append(a, eqLeaf(.int64, x));
            },
            .FLOAT => {
                const v = try a.alloc(f32, n);
                for (v) |*x| {
                    const bits: u32 = @truncate(pick(r, pool));
                    // Signed zeros on purpose; NaN never (no reader probes it).
                    const signed_zero: f32 = if (bits % 2 == 0) 0.0 else -0.0;
                    x.* = if (bits % 7 == 0) signed_zero else @floatFromInt(@as(i32, @bitCast(bits)) >> 8);
                }
                col = .{ .f32 = .{ .values = v, .def_levels = def, .max_def = 1 } };
                for (v, def) |x, d| if (d == 1) try leaves.append(a, eqLeaf(.float, x));
            },
            .DOUBLE => {
                const v = try a.alloc(f64, n);
                for (v) |*x| {
                    const bits = pick(r, pool);
                    const signed_zero: f64 = if (bits % 2 == 0) 0.0 else -0.0;
                    const finite = @as(f64, @floatFromInt(@as(i64, @bitCast(bits)) >> 12)) / 1024.0;
                    x.* = if (bits % 7 == 0) signed_zero else finite;
                }
                col = .{ .f64 = .{ .values = v, .def_levels = def, .max_def = 1 } };
                for (v, def) |x, d| if (d == 1) try leaves.append(a, eqLeaf(.double, x));
            },
            .BYTE_ARRAY, .FIXED_LEN_BYTE_ARRAY => {
                const v = try a.alloc([]const u8, n);
                for (v) |*x| {
                    const seed = pick(r, pool);
                    const len = if (physical == .FIXED_LEN_BYTE_ARRAY) width else @as(usize, @intCast(seed % 40));
                    const buf = try a.alloc(u8, len);
                    var sp = std.Random.DefaultPrng.init(seed);
                    sp.random().bytes(buf);
                    x.* = buf;
                }
                col = .{ .string = .{ .values = v, .def_levels = def, .max_def = 1 } };
                for (v, def) |x, d| if (d == 1) try leaves.append(a, eqLeaf(.string, x));
            },
            else => unreachable,
        }
        const enc = try encoder.encodeColumn(a, .{
            .values = col,
            .schema_elem = &elem,
            .path_in_schema = &.{"c"},
            .codec = .SNAPPY,
            .bloom_fpp = fpp,
        });
        // Read side: the filter as a reader parses it, each written value keyed the way the reader keys a literal.
        const bits = bloom.bitset(enc.bloom) orelse return error.TestUnexpectedResult;
        try testing.expect(std.math.isPowerOfTwo(bits.len));
        for (leaves.items) |leaf| {
            const k = storedForm(leaf, physical, elem.type_length) orelse return error.TestUnexpectedResult;
            const present = for (0..k.n) |i| {
                if (bloom.Probe.init(bloom.hashBytes(k.form(i))).mightContain(bits)) break true;
            } else false;
            testing.expect(present) catch |err| {
                std.debug.print("round {d} {s}: a written value tests absent\n", .{ round, @tagName(physical) });
                return err;
            };
        }
        // And the filter is no bigger than the probability needs: fresh random hashes pass at about `fpp`.
        var fp: usize = 0;
        for (0..4000) |_| fp += @intFromBool(bloom.Probe.init(r.int(u64)).mightContain(bits));
        try testing.expect(@as(f64, @floatFromInt(fp)) / 4000.0 < @max(3.0 * fpp, 0.02));
    }
}

test "regression: one file mixing header shapes; blocks count only under their own filter's header" {
    var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_state.deinit();
    const a = arena_state.allocator();
    // The first filter's header is canonical and the rest carry an extra field: verifying one header per file would
    // locate every later filter's blocks wrong and miss present keys.
    const sf = try SynthFile.initOpts(a, 16, 200_000, 1, 0);
    const metas = [_]schema.FileMetaData{sf.meta};
    var remote: FakeRemote = .{ .files = &.{sf.bytes}, .cost = .{ .request_bytes = 4 << 10, .concurrency = 8 } };
    var buf: [32]u8 = undefined;
    for (0..16) |g| {
        for (metas[0].row_groups.items) |rg| for (rg.columns.items) |*c| {
            c.absent = &.{};
        };
        const f = try parser.parse(a, try std.fmt.bufPrint(&buf, "c = 'k{d}-77'", .{g}), &metas[0], null);
        _ = try load(a, testing.allocator, f, &metas, remote.source(), .{});
        try testing.expect(prune.pruneRowGroup(&metas[0].row_groups.items[g], f, &metas[0]) != .skip);
    }
}

test "regression: a header whose size disagrees with the footer's length is no filter" {
    var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_state.deinit();
    const a = arena_state.allocator();
    const fx = try openFixture(a, "ci/fixtures/parquet/bloom_keys.parquet");
    // Halve every `id` filter's numBytes (1024 -> 512 keeps its varint two bytes): the bits read would be the wrong
    // half's, and present ids would test absent.
    for (fx.meta.row_groups.items) |rg| {
        const off: usize = @intCast(rg.columns.items[0].meta_data.?.bloom_filter_offset.?);
        try testing.expectEqualSlices(u8, &.{ 0x15, 0x80, 0x10 }, fx.bytes[off..][0..3]);
        fx.bytes[off + 2] = 0x08;
    }
    const metas = [_]schema.FileMetaData{fx.meta};
    const mapped: MappedSource = .{ .files = &.{fx.bytes} };
    for (0..8) |g| for ([_]usize{ 0, 61, 333, 499 }) |j| {
        for (metas[0].row_groups.items) |rg| for (rg.columns.items) |*c| {
            c.absent = &.{};
        };
        const f = try parser.parse(a, try std.fmt.allocPrint(a, "id = {d}", .{2 * (8 * j + g)}), &metas[0], null);
        const st = try load(a, testing.allocator, f, &metas, mapped.source(), .{});
        try testing.expectEqual(@as(usize, 0), st.filters);
        try testing.expect(prune.pruneRowGroup(&metas[0].row_groups.items[g], f, &metas[0]) != .skip);
    };
}

test "regression: a footer length past any real filter is no filter" {
    var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_state.deinit();
    const a = arena_state.allocator();
    const fx = try openFixture(a, "ci/fixtures/parquet/bloom_keys.parquet");
    for (fx.meta.row_groups.items) |rg| rg.columns.items[0].meta_data.?.bloom_filter_length = std.math.maxInt(i32);
    const metas = [_]schema.FileMetaData{fx.meta};
    var remote: FakeRemote = .{ .files = &.{fx.bytes} };
    const f = try parser.parse(a, "id = 1001", &metas[0], null);
    const st = try load(a, testing.allocator, f, &metas, remote.source(), .{});
    try testing.expectEqual(@as(usize, 0), st.filters);
    try testing.expectEqual(@as(usize, 0), remote.requests);
}

test "regression: a dictionary page holding more than it says proves nothing" {
    var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_state.deinit();
    const a = arena_state.allocator();
    const fx = try openFixture(a, "ci/fixtures/parquet/bloom_keys.parquet");
    const cm = fx.meta.row_groups.items[0].columns.items[2].meta_data.?;
    const page = fx.bytes[@intCast(cm.dictionary_page_offset.?)..@intCast(cm.data_page_offset)];
    const values = dictionaryValues(a, page, cm.codec) orelse return error.TestUnexpectedResult;
    const slot: Slot = .{ .physical = .BYTE_ARRAY, .type_length = null };
    var found: [1]bool = .{false};
    try testing.expect(scanDictionary(values.bytes, values.count, slot, &.{"m-0"}, &found));
    try testing.expect(!scanDictionary(values.bytes, values.count - 1, slot, &.{"m-0"}, &found));
}

test "regression: an 8000-value IN list probes in time linear in its length" {
    var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_state.deinit();
    const a = arena_state.allocator();
    const sf = try SynthFile.init(a, 16, 2000);
    const metas = [_]schema.FileMetaData{sf.meta};
    var expr: std.ArrayList(u8) = .empty;
    try expr.appendSlice(a, "c IN (");
    for (0..8000) |i| try expr.print(a, "{s}'z{d}'", .{ if (i == 0) "" else ", ", i });
    try expr.appendSlice(a, ", 'k3-5')");
    const f = try parser.parse(a, expr.items, &metas[0], null);
    var remote: FakeRemote = .{ .files = &.{sf.bytes}, .cost = .{ .request_bytes = 4 << 10, .concurrency = 8 } };
    const t0 = nowMonoNs();
    _ = try load(a, testing.allocator, f, &metas, remote.source(), .{});
    const ms = @divTrunc(nowMonoNs() - t0, std.time.ns_per_ms);
    // Quadratic structures took tens of seconds here; a debug build does this in well under one.
    try testing.expect(ms < 5000);
    for (metas[0].row_groups.items, 0..) |*rg, g| {
        if (g == 3) try testing.expect(prune.pruneRowGroup(rg, f, &metas[0]) != .skip);
    }
}

test "regression: cached probes own transient key bytes through growth" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var cache: ProbeCache = .{};
    var bytes: [8]u8 = undefined;
    for (0..1024) |i| {
        std.mem.writeInt(u64, &bytes, i, .little);
        _ = try cache.get(a, &bytes);
    }
    try testing.expectEqual(@as(usize, 1024), cache.forms.count());
    for (0..1024) |i| {
        std.mem.writeInt(u64, &bytes, i, .little);
        const got = try cache.get(a, &bytes);
        const want = bloom.Probe.init(bloom.hashBytes(&bytes));
        try testing.expectEqual(want.hi, got.hi);
        try testing.expect(@reduce(.And, want.m == got.m));
    }
    try testing.expectEqual(@as(usize, 1024), cache.forms.count());
}
