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
//! to read, how to merge them and when to give up is decided here, from the source's `Cost`.

const std = @import("std");
const builtin = @import("builtin");
const ast = @import("ast.zig");
const prune = @import("prune.zig");
const schema = @import("../schema.zig");
const bloom = @import("../parquet/bloom.zig");
const compression = @import("../parquet/compression.zig");
const thrift = @import("../thrift.zig");
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
    };

    fn prefetch(self: Source, spans: []const Span) void {
        if (spans.len > 0) if (self.vtable.prefetch) |f| f(self.ctx, spans);
    }

    fn read(self: Source, arena: std.mem.Allocator, spans: []const Span, out: [][]const u8) !void {
        return self.vtable.read(self.ctx, arena, spans, out);
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

fn merge(arena: std.mem.Allocator, ranges: []const Span, gap: u64) ![]Span {
    var out: std.ArrayList(Span) = .empty;
    var cur = ranges[0];
    for (ranges[1..]) |r| {
        if (r.file == cur.file and r.start <= cur.end +| gap) {
            cur.end = @max(cur.end, r.end);
        } else {
            try out.append(arena, cur);
            cur = r;
        }
    }
    try out.append(arena, cur);
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

/// Read every request through `src`, merged and split per its cost; a request split across spans is reassembled.
fn readAll(arena: std.mem.Allocator, src: Source, reqs: []Req, stats: *Stats) !void {
    if (reqs.len == 0) return;
    const spans = try planSpans(arena, try sortedSpans(arena, reqs), src.cost);
    const got = try arena.alloc([]const u8, spans.len);
    try src.read(arena, spans, got);
    // A mapping reads nothing up front; the pages a probe touches fault in on their own.
    if (src.cost.remote()) {
        stats.requests += spans.len;
        for (got) |g| stats.bytes += g.len;
    }
    for (reqs) |*r| r.out = try assemble(arena, spans, got, r.span);
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
    /// Requests made, and bytes read by them (remote sources only).
    requests: usize = 0,
    bytes: u64 = 0,
    /// A first wave pruned too little to pay for reading the rest of its kind.
    bloom_stopped: bool = false,
    dict_stopped: bool = false,
    ns: u64 = 0,

    pub fn add(self: *Stats, o: Stats) void {
        self.row_groups_bloom += o.row_groups_bloom;
        self.row_groups_dict += o.row_groups_dict;
        self.filters += o.filters;
        self.dictionaries += o.dictionaries;
        self.requests += o.requests;
        self.bytes += o.bytes;
        self.bloom_stopped = self.bloom_stopped or o.bloom_stopped;
        self.dict_stopped = self.dict_stopped or o.dict_stopped;
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

fn isAbsent(t: *const Target, k: *const Key) bool {
    for (t.absent.items) |a| if (std.mem.eql(u8, a, k.form(0))) return true;
    return false;
}

fn orderBytes(want: []const u8, have: []const u8) std.math.Order {
    return std.mem.order(u8, want, have);
}

/// Probes hoisted per literal form: the hash and its 8-lane mask, computed once however many chunks they test.
const ProbeCache = struct {
    forms: std.ArrayList(struct { bytes: []const u8, probe: bloom.Probe }) = .empty,

    fn get(self: *ProbeCache, arena: std.mem.Allocator, bytes: []const u8) !bloom.Probe {
        for (self.forms.items) |f| if (std.mem.eql(u8, f.bytes, bytes)) return f.probe;
        const p = bloom.Probe.init(bloom.hashBytes(bytes));
        try self.forms.append(arena, .{ .bytes = try arena.dupe(u8, bytes), .probe = p });
        return p;
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
    var stats: Stats = .{};
    var p: Probing = .{
        .arena = arena,
        .scratch = scratch.allocator(),
        .metas = metas,
        .src = src,
        .stats = &stats,
        .filter = filter,
        .opts = opts,
    };
    try p.run();
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
        if (l <= 0) return null;
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
    const DICTIONARY_PAGE = @intFromEnum(schema.PageType.DICTIONARY_PAGE);
    const plain_dictionary = @intFromEnum(schema.Encoding.PLAIN_DICTIONARY);
    const rle_dictionary = @intFromEnum(schema.Encoding.RLE_DICTIONARY);
    if (cm.encoding_stats) |es| {
        var dict_page = false;
        var data_page = false;
        for (es.items) |e| {
            if (e.count <= 0) continue;
            switch (e.page_type) {
                DICTIONARY_PAGE => dict_page = true,
                @intFromEnum(schema.PageType.INDEX_PAGE) => {},
                @intFromEnum(schema.PageType.DATA_PAGE), @intFromEnum(schema.PageType.DATA_PAGE_V2) => {
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
    return true;
}

/// Whether a file's writer emits the canonical header, which is what lets a block be located without its header.
const Verdict = enum { unknown, canonical, other };

/// Bytes read to learn a filter's size when the footer leaves its length out. The header is at most 19.
const header_probe_bytes = 64;

/// How one target's filter comes back from a read.
const Fetch = union(enum) {
    none,
    /// Header and bitset, already in hand.
    bytes: []const u8,
    /// Header and bitset, read by request `req`.
    whole: usize,
    /// Only the blocks the keys select: requests `first..end`, block `blocks[i - first]` each.
    blocks: struct { first: usize, end: usize, num_blocks: u64, blocks: []const u64 },
};

const ReadPlan = struct {
    reqs: std.ArrayList(Req) = .empty,
    fetch: []Fetch,
    /// A real header read alongside a file's blocks, to verify the location they were found by.
    headers: std.ArrayList(struct { file: u32, req: usize, total_len: u64 }) = .empty,
};

const WaveResult = struct { row_groups: usize = 0, pruned: usize = 0 };

const Stage = enum { bloom, dictionary };

const Probing = struct {
    /// Where what is proven is recorded: lives as long as the footers.
    arena: std.mem.Allocator,
    /// Bytes read and decoded along the way.
    scratch: std.mem.Allocator,
    metas: []const schema.FileMetaData,
    src: Source,
    stats: *Stats,
    filter: ast.Filter,
    opts: Options,
    verdicts: []Verdict = &.{},
    probes: ProbeCache = .{},

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
            .bloom => self.stats.row_groups_bloom += res.pruned,
            .dictionary => self.stats.row_groups_dict += res.pruned,
        }
        return res;
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
        var heads: std.ArrayList(Req) = .empty;
        var head_of: std.ArrayList(usize) = .empty;
        for (wave, 0..) |t, i| {
            const s = filterSpan(t, self.metas).?;
            if (s.end != s.start) continue;
            const head: Span = .{ .file = s.file, .start = s.start, .end = s.start + header_probe_bytes };
            try heads.append(a, .{ .span = head });
            try head_of.append(a, i);
        }
        try readAll(a, self.src, heads.items, self.stats);
        const spans = try a.alloc(?Span, wave.len);
        for (wave, spans) |t, *s| {
            const fs = filterSpan(t, self.metas).?;
            s.* = if (fs.end != fs.start) fs else null;
        }
        for (heads.items, head_of.items) |r, i| {
            const h = bloom.parseHeader(r.out) orelse continue;
            const total = h.len + h.num_bytes;
            if (total <= r.out.len) {
                fetch[i] = .{ .bytes = r.out[0..total] };
            } else {
                spans[i] = .{ .file = r.span.file, .start = r.span.start, .end = r.span.start + total };
            }
        }
        for (wave, spans, fetch) |_, s, *f| if (s) |sp| if (self.src.vtable.cached) |cached| {
            if (cached(self.src.ctx, a, sp)) |bytes| f.* = .{ .bytes = bytes };
        };

        // Whole filters, or just the blocks the keys select, by the source's cost model. A whole filter can serve later
        // queries from the warm cache, blocks only this one, so blocks must save a quarter of the time to be chosen.
        var plan = try self.wholePlan(wave, spans, fetch);
        if (self.src.cost.remote()) {
            if (try self.sparsePlan(wave, spans, fetch)) |sparse| {
                const cost = self.src.cost;
                const t_sparse = try estimateReqs(a, sparse.reqs.items, cost);
                if (t_sparse * 4 < try estimateReqs(a, plan.reqs.items, cost) * 3) plan = sparse;
            }
        }
        try readAll(a, self.src, plan.reqs.items, self.stats);
        for (plan.headers.items) |hv| {
            var buf: [bloom.max_header_bytes]u8 = undefined;
            const want = bloom.canonicalHeader(hv.total_len, &buf);
            const ok = want != null and std.mem.eql(u8, want.?, plan.reqs.items[hv.req].out);
            self.verdicts[hv.file] = if (ok) .canonical else .other;
        }

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
                    // Blocks count only once a real header of their file matched the canonical one that located them.
                    if (self.verdicts[t.file] != .canonical) continue;
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

    /// Read the dictionary pages of `wave` and record every key none of them holds.
    fn probeDictionaries(self: *Probing, wave: []const *Target) !void {
        const a = self.scratch;
        // A dictionary page whose length the footer does not give costs a header read first.
        const spans = try a.alloc(?Span, wave.len);
        var heads: std.ArrayList(Req) = .empty;
        var head_of: std.ArrayList(usize) = .empty;
        for (wave, spans, 0..) |t, *s, i| {
            const ds = dictSpan(t, self.metas).?;
            s.* = if (ds.end != ds.start) ds else null;
            if (ds.end == ds.start) {
                const head: Span = .{ .file = ds.file, .start = ds.start, .end = ds.start + page_header_probe };
                try heads.append(a, .{ .span = head });
                try head_of.append(a, i);
            }
        }
        try readAll(a, self.src, heads.items, self.stats);
        for (heads.items, head_of.items) |r, i| {
            const len = dictionaryPageLen(r.out) orelse continue;
            spans[i] = .{ .file = r.span.file, .start = r.span.start, .end = r.span.start + len };
        }
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
        try readAll(a, self.src, reqs.items, self.stats);

        var want: std.ArrayList([]const u8) = .empty;
        for (wave, req_of) |t, ri| {
            const page = reqs.items[ri orelse continue].out;
            const cm = t.chunk(self.metas).meta_data.?;
            const values = dictionaryValues(a, page, cm.codec) orelse continue;
            self.stats.dictionaries += 1;
            // Every form of every key still open, sorted so each entry costs one binary search.
            want.clearRetainingCapacity();
            for (t.keys.items) |*k| {
                if (isAbsent(t, k)) continue;
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
                if (isAbsent(t, k)) continue;
                const held = for (0..k.n) |fi| {
                    const at = std.sort.binarySearch([]const u8, want.items, k.form(fi), orderBytes).?;
                    if (found[at]) break true;
                } else false;
                if (!held) try t.absent.append(self.arena, k.form(0));
            }
        }
    }

    /// The blocks the keys select in each filter of known length, located through the canonical header, plus one real
    /// header per file not yet verified to check that location by. Filters it cannot locate stay whole; null when it
    /// locates none.
    fn sparsePlan(self: *Probing, wave: []const *Target, spans: []const ?Span, fetch: []const Fetch) !?ReadPlan {
        const a = self.scratch;
        var plan: ReadPlan = .{ .fetch = try a.dupe(Fetch, fetch) };
        const header_asked = try a.alloc(bool, self.metas.len);
        @memset(header_asked, false);
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
            for (t.keys.items) |*k| for (0..k.n) |i| {
                const b = (try self.probes.get(a, k.form(i))).block(num_blocks);
                if (std.mem.indexOfScalar(u64, blocks.items, b) == null) try blocks.append(a, b);
            };
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
            } };
            if (self.verdicts[t.file] == .unknown and !header_asked[t.file]) {
                header_asked[t.file] = true;
                try plan.headers.append(a, .{ .file = t.file, .req = plan.reqs.items.len, .total_len = sp.len() });
                try plan.reqs.append(a, .{ .span = .{ .file = sp.file, .start = sp.start, .end = sp.start + h.len } });
            }
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
        const i = std.mem.indexOfScalar(u64, self.blocks, want) orelse return true;
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
    // 100 blocks 300 KB apart: one request each would queue ~13 deep per lane; reading the 30 MB between them in eight
    // pieces is far cheaper.
    var close: [100]Span = undefined;
    for (&close, 0..) |*s, i| s.* = .{ .file = 0, .start = i * 300_000, .end = i * 300_000 + 32 };
    const merged = try planSpans(a, &close, s3ish);
    try testing.expect(merged.len <= 16);
    try testing.expect(try estimate(a, merged, s3ish) < try estimate(a, &close, s3ish));
    for (merged) |s| try testing.expect(s.len() <= s3ish.max_request);
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
        return initOpts(a, n_rgs, per_rg, false);
    }

    /// `forge`: headers carry an extra 32-byte field after the three unions. Still valid (readers skip unknown
    /// fields), but the bitset no longer sits where a header of the canonical shape would put it.
    fn initOpts(a: std.mem.Allocator, n_rgs: usize, per_rg: usize, forge: bool) !SynthFile {
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
            if (forge) {
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

    fn source(self: *FakeRemote) Source {
        return .{ .ctx = self, .vtable = &.{ .read = readFn }, .cost = self.cost };
    }

    fn readFn(ctx: *anyopaque, _: std.mem.Allocator, spans: []const Span, out: [][]const u8) anyerror!void {
        const self: *FakeRemote = @ptrCast(@alignCast(ctx));
        for (spans, out) |s, *o| {
            const f = self.files[s.file];
            o.* = f[@intCast(@min(s.start, f.len))..@intCast(@min(s.end, f.len))];
            self.requests += 1;
            self.bytes += o.len;
            if (s.len() == bloom.block_bytes) self.block_reads += 1;
        }
    }
};

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
    try testing.expect(remote.bytes < 64 << 10);
    // One block per filter, the first file header verified alongside (merged into a neighbouring block's request).
    try testing.expect(remote.block_reads >= 39 and remote.requests <= 41);
    var kept: usize = 0;
    for (metas[0].row_groups.items, 0..) |*rg, i| {
        const d = prune.pruneRowGroup(rg, f, &metas[0]);
        if (i == 7) try testing.expect(d != .skip);
        if (d != .skip) kept += 1;
    }
    try testing.expect(kept <= 3);
    try testing.expect(st.row_groups_bloom >= 37);

    // S3-like costs on the same file: its filters sit back to back, so past the first wave, reading them whole in a
    // few large requests beats a block request per filter. Same answers.
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
    const sf = try SynthFile.initOpts(a, 20, 200_000, true);
    const off0: usize = @intCast(sf.meta.row_groups.items[0].columns.items[0].meta_data.?.bloom_filter_offset.?);
    try testing.expect(bloom.bitset(sf.bytes[off0..]) != null);
    const metas = [_]schema.FileMetaData{sf.meta};
    var remote: FakeRemote = .{ .files = &.{sf.bytes} };
    const f = try parser.parse(a, "c = 'zz-not-there'", &metas[0], null);
    _ = try load(a, testing.allocator, f, &metas, remote.source(), .{});
    for (metas[0].row_groups.items) |*rg| try testing.expect(prune.pruneRowGroup(rg, f, &metas[0]) != .skip);
}

test "remote: a first wave that prunes nothing leaves the other filters unread" {
    var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_state.deinit();
    const a = arena_state.allocator();
    const sf = try SynthFile.init(a, 64, 2000);
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

fn pageStat(page: schema.PageType, enc: schema.Encoding, count: i32) schema.PageEncodingStats {
    return .{ .page_type = @intFromEnum(page), .encoding = @intFromEnum(enc), .count = count };
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
