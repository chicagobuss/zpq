//! Bounds for flat GROUP BY keys. Encoding coverage and the probe cost gate use only footers;
//! entry counts use dictionary headers. Dictionary entries may be unused, so even a union is only an upper bound.
const std = @import("std");
const schema = @import("schema.zig");
const thrift = @import("thrift.zig");
const expr = @import("expr/ast.zig");
const agg = @import("expr/agg.zig");
const agg_dense = @import("expr/agg_dense.zig");
const membership = @import("filter/membership.zig");
const page = @import("parquet/page.zig");
const plain = @import("parquet/encoding/plain.zig");
const spawn = @import("spawn.zig");

pub const Stat = struct {
    source: enum { unknown, dictionary_headers, dictionary_union } = .unknown,
    lo: u64 = 0,
    hi: u64 = 0,
    /// Cost hint from a single-key union, never a proof: dictionaries can contain unused entries.
    estimate: ?u64 = null,
    rows: u64 = 0,
    covered_rows: u64 = 0,
    encoding_stats_rows: u64 = 0,
    confidence: enum { unknown, upper_bound } = .unknown,
    dictionary_bytes: u64 = 0,
    column_bytes: u64 = 0,
    key_bytes: u64 = 0,
    string_keys: bool = false,
    union_attempted: bool = false,
    union_complete: bool = false,
    union_budget_exceeded: bool = false,
};

/// `bytes` is laid out like the file. `probed`: only the ranges `probeRanges` asks for are read, a header from no more
/// than the `probe_prefix` a probe reads; with `wait`, they are still being read in (`io/local_reader.zig`), each read
/// once `wait` says its row group's are in. Explain sets `probed` over a mapping, so it reads what execution reads.
pub const File = struct {
    bytes: []const u8,
    local: bool,
    probed: bool = false,
    wait: ?Wait = null,

    fn headerLimit(f: File) usize {
        return if (f.probed) probe_prefix else map_prefix;
    }
};
pub const Wait = struct { ctx: *anyopaque, wait: *const fn (ctx: *anyopaque, rg: usize) anyerror!void };
const Ref = struct { file: usize, rg: usize };
const max_input = 25 * 1024 * 1024;
/// How far into a dictionary page `header` may parse: through a mapping, as far as a header can sensibly reach; in a
/// reader's buffer, only the prefix its probe read (a page header is tens of bytes). A header past the probe's prefix
/// leaves its row group unbounded there, never read from bytes the reader may still be filling.
const map_prefix = 64 * 1024;
const probe_prefix = 4096;

/// The probe bytes a reader holds beside its window, page-aligned as it reads them: past this, its inputs plan from
/// their footers, as they did with no probe.
pub fn probeCap(max_memory: usize) u64 {
    return @min(max_memory / 16, 256 * 1024 * 1024);
}

/// What `collect` reads, decided from the footers alone so a reader can fetch exactly that before it is read
/// (`probeRanges`): every covered key chunk's dictionary header, and per key whether its whole pages are unioned.
pub const Reads = struct { headers: bool, whole: []const bool };

/// Wanting a union only for the dense fold's domain, at this many workers (`collectRetaining`'s `fold_only`).
pub const FoldOnly = ?usize;

fn planReads(
    arena: std.mem.Allocator,
    metas: []const schema.FileMetaData,
    keys: []const expr.Expr,
    max_memory: usize,
    allow_headers: bool,
    allow_union: bool,
    retain_wanted: bool,
    fold_only: FoldOnly,
) !?Reads {
    if (keys.len == 0) return null;
    for (keys) |key| if (key != .col_ref) return null;
    const whole = try arena.alloc(bool, keys.len);
    var dict_total: u64 = 0;
    var col_total: u64 = 0;
    const dict_sizes = try arena.alloc(u64, keys.len);
    const col_sizes = try arena.alloc(u64, keys.len);
    for (keys, whole, dict_sizes, col_sizes) |key, *w, *d, *c| {
        const ci = key.col_ref.col_idx;
        w.* = true;
        d.* = 0;
        c.* = 0;
        for (metas) |*m| for (m.row_groups.items) |*rg| {
            if (ci >= rg.columns.items.len) {
                w.* = false;
                continue;
            }
            const cm = rg.columns.items[ci].meta_data orelse {
                w.* = false;
                continue;
            };
            const flat = cm.path_in_schema.items.len == 1 and m.getColumnLevels(cm.path_in_schema.items).max_rep == 0;
            if (!flat) return null;
            c.* +|= @intCast(@max(0, cm.total_compressed_size));
            if (dictBytes(cm)) |n| d.* +|= n else w.* = false;
            if (!fullCoverage(&cm)) w.* = false;
        };
        dict_total +|= d.*;
        col_total +|= c.*;
    }
    if (!allow_headers) {
        @memset(whole, false);
        return .{ .headers = false, .whole = whole };
    }
    const total_gate = dict_total <= max_input or dict_total <= col_total / 20;
    for (keys, whole, dict_sizes, col_sizes) |key, *w, d, c| {
        const gated = total_gate and (d <= max_input or d <= c / 20);
        const retain = retain_wanted and keys.len == 1 and
            (key.col_ref.physical_type == .INT32 or key.col_ref.physical_type == .INT64);
        w.* = allow_union and w.* and max_memory >= 1024 * 1024 and (gated or retain);
    }
    if (fold_only) |workers| if (keys.len == 1 and whole[0] and foldDeclined(metas, keys[0], workers)) {
        whole[0] = false;
    };
    return .{ .headers = true, .whole = whole };
}

/// The footers' guess that the dense fold would decline a lone key's domain: `workers` times its widest dictionary,
/// in entries of the narrowest width its stored bytes allow, past all of them. A guess (compression shrinks both
/// sides alike), made the same way wherever the reads are planned.
fn foldDeclined(metas: []const schema.FileMetaData, key: expr.Expr, workers: usize) bool {
    const col = key.col_ref.col_idx;
    var widest: u64 = 0;
    var all: u64 = 0;
    for (metas) |*m| for (m.row_groups.items) |*rg| {
        const cm = rg.columns.items[col].meta_data orelse return false;
        const n = (dictBytes(cm) orelse return false) / @as(u64, if (cm.type == .INT64) 8 else 4);
        widest = @max(widest, n);
        all +|= n + 1;
    };
    return workers *| widest > all;
}

pub const Probe = struct { file: usize, rg: usize, start: u64, end: u64 };

/// The dictionary bytes `collect` will read with these arguments, from footers alone: every key chunk's whole
/// dictionary page where its key is unioned, its header otherwise. A reader that reads these first, and `collect`
/// waiting for them (`File.wait`), plans from the same facts as a mapping does.
pub fn probeRanges(
    arena: std.mem.Allocator,
    metas: []const schema.FileMetaData,
    keys: []const expr.Expr,
    max_memory: usize,
    allow_headers: bool,
    allow_union: bool,
    retain_wanted: bool,
    fold_only: FoldOnly,
) ![]const Probe {
    const planned = try planReads(arena, metas, keys, max_memory, allow_headers, allow_union, retain_wanted, fold_only);
    const reads = planned orelse return &.{};
    if (!reads.headers) return &.{};
    var out: std.ArrayList(Probe) = .empty;
    for (metas, 0..) |*m, fi| for (m.row_groups.items, 0..) |*rg, ri| for (keys, reads.whole) |key, w| {
        const ci = key.col_ref.col_idx;
        if (ci >= rg.columns.items.len) continue;
        const cm = rg.columns.items[ci].meta_data orelse continue;
        if (!fullCoverage(&cm)) continue;
        const len = dictBytes(cm) orelse continue;
        const start: u64 = @intCast(cm.dictionary_page_offset.?);
        const end = start + if (w) len else @min(len, probe_prefix);
        try out.append(arena, .{ .file = fi, .rg = ri, .start = start, .end = end });
    };
    return out.items;
}

pub fn fullCoverage(cm: *const schema.ColumnMetaData) bool {
    // The legacy encodings list does not prove coverage; PLAIN can describe the dictionary OR fallback data.
    return cm.encoding_stats != null and membership.dictionaryCoversChunk(cm);
}

fn dictBytes(cm: schema.ColumnMetaData) ?u64 {
    const start = cm.dictionary_page_offset orelse return null;
    if (start < 0 or cm.data_page_offset <= start) return null;
    return @intCast(cm.data_page_offset - start);
}

fn dictSlice(file: File, rg: usize, cm: schema.ColumnMetaData) ?[]const u8 {
    if (!file.local) return null;
    // A failed read is reported where the scan reads the row group; here it only leaves the bound unproven.
    if (file.wait) |w| w.wait(w.ctx, rg) catch return null;
    const len = dictBytes(cm) orelse return null;
    const start: u64 = @intCast(cm.dictionary_page_offset.?);
    if (start > file.bytes.len or len > file.bytes.len - start) return null;
    return file.bytes[@intCast(start)..][0..@intCast(len)];
}

fn header(bytes: []const u8, limit: usize) ?schema.PageHeader {
    // Bound malformed header work and never cross into the following data page.
    var reader = thrift.Reader.init(bytes[0..@min(bytes.len, limit)]);
    const h = schema.PageHeader.read(&reader) catch return null;
    if (h.type != .DICTIONARY_PAGE or h.compressed_page_size < 0 or h.uncompressed_page_size < 0) return null;
    const d = h.dictionary_page_header orelse return null;
    // Legacy writers label PLAIN dictionary payloads PLAIN_DICTIONARY (the execution reader accepts these too).
    if (d.num_values < 0 or (d.encoding != .PLAIN and d.encoding != .PLAIN_DICTIONARY)) return null;
    if (@as(u64, @intCast(h.compressed_page_size)) + reader.pos != bytes.len) return null;
    return h;
}

fn nullBound(cm: schema.ColumnMetaData) u64 {
    // Missing null_count is not zero. One extra entry is always safe for an optional flat key.
    return if (cm.statistics) |s| if (s.null_count == 0) 0 else 1 else 1;
}

pub fn collect(
    gpa: std.mem.Allocator,
    arena: std.mem.Allocator,
    files: []const File,
    metas: []const schema.FileMetaData,
    keys: []const expr.Expr,
    workers: usize,
    max_memory: usize,
    allow_union: bool,
) !Stat {
    return collectRetaining(gpa, arena, files, metas, keys, workers, max_memory, true, allow_union, null, false);
}

/// `collect`, also keeping a lone INT32/INT64 key's union as an exact domain in `domain` (owned by the caller, gpa)
/// when it is complete and small (`agg_dense.Domain`). Retention never changes the bound: it is dropped, not failed,
/// past `agg_dense.max_ids` values or `max_memory / 16` bytes.
pub fn collectRetaining(
    gpa: std.mem.Allocator,
    arena: std.mem.Allocator,
    files: []const File,
    metas: []const schema.FileMetaData,
    keys: []const expr.Expr,
    workers: usize,
    max_memory: usize,
    /// False reads no dictionary at all (a plan the bound cannot change), as `probeRanges` then asks for none.
    allow_headers: bool,
    allow_union: bool,
    domain: ?*?agg_dense.Domain,
    /// The union is wanted only for the dense fold's domain: skip it when the footers, or then the headers, show the
    /// fold would decline it.
    fold_only: bool,
) !Stat {
    var stat: Stat = .{};
    var refs: std.ArrayList(Ref) = .empty;
    for (metas, 0..) |m, fi| for (m.row_groups.items, 0..) |rg, ri| {
        stat.rows +|= @intCast(@max(0, rg.num_rows));
        try refs.append(arena, .{ .file = fi, .rg = ri });
    };
    stat.hi = stat.rows;
    if (keys.len == 0) return stat;
    for (keys) |key| {
        stat.string_keys = stat.string_keys or key.typeOf() == .str;
        if (key != .col_ref) return stat;
    }
    // The footer's half of every read below, shared with `probeRanges` so a reader fetches exactly what this reads.
    // Null only for a nested key, which returns before any read.
    const fold: FoldOnly = if (fold_only) workers else null;
    const planned = try planReads(arena, metas, keys, max_memory, allow_headers, allow_union, domain != null, fold);
    const reads: Reads = planned orelse .{ .headers = false, .whole = &.{} };
    const sums = try arena.alloc(u64, keys.len);
    const full = try arena.alloc(bool, keys.len);
    const dict_sizes = try arena.alloc(u64, keys.len);
    const col_sizes = try arena.alloc(u64, keys.len);
    const nulls = try arena.alloc(u64, keys.len);
    @memset(sums, 0);
    @memset(full, true);
    @memset(dict_sizes, 0);
    @memset(col_sizes, 0);
    @memset(nulls, 0);
    var sum_products: u64 = 0;
    // A lone key's largest dictionary: the union holds about that many values at least.
    var widest: u64 = 0;
    var raw_key_bytes: u64 = 0;
    var known = false;
    for (refs.items) |ref| {
        const m = &metas[ref.file];
        const rg = &m.row_groups.items[ref.rg];
        const rows: u64 = @intCast(@max(0, rg.num_rows));
        var product: u64 = 1;
        var covered = true;
        var stats_present = true;
        for (keys, 0..) |key, ki| {
            const ci = key.col_ref.col_idx;
            if (ci >= rg.columns.items.len) {
                full[ki] = false;
                covered = false;
                stats_present = false;
                product = rows;
                sums[ki] +|= rows;
                continue;
            }
            const cm = rg.columns.items[ci].meta_data orelse {
                full[ki] = false;
                covered = false;
                stats_present = false;
                product = rows;
                sums[ki] +|= rows;
                continue;
            };
            stats_present = stats_present and cm.encoding_stats != null;
            col_sizes[ki] +|= @intCast(@max(0, cm.total_compressed_size));
            raw_key_bytes +|= @intCast(@max(0, cm.total_uncompressed_size));
            const flat = cm.path_in_schema.items.len == 1 and m.getColumnLevels(cm.path_in_schema.items).max_rep == 0;
            if (!flat) return stat;
            const covers = fullCoverage(&cm);
            if (dictBytes(cm)) |n| dict_sizes[ki] +|= n else full[ki] = false;
            const n: u64 = blk: {
                const file = files[ref.file];
                const limit = file.headerLimit();
                if (covers and reads.headers) if (dictSlice(file, ref.rg, cm)) |b| if (header(b, limit)) |h| {
                    known = true;
                    widest = @max(widest, @as(u64, @intCast(h.dictionary_page_header.?.num_values)));
                    break :blk @min(rows, @as(u64, @intCast(h.dictionary_page_header.?.num_values)) + nullBound(cm));
                };
                covered = false;
                full[ki] = false;
                break :blk rows;
            };
            sums[ki] +|= n;
            nulls[ki] = @max(nulls[ki], nullBound(cm));
            product = @min(rows, product *| n);
        }
        sum_products +|= @min(rows, product);
        if (covered) stat.covered_rows +|= rows;
        if (stats_present) stat.encoding_stats_rows +|= rows;
    }
    for (dict_sizes, col_sizes) |d, c| {
        stat.dictionary_bytes +|= d;
        stat.column_bytes +|= c;
    }
    stat.key_bytes = @max(@as(u64, @intCast(keys.len)) * 9, raw_key_bytes / @max(1, stat.rows));
    stat.hi = @min(stat.rows, sum_products);
    if (known) {
        stat.source = .dictionary_headers;
        stat.confidence = .upper_bound;
    }
    const total_gate = stat.dictionary_bytes <= max_input or stat.dictionary_bytes <= stat.column_bytes / 20;
    var union_product: u64 = 1;
    for (keys, 0..) |key, ki| {
        // Footer gate plus an absolute ceiling on scratch even when 5% admits a large compressed column.
        const gated = total_gate and full[ki] and
            (dict_sizes[ki] <= max_input or dict_sizes[ki] <= col_sizes[ki] / 20);
        // A key a retained domain can fold (`agg_dense`) takes the union past the cost gate: its scan would otherwise
        // resolve every dictionary entry of every row group itself. Past the gate the union gives up, bound-less as if
        // gated, as soon as its values outgrow a domain, so a large key pays for a few dictionaries at most.
        const retain = if (domain != null and keys.len == 1 and
            (key.col_ref.physical_type == .INT32 or key.col_ref.physical_type == .INT64)) domain else null;
        // The dense fold takes a domain only where every worker's lanes cost no more than the entries they stand in
        // for (`scan.prepareAggregate`): `workers` times the widest dictionary past the entries already rules it out.
        const declined = fold_only and keys.len == 1 and workers *| widest > sums[0] +| refs.items.len;
        const run = full[ki] and ki < reads.whole.len and reads.whole[ki] and !declined;
        if (allow_union and full[ki] and !run and !declined) stat.union_budget_exceeded = true;
        if (run) {
            stat.union_attempted = true;
            const col = key.col_ref.col_idx;
            if (try unionBound(gpa, arena, files, metas, refs.items, col, workers, max_memory, retain, !gated)) |n| {
                sums[ki] = @min(sums[ki], n + nulls[ki]);
                // Multiplying marginal cardinalities does not estimate the number of observed tuples.
                if (keys.len == 1) stat.estimate = @min(stat.rows, n + nulls[ki]);
            } else full[ki] = false;
        } else full[ki] = false;
        union_product *|= sums[ki];
    }
    stat.hi = @min(stat.hi, union_product);
    stat.union_complete = allow_union;
    for (full) |f| stat.union_complete = stat.union_complete and f;
    if (stat.union_complete) {
        stat.source = .dictionary_union;
        stat.confidence = .upper_bound;
    }
    return stat;
}

const P = agg.NUM_PARTS;
const Partition = struct {
    lock: std.atomic.Value(bool) align(64) = .init(false),
    set: agg.DistinctSet = .{},
    /// Retaining: each value's index among this partition's `vals`, in the order workers first inserted them. While
    /// `mapped`, these stand in for `set`; once retention is given up they move into it (`migrate`).
    ids: std.HashMapUnmanaged(u64, u32, IntContext, std.hash_map.default_max_load_percentage) = .empty,
    vals: std.ArrayList(u64) = .empty,
    mapped: bool = false,
    /// Values whose retention is already charged: those the presize paid for, then each one past them.
    charged: usize = 0,

    fn count(p: *const Partition) usize {
        return if (p.mapped) p.vals.items.len else p.set.count();
    }

    /// Under the lock: the values retained so far into `set`, which takes every later one.
    fn migrate(p: *Partition, gpa: std.mem.Allocator) !void {
        if (!p.mapped) return;
        for (p.vals.items) |v| try p.set.insertU64(gpa, v);
        p.mapped = false;
    }
};
const IntContext = struct {
    pub fn hash(_: IntContext, k: u64) u64 {
        return std.hash.int(k);
    }
    pub fn eql(_: IntContext, a: u64, b: u64) bool {
        return a == b;
    }
};
/// A retained translation entry before ids are global: the partition in the top bits, its index below.
const part_shift = 24;
const Shared = struct {
    gpa: std.mem.Allocator,
    files: []const File,
    metas: []const schema.FileMetaData,
    refs: []const Ref,
    col: usize,
    parts: *[P]Partition,
    cursor: std.atomic.Value(usize) = .init(0),
    failed: std.atomic.Value(bool) = .init(false),
    page_limit: usize,
    /// Retaining: one translation per ref (gpa), and whether retention was given up (never the union).
    trans: [][]u32 = &.{},
    dropped: std.atomic.Value(bool) = .init(true),
    retained: std.atomic.Value(usize) = .init(0),
    retain_limit: usize = 0,
    /// Values all partitions hold, and whether passing `agg_dense.max_ids` of them (or dropping retention) fails the
    /// union.
    distinct: std.atomic.Value(usize) = .init(0),
    strict: bool = false,

    fn retaining(s: *const Shared) bool {
        return !s.dropped.load(.monotonic);
    }

    /// Charge `bytes` of retained state; false (and retention dropped) past the limit.
    fn charge(s: *Shared, bytes: usize) bool {
        if (s.retained.fetchAdd(bytes, .monotonic) + bytes <= s.retain_limit) return true;
        return s.drop();
    }

    fn drop(s: *Shared) bool {
        s.dropped.store(true, .monotonic);
        if (s.strict) s.failed.store(true, .monotonic);
        return false;
    }
};
const Worker = struct {
    shared: *Shared,
    oom: bool = false,
    fn run(w: *Worker) void {
        w.runErr() catch |err| {
            w.oom = err == error.OutOfMemory;
            w.shared.failed.store(true, .monotonic);
        };
    }
    fn runErr(w: *Worker) !void {
        const s = w.shared;
        var scratch = std.heap.ArenaAllocator.init(s.gpa);
        defer scratch.deinit();
        while (!s.failed.load(.monotonic)) {
            const i = s.cursor.fetchAdd(1, .monotonic);
            if (i >= s.refs.len) return;
            _ = scratch.reset(.retain_capacity);
            const a = scratch.allocator();
            const ref = s.refs[i];
            const cm = s.metas[ref.file].row_groups.items[ref.rg].columns.items[s.col].meta_data.?;
            const bytes = dictSlice(s.files[ref.file], ref.rg, cm) orelse return error.BadDictionary;
            const h = header(bytes, s.files[ref.file].headerLimit()) orelse return error.BadDictionary;
            const n: usize = @intCast(h.dictionary_page_header.?.num_values);
            // Payload plus value views and scatter ids are bounded before any decompression/allocation.
            if (@as(u64, @intCast(h.uncompressed_page_size)) +| (@as(u64, n) *| 32) > s.page_limit)
                return error.ProbeBudget;
            var reader = page.PageReader.init(bytes, cm.codec, a);
            const pg = (try reader.next()) orelse return error.BadDictionary;
            if (cm.type != .INT32 and cm.type != .INT64 and cm.type != .BYTE_ARRAY) return error.UnsupportedDictionary;
            const ints: []u64 = if (cm.type != .BYTE_ARRAY) try a.alloc(u64, n) else &.{};
            const strs: [][]const u8 = if (cm.type == .BYTE_ARRAY) try a.alloc([]const u8, n) else &.{};
            if (cm.type == .INT32) {
                const tmp = try a.alloc(i32, n);
                var d = plain.Decoder(i32).init(pg.bytes);
                if (try d.decode(tmp) != n) return error.BadDictionary;
                for (tmp, ints) |v, *out| out.* = @bitCast(@as(i64, v));
            } else if (cm.type == .INT64) {
                var d = plain.Decoder(i64).init(pg.bytes);
                if (try d.decode(@ptrCast(ints)) != n) return error.BadDictionary;
            } else {
                var d = plain.ByteArrayDecoder.init(pg.bytes);
                if (try d.decode(strs) != n) return error.BadDictionary;
            }
            // Retaining: a translation per entry, plus the null id, kept beyond this dictionary.
            var packed_ids: []u32 = &.{};
            if (ints.len > 0 and s.retaining() and s.charge((n + 1) * @sizeOf(u32))) {
                packed_ids = try s.gpa.alloc(u32, n + 1);
                s.trans[i] = packed_ids;
            }
            const routes = try a.alloc(u8, n);
            var counts: [P]usize = @splat(0);
            for (routes, 0..) |*r, j| {
                const hash = if (ints.len > 0)
                    std.hash.Wyhash.hash(0, std.mem.asBytes(&ints[j]))
                else
                    std.hash.Wyhash.hash(0, strs[j]);
                r.* = @intCast((hash >> 32) % P);
                counts[r.*] += 1;
            }
            var starts: [P + 1]usize = undefined;
            starts[0] = 0;
            for (counts, 0..) |c, p| starts[p + 1] = starts[p] + c;
            var at = starts;
            const ids = try a.alloc(u32, n);
            for (routes, 0..) |r, j| {
                ids[at[r]] = @intCast(j);
                at[r] += 1;
            }
            // Workers that walked the partitions in one order queued behind each other on every lock. Start each
            // dictionary at its own partition and pass over a held lock to come back to it.
            var done: [P]bool = @splat(false);
            var left: usize = P;
            var pi: usize = i % P;
            while (left > 0) : (pi = (pi + 1) % P) {
                if (done[pi]) continue;
                const p = &s.parts[pi];
                if (p.lock.cmpxchgWeak(false, true, .acquire, .monotonic) != null) {
                    std.atomic.spinLoopHint();
                    continue;
                }
                defer p.lock.store(false, .release);
                done[pi] = true;
                left -= 1;
                const entries = ids[starts[pi]..starts[pi + 1]];
                if (p.mapped and !(packed_ids.len > 0 and s.retaining() and
                    retainRun(s, p, @intCast(pi), ints, entries, packed_ids))) try p.migrate(s.gpa);
                if (!p.mapped) for (entries) |j| {
                    if (ints.len > 0) try p.set.insertU64(s.gpa, ints[j]) else try p.set.insertBytes(s.gpa, strs[j]);
                };
            }
        }
    }
};

/// Under partition `p`'s lock: give each of a dictionary's entries `js` in it its index there, the next free one for
/// a value not seen before. False, retention given up, on any failure: `vals` then holds every value inserted so far,
/// so `migrate` loses none and the union stays exact.
fn retainRun(s: *Shared, p: *Partition, pi: u32, ints: []const u64, js: []const u32, packed_ids: []u32) bool {
    const before = p.vals.items.len;
    for (js) |j| {
        const gop = p.ids.getOrPut(s.gpa, ints[j]) catch return s.drop();
        if (!gop.found_existing) {
            if (p.vals.items.len >= agg_dense.max_ids) {
                p.ids.removeByPtr(gop.key_ptr);
                return s.drop();
            }
            p.vals.append(s.gpa, ints[j]) catch {
                p.ids.removeByPtr(gop.key_ptr);
                return s.drop();
            };
            gop.value_ptr.* = @intCast(p.vals.items.len - 1);
        }
        packed_ids[j] = (pi << part_shift) | gop.value_ptr.*;
    }
    const added = p.vals.items.len - before;
    if (s.distinct.fetchAdd(added, .monotonic) + added >= agg_dense.max_ids) return s.drop();
    // A map entry and its value, kept on the high side; the presized share is paid already.
    const over = p.vals.items.len -| p.charged;
    p.charged += over;
    return s.charge(over * 32);
}

fn unionBound(
    gpa: std.mem.Allocator,
    arena: std.mem.Allocator,
    files: []const File,
    metas: []const schema.FileMetaData,
    refs: []const Ref,
    col: usize,
    workers: usize,
    max_memory: usize,
    retain: ?*?agg_dense.Domain,
    /// Past the cost gate: fail (no bound) rather than outgrow a domain or give retention up.
    strict: bool,
) !?u64 {
    // Binding permits schema evolution across inputs. Raw dictionary images only union safely when their
    // physical/logical interpretation matches; otherwise the sum of per-RG bounds remains valid.
    var page_need: u64 = 1;
    var max_entries: u64 = 0;
    if (refs.len > 0) {
        const first = refs[0];
        const cm0 = metas[first.file].row_groups.items[first.rg].columns.items[col].meta_data.?;
        const se0 = metas[first.file].getColumnSchema(cm0.path_in_schema.items) orelse return null;
        for (refs) |ref| {
            const cm = metas[ref.file].row_groups.items[ref.rg].columns.items[col].meta_data.?;
            const se = metas[ref.file].getColumnSchema(cm.path_in_schema.items) orelse return null;
            const bytes = dictSlice(files[ref.file], ref.rg, cm) orelse return null;
            const h = header(bytes, files[ref.file].headerLimit()) orelse return null;
            page_need = @max(page_need, @as(u64, @intCast(h.uncompressed_page_size)) +|
                (@as(u64, @intCast(h.dictionary_page_header.?.num_values)) *| 32));
            max_entries = @max(max_entries, @as(u64, @intCast(h.dictionary_page_header.?.num_values)));
            if (cm.type != cm0.type or se.converted_type != se0.converted_type or
                !std.meta.eql(se.logical_type, se0.logical_type) or se.scale != se0.scale or
                se.precision != se0.precision or se.type_length != se0.type_length) return null;
        }
    }
    const scratch_budget = max_memory / 8;
    if (page_need > @min(16 * 1024 * 1024, scratch_budget)) return null;
    // Lower probe parallelism when a dictionary is large; a small query budget must not unnecessarily lose a
    // useful bound just because the scan itself uses more workers.
    const by_scratch: usize = @intCast(scratch_budget / page_need);
    const n_workers = @max(1, @min(@min(@min(workers, refs.len), 16), by_scratch));
    var budget: agg.SharedBudget = .{ .limit = max_memory / 16, .block = 4096 };
    var parts: [P]Partition = undefined;
    for (&parts) |*p| p.* = .{ .set = .{ .shared = &budget } };
    defer for (&parts) |*p| {
        p.set.deinit(gpa);
        p.ids.deinit(gpa);
        p.vals.deinit(gpa);
    };
    var shared: Shared = .{
        .gpa = gpa,
        .files = files,
        .metas = metas,
        .refs = refs,
        .col = col,
        .parts = &parts,
        .page_limit = @min(16 * 1024 * 1024, max_memory / 8 / n_workers),
    };
    const int_key = refs.len > 0 and switch (metas[refs[0].file].row_groups.items[refs[0].rg].columns.items[col].meta_data.?.type) {
        .INT32, .INT64 => true,
        else => false,
    };
    if (strict and (retain == null or !int_key)) return null;
    if (retain != null and int_key) retain: {
        shared.retain_limit = max_memory / 16;
        shared.strict = strict;
        // The translations' index, and each file's first row group in it, charged before either exists.
        if (!shared.charge(refs.len * @sizeOf([]u32) + metas.len * @sizeOf(usize))) {
            if (strict) return null;
            break :retain;
        }
        shared.trans = try gpa.alloc([]u32, refs.len);
        @memset(shared.trans, &.{});
        shared.dropped = .init(false);
        // The largest dictionary is a floor on the domain: its share of it, sized once, spares each map its growth.
        const each: u32 = @intCast(@min(max_entries, agg_dense.max_ids) / P + 64);
        for (&parts) |*p| {
            p.mapped = true;
            if (!shared.charge(@as(usize, each) * 32)) break;
            p.charged = each;
            p.ids.ensureTotalCapacity(gpa, each) catch {
                _ = shared.drop();
                break;
            };
            p.vals.ensureTotalCapacity(gpa, each) catch {
                _ = shared.drop();
                break;
            };
        }
    }
    defer if (shared.trans.len > 0) {
        for (shared.trans) |t| gpa.free(t);
        gpa.free(shared.trans);
    };
    const ws = try arena.alloc(Worker, n_workers);
    for (ws) |*w| w.* = .{ .shared = &shared };
    if (ws.len == 1 or !spawn.runTree(Worker, ws, .{ .stack_size = 1 << 20 }, Worker.run)) Worker.run(&ws[0]);
    for (ws) |w| if (w.oom) return error.OutOfMemory;
    if (shared.failed.load(.monotonic)) return null;
    var count: u64 = 0;
    for (&parts) |*p| count +|= p.count();
    const kept = for (&parts) |*p| {
        if (!p.mapped) break false;
    } else true;
    if (shared.trans.len > 0 and kept and shared.retaining() and count < agg_dense.max_ids) {
        retain.?.* = try buildDomain(gpa, metas, refs, col, &parts, shared.trans, @intCast(count));
        shared.trans = &.{};
    }
    return count;
}

/// The retained union as a domain: partitions laid end to end give the global ids, and each translation's packed
/// (partition, index) entries become those ids. Takes the translations over; on failure frees nothing it was given.
fn buildDomain(
    gpa: std.mem.Allocator,
    metas: []const schema.FileMetaData,
    refs: []const Ref,
    col: usize,
    parts: *const [P]Partition,
    trans: [][]u32,
    count: usize,
) !agg_dense.Domain {
    var base: [P]u32 = undefined;
    var at: u32 = 0;
    for (parts, &base) |*p, *b| {
        b.* = at;
        at += @intCast(p.vals.items.len);
    }
    std.debug.assert(at == count);
    const values = try gpa.alloc(u64, count);
    errdefer gpa.free(values);
    for (parts, base) |*p, b| @memcpy(values[b..][0..p.vals.items.len], p.vals.items);
    const file_base = try gpa.alloc(usize, metas.len);
    errdefer gpa.free(file_base);
    var at_ref: usize = 0;
    for (metas, file_base) |m, *fb| {
        fb.* = at_ref;
        at_ref += m.row_groups.items.len;
    }
    std.debug.assert(at_ref == refs.len);
    var bytes = values.len * @sizeOf(u64) + trans.len * @sizeOf([]u32) + file_base.len * @sizeOf(usize);
    for (trans, 0..) |_, i| {
        const t = trans[i];
        if (t.len == 0) continue;
        for (t[0 .. t.len - 1]) |*e| e.* = base[e.* >> part_shift] + (e.* & ((1 << part_shift) - 1));
        t[t.len - 1] = @intCast(count);
        bytes += t.len * @sizeOf(u32);
    }
    const int32 = metas[refs[0].file].row_groups.items[refs[0].rg].columns.items[col].meta_data.?.type == .INT32;
    return .{ .values = values, .int32 = int32, .trans = trans, .file_base = file_base, .bytes = bytes };
}

test "ndv: repeated dictionaries tighten an upper bound, including nulls and composites" {
    const a = std.testing.allocator;
    const metadata = @import("parquet/metadata.zig");
    const bytes = try metadata.readFileSlice("ci/fixtures/parquet/topk_skew.parquet", a);
    defer a.free(bytes);
    var state = std.heap.ArenaAllocator.init(a);
    defer state.deinit();
    const arena = state.allocator();
    const meta = try metadata.open(arena, bytes);
    const files = [_]File{.{ .bytes = bytes, .local = true }};
    const keys = [_]expr.Expr{
        .{ .col_ref = .{ .col_idx = 0, .physical_type = .BYTE_ARRAY, .expr_type = .str } },
        .{ .col_ref = .{ .col_idx = 1, .physical_type = .INT64, .expr_type = .i64 } },
    };
    const headers = try collect(a, arena, &files, &.{meta}, keys[0..1], 4, 512 << 20, false);
    try std.testing.expectEqual(.dictionary_headers, headers.source);
    try std.testing.expectEqual(headers.rows, headers.covered_rows);
    for ([_]usize{ 1, 4 }) |workers| {
        spawn.injectFailureAfter(0);
        defer spawn.resetFailure();
        const unioned = try collect(a, arena, &files, &.{meta}, keys[0..1], workers, 512 << 20, true);
        try std.testing.expectEqual(.dictionary_union, unioned.source);
        try std.testing.expectEqual(@as(u64, 9295), unioned.hi);
        try std.testing.expectEqual(@as(?u64, 9295), unioned.estimate);
        try std.testing.expect(unioned.hi < headers.hi);
        try std.testing.expectEqual(@as(u64, 0), unioned.lo); // Unused dictionary entries cannot prove a lower bound.
        if (workers > 1) try std.testing.expect(spawn.failuresInjected() > 0);
    }
    const composite = try collect(a, arena, &files, &.{meta}, &keys, 4, 512 << 20, true);
    try std.testing.expect(composite.hi >= 9295 and composite.hi <= composite.rows);
    try std.testing.expect(composite.estimate == null);
}

test "ndv: partial coverage, missing null counts, unavailable pages and probe budgets stay conservative" {
    const a = std.testing.allocator;
    const metadata = @import("parquet/metadata.zig");
    const bytes = try metadata.readFileSlice("ci/fixtures/parquet/topk_skew.parquet", a);
    defer a.free(bytes);
    var state = std.heap.ArenaAllocator.init(a);
    defer state.deinit();
    const arena = state.allocator();
    var meta = try metadata.open(arena, bytes);
    const keys = [_]expr.Expr{.{ .col_ref = .{ .col_idx = 1, .physical_type = .INT64, .expr_type = .i64 } }};
    const local = [_]File{.{ .bytes = bytes, .local = true }};
    const budgeted = try collect(a, arena, &local, &.{meta}, &keys, 4, 1024 * 1024, true);
    try std.testing.expect(budgeted.union_attempted and !budgeted.union_complete);
    const remote = [_]File{.{ .bytes = &.{}, .local = false }};
    const unavailable = try collect(a, arena, &remote, &.{meta}, &keys, 4, 512 << 20, true);
    try std.testing.expectEqual(.unknown, unavailable.source);
    try std.testing.expectEqual(unavailable.rows, unavailable.hi);
    try std.testing.expect(!unavailable.union_attempted);
    meta.row_groups.items[0].columns.items[1].meta_data.?.encoding_stats = null;
    const partial = try collect(a, arena, &local, &.{meta}, &keys, 4, 512 << 20, true);
    try std.testing.expect(!partial.union_attempted);
    try std.testing.expectEqual(partial.rows - 1250, partial.covered_rows);
    try std.testing.expect(partial.hi >= 9295);
    const cm = &meta.row_groups.items[1].columns.items[1].meta_data.?;
    cm.statistics.?.null_count = null;
    try std.testing.expectEqual(@as(u64, 1), nullBound(cm.*));
    cm.statistics.?.null_count = 0;
    try std.testing.expectEqual(@as(u64, 0), nullBound(cm.*));
    cm.encoding_stats = null;
    try std.testing.expect(!fullCoverage(cm));
}

test "ndv: allocation failures release dictionary scratch and the shared set" {
    const a = std.testing.allocator;
    const metadata = @import("parquet/metadata.zig");
    const bytes = try metadata.readFileSlice("ci/fixtures/parquet/topk_skew.parquet", a);
    defer a.free(bytes);
    var state = std.heap.ArenaAllocator.init(a);
    defer state.deinit();
    var meta = try metadata.open(state.allocator(), bytes);
    meta.row_groups.items = meta.row_groups.items[0..1];
    const files = [_]File{.{ .bytes = bytes, .local = true }};
    try std.testing.checkAllAllocationFailures(a, struct {
        fn run(gpa: std.mem.Allocator, fs: []const File, ms: []const schema.FileMetaData) !void {
            // Arena growth depends on whether a backing allocation can resize in place. Force allocate/copy so
            // failure indices cover a deterministic sequence, while every owned byte still reaches the tracker.
            var backing = gpa;
            const mem: std.mem.Allocator = .{ .ptr = &backing, .vtable = &.{
                .alloc = NoResize.alloc,
                .resize = NoResize.resize,
                .remap = NoResize.remap,
                .free = NoResize.free,
            } };
            var scratch = std.heap.ArenaAllocator.init(mem);
            defer scratch.deinit();
            const keys = [_]expr.Expr{.{ .col_ref = .{
                .col_idx = 0,
                .physical_type = .BYTE_ARRAY,
                .expr_type = .str,
            } }};
            const stat = try collect(mem, scratch.allocator(), fs, ms, &keys, 1, 512 << 20, true);
            try std.testing.expect(stat.union_complete and stat.hi > 1);
        }
    }.run, .{ &files, @as([]const schema.FileMetaData, &.{meta}) });
}

const NoResize = struct {
    fn alloc(ctx: *anyopaque, len: usize, alignment: std.mem.Alignment, ra: usize) ?[*]u8 {
        const a: *std.mem.Allocator = @ptrCast(@alignCast(ctx));
        return a.rawAlloc(len, alignment, ra);
    }
    fn resize(_: *anyopaque, _: []u8, _: std.mem.Alignment, _: usize, _: usize) bool {
        return false;
    }
    fn remap(_: *anyopaque, _: []u8, _: std.mem.Alignment, _: usize, _: usize) ?[*]u8 {
        return null;
    }
    fn free(ctx: *anyopaque, bytes: []u8, alignment: std.mem.Alignment, ra: usize) void {
        const a: *std.mem.Allocator = @ptrCast(@alignCast(ctx));
        a.rawFree(bytes, alignment, ra);
    }
};

test "ndv: schema evolution cannot union incompatible raw dictionary images" {
    const a = std.testing.allocator;
    const metadata = @import("parquet/metadata.zig");
    const bytes = try metadata.readFileSlice("ci/fixtures/parquet/topk_skew.parquet", a);
    defer a.free(bytes);
    var state = std.heap.ArenaAllocator.init(a);
    defer state.deinit();
    const arena = state.allocator();
    const first = try metadata.open(arena, bytes);
    var second = try metadata.open(arena, bytes);
    for (second.row_groups.items) |*rg| rg.columns.items[1].meta_data.?.type = .INT32;
    second.schema.items[2].type = .INT32;
    const files = [_]File{ .{ .bytes = bytes, .local = true }, .{ .bytes = bytes, .local = true } };
    const keys = [_]expr.Expr{.{ .col_ref = .{ .col_idx = 1, .physical_type = .INT64, .expr_type = .i64 } }};
    const stat = try collect(a, arena, &files, &.{ first, second }, &keys, 4, 512 << 20, true);
    try std.testing.expect(stat.union_attempted and !stat.union_complete);
    try std.testing.expectEqual(.dictionary_headers, stat.source);
    try std.testing.expect(stat.hi >= 9295);
}

test "ndv: dictionary headers accept the execution reader's legacy PLAIN_DICTIONARY label" {
    const a = std.testing.allocator;
    for ([_]schema.Encoding{ .PLAIN, .PLAIN_DICTIONARY }) |encoding| {
        var writer = thrift.Writer.init(a);
        defer writer.deinit();
        const h: schema.PageHeader = .{
            .type = .DICTIONARY_PAGE,
            .crc = null,
            .uncompressed_page_size = 0,
            .compressed_page_size = 0,
            .data_page_header = null,
            .data_page_header_v2 = null,
            .dictionary_page_header = .{ .num_values = 0, .encoding = encoding, .is_sorted = null },
        };
        try h.write(&writer);
        try std.testing.expect(header(writer.bytes(), map_prefix) != null);
    }
}

test "ndv: a buffer holding only the probed bytes gives the facts the whole file gives, at any budget" {
    const a = std.testing.allocator;
    const metadata = @import("parquet/metadata.zig");
    const bytes = try metadata.readFileSlice("ci/fixtures/parquet/topk_skew.parquet", a);
    defer a.free(bytes);
    var state = std.heap.ArenaAllocator.init(a);
    defer state.deinit();
    const arena = state.allocator();
    const meta = try metadata.open(arena, bytes);
    const keys = [_]expr.Expr{
        .{ .col_ref = .{ .col_idx = 0, .physical_type = .BYTE_ARRAY, .expr_type = .str } },
        .{ .col_ref = .{ .col_idx = 1, .physical_type = .INT64, .expr_type = .i64 } },
    };
    const Waits = struct {
        n: usize = 0,
        fn wait(ctx: *anyopaque, _: usize) anyerror!void {
            const self: *@This() = @ptrCast(@alignCast(ctx));
            self.n += 1;
        }
    };
    const probed = try a.alloc(u8, bytes.len);
    defer a.free(probed);
    // From a budget that refuses every union's pages, through one key's, to every whole page.
    for ([_]usize{ 16 * 4096, 1 << 20, 4 << 20, 512 << 20 }) |max_memory| for ([_]bool{ false, true }) |headers| {
        for ([_]bool{ false, true }) |allow_union| for ([_]usize{ 1, 2 }) |n_keys| {
            const ks = keys[0..n_keys];
            @memset(probed, 0);
            const ranges = try probeRanges(arena, &.{meta}, ks, max_memory, headers, allow_union, false, null);
            for (ranges) |r| @memcpy(probed[r.start..r.end], bytes[r.start..r.end]);
            if (!headers) try std.testing.expectEqual(@as(usize, 0), ranges.len);
            var waits: Waits = .{};
            const whole = [_]File{.{ .bytes = bytes, .local = true }};
            const wait: Wait = .{ .ctx = &waits, .wait = Waits.wait };
            const part = [_]File{.{ .bytes = probed, .local = true, .probed = true, .wait = wait }};
            const mm = max_memory;
            const want = try collectRetaining(a, arena, &whole, &.{meta}, ks, 4, mm, headers, allow_union, null, false);
            const got = try collectRetaining(a, arena, &part, &.{meta}, ks, 4, mm, headers, allow_union, null, false);
            try std.testing.expectEqualDeep(want, got);
            // Every read waited for the probe, and none was made without one.
            try std.testing.expectEqual(ranges.len == 0, waits.n == 0);
        };
    };
}

test "ndv: a union wanted for the dense fold alone is skipped where the headers rule the fold out" {
    const a = std.testing.allocator;
    const metadata = @import("parquet/metadata.zig");
    const bytes = try metadata.readFileSlice("ci/fixtures/parquet/topk_skew.parquet", a);
    defer a.free(bytes);
    var state = std.heap.ArenaAllocator.init(a);
    defer state.deinit();
    const arena = state.allocator();
    const meta = try metadata.open(arena, bytes);
    const keys = [_]expr.Expr{.{ .col_ref = .{ .col_idx = 1, .physical_type = .INT64, .expr_type = .i64 } }};
    const files = [_]File{.{ .bytes = bytes, .local = true }};
    // One worker's lanes never cost more than the entries; ten thousand workers' always do.
    for ([_]usize{ 1, 10_000 }) |workers| {
        var domain: ?agg_dense.Domain = null;
        defer if (domain) |*d| d.deinit(a);
        const ms = &.{meta};
        const stat = try collectRetaining(a, arena, &files, ms, &keys, workers, 512 << 20, true, true, &domain, true);
        try std.testing.expectEqual(workers == 1, stat.union_attempted);
        try std.testing.expect(!stat.union_budget_exceeded);
        try std.testing.expectEqual(.dictionary_headers, if (workers == 1) .dictionary_headers else stat.source);
    }
}

test "ndv: probe ranges allocate only from the arena they are given" {
    const a = std.testing.allocator;
    const metadata = @import("parquet/metadata.zig");
    const bytes = try metadata.readFileSlice("ci/fixtures/parquet/topk_skew.parquet", a);
    defer a.free(bytes);
    var state = std.heap.ArenaAllocator.init(a);
    defer state.deinit();
    const meta = try metadata.open(state.allocator(), bytes);
    try std.testing.checkAllAllocationFailures(a, struct {
        fn run(gpa: std.mem.Allocator, m: *const schema.FileMetaData) !void {
            var arena = std.heap.ArenaAllocator.init(gpa);
            defer arena.deinit();
            const keys = [_]expr.Expr{.{ .col_ref = .{ .col_idx = 1, .physical_type = .INT64, .expr_type = .i64 } }};
            _ = try probeRanges(arena.allocator(), &.{m.*}, &keys, 512 << 20, true, true, true, null);
        }
    }.run, .{&meta});
}

test "ndv: an empty input has a zero upper bound" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const keys = [_]expr.Expr{.{ .col_ref = .{ .col_idx = 0, .physical_type = .INT64, .expr_type = .i64 } }};
    const stat = try collect(std.testing.allocator, arena.allocator(), &.{}, &.{}, &keys, 1, 512 << 20, true);
    try std.testing.expectEqual(@as(u64, 0), stat.hi);
    try std.testing.expectEqual(.upper_bound, stat.confidence);
}

test "ndv: a retained integer domain translates every dictionary entry to a distinct exact value" {
    const a = std.testing.allocator;
    const metadata = @import("parquet/metadata.zig");
    const bytes = try metadata.readFileSlice("ci/fixtures/parquet/topk_skew.parquet", a);
    defer a.free(bytes);
    var state = std.heap.ArenaAllocator.init(a);
    defer state.deinit();
    const arena = state.allocator();
    const meta = try metadata.open(arena, bytes);
    const files = [_]File{.{ .bytes = bytes, .local = true }};
    const keys = [_]expr.Expr{.{ .col_ref = .{ .col_idx = 1, .physical_type = .INT64, .expr_type = .i64 } }};
    for ([_]usize{ 1, 4 }) |workers| {
        var domain: ?agg_dense.Domain = null;
        defer if (domain) |*d| d.deinit(a);
        const ms = &.{meta};
        const stat = try collectRetaining(a, arena, &files, ms, &keys, workers, 512 << 20, true, true, &domain, false);
        const bound = try collect(a, arena, &files, &.{meta}, &keys, workers, 512 << 20, true);
        try std.testing.expectEqual(bound.hi, stat.hi); // Retention never moves the bound.
        const d = &(domain orelse return error.TestExpectedEqual);
        try std.testing.expectEqual(stat.estimate.?, d.values.len + 1); // the key holds nulls
        const seen = try arena.alloc(bool, d.values.len);
        @memset(seen, false);
        for (0..meta.row_groups.items.len) |rg| {
            const t = d.forRowGroup(0, rg) orelse return error.TestExpectedEqual;
            try std.testing.expectEqual(@as(u32, @intCast(d.values.len)), t[t.len - 1]);
            for (t[0 .. t.len - 1]) |id| seen[id] = true;
        }
        for (seen) |s| try std.testing.expect(s);
        try std.testing.expect(d.forRowGroup(0, meta.row_groups.items.len) == null);
        // Everything the domain owns is in the bytes the scan charges, its indexing arrays included.
        try std.testing.expectEqual(d.values.len * 8 + d.entries() * 4 + d.trans.len * @sizeOf([]u32) +
            d.file_base.len * @sizeOf(usize), d.bytes);
        try std.testing.expect(d.forRowGroup(1, 0) == null);
        var uniq: std.AutoHashMapUnmanaged(u64, void) = .empty;
        for (d.values) |v| try uniq.put(arena, v, {});
        try std.testing.expectEqual(d.values.len, uniq.count());
    }
    // Retention costs what the domain holds, its presized map shares charged once: a budget of exactly that keeps it.
    {
        var full: ?agg_dense.Domain = null;
        _ = try collectRetaining(a, arena, &files, &.{meta}, &keys, 4, 512 << 20, true, true, &full, false);
        const d = &full.?;
        const need = d.values.len * 32 + d.entries() * 4 + d.trans.len * @sizeOf([]u32) + d.file_base.len * @sizeOf(usize);
        d.deinit(a);
        var exact: ?agg_dense.Domain = null;
        defer if (exact) |*e| e.deinit(a);
        _ = try collectRetaining(a, arena, &files, &.{meta}, &keys, 4, need * 16, true, true, &exact, false);
        try std.testing.expect(exact != null);
    }
    // Strings and composites keep no domain; a retention budget too small drops it, not the bound.
    var none: ?agg_dense.Domain = null;
    const str_key = [_]expr.Expr{.{ .col_ref = .{ .col_idx = 0, .physical_type = .BYTE_ARRAY, .expr_type = .str } }};
    _ = try collectRetaining(a, arena, &files, &.{meta}, &str_key, 4, 512 << 20, true, true, &none, false);
    try std.testing.expect(none == null);
    const pair = [_]expr.Expr{ keys[0], keys[0] };
    _ = try collectRetaining(a, arena, &files, &.{meta}, &pair, 4, 512 << 20, true, true, &none, false);
    try std.testing.expect(none == null);
    const tight = try collectRetaining(a, arena, &files, &.{meta}, &keys, 4, 4 << 20, true, true, &none, false);
    try std.testing.expect(tight.union_complete);
    try std.testing.expect(none == null);
    // Retention given up part way moves what it held into the union's sets: the same exact count.
    try std.testing.expectEqual((try collect(a, arena, &files, &.{meta}, &keys, 4, 4 << 20, true)).hi, tight.hi);
    // Past the cost gate (strict) the union only runs to retain: given up, it gives no bound at all.
    var refs: std.ArrayList(Ref) = .empty;
    for (0..meta.row_groups.items.len) |rg| try refs.append(arena, .{ .file = 0, .rg = rg });
    try std.testing.expect(try unionBound(a, arena, &files, &.{meta}, refs.items, 1, 4, 4 << 20, &none, true) == null);
    try std.testing.expect(try unionBound(a, arena, &files, &.{meta}, refs.items, 1, 4, 4 << 20, &none, false) != null);
    try std.testing.expect(try unionBound(a, arena, &files, &.{meta}, refs.items, 1, 4, 512 << 20, null, true) == null);
    const strict = try unionBound(a, arena, &files, &.{meta}, refs.items, 1, 4, 512 << 20, &none, true);
    defer if (none) |*d| d.deinit(a);
    try std.testing.expectEqual(try unionBound(a, arena, &files, &.{meta}, refs.items, 1, 4, 512 << 20, null, false), strict);
    try std.testing.expect(none != null);
}

test "ndv: allocation failures while retaining a domain leak nothing and never fail the bound" {
    const a = std.testing.allocator;
    const metadata = @import("parquet/metadata.zig");
    const bytes = try metadata.readFileSlice("ci/fixtures/parquet/topk_skew.parquet", a);
    defer a.free(bytes);
    var state = std.heap.ArenaAllocator.init(a);
    defer state.deinit();
    var meta = try metadata.open(state.allocator(), bytes);
    meta.row_groups.items = meta.row_groups.items[0..2];
    const files = [_]File{.{ .bytes = bytes, .local = true }};
    const keys = [_]expr.Expr{.{ .col_ref = .{ .col_idx = 1, .physical_type = .INT64, .expr_type = .i64 } }};
    // Retention gives itself up on a failed allocation rather than fail the union, which `checkAllAllocationFailures`
    // counts as swallowed: so fail each allocation in turn by hand and accept either outcome, but no leak.
    var i: usize = 0;
    while (true) : (i += 1) {
        var failing = std.testing.FailingAllocator.init(a, .{ .fail_index = i });
        const gpa = failing.allocator();
        var scratch = std.heap.ArenaAllocator.init(gpa);
        defer scratch.deinit();
        var domain: ?agg_dense.Domain = null;
        defer if (domain) |*d| d.deinit(gpa);
        const sa = scratch.allocator();
        if (collectRetaining(gpa, sa, &files, &.{meta}, &keys, 1, 512 << 20, true, true, &domain, false)) |stat| {
            try std.testing.expect(stat.union_complete);
        } else |err| try std.testing.expectEqual(error.OutOfMemory, err);
        if (!failing.has_induced_failure) {
            try std.testing.expect(domain != null);
            break;
        }
    }
}
