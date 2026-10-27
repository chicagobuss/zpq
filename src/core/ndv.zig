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
const statistics = @import("parquet/statistics.zig");
const KeyMap = @import("bytes.zig").KeyHashMapUnmanaged;

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
    /// Dictionary strings correct the existing width hint for run admission. Other components keep their hint:
    /// prepaying more for them can crowd out decode. Growth must still draw actual room for skew or later keys.
    run_key_bytes: u64 = 0,
    /// Largest row group's string dictionary footprint while a sliced reader retains its decoded copy.
    run_dictionary_peak: u64 = 0,
    string_keys: bool = false,
    union_attempted: bool = false,
    union_complete: bool = false,
    union_budget_exceeded: bool = false,
    /// The union was wanted for the dense fold alone, and the footers or headers say the fold would decline it.
    fold_declined: bool = false,
    /// A lone key's dictionary entries summed over row groups (one more where nulls may be, at most the row group's
    /// rows), when every row group is covered: from the headers, and what grouping by codes resolves, entry by entry.
    /// Null otherwise.
    entries: ?u64 = null,
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
        const retain = retain_wanted and (key.col_ref.physical_type == .INT32 or
            key.col_ref.physical_type == .INT64 or key.col_ref.physical_type == .BYTE_ARRAY);
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

/// A guess from the footers alone at a lone key's dictionary entries summed over row groups: each dictionary's stored
/// bytes over its narrowest entry (4 bytes for INT32 and byte arrays, 8 for INT64). Compression can make it low, so
/// it only decides whether a union is worth reading (`scan.prepareAggregate`), the same in every read mode; null
/// unless every row group's chunk is dictionary-encoded throughout.
pub fn footerEntries(metas: []const schema.FileMetaData, keys: []const expr.Expr) ?u64 {
    if (keys.len != 1 or keys[0] != .col_ref) return null;
    const col = keys[0].col_ref.col_idx;
    var n: u64 = 0;
    for (metas) |*m| for (m.row_groups.items) |*rg| {
        if (col >= rg.columns.items.len) return null;
        const cm = rg.columns.items[col].meta_data orelse return null;
        if (!fullCoverage(&cm)) return null;
        const len = dictBytes(cm) orelse return null;
        n +|= len / @as(u64, if (cm.type == .INT64) 8 else 4);
    };
    return n;
}

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

fn nullBound(rg: *const schema.RowGroup, ci: usize, m: *const schema.FileMetaData) u64 {
    // A missing or uncorroborated null_count is not zero. One extra entry is always safe for an optional flat key.
    return if (statistics.chunkStats(rg, ci, m, .corroborated).null_count == 0) 0 else 1;
}

fn dictionaryDecodePeak(cm: schema.ColumnMetaData, h: schema.PageHeader) u64 {
    if (cm.type != .BYTE_ARRAY) return 0;
    const d = h.dictionary_page_header orelse return 0;
    const entries: u64 = @intCast(@max(0, d.num_values));
    const bytes: u64 = @intCast(@max(0, h.uncompressed_page_size));
    // SliceReader.holdDictionary copies the strings and slice array before its page arena can reset.
    return (bytes +| entries *| @sizeOf([]const u8)) *| 2;
}

fn dictionaryRunExtra(cm: schema.ColumnMetaData, rows: u64, h: schema.PageHeader) u64 {
    if (cm.type != .BYTE_ARRAY) return 0;
    const n: u64 = @intCast(h.dictionary_page_header.?.num_values);
    if (n == 0) return 0;
    const bytes: u64 = @intCast(h.uncompressed_page_size);
    // PLAIN entries already include the u32 string length; serialized keys add a presence byte.
    const width = bytes / n + @intFromBool(bytes % n != 0) + 1;
    const raw: u64 = @intCast(@max(0, cm.total_uncompressed_size));
    return rows *| width -| raw;
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
    return collectRetaining(gpa, arena, files, metas, keys, workers, max_memory, true, allow_union, null, .{});
}

/// `collectRetaining`'s union: `fold_only` wants it only for the dense fold's domain, so skips it where the footers,
/// or then the headers, show the fold would decline it; `limit` where it decides one choice only.
pub const UnionOpts = struct { fold_only: bool = false, limit: ?UnionLimit = null };

/// A union a lone key's plan reads for one choice: the choice needs at most `entries` dictionary entries in all, and a
/// union of at most `values` values or an eighth (`recur`) of the entries. Past either the union cannot change it, so
/// it is not read, or stops, bound-less, as soon as it holds more values (`scan.fewEntries`).
pub const UnionLimit = struct { entries: u64, values: u64, recur: u64 };

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
    union_opts: UnionOpts,
) !Stat {
    const fold_only = union_opts.fold_only;
    var stat: Stat = .{};
    var refs: std.ArrayList(Ref) = .empty;
    for (metas, 0..) |m, fi| for (m.row_groups.items, 0..) |rg, ri| {
        stat.rows +|= @intCast(@max(0, rg.num_rows));
        try refs.append(arena, .{ .file = fi, .rg = ri });
    };
    stat.hi = stat.rows;
    if (keys.len == 0) return stat;
    // Every key's facts first, then the return for a computed one: stopping at the first made the stat depend on
    // the keys' order (`UserID, extract(...), SearchPhrase` never saw its string key).
    for (keys) |key| stat.string_keys = stat.string_keys or key.typeOf() == .str;
    for (keys) |key| if (key != .col_ref) return stat;
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
    var run_key_extra: u64 = 0;
    var known = false;
    for (refs.items) |ref| {
        const m = &metas[ref.file];
        const rg = &m.row_groups.items[ref.rg];
        const rows: u64 = @intCast(@max(0, rg.num_rows));
        var product: u64 = 1;
        var dictionary_peak: u64 = 0;
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
                    run_key_extra +|= dictionaryRunExtra(cm, rows, h);
                    dictionary_peak +|= dictionaryDecodePeak(cm, h);
                    known = true;
                    widest = @max(widest, @as(u64, @intCast(h.dictionary_page_header.?.num_values)));
                    const entries: u64 = @intCast(h.dictionary_page_header.?.num_values);
                    break :blk @min(rows, entries + nullBound(rg, ci, m));
                };
                covered = false;
                full[ki] = false;
                break :blk rows;
            };
            sums[ki] +|= n;
            nulls[ki] = @max(nulls[ki], nullBound(rg, ci, m));
            product = @min(rows, product *| n);
        }
        stat.run_dictionary_peak = @max(stat.run_dictionary_peak, dictionary_peak);
        sum_products +|= @min(rows, product);
        if (covered) stat.covered_rows +|= rows;
        if (stats_present) stat.encoding_stats_rows +|= rows;
    }
    for (dict_sizes, col_sizes) |d, c| {
        stat.dictionary_bytes +|= d;
        stat.column_bytes +|= c;
    }
    stat.key_bytes = @max(@as(u64, @intCast(keys.len)) * 9, raw_key_bytes / @max(1, stat.rows));
    stat.run_key_bytes = stat.key_bytes +| (run_key_extra / @max(1, stat.rows) +
        @intFromBool(run_key_extra % @max(1, stat.rows) != 0));
    stat.hi = @min(stat.rows, sum_products);
    if (known and keys.len == 1 and stat.covered_rows == stat.rows) stat.entries = sums[0];
    if (known) {
        stat.source = .dictionary_headers;
        stat.confidence = .upper_bound;
    }
    const total_gate = stat.dictionary_bytes <= max_input or stat.dictionary_bytes <= stat.column_bytes / 20;
    var axes: []agg_dense.Axis = if (domain != null and allow_union)
        try gpa.alloc(agg_dense.Axis, keys.len)
    else
        &.{};
    var axes_count: usize = 0;
    var retained_bytes: usize = axes.len * @sizeOf(agg_dense.Axis);
    defer {
        for (axes[0..axes_count]) |*axis| axis.deinit(gpa);
        gpa.free(axes);
    }
    var union_product: u64 = 1;
    for (keys, 0..) |key, ki| {
        // Footer gate plus an absolute ceiling on scratch even when 5% admits a large compressed column.
        const gated = total_gate and full[ki] and
            (dict_sizes[ki] <= max_input or dict_sizes[ki] <= col_sizes[ki] / 20);
        // A key a retained domain can fold (`agg_dense`) takes the union past the cost gate: its scan would otherwise
        // resolve every dictionary entry of every row group itself. Past the gate the union gives up, bound-less as if
        // gated, as soon as its values outgrow a domain, so a large key pays for a few dictionaries at most.
        var axis: ?agg_dense.Axis = null;
        const retain = if (axes.len != 0 and axes_count == ki and
            (key.col_ref.physical_type == .INT32 or key.col_ref.physical_type == .INT64 or
                key.col_ref.physical_type == .BYTE_ARRAY)) &axis else null;
        // The dense fold takes a domain only where every worker's lanes cost no more than the entries they stand in
        // for (`scan.prepareAggregate`): `workers` times the widest dictionary past the entries already rules it out.
        const declined = (fold_only and keys.len == 1 and workers *| widest > sums[0] +| refs.items.len) or
            (union_opts.limit != null and (stat.entries == null or stat.entries.? > union_opts.limit.?.entries));
        // More values than the choice allows, past every row: the union can stop there (a union within the rows can
        // still bound them, so it runs whole).
        const stop: ?u64 = if (union_opts.limit) |l| if (stat.entries) |n| blk: {
            const most = @max(l.values, n / l.recur);
            break :blk if (stat.rows > most) most else null;
        } else null else null;
        const run = full[ki] and ki < reads.whole.len and reads.whole[ki] and !declined;
        // The footers' guess (`planReads`) leaves `whole` unset for the same decline.
        const footers_declined = allow_union and full[ki] and ki < reads.whole.len and !reads.whole[ki] and
            foldDeclined(metas, key, workers);
        const fold_declined = fold_only and keys.len == 1 and
            (workers *| widest > sums[0] +| refs.items.len or footers_declined);
        stat.fold_declined = stat.fold_declined or fold_declined;
        if (allow_union and full[ki] and !run and !declined and !fold_declined) stat.union_budget_exceeded = true;
        if (run) {
            stat.union_attempted = true;
            const col = key.col_ref.col_idx;
            const bound = try unionBound(
                gpa,
                arena,
                files,
                metas,
                refs.items,
                col,
                workers,
                max_memory,
                retain,
                max_memory / 16 -| retained_bytes,
                !gated,
                stop,
            );
            if (axis) |kept| {
                axes[axes_count] = kept;
                axes_count += 1;
                retained_bytes += kept.bytes;
            }
            if (bound) |n| {
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
    if (axes.len != 0 and axes_count == keys.len) {
        domain.?.* = .{ .axes = axes, .bytes = retained_bytes };
        axes = &.{};
        axes_count = 0;
    }
    return stat;
}

/// Speculative sparse identity, never NDV evidence. Missing encoding_stats cannot certify complete dictionary
/// coverage, but the decoder can validate every callback and replay the entire owner on a later PLAIN page.
/// Retention has the same byte cap as the certified union; u32 translations have an independent cardinality cap.
pub fn retainIdentity(
    gpa: std.mem.Allocator,
    arena: std.mem.Allocator,
    files: []const File,
    metas: []const schema.FileMetaData,
    keys: []const expr.Expr,
    workers: usize,
    max_memory: usize,
    identity_limit: usize,
) !?agg_dense.Domain {
    if (keys.len == 0 or max_memory < 1 << 20) return null;
    var refs: std.ArrayList(Ref) = .empty;
    for (metas, 0..) |m, fi| for (m.row_groups.items, 0..) |_, ri|
        try refs.append(arena, .{ .file = fi, .rg = ri });
    if (refs.items.len == 0) return null;
    const axes = try gpa.alloc(agg_dense.Axis, keys.len);
    var initialized: usize = 0;
    errdefer {
        for (axes[0..initialized]) |*axis| axis.deinit(gpa);
        gpa.free(axes);
    }
    var bytes = axes.len * @sizeOf(agg_dense.Axis);
    for (keys, axes) |key, *axis| {
        if (key != .col_ref) break;
        const ci = key.col_ref.col_idx;
        var flat = true;
        for (refs.items) |ref| {
            const m = metas[ref.file];
            if (ci >= m.row_groups.items[ref.rg].columns.items.len) {
                flat = false;
                break;
            }
            const cm = m.row_groups.items[ref.rg].columns.items[ci].meta_data orelse {
                flat = false;
                break;
            };
            if (m.getColumnLevels(cm.path_in_schema.items).max_rep != 0) {
                flat = false;
                break;
            }
        }
        if (!flat) break;
        var kept: ?agg_dense.Axis = null;
        _ = try unionBoundForIdentity(
            gpa,
            arena,
            files,
            metas,
            refs.items,
            ci,
            workers,
            max_memory,
            &kept,
            max_memory / 16 -| bytes,
            identity_limit,
            true,
            null,
        );
        const retained = kept orelse break;
        axis.* = retained;
        bytes += axis.bytes;
        initialized += 1;
    }
    if (initialized == axes.len) return .{ .axes = axes, .bytes = bytes };
    for (axes[0..initialized]) |*axis| axis.deinit(gpa);
    gpa.free(axes);
    return null;
}

const P = agg.NUM_PARTS;
const Partition = struct {
    lock: std.atomic.Value(bool) align(64) = .init(false),
    set: agg.DistinctSet = .{},
    /// Retaining: each value's index among this partition's `vals`, in the order workers first inserted them. While
    /// `mapped`, these stand in for `set`; once retention is given up they move into it (`migrate`).
    ids: std.HashMapUnmanaged(u64, u32, IntContext, std.hash_map.default_max_load_percentage) = .empty,
    vals: std.ArrayList(u64) = .empty,
    byte_ids: KeyMap(u32) = .empty,
    byte_vals: std.ArrayList([]u8) = .empty,
    string_key: bool = false,
    mapped: bool = false,
    /// Values whose retention is already charged: those the presize paid for, then each one past them.
    charged: usize = 0,

    fn count(p: *const Partition) usize {
        return if (p.mapped)
            (if (p.string_key) p.byte_vals.items.len else p.vals.items.len)
        else
            p.set.count();
    }

    /// Under the lock: the values retained so far into `set`, which takes every later one.
    fn migrate(p: *Partition, gpa: std.mem.Allocator) !void {
        if (!p.mapped) return;
        if (p.string_key) {
            for (p.byte_vals.items) |v| try p.set.insertBytes(gpa, v);
        } else for (p.vals.items) |v| try p.set.insertU64(gpa, v);
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
    identity_limit: usize = agg_dense.max_ids,
    /// Values all partitions hold, and whether passing the identity limit (or dropping retention) fails the
    /// union.
    distinct: std.atomic.Value(usize) = .init(0),
    strict: bool = false,
    /// See `unionBound`; `held` counts the values every partition holds against it.
    stop: ?u64 = null,
    held: std.atomic.Value(u64) = .init(0),

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
            if (s.retaining() and s.charge((n + 1) * @sizeOf(u32))) {
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
                if (s.failed.load(.monotonic)) return;
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
                const had = p.count();
                defer if (s.stop) |most| {
                    const added = p.count() - had;
                    if (s.held.fetchAdd(added, .monotonic) + added > most) s.failed.store(true, .monotonic);
                };
                if (p.mapped and !(packed_ids.len > 0 and s.retaining() and
                    (if (p.string_key)
                        retainBytes(s, p, @intCast(pi), strs, entries, packed_ids)
                    else
                        retainRun(s, p, @intCast(pi), ints, entries, packed_ids))))
                {
                    // A strict union has no usable result after retention declines. Do not migrate this
                    // partition into another exact set, or make sibling workers finish their dictionaries.
                    if (s.failed.load(.monotonic)) return;
                    try p.migrate(s.gpa);
                }
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
            if (p.vals.items.len >= @min(s.identity_limit, 1 << part_shift)) {
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
    if (s.distinct.fetchAdd(added, .monotonic) + added >= s.identity_limit) return s.drop();
    // A map entry and its value, kept on the high side; the presized share is paid already.
    const over = p.vals.items.len -| p.charged;
    p.charged += over;
    return s.charge(over * 32);
}

/// Values are copied before the dictionary scratch or a gated input window can be released.
fn retainBytes(s: *Shared, p: *Partition, pi: u32, values: []const []const u8, js: []const u32, ids: []u32) bool {
    const before = p.byte_vals.items.len;
    for (js) |j| {
        if (p.byte_ids.get(values[j])) |id| {
            ids[j] = (pi << part_shift) | id;
            continue;
        }
        const n = p.byte_vals.items.len;
        const over = n + 1 -| p.charged;
        if (n >= @min(s.identity_limit, 1 << part_shift) or !s.charge(over * 64 +| values[j].len)) return s.drop();
        p.charged += over;
        const owned = s.gpa.dupe(u8, values[j]) catch return s.drop();
        p.byte_vals.append(s.gpa, owned) catch {
            s.gpa.free(owned);
            return s.drop();
        };
        p.byte_ids.put(s.gpa, owned, @intCast(n)) catch {
            p.byte_vals.items.len -= 1;
            s.gpa.free(owned);
            return s.drop();
        };
        ids[j] = (pi << part_shift) | @as(u32, @intCast(n));
    }
    const added = p.byte_vals.items.len - before;
    if (s.distinct.fetchAdd(added, .monotonic) + added >= s.identity_limit) return s.drop();
    return true;
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
    retain: ?*?agg_dense.Axis,
    retain_limit: usize,
    /// Past the cost gate: fail (no bound) rather than outgrow a domain or give retention up.
    strict: bool,
    /// Fail (no bound) once the union holds more values than this.
    stop: ?u64,
) !?u64 {
    return unionBoundForIdentity(
        gpa,
        arena,
        files,
        metas,
        refs,
        col,
        workers,
        max_memory,
        retain,
        retain_limit,
        agg_dense.max_ids,
        strict,
        stop,
    );
}

fn unionBoundForIdentity(
    gpa: std.mem.Allocator,
    arena: std.mem.Allocator,
    files: []const File,
    metas: []const schema.FileMetaData,
    refs: []const Ref,
    col: usize,
    workers: usize,
    max_memory: usize,
    retain: ?*?agg_dense.Axis,
    retain_limit: usize,
    identity_limit: usize,
    /// Past the cost gate: fail (no bound) rather than outgrow a domain or give retention up.
    strict: bool,
    /// Fail (no bound) once the union holds more values than this.
    stop: ?u64,
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
        p.byte_ids.deinit(gpa);
        for (p.byte_vals.items) |key| gpa.free(key);
        p.byte_vals.deinit(gpa);
    };
    var shared: Shared = .{
        .gpa = gpa,
        .files = files,
        .metas = metas,
        .refs = refs,
        .col = col,
        .parts = &parts,
        .page_limit = @min(16 * 1024 * 1024, max_memory / 8 / n_workers),
        .stop = stop,
        .identity_limit = @min(identity_limit, std.math.maxInt(u32)),
    };
    const physical = if (refs.len > 0)
        metas[refs[0].file].row_groups.items[refs[0].rg].columns.items[col].meta_data.?.type
    else
        null;
    const supported = physical == .INT32 or physical == .INT64 or physical == .BYTE_ARRAY;
    if (strict and (retain == null or !supported)) return null;
    if (retain != null and supported) retain: {
        shared.retain_limit = retain_limit;
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
        const each: u32 = @intCast(@min(max_entries, shared.identity_limit) / P + 64);
        for (&parts) |*p| {
            p.mapped = true;
            p.string_key = physical == .BYTE_ARRAY;
            if (!shared.charge(@as(usize, each) * (if (p.string_key) @as(usize, 64) else 32))) break;
            p.charged = each;
            if (p.string_key) {
                p.byte_ids.ensureTotalCapacity(gpa, each) catch {
                    _ = shared.drop();
                    break;
                };
                p.byte_vals.ensureTotalCapacity(gpa, each) catch {
                    _ = shared.drop();
                    break;
                };
            } else {
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
    if (shared.trans.len > 0 and kept and shared.retaining() and count < shared.identity_limit) {
        retain.?.* = try buildAxis(gpa, metas, refs, col, &parts, shared.trans, @intCast(count));
        shared.trans = &.{};
    }
    return count;
}

/// The retained union as a domain: partitions laid end to end give the global ids, and each translation's packed
/// (partition, index) entries become those ids. Takes the translations over; on failure frees nothing it was given.
fn buildAxis(
    gpa: std.mem.Allocator,
    metas: []const schema.FileMetaData,
    refs: []const Ref,
    col: usize,
    parts: *[P]Partition,
    trans: [][]u32,
    count: usize,
) !agg_dense.Axis {
    var base: [P]u32 = undefined;
    var at: u32 = 0;
    for (parts, &base) |*p, *b| {
        b.* = at;
        at += @intCast(p.count());
    }
    std.debug.assert(at == count);
    const physical = metas[refs[0].file].row_groups.items[refs[0].rg].columns.items[col].meta_data.?.type;
    const values: []u64 = if (physical != .BYTE_ARRAY) try gpa.alloc(u64, count) else &.{};
    errdefer gpa.free(values);
    const strings: [][]u8 = if (physical == .BYTE_ARRAY) try gpa.alloc([]u8, count) else &.{};
    errdefer gpa.free(strings);
    for (parts, base) |*p, b| {
        if (physical == .BYTE_ARRAY) {
            @memcpy(strings[b..][0..p.byte_vals.items.len], p.byte_vals.items);
        } else @memcpy(values[b..][0..p.vals.items.len], p.vals.items);
    }
    const file_base = try gpa.alloc(usize, metas.len);
    errdefer gpa.free(file_base);
    var at_ref: usize = 0;
    for (metas, file_base) |m, *fb| {
        fb.* = at_ref;
        at_ref += m.row_groups.items.len;
    }
    std.debug.assert(at_ref == refs.len);
    var bytes = values.len * @sizeOf(u64) + strings.len * @sizeOf([]u8) +
        trans.len * @sizeOf([]u32) + file_base.len * @sizeOf(usize);
    for (strings) |key| bytes += key.len;
    for (trans, 0..) |_, i| {
        const t = trans[i];
        if (t.len == 0) continue;
        for (t[0 .. t.len - 1]) |*e| e.* = base[e.* >> part_shift] + (e.* & ((1 << part_shift) - 1));
        t[t.len - 1] = @intCast(count);
        bytes += t.len * @sizeOf(u32);
    }
    if (physical == .BYTE_ARRAY) for (parts) |*p| {
        p.byte_vals.items.len = 0;
    };
    return .{ .values = switch (physical) {
        .BYTE_ARRAY => .{ .bytes = strings },
        .INT32 => .{ .int32 = values },
        else => .{ .int64 = values },
    }, .trans = trans, .file_base = file_base, .bytes = bytes };
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
    const rg1 = &meta.row_groups.items[1];
    try std.testing.expectEqual(@as(u64, 1), nullBound(rg1, 1, &meta));
    cm.statistics.?.null_count = 0;
    try std.testing.expectEqual(@as(u64, 0), nullBound(rg1, 1, &meta));
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

test "ndv: run width corrects only dictionary strings and preserves other column hints" {
    var cm: schema.ColumnMetaData = .{
        .type = .BYTE_ARRAY,
        .encodings = .empty,
        .path_in_schema = .empty,
        .codec = .SNAPPY,
        .num_values = 4_000_000,
        .total_uncompressed_size = 66 * 4_000_000,
        .total_compressed_size = 1,
        .data_page_offset = 1,
        .index_page_offset = null,
        .dictionary_page_offset = 0,
    };
    var h: schema.PageHeader = .{
        .type = .DICTIONARY_PAGE,
        .uncompressed_page_size = 700_000 * (80 + 6 + 4),
        .compressed_page_size = 1,
        .crc = null,
        .data_page_header = null,
        .data_page_header_v2 = null,
        .dictionary_page_header = .{ .num_values = 700_000, .encoding = .PLAIN, .is_sorted = null },
    };
    try std.testing.expectEqual(@as(u64, 25 * 4_000_000), dictionaryRunExtra(cm, 4_000_000, h));
    try std.testing.expectEqual(@as(u64, 2 * (63_000_000 + 700_000 * 16)), dictionaryDecodePeak(cm, h));
    h.uncompressed_page_size += 1;
    try std.testing.expectEqual(@as(u64, 26 * 4_000_000), dictionaryRunExtra(cm, 4_000_000, h));
    cm.type = .INT32;
    cm.total_uncompressed_size = 5_000_000;
    try std.testing.expectEqual(@as(u64, 0), dictionaryRunExtra(cm, 4_000_000, h));
    cm.type = .BYTE_ARRAY;
    cm.total_uncompressed_size = 100 * 4_000_000;
    try std.testing.expectEqual(@as(u64, 0), dictionaryRunExtra(cm, 4_000_000, h));
    cm.total_uncompressed_size = 66 * 4_000_000;
    h.dictionary_page_header.?.num_values = 0;
    try std.testing.expectEqual(@as(u64, 0), dictionaryRunExtra(cm, 4_000_000, h));
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
            const want = try collectRetaining(a, arena, &whole, &.{meta}, ks, 4, mm, headers, allow_union, null, .{});
            const got = try collectRetaining(a, arena, &part, &.{meta}, ks, 4, mm, headers, allow_union, null, .{});
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
        const fold: UnionOpts = .{ .fold_only = true };
        const stat = try collectRetaining(a, arena, &files, ms, &keys, workers, 512 << 20, true, true, &domain, fold);
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
        const stat = try collectRetaining(a, arena, &files, ms, &keys, workers, 512 << 20, true, true, &domain, .{});
        const bound = try collect(a, arena, &files, &.{meta}, &keys, workers, 512 << 20, true);
        try std.testing.expectEqual(bound.hi, stat.hi); // Retention never moves the bound.
        const d = &(domain orelse return error.TestExpectedEqual);
        const axis = &d.axes[0];
        try std.testing.expectEqual(stat.estimate.?, axis.len() + 1); // the key holds nulls
        const seen = try arena.alloc(bool, axis.len());
        @memset(seen, false);
        for (0..meta.row_groups.items.len) |rg| {
            const t = d.forRowGroup(0, rg) orelse return error.TestExpectedEqual;
            try std.testing.expectEqual(@as(u32, @intCast(axis.len())), t[t.len - 1]);
            for (t[0 .. t.len - 1]) |id| seen[id] = true;
        }
        for (seen) |s| try std.testing.expect(s);
        try std.testing.expect(d.forRowGroup(0, meta.row_groups.items.len) == null);
        // Everything the domain owns is in the bytes the scan charges, its indexing arrays included.
        try std.testing.expectEqual(axis.len() * 8 + d.entries() * 4 + axis.trans.len * @sizeOf([]u32) +
            axis.file_base.len * @sizeOf(usize) + @sizeOf(agg_dense.Axis), d.bytes);
        try std.testing.expect(d.forRowGroup(1, 0) == null);
        var uniq: std.AutoHashMapUnmanaged(u64, void) = .empty;
        for (axis.values.int64) |v| try uniq.put(arena, v, {});
        try std.testing.expectEqual(axis.len(), uniq.count());
    }
    // Retention costs what the domain holds, its presized map shares charged once: a budget of exactly that keeps it.
    {
        var full: ?agg_dense.Domain = null;
        _ = try collectRetaining(a, arena, &files, &.{meta}, &keys, 4, 512 << 20, true, true, &full, .{});
        const d = &full.?;
        const axis = &d.axes[0];
        const need = axis.len() * 32 + d.entries() * 4 + axis.trans.len * @sizeOf([]u32) +
            axis.file_base.len * @sizeOf(usize) + @sizeOf(agg_dense.Axis);
        d.deinit(a);
        var exact: ?agg_dense.Domain = null;
        defer if (exact) |*e| e.deinit(a);
        _ = try collectRetaining(a, arena, &files, &.{meta}, &keys, 4, need * 16, true, true, &exact, .{});
        try std.testing.expect(exact != null);
    }
    // Strings and composites share the same union authority; a small retention budget drops only the domain.
    var none: ?agg_dense.Domain = null;
    const str_key = [_]expr.Expr{.{ .col_ref = .{ .col_idx = 0, .physical_type = .BYTE_ARRAY, .expr_type = .str } }};
    _ = try collectRetaining(a, arena, &files, &.{meta}, &str_key, 4, 512 << 20, true, true, &none, .{});
    try std.testing.expect(none != null and none.?.axes[0].values == .bytes);
    none.?.deinit(a);
    none = null;
    const pair = [_]expr.Expr{ keys[0], keys[0] };
    _ = try collectRetaining(a, arena, &files, &.{meta}, &pair, 4, 512 << 20, true, true, &none, .{});
    try std.testing.expect(none != null and none.?.axes.len == 2);
    none.?.deinit(a);
    none = null;
    const tight = try collectRetaining(a, arena, &files, &.{meta}, &keys, 4, 4 << 20, true, true, &none, .{});
    try std.testing.expect(tight.union_complete);
    try std.testing.expect(none == null);
    // Retention given up part way moves what it held into the union's sets: the same exact count.
    try std.testing.expectEqual((try collect(a, arena, &files, &.{meta}, &keys, 4, 4 << 20, true)).hi, tight.hi);
    // Past the cost gate (strict) the union only runs to retain: given up, it gives no bound at all.
    var refs: std.ArrayList(Ref) = .empty;
    for (0..meta.row_groups.items.len) |rg| try refs.append(arena, .{ .file = 0, .rg = rg });
    var alone: ?agg_dense.Axis = null;
    defer if (alone) |*axis| axis.deinit(a);
    const small = 4 << 20;
    const ample = 512 << 20;
    const refused = try unionBound(
        a,
        arena,
        &files,
        &.{meta},
        refs.items,
        1,
        4,
        small,
        &alone,
        small / 16,
        true,
        null,
    );
    try std.testing.expect(refused == null);
    const small_bound = try unionBound(
        a,
        arena,
        &files,
        &.{meta},
        refs.items,
        1,
        4,
        small,
        &alone,
        small / 16,
        false,
        null,
    );
    try std.testing.expect(small_bound != null);
    const no_retention = try unionBound(
        a,
        arena,
        &files,
        &.{meta},
        refs.items,
        1,
        4,
        ample,
        null,
        ample / 16,
        true,
        null,
    );
    try std.testing.expect(no_retention == null);
    const strict = try unionBound(
        a,
        arena,
        &files,
        &.{meta},
        refs.items,
        1,
        4,
        ample,
        &alone,
        ample / 16,
        true,
        null,
    );
    const loose = try unionBound(
        a,
        arena,
        &files,
        &.{meta},
        refs.items,
        1,
        4,
        ample,
        null,
        ample / 16,
        false,
        null,
    );
    try std.testing.expectEqual(loose, strict);
    try std.testing.expect(alone != null);
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
        if (collectRetaining(gpa, sa, &files, &.{meta}, &keys, 1, 512 << 20, true, true, &domain, .{})) |stat| {
            try std.testing.expect(stat.union_complete);
        } else |err| try std.testing.expectEqual(error.OutOfMemory, err);
        if (!failing.has_induced_failure) {
            try std.testing.expect(domain != null);
            break;
        }
    }
}

test "ndv: a union read for one choice stops, or is not read, where it cannot change it" {
    const a = std.testing.allocator;
    const metadata = @import("parquet/metadata.zig");
    const bytes = try metadata.readFileSlice("ci/fixtures/parquet/topk_skew.parquet", a);
    defer a.free(bytes);
    var state = std.heap.ArenaAllocator.init(a);
    defer state.deinit();
    const arena = state.allocator();
    const meta = try metadata.open(arena, bytes);
    const files = [_]File{.{ .bytes = bytes, .local = true }};
    const keys = [_]expr.Expr{.{ .col_ref = .{ .col_idx = 0, .physical_type = .BYTE_ARRAY, .expr_type = .str } }};
    const ms = [_]schema.FileMetaData{meta};
    const mm = 512 << 20;
    const whole = try collect(a, arena, &files, &.{meta}, &keys, 4, 512 << 20, true);
    try std.testing.expect(whole.union_complete and whole.hi == 9295);
    for ([_]usize{ 1, 4 }) |workers| {
        // Within its values the union runs whole, to the same bound.
        const within_opts: UnionOpts = .{ .limit = .{
            .entries = 1 << 23,
            .values = 1 << 17,
            .recur = 8,
        } };
        const within = try collectRetaining(a, arena, &files, &ms, &keys, workers, mm, true, true, null, within_opts);
        try std.testing.expect(within.union_complete);
        try std.testing.expectEqual(whole.hi, within.hi);
        // Past them it stops: no bound from the union, the headers' bound stands.
        const past_opts: UnionOpts = .{ .limit = .{
            .entries = 1 << 23,
            .values = 100,
            .recur = 1 << 30,
        } };
        const past = try collectRetaining(a, arena, &files, &ms, &keys, workers, mm, true, true, null, past_opts);
        try std.testing.expect(past.union_attempted and !past.union_complete and !past.union_budget_exceeded);
        try std.testing.expectEqual(.dictionary_headers, past.source);
        // Past its entries it is not read at all.
        const none_opts: UnionOpts = .{ .limit = .{
            .entries = 100,
            .values = 1 << 17,
            .recur = 8,
        } };
        const none = try collectRetaining(a, arena, &files, &ms, &keys, workers, mm, true, true, null, none_opts);
        try std.testing.expect(!none.union_attempted and !none.union_complete and !none.union_budget_exceeded);
        try std.testing.expectEqual(past.entries, none.entries);
    }
}

test "ndv: retained byte translations own values, deduplicate and migrate exactly on refusal" {
    for ([_]usize{ 0, 64, 1 << 20 }) |limit| try testRetainedBytes(std.testing.allocator, limit);
    var i: usize = 0;
    while (true) : (i += 1) {
        var failing = std.testing.FailingAllocator.init(std.testing.allocator, .{ .fail_index = i });
        testRetainedBytes(failing.allocator(), 1 << 20) catch |err|
            try std.testing.expectEqual(error.OutOfMemory, err);
        if (!failing.has_induced_failure) break;
    }
}

fn testRetainedBytes(gpa: std.mem.Allocator, limit: usize) !void {
    var parts: [P]Partition = @splat(.{ .mapped = true, .string_key = true });
    defer for (&parts) |*p| {
        p.set.deinit(gpa);
        p.ids.deinit(gpa);
        p.vals.deinit(gpa);
        p.byte_ids.deinit(gpa);
        for (p.byte_vals.items) |key| gpa.free(key);
        p.byte_vals.deinit(gpa);
    };
    var shared: Shared = .{
        .gpa = gpa,
        .files = &.{},
        .metas = &.{},
        .refs = &.{},
        .col = 0,
        .parts = &parts,
        .page_limit = 4096,
        .retain_limit = limit,
        .dropped = .init(false),
    };
    var backing = [_]u8{ 'a', 0, 'b', 'z', 'z', 'z' };
    const values = [_][]const u8{ backing[0..0], backing[0..3], backing[0..3], backing[3..6], "unused" };
    var ids: [values.len]u32 = undefined;
    const js = [_]u32{ 0, 1, 2, 3, 4 };
    const p = &parts[0];
    if (!retainBytes(&shared, p, 0, &values, &js, &ids)) {
        try p.migrate(gpa);
        for (values) |key| try p.set.insertBytes(gpa, key);
        try std.testing.expectEqual(@as(usize, 4), p.set.count());
        return;
    }
    try std.testing.expectEqual(@as(usize, 4), p.count());
    try std.testing.expectEqual(ids[1], ids[2]);
    @memset(&backing, 0xaa);
    try std.testing.expectEqualStrings("", p.byte_vals.items[ids[0]]);
    try std.testing.expectEqualSlices(u8, "a\x00b", p.byte_vals.items[ids[1]]);
    try std.testing.expectEqualStrings("zzz", p.byte_vals.items[ids[3]]);
    try std.testing.expectEqualStrings("unused", p.byte_vals.items[ids[4]]);
}

test "ndv: composite retention survives worker spawn refusal and shares the union bound" {
    const a = std.testing.allocator;
    const metadata = @import("parquet/metadata.zig");
    const bytes = try metadata.readFileSlice("ci/fixtures/parquet/topk_skew.parquet", a);
    defer a.free(bytes);
    var scratch = std.heap.ArenaAllocator.init(a);
    defer scratch.deinit();
    const arena = scratch.allocator();
    const meta = try metadata.open(arena, bytes);
    const files = [_]File{.{ .bytes = bytes, .local = true }};
    const keys = [_]expr.Expr{
        .{ .col_ref = .{ .col_idx = 0, .physical_type = .BYTE_ARRAY, .expr_type = .str } },
        .{ .col_ref = .{ .col_idx = 1, .physical_type = .INT64, .expr_type = .i64 } },
    };
    spawn.injectFailureAfter(0);
    defer spawn.resetFailure();
    var domain: ?agg_dense.Domain = null;
    defer if (domain) |*d| d.deinit(a);
    const stat = try collectRetaining(a, arena, &files, &.{meta}, &keys, 4, 512 << 20, true, true, &domain, .{});
    try std.testing.expect(stat.union_complete and domain != null);
    const d = &domain.?;
    try std.testing.expectEqual(@as(usize, 2), d.axes.len);
    try std.testing.expect(d.axes[0].values == .bytes and d.axes[1].values == .int64);
    try std.testing.expectEqual((try collect(a, arena, &files, &.{meta}, &keys, 4, 512 << 20, true)).hi, stat.hi);
    try std.testing.expect(d.ids() >= stat.hi);
    try std.testing.expect(d.bytes <= (512 << 20) / 16);
    try std.testing.expect(spawn.failuresInjected() > 0);
}

test "M1 speculative identity retains dictionary translations without strengthening unknown NDV evidence" {
    const a = std.testing.allocator;
    const metadata = @import("parquet/metadata.zig");
    const bytes = try metadata.readFileSlice("ci/fixtures/parquet/topk_skew.parquet", a);
    defer a.free(bytes);
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const temp = arena.allocator();
    const meta = try metadata.open(temp, bytes);
    for (meta.row_groups.items) |*rg| for (rg.columns.items) |*col| {
        if (col.meta_data) |*cm| cm.encoding_stats = null;
    };
    const files = [_]File{.{ .bytes = bytes, .local = true }};
    const keys = [_]expr.Expr{.{ .col_ref = .{ .col_idx = 1, .physical_type = .INT64, .expr_type = .i64 } }};
    const bound = try collect(a, temp, &files, &.{meta}, &keys, 4, 512 << 20, true);
    try std.testing.expectEqual(.unknown, bound.source);
    for ([_]usize{ 2, agg_dense.max_ids * 4 }) |limit| {
        var domain = try retainIdentity(a, temp, &files, &.{meta}, &keys, 4, 512 << 20, limit);
        defer if (domain) |*d| d.deinit(a);
        try std.testing.expectEqual(limit > 2, domain != null);
        if (domain) |d| {
            for (0..meta.row_groups.items.len) |rg| {
                const translation = d.axes[0].forRowGroup(0, rg).?;
                try std.testing.expectEqual(d.axes[0].len(), translation[translation.len - 1]);
                for (translation) |id| try std.testing.expect(id < d.axes[0].ids());
            }
        }
        try std.testing.expectEqualDeep(bound, try collect(a, temp, &files, &.{meta}, &keys, 4, 512 << 20, true));
    }
}

test "ndv: a computed key gives the same stat wherever it sits among the keys" {
    const a = std.testing.allocator;
    const metadata = @import("parquet/metadata.zig");
    const bytes = try metadata.readFileSlice("ci/fixtures/parquet/topk_skew.parquet", a);
    defer a.free(bytes);
    var state = std.heap.ArenaAllocator.init(a);
    defer state.deinit();
    const arena = state.allocator();
    const meta = try metadata.open(arena, bytes);
    const files = [_]File{.{ .bytes = bytes, .local = true }};
    var int_key: expr.Expr = .{ .col_ref = .{ .col_idx = 1, .physical_type = .INT64, .expr_type = .i64 } };
    var one: expr.Expr = .{ .literal = .{ .i64 = 1 } };
    const computed: expr.Expr = .{ .binop = .{
        .op = .add,
        .left = &int_key,
        .right = &one,
        .result_type = .i64,
        .depth = 2,
    } };
    const str_key: expr.Expr = .{ .col_ref = .{ .col_idx = 0, .physical_type = .BYTE_ARRAY, .expr_type = .str } };
    // q18's shape: `UserID, extract(minute ...), SearchPhrase` against the computed key last.
    const middle = [_]expr.Expr{ int_key, computed, str_key };
    const last = [_]expr.Expr{ int_key, str_key, computed };
    const ms = &.{meta};
    const got = try collectRetaining(a, arena, &files, ms, &middle, 4, 512 << 20, true, true, null, .{});
    const want = try collectRetaining(a, arena, &files, ms, &last, 4, 512 << 20, true, true, null, .{});
    try std.testing.expect(want.string_keys);
    try std.testing.expectEqual(want.string_keys, got.string_keys);
    try std.testing.expectEqual(want.rows, got.rows);
    try std.testing.expectEqual(want.hi, got.hi);
    try std.testing.expectEqual(want.confidence, got.confidence);
}
