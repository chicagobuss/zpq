//! Per-row-group scan→consumer protocol.
//!
//! Lambda's parallel-fetcher orchestrator and the CLI's sequential
//! local-file driver both feed the same shape of work to the same
//! two consumers: *encode* (decode → filter → re-encode kept cols)
//! and *copy* (byte-copy kept chunks, shift offsets).
//!
//! The only thing they differ on is *where the bytes come from*:
//! lambda hands in a `RowGroupResult.raw_bytes` window covering the
//! kept-column span, CLI hands in the entire mmap'd file. Both shapes
//! collapse to a single `RGSrc { bytes, byte_origin }` — the per-
//! column offset into the source equals `col.dpo - byte_origin`
//! either way.
//!
//! Output is always a `streaming.Sink` (multipart-S3 in lambda; an
//! fd-backed sink in the CLI). Per-phase timings flow into a shared
//! `Timings` accumulator the caller owns.
//!
//! Layering: this module lives in `core/`, so it must NOT depend on
//! any I/O. The `Sink` is opaque; the source is a borrowed slice.

const std = @import("std");
const builtin = @import("builtin");
const owner_groups = @import("owner_groups.zig");
const msd_groups = @import("msd_groups.zig");
const retained_pages = @import("retained_pages.zig");
const nowMonoNs = @import("../clock.zig").monoNs;
const profile = @import("../profile.zig");

/// Zig's 16 MiB default made thread setup/teardown dominate short queries. Duplicated rather than imported from
/// scan.zig, which imports this module.
const WORKER_STACK_SIZE: usize = 1 << 20;
const spawn_util = @import("spawn.zig");
const blocking = @import("blocking.zig");
const schema = @import("schema.zig");
const filter_ast = @import("filter/ast.zig");
const filter_eval = @import("filter/eval.zig");
pub const SelectedBatch = @import("selected_batch.zig").SelectedBatch;
const filter_selection = @import("filter/selection.zig");
const filter_prune = @import("filter/prune.zig");
const expr_ast = @import("expr/ast.zig");
const expr_eval = @import("expr/eval.zig");
const expr_agg = @import("expr/agg.zig");
const core_memory = @import("memory.zig");
const agg_plan = @import("agg_plan.zig");
const encoder = @import("writer/encoder.zig");
const streaming = @import("writer/streaming.zig");
const fastpath = @import("writer/fastpath.zig");
const page_index = @import("writer/page_index.zig");
const thrift = @import("thrift.zig");
const column_mod = @import("parquet/column.zig");
const page_mod = @import("parquet/page.zig");
const Diag = @import("diag.zig").Diag;
const Text = @import("diag.zig").Text;
const decimal_mod = @import("parquet/decimal.zig");
const compression = @import("parquet/compression.zig");
const invariant = @import("invariant.zig");
const int96_mod = @import("parquet/int96.zig");
const metadata = @import("parquet/metadata.zig");
const statistics = @import("parquet/statistics.zig");
const header_writer = @import("writer/page_header.zig");
const WriterStatsPolicy = @import("parquet/writer_statistics.zig").Policy;
const agg_coded = @import("expr/agg_coded.zig");
const agg_dense = @import("expr/agg_dense.zig");
const partagg = @import("partagg.zig");
const huge_pages = @import("huge_pages.zig");

pub const DecodeOptions = column_mod.DecodeOptions;
pub const DecodeScratch = column_mod.DecodeScratch;
pub const SliceScratch = page_mod.SliceScratch;
pub const RowGroupArena = @import("row_group_arena.zig").RowGroupArena;

pub const types = @import("consumer/types.zig");
pub const Error = types.Error;
pub const RGSrc = types.RGSrc;
pub const Timings = types.Timings;
pub const DecodeSite = types.DecodeSite;
pub const enterChunk = types.enterChunk;
pub const decodeFailure = types.decodeFailure;
pub const setDecodeFailure = types.setDecodeFailure;
pub const resolveDecodeFailure = types.resolveDecodeFailure;
pub const isDecodeError = types.isDecodeError;
pub const RGOut = types.RGOut;
pub const rebaseRowGroupOffsets = types.rebaseRowGroupOffsets;
const decode_order = @import("decode_order.zig");

pub const output = @import("consumer/output.zig");
pub const OutputAggregator = output.OutputAggregator;
pub const initOutputAggregator = output.initOutputAggregator;
pub const resetAggregator = output.resetAggregator;

pub const appendProjectedRG = output.appendProjectedRG;
pub const appendProjectedRGWithOptions = output.appendProjectedRGWithOptions;
pub const ProjectedRG = output.ProjectedRG;
pub const Survivors = output.Survivors;
const gatherRows = output.gatherRows;

const decodeWriteColumn = consumer_column.decodeWriteColumn;

pub const appendSliceRows = output.appendSliceRows;

const namedLevels = output.namedLevels;
const flatLevels = output.flatLevels;

pub const renameLeaf = types.renameLeaf;

const booleanLane = output.booleanLane;
pub const outputLane = output.outputLane;

pub const chunkDictionary = consumer_column.chunkDictionary;

pub const slice_reader = @import("consumer/slice_reader.zig");
pub const SliceReader = slice_reader.SliceReader;

const decimalKind = consumer_column.decimalKind;
const decodeDecimalI128 = consumer_column.decodeDecimalI128;

const appendSelected = output.appendSelected;

const decodeWriteColumnMaybeCoded = consumer_column.decodeWriteColumnMaybeCoded;

const appendIntoBuf = output.appendIntoBuf;
const appendTyped = output.appendTyped;

pub const encode_module = @import("consumer/encode.zig");
pub const encodeAggregator = encode_module.encodeAggregator;
pub const RGEncoder = encode_module.RGEncoder;
pub const plainBits = encode_module.plainBits;
pub const plainBitsPrefix = encode_module.plainBitsPrefix;
pub const placeChunk = encode_module.placeChunk;
pub const AggEncode = encode_module.AggEncode;
pub const answer_rg_rows = encode_module.answer_rg_rows;
pub const writeAggregateRows = encode_module.writeAggregateRows;
pub const AggregateFile = encode_module.AggregateFile;
const cloneColumnMeta = encode_module.cloneColumnMeta;
const sliceOutputCol = encode_module.sliceOutputCol;
const aggEncodeCharge = encode_module.aggEncodeCharge;
const writeAggregateRowsEncoded = encode_module.writeAggregateRowsEncoded;
const appendAggregateRows = encode_module.appendAggregateRows;
pub const bufRows = output.bufRows;
pub const appendRows = output.appendRows;
pub const BufRange = output.BufRange;
pub const joinRows = output.joinRows;

pub const OutputCol = types.OutputCol;
pub const computedSchemaElem = types.computedSchemaElem;
pub const decimalElem = types.decimalElem;

pub const RowSubgroup = types.RowSubgroup;
pub const AggRowGroup = types.AggRowGroup;
pub const selectedWindow = types.selectedWindow;

pub const target_module = @import("consumer/target.zig");
pub const PackedCount = target_module.PackedCount;
pub const IntegerCountSink = target_module.IntegerCountSink;
pub const SelectedRecords = target_module.SelectedRecords;
pub const RecordSink = target_module.RecordSink;

/// `.selected` with a partial mask gathers every column into ascending window indexes and emits those rows.
/// A column that cannot be gathered (nested, histogram, levels-only) keeps the mask and `input_rows` null.
fn emitRecords(
    arena: std.mem.Allocator,
    records: *const RecordSink,
    batch: *const filter_eval.Batch,
    sel: *const filter_selection.SelectionVector,
    input_base: u64,
    legacy_offset: usize,
    lookup: []const ?usize,
) !void {
    const kept = sel.count();
    if (records.* != .selected or kept == batch.num_rows or batch.num_rows > std.math.maxInt(u32)) {
        const window = try selectedWindow(batch, sel, input_base, null, batch.num_rows);
        return records.emit(window, lookup, legacy_offset, sel);
    }
    const rows = try arena.alloc(u32, kept);
    var w: usize = 0;
    for (0..sel.len) |i| if (sel.isActive(i)) {
        rows[w] = @intCast(i);
        w += 1;
    };
    const cols = try arena.alloc(filter_eval.Batch.Column, batch.cols.len);
    for (batch.cols, cols) |c, *o| {
        o.* = (try gatherRows(arena, c, sel, kept)) orelse {
            const window = try selectedWindow(batch, sel, input_base, null, batch.num_rows);
            return records.emit(window, lookup, legacy_offset, sel);
        };
    }
    const gathered: filter_eval.Batch = .{ .cols = cols, .num_rows = kept };
    const window = try selectedWindow(&gathered, null, input_base, rows, batch.num_rows);
    var ones = try filter_selection.SelectionVector.init(arena, kept);
    return records.emit(window, lookup, legacy_offset, &ones);
}

/// Group keys and aggregates over the rows `sel` kept. A sparse selection is gathered first, so `resolveBatch`
/// walks survivors and each group's first row stays the original ordinal (`pos + ordinals[i]`). A sketch clears
/// the caller's selection as it counts, which a gathered copy would not, so that path keeps the mask.
fn foldGroupedBatch(
    arena: std.mem.Allocator,
    gpa: std.mem.Allocator,
    resolver: *expr_agg.GroupKeyResolver,
    table: *expr_agg.GroupTable,
    calls: []const expr_agg.AggCall,
    key_cols: []const filter_eval.Batch.Column,
    batch: *const filter_eval.Batch,
    lookup: []const ?usize,
    sel: *filter_selection.SelectionVector,
    ids: []u32,
    pos: u64,
) !void {
    const kept = sel.count();
    if (table.sketch == null and kept != 0 and kept != sel.len and sel.len <= std.math.maxInt(u32)) {
        if (try gatherUnder(arena, batch.cols, key_cols, sel, kept)) |g| {
            try resolver.resolveBatch(arena, table, g.key_cols, calls, pos, &g.sel, ids, g.ordinals);
            if (!table.countOnly()) for (calls, 0..) |_, i| {
                try expr_agg.updateTableGrouped(arena, gpa, table, ids, i, calls, &g.batch, lookup, &g.sel);
            };
            return;
        }
    }
    try resolver.resolveBatch(arena, table, key_cols, calls, pos, sel, ids, null);
    try expr_agg.sinkSketchRows(table, ids, sel);
    if (!table.countOnly()) for (calls, 0..) |_, i| {
        try expr_agg.updateTableGrouped(arena, gpa, table, ids, i, calls, batch, lookup, sel);
    };
}

const GatheredFold = struct {
    key_cols: []filter_eval.Batch.Column,
    batch: filter_eval.Batch,
    sel: filter_selection.SelectionVector,
    ordinals: []u32,
};

fn gatherUnder(
    arena: std.mem.Allocator,
    cols: []const filter_eval.Batch.Column,
    key_cols: []const filter_eval.Batch.Column,
    sel: *const filter_selection.SelectionVector,
    kept: usize,
) !?GatheredFold {
    const rows = try arena.alloc(u32, kept);
    var w: usize = 0;
    for (0..sel.len) |i| if (sel.isActive(i)) {
        rows[w] = @intCast(i);
        w += 1;
    };
    const out_cols = try arena.alloc(filter_eval.Batch.Column, cols.len);
    for (cols, out_cols) |c, *o| o.* = (try gatherRows(arena, c, sel, kept)) orelse return null;
    const out_keys = try arena.alloc(filter_eval.Batch.Column, key_cols.len);
    for (key_cols, out_keys) |c, *o| o.* = (try gatherRows(arena, c, sel, kept)) orelse return null;
    return .{
        .key_cols = out_keys,
        .batch = .{ .cols = out_cols, .num_rows = kept },
        .sel = try filter_selection.SelectionVector.init(arena, kept),
        .ordinals = rows,
    };
}

pub const AggTarget = target_module.AggTarget;

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
fn admitWhole(ws: AggWorkspace, bytes: usize) bool {
    const r = ws.region orelse return true;
    return r.fitsRowGroup(bytes);
}

/// Give back everything the worker's region holds, the row-group arena's nodes and the decode scratch, with what
/// they drew from the ledger: a row group the ledger refused there then decodes in slices on memory of its own, not
/// beside the pages it was refused for. Nothing allocated on either may be live.
fn dropRegion(ws: AggWorkspace) void {
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
fn refusedDraw(ws: AggWorkspace, err: anyerror) bool {
    if (err != error.OutOfMemory) return false;
    if (ws.region) |r| if (r.refused) {
        r.refused = false;
        return true;
    };
    return false;
}

pub const budget_module = @import("consumer/budget.zig");
pub const DecodeBudget = budget_module.DecodeBudget;
pub const decodeEstimate = budget_module.decodeEstimate;

/// A window of `sel`, rows `[lo, lo + n)`, as its own selection. Bits past `n` stay clear.
fn selWindow(
    a: std.mem.Allocator,
    sel: *const filter_selection.SelectionVector,
    lo: usize,
    n: usize,
) !filter_selection.SelectionVector {
    var w = try filter_selection.SelectionVector.init(a, n);
    var copied: usize = 0;
    while (copied < n) {
        const src_bit = lo + copied;
        const src_word = sel.mask[src_bit / 64];
        const sshift: u6 = @intCast(src_bit % 64);
        const dshift: u6 = @intCast(copied % 64);
        var take = @min(n - copied, @as(usize, 64) - sshift);
        take = @min(take, @as(usize, 64) - dshift);
        const di = copied / 64;
        if (take == 64) {
            w.mask[di] = src_word;
        } else {
            const mask = (@as(u64, 1) << @as(u6, @intCast(take))) - 1;
            const bits = (src_word >> sshift) & mask;
            w.mask[di] = (w.mask[di] & ~(mask << dshift)) | (bits << dshift);
        }
        copied += take;
    }
    return w;
}

fn sliceColumn(col: filter_eval.Batch.Column, lo: usize, n: usize) filter_eval.Batch.Column {
    return switch (col) {
        inline else => |c, tag| blk: {
            var o = c;
            if (c.codes) |codes| if (codes.len >= lo + n) {
                o.codes = codes[lo..][0..n];
            };
            if (c.values.len >= lo + n) o.values = c.values[lo..][0..n];
            if (c.def_levels) |dl| if (dl.len >= lo + n) {
                o.def_levels = dl[lo..][0..n];
            };
            if (c.rep_levels) |rl| if (rl.len >= lo + n) {
                o.rep_levels = rl[lo..][0..n];
            };
            if (c.byte_lengths) |lens| {
                if (lens.len >= lo + n) o.byte_lengths = lens[lo..][0..n];
            }
            if (c.unscaled) |u| o.unscaled = switch (u) {
                .i32 => |v| if (v.len >= lo + n) .{ .i32 = v[lo..][0..n] } else u,
                .i64 => |v| if (v.len >= lo + n) .{ .i64 = v[lo..][0..n] } else u,
                .i128 => |v| if (v.len >= lo + n) .{ .i128 = v[lo..][0..n] } else u,
            };
            if (o.levels_only_rows != 0) o.levels_only_rows = n;
            break :blk @unionInit(filter_eval.Batch.Column, @tagName(tag), o);
        },
    };
}

/// Hash of the key `row` would be stored under, matching `GroupTable.routeHash` after insert.
fn routeRowHash(
    ra: std.mem.Allocator,
    scratch: *std.ArrayList(u8),
    table: *const expr_agg.GroupTable,
    key_cols: []const filter_eval.Batch.Column,
    row: usize,
) !u64 {
    if (table.fixed_stride != 0) {
        var image: [32]u8 = undefined;
        if (expr_agg.packFixedKey(key_cols, row, &image)) |nb| if (nb == table.fixed_stride) {
            return table.hashRoute(image[0..nb]);
        };
    }
    try expr_agg.serializeRowKey(scratch, ra, key_cols, row);
    return table.hashRoute(scratch.items);
}

fn insertRow(
    ra: std.mem.Allocator,
    scratch: *std.ArrayList(u8),
    table: *expr_agg.GroupTable,
    key_cols: []const filter_eval.Batch.Column,
    row: usize,
    calls: []const expr_agg.AggCall,
    pos: u64,
) !u32 {
    if (table.fixed_stride != 0) {
        var image: [32]u8 = undefined;
        if (expr_agg.packFixedKey(key_cols, row, &image)) |nb| if (nb == table.fixed_stride) {
            return table.getOrInsertFixed(image[0..nb], calls, pos + row);
        };
    }
    try expr_agg.serializeRowKey(scratch, ra, key_cols, row);
    return table.getOrInsert(scratch.items, calls, pos + row);
}

/// Abandoned pre-aggregation: each row appended to this worker's run for its key's partition.
fn scatterGrouped(
    ra: std.mem.Allocator,
    values: std.mem.Allocator,
    gpa: std.mem.Allocator,
    g: @FieldType(AggTarget, "grouped"),
    part: *partagg.PartAgg,
    local: *partagg.Local,
    cols: []filter_eval.Batch.Column,
    lookup: []const ?usize,
    win_sel: *const filter_selection.SelectionVector,
    consumed: []const bool,
    prepared_values: *bool,
    pos: u64,
    off: usize,
    n: usize,
) !void {
    if (!prepared_values.*) {
        for (lookup, 0..) |at, ci| if (at) |p| if (ci < consumed.len and consumed[ci]) {
            cols[p] = try agg_coded.materialize(values, cols[p]);
        };
        prepared_values.* = true;
    }
    const win_cols = try ra.alloc(filter_eval.Batch.Column, cols.len);
    for (cols, win_cols) |c, *o| o.* = sliceColumn(c, off, n);
    var win: filter_eval.Batch = .{ .cols = win_cols, .num_rows = n };
    const key_cols = try ra.alloc(filter_eval.Batch.Column, g.keys.len);
    const keyed = win.restrictedTo(win_sel);
    for (g.keys, key_cols) |k, *kc| kc.* = try expr_eval.evalGroupKeyExpr(ra, &keyed, lookup, k);

    const which = try ra.alloc(u16, n);
    var counts: [partagg.n_parts]u32 = @splat(0);
    var scratch: std.ArrayList(u8) = .empty;
    defer scratch.deinit(ra);
    for (0..n) |r| {
        if (!win_sel.isActive(r)) continue;
        const h = try routeRowHash(ra, &scratch, g.table, key_cols, r);
        const p: u16 = @intCast(part.indexOf(h));
        which[r] = p;
        counts[p] += 1;
    }
    var start: [partagg.n_parts + 1]u32 = undefined;
    start[0] = 0;
    for (0..partagg.n_parts) |p| start[p + 1] = start[p] + counts[p];
    const order = try ra.alloc(u32, start[partagg.n_parts]);
    var at = start;
    for (0..n) |r| {
        if (!win_sel.isActive(r)) continue;
        const p = which[r];
        order[at[p]] = @intCast(r);
        at[p] += 1;
    }
    const ids = try ra.alloc(u32, n);
    var preps = try expr_agg.prepareGrouped(ra, g.calls, &win, lookup, win_sel);
    defer preps.deinit();
    if (!part.usesRuns(local)) {
        for (part.parts, 0..) |*pt, p| {
            const lo = start[p];
            const hi = start[p + 1];
            if (lo == hi) continue;
            partagg.PartAgg.lock(pt);
            defer partagg.PartAgg.unlock(pt);
            for (order[lo..hi]) |r| ids[r] = try insertRow(ra, &scratch, &pt.table, key_cols, r, g.calls, pos);
            try preps.fold(gpa, pt.table.accumulators.items, ids, order[lo..hi], g.calls, if (pt.table.compact.stride != 0) &pt.table.compact else null);
        }
        return;
    }
    const runs = local.runs.?;
    for (runs, 0..) |*run, p| {
        const lo = start[p];
        const hi = start[p + 1];
        if (lo == hi) continue;
        for (order[lo..hi]) |r| {
            var image: [32]u8 = undefined;
            const key = try storedRowKey(ra, &scratch, g.table, key_cols, r, &image);
            ids[r] = @intCast(try part.append(run, key, pos + r));
        }
        try preps.fold(gpa, run.accs.items, ids, order[lo..hi], g.calls, if (run.compact.stride != 0) &run.compact else null);
    }
}

/// The key `row` would be stored under in `table`: its fixed image, or its serialized bytes in `scratch`.
fn storedRowKey(
    ra: std.mem.Allocator,
    scratch: *std.ArrayList(u8),
    table: *const expr_agg.GroupTable,
    key_cols: []const filter_eval.Batch.Column,
    row: usize,
    image: *[32]u8,
) ![]const u8 {
    if (table.fixed_stride != 0) {
        if (expr_agg.packFixedKey(key_cols, row, image)) |nb| if (nb == table.fixed_stride) return image[0..nb];
    }
    try expr_agg.serializeRowKey(scratch, ra, key_cols, row);
    if (table.fixed_stride == 0) return scratch.items;
    return expr_agg.fixedImage(table.fixed_stride, scratch.items, image) orelse error.BadGroupKey;
}

/// Fold `cols` through the worker's local table in windows, flushing into `g.part` when the local table fills or
/// stops reducing. One window is what `groupedCoded` or the row path already know how to fold.
fn foldGroupedWindows(
    ra: std.mem.Allocator,
    gpa: std.mem.Allocator,
    target: AggTarget,
    cols: []filter_eval.Batch.Column,
    lookup: []const ?usize,
    sel: *const filter_selection.SelectionVector,
    consumed: []const bool,
    encoded: bool,
    expr_batch_rows: ?usize,
    /// Charged for a coded composite space past the free one, as the whole-row-group fold is.
    budget: ?*DecodeBudget,
    /// The region `ra` allocates from, if it does.
    region: ?*huge_pages.Region,
) !void {
    const g = switch (target) {
        .grouped => |grouped| grouped,
        .scalar => return,
    };
    const part = g.part orelse return;
    const local = g.local orelse return;
    // An armed worker folds a batch the coded path takes whole, as the plain fold does: in windows, each would
    // resolve again nearly every code of a large dictionary.
    if (local.armed and encoded and sel.len > 0) {
        const whole: filter_eval.Batch = .{ .cols = cols, .num_rows = sel.len, .expr_batch_rows = expr_batch_rows };
        if (try agg_coded.groupedCoded(ra, gpa, g.calls, g.keys, g.table, g.pos_base, &whole, lookup, sel, budget, region)) {
            try part.pump(local, g.table, @intCast(sel.count()));
            return;
        }
    }
    // Group tables/runs own their keys and states. Only decoded columns survive a window: otherwise routing,
    // selection and expression scratch accumulates across the row group beside the prepaid run ceiling.
    var window = RowGroupArena.init(ra);
    defer window.deinit();
    var prepared_values = false;
    var off: usize = 0;
    while (off < sel.len) {
        window.nextRowGroup();
        const wa = window.allocator();
        if (part.needsFlush(local, g.table)) try part.pump(local, g.table, 0);
        const n = part.nextSpan(local, g.table, sel.len - off);
        if (n == 0) break;
        var win_sel = try selWindow(wa, sel, off, n);
        defer win_sel.deinit();
        const pos = g.pos_base + @as(u64, @intCast(off));
        // A local table that is not reducing the rows is pure overhead: hash each key once, straight into its partition.
        if (local.abandoned) {
            try scatterGrouped(wa, ra, gpa, g, part, local, cols, lookup, &win_sel, consumed, &prepared_values, pos, off, n);
            off += n;
            continue;
        }
        if (!prepared_values and encoded) {
            const win_cols = try wa.alloc(filter_eval.Batch.Column, cols.len);
            for (cols, win_cols) |c, *o| o.* = sliceColumn(c, off, n);
            const win: filter_eval.Batch = .{ .cols = win_cols, .num_rows = n, .expr_batch_rows = expr_batch_rows };
            if (try agg_coded.groupedCoded(wa, gpa, g.calls, g.keys, g.table, pos, &win, lookup, &win_sel, budget, region)) {
                try part.pump(local, g.table, @intCast(win_sel.count()));
                off += n;
                continue;
            }
        }
        if (!prepared_values) {
            for (lookup, 0..) |at, ci| if (at) |p| if (ci < consumed.len and consumed[ci]) {
                cols[p] = try agg_coded.materialize(ra, cols[p]);
            };
            prepared_values = true;
        }
        const win_cols = try wa.alloc(filter_eval.Batch.Column, cols.len);
        for (cols, win_cols) |c, *o| o.* = sliceColumn(c, off, n);
        var win: filter_eval.Batch = .{ .cols = win_cols, .num_rows = n };
        const key_cols = try wa.alloc(filter_eval.Batch.Column, g.keys.len);
        const keyed = win.restrictedTo(&win_sel);
        for (g.keys, key_cols) |k, *kc| kc.* = try expr_eval.evalGroupKeyExpr(wa, &keyed, lookup, k);
        const ids = try wa.alloc(u32, n);
        @memset(ids, 0);
        var resolver = expr_agg.GroupKeyResolver.init(key_cols);
        defer resolver.deinit(wa);
        try foldGroupedBatch(wa, gpa, &resolver, g.table, g.calls, key_cols, &win, lookup, &win_sel, ids, pos);
        try part.pump(local, g.table, @intCast(win_sel.count()));
        off += n;
    }
}

/// Per-RG aggregator update. Answers what statistics can, then decodes
/// the columns the rest read, applies the outer filter, and folds the
/// surviving rows into `target` (each aggregate's own
/// `FILTER (WHERE ...)` composed in).
///
/// No sink writes, no offset advancement, no RGOut. Aggregates produce
/// nothing per-RG — only at end-of-scan, when the caller finalizes
/// `target`'s accumulators.
///
/// `gpa` owns what outlives the row group: string min/max winners and
/// group-table entries.
/// Returns the rows the outer filter kept (every row without one), for the summary's `rows_matched`.
pub fn scanRGForAgg(
    gpa: std.mem.Allocator,
    in: AggRowGroup,
    stats: agg_plan.StatsUse,
    decode_options: DecodeOptions,
    target: AggTarget,
    ws: AggWorkspace,
) !u64 {
    var execution = ws;
    if (ws.auto_decode_batches and ws.decode_batch_rows == 0 and ws.expr_batch_rows != 0 and
        (target != .grouped or (decode_options.encoded and target.grouped.part == null and
            target.grouped.records == null)))
        execution.decode_batch_rows = automaticCacheRowsWithHints(ws.planning_charged orelse ws.charged orelse gpa, in, target, ws.scan_workers, ws.cache_hints) catch 0;
    if (ws.defer_decode_errors) return scanRGForAggImpl(gpa, in, stats, decode_options, target, execution);
    return scanRGForAggImpl(gpa, in, stats, decode_options, target, execution) catch |err| {
        if (!isDecodeError(err)) return err;
        const site = decodeFailure() orelse return err;
        const failure = canonicalDecodeFailure(
            ws.charged orelse gpa,
            in,
            decode_options,
            target.calls(),
            target == .scalar,
            .{ .err = err, .site = site },
        );
        setDecodeFailure(failure.site);
        return failure.err;
    };
}

/// Error-only work, after releasing the failed scan's arenas. All allocations use the caller's bounded, charged
/// allocator. Resource failure ends the probe with its best confirmed decode error; it never becomes the diagnostic.
pub fn canonicalDecodeFailure(
    a: std.mem.Allocator,
    in: AggRowGroup,
    options: DecodeOptions,
    calls: []const expr_agg.AggCall,
    scalar: bool,
    original: decode_order.Failure,
) decode_order.Failure {
    return (probeDecodeFailure(a, in, options, calls, scalar) catch null) orelse original;
}

/// Probe one full-projection row group. Valid groups contribute no failure; refusal contributes no evidence.
pub fn probeDecodeFailure(
    a: std.mem.Allocator,
    in: AggRowGroup,
    options: DecodeOptions,
    calls: []const expr_agg.AggCall,
    scalar: bool,
) !?decode_order.Failure {
    var planning = std.heap.ArenaAllocator.init(a);
    defer planning.deinit();
    const pa = planning.allocator();
    var check_in = in;
    check_in.read = in.error_read orelse in.read;
    if (in.error_ranks) |rank| check_in.error_order = try rank.make(rank.ctx, pa, check_in.read.len);
    const only = try pa.alloc(bool, check_in.read.len);
    @memset(only, false);
    if (scalar) {
        const target: AggTarget = .{ .scalar = .{ .calls = calls, .accumulators = &.{} } };
        const uses = try planCodeUses(pa, check_in, target, false);
        for (only, 0..) |*v, ci| v.* = uses.levels[ci] and !uses.code[ci] and !uses.value[ci];
    }
    return decode_order.probe(SliceReader, isDecodeError, a, check_in, options, only);
}

// Checked arithmetic keeps its expression-major error order. A forced cache window may change only when
// fault-free expressions are evaluated; memory-driven slicing otherwise retains its existing admission.
fn cacheExpressionsSafe(target: AggTarget) bool {
    for (target.calls()) |call| if (call.arg) |arg| {
        if (arg == .col_ref and (arg.col_ref.unsigned_64 or arg.col_ref.expr_type == .dec)) return false;
        if (arg != .col_ref and !floatTreeSafe(arg)) return false;
    };
    if (target == .grouped) for (target.grouped.keys) |key| {
        if (key == .col_ref and (key.col_ref.unsigned_64 or key.col_ref.expr_type == .dec)) return false;
        if (key != .col_ref and !floatTreeSafe(key)) return false;
    };
    return true;
}

fn floatTreeSafe(e: expr_ast.Expr) bool {
    return switch (e) {
        .literal => |l| l != .str,
        .col_ref => |c| c.expr_type == .f64 and (c.physical_type == .FLOAT or c.physical_type == .DOUBLE),
        .binop => |b| b.result_type == .f64 and b.op != .concat and
            floatTreeSafe(b.left.*) and floatTreeSafe(b.right.*),
        .call, .case_ => false,
    };
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

    fn count(h: *const CacheHints) usize {
        var n = h.inline_shard.facts.count();
        for (h.shards) |shard| n += shard.facts.count();
        return n;
    }

    fn get(h: *CacheHints, in: AggRowGroup, ci: usize) ?CacheDictionary {
        const key: Key = .{ .rg = in.rg, .column = ci };
        const hash = (std.hash_map.AutoContext(Key){}).hash(key);
        const shard = if (h.shards.len == 0) &h.inline_shard else &h.shards[hash % h.shards.len];
        shard.lock.lockUncancelable(@import("blocking.zig").io);
        defer shard.lock.unlock(@import("blocking.zig").io);
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
fn cacheDictionary(in: AggRowGroup, ci: usize) ?CacheDictionary {
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

fn cacheFilterNodes(f: filter_ast.Filter) usize {
    return switch (f) {
        .and_filter, .or_filter => |c| blk: {
            var n: usize = 1;
            for (c.children) |child| n +|= cacheFilterNodes(child);
            break :blk n;
        },
        else => 1,
    };
}

fn cacheExpandedTreeBytes(e: expr_ast.Expr) usize {
    return switch (e) {
        .binop => |b| 12 +| cacheExpandedTreeBytes(b.left.*) +| cacheExpandedTreeBytes(b.right.*),
        .literal => 8,
        else => 16,
    };
}

fn wholeFloatTreeBytes(e: expr_ast.Expr) usize {
    return switch (e) {
        .binop => |b| 12 +| wholeFloatTreeBytes(b.left.*) +| wholeFloatTreeBytes(b.right.*),
        .literal => 8,
        .col_ref => |c| if (c.physical_type == .FLOAT) 8 else 0,
        else => 0,
    };
}

/// Admit only computed fault-free float trees. Decoded inputs, all DAG results, key codes, selection and group
/// IDs share the stable L2 worker budget; pages/dictionaries use the decoder's separate bounded buffers.
fn automaticCacheRows(gpa: std.mem.Allocator, in: AggRowGroup, target: AggTarget, workers: usize) !usize {
    return automaticCacheRowsWithHints(gpa, in, target, workers, null);
}

fn automaticCacheRowsWithHints(gpa: std.mem.Allocator, in: AggRowGroup, target: AggTarget, workers: usize, hints: ?*CacheHints) !usize {
    var computed = false;
    for (target.calls()) |call| {
        if (call.where != null or call.func == .count_distinct or call.result == .bytes) return 0;
        if (call.arg) |arg| {
            computed = computed or arg == .binop;
            if (arg == .col_ref and (arg.col_ref.unsigned_64 or arg.col_ref.expr_type == .dec)) return 0;
        }
    }
    if (!computed or !cacheExpressionsSafe(target)) return 0;
    if (target == .grouped) for (target.grouped.keys) |key| {
        if (key != .col_ref or key.col_ref.unsigned_64 or key.col_ref.expr_type == .dec) return 0;
    };
    if (@import("hw.zig").l2BytesPerWorker() == 0) return 0;
    var planning = std.heap.ArenaAllocator.init(gpa);
    defer planning.deinit();
    const a = planning.allocator();
    const lookup = try a.alloc(?usize, in.meta.schema.items.len);
    @memset(lookup, null);
    var cols: std.ArrayList(filter_eval.Batch.Column) = .empty;
    var copied_bytes: usize = 0;
    var fixed_keys: usize = 0;
    var whole_values: usize = 0;
    var largest_chunk: usize = 0;
    for (in.read, 0..) |needed, ci| {
        if (!needed) continue;
        if (!SliceReader.supports(in.rg, in.meta, ci)) return 0;
        const cm = in.rg.columns.items[ci].meta_data orelse return 0;
        const n: usize = @intCast(@max(0, cm.num_values));
        const dictionary = if (hints) |h| h.get(in, ci) else cacheDictionary(in, ci);
        const lane: usize = if (dictionary != null) 4 else switch (cm.type) {
            .INT32, .FLOAT => 4,
            .BOOLEAN => 1,
            else => 8,
        };
        whole_values +|= n *| lane;
        if (in.meta.getColumnLevels(cm.path_in_schema.items).max_def > 0)
            whole_values +|= if (dictionary != null) n / 8 +| 8 else n *| 4;
        largest_chunk = @max(largest_chunk, @as(usize, @intCast(@max(0, cm.total_uncompressed_size))));
        if (in.meta.getColumnSchema(cm.path_in_schema.items)) |se|
            if (decimal_mod.exactKind(&se) != null) return 0;
        switch (cm.type) {
            .BYTE_ARRAY => {
                const d = dictionary orelse return 0;
                // decodeBytes can copy the same dictionary string on every row. Its entire encoded dictionary
                // bounds one string's width, even when compression makes a footer's per-row estimate tiny.
                copied_bytes +|= d.bytes;
                fixed_keys +|= d.bytes +| d.entries *| @sizeOf([]const u8);
            },
            .FIXED_LEN_BYTE_ARRAY, .INT96 => return 0,
            else => {},
        }
        lookup[ci] = cols.items.len;
        // FloatPlan inspects lane types and layouts before decoding; other input lanes are counted below.
        const col: filter_eval.Batch.Column = switch (cm.type) {
            .FLOAT => .{ .f32 = .{ .values = &.{} } },
            .DOUBLE => .{ .f64 = .{ .values = &.{} } },
            else => .{ .i64 = .{ .values = &.{} } },
        };
        try cols.append(a, col);
    }
    const batch: filter_eval.Batch = .{ .cols = cols.items, .num_rows = 0 };
    var plan: expr_eval.FloatPlan = .{};
    for (target.calls()) |call| if (call.arg) |arg| if (arg != .col_ref) {
        _ = (try plan.add(a, &batch, lookup, arg)) orelse return 0;
    };
    const rows: usize = @intCast(@max(0, in.rg.num_rows));
    if (rows == 0) return 0;
    const grouped = target == .grouped;
    const whole_dictionaries = fixed_keys;
    var space: usize = 1;
    if (grouped) for (target.grouped.keys) |key| {
        const d = (if (hints) |h| h.get(in, key.col_ref.col_idx) else cacheDictionary(in, key.col_ref.col_idx)) orelse return 0;
        fixed_keys +|= agg_coded.plainKeyScratchBytes(d.entries) orelse return 0;
        space *|= d.entries +| 1;
    };
    const inputs = std.math.divCeil(usize, decodeEstimate(in, in.read, &.{}, grouped), rows) catch return 0;
    const key_bytes = if (grouped) 4 * target.grouped.keys.len else 0;
    // Counting the DAG's borrowed input lanes again is conservative and reserves room for widening and key scratch.
    var widened: usize = 0;
    for (target.calls()) |call| if (call.arg) |arg| if (arg == .col_ref and
        (arg.col_ref.physical_type == .INT32 or arg.col_ref.physical_type == .FLOAT))
    {
        widened +|= 8;
    };
    const masks = target.calls().len +| if (in.filter) |f| cacheFilterNodes(f) else @as(usize, 0);
    // A window with only NULL keys can decline recoding. Its general fold must fit too, even without CSE.
    var expanded: usize = 8;
    if (grouped) for (target.calls()) |call| if (call.arg) |arg| {
        if (arg != .col_ref) expanded +|= cacheExpandedTreeBytes(arg);
    };
    const live = inputs +| @max(plan.liveBytesPerRow(), expanded) +| key_bytes +| copied_bytes +| widened +| masks;
    const group_state = if (grouped) space *| (64 +| target.grouped.keys.len *| 32 +|
        target.calls().len *| (2 * @sizeOf(expr_agg.Accumulator) + 32) +| copied_bytes) else 0;
    const fixed = fixed_keys +| group_state +|
        (plan.nodes.items.len + cols.items.len) * @sizeOf(filter_eval.Batch.Column) +|
        in.meta.schema.items.len *| @sizeOf(?usize) *| (3 + if (grouped) target.grouped.keys.len else @as(usize, 0)) +|
        target.calls().len * @sizeOf(expr_agg.Accumulator);
    const cap = expr_eval.floatBatchRows(live, fixed, null);
    // Whole vectors are often cheaper while their hot state fits in L3. This is a cost estimate, independent of
    // the conservative L2 bound above: dictionary codes/presence, values/levels, one page and aggregate scratch.
    const l3 = @import("hw.zig").l3BytesPerActiveWorker(workers);
    if (l3 != 0) {
        var whole = whole_values +| largest_chunk +| rows / 8 +| whole_dictionaries +| group_state;
        if (grouped) {
            whole +|= rows *| 4 +| plan.liveBytesPerRow() *|
                expr_eval.floatBatchRows(plan.liveBytesPerRow(), group_state, null);
        } else for (target.calls()) |call| if (call.arg) |arg| {
            if (arg != .col_ref) whole +|= rows *| wholeFloatTreeBytes(arg);
        };
        if (whole <= l3) return 0;
    }
    return if (cap != 0 and cap < rows) cap else 0;
}

/// Share computed scalar inputs too, then use the ordinary typed folds over their prepared values. Synthesizing a
/// borrowed DOUBLE column keeps NULL masks and canonical 64-row reductions in updateOne; no partial sum is merged.
fn foldScalarCache(
    a: std.mem.Allocator,
    persist: std.mem.Allocator,
    calls: []const expr_agg.AggCall,
    accumulators: []expr_agg.Accumulator,
    handled: []const bool,
    batch: *const filter_eval.Batch,
    lookup: []const ?usize,
    sel: *const filter_selection.SelectionVector,
) !bool {
    var plan: expr_eval.FloatPlan = .{};
    const roots = try a.alloc(?usize, calls.len);
    @memset(roots, null);
    for (calls, handled, roots) |call, h, *root| {
        if (h) continue;
        if (call.where != null or call.func == .count_distinct or call.result == .bytes) return false;
        if (call.arg) |arg| if (arg != .col_ref) {
            root.* = (try plan.add(a, batch, lookup, arg)) orelse return false;
        };
    }
    if (plan.nodes.items.len == 0) return false;
    const restricted = batch.restrictedTo(sel);
    const values = try plan.evaluate(a, &restricted, lookup);
    for (calls, accumulators, handled, roots) |call, *acc, h, root| {
        if (h) continue;
        if (root) |r| {
            var prepared = call;
            prepared.arg = .{ .col_ref = .{ .col_idx = 0, .physical_type = .DOUBLE, .expr_type = .f64 } };
            const input: filter_eval.Batch = .{ .cols = &.{values[r]}, .num_rows = batch.num_rows };
            try expr_agg.updateOne(a, persist, acc, prepared, &input, &.{0}, sel);
        } else try expr_agg.updateOne(a, persist, acc, call, batch, lookup, sel);
    }
    return true;
}

fn scanRGForAggImpl(
    gpa: std.mem.Allocator,
    in: AggRowGroup,
    stats: agg_plan.StatsUse,
    decode_options: DecodeOptions,
    target: AggTarget,
    ws: AggWorkspace,
) !u64 {
    const calls = target.planCalls();
    const num_rows: usize = @intCast(in.rg.num_rows);

    // 1. Stats short-circuit pass. Aggregates answered from the row
    //    group's metadata alone are marked handled so the decode pass
    //    below skips them; the rest fall through to decode.
    const handled_via_stats = try gpa.alloc(bool, calls.len);
    defer gpa.free(handled_via_stats);
    @memset(handled_via_stats, false);
    const any_decode_required = if (in.subgroup != null) true else switch (target) {
        // A chunk statistic describes every group at once, so it answers none of them.
        .grouped => true,
        .scalar => |s| try foldFromStats(s.calls, s.accumulators, in, stats, handled_via_stats, ws.timings),
    };

    // Fast path: every agg answered from stats, no decode needed at
    // all. This is the headline win — `max(cost)`, `count(*)`, etc.
    // complete in microseconds because we never read any data pages.
    // An outer filter disables every stats answer, so here none is pending; with one it decodes to count its rows.
    if (!any_decode_required and in.filter == null) return num_rows;

    // 2. Decode path for the aggs that couldn't be stat-handled.
    // Rewound, not rebuilt: a fresh arena per row group hands its pages back to the OS and re-faults them every time.
    // The price is that each worker's arena holds its high-water mark (one row group) for the whole scan, which the
    // decode budget bounds: a row group past a worker's share decodes staged, on memory handed back when it is done.
    ws.rg_arena.nextRowGroup();
    if (ws.region) |r| r.settle();
    const prof = ws.timings.prof;

    var staged = false;
    var reserved: usize = 0;
    // Staged and sliced decodes live beside the region, on memory drawn from the query's ledger as the region's is.
    const side = ws.charged orelse gpa;
    var plan_arena = std.heap.ArenaAllocator.init(side);
    defer plan_arena.deinit();
    var full_estimate: usize = 0;
    if (in.subgroup) |subgroup| {
        var fallback: DecodeBudget = .{ .limit = decode_options.expand_limit };
        const b = ws.budget orelse &fallback;
        const per_row = @max(1, decodeEstimate(in, in.read, calls, true) / @max(1, num_rows));
        const rows = subgroup.rows.hi - subgroup.rows.lo;
        dropRegion(ws);
        return (try scanRGSliced(gpa, in, decode_options, target, ws, b, handled_via_stats, per_row *| rows, streamWindowRows(in, target, rows))) orelse error.NestedNotSupported;
    }
    const stage_plan: ?StagePlan = if (ws.budget) |b| plan: {
        const ordinary = decodeEstimate(in, in.read, calls, target == .grouped);
        const full = if (target == .grouped and target.grouped.records != null)
            target.grouped.records.?.decodeReservation(ordinary)
        else
            ordinary;
        full_estimate = full;
        if (target == .grouped and target.grouped.table.countOnly()) {
            const keys_only = decodeEstimate(in, in.read, &.{}, true);
            std.debug.assert(full == keys_only);
        }
        reserved = full;
        // Too big for this worker's share, or for what the query's ledger has left: a slice of rows at a time when
        // every column has a sliced form, else one aggregate's columns at a time.
        const fits_share = full <= b.share();
        const discovering = target == .grouped and target.grouped.table.discovery != null;
        const force_cache = ws.decode_batch_rows != 0 and (discovering or cacheExpressionsSafe(target));
        // A filter-only or hash-only string is hashed or compared per page. Decoding the chunk whole faults the
        // decompressed bytes into the row-group region and holds them until the scan releases every region.
        const stream_rows = streamWindowRows(in, target, num_rows);
        const stream = stream_rows != std.math.maxInt(usize);
        const whole = !stream and !force_cache and fits_share and admitWhole(ws, full);
        if (!whole) if (ws.region) |r| if (fits_share) dropRegion(ws) else r.trim();
        if (!whole) if (try scanRGSliced(
            gpa,
            in,
            decode_options,
            target,
            ws,
            b,
            handled_via_stats,
            full,
            stream_rows,
        )) |kept|
            return kept;
        if (!whole) {
            if (target == .grouped and target.grouped.records != null) return error.ExceededMemoryBudget;
            const sp = try StagePlan.init(plan_arena.allocator(), in, target, handled_via_stats);
            if (sp.estimate < full) {
                staged = true;
                reserved = sp.estimate;
                break :plan sp;
            }
        }
        break :plan null;
    } else null;
    if (ws.budget) |b| try b.acquire(reserved);
    var held = reserved;
    defer if (ws.budget) |b| b.release(held);
    // Staged, everything decoded lives on memory of its own, given back with the row group; otherwise on the
    // worker's arena.
    var staged_arena = std.heap.ArenaAllocator.init(side);
    defer staged_arena.deinit();
    const ra = if (staged) staged_arena.allocator() else ws.rg_arena.allocator();

    const prune_span = profile.Span.begin(prof);
    const page_indexes = try loadPageIndexes(ra, in, stats);
    var sel = try filter_selection.SelectionVector.init(ra, num_rows);
    const filter_pages = try pruneFilterPages(ra, in, page_indexes, stats, &sel);
    // Page pruning left few rows: decode just those, and let them stand for the row group from here on.
    const keep = if (staged) null else try compactRows(ra, in, page_indexes, &sel, target);
    const batch_rows = if (keep != null) sel.count() else num_rows;

    const t_decode_start = nowMonoNs();
    var dec: ChunkDecoder = .{
        .in = in,
        .uses = try planCodeUses(ra, in, target, staged),
        .page_indexes = page_indexes,
        .filter_pages = filter_pages,
        .num_rows = num_rows,
        .decode_options = decode_options,
        .ws = ws,
        .target = target,
        .keep = keep,
        .rows_out = if (keep != null) batch_rows else null,
    };
    var batch_cols: std.ArrayList(filter_eval.Batch.Column) = .empty;
    var lookup = try ra.alloc(?usize, in.meta.schema.items.len);
    @memset(lookup, null);
    // Pages are pruned by the rows the filter's page statistics leave, as when every column decodes up front.
    const sel_pre = if (staged) try sel.cloneAlloc(ra) else sel;
    // Compact columns: every kept row is selected so far.
    if (keep != null) sel = try filter_selection.SelectionVector.init(ra, batch_rows);
    // Payload columns decode after the filter, gated by its survivors; a staged plan decodes them with its calls.
    dec.payload = try payloadColumns(ra, in, target);
    var length_of = try ra.alloc(bool, in.read.len);
    @memset(length_of, false);
    var filtered = false;
    var survivors: usize = 0;
    var whole: []bool = &.{};
    var gated = if (!staged and keep == null and stage_plan == null)
        try GatedFilter.init(ra, in, target, dec.page_indexes)
    else
        null;
    if (gated) |*g| dec.filter_gated = g.deferred;
    const all_columns = try ra.alloc(usize, in.read.len);
    for (all_columns, 0..) |*c, i| c.* = i;
    for ([_]bool{ false, true }) |late| for (if (!late and gated != null) gated.?.order else all_columns) |ci| {
        const needed = in.read[ci];
        if (late and dec.payload == null) break;
        if (!needed or ci >= in.rg.columns.items.len) continue;
        if (stage_plan) |sp| if (!sp.base[ci]) {
            // A flat hash-group key is payload: one gated decode after the filter, including on a staged plan.
            const pay = late and if (dec.payload) |p| p[ci] else false;
            if (!pay) continue;
        };
        if (dec.payload) |p| if (p[ci] != late) continue;
        if (!late) if (gated) |*g| if (g.deferred[ci]) {
            const now: filter_eval.Batch = .{
                .cols = batch_cols.items,
                .num_rows = batch_rows,
                .expr_batch_rows = ws.expr_batch_rows,
            };
            try g.evaluate(&now, &sel, lookup, ra, false);
            dec.gate = &sel;
        };
        if (late and !filtered) {
            const first: filter_eval.Batch = .{ .cols = batch_cols.items, .num_rows = batch_rows, .expr_batch_rows = ws.expr_batch_rows };
            if (gated) |*g|
                try g.evaluate(&first, &sel, lookup, ra, true)
            else
                try filter_eval.evaluate(in.filter.?, &first, &sel, lookup, ra);
            filtered = true;
            dec.gate = &sel;
            survivors = sel.count();
        }
        if (late and keep == null and flatGroupKey(target, ci)) if (in.rg.columns.items[ci].meta_data) |cm| {
            if (@import("filter/membership.zig").dictionaryCoversChunk(&cm) and
                (foldWantsCodes(target, ci, in.rg) or keyCodesCheaper(cm, batch_rows, survivors)))
            {
                if (whole.len == 0) {
                    whole = try ra.alloc(bool, in.read.len);
                    @memset(whole, false);
                    dec.whole = whole;
                }
                whole[ci] = true;
            }
        };
        lookup[ci] = batch_cols.items.len;
        const col = dec.column(ra, ci, &sel_pre) catch |err| {
            // Values that expand past the budget read whole (a footer can understate them by any factor) may still
            // stream a slice at a time. Nothing is aggregated before every column decodes, so the row group starts
            // over; its reservation and what it decoded go back first.
            const b = ws.budget orelse return err;
            const refused = refusedDraw(ws, err);
            if ((err != error.DecodeExceededBudget and !refused) or staged) return err;
            b.release(held);
            held = 0;
            if (refused) dropRegion(ws) else {
                ws.rg_arena.nextRowGroup();
                if (ws.region) |r| r.trim();
            }
            // No record has been emitted: release the failed whole-decode payload before rereading it in slices.
            if (target == .grouped) if (target.grouped.records) |records| records.rewindRetained();
            if (try scanRGSliced(
                gpa,
                in,
                decode_options,
                target,
                ws,
                b,
                handled_via_stats,
                full_estimate,
                streamWindowRows(in, target, num_rows),
            )) |kept|
                return kept;
            return err;
        };
        if (col == .string and col.string.byte_lengths != null) length_of[ci] = true;
        try batch_cols.append(ra, col);
    };
    const t_decode_end = nowMonoNs();
    ws.timings.decode_ns += @intCast(t_decode_end - t_decode_start);
    if (prof) |t| {
        t.record(.prune_pages, prune_span.start, t_decode_start, profile.args(.{ dec.pages_total, dec.pages_skipped }));
        const decode_args = profile.args(.{ in.file_index, in.rg_index, batch_cols.items.len });
        t.record(.decode, t_decode_start, t_decode_end, decode_args);
    }

    const batch: filter_eval.Batch = .{
        .cols = batch_cols.items,
        .num_rows = batch_rows,
        .expr_batch_rows = ws.expr_batch_rows,
    };
    if (in.filter) |f| if (!filtered) {
        if (gated) |*g|
            try g.evaluate(&batch, &sel, lookup, ra, true)
        else
            try filter_eval.evaluate(f, &batch, &sel, lookup, ra);
    };
    // A staged plan's call columns decode below, after the filter: payload among them is gated the same way.
    // Length-only predicate columns refill here when the fold or an owner still reads the bytes, and only the
    // rows the filter kept. `codePlainKeys` has to see those bytes, not the empty length-only placeholders.
    if (dec.payload != null or in.filter != null) dec.gate = &sel;
    dec.allow_lengths = false;
    if (in.filter != null) {
        dec.refill = true;
        for (length_of, 0..) |was, ci| if (was and columnFeedsFold(target, ci)) {
            const pos = lookup[ci] orelse continue;
            batch_cols.items[pos] = try dec.column(ra, ci, &sel);
        };
        dec.refill = false;
    }
    if (target == .grouped and target.grouped.records == null and !staged and decode_options.encoded)
        try agg_coded.codePlainKeys(ra, target.grouped.keys, batch_cols.items, lookup);
    const t_eval_end = nowMonoNs();
    ws.timings.eval_ns += @intCast(t_eval_end - t_decode_end);
    const selected: usize = if (in.filter != null) sel.count() else num_rows;
    profile.record(prof, .filter, t_decode_end, t_eval_end, profile.args(.{ batch_rows, selected }));

    switch (target) {
        .grouped => |g| {
            if (g.records) |records| {
                // Partial selections gather into ascending `input_rows`. A full window stays `row_base + i`.
                try emitRecords(ra, records, &batch, &sel, in.row_base, 0, lookup);
                const end = nowMonoNs();
                ws.timings.encode_ns += @intCast(end - t_eval_end);
                profile.record(prof, .aggregate, t_eval_end, end, profile.args(.{ selected, calls.len }));
                return selected;
            }
            if (g.part != null and !staged) {
                try foldGroupedWindows(
                    ra,
                    gpa,
                    target,
                    batch_cols.items,
                    lookup,
                    &sel,
                    in.consumed,
                    decode_options.encoded,
                    ws.expr_batch_rows,
                    ws.budget,
                    ws.region,
                );
                const t_agg_end = nowMonoNs();
                ws.timings.encode_ns += @intCast(t_agg_end - t_eval_end);
                profile.record(prof, .aggregate, t_eval_end, t_agg_end, profile.args(.{ selected, calls.len }));
                return selected;
            }
            const dense = if (g.dense) |d| !staged and decode_options.encoded and
                try d.foldKeys(
                    ra,
                    gpa,
                    g.keys,
                    in.file_index,
                    in.rg_index,
                    g.pos_base,
                    &batch,
                    lookup,
                    &sel,
                ) else false;
            if (dense or (!staged and decode_options.encoded and
                try agg_coded.groupedCoded(ra, gpa, g.calls, g.keys, g.table, g.pos_base, &batch, lookup, &sel, ws.budget, ws.region)))
            {
                const t_agg_end = nowMonoNs();
                ws.timings.encode_ns += @intCast(t_agg_end - t_eval_end);
                profile.record(prof, .aggregate, t_eval_end, t_agg_end, profile.args(.{ selected, calls.len }));
                return selected;
            }
            // The row path reads values: gather any column decoded as codes only, but not one only the outer filter
            // read, which it has evaluated already.
            for (lookup, 0..) |at, ci| if (at) |pos| if (ci < in.consumed.len and in.consumed[ci]) {
                batch_cols.items[pos] = try agg_coded.materialize(ra, batch_cols.items[pos]);
            };
            var key_cols = try ra.alloc(filter_eval.Batch.Column, g.keys.len);
            for (g.keys, 0..) |key_expr, idx| {
                key_cols[idx] = try expr_eval.evalGroupKeyExpr(ra, &batch.restrictedTo(&sel), lookup, key_expr);
            }

            const group_id_arr = try ra.alloc(u32, batch_rows);
            @memset(group_id_arr, 0);

            // Never cached across row groups: the resolver's memo holds pointers into `ra`.
            var key_resolver = expr_agg.GroupKeyResolver.init(key_cols);
            defer key_resolver.deinit(ra);

            const sparse = g.table.sketch == null and stage_plan == null and sel.count() != sel.len and
                sel.len <= std.math.maxInt(u32);
            if (sparse) if (try gatherUnder(ra, batch.cols, key_cols, &sel, sel.count())) |gathered| {
                try key_resolver.resolveBatch(
                    ra,
                    g.table,
                    gathered.key_cols,
                    g.calls,
                    g.pos_base,
                    &gathered.sel,
                    group_id_arr,
                    gathered.ordinals,
                );
                if (!g.table.countOnly()) for (g.calls, 0..) |_, i| {
                    try expr_agg.updateTableGrouped(
                        ra,
                        gpa,
                        g.table,
                        group_id_arr,
                        i,
                        g.calls,
                        &gathered.batch,
                        lookup,
                        &gathered.sel,
                    );
                };
            } else {
                try key_resolver.resolveBatch(ra, g.table, key_cols, g.calls, g.pos_base, &sel, group_id_arr, null);
                try expr_agg.sinkSketchRows(g.table, group_id_arr, &sel);
                if (!g.table.countOnly()) for (g.calls, 0..) |_, i| {
                    try expr_agg.updateTableGrouped(ra, gpa, g.table, group_id_arr, i, g.calls, &batch, lookup, &sel);
                };
            } else {
                try key_resolver.resolveBatch(ra, g.table, key_cols, g.calls, g.pos_base, &sel, group_id_arr, null);
                try expr_agg.sinkSketchRows(g.table, group_id_arr, &sel);
                // A sketch's count pass has read no aggregate's columns, and has no group to fold into.
                if (!g.table.countOnly()) for (g.calls, 0..) |_, i| {
                    var call_arena = std.heap.ArenaAllocator.init(side);
                    defer call_arena.deinit();
                    const ca = if (staged) call_arena.allocator() else ra;
                    const cb = if (stage_plan) |sp|
                        try dec.withCallColumns(ca, sp, i, &batch, lookup, &sel_pre, true)
                    else
                        CallBatch{ .batch = batch, .lookup = lookup };
                    try expr_agg.updateTableGrouped(
                        ca,
                        gpa,
                        g.table,
                        group_id_arr,
                        i,
                        g.calls,
                        &cb.batch,
                        cb.lookup,
                        &sel,
                    );
                };
            }
            // The windowed path returned above. A staged row group folded whole still has to enter the partitions
            // before the next row group, or one worker's local table keeps every key it has seen.
            if (g.part) |p| if (g.local) |local| try p.pump(local, g.table, @intCast(sel.count()));
        },
        .scalar => |s| scalar: {
            // Update only the aggs that weren't handled via stats. The agg's
            // own per-WHERE predicate (if any) is composed inside updateOne.
            if (!staged and try expr_agg.updateSharedInteger(
                ra,
                s.calls,
                s.accumulators,
                handled_via_stats,
                &batch,
                lookup,
                &sel,
            )) break :scalar;
            for (s.calls, s.accumulators, handled_via_stats, 0..) |call, *acc, handled, i| {
                if (handled) continue;
                var call_arena = std.heap.ArenaAllocator.init(side);
                defer call_arena.deinit();
                const ca = if (staged) call_arena.allocator() else ra;
                const cb = if (stage_plan) |sp|
                    try dec.withCallColumns(ca, sp, i, &batch, lookup, &sel_pre, false)
                else
                    CallBatch{ .batch = batch, .lookup = lookup };
                // `ca` is per-RG (or per-call) scratch; `gpa` is the persist allocator for owned results (string
                // min/max winner) that outlive both and the cross-worker merge.
                try expr_agg.updateOne(ca, gpa, acc, call, &cb.batch, cb.lookup, &sel);
            }
        },
    }
    const t_agg_end = nowMonoNs();
    ws.timings.encode_ns += @intCast(t_agg_end - t_eval_end);
    profile.record(prof, .aggregate, t_eval_end, t_agg_end, profile.args(.{ selected, calls.len }));
    return selected;
}

pub const pool_module = @import("consumer/pool.zig");
pub const poolOn = pool_module.poolOn;
pub const releaseSlicePool = pool_module.releaseSlicePool;

/// The rows of a row group its filter keeps, reading only the columns before the payload a slice at a time.
fn filterSurvivors(
    a: std.mem.Allocator,
    side: std.mem.Allocator,
    in: AggRowGroup,
    decode_options: DecodeOptions,
    payload: []const bool,
    num_rows: usize,
    ws: AggWorkspace,
    stream_rows: usize,
) !?filter_selection.SelectionVector {
    const pool = try poolOn(&ws.scratch.slice, side);
    var n_readers: usize = 0;
    for (in.read, payload, 0..) |needed, late, ci| {
        if (!needed or late or ci >= in.rg.columns.items.len) continue;
        n_readers += 1;
    }
    pool.rewind();
    try pool.reserve(n_readers);
    const words = try pool.survivorWords(num_rows);
    var survivors = filter_selection.SelectionVector.borrowed(words, num_rows);
    var readers: std.ArrayList(SliceReader) = .empty;
    defer for (readers.items) |*r| r.deinit();
    const lookup = try a.alloc(?usize, in.meta.schema.items.len);
    @memset(lookup, null);
    for (in.read, payload, 0..) |needed, late, ci| {
        if (!needed or late or ci >= in.rg.columns.items.len) continue;
        const r = try SliceReader.initPooled(a, side, in.rg, in.meta, in.src, ci, decode_options, pool) orelse
            return null;
        lookup[ci] = readers.items.len;
        try readers.append(a, r);
    }
    const slice = pool.valueArena();
    defer _ = slice.reset(.retain_capacity);
    // A memory granule over the filter's own columns; it never changes which rows survive. A streamed string
    // keeps that granule inside one worker's L2, so the copied page is reused instead of the whole chunk.
    const step: usize = @min(@as(usize, 1 << 16), stream_rows);
    var done: usize = 0;
    while (done < num_rows) : (done += step) {
        _ = slice.reset(.retain_capacity);
        const sa = slice.allocator();
        const n = @min(step, num_rows - done);
        const cols = try sa.alloc(filter_eval.Batch.Column, readers.items.len);
        for (readers.items, cols) |*r, *c| c.* = try r.next(sa, n);
        const batch: filter_eval.Batch = .{ .cols = cols, .num_rows = n, .expr_batch_rows = ws.expr_batch_rows };
        var sel = try filter_selection.SelectionVector.init(sa, n);
        try filter_eval.evaluate(in.filter.?, &batch, &sel, lookup, sa);
        for (0..n) |i| if (!sel.isActive(i)) survivors.set(done + i, false);
    }
    return survivors;
}

fn openSlicedPayload(
    pages: std.mem.Allocator,
    side: std.mem.Allocator,
    in: AggRowGroup,
    decode_options: DecodeOptions,
    target: AggTarget,
    pool: *SliceScratch,
    readers: *std.ArrayList(SliceReader),
    lookup: []?usize,
    levels_only: []const bool,
    payload: ?[]const bool,
    survivors: ?*filter_selection.SelectionVector,
    ws: AggWorkspace,
) !?void {
    @memset(lookup, null);
    var n_readers: usize = 0;
    for (in.read, 0..) |needed, ci| {
        if (!needed or ci >= in.rg.columns.items.len) continue;
        if (payload != null and ci < in.consumed.len and !in.consumed[ci]) continue;
        n_readers += 1;
    }
    pool.rewind();
    try pool.reserve(n_readers);
    for (in.read, 0..) |needed, ci| {
        if (!needed or ci >= in.rg.columns.items.len) continue;
        // The first pass already validated and evaluated filter-only columns. Keep only the values the consumer
        // reads; a column shared by the filter and an aggregate still needs its value lane here.
        if (payload != null and ci < in.consumed.len and !in.consumed[ci]) continue;
        var options = decode_options;
        // A streamed string is compared or hashed from its bytes. Codes would take the dictionary fast path on a
        // pure-dictionary window and the value path on the next, which is not how the whole chunk folds.
        options.any_dictionary = (target.codedKey(ci) and !streamByteArray(in, target, ci)) or
            (decode_options.encoded and scalarByteDistinct(in, target) == ci);
        options.decimal_exact_only = target.exactInputs();
        if (payload) |p| if (p[ci]) if (survivors) |sv| {
            options.row_gate = sv;
        };
        var r = try SliceReader.initPooled(pages, side, in.rg, in.meta, in.src, ci, options, pool) orelse return null;
        if (in.subgroup) |subgroup| try r.seekSubgroup(pages, subgroup);
        if (levels_only[ci]) r.levelsOnly();
        if (target == .grouped) if (target.grouped.records) |records| if (records.retention(ci)) |owner| {
            r.retainPayloads(owner);
            r.raw.bytes.pages.prof = ws.timings.prof;
        };
        lookup[ci] = readers.items.len;
        try readers.append(pages, r);
    }
}

/// The streamed scalar string DISTINCT can keep its source codes through the fold. Other calls retain their
/// established decode and expression/error ordering.
fn scalarByteDistinct(in: AggRowGroup, target: AggTarget) ?usize {
    if (in.filter != null or target != .scalar or target.scalar.calls.len != 1) return null;
    const call = target.scalar.calls[0];
    if (call.func != .count_distinct or call.where != null) return null;
    const arg = call.arg orelse return null;
    if (arg != .col_ref or arg.col_ref.physical_type != .BYTE_ARRAY or arg.col_ref.expr_type != .str) return null;
    const ci = arg.col_ref.col_idx;
    return if (streamByteArray(in, target, ci)) ci else null;
}

/// One dictionary's observed codes, retained until its row group ends. The accumulator owns exact copies of the
/// strings; the bitmap is only a cache and can be refused without changing the answer.
threadlocal var test_scalar_distinct_inserts: usize = 0;
const ScalarDistinctSeen = struct {
    dict: ?[*]const []const u8 = null,
    dict_len: usize = 0,
    words: []u64 = &.{},
    disabled: bool = false,

    fn deinit(self: *ScalarDistinctSeen, allocator: std.mem.Allocator) void {
        if (self.words.len != 0) allocator.free(self.words);
        self.* = .{};
    }

    fn fold(
        self: *ScalarDistinctSeen,
        scratch: std.mem.Allocator,
        persist: std.mem.Allocator,
        acc: *expr_agg.DistinctAcc,
        col: filter_eval.ColumnT([]const u8),
        sel: *const filter_selection.SelectionVector,
    ) !void {
        const codes = col.codes.?;
        if (!self.disabled and (self.dict == null or self.dict.? != col.dict.ptr or self.dict_len != col.dict.len)) {
            if (self.words.len != 0) scratch.free(self.words);
            self.words = &.{};
            self.dict = null;
            const n = col.dict.len / 64 + @intFromBool(col.dict.len % 64 != 0);
            self.words = scratch.alloc(u64, n) catch blk: {
                self.disabled = true;
                break :blk &.{};
            };
            if (!self.disabled) {
                @memset(self.words, 0);
                self.dict = col.dict.ptr;
                self.dict_len = col.dict.len;
            }
        }
        for (codes, 0..) |code, row| {
            if (!sel.isActive(row)) continue;
            if (col.def_levels) |def| if (def[row] < col.max_def) continue;
            const word = @as(usize, code) / 64;
            const mask = @as(u64, 1) << @truncate(code % 64);
            if (!self.disabled and self.words[word] & mask != 0) continue;
            // Publish the bit only after the exact table has accepted the string.
            acc.insertBytes(persist, col.dict[code]) catch |err| {
                if (self.disabled or (err != error.ExceededMemoryBudget and err != error.OutOfMemory)) return err;
                // The optional bitmap may be the only thing keeping the exact table from growing.
                self.deinit(scratch);
                self.disabled = true;
                try acc.insertBytes(persist, col.dict[code]);
            };
            if (!self.disabled) {
                self.words[word] |= mask;
                if (builtin.is_test) test_scalar_distinct_inserts += 1;
            }
        }
    }
};

test "scalar dictionary distinct caches observed codes and falls back on memory refusal" {
    const t = std.testing;
    const a = t.allocator;
    const dict = [_][]const u8{ "", "same", "same", "unused", "later" };
    const codes = [_]u32{ 0, 1, 0, 2, 3, 4, 4, 1 };
    const defs = [_]u32{ 1, 1, 0, 1, 0, 1, 1, 1 };
    const col: filter_eval.ColumnT([]const u8) = .{
        .values = &.{},
        .codes = &codes,
        .dict = &dict,
        .def_levels = &defs,
        .max_def = 1,
        .has_nulls = true,
    };
    var sel = try filter_selection.SelectionVector.init(a, codes.len);
    defer sel.deinit();
    for ([_]bool{ false, true }) |refuse| {
        var budget: core_memory.SharedBudget = .{ .limit = if (refuse) 1 else 4096 };
        var charged: core_memory.Charged = .{ .child = a, .budget = &budget };
        var seen: ScalarDistinctSeen = .{};
        var acc: expr_agg.DistinctAcc = .{};
        try seen.fold(charged.allocator(), a, &acc, col, &sel);
        try seen.fold(charged.allocator(), a, &acc, col, &sel);
        try t.expectEqual(refuse, seen.disabled);
        try t.expectEqual(@as(usize, 3), acc.count());
        if (!refuse) try t.expectEqual(@as(u64, 0b10111), seen.words[0]);
        acc.deinit(a);
        seen.deinit(charged.allocator());
        try t.expectEqual(@as(usize, 0), budget.used.load(.monotonic));
    }
    // Refusal by the exact table must not publish a seen bit or leak either reservation.
    var table_budget: core_memory.SharedBudget = .{ .limit = 1 };
    var seen: ScalarDistinctSeen = .{};
    defer seen.deinit(a);
    var acc = expr_agg.DistinctAcc.initEmpty(&table_budget);
    defer acc.deinit(a);
    try t.expectError(error.ExceededMemoryBudget, seen.fold(a, a, &acc, col, &sel));
    try t.expect(seen.disabled and seen.words.len == 0);
    try t.expectEqual(@as(usize, 0), table_budget.used.load(.monotonic));

    // Measure the exact set and arena charge for an empty string; the bitmap must be returned to make it fit.
    var quote_budget: core_memory.SharedBudget = .{ .limit = 4096 };
    var quote = expr_agg.DistinctAcc.initEmpty(&quote_budget);
    defer quote.deinit(a);
    try quote.insertBytes(a, "");
    quote.heap().?.releaseSlack();
    const exact = quote_budget.used.load(.monotonic);
    quote.deinit(a);
    try t.expectEqual(@as(usize, 0), quote_budget.used.load(.monotonic));
    var tight: core_memory.SharedBudget = .{ .limit = exact };
    var tight_scratch: core_memory.Charged = .{ .child = a, .budget = &tight };
    var tight_seen: ScalarDistinctSeen = .{};
    var tight_acc = expr_agg.DistinctAcc.initEmpty(&tight);
    defer tight_seen.deinit(tight_scratch.allocator());
    defer tight_acc.deinit(a);
    var one = try filter_selection.SelectionVector.init(a, 1);
    defer one.deinit();
    var first = col;
    first.codes = codes[0..1];
    first.def_levels = defs[0..1];
    try tight_seen.fold(tight_scratch.allocator(), a, &tight_acc, first, &one);
    try t.expect(tight_seen.disabled and tight_seen.words.len == 0);
    try t.expectEqual(@as(usize, 1), tight_acc.count());
    tight_acc.deinit(a);
    tight_seen.deinit(tight_scratch.allocator());
    try t.expectEqual(@as(usize, 0), tight.used.load(.monotonic));
}

test "scalar dictionary distinct reads mixed pages with nullable and unused entries" {
    const t = std.testing;
    const a = t.allocator;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const ar = arena.allocator();
    const Fixture = struct {
        fn page(out: *std.ArrayList(u8), alloc: std.mem.Allocator, body: []const u8, count: i32, dict: bool, enc: schema.Encoding) !void {
            var w = thrift.Writer.init(alloc);
            try (schema.PageHeader{
                .type = if (dict) .DICTIONARY_PAGE else .DATA_PAGE,
                .uncompressed_page_size = @intCast(body.len),
                .compressed_page_size = @intCast(body.len),
                .crc = null,
                .data_page_header_v2 = null,
                .data_page_header = if (dict) null else .{
                    .num_values = count,
                    .encoding = enc,
                    .definition_level_encoding = .RLE,
                    .repetition_level_encoding = .RLE,
                },
                .dictionary_page_header = if (dict) .{ .num_values = count, .encoding = .PLAIN, .is_sorted = null } else null,
            }).write(&w);
            try out.appendSlice(alloc, w.bytes());
            try out.appendSlice(alloc, body);
        }
        fn bytes(out: *std.ArrayList(u8), alloc: std.mem.Allocator, value: []const u8) !void {
            var size: [4]u8 = undefined;
            std.mem.writeInt(u32, &size, @intCast(value.len), .little);
            try out.appendSlice(alloc, &size);
            try out.appendSlice(alloc, value);
        }
        fn levels(out: *std.ArrayList(u8), alloc: std.mem.Allocator, defs: []const u32) !void {
            const encoded = try @import("parquet/encoding/hybrid_rle.zig").encode(alloc, defs, 1);
            var size: [4]u8 = undefined;
            std.mem.writeInt(u32, &size, @intCast(encoded.len), .little);
            try out.appendSlice(alloc, &size);
            try out.appendSlice(alloc, encoded);
        }
    };
    var chunk: std.ArrayList(u8) = .empty;
    var dictionary: std.ArrayList(u8) = .empty;
    try Fixture.bytes(&dictionary, ar, "");
    try Fixture.bytes(&dictionary, ar, "alpha");
    for (0..128) |i| try Fixture.bytes(&dictionary, ar, try std.fmt.allocPrint(ar, "unused-{d}", .{i}));
    try Fixture.page(&chunk, ar, dictionary.items, 130, true, .PLAIN);
    var defs: [128]u32 = undefined;
    var codes: std.ArrayList(u32) = .empty;
    for (&defs, 0..) |*d, i| {
        d.* = @intFromBool(i % 7 != 0);
        if (d.* != 0) try codes.append(ar, @intCast(i % 2));
    }
    var coded: std.ArrayList(u8) = .empty;
    try Fixture.levels(&coded, ar, &defs);
    try coded.append(ar, 8);
    try coded.appendSlice(ar, try @import("parquet/encoding/hybrid_rle.zig").encode(ar, codes.items, 8));
    try Fixture.page(&chunk, ar, coded.items, 128, false, .RLE_DICTIONARY);
    var plain_defs: [64]u32 = undefined;
    for (&plain_defs, 0..) |*d, i| d.* = @intFromBool(i % 9 != 0);
    var plain: std.ArrayList(u8) = .empty;
    try Fixture.levels(&plain, ar, &plain_defs);
    for (plain_defs, 0..) |d, i| if (d != 0) {
        try Fixture.bytes(&plain, ar, if (i % 2 == 0) "plain" else "alpha");
    };
    try Fixture.page(&chunk, ar, plain.items, 64, false, .PLAIN);
    var other_body: std.ArrayList(u8) = .empty;
    try Fixture.levels(&other_body, ar, &plain_defs);
    for (plain_defs, 0..) |d, i| if (d != 0) {
        try Fixture.bytes(&other_body, ar, if (i % 2 == 0) "new" else "alpha");
    };
    var plain_chunk: std.ArrayList(u8) = .empty;
    try Fixture.page(&plain_chunk, ar, other_body.items, 64, false, .PLAIN);

    const file = try metadata.readFileSlice("ci/fixtures/parquet/census_dictionary_review.parquet", a);
    defer a.free(file);
    var meta = try metadata.open(a, file);
    defer meta.deinit(a);
    var rg = meta.row_groups.items[0];
    rg.columns.items = try ar.dupe(schema.ColumnChunk, rg.columns.items);
    rg.num_rows = 192;
    const cm = &rg.columns.items[2].meta_data.?;
    cm.codec = .UNCOMPRESSED;
    cm.dictionary_page_offset = 0;
    cm.data_page_offset = 0;
    cm.total_compressed_size = @intCast(chunk.items.len);
    cm.total_uncompressed_size = @intCast(chunk.items.len);
    cm.num_values = 192;
    const input: AggRowGroup = .{
        .rg = &rg,
        .meta = &meta,
        .src = .{ .bytes = chunk.items, .byte_origin = 0 },
        .filter = null,
        .read = &.{ false, false, true, false },
        .consumed = &.{ false, false, true, false },
    };
    var plain_rg = rg;
    plain_rg.columns.items = try ar.dupe(schema.ColumnChunk, rg.columns.items);
    plain_rg.num_rows = 64;
    const plain_cm = &plain_rg.columns.items[2].meta_data.?;
    plain_cm.dictionary_page_offset = null;
    plain_cm.total_compressed_size = @intCast(plain_chunk.items.len);
    plain_cm.total_uncompressed_size = @intCast(plain_chunk.items.len);
    plain_cm.num_values = 64;
    var plain_input = input;
    plain_input.rg = &plain_rg;
    plain_input.src.bytes = plain_chunk.items;
    const call = [_]expr_agg.AggCall{.{
        .func = .count_distinct,
        .arg = .{ .col_ref = .{ .col_idx = 2, .physical_type = .BYTE_ARRAY, .expr_type = .str } },
        .where = null,
        .alias = "d",
        .result = .i64,
    }};
    for ([_]bool{ false, true }) |encoded| for ([_]usize{ 64, 128, 192 }) |window| {
        test_scalar_distinct_inserts = 0;
        var acc = [_]expr_agg.Accumulator{.{ .distinct = .{} }};
        defer acc[0].distinct.deinit(a);
        const target: AggTarget = .{ .scalar = .{ .calls = &call, .accumulators = &acc } };
        try t.expectEqual(@as(?usize, 2), scalarByteDistinct(input, target));
        var scratch = DecodeScratch.init(a);
        defer scratch.deinit();
        var region = RowGroupArena.init(a);
        defer region.deinit();
        var timings: Timings = .{};
        var budget: DecodeBudget = .{ .limit = 1 << 20 };
        const ws: AggWorkspace = .{
            .scratch = &scratch,
            .rg_arena = &region,
            .timings = &timings,
            .budget = &budget,
            .decode_batch_rows = window,
        };
        const count = try scanRGSliced(a, input, .{ .encoded = encoded }, target, ws, &budget, &.{false}, 8192, window);
        try t.expectEqual(@as(?u64, 192), count);
        try t.expectEqual(@as(usize, 3), acc[0].distinct.count());
        const next = try scanRGSliced(a, plain_input, .{ .encoded = encoded }, target, ws, &budget, &.{false}, 4096, window);
        try t.expectEqual(@as(?u64, 64), next);
        try t.expectEqual(@as(usize, 4), acc[0].distinct.count());
        try t.expectEqual(@as(usize, if (encoded) 2 else 0), test_scalar_distinct_inserts);
    };
}

/// `scanRGForAgg`'s decode and fold, a slice of rows at a time. Null when some column has no sliced form.
/// At the end of the group, retained scratch is at most one eighth of the ledger it is charged to. A group that
/// still does not fit is refused.
fn scanRGSliced(
    gpa: std.mem.Allocator,
    in: AggRowGroup,
    decode_options: DecodeOptions,
    target: AggTarget,
    ws: AggWorkspace,
    budget: *DecodeBudget,
    handled: []const bool,
    full: usize,
    stream_rows: usize,
) !?u64 {
    const num_rows: usize = if (in.subgroup) |subgroup| subgroup.rows.hi - subgroup.rows.lo else @intCast(@max(0, in.rg.num_rows));
    if (num_rows == 0) return null;
    const side = ws.charged orelse gpa;
    // Bitmap admission is optional; its refusal must not mark the decoder's allocator as refused.
    const seen_alloc = ws.planning_charged orelse side;
    const distinct_ci = if (decode_options.encoded) scalarByteDistinct(in, target) else null;
    var distinct_seen: ScalarDistinctSeen = .{};
    defer distinct_seen.deinit(seen_alloc);
    const pool = try poolOn(&ws.scratch.slice, side);
    // One eighth of the ledger this pool draws from. The next group can use the other seven eighths.
    const carry = core_memory.retainedCarry(if (ws.ledger) |l| l.limit else decode_options.expand_limit);
    defer pool.trimAbove(carry);
    var pages = std.heap.ArenaAllocator.init(side);
    defer pages.deinit();
    var readers: std.ArrayList(SliceReader) = .empty;
    defer for (readers.items) |*r| r.deinit();
    const lookup = try pages.allocator().alloc(?usize, in.meta.schema.items.len);
    @memset(lookup, null);
    // The columns the whole-chunk path reads as levels only (`decodeLevelsOnly`), read so here too.
    const uses = try planCodeUses(pages.allocator(), in, target, false);
    const levels_only = try pages.allocator().alloc(bool, in.read.len);
    for (levels_only, 0..) |*l, ci| l.* = decode_options.encoded and target == .scalar and ci < uses.levels.len and
        uses.levels[ci] and !uses.code[ci] and !uses.value[ci];
    // Payload columns are gated by the filter's survivors over the whole row group, as when it decodes whole.
    const payload = if (in.subgroup != null) null else try payloadColumns(pages.allocator(), in, target);
    var survivors: filter_selection.SelectionVector = undefined;
    if (payload) |p| survivors = (try filterSurvivors(
        pages.allocator(),
        side,
        in,
        decode_options,
        p,
        num_rows,
        ws,
        stream_rows,
    )) orelse
        return null;
    // The filter pass has released its readers. The same slots decode the payload.
    try openSlicedPayload(
        pages.allocator(),
        side,
        in,
        decode_options,
        target,
        pool,
        &readers,
        lookup,
        levels_only,
        payload,
        if (payload != null) &survivors else null,
        ws,
    ) orelse return null;
    const per_row = @max(1, full / num_rows);
    // Half of what the ledger has left, so another worker's slice still fits beside this one; slicing changes no
    // answer, only how many rows decode at once.
    const room = if (ws.ledger) |l| l.sliceAvailable() / 2 else std.math.maxInt(usize);
    const discovering = target == .grouped and target.grouped.table.discovery != null;
    const forced = if (ws.decode_batch_rows != 0 and (discovering or cacheExpressionsSafe(target)))
        ws.decode_batch_rows
    else
        std.math.maxInt(usize);
    const slice_rows = sliceRows(@min(@min(budget.share(), room) / per_row, @min(forced, stream_rows)), num_rows);
    const reserved = @min(budget.limit, slice_rows * per_row);
    try budget.acquire(reserved);
    defer budget.release(reserved);

    const prof = ws.timings.prof;
    const t0 = nowMonoNs();
    var done: usize = 0;
    // Under a ledger, slices are cut by what rows took, not the footer's estimate, which can understate values by any
    // factor (DELTA_BYTE_ARRAY copies a prefix into every one): the first slice is the least, each next is what the
    // reservation, or the ledger's room, holds at the bytes a row of the last one took.
    var rows_now = if (ws.ledger != null) @min(slice_rows, 64) else slice_rows;
    // One arena for every slice, its pages kept from one to the next rather than faulted in afresh; what a row took is
    // measured on the first, when the arena holds nothing else. The worker's own row-group arena when it is a region
    // (nothing is on it by now: the row group starts over here), whose blocks are what the slice asked for; an
    // `ArenaAllocator` reserves past that, and its reservation was charged as if used.
    const on_region = ws.region != null and ws.rg_arena.region != null;
    // Retained across row groups. A later group's first slice may see an earlier group's high water, so its
    // bytes-per-row comes out high and the next slice gets smaller; it does not come out low.
    const slice = pool.scanArena();
    defer _ = slice.reset(.retain_capacity);
    var took: ?usize = null;
    var matched: u64 = 0;
    while (done < num_rows) {
        var n = @min(rows_now, num_rows - done);
        if (distinct_ci) |ci| {
            const at = lookup[ci] orelse return error.BadColumn;
            const reader = &readers.items[at];
            const raw = &reader.raw.bytes;
            if (raw.options.any_dictionary) {
                enterChunk(reader.site.rg, reader.site.column);
                raw.options.fast_levels = true;
                // Keep a dictionary/PLAIN transition out of one window: otherwise the decoder has to expand the
                // window's dictionary prefix and the already-seen entries are hashed again as individual values.
                while (raw.pageValuesLeft() == 0) {
                    reader.resetPages();
                    raw.releaseExpansion();
                    reader.armBytePage(raw);
                    if (!try raw.nextPage()) return error.ShortDecode;
                    try reader.holdDictionary(raw);
                }
                n = @min(n, raw.pageValuesLeft());
            }
        }
        defer done += n;
        if (on_region) ws.rg_arena.nextRowGroup() else _ = slice.reset(.retain_capacity);
        defer if (ws.ledger) |l| {
            const kept = if (on_region) ws.region.?.end else slice.queryCapacity();
            if (took == null) took = @max(1, kept / n);
            rows_now = sliceRows(@min(@min(reserved, l.sliceAvailable() / 2 + kept) / took.?, forced), num_rows);
        };
        const sa = if (on_region) ws.rg_arena.allocator() else slice.allocator();
        const cols = try sa.alloc(filter_eval.Batch.Column, readers.items.len);
        for (readers.items, cols) |*r, *c| {
            c.* = r.next(sa, n) catch |err| {
                // A record slice may read pages that the ordinary indexed plan skips. Replay establishes its error.
                if (in.filter != null and target == .grouped and target.grouped.records != null and isDecodeError(err))
                    return error.RetainedEncodingUnsupported;
                return err;
            };
        }
        const batch: filter_eval.Batch = .{ .cols = cols, .num_rows = n, .expr_batch_rows = ws.expr_batch_rows };
        var sel = if (payload != null)
            try survivors.alignedWindow(done, n)
        else
            try filter_selection.SelectionVector.init(sa, n);
        if (payload == null) if (in.filter) |f| try filter_eval.evaluate(f, &batch, &sel, lookup, sa);
        matched += if (in.filter != null) sel.count() else n;
        const input_base = std.math.add(u64, in.row_base, done) catch return error.BadColumn;
        const window = try selectedWindow(&batch, &sel, input_base, null, n);
        switch (target) {
            .scalar => |s| {
                if (distinct_ci) |ci| if (!handled[0]) {
                    const at = lookup[ci] orelse return error.BadColumn;
                    const col = cols[at];
                    if (col == .string) {
                        if (col.string.codes != null) {
                            try distinct_seen.fold(seen_alloc, gpa, &s.accumulators[0].distinct, col.string, &sel);
                            if (distinct_seen.disabled) readers.items[at].raw.bytes.options.any_dictionary = false;
                            continue;
                        }
                        // A PLAIN page may follow a dictionary page within this chunk. The normal fold handles it
                        // exactly and subsequent pages can remain in the value lane.
                        readers.items[at].raw.bytes.options.any_dictionary = false;
                    }
                };
                if (ws.decode_batch_rows != 0 and cacheExpressionsSafe(target) and
                    try foldScalarCache(sa, gpa, s.calls, s.accumulators, handled, window.batch, lookup, &sel))
                    continue;
                if (try expr_agg.updateSharedInteger(sa, s.calls, s.accumulators, handled, window.batch, lookup, &sel))
                    continue;
                for (s.calls, s.accumulators, handled) |call, *acc, h| {
                    if (h) continue;
                    try expr_agg.updateOne(sa, gpa, acc, call, window.batch, lookup, &sel);
                }
            },
            .grouped => |g| {
                if (g.records) |records| {
                    const emit_start = nowMonoNs();
                    try emitRecords(sa, records, window.batch, &sel, window.input_base, done, lookup);
                    if (records.* == .msd) profile.record(prof, .msd_emit, emit_start, nowMonoNs(), profile.args(.{}));
                    continue;
                }
                if (g.part != null) {
                    var shifted = target;
                    shifted.grouped.pos_base = g.pos_base + @as(u64, @intCast(done));
                    try foldGroupedWindows(
                        sa,
                        gpa,
                        shifted,
                        cols,
                        lookup,
                        &sel,
                        in.consumed,
                        decode_options.encoded,
                        ws.expr_batch_rows,
                        ws.budget,
                        null,
                    );
                    continue;
                }
                if (!discovering and decode_options.encoded) if (g.dense) |d| {
                    if (try d.foldKeys(
                        sa,
                        gpa,
                        g.keys,
                        in.file_index,
                        in.rg_index,
                        g.pos_base + done,
                        window.batch,
                        lookup,
                        &sel,
                    )) continue;
                };
                if (!discovering and decode_options.encoded and ws.decode_batch_rows != 0 and
                    cacheExpressionsSafe(target))
                {
                    try agg_coded.codePlainKeys(sa, g.keys, cols, lookup);
                    if (try agg_coded.groupedCoded(
                        sa,
                        gpa,
                        g.calls,
                        g.keys,
                        g.table,
                        g.pos_base + done,
                        window.batch,
                        lookup,
                        &sel,
                        ws.budget,
                        null,
                    )) continue;
                }
                // A refused dictionary product may leave one key coded and another PLAIN. The ordinary key
                // resolver reads value lanes, so materialize before crossing that fallback boundary.
                for (cols) |*col| col.* = try agg_coded.materialize(sa, col.*);
                const key_cols = try sa.alloc(filter_eval.Batch.Column, g.keys.len);
                const keyed = batch.restrictedTo(&sel);
                for (g.keys, key_cols) |k, *kc| kc.* = try expr_eval.evalGroupKeyExpr(sa, &keyed, lookup, k);
                const ids = try sa.alloc(u32, n);
                @memset(ids, 0);
                var resolver = expr_agg.GroupKeyResolver.init(key_cols);
                defer resolver.deinit(sa);
                // First appearance counts rows from the row group's start, as when it decodes whole.
                try foldGroupedBatch(
                    sa,
                    gpa,
                    &resolver,
                    g.table,
                    g.calls,
                    key_cols,
                    window.batch,
                    lookup,
                    &sel,
                    ids,
                    g.pos_base + done,
                );
            },
        }
        if (discovering and target.grouped.table.discoveryDone(target.grouped.pos_base + done + n)) break;
    }
    const t1 = nowMonoNs();
    ws.timings.decode_ns += @intCast(t1 - t0);
    profile.record(prof, .decode, t0, t1, profile.args(.{ in.file_index, in.rg_index, readers.items.len }));
    return matched;
}

/// A staged row group's decode: the columns the outer filter reads (`base`), decoded first; then each aggregate's
/// own (`calls[i]`, less `base`), decoded for it alone and dropped after. A flat hash-group key is payload, gated
/// by the filter's survivors, on this plan as on the others.
const StagePlan = struct {
    base: []bool,
    calls: [][]bool,
    /// The most the plan holds at once: `base` and the largest call's columns.
    estimate: usize,

    fn init(a: std.mem.Allocator, in: AggRowGroup, target: AggTarget, handled: []const bool) !StagePlan {
        const n = in.read.len;
        const base = try baseColumns(a, in, target);
        const calls = target.planCalls();
        if (target == .grouped and target.grouped.table.countOnly()) {
            std.debug.assert(calls.len == 0);
        }
        const per_call = try a.alloc([]bool, calls.len);
        var widest: usize = 0;
        for (calls, per_call, 0..) |call, *cols, i| {
            cols.* = try a.alloc(bool, n);
            @memset(cols.*, false);
            if (i < handled.len and handled[i]) continue;
            if (call.arg) |arg| arg.collectColumns(cols.*);
            if (call.where) |w| {
                var wc: std.ArrayList(usize) = .empty;
                try w.collectColumns(&wc, a);
                for (wc.items) |ci| if (ci < n) {
                    cols.*[ci] = true;
                };
            }
            for (cols.*, in.read, base) |*c, r, b| c.* = c.* and r and !b;
            widest = @max(widest, decodeEstimate(in, cols.*, calls[i .. i + 1], false));
        }
        const base_bytes = decodeEstimate(in, base, &.{}, target == .grouped);
        return .{ .base = base, .calls = per_call, .estimate = base_bytes + widest };
    }
};

const CallBatch = struct { batch: filter_eval.Batch, lookup: []?usize };

/// The columns of `in.read` a row group needs before its filter has run: the filter's own. With no filter, the
/// group keys too. A flat key of a hash group under a filter is payload (`payloadColumns`), decoded only at the
/// survivors (or, when its dictionary covers the chunk, as codes for every row: `keyCodesCheaper`). A nested key has
/// no row gate, so it stays here. A record owner stays here too: its packed path needs the whole dictionary and
/// applies the survivor mask at the sink, which a row gate would replace with defaults.
fn baseColumns(a: std.mem.Allocator, in: AggRowGroup, target: AggTarget) ![]bool {
    const n = in.read.len;
    const base = try a.alloc(bool, n);
    @memset(base, false);
    if (in.filter) |f| {
        var cols: std.ArrayList(usize) = .empty;
        try f.collectColumns(&cols, a);
        for (cols.items) |ci| if (ci < n) {
            base[ci] = true;
        };
    }
    switch (target) {
        .grouped => |g| if (in.filter == null or g.records != null) {
            for (g.keys) |k| k.collectColumns(base);
        } else {
            const key_cols = try a.alloc(bool, n);
            defer a.free(key_cols);
            @memset(key_cols, false);
            for (g.keys) |k| k.collectColumns(key_cols);
            for (key_cols, 0..) |is_key, ci| {
                if (!is_key or base[ci] or ci >= in.rg.columns.items.len) continue;
                const cm = in.rg.columns.items[ci].meta_data orelse continue;
                if (in.meta.getColumnLevels(cm.path_in_schema.items).max_rep != 0) base[ci] = true;
            }
        },
        .scalar => {},
    }
    for (base, in.read) |*b, r| b.* = b.* and r;
    return base;
}

/// Payload columns: what a filtered row group reads past `baseColumns`, flat. That is an aggregate's columns and a
/// flat hash-group key, including the columns of a key expression. Every decode path (whole, compact, staged, sliced)
/// decodes them after the filter, gated by its survivors (`DecodeOptions.row_gate`), so which of their pages decode
/// depends on the input and the query alone. An owner query (`records`) is included: its payload is still not read
/// for rows the filter dropped. Null when there is no filter — nothing has selected rows yet — or when every read
/// column is already in the pre-filter set.
fn payloadColumns(a: std.mem.Allocator, in: AggRowGroup, target: AggTarget) !?[]const bool {
    if (in.filter == null) return null;
    const base = try baseColumns(a, in, target);
    var any = false;
    for (base, in.read, 0..) |*b, r, ci| {
        const flat = ci < in.rg.columns.items.len and if (in.rg.columns.items[ci].meta_data) |cm|
            in.meta.getColumnLevels(cm.path_in_schema.items).max_rep == 0
        else
            false;
        b.* = r and !b.* and flat;
        any = any or b.*;
    }
    return if (any) base else null;
}

const EmptyWant = enum { absent, empty, nonempty };

/// `ci` is only compared to an empty string, and every such comparison wants the same side. Null when `ci` is
/// tested any other way, so the caller decodes the bytes.
fn emptinessWant(f: filter_ast.Filter, ci: usize) ?EmptyWant {
    switch (f) {
        .string => |l| {
            if (l.col_idx != ci) return .absent;
            if (l.value.len != 0) return null;
            return switch (l.op) {
                .Eq => .empty,
                .NotEq => .nonempty,
                else => null,
            };
        },
        .and_filter, .or_filter => |c| {
            var found: EmptyWant = .absent;
            for (c.children) |child| {
                const want = emptinessWant(child, ci) orelse return null;
                if (want == .absent) continue;
                if (found != .absent and found != want) return null;
                found = want;
            }
            return found;
        },
        .like => |l| return if (l.col_idx == ci) null else .absent,
        .null_check => |l| return if (l.col_idx == ci) null else .absent,
        inline else => |l| return if (l.col_idx == ci) null else .absent,
    }
}

fn plainValueEncodings(encodings: []const schema.Encoding) bool {
    var saw_plain = false;
    for (encodings) |e| switch (e) {
        .PLAIN => saw_plain = true,
        .RLE, .BIT_PACKED => {},
        else => return false,
    };
    return saw_plain;
}

fn exprMentions(e: expr_ast.Expr, ci: usize) bool {
    return switch (e) {
        .literal => false,
        .col_ref => |c| c.col_idx == ci,
        .binop => |b| exprMentions(b.left.*, ci) or exprMentions(b.right.*, ci),
        .call => |c| for (c.args) |arg| {
            if (exprMentions(arg.*, ci)) break true;
        } else false,
        .case_ => |c| blk: {
            for (c.whens) |w| {
                if (filterMentions(w.cond, ci)) break :blk true;
                if (w.then) |t| if (exprMentions(t.*, ci)) break :blk true;
            }
            if (c.else_) |other| if (exprMentions(other.*, ci)) break :blk true;
            break :blk false;
        },
    };
}

fn filterMentions(f: filter_ast.Filter, ci: usize) bool {
    return switch (f) {
        .and_filter, .or_filter => |c| for (c.children) |child| {
            if (filterMentions(child, ci)) break true;
        } else false,
        inline else => |l| l.col_idx == ci,
    };
}

/// PLAIN and dictionary data pages only. Delta byte arrays expand into the arena, so they stay on the
/// whole-chunk decode.
fn reusableByteEncodings(encodings: []const schema.Encoding) bool {
    var saw = false;
    for (encodings) |e| switch (e) {
        .PLAIN, .RLE_DICTIONARY, .PLAIN_DICTIONARY => saw = true,
        .RLE, .BIT_PACKED => {},
        else => return false,
    };
    return saw;
}

fn bareColumn(e: expr_ast.Expr, ci: usize) bool {
    return e == .col_ref and e.col_ref.col_idx == ci;
}

/// The column is hashed and not kept as a value: a bare GROUP BY key, or `count(DISTINCT col)`, and neither
/// a record projection nor any other aggregate.
fn hashOnlyByteArray(target: AggTarget, ci: usize) bool {
    switch (target) {
        .scalar => |s| {
            var saw = false;
            for (s.calls) |call| {
                if (call.where) |w| if (filterMentions(w, ci)) return false;
                if (call.arg) |arg| {
                    if (exprMentions(arg, ci)) {
                        if (call.func != .count_distinct or !bareColumn(arg, ci)) return false;
                        saw = true;
                    }
                }
            }
            return saw;
        },
        .grouped => |g| {
            if (g.records != null) return false;
            var saw = false;
            for (g.keys) |k| {
                if (exprMentions(k, ci)) {
                    if (!bareColumn(k, ci)) return false;
                    saw = true;
                }
            }
            for (g.calls) |call| {
                if (call.where) |w| if (filterMentions(w, ci)) return false;
                if (call.arg) |arg| {
                    if (exprMentions(arg, ci)) {
                        if (call.func != .count_distinct or !bareColumn(arg, ci)) return false;
                        saw = true;
                    }
                }
            }
            return saw;
        },
    }
}

/// Filter-only or hash-only flat BYTE_ARRAY: the scan reads it a cache-sized window at a time instead of
/// decompressing the chunk into the row-group region.
fn streamByteArray(in: AggRowGroup, target: AggTarget, ci: usize) bool {
    if (ci >= in.read.len or !in.read[ci] or ci >= in.rg.columns.items.len) return false;
    const cm = in.rg.columns.items[ci].meta_data orelse return false;
    if (cm.type != .BYTE_ARRAY) return false;
    if (in.meta.getColumnLevels(cm.path_in_schema.items).max_rep != 0) return false;
    if (!reusableByteEncodings(cm.encodings.items)) return false;
    const filtered = if (in.filter) |f| filterMentions(f, ci) else false;
    // A filter-only string its dictionary covers is compared once per dictionary entry from its codes, a few bytes a
    // row: streaming it reads every value instead, and saves no faulted string bytes.
    if (filtered and !columnFeedsFold(target, ci))
        return !@import("filter/membership.zig").dictionaryCoversChunk(&cm);
    // DISTINCT sets and string winners reuse a whole batch's prepared values. Streaming a key forces their
    // payloads into repeated decoder/fold setup too; ordinary memory admission still slices a batch that cannot fit.
    if (target == .grouped) for (target.grouped.calls) |call| {
        if (call.func == .count_distinct or call.result == .bytes) return false;
    };
    // A complete dictionary already keeps hashed keys compact. Forcing it into string windows discards the
    // coded fold (including dense lanes) and repeatedly allocates expanded values. Mixed/PLAIN chunks still stream.
    if (target.codedKey(ci) and cacheDictionary(in, ci) != null) return false;
    return hashOnlyByteArray(target, ci);
}

/// Rows of the widest streamed string that fit in one worker's L2. `maxInt` when nothing streams, so the
/// caller keeps the window it already chose. At least one float-sum block: a shorter window would split a block.
fn streamWindowRows(in: AggRowGroup, target: AggTarget, num_rows: usize) usize {
    var per: usize = 0;
    for (in.read, 0..) |needed, ci| {
        if (!needed or !streamByteArray(in, target, ci)) continue;
        const cm = in.rg.columns.items[ci].meta_data orelse continue;
        const n: usize = @intCast(@max(cm.num_values, 1));
        const bytes: usize = @intCast(@max(cm.total_uncompressed_size, 1));
        per = @max(per, @max(1, bytes / n));
    }
    if (per == 0) return std.math.maxInt(usize);
    const cache = @import("hw.zig").l2BytesPerWorkerOr(1 << 20);
    return @max(64, @min(num_rows, cache / per));
}

/// The fold reads `ci` after the filter: a group key, an aggregate argument, or a per-aggregate FILTER.
/// A top-level AND's filter columns decoded one conjunct at a time: a column only its single-column leaves read
/// decodes after the other base columns, at the survivors of every conjunct whose columns are decoded by then
/// (`DecodeOptions.row_gate`), so a page none of them keeps is never decompressed. Conjuncts evaluate in the plan's
/// order as their columns arrive; AND only clears bits, so the answer is the whole filter's. Which pages skip
/// depends on the input and the query alone. A chunk its dictionary covers keeps its coded evaluation, and a
/// length-only, page-indexed, staged, compacted or record-owned decode keeps the established whole-column path.
const GatedFilter = struct {
    children: []const filter_ast.Filter,
    /// Base columns in decode order: the others in column order, then the deferred by the first conjunct naming them.
    order: []usize,
    deferred: []bool,
    done: []bool,

    fn init(
        a: std.mem.Allocator,
        in: AggRowGroup,
        target: AggTarget,
        page_indexes: []const ?PageIndex,
    ) !?GatedFilter {
        const filter = in.filter orelse return null;
        if (filter != .and_filter) return null;
        const c = filter.and_filter;
        if (c.in_set != null or c.null_member or c.children.len < 2) return null;
        if (target == .grouped and target.grouped.records != null) return null;
        const n = in.read.len;
        const deferred = try a.alloc(bool, n);
        @memset(deferred, false);
        const first = try a.alloc(usize, n);
        @memset(first, std.math.maxInt(usize));
        const marks = try a.alloc(bool, n);
        for (c.children, 0..) |child, k| {
            @memset(marks, false);
            filter_ast.markColumns(child, marks);
            for (marks, 0..) |m, ci| if (m and first[ci] == std.math.maxInt(usize)) {
                first[ci] = k;
            };
        }
        var any = false;
        for (0..n) |ci| {
            if (first[ci] == std.math.maxInt(usize) or !in.read[ci] or ci >= in.rg.columns.items.len) continue;
            if (first[ci] == 0 or page_indexes[ci] != null) continue;
            const cm = in.rg.columns.items[ci].meta_data orelse continue;
            if (in.meta.getColumnLevels(cm.path_in_schema.items).max_rep != 0) continue;
            if (@import("filter/membership.zig").dictionaryCoversChunk(&cm)) continue;
            if (emptinessWant(filter, ci)) |want| if (want != .absent) continue;
            const leaves_only = for (c.children) |child| {
                @memset(marks, false);
                filter_ast.markColumns(child, marks);
                if (marks[ci] and filter_ast.leafColumn(child) != ci) break false;
            } else true;
            if (!leaves_only) continue;
            deferred[ci] = true;
            any = true;
        }
        if (!any) return null;
        const order = try a.alloc(usize, n);
        var len: usize = 0;
        for (0..n) |ci| if (!deferred[ci]) {
            order[len] = ci;
            len += 1;
        };
        for (c.children, 0..) |_, k| for (0..n) |ci| if (deferred[ci] and first[ci] == k) {
            order[len] = ci;
            len += 1;
        };
        std.debug.assert(len == n);
        const done = try a.alloc(bool, c.children.len);
        @memset(done, false);
        return .{ .children = c.children, .order = order, .deferred = deferred, .done = done };
    }

    /// Evaluate, in order, every conjunct not yet evaluated whose columns are all decoded; with `all`, the rest too,
    /// failing as the whole filter would on a column that never decoded.
    fn evaluate(
        g: *GatedFilter,
        batch: *const filter_eval.Batch,
        sel: *filter_selection.SelectionVector,
        lookup: []const ?usize,
        a: std.mem.Allocator,
        all: bool,
    ) !void {
        const marks = try a.alloc(bool, lookup.len);
        for (g.children, g.done) |child, *done| {
            if (done.*) continue;
            @memset(marks, false);
            filter_ast.markColumns(child, marks);
            const ready = for (marks, lookup) |m, pos| {
                if (m and pos == null) break false;
            } else true;
            if (!ready and !all) continue;
            try filter_eval.evaluate(child, batch, sel, lookup, a);
            done.* = true;
        }
    }
};

/// Whether a group key's codes feed a fold that groups by them: dense lanes, a record owner keeping the original
/// dictionary, or the coded fold, which needs every key coded, so every flat key's chunk must be one its dictionary
/// covers. Survivor values would send every row through key serialization and hashing instead, which costs far more
/// than the codes `keyCodesCheaper` weighs. Keys bound for hash grouping anyway are left to that rule.
fn foldWantsCodes(target: AggTarget, ci: usize, rg: *const schema.RowGroup) bool {
    if (target != .grouped) return false;
    if (!target.codedKey(ci)) return target.grouped.dense != null;
    if (target.grouped.dense != null or target.grouped.records != null) return true;
    for (target.grouped.keys) |k| {
        if (k != .col_ref) return false;
        const kc = k.col_ref.col_idx;
        if (kc >= rg.columns.items.len) return false;
        const km = rg.columns.items[kc].meta_data orelse return false;
        if (!@import("filter/membership.zig").dictionaryCoversChunk(&km)) return false;
    }
    return true;
}

fn flatGroupKey(target: AggTarget, ci: usize) bool {
    if (target != .grouped) return false;
    for (target.grouped.keys) |k| if (k == .col_ref and k.col_ref.col_idx == ci) return true;
    return false;
}

/// Whether a filtered group's dictionary-covered key costs less as its codes for every row than as values at the
/// filter's survivors, by what each materializes: a four-byte code a row, against a survivor's value (a string's
/// slice, which the fold codes again, or the physical width).
fn keyCodesCheaper(cm: schema.ColumnMetaData, rows: usize, survivors: usize) bool {
    const value_bytes: usize = switch (cm.type) {
        .BYTE_ARRAY, .FIXED_LEN_BYTE_ARRAY => @sizeOf([]const u8),
        .INT96 => 12,
        .INT64, .DOUBLE => 8,
        else => 4,
    };
    return rows *| @sizeOf(u32) <= survivors *| value_bytes;
}

fn columnFeedsFold(target: AggTarget, ci: usize) bool {
    switch (target) {
        .scalar => |s| for (s.calls) |call| {
            if (call.arg) |arg| if (exprMentions(arg, ci)) return true;
            if (call.where) |w| if (filterMentions(w, ci)) return true;
        },
        .grouped => |g| {
            for (g.keys) |k| if (exprMentions(k, ci)) return true;
            for (g.calls) |call| {
                if (call.arg) |arg| if (exprMentions(arg, ci)) return true;
                if (call.where) |w| if (filterMentions(w, ci)) return true;
            }
            if (g.records != null) return true;
        },
    }
    return false;
}

/// PLAIN BYTE_ARRAY lengths when `ci`'s only predicate is `= ''` or `<> ''`. Null when the chunk is not that
/// shape; the caller decodes values. The payloads are not copied.
fn plainLengthColumn(
    a: std.mem.Allocator,
    in: AggRowGroup,
    ci: usize,
    chunk: []const u8,
    cm: schema.ColumnMetaData,
    options: DecodeOptions,
) !?filter_eval.Batch.Column {
    if (cm.type != .BYTE_ARRAY) return null;
    const filter = in.filter orelse return null;
    const want = emptinessWant(filter, ci) orelse return null;
    if (want == .absent) return null;
    if (!plainValueEncodings(cm.encodings.items)) return null;
    const levels = in.meta.getColumnLevels(cm.path_in_schema.items);
    if (levels.max_rep != 0) return null;
    const n: usize = @intCast(cm.num_values);
    // The caller's options already carry the chunk's inflate cap. An empty literal would decode a page the
    // value path rejects for claiming more bytes than the chunk.
    var reader = column_mod.ColumnChunkReader([]const u8).initWithOptions(chunk, cm.codec, levels, a, options, null);
    const got = reader.readPlainLengths(n) catch |err| switch (err) {
        error.UnsupportedEncoding => return null,
        else => return err,
    };
    return .{ .string = .{
        .values = &.{},
        .byte_lengths = got.lengths,
        .def_levels = got.def_levels,
        .max_def = @intCast(levels.max_def),
        .has_nulls = got.has_nulls,
    } };
}

/// Decodes one row group's column chunks into batch lanes, the way every column of `in.read` is decoded.
const ChunkDecoder = struct {
    in: AggRowGroup,
    uses: CodeUses,
    page_indexes: []const ?PageIndex,
    filter_pages: FilterPages,
    num_rows: usize,
    decode_options: DecodeOptions,
    ws: AggWorkspace,
    target: AggTarget,
    /// Decode just these rows (`compactRows`); `rows_out` is their count.
    keep: ?[]const RowRange = null,
    rows_out: ?usize = null,
    /// `payloadColumns`, gated by `gate` (the filter's survivors, in batch rows) once the filter has run.
    payload: ?[]const bool = null,
    gate: ?*const filter_selection.SelectionVector = null,
    /// Payload columns decoded for every row after all, ungated (`keyCodesCheaper`).
    whole: ?[]const bool = null,
    /// Filter columns decoded at the survivors of the conjuncts already evaluated (`GatedFilter`).
    filter_gated: ?[]const bool = null,
    /// The pre-filter pass may decode a PLAIN `<> ''` / `= ''` column as lengths. A later pass (`refill`) reads
    /// the bytes the fold still needs, and only on the rows the filter kept.
    allow_lengths: bool = true,
    refill: bool = false,
    pages_total: usize = 0,
    pages_skipped: usize = 0,

    fn column(
        self: *ChunkDecoder,
        a: std.mem.Allocator,
        ci: usize,
        sel: *const filter_selection.SelectionVector,
    ) !filter_eval.Batch.Column {
        const in = self.in;
        enterChunk(in.rg, ci);
        const prof = self.ws.timings.prof;
        const cm = in.rg.columns.items[ci].meta_data orelse return error.ColumnMetaMissing;
        const start: usize = if (cm.dictionary_page_offset) |dp| @intCast(dp) else @intCast(cm.data_page_offset);
        const len: usize = @intCast(cm.total_compressed_size);
        invariant.assert(start >= @as(usize, @intCast(in.src.byte_origin)));
        const buf_off = start - @as(usize, @intCast(in.src.byte_origin));
        if (buf_off + len > in.src.bytes.len) return error.MissingChunkBytes;
        const chunk = in.src.bytes[buf_off .. buf_off + len];

        const chunk_span = profile.Span.begin(prof);
        var prune_info: ?PruningInfo = if (self.page_indexes[ci]) |pi|
            try pagePruning(a, pi, self.filter_pages, ci, if (in.preserve) |p| p[ci] else true, sel, self.num_rows, cm, start)
        else
            null;
        if (prune_info) |*p| p.keep = self.keep;
        // Encoded execution on whole chunks: a column only counted
        // reads its levels; one only bare aggregates read, over every row, folds straight into a histogram of its
        // dictionary codes; one filters, keys or aggregates read decodes to codes they evaluate once per entry.
        const uses = self.uses;
        var decode_options = column_mod.chunkOptions(self.decode_options, in.meta, cm);
        if (self.payload) |p| if (p[ci] and !(if (self.whole) |w| w[ci] else false)) {
            std.debug.assert(self.gate != null);
            decode_options.row_gate = self.gate;
        };
        if (self.refill) {
            if (self.gate) |g| decode_options.row_gate = g;
        }
        if (self.filter_gated) |fg| if (fg[ci]) {
            std.debug.assert(self.gate != null);
            decode_options.row_gate = self.gate;
        };
        decode_options.any_dictionary = self.target.codedKey(ci);
        decode_options.decimal_exact_only = self.target.exactInputs();
        const pruned_pages = if (prune_info) |p|
            std.mem.indexOfScalar(bool, p.page_is_skipped, true) != null
        else
            false;
        if (self.target == .grouped) if (self.target.grouped.records) |records| if (records.retention(ci)) |owner| {
            var reader = (try SliceReader.init(a, a, in.rg, in.meta, in.src, ci, decode_options)).?;
            defer reader.deinit();
            reader.retainPayloads(owner);
            reader.raw.bytes.pages.prof = prof;
            const decoded = reader.next(a, self.num_rows) catch |err| {
                // Retention reads complete pages. The ordinary pruned replay decides whether a corrupt page matters.
                if (pruned_pages and isDecodeError(err)) return error.RetainedEncodingUnsupported;
                return err;
            };
            try checkRowShape(decoded, self.num_rows);
            if (chunk_span.t != null) chunk_span.end(.column_chunk, chunkArgs(in.file_index, in.rg_index, ci, cm));
            return decoded;
        };
        if (self.allow_lengths and !self.refill and self.keep == null and !pruned_pages) {
            if (try plainLengthColumn(a, in, ci, chunk, cm, decode_options)) |col| {
                try checkRowShape(col, self.rows_out orelse self.num_rows);
                if (chunk_span.t != null) chunk_span.end(.column_chunk, chunkArgs(in.file_index, in.rg_index, ci, cm));
                return col;
            }
        }
        const scratch = self.ws.scratch;
        const packed_codes = self.target == .grouped and if (self.target.grouped.records) |r|
            r.* == .selected or (r.* == .integer_count_top and r.integer_count_top.packing != null)
        else
            false;
        // Packed owners need the original dictionary. Read complete codes and apply the survivor mask at the sink;
        // pruned replay remains authoritative for failures on pages the query does not need.
        const keyed_whole = decode_options.any_dictionary and self.keep == null and
            (!pruned_pages or packed_codes);
        const encoded = decode_options.encoded and (prune_info == null or keyed_whole) and ci < uses.code.len and
            decode_options.row_gate == null;
        const levels_only = encoded and uses.levels[ci] and !uses.code[ci] and !uses.value[ci] and
            self.target == .scalar;
        const firsts = self.target == .grouped;
        const hist_col: ?filter_eval.Batch.Column = if (encoded and !levels_only and uses.code[ci] and
            uses.hist_ok[ci] and !uses.value[ci])
            try decodeAggColumnHist(a, chunk, cm, in.meta, decode_options, scratch, self.num_rows, firsts)
        else
            null;
        var read_whole_codes = false;
        var decoded = if (hist_col) |hc|
            hc
        else if (levels_only)
            try decodeLevelsOnly(a, chunk, cm, in.meta, decode_options, scratch, self.num_rows)
        else if (encoded and (uses.code[ci] or uses.levels[ci])) coded: {
            const col = decodeAggColumnCoded(a, chunk, cm, in.meta, decode_options, scratch) catch |err| {
                if (!packed_codes or !pruned_pages or !isDecodeError(err)) return err;
                enterChunk(in.rg, ci);
                break :coded try decodeAggColumn(a, chunk, cm, in.meta, prune_info, decode_options, scratch);
            };
            read_whole_codes = col != null;
            break :coded col orelse try decodeAggColumn(a, chunk, cm, in.meta, prune_info, decode_options, scratch);
        } else try decodeAggColumn(a, chunk, cm, in.meta, prune_info, decode_options, scratch);
        if (self.target == .grouped and self.target.grouped.table.countOnly() and !in.consumed[ci] and decoded.codes() != null) {
            std.debug.assert(!uses.value[ci]);
        }
        if (ci < uses.value.len and uses.value[ci]) decoded = try agg_coded.materialize(a, decoded);
        try checkRowShape(decoded, self.rows_out orelse self.num_rows);
        if (chunk_span.t != null) {
            chunk_span.end(.column_chunk, chunkArgs(in.file_index, in.rg_index, ci, cm));
            if (prune_info) |pi| {
                self.pages_total += pi.page_is_skipped.len;
                if (!read_whole_codes) self.pages_skipped += std.mem.count(bool, pi.page_is_skipped, &.{true});
            }
        }
        return decoded;
    }

    /// `batch` with call `i`'s own columns (`plan.calls[i]`) decoded beside it, on `a`.
    fn withCallColumns(
        self: *ChunkDecoder,
        a: std.mem.Allocator,
        plan: StagePlan,
        i: usize,
        batch: *const filter_eval.Batch,
        lookup: []const ?usize,
        sel: *const filter_selection.SelectionVector,
        values: bool,
    ) !CallBatch {
        var cols: std.ArrayList(filter_eval.Batch.Column) = .empty;
        try cols.appendSlice(a, batch.cols);
        const lk = try a.dupe(?usize, lookup);
        for (plan.calls[i], 0..) |needed, ci| {
            if (!needed or (ci < lk.len and lk[ci] != null)) continue;
            var col = try self.column(a, ci, sel);
            // The grouped row fold reads values, not codes.
            if (values) col = try agg_coded.materialize(a, col);
            lk[ci] = cols.items.len;
            try cols.append(a, col);
        }
        return .{ .batch = .{ .cols = cols.items, .num_rows = batch.num_rows }, .lookup = lk };
    }
};

const chunkArgs = types.chunkArgs;

/// Which columns a coded decode serves (`code`) and which still need materialized values (`value`).
/// Rows per slice of a row group read `fit` rows at a time: a multiple of 64, so every slice starts on a block of
/// the float sum order (`agg.sumBlocksF64`) and slicing changes no bit of a float sum or avg.
fn sliceRows(fit: usize, num_rows: usize) usize {
    return @min(num_rows, @max(64, fit / 64 * 64));
}

const CodeUses = struct { code: []bool, value: []bool, levels: []bool, hist_ok: []bool };

fn planCodeUses(ra: std.mem.Allocator, in: AggRowGroup, target: AggTarget, staged: bool) !CodeUses {
    const n = in.read.len;
    const u: CodeUses = .{
        .code = try ra.alloc(bool, n),
        .value = try ra.alloc(bool, n),
        .levels = try ra.alloc(bool, n),
        .hist_ok = try ra.alloc(bool, n),
    };
    @memset(u.code, false);
    @memset(u.value, false);
    @memset(u.levels, false);
    // A column folds straight into a histogram when every row is selected and only unfiltered scalar aggregates
    // read it as a bare argument.
    @memset(u.hist_ok, in.filter == null and target == .scalar);
    // So does a lone GROUP BY key that only counts read, its groups resolved from the histogram
    // (`agg_coded.groupedCoded`). A staged, windowed, sketched or owned-record fold needs per-row identities.
    if (in.filter == null and !staged) switch (target) {
        .grouped => |g| if (g.records == null and g.part == null and g.table.sketch == null and
            g.table.discovery == null)
        {
            if (agg_coded.histGroupKey(g.calls, g.keys)) |key| if (key < n) {
                u.hist_ok[key] = true;
            };
        },
        .scalar => {},
    };
    if (target == .scalar) for (target.calls()) |call| {
        if (call.where) |w| markFilterSet(u.hist_ok, w, false);
        if (call.arg) |arg| {
            if (call.where != null or arg != .col_ref or agg_coded.floatSum(call)) markExprSet(u.hist_ok, arg, false);
        }
    };
    if (in.filter) |f| markFilter(u, f);
    for (target.planCalls()) |call| {
        if (call.where) |w| markFilter(u, w);
        if (call.arg) |arg| switch (arg) {
            .col_ref => |c| mark(if (call.func == .count and target == .scalar) u.levels else u.code, c.col_idx),
            // A scalar fold evaluates a computed argument itself, gathering coded columns (`agg_coded.gatherSelected`).
            else => if (target == .scalar) arg.collectColumns(u.code) else markExprValues(u, arg),
        };
    }
    switch (target) {
        .grouped => |g| for (g.keys) |k| switch (k) {
            .col_ref => |c| mark(u.code, c.col_idx),
            else => markExprValues(u, k),
        },
        .scalar => {},
    }
    return u;
}

fn markFilterSet(set: []bool, f: filter_ast.Filter, v: bool) void {
    switch (f) {
        .and_filter, .or_filter => |c| for (c.children) |child| markFilterSet(set, child, v),
        inline else => |l| if (l.col_idx < set.len) {
            set[l.col_idx] = v;
        },
    }
}

fn markExprSet(set: []bool, e: expr_ast.Expr, v: bool) void {
    switch (e) {
        .literal => {},
        .col_ref => |c| if (c.col_idx < set.len) {
            set[c.col_idx] = v;
        },
        .binop => |b| {
            markExprSet(set, b.left.*, v);
            markExprSet(set, b.right.*, v);
        },
        .call => |c| for (c.args) |a| markExprSet(set, a.*, v),
        .case_ => |c| {
            for (c.whens) |w| {
                markFilterSet(set, w.cond, v);
                if (w.then) |t| markExprSet(set, t.*, v);
            }
            if (c.else_) |other| markExprSet(set, other.*, v);
        },
    }
}

fn mark(set: []bool, ci: usize) void {
    if (ci < set.len) set[ci] = true;
}

fn markFilter(u: CodeUses, f: filter_ast.Filter) void {
    switch (f) {
        .and_filter, .or_filter => |c| for (c.children) |child| markFilter(u, child),
        .int32 => |l| mark(u.code, l.col_idx),
        .int64 => |l| mark(u.code, l.col_idx),
        .uint64 => |l| mark(u.code, l.col_idx),
        .float => |l| mark(u.code, l.col_idx),
        .double => |l| mark(u.code, l.col_idx),
        .decimal => |l| mark(u.code, l.col_idx),
        .string => |l| mark(u.code, l.col_idx),
        .boolean => |l| mark(u.value, l.col_idx),
        .null_check => |l| mark(u.levels, l.col_idx),
        .like => |l| mark(u.code, l.col_idx),
    }
}

fn markExprValues(u: CodeUses, e: expr_ast.Expr) void {
    switch (e) {
        .literal => {},
        .col_ref => |c| mark(u.value, c.col_idx),
        .binop => |b| {
            markExprValues(u, b.left.*);
            markExprValues(u, b.right.*);
        },
        .call => |c| for (c.args) |a| markExprValues(u, a.*),
        // A condition reads values too: the CASE evaluates it over the row values it gives its branches.
        .case_ => |c| {
            for (c.whens) |w| {
                markFilterSet(u.value, w.cond, true);
                if (w.then) |t| markExprValues(u, t.*);
            }
            if (c.else_) |other| markExprValues(u, other.*);
        },
    }
}

/// `decodeAggColumn` as dictionary codes, or null when the chunk's type or encoding has no coded form here.
fn decodeAggColumnCoded(
    ra: std.mem.Allocator,
    chunk: []const u8,
    cm: schema.ColumnMetaData,
    meta: *const schema.FileMetaData,
    decode_options: DecodeOptions,
    scratch: *DecodeScratch,
) !?filter_eval.Batch.Column {
    const levels = meta.getColumnLevels(cm.path_in_schema.items);
    if (levels.max_rep > 0) return null;
    const n_leaves: usize = @intCast(cm.num_values);
    const schema_elem = meta.getColumnSchema(cm.path_in_schema.items);
    const dec_kind: ?decimal_mod.Kind = if (schema_elem) |se| decimal_mod.exactKind(&se) else null;
    if (dec_kind) |k| {
        return switch (k.physical) {
            inline .INT32, .INT64 => |p| blk: {
                const T = if (p == .INT32) i32 else i64;
                const r = (try decodeCodes(T, ra, chunk, cm.codec, levels, n_leaves, decode_options, scratch)) orelse
                    break :blk null;
                switch (r) {
                    .codes => |c| {
                        const d = try ra.alloc(f64, c.dict.len);
                        decimal_mod.applyScaleSimd(T, c.dict, k.scale, d);
                        var col = codedCol(f64, c, d, levels);
                        col.dict_unscaled = @unionInit(filter_eval.Exact, @typeName(T), c.dict);
                        break :blk .{ .f64 = col };
                    },
                    .values => |v| {
                        const d = try ra.alloc(f64, v.values.len);
                        decimal_mod.applyScaleSimd(T, v.values, k.scale, d);
                        var col = scaledCol(f64, d, v, levels);
                        col.unscaled = @unionInit(filter_eval.Exact, @typeName(T), v.values);
                        break :blk .{ .f64 = col };
                    },
                }
            },
            else => null,
        };
    }
    if (schema_elem != null and schema.isFloat16(schema_elem.?)) return null;
    return switch (cm.type) {
        .INT32 => blk: {
            if (schema_elem != null and schema.isUnsignedIntTo32(schema_elem.?)) {
                const r = (try decodeCodes(i32, ra, chunk, cm.codec, levels, n_leaves, decode_options, scratch)) orelse
                    break :blk null;
                switch (r) {
                    .codes => |c| {
                        const d = try ra.alloc(i64, c.dict.len);
                        for (d, c.dict) |*o, v| o.* = @as(u32, @bitCast(v));
                        break :blk .{ .i64 = codedCol(i64, c, d, levels) };
                    },
                    // Rare (unsigned INT32 whose dictionary overflowed): decode again the ordinary way.
                    .values => break :blk null,
                }
            }
            break :blk try codedOrValues(i32, .i32, ra, chunk, cm, levels, n_leaves, decode_options, scratch);
        },
        .INT64 => try codedOrValues(i64, .i64, ra, chunk, cm, levels, n_leaves, decode_options, scratch),
        .FLOAT => try codedOrValues(f32, .f32, ra, chunk, cm, levels, n_leaves, decode_options, scratch),
        .DOUBLE => try codedOrValues(f64, .f64, ra, chunk, cm, levels, n_leaves, decode_options, scratch),
        .BYTE_ARRAY => try codedOrValues([]const u8, .string, ra, chunk, cm, levels, n_leaves, decode_options, scratch),
        else => null,
    };
}

const codedOrValues = consumer_column.codedOrValues;

/// `decodeAggColumnCoded` folded straight into a code histogram, or null when the chunk doesn't qualify (the caller
/// then decodes it afresh). Decimal and unsigned-INT32 dictionaries convert once per entry. `firsts` (a GROUP BY key,
/// `agg_coded.histGroupKey`) also notes each entry's first row, for keys typed exactly as their codes decode.
fn decodeAggColumnHist(
    ra: std.mem.Allocator,
    chunk: []const u8,
    cm: schema.ColumnMetaData,
    meta: *const schema.FileMetaData,
    decode_options: DecodeOptions,
    scratch: *DecodeScratch,
    num_rows: usize,
    firsts: bool,
) !?filter_eval.Batch.Column {
    const levels = meta.getColumnLevels(cm.path_in_schema.items);
    if (levels.max_rep > 0) return null;
    const n_leaves: usize = @intCast(cm.num_values);
    const schema_elem = meta.getColumnSchema(cm.path_in_schema.items);
    const dec_kind: ?decimal_mod.Kind = if (schema_elem) |se| decimal_mod.exactKind(&se) else null;
    if (firsts) {
        if (dec_kind != null) return null;
        // The reader declines a key chunk at its first null row, after allocating and folding up to it, and the chunk
        // then decodes afresh; so only try a nullable column whose statistics count no nulls (a wrong count costs
        // that retry, never an answer).
        if (levels.max_def > 0) {
            const nulls = if (cm.statistics) |st| st.null_count else null;
            if (nulls == null or nulls.? != 0) return null;
        }
        switch (cm.type) {
            .INT64, .BYTE_ARRAY => {},
            .INT32 => if (schema_elem != null and schema.isUnsignedIntTo32(schema_elem.?)) return null,
            else => return null,
        }
    }
    if (dec_kind) |k| {
        return switch (k.physical) {
            inline .INT32, .INT64 => |p| blk: {
                const T = if (p == .INT32) i32 else i64;
                const r = (try histOf(T, ra, chunk, cm.codec, levels, n_leaves, decode_options, scratch, firsts)) orelse
                    break :blk null;
                switch (r) {
                    .hist => |h| {
                        const d = try ra.alloc(f64, h.dict.len);
                        decimal_mod.applyScaleSimd(T, h.dict, k.scale, d);
                        var col = histCol(f64, h, d, num_rows);
                        col.dict_unscaled = @unionInit(filter_eval.Exact, @typeName(T), h.dict);
                        break :blk .{ .f64 = col };
                    },
                    .values => |v| {
                        const d = try ra.alloc(f64, v.values.len);
                        decimal_mod.applyScaleSimd(T, v.values, k.scale, d);
                        var col = scaledCol(f64, d, v, levels);
                        col.unscaled = @unionInit(filter_eval.Exact, @typeName(T), v.values);
                        break :blk .{ .f64 = col };
                    },
                }
            },
            else => null,
        };
    }
    if (schema_elem != null and schema.isFloat16(schema_elem.?)) return null;
    return switch (cm.type) {
        .INT32 => blk: {
            const r = (try histOf(i32, ra, chunk, cm.codec, levels, n_leaves, decode_options, scratch, firsts)) orelse
                break :blk null;
            const unsigned = schema_elem != null and schema.isUnsignedIntTo32(schema_elem.?);
            switch (r) {
                .hist => |h| {
                    if (unsigned) {
                        const d = try ra.alloc(i64, h.dict.len);
                        for (d, h.dict) |*o, v| o.* = @as(u32, @bitCast(v));
                        break :blk .{ .i64 = histCol(i64, h, d, num_rows) };
                    }
                    break :blk .{ .i32 = histCol(i32, h, h.dict, num_rows) };
                },
                .values => |v| {
                    if (unsigned) {
                        const d = try ra.alloc(i64, v.values.len);
                        for (d, v.values) |*o, x| o.* = @as(u32, @bitCast(x));
                        break :blk .{ .i64 = scaledCol(i64, d, v, levels) };
                    }
                    break :blk .{ .i32 = valuesCol(i32, v, levels) };
                },
            }
        },
        inline .INT64, .FLOAT, .DOUBLE, .BYTE_ARRAY => |t| blk: {
            const T = switch (t) {
                .INT64 => i64,
                .FLOAT => f32,
                .DOUBLE => f64,
                else => []const u8,
            };
            const r = (try histOf(T, ra, chunk, cm.codec, levels, n_leaves, decode_options, scratch, firsts)) orelse
                break :blk null;
            const tag = switch (t) {
                .INT64 => "i64",
                .FLOAT => "f32",
                .DOUBLE => "f64",
                else => "string",
            };
            break :blk switch (r) {
                .hist => |h| @unionInit(filter_eval.Batch.Column, tag, histCol(T, h, h.dict, num_rows)),
                .values => |v| @unionInit(filter_eval.Batch.Column, tag, valuesCol(T, v, levels)),
            };
        },
        else => null,
    };
}

fn histCol(comptime T: type, h: anytype, dict: []const T, num_rows: usize) filter_eval.ColumnT(T) {
    return .{
        .values = &.{},
        .dict = dict,
        .hist = h.hist,
        .levels_only_rows = num_rows,
        .has_nulls = h.num_present != num_rows,
        .hist_first = h.first,
    };
}

fn histOf(
    comptime T: type,
    ra: std.mem.Allocator,
    chunk: []const u8,
    codec: schema.CompressionCodec,
    levels: schema.Levels,
    n_leaves: usize,
    decode_options: DecodeOptions,
    scratch: *DecodeScratch,
    firsts: bool,
) !?column_mod.ColumnChunkReader(T).HistOrValues {
    var reader = column_mod.ColumnChunkReader(T).initWithOptions(chunk, codec, levels, ra, decode_options, scratch);
    return reader.readAllHistogram(n_leaves, firsts);
}

/// A column of definition levels only, typed as `decodeAggColumn` would type it so lookups and type checks agree.
fn decodeLevelsOnly(
    ra: std.mem.Allocator,
    chunk: []const u8,
    cm: schema.ColumnMetaData,
    meta: *const schema.FileMetaData,
    decode_options: DecodeOptions,
    scratch: *DecodeScratch,
    num_rows: usize,
) !filter_eval.Batch.Column {
    _ = num_rows;
    const levels = meta.getColumnLevels(cm.path_in_schema.items);
    const n_leaves: usize = @intCast(cm.num_values);
    var dl: ?[]u32 = null;
    if (levels.max_def > 0) {
        // The reader of the column's own physical type, though only levels are read: pages still install their value
        // decoders, and a dictionary its entries, which another type's reader would mis-size or refuse (a FLOAT16
        // dictionary's 2-byte entries, a DELTA_BYTE_ARRAY page).
        const r = switch (cm.type) {
            inline else => |t| blk: {
                const T = switch (t) {
                    .BOOLEAN => bool,
                    .INT32 => i32,
                    .INT64 => i64,
                    .FLOAT => f32,
                    .DOUBLE => f64,
                    .INT96, .BYTE_ARRAY, .FIXED_LEN_BYTE_ARRAY => []const u8,
                };
                var reader = column_mod.ColumnChunkReader(T).initWithOptions(
                    chunk,
                    cm.codec,
                    levels,
                    ra,
                    decode_options,
                    scratch,
                );
                reader.type_length = switch (t) {
                    .FIXED_LEN_BYTE_ARRAY => flbaWidth(meta.getColumnSchema(cm.path_in_schema.items)),
                    .INT96 => int96_mod.INT96_WIDTH,
                    else => 0,
                };
                break :blk try reader.readAllLevels(n_leaves);
            },
        } orelse return decodeAggColumn(ra, chunk, cm, meta, null, decode_options, scratch);
        dl = r.def_levels;
    }
    var has_nulls = false;
    if (dl) |d| for (d) |x| {
        if (x < levels.max_def) {
            has_nulls = true;
            break;
        }
    };
    // The lane tag is irrelevant to count(col) and IS [NOT] NULL; i64 keeps every reader of nullInfo happy.
    return .{ .i64 = .{
        .values = &.{},
        .def_levels = dl,
        .max_def = @intCast(levels.max_def),
        .has_nulls = has_nulls,
        .levels_only_rows = if (dl) |d| d.len else n_leaves,
    } };
}

/// A values column built from converted values `d` (decimal scaling, unsigned widening) with `v`'s levels.
fn scaledCol(comptime T: type, d: []const T, v: anytype, levels: schema.Levels) filter_eval.ColumnT(T) {
    return .{ .values = d, .def_levels = v.def_levels, .max_def = @intCast(levels.max_def), .has_nulls = v.has_nulls };
}

const codedCol = consumer_column.codedCol;

const valuesCol = consumer_column.valuesCol;

const decodeCodes = consumer_column.decodeCodes;

/// Fold every call statistics can answer for this row group into its accumulator and mark it in `handled`. Returns
/// whether any call still needs the row group decoded.
fn foldFromStats(
    calls: []const expr_agg.AggCall,
    accumulators: []expr_agg.Accumulator,
    in: AggRowGroup,
    stats: agg_plan.StatsUse,
    handled: []bool,
    timings: *Timings,
) !bool {
    std.debug.assert(calls.len == accumulators.len);
    const t_stats_start = nowMonoNs();
    var answered: usize = 0;
    defer {
        const t_stats_end = nowMonoNs();
        timings.eval_ns += @intCast(t_stats_end - t_stats_start);
        profile.record(timings.prof, .stats_fold, t_stats_start, t_stats_end, profile.args(.{answered}));
    }
    var any_decode_required = false;
    for (calls, accumulators, handled) |call, *acc, *h| {
        if (stats == .ignore or !expr_agg.canStatShortCircuit(call, in.filter != null, stats == .trust)) {
            any_decode_required = true;
            continue;
        }
        if (try expr_agg.updateOneFromStats(acc, call, in.rg, in.meta)) {
            h.* = true;
            answered += 1;
        } else {
            any_decode_required = true;
        }
    }
    return any_decode_required;
}

const pruning = @import("consumer/pruning.zig");
const PageIndex = pruning.PageIndex;
const FilterPages = pruning.FilterPages;
const loadPageIndexes = pruning.loadPageIndexes;
const pruneFilterPages = pruning.pruneFilterPages;
const pagePruning = pruning.pagePruning;
const compactRows = pruning.compactRows;

/// Decode one column chunk into the batch lane its physical and logical type select.
fn decodeAggColumn(
    ra: std.mem.Allocator,
    chunk: []const u8,
    cm: schema.ColumnMetaData,
    meta: *const schema.FileMetaData,
    prune_info: ?PruningInfo,
    decode_options: DecodeOptions,
    scratch: *DecodeScratch,
) !filter_eval.Batch.Column {
    const levels = meta.getColumnLevels(cm.path_in_schema.items);
    const n_leaves: usize = @intCast(cm.num_values);
    const schema_elem = meta.getColumnSchema(cm.path_in_schema.items);
    const dec_kind: ?decimal_mod.Kind = if (schema_elem) |se| decimal_mod.exactKind(&se) else null;

    if (dec_kind) |k| return decimal_mod.decodeAggregateColumn(
        ra,
        chunk,
        cm.codec,
        levels,
        n_leaves,
        k,
        decode_options,
        scratch,
    );
    if (schema_elem != null and schema.isFloat16(schema_elem.?)) return .{
        .f64 = try decodeFloat16ColumnAsF64Pruned(
            ra,
            chunk,
            cm.codec,
            levels,
            n_leaves,
            prune_info,
            decode_options,
            scratch,
        ),
    };
    return switch (cm.type) {
        .INT32 => if (schema_elem != null and schema.isUnsignedIntTo32(schema_elem.?))
            .{ .i64 = try decodeU32ColumnAsI64Pruned(
                ra,
                chunk,
                cm.codec,
                levels,
                n_leaves,
                prune_info,
                decode_options,
                scratch,
            ) }
        else
            .{ .i32 = try decodeColumnTPruned(
                i32,
                ra,
                chunk,
                cm.codec,
                levels,
                n_leaves,
                prune_info,
                decode_options,
                scratch,
            ) },
        .INT64 => .{ .i64 = try decodeColumnTPruned(
            i64,
            ra,
            chunk,
            cm.codec,
            levels,
            n_leaves,
            prune_info,
            decode_options,
            scratch,
        ) },
        .FLOAT => .{ .f32 = try decodeColumnTPruned(
            f32,
            ra,
            chunk,
            cm.codec,
            levels,
            n_leaves,
            prune_info,
            decode_options,
            scratch,
        ) },
        .DOUBLE => .{ .f64 = try decodeColumnTPruned(
            f64,
            ra,
            chunk,
            cm.codec,
            levels,
            n_leaves,
            prune_info,
            decode_options,
            scratch,
        ) },
        .BYTE_ARRAY => .{ .string = try decodeColumnTPruned(
            []const u8,
            ra,
            chunk,
            cm.codec,
            levels,
            n_leaves,
            prune_info,
            decode_options,
            scratch,
        ) },
        .BOOLEAN => .{ .boolean = try decodeColumnTPruned(
            bool,
            ra,
            chunk,
            cm.codec,
            levels,
            n_leaves,
            prune_info,
            decode_options,
            scratch,
        ) },
        .INT96 => .{ .i64 = try int96_mod.decodeColumnAsI64Nanos(
            ra,
            chunk,
            cm.codec,
            levels,
            n_leaves,
            decode_options.page_bytes_limit,
        ) },
        .FIXED_LEN_BYTE_ARRAY => .{ .string = try decodeFlbaColumnPruned(
            ra,
            chunk,
            cm.codec,
            levels,
            n_leaves,
            flbaWidth(schema_elem),
            prune_info,
            decode_options,
            scratch,
        ) },
    };
}

pub const copy_module = @import("consumer/copy.zig");
pub const copyRG = copy_module.copyRG;
pub const BloomWrite = copy_module.BloomWrite;
pub const Recompress = copy_module.Recompress;
pub const RecompressedChunk = copy_module.RecompressedChunk;
pub const recompressRowGroup = copy_module.recompressRowGroup;
const dictionaryBloom = copy_module.dictionaryBloom;
const PageMove = copy_module.PageMove;
const recompressChunk = copy_module.recompressChunk;
const sourceBytes = types.sourceBytes;


fn pageForTest(arena: std.mem.Allocator, header: schema.PageHeader, body: []const u8) ![]u8 {
    var w = thrift.Writer.init(arena);
    try header.write(&w);
    var out: std.ArrayList(u8) = .empty;
    try out.appendSlice(arena, w.bytes());
    try out.appendSlice(arena, body);
    return out.items;
}

test "a page claiming more values than its chunk is rejected before sizing buffers from the claim" {
    // A few bytes can claim any count: one RLE def-level run of N present values. The level buffer used to be
    // sized from that claim, so a one-row chunk could make the reader allocate gigabytes.
    const testing = std.testing;
    var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_state.deinit();
    const a = arena_state.allocator();

    const claimed: u32 = 1 << 24;
    var body: std.ArrayList(u8) = .empty;
    var run: [8]u8 = undefined; // ULEB128 run header (claimed << 1), then the 1-byte value
    var n: u32 = claimed << 1;
    var len: usize = 0;
    while (true) : (len += 1) {
        run[len] = @truncate(n & 0x7f);
        n >>= 7;
        if (n == 0) break;
        run[len] |= 0x80;
    }
    len += 1;
    run[len] = 1;
    len += 1;
    var prefix: [4]u8 = undefined;
    std.mem.writeInt(u32, &prefix, @intCast(len), .little);
    try body.appendSlice(a, &prefix);
    try body.appendSlice(a, run[0..len]);
    try body.appendSlice(a, &[_]u8{ 42, 0, 0, 0 });
    const page = try pageForTest(a, .{
        .type = .DATA_PAGE,
        .uncompressed_page_size = @intCast(body.items.len),
        .compressed_page_size = @intCast(body.items.len),
        .crc = null,
        .data_page_header = .{ .num_values = claimed, .encoding = .PLAIN, .definition_level_encoding = .RLE, .repetition_level_encoding = .RLE },
        .dictionary_page_header = null,
        .data_page_header_v2 = null,
    }, body.items);
    const optional: schema.Levels = .{ .max_def = 1, .max_rep = 0 };
    try testing.expectError(error.PageValueCountExceedsChunk, decodeColumnT(i32, a, page, .UNCOMPRESSED, optional, 1));

    // A dictionary page claiming far more entries than its bytes can hold.
    const dict_body = [_]u8{ 1, 0, 0, 0, 'a' };
    const dict_page = try pageForTest(a, .{
        .type = .DICTIONARY_PAGE,
        .uncompressed_page_size = dict_body.len,
        .compressed_page_size = dict_body.len,
        .crc = null,
        .data_page_header = null,
        .dictionary_page_header = .{ .num_values = 1 << 20, .encoding = .PLAIN, .is_sorted = null },
        .data_page_header_v2 = null,
    }, &dict_body);
    const required: schema.Levels = .{ .max_def = 0, .max_rep = 0 };
    try testing.expectError(error.DictionaryLargerThanPage, decodeColumnT([]const u8, a, dict_page, .UNCOMPRESSED, required, 1));
}

pub const consumer_column = @import("consumer/column.zig");
pub const checkRowShape = consumer_column.checkRowShape;
pub const PruningInfo = consumer_column.PruningInfo;
pub const RowRange = consumer_column.RowRange;
pub const decodeColumnTPruned = consumer_column.decodeColumnTPruned;
pub const decodeFlbaColumnPruned = consumer_column.decodeFlbaColumnPruned;
pub const decodeColumnT = consumer_column.decodeColumnT;
pub const decodeColumnTWithOptions = consumer_column.decodeColumnTWithOptions;
pub const decodeFlbaColumn = consumer_column.decodeFlbaColumn;
pub const decodeFlbaColumnWithOptions = consumer_column.decodeFlbaColumnWithOptions;
pub const flbaWidth = consumer_column.flbaWidth;
const originSlice = consumer_column.originSlice;
const defaultVal = consumer_column.defaultVal;
const decodeWithReaderPruned = consumer_column.decodeWithReaderPruned;
const decodeFloat16ColumnAsF64Pruned = consumer_column.decodeFloat16ColumnAsF64Pruned;
const decodeU32ColumnAsI64Pruned = consumer_column.decodeU32ColumnAsI64Pruned;
const decodeFloat16ColumnAsF64 = consumer_column.decodeFloat16ColumnAsF64;

test "row subgroups seek misaligned columns and share dictionary backing across reader lifetimes" {
    const t = std.testing;
    const subgroups = @import("row_subgroups.zig");
    var arena = std.heap.ArenaAllocator.init(t.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const fixture = try subgroups.testFixture(a);
    const rg = &fixture.meta.row_groups.items[0];
    const group = (try subgroups.Group.init(a, fixture.bytes, rg, &fixture.meta, &.{ true, true, true }, 8)).?;
    const src: RGSrc = .{ .bytes = fixture.bytes, .byte_origin = 0 };
    for (0..3) |ci| {
        var whole = (try SliceReader.init(a, t.allocator, rg, &fixture.meta, src, ci, .{})).?;
        defer whole.deinit();
        const expected = try whole.next(a, 4096);
        for (group.ranges) |range| {
            var r = (try SliceReader.init(a, t.allocator, rg, &fixture.meta, src, ci, .{})).?;
            defer r.deinit();
            try r.seekSubgroup(a, .{ .group = &group, .rows = range });
            if (ci == 1) try t.expect(r.raw.bytes.dictionary.?.ptr == group.columns[ci].?.dictionary.?.bytes.ptr);
            const got = try r.next(a, range.hi - range.lo);
            switch (expected) {
                inline .i64, .string => |want, tag| {
                    const actual = @field(got, @tagName(tag));
                    try t.expectEqual(range.hi - range.lo, actual.values.len);
                    for (actual.values, 0..) |value, i| {
                        const def = if (want.def_levels) |d| d[range.lo + i] else want.max_def;
                        try t.expectEqual(def, if (actual.def_levels) |d| d[i] else actual.max_def);
                        if (def < want.max_def) continue;
                        if (tag == .string) try t.expectEqualStrings(want.values[range.lo + i], value) else try t.expectEqual(want.values[range.lo + i], value);
                    }
                },
                else => unreachable,
            }
        }
    }
}

/// Test-only: off compares compact decoding with whole columns; the count shows a test reached it.
pub var compact_rows_in_tests = true;
pub var compacted_row_groups: std.atomic.Value(usize) = .init(0);

// ============================================================
// Compile-time API check. End-to-end behavior is exercised through
// the lambda integration tests + CLI golden tests.
// ============================================================
test "consumer: API is well-typed" {
    _ = appendProjectedRG;
    _ = encodeAggregator;
    _ = copyRG;
    _ = decodeColumnT;
}

// Regression: copyRG(no_projection) used to write whatever `src.bytes`
// the caller passed, even when those bytes covered more than just this
// RG. The engine passes the *whole file*, so a file with N row groups
// produced an N×-bloated output. Lock the fix: with a hand-built 2-RG
// layout in a 1000-byte buffer, copying RG0 must write only RG0's
// bounding box (60 bytes), not the whole buffer.
test "copyRG no-projection: writes only this RG's bounding box, not src.bytes" {
    const testing = std.testing;
    var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_state.deinit();
    const a = arena_state.allocator();

    // Fake file layout: 1000 bytes total. RG0's columns span
    // [100, 160), RG1's span [500, 580). The two RGs are far apart in
    // the source file (typical for real parquet — pages + dict pages
    // + column metadata between them).
    const total_bytes: usize = 1000;
    var buf = try a.alloc(u8, total_bytes);
    @memset(buf, 0); // start with zeros so we can identify what got copied

    // Stamp recognizable patterns into RG0's region so we can assert
    // the right bytes ended up in the output.
    for (100..160) |i| buf[i] = @intCast((i - 100) + 1); // 1, 2, 3, ...

    var path: schema.StringList = .empty;
    try path.append(a, "x");
    var encs: schema.EncodingList = .empty;
    try encs.append(a, .PLAIN);

    // RG0: two columns at offsets 100 and 140 (each 20 bytes
    // compressed). Bounding box: [100, 160).
    var rg0_cols: std.ArrayListUnmanaged(schema.ColumnChunk) = .empty;
    try rg0_cols.append(a, .{
        .file_path = null,
        .file_offset = 0,
        .meta_data = .{
            .type = .INT64,
            .encodings = encs,
            .path_in_schema = path,
            .codec = .UNCOMPRESSED,
            .num_values = 10,
            .total_uncompressed_size = 50,
            .total_compressed_size = 20,
            .data_page_offset = 100,
            .index_page_offset = null,
            .dictionary_page_offset = null,
        },
    });
    try rg0_cols.append(a, .{
        .file_path = null,
        .file_offset = 0,
        .meta_data = .{
            .type = .INT64,
            .encodings = encs,
            .path_in_schema = path,
            .codec = .UNCOMPRESSED,
            .num_values = 10,
            .total_uncompressed_size = 30,
            .total_compressed_size = 20,
            .data_page_offset = 140,
            .index_page_offset = null,
            .dictionary_page_offset = null,
        },
    });
    // ALSO add a column from far away in the file to make sure copyRG
    // doesn't include it (it shouldn't — it's not in RG0). Sit it at
    // offset 800 — well past RG0's bounding box. (In a real file this
    // wouldn't happen; here it's a guardrail against "did we accidentally
    // re-fall-through to whole-file copy?".)
    // Note: copyRG operates on rg.columns.items only, so adding bytes
    // outside that doesn't affect it. RG0's bounding box is determined
    // entirely from RG0's columns.

    const rg0: schema.RowGroup = .{
        .columns = rg0_cols,
        .total_byte_size = 60,
        .num_rows = 10,
    };

    // Capture sink — records writes into an ArrayList.
    const Capture = struct {
        bytes: std.ArrayList(u8) = .empty,
        alloc: std.mem.Allocator,
        fn write(ctx: *anyopaque, b: []const u8) anyerror!void {
            const self: *@This() = @ptrCast(@alignCast(ctx));
            try self.bytes.appendSlice(self.alloc, b);
        }
    };
    var cap: Capture = .{ .alloc = a };
    const sink: streaming.Sink = .{
        .ctx = &cap,
        .write_fn = Capture.write,
    };

    var out_offset: u64 = 0;
    var timings: Timings = .{};
    const out = try copyRG(
        a,
        &rg0,
        .{ .bytes = buf, .byte_origin = 0 },
        null, // no projection — exercises the fixed branch
        sink,
        &out_offset,
        &timings,
        true, // direct-to-file: emit page index
        null,
        .{},
    );

    // Bounding box for RG0 is [100, 160) = 60 bytes. Anything more
    // means we're back to the old bug.
    try testing.expectEqual(@as(usize, 60), cap.bytes.items.len);
    try testing.expectEqual(@as(u64, 60), out_offset);
    // Content should match buf[100..160] exactly (the stamped pattern).
    try testing.expectEqualSlices(u8, buf[100..160], cap.bytes.items);
    // The output RG's total_byte_size is its chunks' uncompressed bytes (50 + 30), not the 60 bytes copied.
    try testing.expect(out.rg != null);
    try testing.expectEqual(@as(i64, 80), out.rg.?.total_byte_size);
    try testing.expectEqual(@as(i64, 10), out.rg.?.num_rows);
    // Column offsets must be shifted to point into the output stream.
    // Column 0 was at source offset 100; output bounding box starts at
    // output offset 0; so new offset = 0 (i.e. new_start - rg_min = 0).
    try testing.expectEqual(@as(i64, 0), out.rg.?.columns.items[0].meta_data.?.data_page_offset);
    // Column 1 was at source offset 140 → output offset 40 (140 - 100).
    try testing.expectEqual(@as(i64, 40), out.rg.?.columns.items[1].meta_data.?.data_page_offset);
}

// --- DECIMAL decode: fixture-free encode→decode round-trip ---
// The decimal *decode* tests in decimal.zig read parquet-testing fixtures
// and skip in CI without the corpus (as do all of column.zig's decode
// tests). This exercises the full INT32/INT64-backed decimal decode path
// — page parse → ColumnChunkReader → scale → f64 — with no fixture, by
// encoding a known int column and decoding it back as a DECIMAL.
test "decimal: INT32/INT64 backings decode round-trip through the encoder" {
    const testing = std.testing;
    var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_state.deinit();
    const arena = arena_state.allocator();

    { // INT32-backed, scale 2 → raw / 100
        const raw = [_]i32{ 12345, -6789, 100, 0, 999999 };
        const elem = schema.SchemaElement{
            .type = .INT32,
            .type_length = null,
            .repetition_type = .REQUIRED,
            .name = "value",
            .num_children = 0,
            .converted_type = .DECIMAL,
            .logical_type = null,
            .scale = 2,
            .precision = 9,
            .field_id = null,
        };
        const enc = try encoder.encodeColumn(arena, .{
            .values = .{ .i32 = .{ .values = &raw } },
            .schema_elem = &elem,
            .path_in_schema = &[_][]const u8{"value"},
            .codec = .UNCOMPRESSED,
        });
        const kind = decimal_mod.kindFromSchema(&elem).?;
        const col = try decimal_mod.decodeColumnAsF64(arena, enc.bytes, .UNCOMPRESSED, .{ .max_def = 0, .max_rep = 0 }, raw.len, kind);
        const want = [_]f64{ 123.45, -67.89, 1.00, 0.0, 9999.99 };
        try testing.expectEqual(want.len, col.values.len);
        for (want, col.values) |w, g| try testing.expectApproxEqAbs(w, g, 1e-9);
    }

    { // INT64-backed, scale 3
        const raw = [_]i64{ 1000, -250, 0, 123456789 };
        const elem = schema.SchemaElement{
            .type = .INT64,
            .type_length = null,
            .repetition_type = .REQUIRED,
            .name = "value",
            .num_children = 0,
            .converted_type = .DECIMAL,
            .logical_type = null,
            .scale = 3,
            .precision = 18,
            .field_id = null,
        };
        const enc = try encoder.encodeColumn(arena, .{
            .values = .{ .i64 = .{ .values = &raw } },
            .schema_elem = &elem,
            .path_in_schema = &[_][]const u8{"value"},
            .codec = .UNCOMPRESSED,
        });
        const kind = decimal_mod.kindFromSchema(&elem).?;
        const col = try decimal_mod.decodeColumnAsF64(arena, enc.bytes, .UNCOMPRESSED, .{ .max_def = 0, .max_rep = 0 }, raw.len, kind);
        const want = [_]f64{ 1.0, -0.25, 0.0, 123456.789 };
        try testing.expectEqual(want.len, col.values.len);
        for (want, col.values) |w, g| try testing.expectApproxEqAbs(w, g, 1e-6);
    }
}

// --- --fast-levels: all-present definition-level detection ---
//
// The flag lets a decode skip materialising definition levels for any
// page whose level stream proves every value present. These build the
// awkward shapes by hand — an all-present page, a page with real nulls,
// and a chunk that changes from one to the other mid-stream — because
// the mid-stream transition is the only place the optimisation has to
// hand work back to the ordinary path, and getting the hand-off wrong
// would silently shift every value after it.
//
// Every case asserts the SAME values with the flag on and off. A faster
// wrong answer is the failure mode worth guarding, not a slower one.

const fl_test_elem = schema.SchemaElement{
    .type = .INT32,
    .type_length = null,
    .repetition_type = .OPTIONAL,
    .name = "v",
    .num_children = 0,
    .converted_type = null,
    .logical_type = null,
    .scale = null,
    .precision = null,
    .field_id = null,
};

/// Encode one OPTIONAL INT32 page. `def` of null means "no level array
/// supplied" — the encoder then writes an all-present stream itself.
fn flEncodePage(
    arena: std.mem.Allocator,
    values: []const i32,
    def: ?[]const u32,
) ![]u8 {
    const enc = try encoder.encodeColumn(arena, .{
        .values = .{ .i32 = .{ .values = values, .def_levels = def, .max_def = 1 } },
        .schema_elem = &fl_test_elem,
        .path_in_schema = &[_][]const u8{"v"},
        .codec = .UNCOMPRESSED,
    });
    return enc.bytes;
}

const fl_levels = schema.Levels{ .max_def = 1, .max_rep = 0 };

const req_i32_elem = schema.SchemaElement{
    .type = .INT32,
    .type_length = null,
    .repetition_type = .REQUIRED,
    .name = "v",
    .num_children = 0,
    .converted_type = null,
    .logical_type = null,
    .scale = null,
    .precision = null,
    .field_id = null,
};

test "a required row gate skips an unread page and defaults the unselected slots of a partial page" {
    const testing = std.testing;
    var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_state.deinit();
    const arena = arena_state.allocator();
    const levels = schema.Levels{ .max_def = 0, .max_rep = 0 };

    // i32/i64 pages are DELTA_BINARY_PACKED, which has to be read in order. PLAIN f64 is the within-page gather.
    const req_f64 = schema.SchemaElement{
        .type = .DOUBLE,
        .type_length = null,
        .repetition_type = .REQUIRED,
        .name = "v",
        .num_children = 0,
        .converted_type = null,
        .logical_type = null,
        .scale = null,
        .precision = null,
        .field_id = null,
    };
    var one_page: [32]f64 = undefined;
    for (&one_page, 0..) |*v, i| v.* = @floatFromInt(i + 1);
    const whole = try encoder.encodeColumn(arena, .{
        .values = .{ .f64 = .{ .values = &one_page } },
        .schema_elem = &req_f64,
        .path_in_schema = &.{"v"},
        .codec = .UNCOMPRESSED,
        .dictionary_bloom = false,
        .distinct = true,
        .page_rows = 64,
    });
    try testing.expect(plainValueEncodings(whole.meta.encodings.items));
    const full = try decodeColumnT(f64, arena, whole.bytes, .UNCOMPRESSED, levels, one_page.len);
    var odds_off = try filter_selection.SelectionVector.init(arena, one_page.len);
    for (0..one_page.len) |i| if (i % 2 == 1) odds_off.set(i, false);
    const gathered = try decodeColumnTWithOptions(f64, arena, whole.bytes, .UNCOMPRESSED, levels, one_page.len, .{
        .row_gate = &odds_off,
    });
    for (full.values, gathered.values, 0..) |want, got, i| {
        try testing.expectEqual(if (i % 2 == 0) want else 0, got);
    }

    var paged: [32]i32 = undefined;
    for (&paged, 0..) |*v, i| v.* = @intCast(i + 1);
    const pages = try encoder.encodeColumn(arena, .{
        .values = .{ .i32 = .{ .values = &paged } },
        .schema_elem = &req_i32_elem,
        .path_in_schema = &.{"v"},
        .codec = .UNCOMPRESSED,
        .dictionary_bloom = false,
        .distinct = true,
        .page_rows = 16,
    });
    const clean = try decodeColumnT(i32, arena, pages.bytes, .UNCOMPRESSED, levels, paged.len);
    var corrupt = try arena.dupe(u8, pages.bytes);
    var walker = page_mod.PageReader.init(corrupt, .UNCOMPRESSED, arena);
    const first = (try walker.peek()) orelse return error.UnexpectedPage;
    const header_len = first.size - @as(usize, @intCast(first.header.compressed_page_size));
    try testing.expect(first.header.compressed_page_size > 4);
    corrupt[walker.pos + header_len + 4] ^= 0xff;
    const page_rows: usize = @intCast(first.header.data_page_header.?.num_values);
    var tail = try filter_selection.SelectionVector.init(arena, paged.len);
    for (0..page_rows) |i| tail.set(i, false);
    const skipped = try decodeColumnTWithOptions(i32, arena, corrupt, .UNCOMPRESSED, levels, paged.len, .{
        .row_gate = &tail,
    });
    for (clean.values, skipped.values, 0..) |want, got, i| {
        try testing.expectEqual(if (i < page_rows) 0 else want, got);
    }
}

test "regression: a filtered group's dictionary keys stay codes for a fold that groups by them" {
    var dict_only = [_]schema.Encoding{.PLAIN_DICTIONARY};
    var fell_back = [_]schema.Encoding{ .PLAIN_DICTIONARY, .PLAIN };
    var path = [_][]const u8{"k"};
    const chunk = struct {
        fn of(encs: []schema.Encoding, p: *[1][]const u8) schema.ColumnChunk {
            return .{ .file_path = null, .file_offset = 0, .meta_data = .{
                .type = .INT32,
                .encodings = .{ .items = encs, .capacity = encs.len, .pointer_stability = .{} },
                .path_in_schema = .{ .items = p, .capacity = 1, .pointer_stability = .{} },
                .codec = .UNCOMPRESSED,
                .num_values = 1000,
                .total_uncompressed_size = 0,
                .total_compressed_size = 0,
                .data_page_offset = 0,
                .index_page_offset = null,
                .dictionary_page_offset = 0,
            } };
        }
    }.of;
    var columns = [_]schema.ColumnChunk{ chunk(&dict_only, &path), chunk(&dict_only, &path) };
    var rg: schema.RowGroup = .{
        .columns = .{ .items = &columns, .capacity = 2, .pointer_stability = .{} },
        .total_byte_size = 0,
        .num_rows = 1000,
    };
    const keys = [_]expr_ast.Expr{
        .{ .col_ref = .{ .col_idx = 0, .physical_type = .INT32, .expr_type = .i64 } },
        .{ .col_ref = .{ .col_idx = 1, .physical_type = .INT32, .expr_type = .i64 } },
    };
    const calls = [_]expr_agg.AggCall{.{ .func = .count, .arg = null, .where = null, .alias = "c", .result = .i64 }};
    var table = expr_agg.GroupTable.init(std.testing.allocator, 0);
    defer table.deinit();
    const target: AggTarget = .{ .grouped = .{ .calls = &calls, .keys = &keys, .table = &table, .pos_base = 0 } };
    // ad5220d decoded a 30%-selected INT32 key as survivor values (four-byte codes for every row weighed more), and
    // the coded fold, given values, sent every row through key serialization and hashing: 4-9x slower.
    try std.testing.expect(!keyCodesCheaper(columns[0].meta_data.?, 1000, 300));
    try std.testing.expect(foldWantsCodes(target, 0, &rg));
    // One key its dictionary does not cover leaves the composite uncoded: that key's peer goes to the byte rule.
    columns[1] = chunk(&fell_back, &path);
    try std.testing.expect(!foldWantsCodes(target, 0, &rg));
}

test "a filtered group's dictionary key decodes as codes only where they materialize less than its survivors" {
    var cm: schema.ColumnMetaData = .{
        .type = .BYTE_ARRAY,
        .encodings = .empty,
        .path_in_schema = .empty,
        .codec = .UNCOMPRESSED,
        .num_values = 1000,
        .total_uncompressed_size = 0,
        .total_compressed_size = 0,
        .data_page_offset = 0,
        .index_page_offset = null,
        .dictionary_page_offset = null,
    };
    // A string survivor is a slice the fold codes again: codes win once a quarter of the rows survive.
    try std.testing.expect(keyCodesCheaper(cm, 1000, 985));
    try std.testing.expect(keyCodesCheaper(cm, 1000, 250));
    try std.testing.expect(!keyCodesCheaper(cm, 1000, 249));
    // An integer survivor costs its width: four-byte codes for every row win only when every row survives.
    cm.type = .INT32;
    try std.testing.expect(keyCodesCheaper(cm, 1000, 1000));
    try std.testing.expect(!keyCodesCheaper(cm, 1000, 999));
    cm.type = .INT64;
    try std.testing.expect(keyCodesCheaper(cm, 1000, 500));
    try std.testing.expect(!keyCodesCheaper(cm, 1000, 130));
}

test "PLAIN byte-array lengths record emptiness without the payloads" {
    const testing = std.testing;
    var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_state.deinit();
    const arena = arena_state.allocator();
    // The column encoder writes strings as delta or dictionary, never PLAIN. This page is the length-prefix layout.
    var body: std.ArrayList(u8) = .empty;
    var len = [_]u8{ 0, 0, 0, 0 };
    try body.appendSlice(arena, &len);
    std.mem.writeInt(u32, &len, 2, .little);
    try body.appendSlice(arena, &len);
    try body.appendSlice(arena, "ab");
    const header = schema.PageHeader{
        .type = .DATA_PAGE,
        .uncompressed_page_size = @intCast(body.items.len),
        .compressed_page_size = @intCast(body.items.len),
        .crc = null,
        .data_page_header = .{
            .num_values = 2,
            .encoding = .PLAIN,
            .definition_level_encoding = .RLE,
            .repetition_level_encoding = .RLE,
        },
        .dictionary_page_header = null,
        .data_page_header_v2 = null,
    };
    var w: thrift.Writer = .init(arena);
    try header.write(&w);
    var chunk: std.ArrayList(u8) = .empty;
    try chunk.appendSlice(arena, w.bytes());
    try chunk.appendSlice(arena, body.items);
    const levels = schema.Levels{ .max_def = 0, .max_rep = 0 };
    var reader = column_mod.ColumnChunkReader([]const u8).initWithOptions(
        chunk.items,
        .UNCOMPRESSED,
        levels,
        arena,
        .{},
        null,
    );
    const got = try reader.readPlainLengths(2);
    try testing.expectEqualSlices(u32, &.{ 0, 2 }, got.lengths);
    try testing.expect(got.def_levels == null);
}

fn appendV1Plain(
    arena: std.mem.Allocator,
    chunk: *std.ArrayList(u8),
    num_values: i32,
    body: []const u8,
    claimed: i32,
) !void {
    const header = schema.PageHeader{
        .type = .DATA_PAGE,
        .uncompressed_page_size = claimed,
        .compressed_page_size = @intCast(body.len),
        .crc = null,
        .data_page_header = .{
            .num_values = num_values,
            .encoding = .PLAIN,
            .definition_level_encoding = .RLE,
            .repetition_level_encoding = .RLE,
        },
        .dictionary_page_header = null,
        .data_page_header_v2 = null,
    };
    var w: thrift.Writer = .init(arena);
    try header.write(&w);
    try chunk.appendSlice(arena, w.bytes());
    try chunk.appendSlice(arena, body);
}

test "PLAIN lengths and values both reject a page past the chunk's inflate cap" {
    const testing = std.testing;
    var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_state.deinit();
    const arena = arena_state.allocator();
    // Header claims 4 KiB behind a 4-byte body. Under the cap both paths refuse before inflating it; without the
    // cap the length reader would accept a page the value reader rejects.
    var chunk: std.ArrayList(u8) = .empty;
    try appendV1Plain(arena, &chunk, 1, &[_]u8{ 0, 0, 0, 0 }, 4096);
    const levels = schema.Levels{ .max_def = 0, .max_rep = 0 };
    const options: DecodeOptions = .{ .page_bytes_limit = 32 };
    var lengths = column_mod.ColumnChunkReader([]const u8).initWithOptions(
        chunk.items,
        .UNCOMPRESSED,
        levels,
        arena,
        options,
        null,
    );
    try testing.expectError(error.PageLargerThanChunk, lengths.readPlainLengths(1));
    var values = column_mod.ColumnChunkReader([]const u8).initWithOptions(
        chunk.items,
        .UNCOMPRESSED,
        levels,
        arena,
        options,
        null,
    );
    var dest: [1][]const u8 = undefined;
    try testing.expectError(error.PageLargerThanChunk, values.decode(&dest));

    var encs = [_]schema.Encoding{.PLAIN};
    var path = [_][]const u8{"s"};
    const cm = schema.ColumnMetaData{
        .type = .BYTE_ARRAY,
        .encodings = .{ .items = &encs, .capacity = 1, .pointer_stability = .{} },
        .path_in_schema = .{ .items = &path, .capacity = 1, .pointer_stability = .{} },
        .codec = .UNCOMPRESSED,
        .num_values = 1,
        .total_uncompressed_size = @intCast(chunk.items.len),
        .total_compressed_size = @intCast(chunk.items.len),
        .data_page_offset = 0,
        .index_page_offset = null,
        .dictionary_page_offset = null,
    };
    var rg: schema.RowGroup = .{ .columns = .empty, .total_byte_size = 0, .num_rows = 1 };
    const meta: schema.FileMetaData = .{
        .version = 1,
        .schema = .empty,
        .num_rows = 1,
        .created_by = null,
        .row_groups = .empty,
    };
    const in: AggRowGroup = .{
        .rg = &rg,
        .meta = &meta,
        .src = .{ .bytes = chunk.items, .byte_origin = 0 },
        .filter = .{ .string = .{ .col_idx = 0, .op = .Eq, .value = "" } },
        .read = &.{true},
        .consumed = &.{false},
    };
    try testing.expectError(error.PageLargerThanChunk, plainLengthColumn(arena, in, 0, chunk.items, cm, options));
}

test "PLAIN lengths skip a leading or intermediate zero-row page and still reject an oversized count" {
    const testing = std.testing;
    var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_state.deinit();
    const arena = arena_state.allocator();
    var ab: [8]u8 = undefined;
    std.mem.writeInt(u32, ab[0..4], 2, .little);
    @memcpy(ab[4..6], "ab");
    var chunk: std.ArrayList(u8) = .empty;
    try appendV1Plain(arena, &chunk, 0, &.{}, 0);
    try appendV1Plain(arena, &chunk, 1, ab[0..6], 6);
    try appendV1Plain(arena, &chunk, 0, &.{}, 0);
    try appendV1Plain(arena, &chunk, 1, &[_]u8{ 0, 0, 0, 0 }, 4);
    const levels = schema.Levels{ .max_def = 0, .max_rep = 0 };
    var reader = column_mod.ColumnChunkReader([]const u8).initWithOptions(
        chunk.items,
        .UNCOMPRESSED,
        levels,
        arena,
        .{},
        null,
    );
    const lengths = try reader.readPlainLengths(2);
    try testing.expectEqualSlices(u32, &.{ 2, 0 }, lengths.lengths);
    var again = column_mod.ColumnChunkReader([]const u8).initWithOptions(
        chunk.items,
        .UNCOMPRESSED,
        levels,
        arena,
        .{},
        null,
    );
    var dest: [2][]const u8 = undefined;
    try testing.expectEqual(@as(usize, 2), try again.decode(&dest));
    try testing.expectEqualStrings("ab", dest[0]);
    try testing.expectEqualStrings("", dest[1]);

    var over: std.ArrayList(u8) = .empty;
    try appendV1Plain(arena, &over, 0, &.{}, 0);
    try appendV1Plain(arena, &over, 5, &.{}, 0);
    var bad = column_mod.ColumnChunkReader([]const u8).initWithOptions(
        over.items,
        .UNCOMPRESSED,
        levels,
        arena,
        .{},
        null,
    );
    try testing.expectError(error.UnexpectedPage, bad.readPlainLengths(2));
}

/// Compaction (`compact_rows_in_tests`) decodes `keep`. A page inside those rows can still have no SQL survivor.
/// Filling it must not move the next page's values.
fn compactionAlignsNextPage(
    comptime T: type,
    arena: std.mem.Allocator,
    elem: *const schema.SchemaElement,
    col: filter_eval.Batch.Column,
    plain: []const T,
) !void {
    const path = [_][]const u8{elem.name};
    const enc = try encoder.encodeColumn(arena, .{
        .values = col,
        .schema_elem = elem,
        .path_in_schema = &path,
        .codec = .UNCOMPRESSED,
        .distinct = true,
        .page_rows = 4,
    });
    try std.testing.expect(enc.pages.len >= 2);
    const split: usize = @intCast(enc.pages[0].rows);
    try std.testing.expect(split > 0 and split < plain.len);
    try std.testing.expect(enc.pages[1].offset > 0);
    // That page is not decoded. Its last byte would fail a reader that opened it.
    enc.bytes[@as(usize, @intCast(enc.pages[1].offset)) - 1] ^= 0xff;
    const locs = try arena.alloc(schema.PageLocation, enc.pages.len);
    for (enc.pages, locs) |p, *loc| loc.* = .{
        .offset = @intCast(p.offset),
        .compressed_page_size = @intCast(p.size),
        .first_row_index = @intCast(p.first_row),
    };
    const quiet = try arena.alloc(bool, locs.len);
    @memset(quiet, false);
    const keep = [_]RowRange{.{ .lo = 0, .hi = plain.len }};
    const prune = PruningInfo{
        .locations = locs,
        .page_is_skipped = quiet,
        .page_is_always_match = quiet,
        .dictionary_page_offset = if (enc.pages[0].offset > 0) @as(?i64, 0) else null,
        .chunk_file_offset = 0,
        .col_index = .{},
        .keep = &keep,
    };
    var gate = try filter_selection.SelectionVector.init(arena, plain.len);
    defer gate.deinit();
    for (0..split) |i| gate.set(i, false);
    const levels = schema.Levels{ .max_def = 0, .max_rep = 0 };
    var reader = column_mod.ColumnChunkReader(T).initWithOptions(
        enc.bytes,
        .UNCOMPRESSED,
        levels,
        arena,
        .{ .row_gate = &gate },
        null,
    );
    const got = try decodeWithReaderPruned(T, arena, &reader, levels, plain.len, prune);
    try std.testing.expectEqual(plain.len, got.values.len);
    try std.testing.expect(got.def_levels == null);
    if (comptime T == []const u8) {
        for (got.values[0..split]) |v| try std.testing.expectEqualStrings("", v);
        for (plain[split..], got.values[split..]) |want, g| try std.testing.expectEqualStrings(want, g);
    } else {
        for (got.values[0..split]) |v| try std.testing.expectEqual(defaultVal(T), v);
        try std.testing.expectEqualSlices(T, plain[split..], got.values[split..]);
    }
}

test "compaction fills a kept required page with no SQL survivors and aligns the next page" {
    const testing = std.testing;
    const prior = compact_rows_in_tests;
    defer compact_rows_in_tests = prior;
    compact_rows_in_tests = true;
    var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_state.deinit();
    const arena = arena_state.allocator();

    const nums = [_]i32{ 10, 11, 12, 13, 20, 21, 22, 23 };
    try compactionAlignsNextPage(i32, arena, &req_i32_elem, .{ .i32 = .{ .values = &nums } }, &nums);

    const req_ba = schema.SchemaElement{
        .type = .BYTE_ARRAY,
        .type_length = null,
        .repetition_type = .REQUIRED,
        .name = "s",
        .num_children = 0,
        .converted_type = null,
        .logical_type = null,
        .scale = null,
        .precision = null,
        .field_id = null,
    };
    const strs = [_][]const u8{ "a0", "a1", "a2", "a3", "b0", "b1", "b2", "b3" };
    try compactionAlignsNextPage([]const u8, arena, &req_ba, .{ .string = .{ .values = &strs } }, &strs);
}

test "a partial record batch gathers ascending original rows" {
    const testing = std.testing;
    var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_state.deinit();
    const arena = arena_state.allocator();
    const values = [_]i32{ 1, 2, 3, 4 };
    const cols = [_]filter_eval.Batch.Column{.{ .i32 = .{ .values = &values } }};
    const rows: filter_eval.Batch = .{ .cols = &cols, .num_rows = values.len };
    var sel = try filter_selection.SelectionVector.init(testing.allocator, values.len);
    defer sel.deinit();
    sel.set(0, false);
    sel.set(2, false);
    const Capture = struct {
        n: usize = 0,
        ords: [2]u64 = undefined,
        vals: [2]i32 = undefined,
        fn start(_: *anyopaque, _: u64, _: usize) void {}
        fn batch(ctx: *anyopaque, view: SelectedBatch, _: []const ?usize) !void {
            const c: *@This() = @ptrCast(@alignCast(ctx));
            for (0..view.batch.num_rows) |i| {
                c.ords[c.n] = try view.ordinal(i);
                c.vals[c.n] = view.batch.cols[0].i32.values[i];
                c.n += 1;
            }
        }
    };
    var cap: Capture = .{};
    var arm: RecordSink = .{ .selected = .{
        .ctx = @ptrCast(&cap),
        .startFn = Capture.start,
        .batchFn = Capture.batch,
    } };
    try emitRecords(arena, &arm, &rows, &sel, 50, 7, &.{});
    try testing.expectEqual(@as(u64, 51), cap.ords[0]);
    try testing.expectEqual(@as(u64, 53), cap.ords[1]);
    try testing.expectEqual(@as(i32, 2), cap.vals[0]);
    try testing.expectEqual(@as(i32, 4), cap.vals[1]);
}

test "fast-levels: all-present OPTIONAL column decodes identically with the flag on" {
    const testing = std.testing;
    var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_state.deinit();
    const arena = arena_state.allocator();

    // High-cardinality so the encoder stays PLAIN rather than dict.
    var vals: [512]i32 = undefined;
    for (&vals, 0..) |*v, i| v.* = @intCast(i * 7919);
    const chunk = try flEncodePage(arena, &vals, null);

    const off = try decodeColumnT(i32, arena, chunk, .UNCOMPRESSED, fl_levels, vals.len);
    const on = try decodeColumnTWithOptions(
        i32,
        arena,
        chunk,
        .UNCOMPRESSED,
        fl_levels,
        vals.len,
        .{ .fast_levels = true },
    );

    try testing.expectEqualSlices(i32, &vals, off.values);
    try testing.expectEqualSlices(i32, &vals, on.values);

    // The whole point: with the flag on the level array is never built.
    // `def_levels == null` alongside `max_def == 1` is how the column
    // reports "no nulls", which is exactly what the off path spells out
    // one u32 at a time.
    try testing.expect(off.def_levels != null);
    for (off.def_levels.?) |d| try testing.expectEqual(@as(u32, 1), d);
    try testing.expect(on.def_levels == null);
    try testing.expectEqual(@as(u32, 1), on.max_def);
    try testing.expect(!on.has_nulls);
}

test "fast-levels: a page with real nulls still decodes through the ordinary path" {
    const testing = std.testing;
    var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_state.deinit();
    const arena = arena_state.allocator();

    // 300 slots, every 5th one null. Values sit at their logical index
    // so a misplaced scatter is visible, not just a wrong count.
    const n = 300;
    var vals: [n]i32 = undefined;
    var def: [n]u32 = undefined;
    for (0..n) |i| {
        const is_null = (i % 5) == 0;
        def[i] = if (is_null) 0 else 1;
        vals[i] = if (is_null) 0 else @intCast(i * 31 + 1);
    }
    const chunk = try flEncodePage(arena, &vals, &def);

    const off = try decodeColumnT(i32, arena, chunk, .UNCOMPRESSED, fl_levels, n);
    const on = try decodeColumnTWithOptions(
        i32,
        arena,
        chunk,
        .UNCOMPRESSED,
        fl_levels,
        n,
        .{ .fast_levels = true },
    );

    try testing.expectEqualSlices(i32, &vals, off.values);
    try testing.expectEqualSlices(i32, &vals, on.values);
    // Nulls are present, so the levels must be materialised either way.
    try testing.expect(on.def_levels != null);
    try testing.expectEqualSlices(u32, &def, on.def_levels.?);
    try testing.expect(on.has_nulls);
}

test "fast-levels: chunk that turns null part-way hands back to the level path" {
    const testing = std.testing;
    var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_state.deinit();
    const arena = arena_state.allocator();

    // Page 1: 256 values, all present — taken by the fast path.
    // Page 2: 256 values, half null — must stop the fast path, allocate
    //         the level array, back-fill page 1 as all-present, and
    //         resume without dropping or shifting a value.
    const per_page = 256;
    var p1: [per_page]i32 = undefined;
    for (&p1, 0..) |*v, i| v.* = @intCast(i * 7919 + 3);

    var p2: [per_page]i32 = undefined;
    var p2_def: [per_page]u32 = undefined;
    for (0..per_page) |i| {
        const is_null = (i % 2) == 1;
        p2_def[i] = if (is_null) 0 else 1;
        p2[i] = if (is_null) 0 else @intCast(1_000_000 + i * 13);
    }

    // `EncodedColumn.bytes` is documented as header-plus-data ready to
    // concatenate, which is what makes a two-page chunk buildable here.
    const page1 = try flEncodePage(arena, &p1, null);
    const page2 = try flEncodePage(arena, &p2, &p2_def);
    const chunk = try arena.alloc(u8, page1.len + page2.len);
    @memcpy(chunk[0..page1.len], page1);
    @memcpy(chunk[page1.len..], page2);

    const total = per_page * 2;
    var want_vals: [total]i32 = undefined;
    var want_def: [total]u32 = undefined;
    @memcpy(want_vals[0..per_page], &p1);
    @memcpy(want_vals[per_page..], &p2);
    @memset(want_def[0..per_page], 1);
    @memcpy(want_def[per_page..], &p2_def);

    const off = try decodeColumnT(i32, arena, chunk, .UNCOMPRESSED, fl_levels, total);
    const on = try decodeColumnTWithOptions(
        i32,
        arena,
        chunk,
        .UNCOMPRESSED,
        fl_levels,
        total,
        .{ .fast_levels = true },
    );

    try testing.expectEqualSlices(i32, &want_vals, off.values);
    try testing.expectEqualSlices(i32, &want_vals, on.values);
    try testing.expect(on.def_levels != null);
    // Page 1's levels were never decoded from the wire — they are the
    // back-fill. If the hand-off is off by a page these are 0, not 1.
    try testing.expectEqualSlices(u32, &want_def, on.def_levels.?);
    try testing.expect(on.has_nulls);
}

test "fast-levels: all-present pages on both sides of a null page" {
    const testing = std.testing;
    var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_state.deinit();
    const arena = arena_state.allocator();

    // The fast path only runs while it has never seen a null. Once it
    // hands off it must not resume, or the third page's levels would go
    // unwritten and read back as the zeroes nobody wrote.
    const per_page = 128;
    var present: [per_page]i32 = undefined;
    for (&present, 0..) |*v, i| v.* = @intCast(i * 104729 + 5);

    var mid: [per_page]i32 = undefined;
    var mid_def: [per_page]u32 = undefined;
    for (0..per_page) |i| {
        const is_null = i < 4;
        mid_def[i] = if (is_null) 0 else 1;
        mid[i] = if (is_null) 0 else @intCast(500 + i);
    }

    const pages = [_][]u8{
        try flEncodePage(arena, &present, null),
        try flEncodePage(arena, &mid, &mid_def),
        try flEncodePage(arena, &present, null),
    };
    var total_len: usize = 0;
    for (pages) |p| total_len += p.len;
    const chunk = try arena.alloc(u8, total_len);
    var at: usize = 0;
    for (pages) |p| {
        @memcpy(chunk[at .. at + p.len], p);
        at += p.len;
    }

    const total = per_page * 3;
    const off = try decodeColumnT(i32, arena, chunk, .UNCOMPRESSED, fl_levels, total);
    const on = try decodeColumnTWithOptions(
        i32,
        arena,
        chunk,
        .UNCOMPRESSED,
        fl_levels,
        total,
        .{ .fast_levels = true },
    );

    try testing.expectEqualSlices(i32, off.values, on.values);
    try testing.expectEqualSlices(u32, off.def_levels.?, on.def_levels.?);
    try testing.expectEqualSlices(i32, &present, on.values[per_page * 2 ..]);
    for (on.def_levels.?[per_page * 2 ..]) |d| try testing.expectEqual(@as(u32, 1), d);
}

test "regression: a BOOLEAN expression is a BOOLEAN output leaf and buffer, its 0/1 lane stored as booleans" {
    const testing = std.testing;
    var b: expr_ast.Expr = .{ .col_ref = .{ .col_idx = 0, .physical_type = .BOOLEAN, .expr_type = .i64 } };
    var src: schema.SchemaElement = .{
        .type = .BOOLEAN,
        .type_length = null,
        .repetition_type = .OPTIONAL,
        .name = "b",
        .num_children = null,
        .scale = null,
        .precision = null,
        .field_id = null,
    };
    // An aliased reference keeps its column's leaf, nullable as the column is.
    const aliased = computedSchemaElem(.{ .expr = b, .alias = "k", .source = src });
    try testing.expectEqual(schema.Type.BOOLEAN, aliased.type.?);
    try testing.expectEqual(schema.FieldRepetitionType.OPTIONAL, aliased.repetition_type.?);
    try testing.expectEqualStrings("k", aliased.name);
    src.type = .INT32;
    const always: filter_ast.Filter = .{ .and_filter = .{ .children = &.{} } };
    const whens = [_]expr_ast.Case.When{.{ .cond = always, .then = &b }};
    const case: expr_ast.Expr = .{ .case_ = .{ .whens = &whens, .else_ = null, .result_type = .i64, .depth = 1 } };
    const computed = computedSchemaElem(.{ .expr = case, .alias = "k" });
    try testing.expectEqual(schema.Type.BOOLEAN, computed.type.?);
    try testing.expectEqual(schema.FieldRepetitionType.OPTIONAL, computed.repetition_type.?);

    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const vals = [_]i64{ 1, 0, 0 };
    const defs = [_]u32{ 1, 1, 0 };
    const col: filter_eval.Batch.Column = .{ .i64 = .{ .values = &vals, .def_levels = &defs, .max_def = 1, .has_nulls = true } };
    const lane = try booleanLane(arena.allocator(), .{ .boolean = .{} }, col);
    try testing.expectEqualSlices(bool, &.{ true, false, false }, lane.boolean.values);
    try testing.expectEqualSlices(u32, &defs, lane.boolean.def_levels.?);
    // Any other buffer takes the lane as it is.
    try testing.expect((try booleanLane(arena.allocator(), .{ .i64 = .{} }, col)) == .i64);
}

test "recompressChunk: a codec round trip gives back the chunk byte for byte" {
    // tiny_pages: uncompressed v1 pages, dictionaries, no CRCs, so UNCOMPRESSED -> ZSTD -> UNCOMPRESSED must reproduce
    // every header field and payload exactly. datapage_v2: v2 pages, whose levels stay outside the compressed values.
    const testing = std.testing;
    const Case = struct { path: []const u8, via: schema.CompressionCodec };
    const cases = [_]Case{
        .{ .path = "data/parquet-testing/data/alltypes_tiny_pages.parquet", .via = .ZSTD },
        .{ .path = "data/parquet-testing/data/datapage_v2.snappy.parquet", .via = .GZIP },
    };
    for (cases) |case| {
        const bytes = metadata.readFileSlice(case.path, testing.allocator) catch |err| {
            if (err == error.FileNotFound) {
                std.debug.print("skipping: {s} not present\n", .{case.path});
                return error.SkipZigTest;
            }
            return err;
        };
        defer testing.allocator.free(bytes);
        var meta = try metadata.open(testing.allocator, bytes);
        defer meta.deinit(testing.allocator);

        var arena = std.heap.ArenaAllocator.init(testing.allocator);
        defer arena.deinit();
        const a = arena.allocator();
        const max = std.math.maxInt(usize);
        var pages: usize = 0;
        for (meta.row_groups.items) |rg| for (rg.columns.items) |chunk| {
            const cm = chunk.meta_data.?;
            const start: usize = @intCast(cm.dictionary_page_offset orelse cm.data_page_offset);
            const src = bytes[start..][0..@intCast(cm.total_compressed_size)];
            // Normalise to UNCOMPRESSED first, so the comparison does not depend on one compressor's output.
            const alloc = testing.allocator;
            const policy = cm.writer_stats;
            const plain = try recompressChunk(a, alloc, src, cm.codec, .UNCOMPRESSED, .{}, max, policy);
            const there = try recompressChunk(a, alloc, plain.bytes, .UNCOMPRESSED, case.via, .{}, max, policy);
            const back = try recompressChunk(a, alloc, there.bytes, case.via, .UNCOMPRESSED, .{}, max, policy);
            try testing.expectEqualSlices(u8, plain.bytes, back.bytes);
            if (cm.codec == .UNCOMPRESSED) try testing.expectEqualSlices(u8, src, plain.bytes);
            try testing.expectEqual(plain.pages.len, there.pages.len);
            // `there` was cut from `plain`, so each of its pages starts where `plain` put that page.
            for (there.pages, plain.pages) |t, p| try testing.expectEqual(p.dst_off, t.src_off);
            pages += there.pages.len;
        };
        try testing.expect(pages > 0);
    }
}

test "recompressRowGroup over several threads writes the bytes one thread writes, and fails where it fails" {
    const testing = std.testing;
    const paths = [_][]const u8{
        "data/parquet-testing/data/alltypes_tiny_pages.parquet",
        "data/parquet-testing/data/datapage_v2.snappy.parquet",
    };
    for (paths) |path| {
        const bytes = metadata.readFileSlice(path, testing.allocator) catch |err| {
            if (err == error.FileNotFound) {
                std.debug.print("skipping: {s} not present\n", .{path});
                return error.SkipZigTest;
            }
            return err;
        };
        defer testing.allocator.free(bytes);
        var meta = try metadata.open(testing.allocator, bytes);
        defer meta.deinit(testing.allocator);
        var arena = std.heap.ArenaAllocator.init(testing.allocator);
        defer arena.deinit();
        const a = arena.allocator();
        var timings: Timings = .{};
        for (meta.row_groups.items) |*rg| for ([_]schema.CompressionCodec{ .ZSTD, .GZIP, .UNCOMPRESSED }) |codec| {
            // Every column, and every other one.
            const alternate = try a.alloc(bool, rg.columns.items.len);
            for (alternate, 0..) |*k, i| k.* = i % 2 == 0;
            for ([_]?[]const bool{ null, alternate }) |kept| {
                const src: RGSrc = .{ .bytes = bytes, .byte_origin = 0 };
                const one = try recompressRowGroup(a, testing.allocator, rg, src, kept, codec, .{}, &timings, 1);
                for ([_]usize{ 2, 5 }) |workers| {
                    const many =
                        try recompressRowGroup(a, testing.allocator, rg, src, kept, codec, .{}, &timings, workers);
                    try testing.expectEqual(one.len, many.len);
                    for (one, many) |x, y| {
                        try testing.expectEqual(x == null, y == null);
                        const p = x orelse continue;
                        const q = y.?;
                        try testing.expectEqualSlices(u8, p.bytes, q.bytes);
                        try testing.expectEqualSlices(PageMove, p.pages, q.pages);
                        try testing.expectEqual(p.header_delta, q.header_delta);
                    }
                }
            }
        };

        // A page body that does not decompress, in the first chunk, and a page header that does not parse, in the
        // last: one thread fails at the body first, and so must several.
        const rg = &meta.row_groups.items[0];
        if (rg.columns.items.len < 2 or rg.columns.items[0].meta_data.?.codec == .UNCOMPRESSED) continue;
        const broken = try a.dupe(u8, bytes);
        const first = rg.columns.items[0].meta_data.?;
        const first_start: usize = @intCast(first.dictionary_page_offset orelse first.data_page_offset);
        var r = thrift.Reader.init(broken[first_start..]);
        const h = try schema.PageHeader.read(&r);
        @memset(broken[first_start + r.pos ..][0..@intCast(@min(h.compressed_page_size, 16))], 0xFF);
        const last = rg.columns.items[rg.columns.items.len - 1].meta_data.?;
        const last_start: usize = @intCast(last.dictionary_page_offset orelse last.data_page_offset);
        @memset(broken[last_start..][0..4], 0xFF);
        const src: RGSrc = .{ .bytes = broken, .byte_origin = 0 };
        const want = if (recompressRowGroup(a, testing.allocator, rg, src, null, .ZSTD, .{}, &timings, 1)) |_|
            return error.TestExpectedError
        else |err|
            err;
        try testing.expectError(want, recompressRowGroup(a, testing.allocator, rg, src, null, .ZSTD, .{}, &timings, 4));
    }
}

test "recompressRowGroup over several threads reports a bad page body ahead of a later chunk with no metadata" {
    const testing = std.testing;
    const path = "data/parquet-testing/data/datapage_v2.snappy.parquet";
    const bytes = metadata.readFileSlice(path, testing.allocator) catch |err| {
        if (err == error.FileNotFound) {
            std.debug.print("skipping: {s} not present\n", .{path});
            return error.SkipZigTest;
        }
        return err;
    };
    defer testing.allocator.free(bytes);
    var meta = try metadata.open(testing.allocator, bytes);
    defer meta.deinit(testing.allocator);
    var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_state.deinit();
    const a = arena_state.allocator();
    const rg = &meta.row_groups.items[0];
    // The first chunk's first page body does not decompress; the last chunk has no metadata.
    const broken = try a.dupe(u8, bytes);
    const m = rg.columns.items[0].meta_data.?;
    const start: usize = @intCast(m.dictionary_page_offset orelse m.data_page_offset);
    var r = thrift.Reader.init(broken[start..]);
    const h = try schema.PageHeader.read(&r);
    @memset(broken[start + r.pos ..][0..@intCast(@min(h.compressed_page_size, 16))], 0xFF);
    const last = &rg.columns.items[rg.columns.items.len - 1];
    const saved = last.meta_data;
    defer last.meta_data = saved;
    last.meta_data = null;
    var timings: Timings = .{};
    const src: RGSrc = .{ .bytes = broken, .byte_origin = 0 };
    const want = if (recompressRowGroup(a, testing.allocator, rg, src, null, .ZSTD, .{}, &timings, 1)) |_|
        return error.TestExpectedError
    else |err|
        err;
    try testing.expectError(want, recompressRowGroup(a, testing.allocator, rg, src, null, .ZSTD, .{}, &timings, 4));
}

test "decimal: nullable BYTE_ARRAY with filter dropping rows preserves definition levels and exactness" {
    const testing = std.testing;
    var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_state.deinit();
    const arena = arena_state.allocator();

    const elem = schema.SchemaElement{
        .type = .BYTE_ARRAY,
        .type_length = null,
        .repetition_type = .OPTIONAL,
        .name = "balance",
        .num_children = 0,
        .converted_type = .DECIMAL,
        .logical_type = .{ .DECIMAL = .{ .scale = 2, .precision = 9 } },
        .scale = 2,
        .precision = 9,
        .field_id = null,
    };

    const raw_strs = [_][]const u8{
        "",
        &[_]u8{ 0x30, 0x39 },
        &[_]u8{ 0xEC, 0x78 },
        "",
        &[_]u8{ 0x0F, 0x42, 0x3F },
        "",
        &[_]u8{0x05},
        "",
    };
    const def_levels = [_]u32{ 0, 1, 1, 0, 1, 0, 1, 0 };
    const enc = try encoder.encodeColumn(arena, .{
        .values = .{ .string = .{
            .values = &raw_strs,
            .def_levels = &def_levels,
            .max_def = 1,
        } },
        .schema_elem = &elem,
        .path_in_schema = &[_][]const u8{"balance"},
        .codec = .UNCOMPRESSED,
    });

    const kind = decimal_mod.kindFromSchema(&elem).?;
    const levels = schema.Levels{ .max_def = 1, .max_rep = 0 };

    const decoded = try decimal_mod.decodeColumnAsI128(arena, enc.bytes, .UNCOMPRESSED, levels, 8, kind);
    try testing.expectEqual(@as(usize, 8), decoded.values.len);
    try testing.expect(decoded.def_levels != null);
    try testing.expectEqualSlices(u32, &def_levels, decoded.def_levels.?);
    try testing.expectEqual(@as(i128, 0), decoded.values[0]);
    try testing.expectEqual(@as(i128, 12345), decoded.values[1]);
    try testing.expectEqual(@as(i128, -5000), decoded.values[2]);
    try testing.expectEqual(@as(i128, 0), decoded.values[3]);
    try testing.expectEqual(@as(i128, 999999), decoded.values[4]);
    try testing.expectEqual(@as(i128, 0), decoded.values[5]);
    try testing.expectEqual(@as(i128, 5), decoded.values[6]);
    try testing.expectEqual(@as(i128, 0), decoded.values[7]);

    var sel = try filter_selection.SelectionVector.init(arena, 8);
    sel.mask[0] = 0b00111100;
    const filtered = try encoder.applySelectionI128(arena, decoded, &sel);
    try testing.expectEqual(@as(usize, 4), filtered.values.len);

    var out_elem = elem;
    out_elem.type = .FIXED_LEN_BYTE_ARRAY;
    out_elem.type_length = decimal_mod.minBytesForPrecision(kind.precision);
    const re_enc = try encoder.encodeDecimalFlba(
        arena,
        filtered,
        &out_elem,
        &[_][]const u8{"balance"},
        .UNCOMPRESSED,
        .{},
    );

    const flba_kind = decimal_mod.kindFromSchema(&out_elem).?;
    const back = try decimal_mod.decodeColumnAsI128(arena, re_enc.bytes, .UNCOMPRESSED, levels, 4, flba_kind);
    try testing.expectEqual(@as(usize, 4), back.values.len);
    try testing.expect(back.def_levels != null);
    const want_defs = [_]u32{ 1, 0, 1, 0 };
    try testing.expectEqualSlices(u32, &want_defs, back.def_levels.?);
    try testing.expectEqual(@as(i128, -5000), back.values[0]);
    try testing.expectEqual(@as(i128, 0), back.values[1]);
    try testing.expectEqual(@as(i128, 999999), back.values[2]);
    try testing.expectEqual(@as(i128, 0), back.values[3]);
}

test "StagePlan.init in countOnly mode plans zero calls and charges only base columns" {
    var arena_state = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena_state.deinit();
    const arena = arena_state.allocator();

    var file_meta: schema.FileMetaData = .{
        .version = 1,
        .schema = .empty,
        .num_rows = 0,
        .created_by = null,
        .row_groups = .empty,
    };
    try file_meta.schema.append(arena, .{
        .name = "schema",
        .num_children = 2,
        .type = null,
        .type_length = null,
        .repetition_type = .REQUIRED,
        .scale = null,
        .precision = null,
        .field_id = null,
    });
    try file_meta.schema.append(arena, .{
        .name = "k",
        .type = .INT64,
        .num_children = 0,
        .repetition_type = .REQUIRED,
        .type_length = null,
        .scale = null,
        .precision = null,
        .field_id = 0,
    });
    try file_meta.schema.append(arena, .{
        .name = "v",
        .type = .INT64,
        .num_children = 0,
        .repetition_type = .REQUIRED,
        .type_length = null,
        .scale = null,
        .precision = null,
        .field_id = 1,
    });

    var path_k: schema.StringList = .empty;
    try path_k.append(arena, "k");
    var path_v: schema.StringList = .empty;
    try path_v.append(arena, "v");

    var columns: std.ArrayListUnmanaged(schema.ColumnChunk) = .empty;
    try columns.append(arena, .{
        .file_path = null,
        .file_offset = 0,
        .meta_data = .{
            .type = .INT64,
            .encodings = .empty,
            .path_in_schema = path_k,
            .codec = .UNCOMPRESSED,
            .num_values = 100,
            .total_uncompressed_size = 800,
            .total_compressed_size = 800,
            .data_page_offset = 0,
            .index_page_offset = null,
            .dictionary_page_offset = null,
        },
    });
    try columns.append(arena, .{
        .file_path = null,
        .file_offset = 0,
        .meta_data = .{
            .type = .INT64,
            .encodings = .empty,
            .path_in_schema = path_v,
            .codec = .UNCOMPRESSED,
            .num_values = 100,
            .total_uncompressed_size = 800,
            .total_compressed_size = 800,
            .data_page_offset = 0,
            .index_page_offset = null,
            .dictionary_page_offset = null,
        },
    });
    try file_meta.row_groups.append(arena, .{
        .columns = columns,
        .total_byte_size = 1600,
        .num_rows = 100,
    });

    const in: AggRowGroup = .{
        .rg = &file_meta.row_groups.items[0],
        .file_index = 0,
        .rg_index = 0,
        .meta = &file_meta,
        .src = .{ .bytes = &.{}, .byte_origin = 0 },
        .filter = null,
        .read = &.{ true, true },
        .consumed = &.{ true, false },
    };

    var table: expr_agg.GroupTable = .init(std.testing.allocator, 1 << 20);
    defer table.deinit();
    var sketch: expr_agg.CountSketch = .{ .cells = try arena.alloc(u32, 16), .shift = 60 };
    @memset(sketch.cells, 0);
    table.sketch = &sketch;
    try std.testing.expect(table.countOnly());

    const left_expr = try arena.create(expr_ast.Expr);
    left_expr.* = .{ .col_ref = .{ .col_idx = 1, .physical_type = .INT64, .expr_type = .i64 } };
    const right_expr = try arena.create(expr_ast.Expr);
    right_expr.* = .{ .col_ref = .{ .col_idx = 1, .physical_type = .INT64, .expr_type = .i64 } };
    const arg_expr: expr_ast.Expr = .{
        .binop = .{
            .op = .add,
            .left = left_expr,
            .right = right_expr,
            .result_type = .i64,
            .depth = 2,
        },
    };

    const calls = [_]expr_agg.AggCall{.{
        .func = .sum,
        .alias = "s",
        .result = .i64,
        .arg = arg_expr,
        .where = null,
    }};
    const keys = [_]expr_ast.Expr{.{ .col_ref = .{ .col_idx = 0, .physical_type = .INT64, .expr_type = .i64 } }};
    const target: AggTarget = .{ .grouped = .{
        .calls = &calls,
        .keys = &keys,
        .table = &table,
        .pos_base = 0,
    } };

    const sp = try StagePlan.init(arena, in, target, &.{false});
    try std.testing.expectEqual(@as(usize, 0), sp.calls.len);
    try std.testing.expectEqual(decodeEstimate(in, sp.base, &.{}, true), sp.estimate);
}

test "StagePlan.init in countOnly mode: allocation failures cleanly unwind without leak" {
    try std.testing.checkAllAllocationFailures(std.testing.allocator, struct {
        fn run(gpa: std.mem.Allocator) !void {
            var arena_state = std.heap.ArenaAllocator.init(gpa);
            defer arena_state.deinit();
            const arena = arena_state.allocator();

            var file_meta: schema.FileMetaData = .{
                .version = 1,
                .schema = .empty,
                .num_rows = 0,
                .created_by = null,
                .row_groups = .empty,
            };
            try file_meta.schema.append(arena, .{
                .name = "schema",
                .num_children = 2,
                .type = null,
                .type_length = null,
                .repetition_type = .REQUIRED,
                .scale = null,
                .precision = null,
                .field_id = null,
            });
            try file_meta.schema.append(arena, .{
                .name = "k",
                .type = .INT64,
                .num_children = 0,
                .repetition_type = .REQUIRED,
                .type_length = null,
                .scale = null,
                .precision = null,
                .field_id = 0,
            });
            try file_meta.schema.append(arena, .{
                .name = "v",
                .type = .INT64,
                .num_children = 0,
                .repetition_type = .REQUIRED,
                .type_length = null,
                .scale = null,
                .precision = null,
                .field_id = 1,
            });

            var path_k: schema.StringList = .empty;
            try path_k.append(arena, "k");
            var path_v: schema.StringList = .empty;
            try path_v.append(arena, "v");

            var columns: std.ArrayListUnmanaged(schema.ColumnChunk) = .empty;
            try columns.append(arena, .{
                .file_path = null,
                .file_offset = 0,
                .meta_data = .{
                    .type = .INT64,
                    .encodings = .empty,
                    .path_in_schema = path_k,
                    .codec = .UNCOMPRESSED,
                    .num_values = 100,
                    .total_uncompressed_size = 800,
                    .total_compressed_size = 800,
                    .data_page_offset = 0,
                    .index_page_offset = null,
                    .dictionary_page_offset = null,
                },
            });
            try columns.append(arena, .{
                .file_path = null,
                .file_offset = 0,
                .meta_data = .{
                    .type = .INT64,
                    .encodings = .empty,
                    .path_in_schema = path_v,
                    .codec = .UNCOMPRESSED,
                    .num_values = 100,
                    .total_uncompressed_size = 800,
                    .total_compressed_size = 800,
                    .data_page_offset = 0,
                    .index_page_offset = null,
                    .dictionary_page_offset = null,
                },
            });
            try file_meta.row_groups.append(arena, .{
                .columns = columns,
                .total_byte_size = 1600,
                .num_rows = 100,
            });

            const in: AggRowGroup = .{
                .rg = &file_meta.row_groups.items[0],
                .file_index = 0,
                .rg_index = 0,
                .meta = &file_meta,
                .src = .{ .bytes = &.{}, .byte_origin = 0 },
                .filter = null,
                .read = &.{ true, true },
                .consumed = &.{ true, false },
            };

            var table: expr_agg.GroupTable = .init(arena, 1 << 20);
            var sketch: expr_agg.CountSketch = .{ .cells = try arena.alloc(u32, 16), .shift = 60 };
            @memset(sketch.cells, 0);
            table.sketch = &sketch;

            const left_expr = try arena.create(expr_ast.Expr);
            left_expr.* = .{ .col_ref = .{ .col_idx = 1, .physical_type = .INT64, .expr_type = .i64 } };
            const right_expr = try arena.create(expr_ast.Expr);
            right_expr.* = .{ .col_ref = .{ .col_idx = 1, .physical_type = .INT64, .expr_type = .i64 } };
            const arg_expr: expr_ast.Expr = .{
                .binop = .{
                    .op = .add,
                    .left = left_expr,
                    .right = right_expr,
                    .result_type = .i64,
                    .depth = 2,
                },
            };

            const calls = [_]expr_agg.AggCall{.{
                .func = .sum,
                .alias = "s",
                .result = .i64,
                .arg = arg_expr,
                .where = null,
            }};
            const keys = [_]expr_ast.Expr{.{ .col_ref = .{ .col_idx = 0, .physical_type = .INT64, .expr_type = .i64 } }};
            const target: AggTarget = .{ .grouped = .{
                .calls = &calls,
                .keys = &keys,
                .table = &table,
                .pos_base = 0,
            } };

            const sp = try StagePlan.init(arena, in, target, &.{false});
            try std.testing.expectEqual(@as(usize, 0), sp.calls.len);
        }
    }.run, .{});
}

test "a key chunk whose statistics claim no nulls but holds one declines the first-row histogram and decodes as codes" {
    const testing = std.testing;
    const hybrid_rle = @import("parquet/encoding/hybrid_rle.zig");
    var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_state.deinit();
    const a = arena_state.allocator();

    var file_meta = schema.FileMetaData{
        .version = 2,
        .schema = .empty,
        .num_rows = 64,
        .created_by = null,
        .row_groups = .empty,
    };
    try file_meta.schema.append(a, .{
        .type = null,
        .type_length = null,
        .repetition_type = .REQUIRED,
        .name = "schema",
        .num_children = 1,
        .scale = null,
        .precision = null,
        .field_id = null,
    });
    try file_meta.schema.append(a, .{
        .type = .INT64,
        .type_length = null,
        .repetition_type = .OPTIONAL,
        .name = "k",
        .num_children = 0,
        .scale = null,
        .precision = null,
        .field_id = 1,
    });

    // A 4-entry dictionary; 64 leaves, the 40th null. The chunk statistics wrongly count no nulls.
    const dict = std.mem.toBytes([4]i64{ 400, 100, 300, 200 });
    var levels: [64]u32 = @splat(1);
    levels[39] = 0;
    var codes: [63]u32 = undefined;
    for (&codes, 0..) |*c, i| c.* = @intCast((i * 5 / 3) % 4);
    var body: std.ArrayList(u8) = .empty;
    const def_bytes = try hybrid_rle.encode(a, &levels, 1);
    try body.appendSlice(a, &std.mem.toBytes(@as(u32, @intCast(def_bytes.len))));
    try body.appendSlice(a, def_bytes);
    try body.append(a, 2);
    try body.appendSlice(a, try hybrid_rle.encode(a, &codes, 2));
    var chunk: std.ArrayList(u8) = .empty;
    try chunk.appendSlice(a, try pageForTest(a, .{
        .type = .DICTIONARY_PAGE,
        .uncompressed_page_size = dict.len,
        .compressed_page_size = dict.len,
        .crc = null,
        .data_page_header = null,
        .dictionary_page_header = .{ .num_values = 4, .encoding = .PLAIN, .is_sorted = null },
        .data_page_header_v2 = null,
    }, &dict));
    try chunk.appendSlice(a, try pageForTest(a, .{
        .type = .DATA_PAGE,
        .uncompressed_page_size = @intCast(body.items.len),
        .compressed_page_size = @intCast(body.items.len),
        .crc = null,
        .data_page_header = .{
            .num_values = 64,
            .encoding = .RLE_DICTIONARY,
            .definition_level_encoding = .RLE,
            .repetition_level_encoding = .RLE,
        },
        .dictionary_page_header = null,
        .data_page_header_v2 = null,
    }, body.items));

    var path: schema.StringList = .empty;
    try path.append(a, "k");
    const cm: schema.ColumnMetaData = .{
        .type = .INT64,
        .encodings = .empty,
        .path_in_schema = path,
        .codec = .UNCOMPRESSED,
        .num_values = 64,
        .total_uncompressed_size = @intCast(chunk.items.len),
        .total_compressed_size = @intCast(chunk.items.len),
        .data_page_offset = 0,
        .index_page_offset = null,
        .dictionary_page_offset = null,
        .statistics = .{ .null_count = 0 },
    };
    var scratch = DecodeScratch.init(a);
    const declined = try decodeAggColumnHist(a, chunk.items, cm, &file_meta, .{ .encoded = true }, &scratch, 64, true);
    try testing.expect(declined == null);
    // The retry the scan then makes: the right codes, and the null row marked.
    const col = (try decodeAggColumnCoded(a, chunk.items, cm, &file_meta, .{ .encoded = true }, &scratch)).?;
    const got = col.i64;
    try testing.expect(got.has_nulls);
    try testing.expectEqualSlices(u32, &levels, got.def_levels.?);
    var next: usize = 0;
    for (levels, 0..) |l, r| if (l == 1) {
        try testing.expectEqual(codes[next], got.codes.?[r]);
        next += 1;
    };
    try testing.expectEqualSlices(i64, &.{ 400, 100, 300, 200 }, got.dict);
}

fn expectSameColumn(want: filter_eval.Batch.Column, got: filter_eval.Batch.Column) !void {
    const testing = std.testing;
    try testing.expectEqual(std.meta.activeTag(want), std.meta.activeTag(got));
    switch (want) {
        inline else => |w, tag| {
            const g = @field(got, @tagName(tag));
            const T = @typeInfo(@TypeOf(w.values)).pointer.child;
            try testing.expectEqual(w.values.len, g.values.len);
            if (T == []const u8) {
                for (w.values, g.values) |x, y| try testing.expectEqualStrings(x, y);
                try testing.expectEqual(w.dict.len, g.dict.len);
                for (w.dict, g.dict) |x, y| try testing.expectEqualStrings(x, y);
            } else {
                try testing.expectEqualSlices(u8, std.mem.sliceAsBytes(w.values), std.mem.sliceAsBytes(g.values));
                try testing.expectEqualSlices(u8, std.mem.sliceAsBytes(w.dict), std.mem.sliceAsBytes(g.dict));
            }
            if (w.codes) |c| try testing.expectEqualSlices(u32, c, g.codes.?) else try testing.expect(g.codes == null);
            if (w.def_levels) |d|
                try testing.expectEqualSlices(u32, d, g.def_levels.?)
            else
                try testing.expect(g.def_levels == null);
        },
    }
}

test "a pooled slice reader reuses its page and decodes the same rows" {
    const testing = std.testing;
    const bytes = metadata.readFileSlice("ci/fixtures/parquet/topn_gather_a.parquet", testing.allocator) catch |err| {
        if (err == error.FileNotFound) return error.SkipZigTest;
        return err;
    };
    defer testing.allocator.free(bytes);
    var meta = try metadata.open(testing.allocator, bytes);
    defer meta.deinit(testing.allocator);
    var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_state.deinit();
    const a = arena_state.allocator();
    const src: RGSrc = .{ .bytes = bytes, .byte_origin = 0 };
    const rg = &meta.row_groups.items[1];
    var pool = SliceScratch.init(testing.allocator);
    defer pool.deinit();
    try pool.reserve(1);
    for (0..rg.columns.items.len) |ci| {
        var owned = (try SliceReader.init(a, testing.allocator, rg, &meta, src, ci, .{})).?;
        defer owned.deinit();
        const n: usize = @intCast(rg.num_rows);
        const want = try owned.next(a, n);
        pool.rewind();
        var pooled = (try SliceReader.initPooled(a, testing.allocator, rg, &meta, src, ci, .{}, &pool)).?;
        const got = try pooled.next(a, n);
        try expectSameColumn(want, got);
        const page_ptr = pooled.scratch.page.ptr;
        const page_len = pooled.scratch.page.len;
        pooled.deinit();
        pool.rewind();
        if (page_len > 0) {
            const slot = pool.checkout();
            try testing.expectEqual(page_ptr, slot.scratch.page.ptr);
            if (std.debug.runtime_safety) try testing.expectEqual(SliceScratch.reuse_poison, slot.scratch.page[0]);
            pool.rewind();
        }
        var again = (try SliceReader.initPooled(a, testing.allocator, rg, &meta, src, ci, .{}, &pool)).?;
        defer again.deinit();
        try expectSameColumn(want, try again.next(a, n));
    }
}

test "a sliced string reader under chargePages draws each page and its dictionary, and gives all back at deinit" {
    const testing = std.testing;
    // A dictionary-coded, snappy string column (`s`) of eight row groups.
    const path = "ci/fixtures/parquet/census_dictionary_review.parquet";
    const bytes = metadata.readFileSlice(path, testing.allocator) catch |err| {
        if (err == error.FileNotFound) return error.SkipZigTest;
        return err;
    };
    defer testing.allocator.free(bytes);
    var meta = try metadata.open(testing.allocator, bytes);
    defer meta.deinit(testing.allocator);
    var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_state.deinit();
    const a = arena_state.allocator();
    const src: RGSrc = .{ .bytes = bytes, .byte_origin = 0 };
    const rg = &meta.row_groups.items[0];
    const rows: usize = @intCast(rg.num_rows);
    var plain = (try SliceReader.init(a, testing.allocator, rg, &meta, src, 2, .{})).?;
    defer plain.deinit();
    const want = try plain.next(a, rows);
    var budget: core_memory.SharedBudget = .{ .limit = 1 << 30 };
    {
        var r = (try SliceReader.init(a, testing.allocator, rg, &meta, src, 2, .{})).?;
        defer r.deinit();
        r.chargePages(testing.allocator, &budget);
        var at: usize = 0;
        while (at < rows) {
            const n = @min(rows - at, 37);
            const got = try r.next(a, n);
            for (want.string.values[at..][0..n], got.string.values) |w, g| try testing.expectEqualStrings(w, g);
            at += n;
        }
        try testing.expect(budget.peak.load(.monotonic) > 0);
    }
    try testing.expectEqual(@as(usize, 0), budget.used.load(.monotonic));
    // A page the budget cannot take is refused before it is decompressed.
    var tiny: core_memory.SharedBudget = .{ .limit = 1 };
    var r = (try SliceReader.init(a, testing.allocator, rg, &meta, src, 2, .{})).?;
    defer r.deinit();
    r.chargePages(testing.allocator, &tiny);
    try testing.expectError(error.ExceededMemoryBudget, r.next(a, rows));
    try testing.expectEqual(@as(usize, 0), tiny.used.load(.monotonic));
}

test "retained delta pages reset under the decode ledger, including optional levels" {
    const testing = std.testing;
    var build = std.heap.ArenaAllocator.init(testing.allocator);
    defer build.deinit();
    const arena = build.allocator();
    // Shared prefixes so the chunk is DELTA_BYTE_ARRAY. Each page reconstructs about a mebibyte of strings, and
    // enough pages that keeping those strings would pass the 32 MiB decode ledger. Every fifth row is null.
    const value_len = 2048;
    const prefix_len = 2000;
    const per_page = (1 << 20) / (value_len + 4);
    const n = per_page * 40 * 5 / 4;
    const prefix = try arena.alloc(u8, prefix_len);
    @memset(prefix, 'p');
    const values = try arena.alloc([]const u8, n);
    const defs = try arena.alloc(u32, n);
    for (values, defs, 0..) |*v, *d, i| {
        const present = i % 5 != 0;
        d.* = @intFromBool(present);
        if (!present) {
            v.* = "";
            continue;
        }
        const buf = try arena.alloc(u8, value_len);
        @memcpy(buf[0..prefix_len], prefix);
        _ = std.fmt.bufPrint(buf[prefix_len..], "{d:0>48}", .{i}) catch unreachable;
        v.* = buf;
    }
    const elem: schema.SchemaElement = .{
        .name = "s",
        .type = .BYTE_ARRAY,
        .repetition_type = .OPTIONAL,
        .type_length = null,
        .num_children = 0,
        .scale = null,
        .precision = null,
        .field_id = null,
    };
    const enc = try encoder.encodeColumn(arena, .{
        .values = .{ .string = .{
            .values = values,
            .def_levels = defs,
            .max_def = 1,
            .has_nulls = true,
        } },
        .schema_elem = &elem,
        .path_in_schema = &.{"s"},
        .codec = .UNCOMPRESSED,
        .distinct = true,
        .dictionary_bloom = false,
        .page_bytes = 1 << 20,
    });
    var delta = false;
    for (enc.meta.encodings.items) |e| if (e == .DELTA_BYTE_ARRAY) {
        delta = true;
    };
    try testing.expect(delta);
    try testing.expect(enc.pages.len >= 8);
    var present_bytes: usize = 0;
    for (values, defs) |v, d| if (d == 1) {
        present_bytes += v.len;
    };
    try testing.expect(present_bytes > core_memory.lean_decode);

    var file_meta = schema.FileMetaData{
        .version = 2,
        .schema = .empty,
        .num_rows = @intCast(n),
        .created_by = null,
        .row_groups = .empty,
    };
    try file_meta.schema.append(arena, .{
        .type = null,
        .type_length = null,
        .repetition_type = .REQUIRED,
        .name = "schema",
        .num_children = 1,
        .scale = null,
        .precision = null,
        .field_id = null,
    });
    try file_meta.schema.append(arena, elem);
    var columns: std.ArrayListUnmanaged(schema.ColumnChunk) = .empty;
    try columns.append(arena, .{ .file_path = null, .file_offset = 0, .meta_data = enc.meta });
    try file_meta.row_groups.append(arena, .{
        .columns = columns,
        .total_byte_size = @intCast(enc.bytes.len),
        .num_rows = @intCast(n),
    });

    var budget: core_memory.SharedBudget = .{ .limit = core_memory.lean_decode };
    var charged: core_memory.Charged = .{ .child = testing.allocator, .budget = &budget };
    var pool = SliceScratch.init(charged.allocator());
    var pool_live = true;
    defer if (pool_live) pool.deinit();
    try pool.reserve(1);
    const src: RGSrc = .{ .bytes = enc.bytes, .byte_origin = 0 };
    const rg = &file_meta.row_groups.items[0];
    var reader = (try SliceReader.initPooled(arena, testing.allocator, rg, &file_meta, src, 0, .{}, &pool)).?;
    var reader_live = true;
    defer if (reader_live) reader.deinit();
    const first: usize = @intCast(enc.pages[0].rows);
    var at: usize = 0;
    var after_one: usize = 0;
    const mid = n / 2;
    while (at < n) {
        const k = @min(n - at, first);
        const got = try reader.next(arena, k);
        if (at == 0) {
            try testing.expect(got.string.has_nulls);
            try testing.expectEqual(@as(u32, 1), got.string.max_def);
            try testing.expectEqual(@as(u32, 0), got.string.def_levels.?[0]);
            try testing.expectEqualStrings(values[1], got.string.values[1]);
            after_one = reader.pages.queryCapacity();
            try testing.expect(after_one > 0);
        }
        if (at <= mid and at + k > mid and defs[mid] == 1) {
            try testing.expectEqualStrings(values[mid], got.string.values[mid - at]);
        }
        at += k;
    }
    const after_all = reader.pages.queryCapacity();
    try testing.expect(after_all <= after_one * 2);
    try testing.expect(after_all < core_memory.lean_decode / 2);
    try testing.expect(reader.dict_store.?.queryCapacity() < core_memory.lean_decode / 8);
    try testing.expect(budget.used.load(.monotonic) <= core_memory.lean_decode);
    reader.deinit();
    reader_live = false;
    // Reader teardown returns its scratch to the pool; the pool owns that retained charge.
    pool.deinit();
    pool_live = false;
    try testing.expectEqual(@as(usize, 0), budget.used.load(.monotonic));
}

/// Columns of each lane and encoding an answer writes: near-unique integers (DELTA), repeating ones (dictionary),
/// floats and strings with NULLs, and a DECIMAL whose value at `bad_row`, when set, its INT32 cannot hold.
fn aggAnswerCols(a: std.mem.Allocator, n: usize, bad_row: ?usize) ![]expr_agg.OutputCol {
    const uniq = try a.alloc(i64, n);
    const reps = try a.alloc(i64, n);
    const fl = try a.alloc(f64, n);
    const strs = try a.alloc([]const u8, n);
    const decs = try a.alloc(i128, n);
    const defs = try a.alloc(u32, n);
    for (0..n) |i| {
        uniq[i] = @bitCast(@as(u64, i) *% 0x9E3779B97F4A7C15);
        reps[i] = @intCast(i % 37);
        fl[i] = @as(f64, @floatFromInt(i % 1000)) * 0.25;
        strs[i] = try std.fmt.allocPrint(a, "v{d}", .{i % 500});
        decs[i] = @intCast(i % 9000);
        defs[i] = @intFromBool(i % 7 != 0);
    }
    if (bad_row) |r| decs[r] = 1 << 40;
    const cols = try a.alloc(expr_agg.OutputCol, 5);
    cols[0] = .{
        .name = "u",
        .col = .{ .i64 = .{ .values = uniq, .max_def = 0 } },
        .parquet_type = .INT64,
        .nullable = false,
    };
    cols[1] = .{
        .name = "r",
        .col = .{ .i64 = .{ .values = reps, .max_def = 0 } },
        .parquet_type = .INT64,
        .nullable = false,
    };
    cols[2] = .{
        .name = "d",
        .col = .{ .i128 = .{ .values = decs, .max_def = 0 } },
        .parquet_type = .INT32,
        .nullable = false,
        .dec = .{ .precision = 9, .scale = 2 },
    };
    cols[3] = .{
        .name = "f",
        .col = .{ .f64 = .{ .values = fl, .def_levels = defs, .max_def = 1, .has_nulls = true } },
        .parquet_type = .DOUBLE,
        .nullable = true,
    };
    cols[4] = .{
        .name = "s",
        .col = .{ .string = .{ .values = strs, .def_levels = defs, .max_def = 1, .has_nulls = true } },
        .parquet_type = .BYTE_ARRAY,
        .nullable = true,
    };
    return cols;
}

/// Keeps what is written; fails the write numbered `fail_at` (0 is the magic), if set.
const AnswerCapture = struct {
    a: std.mem.Allocator,
    bytes: std.ArrayList(u8) = .empty,
    writes: usize = 0,
    fail_at: ?usize = null,

    fn sink(c: *AnswerCapture) streaming.Sink {
        return .{ .ctx = @ptrCast(c), .write_fn = write };
    }

    fn write(ctx: *anyopaque, b: []const u8) anyerror!void {
        const c: *AnswerCapture = @ptrCast(@alignCast(ctx));
        defer c.writes += 1;
        if (c.fail_at == c.writes) return error.TestSinkFailed;
        try c.bytes.appendSlice(c.a, b);
    }
};

test "writeAggregateRows: columns encoded on threads write the serial bytes, whatever spawns fail" {
    var arena_state = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena_state.deinit();
    const a = arena_state.allocator();
    const n = 70_000;
    const cols = try aggAnswerCols(a, n, null);
    for ([_]schema.CompressionCodec{ .SNAPPY, .ZSTD, .UNCOMPRESSED }) |codec| {
        var serial: AnswerCapture = .{ .a = a };
        const want = try writeAggregateRowsEncoded(a, serial.sink(), cols, n, answer_rg_rows, codec, .{}, .{});
        try std.testing.expectEqual(@as(u64, serial.bytes.items.len), want);
        // Every column in one wave; two columns' charge a wave, so waves of two and lone columns; and spawns failing.
        const two = aggEncodeCharge(cols[0]) + aggEncodeCharge(cols[1]);
        const Run = struct { encode: AggEncode, fail_after: ?usize = null };
        for ([_]Run{
            .{ .encode = .{ .workers = 8, .budget = std.math.maxInt(u64) } },
            .{ .encode = .{ .workers = 3, .budget = two } },
            .{ .encode = .{ .workers = 8, .budget = std.math.maxInt(u64) }, .fail_after = 0 },
            .{ .encode = .{ .workers = 8, .budget = std.math.maxInt(u64) }, .fail_after = 2 },
        }) |run| {
            if (run.fail_after) |k| spawn_util.injectFailureAfter(k);
            defer spawn_util.resetFailure();
            var par: AnswerCapture = .{ .a = a };
            const got = try writeAggregateRowsEncoded(a, par.sink(), cols, n, answer_rg_rows, codec, .{}, run.encode);
            try std.testing.expectEqual(want, got);
            try std.testing.expectEqualSlices(u8, serial.bytes.items, par.bytes.items);
        }
    }
}

test "writeAggregateRows: on threads, the serial error and the bytes written before it" {
    var arena_state = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena_state.deinit();
    const a = arena_state.allocator();
    const n = 1000;
    // Column 2 cannot encode; write 1 (column 0) fails, or write 2 (column 1), or none does.
    const cols = try aggAnswerCols(a, n, 600);
    for ([_]?usize{ null, 1, 2 }) |fail_at| {
        var serial: AnswerCapture = .{ .a = a, .fail_at = fail_at };
        const want = writeAggregateRowsEncoded(a, serial.sink(), cols, n, answer_rg_rows, .SNAPPY, .{}, .{});
        var par: AnswerCapture = .{ .a = a, .fail_at = fail_at };
        const all: AggEncode = .{ .workers = 8, .budget = std.math.maxInt(u64) };
        const got = writeAggregateRowsEncoded(a, par.sink(), cols, n, answer_rg_rows, .SNAPPY, .{}, all);
        try std.testing.expectError(if (fail_at == null) error.UnsupportedType else error.TestSinkFailed, want);
        try std.testing.expectEqual(want, got);
        try std.testing.expectEqualSlices(u8, serial.bytes.items, par.bytes.items);
    }
}

fn writeAnswerOnThreads(a: std.mem.Allocator) !void {
    var arena_state = std.heap.ArenaAllocator.init(a);
    defer arena_state.deinit();
    const arena = arena_state.allocator();
    const cols = try aggAnswerCols(arena, 300, null);
    var out: AnswerCapture = .{ .a = arena };
    _ = try writeAggregateRowsEncoded(
        arena,
        out.sink(),
        cols,
        300,
        answer_rg_rows,
        .SNAPPY,
        .{},
        .{ .workers = 4, .budget = 1 << 40 },
    );
}

test "writeAggregateRows: one worker, or a budget no two columns fit, starts no thread" {
    var arena_state = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena_state.deinit();
    const a = arena_state.allocator();
    const n = 70_000;
    const cols = try aggAnswerCols(a, n, null);
    for ([_]AggEncode{ .{ .workers = 1, .budget = std.math.maxInt(u64) }, .{ .workers = 16, .budget = 0 } }) |encode| {
        // A spawn would fail and be counted.
        spawn_util.injectFailureAfter(0);
        defer spawn_util.resetFailure();
        var out: AnswerCapture = .{ .a = a };
        _ = try writeAggregateRows(a, out.sink(), cols, n, .SNAPPY, .{}, encode);
        try std.testing.expectEqual(@as(usize, 0), spawn_util.failuresInjected());
    }
}

test "writeAggregateRows: a column encoded alone still draws from the ledger" {
    var arena_state = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena_state.deinit();
    const a = arena_state.allocator();
    const cols = try aggAnswerCols(a, 1000, null);
    // One column, and few rows: no waves, but the column is the ledger's to refuse.
    var ledger: expr_agg.SharedBudget = .{ .limit = 1 };
    var out: AnswerCapture = .{ .a = a };
    const encode: AggEncode = .{ .workers = 16, .budget = std.math.maxInt(u64), .ledger = &ledger };
    const one = writeAggregateRows(a, out.sink(), cols[0..1], 1000, .SNAPPY, .{}, encode);
    try std.testing.expectError(error.ExceededMemoryBudget, one);
    ledger.limit = 1 << 30;
    _ = try writeAggregateRows(a, out.sink(), cols, 1000, .SNAPPY, .{}, encode);
    try std.testing.expectEqual(@as(usize, 0), ledger.used.load(.monotonic));
}

test "writeAggregateRows: on threads, an allocation failure frees everything" {
    try std.testing.checkAllAllocationFailures(std.testing.allocator, writeAnswerOnThreads, .{});
}

/// The bytes between a written answer's leading magic and its footer: its column chunks, in order.
fn answerChunks(a: std.mem.Allocator, file: []const u8) ![]const u8 {
    const meta = try answerFooter(a, file);
    const footer_len = std.mem.readInt(u32, file[file.len - 8 ..][0..4], .little);
    var end = file.len - 8 - footer_len;
    for (meta.row_groups.items) |rg| for (rg.columns.items) |c| {
        if (c.meta_data) |cm| {
            if (cm.bloom_filter_offset) |off| end = @min(end, @as(usize, @intCast(off)));
        }
    };
    return file[4..end];
}

fn answerFooter(a: std.mem.Allocator, file: []const u8) !schema.FileMetaData {
    const footer_len = std.mem.readInt(u32, file[file.len - 8 ..][0..4], .little);
    var reader = thrift.Reader.init(file[file.len - 8 - footer_len .. file.len - 8]);
    return schema.FileMetaData.read(a, &reader);
}

test "writeAggregateRows: big answers use fixed row groups with serial chunks at every worker count" {
    var arena_state = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena_state.deinit();
    const a = arena_state.allocator();
    const n = 1000;
    const rg = 300;
    const cols = try aggAnswerCols(a, n, null);
    var serial: AnswerCapture = .{ .a = a };
    _ = try writeAggregateRowsEncoded(a, serial.sink(), cols, n, rg, .SNAPPY, .{}, .{});
    const meta = try answerFooter(a, serial.bytes.items);
    try std.testing.expectEqual(@as(i64, n), meta.num_rows);
    try std.testing.expectEqual(@as(usize, 4), meta.row_groups.items.len);
    for (meta.row_groups.items, [_]i64{ 300, 300, 300, 100 }) |g, rows| {
        try std.testing.expectEqual(rows, g.num_rows);
        try std.testing.expectEqual(cols.len, g.columns.items.len);
    }
    // Row group g's chunks are what its rows encode to as an answer of their own, in order.
    var want: std.ArrayList(u8) = .empty;
    for (0..4) |g| {
        const lo = g * rg;
        const hi = @min(n, lo + rg);
        const part = try a.alloc(expr_agg.OutputCol, cols.len);
        for (cols, part) |c, *p| p.* = sliceOutputCol(c, lo, hi);
        var alone: AnswerCapture = .{ .a = a };
        _ = try writeAggregateRowsEncoded(a, alone.sink(), part, @intCast(hi - lo), rg, .SNAPPY, .{}, .{});
        try want.appendSlice(a, try answerChunks(a, alone.bytes.items));
    }
    try std.testing.expectEqualSlices(u8, want.items, try answerChunks(a, serial.bytes.items));
    // The same file whatever the workers, the budget's waves, or failing spawns.
    const two = aggEncodeCharge(sliceOutputCol(cols[0], 0, rg)) + aggEncodeCharge(sliceOutputCol(cols[1], 0, rg));
    const Run = struct { encode: AggEncode, fail_after: ?usize = null };
    for ([_]Run{
        .{ .encode = .{ .workers = 16, .budget = std.math.maxInt(u64) } },
        .{ .encode = .{ .workers = 3, .budget = two } },
        .{ .encode = .{ .workers = 7, .budget = std.math.maxInt(u64) }, .fail_after = 1 },
    }) |run| {
        if (run.fail_after) |k| spawn_util.injectFailureAfter(k);
        defer spawn_util.resetFailure();
        var par: AnswerCapture = .{ .a = a };
        _ = try writeAggregateRowsEncoded(a, par.sink(), cols, n, rg, .SNAPPY, .{}, run.encode);
        try std.testing.expectEqualSlices(u8, serial.bytes.items, par.bytes.items);
    }
}

test "writeAggregateRows: an answer of a row group's rows or fewer is one row group" {
    var arena_state = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena_state.deinit();
    const a = arena_state.allocator();
    const cols = try aggAnswerCols(a, 300, null);
    for ([_]usize{ 0, 1, 300 }) |n| {
        const part = try a.alloc(expr_agg.OutputCol, cols.len);
        for (cols, part) |c, *p| p.* = sliceOutputCol(c, 0, n);
        var out: AnswerCapture = .{ .a = a };
        _ = try writeAggregateRowsEncoded(a, out.sink(), part, @intCast(n), 300, .SNAPPY, .{}, .{});
        const meta = try answerFooter(a, out.bytes.items);
        try std.testing.expectEqual(@as(usize, 1), meta.row_groups.items.len);
        try std.testing.expectEqual(@as(i64, @intCast(n)), meta.row_groups.items[0].num_rows);
    }
}

fn writeRowGroupsOnThreads(a: std.mem.Allocator) !void {
    var arena_state = std.heap.ArenaAllocator.init(a);
    defer arena_state.deinit();
    const arena = arena_state.allocator();
    const cols = try aggAnswerCols(arena, 300, null);
    var out: AnswerCapture = .{ .a = arena };
    _ = try writeAggregateRowsEncoded(
        arena,
        out.sink(),
        cols,
        300,
        128,
        .SNAPPY,
        .{},
        .{ .workers = 4, .budget = 1 << 40 },
    );
}

test "writeAggregateRows: row groups on threads, an allocation failure frees everything" {
    try std.testing.checkAllAllocationFailures(std.testing.allocator, writeRowGroupsOnThreads, .{});
}

fn writeAnswerWindows(a: std.mem.Allocator) !void {
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const alloc = arena.allocator();
    const cols = try aggAnswerCols(alloc, 1000, null);
    var whole: AnswerCapture = .{ .a = alloc };
    _ = try writeAggregateRowsEncoded(alloc, whole.sink(), cols, 1000, 300, .SNAPPY, .{}, .{});
    var windows: AnswerCapture = .{ .a = alloc };
    var file = try AggregateFile.init(alloc, windows.sink());
    defer file.deinit();
    for ([_][2]usize{ .{ 0, 600 }, .{ 600, 1000 } }) |range| {
        const part = try alloc.alloc(expr_agg.OutputCol, cols.len);
        for (cols, part) |c, *p| p.* = sliceOutputCol(c, range[0], range[1]);
        try appendAggregateRows(
            &file,
            part,
            @intCast(range[1] - range[0]),
            300,
            .SNAPPY,
            .{},
            .{ .workers = 4, .budget = 1 << 30 },
        );
    }
    _ = try file.finish(cols);
    try std.testing.expectEqualSlices(u8, whole.bytes.items, windows.bytes.items);
}

test "writeAggregateRows: bounded windows write the whole answer's bytes and free every failed allocation" {
    try writeAnswerWindows(std.testing.allocator);
    try std.testing.checkAllAllocationFailures(std.testing.allocator, writeAnswerWindows, .{});
}

fn cachedIntegerWindows(a: std.mem.Allocator) !void {
    var budget: expr_agg.SharedBudget = .{ .limit = 1 << 30 };
    defer std.debug.assert(budget.used.load(.monotonic) == 0);
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const alloc = arena.allocator();
    const all = try aggAnswerCols(alloc, 1201, null);
    var cols = [_]expr_agg.OutputCol{ all[0], all[1], all[0] };
    cols[2].name = "s";
    var whole: AnswerCapture = .{ .a = alloc };
    _ = try writeAggregateRowsEncoded(alloc, whole.sink(), &cols, 1201, 300, .SNAPPY, .{}, .{});
    var windows: AnswerCapture = .{ .a = alloc };
    var file = try AggregateFile.init(alloc, windows.sink());
    defer file.deinit();
    const encode: AggEncode = .{ .workers = 11, .budget = 128 << 20, .ledger = &budget };
    try file.reserveIntegerWorkspace(encode);
    try std.testing.expectEqual(@as(usize, 12 * answer_rg_rows * 12 * 3), file.cached_charge);
    try std.testing.expectEqual(file.cached_charge, budget.used.load(.monotonic));
    for ([_][2]usize{ .{ 0, 1200 }, .{ 1200, 1201 } }) |range| {
        var part: [3]expr_agg.OutputCol = undefined;
        for (cols, &part) |c, *p| p.* = sliceOutputCol(c, range[0], range[1]);
        try appendAggregateRows(&file, &part, @intCast(range[1] - range[0]), 300, .SNAPPY, .{}, encode);
    }
    _ = try file.finish(&cols);
    try std.testing.expectEqualSlices(u8, whole.bytes.items, windows.bytes.items);
    // A later wider shape returns every retained arena before drawing its ordinary per-wave workspace.
    file.releaseWorkspace();
    var fallback: AnswerCapture = .{ .a = alloc };
    var wider = try AggregateFile.init(alloc, fallback.sink());
    defer wider.deinit();
    try wider.reserveIntegerWorkspace(encode);
    wider.cached_unit_charge = 1;
    try appendAggregateRows(&wider, &cols, 1201, 300, .SNAPPY, .{}, encode);
    _ = try wider.finish(&cols);
    try std.testing.expectEqualSlices(u8, whole.bytes.items, fallback.bytes.items);
    try std.testing.expectEqual(@as(usize, 0), wider.cached_charge);
    try std.testing.expectEqual(@as(usize, 0), budget.used.load(.monotonic));
}

test "writeAggregateRows: retained integer encode scratch stays charged through windows and unwinds on failure" {
    try cachedIntegerWindows(std.testing.allocator);
    try std.testing.checkAllAllocationFailures(std.testing.allocator, cachedIntegerWindows, .{});
}

fn checkPartitionWindowScratch(a: std.mem.Allocator, n: usize, coded: bool) !void {
    const strings = try a.alloc([]const u8, n);
    defer a.free(strings);
    const text = try a.alloc([96]u8, n);
    defer a.free(text);
    for (strings, text, 0..) |*s, *buf, i| {
        s.* = try std.fmt.bufPrint(buf, "a distinct long key whose bytes must outlive grouping window {d:0>8}", .{i});
    }
    const ints = try a.alloc(i64, n);
    defer a.free(ints);
    for (ints, 0..) |*v, i| v.* = @intCast(i);
    const codes = try a.alloc(u32, n);
    defer a.free(codes);
    for (codes, 0..) |*code, i| code.* = @intCast(i);
    var cols = [_]filter_eval.Batch.Column{
        .{ .string = .{
            .values = if (coded) &.{} else strings,
            .dict = if (coded) strings else &.{},
            .codes = if (coded) codes else null,
            .max_def = 0,
        } },
        .{ .i64 = .{ .values = ints, .max_def = 0 } },
    };
    const keys = [_]expr_ast.Expr{
        .{ .col_ref = .{ .col_idx = 0, .physical_type = .BYTE_ARRAY, .expr_type = .str } },
        .{ .col_ref = .{ .col_idx = 1, .physical_type = .INT64, .expr_type = .i64 } },
    };
    const calls = [_]expr_agg.AggCall{.{ .func = .count, .arg = null, .where = null, .alias = "c", .result = .i64 }};
    const Merge = struct {
        fn count(dst: *expr_agg.Accumulator, src: *expr_agg.Accumulator, _: std.mem.Allocator) !void {
            dst.count += src.count;
        }
    };
    var budget: expr_agg.SharedBudget = .{ .limit = 64 << 20 };
    defer std.debug.assert(budget.used.load(.monotonic) == 0);
    var part = try partagg.PartAgg.init(a, &budget, &calls, 0, Merge.count, 4, 4, 1, true);
    defer part.deinit();
    part.compact = true;
    try std.testing.expect(try part.admitPrivate(n, 100));
    var table = expr_agg.GroupTable.init(a, 0);
    table.shared = &budget;
    table.compact.configure(&calls);
    defer table.deinit();
    var local: partagg.Local = .{ .runs = &part.runs[0] };
    var sel = try filter_selection.SelectionVector.init(a, n);
    defer sel.deinit();
    var scratch_budget: expr_agg.SharedBudget = .{ .limit = 512 << 10 };
    var charged: @import("memory.zig").Charged = .{ .child = a, .budget = &scratch_budget };
    var arena = RowGroupArena.init(charged.allocator());
    defer arena.deinit();
    const target: AggTarget = .{ .grouped = .{
        .calls = &calls,
        .keys = &keys,
        .table = &table,
        .pos_base = 0,
        .part = &part,
        .local = &local,
    } };
    try foldGroupedWindows(
        arena.allocator(),
        a,
        target,
        &cols,
        &.{ 0, 1 },
        &sel,
        &.{ coded, false },
        coded,
        null,
        null,
        null,
    );
    try part.flush(&local, &table);
    try part.foldRuns(1);
    var groups: usize = 0;
    for (part.parts) |*p| {
        groups += p.table.groupCount();
        for (0..p.table.groupCount()) |g| {
            var tmp: [16]expr_agg.Accumulator = undefined;
            try std.testing.expectEqual(@as(u64, 1), p.table.readGroup(g, 1, &tmp)[0].count);
            const k = p.table.keyAt(g);
            const id = std.mem.readInt(i64, k[k.len - 8 ..][0..8], .little);
            try std.testing.expectEqual(@as(u64, @intCast(id)), p.table.first.items[g]);
            const len = std.mem.readInt(u32, k[1..5], .little);
            try std.testing.expectEqualStrings(strings[@intCast(id)], k[5..][0..len]);
        }
    }
    try std.testing.expectEqual(@as(usize, n), groups);
}

test "partition windows reuse charged scratch instead of retaining a row group's windows" {
    try checkPartitionWindowScratch(std.testing.allocator, 32_768, false);
}

test "partition window scratch keeps materialized dictionaries live across resets" {
    try checkPartitionWindowScratch(std.testing.allocator, 8192, true);
}

test "partition window scratch returns every charge on allocation failure" {
    try std.testing.checkAllAllocationFailures(std.testing.allocator, checkPartitionWindowScratch, .{ 32, false });
}

test "cloneColumnMeta keeps every statistics field, the bounds' exactness among them" {
    var arena_state = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena_state.deinit();
    const a = arena_state.allocator();
    const st: schema.Statistics = .{
        .max = "b",
        .min = "a",
        .null_count = 3,
        .distinct_count = 7,
        .max_value = "y",
        .min_value = "x",
        .is_max_value_exact = false,
        .is_min_value_exact = true,
        .nan_count = 2,
    };
    const cm: schema.ColumnMetaData = .{
        .type = .BYTE_ARRAY,
        .encodings = .empty,
        .path_in_schema = .empty,
        .codec = .UNCOMPRESSED,
        .num_values = 10,
        .total_uncompressed_size = 0,
        .total_compressed_size = 0,
        .data_page_offset = 0,
        .index_page_offset = null,
        .dictionary_page_offset = null,
        .statistics = st,
    };
    const got = (try cloneColumnMeta(a, cm)).statistics.?;
    inline for (@typeInfo(schema.Statistics).@"struct".field_names) |name| {
        const want = @field(st, name);
        const have = @field(got, name);
        if (@TypeOf(want) == ?[]const u8) {
            try std.testing.expectEqualStrings(want.?, have.?);
        } else try std.testing.expectEqual(want, have);
    }
}

test "plainBitsPrefix: every range's bits are plainBits' over the same rows, whatever the lanes and NULLs" {
    const testing = std.testing;
    var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_state.deinit();
    const a = arena_state.allocator();
    const n = 257;
    var agg: OutputAggregator = .{
        .arena = a,
        .cols = try a.alloc(OutputAggregator.ColumnBuf, 4),
        .schema_elems = try a.alloc(schema.SchemaElement, 4),
        .paths = &.{},
        .num_rows = n,
        .captured = true,
    };
    const base: schema.SchemaElement = .{
        .type = .INT64,
        .type_length = null,
        .repetition_type = .OPTIONAL,
        .name = "c",
        .num_children = null,
        .scale = null,
        .precision = null,
        .field_id = null,
    };
    for (agg.schema_elems) |*e| e.* = base;
    agg.schema_elems[1].type = .BYTE_ARRAY;
    agg.schema_elems[2].type = .BYTE_ARRAY;
    agg.schema_elems[3].type = .BOOLEAN;
    agg.cols[0] = .{ .i64 = .{} };
    agg.cols[1] = .{ .string = .{} };
    agg.cols[2] = .{ .string = .{ .coded = true, .dict = &.{ "x", "yyyy", "" }, .max_def = 1 } };
    agg.cols[3] = .{ .boolean = .{ .max_def = 1 } };
    for (0..n) |r| {
        try agg.cols[0].i64.values.append(a, @intCast(r));
        try agg.cols[1].string.values.append(a, "abcdefghij"[0 .. r % 11]);
        try agg.cols[2].string.codes.append(a, @intCast(r % 3));
        try agg.cols[2].string.def_levels.append(a, @intFromBool(r % 4 != 0));
        try agg.cols[3].boolean.values.append(a, r % 2 == 0);
        try agg.cols[3].boolean.def_levels.append(a, @intFromBool(r % 5 != 0));
    }
    const prefix = try plainBitsPrefix(a, &agg);
    try testing.expectEqual(@as(usize, n + 1), prefix.len);
    for ([_][2]usize{ .{ 0, n }, .{ 0, 0 }, .{ 3, 4 }, .{ 17, 200 }, .{ 100, n } }) |lohi| {
        var want: u64 = 0;
        for (agg.cols, agg.schema_elems) |*c, *e| want += plainBits(c, e, lohi[0], lohi[1]);
        try testing.expectEqual(want, prefix[lohi[1]] - prefix[lohi[0]]);
    }
}

test "canonical decode failure checks every competing column and unwinds allocation failures" {
    var storage = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer storage.deinit();
    const a = storage.allocator();
    const header: schema.PageHeader = .{
        .type = .DATA_PAGE,
        .uncompressed_page_size = 4,
        .compressed_page_size = 4,
        .crc = null,
        .data_page_header = .{
            .num_values = 300,
            .encoding = .PLAIN,
            .definition_level_encoding = .RLE,
            .repetition_level_encoding = .RLE,
        },
        .dictionary_page_header = null,
        .data_page_header_v2 = null,
    };
    var writer = thrift.Writer.init(a);
    try header.write(&writer);
    const bytes = try std.mem.concat(a, u8, &.{ writer.bytes(), &.{ 255, 255, 255, 255 } });
    var elements = [_]schema.SchemaElement{ fl_test_elem, fl_test_elem, fl_test_elem };
    elements[0].name = "schema";
    elements[0].type = null;
    elements[0].repetition_type = .REQUIRED;
    elements[0].num_children = 2;
    elements[1].name = "a";
    elements[2].name = "b";
    var paths = [_][1][]const u8{ .{"a"}, .{"b"} };
    var columns: [2]schema.ColumnChunk = undefined;
    for (&columns, &paths) |*c, *path| c.* = .{
        .file_path = null,
        .file_offset = 0,
        .meta_data = .{
            .type = .INT32,
            .encodings = .empty,
            .path_in_schema = .{ .items = path, .capacity = 1, .pointer_stability = .{} },
            .codec = .UNCOMPRESSED,
            .num_values = 300,
            .total_uncompressed_size = @intCast(bytes.len),
            .total_compressed_size = @intCast(bytes.len),
            .data_page_offset = 0,
            .index_page_offset = null,
            .dictionary_page_offset = null,
        },
    };
    var groups = [_]schema.RowGroup{.{
        .columns = .{ .items = &columns, .capacity = 2, .pointer_stability = .{} },
        .total_byte_size = @intCast(bytes.len * 2),
        .num_rows = 300,
    }};
    const meta: schema.FileMetaData = .{
        .version = 1,
        .schema = .{ .items = &elements, .capacity = 3, .pointer_stability = .{} },
        .num_rows = 300,
        .row_groups = .{ .items = &groups, .capacity = 1, .pointer_stability = .{} },
        .created_by = null,
    };
    const in: AggRowGroup = .{
        .rg = &groups[0],
        .meta = &meta,
        .src = .{ .bytes = bytes, .byte_origin = 0 },
        .filter = null,
        .read = &.{ true, true },
        .consumed = &.{ true, true },
        .error_ranks = .{ .ctx = &meta, .make = struct {
            fn make(_: *const anyopaque, alloc: std.mem.Allocator, n: usize) anyerror![]const usize {
                const ranks = try alloc.alloc(usize, n);
                for (ranks, 0..) |*rank, i| rank.* = i;
                return ranks;
            }
        }.make },
    };
    // Probe OOM is intentionally absorbed: exhaust every allocation site and assert cleanup/diagnostic preservation.
    var count: usize = 0;
    while (count < 64) : (count += 1) {
        var failing = std.testing.FailingAllocator.init(std.testing.allocator, .{ .fail_index = count });
        const failure = canonicalDecodeFailure(
            failing.allocator(),
            in,
            .{},
            &.{},
            true,
            .{ .err = error.UnexpectedPage, .site = .{ .rg = in.rg, .column = 1, .page = 0 } },
        );
        try std.testing.expectEqual(error.UnexpectedPage, failure.err);
        try std.testing.expect(failure.site.column == 0 or failure.site.column == 1);
        try std.testing.expectEqual(failing.allocated_bytes, failing.freed_bytes);
        if (!failing.has_induced_failure) {
            try std.testing.expectEqual(@as(usize, 0), failure.site.column);
            break;
        }
    }
    try std.testing.expect(count < 64);
    var ledger: core_memory.SharedBudget = .{ .limit = 1024 };
    var charged: core_memory.Charged = .{ .child = std.testing.allocator, .budget = &ledger };
    const original: decode_order.Failure = .{
        .err = error.UnexpectedPage,
        .site = .{ .rg = in.rg, .column = 1, .page = 0 },
    };
    const bounded = decode_order.canonical(SliceReader, isDecodeError, charged.allocator(), in, .{}, &.{ false, false }, original);
    try std.testing.expectEqual(original.err, bounded.err);
    try std.testing.expectEqual(@as(usize, 0), ledger.used.load(.monotonic));
    try std.testing.expect(ledger.peak.load(.monotonic) <= ledger.limit);
    // A marker whose failure cannot be reproduced never outranks a real page, even at the same row.
    columns[0].meta_data.?.num_values = 0;
    const reproduced = decode_order.canonical(SliceReader, isDecodeError, std.testing.allocator, in, .{}, &.{ false, false }, .{ .err = error.UnexpectedPage, .site = .{ .rg = in.rg, .column = 0, .page = 0 } });
    try std.testing.expectEqual(@as(usize, 1), reproduced.site.column);
}

test "cache decode admission counts inputs and DAG, follows sharing, and releases failed planning allocations" {
    const hw = @import("hw.zig");
    defer hw.test_cpu = null;
    hw.test_cpu = .{ .l2 = .{ .bytes = 1 << 20, .max_sharing = 2 } };
    var elements = [_]schema.SchemaElement{ fl_test_elem, fl_test_elem };
    elements[0].name = "schema";
    elements[0].type = null;
    elements[0].repetition_type = .REQUIRED;
    elements[0].num_children = 1;
    elements[1].type = .DOUBLE;
    var path = [_][]const u8{"v"};
    var columns = [_]schema.ColumnChunk{.{ .file_path = null, .file_offset = 0, .meta_data = .{
        .type = .DOUBLE,
        .encodings = .empty,
        .path_in_schema = .{ .items = &path, .capacity = 1, .pointer_stability = .{} },
        .codec = .UNCOMPRESSED,
        .num_values = 65553,
        .total_uncompressed_size = 65553 * 8,
        .total_compressed_size = 65553 * 8,
        .data_page_offset = 0,
        .index_page_offset = null,
        .dictionary_page_offset = null,
    } }};
    var rg: schema.RowGroup = .{
        .columns = .{ .items = &columns, .capacity = 1, .pointer_stability = .{} },
        .total_byte_size = 65553 * 8,
        .num_rows = 65553,
    };
    const meta: schema.FileMetaData = .{
        .version = 1,
        .schema = .{ .items = &elements, .capacity = 2, .pointer_stability = .{} },
        .num_rows = 65553,
        .created_by = null,
        .row_groups = .empty,
    };
    const in: AggRowGroup = .{
        .rg = &rg,
        .meta = &meta,
        .src = .{ .bytes = &.{}, .byte_origin = 0 },
        .filter = null,
        .read = &.{true},
        .consumed = &.{true},
    };
    var x: expr_ast.Expr = .{ .col_ref = .{ .col_idx = 0, .physical_type = .DOUBLE, .expr_type = .f64 } };
    var one: expr_ast.Expr = .{ .literal = .{ .i64 = 1 } };
    const sum: expr_ast.Expr = .{ .binop = .{
        .op = .add,
        .left = &x,
        .right = &one,
        .result_type = .f64,
        .depth = 2,
    } };
    var call = [_]expr_agg.AggCall{.{ .func = .sum, .arg = sum, .result = .f64, .where = null, .alias = "s" }};
    const target: AggTarget = .{ .scalar = .{ .calls = &call, .accumulators = &.{} } };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, struct {
        fn run(a: std.mem.Allocator, input: AggRowGroup, t: AggTarget) !void {
            const n = try automaticCacheRows(a, input, t, 16);
            try std.testing.expect(n >= 64 and n < 65553 and n % 64 == 0);
        }
    }.run, .{ in, target });
    const small = try automaticCacheRows(std.testing.allocator, in, target, 16);
    hw.test_cpu.?.l2.bytes = 1280 << 10;
    const larger = try automaticCacheRows(std.testing.allocator, in, target, 16);
    try std.testing.expect(larger > small);
    hw.test_cpu.?.l3.bytes = 16 << 20;
    try std.testing.expectEqual(@as(usize, 0), try automaticCacheRows(std.testing.allocator, in, target, 1));
    try std.testing.expect(try automaticCacheRows(std.testing.allocator, in, target, 16) > 0);
    hw.test_cpu.?.l3.bytes = 0;
    hw.test_cpu.?.l2.bytes = 0;
    try std.testing.expectEqual(@as(usize, 0), try automaticCacheRows(std.testing.allocator, in, target, 16));
    hw.test_cpu.?.l2.bytes = 1 << 20;
    call[0].arg = x;
    try std.testing.expectEqual(@as(usize, 0), try automaticCacheRows(std.testing.allocator, in, target, 16));
}

test "cache decode admission bounds dictionary strings and composite domains from every page header" {
    const testing = std.testing;
    const hw = @import("hw.zig");
    defer hw.test_cpu = null;
    hw.test_cpu = .{ .l2 = .{ .bytes = 1 << 20, .max_sharing = 2 } };
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const Fixture = struct {
        fn bytes(alloc: std.mem.Allocator, entries: i32, width: i32, last: schema.Encoding) ![]const u8 {
            var w = thrift.Writer.init(alloc);
            var h: schema.PageHeader = .{
                .type = .DICTIONARY_PAGE,
                .uncompressed_page_size = width,
                .compressed_page_size = 1,
                .crc = null,
                .data_page_header = null,
                .data_page_header_v2 = null,
                .dictionary_page_header = .{ .num_values = entries, .encoding = .PLAIN, .is_sorted = null },
            };
            try h.write(&w);
            try w.buffer.append(alloc, 0);
            h.type = .DATA_PAGE;
            h.uncompressed_page_size = 1;
            h.dictionary_page_header = null;
            h.data_page_header = .{
                .num_values = 32768,
                .encoding = .RLE_DICTIONARY,
                .definition_level_encoding = .RLE,
                .repetition_level_encoding = .RLE,
            };
            try h.write(&w);
            try w.buffer.append(alloc, 0);
            h.type = .DATA_PAGE_V2;
            h.data_page_header = null;
            h.data_page_header_v2 = .{
                .num_values = 32785,
                .num_nulls = 0,
                .num_rows = 32785,
                .encoding = last,
                .definition_levels_byte_length = 0,
                .repetition_levels_byte_length = 0,
                .is_compressed = true,
            };
            try h.write(&w);
            try w.buffer.append(alloc, 0);
            return w.bytes(); // Bodies deliberately aren't decoded by this admission-only hint.
        }
    };
    var elements = [_]schema.SchemaElement{ fl_test_elem, fl_test_elem, fl_test_elem };
    elements[0].name = "schema";
    elements[0].type = null;
    elements[0].repetition_type = .REQUIRED;
    elements[0].num_children = 2;
    elements[1].type = .DOUBLE;
    elements[1].name = "x";
    elements[2].type = .BYTE_ARRAY;
    elements[2].name = "g";
    var paths = [_][1][]const u8{ .{"x"}, .{"g"} };
    var columns: [2]schema.ColumnChunk = undefined;
    for (&columns, &paths, 0..) |*c, *path, i| c.* = .{
        .file_path = null,
        .file_offset = 1000,
        .meta_data = .{
            .type = if (i == 0) .DOUBLE else .BYTE_ARRAY,
            .encodings = .empty,
            .path_in_schema = .{ .items = path, .capacity = 1, .pointer_stability = .{} },
            .codec = .ZSTD,
            .num_values = 65553,
            .total_uncompressed_size = 65553 * 8,
            .total_compressed_size = 0,
            .data_page_offset = 1000,
            .index_page_offset = null,
            .dictionary_page_offset = if (i == 1) 1000 else null,
        },
    };
    var rg: schema.RowGroup = .{
        .columns = .{ .items = &columns, .capacity = 2, .pointer_stability = .{} },
        .total_byte_size = 65553 * 8,
        .num_rows = 65553,
    };
    const meta: schema.FileMetaData = .{
        .version = 1,
        .schema = .{ .items = &elements, .capacity = 3, .pointer_stability = .{} },
        .num_rows = rg.num_rows,
        .created_by = null,
        .row_groups = .empty,
    };
    var in: AggRowGroup = .{
        .rg = &rg,
        .meta = &meta,
        .src = .{ .bytes = &.{}, .byte_origin = 1000 },
        .filter = null,
        .read = &.{ true, true },
        .consumed = &.{ true, true },
    };
    var x: expr_ast.Expr = .{ .col_ref = .{ .col_idx = 0, .physical_type = .DOUBLE, .expr_type = .f64 } };
    var one: expr_ast.Expr = .{ .literal = .{ .i64 = 1 } };
    const computed: expr_ast.Expr = .{ .binop = .{
        .op = .add,
        .left = &x,
        .right = &one,
        .result_type = .f64,
        .depth = 2,
    } };
    const calls = [_]expr_agg.AggCall{.{ .func = .sum, .arg = computed, .result = .f64, .where = null, .alias = "s" }};
    const key: expr_ast.Expr = .{ .col_ref = .{ .col_idx = 1, .physical_type = .BYTE_ARRAY, .expr_type = .str } };
    var target: AggTarget = .{ .grouped = .{ .calls = &calls, .keys = &.{key}, .table = undefined, .pos_base = 0 } };
    var small: usize = 0;
    for ([_]i32{ 24, 4000, 2 << 20 }) |width| {
        const bytes = try Fixture.bytes(a, 2, width, .RLE_DICTIONARY);
        in.src.bytes = bytes;
        columns[1].meta_data.?.total_compressed_size = @intCast(bytes.len);
        const proof = cacheDictionary(in, 1).?;
        var funding: core_memory.SharedBudget = .{ .limit = 64 << 10 };
        var charge: core_memory.Charged = .{ .child = testing.allocator, .budget = &funding };
        var memo: CacheHints = .{ .allocator = charge.allocator() };
        try memo.initShards(3);
        try testing.expectEqualDeep(proof, memo.get(in, 1).?);
        try testing.expectEqualDeep(proof, memo.get(in, 1).?);
        try testing.expectEqual(@as(usize, 1), memo.count());
        memo.deinit();
        try testing.expectEqual(@as(usize, 0), funding.used.load(.monotonic));
        var failing = testing.FailingAllocator.init(testing.allocator, .{ .fail_index = 0 });
        var uncached: CacheHints = .{ .allocator = failing.allocator() };
        try testing.expectError(error.OutOfMemory, uncached.initShards(3));
        try testing.expectEqual(@as(usize, 0), uncached.shards.len);
        try testing.expectEqualDeep(proof, uncached.get(in, 1).?);
        try testing.expectEqual(@as(usize, 0), uncached.count());
        uncached.deinit();

        try testing.expectEqual(@as(usize, @intCast(width)), proof.bytes);
        const rows = try automaticCacheRows(testing.allocator, in, target, 16);
        if (width == 24) {
            small = rows;
            try testing.expect(small >= 64 and small < 65553);
        } else try testing.expect(rows < small);
        if (width == 2 << 20) try testing.expectEqual(@as(usize, 0), rows);
    }
    // Every data page must qualify, including V2 after an eligible V1 page.
    in.src.bytes = try Fixture.bytes(a, 2, 24, .PLAIN);
    columns[1].meta_data.?.total_compressed_size = @intCast(in.src.bytes.len);
    try testing.expect(cacheDictionary(in, 1) == null);
    try testing.expectEqual(@as(usize, 0), try automaticCacheRows(testing.allocator, in, target, 16));
    in.src.bytes = try Fixture.bytes(a, 256, 4096, .RLE_DICTIONARY);
    columns[1].meta_data.?.total_compressed_size = @intCast(in.src.bytes.len);
    // Two individually bounded keys can still have a composite table much larger than L2.
    target.grouped.keys = &.{ key, key };
    try testing.expectEqual(@as(usize, 0), try automaticCacheRows(testing.allocator, in, target, 16));
    target.grouped.keys = &.{key};
    in.src.bytes = try Fixture.bytes(a, 257, 24, .RLE_DICTIONARY);
    columns[1].meta_data.?.total_compressed_size = @intCast(in.src.bytes.len);
    try testing.expectEqual(@as(usize, 0), try automaticCacheRows(testing.allocator, in, target, 16));
    columns[1].meta_data.?.dictionary_page_offset = -1;
    try testing.expect(cacheDictionary(in, 1) == null);
}

test "scalar cache DAG shares nullable inputs without changing canonical sum blocks, including allocation failure" {
    try std.testing.checkAllAllocationFailures(std.testing.allocator, testScalarCache, .{});
}

fn testScalarCache(backing: std.mem.Allocator) !void {
    const testing = std.testing;
    var arena = std.heap.ArenaAllocator.init(backing);
    defer arena.deinit();
    const a = arena.allocator();
    const n = 193;
    const values = try a.alloc(f64, n);
    const defs = try a.alloc(u32, n);
    for (values, defs, 0..) |*v, *d, i| {
        v.* = switch (i % 6) {
            0 => 1e16,
            1 => 1,
            2 => -1e16,
            3 => 0.1,
            4 => -0.0,
            else => std.math.nan(f64),
        };
        d.* = @intFromBool(i % 17 != 0);
    }
    var x: expr_ast.Expr = .{ .col_ref = .{ .col_idx = 0, .physical_type = .DOUBLE, .expr_type = .f64 } };
    var one: expr_ast.Expr = .{ .literal = .{ .i64 = 1 } };
    var base: expr_ast.Expr = .{ .binop = .{
        .op = .add,
        .left = &x,
        .right = &one,
        .result_type = .f64,
        .depth = 2,
    } };
    var repeated = base;
    const square: expr_ast.Expr = .{ .binop = .{
        .op = .mul,
        .left = &base,
        .right = &repeated,
        .result_type = .f64,
        .depth = 3,
    } };
    const calls = [_]expr_agg.AggCall{
        .{ .func = .sum, .arg = base, .result = .f64, .where = null, .alias = "s" },
        .{ .func = .sum, .arg = square, .result = .f64, .where = null, .alias = "ss" },
        .{ .func = .avg, .arg = base, .result = .avg_f64, .where = null, .alias = "a" },
        .{ .func = .count, .arg = base, .result = .i64, .where = null, .alias = "n" },
        .{ .func = .min, .arg = base, .result = .f64, .where = null, .alias = "lo" },
    };
    const cols = [_]filter_eval.Batch.Column{.{ .f64 = .{
        .values = values,
        .def_levels = defs,
        .max_def = 1,
        .has_nulls = true,
    } }};
    const batch: filter_eval.Batch = .{ .cols = &cols, .num_rows = n };
    for ([_]bool{ false, true }) |empty| {
        var sel = try filter_selection.SelectionVector.init(a, n);
        for (0..n) |i| sel.set(i, !empty and i % 3 != 1);
        var want: [calls.len]expr_agg.Accumulator = undefined;
        var got: [calls.len]expr_agg.Accumulator = undefined;
        for (calls, &want, &got) |call, *w, *g| {
            w.* = .init(call);
            g.* = .init(call);
            try expr_agg.updateOne(a, a, w, call, &batch, &.{0}, &sel);
        }
        var off: usize = 0;
        while (off < n) : (off += 64) {
            const count = @min(64, n - off);
            const col = try cols[0].sliceRows(off, count);
            const part: filter_eval.Batch = .{ .cols = &.{col}, .num_rows = count };
            const mask = try sel.alignedWindow(off, count);
            try testing.expect(try foldScalarCache(
                a,
                a,
                &calls,
                &got,
                &@as([calls.len]bool, @splat(false)),
                &part,
                &.{0},
                &mask,
            ));
        }
        for (want, got) |w, g| switch (w) {
            .sum_f, .min_f => |expected| {
                const actual = if (g == .sum_f) g.sum_f else g.min_f;
                try testing.expectEqual(expected != null, actual != null);
                if (expected) |v| try testing.expectEqual(@as(u64, @bitCast(v)), @as(u64, @bitCast(actual.?)));
            },
            .avg => |avg| {
                try testing.expectEqual(avg.count, g.avg.count);
                try testing.expectEqual(@as(u64, @bitCast(avg.sum)), @as(u64, @bitCast(g.avg.sum)));
            },
            .count => |count| try testing.expectEqual(count, g.count),
            else => unreachable,
        };
    }
}

test "writeAggregateRows preserves the automatic Bloom policy for small and parallel answers" {
    var arena_state = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena_state.deinit();
    const a = arena_state.allocator();
    for ([_]usize{ 1000, 70000 }) |n| {
        const cols = try aggAnswerCols(a, n, null);
        for ([_]bool{ false, true }) |enabled| for ([_]usize{ 1, 3 }) |workers| {
            var capture: AnswerCapture = .{ .a = a };
            _ = try writeAggregateRows(a, capture.sink(), cols, @intCast(n), .SNAPPY, .{}, .{
                .dictionary_bloom = enabled,
                .workers = workers,
                .budget = std.math.maxInt(u64),
            });
            const meta = try answerFooter(a, capture.bytes.items);
            const repeated = meta.row_groups.items[0].columns.items[1].meta_data.?;
            try std.testing.expect(repeated.dictionary_page_offset != null);
            try std.testing.expectEqual(enabled, repeated.bloom_filter_offset != null);
        };
    }
}

test "copied INT96 dictionary filters hash raw entries and decline noncanonical times" {
    var arena_state = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena_state.deinit();
    const a = arena_state.allocator();
    const bloom = @import("parquet/bloom.zig");
    var elem: schema.SchemaElement = .{
        .name = "t",
        .type = .FIXED_LEN_BYTE_ARRAY,
        .type_length = int96_mod.INT96_WIDTH,
        .repetition_type = .REQUIRED,
        .num_children = 0,
        .scale = null,
        .precision = null,
        .field_id = null,
    };
    var raw: [2][12]u8 = @splat(@splat(0));
    for (&raw) |*v| std.mem.writeInt(u32, v[8..12], 2440588, .little);
    var values: [256][]const u8 = undefined;
    for (&values, 0..) |*v, i| v.* = &raw[i % 2];
    for ([_]bool{ false, true }) |noncanonical| {
        std.mem.writeInt(i64, raw[1][0..8], if (noncanonical) std.time.ns_per_day else 2_000_000_000, .little);
        const encoded = try encoder.encodeColumn(a, .{
            .values = .{ .string = .{ .values = &values } },
            .schema_elem = &elem,
            .path_in_schema = &.{"t"},
            .dictionary_bloom = false,
        });
        var cm = encoded.meta;
        cm.type = .INT96;
        var root = elem;
        root.name = "root";
        root.type = null;
        root.type_length = null;
        root.num_children = 1;
        var meta: schema.FileMetaData = .{
            .version = 2,
            .schema = .empty,
            .num_rows = values.len,
            .created_by = schema.ZPQ_CREATED_BY,
            .row_groups = .empty,
        };
        try meta.schema.append(a, root);
        var leaf = elem;
        leaf.type = .INT96;
        leaf.type_length = null;
        try meta.schema.append(a, leaf);
        const filter = try dictionaryBloom(a, encoded.bytes, cm, .{ .max_def = 0, .max_rep = 0 }, &meta);
        if (noncanonical) {
            try std.testing.expect(filter == null);
        } else {
            const bits = bloom.bitset(filter.?).?;
            for (raw) |v| try std.testing.expect(bloom.Probe.init(bloom.hashBytes(&v)).mightContain(bits));
        }
    }
}

test "grouped key dictionaries keep later-key codes with and without dense lanes" {
    const testing = std.testing;
    var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_state.deinit();
    const a = arena_state.allocator();
    var ints: [256]i32 = undefined;
    var names: [256][]const u8 = undefined;
    for (&ints, &names, 0..) |*k, *v, i| {
        k.* = @intCast(i % 2);
        v.* = try std.fmt.allocPrint(a, "dictionary-value-{d}", .{i % 64});
    }
    const cols = [_]expr_agg.OutputCol{
        .{ .name = "a", .parquet_type = .INT32, .nullable = false, .col = .{ .i32 = .{ .values = &ints } } },
        .{ .name = "b", .parquet_type = .BYTE_ARRAY, .nullable = false, .col = .{ .string = .{ .values = &names } } },
    };
    var capture: AnswerCapture = .{ .a = a };
    _ = try writeAggregateRows(a, capture.sink(), &cols, ints.len, .SNAPPY, .{}, .{});
    const meta = try metadata.open(a, capture.bytes.items);
    const rg = &meta.row_groups.items[0];
    const src: RGSrc = .{ .bytes = capture.bytes.items, .byte_origin = 0 };
    var lanes: agg_dense.Lanes = undefined;
    var target: AggTarget = .{ .grouped = .{
        .calls = &.{},
        .keys = &.{
            .{ .col_ref = .{ .col_idx = 0, .physical_type = .INT32, .expr_type = .i64 } },
            .{ .col_ref = .{ .col_idx = 1, .physical_type = .BYTE_ARRAY, .expr_type = .str } },
        },
        .dense = &lanes,
        .table = undefined,
        .pos_base = 0,
    } };
    try testing.expect(target.codedKey(0));
    try testing.expect(target.codedKey(1));
    try testing.expect(!target.codedKey(2));
    target.grouped.dense = null;
    try testing.expect(target.codedKey(0));
    try testing.expect(target.codedKey(1));
    const in: AggRowGroup = .{
        .rg = rg,
        .meta = &meta,
        .src = src,
        .filter = null,
        .read = &.{ true, true },
        .consumed = &.{ true, true },
    };
    try testing.expect(cacheDictionary(in, 1) != null);
    try testing.expect(!streamByteArray(in, target, 1));
    try testing.expectEqual(std.math.maxInt(usize), streamWindowRows(in, target, ints.len));
    const cm = rg.columns.items[1].meta_data.?;
    const start: usize = @intCast(cm.dictionary_page_offset.?);
    const chunk = capture.bytes.items[start..][0..@intCast(cm.total_compressed_size)];
    for ([_]bool{ false, true }) |force| {
        const options: DecodeOptions = .{ .encoded = true, .any_dictionary = force and target.codedKey(1) };
        var scratch = DecodeScratch.init(testing.allocator);
        defer scratch.deinit();
        const col = (try decodeAggColumnCoded(a, chunk, cm, &meta, options, &scratch)).?;
        try testing.expectEqual(force, col.codes() != null);
        if (!force) continue;
        for ([_]usize{ 1, 17, 64, 256 }) |step| {
            var reader = (try SliceReader.init(a, testing.allocator, rg, &meta, src, 1, options)).?;
            defer reader.deinit();
            var slice = std.heap.ArenaAllocator.init(testing.allocator);
            defer slice.deinit();
            var done: usize = 0;
            while (done < names.len) {
                _ = slice.reset(.free_all);
                const got = try reader.next(slice.allocator(), step);
                const codes = got.codes().?;
                try testing.expect(got.string.dict.len > names.len / 8);
                for (codes, 0..) |code, i|
                    try testing.expectEqualStrings(names[done + i], got.string.dict[code]);
                done += codes.len;
            }
        }
    }
    // A slice spanning the dictionary/PLAIN boundary must gather its prefix without rereading pages.
    var mixed: std.ArrayList(u8) = .empty;
    var coded_body: std.ArrayList(u8) = .empty;
    try coded_body.append(a, 1);
    try coded_body.appendSlice(a, try @import("parquet/encoding/hybrid_rle.zig").encode(a, &.{ 0, 1, 0, 1 }, 1));
    const dict_values = [_]i32{ 10, 20 };
    const plain_values = [_]i32{ 30, 40, 50, 60 };
    const bodies = [_][]const u8{ std.mem.asBytes(&dict_values), coded_body.items, std.mem.asBytes(&plain_values) };
    for (bodies, 0..) |body, i| {
        var w = thrift.Writer.init(a);
        const h: schema.PageHeader = .{
            .type = if (i == 0) .DICTIONARY_PAGE else .DATA_PAGE,
            .uncompressed_page_size = @intCast(body.len),
            .compressed_page_size = @intCast(body.len),
            .crc = null,
            .data_page_header_v2 = null,
            .data_page_header = if (i == 0) null else .{
                .num_values = 4,
                .encoding = if (i == 1) .RLE_DICTIONARY else .PLAIN,
                .definition_level_encoding = .RLE,
                .repetition_level_encoding = .RLE,
            },
            .dictionary_page_header = if (i == 0) .{ .num_values = 2, .encoding = .PLAIN, .is_sorted = null } else null,
        };
        try h.write(&w);
        try mixed.appendSlice(a, w.bytes());
        try mixed.appendSlice(a, body);
    }
    var mixed_rg = rg.*;
    mixed_rg.columns.items = try a.dupe(schema.ColumnChunk, rg.columns.items);
    const mixed_cm = &mixed_rg.columns.items[0].meta_data.?;
    mixed_cm.codec = .UNCOMPRESSED;
    mixed_cm.dictionary_page_offset = 0;
    mixed_cm.data_page_offset = 0;
    mixed_cm.total_compressed_size = @intCast(mixed.items.len);
    mixed_cm.num_values = 8;
    const expected = [_]i32{ 10, 20, 10, 20, 30, 40, 50, 60 };
    for ([_]usize{ 1, 3, 5, 8 }) |step| {
        var r = (try SliceReader.init(
            a,
            testing.allocator,
            &mixed_rg,
            &meta,
            .{ .bytes = mixed.items, .byte_origin = 0 },
            0,
            .{ .encoded = true, .any_dictionary = true },
        )).?;
        defer r.deinit();
        var done: usize = 0;
        while (done < expected.len) {
            const c = (try r.next(a, step)).i32;
            const n = if (c.codes) |codes| codes.len else c.values.len;
            for (0..n) |i| try testing.expectEqual(
                expected[done + i],
                if (c.codes) |codes| c.dict[codes[i]] else c.values[i],
            );
            done += n;
        }
    }
    const Trial = struct {
        fn run(gpa: std.mem.Allocator, bytes: []const u8, m: *const schema.FileMetaData) !void {
            var arena = std.heap.ArenaAllocator.init(gpa);
            defer arena.deinit();
            const alloc = arena.allocator();
            var reader = (try SliceReader.init(
                alloc,
                gpa,
                &m.row_groups.items[0],
                m,
                .{ .bytes = bytes, .byte_origin = 0 },
                1,
                .{ .encoded = true, .any_dictionary = true },
            )).?;
            defer reader.deinit();
            var slice = std.heap.ArenaAllocator.init(gpa);
            defer slice.deinit();
            while (reader.left != 0) {
                _ = slice.reset(.free_all);
                const c = try reader.next(slice.allocator(), 17);
                try std.testing.expect(c.codes() != null);
            }
        }
    };
    try testing.checkAllAllocationFailures(testing.allocator, Trial.run, .{ capture.bytes.items, &meta });
}

test "filter-only and hash-only strings stream a cache-sized window, other strings do not" {
    const hw = @import("hw.zig");
    defer hw.test_cpu = null;
    hw.test_cpu = .{ .l2 = .{ .bytes = 1 << 20, .max_sharing = 1 } };
    var enc_plain = [_]schema.Encoding{ .PLAIN, .RLE, .RLE_DICTIONARY };
    var enc_delta = [_]schema.Encoding{ .PLAIN, .DELTA_BYTE_ARRAY };
    var path = [_][]const u8{"k"};
    var columns = [1]schema.ColumnChunk{.{
        .file_path = null,
        .file_offset = 0,
        .meta_data = .{
            .type = .BYTE_ARRAY,
            .encodings = .{ .items = &enc_plain, .capacity = enc_plain.len, .pointer_stability = .{} },
            .path_in_schema = .{ .items = &path, .capacity = 1, .pointer_stability = .{} },
            .codec = .SNAPPY,
            .num_values = 100_000,
            .total_uncompressed_size = 130 * 100_000,
            .total_compressed_size = 40 * 1000,
            .data_page_offset = 0,
            .index_page_offset = null,
            .dictionary_page_offset = null,
        },
    }};
    const group = schema.RowGroup{
        .columns = .{ .items = &columns, .capacity = 1, .pointer_stability = .{} },
        .total_byte_size = 40 * 1000,
        .num_rows = 1000,
    };
    var elements = [_]schema.SchemaElement{ fl_test_elem, fl_test_elem };
    elements[0].name = "schema";
    elements[0].type = null;
    elements[0].num_children = 1;
    elements[1].name = "k";
    elements[1].type = .BYTE_ARRAY;
    elements[1].repetition_type = .REQUIRED;
    var groups = [_]schema.RowGroup{group};
    const meta = schema.FileMetaData{
        .version = 1,
        .schema = .{ .items = &elements, .capacity = 2, .pointer_stability = .{} },
        .num_rows = 1000,
        .row_groups = .{ .items = &groups, .capacity = 1, .pointer_stability = .{} },
        .created_by = null,
    };
    const filter: filter_ast.Filter = .{ .string = .{ .col_idx = 0, .op = .Eq, .value = "x" } };
    const in = AggRowGroup{
        .rg = &groups[0],
        .meta = &meta,
        .src = .{ .bytes = &.{}, .byte_origin = 0 },
        .filter = filter,
        .read = &.{true},
        .consumed = &.{false},
    };
    const key: expr_ast.Expr = .{ .col_ref = .{ .col_idx = 0, .physical_type = .BYTE_ARRAY, .expr_type = .str } };
    const scalar_filter: AggTarget = .{ .scalar = .{ .calls = &.{}, .accumulators = &.{} } };
    try std.testing.expect(streamByteArray(in, scalar_filter, 0));
    const rows = streamWindowRows(in, scalar_filter, 100_000);
    // 130 bytes of string per row into 1 MiB of L2. The window is that many rows, not the chunk.
    try std.testing.expectEqual(@as(usize, (1 << 20) / 130), rows);
    // Regression (9bcb030 on reads-sf1 B6_string_eq, 2x slower): a filter-only string whose dictionary covers the
    // chunk keeps the whole-chunk coded decode, its predicate evaluated per dictionary entry.
    var enc_dict = [_]schema.Encoding{ .PLAIN_DICTIONARY, .RLE };
    columns[0].meta_data.?.encodings = .{ .items = &enc_dict, .capacity = enc_dict.len, .pointer_stability = .{} };
    try std.testing.expect(!streamByteArray(in, scalar_filter, 0));
    try std.testing.expectEqual(std.math.maxInt(usize), streamWindowRows(in, scalar_filter, 100_000));
    columns[0].meta_data.?.encodings = .{ .items = &enc_plain, .capacity = enc_plain.len, .pointer_stability = .{} };

    const distinct: expr_agg.AggCall = .{
        .func = .count_distinct,
        .arg = key,
        .where = null,
        .alias = "d",
        .result = .i64,
    };
    var no_filter = in;
    no_filter.filter = null;
    no_filter.consumed = &.{true};
    const scalar_distinct: AggTarget = .{ .scalar = .{ .calls = &.{distinct}, .accumulators = &.{} } };
    try std.testing.expect(streamByteArray(no_filter, scalar_distinct, 0));

    var table: expr_agg.GroupTable = undefined;
    const grouped: AggTarget = .{ .grouped = .{
        .calls = &.{},
        .keys = &.{key},
        .table = &table,
        .pos_base = 0,
    } };
    try std.testing.expect(streamByteArray(no_filter, grouped, 0));
    var other = key;
    other.col_ref.col_idx = 1;
    for ([_]@FieldType(expr_agg.AggCall, "func"){ .count_distinct, .min, .max }) |func| {
        var with_payload = grouped;
        const call: expr_agg.AggCall = .{
            .func = func,
            .arg = other,
            .where = null,
            .alias = "payload",
            .result = if (func == .count_distinct) .i64 else .bytes,
        };
        with_payload.grouped.calls = &.{call};
        try std.testing.expect(hashOnlyByteArray(with_payload, 0));
        try std.testing.expect(!streamByteArray(no_filter, with_payload, 0));
    }

    const min_call: expr_agg.AggCall = .{
        .func = .min,
        .arg = key,
        .where = null,
        .alias = "m",
        .result = .bytes,
    };
    const scalar_min: AggTarget = .{ .scalar = .{ .calls = &.{min_call}, .accumulators = &.{} } };
    try std.testing.expect(!streamByteArray(no_filter, scalar_min, 0));

    var delta = columns[0].meta_data.?;
    delta.encodings = .{ .items = &enc_delta, .capacity = enc_delta.len, .pointer_stability = .{} };
    columns[0].meta_data = delta;
    try std.testing.expect(!streamByteArray(in, scalar_filter, 0));
    try std.testing.expectEqual(std.math.maxInt(usize), streamWindowRows(in, scalar_filter, 100_000));
}
