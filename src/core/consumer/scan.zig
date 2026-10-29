const SelectedBatch = @import("../selected_batch.zig").SelectedBatch;
const RowGroupArena = @import("../row_group_arena.zig").RowGroupArena;
const DecodeScratch = @import("../parquet/column.zig").DecodeScratch;
const builtin = @import("builtin");
const huge_pages = @import("../huge_pages.zig");
const decimal_mod = @import("../parquet/decimal.zig");
const SliceScratch = @import("../parquet/page.zig").SliceScratch;
const thrift = @import("../thrift.zig");
const metadata = @import("../parquet/metadata.zig");
const std = @import("std");
const schema = @import("../schema.zig");
const filter_ast = @import("../filter/ast.zig");
const filter_eval = @import("../filter/eval.zig");
const filter_selection = @import("../filter/selection.zig");
const expr_ast = @import("../expr/ast.zig");
const expr_eval = @import("../expr/eval.zig");
const expr_agg = @import("../expr/agg.zig");
const agg_coded = @import("../expr/agg_coded.zig");
const agg_dense = @import("../expr/agg_dense.zig");
const agg_plan = @import("../agg_plan.zig");
const partagg = @import("../partagg.zig");
const core_memory = @import("../memory.zig");
const column_mod = @import("../parquet/column.zig");
const page_mod = @import("../parquet/page.zig");
const decode_order = @import("../decode_order.zig");
const profile = @import("../../profile.zig");
const nowMonoNs = @import("../../clock.zig").monoNs;
const types = @import("types.zig");
const AggRowGroup = types.AggRowGroup;
const Timings = types.Timings;
const selectedWindow = types.selectedWindow;
const enterChunk = types.enterChunk;
const isDecodeError = types.isDecodeError;
const decodeFailure = types.decodeFailure;
const setDecodeFailure = types.setDecodeFailure;
const resolveDecodeFailure = types.resolveDecodeFailure;
const target_module = @import("target.zig");
const AggTarget = target_module.AggTarget;
const RecordSink = target_module.RecordSink;
const workspace_module = @import("workspace.zig");
const AggWorkspace = workspace_module.AggWorkspace;
const CacheHints = workspace_module.CacheHints;
const admitWhole = workspace_module.admitWhole;
const dropRegion = workspace_module.dropRegion;
const refusedDraw = workspace_module.refusedDraw;
const cacheDictionary = workspace_module.cacheDictionary;
const budget_module = @import("budget.zig");
const DecodeBudget = budget_module.DecodeBudget;
const decodeEstimate = budget_module.decodeEstimate;
const DecodeOptions = column_mod.DecodeOptions;
const SliceReader = @import("slice_reader.zig").SliceReader;
const poolOn = @import("pool.zig").poolOn;
const gatherRows = @import("output.zig").gatherRows;
const pruning = @import("pruning.zig");
const loadPageIndexes = pruning.loadPageIndexes;
const pruneFilterPages = pruning.pruneFilterPages;
const compactRows = pruning.compactRows;
const scan_plan = @import("plan.zig");
const StagePlan = scan_plan.StagePlan;
const CallBatch = scan_plan.CallBatch;
const payloadColumns = scan_plan.payloadColumns;
const plainValueEncodings = scan_plan.plainValueEncodings;
const hashOnlyByteArray = scan_plan.hashOnlyByteArray;
const streamByteArray = scan_plan.streamByteArray;
const streamWindowRows = scan_plan.streamWindowRows;
const GatedFilter = scan_plan.GatedFilter;
const foldWantsCodes = scan_plan.foldWantsCodes;
const flatGroupKey = scan_plan.flatGroupKey;
const keyCodesCheaper = scan_plan.keyCodesCheaper;
const columnFeedsFold = scan_plan.columnFeedsFold;
const plainLengthColumn = scan_plan.plainLengthColumn;
const ChunkDecoder = scan_plan.ChunkDecoder;
const sliceRows = scan_plan.sliceRows;
const planCodeUses = scan_plan.planCodeUses;
const mark = scan_plan.mark;

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
pub fn automaticCacheRows(gpa: std.mem.Allocator, in: AggRowGroup, target: AggTarget, workers: usize) !usize {
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
    if (@import("../hw.zig").l2BytesPerWorker() == 0) return 0;
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
    const l3 = @import("../hw.zig").l3BytesPerActiveWorker(workers);
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
            if (@import("../filter/membership.zig").dictionaryCoversChunk(&cm) and
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
            const encoded = try @import("../parquet/encoding/hybrid_rle.zig").encode(alloc, defs, 1);
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
    try coded.appendSlice(ar, try @import("../parquet/encoding/hybrid_rle.zig").encode(ar, codes.items, 8));
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
    var charged: @import("../memory.zig").Charged = .{ .child = a, .budget = &scratch_budget };
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

