const checkRowShape = @import("column.zig").checkRowShape;
const isDecodeError = @import("types.zig").isDecodeError;
const SliceReader = @import("slice_reader.zig").SliceReader;
const std = @import("std");
const schema = @import("../schema.zig");
const filter_ast = @import("../filter/ast.zig");
const filter_eval = @import("../filter/eval.zig");
const filter_selection = @import("../filter/selection.zig");
const expr_ast = @import("../expr/ast.zig");
const expr_agg = @import("../expr/agg.zig");
const agg_coded = @import("../expr/agg_coded.zig");
const column_mod = @import("../parquet/column.zig");
const invariant = @import("../invariant.zig");
const profile = @import("../../profile.zig");
const nowMonoNs = @import("../../clock.zig").monoNs;
const types = @import("types.zig");
const AggRowGroup = types.AggRowGroup;
const enterChunk = types.enterChunk;
const AggTarget = @import("target.zig").AggTarget;
const AggWorkspace = @import("workspace.zig").AggWorkspace;
const cacheDictionary = @import("workspace.zig").cacheDictionary;
const decodeEstimate = @import("budget.zig").decodeEstimate;
const pruning = @import("pruning.zig");
const PageIndex = pruning.PageIndex;
const FilterPages = pruning.FilterPages;
const pagePruning = pruning.pagePruning;
const consumer_column = @import("column.zig");
const PruningInfo = consumer_column.PruningInfo;
const RowRange = consumer_column.RowRange;
const DecodeOptions = column_mod.DecodeOptions;
const agg_column = @import("agg_column.zig");
const decodeAggColumn = agg_column.decodeAggColumn;
const decodeAggColumnCoded = agg_column.decodeAggColumnCoded;
const decodeAggColumnHist = agg_column.decodeAggColumnHist;
const decodeLevelsOnly = agg_column.decodeLevelsOnly;

/// A staged row group's decode: the columns the outer filter reads (`base`), decoded first; then each aggregate's
/// own (`calls[i]`, less `base`), decoded for it alone and dropped after. A flat hash-group key is payload, gated
/// by the filter's survivors, on this plan as on the others.
pub const StagePlan = struct {
    base: []bool,
    calls: [][]bool,
    /// The most the plan holds at once: `base` and the largest call's columns.
    estimate: usize,

    pub fn init(a: std.mem.Allocator, in: AggRowGroup, target: AggTarget, handled: []const bool) !StagePlan {
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

pub const CallBatch = struct { batch: filter_eval.Batch, lookup: []?usize };

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
pub fn payloadColumns(a: std.mem.Allocator, in: AggRowGroup, target: AggTarget) !?[]const bool {
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

pub fn plainValueEncodings(encodings: []const schema.Encoding) bool {
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
pub fn hashOnlyByteArray(target: AggTarget, ci: usize) bool {
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
pub fn streamByteArray(in: AggRowGroup, target: AggTarget, ci: usize) bool {
    if (ci >= in.read.len or !in.read[ci] or ci >= in.rg.columns.items.len) return false;
    const cm = in.rg.columns.items[ci].meta_data orelse return false;
    if (cm.type != .BYTE_ARRAY) return false;
    if (in.meta.getColumnLevels(cm.path_in_schema.items).max_rep != 0) return false;
    if (!reusableByteEncodings(cm.encodings.items)) return false;
    const filtered = if (in.filter) |f| filterMentions(f, ci) else false;
    // A filter-only string its dictionary covers is compared once per dictionary entry from its codes, a few bytes a
    // row: streaming it reads every value instead, and saves no faulted string bytes.
    if (filtered and !columnFeedsFold(target, ci))
        return !@import("../filter/membership.zig").dictionaryCoversChunk(&cm);
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
pub fn streamWindowRows(in: AggRowGroup, target: AggTarget, num_rows: usize) usize {
    var per: usize = 0;
    for (in.read, 0..) |needed, ci| {
        if (!needed or !streamByteArray(in, target, ci)) continue;
        const cm = in.rg.columns.items[ci].meta_data orelse continue;
        const n: usize = @intCast(@max(cm.num_values, 1));
        const bytes: usize = @intCast(@max(cm.total_uncompressed_size, 1));
        per = @max(per, @max(1, bytes / n));
    }
    if (per == 0) return std.math.maxInt(usize);
    const cache = @import("../hw.zig").l2BytesPerWorkerOr(1 << 20);
    return @max(64, @min(num_rows, cache / per));
}

/// The fold reads `ci` after the filter: a group key, an aggregate argument, or a per-aggregate FILTER.
/// A top-level AND's filter columns decoded one conjunct at a time: a column only its single-column leaves read
/// decodes after the other base columns, at the survivors of every conjunct whose columns are decoded by then
/// (`DecodeOptions.row_gate`), so a page none of them keeps is never decompressed. Conjuncts evaluate in the plan's
/// order as their columns arrive; AND only clears bits, so the answer is the whole filter's. Which pages skip
/// depends on the input and the query alone. A chunk its dictionary covers keeps its coded evaluation, and a
/// length-only, page-indexed, staged, compacted or record-owned decode keeps the established whole-column path.
pub const GatedFilter = struct {
    children: []const filter_ast.Filter,
    /// Base columns in decode order: the others in column order, then the deferred by the first conjunct naming them.
    order: []usize,
    deferred: []bool,
    done: []bool,

    pub fn init(
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
            if (@import("../filter/membership.zig").dictionaryCoversChunk(&cm)) continue;
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
    pub fn evaluate(
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
pub fn foldWantsCodes(target: AggTarget, ci: usize, rg: *const schema.RowGroup) bool {
    if (target != .grouped) return false;
    if (!target.codedKey(ci)) return target.grouped.dense != null;
    if (target.grouped.dense != null or target.grouped.records != null) return true;
    for (target.grouped.keys) |k| {
        if (k != .col_ref) return false;
        const kc = k.col_ref.col_idx;
        if (kc >= rg.columns.items.len) return false;
        const km = rg.columns.items[kc].meta_data orelse return false;
        if (!@import("../filter/membership.zig").dictionaryCoversChunk(&km)) return false;
    }
    return true;
}

pub fn flatGroupKey(target: AggTarget, ci: usize) bool {
    if (target != .grouped) return false;
    for (target.grouped.keys) |k| if (k == .col_ref and k.col_ref.col_idx == ci) return true;
    return false;
}

/// Whether a filtered group's dictionary-covered key costs less as its codes for every row than as values at the
/// filter's survivors, by what each materializes: a four-byte code a row, against a survivor's value (a string's
/// slice, which the fold codes again, or the physical width).
pub fn keyCodesCheaper(cm: schema.ColumnMetaData, rows: usize, survivors: usize) bool {
    const value_bytes: usize = switch (cm.type) {
        .BYTE_ARRAY, .FIXED_LEN_BYTE_ARRAY => @sizeOf([]const u8),
        .INT96 => 12,
        .INT64, .DOUBLE => 8,
        else => 4,
    };
    return rows *| @sizeOf(u32) <= survivors *| value_bytes;
}

pub fn columnFeedsFold(target: AggTarget, ci: usize) bool {
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
pub fn plainLengthColumn(
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
pub const ChunkDecoder = struct {
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

    pub fn column(
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
    pub fn withCallColumns(
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
pub fn sliceRows(fit: usize, num_rows: usize) usize {
    return @min(num_rows, @max(64, fit / 64 * 64));
}

const CodeUses = struct { code: []bool, value: []bool, levels: []bool, hist_ok: []bool };

pub fn planCodeUses(ra: std.mem.Allocator, in: AggRowGroup, target: AggTarget, staged: bool) !CodeUses {
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

pub fn mark(set: []bool, ci: usize) void {
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

