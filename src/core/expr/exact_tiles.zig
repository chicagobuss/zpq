//! Exact DECIMAL SUM/AVG and COUNT(*) folded one decode tile at a time, for a row group the scan reads in
//! cache-sized slices (`consumer.scanRGSliced`).
//!
//! Each tile proves, from the values it actually holds, that every node of every argument tree and every
//! composite's sum stays inside i64 and inside its DECIMAL precision. A proven tile can raise no error, so it folds
//! with wrapping 64-bit arithmetic in one pass of L1-sized windows: a dictionary-coded column is read through its
//! codes, a node over one coded column is evaluated once per dictionary entry, and nothing row-sized is written but
//! the windows. A column handed over as a `CodePull` is unpacked into the window it is folded in, instead of into a
//! tile-length code buffer that the fold would then read back. Sums widen into `DecSum`s once per composite per
//! tile. A tile whose proof fails folds through the checked wide path (`agg_coded.exactRootSums`); a pull is drained
//! into codes only then, so a proof that fails never unpacks twice. Nothing reaches the group table until the row
//! group ends, so a tile that would fail, or a shape this path cannot read, leaves the whole row group to the
//! established path, which raises the error it always did, in the order it always did.

const std = @import("std");
const filter_eval = @import("../filter/eval.zig");
const filter_selection = @import("../filter/selection.zig");
const expr_ast = @import("ast.zig");
const expr_eval = @import("eval.zig");
const agg = @import("agg.zig");
const agg_coded = @import("agg_coded.zig");

const Column = filter_eval.Batch.Column;
const Sel = filter_selection.SelectionVector;
const AggCall = agg.AggCall;

/// Forced-path control for tests: the largest bound a tile's sums may prove in place of i64's; 0 fails every tile.
pub var forced_limit: ?u64 = null;

/// Dictionary codes for one column of a tile, unpacked by the fold a window at a time. `pull` appends the next
/// `out.len` codes (the page is all-present; nulls stay on the materialized path). `rows` is the tile length.
pub const CodePull = struct {
    ctx: *anyopaque,
    pull: *const fn (*anyopaque, []u32) anyerror!void,
    rows: usize,
};

/// Rows per fused window: a node's lane of them is 4 KiB, so a tree's lanes stay in L1.
const window_rows = 512;
/// Interleaved partials per composite, so a run of rows in one composite does not serialize on one add.
const lanes = 4;
/// Partials past this many composites are not worth a fused pass over a tile (the scratch outgrows L2).
const max_space = 1 << 12;

const no_row = agg_coded.no_row;
const i64_max: u128 = std.math.maxInt(i64);

const Node = struct {
    expr: expr_ast.Expr,
    kind: union(enum) {
        literal: i64,
        /// A column's own unscaled values, by leaf index.
        col: usize,
        bin: struct { op: expr_ast.Op, l: usize, r: usize, lf: i64, rf: i64 },
    },
    /// Values must stay under this (10^precision); i64's bound past 18 digits.
    limit: u128,
    /// The one column the node reads, if only one: evaluated per dictionary entry while that column is coded.
    source: Source,

    const Source = union(enum) { none, one: usize, many };
};

/// How a tile reads one node's values.
const Access = union(enum) {
    scalar: i64,
    /// Per dictionary entry, read through the codes. `base` is the row `codes[0]` stands for: 0 for a tile-length
    /// buffer, the window start when `codes` is only that window.
    table: struct { t: []const i64, codes: []const u32, base: usize = 0 },
    lane64: []const i64,
    lane32: []const i32,
    /// A node computed per row into window lane `n`.
    tmp: usize,
};

/// A dictionary-evaluated node, cached for the row group: the chunk has one dictionary per column.
const DictTable = struct {
    dict: usize = 0,
    len: usize = 0,
    values: []i64 = &.{},
    bound: u128 = 0,
    ok: bool = false,
};

/// Tiles by how they folded, process-wide: what tests and diagnostics read to see which path ran.
pub const counters = struct {
    pub var proved: std.atomic.Value(u64) = .init(0);
    pub var wide: std.atomic.Value(u64) = .init(0);
    /// Row groups left to the established path.
    pub var abandoned: std.atomic.Value(u64) = .init(0);

    pub fn reset() void {
        proved.store(0, .monotonic);
        wide.store(0, .monotonic);
        abandoned.store(0, .monotonic);
    }
};

/// The calls this path folds: SUM and AVG over `+ - *` trees of INT32/INT64 DECIMAL columns and literals, and
/// COUNT(*), grouped by plain column keys. Anything else stays on the established path.
pub fn admits(calls: []const AggCall, keys: []const expr_ast.Expr) bool {
    var any = false;
    for (calls) |call| {
        if (call.where != null or call.offset != 0) return false;
        switch (call.func) {
            .sum, .avg => {
                const arg = call.arg orelse return false;
                if (arg.typeOf() != .dec or !treeAdmits(arg)) return false;
                any = true;
            },
            .count => if (call.arg != null) return false,
            else => return false,
        }
    }
    for (keys) |k| {
        if (k != .col_ref or k.col_ref.expr_type == .dec or k.col_ref.unsigned_64) return false;
    }
    return any;
}

fn treeAdmits(e: expr_ast.Expr) bool {
    return switch (e) {
        .literal => |l| (l == .dec or l == .i64) and literalValue(e) != null,
        .col_ref => |c| c.expr_type == .dec and (c.physical_type == .INT32 or c.physical_type == .INT64) and
            !c.unsigned_32 and !c.unsigned_64,
        .binop => |b| blk: {
            if (b.result_type != .dec or b.dec == null) break :blk false;
            const spec = b.dec.?;
            const ls = b.left.decSpec() orelse break :blk false;
            const rs = b.right.decSpec() orelse break :blk false;
            switch (b.op) {
                // Past 38 digits the common type caps and checks each operand on its own (`decBinOpLaneCached`).
                .add, .sub => if (@as(u16, @max(ls.precision - ls.scale, rs.precision - rs.scale)) +
                    @max(ls.scale, rs.scale) > expr_ast.Dec.max_precision or
                    spec.scale < ls.scale or spec.scale < rs.scale or spec.scale - @min(ls.scale, rs.scale) > 18)
                    break :blk false,
                .mul => if (@as(u16, ls.scale) + rs.scale != spec.scale) break :blk false,
                else => break :blk false,
            }
            break :blk treeAdmits(b.left.*) and treeAdmits(b.right.*);
        },
        else => false,
    };
}

/// A literal's unscaled value at its own scale, when it fits i64.
fn literalValue(e: expr_ast.Expr) ?i64 {
    return switch (e.literal) {
        .i64 => |v| v,
        .dec => |d| std.math.cast(i64, d.unscaled),
        else => null,
    };
}

pub const ExactTiles = struct {
    calls: []const AggCall,
    keys: []const expr_ast.Expr,
    nodes: []Node,
    /// Per call: its root node, or null for COUNT(*).
    roots: []?usize,
    /// Distinct roots, in first-call order: the order the wide path evaluates them in.
    root_list: []usize,
    /// Per call: its index in `root_list`.
    root_slot: []?usize,
    tables: []DictTable,
    lookup_len: usize,
    /// The row group's arena: the state below and the dictionary tables.
    arena: std.mem.Allocator,

    // Row-group state, set by the first tile.
    space: usize = 0,
    strides: []usize = &.{},
    /// The key dictionaries every tile must share, by identity and length.
    key_dict: []usize = &.{},
    key_len: []usize = &.{},
    kd_cols: []Column = &.{},
    /// `root_list.len * space` sums, widened per tile.
    sums: []agg.DecSum = &.{},
    /// Selected rows per composite.
    counts: []u64 = &.{},
    first_row: []u32 = &.{},

    /// Null when the calls' trees do not fit this path. `a` holds the row group's state.
    pub fn init(a: std.mem.Allocator, calls: []const AggCall, keys: []const expr_ast.Expr, lookup_len: usize) !?ExactTiles {
        if (!admits(calls, keys)) return null;
        var nodes: std.ArrayList(Node) = .empty;
        const roots = try a.alloc(?usize, calls.len);
        var root_list: std.ArrayList(usize) = .empty;
        const root_slot = try a.alloc(?usize, calls.len);
        for (calls, roots, root_slot) |call, *root, *slot| {
            root.* = null;
            slot.* = null;
            const arg = call.arg orelse continue;
            const r = (try addNode(a, &nodes, arg)) orelse return null;
            root.* = r;
            slot.* = std.mem.indexOfScalar(usize, root_list.items, r) orelse blk: {
                try root_list.append(a, r);
                break :blk root_list.items.len - 1;
            };
        }
        const tables = try a.alloc(DictTable, nodes.items.len);
        @memset(tables, .{});
        return .{
            .calls = calls,
            .keys = keys,
            .nodes = nodes.items,
            .roots = roots,
            .root_list = root_list.items,
            .root_slot = root_slot,
            .tables = tables,
            .lookup_len = lookup_len,
            .arena = a,
        };
    }

    /// The DECIMAL leaves this path reads: a coded one is read through its codes.
    pub fn readsLeaf(self: *const ExactTiles, ci: usize) bool {
        for (self.nodes) |n| if (n.kind == .col and n.kind.col == ci) return true;
        return false;
    }

    fn addNode(a: std.mem.Allocator, nodes: *std.ArrayList(Node), e: expr_ast.Expr) !?usize {
        for (nodes.items, 0..) |n, i| if (n.expr.eql(e)) return i;
        var node: Node = .{ .expr = e, .kind = undefined, .limit = i64_max + 1, .source = .none };
        switch (e) {
            .literal => node.kind = .{ .literal = literalValue(e) orelse return null },
            .col_ref => |c| {
                node.kind = .{ .col = c.col_idx };
                node.source = .{ .one = c.col_idx };
            },
            .binop => |b| {
                const l = (try addNode(a, nodes, b.left.*)) orelse return null;
                const r = (try addNode(a, nodes, b.right.*)) orelse return null;
                const spec = b.dec.?;
                const lf: i64, const rf: i64 = switch (b.op) {
                    .add, .sub => .{
                        @intCast(expr_ast.pow10(spec.scale - b.left.decSpec().?.scale)),
                        @intCast(expr_ast.pow10(spec.scale - b.right.decSpec().?.scale)),
                    },
                    else => .{ 1, 1 },
                };
                node.kind = .{ .bin = .{ .op = b.op, .l = l, .r = r, .lf = lf, .rf = rf } };
                node.limit = @min(node.limit, @as(u128, @intCast(spec.bound())));
                const ls = nodes.items[l].source;
                const rs = nodes.items[r].source;
                node.source = if (ls == .none) rs else if (rs == .none) ls else if (ls == .one and rs == .one and
                    ls.one == rs.one) ls else .many;
            },
            else => return null,
        }
        try nodes.append(a, node);
        return nodes.items.len - 1;
    }

    pub const Outcome = enum { folded, abandon };

    /// Fold one tile: `cols` by `lookup`, rows `sel` keeps, starting at row `base` of the row group. `ra` holds the
    /// row group's state; `ta` the tile's scratch. `pulls`, when set, is one slot per `cols` entry: a dictionary
    /// column whose codes are still packed. `.abandon` leaves the row group to the established path.
    pub fn fold(
        self: *ExactTiles,
        ra: std.mem.Allocator,
        ta: std.mem.Allocator,
        cols: []const Column,
        lookup: []const ?usize,
        sel: *const Sel,
        base: usize,
        pulls: ?[]const ?CodePull,
    ) !Outcome {
        const n = sel.len;
        if (n == 0) return .folded;
        if (!try self.bindKeys(ra, cols, lookup, pulls)) return .abandon;
        // Every column a tree reads: coded with its exact dictionary, or exact values; flat either way. A pull is a
        // coded column whose codes are not buffered yet.
        for (self.nodes) |node| if (node.kind == .col) {
            const at = lookup[node.kind.col] orelse return .abandon;
            const col = cols[at];
            if (col.nullInfo().max_rep != 0 or col.levelsOnly() or col.hist() != null) return .abandon;
            if (col.codes() != null or pulling(pulls, at)) {
                if (dictExact(col) == null) return .abandon;
            } else if (valuesExact(col) == null) return .abandon;
        };
        var used = cols;
        if (pulls) |ps| if (anyPull(ps)) {
            if (try self.foldPulled(ta, used, lookup, sel, ps, base)) {
                _ = counters.proved.fetchAdd(1, .monotonic);
                return .folded;
            }
            // The proof failed before a code was pulled. Drain each stream once and fold the buffered tile.
            used = try realizePulls(ta, used, ps);
        };
        const comp = try self.composites(ta, used, lookup, n);
        // Selected rows per composite, and each composite's first.
        const tile_counts = try ta.alloc(u64, self.space);
        @memset(tile_counts, 0);
        var kept: u64 = 0;
        for (sel.mask) |word| kept += @popCount(word);
        // A sparse selection walks only its rows here and in the fold; lanes would compute every row.
        const sparse = kept * 8 < n;
        if (sparse) {
            for (sel.mask, 0..) |word, w| {
                var bits = word;
                while (bits != 0) : (bits &= bits - 1) {
                    const r = w * 64 + @ctz(bits);
                    tile_counts[comp[r]] += 1;
                    if (self.first_row[comp[r]] == no_row) self.first_row[comp[r]] = @intCast(base + r);
                }
            }
        } else forEachWordRow(sel, n, struct {
            fn f(c: anytype, word: u64, r: usize) void {
                const bit: u1 = @truncate(word >> @intCast(r % 64));
                c.cnt[c.comp[r]] += bit;
                if (bit == 1 and c.first[c.comp[r]] == no_row) c.first[c.comp[r]] = @intCast(c.base + r);
            }
        }.f, .{ .cnt = tile_counts, .comp = comp, .first = self.first_row, .base = base });
        for (self.counts, tile_counts) |*c, t| c.* += t;

        if (try self.foldNarrow(ta, used, lookup, sel, comp, tile_counts, n, sparse)) {
            _ = counters.proved.fetchAdd(1, .monotonic);
            return .folded;
        }
        _ = counters.wide.fetchAdd(1, .monotonic);
        return if (try self.foldWide(ta, used, lookup, sel, comp)) .folded else .abandon;
    }

    /// The tile's composite per row, binding the key dictionaries on the first tile. False when a key is not coded
    /// or its dictionary differs from the first tile's. A pulled key is coded: its dictionary is already installed.
    fn bindKeys(
        self: *ExactTiles,
        ra: std.mem.Allocator,
        cols: []const Column,
        lookup: []const ?usize,
        pulls: ?[]const ?CodePull,
    ) !bool {
        const first = self.space == 0;
        if (first) {
            self.strides = try ra.alloc(usize, self.keys.len);
            self.key_dict = try ra.alloc(usize, self.keys.len);
            self.key_len = try ra.alloc(usize, self.keys.len);
            self.kd_cols = try ra.alloc(Column, self.keys.len);
        }
        var space: usize = 1;
        for (self.keys, 0..) |k, i| {
            const at = lookup[k.col_ref.col_idx] orelse return false;
            const col = cols[at];
            if (col.nullInfo().max_rep != 0) return false;
            if (col.codes() == null and !pulling(pulls, at)) return false;
            // A pulled key is read through the dictionary already installed. An empty one has no code to unpack.
            if (pulling(pulls, at) and col.dictLen() == 0) return false;
            const id = dictIdentity(col);
            if (first) {
                self.strides[i] = space;
                self.key_dict[i] = id;
                self.key_len[i] = col.dictLen();
                self.kd_cols[i] = try agg_coded.keyDictColumn(ra, col, k.col_ref, self.lookup_len);
            } else if (self.key_dict[i] != id or self.key_len[i] != col.dictLen()) return false;
            space *= col.dictLen() + 1; // + the null slot
            if (space > max_space) return false;
        }
        if (first) {
            self.space = space;
            self.sums = try ra.alloc(agg.DecSum, self.root_list.len * space);
            @memset(self.sums, .{});
            self.counts = try ra.alloc(u64, space);
            @memset(self.counts, 0);
            self.first_row = try ra.alloc(u32, space);
            @memset(self.first_row, no_row);
        }
        return true;
    }

    fn composites(self: *ExactTiles, ta: std.mem.Allocator, cols: []const Column, lookup: []const ?usize, n: usize) ![]const u32 {
        if (self.keys.len == 1) {
            const col = cols[lookup[self.keys[0].col_ref.col_idx].?];
            if (col.nullInfo().def_levels == null) return col.codes().?[0..n];
        }
        const out = try ta.alloc(u32, n);
        @memset(out, 0);
        for (self.keys, self.strides) |k, stride| {
            const col = cols[lookup[k.col_ref.col_idx].?];
            const codes = col.codes().?;
            const ni = col.nullInfo();
            const null_slot: u32 = @intCast(col.dictLen());
            const st: u32 = @intCast(stride);
            if (ni.def_levels) |dl| {
                for (out, codes[0..n], dl[0..n]) |*cp, code, d| cp.* += (if (d < ni.max_def) null_slot else code) * st;
            } else {
                for (out, codes[0..n]) |*cp, code| cp.* += code * st;
            }
        }
        return out;
    }

    /// The proven fold of a tile whose dictionary codes are still packed. False, having pulled nothing and added
    /// nothing, when the values do not prove the narrow arithmetic. The scan is in order: a sparse selection still
    /// walks every row, and the mask drops the ones it does not keep.
    fn foldPulled(
        self: *ExactTiles,
        ta: std.mem.Allocator,
        cols: []const Column,
        lookup: []const ?usize,
        sel: *const Sel,
        pulls: []const ?CodePull,
        base: usize,
    ) !bool {
        const n = sel.len;
        for (pulls) |p| if (p) |pull| std.debug.assert(pull.rows == n);
        const win = try ta.alloc([]u32, cols.len);
        for (pulls, 0..) |p, i| win[i] = if (p != null) try ta.alloc(u32, window_rows) else &.{};
        const access = try ta.alloc(Access, self.nodes.len);
        const bound = try ta.alloc(u128, self.nodes.len);
        const streamed = try ta.alloc(bool, self.nodes.len);
        @memset(streamed, false);
        var tmps: usize = 0;
        for (self.nodes, 0..) |node, i| {
            switch (node.kind) {
                .literal => |v| {
                    access[i] = .{ .scalar = v };
                    bound[i] = @abs(v);
                },
                .col => |ci| {
                    const at = lookup[ci].?;
                    if (pulling(pulls, at) or cols[at].codes() != null) {
                        const t = try self.table(i, cols[at]) orelse return false;
                        access[i] = .{ .table = .{ .t = t.values, .codes = if (pulling(pulls, at)) win[at] else cols[at].codes().? } };
                        streamed[i] = pulling(pulls, at);
                        bound[i] = t.bound;
                    } else switch (valuesExact(cols[at]).?) {
                        .i64 => |v| {
                            access[i] = .{ .lane64 = v[0..n] };
                            bound[i] = maxAbs(i64, v[0..n], cols[at], sel, false);
                        },
                        .i32 => |v| {
                            access[i] = .{ .lane32 = v[0..n] };
                            bound[i] = maxAbs(i32, v[0..n], cols[at], sel, false);
                        },
                        .i128 => return false,
                    }
                },
                .bin => |b| {
                    if (node.source == .one) {
                        const at = lookup[node.source.one].?;
                        if (pulling(pulls, at) or cols[at].codes() != null) {
                            const t = try self.table(i, cols[at]) orelse return false;
                            access[i] = .{ .table = .{ .t = t.values, .codes = if (pulling(pulls, at)) win[at] else cols[at].codes().? } };
                            streamed[i] = pulling(pulls, at);
                            bound[i] = t.bound;
                            if (bound[i] >= node.limit) return false;
                            continue;
                        }
                    }
                    bound[i] = switch (b.op) {
                        .add, .sub => sat(sat(bound[b.l], @abs(b.lf), .mul), sat(bound[b.r], @abs(b.rf), .mul), .add),
                        .mul => sat(bound[b.l], bound[b.r], .mul),
                        else => unreachable,
                    };
                    access[i] = .{ .tmp = tmps };
                    tmps += 1;
                },
            }
            if (bound[i] >= node.limit) return false;
        }
        const limit: u128 = forced_limit orelse i64_max;
        for (self.root_list) |r| if (sat(bound[r], n, .mul) > limit) return false;

        const lane_buf = try ta.alloc(i64, tmps * window_rows);
        const R = self.root_list.len;
        const part = try ta.alloc(i64, R * self.space * lanes);
        @memset(part, 0);
        const nulls = try ta.alloc(?[]u64, R);
        for (self.root_list, nulls) |r, *nl| {
            nl.* = null;
            if (!self.hasNullInput(r, cols, lookup)) continue;
            const c = try ta.alloc(u64, self.space * lanes);
            @memset(c, 0);
            nl.* = c;
        }
        const comp_win = try ta.alloc(u32, window_rows);
        const tile_counts = try ta.alloc(u64, self.space);
        @memset(tile_counts, 0);
        const valid = try ta.alloc(u1, window_rows);
        var w0: usize = 0;
        while (w0 < n) : (w0 += window_rows) {
            const wn = @min(window_rows, n - w0);
            for (pulls, win) |p, w| if (p) |pull| try pull.pull(pull.ctx, w[0..wn]);
            for (streamed, 0..) |on, i| {
                if (!on) continue;
                switch (access[i]) {
                    .table => |*t| t.base = w0,
                    else => unreachable,
                }
            }
            @memset(comp_win[0..wn], 0);
            for (self.keys, self.strides) |k, stride| {
                const at = lookup[k.col_ref.col_idx].?;
                const st: u32 = @intCast(stride);
                if (pulling(pulls, at)) {
                    for (comp_win[0..wn], win[at][0..wn]) |*cp, code| cp.* += code * st;
                } else {
                    const col = cols[at];
                    const codes = col.codes().?;
                    const ni = col.nullInfo();
                    const null_slot: u32 = @intCast(col.dictLen());
                    if (ni.def_levels) |dl| {
                        for (comp_win[0..wn], codes[w0..][0..wn], dl[w0..][0..wn]) |*cp, code, d| {
                            cp.* += (if (d < ni.max_def) null_slot else code) * st;
                        }
                    } else for (comp_win[0..wn], codes[w0..][0..wn]) |*cp, code| cp.* += code * st;
                }
            }
            for (self.nodes, 0..) |node, i| if (access[i] == .tmp) {
                const b = node.kind.bin;
                const out = lane_buf[access[i].tmp * window_rows ..][0..wn];
                evalLane(b.op, access[b.l], access[b.r], b.lf, b.rf, lane_buf, out, w0);
            };
            for (self.root_list, 0..) |r, ri| {
                const slots = part[ri * self.space * lanes ..][0 .. self.space * lanes];
                if (nulls[ri]) |cnt| {
                    @memset(valid[0..wn], 1);
                    self.maskNulls(r, cols, lookup, valid[0..wn], w0);
                    foldRoot(true, access[r], lane_buf, slots, cnt, valid, comp_win, w0, sel, w0, wn);
                } else foldRoot(false, access[r], lane_buf, slots, &.{}, valid, comp_win, w0, sel, w0, wn);
            }
            var word = w0 / 64;
            while (word * 64 < w0 + wn) : (word += 1) {
                const bits = sel.mask[word];
                if (bits == 0) continue;
                const lo = word * 64;
                for (lo..@min(lo + 64, w0 + wn)) |r| {
                    const keep: u1 = @truncate(bits >> @intCast(r % 64));
                    if (keep == 0) continue;
                    const cp = comp_win[r - w0];
                    tile_counts[cp] += 1;
                    if (self.first_row[cp] == no_row) self.first_row[cp] = @intCast(base + r);
                }
            }
        }
        self.widenTile(part, nulls, tile_counts);
        for (self.counts, tile_counts) |*c, t| c.* += t;
        return true;
    }

    /// The proven fused fold. False, with nothing added, when the tile's values do not prove it safe.
    fn foldNarrow(
        self: *ExactTiles,
        ta: std.mem.Allocator,
        cols: []const Column,
        lookup: []const ?usize,
        sel: *const Sel,
        comp: []const u32,
        tile_counts: []const u64,
        n: usize,
        sparse: bool,
    ) !bool {
        const access = try ta.alloc(Access, self.nodes.len);
        const bound = try ta.alloc(u128, self.nodes.len);
        var tmps: usize = 0;
        for (self.nodes, 0..) |node, i| {
            switch (node.kind) {
                .literal => |v| {
                    access[i] = .{ .scalar = v };
                    bound[i] = @abs(v);
                },
                .col => |ci| {
                    const col = cols[lookup[ci].?];
                    if (col.codes()) |codes| {
                        const t = try self.table(i, col) orelse return false;
                        access[i] = .{ .table = .{ .t = t.values, .codes = codes[0..n] } };
                        bound[i] = t.bound;
                    } else switch (valuesExact(col).?) {
                        .i64 => |v| {
                            access[i] = .{ .lane64 = v[0..n] };
                            bound[i] = maxAbs(i64, v[0..n], col, sel, sparse);
                        },
                        .i32 => |v| {
                            access[i] = .{ .lane32 = v[0..n] };
                            bound[i] = maxAbs(i32, v[0..n], col, sel, sparse);
                        },
                        .i128 => return false,
                    }
                },
                .bin => |b| {
                    if (node.source == .one) {
                        const col = cols[lookup[node.source.one].?];
                        if (col.codes()) |codes| {
                            const t = try self.table(i, col) orelse return false;
                            access[i] = .{ .table = .{ .t = t.values, .codes = codes[0..n] } };
                            bound[i] = t.bound;
                            if (bound[i] >= node.limit) return false;
                            continue;
                        }
                    }
                    bound[i] = switch (b.op) {
                        .add, .sub => sat(sat(bound[b.l], @abs(b.lf), .mul), sat(bound[b.r], @abs(b.rf), .mul), .add),
                        .mul => sat(bound[b.l], bound[b.r], .mul),
                        else => unreachable,
                    };
                    access[i] = .{ .tmp = tmps };
                    tmps += 1;
                },
            }
            if (bound[i] >= node.limit) return false;
        }
        const limit: u128 = forced_limit orelse i64_max;
        for (self.root_list) |r| if (sat(bound[r], n, .mul) > limit) return false;

        // Nothing past here can fail but an allocation, and nothing reaches `sums` before the last window.
        const lane_buf = try ta.alloc(i64, tmps * window_rows);
        const R = self.root_list.len;
        const part = try ta.alloc(i64, R * self.space * lanes);
        @memset(part, 0);
        // Rows each root keeps where some input is null; null when none of its inputs has a null row here.
        const nulls = try ta.alloc(?[]u64, R);
        for (self.root_list, nulls) |r, *nl| {
            nl.* = null;
            // Sparse, every root checks its rows' levels: cheaper than scanning a tile of them.
            if (!sparse and !self.hasNullInput(r, cols, lookup)) continue;
            const c = try ta.alloc(u64, self.space * lanes);
            @memset(c, 0);
            nl.* = c;
        }
        if (sparse) {
            for (self.root_list, 0..) |r, ri| {
                const slots = part[ri * self.space * lanes ..][0 .. self.space * lanes];
                for (sel.mask, 0..) |word, w| {
                    var bits = word;
                    while (bits != 0) : (bits &= bits - 1) {
                        const row = w * 64 + @ctz(bits);
                        if (!self.rowValid(r, cols, lookup, row)) continue;
                        const slot = @as(usize, comp[row]) * lanes + row % lanes;
                        slots[slot] +%= self.rowValue(access, r, row);
                        nulls[ri].?[slot] += 1;
                    }
                }
            }
            self.widenTile(part, nulls, tile_counts);
            return true;
        }
        const valid = try ta.alloc(u1, window_rows);
        var w0: usize = 0;
        while (w0 < n) : (w0 += window_rows) {
            const wn = @min(window_rows, n - w0);
            for (self.nodes, 0..) |node, i| if (access[i] == .tmp) {
                const b = node.kind.bin;
                const out = lane_buf[access[i].tmp * window_rows ..][0..wn];
                evalLane(b.op, access[b.l], access[b.r], b.lf, b.rf, lane_buf, out, w0);
            };
            for (self.root_list, 0..) |r, ri| {
                const slots = part[ri * self.space * lanes ..][0 .. self.space * lanes];
                if (nulls[ri]) |cnt| {
                    @memset(valid[0..wn], 1);
                    self.maskNulls(r, cols, lookup, valid[0..wn], w0);
                    foldRoot(true, access[r], lane_buf, slots, cnt, valid, comp, 0, sel, w0, wn);
                } else foldRoot(false, access[r], lane_buf, slots, &.{}, valid, comp, 0, sel, w0, wn);
            }
        }
        self.widenTile(part, nulls, tile_counts);
        return true;
    }

    /// The tile's interleaved partials into the row group's 192-bit sums.
    fn widenTile(self: *ExactTiles, part: []const i64, nulls: []const ?[]u64, tile_counts: []const u64) void {
        for (self.root_list, 0..) |_, ri| {
            const slots = part[ri * self.space * lanes ..][0 .. self.space * lanes];
            for (0..self.space) |cp| {
                var sum: i128 = 0;
                var kept_rows: u64 = 0;
                for (0..lanes) |l| {
                    sum += slots[cp * lanes + l];
                    if (nulls[ri]) |cnt| kept_rows += cnt[cp * lanes + l];
                }
                if (nulls[ri] == null) kept_rows = tile_counts[cp];
                if (kept_rows != 0) self.sums[ri * self.space + cp].add(sum, kept_rows);
            }
        }
    }

    /// Node `i` at one row, wrapping like `evalLane`: computed nodes are evaluated through their children.
    fn rowValue(self: *const ExactTiles, access: []const Access, i: usize, row: usize) i64 {
        return switch (access[i]) {
            .scalar => |v| v,
            .table => |t| t.t[t.codes[row - t.base]],
            .lane64 => |v| v[row],
            .lane32 => |v| v[row],
            .tmp => blk: {
                const b = self.nodes[i].kind.bin;
                const x = self.rowValue(access, b.l, row);
                const y = self.rowValue(access, b.r, row);
                break :blk switch (b.op) {
                    .add => (x *% b.lf) +% (y *% b.rf),
                    .sub => (x *% b.lf) -% (y *% b.rf),
                    .mul => x *% y,
                    else => unreachable,
                };
            },
        };
    }

    fn rowValid(self: *const ExactTiles, i: usize, cols: []const Column, lookup: []const ?usize, row: usize) bool {
        return switch (self.nodes[i].kind) {
            .literal => true,
            .col => |ci| blk: {
                const ni = cols[lookup[ci].?].nullInfo();
                break :blk if (ni.def_levels) |dl| dl[row] >= ni.max_def else true;
            },
            .bin => |b| self.rowValid(b.l, cols, lookup, row) and self.rowValid(b.r, cols, lookup, row),
        };
    }

    fn hasNullInput(self: *const ExactTiles, r: usize, cols: []const Column, lookup: []const ?usize) bool {
        return switch (self.nodes[r].kind) {
            .literal => false,
            .col => |ci| hasNullRows(cols[lookup[ci].?]),
            .bin => |b| self.hasNullInput(b.l, cols, lookup) or self.hasNullInput(b.r, cols, lookup),
        };
    }

    fn maskNulls(self: *const ExactTiles, r: usize, cols: []const Column, lookup: []const ?usize, valid: []u1, w0: usize) void {
        switch (self.nodes[r].kind) {
            .literal => {},
            .col => |ci| {
                const ni = cols[lookup[ci].?].nullInfo();
                if (ni.def_levels) |dl| for (valid, dl[w0..][0..valid.len]) |*v, d| {
                    v.* &= @intFromBool(d >= ni.max_def);
                };
            },
            .bin => |b| {
                self.maskNulls(b.l, cols, lookup, valid, w0);
                self.maskNulls(b.r, cols, lookup, valid, w0);
            },
        }
    }

    /// Node `i` evaluated per entry of `col`'s dictionary, exactly; null when an entry leaves i64 or the node's
    /// precision (that entry may be one no row holds: the tile then folds wide, which decides).
    fn table(self: *ExactTiles, i: usize, col: Column) !?*const DictTable {
        const t = &self.tables[i];
        const id = dictIdentity(col);
        if (t.dict == id and t.len == col.dictLen()) return if (t.ok) t else null;
        t.* = .{ .dict = id, .len = col.dictLen() };
        const exact = dictExact(col).?;
        // Row-group lifetime, like the dictionary it is evaluated from. An all-NULL chunk has no entries, yet its
        // masked rows still read entry 0: keep one, zero.
        const buf = try self.arena.alloc(i64, @max(1, col.dictLen()));
        buf[0] = 0;
        var b: u128 = 0;
        for (buf[0..col.dictLen()], 0..) |*o, k| {
            const v = self.entry(i, exact.at(k)) orelse return null;
            if (@abs(v) > i64_max) return null;
            o.* = @intCast(v);
            b = @max(b, @abs(v));
        }
        t.values = buf;
        t.bound = b;
        t.ok = true;
        return t;
    }

    /// Node `i` at one value of its single source column, in i128 with every node's precision checked.
    fn entry(self: *const ExactTiles, i: usize, x: i128) ?i128 {
        const node = self.nodes[i];
        const v: i128 = switch (node.kind) {
            .literal => |l| l,
            .col => x,
            .bin => |b| blk: {
                const l = self.entry(b.l, x) orelse return null;
                const r = self.entry(b.r, x) orelse return null;
                break :blk switch (b.op) {
                    .add => std.math.add(i128, std.math.mul(i128, l, b.lf) catch return null, std.math.mul(i128, r, b.rf) catch return null) catch return null,
                    .sub => std.math.sub(i128, std.math.mul(i128, l, b.lf) catch return null, std.math.mul(i128, r, b.rf) catch return null) catch return null,
                    .mul => std.math.mul(i128, l, r) catch return null,
                    else => unreachable,
                };
            },
        };
        if (@abs(v) >= node.limit) return null;
        return v;
    }

    /// The checked wide path over the tile: each distinct root, in call order, through `agg_coded.exactRootSums`.
    /// False on any error, which the whole row group then reproduces in its own order.
    fn foldWide(
        self: *ExactTiles,
        ta: std.mem.Allocator,
        cols: []const Column,
        lookup: []const ?usize,
        sel: *const Sel,
        comp: []const u32,
    ) !bool {
        const mat = try ta.alloc(Column, cols.len);
        for (cols, mat) |c, *m| m.* = try decodedView(ta, c);
        const batch: filter_eval.Batch = .{ .cols = mat, .num_rows = sel.len };
        var plan: expr_eval.ExactPlan = .{};
        const ids = try ta.alloc(usize, self.root_list.len);
        for (self.root_list, ids) |r, *id| id.* = (try plan.add(ta, &batch, lookup, self.nodes[r].expr)) orelse return false;
        const deltas = try ta.alloc([]agg.DecSum, self.root_list.len);
        for (ids, deltas) |id, *d| {
            d.* = agg_coded.exactRootSums(ta, &plan, id, self.space, comp, &batch, lookup, sel) catch |err| switch (err) {
                error.OutOfMemory => return err,
                else => return false,
            };
        }
        for (deltas, 0..) |d, ri| for (d, 0..) |delta, cp| {
            if (delta.n != 0) self.sums[ri * self.space + cp].add(delta.get(), delta.n);
        };
        return true;
    }

    /// Fold the row group's sums into the target: each composite with a selected row becomes its group at that row's
    /// position, as `groupedCoded` makes them. `scalar`: the one composite's accumulators.
    pub fn finish(
        self: *ExactTiles,
        ra: std.mem.Allocator,
        groups: ?*agg.GroupTable,
        scalar: []agg.Accumulator,
        pos_base: u64,
    ) !void {
        if (self.space == 0) return;
        var scratch: std.ArrayList(u8) = .empty;
        const rows = try ra.alloc(usize, self.keys.len);
        for (0..self.space) |cp| {
            if (self.first_row[cp] == no_row) continue;
            const accs: []agg.Accumulator = if (groups) |t| blk: {
                var rem: usize = cp;
                var i: usize = self.keys.len;
                while (i > 0) {
                    i -= 1;
                    rows[i] = rem / self.strides[i];
                    rem %= self.strides[i];
                }
                try agg_coded.serializeKeyAt(&scratch, ra, self.kd_cols, rows);
                const g = try t.getOrInsert(scratch.items, self.calls, pos_base + self.first_row[cp]);
                break :blk t.accumulators.items[g * self.calls.len ..][0..self.calls.len];
            } else scalar;
            for (self.calls, accs, self.root_slot) |call, *acc, slot| {
                if (slot) |ri| {
                    const d = self.sums[ri * self.space + cp];
                    if (d.n == 0) continue;
                    switch (acc.*) {
                        .sum_d, .avg_d => |*s| s.add(d.get(), d.n),
                        else => return error.UnsupportedAggType,
                    }
                } else if (call.func == .count) switch (acc.*) {
                    .count => |*c| c.* += self.counts[cp],
                    else => return error.UnsupportedAggType,
                };
            }
        }
    }
};

fn pulling(pulls: ?[]const ?CodePull, at: usize) bool {
    const ps = pulls orelse return false;
    return at < ps.len and ps[at] != null;
}

fn anyPull(pulls: []const ?CodePull) bool {
    for (pulls) |p| if (p != null) return true;
    return false;
}

/// Unpack every pull into a tile-length code buffer and hand the fold the column it would have decoded up front.
fn realizePulls(ta: std.mem.Allocator, cols: []const Column, pulls: []const ?CodePull) ![]Column {
    const out = try ta.alloc(Column, cols.len);
    for (cols, pulls, out) |c, p, *o| {
        o.* = c;
        if (p) |pull| {
            const codes = try ta.alloc(u32, pull.rows);
            try pull.pull(pull.ctx, codes);
            o.* = putCodes(c, codes);
        }
    }
    return out;
}

fn putCodes(col: Column, codes: []const u32) Column {
    return switch (col) {
        inline else => |c, tag| blk: {
            var o = c;
            o.codes = codes;
            break :blk @unionInit(Column, @tagName(tag), o);
        },
    };
}

fn sat(a: u128, b: u128, comptime op: enum { add, mul }) u128 {
    return switch (op) {
        .add => std.math.add(u128, a, b) catch std.math.maxInt(u128),
        .mul => std.math.mul(u128, a, b) catch std.math.maxInt(u128),
    };
}

/// The largest magnitude among the rows `sel` keeps that are present. Other rows' values are masked out of every
/// sum, and a gated decode leaves them zero or stale.
fn maxAbs(comptime T: type, v: []const T, col: Column, sel: *const Sel, sparse: bool) u128 {
    const ni = col.nullInfo();
    var m: u64 = 0;
    if (sparse) {
        for (sel.mask, 0..) |word, w| {
            var bits = word;
            while (bits != 0) : (bits &= bits - 1) {
                const r = w * 64 + @ctz(bits);
                if (ni.def_levels) |d| if (d[r] < ni.max_def) continue;
                m = @max(m, @as(u64, @abs(v[r])));
            }
        }
        return m;
    }
    const dl: ?[]const u32 = if (ni.def_levels != null and hasNullRows(col)) ni.def_levels.? else null;
    for (sel.mask, 0..) |word, w| {
        if (word == 0) continue;
        const base = w * 64;
        if (base >= v.len) break;
        for (base..@min(base + 64, v.len)) |r| {
            var keep: u1 = @truncate(word >> @intCast(r % 64));
            if (dl) |d| keep &= @intFromBool(d[r] >= ni.max_def);
            m = @max(m, @as(u64, @abs(v[r])) & -%@as(u64, keep));
        }
    }
    return m;
}

fn columnHasNulls(col: Column) bool {
    return switch (col) {
        inline else => |c| c.has_nulls,
    };
}

fn hasNullRows(col: Column) bool {
    const ni = col.nullInfo();
    const dl = ni.def_levels orelse return false;
    // A slice the decoder marked empty of nulls is empty of them: the levels are present only because the
    // column is optional. A marked slice is checked, with no early exit so the check vectorizes.
    if (!columnHasNulls(col)) {
        // Safe builds hold the decoder to its mark: one that forgot to set it would sum null slots as values.
        if (std.debug.runtime_safety) for (dl) |d| std.debug.assert(d >= ni.max_def);
        return false;
    }
    var any: bool = false;
    for (dl) |d| any = any or d < ni.max_def;
    return any;
}

/// A coded column's dictionary identity: the same chunk dictionary keeps one backing for the row group.
fn dictIdentity(col: Column) usize {
    return switch (col) {
        inline else => |c| @intFromPtr(c.dict.ptr),
    };
}

/// A coded DECIMAL column's exact dictionary, when it carries one in a 64-bit (or narrower) lane.
fn dictExact(col: Column) ?filter_eval.Exact {
    const du = switch (col) {
        inline .f64, .i32, .i64 => |c| c.dict_unscaled,
        else => null,
    } orelse return null;
    return if (du == .i128) null else du;
}

/// A decoded DECIMAL column's exact values, when it carries them in a 64-bit (or narrower) lane.
fn valuesExact(col: Column) ?filter_eval.Exact {
    const u = switch (col) {
        inline .f64, .i32, .i64 => |c| c.unscaled,
        else => null,
    } orelse return null;
    return if (u == .i128) null else u;
}

/// A tile column as the established exact path reads it: coded columns gathered to values, codes dropped.
fn decodedView(ta: std.mem.Allocator, col: Column) !Column {
    if (col.codes() == null) return col;
    const m = try agg_coded.materialize(ta, col);
    return switch (m) {
        inline else => |c, tag| blk: {
            var o = c;
            o.codes = null;
            o.dict = &.{};
            o.dict_unscaled = null;
            break :blk @unionInit(Column, @tagName(tag), o);
        },
    };
}

inline fn get(comptime tag: std.meta.Tag(Access), a: anytype, tmp: []const i64, r: usize, w0: usize) i64 {
    return switch (tag) {
        .scalar => a,
        .table => a.t[a.codes[r - a.base]],
        .lane64 => a[r],
        .lane32 => a[r],
        .tmp => tmp[a * window_rows + (r - w0)],
    };
}

/// One per-row node over rows `[w0, w0 + out.len)`, wrapping: the tile's proof holds every value in range.
fn evalLane(op: expr_ast.Op, l: Access, r: Access, lf: i64, rf: i64, tmp: []const i64, out: []i64, w0: usize) void {
    switch (op) {
        inline .add, .sub, .mul => |o| switch (l) {
            inline else => |lv, lt| switch (r) {
                inline else => |rv, rt| for (out, w0..) |*dst, row| {
                    const x = get(lt, lv, tmp, row, w0);
                    const y = get(rt, rv, tmp, row, w0);
                    dst.* = switch (o) {
                        .add => (x *% lf) +% (y *% rf),
                        .sub => (x *% lf) -% (y *% rf),
                        .mul => x *% y,
                        else => comptime unreachable,
                    };
                },
            },
        },
        else => unreachable,
    }
}

/// Add a root's selected values over rows `[w0, w0 + wn)` into its interleaved partials; `valid` (when `masked`)
/// drops rows with a null input and `cnt` counts the rows kept.
fn foldRoot(
    comptime masked: bool,
    a: Access,
    tmp: []const i64,
    part: []i64,
    cnt: []u64,
    valid: []const u1,
    comp: []const u32,
    comp_base: usize,
    sel: *const Sel,
    w0: usize,
    wn: usize,
) void {
    switch (a) {
        inline else => |av, tag| {
            var w = w0 / 64;
            while (w * 64 < w0 + wn) : (w += 1) {
                const word = sel.mask[w];
                if (word == 0) continue;
                const lo = w * 64;
                for (lo..@min(lo + 64, w0 + wn)) |r| {
                    var keep: u1 = @truncate(word >> @intCast(r % 64));
                    if (masked) keep &= valid[r - w0];
                    const slot = @as(usize, comp[r - comp_base]) * lanes + r % lanes;
                    part[slot] +%= get(tag, av, tmp, r, w0) & -%@as(i64, keep);
                    if (masked) cnt[slot] += keep;
                }
            }
        },
    }
}

/// Calls `f(ctx, word, r)` for every row `r < n` of a word of `sel` with any row active, `word` holding `r`'s bit.
inline fn forEachWordRow(sel: *const Sel, n: usize, comptime f: anytype, ctx: anytype) void {
    for (sel.mask, 0..) |word, w| {
        if (word == 0) continue;
        const base = w * 64;
        if (base >= n) break;
        for (base..@min(base + 64, n)) |r| f(ctx, word, r);
    }
}

// ------------------------------------------------------------------------------------------------------------------
// Tests

const testing = std.testing;

const Fx = struct {
    const d152: expr_ast.Dec = .{ .precision = 15, .scale = 2 };

    fn col(a: std.mem.Allocator, idx: usize, spec: expr_ast.Dec) !*expr_ast.Expr {
        const e = try a.create(expr_ast.Expr);
        e.* = .{ .col_ref = .{ .col_idx = idx, .physical_type = .INT64, .expr_type = .dec, .dec = spec } };
        return e;
    }

    fn int(a: std.mem.Allocator, v: i64) !*expr_ast.Expr {
        const e = try a.create(expr_ast.Expr);
        e.* = .{ .literal = .{ .i64 = v } };
        return e;
    }

    fn bin(a: std.mem.Allocator, op: expr_ast.Op, l: *expr_ast.Expr, r: *expr_ast.Expr, spec: expr_ast.Dec) !*expr_ast.Expr {
        const e = try a.create(expr_ast.Expr);
        e.* = .{ .binop = .{ .op = op, .left = l, .right = r, .result_type = .dec, .depth = 2, .dec = spec } };
        return e;
    }

    fn key(idx: usize) expr_ast.Expr {
        return .{ .col_ref = .{ .col_idx = idx, .physical_type = .BYTE_ARRAY, .expr_type = .str } };
    }

    /// A coded DECIMAL column the slice reader would give: codes, a scaled dictionary, the exact one beside it.
    fn coded(a: std.mem.Allocator, dict: []const i64, codes: []const u32, def: ?[]const u32) !Column {
        const scaled = try a.alloc(f64, dict.len);
        for (scaled, dict) |*s, d| s.* = @as(f64, @floatFromInt(d)) / 100;
        var c: filter_eval.ColumnT(f64) = .{ .values = &.{}, .codes = codes, .dict = scaled, .def_levels = def };
        c.max_def = 1;
        c.has_nulls = def != null;
        c.dict_unscaled = .{ .i64 = dict };
        return .{ .f64 = c };
    }

    fn plain(vals: []const i64, def: ?[]const u32) Column {
        return .{ .i64 = .{
            .values = vals,
            .unscaled = .{ .i64 = vals },
            .def_levels = def,
            .max_def = 1,
            .has_nulls = def != null,
        } };
    }

    fn strings(codes: []const u32, dict: []const []const u8, def: ?[]const u32) Column {
        return .{ .string = .{
            .values = &.{},
            .codes = codes,
            .dict = dict,
            .def_levels = def,
            .max_def = 1,
            .has_nulls = def != null,
        } };
    }

    /// The established path's per-composite sums for each root over the whole batch: the oracle.
    fn oracle(
        a: std.mem.Allocator,
        x: *const ExactTiles,
        cols: []const Column,
        lookup: []const ?usize,
        comp: []const u32,
        sel: *const Sel,
    ) ![][]agg.DecSum {
        const mat = try a.alloc(Column, cols.len);
        for (cols, mat) |c, *m| m.* = try decodedView(a, c);
        const batch: filter_eval.Batch = .{ .cols = mat, .num_rows = sel.len };
        var plan: expr_eval.ExactPlan = .{};
        const out = try a.alloc([]agg.DecSum, x.root_list.len);
        for (x.root_list, out) |r, *o| {
            const id = (try plan.add(a, &batch, lookup, x.nodes[r].expr)).?;
            o.* = try agg_coded.exactRootSums(a, &plan, id, x.space, comp, &batch, lookup, sel);
        }
        return out;
    }
};

test "tiles fold the established exact sums, proven or forced wide, across nulls and a dictionary fallback" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    defer forced_limit = null;
    const n = 3000;
    const tile = 1024;
    // Leaves: 0 k0, 1 k1, 2 quantity, 3 price, 4 discount, 5 tax.
    const k0_dict = [_][]const u8{ "A", "N", "R" };
    const k1_dict = [_][]const u8{ "F", "O" };
    var q_dict: [50]i64 = undefined;
    for (&q_dict, 0..) |*v, i| v.* = @intCast(100 * (i + 1));
    var d_dict: [11]i64 = undefined;
    for (&d_dict, 0..) |*v, i| v.* = @intCast(i);
    var t_dict: [9]i64 = undefined;
    for (&t_dict, 0..) |*v, i| v.* = @intCast(i);
    const k0 = try a.alloc(u32, n);
    const k1 = try a.alloc(u32, n);
    const k1_def = try a.alloc(u32, n);
    const qc = try a.alloc(u32, n);
    const price = try a.alloc(i64, n);
    const p_def = try a.alloc(u32, n);
    const dc = try a.alloc(u32, n);
    const d_def = try a.alloc(u32, n);
    const tc = try a.alloc(u32, n);
    const t_vals = try a.alloc(i64, n);
    for (0..n) |i| {
        k0[i] = @intCast((i * 7 + i / 5) % 3);
        k1_def[i] = @intFromBool(i % 13 != 4);
        k1[i] = if (k1_def[i] == 1) @intCast((i / 3) % 2) else 0;
        qc[i] = @intCast((i * 31) % 50);
        p_def[i] = @intFromBool(i % 17 != 3);
        price[i] = if (p_def[i] == 1) @intCast(90091 + (i * 7919) % 10404859) else 0;
        d_def[i] = @intFromBool(i % 19 != 5);
        dc[i] = if (d_def[i] == 1) @intCast((i * 5) % 11) else 0;
        tc[i] = @intCast((i * 3) % 9);
        t_vals[i] = t_dict[tc[i]];
    }
    const whole = [_]Column{
        Fx.strings(k0, &k0_dict, null),
        Fx.strings(k1, &k1_dict, k1_def),
        try Fx.coded(a, &q_dict, qc, null),
        Fx.plain(price, p_def),
        try Fx.coded(a, &d_dict, dc, d_def),
        try Fx.coded(a, &t_dict, tc, null),
    };
    const lookup = [_]?usize{ 0, 1, 2, 3, 4, 5 };
    // q1's trees: 1 - d and 1 + t are DECIMAL(16,2); the products add scales.
    const q = try Fx.col(a, 2, Fx.d152);
    const p = try Fx.col(a, 3, Fx.d152);
    const d = try Fx.col(a, 4, Fx.d152);
    const t = try Fx.col(a, 5, Fx.d152);
    const one_d = try Fx.bin(a, .sub, try Fx.int(a, 1), d, .{ .precision = 16, .scale = 2 });
    const one_t = try Fx.bin(a, .add, try Fx.int(a, 1), t, .{ .precision = 16, .scale = 2 });
    const disc = try Fx.bin(a, .mul, p, one_d, .{ .precision = 31, .scale = 4 });
    const charge = try Fx.bin(a, .mul, disc, one_t, .{ .precision = 38, .scale = 6 });
    const calls = [_]AggCall{
        .{ .func = .sum, .arg = q.*, .result = .dec, .where = null, .alias = "sq" },
        .{ .func = .sum, .arg = p.*, .result = .dec, .where = null, .alias = "sp" },
        .{ .func = .sum, .arg = disc.*, .result = .dec, .where = null, .alias = "sd" },
        .{ .func = .sum, .arg = charge.*, .result = .dec, .where = null, .alias = "sc" },
        .{ .func = .avg, .arg = q.*, .result = .avg_dec, .where = null, .alias = "aq" },
        .{ .func = .avg, .arg = d.*, .result = .avg_dec, .where = null, .alias = "ad" },
        .{ .func = .count, .arg = null, .result = .i64, .where = null, .alias = "n" },
    };
    const keys = [_]expr_ast.Expr{ Fx.key(0), Fx.key(1) };
    try testing.expect(admits(&calls, &keys));
    // Dense, then sparse enough that the tiles walk only the selected rows.
    for ([_]bool{ false, true }) |sparse| for ([_]?u64{ null, 0 }) |limit| {
        var sel = try Sel.init(a, n);
        for (0..n) |i| if (if (sparse) i % 11 != 0 else i % 7 == 0) sel.set(i, false);
        forced_limit = limit;
        counters.reset();
        var x = (try ExactTiles.init(a, &calls, &keys, lookup.len)).?;
        // Roots dedupe: sum(q) and avg(q) share one.
        try testing.expectEqual(@as(usize, 5), x.root_list.len);
        var off: usize = 0;
        while (off < n) : (off += tile) {
            const len = @min(tile, n - off);
            const cols = try a.alloc(Column, whole.len);
            for (whole, cols) |c, *o| o.* = try c.sliceRows(off, len);
            // The second tile's tax pages fell back from the dictionary to PLAIN.
            if (off == tile) cols[5] = Fx.plain(t_vals[off..][0..len], null);
            const w = try sel.alignedWindow(off, len);
            try testing.expectEqual(ExactTiles.Outcome.folded, try x.fold(a, a, cols, &lookup, &w, off, null));
        }
        if (limit == null) {
            try testing.expectEqual(@as(u64, 3), counters.proved.load(.monotonic));
        } else {
            try testing.expectEqual(@as(u64, 3), counters.wide.load(.monotonic));
        }
        const comp = try x.composites(a, &whole, &lookup, n);
        const want = try Fx.oracle(a, &x, &whole, &lookup, comp, &sel);
        var rows: u64 = 0;
        for (want, 0..) |w, ri| for (w, 0..) |s, cp| {
            const got = x.sums[ri * x.space + cp];
            try testing.expectEqual(s.n, got.n);
            try testing.expectEqual(s.get(), got.get());
        };
        for (0..x.space) |cp| rows += x.counts[cp];
        try testing.expectEqual(@as(u64, sel.count()), rows);

        // The groups and their first rows, as the coded fold makes them.
        var table = agg.GroupTable.init(a, 1 << 20);
        try x.finish(a, &table, &.{}, 1000);
        var groups: usize = 0;
        for (x.first_row) |fr| groups += @intFromBool(fr != no_row);
        try testing.expectEqual(groups, table.groupCount());
        var total: u64 = 0;
        for (0..table.groupCount()) |g| total += table.accumulators.items[g * calls.len + 6].count;
        try testing.expectEqual(@as(u64, sel.count()), total);
    };
}

test "a tile whose sums leave i64 folds wide and exactly, cancellation included; a selected overflow abandons" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const n = 2048;
    const d182: expr_ast.Dec = .{ .precision = 18, .scale = 2 };
    const vals = try a.alloc(i64, n);
    // Row values just under 10^18: any 10 of one sign leave i64, so every tile folds wide, and the second half
    // cancels the first. The sum stays near 10^21; the test below takes it past i128.
    for (vals, 0..) |*v, i| v.* = if (i < n / 2) 999_999_999_999_999_999 else -999_999_999_999_999_998;
    const keys = [_]expr_ast.Expr{};
    const p = try Fx.col(a, 0, d182);
    const prod = try Fx.bin(a, .mul, p, try Fx.int(a, 3), .{ .precision = 18, .scale = 2 });
    const calls = [_]AggCall{.{ .func = .sum, .arg = p.*, .result = .dec, .where = null, .alias = "s" }};
    const lookup = [_]?usize{0};
    counters.reset();
    var x = (try ExactTiles.init(a, &calls, &keys, 1)).?;
    var sel = try Sel.init(a, n);
    for ([_]usize{ 0, 1024 }) |off| {
        const cols = [_]Column{Fx.plain(vals[off..][0..1024], null)};
        const w = try sel.alignedWindow(off, 1024);
        try testing.expectEqual(ExactTiles.Outcome.folded, try x.fold(a, a, &cols, &lookup, &w, off, null));
    }
    try testing.expectEqual(@as(u64, 0), counters.proved.load(.monotonic));
    try testing.expectEqual(@as(u64, 2), counters.wide.load(.monotonic));
    try testing.expectEqual(@as(i192, n / 2), x.sums[0].get());
    try testing.expectEqual(@as(u64, n), x.sums[0].n);

    // 3·p leaves DECIMAL(18,2) in a selected row: the tile leaves the row group to the established path, which
    // raises that error in its own order. Unselected, the same row is no error and the tile proves.
    const big = [_]Column{Fx.plain(vals[0..64], null)};
    const calls3 = [_]AggCall{.{ .func = .sum, .arg = prod.*, .result = .dec, .where = null, .alias = "s" }};
    var y = (try ExactTiles.init(a, &calls3, &keys, 1)).?;
    var s64 = try Sel.init(a, 64);
    try testing.expectEqual(ExactTiles.Outcome.abandon, try y.fold(a, a, &big, &lookup, &s64, 0, null));
    const small = try a.alloc(i64, 64);
    for (small, 0..) |*v, i| v.* = @intCast(i);
    small[5] = vals[0];
    const mixed = [_]Column{Fx.plain(small, null)};
    var z = (try ExactTiles.init(a, &calls3, &keys, 1)).?;
    s64.set(5, false);
    counters.reset();
    try testing.expectEqual(ExactTiles.Outcome.folded, try z.fold(a, a, &mixed, &lookup, &s64, 0, null));
    try testing.expectEqual(@as(u64, 1), counters.proved.load(.monotonic));
    try testing.expectEqual(@as(i192, 3 * (63 * 64 / 2 - 5)), z.sums[0].get());
}

test "a slice marked free of nulls is summed whole, and a marked null is still dropped" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const n: usize = 64;
    const vals = try a.alloc(i64, n);
    const present = try a.alloc(u32, n);
    const mixed = try a.alloc(u32, n);
    for (vals, present, mixed, 0..) |*v, *p, *m, i| {
        v.* = @intCast(i + 1);
        p.* = 1;
        m.* = 1;
    }
    mixed[4] = 0;
    const marked: Column = .{ .i64 = .{
        .values = vals,
        .unscaled = .{ .i64 = vals },
        .def_levels = present,
        .max_def = 1,
        .has_nulls = false,
    } };
    const with_null: Column = .{ .i64 = .{
        .values = vals,
        .unscaled = .{ .i64 = vals },
        .def_levels = mixed,
        .max_def = 1,
        .has_nulls = true,
    } };
    const d182: expr_ast.Dec = .{ .precision = 18, .scale = 2 };
    const calls = [_]AggCall{
        .{ .func = .sum, .arg = (try Fx.col(a, 0, d182)).*, .result = .dec, .where = null, .alias = "a" },
        .{ .func = .sum, .arg = (try Fx.col(a, 1, d182)).*, .result = .dec, .where = null, .alias = "b" },
    };
    const cols = [_]Column{ marked, with_null };
    const lookup = [_]?usize{ 0, 1 };
    const sel = try Sel.init(a, n);
    var x = (try ExactTiles.init(a, &calls, &.{}, lookup.len)).?;
    try testing.expectEqual(ExactTiles.Outcome.folded, try x.fold(a, a, &cols, &lookup, &sel, 0, null));
    // 1+…+64, and the same without 5.
    try testing.expectEqual(@as(i192, 64 * 65 / 2), x.sums[0].get());
    try testing.expectEqual(@as(u64, n), x.sums[0].n);
    try testing.expectEqual(@as(i192, 64 * 65 / 2 - 5), x.sums[1].get());
    try testing.expectEqual(@as(u64, n - 1), x.sums[1].n);
}

test "wide tiles carry a sum past i128 and back: 192-bit partials, the established fold and the exact answer agree" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const half = 100_000;
    const n = 2 * half;
    const tile = 1024;
    const x: i64 = 999_999_999_999_999_999;
    // 100,000 products of x·x, then 100,000 of -(x - 1)·x: the running sum reaches 10^41, past i128's 1.7·10^38,
    // inside a tile and across them, before the second half cancels it to 100,000·x.
    try testing.expect(@as(i192, half) * x * x > std.math.maxInt(i128));
    const lhs = try a.alloc(i64, n);
    const rhs = try a.alloc(i64, n);
    for (lhs, rhs, 0..) |*l, *r, i| {
        l.* = if (i < half) x else -(x - 1);
        r.* = x;
    }
    const d180: expr_ast.Dec = .{ .precision = 18, .scale = 0 };
    const prod = try Fx.bin(a, .mul, try Fx.col(a, 0, d180), try Fx.col(a, 1, d180), .{ .precision = 36, .scale = 0 });
    const calls = [_]AggCall{
        .{ .func = .sum, .arg = prod.*, .result = .dec, .where = null, .alias = "s" },
        .{ .func = .avg, .arg = prod.*, .result = .avg_dec, .where = null, .alias = "m" },
        .{ .func = .count, .arg = null, .result = .i64, .where = null, .alias = "n" },
    };
    const whole = [_]Column{ Fx.plain(lhs, null), Fx.plain(rhs, null) };
    const lookup = [_]?usize{ 0, 1 };
    const sel = try Sel.init(a, n);
    counters.reset();
    var t = (try ExactTiles.init(a, &calls, &.{}, lookup.len)).?;
    var off: usize = 0;
    while (off < n) : (off += tile) {
        const len = @min(tile, n - off);
        const cols = [_]Column{ try whole[0].sliceRows(off, len), try whole[1].sliceRows(off, len) };
        const w = try sel.alignedWindow(off, len);
        try testing.expectEqual(ExactTiles.Outcome.folded, try t.fold(a, a, &cols, &lookup, &w, off, null));
    }
    try testing.expectEqual(@as(u64, 0), counters.proved.load(.monotonic));
    try testing.expectEqual(@as(u64, (n + tile - 1) / tile), counters.wide.load(.monotonic));

    // The established whole-batch fold, and the answer computed without either.
    const comp = try t.composites(a, &whole, &lookup, n);
    const want = try Fx.oracle(a, &t, &whole, &lookup, comp, &sel);
    try testing.expectEqual(want[0][0].get(), t.sums[0].get());
    try testing.expectEqual(want[0][0].n, t.sums[0].n);
    const exact: i192 = @as(i192, half) * x * x - @as(i192, half) * (x - 1) * x;
    try testing.expectEqual(@as(i192, 99_999_999_999_999_999_900_000), exact);
    try testing.expectEqual(exact, t.sums[0].get());

    var accs: [calls.len]agg.Accumulator = undefined;
    for (&accs, calls) |*acc, call| acc.* = agg.Accumulator.init(call);
    try t.finish(a, null, &accs, 0);
    try testing.expectEqual(@as(?i128, 99_999_999_999_999_999_900_000), try accs[0].sum_d.result());
    // avg over DECIMAL(36,0) is DECIMAL(38,2): 499999999999999999.50.
    try testing.expectEqual(@as(?i128, 49_999_999_999_999_999_950), try agg.avgDecMean(calls[1], accs[1].avg_d));
    try testing.expectEqual(@as(u64, n), accs[2].count);
}

test "other DECIMAL shapes stay on the established path" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const p = try Fx.col(a, 0, Fx.d152);
    const sum_p: AggCall = .{ .func = .sum, .arg = p.*, .result = .dec, .where = null, .alias = "s" };
    try testing.expect(admits(&.{sum_p}, &.{}));
    const flba: expr_ast.Expr = .{ .col_ref = .{
        .col_idx = 1,
        .physical_type = .FIXED_LEN_BYTE_ARRAY,
        .expr_type = .dec,
        .dec = .{ .precision = 38, .scale = 2 },
    } };
    var bad = sum_p;
    bad.arg = flba;
    try testing.expect(!admits(&.{bad}, &.{}));
    var minp = sum_p;
    minp.func = .min;
    try testing.expect(!admits(&.{minp}, &.{}));
    var filtered = sum_p;
    filtered.where = .{ .and_filter = .{ .children = &.{} } };
    try testing.expect(!admits(&.{filtered}, &.{}));
    var count_p = sum_p;
    count_p.func = .count;
    try testing.expect(!admits(&.{count_p}, &.{}));
    const quotient = try Fx.bin(a, .div, p, p, .{ .precision = 38, .scale = 6 });
    var div = sum_p;
    div.arg = quotient.*;
    try testing.expect(!admits(&.{div}, &.{}));
    const float: expr_ast.Expr = .{ .col_ref = .{ .col_idx = 2, .physical_type = .DOUBLE, .expr_type = .f64 } };
    var fsum = sum_p;
    fsum.arg = float;
    fsum.result = .f64;
    try testing.expect(!admits(&.{fsum}, &.{}));
    // A DECIMAL group key keeps its scaled view.
    try testing.expect(!admits(&.{sum_p}, &.{p.*}));
    // Count alone has no exact root to fold.
    const count_star: AggCall = .{ .func = .count, .arg = null, .result = .i64, .where = null, .alias = "n" };
    try testing.expect(!admits(&.{count_star}, &.{}));
}

test "an all-NULL coded chunk, with no dictionary entries, folds to no values" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const n = 128;
    const codes = try a.alloc(u32, n);
    @memset(codes, 0);
    const def = try a.alloc(u32, n);
    @memset(def, 0);
    const cols = [_]Column{try Fx.coded(a, &.{}, codes, def)};
    const d = try Fx.col(a, 0, Fx.d152);
    const prod = try Fx.bin(a, .mul, d, try Fx.int(a, 3), .{ .precision = 16, .scale = 2 });
    const calls = [_]AggCall{
        .{ .func = .sum, .arg = d.*, .result = .dec, .where = null, .alias = "s" },
        .{ .func = .avg, .arg = prod.*, .result = .avg_dec, .where = null, .alias = "a" },
        .{ .func = .count, .arg = null, .result = .i64, .where = null, .alias = "n" },
    };
    var x = (try ExactTiles.init(a, &calls, &.{}, 1)).?;
    var sel = try Sel.init(a, n);
    try testing.expectEqual(ExactTiles.Outcome.folded, try x.fold(a, a, &cols, &.{0}, &sel, 0, null));
    for (x.sums) |s| try testing.expectEqual(@as(u64, 0), s.n);
    try testing.expectEqual(@as(u64, n), x.counts[0]);
}

test "packed dictionary codes unpack inside the fold, and a failed proof drains them once onto the wide path" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    defer forced_limit = null;
    const n = 2000;
    const tile = 1024;
    const k0_dict = [_][]const u8{ "A", "N", "R" };
    const k1_dict = [_][]const u8{ "F", "O" };
    var q_dict: [50]i64 = undefined;
    for (&q_dict, 0..) |*v, i| v.* = @intCast(100 * (i + 1));
    var d_dict: [11]i64 = undefined;
    for (&d_dict, 0..) |*v, i| v.* = @intCast(i);
    const k0c = try a.alloc(u32, n);
    const k1c = try a.alloc(u32, n);
    const qc = try a.alloc(u32, n);
    const q_plain = try a.alloc(i64, n);
    const price = try a.alloc(i64, n);
    const dc = try a.alloc(u32, n);
    for (0..n) |i| {
        k0c[i] = @intCast(i % 3);
        k1c[i] = @intCast(i % 2);
        qc[i] = @intCast(i % 50);
        q_plain[i] = q_dict[qc[i]];
        price[i] = @intCast(1000 + i % 100);
        dc[i] = @intCast(i % 11);
    }
    const Cursor = struct {
        codes: []const u32,
        at: usize = 0,
        fn pull(ctx: *anyopaque, out: []u32) anyerror!void {
            const s: *@This() = @ptrCast(@alignCast(ctx));
            if (s.at + out.len > s.codes.len) return error.ShortDecode;
            @memcpy(out, s.codes[s.at..][0..out.len]);
            s.at += out.len;
        }
    };
    var k0_cur: Cursor = .{ .codes = k0c };
    var q_cur: Cursor = .{ .codes = qc };
    const q = try Fx.col(a, 2, Fx.d152);
    const p = try Fx.col(a, 3, Fx.d152);
    const d = try Fx.col(a, 4, Fx.d152);
    const one_d = try Fx.bin(a, .sub, try Fx.int(a, 1), d, .{ .precision = 16, .scale = 2 });
    const disc = try Fx.bin(a, .mul, p, one_d, .{ .precision = 31, .scale = 4 });
    const calls = [_]AggCall{
        .{ .func = .sum, .arg = q.*, .result = .dec, .where = null, .alias = "sq" },
        .{ .func = .sum, .arg = disc.*, .result = .dec, .where = null, .alias = "sd" },
        .{ .func = .count, .arg = null, .result = .i64, .where = null, .alias = "n" },
    };
    const keys = [_]expr_ast.Expr{ Fx.key(0), Fx.key(1) };
    const lookup = [_]?usize{ 0, 1, 2, 3, 4 };
    const buffered = [_]Column{
        Fx.strings(k0c, &k0_dict, null),
        Fx.strings(k1c, &k1_dict, null),
        try Fx.coded(a, &q_dict, qc, null),
        Fx.plain(price, null),
        try Fx.coded(a, &d_dict, dc, null),
    };
    // Packed views share the dictionaries and carry no code buffer: the fold unpacks them.
    var packed_key = buffered[0];
    packed_key.string.codes = null;
    var packed_q = buffered[2];
    packed_q.f64.codes = null;
    counters.reset();
    var x = (try ExactTiles.init(a, &calls, &keys, lookup.len)).?;
    var sel = try Sel.init(a, n);
    for (0..n) |i| if (i % 17 == 0) sel.set(i, false);
    var off: usize = 0;
    while (off < n) : (off += tile) {
        const len = @min(tile, n - off);
        const cols = try a.alloc(Column, buffered.len);
        cols[0] = packed_key;
        cols[1] = try buffered[1].sliceRows(off, len);
        cols[3] = try buffered[3].sliceRows(off, len);
        cols[4] = try buffered[4].sliceRows(off, len);
        var pulls = [_]?CodePull{
            .{ .ctx = &k0_cur, .pull = Cursor.pull, .rows = len },
            null,
            .{ .ctx = &q_cur, .pull = Cursor.pull, .rows = len },
            null,
            null,
        };
        if (off == 0) {
            cols[2] = packed_q;
        } else {
            // Dictionary page ended; the next page is PLAIN values, already decoded.
            cols[2] = Fx.plain(q_plain[off..][0..len], null);
            pulls[2] = null;
        }
        const w = try sel.alignedWindow(off, len);
        try testing.expectEqual(ExactTiles.Outcome.folded, try x.fold(a, a, cols, &lookup, &w, off, &pulls));
    }
    try testing.expectEqual(@as(usize, n), k0_cur.at);
    try testing.expectEqual(@as(usize, tile), q_cur.at);
    try testing.expectEqual(@as(u64, 2), counters.proved.load(.monotonic));
    try testing.expectEqual(@as(u64, 0), counters.wide.load(.monotonic));
    const comp = try x.composites(a, &buffered, &lookup, n);
    const want = try Fx.oracle(a, &x, &buffered, &lookup, comp, &sel);
    for (want, 0..) |w, ri| for (w, 0..) |s, cp| {
        const got = x.sums[ri * x.space + cp];
        try testing.expectEqual(s.n, got.n);
        try testing.expectEqual(s.get(), got.get());
    };

    // A bound of 0 fails the proof before any code is pulled, then the drain feeds the wide path once.
    forced_limit = 0;
    k0_cur.at = 0;
    q_cur.at = 0;
    counters.reset();
    var y = (try ExactTiles.init(a, &calls, &keys, lookup.len)).?;
    const cols0 = [_]Column{
        packed_key,
        try buffered[1].sliceRows(0, tile),
        packed_q,
        try buffered[3].sliceRows(0, tile),
        try buffered[4].sliceRows(0, tile),
    };
    const pulls0 = [_]?CodePull{
        .{ .ctx = &k0_cur, .pull = Cursor.pull, .rows = tile },
        null,
        .{ .ctx = &q_cur, .pull = Cursor.pull, .rows = tile },
        null,
        null,
    };
    const w0 = try sel.alignedWindow(0, tile);
    try testing.expectEqual(ExactTiles.Outcome.folded, try y.fold(a, a, &cols0, &lookup, &w0, 0, &pulls0));
    try testing.expectEqual(@as(u64, 1), counters.wide.load(.monotonic));
    try testing.expectEqual(@as(u64, 0), counters.proved.load(.monotonic));
    try testing.expectEqual(@as(usize, tile), k0_cur.at);
    try testing.expectEqual(@as(usize, tile), q_cur.at);
    const tile_sel = try sel.alignedWindow(0, tile);
    const oracle_cols = [_]Column{
        Fx.strings(k0c[0..tile], &k0_dict, null),
        Fx.strings(k1c[0..tile], &k1_dict, null),
        try Fx.coded(a, &q_dict, qc[0..tile], null),
        Fx.plain(price[0..tile], null),
        try buffered[4].sliceRows(0, tile),
    };
    const ycomp = try y.composites(a, &oracle_cols, &lookup, tile);
    const ywant = try Fx.oracle(a, &y, &oracle_cols, &lookup, ycomp, &tile_sel);
    for (ywant, 0..) |w, ri| for (w, 0..) |s, cp| {
        const got = y.sums[ri * y.space + cp];
        try testing.expectEqual(s.n, got.n);
        try testing.expectEqual(s.get(), got.get());
    };
}
