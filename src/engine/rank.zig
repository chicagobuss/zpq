const ENCODE_REGION_RESERVE = @import("write_window.zig").ENCODE_REGION_RESERVE;
const filter_prune = @import("../core/filter/prune.zig");
const filter_selection = @import("../core/filter/selection.zig");
const huge_pages = @import("../core/huge_pages.zig");
const spawn_util = @import("../core/spawn.zig");
const decimal_mod = @import("../core/parquet/decimal.zig");
const s3FetchWindow = @import("s3_fetch.zig").s3FetchWindow;
const system = @import("../core/system.zig");
const nowMonoNs = @import("../clock.zig").monoNs;
const profile = @import("../profile.zig");
const work_cursor = @import("../io/work_cursor.zig");
const filter_parser = @import("../core/filter/parser.zig");
const filter_eval = @import("../core/filter/eval.zig");
const engine_types = @import("types.zig");
const Context = engine_types.Context;
const QueryArgs = engine_types.QueryArgs;
const Timings = engine_types.Timings;
const envDisables = @import("env.zig").envDisables;
const std = @import("std");
const schema = @import("../core/schema.zig");
const metadata = @import("../core/parquet/metadata.zig");
const statistics = @import("../core/parquet/statistics.zig");
const consumer = @import("../core/consumer.zig");
const scan = @import("../core/scan.zig");
const core_memory = @import("../core/memory.zig");
const filter_ast = @import("../core/filter/ast.zig");

pub const RowSortKey = engine_types.RowSortKey;

/// One sort key value of a row. NULL sorts last in either direction.
pub const SortCell = union(enum) {
    null,
    int: i128,
    float: f64,
    str: []const u8,

    fn order(a: SortCell, b: SortCell) std.math.Order {
        if (a == .null or b == .null) return if (a == .null and b == .null) .eq else if (a == .null) .gt else .lt;
        return switch (a) {
            .int => |x| std.math.order(x, b.int),
            .float => |x| floatOrder(x, b.float),
            .str => |x| std.mem.order(u8, x, b.str),
            .null => unreachable,
        };
    }

    fn floatOrder(a: f64, b: f64) std.math.Order {
        if (std.math.isNan(a) or std.math.isNan(b)) {
            return if (std.math.isNan(a) == std.math.isNan(b)) .eq else if (std.math.isNan(a)) .gt else .lt;
        }
        return std.math.order(a, b);
    }
};

/// Key `k`'s value in row `row` of a buffer holding the key columns, borrowed from it.
pub fn sortCell(buf: consumer.OutputAggregator.ColumnBuf, row: usize, unsigned: bool) SortCell {
    switch (buf) {
        inline else => |tb, tag| {
            if (tb.def_levels.items.len > 0 and tb.def_levels.items[row] < tb.max_def) return .null;
            const v = tb.values.items[row];
            return switch (tag) {
                .i32 => .{ .int = if (unsigned) @as(u32, @bitCast(v)) else v },
                .i64 => .{ .int = if (unsigned) @as(u64, @bitCast(v)) else v },
                .i128 => .{ .int = v },
                .boolean => .{ .int = @intFromBool(v) },
                .f32, .f64 => .{ .float = v },
                .string => .{ .str = v },
            };
        },
    }
}

/// Where a row stands in the input, for ties: file, row group, and its place among the row group's filtered rows.
pub const RowPos = struct {
    file: u32,
    rg: u32,
    ord: u32 = 0,

    pub fn order(a: RowPos, b: RowPos) std.math.Order {
        if (a.file != b.file) return std.math.order(a.file, b.file);
        if (a.rg != b.rg) return std.math.order(a.rg, b.rg);
        return std.math.order(a.ord, b.ord);
    }
};

/// Keeps the first `h` (OFFSET + LIMIT) plain rows by a SQL ORDER BY across row groups, ties by input position, as a
/// stable sort of every row would. A row group's filter and key columns decode first; only a row group holding a row
/// that enters the heap decodes its output columns, and those rows are copied out, owned, before it is released. The
/// heap's top is the last row kept, so a row that sorts before it replaces it; one that ties it comes later in the
/// input and does not.
pub const TopRows = struct {
    gpa: std.mem.Allocator,
    meta: *const schema.FileMetaData,
    specs: []const consumer.OutputCol,
    keys: []const RowSortKey,
    key_specs: []consumer.OutputCol,
    h: usize,
    /// Bytes the kept rows and their heap may take, half of `limit` (or of `decode_floor`: under a tiny budget the
    /// heap's layout, which decides the store's capacity, decided the answer); past it the query fails cleanly.
    budget: usize,
    /// The query's budget, `--max-memory`: what the kept rows and a row group's decode hold together.
    limit: usize,
    /// The live account of that budget: the store, the heap, a compaction's copy and a row group's decode all draw
    /// from it before they allocate. Kept off the struct, which is moved after `init`.
    mem: *Mem,
    /// The rows admitted so far, their output columns copied; a heap entry indexes one. Rows evicted stay until
    /// `compact` drops them.
    store_arena: *std.heap.ArenaAllocator,
    store: consumer.OutputAggregator,
    heap: std.PriorityQueue(Entry, *const TopRows, later),

    const pending = std.math.maxInt(usize);
    /// What the account holds at least, a fixed slack: under a tiny budget a few bytes of noise decided the answer.
    const decode_floor: usize = 1 << 20;

    const Mem = struct {
        ledger: core_memory.SharedBudget,
        charged: core_memory.Charged,

        fn allocator(m: *Mem) std.mem.Allocator {
            return m.charged.allocator();
        }
    };

    const Entry = struct {
        cells: []SortCell,
        pos: RowPos,
        /// Its row in `store`, or `pending` until its row group's output columns are copied.
        row: usize,
        /// Per cell, the bytes its string's block holds, which a newcomer reusing it may fill (its own key may be
        /// shorter). Empty: each string's length.
        caps: []usize = &.{},
    };

    pub fn init(
        gpa: std.mem.Allocator,
        meta: *const schema.FileMetaData,
        specs: []const consumer.OutputCol,
        keys: []const RowSortKey,
        h: usize,
        limit: usize,
    ) !TopRows {
        const key_specs = try gpa.alloc(consumer.OutputCol, keys.len);
        errdefer gpa.free(key_specs);
        for (keys, key_specs) |k, *spec| spec.* = k.spec;
        const mem = try gpa.create(Mem);
        errdefer gpa.destroy(mem);
        mem.* = .{ .ledger = .{ .limit = @max(limit, decode_floor) }, .charged = undefined };
        mem.charged = .{ .child = gpa, .budget = &mem.ledger };
        const store_arena = try gpa.create(std.heap.ArenaAllocator);
        store_arena.* = .init(mem.allocator());
        errdefer {
            store_arena.deinit();
            gpa.destroy(store_arena);
        }
        return .{
            .gpa = gpa,
            .meta = meta,
            .specs = specs,
            .keys = keys,
            .key_specs = key_specs,
            .h = h,
            .budget = @max(limit, decode_floor) / 2,
            .limit = limit,
            .mem = mem,
            .store_arena = store_arena,
            .store = try consumer.initOutputAggregator(store_arena.allocator(), meta, specs),
            .heap = .empty,
        };
    }

    pub fn deinit(self: *TopRows) void {
        self.heap.deinit(self.mem.allocator());
        self.store_arena.deinit();
        self.gpa.destroy(self.store_arena);
        self.gpa.destroy(self.mem);
        self.gpa.free(self.key_specs);
    }

    /// Whether row (`a_cells`, `a_pos`) sorts before row (`b_cells`, `b_pos`). NULL is last in either direction.
    pub fn before(keys: []const RowSortKey, a_cells: []const SortCell, a_pos: RowPos, b_cells: []const SortCell, b_pos: RowPos) bool {
        for (keys, a_cells, b_cells) |k, a, b| {
            var ord = a.order(b);
            if (k.descending and a != .null and b != .null) ord = ord.invert();
            if (ord != .eq) return ord == .lt;
        }
        return a_pos.order(b_pos) == .lt;
    }

    fn later(self: *const TopRows, a: Entry, b: Entry) std.math.Order {
        if (before(self.keys, b.cells, b.pos, a.cells, a.pos)) return .lt;
        if (before(self.keys, a.cells, a.pos, b.cells, b.pos)) return .gt;
        return .eq;
    }

    /// Offer one row group's filtered rows.
    pub fn offer(
        self: *TopRows,
        rg: *const schema.RowGroup,
        meta: *const schema.FileMetaData,
        src: consumer.RGSrc,
        filter: ?filter_ast.Filter,
        fetch: []const bool,
        at: RowPos,
        timings: *consumer.Timings,
        options: consumer.DecodeOptions,
    ) !void {
        if (self.h == 0) return;
        // The row group decodes on the same account as the rows it keeps, each drawn before it is allocated: what does
        // not fit refuses there, not after it was resident.
        return self.offerOn(self.mem.allocator(), rg, meta, src, filter, fetch, at, timings, options) catch |e|
            self.mem.charged.refusal(e);
    }

    fn offerOn(
        self: *TopRows,
        dg: std.mem.Allocator,
        rg: *const schema.RowGroup,
        meta: *const schema.FileMetaData,
        src: consumer.RGSrc,
        filter: ?filter_ast.Filter,
        fetch: []const bool,
        at: RowPos,
        timings: *consumer.Timings,
        options: consumer.DecodeOptions,
    ) !void {
        var rg_arena = std.heap.ArenaAllocator.init(dg);
        defer rg_arena.deinit();
        const ra = rg_arena.allocator();
        var keyed = try consumer.initOutputAggregator(ra, meta, self.key_specs);
        const n = try consumer.appendProjectedRGWithOptions(
            &keyed,
            dg,
            rg,
            meta,
            src,
            filter,
            fetch,
            self.key_specs,
            timings,
            options,
        );
        timings.rows_scanned += @intCast(rg.num_rows);
        timings.rows_matched += n;
        if (n == 0) return;

        self.heap.context = self;
        const scratch = try ra.alloc(SortCell, self.keys.len);
        var admitted = false;
        const sa = self.store_arena.allocator();
        for (0..n) |i| {
            for (self.keys, keyed.cols, scratch) |k, buf, *cell| cell.* = sortCell(buf, i, k.unsigned);
            const pos: RowPos = .{ .file = at.file, .rg = at.rg, .ord = @intCast(i) };
            // An evicted row's keys are the newcomer's: its cells, and its strings where the new ones fit, so input
            // that improves on every row (descending, for an ascending key) holds one copy per kept row, not per row.
            var cells: []SortCell = &.{};
            var caps: []usize = &.{};
            if (self.heap.count() == self.h) {
                const top = self.heap.peek().?;
                if (!before(self.keys, scratch, pos, top.cells, top.pos)) continue;
                const out = self.heap.pop().?;
                cells = out.cells;
                const fresh_caps = out.caps.len != cells.len;
                caps = if (fresh_caps) try sa.alloc(usize, cells.len) else out.caps;
                for (scratch, cells, caps) |c, *d, *cap| {
                    if (fresh_caps) cap.* = if (d.* == .str) d.str.len else 0;
                    d.* = try reuseCell(sa, d.*, cap, c);
                }
            } else {
                cells = try sa.alloc(SortCell, self.keys.len);
                caps = try sa.alloc(usize, self.keys.len);
                for (scratch, cells, caps) |c, *d, *cap| {
                    d.* = try ownCell(sa, c);
                    cap.* = if (c == .str) c.str.len else 0;
                }
            }
            try self.heap.push(self.mem.allocator(), .{ .cells = cells, .pos = pos, .row = pending, .caps = caps });
            admitted = true;
        }
        if (!admitted) return;

        // The rows that entered and are still kept: copy their output columns, decoded once for the row group.
        var outputs = try consumer.initOutputAggregator(ra, meta, self.specs);
        const m = try consumer.appendProjectedRGWithOptions(
            &outputs,
            dg,
            rg,
            meta,
            src,
            filter,
            fetch,
            self.specs,
            timings,
            options,
        );
        if (m != n) return error.ShortDecode;
        for (self.heap.items) |*e| if (e.row == pending) {
            try copyRow(&self.store, &outputs, e.pos.ord);
            e.row = self.store.num_rows - 1;
        };
        if (self.store.num_rows > 2 *| self.h +| 1024) try self.compact();
        const held = self.storeUsed() + self.heap.cap * @sizeOf(Entry);
        if (held > self.budget) return error.ExceededMemoryBudget;
    }

    /// Bytes the store's arena has handed out: its nodes' used ends, not the tails a node reserves half again past
    /// them (counted, a few bytes more per row moved the answer's threshold by a node). The account holds the rest.
    fn storeUsed(self: *const TopRows) usize {
        var used: usize = 0;
        var it = self.store_arena.state.used_list;
        while (it) |node| : (it = node.next) {
            // The low bit of a node's size marks a resize in flight; sizes are even.
            const size = @as(usize, @bitCast(node.size)) & ~@as(usize, 1);
            used += @min(node.end_index, size - @sizeOf(@TypeOf(node.*)));
        }
        return used;
    }

    /// Whether a row group whose rows' first key is at best `best` holds no row the heap would keep: the heap is full
    /// and `best` sorts strictly after its last row's first key, so every row of the group sorts after that row
    /// whatever its later keys and position. A tie keeps the group.
    pub fn cutsOff(self: *const TopRows, best: SortCell) bool {
        if (self.h == 0 or self.heap.count() < self.h) return false;
        return sortsAfter(self.keys[0], best, self.heap.peek().?.cells[0]);
    }

    fn ownCell(arena: std.mem.Allocator, c: SortCell) !SortCell {
        return if (c == .str) .{ .str = try arena.dupe(u8, c.str) } else c;
    }

    /// `c` owned in place of `old`, an evicted row's cell whose string's block holds `cap.*` bytes: into that block
    /// when it fits (keys whose lengths alternate kept shrinking it), else copied anew, `cap` becoming its size.
    fn reuseCell(arena: std.mem.Allocator, old: SortCell, cap: *usize, c: SortCell) !SortCell {
        if (c != .str) return c;
        if (old == .str and cap.* >= c.str.len) {
            const into = @constCast(old.str.ptr)[0..c.str.len];
            @memcpy(into, c.str);
            return .{ .str = into };
        }
        const owned = try ownCell(arena, c);
        cap.* = c.str.len;
        return owned;
    }

    /// Rebuild `store` with only the rows the heap still keeps.
    fn compact(self: *TopRows) !void {
        const fresh = try self.gpa.create(std.heap.ArenaAllocator);
        fresh.* = .init(self.mem.allocator());
        errdefer {
            fresh.deinit();
            self.gpa.destroy(fresh);
        }
        const fa = fresh.allocator();
        var store = try consumer.initOutputAggregator(fa, self.meta, self.specs);
        const cells = try fa.alloc([]SortCell, self.heap.items.len);
        const caps = try fa.alloc([]usize, self.heap.items.len);
        for (self.heap.items, cells, caps) |e, *dst, *cap| {
            try copyRow(&store, &self.store, e.row);
            dst.* = try fa.alloc(SortCell, e.cells.len);
            cap.* = try fa.alloc(usize, e.cells.len);
            for (e.cells, dst.*, cap.*) |c, *d, *k| {
                d.* = try ownCell(fa, c);
                k.* = if (c == .str) c.str.len else 0;
            }
        }
        // Nothing can fail from here: swap the rows in.
        for (self.heap.items, cells, caps, 0..) |*e, c, k, i| {
            e.cells = c;
            e.caps = k;
            e.row = i;
        }
        self.store_arena.deinit();
        self.gpa.destroy(self.store_arena);
        self.store_arena = fresh;
        self.store = store;
    }

    /// The kept rows in their order, on `arena`.
    pub fn ranked(self: *TopRows, arena: std.mem.Allocator) !consumer.OutputAggregator {
        const entries = try arena.dupe(Entry, self.heap.items);
        std.mem.sortUnstable(Entry, entries, self.keys, struct {
            fn lt(keys: []const RowSortKey, a: Entry, b: Entry) bool {
                return before(keys, a.cells, a.pos, b.cells, b.pos);
            }
        }.lt);
        var out = try consumer.initOutputAggregator(arena, self.meta, self.specs);
        for (entries) |e| try copyRow(&out, &self.store, e.row);
        return out;
    }
};

/// The best first-key value any row of a row group can have, from the column chunk's statistics: its min ascending, its
/// max descending, NULL when every row is NULL. Null when the statistics prove nothing: a computed key, bounds missing,
/// in an order zpq does not read, or of a type whose cell is not plainly the bound. Floats descending prove nothing
/// either: a NaN sorts first there, and writers leave NaN out of the bounds.
fn rowGroupBest(rg: *const schema.RowGroup, meta: *const schema.FileMetaData, key: RowSortKey) ?SortCell {
    const ci = switch (key.spec) {
        .passthrough => |pt| pt.ci,
        .computed => return null,
    };
    if (ci >= rg.columns.items.len) return null;
    const cm = rg.columns.items[ci].meta_data orelse return null;
    const st = statistics.chunkStats(rg, ci, meta, .corroborated);
    const b = st.bounds orelse {
        const nulls = st.null_count orelse return null;
        return if (rg.num_rows > 0 and nulls == rg.num_rows) .null else null;
    };
    const bytes = if (key.descending) b.max else b.min;
    const elem = meta.getColumnSchema(cm.path_in_schema.items) orelse return null;
    if (elem.type == null or elem.type.? != cm.type) return null;
    switch (cm.type) {
        .INT32, .INT64 => {
            // DECIMAL, among others, is neither: its cell is not read from the bound here.
            if (!statistics.signedIntOrder(elem) and !schema.isUnsignedInt(elem)) return null;
            if (cm.type == .INT32) {
                if (bytes.len != 4) return null;
                const v = std.mem.readInt(i32, bytes[0..4], .little);
                return .{ .int = if (key.unsigned) @as(u32, @bitCast(v)) else v };
            }
            if (bytes.len != 8) return null;
            const v = std.mem.readInt(i64, bytes[0..8], .little);
            return .{ .int = if (key.unsigned) @as(u64, @bitCast(v)) else v };
        },
        .FLOAT, .DOUBLE => {
            if (key.descending or elem.logical_type != null or elem.converted_type != null) return null;
            const v: f64 = if (cm.type == .FLOAT) blk: {
                if (bytes.len != 4) return null;
                break :blk @as(f32, @bitCast(std.mem.readInt(u32, bytes[0..4], .little)));
            } else blk: {
                if (bytes.len != 8) return null;
                break :blk @as(f64, @bitCast(std.mem.readInt(u64, bytes[0..8], .little)));
            };
            return if (std.math.isNan(v)) null else .{ .float = v };
        },
        .BYTE_ARRAY => return if (statistics.bytewiseOrder(elem)) .{ .str = bytes } else null,
        else => return null,
    }
}

/// Whether every output column and sort key is a bare input column, which no row can fail to produce.
pub fn allPassthrough(specs: []const consumer.OutputCol, keys: []const RowSortKey) bool {
    // A named leaf is renamed and flattened by the general path; the gather copies leaves as they are.
    for (specs) |spec| if (spec == .computed or spec.passthrough.name != null) return false;
    for (keys) |key| if (key.spec == .computed) return false;
    return true;
}

/// A row group of the plain-row Top-N scan, and the best first key its statistics allow (`rowGroupBest`).
pub const RankedRG = struct {
    file: u32,
    rg: u32,
    best: ?SortCell,

    /// Most promising first, so the heap fills with good rows early and its cutoff rules more row groups out. Row groups
    /// without a bound go first, in input order: they are read whatever the cutoff.
    fn sooner(key: RowSortKey, a: RankedRG, b: RankedRG) bool {
        const x = a.best orelse return b.best != null;
        const y = b.best orelse return false;
        var ord = x.order(y);
        if (key.descending and x != .null and y != .null) ord = ord.invert();
        return ord == .lt;
    }
};

/// Every row group of every input, with its first-key bound. Ordered most promising first when every input's bytes
/// are all in; a gated input is fetched in input order, so there the order stays and only the bounds are used.
pub fn rankRowGroups(
    arena: std.mem.Allocator,
    inputs: []const scan.Input,
    metas: []const schema.FileMetaData,
    key: RowSortKey,
    bounded: bool,
) ![]RankedRG {
    var n: usize = 0;
    for (metas) |*m| n += m.row_groups.items.len;
    const out = try arena.alloc(RankedRG, n);
    var at: usize = 0;
    for (metas, 0..) |*m, fi| for (m.row_groups.items, 0..) |*rg, ri| {
        out[at] = .{ .file = @intCast(fi), .rg = @intCast(ri), .best = if (bounded) rowGroupBest(rg, m, key) else null };
        at += 1;
    };
    if (!bounded) return out;
    for (inputs) |in| if (in.gate != null) return out;
    std.mem.sort(RankedRG, out, key, RankedRG.sooner);
    return out;
}

/// Append row `row` of `src` to `dst`, both built for the same output columns, its strings copied onto `dst`'s arena.
/// A column whose rows so far carried no definition levels gains them, all present, once one row does.
fn copyRow(dst: *consumer.OutputAggregator, src: *const consumer.OutputAggregator, row: usize) !void {
    for (dst.cols, src.cols) |*d, s| switch (d.*) {
        inline else => |*dt, tag| {
            const st = @field(s, @tagName(tag));
            std.debug.assert(!st.coded);
            var v = st.values.items[row];
            if (@TypeOf(v) == []const u8) v = try dst.arena.dupe(u8, v);
            try dt.values.append(dst.arena, v);
            if (st.def_levels.items.len > 0 or dt.def_levels.items.len > 0) {
                dt.max_def = @max(dt.max_def, st.max_def);
                while (dt.def_levels.items.len + 1 < dt.values.items.len) try dt.def_levels.append(dst.arena, dt.max_def);
                const level = if (st.def_levels.items.len > 0) st.def_levels.items[row] else dt.max_def;
                try dt.def_levels.append(dst.arena, level);
            }
        },
    };
    dst.num_rows += 1;
}

/// Whether first key `a` sorts strictly after `b` under `key` (NULL last in either direction).
pub fn sortsAfter(key: RowSortKey, a: SortCell, b: SortCell) bool {
    var ord = a.order(b);
    if (key.descending and a != .null and b != .null) ord = ord.invert();
    return ord == .gt;
}

test "rowGroupBest reads the first key a row group can reach, and only what its statistics prove" {
    const testing = std.testing;
    const fixtures = @import("../core/parquet/test_fixtures.zig");
    var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_state.deinit();
    const a = arena_state.allocator();
    const asc: RowSortKey = .{ .spec = .{ .passthrough = .{ .ci = 0 } }, .descending = false, .unsigned = false };
    const desc: RowSortKey = .{ .spec = .{ .passthrough = .{ .ci = 0 } }, .descending = true, .unsigned = false };
    var lo: [8]u8 = undefined;
    var hi: [8]u8 = undefined;
    std.mem.writeInt(i64, &lo, -7, .little);
    std.mem.writeInt(i64, &hi, 40, .little);

    var c = try fixtures.StatsColumn.init(a, fixtures.statsLeaf(.INT64), .{ .min_value = &lo, .max_value = &hi });
    try testing.expectEqual(@as(i128, -7), rowGroupBest(&c.rg, &c.fm, asc).?.int);
    try testing.expectEqual(@as(i128, 40), rowGroupBest(&c.rg, &c.fm, desc).?.int);
    const computed: RowSortKey = .{ .spec = .{ .computed = .{ .expr = undefined, .alias = "e" } }, .descending = false, .unsigned = false };
    try testing.expect(rowGroupBest(&c.rg, &c.fm, computed) == null);

    // Unsigned: the max's bits are a value above every signed one.
    var uleaf = fixtures.statsLeaf(.INT64);
    uleaf.logical_type = .{ .INTEGER = .{ .bitWidth = 64, .isSigned = false } };
    const all_ones: [8]u8 = @splat(0xff);
    c = try fixtures.StatsColumn.init(a, uleaf, .{ .min_value = &lo, .max_value = &all_ones });
    const udesc: RowSortKey = .{ .spec = .{ .passthrough = .{ .ci = 0 } }, .descending = true, .unsigned = true };
    try testing.expectEqual(@as(i128, std.math.maxInt(u64)), rowGroupBest(&c.rg, &c.fm, udesc).?.int);

    // DECIMAL is not read; a double proves its min ascending, nothing descending, and a NaN bound nothing.
    var dec = fixtures.statsLeaf(.INT64);
    dec.logical_type = .{ .DECIMAL = .{ .scale = 2, .precision = 18 } };
    c = try fixtures.StatsColumn.init(a, dec, .{ .min_value = &lo, .max_value = &hi });
    try testing.expect(rowGroupBest(&c.rg, &c.fm, asc) == null);
    var dlo: [8]u8 = undefined;
    std.mem.writeInt(u64, &dlo, @bitCast(@as(f64, -2.5)), .little);
    c = try fixtures.StatsColumn.init(a, fixtures.statsLeaf(.DOUBLE), .{ .min_value = &dlo, .max_value = &dlo });
    try testing.expectEqual(@as(f64, -2.5), rowGroupBest(&c.rg, &c.fm, asc).?.float);
    try testing.expect(rowGroupBest(&c.rg, &c.fm, desc) == null);
    std.mem.writeInt(u64, &dlo, @bitCast(std.math.nan(f64)), .little);
    c = try fixtures.StatsColumn.init(a, fixtures.statsLeaf(.DOUBLE), .{ .min_value = &dlo, .max_value = &dlo });
    try testing.expect(rowGroupBest(&c.rg, &c.fm, asc) == null);

    // Strings in bytewise order; a group of NULLs only is NULL at best; no statistics prove nothing.
    var sleaf = fixtures.statsLeaf(.BYTE_ARRAY);
    sleaf.logical_type = .{ .STRING = .{} };
    c = try fixtures.StatsColumn.init(a, sleaf, .{ .min_value = "apple", .max_value = "pear" });
    try testing.expectEqualStrings("pear", rowGroupBest(&c.rg, &c.fm, desc).?.str);
    c = try fixtures.StatsColumn.init(a, fixtures.statsLeaf(.INT64), .{ .null_count = 100 });
    try testing.expect(rowGroupBest(&c.rg, &c.fm, asc).? == .null);
    c = try fixtures.StatsColumn.init(a, fixtures.statsLeaf(.INT64), .{ .null_count = 3 });
    try testing.expect(rowGroupBest(&c.rg, &c.fm, asc) == null);
}

test "TopRows.cutsOff passes over a row group only when its best first key sorts strictly after the last row kept" {
    const testing = std.testing;
    inline for (.{ false, true }) |descending| {
        const keys = [_]RowSortKey{.{ .spec = .{ .passthrough = .{ .ci = 0 } }, .descending = descending, .unsigned = false }};
        var top: TopRows = undefined;
        top.keys = &keys;
        top.h = 2;
        top.heap = .empty;
        top.heap.context = &top;
        defer top.heap.deinit(testing.allocator);
        var c5 = [_]SortCell{.{ .int = 5 }};
        var c9 = [_]SortCell{.{ .int = 9 }};
        try top.heap.push(testing.allocator, .{ .cells = &c5, .pos = .{ .file = 0, .rg = 3 }, .row = 0 });
        // Not full yet: nothing is ruled out.
        try testing.expect(!top.cutsOff(.{ .int = 1000 }));
        try top.heap.push(testing.allocator, .{ .cells = &c9, .pos = .{ .file = 0, .rg = 1 }, .row = 1 });
        // The last row kept is 9 ascending, 5 descending. A tie is kept: the group's rows may sit earlier.
        const last: i128 = if (descending) 5 else 9;
        const worse: i128 = if (descending) 4 else 10;
        const better: i128 = if (descending) 6 else 8;
        try testing.expect(top.cutsOff(.{ .int = worse }));
        try testing.expect(!top.cutsOff(.{ .int = last }));
        try testing.expect(!top.cutsOff(.{ .int = better }));
        // NULL sorts last either way.
        try testing.expect(top.cutsOff(.null));
    }
    // Once the last row kept is NULL, no group sorts strictly after it.
    const keys = [_]RowSortKey{.{ .spec = .{ .passthrough = .{ .ci = 0 } }, .descending = false, .unsigned = false }};
    var top: TopRows = undefined;
    top.keys = &keys;
    top.h = 1;
    top.heap = .empty;
    top.heap.context = &top;
    defer top.heap.deinit(testing.allocator);
    var cn = [_]SortCell{.null};
    try top.heap.push(testing.allocator, .{ .cells = &cn, .pos = .{ .file = 0, .rg = 0 }, .row = 0 });
    try testing.expect(!top.cutsOff(.null));
    try testing.expect(!top.cutsOff(.{ .int = 1 }));
}

test "RankedRG.sooner reads unbounded row groups first in input order, then the most promising, NULL last" {
    const testing = std.testing;
    inline for (.{ false, true }) |descending| {
        const key: RowSortKey = .{ .spec = .{ .passthrough = .{ .ci = 0 } }, .descending = descending, .unsigned = false };
        var rgs = [_]RankedRG{
            .{ .file = 0, .rg = 0, .best = .{ .int = 3 } },
            .{ .file = 0, .rg = 1, .best = null },
            .{ .file = 0, .rg = 2, .best = .null },
            .{ .file = 1, .rg = 0, .best = .{ .int = 7 } },
            .{ .file = 1, .rg = 1, .best = null },
            .{ .file = 1, .rg = 2, .best = .{ .int = 3 } },
        };
        std.mem.sort(RankedRG, &rgs, key, RankedRG.sooner);
        const want: [6][2]u32 = if (descending)
            .{ .{ 0, 1 }, .{ 1, 1 }, .{ 1, 0 }, .{ 0, 0 }, .{ 1, 2 }, .{ 0, 2 } }
        else
            .{ .{ 0, 1 }, .{ 1, 1 }, .{ 0, 0 }, .{ 1, 2 }, .{ 1, 0 }, .{ 0, 2 } };
        for (rgs, want) |r, w| {
            try testing.expectEqual(w[0], r.file);
            try testing.expectEqual(w[1], r.rg);
        }
    }
}

test "allPassthrough keeps a computed output or key off the bound-first Top-N" {
    const testing = std.testing;
    const bare = [_]consumer.OutputCol{ .{ .passthrough = .{ .ci = 0 } }, .{ .passthrough = .{ .ci = 3 } } };
    const key: RowSortKey = .{ .spec = .{ .passthrough = .{ .ci = 1 } }, .descending = false, .unsigned = false };
    try testing.expect(allPassthrough(&bare, &.{key}));
    const computed: consumer.OutputCol = .{ .computed = .{ .expr = undefined, .alias = "z" } };
    try testing.expect(!allPassthrough(&.{ bare[0], computed }, &.{key}));
    try testing.expect(!allPassthrough(&bare, &.{ key, .{ .spec = computed, .descending = true, .unsigned = false } }));
}

test "the serial Top-N keeps one key copy per kept row when every row evicts the last, within its budget" {
    // 100000 descending values in one row group: under ORDER BY k LIMIT 1 each row improves on the one kept. Its key
    // was copied anew per row on memory the budget did not see (a 4M-row file held 150 MiB at 64M, or was killed).
    const path_in = "ci/fixtures/parquet/topn_descending.parquet";
    const gpa = std.testing.allocator;
    const bytes = metadata.readFileSlice(path_in, gpa) catch |err| {
        if (err == error.FileNotFound) return error.SkipZigTest;
        return err;
    };
    defer gpa.free(bytes);
    var arena_state = std.heap.ArenaAllocator.init(gpa);
    defer arena_state.deinit();
    const meta = try metadata.open(arena_state.allocator(), bytes);
    const specs = [_]consumer.OutputCol{.{ .passthrough = .{ .ci = 0 } }};
    const keys = [_]RowSortKey{.{ .spec = .{ .passthrough = .{ .ci = 0 } }, .descending = false, .unsigned = false }};
    var top: TopRows = try .init(gpa, &meta, &specs, &keys, 1, 64 << 20);
    defer top.deinit();
    var timings: consumer.Timings = .{};
    try top.offer(&meta.row_groups.items[0], &meta, .{ .bytes = bytes, .byte_origin = 0 }, null, &.{true}, .{
        .file = 0,
        .rg = 0,
    }, &timings, .{});
    try std.testing.expectEqual(@as(usize, 1), top.heap.count());
    try std.testing.expectEqual(SortCell{ .int = 0 }, top.heap.peek().?.cells[0]);
    // One kept row's keys and output, not a copy per row (3.2 MB); and every byte of it on the live account.
    try std.testing.expect(top.store_arena.queryCapacity() < 64 << 10);
    try std.testing.expect(top.mem.ledger.used.load(.monotonic) >= top.store_arena.queryCapacity());
    try std.testing.expect(top.mem.ledger.used.load(.monotonic) < 256 << 10);
    // Past the account, the row group's decode refuses cleanly before it is allocated.
    var tight: TopRows = try .init(gpa, &meta, &specs, &keys, 1, 0);
    defer tight.deinit();
    const src: consumer.RGSrc = .{ .bytes = bytes, .byte_origin = 0 };
    const refused = tight.offer(&meta.row_groups.items[0], &meta, src, null, &.{true}, .{ .file = 0, .rg = 0 }, &timings, .{});
    if (refused) |_| {} else |e| try std.testing.expectEqual(error.ExceededMemoryBudget, e);
}

test "an evicted key's string block is reused up to its size, not its last key's length" {
    var arena_state = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena_state.deinit();
    const a = arena_state.allocator();
    var cap: usize = 0;
    var cell = try TopRows.reuseCell(a, .null, &cap, .{ .str = "a long first key" });
    const block = cell.str.ptr;
    try std.testing.expectEqual(@as(usize, 16), cap);
    // Shorter, then long again: both into the first block, which keeps its size.
    cell = try TopRows.reuseCell(a, cell, &cap, .{ .str = "short" });
    try std.testing.expectEqual(block, cell.str.ptr);
    cell = try TopRows.reuseCell(a, cell, &cap, .{ .str = "sixteen bytes ok" });
    try std.testing.expectEqual(block, cell.str.ptr);
    try std.testing.expectEqualStrings("sixteen bytes ok", cell.str);
    // Longer than the block: copied anew, its size the new key's.
    cell = try TopRows.reuseCell(a, cell, &cap, .{ .str = "a key longer than the block" });
    try std.testing.expect(cell.str.ptr != block);
    try std.testing.expectEqual(@as(usize, 27), cap);
}

/// The most rows the parallel plain-row Top-N keeps (OFFSET + LIMIT); past it, or without a LIMIT, `TopRows` runs.
/// Every worker holds a heap of that many keys, so it bounds the heaps as well as the gather.
const max_gather_rows = 1 << 16;

/// Bytes the parallel plain-row Top-N may hold at once: its heaps and their key copies, the merge, and the winners'
/// gathered columns. Each is reserved before it is allocated and given back when freed, so a refusal comes before the
/// memory does; it sends the query to `TopRows`, which answers the same. Row group decoding is not counted here, as it
/// is not for `TopRows`.
const Reservation = struct {
    /// What the heaps, the gather and the workers' decode regions (`RankWorker.decode`) hold together.
    ledger: core_memory.SharedBudget,

    fn take(self: *Reservation, n: usize) error{ExceededMemoryBudget}!void {
        _ = self.ledger.drawExact(n) orelse return error.ExceededMemoryBudget;
    }

    fn give(self: *Reservation, n: usize) void {
        self.ledger.release(n);
    }
};

/// Exact blocks on `gpa`, each reserved before it is allocated: no arena, whose next block grows with the last, so
/// what is held is what was reserved. One thread's at a time.
const ReservedMem = struct {
    gpa: std.mem.Allocator,
    res: *Reservation,
    blocks: std.ArrayList([]align(16) u8) = .empty,

    fn alloc(m: *ReservedMem, comptime T: type, n: usize) ![]T {
        comptime std.debug.assert(@alignOf(T) <= 16);
        const size = std.math.mul(usize, n, @sizeOf(T)) catch return error.ExceededMemoryBudget;
        try m.res.take(size);
        errdefer m.res.give(size);
        try m.blocks.ensureUnusedCapacity(m.gpa, 1);
        const b = try m.gpa.alignedAlloc(u8, .@"16", size);
        m.blocks.appendAssumeCapacity(b);
        return @alignCast(std.mem.bytesAsSlice(T, b));
    }

    fn deinit(m: *ReservedMem) void {
        for (m.blocks.items) |b| {
            m.res.give(b.len);
            m.gpa.free(b);
        }
        m.blocks.deinit(m.gpa);
    }
};

/// The plain-row Top-N over inputs whose bytes are all in, in two passes. Workers rank row groups by their filter and
/// key columns alone, each into a heap of its best `h` (keys, physical row position): a row outside a worker's best h
/// has h rows ahead of it overall, so the heaps together hold the answer. They share the best cutoff any full heap has
/// reached, and pass over a row group or row whose first key sorts strictly after it; a stale cutoff is only weaker.
/// Once the heaps merge to the final h rows, the output columns are read for those rows alone (`GatherTask`).
const RankShared = struct {
    gpa: std.mem.Allocator,
    io: std.Io,
    inputs: []const scan.Input,
    metas: []const schema.FileMetaData,
    filter: ?filter_ast.Filter,
    fetch: []const bool,
    keys: []const RowSortKey,
    key_specs: []const consumer.OutputCol,
    h: usize,
    prune: bool,
    /// The first key is a bare string column (`dictionaryBest`), and whether the filter reads that column alone.
    dict_key: ?DictKey,
    options: consumer.DecodeOptions,
    profiler: ?*profile.Profiler,
    cursor: work_cursor.AtomicWorkCursor(RankedRG),
    res: *Reservation,
    /// A worker failed: the others stop taking row groups.
    failed: std.atomic.Value(bool) = .init(false),
    /// Bumped with every new cutoff, so a worker copies it only when it changed.
    cut_version: std.atomic.Value(u64) = .init(0),
    cut_mutex: std.Io.Mutex = .init,
    /// The first key of some full heap's last row, its string on `gpa`. Guarded by `cut_mutex`.
    cut: ?SortCell = null,

    fn deinit(self: *RankShared) void {
        if (self.cut) |c| if (c == .str) {
            self.res.give(c.str.len);
            self.gpa.free(c.str);
        };
    }
};

const DictKey = struct { ci: usize, filtered: bool };

const RankEntry = struct {
    cells: []SortCell,
    /// `ord` is the row's physical place in its row group, before the filter.
    pos: RowPos,
    /// `cells` and their strings: one block on `gpa`, reserved, freed when the entry leaves the heap.
    block: []align(@alignOf(SortCell)) u8,

    fn later(keys: []const RowSortKey, a: RankEntry, b: RankEntry) std.math.Order {
        if (TopRows.before(keys, b.cells, b.pos, a.cells, a.pos)) return .lt;
        if (TopRows.before(keys, a.cells, a.pos, b.cells, b.pos)) return .gt;
        return .eq;
    }

    fn lessThan(keys: []const RowSortKey, a: RankEntry, b: RankEntry) bool {
        return TopRows.before(keys, a.cells, a.pos, b.cells, b.pos);
    }
};

const RankWorker = struct {
    shared: *RankShared,
    index: usize,
    /// Its capacity, `h`, reserved and allocated up front: a push never grows it.
    heap: std.PriorityQueue(RankEntry, []const RowSortKey, RankEntry.later),
    /// This worker's copy of the shared cutoff, as of `cut_seen`; its string on `cut_buf`.
    cut: ?SortCell = null,
    cut_buf: std.ArrayList(u8) = .empty,
    cut_seen: u64 = 0,
    timings: consumer.Timings = .{},
    err: ?anyerror = null,
    /// Where `err` was raised when it is a decode error (`consumer.decodeFailure`).
    failure: ?consumer.DecodeSite = null,
    /// The item that failed, for a deterministic choice among failing workers.
    err_at: usize = std.math.maxInt(usize),
    /// Row groups whose rows this worker offered, for its profile span.
    ranked: usize = 0,
    /// What a row group decodes on, rewound for the next with its pages kept: on the general allocator, which maps
    /// every large block afresh (`core.memory.fixHeapThreshold`), each row group's decode was faulted in anew. Drawn
    /// from the reservation as its blocks reach new pages, and held while kept: a row group it cannot take fails the
    /// ranking (`ExceededMemoryBudget`), which then runs as `TopRows`.
    decode: huge_pages.Region,

    fn init(shared: *RankShared, index: usize) !RankWorker {
        const size = std.math.mul(usize, shared.h, @sizeOf(RankEntry)) catch return error.ExceededMemoryBudget;
        try shared.res.take(size);
        errdefer shared.res.give(size);
        var heap: std.PriorityQueue(RankEntry, []const RowSortKey, RankEntry.later) = .initContext(shared.keys);
        try heap.ensureTotalCapacityPrecise(shared.gpa, shared.h);
        var decode: huge_pages.Region = .init(shared.gpa, ENCODE_REGION_RESERVE);
        decode.budget = &shared.res.ledger;
        return .{ .shared = shared, .index = index, .heap = heap, .decode = decode };
    }

    fn deinit(w: *RankWorker) void {
        const s = w.shared;
        for (w.heap.items) |e| w.free(e);
        s.res.give(w.heap.capacity() * @sizeOf(RankEntry));
        w.heap.deinit(s.gpa);
        s.res.give(w.cut_buf.capacity);
        w.cut_buf.deinit(s.gpa);
        w.decode.deinit();
    }

    fn run(w: *RankWorker) void {
        const s = w.shared;
        const prof = profile.threadOf(s.profiler, .scan);
        const span = profile.Span.begin(prof);
        w.timings.prof = prof;
        defer span.end(.scan_worker, profile.args(.{ w.index, w.ranked }));
        while (!s.failed.load(.monotonic)) {
            const item = s.cursor.next() orelse break;
            w.step(item) catch |e| {
                w.err = if (e == error.OutOfMemory and w.decode.refused) error.ExceededMemoryBudget else e;
                w.failure = consumer.decodeFailure();
                w.err_at = item - s.cursor.items.ptr;
                s.failed.store(true, .monotonic);
                break;
            };
        }
    }

    fn step(w: *RankWorker, item: *const RankedRG) !void {
        consumer.setDecodeFailure(null);
        const s = w.shared;
        const meta = &s.metas[item.file];
        const rg = &meta.row_groups.items[item.rg];
        if (s.prune) if (s.filter) |f| if (filter_prune.pruneRowGroup(rg, f, meta) == .skip) return;
        try w.refreshCut();
        if (item.best) |best| if (w.cut) |c| if (sortsAfter(s.keys[0], best, c)) return;
        if (s.dict_key != null) if (w.cut) |c| {
            defer w.decode.rewind();
            var dict_mem = std.heap.ArenaAllocator.init(w.decode.allocator());
            defer dict_mem.deinit();
            if (try w.dictionaryBest(dict_mem.allocator(), rg, meta, item.*)) |best| {
                if (sortsAfter(s.keys[0], best, c)) return;
            }
        };
        try w.rank(rg, meta, item.*);
        try w.publishCut();
    }

    /// Offer one row group's filtered rows to the heap, by their keys alone.
    fn rank(w: *RankWorker, rg: *const schema.RowGroup, meta: *const schema.FileMetaData, item: RankedRG) !void {
        const s = w.shared;
        defer w.decode.rewind();
        const da = w.decode.allocator();
        var rg_arena = std.heap.ArenaAllocator.init(da);
        defer rg_arena.deinit();
        const ra = rg_arena.allocator();
        var keyed = try consumer.initOutputAggregator(ra, meta, s.key_specs);
        const src: consumer.RGSrc = .{ .bytes = s.inputs[item.file].bytes, .byte_origin = 0 };
        var p: consumer.ProjectedRG = undefined;
        try p.init(&keyed, da, rg, meta, src, s.filter, s.fetch, s.key_specs, &w.timings, s.options);
        defer p.deinit();
        w.ranked += 1;
        w.timings.rows_scanned += @intCast(rg.num_rows);
        w.timings.rows_matched += p.surviving;
        if (p.surviving == 0) return;
        for (0..s.key_specs.len) |i| try p.projectColumn(&keyed, i, da, &w.timings);
        keyed.num_rows += p.surviving;

        const scratch = try ra.alloc(SortCell, s.keys.len);
        var i: usize = 0;
        for (p.sel.mask, 0..) |word, wi| {
            var bits = word;
            while (bits != 0) : (bits &= bits - 1) {
                const at = i;
                i += 1;
                for (s.keys, keyed.cols, scratch) |k, buf, *cell| cell.* = sortCell(buf, at, k.unsigned);
                if (w.cut) |c| if (sortsAfter(s.keys[0], scratch[0], c)) continue;
                const pos: RowPos = .{ .file = item.file, .rg = item.rg, .ord = @intCast(wi * 64 + @ctz(bits)) };
                if (w.heap.count() == s.h) {
                    const top = w.heap.peek().?;
                    if (!TopRows.before(s.keys, scratch, pos, top.cells, top.pos)) continue;
                    w.free(w.heap.pop().?);
                }
                try w.admit(scratch, pos);
            }
        }
    }

    /// Push a row into the heap (which has room), its cells and strings copied into one reserved block.
    fn admit(w: *RankWorker, cells_in: []const SortCell, pos: RowPos) !void {
        const s = w.shared;
        const head = cells_in.len * @sizeOf(SortCell);
        var size = head;
        for (cells_in) |c| if (c == .str) {
            size += c.str.len;
        };
        try s.res.take(size);
        errdefer s.res.give(size);
        const block = try s.gpa.alignedAlloc(u8, .of(SortCell), size);
        const cells: []SortCell = @alignCast(std.mem.bytesAsSlice(SortCell, block[0..head]));
        var tail = block[head..];
        for (cells_in, cells) |c, *d| {
            d.* = c;
            if (c == .str) {
                @memcpy(tail[0..c.str.len], c.str);
                d.* = .{ .str = tail[0..c.str.len] };
                tail = tail[c.str.len..];
            }
        }
        std.debug.assert(w.heap.count() < w.heap.capacity());
        w.heap.push(s.gpa, .{ .cells = cells, .pos = pos, .block = block }) catch unreachable;
    }

    fn free(w: *RankWorker, e: RankEntry) void {
        w.shared.res.give(e.block.len);
        w.shared.gpa.free(e.block);
    }

    /// The best first key any row of a row group can have, from its string key chunk's dictionary: the smallest entry
    /// ascending, the largest descending, among those the filter keeps when it reads that column alone. NULL when no
    /// entry qualifies (the group's rows then have a NULL key or none qualifies). A safe bound whether or not a row
    /// uses the entry; null when the chunk is not dictionary-encoded throughout, so a value may not be an entry.
    fn dictionaryBest(
        w: *RankWorker,
        ra: std.mem.Allocator,
        rg: *const schema.RowGroup,
        meta: *const schema.FileMetaData,
        item: RankedRG,
    ) !?SortCell {
        const s = w.shared;
        const dk = s.dict_key.?;
        const src: consumer.RGSrc = .{ .bytes = s.inputs[item.file].bytes, .byte_origin = 0 };
        const dict = (try consumer.chunkDictionary(ra, rg, meta, src, dk.ci)) orelse return null;
        var kept: ?filter_selection.SelectionVector = null;
        if (dk.filtered) if (s.filter) |f| {
            const cols = [_]filter_eval.Batch.Column{.{ .string = .{ .values = dict } }};
            const lookup = try ra.alloc(?usize, meta.schema.items.len);
            @memset(lookup, null);
            lookup[dk.ci] = 0;
            var sel = try filter_selection.SelectionVector.init(ra, dict.len);
            try filter_eval.evaluate(f, &.{ .cols = &cols, .num_rows = dict.len }, &sel, lookup, ra);
            kept = sel;
        };
        var best: ?[]const u8 = null;
        const want: std.math.Order = if (s.keys[0].descending) .gt else .lt;
        for (dict, 0..) |v, k| {
            if (kept) |sel| if (!sel.isActive(k)) continue;
            if (best == null or std.mem.order(u8, v, best.?) == want) best = v;
        }
        return if (best) |b| .{ .str = b } else .null;
    }

    /// Copy the shared cutoff when it changed since this worker last looked.
    fn refreshCut(w: *RankWorker) !void {
        const s = w.shared;
        if (s.cut_version.load(.acquire) == w.cut_seen) return;
        s.cut_mutex.lockUncancelable(s.io);
        defer s.cut_mutex.unlock(s.io);
        const c = s.cut orelse return;
        if (c == .str and c.str.len > w.cut_buf.capacity) {
            const grow = c.str.len - w.cut_buf.capacity;
            try s.res.take(grow);
            w.cut_buf.ensureTotalCapacityPrecise(s.gpa, c.str.len) catch |e| {
                s.res.give(grow);
                return e;
            };
        }
        w.cut_buf.clearRetainingCapacity();
        w.cut = if (c == .str) .{ .str = blk: {
            w.cut_buf.appendSliceAssumeCapacity(c.str);
            break :blk w.cut_buf.items;
        } } else c;
        w.cut_seen = s.cut_version.load(.monotonic);
    }

    /// Share this worker's cutoff when its heap is full and its last row's first key beats the shared one.
    fn publishCut(w: *RankWorker) !void {
        const s = w.shared;
        if (w.heap.count() < s.h) return;
        const mine = w.heap.peek().?.cells[0];
        if (w.cut) |c| if (!sortsAfter(s.keys[0], c, mine)) return;
        s.cut_mutex.lockUncancelable(s.io);
        defer s.cut_mutex.unlock(s.io);
        if (s.cut) |c| if (!sortsAfter(s.keys[0], c, mine)) return;
        const owned: SortCell = if (mine == .str) blk: {
            try s.res.take(mine.str.len);
            break :blk .{ .str = s.gpa.dupe(u8, mine.str) catch |e| {
                s.res.give(mine.str.len);
                return e;
            } };
        } else mine;
        if (s.cut) |c| if (c == .str) {
            s.res.give(c.str.len);
            s.gpa.free(c.str);
        };
        s.cut = owned;
        _ = s.cut_version.fetchAdd(1, .release);
    }
};

test "a Top-N worker's decode is drawn from the reservation before it is allocated, and refused past it" {
    var reservation: Reservation = .{ .ledger = .{ .limit = 4 << 20 } };
    var shared: RankShared = undefined;
    shared.gpa = std.testing.allocator;
    shared.res = &reservation;
    shared.h = 1;
    shared.keys = &.{};
    var worker = try RankWorker.init(&shared, 0);
    defer worker.deinit();
    if (worker.decode.buf.len == 0) return error.SkipZigTest;
    const before = reservation.ledger.used.load(.monotonic);
    // Past the reservation: refused before anything is allocated, as the ranking's own refusal.
    try std.testing.expectError(error.OutOfMemory, worker.decode.allocator().alloc(u8, 8 << 20));
    try std.testing.expect(worker.decode.refused);
    try std.testing.expectEqual(before, reservation.ledger.used.load(.monotonic));
    worker.decode.refused = false;
    // Within it: held, a huge page at a time, as long as the worker keeps it.
    const bytes = try worker.decode.allocator().alloc(u8, 1 << 20);
    @memset(bytes, 1);
    try std.testing.expect(reservation.ledger.used.load(.monotonic) >= before + bytes.len);
}

/// The winners of one row group, by their physical rows, ascending.
const GatherGroup = struct {
    file: u32,
    rg: u32,
    rows: []const u32,
};

/// One output column of one row group holding winners: its winners' values, read a run of rows at a time with the
/// pages between them passed over (`consumer.SliceReader.skip`). Its buffers are sized once, for its rows, and every
/// byte is reserved first.
const GatherTask = struct {
    inputs: []const scan.Input,
    metas: []const schema.FileMetaData,
    group: *const GatherGroup,
    ci: usize,
    options: consumer.DecodeOptions,
    /// In the output column's lane; a string's values point into `bytes` once `read` returns.
    buf: consumer.OutputAggregator.ColumnBuf,
    mem: ReservedMem,
    bytes: std.ArrayList(u8) = .empty,
    err: ?anyerror = null,

    fn deinit(t: *GatherTask) void {
        t.mem.res.give(t.bytes.capacity);
        t.bytes.deinit(t.mem.gpa);
        t.mem.deinit();
    }

    /// Back to before `run`, everything it held given back: for a task the reservation refused beside the others,
    /// to run again alone.
    fn reset(t: *GatherTask) void {
        t.deinit();
        t.bytes = .empty;
        t.mem = .{ .gpa = t.mem.gpa, .res = t.mem.res };
        t.buf = switch (t.buf) {
            inline else => |_, tag| @unionInit(consumer.OutputAggregator.ColumnBuf, @tagName(tag), .{}),
        };
        t.err = null;
    }

    fn run(t: *GatherTask) void {
        // The column's decode (its scratch, a page, a byte-array dictionary and a run of rows) draws from the
        // reservation before it is allocated, as the heaps and the gathered values do: runners decoding at once
        // held a page or a dictionary each past it. Refused, the gather fails, and the query ranks serially.
        var decode: core_memory.Charged = .{ .child = t.mem.gpa, .budget = &t.mem.res.ledger };
        t.read(decode.allocator()) catch |e| {
            t.err = decode.refusal(e);
        };
    }

    fn read(t: *GatherTask, dg: std.mem.Allocator) !void {
        const g = t.group;
        const n = g.rows.len;
        const meta = &t.metas[g.file];
        const rg = &meta.row_groups.items[g.rg];
        const src: consumer.RGSrc = .{ .bytes = t.inputs[g.file].bytes, .byte_origin = 0 };
        const cm = rg.columns.items[t.ci].meta_data orelse return error.ColumnMetaMissing;
        const max_def = meta.getColumnLevels(cm.path_in_schema.items).max_def;
        // A string's byte offsets into `bytes`, one past each value.
        var ends: []usize = &.{};
        switch (t.buf) {
            inline else => |*tb| {
                const T = @typeInfo(@TypeOf(tb.values.items)).pointer.child;
                tb.values = .initBuffer(try t.mem.alloc(T, n));
                if (max_def > 0) tb.def_levels = .initBuffer(try t.mem.alloc(u32, n));
                if (T == []const u8) ends = try t.mem.alloc(usize, n);
            },
        }
        // A byte-array dictionary and its pages each draw their own inflated size (`SliceReader.chargePages`): on an
        // arena on `dg` a 60 MB page was charged as the 90 MB node holding it.
        var held = std.heap.ArenaAllocator.init(t.mem.gpa);
        defer held.deinit();
        var r = (try consumer.SliceReader.init(held.allocator(), dg, rg, meta, src, t.ci, t.options)) orelse
            return error.NestedRowOutputNotSupported;
        defer r.deinit();
        r.chargePages(t.mem.gpa, &t.mem.res.ledger);
        var at: usize = 0;
        var i: usize = 0;
        while (i < n) {
            var j = i + 1;
            while (j < n and g.rows[j] == g.rows[j - 1] + 1) j += 1;
            var slice = std.heap.ArenaAllocator.init(dg);
            defer slice.deinit();
            const sa = slice.allocator();
            try r.skip(sa, g.rows[i] - at);
            const col = try consumer.outputLane(t.buf, try r.next(sa, j - i), sa);
            switch (col) {
                inline else => |c, tag| {
                    if (!std.mem.eql(u8, @tagName(std.meta.activeTag(t.buf)), @tagName(tag))) return error.TypeMismatch;
                    const tb = &@field(t.buf, @tagName(tag));
                    if (@TypeOf(c.values) == []const []const u8) {
                        var run_bytes: usize = 0;
                        for (c.values) |v| run_bytes += v.len;
                        try t.growBytes(run_bytes);
                        for (c.values, ends[i..j]) |v, *e| {
                            t.bytes.appendSliceAssumeCapacity(v);
                            e.* = t.bytes.items.len;
                        }
                        tb.values.items.len += c.values.len;
                    } else tb.values.appendSliceAssumeCapacity(c.values);
                    if (max_def > 0) {
                        if (c.def_levels) |dl| tb.def_levels.appendSliceAssumeCapacity(dl) else {
                            tb.def_levels.appendNTimesAssumeCapacity(c.max_def, j - i);
                        }
                        tb.max_def = c.max_def;
                    }
                },
            }
            at = g.rows[j - 1] + 1;
            i = j;
        }
        if (t.buf == .string) for (t.buf.string.values.items, ends, 0..) |*v, end, k| {
            v.* = t.bytes.items[if (k == 0) 0 else ends[k - 1]..end];
        };
    }

    /// Room for `more` string bytes, the grown buffer reserved before it is allocated.
    fn growBytes(t: *GatherTask, more: usize) !void {
        const need = t.bytes.items.len + more;
        if (need <= t.bytes.capacity) return;
        const old = t.bytes.capacity;
        const cap = @max(need, old *| 2);
        // Both blocks while the bytes move.
        try t.mem.res.take(cap);
        t.bytes.ensureTotalCapacityPrecise(t.mem.gpa, cap) catch |e| {
            t.mem.res.give(cap);
            return e;
        };
        t.mem.res.give(old);
    }
};

const GatherRunner = struct {
    cursor: *work_cursor.AtomicWorkCursor(GatherTask),

    fn run(g: *GatherRunner) void {
        while (g.cursor.next()) |t| t.run();
    }
};

/// Run `ctxs` on threads of their own, or on this one when there is one context or no thread starts.
fn runAll(comptime Ctx: type, ctxs: []Ctx, comptime func: fn (*Ctx) void) void {
    if (ctxs.len > 1 and spawn_util.runTree(Ctx, ctxs, .{ .stack_size = scan.WORKER_STACK_SIZE }, func)) return;
    func(&ctxs[0]);
}

/// Leaf `ci` when the first sort key is a bare string column, whose dictionary bounds a row group's keys
/// (`RankWorker.dictionaryBest`): BYTE_ARRAY, not DECIMAL (whose lane is numeric), compared as raw bytes as its
/// `SortCell` is. `filtered` when the filter reads that column alone, so it can judge each dictionary entry.
fn dictionaryKey(meta: *const schema.FileMetaData, key: RowSortKey, filter: ?filter_ast.Filter) ?DictKey {
    const ci = switch (key.spec) {
        .passthrough => |pt| pt.ci,
        .computed => return null,
    };
    var walk: schema.LeafIterator = undefined;
    const leaf = meta.leafAt(ci, &walk) orelse return null;
    if (leaf.element.type != .BYTE_ARRAY or leaf.max_rep > 0 or decimal_mod.kindFromSchema(leaf.element) != null) return null;
    const f = filter orelse return .{ .ci = ci, .filtered = false };
    var buf: [64]usize = undefined;
    var fba = std.heap.FixedBufferAllocator.init(std.mem.sliceAsBytes(&buf));
    var cols: std.ArrayList(usize) = .empty;
    f.collectColumns(&cols, fba.allocator()) catch return .{ .ci = ci, .filtered = false };
    for (cols.items) |c| if (c != ci) return .{ .ci = ci, .filtered = false };
    return .{ .ci = ci, .filtered = true };
}

/// Whether the plain-row Top-N can rank in parallel and gather its winners (`RankShared`): a LIMIT keeping at most
/// `max_gather_rows`, bare output columns and keys (a computed one can fail on a row, and passing over its row group
/// would answer where reading it raises the error), flat outputs (read a slice at a time), and every input's bytes
/// already in: a gated input releases a row group's bytes once read, and the gather reads winners' groups again.
pub fn gatherEligible(
    env: std.process.Environ,
    inputs: []const scan.Input,
    meta: *const schema.FileMetaData,
    limit: ?usize,
    keep: usize,
    specs: []const consumer.OutputCol,
    keys: []const RowSortKey,
) bool {
    if (envDisables(env, "ZPQ_TOPN_GATHER")) return false;
    const l = limit orelse return false;
    if (l == 0 or keep > max_gather_rows) return false;
    if (!allPassthrough(specs, keys)) return false;
    for (inputs) |in| if (in.gate != null) return false;
    var walk: schema.LeafIterator = undefined;
    for (specs) |spec| {
        const leaf = meta.leafAt(spec.passthrough.ci, &walk) orelse continue;
        if (leaf.max_rep > 0) return false;
    }
    return true;
}

/// The parallel plain-row Top-N's answer: the first `h` rows in order (`TopRows.ranked`'s), held in reserved memory
/// on `gpa` until `deinit`.
const Gathered = struct {
    out: consumer.OutputAggregator,
    /// The answer's columns and the merge's arrays.
    mem: ReservedMem,
    /// The winners' columns per row group; the answer's strings point into them.
    tasks: []GatherTask = &.{},

    pub fn deinit(g: *Gathered) void {
        for (g.tasks) |*t| t.deinit();
        g.mem.deinit();
    }
};

/// What a plain-row Top-N may hold: the query's budget, less the S3 fetch window beside it when an input is fetched
/// (the window is its own account), and at most half of it, as an aggregate's decode leaves the other half.
pub fn topnLimit(ctx: Context, args: QueryArgs, inputs: []const scan.Input) usize {
    const window: usize = @min(@as(usize, @intCast(s3FetchWindow(ctx.env, args.max_memory))), args.max_memory / 2);
    for (inputs) |in| if (in.gate != null) return args.max_memory -| window;
    return args.max_memory;
}

/// The plain-row Top-N ranked in parallel and gathered (see `RankShared`). Null when its memory would pass the
/// budget, refused before it is allocated and with everything given back; the caller then runs `TopRows`.
pub fn rankAndGather(
    ctx: Context,
    arena: std.mem.Allocator,
    args: QueryArgs,
    inputs: []const scan.Input,
    metas: []const schema.FileMetaData,
    filter: ?filter_ast.Filter,
    fetch: []const bool,
    specs: []const consumer.OutputCol,
    keys: []const RowSortKey,
    h: usize,
    items: []RankedRG,
    t: *Timings,
) !?Gathered {
    const res = try arena.create(Reservation);
    // Heaps, gather and the workers' decode together: the query's budget.
    res.* = .{ .ledger = .{ .limit = topnLimit(ctx, args, inputs) } };
    var g: Gathered = .{
        .out = try consumer.initOutputAggregator(arena, &metas[0], specs),
        .mem = .{ .gpa = ctx.gpa, .res = res },
    };
    gatherInto(&g, ctx, arena, args, inputs, metas, filter, fetch, specs, keys, h, items, t) catch |e| {
        g.deinit();
        return if (e == error.ExceededMemoryBudget) null else e;
    };
    return g;
}

fn gatherInto(
    g: *Gathered,
    ctx: Context,
    arena: std.mem.Allocator,
    args: QueryArgs,
    inputs: []const scan.Input,
    metas: []const schema.FileMetaData,
    filter: ?filter_ast.Filter,
    fetch: []const bool,
    specs: []const consumer.OutputCol,
    keys: []const RowSortKey,
    h: usize,
    items: []RankedRG,
    t: *Timings,
) !void {
    const gpa = ctx.gpa;
    const res = g.mem.res;
    const prof = profile.threadOf(args.profiler, .main);
    if (items.len == 0) return;
    const key_specs = try arena.alloc(consumer.OutputCol, keys.len);
    for (keys, key_specs) |k, *spec| spec.* = k.spec;
    const threads = if (args.parallelism > 0) args.parallelism else system.discoverAvailableParallelism(ctx.env);
    // Every worker holds a heap of h: fewer workers rather than heaps taking more than an eighth of the budget.
    var n_workers = std.math.clamp(threads, 1, items.len);
    while (n_workers > 1 and n_workers *| h *| @sizeOf(RankEntry) > res.ledger.limit / 8) n_workers -= 1;

    const t_rank = nowMonoNs();
    var shared: RankShared = .{
        .gpa = gpa,
        .io = ctx.io,
        .inputs = inputs,
        .metas = metas,
        .filter = filter,
        .fetch = fetch,
        .keys = keys,
        .key_specs = key_specs,
        .h = h,
        .prune = !args.scan_all,
        .dict_key = dictionaryKey(&metas[0], keys[0], filter),
        .options = .{ .fast_levels = args.fast_levels, .expand_limit = args.max_memory },
        .profiler = args.profiler,
        .cursor = .{ .items = items },
        .res = res,
    };
    defer shared.deinit();
    const workers = try g.mem.alloc(RankWorker, n_workers);
    var live: usize = 0;
    defer for (workers[0..live]) |*w| w.deinit();
    for (workers) |*w| {
        w.* = try .init(&shared, live);
        live += 1;
    }
    runAll(RankWorker, workers, RankWorker.run);
    const t_merge = nowMonoNs();
    profile.record(prof, .scan, t_rank, t_merge, profile.args(.{n_workers}));

    var failed: ?*const RankWorker = null;
    for (workers) |*w| {
        t.core.decode_ns += w.timings.decode_ns;
        t.core.eval_ns += w.timings.eval_ns;
        t.core.rows_scanned += w.timings.rows_scanned;
        t.core.rows_matched += w.timings.rows_matched;
        if (w.err != null and (failed == null or w.err_at < failed.?.err_at)) failed = w;
    }
    if (failed) |w| {
        consumer.setDecodeFailure(w.failure);
        return w.err.?;
    }

    var n_entries: usize = 0;
    for (workers) |w| n_entries += w.heap.count();
    const entries = try g.mem.alloc(RankEntry, n_entries);
    var at: usize = 0;
    for (workers) |w| {
        @memcpy(entries[at..][0..w.heap.count()], w.heap.items);
        at += w.heap.count();
    }
    std.mem.sortUnstable(RankEntry, entries, keys, RankEntry.lessThan);
    const winners = entries[0..@min(h, entries.len)];
    profile.record(prof, .merge, t_merge, nowMonoNs(), profile.args(.{ n_entries, winners.len }));
    if (winners.len == 0) return;

    // Winners by place in the input, grouped by row group; `slots[k]` is winner k's group and row within it.
    const t_gather = nowMonoNs();
    const by_pos = try g.mem.alloc(u32, winners.len);
    for (by_pos, 0..) |*b, k| b.* = @intCast(k);
    std.mem.sortUnstable(u32, by_pos, winners, struct {
        fn lt(ws: []const RankEntry, a: u32, b: u32) bool {
            return ws[a].pos.order(ws[b].pos) == .lt;
        }
    }.lt);
    const WinnerSlot = struct { group: u32, row: u32 };
    const slots = try g.mem.alloc(WinnerSlot, winners.len);
    const rows = try g.mem.alloc(u32, winners.len);
    var n_groups: usize = 0;
    for (by_pos, 0..) |k, n| {
        if (n == 0 or winners[k].pos.file != winners[by_pos[n - 1]].pos.file or
            winners[k].pos.rg != winners[by_pos[n - 1]].pos.rg) n_groups += 1;
    }
    const groups = try g.mem.alloc(GatherGroup, n_groups);
    var gi: usize = 0;
    var start: usize = 0;
    for (by_pos, 0..) |k, n| {
        const p = winners[k].pos;
        rows[n] = p.ord;
        const last = n + 1 == by_pos.len or winners[by_pos[n + 1]].pos.file != p.file or
            winners[by_pos[n + 1]].pos.rg != p.rg;
        slots[k] = .{ .group = @intCast(gi), .row = @intCast(n - start) };
        if (last) {
            groups[gi] = .{ .file = p.file, .rg = p.rg, .rows = rows[start .. n + 1] };
            gi += 1;
            start = n + 1;
        }
    }
    // Only positions are needed from here: the heaps go before the winners' columns are read.
    for (workers[0..live]) |*w| w.deinit();
    live = 0;

    const tasks = try g.mem.alloc(GatherTask, groups.len * specs.len);
    for (groups, 0..) |*grp, k| for (specs, g.out.cols, 0..) |spec, col, ci| {
        tasks[k * specs.len + ci] = .{
            .inputs = inputs,
            .metas = metas,
            .group = grp,
            .ci = spec.passthrough.ci,
            .options = .{ .fast_levels = args.fast_levels, .expand_limit = args.max_memory },
            .buf = switch (col) {
                inline else => |_, tag| @unionInit(consumer.OutputAggregator.ColumnBuf, @tagName(tag), .{}),
            },
            .mem = .{ .gpa = gpa, .res = res },
        };
    };
    g.tasks = tasks;
    var cursor: work_cursor.AtomicWorkCursor(GatherTask) = .{ .items = tasks };
    const runners = try arena.alloc(GatherRunner, std.math.clamp(threads, 1, tasks.len));
    for (runners) |*r| r.* = .{ .cursor = &cursor };
    runAll(GatherRunner, runners, GatherRunner.run);
    // A task the reservation refused beside the others runs again alone, after them: fewer at once rather than none.
    if (runners.len > 1) for (tasks) |*task| if (task.err) |e| if (e == error.ExceededMemoryBudget) {
        task.reset();
        task.run();
    };
    // The first failure in task order, so the error does not depend on which thread met it first.
    for (tasks) |*task| if (task.err) |e| return e;

    // The answer's columns, in rank order, borrowing the tasks' strings.
    for (g.out.cols, 0..) |*col, ci| switch (col.*) {
        inline else => |*ob, tag| {
            const T = @typeInfo(@TypeOf(ob.values.items)).pointer.child;
            const first = &@field(tasks[ci].buf, @tagName(tag));
            const vals = try g.mem.alloc(T, winners.len);
            const defs: ?[]u32 = if (first.def_levels.capacity > 0) try g.mem.alloc(u32, winners.len) else null;
            for (slots, 0..) |s, k| {
                const src = &@field(tasks[s.group * specs.len + ci].buf, @tagName(tag));
                vals[k] = src.values.items[s.row];
                if (defs) |d| d[k] = src.def_levels.items[s.row];
            }
            ob.values = .initBuffer(vals);
            ob.values.items.len = vals.len;
            if (defs) |d| {
                ob.def_levels = .initBuffer(d);
                ob.def_levels.items.len = d.len;
            }
            ob.max_def = first.max_def;
        },
    };
    g.out.num_rows = winners.len;
    profile.record(prof, .project, t_gather, nowMonoNs(), profile.args(.{ winners.len, specs.len }));
}

test "dictionaryKey takes a bare string first key, judged by the filter only when it reads that column alone" {
    std.Io.Dir.cwd().access(std.testing.io, topn_gather_inputs[0], .{}) catch return error.SkipZigTest;
    const gpa = std.testing.allocator;
    var arena_state = std.heap.ArenaAllocator.init(gpa);
    defer arena_state.deinit();
    const a = arena_state.allocator();
    const meta = try metadata.open(a, try metadata.readFileSlice(topn_gather_inputs[0], a));
    const key = struct {
        fn of(ci: usize) RowSortKey {
            return .{ .spec = .{ .passthrough = .{ .ci = ci } }, .descending = false, .unsigned = false };
        }
    }.of;
    const on_c = try filter_parser.parse(a, "c != ''", &meta, null);
    const on_k = try filter_parser.parse(a, "c != '' AND k > 3", &meta, null);
    try std.testing.expectEqual(DictKey{ .ci = 7, .filtered = true }, dictionaryKey(&meta, key(7), on_c).?);
    try std.testing.expectEqual(DictKey{ .ci = 7, .filtered = false }, dictionaryKey(&meta, key(7), on_k).?);
    try std.testing.expectEqual(DictKey{ .ci = 2, .filtered = false }, dictionaryKey(&meta, key(2), null).?);
    // Not strings: an integer, and a DECIMAL whose lane is numeric.
    try std.testing.expect(dictionaryKey(&meta, key(0), null) == null);
    try std.testing.expect(dictionaryKey(&meta, key(5), null) == null);
}

pub const topn_gather_inputs = [_][]const u8{
    "ci/fixtures/parquet/topn_gather_a.parquet",
    "ci/fixtures/parquet/topn_gather_b.parquet",
};
