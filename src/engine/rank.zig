const std = @import("std");
const schema = @import("../core/schema.zig");
const metadata = @import("../core/parquet/metadata.zig");
const statistics = @import("../core/parquet/statistics.zig");
const consumer = @import("../core/consumer.zig");
const scan = @import("../core/scan.zig");
const core_memory = @import("../core/memory.zig");
const filter_ast = @import("../core/filter/ast.zig");

/// A plain-row ORDER BY key bound to its input column (`expr_ast.RowKey`).
pub const RowSortKey = struct {
    /// The key as an output column of the row: a passthrough leaf, or an expression computed like a written column.
    spec: consumer.OutputCol,
    descending: bool,
    /// An unsigned integer column: its lane holds raw bits, compared as unsigned.
    unsigned: bool,
};

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

