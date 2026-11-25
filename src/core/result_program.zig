//! The one stage every aggregate answer passes through after its groups are complete, whatever strategy produced
//! them (scalar, plain or partitioned hash, sketch, census, first-keys, TPUT or ResultCut): the engine runs it on the
//! rows `scan.runMultiAggregate` returns. In SQL order: keep the complete groups HAVING makes TRUE, rank them by
//! ORDER BY, cut them to OFFSET/LIMIT once, then drop the hidden columns HAVING and ranking read
//! (`ResultOrder.hidden`), so no output format ever sees them. Without GROUP BY there is one group, kept or not.
//!
//! The strategies before it may cut rows early only where the cut provably keeps every row this stage would keep;
//! the answer is this stage's either way. `cutOrder` is the one place that decides what they may see of the order.
//!
//! A result column that is an expression over aggregates (`sum(a) / sum(b)`, `ORDER BY avg(x) * 2`) is computed here,
//! once per group, from the group's other cells: `finishPost` widens each row to the SELECT list followed by the
//! hidden terms, filling the `derived` cells, before HAVING and the ranking read them.

const std = @import("std");
const scan = @import("scan.zig");
const schema = @import("schema.zig");
const expr_ast = @import("expr/ast.zig");
const expr_agg = @import("expr/agg.zig");
const core_memory = @import("memory.zig");
const BorrowedSpan = @import("scan/types.zig").BorrowedSpan;
const decimalMean = expr_ast.decimalMean;

pub const Error = error{
    OutOfMemory,
    /// An expression over results mixes a string, a date or a boolean with arithmetic.
    TypeMismatch,
    HavingTypeMismatch,
    DuplicateOutputColumn,
    IntegerOverflowAdd,
    IntegerOverflowSub,
    IntegerOverflowMul,
    IntegerOverflowRound,
    IntegerOverflowAbs,
    DecimalOverflow,
    DecimalScaleTooLarge,
    /// The arithmetic reads a value whose SQL type zpq does not resolve (the min or max of an expression or of a FLOAT
    /// column): refused rather than computed in a type DuckDB would not use.
    PostAggTypeUnknown,
    /// The rows a repeat makes (`ResultOrder.repeat`) do not fit the query's ledger.
    ExceededMemoryBudget,
};

pub const ResultProgram = struct {
    order: expr_ast.ResultOrder = .{},
    /// How many leading columns of each GROUP BY row are shown (`MultiAggResult.visible_cols`); null shows all.
    visible: ?usize = null,
    /// The SELECT list, whose `derived` columns `order.post` computes here; the scan's row has none of them.
    select: []const expr_ast.SelectColumn = &.{},
    /// The scan's aggregate calls and each one's pass-through source (`ScanState`): they type the integer columns the
    /// computed columns read, which the scan's own column types (`GroupColType`) leave to the lane.
    calls: []const expr_agg.AggCall = &.{},
    sources: []const ?schema.SchemaElement = &.{},
    /// The query has a GROUP BY, whether or not any key is selected: it picks the plan an AVG of a DECIMAL is computed
    /// by (`Facts`), and no `key` column says so when the keys are projected away.
    grouped: bool = false,
    /// Threads the per-row passes over a large answer (`widen`, `project`) may run on.
    workers: usize = 1,
    /// The query's ledger, which the rows a repeat makes draw from (`repeatRows`).
    budget: ?*core_memory.SharedBudget = null,

    /// The order the strategies before this stage may cut by. A cut ranks groups before HAVING has dropped any, so
    /// under HAVING it would keep the top rows of the wrong set and underfill the answer: the limit is masked there,
    /// the keys and hidden terms kept for binding. The semantic limit stays with this program.
    pub fn cutOrder(order: expr_ast.ResultOrder) expr_ast.ResultOrder {
        var masked = order;
        // The keys and HAVING index a row wider than the scan's, with columns computed here: the strategies see
        // neither, and so cut nothing.
        if (order.post) {
            masked.keys = &.{};
            masked.having = null;
            masked.limit = null;
            masked.offset = 0;
            return masked;
        }
        // A NULLS FIRST key ranks NULLs the strategies' cuts (which rank them last) would drop: keep every group.
        const nulls_first = for (order.keys) |k| {
            if (k.nulls_first) break true;
        } else false;
        if (order.having != null or nulls_first) {
            masked.limit = null;
            masked.offset = 0;
        }
        return masked;
    }

    /// Finish `out` in place. `col_types`, one per row column, is cut to the visible columns alongside; it is borrowed
    /// from the scan's arena, so only the slice changes. On failure `out` is whole (unranked or ranked), never torn.
    pub fn finish(
        self: ResultProgram,
        gpa: std.mem.Allocator,
        out: *scan.AggOutput,
        col_types: ?*?[]const scan.GroupColType,
    ) Error!void {
        if (self.order.post) return self.finishPost(gpa, out, col_types);
        if (self.order.having) |h| {
            try applyHaving(gpa, out, h.*, if (out.group_rows == null) .{ .calls = self.calls } else .{});
        }
        const copies = try self.rank(gpa, out);
        defer if (copies) |c| gpa.free(c);
        try self.printMeans(out, if (col_types) |ct| ct.* else null);
        if (out.group_rows == null) return self.projectScalar(gpa, out);
        try project(self.visible, self.workers, gpa, out, col_types);
        // After the hidden columns are gone: a copy holds only what is shown.
        if (copies) |c| try self.repeatRows(gpa, out, c);
        // A cut leaves few of the rows the slab was built for: release the rest. Only memory is at stake, so a
        // refused allocation keeps the larger slab rather than fail an answer already final.
        out.compactRows(gpa) catch {};
    }

    /// ORDER BY, OFFSET and LIMIT. Rows that repeat (`ResultOrder.repeat`) are ranked and cut as groups: the first
    /// OFFSET + LIMIT rows lie in as many groups, each standing for one row at least. Returned for them, on `gpa`: how
    /// often each ranked group's row is shown once OFFSET and LIMIT have counted rows (`repeatRows`).
    fn rank(self: ResultProgram, gpa: std.mem.Allocator, out: *scan.AggOutput) Error!?[]usize {
        const count = self.order.repeat orelse {
            try out.applyResultOrder(gpa, self.order);
            return null;
        };
        var groups = self.order;
        groups.offset = 0;
        if (self.order.limit) |n| groups.limit = self.order.offset +| n;
        try out.applyResultOrder(gpa, groups);
        const rows = out.group_rows orelse return null;
        const copies = try gpa.alloc(usize, rows.len);
        errdefer gpa.free(copies);
        var skip: u64 = self.order.offset;
        var room: u64 = self.order.limit orelse std.math.maxInt(u64);
        for (rows, copies) |row, *n| {
            var c: u64 = switch (row[count]) {
                .i => |x| std.math.cast(u64, x) orelse return error.TypeMismatch,
                else => return error.TypeMismatch,
            };
            const skipped = @min(skip, c);
            skip -= skipped;
            c = @min(c - skipped, room);
            room -= c;
            n.* = std.math.cast(usize, c) orelse return error.ExceededMemoryBudget;
        }
        return copies;
    }

    /// Row `i` as `copies[i]` rows: a group's row becomes the rows it stands for, in one new slab drawn from the ledger
    /// and allocated whole before a row changes hands, so a refusal leaves `out` as it was. A row shown more than once
    /// shares its strings among its copies: the answer takes them over, as it holds the strings it fetched, and no
    /// copy owns one.
    fn repeatRows(self: ResultProgram, gpa: std.mem.Allocator, out: *scan.AggOutput, copies: []const usize) Error!void {
        const rows = out.group_rows.?;
        const width = out.group_cols.?.len;
        var total: usize = 0;
        for (copies) |n| total = std.math.add(usize, total, n) catch return error.ExceededMemoryBudget;
        const bytes = std.math.mul(usize, total, width * @sizeOf(scan.AggValue) + @sizeOf([]const scan.AggValue)) catch
            return error.ExceededMemoryBudget;
        if (self.budget) |b| _ = b.drawExact(bytes) orelse return error.ExceededMemoryBudget;
        errdefer if (self.budget) |b| b.release(bytes);
        var shared: std.ArrayList([]u8) = .empty;
        defer shared.deinit(gpa);
        for (rows, copies) |row, n| if (n > 1) for (row) |v| {
            if (v == .s and v.s.len != 0 and !BorrowedSpan.contains(out.borrowed_strings, v.s)) try shared.append(gpa, @constCast(v.s));
        };
        const slab = try gpa.alloc(scan.AggValue, total * width);
        errdefer gpa.free(slab);
        const views = try gpa.alloc([]const scan.AggValue, total);
        errdefer gpa.free(views);
        out.adoptFetched(gpa, shared.items) catch return error.OutOfMemory;
        var at: usize = 0;
        for (rows, copies) |row, n| {
            for (0..n) |_| {
                const cells = slab[at * width ..][0..width];
                @memcpy(cells, row);
                views[at] = cells;
                at += 1;
            }
            // A row shown once hands its strings to its one copy; one never shown frees them.
            if (n == 0) out.releaseRow(gpa, row) else out.releaseRowSlice(gpa, row);
        }
        gpa.free(rows);
        gpa.free(out.slab);
        out.slab = slab;
        out.group_rows = views;
        // The answer holds the draw as it holds its string arenas': released with it.
        if (self.budget) |b| if (out.arena_ledger == null or out.arena_ledger == b) {
            out.arena_ledger = b;
            out.arena_credit += bytes;
        };
    }

    /// Drop each row's columns past the first `visible`, building the narrowed rows in one new slab before anything
    /// is released, so a failure leaves `out` as it was. They take over the visible values (and the strings those
    /// own) from the old rows.
    fn project(
        visible: ?usize,
        workers: usize,
        gpa: std.mem.Allocator,
        out: *scan.AggOutput,
        col_types: ?*?[]const scan.GroupColType,
    ) error{OutOfMemory}!void {
        const n = visible orelse return;
        const rows = out.group_rows orelse return;
        const cols = out.group_cols.?;
        if (n >= cols.len) return;
        const slab = try gpa.alloc(scan.AggValue, rows.len * n);
        errdefer gpa.free(slab);
        const narrowed = try gpa.alloc([]const scan.AggValue, rows.len);
        errdefer gpa.free(narrowed);
        const names = try gpa.dupe([]const u8, cols[0..n]);
        const Narrow = struct {
            rows: []const []const scan.AggValue,
            narrowed: [][]const scan.AggValue,
            slab: []scan.AggValue,
            n: usize,
            out: *const scan.AggOutput,
            gpa: std.mem.Allocator,
            fn run(c: @This(), lo: usize, hi: usize) void {
                for (c.rows[lo..hi], c.narrowed[lo..hi], lo..) |row, *dst, i| {
                    const cells = c.slab[i * c.n ..][0..c.n];
                    @memcpy(cells, row[0..c.n]);
                    dst.* = cells;
                    // A hidden column's string may be the answer's to free or borrowed (`AggOutput.borrowed_strings`).
                    for (row[c.n..]) |v| c.out.freeValue(c.gpa, v);
                    c.out.releaseRowSlice(c.gpa, row);
                }
            }
        };
        eachRowBlock(workers, rows.len, Narrow{ .rows = rows, .narrowed = narrowed, .slab = slab, .n = n, .out = out, .gpa = gpa }, Narrow.run);
        gpa.free(rows);
        gpa.free(out.slab);
        for (cols[n..]) |c| gpa.free(c);
        gpa.free(cols);
        out.slab = slab;
        out.group_rows = narrowed;
        out.group_cols = names;
        if (col_types) |ct| if (ct.*) |types| {
            ct.* = types[0..n];
        };
    }

    /// Without GROUP BY the hidden terms are trailing aggregates; drop them from `aggs`.
    fn projectScalar(self: ResultProgram, gpa: std.mem.Allocator, out: *scan.AggOutput) error{OutOfMemory}!void {
        const n = self.visible orelse return;
        if (n >= out.aggs.len) return;
        const kept = try gpa.dupe(scan.AggOutputItem, out.aggs[0..n]);
        for (out.aggs[n..]) |item| {
            gpa.free(item.alias);
            freeValue(gpa, item.value);
        }
        gpa.free(out.aggs);
        out.aggs = kept;
    }

    /// Which result columns DuckDB plans with no GROUP BY and no FILTER, where an AVG is its sum over its count
    /// (`decimalMean`): by column (`finishPost`'s facts) or, for the scan's aggregates without GROUP BY, by call.
    const Plans = struct {
        rewritten: []const bool = &.{},
        calls: []const expr_agg.AggCall = &.{},

        fn of(self: Plans, c: usize) bool {
            if (c < self.rewritten.len) return self.rewritten[c];
            return c < self.calls.len and self.calls[c].where == null;
        }
    };

    /// Drop every group `having` does not make TRUE. The kept rows are listed before any is released.
    fn applyHaving(
        gpa: std.mem.Allocator,
        out: *scan.AggOutput,
        having: expr_ast.Having,
        plans: Plans,
    ) error{OutOfMemory}!void {
        const rows = out.group_rows orelse {
            const Scalar = struct {
                aggs: []const scan.AggOutputItem,
                plans: Plans,
                fn at(self: @This(), c: usize) scan.AggValue {
                    return planned(self.aggs[c].value, self.plans.of(c));
                }
            };
            out.no_row = truth(having, Scalar{ .aggs = out.aggs, .plans = plans }) != true;
            return;
        };
        const Row = struct {
            row: []const scan.AggValue,
            plans: Plans,
            fn at(self: @This(), c: usize) scan.AggValue {
                return planned(self.row[c], self.plans.of(c));
            }
        };
        var n: usize = 0;
        for (rows) |row| {
            if (truth(having, Row{ .row = row, .plans = plans }) == true) n += 1;
        }
        if (n == rows.len) return;
        const kept = try gpa.alloc([]const scan.AggValue, n);
        var k: usize = 0;
        for (rows) |row| {
            if (truth(having, Row{ .row = row, .plans = plans }) == true) {
                kept[k] = row;
                k += 1;
            } else out.releaseRow(gpa, row);
        }
        gpa.free(rows);
        out.group_rows = kept;
    }

    /// A DECIMAL AVG's pair as the DOUBLE of the plan with no GROUP BY and no FILTER, when it is that plan's; any other
    /// value as it is (`compareLiteral` reads a pair as the aggregate finalizes it).
    fn planned(v: scan.AggValue, rewritten: bool) scan.AggValue {
        if (!rewritten or v != .avg_dec) return v;
        const a = v.avg_dec;
        if (a.count == 0 or a.sum == null) return v;
        return .{ .f = decimalMean(a.sum.?, a.spec, a.count, true) };
    }

    /// A bare AVG of a DECIMAL that HAVING or ORDER BY compared (`AggCall.compared`) kept its exact pair for them;
    /// once they have, it answers what every bare one does: the exact mean rounded once to its DECIMAL type. Before
    /// the rows are projected, so the hidden ones are dropped as they are.
    fn printMeans(self: ResultProgram, out: *scan.AggOutput, col_types: ?[]const scan.GroupColType) Error!void {
        const rows = out.group_rows orelse {
            for (out.aggs, 0..) |*item, c| {
                if (item.value != .avg_dec or c >= self.calls.len or !self.calls[c].compared) continue;
                item.value = try printedMean(item.value.avg_dec, expr_agg.resultDec(self.calls[c]));
            }
            return;
        };
        const types = col_types orelse return;
        if (rows.len == 0) return;
        // A column is a compared AVG's when its pair sits in a DECIMAL lane (avg_state's lane is its own).
        for (types, 0..) |t, j| {
            if (t.lane != .dec or j >= rows[0].len or rows[0][j] != .avg_dec) continue;
            // The rows are the answer's own (its slab or its copies), so their cells may be rewritten in place.
            for (rows) |row| @constCast(row)[j] = try printedMean(row[j].avg_dec, t.dec.?);
        }
    }

    /// Three-valued truth of `h` for one result row (`row.at(column)` its value): null is UNKNOWN.
    pub fn truth(h: expr_ast.Having, row: anytype) ?bool {
        switch (h) {
            .all => |terms| {
                var unknown = false;
                for (terms) |t| {
                    const v = truth(t, row) orelse {
                        unknown = true;
                        continue;
                    };
                    if (!v) return false;
                }
                return if (unknown) null else true;
            },
            .any => |terms| {
                var unknown = false;
                for (terms) |t| {
                    const v = truth(t, row) orelse {
                        unknown = true;
                        continue;
                    };
                    if (v) return true;
                }
                return if (unknown) null else false;
            },
            .null_check => |nc| return isNull(row.at(nc.column)) != nc.is_not,
            .cmp => |c| {
                const ord = compareLiteral(row.at(c.column), c.literal) orelse return null;
                return c.op.holds(ord);
            },
            .cmp_col => |c| {
                const ord = compareCells(row.at(c.left), row.at(c.right)) orelse return null;
                return c.op.holds(ord);
            },
        }
    }

    /// `finish` for a result with columns computed here (`ResultOrder.post`). Each group's row is widened to the
    /// SELECT list followed by the hidden terms, the scan's cells moved to their places and the `derived` cells
    /// filled. HAVING's own columns come first, so no group HAVING drops is evaluated (a division it makes
    /// impossible, an overflow); the rest are computed for the groups it keeps, before the ranking reads them.
    fn finishPost(
        self: ResultProgram,
        gpa: std.mem.Allocator,
        out: *scan.AggOutput,
        col_types: ?*?[]const scan.GroupColType,
    ) Error!void {
        if (out.group_rows == null) return;
        const raw_types = col_types.?.*.?;
        const width = self.select.len + self.order.hidden.len;

        var scratch: std.heap.ArenaAllocator = .init(gpa);
        defer scratch.deinit();
        const arena = scratch.allocator();
        const types = try arena.alloc(scan.GroupColType, width);
        const ints = try arena.alloc(?expr_ast.IntType, width);
        const rewritten = try arena.alloc(bool, width);
        const programs = try arena.alloc(?*const Compiled, width);
        const grouped = self.grouped;
        var raw: usize = 0;
        // Without GROUP BY every scanned column is an aggregate (`.named`); with it, the `.agg` ones are, and each takes
        // the next call, as the scan binds them.
        var next_call: usize = 0;
        for (0..width) |j| {
            programs[j] = null;
            ints[j] = null;
            rewritten[j] = false;
            const role = self.column(j).role;
            if (role == .derived) continue;
            types[j] = raw_types[raw];
            raw += 1;
            if (role == .agg or role == .named) {
                if (next_call < self.calls.len) {
                    const source = if (next_call < self.sources.len) self.sources[next_call] else null;
                    ints[j] = scan.GroupColType.intType(self.calls[next_call], source);
                    rewritten[j] = !grouped and self.calls[next_call].where == null;
                }
                next_call += 1;
            }
        }
        std.debug.assert(raw == out.group_cols.?.len);
        for (0..width) |j| {
            const spec = self.column(j);
            if (spec.role != .derived) continue;
            const program = try compile(arena, spec.post.?.*, types, .{ .ints = ints, .rewritten = rewritten });
            programs[j] = program;
            types[j] = program.ty.column();
        }
        for (0..self.select.len) |j| for (0..j) |i| {
            if (std.ascii.eqlIgnoreCase(self.columnName(i), self.columnName(j))) return error.DuplicateOutputColumn;
        };
        if (self.order.having) |h| {
            const lanes = try arena.alloc(Lane, width);
            for (lanes, types) |*lane, t| lane.* = if (t.lane == .string) .string else .number;
            try checkHaving(h.*, lanes);
        }
        try self.widen(gpa, out, col_types, programs, types);

        const wanted = try arena.alloc(bool, width);
        const computed = try arena.alloc(bool, width);
        @memset(wanted, false);
        @memset(computed, false);
        if (self.order.having) |h| {
            const Reads = struct {
                wanted: []bool,
                fn mark(r: @This(), at: usize) void {
                    r.wanted[at] = true;
                }
            };
            h.eachColumn(Reads{ .wanted = wanted }, Reads.mark);
            try computeDerived(out.group_rows.?, programs, wanted, computed, self.workers);
            try applyHaving(gpa, out, h.*, .{ .rewritten = rewritten });
        }
        @memset(wanted, true);
        try computeDerived(out.group_rows.?, programs, wanted, computed, self.workers);
        const copies = try self.rank(gpa, out);
        defer if (copies) |c| gpa.free(c);
        try self.printMeans(out, col_types.?.*);
        try project(self.select.len, self.workers, gpa, out, col_types);
        if (copies) |c| try self.repeatRows(gpa, out, c);
        out.compactRows(gpa) catch {};
    }

    /// Give every row `programs.len` cells, the scan's in the places the underived columns hold and NULL in the rest.
    /// The names, the column types and the rows are all allocated before any is swapped in, so a refused allocation
    /// leaves `out` as it was; once swapped, the cells (and the strings they own) belong to the wide rows.
    fn widen(
        self: ResultProgram,
        gpa: std.mem.Allocator,
        out: *scan.AggOutput,
        col_types: ?*?[]const scan.GroupColType,
        programs: []const ?*const Compiled,
        types: []const scan.GroupColType,
    ) error{OutOfMemory}!void {
        const rows = out.group_rows.?;
        const raw_names = out.group_cols.?;
        const width = programs.len;
        const owned_types = try gpa.dupe(scan.GroupColType, types);
        errdefer gpa.free(owned_types);
        const names = try gpa.alloc([]const u8, width);
        errdefer gpa.free(names);
        var named: usize = 0;
        errdefer for (names[0..named], programs[0..named]) |name, program| if (program != null) gpa.free(name);
        var raw: usize = 0;
        for (names, programs) |*name, program| {
            if (program != null) {
                name.* = try gpa.dupe(u8, self.columnName(named));
            } else {
                name.* = raw_names[raw];
                raw += 1;
            }
            named += 1;
        }
        // Only the original, uncut rows in a slab with the final stride can be widened in place. Other producers
        // and already-cut answers keep the allocation-first path, whose failure leaves the answer whole.
        const reuse = out.slab.len == rows.len * width and for (rows, 0..) |row, i| {
            if (row.ptr != out.slab[i * width ..].ptr or row.len != raw_names.len) break false;
        } else true;
        const slab = if (reuse) out.slab else try gpa.alloc(scan.AggValue, rows.len * width);
        errdefer if (!reuse) gpa.free(slab);
        const wide = if (reuse) @constCast(rows) else try gpa.alloc([]const scan.AggValue, rows.len);
        const Widen = struct {
            rows: []const []const scan.AggValue,
            wide: [][]const scan.AggValue,
            slab: []scan.AggValue,
            programs: []const ?*const Compiled,
            out: *const scan.AggOutput,
            gpa: std.mem.Allocator,
            reuse: bool,
            fn run(c: @This(), lo: usize, hi: usize) void {
                const w = c.programs.len;
                for (c.rows[lo..hi], c.wide[lo..hi], lo..) |row, *dst, i| {
                    const cells = c.slab[i * w ..][0..w];
                    // Underived columns keep their relative order. Copy backwards so an inserted column never
                    // overwrites a raw cell still to be read when both views share the same row.
                    var at = row.len;
                    var j = w;
                    while (j > 0) {
                        j -= 1;
                        if (c.programs[j] != null) {
                            cells[j] = .null_val;
                        } else {
                            at -= 1;
                            cells[j] = row[at];
                        }
                    }
                    dst.* = cells;
                    if (!c.reuse) c.out.releaseRowSlice(c.gpa, row);
                }
            }
        };
        eachRowBlock(self.workers, rows.len, Widen{ .rows = rows, .wide = wide, .slab = slab, .programs = programs, .out = out, .gpa = gpa, .reuse = reuse }, Widen.run);
        if (!reuse) {
            gpa.free(rows);
            gpa.free(out.slab);
        }
        gpa.free(raw_names);
        gpa.free(out.owned_types);
        out.slab = slab;
        out.group_rows = wide;
        out.group_cols = names;
        out.owned_types = owned_types;
        col_types.?.* = owned_types;
    }

    /// `f(ctx, lo, hi)` over `n` rows in blocks, on up to `workers` threads; the blocks are disjoint, so each row is one
    /// thread's. Short answers, and a host that cannot start a thread, take one pass on this one.
    fn eachRowBlock(workers: usize, n: usize, ctx: anytype, comptime f: fn (@TypeOf(ctx), usize, usize) void) void {
        const block = 4096;
        const Task = struct {
            ctx: @TypeOf(ctx),
            cursor: *std.atomic.Value(usize),
            n: usize,
            fn run(t: *@This()) void {
                while (true) {
                    const lo = t.cursor.fetchAdd(block, .monotonic);
                    if (lo >= t.n) return;
                    f(t.ctx, lo, @min(t.n, lo + block));
                }
            }
        };
        const threads = @max(1, @min(workers, 64, n / block));
        var cursor: std.atomic.Value(usize) = .init(0);
        var storage: [64]Task = undefined;
        const tasks = storage[0..threads];
        for (tasks) |*t| t.* = .{ .ctx = ctx, .cursor = &cursor, .n = n };
        if (threads == 1 or !@import("spawn.zig").runTree(Task, tasks, .{ .stack_size = 1 << 20 }, Task.run)) tasks[0].run();
    }

    /// The scan's column result column `j` (the SELECT list, then the hidden terms) is, or null for one computed here.
    pub fn scanColumn(self: ResultProgram, j: usize) ?usize {
        if (!self.order.post) return j;
        if (self.column(j).role == .derived) return null;
        var raw: usize = 0;
        for (0..j) |i| raw += @intFromBool(self.column(i).role != .derived);
        return raw;
    }

    /// How many columns the finished answer shows, of the scan's `scan_cols`.
    pub fn shownColumns(self: ResultProgram, scan_cols: usize) usize {
        return if (self.order.post) self.select.len else self.visible orelse scan_cols;
    }

    /// Whether the ranking, HAVING or a computed column reads scan column `raw`: one whose value `finish` needs.
    pub fn readsScanColumn(self: ResultProgram, raw: usize) bool {
        for (self.order.keys) |k| if (self.readsThrough(k.column, raw, 0)) return true;
        if (self.order.having) |h| {
            const Hit = struct {
                p: ResultProgram,
                raw: usize,
                hit: *bool,
                fn mark(c: @This(), at: usize) void {
                    if (c.p.readsThrough(at, c.raw, 0)) c.hit.* = true;
                }
            };
            var hit = false;
            h.eachColumn(Hit{ .p = self, .raw = raw, .hit = &hit }, Hit.mark);
            if (hit) return true;
        }
        if (self.order.post) {
            const width = self.select.len + self.order.hidden.len;
            for (0..width) |j| if (self.column(j).role == .derived and self.readsThrough(j, raw, 0)) return true;
        }
        return false;
    }

    /// Whether result column `j` is scan column `raw`, or a computed column reading it, however deep.
    fn readsThrough(self: ResultProgram, j: usize, raw: usize, depth: usize) bool {
        if (self.scanColumn(j)) |r| return r == raw;
        const width = self.select.len + self.order.hidden.len;
        if (depth > width or j >= width) return true;
        const post = self.column(j).post orelse return false;
        return self.postReads(post, raw, depth + 1);
    }

    fn postReads(self: ResultProgram, e: *const expr_ast.PostExpr, raw: usize, depth: usize) bool {
        return switch (e.*) {
            .col => |c| self.readsThrough(c.column, raw, depth),
            .number => false,
            .neg, .abs => |x| self.postReads(x, raw, depth),
            .binary => |b| self.postReads(b.left, raw, depth) or self.postReads(b.right, raw, depth),
            .round => |r| self.postReads(r.value, raw, depth),
            .nullif => |n| self.postReads(n.value, raw, depth) or self.postReads(n.other, raw, depth),
        };
    }

    fn column(self: ResultProgram, j: usize) expr_ast.SelectColumn {
        return if (j < self.select.len) self.select[j] else self.order.hidden[j - self.select.len];
    }

    /// The name a visible result column prints under: its alias, else the expression as written.
    fn columnName(self: ResultProgram, j: usize) []const u8 {
        const spec = self.column(j);
        return spec.alias orelse spec.expr;
    }

    fn isNull(v: scan.AggValue) bool {
        return switch (v) {
            .null_val => true,
            .avg => |a| a.count == 0 or a.sum == null,
            .avg_dec => |a| a.count == 0 or a.mean == null,
            else => false,
        };
    }

    fn freeValue(gpa: std.mem.Allocator, v: scan.AggValue) void {
        switch (v) {
            .s => |s| gpa.free(s),
            else => {},
        }
    }
};

/// A compared AVG's pair as a bare AVG answers it: the exact mean rounded once to `spec`, its DECIMAL type.
fn printedMean(a: @FieldType(scan.AggValue, "avg_dec"), spec: expr_ast.Dec) Error!scan.AggValue {
    if (a.count == 0 or a.sum == null) return .null_val;
    var sum: expr_agg.DecSum = .{};
    sum.add(a.sum.?, @intCast(a.count));
    const v = (sum.meanAt(spec.scale - a.spec.scale) catch return error.DecimalOverflow) orelse return .null_val;
    return .{ .dec = .{ .v = v, .spec = spec } };
}

/// A HAVING number literal, typed by its text: an integer, an exact decimal (unscaled digits and scale), or a double.
pub const Number = union(enum) {
    int: i128,
    dec: struct { unscaled: i128, scale: u8 },
    float: f64,

    pub fn parse(text: []const u8) ?Number {
        if (std.fmt.parseInt(i128, text, 10)) |v| return .{ .int = v } else |_| {}
        const dot = std.mem.indexOfScalar(u8, text, '.');
        const exp = std.mem.indexOfAny(u8, text, "eE");
        if (dot) |d| if (exp == null) {
            const frac = text[d + 1 ..];
            if (frac.len <= expr_ast.Dec.max_precision) {
                var digits: [80]u8 = undefined;
                if (text.len <= digits.len) {
                    @memcpy(digits[0..d], text[0..d]);
                    @memcpy(digits[d..][0..frac.len], frac);
                    if (std.fmt.parseInt(i128, digits[0 .. d + frac.len], 10)) |u| {
                        return .{ .dec = .{ .unscaled = u, .scale = @intCast(frac.len) } };
                    } else |_| {}
                }
            }
        };
        const f = std.fmt.parseFloat(f64, text) catch return null;
        return .{ .float = f };
    }

    fn asF64(self: Number) f64 {
        return switch (self) {
            .int => |v| @floatFromInt(v),
            .dec => |d| @as(f64, @floatFromInt(d.unscaled)) / std.math.pow(f64, 10, @floatFromInt(d.scale)),
            .float => |f| f,
        };
    }
};

// LLVM cannot lower i192-to-f64 on ARM; the limb conversion preserves nearest-even rounding.
fn wideAsFloat(v: i192) f64 {
    const Limb = std.math.big.Limb;
    const width = @bitSizeOf(Limb);
    var limbs: [192 / width]Limb = undefined;
    const magnitude = @abs(v);
    for (&limbs, 0..) |*limb, i| limb.* = @truncate(magnitude >> @intCast(i * width));
    const number: std.math.big.int.Const = .{ .limbs = &limbs, .positive = v >= 0 };
    return number.toFloat(f64, .nearest_even)[0];
}

/// How `v` orders against `lit`, or null when the comparison is UNKNOWN: `v` NULL, a NULL literal, or kinds that do
/// not compare (refused when the query binds, see `checkHaving`). Integers and DECIMALs compare exactly against
/// integer and decimal literals; anything with a double compares as doubles, NaN above every number as zpq orders
/// floats; an AVG compares its mean as the double an expression reads (`decimalMean`), as SQL types it.
pub fn compareLiteral(v: scan.AggValue, lit: expr_ast.HavingLiteral) ?std.math.Order {
    if (lit == .null) return null;
    if (lit == .string) return switch (v) {
        .s => |s| std.mem.order(u8, s, lit.string),
        else => null,
    };
    const num = Number.parse(lit.number) orelse return null;
    return switch (v) {
        .null_val, .s => null,
        .i => |i| exactOrder(i, 0, num),
        .dec => |d| exactOrder(d.v, d.spec.scale, num),
        .f => |f| floatOrder(f, num.asF64()),
        .avg => |a| if (a.count == 0 or a.sum == null) null else floatOrder(a.sum.? / @as(f64, @floatFromInt(a.count)), num.asF64()),
        .avg_dec => |a| if (a.count == 0 or a.sum == null)
            null
        else
            floatOrder(decimalMean(a.sum.?, a.spec, a.count, false), num.asF64()),
    };
}

/// `unscaled / 10^scale` against `num`, exactly unless `num` is a double.
fn exactOrder(unscaled: i128, scale: u8, num: Number) std.math.Order {
    const lit_unscaled: i128, const lit_scale: u8 = switch (num) {
        .int => |i| .{ i, 0 },
        .dec => |d| .{ d.unscaled, d.scale },
        .float => |f| return floatOrder(@as(f64, @floatFromInt(unscaled)) / std.math.pow(f64, 10, @floatFromInt(scale)), f),
    };
    const top = @max(scale, lit_scale);
    const a = @as(i256, unscaled) * @as(i256, expr_ast.pow10(top - scale));
    const b = @as(i256, lit_unscaled) * @as(i256, expr_ast.pow10(top - lit_scale));
    return std.math.order(a, b);
}

fn floatOrder(a: f64, b: f64) std.math.Order {
    if (std.math.isNan(a) or std.math.isNan(b)) {
        return if (std.math.isNan(a) == std.math.isNan(b)) .eq else if (std.math.isNan(a)) .gt else .lt;
    }
    return std.math.order(a, b);
}

/// A result value as a number: exactly (unscaled digits and scale) or as a double. An AVG compares as its mean.
const CellNumber = union(enum) {
    exact: struct { v: i128, scale: u8 },
    float: f64,

    fn of(v: scan.AggValue) ?CellNumber {
        return switch (v) {
            .null_val, .s => null,
            .i => |i| .{ .exact = .{ .v = i, .scale = 0 } },
            .dec => |d| .{ .exact = .{ .v = d.v, .scale = d.spec.scale } },
            .f => |f| .{ .float = f },
            .avg => |a| if (a.count == 0 or a.sum == null) null else .{ .float = a.sum.? / @as(f64, @floatFromInt(a.count)) },
            .avg_dec => |a| if (a.count == 0 or a.sum == null)
                null
            else
                .{ .float = decimalMean(a.sum.?, a.spec, a.count, false) },
        };
    }

    fn asF64(self: CellNumber) f64 {
        return switch (self) {
            .exact => |e| decimalAsFloat(e.v, e.scale),
            .float => |f| f,
        };
    }
};

/// How two result values of one HAVING comparison order, or null when it is UNKNOWN: either is NULL, or the kinds do
/// not compare (refused before any row is read, see `checkHaving`). Exact against exact, else as doubles.
pub fn compareCells(a: scan.AggValue, b: scan.AggValue) ?std.math.Order {
    if (a == .s or b == .s) return if (a == .s and b == .s) std.mem.order(u8, a.s, b.s) else null;
    const x = CellNumber.of(a) orelse return null;
    const y = CellNumber.of(b) orelse return null;
    if (x == .exact and y == .exact) {
        const num: Number = if (y.exact.scale == 0)
            .{ .int = y.exact.v }
        else
            .{ .dec = .{ .unscaled = y.exact.v, .scale = y.exact.scale } };
        return exactOrder(x.exact.v, x.exact.scale, num);
    }
    return floatOrder(x.asF64(), y.asF64());
}

/// A DECIMAL as the double SQL casts it to: the unscaled value over 10^scale.
fn decimalAsFloat(v: i128, scale: u8) f64 {
    return @as(f64, @floatFromInt(v)) / @as(f64, @floatFromInt(expr_ast.pow10(scale)));
}

/// What a post-aggregate expression evaluates to, as DuckDB types it: `+ - *` stay exact over integers (each checked in
/// the SQL integer type DuckDB gives the operation) and DECIMALs (a DECIMAL when either side is, by `Dec`'s rules),
/// `/` and anything with a double are DOUBLE.
const PostType = union(enum) {
    int: expr_ast.IntType,
    dec: expr_ast.Dec,
    float,

    fn column(self: PostType) scan.GroupColType {
        return switch (self) {
            .int => .{ .lane = .i64, .nullable = true },
            .float => .{ .lane = .f64, .nullable = true },
            .dec => |d| .{ .lane = .dec, .dec = d, .nullable = true },
        };
    }

    /// The DECIMAL an operand of a DECIMAL operation takes: an integer widens to the DECIMAL of its type's width.
    fn asDec(self: PostType) expr_ast.Dec {
        return switch (self) {
            .dec => |d| d,
            .int => |t| expr_ast.Dec.ofInt(t.digits()),
            .float => unreachable,
        };
    }

    fn scale(self: PostType) u8 {
        return switch (self) {
            .dec => |d| d.scale,
            else => 0,
        };
    }
};

/// A `PostExpr` with its types resolved against the result columns' lanes, once, before any group is read.
const Compiled = struct {
    ty: PostType,
    node: Node,
    /// An integer literal's value: DuckDB gives a literal the type of the other operand when the value fits it, else
    /// its own (INTEGER, BIGINT, HUGEINT), so it is typed where it meets an operator.
    lit: ?i128 = null,

    const Node = union(enum) {
        col: usize,
        /// An `avg_state` pair read as the double SQL types an AVG, by the formula DuckDB's plan for that AVG uses.
        mean: struct { column: usize, rewritten: bool },
        int: i128,
        dec: i128,
        float: f64,
        neg: *const Compiled,
        arith: struct { op: expr_ast.PostOp, left: *const Compiled, right: *const Compiled },
        round: struct { value: *const Compiled, digits: i32 },
        abs: *const Compiled,
        nullif: struct { value: *const Compiled, other: *const Compiled },
    };
};

/// Only plain numbers do arithmetic: a date, a time, a timestamp or a boolean passing through min/max does not (its
/// SQL arithmetic is not this one).
fn plainNumber(source: ?schema.SchemaElement) bool {
    const src = source orelse return true;
    if (src.type == .BOOLEAN or src.type == .INT96) return false;
    const lt = src.effectiveLogicalType() orelse return true;
    return switch (lt) {
        .DATE, .TIME, .TIMESTAMP => false,
        else => true,
    };
}

/// The min or max of a FLOAT column is a FLOAT, whose SQL arithmetic is in single precision: zpq's double lane would
/// print other digits (`max(f) + 1` is 1.1 as a FLOAT, 1.1000000014901161 as a double). A FLOAT16 is no SQL type here.
fn singlePrecision(source: ?schema.SchemaElement) bool {
    const src = source orelse return false;
    return src.type == .FLOAT or schema.isFloat16(src);
}

/// A decimal literal's type, as DuckDB gives it: its digits, at least one before the point.
fn literalDec(unscaled: i128, scale: u8) expr_ast.Dec {
    var digits: u8 = 1;
    var rest = @abs(unscaled);
    while (rest >= 10) : (rest /= 10) digits += 1;
    return .{ .precision = @max(digits, scale + 1), .scale = scale };
}

/// The type of `+`, `-` or `*` between two integer-typed operands. A literal takes the other operand's type when its
/// value fits there (`max(tinyint) + 1` is TINYINT), and is its own otherwise (`max(tinyint) + 1000` is INTEGER).
fn intOperation(l: *const Compiled, r: *const Compiled) expr_ast.IntType {
    const lt = l.ty.int;
    const rt = r.ty.int;
    if (l.lit) |v| if (r.lit == null and rt.holds(v)) return rt;
    if (r.lit) |v| if (l.lit == null and lt.holds(v)) return lt;
    return expr_ast.IntType.combine(lt, rt);
}

/// The DECIMAL type of `a * b`: the sum of the precisions and scales, but two operands that fit 18 digits give at most
/// 18, as DuckDB does (their product is held in 64 bits, and a value past 10^18 is an overflow error).
fn mulDec(a: expr_ast.Dec, b: expr_ast.Dec) error{DecimalScaleTooLarge}!expr_ast.Dec {
    const wide = try expr_ast.Dec.mul(a, b);
    if (a.precision > 18 or b.precision > 18) return wide;
    return .{ .precision = @max(@min(wide.precision, 18), wide.scale), .scale = wide.scale };
}

/// What `compile` knows of the scan's columns beside their lanes.
const Facts = struct {
    /// Each column's SQL integer type, where it has one.
    ints: []const ?expr_ast.IntType,
    /// An AVG column DuckDB plans as `CAST(sum AS DOUBLE) / CAST(count AS DOUBLE)`: one of a query with no GROUP BY, and
    /// of a call with no FILTER. Every other AVG is finalized from its sum and count by the aggregate itself.
    rewritten: []const bool,
};

fn compile(
    arena: std.mem.Allocator,
    e: expr_ast.PostExpr,
    types: []const scan.GroupColType,
    facts: Facts,
) Error!*const Compiled {
    const out = try arena.create(Compiled);
    out.* = switch (e) {
        .col => |c| blk: {
            const t = types[c.column];
            if (singlePrecision(t.source)) return error.PostAggTypeUnknown;
            if (!plainNumber(t.source)) return error.TypeMismatch;
            if (c.mean) {
                // An AVG is DOUBLE whatever it averages, from the exact sum and count (`avg_state`), never from the
                // DECIMAL mean zpq answers a bare AVG of a DECIMAL with.
                if (t.lane != .avg and t.lane != .avg_dec) return error.TypeMismatch;
                break :blk .{ .ty = .float, .node = .{ .mean = .{ .column = c.column, .rewritten = facts.rewritten[c.column] } } };
            }
            break :blk switch (t.lane) {
                // A min or max of an expression has no integer type zpq resolves.
                .i64 => .{ .ty = .{ .int = facts.ints[c.column] orelse return error.PostAggTypeUnknown }, .node = .{ .col = c.column } },
                .f64 => .{ .ty = .float, .node = .{ .col = c.column } },
                .dec => .{ .ty = .{ .dec = t.dec.? }, .node = .{ .col = c.column } },
                .string, .avg, .avg_dec => return error.TypeMismatch,
            };
        },
        .number => |text| switch (Number.parse(text) orelse return error.TypeMismatch) {
            .int => |v| .{ .ty = .{ .int = expr_ast.IntType.ofValue(v) }, .node = .{ .int = v }, .lit = v },
            .dec => |d| .{ .ty = .{ .dec = literalDec(d.unscaled, d.scale) }, .node = .{ .dec = d.unscaled } },
            .float => |f| .{ .ty = .float, .node = .{ .float = f } },
        },
        .neg => |inner| blk: {
            const v = try compile(arena, inner.*, types, facts);
            // `-2147483648` is the literal INTEGER minimum, not the negation of a BIGINT.
            if (v.lit) |n| {
                const m = std.math.negate(n) catch return error.IntegerOverflowSub;
                break :blk .{ .ty = .{ .int = expr_ast.IntType.ofValue(m) }, .node = .{ .int = m }, .lit = m };
            }
            break :blk .{ .ty = v.ty, .node = .{ .neg = v } };
        },
        .binary => |b| blk: {
            const left = try compile(arena, b.left.*, types, facts);
            const right = try compile(arena, b.right.*, types, facts);
            const ty: PostType = if (b.op == .div or left.ty == .float or right.ty == .float)
                .float
            else if (left.ty == .dec or right.ty == .dec)
                .{ .dec = if (b.op == .mul)
                    try mulDec(left.ty.asDec(), right.ty.asDec())
                else
                    expr_ast.Dec.addSub(left.ty.asDec(), right.ty.asDec()) }
            else
                .{ .int = intOperation(left, right) };
            break :blk .{ .ty = ty, .node = .{ .arith = .{ .op = b.op, .left = left, .right = right } } };
        },
        .round => |r| blk: {
            const v = try compile(arena, r.value.*, types, facts);
            // Fewer digits than the scale: the type keeps its precision at the digits (none below the point for a
            // negative count); otherwise rounding changes nothing. DuckDB 2.0 gives a whole number rounded to tens, to
            // hundreds... one more digit (`round(95::DECIMAL(2,0), -1)` is DECIMAL(3,0)), up to 38: only there can the
            // rounded value leave the type.
            const ty: PostType = switch (v.ty) {
                .dec => |d| .{ .dec = expr_ast.roundedDec(d, r.digits) },
                else => v.ty,
            };
            break :blk .{ .ty = ty, .node = .{ .round = .{ .value = v, .digits = r.digits } } };
        },
        .abs => |inner| blk: {
            const v = try compile(arena, inner.*, types, facts);
            break :blk .{ .ty = v.ty, .node = .{ .abs = v } };
        },
        // The value's type, as DuckDB's: `other` only decides which rows are NULL.
        .nullif => |n| blk: {
            const value = try compile(arena, n.value.*, types, facts);
            const other = try compile(arena, n.other.*, types, facts);
            break :blk .{ .ty = value.ty, .node = .{ .nullif = .{ .value = value, .other = other } } };
        },
    };
    return out;
}

/// A value of a `PostType`, which the type says how to read: integers and DECIMALs (their unscaled digits) in `i`.
const Raw = union { i: i128, f: f64 };

fn asFloat(ty: PostType, raw: Raw) f64 {
    return switch (ty) {
        .float => raw.f,
        .int => @floatFromInt(raw.i),
        .dec => |d| decimalAsFloat(raw.i, d.scale),
    };
}

/// An integer result, which must lie in its SQL type: DuckDB's overflow error, in that type's range, not i128's.
fn inType(t: expr_ast.IntType, v: i128, err: Error) Error!i128 {
    return if (t.holds(v)) v else err;
}

fn eval(node: *const Compiled, row: []const scan.AggValue) Error!?Raw {
    switch (node.node) {
        .col => |c| return switch (row[c]) {
            .null_val => null,
            .i => |v| .{ .i = v },
            .f => |f| .{ .f = f },
            .dec => |d| .{ .i = d.v },
            else => error.TypeMismatch,
        },
        .mean => |m| return switch (row[m.column]) {
            .null_val => null,
            .avg => |a| if (a.count == 0) null else .{ .f = (a.sum orelse return null) / @as(f64, @floatFromInt(a.count)) },
            .avg_dec => |a| if (a.count == 0) null else .{ .f = decimalMean(a.sum orelse return null, a.spec, a.count, m.rewritten) },
            else => error.TypeMismatch,
        },
        .int, .dec => |v| return .{ .i = v },
        .float => |f| return .{ .f = f },
        .neg => |inner| {
            const v = (try eval(inner, row)) orelse return null;
            return switch (node.ty) {
                .float => .{ .f = -v.f },
                .int => |t| .{ .i = try inType(t, std.math.negate(v.i) catch return error.IntegerOverflowSub, error.IntegerOverflowSub) },
                .dec => .{ .i = std.math.negate(v.i) catch return error.IntegerOverflowSub },
            };
        },
        .arith => |a| {
            const l = (try eval(a.left, row)) orelse return null;
            const r = (try eval(a.right, row)) orelse return null;
            switch (node.ty) {
                .float => {
                    const x = asFloat(a.left.ty, l);
                    const y = asFloat(a.right.ty, r);
                    return .{
                        .f = switch (a.op) {
                            .add => x + y,
                            .sub => x - y,
                            .mul => x * y,
                            // IEEE, as DuckDB's `/`: a zero divisor gives an infinity or NaN, never an error.
                            .div => x / y,
                        },
                    };
                },
                .int => |t| return .{ .i = switch (a.op) {
                    .add => try inType(t, std.math.add(i128, l.i, r.i) catch return error.IntegerOverflowAdd, error.IntegerOverflowAdd),
                    .sub => try inType(t, std.math.sub(i128, l.i, r.i) catch return error.IntegerOverflowSub, error.IntegerOverflowSub),
                    .mul => try inType(t, std.math.mul(i128, l.i, r.i) catch return error.IntegerOverflowMul, error.IntegerOverflowMul),
                    .div => unreachable,
                } },
                .dec => |spec| {
                    // Two 38-digit operands brought to a common scale fit 256 bits; the result is checked against
                    // the type, so an overflow is an error, never a wrapped value.
                    const x: i256 = l.i;
                    const y: i256 = r.i;
                    const v: i256 = switch (a.op) {
                        .add, .sub => blk: {
                            const p = x * expr_ast.pow10(spec.scale - a.left.ty.scale());
                            const q = y * expr_ast.pow10(spec.scale - a.right.ty.scale());
                            break :blk if (a.op == .add) p + q else p - q;
                        },
                        .mul => x * y,
                        .div => unreachable,
                    };
                    if (@abs(v) >= @as(u256, @intCast(expr_ast.pow10(spec.precision)))) return error.DecimalOverflow;
                    return .{ .i = @intCast(v) };
                },
            }
        },
        .abs => |inner| {
            const v = (try eval(inner, row)) orelse return null;
            return switch (node.ty) {
                .float => .{ .f = @abs(v.f) },
                // DuckDB: "Overflow on abs(-2147483648)" for the minimum of the type.
                .int => |t| .{ .i = try inType(t, if (v.i < 0) std.math.negate(v.i) catch return error.IntegerOverflowAbs else v.i, error.IntegerOverflowAbs) },
                .dec => .{ .i = if (v.i < 0) std.math.negate(v.i) catch return error.IntegerOverflowAbs else v.i },
            };
        },
        .nullif => |n| {
            const v = (try eval(n.value, row)) orelse return null;
            const other = (try eval(n.other, row)) orelse return v;
            return if (rawOrder(n.value.ty, v, n.other.ty, other) == .eq) null else v;
        },
        .round => |r| {
            const v = (try eval(r.value, row)) orelse return null;
            return switch (r.value.ty) {
                .float => .{ .f = roundDouble(v.f, r.digits) },
                // An integer rounds to the same type, and DuckDB raises an overflow when the rounded value leaves it.
                .int => |t| .{ .i = try inType(t, try roundDecimal(v.i, 0, r.digits), error.IntegerOverflowRound) },
                // The result keeps the argument's precision, which a rounding up to the next power of ten can pass
                // (`round(99, -1)` of a DECIMAL(2, 0)): DuckDB's "Overflow in ROUND of DECIMAL(w)".
                .dec => |d| .{ .i = try inPrecision(node.ty.asDec(), try roundDecimal(v.i, d.scale, r.digits)) },
            };
        },
    }
}

/// `v`, which must be a value of the DECIMAL `spec`: fewer digits than its precision.
fn inPrecision(spec: expr_ast.Dec, v: i128) error{DecimalOverflow}!i128 {
    return if (@abs(v) >= @as(u128, @intCast(expr_ast.pow10(spec.precision)))) error.DecimalOverflow else v;
}

/// The double nearest `sum / 10^scale`, ties to even: DuckDB's cast of a DECIMAL to DOUBLE.
fn decimalAsDouble(sum: i192, scale: u8) f64 {
    return expr_ast.decimalToDouble(sum, scale);
}

/// How two values of `PostType`s order: exactly when both are integers or DECIMALs (at the larger scale), else as
/// doubles. NaN orders equal to itself and above every number, as zpq orders floats.
fn rawOrder(lt: PostType, l: Raw, rt: PostType, r: Raw) std.math.Order {
    if (lt == .float or rt == .float) return floatOrder(asFloat(lt, l), asFloat(rt, r));
    const scale = @max(lt.scale(), rt.scale());
    const x = @as(i256, l.i) * @as(i256, expr_ast.pow10(scale - lt.scale()));
    const y = @as(i256, r.i) * @as(i256, expr_ast.pow10(scale - rt.scale()));
    return std.math.order(x, y);
}

fn roundDouble(x: f64, digits: i32) f64 {
    return expr_ast.roundDouble(x, digits);
}

fn roundDecimal(v: i128, scale: u8, digits: i32) error{DecimalOverflow}!i128 {
    return expr_ast.roundDecimal(v, scale, digits);
}

fn postCell(ty: PostType, v: ?Raw) scan.AggValue {
    const raw = v orelse return .null_val;
    return switch (ty) {
        .int => .{ .i = raw.i },
        .float => .{ .f = raw.f },
        .dec => |d| .{ .dec = .{ .v = raw.i, .spec = d } },
    };
}

/// Fill the derived cells of `rows` for the columns in `wanted` not yet `computed`.
fn computeDerived(
    rows: []const []const scan.AggValue,
    programs: []const ?*const Compiled,
    wanted: []const bool,
    computed: []bool,
    workers: usize,
) Error!void {
    for (programs, wanted, computed, 0..) |program, want, *done, j| {
        const p = program orelse continue;
        if (!want or done.*) continue;
        const Fill = struct {
            rows: []const []const scan.AggValue,
            program: *const Compiled,
            column: usize,
            failed: *std.atomic.Value(usize),
            fn run(c: @This(), lo: usize, hi: usize) void {
                for (lo..hi) |i| {
                    const value = eval(c.program, c.rows[i]) catch {
                        _ = c.failed.fetchMin(i, .monotonic);
                        return;
                    };
                    @constCast(c.rows[i])[c.column] = postCell(c.program.ty, value);
                }
            }
        };
        var failed: std.atomic.Value(usize) = .init(std.math.maxInt(usize));
        ResultProgram.eachRowBlock(workers, rows.len, Fill{
            .rows = rows,
            .program = p,
            .column = j,
            .failed = &failed,
        }, Fill.run);
        const first = failed.load(.monotonic);
        if (first < rows.len) {
            // Evaluation is pure, and preceding columns are complete: replay the first failed row for its error.
            _ = try eval(p, rows[first]);
            unreachable;
        }
        done.* = true;
    }
}

/// What a result column holds, for checking HAVING literals against it before any row is read.
pub const Lane = enum { number, string };

/// Refuse a HAVING leaf whose literal cannot compare with its column: a string against a number or the reverse, or a
/// number literal that is none.
pub fn checkHaving(h: expr_ast.Having, lanes: []const Lane) error{HavingTypeMismatch}!void {
    switch (h) {
        .all, .any => |terms| for (terms) |t| try checkHaving(t, lanes),
        .null_check => {},
        .cmp => |c| switch (c.literal) {
            .null => {},
            .string => if (lanes[c.column] != .string) return error.HavingTypeMismatch,
            .number => |text| if (lanes[c.column] != .number or Number.parse(text) == null) return error.HavingTypeMismatch,
        },
        .cmp_col => |c| if (lanes[c.left] != lanes[c.right]) return error.HavingTypeMismatch,
    }
}

test "finish ranks by hidden columns, cuts once, then drops them" {
    const gpa = std.testing.allocator;
    var out: scan.AggOutput = .{};
    defer out.deinit(gpa);
    // Rows (k, hidden n): ranked by n DESC, ties by first appearance; k is a string the rows own.
    const ks = [_][]const u8{ "a", "b", "c", "d" };
    const ns = [_]i128{ 1, 5, 5, 3 };
    const rows = try gpa.alloc([]const scan.AggValue, ks.len);
    @memset(rows, &.{});
    out.group_rows = rows;
    for (rows, ks, ns) |*row, k, n| {
        const r = try gpa.alloc(scan.AggValue, 2);
        r[0] = .null_val;
        r[1] = .{ .i = n };
        row.* = r;
        const owned = try gpa.dupe(u8, k);
        r[0] = .{ .s = owned };
    }
    const cols = try gpa.alloc([]const u8, 2);
    @memset(cols, "");
    out.group_cols = cols;
    cols[0] = try gpa.dupe(u8, "k");
    cols[1] = try gpa.dupe(u8, "");
    const types = [_]scan.GroupColType{ .{ .lane = .string, .nullable = true }, .{ .lane = .i64, .nullable = false } };
    var col_types: ?[]const scan.GroupColType = &types;

    const program: ResultProgram = .{
        .order = .{ .keys = &.{.{ .column = 1, .descending = true }}, .limit = 2, .offset = 1 },
        .visible = 1,
    };
    try program.finish(gpa, &out, &col_types);
    try std.testing.expectEqual(@as(usize, 1), out.group_cols.?.len);
    try std.testing.expectEqual(@as(usize, 1), col_types.?.len);
    const got = out.group_rows.?;
    try std.testing.expectEqual(@as(usize, 2), got.len);
    for (got, [_][]const u8{ "c", "d" }) |row, want| {
        try std.testing.expectEqual(@as(usize, 1), row.len);
        try std.testing.expectEqualStrings(want, row[0].s);
    }
}

/// Rows (k, hidden n) whose strings the rows own: a x1, b x3, c x0, d x2.
fn testRepeatRows(gpa: std.mem.Allocator) !scan.AggOutput {
    var out: scan.AggOutput = .{};
    errdefer out.deinit(gpa);
    const ks = [_][]const u8{ "d", "b", "a", "c" };
    const ns = [_]i128{ 2, 3, 1, 0 };
    const rows = try gpa.alloc([]const scan.AggValue, ks.len);
    @memset(rows, &.{});
    out.group_rows = rows;
    for (rows, ks, ns) |*row, k, n| {
        const r = try gpa.alloc(scan.AggValue, 2);
        r[0] = .null_val;
        r[1] = .{ .i = n };
        row.* = r;
        const owned = try gpa.dupe(u8, k);
        r[0] = .{ .s = owned };
    }
    const cols = try gpa.alloc([]const u8, 2);
    @memset(cols, "");
    out.group_cols = cols;
    cols[0] = try gpa.dupe(u8, "k");
    cols[1] = try gpa.dupe(u8, "");
    return out;
}

test "finish shows each row as often as its hidden count says; OFFSET and LIMIT count the rows shown" {
    const gpa = std.testing.allocator;
    const types = [_]scan.GroupColType{ .{ .lane = .string, .nullable = true }, .{ .lane = .i64, .nullable = false } };
    const by_k = [_]expr_ast.OrderKey{.{ .column = 0, .descending = false }};
    // Ranked a, b, b, b, d, d: every cut of it, inside a group and across groups.
    const all = [_][]const u8{ "a", "b", "b", "b", "d", "d" };
    for ([_][2]u64{ .{ 0, 99 }, .{ 2, 3 }, .{ 0, 1 }, .{ 1, 1 }, .{ 3, 2 }, .{ 5, 9 }, .{ 6, 2 }, .{ 0, 0 }, .{ 4, 0 } }) |cut| {
        var out = try testRepeatRows(gpa);
        defer out.deinit(gpa);
        var col_types: ?[]const scan.GroupColType = &types;
        var budget: core_memory.SharedBudget = .{ .limit = 1 << 20 };
        const program: ResultProgram = .{
            .order = .{ .keys = &by_k, .offset = cut[0], .limit = cut[1], .repeat = 1 },
            .visible = 1,
            .budget = &budget,
        };
        try program.finish(gpa, &out, &col_types);
        const lo: usize = @intCast(@min(cut[0], all.len));
        const want = all[lo..@min(all.len, lo + cut[1])];
        const got = out.group_rows.?;
        try std.testing.expectEqual(want.len, got.len);
        try std.testing.expectEqual(@as(usize, 1), out.group_cols.?.len);
        for (got, want) |row, k| {
            try std.testing.expectEqual(@as(usize, 1), row.len);
            try std.testing.expectEqualStrings(k, row[0].s);
        }
        // What the copies hold is drawn from the ledger and goes back with the answer.
        try std.testing.expect(want.len == 0 or budget.used.load(.monotonic) != 0);
        out.deinit(gpa);
        try std.testing.expectEqual(@as(usize, 0), budget.used.load(.monotonic));
    }
    // No limit: every row. And rows the ledger cannot hold refuse, the groups left ranked and whole.
    var out = try testRepeatRows(gpa);
    defer out.deinit(gpa);
    var col_types: ?[]const scan.GroupColType = &types;
    var tight: core_memory.SharedBudget = .{ .limit = 64 };
    const program: ResultProgram = .{ .order = .{ .keys = &by_k, .repeat = 1 }, .visible = 1, .budget = &tight };
    try std.testing.expectError(error.ExceededMemoryBudget, program.finish(gpa, &out, &col_types));
    try std.testing.expectEqual(@as(usize, 4), out.group_rows.?.len);
    try std.testing.expectEqual(@as(usize, 0), tight.used.load(.monotonic));
    const roomy: ResultProgram = .{ .order = .{ .keys = &by_k, .repeat = 1 }, .visible = 1 };
    // Already ranked and projected by the refused run: repeating is all that is left to do.
    try roomy.repeatRows(gpa, &out, &.{ 1, 3, 0, 2 });
    try std.testing.expectEqual(@as(usize, 6), out.group_rows.?.len);
}

test "a repeat that runs out of memory leaks nothing and frees no string twice" {
    try std.testing.checkAllAllocationFailures(std.testing.allocator, struct {
        fn run(gpa: std.mem.Allocator) !void {
            var out = try testRepeatRows(gpa);
            defer out.deinit(gpa);
            const types = [_]scan.GroupColType{ .{ .lane = .string, .nullable = true }, .{ .lane = .i64, .nullable = false } };
            var col_types: ?[]const scan.GroupColType = &types;
            const program: ResultProgram = .{
                .order = .{ .keys = &.{.{ .column = 0, .descending = true }}, .offset = 1, .limit = 4, .repeat = 1 },
                .visible = 1,
            };
            try program.finish(gpa, &out, &col_types);
            try std.testing.expectEqual(@as(usize, 4), out.group_rows.?.len);
        }
    }.run, .{});
}

test "finish leaves the answer whole when it runs out of memory" {
    const Probe = struct {
        fn run(gpa: std.mem.Allocator) !void {
            var out: scan.AggOutput = .{};
            defer out.deinit(gpa);
            const rows = try gpa.alloc([]const scan.AggValue, 3);
            @memset(rows, &.{});
            out.group_rows = rows;
            for (rows, 0..) |*row, i| {
                const r = try gpa.alloc(scan.AggValue, 2);
                r[0] = .{ .i = @intCast(i) };
                r[1] = .null_val;
                row.* = r;
                // Bound first: a union initialized with a `try` inside may take its tag before the payload fails.
                const owned = try gpa.dupe(u8, "hidden");
                r[1] = .{ .s = owned };
            }
            const cols = try gpa.alloc([]const u8, 2);
            @memset(cols, "");
            out.group_cols = cols;
            for (cols) |*c| c.* = try gpa.dupe(u8, "c");
            const program: ResultProgram = .{ .order = .{ .keys = &.{.{ .column = 0, .descending = true }} }, .visible = 1 };
            try program.finish(gpa, &out, null);
            try std.testing.expectEqual(@as(i128, 2), out.group_rows.?[0][0].i);
        }
    };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Probe.run, .{});
}

test "finish leaves every row as it was when an allocation fails part way" {
    const gpa = std.testing.allocator;
    var fail_index: usize = 0;
    while (true) : (fail_index += 1) {
        var out: scan.AggOutput = .{};
        defer out.deinit(gpa);
        const rows = try gpa.alloc([]const scan.AggValue, 4);
        @memset(rows, &.{});
        out.group_rows = rows;
        for (rows, 0..) |*row, i| {
            const r = try gpa.alloc(scan.AggValue, 2);
            r[0] = .{ .i = @intCast(i) };
            r[1] = .null_val;
            row.* = r;
            const owned = try gpa.dupe(u8, "hidden");
            r[1] = .{ .s = owned };
        }
        const cols = try gpa.alloc([]const u8, 2);
        @memset(cols, "");
        out.group_cols = cols;
        for (cols) |*c| c.* = try gpa.dupe(u8, "c");

        var failing: std.testing.FailingAllocator = .init(gpa, .{ .fail_index = fail_index });
        const program: ResultProgram = .{ .visible = 1 };
        program.finish(failing.allocator(), &out, null) catch |err| {
            try std.testing.expectEqual(error.OutOfMemory, err);
            // Untouched: every row still wide, the hidden strings still owned.
            try std.testing.expectEqual(@as(usize, 2), out.group_cols.?.len);
            for (out.group_rows.?, 0..) |row, i| {
                try std.testing.expectEqual(@as(usize, 2), row.len);
                try std.testing.expectEqual(@as(i128, @intCast(i)), row[0].i);
                try std.testing.expectEqualStrings("hidden", row[1].s);
            }
            continue;
        };
        for (out.group_rows.?) |row| try std.testing.expectEqual(@as(usize, 1), row.len);
        break;
    }
    try std.testing.expect(fail_index > 0);
}

test "HAVING literals compare exactly against integers and DECIMALs, as doubles otherwise" {
    const lit = struct {
        fn n(text: []const u8) expr_ast.HavingLiteral {
            return .{ .number = text };
        }
    }.n;
    const O = std.math.Order;
    const dec: scan.AggValue = .{ .dec = .{ .v = 12345, .spec = .{ .precision = 10, .scale = 2 } } }; // 123.45
    try std.testing.expectEqual(@as(?O, .eq), compareLiteral(dec, lit("123.45")));
    try std.testing.expectEqual(@as(?O, .eq), compareLiteral(dec, lit("123.4500")));
    try std.testing.expectEqual(@as(?O, .gt), compareLiteral(dec, lit("123")));
    try std.testing.expectEqual(@as(?O, .lt), compareLiteral(dec, lit("123.451")));
    // Past 2^53 a double cannot tell these apart; the exact comparison can.
    const big: scan.AggValue = .{ .i = (1 << 60) + 1 };
    try std.testing.expectEqual(@as(?O, .gt), compareLiteral(big, lit("1152921504606846976")));
    try std.testing.expectEqual(@as(?O, .lt), compareLiteral(.{ .i = 100000 }, lit("100000.5")));
    try std.testing.expectEqual(@as(?O, .gt), compareLiteral(.{ .f = std.math.nan(f64) }, lit("1e300")));
    try std.testing.expectEqual(@as(?O, .eq), compareLiteral(.{ .avg = .{ .sum = 7, .count = 2 } }, lit("3.5")));
    try std.testing.expectEqual(@as(?O, null), compareLiteral(.{ .avg = .{ .sum = null, .count = 0 } }, lit("0")));
    try std.testing.expectEqual(@as(?O, null), compareLiteral(.null_val, lit("0")));
    try std.testing.expectEqual(@as(?O, .lt), compareLiteral(.{ .s = "abc" }, .{ .string = "abd" }));
    try std.testing.expectError(error.HavingTypeMismatch, checkHaving(.{ .cmp = .{ .column = 0, .op = .eq, .literal = .{ .string = "x" } } }, &.{.number}));
}

test "HAVING keeps only TRUE groups before the cut; without GROUP BY it may leave no row" {
    const gpa = std.testing.allocator;
    var out: scan.AggOutput = .{};
    defer out.deinit(gpa);
    // Rows (k, n): n NULL in one row, which neither `n > 2` nor its negation keeps.
    const ns = [_]?i128{ 1, 5, null, 3, 4 };
    const rows = try gpa.alloc([]const scan.AggValue, ns.len);
    @memset(rows, &.{});
    out.group_rows = rows;
    for (rows, ns, 0..) |*row, n, i| {
        const r = try gpa.alloc(scan.AggValue, 2);
        r[0] = .{ .i = @intCast(i) };
        r[1] = if (n) |v| .{ .i = v } else .null_val;
        row.* = r;
    }
    const cols = try gpa.alloc([]const u8, 2);
    @memset(cols, "");
    out.group_cols = cols;
    const gt2: expr_ast.Having = .{ .cmp = .{ .column = 1, .op = .gt, .literal = .{ .number = "2" } } };
    const program: ResultProgram = .{ .order = .{
        .keys = &.{.{ .column = 1, .descending = false }},
        .limit = 2,
        .having = &gt2,
    } };
    // A cut before HAVING would keep 1 and 3; the masked order keeps nothing from the strategies.
    try std.testing.expectEqual(@as(?u64, null), ResultProgram.cutOrder(program.order).limit);
    try program.finish(gpa, &out, null);
    const got = out.group_rows.?;
    try std.testing.expectEqual(@as(usize, 2), got.len);
    try std.testing.expectEqual(@as(i128, 3), got[0][1].i);
    try std.testing.expectEqual(@as(i128, 4), got[1][1].i);

    var scalar: scan.AggOutput = .{};
    defer scalar.deinit(gpa);
    scalar.aggs = try gpa.alloc(scan.AggOutputItem, 2);
    scalar.aggs[0] = .{ .alias = try gpa.dupe(u8, "c"), .value = .{ .i = 0 } };
    scalar.aggs[1] = .{ .alias = try gpa.dupe(u8, "hidden"), .value = .{ .i = 0 } };
    const pos: expr_ast.Having = .{ .cmp = .{ .column = 1, .op = .gt, .literal = .{ .number = "0" } } };
    const global: ResultProgram = .{ .order = .{ .having = &pos }, .visible = 1 };
    try global.finish(gpa, &scalar, null);
    try std.testing.expect(scalar.no_row);
    try std.testing.expectEqual(@as(usize, 1), scalar.aggs.len);
}

test "a compared AVG over a DECIMAL filters and ranks by its DOUBLE, then answers its rounded mean" {
    const gpa = std.testing.allocator;
    var out: scan.AggOutput = .{};
    defer out.deinit(gpa);
    // Rows (g, avg of a DECIMAL(4, 0)): one 1 among g values. 1/200, 1/201 and 1/202 all print 0.0050 at DECIMAL(8, 4);
    // only the DOUBLE tells them apart, as DuckDB's does.
    const groups = [_]i64{ 200, 201, 202 };
    const rows = try gpa.alloc([]const scan.AggValue, groups.len);
    @memset(rows, &.{});
    out.group_rows = rows;
    for (rows, groups) |*row, g| {
        const r = try gpa.alloc(scan.AggValue, 2);
        r[0] = .{ .i = g };
        r[1] = .{ .avg_dec = .{ .sum = 1, .count = g, .mean = 0, .spec = .{ .precision = 4, .scale = 0 } } };
        row.* = r;
    }
    const cols = try gpa.alloc([]const u8, 2);
    @memset(cols, "");
    out.group_cols = cols;
    const types = [_]scan.GroupColType{
        .{ .lane = .i64, .nullable = true },
        .{ .lane = .dec, .dec = .{ .precision = 8, .scale = 4 }, .nullable = true },
    };
    var col_types: ?[]const scan.GroupColType = &types;
    const below: expr_ast.Having = .{ .cmp = .{ .column = 1, .op = .lt, .literal = .{ .number = "0.005" } } };
    const program: ResultProgram = .{ .order = .{
        .keys = &.{.{ .column = 1, .descending = false }},
        .having = &below,
    } };
    try program.finish(gpa, &out, &col_types);
    const got = out.group_rows.?;
    try std.testing.expectEqual(@as(usize, 2), got.len);
    for (got, [_]i128{ 202, 201 }) |row, g| {
        try std.testing.expectEqual(g, row[0].i);
        const printed: scan.AggValue = .{ .dec = .{ .v = 50, .spec = .{ .precision = 8, .scale = 4 } } };
        try std.testing.expectEqual(printed, row[1]);
    }
}

test "slab rows: a cut keeps a few, compaction releases the rest, strings stay owned" {
    const gpa = std.testing.allocator;
    var out: scan.AggOutput = .{};
    defer out.deinit(gpa);
    const n_rows = 10_000;
    const rows = try gpa.alloc([]const scan.AggValue, n_rows);
    @memset(rows, &.{});
    out.group_rows = rows;
    const slab = try gpa.alloc(scan.AggValue, n_rows * 2);
    @memset(slab, .null_val);
    out.slab = slab;
    for (rows, 0..) |*row, i| {
        const cells = slab[i * 2 ..][0..2];
        cells[0] = .{ .i = @intCast(i) };
        row.* = cells;
        const owned = try std.fmt.allocPrint(gpa, "k{d}", .{i});
        cells[1] = .{ .s = owned };
    }
    const cols = try gpa.alloc([]const u8, 2);
    @memset(cols, "");
    out.group_cols = cols;
    for (cols) |*c| c.* = try gpa.dupe(u8, "c");
    const program: ResultProgram = .{ .order = .{ .keys = &.{.{ .column = 0, .descending = true }}, .limit = 3 } };
    try program.finish(gpa, &out, null);
    try std.testing.expectEqual(@as(usize, 3), out.group_rows.?.len);
    // Compacted: the slab holds just the kept rows.
    try std.testing.expectEqual(@as(usize, 6), out.slab.len);
    for (out.group_rows.?, 0..) |row, j| {
        try std.testing.expectEqual(@as(i128, n_rows - 1 - @as(i128, @intCast(j))), row[0].i);
        try std.testing.expect(std.mem.startsWith(u8, row[1].s, "k"));
    }
}

test "slab rows: compaction that runs out of memory leaves the rows as they were and leaks nothing" {
    const gpa = std.testing.allocator;
    for (0..2) |fail_at| {
        var out: scan.AggOutput = .{};
        defer out.deinit(gpa);
        const slab = try gpa.alloc(scan.AggValue, 8192);
        @memset(slab, .null_val);
        out.slab = slab;
        slab[100] = .{ .i = 7 };
        const rows = try gpa.alloc([]const scan.AggValue, 1);
        rows[0] = slab[100..101];
        out.group_rows = rows;
        // The slab far outsizes its one live cell, so compaction runs and fails at its first or second allocation.
        var failing = std.testing.FailingAllocator.init(gpa, .{ .fail_index = fail_at });
        try std.testing.expectError(error.OutOfMemory, out.compactRows(failing.allocator()));
        try std.testing.expectEqual(slab.ptr, out.slab.ptr);
        try std.testing.expectEqual(@as(i128, 7), out.group_rows.?[0][0].i);
    }
}

test "wide DECIMAL AVG preserves exact and rounded HAVING comparisons" {
    const exact: i192 = 1 << 53;
    try std.testing.expectEqual(@as(f64, 9007199254740992), wideAsFloat(exact));
    try std.testing.expectEqual(@as(f64, 9007199254740992), wideAsFloat(exact + 1));
    try std.testing.expectEqual(@as(f64, 9007199254740996), wideAsFloat(exact + 3));
    try std.testing.expectEqual(@as(f64, -9007199254740992), wideAsFloat(-exact - 1));
    for ([_]i192{ exact, exact + 1, exact + 3 }) |sum| {
        const value: scan.AggValue = .{ .avg_dec = .{
            .sum = sum,
            .count = 1,
            .mean = @intCast(sum),
            .spec = .{ .precision = 38, .scale = 0 },
        } };
        const expected: std.math.Order = if (sum == exact + 3) .gt else .eq;
        try std.testing.expectEqual(@as(?std.math.Order, expected), compareLiteral(value, .{ .number = "9007199254740992" }));
        // Ordinary DECIMAL remains exact even where AVG's double representation rounds.
        const decimal: scan.AggValue = .{ .dec = .{
            .v = @intCast(sum),
            .spec = .{ .precision = 38, .scale = 0 },
        } };
        try std.testing.expectEqual(@as(?std.math.Order, if (sum == exact) .eq else .gt), compareLiteral(decimal, .{ .number = "9007199254740992" }));
    }
    if (@import("builtin").cpu.arch == .x86_64) {
        var rng = std.Random.DefaultPrng.init(0x192f64);
        for (0..10_000) |_| {
            const sum = rng.random().int(i192);
            const legacy: f64 = @floatFromInt(sum);
            try std.testing.expectEqual(@as(u64, @bitCast(legacy)), @as(u64, @bitCast(wideAsFloat(sum))));
            const value: scan.AggValue = .{ .avg_dec = .{
                .sum = sum,
                .count = 7,
                .mean = null,
                .spec = .{ .precision = 38, .scale = 9 },
            } };
            const expected = floatOrder(legacy / 1e9 / 7, 1e30);
            try std.testing.expectEqual(@as(?std.math.Order, expected), compareLiteral(value, .{ .number = "1e30" }));
        }
    }
}

fn threeColumnOutput(gpa: std.mem.Allocator, keys: []const []const u8, a: []const scan.AggValue, b: []const scan.AggValue) !scan.AggOutput {
    var out: scan.AggOutput = .{};
    errdefer out.deinit(gpa);
    const rows = try gpa.alloc([]const scan.AggValue, keys.len);
    @memset(rows, &.{});
    out.group_rows = rows;
    for (rows, keys, a, b) |*row, key, av, bv| {
        const r = try gpa.alloc(scan.AggValue, 3);
        r[0] = .null_val;
        r[1] = av;
        r[2] = bv;
        row.* = r;
        // Bound first: a union initialized with a `try` inside may take its tag before the payload fails.
        const owned = try gpa.dupe(u8, key);
        r[0] = .{ .s = owned };
    }
    const cols = try gpa.alloc([]const u8, 3);
    @memset(cols, "");
    out.group_cols = cols;
    for (cols, [_][]const u8{ "k", "", "" }) |*col, name| col.* = try gpa.dupe(u8, name);
    return out;
}

fn dec2(v: i128) scan.AggValue {
    return .{ .dec = .{ .v = v, .spec = .{ .precision = 38, .scale = 2 } } };
}

const scan_types = [_]scan.GroupColType{
    .{ .lane = .string, .nullable = true },
    .{ .lane = .dec, .dec = .{ .precision = 38, .scale = 2 }, .nullable = true },
    .{ .lane = .i64, .nullable = true },
};

// The scan's aggregate calls behind the two hidden columns: sum(a) over a DECIMAL, sum(b) over an integer (HUGEINT).
const post_calls = [_]expr_agg.AggCall{
    .{ .func = .sum, .arg = null, .where = null, .alias = "a", .result = .dec },
    .{ .func = .sum, .arg = null, .where = null, .alias = "b", .result = .i64 },
};

// SELECT k, sum(a) / sum(b) AS r, sum(a) * 2 AS m: the scan's row is (k, sum(a), sum(b)); the finished row is
// (k, r, m, sum(a), sum(b)).
const ratio: expr_ast.PostExpr = .{ .binary = .{ .op = .div, .left = &.{ .col = .{ .column = 3 } }, .right = &.{ .col = .{ .column = 4 } } } };
const doubled: expr_ast.PostExpr = .{ .binary = .{ .op = .mul, .left = &.{ .col = .{ .column = 3 } }, .right = &.{ .number = "2" } } };
const post_select = [_]expr_ast.SelectColumn{
    .{ .expr = "k", .role = .key },
    .{ .expr = "sum(a) / sum(b)", .alias = "r", .role = .derived, .post = &ratio },
    .{ .expr = "sum(a) * 2", .alias = "m", .role = .derived, .post = &doubled },
};
const post_hidden = [_]expr_ast.SelectColumn{ .{ .expr = "sum(a)", .role = .agg }, .{ .expr = "sum(b)", .role = .agg } };

test "finish computes derived columns once per group, typed as SQL types them, then ranks by them" {
    const gpa = std.testing.allocator;
    var out = try threeColumnOutput(gpa, &.{ "a", "b", "c" }, &.{ dec2(1050), dec2(300), .null_val }, &.{ .{ .i = 4 }, .{ .i = 0 }, .{ .i = 5 } });
    defer out.deinit(gpa);
    var col_types: ?[]const scan.GroupColType = &scan_types;
    const program: ResultProgram = .{
        .order = .{ .keys = &.{.{ .column = 1, .descending = true }}, .hidden = &post_hidden, .post = true },
        .select = &post_select,
        .calls = &post_calls,
        .grouped = true,
        .visible = 3,
    };
    // The strategies before this stage cut nothing, and see neither key nor hidden-term positions.
    try std.testing.expectEqual(@as(usize, 0), ResultProgram.cutOrder(program.order).keys.len);
    try program.finish(gpa, &out, &col_types);
    const rows = out.group_rows.?;
    try std.testing.expectEqual(@as(usize, 3), rows.len);
    // r DESC: a zero divisor is an infinity (as DuckDB's `/`), NULL (no sum) last.
    try std.testing.expectEqualStrings("b", rows[0][0].s);
    try std.testing.expect(std.math.isInf(rows[0][1].f));
    try std.testing.expectEqualStrings("a", rows[1][0].s);
    try std.testing.expectEqual(@as(f64, 2.625), rows[1][1].f);
    try std.testing.expectEqualStrings("c", rows[2][0].s);
    try std.testing.expect(rows[2][1] == .null_val and rows[2][2] == .null_val);
    // sum(a) * 2 stays exact: DECIMAL(38,2), 10.50 * 2 = 21.00.
    try std.testing.expectEqual(@as(i128, 2100), rows[1][2].dec.v);
    try std.testing.expectEqual(@as(u8, 2), rows[1][2].dec.spec.scale);
    for (rows) |row| try std.testing.expectEqual(@as(usize, 3), row.len);
    try std.testing.expectEqualStrings("r", out.group_cols.?[1]);
    try std.testing.expectEqualStrings("m", out.group_cols.?[2]);
    try std.testing.expectEqual(@as(usize, 3), col_types.?.len);
    try std.testing.expect(col_types.?[0].lane == .string and col_types.?[1].lane == .f64 and col_types.?[2].lane == .dec);
    try std.testing.expectEqual(@as(u8, 2), col_types.?[2].dec.?.scale);
}

test "HAVING reads its columns first: a group it drops is never evaluated" {
    const gpa = std.testing.allocator;
    // sum(b) * (2^127 - 1) overflows for every sum(b) above 1: only group a (1) survives, and only it is computed.
    const huge: expr_ast.PostExpr = .{ .binary = .{ .op = .mul, .left = &.{ .col = .{ .column = 4 } }, .right = &.{ .number = "170141183460469231731687303715884105727" } } };
    const select = [_]expr_ast.SelectColumn{
        .{ .expr = "k", .role = .key },
        .{ .expr = "sum(b) * big", .alias = "x", .role = .derived, .post = &huge },
        .{ .expr = "sum(a) * 2", .alias = "m", .role = .derived, .post = &doubled },
    };
    const only_a: expr_ast.Having = .{ .cmp = .{ .column = 0, .op = .eq, .literal = .{ .string = "a" } } };
    var kept = try threeColumnOutput(gpa, &.{ "a", "b", "c" }, &.{ dec2(100), dec2(200), dec2(300) }, &.{ .{ .i = 1 }, .{ .i = 5 }, .{ .i = 7 } });
    defer kept.deinit(gpa);
    var col_types: ?[]const scan.GroupColType = &scan_types;
    const with_having: ResultProgram = .{
        .order = .{ .hidden = &post_hidden, .post = true, .having = &only_a },
        .select = &select,
        .calls = &post_calls,
        .grouped = true,
    };
    try with_having.finish(gpa, &kept, &col_types);
    try std.testing.expectEqual(@as(usize, 1), kept.group_rows.?.len);
    try std.testing.expectEqual(@as(i128, 170141183460469231731687303715884105727), kept.group_rows.?[0][1].i);

    var all = try threeColumnOutput(gpa, &.{ "a", "b", "c" }, &.{ dec2(100), dec2(200), dec2(300) }, &.{ .{ .i = 1 }, .{ .i = 5 }, .{ .i = 7 } });
    defer all.deinit(gpa);
    col_types = &scan_types;
    const without: ResultProgram = .{
        .order = .{ .hidden = &post_hidden, .post = true },
        .select = &select,
        .calls = &post_calls,
        .grouped = true,
    };
    try std.testing.expectError(error.IntegerOverflowMul, without.finish(gpa, &all, &col_types));
}

test "HAVING compares two result columns, and a derived column against a literal" {
    const gpa = std.testing.allocator;
    var out = try threeColumnOutput(gpa, &.{ "a", "b", "c", "d" }, &.{ dec2(1050), dec2(300), .null_val, dec2(900) }, &.{ .{ .i = 4 }, .{ .i = 0 }, .{ .i = 5 }, .{ .i = 100 } });
    defer out.deinit(gpa);
    // sum(a) > sum(b) (decimal against integer, exactly) and r > 1: a (10.50 > 4 and 2.625), b (3.00 > 0 and inf).
    // d fails the first (9.00 > 100), c has a NULL.
    const terms = [_]expr_ast.Having{
        .{ .cmp_col = .{ .left = 3, .op = .gt, .right = 4 } },
        .{ .cmp = .{ .column = 1, .op = .gt, .literal = .{ .number = "1" } } },
    };
    const having: expr_ast.Having = .{ .all = &terms };
    var col_types: ?[]const scan.GroupColType = &scan_types;
    const program: ResultProgram = .{
        .order = .{ .hidden = &post_hidden, .post = true, .having = &having },
        .select = &post_select,
        .calls = &post_calls,
        .grouped = true,
    };
    try program.finish(gpa, &out, &col_types);
    try std.testing.expectEqual(@as(usize, 2), out.group_rows.?.len);
    try std.testing.expectEqualStrings("a", out.group_rows.?[0][0].s);
    try std.testing.expectEqualStrings("b", out.group_rows.?[1][0].s);
    try std.testing.expectError(error.HavingTypeMismatch, checkHaving(.{ .cmp_col = .{ .left = 0, .op = .eq, .right = 1 } }, &.{ .string, .number }));
    try std.testing.expectEqual(@as(?std.math.Order, .gt), compareCells(dec2(1050), .{ .i = 10 }));
    try std.testing.expectEqual(@as(?std.math.Order, .eq), compareCells(dec2(1000), .{ .f = 10 }));
    try std.testing.expectEqual(@as(?std.math.Order, null), compareCells(.null_val, .{ .i = 1 }));
    try std.testing.expectEqual(@as(?std.math.Order, .lt), compareCells(.{ .s = "a" }, .{ .s = "b" }));
}

fn evalPostInts(
    e: expr_ast.PostExpr,
    types: []const scan.GroupColType,
    ints: []const ?expr_ast.IntType,
    row: []const scan.AggValue,
) !scan.AggValue {
    var arena: std.heap.ArenaAllocator = .init(std.testing.allocator);
    defer arena.deinit();
    const rewritten: [16]bool = @splat(false);
    const program = try compile(arena.allocator(), e, types, .{ .ints = ints, .rewritten = rewritten[0..types.len] });
    return postCell(program.ty, try eval(program, row));
}

/// `evalPostInts` over columns whose integer types are all unknown (the tests that read no integer column).
fn evalPost(e: expr_ast.PostExpr, types: []const scan.GroupColType, row: []const scan.AggValue) !scan.AggValue {
    const unknown: [16]?expr_ast.IntType = @splat(null);
    return evalPostInts(e, types, unknown[0..types.len], row);
}

test "post-aggregate arithmetic types and rounds as DuckDB does" {
    const col0: expr_ast.PostExpr = .{ .col = .{ .column = 0 } };
    const col1: expr_ast.PostExpr = .{ .col = .{ .column = 1 } };
    const mean4: expr_ast.PostExpr = .{ .col = .{ .column = 4, .mean = true } };
    const types = [_]scan.GroupColType{
        .{ .lane = .dec, .dec = .{ .precision = 38, .scale = 2 }, .nullable = true },
        .{ .lane = .dec, .dec = .{ .precision = 38, .scale = 4 }, .nullable = true },
        .{ .lane = .i64, .nullable = true },
        .{ .lane = .f64, .nullable = true },
        .{ .lane = .avg_dec, .dec = .{ .precision = 18, .scale = 2 }, .nullable = true },
    };
    const ints = [_]?expr_ast.IntType{ null, null, .i128, null, null };
    const row = [_]scan.AggValue{
        dec2(12345),
        .{ .dec = .{ .v = 250000, .spec = .{ .precision = 38, .scale = 4 } } },
        .{ .i = 7 },
        .{ .f = 2.5 },
        .{ .avg_dec = .{ .sum = 5000, .count = 2, .mean = 2500, .spec = .{ .precision = 18, .scale = 2 } } },
    };
    // DECIMAL(38,2) * DECIMAL(38,4) is DECIMAL(38,6), exactly: 123.45 * 25 = 3086.25.
    const product = try evalPostInts(.{ .binary = .{ .op = .mul, .left = &col0, .right = &col1 } }, &types, &ints, &row);
    try std.testing.expectEqual(@as(i128, 3086250000), product.dec.v);
    try std.testing.expectEqual(@as(u8, 6), product.dec.spec.scale);
    // `+` takes the larger scale; an integer widens to DECIMAL.
    const sum = try evalPostInts(.{ .binary = .{ .op = .add, .left = &col0, .right = &.{ .col = .{ .column = 2 } } } }, &types, &ints, &row);
    try std.testing.expectEqual(@as(i128, 12345 + 700), sum.dec.v);
    // `/` is DOUBLE, for integers and DECIMALs alike; an AVG's decimal mean is a double already.
    const quotient = try evalPostInts(.{ .binary = .{ .op = .div, .left = &.{ .col = .{ .column = 2 } }, .right = &.{ .number = "2" } } }, &types, &ints, &row);
    try std.testing.expectEqual(@as(f64, 3.5), quotient.f);
    const ratio_dec = try evalPostInts(.{ .binary = .{ .op = .div, .left = &col0, .right = &col1 } }, &types, &ints, &row);
    try std.testing.expectEqual(@as(f64, 123.45 / 25.0), ratio_dec.f);
    // An AVG over a DECIMAL is the exact sum over the count: 50.00 / 2.
    try std.testing.expectEqual(@as(f64, 25.0), (try evalPostInts(mean4, &types, &ints, &row)).f);
    // Double arithmetic stays double; NULL propagates; a zero divisor is an infinity, 0/0 NaN.
    const sum_f = try evalPostInts(.{ .binary = .{ .op = .add, .left = &.{ .col = .{ .column = 3 } }, .right = &.{ .number = "1" } } }, &types, &ints, &row);
    try std.testing.expectEqual(@as(f64, 3.5), sum_f.f);
    const null_row = [_]scan.AggValue{ .null_val, .null_val, .null_val, .null_val, .null_val };
    try std.testing.expect((try evalPostInts(.{ .binary = .{ .op = .mul, .left = &col0, .right = &.{ .number = "2" } } }, &types, &ints, &null_row)) == .null_val);
    try std.testing.expect(std.math.isInf((try evalPostInts(.{ .binary = .{ .op = .div, .left = &.{ .number = "1" }, .right = &.{ .number = "0" } } }, &types, &ints, &row)).f));
    try std.testing.expect(std.math.isNan((try evalPostInts(.{ .binary = .{ .op = .div, .left = &.{ .number = "0" }, .right = &.{ .number = "0.0" } } }, &types, &ints, &row)).f));
    // round(x, n): half away from zero; a DOUBLE by its exact value (2.675 is just under, so 2.67), a DECIMAL exactly.
    const rounded_f = try evalPostInts(.{ .round = .{ .value = &.{ .number = "2.675e0" }, .digits = 2 } }, &types, &ints, &row);
    try std.testing.expectEqual(@as(f64, 2.67), rounded_f.f);
    const rounded_neg = try evalPostInts(.{ .round = .{ .value = &.{ .neg = &.{ .number = "2.5e0" } }, .digits = 0 } }, &types, &ints, &row);
    try std.testing.expectEqual(@as(f64, -3), rounded_neg.f);
    const rounded_dec = try evalPostInts(.{ .round = .{ .value = &col0, .digits = 1 } }, &types, &ints, &row);
    try std.testing.expectEqual(@as(i128, 1235), rounded_dec.dec.v); // 123.45 -> 123.5
    try std.testing.expectEqual(@as(u8, 1), rounded_dec.dec.spec.scale);
    const rounded_tens = try evalPostInts(.{ .round = .{ .value = &col0, .digits = -1 } }, &types, &ints, &row);
    try std.testing.expectEqual(@as(i128, 120), rounded_tens.dec.v); // 123.45 -> 120
    try std.testing.expectEqual(@as(u8, 0), rounded_tens.dec.spec.scale);
    // Exact arithmetic that does not fit its type is an error, never a wrapped value.
    const wide = [_]scan.GroupColType{.{ .lane = .dec, .dec = .{ .precision = 38, .scale = 0 }, .nullable = true }};
    const big = [_]scan.AggValue{.{ .dec = .{ .v = 9 * expr_ast.pow10(37), .spec = .{ .precision = 38, .scale = 0 } } }};
    try std.testing.expectError(error.DecimalOverflow, evalPost(.{ .binary = .{ .op = .mul, .left = &.{ .col = .{ .column = 0 } }, .right = &.{ .number = "100" } } }, &wide, &big));
    // A string does not do arithmetic; a DECIMAL product past 38 digits of scale is refused when it compiles.
    const strings = [_]scan.GroupColType{.{ .lane = .string, .nullable = true }};
    try std.testing.expectError(error.TypeMismatch, evalPost(.{ .neg = &.{ .col = .{ .column = 0 } } }, &strings, &.{.{ .s = "x" }}));
    const scaled = [_]scan.GroupColType{.{ .lane = .dec, .dec = .{ .precision = 38, .scale = 20 }, .nullable = true }};
    try std.testing.expectError(error.DecimalScaleTooLarge, evalPost(.{ .binary = .{ .op = .mul, .left = &.{ .col = .{ .column = 0 } }, .right = &.{ .col = .{ .column = 0 } } } }, &scaled, &.{.null_val}));
}

fn intCol(n: usize) expr_ast.PostExpr {
    return .{ .col = .{ .column = n } };
}

fn binary(op: expr_ast.PostOp, l: *const expr_ast.PostExpr, r: *const expr_ast.PostExpr) expr_ast.PostExpr {
    return .{ .binary = .{ .op = op, .left = l, .right = r } };
}

test "integer arithmetic is checked in the SQL type DuckDB gives it, not in 128 bits" {
    // count(*) is BIGINT, sum HUGEINT, max(int32 column) INTEGER, max(int64 column) BIGINT.
    const kinds = [_]?expr_ast.IntType{ .i64, .i128, .i32, .i64, .i8, .u8, .u64, .i16 };
    const types: [kinds.len]scan.GroupColType = @splat(.{ .lane = .i64, .nullable = true });
    const row = [_]scan.AggValue{
        .{ .i = 15 },
        .{ .i = std.math.maxInt(i64) },
        .{ .i = 3 },
        .{ .i = std.math.maxInt(i64) },
        .{ .i = 100 },
        .{ .i = 200 },
        .{ .i = std.math.maxInt(u64) },
        .{ .i = 20_000 },
    };
    const lit = struct {
        fn n(text: []const u8) expr_ast.PostExpr {
            return .{ .number = text };
        }
    }.n;
    // The reviewer's four: each is an overflow in BIGINT or INTEGER where i128 would hold the number.
    try std.testing.expectError(error.IntegerOverflowAdd, evalPostInts(binary(.add, &intCol(0), &lit("9223372036854775807")), &types, &kinds, &row));
    try std.testing.expectError(error.IntegerOverflowMul, evalPostInts(binary(.mul, &intCol(0), &lit("9223372036854775807")), &types, &kinds, &row));
    try std.testing.expectError(error.IntegerOverflowAdd, evalPostInts(binary(.add, &intCol(2), &lit("2147483647")), &types, &kinds, &row));
    try std.testing.expectError(error.IntegerOverflowAdd, evalPostInts(binary(.add, &intCol(3), &lit("1")), &types, &kinds, &row));
    // A HUGEINT sum widens: sum + 1 is HUGEINT and holds it, but a product past 128 bits is an error.
    try std.testing.expectEqual(@as(i128, std.math.maxInt(i64)) + 1, (try evalPostInts(binary(.add, &intCol(1), &lit("1")), &types, &kinds, &row)).i);
    try std.testing.expectError(error.IntegerOverflowMul, evalPostInts(binary(.mul, &intCol(1), &binary(.mul, &intCol(1), &intCol(1))), &types, &kinds, &row));
    // Within range they answer: INTEGER + 1 is INTEGER, BIGINT - 1 BIGINT.
    const ok = try evalPostInts(binary(.add, &intCol(2), &lit("1")), &types, &kinds, &row);
    try std.testing.expectEqual(@as(i128, 4), ok.i);
    // Two TINYINTs sum in TINYINT (100 + 100 overflows 8 bits); a TINYINT and a literal that fits stay TINYINT, but a
    // literal that does not fit makes the operation INTEGER.
    try std.testing.expectError(error.IntegerOverflowAdd, evalPostInts(binary(.add, &intCol(4), &intCol(4)), &types, &kinds, &row));
    try std.testing.expectError(error.IntegerOverflowAdd, evalPostInts(binary(.add, &intCol(4), &lit("100")), &types, &kinds, &row));
    try std.testing.expectEqual(@as(i128, 1100), (try evalPostInts(binary(.add, &intCol(4), &lit("1000")), &types, &kinds, &row)).i);
    // UTINYINT: 200 + 100 overflows, 200 - 201 goes below zero, and an INTEGER-typed column makes it INTEGER.
    try std.testing.expectError(error.IntegerOverflowAdd, evalPostInts(binary(.add, &intCol(5), &lit("100")), &types, &kinds, &row));
    try std.testing.expectError(error.IntegerOverflowSub, evalPostInts(binary(.sub, &intCol(5), &lit("201")), &types, &kinds, &row));
    try std.testing.expectEqual(@as(i128, 203), (try evalPostInts(binary(.add, &intCol(5), &intCol(2)), &types, &kinds, &row)).i);
    // A negative literal does not fit an unsigned type: UTINYINT + (-1) is INTEGER.
    try std.testing.expectEqual(@as(i128, 199), (try evalPostInts(binary(.add, &intCol(5), &.{ .neg = &lit("1") }), &types, &kinds, &row)).i);
    // UBIGINT maximum + 1 overflows UBIGINT; UBIGINT with an INTEGER column is HUGEINT.
    try std.testing.expectError(error.IntegerOverflowAdd, evalPostInts(binary(.add, &intCol(6), &lit("1")), &types, &kinds, &row));
    try std.testing.expectEqual(@as(i128, std.math.maxInt(u64)) + 3, (try evalPostInts(binary(.add, &intCol(6), &intCol(2)), &types, &kinds, &row)).i);
    // The literal INTEGER minimum is INTEGER, and one less than it overflows INTEGER; one more is fine.
    try std.testing.expectError(error.IntegerOverflowSub, evalPostInts(binary(.sub, &.{ .neg = &lit("2147483648") }, &lit("1")), &types, &kinds, &row));
    try std.testing.expectEqual(@as(i128, -2147483648), (try evalPostInts(binary(.sub, &.{ .neg = &lit("2147483647") }, &lit("1")), &types, &kinds, &row)).i);
    // A negation of a computed BIGINT stays BIGINT: -(2147483648 - 1) - 2 is -2147483649 (it is not narrowed again).
    const computed = binary(.sub, &.{ .neg = &binary(.sub, &lit("2147483648"), &lit("1")) }, &lit("2"));
    try std.testing.expectEqual(@as(i128, -2147483649), (try evalPostInts(computed, &types, &kinds, &row)).i);
}

test "negation and rounding of an integer overflow in its type" {
    const kinds = [_]?expr_ast.IntType{ .i32, .i8, .u8, .i128 };
    const types: [kinds.len]scan.GroupColType = @splat(.{ .lane = .i64, .nullable = true });
    // -INT_MIN overflows INTEGER, -(-128) overflows TINYINT, a positive UTINYINT cannot be negated but 0 can.
    try std.testing.expectError(error.IntegerOverflowSub, evalPostInts(.{ .neg = &intCol(0) }, &types, &kinds, &[_]scan.AggValue{ .{ .i = -2147483648 }, .{ .i = 0 }, .{ .i = 0 }, .{ .i = 0 } }));
    try std.testing.expectError(error.IntegerOverflowSub, evalPostInts(.{ .neg = &intCol(1) }, &types, &kinds, &[_]scan.AggValue{ .{ .i = 0 }, .{ .i = -128 }, .{ .i = 0 }, .{ .i = 0 } }));
    try std.testing.expectError(error.IntegerOverflowSub, evalPostInts(.{ .neg = &intCol(2) }, &types, &kinds, &[_]scan.AggValue{ .{ .i = 0 }, .{ .i = 0 }, .{ .i = 5 }, .{ .i = 0 } }));
    try std.testing.expectEqual(@as(i128, 0), (try evalPostInts(.{ .neg = &intCol(2) }, &types, &kinds, &[_]scan.AggValue{ .{ .i = 0 }, .{ .i = 0 }, .{ .i = 0 }, .{ .i = 0 } })).i);
    try std.testing.expectEqual(@as(i128, 2147483647), (try evalPostInts(.{ .neg = &intCol(0) }, &types, &kinds, &[_]scan.AggValue{ .{ .i = -2147483647 }, .{ .i = 0 }, .{ .i = 0 }, .{ .i = 0 } })).i);
    // round(INTEGER max, -1) is 2147483650, past INTEGER; round(TINYINT 127, -2) is 100.
    try std.testing.expectError(error.IntegerOverflowRound, evalPostInts(.{ .round = .{ .value = &intCol(0), .digits = -1 } }, &types, &kinds, &[_]scan.AggValue{ .{ .i = 2147483647 }, .{ .i = 0 }, .{ .i = 0 }, .{ .i = 0 } }));
    try std.testing.expectEqual(@as(i128, 100), (try evalPostInts(.{ .round = .{ .value = &intCol(1), .digits = -2 } }, &types, &kinds, &[_]scan.AggValue{ .{ .i = 0 }, .{ .i = 127 }, .{ .i = 0 }, .{ .i = 0 } })).i);
}

test "an integer of unknown width, and a FLOAT, are refused by name" {
    // The min or max of an expression has no integer type here; a FLOAT column's arithmetic is single precision.
    const unknown = [_]scan.GroupColType{.{ .lane = .i64, .nullable = true }};
    try std.testing.expectError(error.PostAggTypeUnknown, evalPost(binary(.add, &intCol(0), &.{ .number = "1" }), &unknown, &.{.{ .i = 1 }}));
    const f32_source: schema.SchemaElement = .{
        .type = .FLOAT,
        .type_length = null,
        .repetition_type = .OPTIONAL,
        .name = "f",
        .num_children = null,
        .scale = null,
        .precision = null,
        .field_id = null,
        .converted_type = null,
        .logical_type = null,
    };
    const single = [_]scan.GroupColType{.{ .lane = .f64, .nullable = true, .source = f32_source }};
    try std.testing.expectError(error.PostAggTypeUnknown, evalPost(binary(.add, &intCol(0), &.{ .number = "1" }), &single, &.{.{ .f = 0.1 }}));
}

test "narrow DECIMAL products keep 18 digits, as DuckDB's do" {
    const D = expr_ast.Dec;
    // INTEGER x DECIMAL(9,2) is DECIMAL(18,2), not (19,2); BIGINT x DECIMAL(9,2) is (28,2); DECIMAL(18,2)^2 is (18,4).
    try std.testing.expectEqual(D{ .precision = 18, .scale = 2 }, try mulDec(D.ofInt(expr_ast.IntType.i32.digits()), .{ .precision = 9, .scale = 2 }));
    try std.testing.expectEqual(D{ .precision = 28, .scale = 2 }, try mulDec(D.ofInt(expr_ast.IntType.i64.digits()), .{ .precision = 9, .scale = 2 }));
    try std.testing.expectEqual(D{ .precision = 18, .scale = 4 }, try mulDec(.{ .precision = 18, .scale = 2 }, .{ .precision = 18, .scale = 2 }));
    try std.testing.expectEqual(D{ .precision = 14, .scale = 2 }, try mulDec(D.ofInt(10), .{ .precision = 4, .scale = 2 }));
    try std.testing.expectEqual(D{ .precision = 38, .scale = 2 }, try mulDec(D.ofInt(38), .{ .precision = 4, .scale = 2 }));
    // And a product past 10^18 of two such operands is DuckDB's "Overflow in multiplication of DECIMAL(18)".
    const types = [_]scan.GroupColType{.{ .lane = .dec, .dec = .{ .precision = 18, .scale = 0 }, .nullable = true }};
    const row = [_]scan.AggValue{.{ .dec = .{ .v = 2_200_000_000, .spec = .{ .precision = 18, .scale = 0 } } }};
    try std.testing.expectError(error.DecimalOverflow, evalPost(binary(.mul, &intCol(0), &intCol(0)), &types, &row));
}

test "an AVG over a DECIMAL is the exact mean rounded once, never the rounded DECIMAL mean" {
    const types = [_]scan.GroupColType{.{ .lane = .avg_dec, .dec = .{ .precision = 18, .scale = 0 }, .nullable = true }};
    const mean: expr_ast.PostExpr = .{ .col = .{ .column = 0, .mean = true } };
    // One 1 among 201 values: 1/201 is 0.004975..., which round(., 2) takes to 0.0. The DECIMAL mean 0.0050 rounds up.
    const row = [_]scan.AggValue{.{ .avg_dec = .{ .sum = 1, .count = 201, .mean = 0, .spec = .{ .precision = 18, .scale = 0 } } }};
    const r = try evalPost(.{ .round = .{ .value = &mean, .digits = 2 } }, &types, &row);
    try std.testing.expectEqual(@as(f64, 0.0), r.f);
    try std.testing.expectEqual(@as(f64, 1.0 / 201.0), (try evalPost(mean, &types, &row)).f);
    // 0.00, 0.01, 0.01 at scale 2: sum 2, count 3, the double nearest to 1/150.
    const cents = [_]scan.AggValue{.{ .avg_dec = .{ .sum = 2, .count = 3, .mean = 1, .spec = .{ .precision = 18, .scale = 2 } } }};
    try std.testing.expectEqual(@as(f64, 1.0 / 150.0), (try evalPost(mean, &types, &cents)).f);
    // No values is NULL; a sum past 128 bits still averages.
    const none = [_]scan.AggValue{.{ .avg_dec = .{ .sum = null, .count = 0, .mean = null, .spec = .{ .precision = 18, .scale = 0 } } }};
    try std.testing.expect((try evalPost(mean, &types, &none)) == .null_val);
    const huge = [_]scan.AggValue{.{ .avg_dec = .{ .sum = @as(i192, 1) << 150, .count = 2, .mean = null, .spec = .{ .precision = 38, .scale = 0 } } }};
    try std.testing.expectEqual(@as(f64, 0x1p149), (try evalPost(mean, &types, &huge)).f);
    // An AVG of integers or doubles is its avg_state pair's sum over its count.
    const pair = [_]scan.GroupColType{.{ .lane = .avg, .nullable = true }};
    try std.testing.expectEqual(@as(f64, 2.5), (try evalPost(mean, &pair, &.{.{ .avg = .{ .sum = 5, .count = 2 } }})).f);
    try std.testing.expect((try evalPost(mean, &pair, &.{.{ .avg = .{ .sum = null, .count = 0 } }})) == .null_val);
}

/// `e` over one column with the AVG plan chosen: DuckDB's rewrite (no GROUP BY, no FILTER) or the aggregate's own.
fn evalPostPlan(e: expr_ast.PostExpr, types: []const scan.GroupColType, row: []const scan.AggValue, rewritten: bool) !scan.AggValue {
    var arena: std.heap.ArenaAllocator = .init(std.testing.allocator);
    defer arena.deinit();
    const ints: [16]?expr_ast.IntType = @splat(null);
    const plan: [16]bool = @splat(rewritten);
    const program = try compile(arena.allocator(), e, types, .{ .ints = ints[0..types.len], .rewritten = plan[0..types.len] });
    return postCell(program.ty, try eval(program, row));
}

test "a high-scale DECIMAL AVG is DuckDB's double on each plan: the grouped finalizer's and the ungrouped rewrite's" {
    // 0.145 as DECIMAL(38, 37), one value among two rows, one NULL. Grouped, DuckDB divides the sum by the double
    // 10^37 (which is not 10^37) in long double: 0.14500000000000002, and round(., 2) is 0.15. With no GROUP BY it
    // casts the sum to a double first: 0.145, and 0.14. Both verified against DuckDB 2.0.0.
    const types = [_]scan.GroupColType{.{ .lane = .avg_dec, .dec = .{ .precision = 38, .scale = 37 }, .nullable = true }};
    const mean: expr_ast.PostExpr = .{ .col = .{ .column = 0, .mean = true } };
    const spec: expr_ast.Dec = .{ .precision = 38, .scale = 37 };
    const row = [_]scan.AggValue{.{ .avg_dec = .{ .sum = 145 * std.math.pow(i192, 10, 34), .count = 1, .mean = null, .spec = spec } }};
    try std.testing.expectEqual(@as(f64, 0.14500000000000002), (try evalPostPlan(mean, &types, &row, false)).f);
    try std.testing.expectEqual(@as(f64, 0.145), (try evalPostPlan(mean, &types, &row, true)).f);
    const rounded: expr_ast.PostExpr = .{ .round = .{ .value = &mean, .digits = 2 } };
    try std.testing.expectEqual(@as(f64, 0.15), (try evalPostPlan(rounded, &types, &row, false)).f);
    try std.testing.expectEqual(@as(f64, 0.14), (try evalPostPlan(rounded, &types, &row, true)).f);
    // Up to 4 digits of precision the finalizer is in double, as DuckDB's SMALLINT states are.
    const small = [_]scan.AggValue{.{ .avg_dec = .{ .sum = 123 + 456 + 789, .count = 3, .mean = null, .spec = .{ .precision = 4, .scale = 3 } } }};
    const small_types = [_]scan.GroupColType{.{ .lane = .avg_dec, .dec = .{ .precision = 4, .scale = 3 }, .nullable = true }};
    try std.testing.expectEqual(@as(f64, 0.456), (try evalPostPlan(mean, &small_types, &small, false)).f);
    try std.testing.expectEqual(@as(f64, 0.456), (try evalPostPlan(mean, &small_types, &small, true)).f);
}

test "a DECIMAL becomes the double nearest it, ties to even, however far below a tie it sits" {
    try std.testing.expectEqual(@as(f64, 1), decimalAsDouble(1, 0));
    try std.testing.expectEqual(@as(f64, -0.3), decimalAsDouble(-3, 1));
    try std.testing.expectEqual(@as(f64, 0.145), decimalAsDouble(145 * std.math.pow(i192, 10, 34), 37));
    // 2^53 + 1 is a tie between 2^53 and 2^53 + 2: the even one. 2^53 + 3 is a tie between two odd multiples of 2.
    const two53: i192 = 1 << 53;
    try std.testing.expectEqual(@as(f64, 0x1p53), decimalAsDouble(two53 + 1, 0));
    try std.testing.expectEqual(@as(f64, 0x1p53 + 4), decimalAsDouble(two53 + 3, 0));
    // A tie plus a digit 19 places below the point: above the tie, so up. A quotient kept in 113 bits loses that digit.
    const ten19 = std.math.pow(i192, 10, 19);
    try std.testing.expectEqual(@as(f64, 0x1p53 + 2), decimalAsDouble((two53 + 1) * ten19 + 1, 19));
    try std.testing.expectEqual(@as(f64, 0x1p53), decimalAsDouble((two53 + 1) * ten19 - 1, 19));
    try std.testing.expectEqual(@as(f64, 0x1p53), decimalAsDouble((two53 + 1) * ten19, 19));
    // Sums past 128 bits still convert.
    try std.testing.expectEqual(@as(f64, 0x1p150), decimalAsDouble(@as(i192, 1) << 150, 0));
}

test "ROUND of a DECIMAL fails where the result leaves the precision, and not before" {
    const wide = [_]scan.GroupColType{.{ .lane = .dec, .dec = .{ .precision = 38, .scale = 0 }, .nullable = true }};
    const narrow = [_]scan.GroupColType{.{ .lane = .dec, .dec = .{ .precision = 2, .scale = 0 }, .nullable = true }};
    const col: expr_ast.PostExpr = .{ .col = .{ .column = 0 } };
    const tens: expr_ast.PostExpr = .{ .round = .{ .value = &col, .digits = -1 } };
    const ten38 = std.math.pow(i128, 10, 38);
    for ([_]i128{ 1, -1 }) |sign| {
        // 10^38 - 6 rounds down to 10^38 - 10 and stays; 10^38 - 5 is a tie away from zero, to 10^38: 39 digits.
        const below = [_]scan.AggValue{.{ .dec = .{ .v = sign * (ten38 - 6), .spec = .{ .precision = 38, .scale = 0 } } }};
        const tie = [_]scan.AggValue{.{ .dec = .{ .v = sign * (ten38 - 5), .spec = .{ .precision = 38, .scale = 0 } } }};
        try std.testing.expectEqual(sign * (ten38 - 10), (try evalPost(tens, &wide, &below)).dec.v);
        try std.testing.expectError(error.DecimalOverflow, evalPost(tens, &wide, &tie));
        // A DECIMAL(2, 0) rounded to tens is a DECIMAL(3, 0), as DuckDB types it: 94 to 90, 95 to 100, both held.
        const low = [_]scan.AggValue{.{ .dec = .{ .v = sign * 94, .spec = .{ .precision = 2, .scale = 0 } } }};
        const high = [_]scan.AggValue{.{ .dec = .{ .v = sign * 95, .spec = .{ .precision = 2, .scale = 0 } } }};
        try std.testing.expectEqual(sign * 90, (try evalPost(tens, &narrow, &low)).dec.v);
        const widened = try evalPost(tens, &narrow, &high);
        try std.testing.expectEqual(sign * 100, widened.dec.v);
        try std.testing.expectEqual(@as(u8, 3), widened.dec.spec.precision);
    }
    // Digits that keep the scale cannot overflow, and a scale above 0 does not widen: 99.99 rounds to 100 in a
    // DECIMAL(4, 2) held as DECIMAL(4, 0).
    const cents = [_]scan.GroupColType{.{ .lane = .dec, .dec = .{ .precision = 4, .scale = 2 }, .nullable = true }};
    const row = [_]scan.AggValue{.{ .dec = .{ .v = 9999, .spec = .{ .precision = 4, .scale = 2 } } }};
    const whole: expr_ast.PostExpr = .{ .round = .{ .value = &col, .digits = 0 } };
    const at_tens = try evalPost(tens, &cents, &row);
    try std.testing.expectEqual(@as(i128, 100), (try evalPost(whole, &cents, &row)).dec.v);
    try std.testing.expectEqual(@as(i128, 100), at_tens.dec.v);
    try std.testing.expectEqual(@as(u8, 4), at_tens.dec.spec.precision);
}

test "ROUND keeps a non-finite input whatever the digits, and zeroes only a finite input that overflows" {
    const inf = std.math.inf(f64);
    for ([_]i32{ -400, -308, -1, 0, 1, 2, 308, 400 }) |digits| {
        try std.testing.expectEqual(inf, roundDouble(inf, digits));
        try std.testing.expectEqual(-inf, roundDouble(-inf, digits));
        try std.testing.expect(std.math.isNan(roundDouble(std.math.nan(f64), digits)));
    }
    // A finite input: ties away from zero; a scaling that overflows is 0 (negative digits) or the input (positive).
    try std.testing.expectEqual(@as(f64, 130), roundDouble(125, -1));
    try std.testing.expectEqual(@as(f64, -130), roundDouble(-125, -1));
    try std.testing.expectEqual(@as(f64, 0), roundDouble(123.456, -400));
    try std.testing.expectEqual(@as(f64, 123.456), roundDouble(123.456, 400));
    try std.testing.expectEqual(@as(f64, 1.7e308), roundDouble(1.7e308, -1));
}

test "ROUND takes every digit count the parser admits, to the extremes of i32" {
    const lo = std.math.minInt(i32);
    const hi = std.math.maxInt(i32);
    // The negation and the distance from the scale both overflow i32 near the minimum.
    for ([_]i32{ lo, lo + 1, lo + 2, -1_000_000, -39 }) |digits| {
        try std.testing.expectEqual(@as(f64, 0), roundDouble(1.25, digits));
        try std.testing.expectEqual(@as(f64, 0), roundDouble(-1.25, digits));
        try std.testing.expectEqual(@as(i128, 0), try roundDecimal(125, 2, digits));
        try std.testing.expectEqual(@as(i128, 0), try roundDecimal(-125, 2, digits));
        try std.testing.expectEqual(@as(i128, 0), try roundDecimal(std.math.pow(i128, 10, 38) - 1, 38, digits));
    }
    for ([_]i32{ hi, hi - 1, 1_000_000, 400 }) |digits| {
        try std.testing.expectEqual(@as(f64, 1.25), roundDouble(1.25, digits));
        try std.testing.expectEqual(@as(i128, 125), try roundDecimal(125, 2, digits));
    }
    // The last count that still divides (10^38 at scale 2): 0.5 * 10^36 rounds away from zero to 10^36, 49 to 0.
    try std.testing.expectEqual(@as(i128, 0), try roundDecimal(49, 2, -36));
    try std.testing.expectEqual(std.math.pow(i128, 10, 36), try roundDecimal(5 * std.math.pow(i128, 10, 37), 2, -36));
}

test "abs keeps its type and overflows at the type's minimum; nullif is NULL where the value equals the other" {
    const kinds = [_]?expr_ast.IntType{ null, .i32, null, .i32, .i32 };
    const types = [_]scan.GroupColType{
        .{ .lane = .dec, .dec = .{ .precision = 38, .scale = 2 }, .nullable = true },
        .{ .lane = .i64, .nullable = true },
        .{ .lane = .f64, .nullable = true },
        .{ .lane = .i64, .nullable = true },
        .{ .lane = .i64, .nullable = true },
    };
    const row = [_]scan.AggValue{ dec2(-12345), .{ .i = -7 }, .{ .f = -2.5 }, .null_val, .{ .i = -2147483648 } };
    const col = struct {
        fn at(n: usize) expr_ast.PostExpr {
            return .{ .col = .{ .column = n } };
        }
    }.at;
    const abs_dec = try evalPostInts(.{ .abs = &col(0) }, &types, &kinds, &row);
    try std.testing.expectEqual(@as(i128, 12345), abs_dec.dec.v);
    try std.testing.expectEqual(@as(u8, 2), abs_dec.dec.spec.scale);
    try std.testing.expectEqual(@as(i128, 7), (try evalPostInts(.{ .abs = &col(1) }, &types, &kinds, &row)).i);
    try std.testing.expectEqual(@as(f64, 2.5), (try evalPostInts(.{ .abs = &col(2) }, &types, &kinds, &row)).f);
    try std.testing.expect((try evalPostInts(.{ .abs = &col(3) }, &types, &kinds, &row)) == .null_val);
    // abs(INTEGER minimum) is an overflow in INTEGER, as DuckDB's "Overflow on abs(-2147483648)".
    try std.testing.expectError(error.IntegerOverflowAbs, evalPostInts(.{ .abs = &col(4) }, &types, &kinds, &row));
    // nullif compares exactly (a DECIMAL against a decimal literal of another scale, an integer against a literal).
    const equal = try evalPostInts(.{ .nullif = .{ .value = &col(0), .other = &.{ .neg = &.{ .number = "123.450" } } } }, &types, &kinds, &row);
    try std.testing.expect(equal == .null_val);
    const differs = try evalPostInts(.{ .nullif = .{ .value = &col(0), .other = &.{ .number = "123" } } }, &types, &kinds, &row);
    try std.testing.expectEqual(@as(i128, -12345), differs.dec.v);
    try std.testing.expect((try evalPostInts(.{ .nullif = .{ .value = &col(1), .other = &.{ .neg = &.{ .number = "7" } } } }, &types, &kinds, &row)) == .null_val);
    // A NULL other leaves the value; a NULL value stays NULL.
    const kept = try evalPostInts(.{ .nullif = .{ .value = &col(1), .other = &col(3) } }, &types, &kinds, &row);
    try std.testing.expectEqual(@as(i128, -7), kept.i);
    try std.testing.expect((try evalPostInts(.{ .nullif = .{ .value = &col(3), .other = &col(1) } }, &types, &kinds, &row)) == .null_val);
}

test "finish with computed columns leaves the answer whole when it runs out of memory" {
    const Probe = struct {
        fn run(gpa: std.mem.Allocator) !void {
            var out = try threeColumnOutput(gpa, &.{ "a", "b", "c" }, &.{ dec2(1050), dec2(300), .null_val }, &.{ .{ .i = 4 }, .{ .i = 0 }, .{ .i = 5 } });
            defer out.deinit(gpa);
            var col_types: ?[]const scan.GroupColType = &scan_types;
            const having: expr_ast.Having = .{ .cmp = .{ .column = 1, .op = .gt, .literal = .{ .number = "1" } } };
            const program: ResultProgram = .{
                .order = .{ .keys = &.{.{ .column = 1, .descending = true }}, .hidden = &post_hidden, .post = true, .having = &having, .limit = 1 },
                .select = &post_select,
                .calls = &post_calls,
                .grouped = true,
            };
            try program.finish(gpa, &out, &col_types);
            try std.testing.expectEqualStrings("b", out.group_rows.?[0][0].s);
        }
    };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Probe.run, .{});
}

test "a result column's scan column, and whether ranking, HAVING or a computed column reads one" {
    // SELECT k, name, sum(x) AS s, round(s / 2) AS r ... ORDER BY r: r is computed here, over s.
    const s_col: expr_ast.PostExpr = .{ .col = .{ .column = 2 } };
    const two: expr_ast.PostExpr = .{ .number = "2" };
    const half: expr_ast.PostExpr = .{ .binary = .{ .op = .div, .left = &s_col, .right = &two } };
    const rounded: expr_ast.PostExpr = .{ .round = .{ .value = &half } };
    const select = [_]expr_ast.SelectColumn{
        .{ .expr = "k", .role = .key },
        .{ .expr = "name", .role = .key },
        .{ .expr = "sum(x)", .alias = "s", .role = .agg },
        .{ .expr = "round(s / 2)", .alias = "r", .role = .derived, .post = &rounded },
    };
    const by_r = [_]expr_ast.OrderKey{.{ .column = 3, .descending = true }};
    const p: ResultProgram = .{ .order = .{ .keys = &by_r, .limit = 10, .post = true }, .select = &select, .grouped = true };
    try std.testing.expectEqual(@as(?usize, 0), p.scanColumn(0));
    try std.testing.expectEqual(@as(?usize, 2), p.scanColumn(2));
    try std.testing.expectEqual(@as(?usize, null), p.scanColumn(3));
    try std.testing.expectEqual(@as(usize, 4), p.shownColumns(3));
    // The ranking reads r, which reads s: s is needed before the cut, name is not.
    try std.testing.expect(p.readsScanColumn(2));
    try std.testing.expect(!p.readsScanColumn(1));
    try std.testing.expect(!p.readsScanColumn(0));
    // Ranked by name itself, it is.
    const by_name = [_]expr_ast.OrderKey{.{ .column = 1, .descending = false }};
    const q: ResultProgram = .{ .order = .{ .keys = &by_name, .limit = 10, .post = true }, .select = &select, .grouped = true };
    try std.testing.expect(q.readsScanColumn(1));
}

test "post-aggregate reserved rows widen in place and stay whole on allocation failure" {
    const Probe = struct {
        fn run(gpa: std.mem.Allocator) !void {
            var out = try threeColumnOutput(gpa, &.{ "a", "b", "c" }, &.{ dec2(10), dec2(20), dec2(30) }, &.{ .{ .i = 1 }, .{ .i = 2 }, .{ .i = 3 } });
            defer out.deinit(gpa);
            const slab = try gpa.alloc(scan.AggValue, 15);
            @memset(slab, .null_val);
            for (@constCast(out.group_rows.?), 0..) |*row, i| {
                @memcpy(slab[i * 5 ..][0..3], row.*);
                gpa.free(row.*);
                row.* = slab[i * 5 ..][0..3];
            }
            out.slab = slab;
            var col_types: ?[]const scan.GroupColType = &scan_types;
            var arena: std.heap.ArenaAllocator = .init(gpa);
            defer arena.deinit();
            const compiled = try compile(arena.allocator(), .{ .number = "1" }, &.{}, .{ .ints = &.{}, .rewritten = &.{} });
            const programs = [_]?*const Compiled{ null, compiled, compiled, null, null };
            const types = [_]scan.GroupColType{ scan_types[0], scan_types[2], scan_types[2], scan_types[1], scan_types[2] };
            const p: ResultProgram = .{ .select = &post_select, .order = .{ .hidden = &post_hidden }, .workers = 8 };
            p.widen(gpa, &out, &col_types, &programs, &types) catch |err| {
                for (out.group_rows.?, 0..) |row, i| {
                    try std.testing.expectEqual(@as(usize, 3), row.len);
                    try std.testing.expectEqual(@as(i128, @intCast(i + 1)), row[2].i);
                }
                return err;
            };
            try std.testing.expectEqual(slab.ptr, out.slab.ptr);
            for (out.group_rows.?, 0..) |row, i| {
                try std.testing.expectEqual(@as(usize, 5), row.len);
                try std.testing.expectEqual(@as(i128, @intCast((i + 1) * 10)), row[3].dec.v);
                try std.testing.expectEqual(@as(i128, @intCast(i + 1)), row[4].i);
                try std.testing.expect(row[1] == .null_val and row[2] == .null_val);
                try std.testing.expectEqualStrings((&[_][]const u8{ "a", "b", "c" })[i], row[0].s);
            }
        }
    };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Probe.run, .{});
}

test "post-aggregate parallel evaluation preserves the earliest row error" {
    const gpa = std.testing.allocator;
    var state: std.heap.ArenaAllocator = .init(gpa);
    defer state.deinit();
    const arena = state.allocator();
    const n = 10000;
    const cells = try arena.alloc(scan.AggValue, n * 3);
    const rows = try arena.alloc([]const scan.AggValue, n);
    for (rows, 0..) |*row, i| row.* = cells[i * 3 ..][0..3];
    const types = [_]scan.GroupColType{ .{ .lane = .i64, .nullable = false }, .{ .lane = .i64, .nullable = false } };
    const add: expr_ast.PostExpr = .{ .binary = .{
        .op = .add,
        .left = &.{ .col = .{ .column = 0 } },
        .right = &.{ .number = "1" },
    } };
    const expr: expr_ast.PostExpr = .{ .binary = .{
        .op = .mul,
        .left = &add,
        .right = &.{ .col = .{ .column = 1 } },
    } };
    const program = try compile(arena, expr, &types, .{ .ints = &.{ .i32, .i32 }, .rewritten = &.{ false, false } });
    const programs = [_]?*const Compiled{ null, null, program };
    for ([_]usize{ 1, 8 }) |workers| {
        for (rows) |row| {
            @constCast(row)[0] = .{ .i = 1 };
            @constCast(row)[1] = .{ .i = 1 };
            @constCast(row)[2] = .null_val;
        }
        @constCast(rows[137])[0] = .{ .i = std.math.maxInt(i32) };
        @constCast(rows[8000])[0] = .{ .i = 100000 };
        @constCast(rows[8000])[1] = .{ .i = 100000 };
        var computed = [_]bool{ false, false, false };
        try std.testing.expectError(error.IntegerOverflowAdd, computeDerived(rows, &programs, &.{ false, false, true }, &computed, workers));
        try std.testing.expect(!computed[2]);
        @constCast(rows[137])[0] = .{ .i = 1 };
        @constCast(rows[8000])[0] = .{ .i = 1 };
        @constCast(rows[8000])[1] = .{ .i = 1 };
        try computeDerived(rows, &programs, &.{ false, false, true }, &computed, workers);
        try std.testing.expect(computed[2]);
        for (rows) |row| try std.testing.expectEqual(@as(i128, 2), row[2].i);
    }
}
