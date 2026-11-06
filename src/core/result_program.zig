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

pub const Error = error{
    OutOfMemory,
    /// An expression over results mixes a string, a date or a boolean with arithmetic.
    TypeMismatch,
    HavingTypeMismatch,
    DuplicateOutputColumn,
    IntegerOverflowAdd,
    IntegerOverflowSub,
    IntegerOverflowMul,
    DecimalOverflow,
    DecimalScaleTooLarge,
};

pub const ResultProgram = struct {
    order: expr_ast.ResultOrder = .{},
    /// How many leading columns of each GROUP BY row are shown (`MultiAggResult.visible_cols`); null shows all.
    visible: ?usize = null,
    /// The SELECT list, whose `derived` columns `order.post` computes here; the scan's row has none of them.
    select: []const expr_ast.SelectColumn = &.{},

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
        if (self.order.having) |h| try applyHaving(gpa, out, h.*);
        try out.applyResultOrder(gpa, self.order);
        if (out.group_rows == null) return self.projectScalar(gpa, out);
        try project(self.visible, gpa, out, col_types);
        // A cut leaves few of the rows the slab was built for: release the rest. Only memory is at stake, so a
        // refused allocation keeps the larger slab rather than fail an answer already final.
        out.compactRows(gpa) catch {};
    }

    /// Drop each row's columns past the first `visible`, building the narrowed rows in one new slab before anything
    /// is released, so a failure leaves `out` as it was. They take over the visible values (and the strings those
    /// own) from the old rows.
    fn project(
        visible: ?usize,
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
        for (rows, narrowed, 0..) |row, *dst, i| {
            const cells = slab[i * n ..][0..n];
            @memcpy(cells, row[0..n]);
            dst.* = cells;
        }
        for (rows) |row| {
            for (row[n..]) |v| freeValue(gpa, v);
            out.releaseRowSlice(gpa, row);
        }
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

    /// Drop every group `having` does not make TRUE. The kept rows are listed before any is released.
    fn applyHaving(gpa: std.mem.Allocator, out: *scan.AggOutput, having: expr_ast.Having) error{OutOfMemory}!void {
        const rows = out.group_rows orelse {
            const Scalar = struct {
                aggs: []const scan.AggOutputItem,
                fn at(self: @This(), c: usize) scan.AggValue {
                    return self.aggs[c].value;
                }
            };
            out.no_row = truth(having, Scalar{ .aggs = out.aggs }) != true;
            return;
        };
        const Row = struct {
            row: []const scan.AggValue,
            fn at(self: @This(), c: usize) scan.AggValue {
                return self.row[c];
            }
        };
        var n: usize = 0;
        for (rows) |row| {
            if (truth(having, Row{ .row = row }) == true) n += 1;
        }
        if (n == rows.len) return;
        const kept = try gpa.alloc([]const scan.AggValue, n);
        var k: usize = 0;
        for (rows) |row| {
            if (truth(having, Row{ .row = row }) == true) {
                kept[k] = row;
                k += 1;
            } else out.releaseRow(gpa, row);
        }
        gpa.free(rows);
        out.group_rows = kept;
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
        const programs = try arena.alloc(?*const Compiled, width);
        var raw: usize = 0;
        for (0..width) |j| {
            programs[j] = null;
            if (self.column(j).role == .derived) continue;
            types[j] = raw_types[raw];
            raw += 1;
        }
        std.debug.assert(raw == out.group_cols.?.len);
        for (0..width) |j| {
            const spec = self.column(j);
            if (spec.role != .derived) continue;
            const program = try compile(arena, spec.post.?.*, types);
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
            try computeDerived(out.group_rows.?, programs, wanted, computed);
            try applyHaving(gpa, out, h.*);
        }
        @memset(wanted, true);
        try computeDerived(out.group_rows.?, programs, wanted, computed);
        try out.applyResultOrder(gpa, self.order);
        try project(self.select.len, gpa, out, col_types);
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
        const slab = try gpa.alloc(scan.AggValue, rows.len * width);
        errdefer gpa.free(slab);
        const wide = try gpa.alloc([]const scan.AggValue, rows.len);
        for (rows, wide, 0..) |row, *dst, i| {
            const cells = slab[i * width ..][0..width];
            raw = 0;
            for (cells, programs) |*cell, program| {
                if (program != null) {
                    cell.* = .null_val;
                } else {
                    cell.* = row[raw];
                    raw += 1;
                }
            }
            dst.* = cells;
        }
        for (rows) |row| out.releaseRowSlice(gpa, row);
        gpa.free(rows);
        gpa.free(out.slab);
        gpa.free(raw_names);
        gpa.free(out.owned_types);
        out.slab = slab;
        out.group_rows = wide;
        out.group_cols = names;
        out.owned_types = owned_types;
        col_types.?.* = owned_types;
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
/// floats; an AVG compares its mean as a double, as SQL types it.
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
        .avg_dec => |a| if (a.count == 0 or a.sum == null) null else floatOrder(
            wideAsFloat(a.sum.?) / std.math.pow(f64, 10, @floatFromInt(a.spec.scale)) /
                @as(f64, @floatFromInt(a.count)),
            num.asF64(),
        ),
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
            .avg_dec => |a| if (a.count == 0 or a.sum == null) null else .{ .float = wideAsFloat(a.sum.?) /
                std.math.pow(f64, 10, @floatFromInt(a.spec.scale)) / @as(f64, @floatFromInt(a.count)) },
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

/// What a post-aggregate expression evaluates to, as DuckDB types it: `+ - *` stay exact over integers and DECIMALs
/// (a DECIMAL when either side is, by `Dec`'s rules), `/` and anything with a double are DOUBLE.
const PostType = union(enum) {
    int,
    dec: expr_ast.Dec,
    float,

    fn column(self: PostType) scan.GroupColType {
        return switch (self) {
            .int => .{ .lane = .i64, .nullable = true },
            .float => .{ .lane = .f64, .nullable = true },
            .dec => |d| .{ .lane = .dec, .dec = d, .nullable = true },
        };
    }

    /// The DECIMAL an operand of a DECIMAL operation takes: an integer widens to the widest one.
    fn asDec(self: PostType) expr_ast.Dec {
        return switch (self) {
            .dec => |d| d,
            else => expr_ast.Dec.ofInt(expr_ast.Dec.max_precision),
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

    const Node = union(enum) {
        col: usize,
        /// A DECIMAL mean, read as the double SQL types an AVG.
        mean: usize,
        int: i128,
        dec: i128,
        float: f64,
        neg: *const Compiled,
        arith: struct { op: expr_ast.PostOp, left: *const Compiled, right: *const Compiled },
        round: struct { value: *const Compiled, digits: i32 },
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

/// A decimal literal's type, as DuckDB gives it: its digits, at least one before the point.
fn literalDec(unscaled: i128, scale: u8) expr_ast.Dec {
    var digits: u8 = 1;
    var rest = @abs(unscaled);
    while (rest >= 10) : (rest /= 10) digits += 1;
    return .{ .precision = @max(digits, scale + 1), .scale = scale };
}

fn compile(arena: std.mem.Allocator, e: expr_ast.PostExpr, types: []const scan.GroupColType) Error!*const Compiled {
    const out = try arena.create(Compiled);
    out.* = switch (e) {
        .col => |c| blk: {
            const t = types[c.column];
            if (!plainNumber(t.source)) return error.TypeMismatch;
            break :blk switch (t.lane) {
                .i64 => .{ .ty = .int, .node = .{ .col = c.column } },
                .f64 => .{ .ty = .float, .node = .{ .col = c.column } },
                .dec => if (c.mean)
                    .{ .ty = .float, .node = .{ .mean = c.column } }
                else
                    .{ .ty = .{ .dec = t.dec.? }, .node = .{ .col = c.column } },
                .string, .avg, .avg_dec => return error.TypeMismatch,
            };
        },
        .number => |text| switch (Number.parse(text) orelse return error.TypeMismatch) {
            .int => |v| .{ .ty = .int, .node = .{ .int = v } },
            .dec => |d| .{ .ty = .{ .dec = literalDec(d.unscaled, d.scale) }, .node = .{ .dec = d.unscaled } },
            .float => |f| .{ .ty = .float, .node = .{ .float = f } },
        },
        .neg => |inner| blk: {
            const v = try compile(arena, inner.*, types);
            break :blk .{ .ty = v.ty, .node = .{ .neg = v } };
        },
        .binary => |b| blk: {
            const left = try compile(arena, b.left.*, types);
            const right = try compile(arena, b.right.*, types);
            const ty: PostType = if (b.op == .div or left.ty == .float or right.ty == .float)
                .float
            else if (left.ty == .dec or right.ty == .dec)
                .{ .dec = if (b.op == .mul)
                    try expr_ast.Dec.mul(left.ty.asDec(), right.ty.asDec())
                else
                    expr_ast.Dec.addSub(left.ty.asDec(), right.ty.asDec()) }
            else
                .int;
            break :blk .{ .ty = ty, .node = .{ .arith = .{ .op = b.op, .left = left, .right = right } } };
        },
        .round => |r| blk: {
            const v = try compile(arena, r.value.*, types);
            // Fewer digits than the scale: the type keeps its precision at the digits (none below the point for a
            // negative count); otherwise rounding changes nothing.
            const ty: PostType = switch (v.ty) {
                .dec => |d| if (r.digits >= d.scale) v.ty else .{ .dec = .{
                    .precision = d.precision,
                    .scale = @intCast(@max(0, r.digits)),
                } },
                else => v.ty,
            };
            break :blk .{ .ty = ty, .node = .{ .round = .{ .value = v, .digits = r.digits } } };
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

fn eval(node: *const Compiled, row: []const scan.AggValue) Error!?Raw {
    switch (node.node) {
        .col => |c| return switch (row[c]) {
            .null_val => null,
            .i => |v| .{ .i = v },
            .f => |f| .{ .f = f },
            .dec => |d| .{ .i = d.v },
            else => error.TypeMismatch,
        },
        .mean => |c| return switch (row[c]) {
            .null_val => null,
            .f => |f| .{ .f = f },
            .dec => |d| .{ .f = decimalAsFloat(d.v, d.spec.scale) },
            else => error.TypeMismatch,
        },
        .int, .dec => |v| return .{ .i = v },
        .float => |f| return .{ .f = f },
        .neg => |inner| {
            const v = (try eval(inner, row)) orelse return null;
            return switch (node.ty) {
                .float => .{ .f = -v.f },
                .int, .dec => .{ .i = std.math.negate(v.i) catch return error.IntegerOverflowSub },
            };
        },
        .arith => |a| {
            const l = (try eval(a.left, row)) orelse return null;
            const r = (try eval(a.right, row)) orelse return null;
            switch (node.ty) {
                .float => {
                    const x = asFloat(a.left.ty, l);
                    const y = asFloat(a.right.ty, r);
                    return .{ .f = switch (a.op) {
                        .add => x + y,
                        .sub => x - y,
                        .mul => x * y,
                        // IEEE, as DuckDB's `/`: a zero divisor gives an infinity or NaN, never an error.
                        .div => x / y,
                    } };
                },
                .int => return .{ .i = switch (a.op) {
                    .add => std.math.add(i128, l.i, r.i) catch return error.IntegerOverflowAdd,
                    .sub => std.math.sub(i128, l.i, r.i) catch return error.IntegerOverflowSub,
                    .mul => std.math.mul(i128, l.i, r.i) catch return error.IntegerOverflowMul,
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
        .round => |r| {
            const v = (try eval(r.value, row)) orelse return null;
            return switch (r.value.ty) {
                .float => .{ .f = roundDouble(v.f, r.digits) },
                .int => .{ .i = try roundDecimal(v.i, 0, r.digits) },
                .dec => |d| .{ .i = try roundDecimal(v.i, d.scale, r.digits) },
            };
        },
    }
}

/// `round(x, digits)` of a double as DuckDB computes it: scale, round half away from zero, scale back. A result
/// that is not finite is the input (positive digits) or 0 (negative).
fn roundDouble(x: f64, digits: i32) f64 {
    if (digits < 0) {
        const m = std.math.pow(f64, 10, @floatFromInt(-digits));
        const r = @round(x / m) * m;
        return if (std.math.isFinite(r)) r else 0;
    }
    const m = std.math.pow(f64, 10, @floatFromInt(digits));
    const r = @round(x * m) / m;
    return if (std.math.isFinite(r)) r else x;
}

/// `round(v, digits)` of an exact decimal at `scale`, half away from zero, as the unscaled value of the type
/// `compile` gave it: scale `digits` when that is fewer than `scale` (0 when negative), else `v` itself.
fn roundDecimal(v: i128, scale: u8, digits: i32) error{DecimalOverflow}!i128 {
    if (digits >= scale) return v;
    const drop: i32 = scale - digits;
    // A divisor past 10^38 is past twice any value a DECIMAL holds: everything rounds to zero.
    if (drop > expr_ast.Dec.max_precision) return 0;
    const divisor = expr_ast.pow10(@intCast(drop));
    var q = @divTrunc(v, divisor);
    if (@as(u128, @abs(@rem(v, divisor))) * 2 >= @as(u128, @intCast(divisor))) q += if (v < 0) -1 else 1;
    if (digits >= 0) return q;
    return std.math.mul(i128, q, expr_ast.pow10(@intCast(-digits))) catch error.DecimalOverflow;
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
) Error!void {
    for (programs, wanted, computed, 0..) |program, want, *done, j| {
        const p = program orelse continue;
        if (!want or done.*) continue;
        for (rows) |row| {
            // The wide rows are views into the slab `widen` allocated, which nothing else holds.
            const cells: []scan.AggValue = @constCast(row);
            cells[j] = postCell(p.ty, try eval(p, row));
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
    };
    try with_having.finish(gpa, &kept, &col_types);
    try std.testing.expectEqual(@as(usize, 1), kept.group_rows.?.len);
    try std.testing.expectEqual(@as(i128, 170141183460469231731687303715884105727), kept.group_rows.?[0][1].i);

    var all = try threeColumnOutput(gpa, &.{ "a", "b", "c" }, &.{ dec2(100), dec2(200), dec2(300) }, &.{ .{ .i = 1 }, .{ .i = 5 }, .{ .i = 7 } });
    defer all.deinit(gpa);
    col_types = &scan_types;
    const without: ResultProgram = .{ .order = .{ .hidden = &post_hidden, .post = true }, .select = &select };
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

fn evalPost(e: expr_ast.PostExpr, types: []const scan.GroupColType, row: []const scan.AggValue) !scan.AggValue {
    var arena: std.heap.ArenaAllocator = .init(std.testing.allocator);
    defer arena.deinit();
    const program = try compile(arena.allocator(), e, types);
    return postCell(program.ty, try eval(program, row));
}

test "post-aggregate arithmetic types and rounds as DuckDB does" {
    const col0: expr_ast.PostExpr = .{ .col = .{ .column = 0 } };
    const col1: expr_ast.PostExpr = .{ .col = .{ .column = 1 } };
    const mean1: expr_ast.PostExpr = .{ .col = .{ .column = 1, .mean = true } };
    const types = [_]scan.GroupColType{
        .{ .lane = .dec, .dec = .{ .precision = 38, .scale = 2 }, .nullable = true },
        .{ .lane = .dec, .dec = .{ .precision = 38, .scale = 4 }, .nullable = true },
        .{ .lane = .i64, .nullable = true },
        .{ .lane = .f64, .nullable = true },
    };
    const row = [_]scan.AggValue{ dec2(12345), .{ .dec = .{ .v = 250000, .spec = .{ .precision = 38, .scale = 4 } } }, .{ .i = 7 }, .{ .f = 2.5 } };
    // DECIMAL(38,2) * DECIMAL(38,4) is DECIMAL(38,6), exactly: 123.45 * 25 = 3086.25.
    const product = try evalPost(.{ .binary = .{ .op = .mul, .left = &col0, .right = &col1 } }, &types, &row);
    try std.testing.expectEqual(@as(i128, 3086250000), product.dec.v);
    try std.testing.expectEqual(@as(u8, 6), product.dec.spec.scale);
    // `+` takes the larger scale; an integer widens to DECIMAL.
    const sum = try evalPost(.{ .binary = .{ .op = .add, .left = &col0, .right = &.{ .col = .{ .column = 2 } } } }, &types, &row);
    try std.testing.expectEqual(@as(i128, 12345 + 700), sum.dec.v);
    // `/` is DOUBLE, for integers and DECIMALs alike; an AVG's decimal mean is a double already.
    const quotient = try evalPost(.{ .binary = .{ .op = .div, .left = &.{ .col = .{ .column = 2 } }, .right = &.{ .number = "2" } } }, &types, &row);
    try std.testing.expectEqual(@as(f64, 3.5), quotient.f);
    const ratio_dec = try evalPost(.{ .binary = .{ .op = .div, .left = &col0, .right = &col1 } }, &types, &row);
    try std.testing.expectEqual(@as(f64, 123.45 / 25.0), ratio_dec.f);
    try std.testing.expectEqual(@as(f64, 25.0), (try evalPost(mean1, &types, &row)).f);
    // Double arithmetic stays double; NULL propagates; a zero divisor is an infinity, 0/0 NaN.
    const sum_f = try evalPost(.{ .binary = .{ .op = .add, .left = &.{ .col = .{ .column = 3 } }, .right = &.{ .number = "1" } } }, &types, &row);
    try std.testing.expectEqual(@as(f64, 3.5), sum_f.f);
    const null_row = [_]scan.AggValue{ .null_val, .null_val, .null_val, .null_val };
    try std.testing.expect((try evalPost(.{ .binary = .{ .op = .mul, .left = &col0, .right = &.{ .number = "2" } } }, &types, &null_row)) == .null_val);
    try std.testing.expect(std.math.isInf((try evalPost(.{ .binary = .{ .op = .div, .left = &.{ .number = "1" }, .right = &.{ .number = "0" } } }, &types, &row)).f));
    try std.testing.expect(std.math.isNan((try evalPost(.{ .binary = .{ .op = .div, .left = &.{ .number = "0" }, .right = &.{ .number = "0.0" } } }, &types, &row)).f));
    // round(x, n): half away from zero; a DOUBLE as DuckDB scales it (2.675 is 2.68 there), a DECIMAL exactly.
    const rounded_f = try evalPost(.{ .round = .{ .value = &.{ .number = "2.675e0" }, .digits = 2 } }, &types, &row);
    try std.testing.expectEqual(@as(f64, 2.68), rounded_f.f);
    const rounded_neg = try evalPost(.{ .round = .{ .value = &.{ .neg = &.{ .number = "2.5e0" } }, .digits = 0 } }, &types, &row);
    try std.testing.expectEqual(@as(f64, -3), rounded_neg.f);
    const rounded_dec = try evalPost(.{ .round = .{ .value = &col0, .digits = 1 } }, &types, &row);
    try std.testing.expectEqual(@as(i128, 1235), rounded_dec.dec.v); // 123.45 -> 123.5
    try std.testing.expectEqual(@as(u8, 1), rounded_dec.dec.spec.scale);
    const rounded_tens = try evalPost(.{ .round = .{ .value = &col0, .digits = -1 } }, &types, &row);
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
            };
            try program.finish(gpa, &out, &col_types);
            try std.testing.expectEqualStrings("b", out.group_rows.?[0][0].s);
        }
    };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Probe.run, .{});
}
