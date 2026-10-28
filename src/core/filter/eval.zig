//! Value-level filter evaluation against a decoded batch.
//!
//! Inputs:
//!   - A typed slice of decoded values (one per row).
//!   - A SelectionVector tracking which rows are still "alive."
//!   - A leaf predicate or composite Filter.
//!
//! Output: the SelectionVector is mutated in place. Rows that fail
//! the predicate get their bit cleared.
//!
//! AND composes naturally: evaluate left, then right — bits cleared
//! by either stay cleared. OR uses a snapshot/restore + bitwise OR
//! of the two children's results.

const std = @import("std");
const ast = @import("ast.zig");
const selection = @import("selection.zig");
const in_set = @import("in_set.zig");

pub const Error = error{
    TypeMismatch,
    BadColumn,
    /// Filter referenced a column with max_rep > 0 (LIST / MAP).
    /// List-element predicates need repetition-aware semantics, so this
    /// evaluator rejects them instead of flattening ambiguously.
    NestedFilterNotSupported,
    /// A DECIMAL leaf met a column decoded without its exact integers (see `ColumnT.unscaled`).
    DecimalUnavailable,
} || std.mem.Allocator.Error;

inline fn rejectIfNested(col: anytype) Error!void {
    if (col.max_rep > 0) return error.NestedFilterNotSupported;
}

/// SQL three-valued logic: any predicate involving NULL evaluates to
/// NULL → row not selected. `def_levels[i] < max_def` means row i is
/// null for this column. Returns true iff the row should be excluded
/// purely on null grounds (caller skips the value comparison).
inline fn rowIsNull(def_levels: ?[]const u32, max_def: u32, i: usize) bool {
    if (def_levels) |dl| {
        return dl[i] < max_def;
    }
    return false;
}

/// Evaluate a leaf predicate of the given comptime type.
/// `values[i]` corresponds to row `i` in the SelectionVector.
/// When `def_levels` is non-null, rows where def_levels[i] < max_def
/// are treated as null and fail the predicate (SQL three-valued logic).
pub fn evalLeaf(
    comptime T: type,
    values: []const T,
    def_levels: ?[]const u32,
    max_def: u32,
    op: ast.Operator,
    needle: T,
    sel: *selection.SelectionVector,
) void {
    std.debug.assert(values.len == sel.len);
    switch (op) {
        inline else => |o| filterWords(T, T, o, values, def_levels, max_def, needle, sel.mask),
    }
}

/// A DECIMAL leaf over unscaled integers of the width they were decoded at. A needle past the lane's range compares
/// as the wider value would: every stored value is below a positive one and above a negative one.
fn evalLeafExact(
    comptime T: type,
    values: []const T,
    def_levels: ?[]const u32,
    max_def: u32,
    op: ast.Operator,
    needle: i128,
    sel: *selection.SelectionVector,
) void {
    const n = std.math.cast(T, needle) orelse {
        const above = needle > 0;
        const holds = switch (op) {
            .Eq => false,
            .NotEq => true,
            .Lt, .LtEq => above,
            .Gt, .GtEq => !above,
        };
        if (holds) evalNotNull(def_levels, max_def, sel) else @memset(sel.mask, 0);
        return;
    };
    evalLeaf(T, values, def_levels, max_def, op, n, sel);
}

/// Unsigned predicate over a signed lane. `U` is the lane's unsigned twin: u32 for raw INT32 bits, u64 for an i64
/// lane, which holds either a zero-extended u32 or raw u64 bits; either way the bitcast is the stored value.
pub fn evalLeafUnsigned(
    comptime U: type,
    values: []const @Int(.signed, @bitSizeOf(U)),
    def_levels: ?[]const u32,
    max_def: u32,
    op: ast.Operator,
    needle: u64,
    sel: *selection.SelectionVector,
) void {
    std.debug.assert(values.len == sel.len);
    // A needle past the lane's range compares as `u64` would: every stored value is below it.
    const n = std.math.cast(U, needle) orelse {
        const holds = switch (op) {
            .Eq, .Gt, .GtEq => false,
            .NotEq, .Lt, .LtEq => true,
        };
        if (holds) evalNotNull(def_levels, max_def, sel) else @memset(sel.mask, 0);
        return;
    };
    switch (op) {
        inline else => |o| filterWords(@TypeOf(values[0]), U, o, values, def_levels, max_def, n, sel.mask),
    }
}

/// `mask[w] &= rows w*64.. present and passing `v op needle``: 64 rows compared as one vector into a word of results,
/// then ANDed with validity. No branch looks at a row; a word the selection already cleared is skipped whole. `C` is
/// the type compared in (an unsigned twin bit-casts the lane).
fn filterWords(
    comptime L: type,
    comptime C: type,
    comptime op: ast.Operator,
    values: []const L,
    def_levels: ?[]const u32,
    max_def: u32,
    needle: C,
    mask: []u64,
) void {
    const V = @Vector(64, C);
    const nv: V = @splat(needle);
    const full = values.len / 64;
    for (mask[0..full], 0..) |*word, w| {
        if (word.* == 0) continue;
        const v: V = @bitCast(@as(@Vector(64, L), values[w * 64 ..][0..64].*));
        var bits: u64 = @bitCast(compare(op, V, v, nv));
        if (def_levels) |dl| {
            const d: @Vector(64, u32) = dl[w * 64 ..][0..64].*;
            bits &= @bitCast(d == @as(@Vector(64, u32), @splat(max_def)));
        }
        word.* &= bits;
    }
    if (full * 64 < values.len) {
        const word = &mask[full];
        var bits: u64 = 0;
        for (values[full * 64 ..], full * 64..) |x, i| {
            const pass = ast.applyOp(C, @bitCast(x), op, needle) and !rowIsNull(def_levels, max_def, i);
            bits |= @as(u64, @intFromBool(pass)) << @intCast(i - full * 64);
        }
        word.* &= bits;
    }
}

inline fn compare(comptime op: ast.Operator, comptime V: type, a: V, b: V) @Vector(@typeInfo(V).vector.len, bool) {
    if (comptime @typeInfo(@typeInfo(V).vector.child) == .float) return ast.floatOp(op, V, a, b);
    return switch (op) {
        .Eq => a == b,
        .NotEq => a != b,
        .Lt => a < b,
        .LtEq => a <= b,
        .Gt => a > b,
        .GtEq => a >= b,
    };
}

/// `sel &= present`, for a predicate every present value passes.
fn evalNotNull(def_levels: ?[]const u32, max_def: u32, sel: *selection.SelectionVector) void {
    const dl = def_levels orelse return;
    for (dl, 0..) |d, i| if (d < max_def) sel.set(i, false);
}

/// `IS NULL` / `IS NOT NULL` against a column's definition levels.
/// A row is null when `def_levels[i] < max_def`; a REQUIRED column
/// (def_levels == null or max_def == 0) has no nulls, so IS NULL keeps
/// nothing and IS NOT NULL keeps everything. Unlike value predicates,
/// this is the one filter where null rows can SELECT (IS NULL).
pub fn evalNullCheck(
    def_levels: ?[]const u32,
    max_def: u32,
    is_not: bool,
    sel: *selection.SelectionVector,
) void {
    var i: usize = 0;
    while (i < sel.len) : (i += 1) {
        if (!sel.isActive(i)) continue;
        const is_null = rowIsNull(def_levels, max_def, i);
        const keep = if (is_not) !is_null else is_null;
        if (!keep) sel.set(i, false);
    }
}

/// `LIKE` / `NOT LIKE` on a string column. Null rows fail (SQL 3VL).
pub fn evalLike(
    values: []const []const u8,
    def_levels: ?[]const u32,
    max_def: u32,
    m: ast.LikeMatch,
    sel: *selection.SelectionVector,
) void {
    std.debug.assert(values.len == sel.len);
    var i: usize = 0;
    while (i < values.len) : (i += 1) {
        if (!sel.isActive(i)) continue;
        if (rowIsNull(def_levels, max_def, i)) {
            sel.set(i, false);
            continue;
        }
        const hit = ast.likeMatches(m.kind, m.operand, values[i]);
        if (hit == m.negate) sel.set(i, false); // NOT LIKE flips the keep test
    }
}

/// One compare per dictionary entry, then a code lookup. A codes-only column has no per-row bytes.
fn evalStringColumn(
    col: ColumnT([]const u8),
    op: ast.Operator,
    needle: []const u8,
    sel: *selection.SelectionVector,
    arena: std.mem.Allocator,
) !void {
    if (col.codes) |codes| {
        const match = try arena.alloc(u8, col.dict.len);
        for (col.dict, match) |entry, *bit| bit.* = @intFromBool(ast.applyOpStr(entry, op, needle));
        applyCodeMatch(codes, match, col.def_levels, col.max_def, sel);
        return;
    }
    if (col.byte_lengths) |lens| {
        if (needle.len == 0 and (op == .Eq or op == .NotEq)) {
            evalLengths(lens, col.def_levels, col.max_def, op == .NotEq, sel);
            return;
        }
    }
    evalLeafStr(col.values, col.def_levels, col.max_def, op, needle, sel);
}

/// `= ''` / `<> ''` from PLAIN lengths. `want_nonempty` keeps rows whose length is not 0.
fn evalLengths(
    lengths: []const u32,
    def_levels: ?[]const u32,
    max_def: u32,
    want_nonempty: bool,
    sel: *selection.SelectionVector,
) void {
    std.debug.assert(lengths.len == sel.len);
    var i: usize = 0;
    while (i < lengths.len) : (i += 1) {
        if (!sel.isActive(i)) continue;
        if (rowIsNull(def_levels, max_def, i)) {
            sel.set(i, false);
            continue;
        }
        if ((lengths[i] != 0) != want_nonempty) sel.set(i, false);
    }
}

/// String predicate: byte-comparison.
pub fn evalLeafStr(
    values: []const []const u8,
    def_levels: ?[]const u32,
    max_def: u32,
    op: ast.Operator,
    needle: []const u8,
    sel: *selection.SelectionVector,
) void {
    std.debug.assert(values.len == sel.len);
    var i: usize = 0;
    while (i < values.len) : (i += 1) {
        if (!sel.isActive(i)) continue;
        if (rowIsNull(def_levels, max_def, i)) {
            sel.set(i, false);
            continue;
        }
        if (!ast.applyOpStr(values[i], op, needle)) {
            sel.set(i, false);
        }
    }
}

/// Boolean predicate.
pub fn evalLeafBool(
    values: []const bool,
    def_levels: ?[]const u32,
    max_def: u32,
    op: ast.Operator,
    needle: bool,
    sel: *selection.SelectionVector,
) void {
    std.debug.assert(values.len == sel.len);
    var i: usize = 0;
    while (i < values.len) : (i += 1) {
        if (!sel.isActive(i)) continue;
        if (rowIsNull(def_levels, max_def, i)) {
            sel.set(i, false);
            continue;
        }
        const v = values[i];
        // Boolean ordering: false (0) < true (1), matching DuckDB/SQL.
        // (Previously range ops silently returned false — a footgun.)
        const a: u1 = @intFromBool(v);
        const b: u1 = @intFromBool(needle);
        const pass = switch (op) {
            .Eq => a == b,
            .NotEq => a != b,
            .Lt => a < b,
            .LtEq => a <= b,
            .Gt => a > b,
            .GtEq => a >= b,
        };
        if (!pass) sel.set(i, false);
    }
}

/// Per-column view of decoded values + null mask + (for nested
/// columns) repetition levels.
///
/// For flat / struct-of-primitive columns: `rep_levels == null`,
/// `max_rep == 0`. Each leaf value corresponds 1:1 to a logical row.
///
/// For nested columns (list/map): `rep_levels` non-null. Each value
/// is a leaf (which may have multiple leaves per logical row). A
/// logical row starts at every position where `rep_levels[i] == 0`.
/// Higher rep values mean "continuation of this row at depth N."
/// A DECIMAL column's exact unscaled integers at the width they were decoded at, so an INT32/INT64-backed decimal
/// borrows the decoder's own buffer rather than a widened copy.
pub const Exact = union(enum) {
    i32: []const i32,
    i64: []const i64,
    i128: []const i128,

    pub fn at(self: Exact, i: usize) i128 {
        return switch (self) {
            inline else => |v| v[i],
        };
    }
};

pub fn ColumnT(comptime T: type) type {
    return struct {
        values: []const T,
        /// Null when no leaf in this column is null — either because
        /// the source is REQUIRED at every level (max_def == 0), or
        /// because `--fast-levels` proved an OPTIONAL column all-present
        /// and never materialised its levels. Both mean the same thing
        /// to every reader: no null rows, so `max_def` may be > 0 here.
        /// Otherwise length == values.len; entry < max_def → leaf null.
        def_levels: ?[]const u32 = null,
        max_def: u32 = 0,
        /// Null when max_rep == 0. Otherwise length == values.len.
        rep_levels: ?[]const u32 = null,
        max_rep: u32 = 0,
        has_nulls: bool = false,
        /// A dictionary-encoded view of the same rows. When non-null, `codes[i]` indexes
        /// `dict` for row i (0 at null rows, which `def_levels` marks). `values` may then be empty: such a column
        /// is only handed to consumers that read codes (see `consumer.scanRGForAgg`), or materialized first.
        codes: ?[]const u32 = null,
        dict: []const T = &.{},
        /// Nonzero when only the levels were decoded (no values, no codes) for this many rows.
        /// With `hist` set, the column was folded straight into a histogram over `dict` (no levels either).
        levels_only_rows: usize = 0,
        hist: ?[]const u64 = null,
        /// With `hist`, for a GROUP BY key: each dictionary entry's first row, `maxInt(u32)` for an entry no row holds.
        hist_first: ?[]const u32 = null,
        /// A DECIMAL column's exact unscaled integers, row for row with `values` (the f64 view); filters compare
        /// these. Only set on a decoded decimal; anything that moves rows (gather, selection) must move these too or
        /// drop them, and exact decimal evaluation refuses a column without them rather than fall back to the f64 view.
        unscaled: ?Exact = null,
        /// The same for a coded column's dictionary: entry for entry with `dict`.
        dict_unscaled: ?Exact = null,
        /// PLAIN BYTE_ARRAY lengths, one per row, when the payload was not copied. An empty compare (`= ''`,
        /// `<> ''`) reads these. Null rows stay in `def_levels` and hold length 0 here.
        byte_lengths: ?[]const u32 = null,

        pub fn rowCount(self: @This()) usize {
            if (self.levels_only_rows != 0) return self.levels_only_rows;
            if (self.codes) |c| return c.len;
            if (self.values.len != 0) return self.values.len;
            if (self.byte_lengths) |b| return b.len;
            return 0;
        }

        /// Values not materialized: only the codes are present.
        pub fn codesOnly(self: @This()) bool {
            return self.codes != null and self.values.len != self.codes.?.len;
        }
    };
}

/// `Batch` — caller-supplied views of decoded columns.
/// Indexed by `col_idx` from the AST.
pub const Batch = struct {
    /// Per-column views. Only the columns referenced by the filter
    /// need to be present (caller decides what to provide).
    cols: []const Column,
    num_rows: usize,
    /// Query-local execution override, read once from the caller's environment; null uses the cache budget.
    expr_batch_rows: ?usize = null,
    /// The rows a computed result is wanted for, when not all. Exact DECIMAL evaluation checks only these: a value
    /// too wide in a row the filter dropped is no error, as the SQL engines filter before they compute.
    sel: ?*const selection.SelectionVector = null,

    /// This batch, with computed results wanted only for `s`'s rows (see `sel`).
    pub fn restrictedTo(self: *const Batch, s: *const selection.SelectionVector) Batch {
        var b = self.*;
        b.sel = s;
        return b;
    }

    pub const Column = union(enum) {
        i32: ColumnT(i32),
        i64: ColumnT(i64),
        f32: ColumnT(f32),
        f64: ColumnT(f64),
        string: ColumnT([]const u8),
        boolean: ColumnT(bool),
        /// Exact DECIMAL values, unscaled; the scale travels with the expression type (`expr_ast.Dec`), not here.
        /// Produced by expression evaluation and aggregate output, never by decode.
        i128: ColumnT(i128),

        /// A borrowed flat row window; dictionaries remain shared while row-indexed buffers move together.
        pub fn sliceRows(col: Column, lo: usize, n: usize) error{BadColumn}!Column {
            return switch (col) {
                inline else => |c, tag| blk: {
                    var o = c;
                    // Empty value buffers are legitimate only for dictionary/level-only views. Every buffer
                    // that carries rows must cover the requested window; retaining a short buffer misindexes it.
                    if (c.hist == null and (lo > c.rowCount() or n > c.rowCount() - lo)) return error.BadColumn;
                    if (c.codes) |cs| o.codes = try rowWindow(u32, cs, lo, n);
                    // A string whose bytes were not copied carries only its lengths.
                    const lengths_only = c.values.len == 0 and c.byte_lengths != null;
                    if (c.values.len != 0 or
                        (!lengths_only and c.codes == null and c.levels_only_rows == 0 and c.hist == null))
                        o.values = try rowWindow(@TypeOf(c.values[0]), c.values, lo, n);
                    if (c.byte_lengths) |bl| o.byte_lengths = try rowWindow(u32, bl, lo, n);
                    if (c.def_levels) |dl| o.def_levels = try rowWindow(u32, dl, lo, n);
                    if (c.rep_levels) |rl| o.rep_levels = try rowWindow(u32, rl, lo, n);
                    if (c.unscaled) |u| o.unscaled = switch (u) {
                        inline else => |v, utag| @unionInit(
                            Exact,
                            @tagName(utag),
                            try rowWindow(@TypeOf(v[0]), v, lo, n),
                        ),
                    };
                    if (o.levels_only_rows != 0) o.levels_only_rows = n;
                    break :blk @unionInit(Column, @tagName(tag), o);
                },
            };
        }

        fn rowWindow(comptime T: type, values: []const T, lo: usize, n: usize) error{BadColumn}![]const T {
            if (lo > values.len or n > values.len - lo) return error.BadColumn;
            return values[lo..][0..n];
        }

        /// Null mask + nesting, independent of the value type — used by
        /// IS NULL / IS NOT NULL, which never touch values.
        pub fn levelsOnly(self: Column) bool {
            return switch (self) {
                inline else => |c| c.levels_only_rows != 0 and c.hist == null,
            };
        }

        pub fn hist(self: Column) ?[]const u64 {
            return switch (self) {
                inline else => |c| c.hist,
            };
        }

        pub fn histFirst(self: Column) ?[]const u32 {
            return switch (self) {
                inline else => |c| c.hist_first,
            };
        }

        pub fn codes(self: Column) ?[]const u32 {
            return switch (self) {
                inline else => |c| c.codes,
            };
        }

        /// The dictionary as a column of its own (one row per entry, no nulls), for evaluating per entry.
        pub fn dictColumn(self: Column) Column {
            return switch (self) {
                inline else => |c, tag| @unionInit(
                    Column,
                    @tagName(tag),
                    .{ .values = c.dict, .unscaled = c.dict_unscaled },
                ),
            };
        }

        pub fn dictLen(self: Column) usize {
            return switch (self) {
                inline else => |c| c.dict.len,
            };
        }

        pub fn nullInfo(self: Column) struct { def_levels: ?[]const u32, max_def: u32, max_rep: u32 } {
            return switch (self) {
                inline else => |c| .{ .def_levels = c.def_levels, .max_def = c.max_def, .max_rep = c.max_rep },
            };
        }
    };
};

/// Walk the AST and apply each predicate to `sel` against `batch`.
/// `column_lookup[col_idx]` returns the index into `batch.cols` for
/// the column that AST leaf nodes reference, or null if the caller
/// didn't supply that column. Returns error.BadColumn in that case
/// (the caller should fetch + decode the missing column first).
pub fn evaluate(
    filter: ast.Filter,
    batch: *const Batch,
    sel: *selection.SelectionVector,
    column_lookup: []const ?usize,
    arena: std.mem.Allocator,
) Error!void {
    if (leafColumn(filter)) |ci| {
        const col_pos = column_lookup[ci] orelse return error.BadColumn;
        const col = batch.cols[col_pos];
        if (col.codes()) |codes| return evalCodedLeaf(filter, ci, col, codes, sel, column_lookup.len, arena);
    }
    switch (filter) {
        .int32 => |leaf| {
            const col_pos = column_lookup[leaf.col_idx] orelse return error.BadColumn;
            const col = batch.cols[col_pos];
            if (col != .i32) return error.TypeMismatch;
            try rejectIfNested(col.i32);
            evalLeaf(i32, col.i32.values, col.i32.def_levels, col.i32.max_def, leaf.op, leaf.value, sel);
        },
        .int64 => |leaf| {
            const col_pos = column_lookup[leaf.col_idx] orelse return error.BadColumn;
            const col = batch.cols[col_pos];
            if (col != .i64) return error.TypeMismatch;
            try rejectIfNested(col.i64);
            evalLeaf(i64, col.i64.values, col.i64.def_levels, col.i64.max_def, leaf.op, leaf.value, sel);
        },
        .uint64 => |leaf| {
            const col_pos = column_lookup[leaf.col_idx] orelse return error.BadColumn;
            switch (batch.cols[col_pos]) {
                .i32 => |c| {
                    try rejectIfNested(c);
                    evalLeafUnsigned(u32, c.values, c.def_levels, c.max_def, leaf.op, leaf.value, sel);
                },
                .i64 => |c| {
                    try rejectIfNested(c);
                    evalLeafUnsigned(u64, c.values, c.def_levels, c.max_def, leaf.op, leaf.value, sel);
                },
                else => return error.TypeMismatch,
            }
        },
        .float => |leaf| {
            const col_pos = column_lookup[leaf.col_idx] orelse return error.BadColumn;
            const col = batch.cols[col_pos];
            if (col != .f32) return error.TypeMismatch;
            try rejectIfNested(col.f32);
            evalLeaf(f32, col.f32.values, col.f32.def_levels, col.f32.max_def, leaf.op, leaf.value, sel);
        },
        .double => |leaf| {
            const col_pos = column_lookup[leaf.col_idx] orelse return error.BadColumn;
            const col = batch.cols[col_pos];
            if (col != .f64) return error.TypeMismatch;
            try rejectIfNested(col.f64);
            evalLeaf(f64, col.f64.values, col.f64.def_levels, col.f64.max_def, leaf.op, leaf.value, sel);
        },
        .decimal => |leaf| {
            const col = batch.cols[column_lookup[leaf.col_idx] orelse return error.BadColumn];
            switch (col) {
                inline .f64, .i32, .i64 => |c, tag| {
                    try rejectIfNested(c);
                    const exact = c.unscaled orelse return if (tag == .f64)
                        error.DecimalUnavailable
                    else
                        error.TypeMismatch;
                    switch (exact) {
                        inline else => |v| {
                            evalLeafExact(@TypeOf(v[0]), v, c.def_levels, c.max_def, leaf.op, leaf.value, sel);
                        },
                    }
                },
                else => return error.TypeMismatch,
            }
        },
        .string => |leaf| {
            const col_pos = column_lookup[leaf.col_idx] orelse return error.BadColumn;
            const col = batch.cols[col_pos];
            if (col != .string) return error.TypeMismatch;
            try rejectIfNested(col.string);
            try evalStringColumn(col.string, leaf.op, leaf.value, sel, arena);
        },
        .boolean => |leaf| {
            const col_pos = column_lookup[leaf.col_idx] orelse return error.BadColumn;
            const col = batch.cols[col_pos];
            if (col != .boolean) return error.TypeMismatch;
            try rejectIfNested(col.boolean);
            evalLeafBool(col.boolean.values, col.boolean.def_levels, col.boolean.max_def, leaf.op, leaf.value, sel);
        },
        .null_check => |nc| {
            const col_pos = column_lookup[nc.col_idx] orelse return error.BadColumn;
            const ni = batch.cols[col_pos].nullInfo();
            if (ni.max_rep > 0) return error.NestedFilterNotSupported;
            evalNullCheck(ni.def_levels, ni.max_def, nc.is_not, sel);
        },
        .like => |m| {
            const col_pos = column_lookup[m.col_idx] orelse return error.BadColumn;
            const col = batch.cols[col_pos];
            if (col != .string) return error.TypeMismatch;
            try rejectIfNested(col.string);
            if (col.string.codes) |codes| {
                const match = try arena.alloc(u8, col.string.dict.len);
                for (col.string.dict, match) |entry, *bit| {
                    const hit = ast.likeMatches(m.kind, m.operand, entry);
                    bit.* = @intFromBool(hit != m.negate);
                }
                applyCodeMatch(codes, match, col.string.def_levels, col.string.max_def, sel);
            } else {
                evalLike(col.string.values, col.string.def_levels, col.string.max_def, m, sel);
            }
        },
        .and_filter => |c| {
            if (c.neverTrue(true)) return sel.clearAll();
            if (c.in_set) |s| return evalInList(filter, s, batch, sel, column_lookup, arena);
            // Leaves of one AND chain that test the same coded column combine into one
            // per-entry match table and a single pass over the rows.
            if (try evalAndCoded(filter, batch, sel, column_lookup, arena)) return;
            // AND composes naturally: each operand clears more bits. No state to save.
            for (c.children) |child| try evaluate(child, batch, sel, column_lookup, arena);
        },
        .or_filter => |c| {
            if (c.neverTrue(false)) return sel.clearAll();
            if (c.in_set) |s| return evalInList(filter, s, batch, sel, column_lookup, arena);
            // OR: every operand evaluates against the same entry set; their results are merged.
            var entry = try sel.cloneAlloc(arena);
            defer entry.deinit();
            var merged = try sel.cloneAlloc(arena);
            defer merged.deinit();
            @memset(merged.mask, 0);
            for (c.children) |child| {
                sel.copyFrom(&entry);
                try evaluate(child, batch, sel, column_lookup, arena);
                merged.unionWith(sel);
            }
            sel.copyFrom(&merged);
        },
    }
}

/// An IN / NOT IN list (`filter`, bound to `s`). Over a dictionary-coded column the set answers once per dictionary
/// entry and rows look their code up; otherwise it tests every row.
fn evalInList(
    filter: ast.Filter,
    s: *const in_set.InSet,
    batch: *const Batch,
    sel: *selection.SelectionVector,
    column_lookup: []const ?usize,
    arena: std.mem.Allocator,
) Error!void {
    const col_pos = column_lookup[s.col_idx] orelse return error.BadColumn;
    const col = batch.cols[col_pos];
    if (col.codes()) |codes| return evalCodedLeaf(filter, s.col_idx, col, codes, sel, column_lookup.len, arena);
    return evalInSet(s, batch, sel, column_lookup);
}

/// An IN / NOT IN list in one pass over the rows (see `in_set.zig`).
fn evalInSet(
    s: *const in_set.InSet,
    batch: *const Batch,
    sel: *selection.SelectionVector,
    column_lookup: []const ?usize,
) Error!void {
    const col_pos = column_lookup[s.col_idx] orelse return error.BadColumn;
    const col = batch.cols[col_pos];
    switch (s.probe) {
        // As `evalLeafUnsigned`: an INT32 lane holds the u32's raw bits, an i64 lane the u64's.
        .u64 => |*set| switch (col) {
            inline .i32, .i64 => |c| {
                try rejectIfNested(c);
                const L = @TypeOf(c.values[0]);
                const lane: UnsignedLane(L) = .{ .set = set };
                in_set.filterWords(L, &lane, s.negated, c.values, c.def_levels, c.max_def, sel.mask);
            },
            else => return error.TypeMismatch,
        },
        inline else => |*set, tag| {
            if (col != @field(std.meta.Tag(Batch.Column), @tagName(tag))) return error.TypeMismatch;
            const c = @field(col, @tagName(tag));
            try rejectIfNested(c);
            std.debug.assert(c.values.len == sel.len);
            in_set.filterWords(@TypeOf(c.values[0]), set, s.negated, c.values, c.def_levels, c.max_def, sel.mask);
        },
    }
}

fn UnsignedLane(comptime L: type) type {
    return struct {
        set: *const in_set.NumSet(u64),

        pub inline fn contains(self: *const @This(), v: L) bool {
            const U = @Int(.unsigned, @bitSizeOf(L));
            return self.set.contains(@as(U, @bitCast(v)));
        }
    };
}

/// The column a value leaf tests; null for AND/OR and IS [NOT] NULL (which reads levels only).
fn leafColumn(f: ast.Filter) ?usize {
    return switch (f) {
        .int32 => |l| l.col_idx,
        .int64 => |l| l.col_idx,
        .uint64 => |l| l.col_idx,
        .float => |l| l.col_idx,
        .double => |l| l.col_idx,
        .decimal => |l| l.col_idx,
        .string => |l| l.col_idx,
        .boolean => |l| l.col_idx,
        .like => |l| l.col_idx,
        .null_check, .and_filter, .or_filter => null,
    };
}

/// A value leaf over a dictionary-coded column. The predicate runs once per dictionary entry
/// (through the ordinary kernels, over the dictionary as a column), then rows reduce to one table lookup by code
/// plus the null test, 64 rows per selection word.
fn evalCodedLeaf(
    filter: ast.Filter,
    col_idx: usize,
    col: Batch.Column,
    codes: []const u32,
    sel: *selection.SelectionVector,
    lookup_len: usize,
    arena: std.mem.Allocator,
) Error!void {
    const ni = col.nullInfo();
    if (ni.max_rep > 0) return error.NestedFilterNotSupported;
    const n_dict = col.dictLen();
    const dict_cols = [_]Batch.Column{col.dictColumn()};
    const dict_batch: Batch = .{ .cols = &dict_cols, .num_rows = n_dict };
    const dict_lookup = try arena.alloc(?usize, lookup_len);
    defer arena.free(dict_lookup);
    @memset(dict_lookup, null);
    dict_lookup[col_idx] = 0;
    var dict_sel = try selection.SelectionVector.init(arena, n_dict);
    defer dict_sel.deinit();
    try evaluate(filter, &dict_batch, &dict_sel, dict_lookup, arena);
    const match = try arena.alloc(u8, n_dict);
    defer arena.free(match);
    for (match, 0..) |*m, k| m.* = @intFromBool(dict_sel.isActive(k));
    applyCodeMatch(codes, match, ni.def_levels, ni.max_def, sel);
}

fn flattenAnd(f: ast.Filter, out: *std.ArrayList(ast.Filter), arena: std.mem.Allocator) Error!void {
    switch (f) {
        // A NOT IN list stays whole: its set tests it in one pass, its `!=` leaves one pass each.
        .and_filter => |c| if (c.in_set != null or c.null_member) try out.append(arena, f) else {
            for (c.children) |child| try flattenAnd(child, out, arena);
        },
        else => try out.append(arena, f),
    }
}

/// Evaluate an AND chain, merging leaves on the same coded column. Returns false (nothing done) when no coded
/// column carries two or more leaves, leaving the ordinary recursion to it.
fn evalAndCoded(
    f: ast.Filter,
    batch: *const Batch,
    sel: *selection.SelectionVector,
    column_lookup: []const ?usize,
    arena: std.mem.Allocator,
) Error!bool {
    var leaves: std.ArrayList(ast.Filter) = .empty;
    defer leaves.deinit(arena);
    try flattenAnd(f, &leaves, arena);
    var any_group = false;
    for (leaves.items, 0..) |a, i| {
        const ca = leafColumn(a) orelse continue;
        const pa = column_lookup[ca] orelse continue;
        if (batch.cols[pa].codes() == null) continue;
        for (leaves.items[i + 1 ..]) |b| {
            if (leafColumn(b) == ca) any_group = true;
        }
    }
    if (!any_group) return false;
    const done = try arena.alloc(bool, leaves.items.len);
    defer arena.free(done);
    @memset(done, false);
    for (leaves.items, 0..) |a, i| {
        if (done[i]) continue;
        const ca = leafColumn(a) orelse continue;
        const pa = column_lookup[ca] orelse return error.BadColumn;
        const col = batch.cols[pa];
        const codes = col.codes() orelse continue;
        const ni = col.nullInfo();
        if (ni.max_rep > 0) return error.NestedFilterNotSupported;
        const n_dict = col.dictLen();
        const dict_cols = [_]Batch.Column{col.dictColumn()};
        const dict_batch: Batch = .{ .cols = &dict_cols, .num_rows = n_dict };
        const dict_lookup = try arena.alloc(?usize, column_lookup.len);
        @memset(dict_lookup, null);
        dict_lookup[ca] = 0;
        var dict_sel = try selection.SelectionVector.init(arena, n_dict);
        for (leaves.items[i..], i..) |b, j| {
            if (leafColumn(b) != ca) continue;
            try evaluate(b, &dict_batch, &dict_sel, dict_lookup, arena);
            done[j] = true;
        }
        const match = try arena.alloc(u8, n_dict);
        for (match, 0..) |*m, k| m.* = @intFromBool(dict_sel.isActive(k));
        applyCodeMatch(codes, match, ni.def_levels, ni.max_def, sel);
    }
    for (leaves.items, done) |b, d| if (!d) try evaluate(b, batch, sel, column_lookup, arena);
    return true;
}

/// `sel[r] &= match[codes[r]] and row r present`, a selection word at a time.
pub fn applyCodeMatch(
    codes: []const u32,
    match: []const u8,
    def_levels: ?[]const u32,
    max_def: u32,
    sel: *selection.SelectionVector,
) void {
    std.debug.assert(codes.len == sel.len);
    const n = codes.len;
    var w: usize = 0;
    while (w * 64 < n) : (w += 1) {
        const word = sel.mask[w];
        if (word == 0) continue;
        const base = w * 64;
        const end = @min(base + 64, n);
        var bits: u64 = 0;
        if (def_levels) |dl| {
            for (base..end) |r| {
                const keep: u64 = match[codes[r]] & @intFromBool(dl[r] == max_def);
                bits |= keep << @intCast(r - base);
            }
        } else {
            for (base..end) |r| {
                const keep: u64 = match[codes[r]];
                bits |= keep << @intCast(r - base);
            }
        }
        sel.mask[w] = word & bits;
    }
}

// ============================================================
// Tests
// ============================================================

const testing = std.testing;

test "regression: a DECIMAL leaf compares the unscaled integers, exact past 2^53 and past the lane's range" {
    // 2^53 + 1 and 2^53 round to the same f64: compared through it, `= 2^53 + 1` matched both.
    const big: i64 = (1 << 53) + 1;
    const vals = [_]i64{ big, big - 1, big + 2, 5 };
    const f64_view = [_]f64{ @floatFromInt(vals[0]), @floatFromInt(vals[1]), @floatFromInt(vals[2]), 5 };
    const dl = [_]u32{ 1, 1, 1, 0 };
    const cols = [_]Batch.Column{.{ .f64 = .{
        .values = &f64_view,
        .def_levels = &dl,
        .max_def = 1,
        .unscaled = .{ .i64 = &vals },
    } }};
    const batch: Batch = .{ .cols = &cols, .num_rows = vals.len };
    const lookup = [_]?usize{0};
    const Case = struct { op: ast.Operator, value: i128, want: []const bool };
    const huge: i128 = std.math.maxInt(i64) + @as(i128, 1);
    for ([_]Case{
        .{ .op = .Eq, .value = big, .want = &.{ true, false, false, false } },
        .{ .op = .Gt, .value = big - 1, .want = &.{ true, false, true, false } },
        .{ .op = .NotEq, .value = big, .want = &.{ false, true, true, false } },
        .{ .op = .Lt, .value = huge, .want = &.{ true, true, true, false } },
        .{ .op = .Gt, .value = huge, .want = &.{ false, false, false, false } },
        .{ .op = .Gt, .value = -huge, .want = &.{ true, true, true, false } },
        .{ .op = .Eq, .value = -huge, .want = &.{ false, false, false, false } },
    }) |c| {
        var sv = try selection.SelectionVector.init(testing.allocator, vals.len);
        defer sv.deinit();
        const leaf: ast.Filter = .{ .decimal = .{ .col_idx = 0, .op = c.op, .value = c.value } };
        try evaluate(leaf, &batch, &sv, &lookup, testing.allocator);
        for (c.want, 0..) |w, i| try testing.expectEqual(w, sv.isActive(i));
    }
    // A decimal decoded without its exact integers is refused, never compared through the f64 view.
    const bare = [_]Batch.Column{.{ .f64 = .{ .values = &f64_view } }};
    const bare_batch: Batch = .{ .cols = &bare, .num_rows = vals.len };
    var sv = try selection.SelectionVector.init(testing.allocator, vals.len);
    defer sv.deinit();
    try testing.expectError(error.DecimalUnavailable, evaluate(
        .{ .decimal = .{ .col_idx = 0, .op = .Eq, .value = 5 } },
        &bare_batch,
        &sv,
        &lookup,
        testing.allocator,
    ));
}

test "evalLeaf i32 equality" {
    const data = [_]i32{ 1, 2, 3, 2, 5 };
    var sv = try selection.SelectionVector.init(testing.allocator, data.len);
    defer sv.deinit();

    evalLeaf(i32, &data, null, 0, .Eq, 2, &sv);
    try testing.expectEqual(@as(usize, 2), sv.count());
    try testing.expect(sv.isActive(1));
    try testing.expect(sv.isActive(3));
}

test "evalLeaf i32 range" {
    const data = [_]i32{ 1, 2, 3, 4, 5 };
    var sv = try selection.SelectionVector.init(testing.allocator, data.len);
    defer sv.deinit();

    evalLeaf(i32, &data, null, 0, .GtEq, 3, &sv);
    try testing.expectEqual(@as(usize, 3), sv.count()); // 3, 4, 5
}

test "evalLeaf with nulls fails predicate (SQL three-valued logic)" {
    // Rows: 1, NULL, 3, NULL, 5. Filter `>= 2` should match rows 2,4
    // (values 3 and 5). The two nulls fail the predicate via SQL 3VL.
    const data = [_]i32{ 1, 0, 3, 0, 5 };
    const def_levels = [_]u32{ 1, 0, 1, 0, 1 };
    var sv = try selection.SelectionVector.init(testing.allocator, data.len);
    defer sv.deinit();

    evalLeaf(i32, &data, &def_levels, 1, .GtEq, 2, &sv);
    try testing.expectEqual(@as(usize, 2), sv.count());
    try testing.expect(sv.isActive(2));
    try testing.expect(sv.isActive(4));
    // Even though the null rows have value=0 which would not match
    // anyway, the test would fail if the null check were absent for
    // predicates like `< 1` that nulls' default value would satisfy.
}

test "evalLeaf treats null < anything as fail" {
    // Filter `< 100`. If we ignored nulls, all rows would pass since
    // the default 0 < 100. With proper null handling the two nulls
    // are excluded.
    const data = [_]i32{ 50, 0, 75, 0, 25 };
    const def_levels = [_]u32{ 1, 0, 1, 0, 1 };
    var sv = try selection.SelectionVector.init(testing.allocator, data.len);
    defer sv.deinit();

    evalLeaf(i32, &data, &def_levels, 1, .Lt, 100, &sv);
    try testing.expectEqual(@as(usize, 3), sv.count());
    try testing.expect(sv.isActive(0));
    try testing.expect(!sv.isActive(1)); // null
    try testing.expect(sv.isActive(2));
    try testing.expect(!sv.isActive(3)); // null
    try testing.expect(sv.isActive(4));
}

test "evalNullCheck IS NULL / IS NOT NULL + REQUIRED column" {
    // rows: present, null, present, null, present
    const def = [_]u32{ 1, 0, 1, 0, 1 };
    { // IS NULL → the two null rows
        var sv = try selection.SelectionVector.init(testing.allocator, def.len);
        defer sv.deinit();
        evalNullCheck(&def, 1, false, &sv);
        try testing.expectEqual(@as(usize, 2), sv.count());
        try testing.expect(sv.isActive(1) and sv.isActive(3));
    }
    { // IS NOT NULL → the three present rows
        var sv = try selection.SelectionVector.init(testing.allocator, def.len);
        defer sv.deinit();
        evalNullCheck(&def, 1, true, &sv);
        try testing.expectEqual(@as(usize, 3), sv.count());
        try testing.expect(sv.isActive(0) and sv.isActive(2) and sv.isActive(4));
    }
    { // REQUIRED column (no def_levels): IS NULL keeps none, IS NOT NULL keeps all
        var a = try selection.SelectionVector.init(testing.allocator, 4);
        defer a.deinit();
        evalNullCheck(null, 0, false, &a);
        try testing.expectEqual(@as(usize, 0), a.count());
        var b = try selection.SelectionVector.init(testing.allocator, 4);
        defer b.deinit();
        evalNullCheck(null, 0, true, &b);
        try testing.expectEqual(@as(usize, 4), b.count());
    }
}

test "evalLeafStr equality" {
    const data = [_][]const u8{ "alpha", "beta", "gamma", "alpha", "beta" };
    var sv = try selection.SelectionVector.init(testing.allocator, data.len);
    defer sv.deinit();

    evalLeafStr(&data, null, 0, .Eq, "alpha", &sv);
    try testing.expectEqual(@as(usize, 2), sv.count());
    try testing.expect(sv.isActive(0));
    try testing.expect(sv.isActive(3));
}

test "evalLeafStr range comparisons (bytewise order)" {
    // bytewise sort: alpha < beta < delta < gamma  ('d' 0x64 < 'g' 0x67)
    const data = [_][]const u8{ "alpha", "beta", "gamma", "delta" };

    { // Lt "delta": alpha, beta
        var sv = try selection.SelectionVector.init(testing.allocator, data.len);
        defer sv.deinit();
        evalLeafStr(&data, null, 0, .Lt, "delta", &sv);
        try testing.expect(sv.isActive(0));
        try testing.expect(sv.isActive(1));
        try testing.expect(!sv.isActive(2)); // gamma > delta
        try testing.expect(!sv.isActive(3)); // delta not < delta
    }
    { // GtEq "beta": beta, gamma, delta
        var sv = try selection.SelectionVector.init(testing.allocator, data.len);
        defer sv.deinit();
        evalLeafStr(&data, null, 0, .GtEq, "beta", &sv);
        try testing.expect(!sv.isActive(0)); // alpha < beta
        try testing.expect(sv.isActive(1));
        try testing.expect(sv.isActive(2));
        try testing.expect(sv.isActive(3));
    }
    { // Gt "beta": gamma, delta (strict)
        var sv = try selection.SelectionVector.init(testing.allocator, data.len);
        defer sv.deinit();
        evalLeafStr(&data, null, 0, .Gt, "beta", &sv);
        try testing.expect(!sv.isActive(0));
        try testing.expect(!sv.isActive(1)); // beta not > beta
        try testing.expect(sv.isActive(2));
        try testing.expect(sv.isActive(3));
    }
}

test "evalLeafBool ordering (false < true, matches DuckDB)" {
    const data = [_]bool{ true, false, true, false };
    { // Eq true → the 2 true rows
        var sv = try selection.SelectionVector.init(testing.allocator, data.len);
        defer sv.deinit();
        evalLeafBool(&data, null, 0, .Eq, true, &sv);
        try testing.expectEqual(@as(usize, 2), sv.count());
    }
    { // Gt false → true rows (false < true); was silently 0 before
        var sv = try selection.SelectionVector.init(testing.allocator, data.len);
        defer sv.deinit();
        evalLeafBool(&data, null, 0, .Gt, false, &sv);
        try testing.expectEqual(@as(usize, 2), sv.count());
        try testing.expect(sv.isActive(0)); // true > false
        try testing.expect(!sv.isActive(1)); // false not > false
    }
    { // GtEq false → all rows
        var sv = try selection.SelectionVector.init(testing.allocator, data.len);
        defer sv.deinit();
        evalLeafBool(&data, null, 0, .GtEq, false, &sv);
        try testing.expectEqual(@as(usize, 4), sv.count());
    }
}

test "evaluate AND composite — both narrow the selection" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    const ints = [_]i32{ 10, 20, 30, 40, 50 };
    const strs = [_][]const u8{ "x", "y", "x", "y", "x" };
    const cols = [_]Batch.Column{
        .{ .i32 = .{ .values = &ints } },
        .{ .string = .{ .values = &strs } },
    };
    const batch: Batch = .{ .cols = &cols, .num_rows = 5 };
    var sv = try selection.SelectionVector.init(a, 5);
    defer sv.deinit();

    // col_idx 0 is i32 column at batch.cols[0]; col_idx 1 is string at batch.cols[1].
    const lookup = [_]?usize{ 0, 1 };

    // AST: int_col >= 20 AND str_col = "x"
    const f = try ast.join(a, true, &.{
        .{ .int32 = .{ .col_idx = 0, .op = .GtEq, .value = 20 } },
        .{ .string = .{ .col_idx = 1, .op = .Eq, .value = "x" } },
    });

    try evaluate(f, &batch, &sv, &lookup, a);

    // Rows: idx2 (30, "x") and idx4 (50, "x") match.
    try testing.expectEqual(@as(usize, 2), sv.count());
    try testing.expect(sv.isActive(2));
    try testing.expect(sv.isActive(4));
}

test "evaluate OR composite — union of branches" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    const ints = [_]i32{ 10, 20, 30, 40, 50 };
    const cols = [_]Batch.Column{.{ .i32 = .{ .values = &ints } }};
    const batch: Batch = .{ .cols = &cols, .num_rows = 5 };
    var sv = try selection.SelectionVector.init(a, 5);
    defer sv.deinit();

    const lookup = [_]?usize{0};

    // AST: int_col = 10 OR int_col >= 40
    const f = try ast.join(a, false, &.{
        .{ .int32 = .{ .col_idx = 0, .op = .Eq, .value = 10 } },
        .{ .int32 = .{ .col_idx = 0, .op = .GtEq, .value = 40 } },
    });

    try evaluate(f, &batch, &sv, &lookup, a);

    try testing.expectEqual(@as(usize, 3), sv.count());
    try testing.expect(sv.isActive(0)); // 10
    try testing.expect(sv.isActive(3)); // 40
    try testing.expect(sv.isActive(4)); // 50
}

test "evaluate unsigned leaf over raw i32, zero-extended i64, and raw u64 lanes" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const raw32 = [_]i32{ 1, @bitCast(@as(u32, 3_000_000_000)), 2 };
    const wide32 = [_]i64{ 1, 3_000_000_000, 2 };
    const raw64 = [_]i64{ 1, @bitCast(@as(u64, 18_000_000_000_000_000_000)), 2 };
    const cols = [_]Batch.Column{
        .{ .i32 = .{ .values = &raw32 } },
        .{ .i64 = .{ .values = &wide32 } },
        .{ .i64 = .{ .values = &raw64 } },
    };
    const batch: Batch = .{ .cols = &cols, .num_rows = 3 };
    const lookup = [_]?usize{ 0, 1, 2 };
    const needles = [_]u64{ 2_147_483_648, 2_147_483_648, 9_223_372_036_854_775_808 };
    for (needles, 0..) |needle, col| {
        var sv = try selection.SelectionVector.init(testing.allocator, 3);
        defer sv.deinit();
        const f: ast.Filter = .{ .uint64 = .{ .col_idx = col, .op = .Gt, .value = needle } };
        try evaluate(f, &batch, &sv, &lookup, a);
        try testing.expectEqual(@as(usize, 1), sv.count());
        try testing.expect(sv.isActive(1));
    }
}

test "a coded column filters exactly as its values do, nulls and same-column AND chains included" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    // Rows: x, NULL, z, y, x, z, NULL, y over the dictionary {x, y, z}; the coded view holds 0 at null rows.
    const dict = [_][]const u8{ "x", "y", "z" };
    const codes = [_]u32{ 0, 0, 2, 1, 0, 2, 0, 1 };
    const defs = [_]u32{ 1, 0, 1, 1, 1, 1, 0, 1 };
    const vals = [_][]const u8{ "x", "", "z", "y", "x", "z", "", "y" };
    const ints = [_]i32{ 5, 6, 7, 8, 9, 10, 11, 12 };
    const coded: Batch.Column = .{ .string = .{
        .values = &.{},
        .codes = &codes,
        .dict = &dict,
        .def_levels = &defs,
        .max_def = 1,
        .has_nulls = true,
    } };
    const plain: Batch.Column = .{ .string = .{ .values = &vals, .def_levels = &defs, .max_def = 1, .has_nulls = true } };
    const lookup = [_]?usize{ 0, 1 };

    const ge_y = try a.create(ast.Filter);
    ge_y.* = .{ .string = .{ .col_idx = 0, .op = .GtEq, .value = "y" } };
    const ne_z = try a.create(ast.Filter);
    ne_z.* = .{ .string = .{ .col_idx = 0, .op = .NotEq, .value = "z" } };
    const int_ge = try a.create(ast.Filter);
    int_ge.* = .{ .int32 = .{ .col_idx = 1, .op = .GtEq, .value = 7 } };
    const same_col = try ast.join(a, true, &.{ ge_y.*, ne_z.* });
    const mixed = try ast.join(a, true, &.{ same_col, int_ge.* });

    for ([_]ast.Filter{ ge_y.*, ne_z.*, same_col, mixed }) |f| {
        var want = try selection.SelectionVector.init(a, vals.len);
        const plain_cols = [_]Batch.Column{ plain, .{ .i32 = .{ .values = &ints } } };
        try evaluate(f, &.{ .cols = &plain_cols, .num_rows = vals.len }, &want, &lookup, a);
        var got = try selection.SelectionVector.init(a, vals.len);
        const coded_cols = [_]Batch.Column{ coded, .{ .i32 = .{ .values = &ints } } };
        try evaluate(f, &.{ .cols = &coded_cols, .num_rows = vals.len }, &got, &lookup, a);
        try testing.expectEqualSlices(u64, want.mask, got.mask);
    }
}

test "a codes-only string compares once per dictionary entry, including an empty literal" {
    const dict = [_][]const u8{ "", "ab", "a" };
    const codes = [_]u32{ 1, 0, 2, 1 };
    const coded: Batch.Column = .{ .string = .{ .values = &.{}, .codes = &codes, .dict = &dict } };
    const plain_vals = [_][]const u8{ "ab", "", "a", "ab" };
    const plain: Batch.Column = .{ .string = .{ .values = &plain_vals } };
    const lookup = [_]?usize{0};
    for ([_]ast.Filter{
        .{ .string = .{ .col_idx = 0, .op = .Eq, .value = "ab" } },
        .{ .string = .{ .col_idx = 0, .op = .NotEq, .value = "" } },
        .{ .string = .{ .col_idx = 0, .op = .Eq, .value = "" } },
    }) |f| {
        var want = try selection.SelectionVector.init(testing.allocator, codes.len);
        defer want.deinit();
        var got = try selection.SelectionVector.init(testing.allocator, codes.len);
        defer got.deinit();
        try evaluate(f, &.{ .cols = &.{plain}, .num_rows = codes.len }, &want, &lookup, testing.allocator);
        try evaluate(f, &.{ .cols = &.{coded}, .num_rows = codes.len }, &got, &lookup, testing.allocator);
        try testing.expectEqualSlices(u64, want.mask, got.mask);
    }
}

test "an empty compare reads PLAIN lengths and does not need the bytes" {
    const lengths = [_]u32{ 0, 4, 0, 1 };
    const defs = [_]u32{ 1, 1, 0, 1 };
    const col: Batch.Column = .{ .string = .{
        .values = &.{},
        .byte_lengths = &lengths,
        .def_levels = &defs,
        .max_def = 1,
        .has_nulls = true,
    } };
    const lookup = [_]?usize{0};
    const nonempty: ast.Filter = .{ .string = .{ .col_idx = 0, .op = .NotEq, .value = "" } };
    var sv = try selection.SelectionVector.init(testing.allocator, lengths.len);
    defer sv.deinit();
    try evaluate(nonempty, &.{ .cols = &.{col}, .num_rows = lengths.len }, &sv, &lookup, testing.allocator);
    try testing.expect(!sv.isActive(0));
    try testing.expect(sv.isActive(1));
    try testing.expect(!sv.isActive(2));
    try testing.expect(sv.isActive(3));
}

test "a window of a lengths-only string answers its empty compare as the same rows of the whole column do" {
    var lengths: [130]u32 = undefined;
    var defs: [130]u32 = undefined;
    for (&lengths, &defs, 0..) |*l, *d, r| {
        d.* = @intFromBool(r % 5 != 0);
        l.* = if (d.* == 0) 0 else @intCast(r % 3);
    }
    const col: Batch.Column = .{ .string = .{
        .values = &.{},
        .byte_lengths = &lengths,
        .def_levels = &defs,
        .max_def = 1,
        .has_nulls = true,
    } };
    const lookup = [_]?usize{0};
    const nonempty: ast.Filter = .{ .string = .{ .col_idx = 0, .op = .NotEq, .value = "" } };
    var whole = try selection.SelectionVector.init(testing.allocator, lengths.len);
    defer whole.deinit();
    try evaluate(nonempty, &.{ .cols = &.{col}, .num_rows = lengths.len }, &whole, &lookup, testing.allocator);
    const lo = 64;
    const n = lengths.len - lo;
    const win = try col.sliceRows(lo, n);
    try testing.expectEqual(n, win.string.byte_lengths.?.len);
    var part = try selection.SelectionVector.init(testing.allocator, n);
    defer part.deinit();
    try evaluate(nonempty, &.{ .cols = &.{win}, .num_rows = n }, &part, &lookup, testing.allocator);
    for (0..n) |r| try testing.expectEqual(whole.isActive(lo + r), part.isActive(r));
}

test "leaf kernels select exactly what a row-by-row compare does: every op, nulls, NaN, ±0, unsigned, ragged tails" {
    var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_state.deinit();
    const a = arena_state.allocator();
    var prng = std.Random.DefaultPrng.init(0x5e1);
    const r = prng.random();
    const ops = [_]ast.Operator{ .Eq, .NotEq, .Lt, .LtEq, .Gt, .GtEq };
    for ([_]usize{ 0, 1, 63, 64, 65, 200, 1000 }) |n| {
        const defs = try a.alloc(u32, n);
        for (defs) |*d| d.* = @intFromBool(r.uintLessThan(u8, 5) != 0);
        const start = try a.alloc(bool, n);
        for (start) |*s| s.* = r.uintLessThan(u8, 4) != 0;
        inline for (.{ i32, i64, f32, f64 }) |T| {
            const vals = try a.alloc(T, n);
            const specials = [_]f64{ 0.0, -0.0, std.math.nan(f64), std.math.inf(f64), -std.math.inf(f64), 3, -3 };
            for (vals) |*v| v.* = if (@typeInfo(T) == .float)
                @floatCast(specials[r.uintLessThan(usize, specials.len)])
            else
                r.intRangeAtMost(T, -4, 4);
            const needles: []const T = if (@typeInfo(T) == .float)
                &.{ 0.0, -0.0, std.math.nan(T), 3, std.math.inf(T) }
            else
                &.{ 0, 3, -4, std.math.minInt(T), std.math.maxInt(T) };
            for (ops) |op| for (needles) |needle| for ([_]bool{ false, true }) |nullable| {
                var sel = try selection.SelectionVector.init(a, n);
                for (start, 0..) |s, i| sel.set(i, s);
                const dl: ?[]const u32 = if (nullable) defs else null;
                evalLeaf(T, vals, dl, 1, op, needle, &sel);
                for (0..n) |i| {
                    const want = start[i] and !(nullable and defs[i] == 0) and ast.applyOp(T, vals[i], op, needle);
                    try testing.expectEqual(want, sel.isActive(i));
                }
            };
        }
        inline for (.{ u32, u64 }) |U| {
            const S = @Int(.signed, @bitSizeOf(U));
            const vals = try a.alloc(S, n);
            for (vals) |*v| {
                const u: U = if (r.boolean()) r.uintLessThan(U, 5) else std.math.maxInt(U) - r.uintLessThan(U, 3);
                v.* = @bitCast(u);
            }
            const needles = [_]u64{ 0, 3, std.math.maxInt(U), std.math.maxInt(U) - 1, 1 << 40, std.math.maxInt(u64) };
            for (ops) |op| for (needles) |needle| {
                var sel = try selection.SelectionVector.init(a, n);
                for (start, 0..) |s, i| sel.set(i, s);
                evalLeafUnsigned(U, vals, defs, 1, op, needle, &sel);
                for (0..n) |i| {
                    const v: u64 = @as(U, @bitCast(vals[i]));
                    const want = start[i] and defs[i] == 1 and ast.applyOp(u64, v, op, needle);
                    try testing.expectEqual(want, sel.isActive(i));
                }
            };
        }
    }
}

test "IN and NOT IN sets over a coded column select as they do over its values, inside AND chains too" {
    var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_state.deinit();
    const a = arena_state.allocator();
    var prng = std.Random.DefaultPrng.init(0xc0de);
    const r = prng.random();
    const dict = [_][]const u8{ "a", "b", "c", "d", "e", "f", "g", "h", "i", "j" };
    const n = 333;
    var codes: [n]u32 = undefined;
    var defs: [n]u32 = undefined;
    var vals: [n][]const u8 = undefined;
    for (0..n) |i| {
        defs[i] = @intFromBool(r.uintLessThan(u8, 6) != 0);
        codes[i] = if (defs[i] == 1) r.uintLessThan(u32, dict.len) else 0;
        vals[i] = if (defs[i] == 1) dict[codes[i]] else "";
    }
    const coded: Batch.Column = .{ .string = .{
        .values = &.{},
        .codes = &codes,
        .dict = &dict,
        .def_levels = &defs,
        .max_def = 1,
        .has_nulls = true,
    } };
    const plain: Batch.Column = .{ .string = .{ .values = &vals, .def_levels = &defs, .max_def = 1, .has_nulls = true } };
    const lookup = [_]?usize{0};
    const list = [_][]const u8{ "b", "d", "e", "zz", "h" };

    for ([_]bool{ false, true }) |negated| {
        // The parser's shape: one node of leaves joined by OR (AND for NOT IN), the set bound on it.
        var leaves: [list.len]ast.Filter = undefined;
        for (list, &leaves) |v, *leaf|
            leaf.* = .{ .string = .{ .col_idx = 0, .op = if (negated) .NotEq else .Eq, .value = v } };
        const set = try a.create(in_set.InSet);
        set.* = .{ .col_idx = 0, .negated = negated, .probe = .{ .string = try .init(a, &list) } };
        const node: ast.Composite = .{ .children = &leaves, .in_set = set };
        const tree: ?ast.Filter = if (negated) .{ .and_filter = node } else .{ .or_filter = node };
        // Also inside an AND chain with another leaf on the same coded column: the list stays one operand.
        const other: ast.Filter = .{ .string = .{ .col_idx = 0, .op = .LtEq, .value = "f" } };
        const chained = try ast.join(a, true, &.{ other, tree.? });

        for ([_]ast.Filter{ tree.?, chained }) |f| {
            var want = try selection.SelectionVector.init(a, n);
            try evaluate(f, &.{ .cols = &.{plain}, .num_rows = n }, &want, &lookup, a);
            var got = try selection.SelectionVector.init(a, n);
            try evaluate(f, &.{ .cols = &.{coded}, .num_rows = n }, &got, &lookup, a);
            try testing.expectEqualSlices(u64, want.mask, got.mask);
        }
        // And the plain answer is the list's: present rows, listed (or not, for NOT IN).
        var plain_sel = try selection.SelectionVector.init(a, n);
        try evaluate(tree.?, &.{ .cols = &.{plain}, .num_rows = n }, &plain_sel, &lookup, a);
        for (0..n) |i| {
            var listed = false;
            for (list) |v| listed = listed or std.mem.eql(u8, v, vals[i]);
            try testing.expectEqual(defs[i] == 1 and listed != negated, plain_sel.isActive(i));
        }
    }
}

test "row windows preserve coded and exact views and reject short buffers" {
    const vals = [_]i32{ 10, 20, 30 };
    const codes = [_]u32{ 0, 1, 0 };
    const defs = [_]u32{ 1, 0, 1 };
    const exact = [_]i64{ 100, 200, 300 };
    const plain: Batch.Column = .{ .i32 = .{
        .values = &vals,
        .def_levels = &defs,
        .unscaled = .{ .i64 = &exact },
        .max_def = 1,
    } };
    const win = try plain.sliceRows(1, 2);
    try std.testing.expectEqualSlices(i32, vals[1..], win.i32.values);
    try std.testing.expectEqualSlices(u32, defs[1..], win.i32.def_levels.?);
    try std.testing.expectEqualSlices(i64, exact[1..], win.i32.unscaled.?.i64);
    var coded: Batch.Column = .{ .i32 = .{ .values = &.{}, .codes = &codes, .dict = &vals } };
    const cw = try coded.sliceRows(1, 2);
    try std.testing.expectEqualSlices(u32, codes[1..], cw.i32.codes.?);
    try std.testing.expectEqual(@as(usize, 0), cw.i32.values.len);
    coded.i32.def_levels = defs[0..1];
    try std.testing.expectError(error.BadColumn, coded.sliceRows(1, 2));
    coded.i32.def_levels = null;
    coded.i32.values = vals[0..1];
    try std.testing.expectError(error.BadColumn, coded.sliceRows(1, 2));
    try std.testing.expectError(error.BadColumn, plain.sliceRows(2, 2));
}

test "an AND ranked cheapest first selects what its written order selects, and fails with the same first error" {
    var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_state.deinit();
    const a = arena_state.allocator();
    var prng: std.Random.DefaultPrng = .init(0xc0471);
    const r = prng.random();
    const n = 1000;
    const ints = try a.alloc(i32, n);
    const int_dl = try a.alloc(u32, n);
    const phrases = try a.alloc([]const u8, n);
    const phrase_dl = try a.alloc(u32, n);
    const urls = try a.alloc([]const u8, n);
    const words = [_][]const u8{ "", "", "", "google", "a google b", "http://x/", "goo", "ab" };
    for (0..n) |i| {
        ints[i] = r.intRangeAtMost(i32, -3, 3);
        int_dl[i] = @intFromBool(r.uintLessThan(u8, 5) != 0);
        phrases[i] = words[r.uintLessThan(usize, words.len)];
        phrase_dl[i] = @intFromBool(r.uintLessThan(u8, 6) != 0);
        urls[i] = words[r.uintLessThan(usize, words.len)];
    }
    const f64_view = try a.alloc(f64, n);
    @memset(f64_view, 0);
    const cols = [_]Batch.Column{
        .{ .i32 = .{ .values = ints, .def_levels = int_dl, .max_def = 1, .has_nulls = true } },
        .{ .string = .{ .values = phrases, .def_levels = phrase_dl, .max_def = 1, .has_nulls = true } },
        .{ .string = .{ .values = urls } },
        // A DECIMAL decoded without its exact integers: its leaf fails on evaluation.
        .{ .f64 = .{ .values = f64_view } },
    };
    const batch: Batch = .{ .cols = &cols, .num_rows = n };
    const lookup = [_]?usize{ 0, 1, 2, 3 };
    const contains: ast.Filter = .{ .like = .{ .col_idx = 2, .kind = .contains, .operand = "google", .negate = false } };
    const prefix: ast.Filter = .{ .like = .{ .col_idx = 2, .kind = .prefix, .operand = "a", .negate = true } };
    const nonempty: ast.Filter = .{ .string = .{ .col_idx = 1, .op = .NotEq, .value = "" } };
    const word: ast.Filter = .{ .string = .{ .col_idx = 1, .op = .NotEq, .value = "goo" } };
    const small: ast.Filter = .{ .int32 = .{ .col_idx = 0, .op = .LtEq, .value = 2 } };
    const never: ast.Filter = .{ .int32 = .{ .col_idx = 0, .op = .Gt, .value = std.math.maxInt(i32) } };
    const present: ast.Filter = .{ .null_check = .{ .col_idx = 1, .is_not = true } };
    const cases = [_][]const ast.Filter{
        &.{ contains, nonempty, small, prefix, word, present },
        &.{ prefix, contains, word },
        &.{ contains, never, nonempty },
        &.{ present, contains },
    };
    for (cases) |written| {
        const ranked = try ast.join(a, true, written);
        // Cheapest first; the join kept every child.
        for (ranked.and_filter.children[1..], ranked.and_filter.children[0 .. written.len - 1]) |later, earlier|
            try testing.expect(@backingInt(ast.cost(earlier)) <= @backingInt(ast.cost(later)));
        var want = try selection.SelectionVector.init(a, n);
        try evaluate(.{ .and_filter = .{ .children = written } }, &batch, &want, &lookup, a);
        var got = try selection.SelectionVector.init(a, n);
        try evaluate(ranked, &batch, &got, &lookup, a);
        try testing.expectEqualSlices(u64, want.mask, got.mask);
    }
    // Two leaves that fail, in one cost class: the first written is the one that reports, ranked or not.
    const mismatch: ast.Filter = .{ .int32 = .{ .col_idx = 1, .op = .Eq, .value = 1 } };
    const bare: ast.Filter = .{ .decimal = .{ .col_idx = 3, .op = .Eq, .value = 1 } };
    for ([_][]const ast.Filter{ &.{ contains, mismatch, bare }, &.{ contains, bare, mismatch } }, [_]anyerror{
        error.TypeMismatch,
        error.DecimalUnavailable,
    }) |written, err| {
        var sv = try selection.SelectionVector.init(a, n);
        try testing.expectError(err, evaluate(.{ .and_filter = .{ .children = written } }, &batch, &sv, &lookup, a));
        sv = try selection.SelectionVector.init(a, n);
        try testing.expectError(err, evaluate(try ast.join(a, true, written), &batch, &sv, &lookup, a));
    }
}
