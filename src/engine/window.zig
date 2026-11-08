//! Window functions over a Parquet file that is already in window order. The file's rows are sorted by the window's
//! PARTITION BY columns and then its ORDER BY columns (the SQL layer has the engine's own ORDER BY do that), so a
//! partition is a run of rows equal on the first and a peer group a run equal on the second. One pass finds the runs,
//! each call is computed over them, and the file's columns plus one column per call are written to a new file.
//!
//! The whole file is held in memory (decoded, then the answer beside it): the same bound as an ORDER BY with no LIMIT,
//! refused as `ExceededMemoryBudget` past `--max-memory`. Columns are carried in the lane they were decoded in, so a
//! value passes through to the output exactly as it was read; only the new columns are computed.
const std = @import("std");
const schema = @import("../core/schema.zig");
const metadata = @import("../core/parquet/metadata.zig");
const column_mod = @import("../core/parquet/column.zig");
const decimal_mod = @import("../core/parquet/decimal.zig");
const consumer = @import("../core/consumer.zig");
const consumer_column = @import("../core/consumer/column.zig");
const filter_eval = @import("../core/filter/eval.zig");
const expr_agg = @import("../core/expr/agg.zig");
const expr_ast = @import("../core/expr/ast.zig");
const streaming = @import("../core/writer/streaming.zig");
const local_fs = @import("../local_fs.zig");
const write = @import("write.zig");

const Column = filter_eval.Batch.Column;
const OutputCol = expr_agg.OutputCol;

const plan_types = @import("../core/expr/window_plan.zig");
pub const Func = plan_types.Func;
pub const Bound = plan_types.Bound;
pub const Frame = plan_types.Frame;
pub const Default = plan_types.Default;

/// A call with its argument resolved to a column of the table.
pub const Call = struct {
    func: Func,
    arg: ?usize = null,
    offset: u32 = 1,
    default: Default = .null,
    frame: Frame = .{},
};

pub const Spec = struct {
    partition: []const usize,
    order: []const usize,
    calls: []const Call,
};

pub const Error = error{
    /// A column of a type the pass does not carry (nested, INT96, fixed-length bytes that are no DECIMAL).
    WindowColumnType,
    /// More rows than the pass indexes.
    WindowTooManyRows,
    /// `lag` / `lead` default of a type the column's lane does not take.
    WindowDefaultType,
    /// A SUM past what its result type holds.
    WindowSumOverflow,
    /// A window function over a type it has no meaning for (SUM of text).
    WindowArgumentType,
    /// SUM or AVG of a DOUBLE or FLOAT over a window.
    WindowFloatAggregate,
    ExceededMemoryBudget,
};

/// The columns of a file in memory, each in the lane it decoded to; a DECIMAL is its unscaled integers.
pub const Table = struct {
    rows: usize,
    cols: []const Column,
    elems: []const schema.SchemaElement,
    /// Column `i`'s DECIMAL type, when it has one.
    decs: []const ?expr_ast.Dec,
};

// ---- cells

fn nullAt(c: Column, i: usize) bool {
    return switch (c) {
        inline else => |x| if (x.def_levels) |dl| dl[i] < x.max_def else false,
    };
}

fn floatSame(a: anytype, b: @TypeOf(a)) bool {
    return a == b or (std.math.isNan(a) and std.math.isNan(b));
}

/// Whether rows `i` and `j` hold the same value: NULL equals NULL and NaN equals NaN, as PARTITION BY and ORDER BY
/// peers do.
fn sameAt(c: Column, i: usize, j: usize) bool {
    const ni = nullAt(c, i);
    const nj = nullAt(c, j);
    if (ni or nj) return ni and nj;
    return switch (c) {
        .f32 => |x| floatSame(x.values[i], x.values[j]),
        .f64 => |x| floatSame(x.values[i], x.values[j]),
        .string => |x| std.mem.eql(u8, x.values[i], x.values[j]),
        inline else => |x| x.values[i] == x.values[j],
    };
}

fn floatOrder(a: anytype, b: @TypeOf(a)) std.math.Order {
    const na = std.math.isNan(a);
    const nb = std.math.isNan(b);
    if (na or nb) return if (na and nb) .eq else if (na) .gt else .lt;
    return std.math.order(a, b);
}

/// Rows `i` and `j`, neither NULL, in value order (NaN above every number, as DuckDB orders it).
fn orderAt(c: Column, i: usize, j: usize) std.math.Order {
    return switch (c) {
        .f32 => |x| floatOrder(x.values[i], x.values[j]),
        .f64 => |x| floatOrder(x.values[i], x.values[j]),
        .string => |x| std.mem.order(u8, x.values[i], x.values[j]),
        .boolean => |x| std.math.order(@intFromBool(x.values[i]), @intFromBool(x.values[j])),
        inline else => |x| std.math.order(x.values[i], x.values[j]),
    };
}

// ---- partitions and peers

const Runs = struct {
    /// Per row: the first row of its partition, the row after its partition's last, the first row of its peer group
    /// and the row after the group's last; and the number of peer groups the partition has had up to the row.
    pstart: []u32,
    pend: []u32,
    gstart: []u32,
    gend: []u32,
    dense: []u32,
};

fn runs(a: std.mem.Allocator, t: Table, spec: Spec) !Runs {
    const n = t.rows;
    const r: Runs = .{
        .pstart = try a.alloc(u32, n),
        .pend = try a.alloc(u32, n),
        .gstart = try a.alloc(u32, n),
        .gend = try a.alloc(u32, n),
        .dense = try a.alloc(u32, n),
    };
    for (0..n) |i| {
        var new_part = i == 0;
        var new_peer = i == 0;
        if (i > 0) {
            for (spec.partition) |c| if (!sameAt(t.cols[c], i - 1, i)) {
                new_part = true;
            };
            new_peer = new_part;
            if (!new_peer) for (spec.order) |c| if (!sameAt(t.cols[c], i - 1, i)) {
                new_peer = true;
            };
        }
        const me: u32 = @intCast(i);
        r.pstart[i] = if (new_part) me else r.pstart[i - 1];
        r.gstart[i] = if (new_peer) me else r.gstart[i - 1];
        r.dense[i] = if (new_part) 1 else r.dense[i - 1] + @intFromBool(new_peer);
    }
    var i = n;
    while (i > 0) {
        i -= 1;
        const next: u32 = @intCast(i + 1);
        r.pend[i] = if (i + 1 == n or r.pstart[i + 1] == next) next else r.pend[i + 1];
        r.gend[i] = if (i + 1 == n or r.gstart[i + 1] == next) next else r.gend[i + 1];
    }
    return r;
}

// ---- frames

const Span = struct { lo: usize, hi: usize };

/// The rows `[lo, hi)` the call's frame covers for row `i`; `lo >= hi` is an empty frame.
fn frameOf(f: Frame, ordered: bool, r: Runs, i: usize) Span {
    const ps: i64 = r.pstart[i];
    const pe: i64 = r.pend[i];
    const row: i64 = @intCast(i);
    var lo: i64 = undefined;
    var hi: i64 = undefined;
    if (!f.rows) {
        lo = ps;
        hi = if (f.end == .unbounded_following or !ordered) pe else r.gend[i];
    } else {
        lo = switch (f.start) {
            .unbounded_preceding => ps,
            .preceding => |k| row - k,
            .current_row => row,
            .following => |k| row + k,
            .unbounded_following => pe,
        };
        hi = switch (f.end) {
            .unbounded_preceding => ps,
            .preceding => |k| row - k + 1,
            .current_row => row + 1,
            .following => |k| row + k + 1,
            .unbounded_following => pe,
        };
    }
    // Both ends inside the partition, a frame that ends before it starts empty.
    lo = std.math.clamp(lo, ps, pe);
    hi = std.math.clamp(hi, lo, pe);
    return .{ .lo = @intCast(lo), .hi = @intCast(hi) };
}

// ---- results

/// A computed column and the type to write it as.
const Out = struct {
    col: Column,
    physical: schema.Type,
    /// The column whose annotations the result keeps (a value passed through: lag, lead, min, max).
    from: ?usize = null,
    dec: ?expr_ast.Dec = null,
    nullable: bool,
};

fn i64Out(values: []const i64, def: ?[]const u32, nullable: bool) Out {
    return .{
        .col = .{ .i64 = .{ .values = values, .def_levels = def, .max_def = if (def != null) 1 else 0, .has_nulls = def != null } },
        .physical = .INT64,
        .nullable = nullable,
    };
}

/// The rows `pick` names, in `c`'s lane: a negative pick is NULL, or `default` where one is given.
fn gather(a: std.mem.Allocator, c: Column, pick: []const i64, default: Default) !Column {
    const n = pick.len;
    const def = try a.alloc(u32, n);
    var any_null = false;
    switch (c) {
        inline else => |x, tag| {
            const T = @typeInfo(@TypeOf(x.values)).pointer.child;
            const out = try a.alloc(T, n);
            for (pick, out, def) |p, *o, *d| {
                if (p >= 0) {
                    const j: usize = @intCast(p);
                    o.* = x.values[j];
                    d.* = if (nullAt(c, j)) 0 else 1;
                    if (d.* == 0) any_null = true;
                    continue;
                }
                o.* = if (T == []const u8) "" else std.mem.zeroes(T);
                switch (default) {
                    .null => {
                        d.* = 0;
                        any_null = true;
                    },
                    .int => |v| {
                        d.* = 1;
                        o.* = switch (tag) {
                            .i32 => std.math.cast(i32, v) orelse return error.WindowDefaultType,
                            .i64 => v,
                            .f32 => @floatFromInt(v),
                            .f64 => @floatFromInt(v),
                            else => return error.WindowDefaultType,
                        };
                    },
                    .float => |v| {
                        d.* = 1;
                        o.* = switch (tag) {
                            .f32 => @floatCast(v),
                            .f64 => v,
                            else => return error.WindowDefaultType,
                        };
                    },
                    .string => |v| {
                        d.* = 1;
                        o.* = switch (tag) {
                            .string => v,
                            else => return error.WindowDefaultType,
                        };
                    },
                }
            }
            return @unionInit(Column, @tagName(tag), .{ .values = out, .def_levels = def, .max_def = 1, .has_nulls = any_null });
        },
    }
}

fn argOf(call: Call) !usize {
    return call.arg orelse error.WindowArgumentType;
}

/// Whether the frame the call asks for grows from the partition's start, so a running total is its answer.
fn fromStart(f: Frame) bool {
    return !f.rows or f.start == .unbounded_preceding;
}

fn compute(a: std.mem.Allocator, t: Table, r: Runs, spec: Spec, call: Call) !Out {
    const n = t.rows;
    const ordered = spec.order.len > 0;
    switch (call.func) {
        .row_number, .rank, .dense_rank => {
            const out = try a.alloc(i64, n);
            for (out, 0..) |*o, i| o.* = switch (call.func) {
                .row_number => @as(i64, @intCast(i - r.pstart[i])) + 1,
                .rank => @as(i64, @intCast(r.gstart[i] - r.pstart[i])) + 1,
                else => r.dense[i],
            };
            return i64Out(out, null, false);
        },
        .lag, .lead => {
            const ci = try argOf(call);
            const pick = try a.alloc(i64, n);
            for (pick, 0..) |*p, i| {
                const row: i64 = @intCast(i);
                const j = if (call.func == .lag) row - call.offset else row + call.offset;
                p.* = if (j >= r.pstart[i] and j < r.pend[i]) j else -1;
            }
            return .{ .col = try gather(a, t.cols[ci], pick, call.default), .physical = elemType(t, ci), .from = ci, .dec = t.decs[ci], .nullable = true };
        },
        .min, .max => {
            const ci = try argOf(call);
            const c = t.cols[ci];
            const pick = try a.alloc(i64, n);
            const queue = try a.alloc(u32, n);
            const want: std.math.Order = if (call.func == .min) .lt else .gt;
            var i: usize = 0;
            while (i < n) {
                const ps = r.pstart[i];
                const pe = r.pend[i];
                var head: usize = 0;
                var tail: usize = 0;
                var added: usize = ps;
                for (ps..pe) |row| {
                    const span = frameOf(call.frame, ordered, r, row);
                    while (added < span.hi) : (added += 1) {
                        if (nullAt(c, added)) continue;
                        // A later value at least as good as the ones queued behind it replaces them.
                        while (tail > head) {
                            const back = queue[tail - 1];
                            const o = orderAt(c, added, back);
                            if (o == want or o == .eq) tail -= 1 else break;
                        }
                        queue[tail] = @intCast(added);
                        tail += 1;
                    }
                    while (head < tail and queue[head] < span.lo) head += 1;
                    pick[row] = if (head < tail) @as(i64, queue[head]) else -1;
                }
                i = pe;
            }
            return .{ .col = try gather(a, c, pick, .null), .physical = elemType(t, ci), .from = ci, .dec = t.decs[ci], .nullable = true };
        },
        .count_star, .count => {
            const out = try a.alloc(i64, n);
            if (call.func == .count_star) {
                for (out, 0..) |*o, i| {
                    const s = frameOf(call.frame, ordered, r, i);
                    o.* = @intCast(s.hi - s.lo);
                }
                return i64Out(out, null, false);
            }
            const c = t.cols[try argOf(call)];
            const pre = try a.alloc(u32, n + 1);
            pre[0] = 0;
            for (0..n) |i| pre[i + 1] = pre[i] + @intFromBool(!nullAt(c, i));
            for (out, 0..) |*o, i| {
                const s = frameOf(call.frame, ordered, r, i);
                o.* = pre[s.hi] - pre[s.lo];
            }
            return i64Out(out, null, false);
        },
        .sum, .avg => return sumOrAvg(a, t, r, spec, call),
    }
}

fn elemType(t: Table, ci: usize) schema.Type {
    if (t.decs[ci]) |d| return d.physical();
    return t.elems[ci].type.?;
}

/// SUM and AVG over the frame. Integers and DECIMALs sum exactly (a prefix of i128); a DOUBLE sums left to right from
/// the frame's first row, a running total when the frame starts at the partition's.
/// The bits of the integer type DuckDB reads column `ci` as, or an error for a column it does not sum as an integer or DECIMAL.
fn integerWidth(t: Table, ci: usize) !u8 {
    if (t.decs[ci] != null) return 128;
    const e = t.elems[ci];
    if (e.logical_type) |lt| switch (lt) {
        .INTEGER => |i| return if (i.isSigned) @intCast(i.bitWidth) else error.WindowArgumentType,
        else => return error.WindowArgumentType,
    };
    if (e.converted_type) |ct| switch (ct) {
        .INT_8 => return 8,
        .INT_16 => return 16,
        .INT_32 => return 32,
        .INT_64 => return 64,
        else => return error.WindowArgumentType,
    };
    return switch (e.type orelse return error.WindowArgumentType) {
        .INT32 => 32,
        .INT64 => 64,
        else => error.WindowArgumentType,
    };
}

/// AVG of `k` values that sum to `total`, as DuckDB finalizes it: up to 16 bits (a DECIMAL of up to 4 digits) the sum over the
/// count and the scale in doubles, wider in long doubles and rounded once to a double.
fn averageOf(total: i128, k: u32, width: u8, dec: ?expr_ast.Dec) f64 {
    const scale: f64 = if (dec) |d| @floatFromInt(expr_ast.pow10(d.scale)) else 1;
    const small = if (dec) |d| d.precision <= 4 else width <= 16;
    if (small) return @as(f64, @floatFromInt(total)) / (@as(f64, @floatFromInt(k)) * scale);
    const divisor: f80 = @as(f80, @floatFromInt(k)) * @as(f80, scale);
    return @floatCast(@as(f80, @floatFromInt(total)) / divisor);
}

fn sumOrAvg(a: std.mem.Allocator, t: Table, r: Runs, spec: Spec, call: Call) !Out {
    const n = t.rows;
    const ordered = spec.order.len > 0;
    const ci = try argOf(call);
    const c = t.cols[ci];
    const is_avg = call.func == .avg;
    const def = try a.alloc(u32, n);
    var any_null = false;
    const cnt = try a.alloc(u32, n + 1);
    cnt[0] = 0;
    for (0..n) |i| cnt[i + 1] = cnt[i] + @intFromBool(!nullAt(c, i));

    switch (c) {
        .i32, .i64, .i128 => {
            // Only the types DuckDB sums as integers and DECIMALs: a DATE, a time or an unsigned integer is another function's.
            const width = try integerWidth(t, ci);
            const pre = try a.alloc(i128, n + 1);
            pre[0] = 0;
            for (0..n) |i| {
                const v: i128 = if (nullAt(c, i)) 0 else switch (c) {
                    .i32 => |x| x.values[i],
                    .i64 => |x| x.values[i],
                    .i128 => |x| x.values[i],
                    else => unreachable,
                };
                pre[i + 1] = std.math.add(i128, pre[i], v) catch return error.WindowSumOverflow;
            }
            if (is_avg) {
                const out = try a.alloc(f64, n);
                for (out, def, 0..) |*o, *d, i| {
                    const s = frameOf(call.frame, ordered, r, i);
                    const k = cnt[s.hi] - cnt[s.lo];
                    d.* = @intFromBool(k > 0);
                    if (k == 0) {
                        o.* = 0;
                        any_null = true;
                        continue;
                    }
                    o.* = averageOf(pre[s.hi] - pre[s.lo], k, width, t.decs[ci]);
                }
                return .{ .col = .{ .f64 = .{ .values = out, .def_levels = def, .max_def = 1, .has_nulls = any_null } }, .physical = .DOUBLE, .nullable = true };
            }
            if (t.decs[ci]) |dd| {
                const out = try a.alloc(i128, n);
                const limit = expr_ast.pow10(38);
                for (out, def, 0..) |*o, *d, i| {
                    const s = frameOf(call.frame, ordered, r, i);
                    d.* = @intFromBool(cnt[s.hi] > cnt[s.lo]);
                    o.* = pre[s.hi] - pre[s.lo];
                    if (d.* == 0) any_null = true;
                    if (o.* >= limit or o.* <= -limit) return error.WindowSumOverflow;
                }
                const wide: expr_ast.Dec = .{ .precision = 38, .scale = dd.scale };
                return .{ .col = .{ .i128 = .{ .values = out, .def_levels = def, .max_def = 1, .has_nulls = any_null } }, .physical = wide.physical(), .dec = wide, .nullable = true };
            }
            if (c == .i128) return error.WindowArgumentType;
            const out = try a.alloc(i64, n);
            for (out, def, 0..) |*o, *d, i| {
                const s = frameOf(call.frame, ordered, r, i);
                d.* = @intFromBool(cnt[s.hi] > cnt[s.lo]);
                if (d.* == 0) any_null = true;
                o.* = std.math.cast(i64, pre[s.hi] - pre[s.lo]) orelse return error.WindowSumOverflow;
            }
            return .{ .col = .{ .i64 = .{ .values = out, .def_levels = def, .max_def = 1, .has_nulls = any_null } }, .physical = .INT64, .nullable = true };
        },
        // DuckDB adds a frame of DOUBLEs in the order of a segment tree (blocks of 16 rows, then their blocks), and AVG with
        // compensated sums; neither is a left-to-right sum, and the last bits differ from one in a partition of 32 rows.
        .f32, .f64 => return error.WindowFloatAggregate,
        else => return error.WindowArgumentType,
    }
}

/// The window columns for `spec`'s calls over `t`, in call order.
pub fn columns(a: std.mem.Allocator, t: Table, spec: Spec) ![]const Out {
    if (t.rows > std.math.maxInt(u32) - 1) return error.WindowTooManyRows;
    const r = try runs(a, t, spec);
    const out = try a.alloc(Out, spec.calls.len);
    for (spec.calls, out) |call, *o| o.* = try compute(a, t, r, spec, call);
    return out;
}

// ---- files

const Loaded = struct { table: Table, names: []const []const u8 };

fn chunkOf(bytes: []const u8, cm: schema.ColumnMetaData) ![]const u8 {
    const start: usize = @intCast(cm.dictionary_page_offset orelse cm.data_page_offset);
    const len: usize = @intCast(cm.total_compressed_size);
    if (start > bytes.len or len > bytes.len - start) return error.MissingChunkBytes;
    return bytes[start..][0..len];
}

/// Leaf `ci` of every row group, decoded in its lane and joined into one column.
fn loadColumn(a: std.mem.Allocator, bytes: []const u8, meta: *const schema.FileMetaData, ci: usize, rows: usize) !Column {
    const elem = meta.schema.items[ci + 1];
    const kind = decimal_mod.kindFromSchema(&elem);
    const phys = elem.type orelse return error.WindowColumnType;
    if (kind == null) switch (phys) {
        .INT32, .INT64, .FLOAT, .DOUBLE, .BOOLEAN, .BYTE_ARRAY => {},
        else => return error.WindowColumnType,
    };
    const which: LaneTag = if (kind != null) .i128 else lane(phys);
    switch (which) {
        inline else => |tag| {
            const T = LaneType(tag);
            const values = try a.alloc(T, rows);
            var defs: ?[]u32 = null;
            var max_def: u32 = 0;
            var at: usize = 0;
            var any_null = false;
            for (meta.row_groups.items) |*rg| {
                const cm = rg.columns.items[ci].meta_data orelse return error.ColumnMetaMissing;
                const chunk = try chunkOf(bytes, cm);
                const levels = meta.getColumnLevels(cm.path_in_schema.items);
                const n: usize = @intCast(rg.num_rows);
                const options = column_mod.chunkOptions(.{}, meta, cm);
                const got = if (comptime tag == .i128)
                    try decimal_mod.decodeColumnAsI128WithOptions(a, chunk, cm.codec, levels, n, kind.?, options)
                else
                    try consumer_column.decodeColumnTWithOptions(T, a, chunk, cm.codec, levels, n, options);
                if (got.values.len != n) return error.WindowColumnType;
                @memcpy(values[at..][0..n], got.values);
                const md: u32 = @intCast(levels.max_def);
                max_def = md;
                if (md > 0) {
                    if (defs == null) {
                        defs = try a.alloc(u32, rows);
                        @memset(defs.?, md);
                    }
                    if (got.def_levels) |dl| {
                        @memcpy(defs.?[at..][0..n], dl);
                        for (dl) |d| if (d < md) {
                            any_null = true;
                        };
                    }
                }
                at += n;
            }
            return @unionInit(Column, @tagName(tag), .{ .values = values, .def_levels = defs, .max_def = max_def, .has_nulls = any_null });
        },
    }
}

const LaneTag = enum { i32, i64, f32, f64, string, boolean, i128 };

fn lane(phys: schema.Type) LaneTag {
    return switch (phys) {
        .INT32 => .i32,
        .INT64 => .i64,
        .FLOAT => .f32,
        .DOUBLE => .f64,
        .BOOLEAN => .boolean,
        else => .string,
    };
}

fn LaneType(comptime tag: LaneTag) type {
    return switch (tag) {
        .i32 => i32,
        .i64 => i64,
        .f32 => f32,
        .f64 => f64,
        .string => []const u8,
        .boolean => bool,
        .i128 => i128,
    };
}

fn loadTable(a: std.mem.Allocator, bytes: []const u8, meta: *const schema.FileMetaData) !Loaded {
    const ncols = meta.schema.items.len - 1;
    var rows: usize = 0;
    for (meta.row_groups.items) |*rg| rows += @intCast(rg.num_rows);
    const cols = try a.alloc(Column, ncols);
    const elems = try a.alloc(schema.SchemaElement, ncols);
    const decs = try a.alloc(?expr_ast.Dec, ncols);
    const names = try a.alloc([]const u8, ncols);
    for (0..ncols) |ci| {
        const elem = meta.schema.items[ci + 1];
        if ((elem.num_children orelse 0) > 0) return error.WindowColumnType;
        if (elem.repetition_type == .REPEATED) return error.WindowColumnType;
        elems[ci] = elem;
        names[ci] = elem.name;
        decs[ci] = null;
        if (decimal_mod.kindFromSchema(&elem)) |k| {
            if (k.precision > 38 or k.precision < 1 or k.scale < 0) return error.WindowColumnType;
            decs[ci] = .{ .precision = @intCast(k.precision), .scale = @intCast(k.scale) };
        }
        cols[ci] = try loadColumn(a, bytes, meta, ci, rows);
    }
    return .{ .table = .{ .rows = rows, .cols = cols, .elems = elems, .decs = decs }, .names = names };
}

/// An output column for a decoded one, typed as the file typed it.
fn passThrough(name: []const u8, c: Column, elem: schema.SchemaElement, dec: ?expr_ast.Dec) OutputCol {
    return .{
        .name = name,
        .col = c,
        .parquet_type = if (dec) |d| d.physical() else elem.type.?,
        .type_length = if (dec != null) null else elem.type_length,
        .nullable = (elem.repetition_type orelse .REQUIRED) != .REQUIRED,
        .logical_type = if (dec != null) null else elem.logical_type,
        .logical_type_raw = if (dec != null) &.{} else elem.logical_type_raw,
        .converted_type = if (dec != null) null else elem.converted_type,
        .dec = dec,
    };
}

pub const Request = struct {
    input: []const u8,
    output: []const u8,
    pass: plan_types.Pass,
    max_memory: usize = std.math.maxInt(usize),
};

fn indexOf(names: []const []const u8, name: []const u8) !usize {
    for (names, 0..) |n, i| if (std.mem.eql(u8, n, name)) return i;
    return error.WindowColumnType;
}

/// Read `req.input` (already in window order), compute the calls and write the file's columns and theirs to
/// `req.output`.
pub fn run(gpa: std.mem.Allocator, req: Request) !void {
    var arena_state = std.heap.ArenaAllocator.init(gpa);
    defer arena_state.deinit();
    const a = arena_state.allocator();

    const mapped = try local_fs.mapFile(req.input);
    defer mapped.unmap();
    const meta = try metadata.open(a, mapped.bytes);
    // What the file decodes to, twice (the columns and the answer's), with the runs; refused up front.
    var held: u64 = 0;
    var rows_total: u64 = 0;
    for (meta.row_groups.items) |*rg| {
        rows_total += @intCast(rg.num_rows);
        for (rg.columns.items) |col| held += @intCast((col.meta_data orelse continue).total_uncompressed_size);
    }
    held = held * 2 + rows_total * (24 + 16 * req.pass.calls.len);
    if (held > req.max_memory) return error.ExceededMemoryBudget;

    const loaded = try loadTable(a, mapped.bytes, &meta);
    const t = loaded.table;
    const part = try a.alloc(usize, req.pass.partition.len);
    for (req.pass.partition, part) |n, *p| p.* = try indexOf(loaded.names, n);
    const order = try a.alloc(usize, req.pass.order.len);
    for (req.pass.order, order) |n, *p| p.* = try indexOf(loaded.names, n);
    const calls = try a.alloc(Call, req.pass.calls.len);
    for (req.pass.calls, calls) |nc, *c| c.* = .{
        .func = nc.func,
        .arg = if (nc.arg) |n| try indexOf(loaded.names, n) else null,
        .offset = nc.offset,
        .default = nc.default,
        .frame = nc.frame,
    };
    const outs = try columns(a, t, .{ .partition = part, .order = order, .calls = calls });

    const all = try a.alloc(OutputCol, t.cols.len + outs.len);
    for (t.cols, 0..) |c, i| all[i] = passThrough(loaded.names[i], c, t.elems[i], t.decs[i]);
    for (outs, req.pass.results, 0..) |o, name, i| {
        var col: OutputCol = if (o.from) |from| passThrough(name, o.col, t.elems[from], t.decs[from]) else .{
            .name = name,
            .col = o.col,
            .parquet_type = o.physical,
            .nullable = o.nullable,
            .dec = o.dec,
        };
        col.name = name;
        col.nullable = o.nullable;
        all[t.cols.len + i] = col;
    }

    const fd = try write.createFile(req.output);
    var open = true;
    defer if (open) local_fs.close(fd);
    errdefer local_fs.removeFile(req.output);
    var fd_sink: write.FdSink = .{ .fd = fd };
    const sink: streaming.Sink = fd_sink.sink();
    _ = try consumer.writeAggregateRows(a, sink, all, @intCast(t.rows), .ZSTD, .{}, .{});
    open = false;
    try local_fs.closeChecked(fd);
}

test "window: runs find partitions and peers" {
    const T = std.testing;
    var arena_state = std.heap.ArenaAllocator.init(T.allocator);
    defer arena_state.deinit();
    const a = arena_state.allocator();
    const p = [_]i64{ 1, 1, 1, 2, 2, 3 };
    const o = [_]i64{ 5, 5, 7, 1, 2, 9 };
    const cols = [_]Column{
        .{ .i64 = .{ .values = &p } },
        .{ .i64 = .{ .values = &o } },
    };
    const t: Table = .{ .rows = 6, .cols = &cols, .elems = &.{}, .decs = &.{ null, null } };
    const r = try runs(a, t, .{ .partition = &.{0}, .order = &.{1}, .calls = &.{} });
    try T.expectEqualSlices(u32, &.{ 0, 0, 0, 3, 3, 5 }, r.pstart);
    try T.expectEqualSlices(u32, &.{ 3, 3, 3, 5, 5, 6 }, r.pend);
    try T.expectEqualSlices(u32, &.{ 0, 0, 2, 3, 4, 5 }, r.gstart);
    try T.expectEqualSlices(u32, &.{ 2, 2, 3, 4, 5, 6 }, r.gend);
    try T.expectEqualSlices(u32, &.{ 1, 1, 2, 1, 2, 1 }, r.dense);
}

test "window: ranking, offsets and frames over partitions of a sorted table" {
    const T = std.testing;
    var arena_state = std.heap.ArenaAllocator.init(T.allocator);
    defer arena_state.deinit();
    const a = arena_state.allocator();
    // Partitions 1 (rows 0..3) and 2 (rows 3..6); the order key ties in rows 0 and 1.
    const p = [_]i64{ 1, 1, 1, 2, 2, 2 };
    const o = [_]i64{ 5, 5, 7, 1, 2, 2 };
    const v = [_]i64{ 10, 20, 30, 40, 50, 60 };
    const cols = [_]Column{
        .{ .i64 = .{ .values = &p } },
        .{ .i64 = .{ .values = &o } },
        .{ .i64 = .{ .values = &v } },
    };
    const elems = [_]schema.SchemaElement{ intElem(), intElem(), intElem() };
    const t: Table = .{ .rows = 6, .cols = &cols, .elems = &elems, .decs = &.{ null, null, null } };
    const calls = [_]Call{
        .{ .func = .row_number },
        .{ .func = .rank },
        .{ .func = .dense_rank },
        .{ .func = .lag, .arg = 2, .default = .{ .int = -1 } },
        .{ .func = .sum, .arg = 2 },
        .{ .func = .sum, .arg = 2, .frame = .{ .rows = true, .start = .{ .preceding = 1 }, .end = .current_row } },
        .{ .func = .max, .arg = 2, .frame = .{ .rows = true, .start = .current_row, .end = .unbounded_following } },
        .{ .func = .count_star, .frame = .{ .rows = true, .start = .{ .following = 5 }, .end = .{ .following = 9 } } },
    };
    const out = try columns(a, t, .{ .partition = &.{0}, .order = &.{1}, .calls = &calls });
    try T.expectEqualSlices(i64, &.{ 1, 2, 3, 1, 2, 3 }, out[0].col.i64.values);
    try T.expectEqualSlices(i64, &.{ 1, 1, 3, 1, 2, 2 }, out[1].col.i64.values);
    try T.expectEqualSlices(i64, &.{ 1, 1, 2, 1, 2, 2 }, out[2].col.i64.values);
    try T.expectEqualSlices(i64, &.{ -1, 10, 20, -1, 40, 50 }, out[3].col.i64.values);
    // The default frame ends at the last peer, so rows 0 and 1 share their sum, and 4 and 5.
    try T.expectEqualSlices(i64, &.{ 30, 30, 60, 40, 150, 150 }, out[4].col.i64.values);
    try T.expectEqualSlices(i64, &.{ 10, 30, 50, 40, 90, 110 }, out[5].col.i64.values);
    try T.expectEqualSlices(i64, &.{ 30, 30, 30, 60, 60, 60 }, out[6].col.i64.values);
    // A frame wholly past the partition's end is empty.
    try T.expectEqualSlices(i64, &.{ 0, 0, 0, 0, 0, 0 }, out[7].col.i64.values);
}

fn intElem() schema.SchemaElement {
    return .{ .type = .INT64, .type_length = null, .repetition_type = .REQUIRED, .name = "c", .num_children = null, .scale = null, .precision = null, .field_id = null };
}

test "window: frames are clamped into the partition" {
    const T = std.testing;
    var pstart = [_]u32{ 2, 2, 2 };
    var pend = [_]u32{ 5, 5, 5 };
    var gstart = [_]u32{ 2, 3, 4 };
    var gend = [_]u32{ 3, 4, 5 };
    var dense = [_]u32{ 1, 2, 3 };
    const r: Runs = .{ .pstart = &pstart, .pend = &pend, .gstart = &gstart, .gend = &gend, .dense = &dense };
    const far = frameOf(.{ .rows = true, .start = .{ .following = 40 }, .end = .{ .following = 50 } }, true, r, 1);
    try T.expectEqual(@as(usize, 5), far.lo);
    try T.expectEqual(@as(usize, 5), far.hi);
    const behind = frameOf(.{ .rows = true, .start = .{ .preceding = 50 }, .end = .{ .preceding = 40 } }, true, r, 1);
    try T.expectEqual(@as(usize, 2), behind.lo);
    try T.expectEqual(@as(usize, 2), behind.hi);
    const default = frameOf(.{}, true, r, 1);
    try T.expectEqual(@as(usize, 2), default.lo);
    try T.expectEqual(@as(usize, 4), default.hi);
    const whole = frameOf(.{}, false, r, 1);
    try T.expectEqual(@as(usize, 5), whole.hi);
}

test "window: AVG finalizes as DuckDB does, short types in doubles and the rest in long doubles" {
    const T = std.testing;
    // 4-digit DECIMALs and 16-bit integers divide in doubles by count * scale; wider ones in long doubles.
    try T.expectEqual(@as(f64, 2.5), averageOf(25, 1, 8, .{ .precision = 4, .scale = 1 }));
    try T.expectEqual(@as(f64, 1.5), averageOf(3, 2, 16, null));
    try T.expectEqual(@as(f64, 12.3456), averageOf(123456, 1, 128, .{ .precision = 12, .scale = 4 }));
    try T.expectEqual(@as(f64, 3.5), averageOf(7, 2, 64, null));
}

