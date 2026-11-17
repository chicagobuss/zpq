//! A join as a lookup (0.6.0 prototype, `exp/joins-lookup`).
//!
//! The build side of a two-table N:1 equi-join is read once into a `Join`: its surviving keys (after its own filter)
//! map to dense slots, and every build column the rest of the query reads becomes a per-slot code into one global
//! dictionary of that column's values. The probe side is the ordinary single-table scan:
//!   - the join is one AND conjunct, a `.lookup` filter leaf on the foreign key (`plan.Conjunct.lookup` on the gated
//!     paths), so the FK decodes deferred like any later conjunct and the lookup narrows the rows that survive;
//!   - each build column is a virtual leaf appended to the probe's schema (`augment`), published after the filter as
//!     a coded column (`column`): codes into the build's dictionary, which filters, CASE, `agg_coded` and grouping
//!     consume as they consume a dictionary-encoded column.
//!
//! Keys decide their structure from facts verified on decode, never from statistics alone: a dense rank map (a bit
//! per key in the surviving domain, a rank per 64 bits) when it costs fewer bytes than a hash table, else an
//! open-addressing table. A key seen twice refuses the join (N:1 only). A NULL foreign key matches nothing, as in SQL.
//! Slots are a function of the keys alone (rank order, or (row group, row) order for the hash table), so nothing
//! downstream depends on `-j`. A build column's NULL is a bit of its slot (`Payload.nulls`), and reads as a LEFT
//! join's miss does.
const std = @import("std");
const schema = @import("../schema.zig");
const filter_ast = @import("../filter/ast.zig");
const filter_eval = @import("../filter/eval.zig");
const selection = @import("../filter/selection.zig");
const column_mod = @import("../parquet/column.zig");
const metadata = @import("../parquet/metadata.zig");
const decimal = @import("../parquet/decimal.zig");
const Column = filter_eval.Batch.Column;
const Selection = selection.SelectionVector;

/// INNER and LEFT builds are N:1 (a key twice refuses); SEMI and ANTI take duplicates. INNER and SEMI keep a probe
/// row with a match, ANTI one without (`NOT IN`), and LEFT every row, a miss reading its build columns as NULL.
pub const Kind = enum {
    inner,
    semi,
    anti,
    left,

    pub fn unique(k: Kind) bool {
        return k == .inner or k == .left;
    }
};

/// A build column's distinct values, in the order its codes number them (first appearance in (row group, row)).
pub const Dict = union(enum) {
    i32: []const i32,
    i64: []const i64,
    /// A DECIMAL stored as bytes. Every DECIMAL payload holds its exact unscaled integers (`column` scales them).
    i128: []const i128,
    f64: []const f64,
    string: []const []const u8,

    fn len(d: Dict) usize {
        return switch (d) {
            inline else => |v| v.len,
        };
    }
};

pub const Payload = struct {
    /// The virtual leaf's name in the probe's schema: the build column's own.
    name: []const u8,
    element: schema.SchemaElement,
    /// Per slot: the code of this build row's value in `dict`.
    codes: Codes,
    dict: Dict,
    /// `dict` holds a value per slot (`codes` index it), not distinct values: a column its writer did not
    /// dictionary-encode, laid out by slot without hashing (`build`). The scan publishes its values, never codes, so
    /// no consumer reads it as a dictionary.
    plain: bool = false,
    /// A snowflake's column, published through this build (`chain`): the payload of this build holding the FK into
    /// `inner`, and the column there. The FK's value determines it, as `inner`'s keys are unique.
    via: ?Via = null,

    /// A dependent string key: values wait at their source rows until the result cut.
    deferred: ?Deferred = null,
    /// A bit per slot, set where the build row holds NULL here; null for a column with none among the build's rows,
    /// which keeps a REQUIRED leaf (`extend`). A NULL slot's code is a placeholder nothing reads.
    nulls: ?[]u64 = null,

    pub fn isNull(p: Payload, s: u32) bool {
        return if (p.nulls) |bits| bitAt(bits, s) else false;
    }

    pub const Locator = struct { rg: u32, row: u32 };
    pub const Deferred = struct {
        scratch: std.mem.Allocator,
        bytes: []const u8,
        meta: *const schema.FileMetaData,
        leaf: usize,
        rows: []Locator,
    };

    pub const Via = struct { fk: usize, inner: *const Join, p: usize };
};

/// Per-slot codes, two bytes each when the dictionary has fewer than 2^16 values.
/// Each slot's code in a payload's dictionary, at the narrowest width the dictionary allows.
pub const Codes = union(enum) {
    u8: []u8,
    u16: []u16,
    u32: []u32,
    /// Each code is its index (`len` of them): a plain payload's in slot order, or a plain local's in row order.
    identity: usize,

    pub fn get(c: Codes, s: u32) u32 {
        return switch (c) {
            .identity => s,
            inline else => |v| v[s],
        };
    }

    fn set(c: Codes, s: u32, code: u32) void {
        switch (c) {
            inline .u8, .u16 => |v| v[s] = @intCast(code),
            .u32 => |v| v[s] = code,
            .identity => unreachable,
        }
    }

    fn free(c: Codes, a: std.mem.Allocator) void {
        switch (c) {
            .identity => {},
            inline else => |v| a.free(v),
        }
    }

    /// `n` codes on `a`, unset, at the narrowest width a dictionary of `entries` allows.
    fn init(a: std.mem.Allocator, n: usize, entries: usize) !Codes {
        return if (entries <= 1 << 8)
            .{ .u8 = try a.alloc(u8, n) }
        else if (entries <= 1 << 16)
            .{ .u16 = try a.alloc(u16, n) }
        else
            .{ .u32 = try a.alloc(u32, n) };
    }

    fn len(c: Codes) usize {
        return switch (c) {
            .identity => |n| n,
            inline else => |v| v.len,
        };
    }

    /// These codes, copied into `a` at the narrowest width a dictionary of `entries` allows.
    fn narrowed(c: Codes, a: std.mem.Allocator, entries: usize) !Codes {
        if (c == .identity) return c;
        const n = c.len();
        const out: Codes = try .init(a, n, entries);
        for (0..n) |i| out.set(@intCast(i), c.get(@intCast(i)));
        return out;
    }

    fn bytes(c: Codes) usize {
        return switch (c) {
            .identity => 0,
            inline else => |v| v.len * @sizeOf(@TypeOf(v[0])),
        };
    }
};

const EMPTY: u32 = std.math.maxInt(u32);

fn bitAt(bits: []const u64, i: usize) bool {
    return bits[i >> 6] & (@as(u64, 1) << @intCast(i & 63)) != 0;
}

/// Set bit `i` of the `n` in `bits`, allocated zeroed on `a` at its first set bit.
fn markBit(a: std.mem.Allocator, bits: *[]u64, n: usize, i: usize) !void {
    if (bits.len == 0) {
        bits.* = try a.alloc(u64, n / 64 + 1);
        @memset(bits.*, 0);
    }
    bits.*[i >> 6] |= @as(u64, 1) << @intCast(i & 63);
}

/// Surviving keys over their verified domain `[lo, lo + 64 * bits.len)`: a set bit is a key, its rank its slot.
const Dense = struct {
    lo: i64,
    bits: []u64,
    rank: []u32,

    fn slot(d: *const Dense, k: i64) ?u32 {
        if (k < d.lo) return null;
        const i: u64 = @bitCast(k -% d.lo);
        if (i >= @as(u64, d.bits.len) * 64) return null;
        const w = d.bits[@intCast(i >> 6)];
        const bit = @as(u64, 1) << @intCast(i & 63);
        if (w & bit == 0) return null;
        return d.rank[@intCast(i >> 6)] + @popCount(w & (bit - 1));
    }
};

const Hash = struct {
    keys: []i64,
    slots: []u32,

    fn home(h: *const Hash, k: i64) usize {
        // Capacity is at least 16, so the shift stays below 64.
        const shift: u6 = @intCast(64 - @as(u7, std.math.log2_int(usize, h.keys.len)));
        return @intCast((@as(u64, @bitCast(k)) *% 0x9E3779B97F4A7C15) >> shift);
    }

    fn slot(h: *const Hash, k: i64) ?u32 {
        const mask = h.keys.len - 1;
        var i = h.home(k);
        while (h.slots[i] != EMPTY) : (i = (i + 1) & mask) if (h.keys[i] == k) return h.slots[i];
        return null;
    }
};

/// A string key's distinct build values, numbered from 0 in (row group, row) order of first appearance. A string's
/// number is its key in the integer structures above, so a string key probes, chains and fills as an integer one.
const Strings = struct {
    values: []const []const u8,
    /// Open addressing by the bytes' hash: a number, or `EMPTY`.
    table: []u32,

    fn home(s: *const Strings, v: []const u8) usize {
        return @intCast(std.hash.Wyhash.hash(0, v) & (s.table.len - 1));
    }

    /// Give `values[n]`, which no smaller number holds, the number `n`.
    fn put(s: *const Strings, n: u32) void {
        var i = s.home(s.values[n]);
        while (s.table[i] != EMPTY) i = (i + 1) & (s.table.len - 1);
        s.table[i] = n;
    }

    fn number(s: *const Strings, v: []const u8) ?u32 {
        var i = s.home(v);
        while (s.table[i] != EMPTY) : (i = (i + 1) & (s.table.len - 1)) {
            if (std.mem.eql(u8, s.values[s.table[i]], v)) return s.table[i];
        }
        return null;
    }
};

/// One column of a key that is not its own integer: a string column, or any column of a composite key. The key the
/// integer structures hold is then each column's place in its range, weighted (a mixed radix): for one string column,
/// its number.
pub const KeyColumn = struct {
    domain: KeyDomain,
    /// A string column's values, numbered.
    strings: ?Strings = null,
    /// The column's range among the build's rows, verified as they were read: its smallest value (0 for a string
    /// column's numbers) and how many values it spans. `weight`: the product of the spans of the columns before it.
    lo: i64 = 0,
    span: u64 = 0,
    weight: u64 = 1,

    /// `v`'s weighted place in the range; null for a value outside it, which no build row holds.
    fn place(k: KeyColumn, v: anytype) ?u64 {
        const at: u64 = if (@TypeOf(v) == []const u8)
            (k.strings orelse return null).number(v) orelse return null
        else if (v < k.lo)
            return null
        else
            @bitCast(@as(i64, v) -% k.lo);
        return if (at < k.span) at * k.weight else null;
    }
};

/// The SQL domain a key's stored integers compare in. Two keys join only within one: the same integers mean the same
/// value there, where a timestamp in another unit, an unsigned value over a signed one or a DECIMAL of another scale
/// would match integers that are not equal values (or miss ones that are). A DECIMAL key is not in this release: the
/// build reads a DECIMAL as its f64 view. Strings compare by their bytes, as DuckDB's do, and bytes with no type
/// (a BLOB) only with their like.
pub const KeyDomain = union(enum) {
    /// Plain or signed integers, of any width (they widen to i64 exactly).
    signed,
    /// Unsigned integers, stored in this physical type: an INT32 and an INT64 differ above 2^31.
    unsigned: schema.Type,
    date,
    timestamp: struct { unit: std.meta.Tag(schema.TimeUnit), utc: bool },
    time: struct { unit: std.meta.Tag(schema.TimeUnit), utc: bool },
    string,
    bytes,

    /// Null for a key no lookup takes: not stored as INT32, INT64 or BYTE_ARRAY, or a logical type with no equality
    /// of integers or bytes (an ENUM, JSON or DECIMAL in a BYTE_ARRAY).
    pub fn of(e: schema.SchemaElement) ?KeyDomain {
        const t = e.type orelse return null;
        if (t == .BYTE_ARRAY) {
            const lt = e.effectiveLogicalType() orelse return .bytes;
            return if (lt == .STRING) .string else null;
        }
        if (t != .INT32 and t != .INT64) return null;
        const lt = e.effectiveLogicalType() orelse return .signed;
        return switch (lt) {
            .INTEGER => |i| if (i.isSigned) .signed else .{ .unsigned = t },
            .DATE => .date,
            .TIMESTAMP => |ts| .{ .timestamp = .{ .unit = std.meta.activeTag(ts.unit), .utc = ts.isAdjustedToUTC } },
            .TIME => |tm| .{ .time = .{ .unit = std.meta.activeTag(tm.unit), .utc = tm.isAdjustedToUTC } },
            else => null,
        };
    }

    pub fn eql(a: KeyDomain, b: KeyDomain) bool {
        return std.meta.eql(a, b);
    }

    fn text(d: KeyDomain) bool {
        return d == .string or d == .bytes;
    }
};

pub const Join = struct {
    kind: Kind,
    /// The build key's domain: the probe's FK must be in it (`KeyDomain`).
    domain: KeyDomain = .signed,
    /// For messages: the build file and its key column.
    build_name: []const u8,
    key_name: []const u8,
    n: usize,
    keys: union(enum) { dense: Dense, hash: Hash },
    /// The key's columns when it is not one integer column (`KeyColumn`): `keys` then holds what they pack to.
    cols: []const KeyColumn = &.{},
    payloads: []const Payload,
    /// Bytes the build holds for the query (charged against `--max-memory`).
    bytes: usize,
    /// The probe's real leaf count: virtual leaf `p` is `virtual_base + p` (`extend`).
    virtual_base: usize = 0,
    /// The probe's own join, whose build columns the scan publishes and whose group-join it folds; false for a SEMI
    /// lookup beside it or inside a build.
    main: bool = false,
    /// Whether the scan publishes its build columns: the main join's and a star's other dimensions'. `slot_index` is
    /// its place among them (the main join's 0), where the filter pass keeps its slots (`GatedFilter.slots`).
    publishes: bool = false,
    slot_index: usize = 0,
    /// The probe's foreign key leaf, and a composite key's further ones, set when the probe query binds.
    fk_leaf: usize = 0,
    fk_more: []const usize = &.{},
    /// Set when the query's GROUP BY is one the build key determines (`GroupJoin`).
    group: ?*GroupJoin = null,
    /// A build row the build's filter kept had a NULL key (`Part.null_key`): an ANTI join then keeps no row.
    null_key: bool = false,
    /// An ANTI join a NOT EXISTS wrote: a NULL matches nothing, so a NULL FK stays and a NULL build key drops no row.
    not_exists: bool = false,

    pub fn slot(j: *const Join, k: i64) ?u32 {
        return switch (j.keys) {
            inline else => |*s| s.slot(k),
        };
    }

    /// Whether a foreign key's value is the key itself: one integer column. Only then does a key read a build row
    /// back (a fetched GROUP BY key, the group-join's FK).
    pub fn ownKey(j: *const Join) bool {
        return j.cols.len == 0;
    }

    /// The slot of a one-column foreign key's value: an integer is its own key, a string its number.
    fn slotOf(j: *const Join, v: anytype) ?u32 {
        if (@TypeOf(v) != []const u8) return j.slot(v);
        if (j.cols.len != 1) return null;
        return j.slot(@intCast(j.cols[0].place(v) orelse return null));
    }

    /// The payload a probe leaf index stands for, or null for a real leaf.
    pub fn virtualPayload(j: *const Join, ci: usize) ?usize {
        if (ci < j.virtual_base or ci >= j.virtual_base + j.payloads.len) return null;
        return ci - j.virtual_base;
    }
};

/// Extend a probe footer with one top-level leaf per build column, after its own leaves: what the query binds the
/// build columns to. A leaf is OPTIONAL only where a row can read NULL (a LEFT join's miss, a build row's own NULL).
/// Row groups keep their own chunks only, so every decode path sees a virtual leaf as a
/// column it has no chunk for, and the scan publishes it from the lookup instead (`column`). Every probe file must
/// have the same leaf count, so the virtual leaves have one index.
pub fn extend(j: *Join, a: std.mem.Allocator, meta: *schema.FileMetaData) !void {
    var leaves: usize = 0;
    var it: schema.LeafIterator = .init(meta.schema.items);
    while (it.next()) |_| leaves += 1;
    if (j.virtual_base != 0 and j.virtual_base != leaves) return error.JoinProbeSchemasDiffer;
    j.virtual_base = leaves;
    // The caller's copy of the footer may share its schema list with another: extend a list of its own.
    meta.schema = try meta.schema.clone(a);
    for (j.payloads) |p| {
        var e = p.element;
        e.repetition_type = if (j.kind == .left or p.nulls != null) .OPTIONAL else .REQUIRED;
        e.name = p.name;
        try meta.schema.append(a, e);
    }
    const root = &meta.schema.items[0];
    root.num_children = (root.num_children orelse 0) + @as(i32, @intCast(j.payloads.len));
    try meta.indexLeaves(a);
}

/// The probe's own join in a bound filter: a top-level `.lookup` conjunct marked `main`.
pub fn ofFilter(filter: ?filter_ast.Filter) ?*const Join {
    const f = filter orelse return null;
    switch (f) {
        .lookup => |l| return if (l.join.main) l.join else null,
        .and_filter => |c| for (c.children) |child| if (child == .lookup and child.lookup.join.main) return child.lookup.join,
        else => {},
    }
    return null;
}

/// The filter's publishing lookups by `slot_index`: the main join and a star's other dimensions.
pub fn publishers(a: std.mem.Allocator, filter: ?filter_ast.Filter) ![]*const Join {
    const f = filter orelse return &.{};
    const kids: []const filter_ast.Filter = switch (f) {
        .lookup => (&f)[0..1],
        .and_filter => |c| c.children,
        else => return &.{},
    };
    var n: usize = 0;
    for (kids) |k| if (k == .lookup and k.lookup.join.publishes) {
        n = @max(n, k.lookup.join.slot_index + 1);
    };
    const out = try a.alloc(*const Join, n);
    for (kids) |k| if (k == .lookup and k.lookup.join.publishes) {
        out[k.lookup.join.slot_index] = k.lookup.join;
    };
    return out;
}

/// Whether a bound filter holds any lookup: a join query, which only the established row-group scan runs.
pub fn inFilter(filter: ?filter_ast.Filter) bool {
    const f = filter orelse return false;
    return switch (f) {
        .lookup => true,
        .and_filter => |c| for (c.children) |child| {
            if (child == .lookup) break true;
        } else false,
        else => false,
    };
}

// ---------------------------------------------------------------- probe

fn keyAt(comptime T: type, c: filter_eval.ColumnT(T), r: usize) ?i64 {
    if (c.def_levels) |dl| if (dl[r] < c.max_def) return null;
    return @intCast(c.values[r]);
}

/// Slots of `fk`'s selected rows into `out` (`EMPTY` where no build key matches), dictionary-direct when `fk` is coded:
/// each dictionary entry is looked up once and its rows take its slot. Rows `sel` drops are left as they were.
fn slotsOf(j: *const Join, comptime T: type, c: filter_eval.ColumnT(T), sel: *const selection.SelectionVector, out: []u32, a: std.mem.Allocator) !void {
    if (c.codes) |codes| {
        const by_code = try a.alloc(u32, c.dict.len);
        defer a.free(by_code);
        for (c.dict, by_code) |k, *s| s.* = j.slotOf(k) orelse EMPTY;
        var it = sel.iterator();
        while (it.next()) |r| {
            const is_null = if (c.def_levels) |dl| dl[r] < c.max_def else false;
            out[r] = if (is_null) EMPTY else by_code[codes[r]];
        }
        return;
    }
    var it = sel.iterator();
    while (it.next()) |r| {
        const is_null = if (c.def_levels) |dl| dl[r] < c.max_def else false;
        out[r] = if (is_null) EMPTY else j.slotOf(c.values[r]) orelse EMPTY;
    }
}

/// Slots of a composite foreign key's selected rows. Each column's value takes its place in the build's range for
/// that column (`KeyColumn.place`), once per dictionary entry where the column is coded, and the places add up to the
/// key the build packed. A NULL in any column, or a value outside its range, matches nothing.
fn slotsOfComposite(j: *const Join, fk: Column, more: []const Column, sel: *const Selection, out: []u32, a: std.mem.Allocator) filter_eval.Error!void {
    if (more.len + 1 != j.cols.len) return error.BadColumn;
    const none = std.math.maxInt(u64);
    const keys = try a.alloc(u64, sel.len);
    defer a.free(keys);
    @memset(keys, 0);
    for (j.cols, 0..) |kc, i| switch (if (i == 0) fk else more[i - 1]) {
        inline .i32, .i64, .string => |c| {
            const by_code: []u64 = if (c.codes != null) try a.alloc(u64, c.dict.len) else &.{};
            defer a.free(by_code);
            if (c.codes != null) for (c.dict, by_code) |v, *at| {
                at.* = kc.place(v) orelse none;
            };
            var it = sel.iterator();
            while (it.next()) |r| {
                if (keys[r] == none) continue;
                const is_null = if (c.def_levels) |dl| dl[r] < c.max_def else false;
                const at = if (is_null)
                    none
                else if (c.codes) |codes|
                    by_code[codes[r]]
                else
                    kc.place(c.values[r]) orelse none;
                keys[r] = if (at == none) none else keys[r] + at;
            }
        },
        else => return error.BadColumn,
    };
    var it = sel.iterator();
    while (it.next()) |r| out[r] = if (keys[r] == none) EMPTY else j.slot(@intCast(keys[r])) orelse EMPTY;
}

fn slotsOfColumn(j: *const Join, fk: Column, more: []const Column, sel: *const Selection, out: []u32, a: std.mem.Allocator) filter_eval.Error!void {
    if (j.cols.len > 1) return slotsOfComposite(j, fk, more, sel, out, a);
    return switch (fk) {
        .i32 => |c| slotsOf(j, i32, c, sel, out, a),
        .i64 => |c| slotsOf(j, i64, c, sel, out, a),
        .string => |c| slotsOf(j, []const u8, c, sel, out, a),
        else => error.BadColumn,
    };
}

/// The slots of `fk`'s selected rows (with `more`, a composite key's further columns), on `a`.
pub fn slotsFor(j: *const Join, fk: Column, more: []const Column, sel: *const Selection, a: std.mem.Allocator) filter_eval.Error![]u32 {
    const out = try a.alloc(u32, sel.len);
    try slotsOfColumn(j, fk, more, sel, out, a);
    return out;
}

/// A composite key's further FK columns in a batch, by their leaves; none for a key of one column.
pub fn moreColumns(a: std.mem.Allocator, cols: []const Column, lookup: []const ?usize, leaves: []const usize) filter_eval.Error![]const Column {
    if (leaves.len == 0) return &.{};
    const out = try a.alloc(Column, leaves.len);
    for (leaves, out) |leaf, *col| col.* = cols[lookup[leaf] orelse return error.BadColumn];
    return out;
}

/// Keep the selected rows whose foreign key has a build match (INNER and SEMI alike: the build is N:1). With
/// `capture` (one entry per row of `sel`), the selected rows' slots stay there for the columns published later.
/// Keep the selected rows the join keeps (`Kind`): INNER and SEMI those whose FK has a build match; ANTI those whose
/// FK is not NULL and has none, and no row when a build key was NULL (SQL's `NOT IN`; a NOT EXISTS keeps every row
/// without a match, `not_exists`); LEFT every row. With `capture`
/// (one entry per row of `sel`), the selected rows' slots stay there for the columns published later.
pub fn narrow(j: *const Join, fk: Column, more: []const Column, sel: *Selection, a: std.mem.Allocator, capture: ?[]u32) filter_eval.Error!void {
    if (j.ownKey() and j.keys == .dense and (j.kind == .inner or j.kind == .semi)) switch (fk) {
        inline .i32, .i64 => |c| if (c.codes == null) {
            narrowDense(j.keys.dense, c, sel, capture);
            return;
        },
        else => {},
    };
    if (j.kind == .anti and !j.not_exists) {
        // `x NOT IN (...)` is UNKNOWN against a NULL in the list, and TRUE for every x, NULL too, against no row.
        if (j.null_key) @memset(sel.mask, 0);
        if (j.null_key or j.n == 0) return;
    }
    const at = capture orelse try a.alloc(u32, sel.len);
    defer if (capture == null) a.free(at);
    try slotsOfColumn(j, fk, more, sel, at, a);
    if (j.kind == .left) return;
    const info = fk.nullInfo();
    for (sel.mask, 0..) |*w, wi| {
        var bits = w.*;
        while (bits != 0) : (bits &= bits - 1) {
            const r = wi * 64 + @ctz(bits);
            const drop = switch (j.kind) {
                .inner, .semi => at[r] == EMPTY,
                .anti => at[r] != EMPTY or (!j.not_exists and if (info.def_levels) |dl| dl[r] < info.max_def else false),
                .left => unreachable,
            };
            if (drop) w.* &= ~(@as(u64, 1) << @intCast(r & 63));
        }
    }
}

/// A plain dense probe can narrow each selection word while capturing its slots. No temporary slot array or
/// second row pass is needed; inactive rows stay untouched, as in `slotsOf`.
fn narrowDense(d: Dense, c: anytype, sel: *selection.SelectionVector, capture: ?[]u32) void {
    const T = @TypeOf(c.values[0]);
    for (sel.mask, 0..) |*word, wi| {
        var bits = word.*;
        var kept = bits;
        while (bits != 0) : (bits &= bits - 1) {
            const bit: u6 = @intCast(@ctz(bits));
            const r = wi * 64 + bit;
            const at = if (keyAt(T, c, r)) |key| d.slot(key) orelse EMPTY else EMPTY;
            if (capture) |out| out[r] = at;
            kept &= ~(@as(u64, @intFromBool(at == EMPTY)) << bit);
        }
        word.* = kept;
    }
}

/// Payload `p` as a coded column over the rows `sel` keeps, from their slots (every one has a match, the lookup
/// conjunct having run first, except under LEFT, where a miss is NULL, as a build row's own NULL is). Rows outside
/// `sel` take code 0; no consumer reads them. With `values`, values are materialized beside the codes for the
/// consumers that read values.
pub fn column(j: *const Join, p: usize, at: []const u32, sel: *const selection.SelectionVector, a: std.mem.Allocator, values: bool) !filter_eval.Batch.Column {
    const payload = j.payloads[p];
    const codes = try a.alloc(u32, sel.len);
    @memset(codes, 0);
    var def: ?[]u32 = null;
    const max_def: u32 = @intFromBool(j.kind == .left or payload.nulls != null);
    var it = sel.iterator();
    while (it.next()) |r| {
        if (at[r] == EMPTY or payload.isNull(at[r])) {
            if (at[r] == EMPTY and j.kind != .left) return error.LookupWithoutMatch;
            if (def == null) {
                def = try a.alloc(u32, sel.len);
                @memset(def.?, 1);
            }
            def.?[r] = 0;
            continue;
        }
        codes[r] = payload.codes.get(at[r]);
    }
    if (payload.deferred) |d| {
        const vals = try a.alloc([]const u8, sel.len);
        @memset(vals, "");
        var scratch: std.heap.ArenaAllocator = .init(d.scratch);
        defer scratch.deinit();
        for (d.meta.row_groups.items, 0..) |*rg, ri| {
            // Kept strings have already moved into `a`; recycle page/value storage across source groups.
            defer _ = scratch.reset(.retain_capacity);
            const sa = scratch.allocator();
            var gate = try selection.SelectionVector.init(sa, @intCast(rg.num_rows));
            gate.clearAll();
            var kept = sel.iterator();
            while (kept.next()) |r| {
                if (!nullAt(def, r) and d.rows[codes[r]].rg == ri) gate.set(d.rows[codes[r]].row, true);
            }
            if (gate.count() == 0) continue;
            const cm = rg.columns.items[d.leaf].meta_data orelse return error.ColumnMetaMissing;
            const levels = d.meta.getColumnLevels(cm.path_in_schema.items);
            const opts = column_mod.chunkOptions(.{ .fast_levels = true, .row_gate = &gate }, d.meta, cm);
            const decoded = try @import("../consumer/column.zig").decodeColumnTWithOptions([]const u8, sa, try chunkOf(d.bytes, cm), cm.codec, levels, gate.len, opts);
            kept = sel.iterator();
            while (kept.next()) |r| {
                if (!nullAt(def, r) and d.rows[codes[r]].rg == ri)
                    vals[r] = try a.dupe(u8, decoded.values[d.rows[codes[r]].row]);
            }
        }
        return .{ .string = .{ .values = vals, .def_levels = def, .max_def = max_def, .has_nulls = def != null } };
    }
    // A DECIMAL reads as the scan decodes one (`decimal.decodeColumnExact`): its f64 view beside its exact integers.
    if (decimal.kindFromSchema(&payload.element)) |k| switch (payload.dict) {
        inline .i32, .i64, .i128 => |dict| {
            const exact = try a.alloc(@TypeOf(dict[0]), sel.len);
            if (dict.len == 0) @memset(exact, 0) else for (codes, exact) |code, *v| {
                v.* = dict[code];
            }
            const vals = try a.alloc(f64, sel.len);
            for (exact, vals) |v, *f| f.* = decimal.applyScaleInt(@TypeOf(v), v, k.scale);
            return .{ .f64 = .{
                .values = vals,
                .unscaled = @unionInit(filter_eval.Exact, @typeName(@TypeOf(dict[0])), exact),
                .def_levels = def,
                .max_def = max_def,
                .has_nulls = def != null,
            } };
        },
        else => unreachable,
    };
    if (payload.plain) return switch (payload.dict) {
        inline else => |dict, tag| blk: {
            const T = @TypeOf(dict[0]);
            const vals = try a.alloc(T, sel.len);
            if (dict.len == 0) @memset(vals, std.mem.zeroes(T)) else for (codes, vals) |code, *v| {
                v.* = dict[code];
            }
            break :blk @unionInit(filter_eval.Batch.Column, @tagName(tag), .{
                .values = vals,
                .def_levels = def,
                .max_def = max_def,
                .has_nulls = def != null,
            });
        },
    };
    return switch (payload.dict) {
        inline else => |dict, tag| blk: {
            const T = @TypeOf(dict[0]);
            var vals: []T = &.{};
            if (values) {
                vals = try a.alloc(T, sel.len);
                if (dict.len == 0) @memset(vals, std.mem.zeroes(T)) else for (codes, vals) |code, *v| {
                    v.* = dict[code];
                }
            }
            break :blk @unionInit(filter_eval.Batch.Column, @tagName(tag), .{
                .values = vals,
                .codes = codes,
                .dict = dict,
                .def_levels = def,
                .max_def = max_def,
                .has_nulls = def != null,
            });
        },
    };
}

fn nullAt(def: ?[]const u32, r: usize) bool {
    return if (def) |d| d[r] == 0 else false;
}

// ---------------------------------------------------------------- build

pub const Spec = struct {
    kind: Kind,
    build_name: []const u8,
    key_name: []const u8,
    /// Build leaf indices. `key_more`: a composite key's further columns.
    key: usize,
    key_more: []const usize = &.{},
    payload: []const usize,
    deferred: []const bool = &.{},
    filter: ?filter_ast.Filter,
    threads: usize,
    /// Bytes the build may hold; null when the query has no `--max-memory`.
    budget: ?usize,

    fn defers(s: Spec, p: usize) bool {
        return p < s.deferred.len and s.deferred[p];
    }
};

/// One build row group's survivors: their keys, and per payload a local dictionary and each survivor's local code.
const Part = struct {
    keys: Keys = .{ .wide = &.{} },
    /// A row the build's filter kept had a NULL key: `NOT IN` is then UNKNOWN for every probe row.
    null_key: bool = false,
    /// A numbered key's columns over the survivors, until they pack into `keys` (`packKeys`): an integer column's
    /// values, or a string column's local dictionary and then its numbers.
    key_cols: []KeyValues = &.{},
    local: []Local = &.{},
    lo: i64 = std.math.maxInt(i64),
    hi: i64 = std.math.minInt(i64),
    /// The survivors as a row mask (a bit per row, not a row list), for the payloads decoded on their own
    /// (`LateLocal`).
    kept: []u64 = &.{},

    const KeyValues = struct { ints: []i64 = &.{}, strings: Local = Local.empty };

    /// One of the locals that unify over the parts: a payload's, or a string key column's.
    const Which = union(enum) { payload: usize, key: usize };

    fn localOf(p: *Part, which: Which) *Local {
        return switch (which) {
            .payload => |i| &p.local[i],
            .key => |i| &p.key_cols[i].strings,
        };
    }

    /// The keys, the survivors' mask and the local codes, which the build allocates from its `gpa` rather than an
    /// arena.
    fn freeLarge(p: *Part, gpa: std.mem.Allocator) void {
        p.keys.free(gpa);
        for (p.key_cols) |kv| {
            gpa.free(kv.ints);
            kv.strings.codes.free(gpa);
        }
        p.key_cols = &.{};
        gpa.free(p.kept);
        for (p.local) |l| l.codes.free(gpa);
        p.keys = .{ .wide = &.{} };
        p.kept = &.{};
        p.local = &.{};
    }
};

/// A part's survivors' keys, in row order: as they are, or as offsets from the part's `lo` when its keys span less
/// than 2^32 (half the bytes, for keys held until the build is laid out).
const Keys = union(enum) {
    wide: []i64,
    narrow: []u32,

    fn len(k: Keys) usize {
        return switch (k) {
            inline else => |v| v.len,
        };
    }

    fn free(k: Keys, a: std.mem.Allocator) void {
        switch (k) {
            inline else => |v| a.free(v),
        }
    }

    /// `keys`, whose range is `[lo, hi]`, held on `gpa`.
    fn from(gpa: std.mem.Allocator, keys: []const i64, lo: i64, hi: i64) !Keys {
        if (keys.len == 0 or @as(i128, hi) - lo > std.math.maxInt(u32)) return .{ .wide = try gpa.dupe(i64, keys) };
        const out = try gpa.alloc(u32, keys.len);
        for (keys, out) |k, *o| o.* = @intCast(k - lo);
        return .{ .narrow = out };
    }

    /// Key `raw` of `v` as an i64, for a part whose smallest key is `lo`.
    inline fn of(raw: anytype, lo: i64) i64 {
        return if (@TypeOf(raw) == u32) lo + raw else raw;
    }
};

const Local = struct {
    /// Each survivor's local code, at its dictionary's width.
    codes: Codes,
    dict: Dict,
    /// Local code -> global code, filled when the dictionaries unify; empty when the local codes are the global ones
    /// (a build of one part).
    global: []u32 = &.{},
    /// A plain payload's (`Payload.plain`): the bytes of its strings, a value per survivor in `dict`.
    bytes: usize = 0,
    /// A bit per survivor, set where its value is NULL (its code a placeholder, 0); empty when none is.
    nulls: []u64 = &.{},

    const empty: Local = .{ .codes = .{ .u32 = &.{} }, .dict = .{ .i64 = &.{} } };

    fn globalOf(l: Local, row: usize) u32 {
        const code = l.codes.get(@intCast(row));
        return if (l.global.len == 0) code else l.global[code];
    }
};

pub const BuildError = error{ DuplicateBuildKey, JoinKeyType, JoinKeyRange, JoinPayloadType, JoinBuildExceedsBudget, OutOfMemory };

const Ctx = struct {
    /// Per build row group, one for the row group and one per payload (`arena`): its survivors' dictionaries and
    /// remaps until the build is laid out, each written by one task at a time.
    arenas: []std.heap.ArenaAllocator,
    /// The survivors' keys and local codes, each an exact allocation (`Part.freeLarge`): in a row group's arena
    /// alone, a first block half again its first request held SF30 orders' 270 MB of them in ~400 MB.
    gpa: std.mem.Allocator,
    /// One scratch arena per worker (`parallel`), reset between its row groups but kept: a row group's scratch
    /// mapped afresh and unmapped again faulted SF30 orders' 45M rows in at ~3.6 GB of new pages.
    workers: []std.heap.ArenaAllocator,
    bytes: []const u8,
    meta: *const schema.FileMetaData,
    spec: Spec,
    parts: []Part,
    /// By leaf: what the filter reads, the key among it. These decode first, with the filter (`buildPart`).
    filter_cols: []const bool,
    /// The payloads (indices into `spec.payload`) no filter reads: each decodes on its own, per row group
    /// (`LateLocal`), so a build of few row groups and many payloads still has a task per core.
    late: []const usize,
    /// By payload: held plain (`Payload.plain`), a column no row group's chunk dictionary-encodes (a filter's, numeric).
    plain: []const bool,

    fn arena(c: *const Ctx, rg_index: usize, slot: usize) std.mem.Allocator {
        return c.arenas[rg_index * (c.spec.payload.len + 1) + slot].allocator();
    }

    /// `l` (on a task's scratch) kept for the layout: its codes narrowed into `gpa`, its dictionary copied into `a`
    /// (the payload's arena, or the row group's for a key).
    fn keepLocal(c: *const Ctx, a: std.mem.Allocator, l: Local) !Local {
        const codes = try l.codes.narrowed(c.gpa, l.dict.len());
        errdefer codes.free(c.gpa);
        const dict: Dict = switch (l.dict) {
            .string => |d| blk: {
                const out = try a.alloc([]const u8, d.len);
                for (d, out) |v, *o| o.* = try a.dupe(u8, v);
                break :blk .{ .string = out };
            },
            inline else => |d, tag| @unionInit(Dict, @tagName(tag), try a.dupe(@TypeOf(d[0]), d)),
        };
        return .{ .codes = codes, .dict = dict, .nulls = try a.dupe(u64, l.nulls) };
    }
};

fn chunkOf(bytes: []const u8, cm: schema.ColumnMetaData) ![]const u8 {
    const start: usize = @intCast(cm.dictionary_page_offset orelse cm.data_page_offset);
    const len: usize = @intCast(cm.total_compressed_size);
    if (start > bytes.len or len > bytes.len - start) return error.MissingChunkBytes;
    return bytes[start..][0..len];
}

fn decodeValues(a: std.mem.Allocator, bytes: []const u8, meta: *const schema.FileMetaData, rg: *const schema.RowGroup, ci: usize) !filter_eval.Batch.Column {
    const cm = rg.columns.items[ci].meta_data orelse return error.ColumnMetaMissing;
    const chunk = try chunkOf(bytes, cm);
    const levels = meta.getColumnLevels(cm.path_in_schema.items);
    const rows: usize = @intCast(rg.num_rows);
    const options = column_mod.chunkOptions(.{}, meta, cm);
    const decode = @import("../consumer/column.zig").decodeColumnTWithOptions;
    if (meta.getColumnSchema(cm.path_in_schema.items)) |e| if (decimal.kindFromSchema(&e)) |k|
        return .{ .f64 = try decimal.decodeColumnExact(a, chunk, cm.codec, levels, rows, k, options, null) };
    return switch (cm.type) {
        .INT32 => .{ .i32 = try decode(i32, a, chunk, cm.codec, levels, rows, options) },
        .INT64 => .{ .i64 = try decode(i64, a, chunk, cm.codec, levels, rows, options) },
        .FLOAT => .{ .f32 = try decode(f32, a, chunk, cm.codec, levels, rows, options) },
        .DOUBLE => .{ .f64 = try decode(f64, a, chunk, cm.codec, levels, rows, options) },
        .BOOLEAN => .{ .boolean = try decode(bool, a, chunk, cm.codec, levels, rows, options) },
        .BYTE_ARRAY => .{ .string = try decode([]const u8, a, chunk, cm.codec, levels, rows, options) },
        else => error.JoinPayloadType,
    };
}

/// A local dictionary of `values` at the selected rows, first appearance first.
fn localDict(comptime T: type, a: std.mem.Allocator, c: filter_eval.ColumnT(T), rows: []const u32) !Local {
    const Ctx2 = if (T == []const u8) std.hash_map.StringContext else struct {
        pub fn hash(_: @This(), v: T) u64 {
            return std.hash.Wyhash.hash(0, std.mem.asBytes(&v));
        }
        pub fn eql(_: @This(), x: T, y: T) bool {
            return std.mem.eql(u8, std.mem.asBytes(&x), std.mem.asBytes(&y));
        }
    };
    var map: std.HashMapUnmanaged(T, u32, Ctx2, 80) = .empty;
    defer map.deinit(a);
    var dict: std.ArrayList(T) = .empty;
    const codes = try a.alloc(u32, rows.len);
    var nulls: []u64 = &.{};
    for (rows, codes, 0..) |r, *code, i| {
        if (c.def_levels) |dl| if (dl[r] < c.max_def) {
            code.* = 0;
            try markBit(a, &nulls, rows.len, i);
            continue;
        };
        const v = c.values[r];
        const gop = try map.getOrPut(a, v);
        if (!gop.found_existing) {
            gop.value_ptr.* = @intCast(dict.items.len);
            try dict.append(a, if (T == []const u8) try a.dupe(u8, v) else v);
        }
        code.* = gop.value_ptr.*;
    }
    const d: Dict = switch (T) {
        i32 => .{ .i32 = dict.items },
        i64 => .{ .i64 = dict.items },
        i128 => .{ .i128 = dict.items },
        f64 => .{ .f64 = dict.items },
        []const u8 => .{ .string = dict.items },
        else => unreachable,
    };
    return .{ .codes = .{ .u32 = codes }, .dict = d, .nulls = nulls };
}

/// A DECIMAL's exact unscaled integers (`decodeValues`), which its payload holds; any other column as it is.
fn exactOf(col: filter_eval.Batch.Column) !filter_eval.Batch.Column {
    return if (col == .f64 and col.f64.unscaled != null) try expr_eval.exactColumn(col) else col;
}

/// A payload chunk that is dictionary-encoded throughout, as its own dictionary and the survivors' codes in it: the
/// dictionary is the local one, hashed once per entry instead of once per row. Null for any other chunk.
fn codedLocal(comptime T: type, a: std.mem.Allocator, bytes: []const u8, meta: *const schema.FileMetaData, rg: *const schema.RowGroup, ci: usize, keep: []const u32) !?Local {
    const cm = rg.columns.items[ci].meta_data orelse return null;
    if (cm.dictionary_page_offset == null) return null;
    const levels = meta.getColumnLevels(cm.path_in_schema.items);
    if (levels.max_rep != 0) return null;
    const options = column_mod.chunkOptions(.{ .any_dictionary = true }, meta, cm);
    var r = column_mod.ColumnChunkReader(T).initWithOptions(try chunkOf(bytes, cm), cm.codec, levels, a, options, null);
    const got = (try r.readAllCodes(@intCast(rg.num_rows))) orelse return null;
    const coded = switch (got) {
        .codes => |x| x,
        .values => return null,
    };
    const codes = try a.alloc(u32, keep.len);
    // A NULL row's code is 0 (`readPageCodes`).
    for (keep, codes) |row, *code| code.* = coded.codes[row];
    var nulls: []u64 = &.{};
    if (coded.has_nulls) for (keep, 0..) |row, i| {
        if (coded.def_levels.?[row] < levels.max_def) try markBit(a, &nulls, keep.len, i);
    };
    const d: Dict = switch (T) {
        i32 => .{ .i32 = coded.dict },
        i64 => .{ .i64 = coded.dict },
        f64 => .{ .f64 = coded.dict },
        []const u8 => .{ .string = coded.dict },
        else => unreachable,
    };
    return .{ .codes = .{ .u32 = codes }, .dict = d, .nulls = nulls };
}

/// A payload's local dictionary over the `keep` rows: the chunk's own when it is dictionary-coded throughout and no
/// filter decoded it, else one built from its values (`decoded`, or decoded here).
fn localOf(c: *const Ctx, sa: std.mem.Allocator, rg: *const schema.RowGroup, p: usize, keep: []const u32, decoded: ?filter_eval.Batch.Column) !Local {
    if (decoded == null) {
        const cm = rg.columns.items[p].meta_data orelse return error.ColumnMetaMissing;
        if (switch (cm.type) {
            .INT32 => try codedLocal(i32, sa, c.bytes, c.meta, rg, p, keep),
            .INT64 => try codedLocal(i64, sa, c.bytes, c.meta, rg, p, keep),
            .DOUBLE => try codedLocal(f64, sa, c.bytes, c.meta, rg, p, keep),
            .BYTE_ARRAY => try codedLocal([]const u8, sa, c.bytes, c.meta, rg, p, keep),
            else => null,
        }) |coded| return coded;
    }
    return switch (try exactOf(decoded orelse try decodeValues(sa, c.bytes, c.meta, rg, p))) {
        .i32 => |pc| try localDict(i32, sa, pc, keep),
        .i64 => |pc| try localDict(i64, sa, pc, keep),
        .i128 => |pc| try localDict(i128, sa, pc, keep),
        .f64 => |pc| try localDict(f64, sa, pc, keep),
        .string => |pc| try localDict([]const u8, sa, pc, keep),
        else => error.JoinPayloadType,
    };
}

/// One row group's filter pass: decode what the filter reads, keep the rows it passes with a non-NULL key, and take
/// the local dictionaries of the payloads it decoded. The other payloads decode after, on their own (`LateLocal`).
fn buildPart(c: *Ctx, worker: usize, rg_index: usize) !void {
    const scratch = &c.workers[worker];
    defer _ = scratch.reset(.retain_capacity);
    const sa = scratch.allocator();
    const rg = &c.meta.row_groups.items[rg_index];
    const rows: usize = @intCast(rg.num_rows);
    const part = &c.parts[rg_index];
    part.local = try c.arena(rg_index, 0).alloc(Local, c.spec.payload.len);
    @memset(part.local, Local.empty);
    var cols: std.ArrayList(filter_eval.Batch.Column) = .empty;
    const lookup = try sa.alloc(?usize, c.meta.schema.items.len);
    @memset(lookup, null);
    for (c.filter_cols, 0..) |f, ci| if (f) {
        lookup[ci] = cols.items.len;
        try cols.append(sa, try decodeValues(sa, c.bytes, c.meta, rg, ci));
    };
    var sel = try selection.SelectionVector.init(sa, rows);
    if (c.spec.filter) |f| {
        const batch: filter_eval.Batch = .{ .cols = cols.items, .num_rows = rows };
        try filter_eval.evaluate(f, &batch, &sel, lookup, sa);
    }
    const key_col = cols.items[lookup[c.spec.key].?];
    const key_more = try sa.alloc(Column, c.spec.key_more.len);
    for (c.spec.key_more, key_more) |leaf, *col| col.* = cols.items[lookup[leaf].?];
    const kept = sel.count();
    var keep: std.ArrayList(u32) = try .initCapacity(sa, kept);
    var keys: std.ArrayList(i64) = try .initCapacity(sa, kept);
    var lo: i64 = std.math.maxInt(i64);
    var hi: i64 = std.math.minInt(i64);
    var it = sel.iterator();
    while (it.next()) |r| {
        const k: ?i64 = switch (key_col) {
            .i32 => |kc| keyAt(i32, kc, r),
            .i64 => |kc| keyAt(i64, kc, r),
            // A string's key is its number among the build's, known once every part is read (`packKeys`).
            .string => |kc| if (kc.def_levels != null and kc.def_levels.?[r] < kc.max_def) null else 0,
            else => return error.JoinKeyType,
        };
        // A NULL build key matches no probe row, NULL ones included; nor does a composite key with a NULL column.
        const key = if (k != null and !(key_more.len != 0 and nullAmong(key_more, r))) k.? else {
            part.null_key = true;
            sel.mask[r / 64] &= ~(@as(u64, 1) << @intCast(r & 63));
            continue;
        };
        keep.appendAssumeCapacity(@intCast(r));
        keys.appendAssumeCapacity(key);
        lo = @min(lo, key);
        hi = @max(hi, key);
    }
    for (c.spec.payload, part.local, 0..) |p, *l, pi| if (c.filter_cols[p]) {
        const col = cols.items[lookup[p].?];
        const pa = c.arena(rg_index, 1 + pi);
        l.* = if (c.plain[pi]) try plainLocal(pa, col, keep.items) else try c.keepLocal(pa, try localOf(c, sa, rg, p, keep.items, col));
    };
    if (c.late.len != 0 or c.spec.deferred.len != 0) part.kept = try c.gpa.dupe(u64, sel.mask);
    if (key_col == .string or key_more.len != 0) {
        const a = c.arena(rg_index, 0);
        part.key_cols = try a.alloc(Part.KeyValues, 1 + key_more.len);
        @memset(part.key_cols, .{});
        for (part.key_cols, 0..) |*kv, i| switch (if (i == 0) key_col else key_more[i - 1]) {
            inline .i32, .i64 => |kc| {
                kv.ints = try c.gpa.alloc(i64, keep.items.len);
                for (keep.items, kv.ints) |r, *v| v.* = kc.values[r];
            },
            .string => |kc| kv.strings = try c.keepLocal(a, try localDict([]const u8, sa, kc, keep.items)),
            else => return error.JoinKeyType,
        };
        return;
    }
    part.keys = try Keys.from(c.gpa, keys.items, lo, hi);
    part.lo = lo;
    part.hi = hi;
}

fn nullAmong(cols: []const Column, r: usize) bool {
    for (cols) |col| {
        const info = col.nullInfo();
        if (info.def_levels) |dl| if (dl[r] < info.max_def) return true;
    }
    return false;
}

/// One payload no filter reads, over one row group's survivors (`Part.kept`): task `i` is row group `i / late.len`'s
/// payload `late[i % late.len]`.
const LateLocal = struct {
    c: *Ctx,

    fn run(self: @This(), worker: usize, i: usize) !void {
        const c = self.c;
        const rg_index = i / c.late.len;
        const pi = c.late[i % c.late.len];
        const scratch = &c.workers[worker];
        defer _ = scratch.reset(.retain_capacity);
        const sa = scratch.allocator();
        const part = &c.parts[rg_index];
        const keep = try sa.alloc(u32, part.keys.len());
        var n: usize = 0;
        for (part.kept, 0..) |w, wi| {
            var bits = w;
            while (bits != 0) : (bits &= bits - 1) {
                keep[n] = @intCast(wi * 64 + @ctz(bits));
                n += 1;
            }
        }
        const rg = &c.meta.row_groups.items[rg_index];
        if (c.spec.defers(pi)) {
            const cm = rg.columns.items[c.spec.payload[pi]].meta_data orelse return error.ColumnMetaMissing;
            const levels = c.meta.getColumnLevels(cm.path_in_schema.items);
            var reader = column_mod.ColumnChunkReader([]const u8).initWithOptions(try chunkOf(c.bytes, cm), cm.codec, levels, sa, column_mod.chunkOptions(.{}, c.meta, cm), null);
            // A deferred payload reads no value here, only which survivors hold NULL.
            if (try reader.readAllLevels(@intCast(rg.num_rows))) |validity| {
                const nulls = &part.local[pi].nulls;
                if (validity.def_levels) |dl| for (keep, 0..) |r, at| {
                    if (dl[r] < levels.max_def) try markBit(c.arena(rg_index, 1 + pi), nulls, keep.len, at);
                };
            }
            return;
        }
        if (!c.plain[pi]) {
            const local = try localOf(c, sa, rg, c.spec.payload[pi], keep, null);
            part.local[pi] = try c.keepLocal(c.arena(rg_index, 1 + pi), local);
            return;
        }
        // Plain: the survivors' values as decoded (strings into the payload's arena, which outlives the layout), unhashed.
        const a = c.arena(rg_index, 1 + pi);
        part.local[pi] = try plainLocal(a, try decodeValues(a, c.bytes, c.meta, rg, c.spec.payload[pi]), keep);
    }
};

/// A plain payload's local (`Payload.plain`): the `keep` rows' values of `col`, copied into `a`, unhashed; strings
/// still point into `col`.
fn plainLocal(a: std.mem.Allocator, col: filter_eval.Batch.Column, keep: []const u32) !Local {
    return switch (try exactOf(col)) {
        inline .i32, .i64, .i128, .f64, .string => |cc, tag| blk: {
            const values = try a.alloc(@TypeOf(cc.values[0]), keep.len);
            var bytes: usize = 0;
            var nulls: []u64 = &.{};
            for (keep, values, 0..) |r, *v, i| {
                if (cc.def_levels) |dl| if (dl[r] < cc.max_def) {
                    v.* = if (tag == .string) "" else 0;
                    try markBit(a, &nulls, keep.len, i);
                    continue;
                };
                v.* = cc.values[r];
                if (tag == .string) bytes += v.len;
            }
            const dict = @unionInit(Dict, @tagName(tag), values);
            break :blk .{ .codes = .{ .identity = keep.len }, .dict = dict, .bytes = bytes, .nulls = nulls };
        },
        else => error.JoinPayloadType,
    };
}

/// The most workers `parallel` runs.
const max_workers = 64;

/// `f(ctx, worker, i)` for every `i` below `n`, on up to `threads` workers numbered from 0.
fn parallel(n: usize, threads: usize, ctx: anytype, comptime f: fn (@TypeOf(ctx), usize, usize) anyerror!void) !void {
    const C = @TypeOf(ctx);
    const W = struct {
        fn run(c: C, worker: usize, cursor: *std.atomic.Value(usize), total: usize, errs: []?anyerror) void {
            while (true) {
                const i = cursor.fetchAdd(1, .monotonic);
                if (i >= total) return;
                f(c, worker, i) catch |e| {
                    errs[i] = e;
                    cursor.store(total, .monotonic);
                    return;
                };
            }
        }
    };
    var cursor = std.atomic.Value(usize).init(0);
    const errs = try std.heap.page_allocator.alloc(?anyerror, n);
    defer std.heap.page_allocator.free(errs);
    @memset(errs, null);
    var pool: [max_workers]std.Thread = undefined;
    const t = @max(1, @min(threads, pool.len, n));
    var started: usize = 0;
    while (started < t) : (started += 1) {
        pool[started] = std.Thread.spawn(.{}, W.run, .{ ctx, started, &cursor, n, errs }) catch |e| {
            cursor.store(n, .monotonic);
            for (pool[0..started]) |th| th.join();
            return e;
        };
    }
    for (pool[0..started]) |th| th.join();
    // The lowest failing row group's error, whatever the scheduling.
    for (errs) |e| if (e) |err| return err;
}

/// One payload's dictionaries unified (`Unify.run`): its distinct values in global-code order, borrowed from the parts'
/// local dictionaries, and their string bytes; then the dictionary in the build's allocator (`Unify.copy`).
const Unified = struct {
    values: Dict = .{ .i64 = &.{} },
    /// Whether `values` is a list of its own (in `gpa`) rather than a part's local dictionary.
    owned: bool = false,
    bytes: usize = 0,
    dict: Dict = .{ .i64 = &.{} },
    blob: []u8 = &.{},

    fn deinit(u: Unified, gpa: std.mem.Allocator) void {
        if (u.owned) switch (u.values) {
            inline else => |v| gpa.free(v),
        };
    }
};

/// Unify payload `p`'s local dictionaries in row-group order: global codes number values by first appearance. Each
/// part's local-to-global map goes in that part's arena for `p` (freed with the build's other scratch); the map that
/// dedupes lives in `gpa` and goes when the task ends. One part's local dictionary is already the global one.
const Unify = struct {
    c: *Ctx,
    out: []Unified,

    fn run(self: @This(), _: usize, p: usize) !void {
        const c = self.c;
        if (c.parts.len == 0 or c.plain[p] or c.spec.defers(p)) return;
        self.out[p] = switch (c.parts[0].local[p].dict) {
            .i32 => try unify(i32, c, .{ .payload = p }),
            .i64 => try unify(i64, c, .{ .payload = p }),
            .i128 => try unify(i128, c, .{ .payload = p }),
            .f64 => try unify(f64, c, .{ .payload = p }),
            .string => try unify([]const u8, c, .{ .payload = p }),
        };
    }

    /// The values into the dictionary allocated for them, strings into one blob.
    fn copy(self: @This(), _: usize, p: usize) !void {
        if (self.c.plain[p] or self.c.spec.defers(p)) return;
        const u = self.out[p];
        switch (u.values) {
            .string => |v| {
                var at: usize = 0;
                for (v, @constCast(u.dict.string)) |x, *o| {
                    @memcpy(u.blob[at..][0..x.len], x);
                    o.* = u.blob[at..][0..x.len];
                    at += x.len;
                }
            },
            inline else => |v, tag| @memcpy(@constCast(@field(u.dict, @tagName(tag))), v),
        }
    }
};

/// The parts' local dictionaries of one payload, or of one string key column, as one.
fn unify(comptime T: type, c: *Ctx, p: Part.Which) !Unified {
    const tag = switch (T) {
        i32 => "i32",
        i64 => "i64",
        i128 => "i128",
        f64 => "f64",
        else => "string",
    };
    const gpa = c.gpa;
    var bytes: usize = 0;
    if (c.parts.len == 1) {
        const local = @field(c.parts[0].localOf(p).dict, tag);
        if (T == []const u8) for (local) |v| {
            bytes += v.len;
        };
        return .{ .values = @unionInit(Dict, tag, local), .bytes = bytes };
    }
    const Ctx2 = if (T == []const u8) std.hash_map.StringContext else struct {
        pub fn hash(_: @This(), v: T) u64 {
            return std.hash.Wyhash.hash(0, std.mem.asBytes(&v));
        }
        pub fn eql(_: @This(), x: T, y: T) bool {
            return std.mem.eql(u8, std.mem.asBytes(&x), std.mem.asBytes(&y));
        }
    };
    var map: std.HashMapUnmanaged(T, u32, Ctx2, 80) = .empty;
    defer map.deinit(gpa);
    var dict: std.ArrayList(T) = .empty;
    errdefer dict.deinit(gpa);
    for (c.parts, 0..) |*part, rg_index| {
        const l = part.localOf(p);
        const local = @field(l.dict, tag);
        l.global = try c.arena(rg_index, if (p == .payload) 1 + p.payload else 0).alloc(u32, local.len);
        for (local, l.global) |v, *g| {
            const gop = try map.getOrPut(gpa, v);
            if (!gop.found_existing) {
                gop.value_ptr.* = @intCast(dict.items.len);
                try dict.append(gpa, v);
                if (T == []const u8) bytes += v.len;
            }
            g.* = gop.value_ptr.*;
        }
    }
    return .{ .values = @unionInit(Dict, tag, try dict.toOwnedSlice(gpa)), .owned = true, .bytes = bytes };
}

/// String key column `col`'s values over every part, numbered into `a` (the build's), and each part's survivors as
/// those numbers.
fn numberStrings(a: std.mem.Allocator, c: *Ctx, col: usize) !Strings {
    const u = try unify([]const u8, c, .{ .key = col });
    defer u.deinit(c.gpa);
    const values = try a.alloc([]const u8, u.values.string.len);
    const blob = try a.alloc(u8, u.bytes);
    const s: Strings = .{ .values = values, .table = try a.alloc(u32, std.math.ceilPowerOfTwoAssert(usize, @max(16, values.len * 2))) };
    @memset(s.table, EMPTY);
    var at: usize = 0;
    for (u.values.string, values, 0..) |v, *out, number| {
        @memcpy(blob[at..][0..v.len], v);
        out.* = blob[at..][0..v.len];
        at += v.len;
        s.put(@intCast(number));
    }
    for (c.parts) |*part| {
        const kv = &part.key_cols[col];
        kv.ints = try c.gpa.alloc(i64, kv.strings.codes.len());
        for (kv.ints, 0..) |*number, row| number.* = kv.strings.globalOf(row);
    }
    return s;
}

/// The keys of a build whose key is not one integer column. Each string column is numbered; each column's range is
/// the one its surviving rows span; and a row's key is its columns' places in their ranges, packed by mixed radix
/// (one string column's number, as it is). A key twice among the rows is then an integer twice, which the key
/// structure sees. Ranges whose product does not fit 63 bits refuse.
fn packKeys(a: std.mem.Allocator, c: *Ctx, domains: []const KeyDomain) ![]const KeyColumn {
    const cols = try a.alloc(KeyColumn, domains.len);
    var weight: u64 = 1;
    for (cols, domains, 0..) |*kc, domain, i| {
        kc.* = .{ .domain = domain, .weight = weight };
        if (domain.text()) {
            kc.strings = try numberStrings(a, c, i);
            kc.span = kc.strings.?.values.len;
        } else {
            var lo: i64 = std.math.maxInt(i64);
            var hi: i64 = std.math.minInt(i64);
            for (c.parts) |part| for (part.key_cols[i].ints) |v| {
                lo = @min(lo, v);
                hi = @max(hi, v);
            };
            if (lo <= hi) {
                kc.lo = lo;
                kc.span = std.math.cast(u64, @as(i128, hi) - lo + 1) orelse return error.JoinKeyRange;
            }
        }
        weight = std.math.mul(u64, weight, @max(kc.span, 1)) catch return error.JoinKeyRange;
        if (weight > std.math.maxInt(i64)) return error.JoinKeyRange;
    }
    for (c.parts) |*part| {
        const keys = try c.gpa.alloc(i64, part.key_cols[0].ints.len);
        defer c.gpa.free(keys);
        @memset(keys, 0);
        for (cols, part.key_cols) |kc, kv| for (keys, kv.ints) |*k, v| {
            k.* += @intCast(@as(u64, @bitCast(v -% kc.lo)) * kc.weight);
        };
        for (keys) |k| {
            part.lo = @min(part.lo, k);
            part.hi = @max(part.hi, k);
        }
        part.keys = try Keys.from(c.gpa, keys, part.lo, part.hi);
    }
    return cols;
}

/// Each part's keys into the dense map's bits, flagging a key set twice. With `disjoint` (no two parts' key ranges
/// overlap, as in a file sorted by its key) a word strictly inside a part's own range is that part's alone and takes
/// a plain store; only its two edge words, which a neighbour may share, take an atomic one.
const SetBits = struct {
    parts: []const Part,
    d: *const Dense,
    dup: *std.atomic.Value(bool),
    disjoint: bool,
    fn run(c: @This(), _: usize, i: usize) !void {
        const part = c.parts[i];
        if (part.keys.len() == 0) return;
        const first: u64 = @as(u64, @bitCast(part.lo -% c.d.lo)) >> 6;
        const last: u64 = @as(u64, @bitCast(part.hi -% c.d.lo)) >> 6;
        var dup = false;
        switch (part.keys) {
            inline else => |keys| for (keys) |raw| {
                const k = Keys.of(raw, part.lo);
                const at: u64 = @bitCast(k -% c.d.lo);
                const w = at >> 6;
                const bit = @as(u64, 1) << @intCast(at & 63);
                const word = &c.d.bits[@intCast(w)];
                const old = if (c.disjoint and w > first and w < last) blk: {
                    const o = word.*;
                    word.* = o | bit;
                    break :blk o;
                } else @atomicRmw(u64, word, .Or, bit, .monotonic);
                dup = dup or old & bit != 0;
            },
        }
        if (dup) c.dup.store(true, .monotonic);
    }

    /// Whether no two parts' `[lo, hi]` key ranges overlap.
    fn disjointParts(a: std.mem.Allocator, parts: []const Part) !bool {
        var order: std.ArrayList(*const Part) = .empty;
        defer order.deinit(a);
        for (parts) |*p| if (p.keys.len() != 0) try order.append(a, p);
        std.mem.sort(*const Part, order.items, {}, struct {
            fn lt(_: void, x: *const Part, y: *const Part) bool {
                return x.lo < y.lo;
            }
        }.lt);
        for (order.items[0..order.items.len -| 1], order.items[@min(1, order.items.len)..]) |x, y| if (x.hi >= y.lo) return false;
        return true;
    }
};

const FillCodes = struct {
    j: *const Join,
    gpa: std.mem.Allocator,
    parts: []const Part,
    codes: []const Codes,
    /// By payload: a plain one's values by slot and the blob its strings copy into, from each part's start in it.
    plain: []const Plain,
    deferred: []const bool,
    locators: []Payload.Locator,

    /// `values` is the payload's own dictionary, written here; a string's bytes copy into `blob`.
    const Plain = struct { on: bool = false, values: Dict = .{ .i64 = &.{} }, blob: []u8 = &.{}, starts: []usize = &.{} };

    fn run(c: @This(), _: usize, i: usize) !void {
        const part = c.parts[i];
        const cursor = try c.gpa.alloc(usize, c.plain.len);
        defer c.gpa.free(cursor);
        for (c.plain, cursor) |pl, *at| at.* = if (pl.on) pl.starts[i] else 0;
        // A part with no NULL fills as it always did.
        const any_null = for (part.local) |l| {
            if (l.nulls.len != 0) break true;
        } else false;
        var source_row: usize = 0;
        switch (part.keys) {
            inline else => |keys| for (keys, 0..) |raw, row| {
                const s = c.j.slot(Keys.of(raw, part.lo)).?;
                if (c.locators.len != 0) {
                    while (part.kept[source_row / 64] & (@as(u64, 1) << @intCast(source_row % 64)) == 0)
                        source_row += 1;
                    c.locators[s] = .{ .rg = @intCast(i), .row = @intCast(source_row) };
                    source_row += 1;
                }
                // Slots of several parts share a word.
                if (any_null) for (part.local, 0..) |l, pi| if (l.nulls.len != 0 and bitAt(l.nulls, row)) {
                    const word = &c.j.payloads[pi].nulls.?[s >> 6];
                    _ = @atomicRmw(u64, word, .Or, @as(u64, 1) << @intCast(s & 63), .monotonic);
                };
                for (c.codes, part.local, c.plain, cursor, 0..) |dst, l, pl, *at, pi| {
                    if (pi < c.deferred.len and c.deferred[pi]) continue;
                    if (!pl.on) {
                        dst.set(s, l.globalOf(row));
                        continue;
                    }
                    switch (pl.values) {
                        .string => |values| {
                            const v = l.dict.string[row];
                            const out = pl.blob[at.*..][0..v.len];
                            @memcpy(out, v);
                            @constCast(values)[s] = out;
                            at.* += v.len;
                        },
                        inline else => |values, tag| @constCast(values)[s] = @field(l.dict, @tagName(tag))[row],
                    }
                }
            },
        }
    }
};

/// Read the build side once: decode its key, filter and payload columns per row group in parallel, keep its
/// survivors, pick the key structure from their verified domain, and lay every payload out by slot.
fn lap(what: []const u8, since: i64) i64 {
    const now = @import("../../clock.zig").monoNs();
    if (@import("builtin").os.tag == .windows) {
        std.debug.print("join build: {s} {d:.1} ms\n", .{ what, @as(f64, @floatFromInt(now - since)) / 1e6 });
        return now;
    }
    const ru = std.posix.getrusage(0);
    std.debug.print("join build: {s} {d:.1} ms (minor faults so far {d})\n", .{ what, @as(f64, @floatFromInt(now - since)) / 1e6, ru.minflt });
    return now;
}

/// The build lives in `a` (used from this thread only) as long as the query; `gpa` (thread-safe) backs each row
/// group's survivors and decode scratch until the build is laid out. Both draw from the query's ledger as they
/// allocate (`engine/join.zig`), so a build that does not fit refuses while it builds.
pub fn build(a: std.mem.Allocator, gpa: std.mem.Allocator, bytes: []const u8, meta: *const schema.FileMetaData, spec: Spec) !*Join {
    const domains = try a.alloc(KeyDomain, 1 + spec.key_more.len);
    for (domains, 0..) |*d, i| {
        const leaf = if (i == 0) spec.key else spec.key_more[i - 1];
        const element = metadata.leafSchemaElement(meta, leaf) orelse return error.JoinKeyType;
        d.* = KeyDomain.of(element) orelse return error.JoinKeyType;
    }
    const domain = domains[0];
    const parts = try a.alloc(Part, meta.row_groups.items.len);
    @memset(parts, .{});
    const arenas = try a.alloc(std.heap.ArenaAllocator, parts.len * (spec.payload.len + 1));
    for (arenas) |*ar| ar.* = .init(gpa);
    defer for (arenas) |*ar| ar.deinit();
    var workers: [max_workers]std.heap.ArenaAllocator = undefined;
    for (&workers) |*w| w.* = .init(gpa);
    defer for (&workers) |*w| w.deinit();
    defer for (parts) |*p| p.freeLarge(gpa);
    // By the row groups' leaves: a build with none decodes nothing, and its filter marks nothing.
    const leaves = if (parts.len == 0) 0 else meta.row_groups.items[0].columns.items.len;
    const filter_cols = try gpa.alloc(bool, leaves);
    defer gpa.free(filter_cols);
    @memset(filter_cols, false);
    if (leaves != 0) filter_cols[spec.key] = true;
    if (leaves != 0) for (spec.key_more) |leaf| {
        filter_cols[leaf] = true;
    };
    if (leaves != 0) if (spec.filter) |f| {
        var fc: std.ArrayList(usize) = .empty;
        defer fc.deinit(gpa);
        try f.collectColumns(&fc, gpa);
        for (fc.items) |ci| filter_cols[ci] = true;
    };
    var late: std.ArrayList(usize) = .empty;
    defer late.deinit(gpa);
    if (leaves != 0) for (spec.payload, 0..) |leaf, pi| if (!filter_cols[leaf]) try late.append(gpa, pi);
    // A column its writer kept PLAIN in every row group (no dictionary page) is one it judged not to dedupe: hashing it
    // into a dictionary would cost a probe per value and save nothing, so it is held plain.
    const plain = try gpa.alloc(bool, spec.payload.len);
    defer gpa.free(plain);
    @memset(plain, false);
    for (spec.payload, plain) |leaf, *pl| pl.* = parts.len != 0 and for (meta.row_groups.items) |*rg| {
        const cm = rg.columns.items[leaf].meta_data orelse break false;
        if (cm.dictionary_page_offset != null) break false;
        switch (cm.type) {
            .INT32, .INT64, .DOUBLE => {},
            // A filter's strings decode into the row group's scratch; only a late payload's outlive it (`LateLocal`).
            .BYTE_ARRAY => if (filter_cols[leaf]) break false,
            else => break false,
        }
    } else true;
    var ctx: Ctx = .{ .arenas = arenas, .gpa = gpa, .workers = &workers, .bytes = bytes, .meta = meta, .spec = spec, .parts = parts, .filter_cols = filter_cols, .late = late.items, .plain = plain };
    const trace = std.c.getenv("ZPQ_JOIN_TRACE") != null;
    var t = @import("../../clock.zig").monoNs();
    if (trace) t = lap("start", t);
    try parallel(parts.len, spec.threads, &ctx, struct {
        fn f(c: *Ctx, worker: usize, i: usize) anyerror!void {
            return buildPart(c, worker, i);
        }
    }.f);
    if (trace) t = lap("decode+local dictionaries", t);
    const key_cols: []const KeyColumn = if (domain.text() or domains.len > 1) try packKeys(a, &ctx, domains) else &.{};
    var n: usize = 0;
    var lo: i64 = std.math.maxInt(i64);
    var hi: i64 = std.math.minInt(i64);
    var null_key = false;
    for (parts) |p| {
        n += p.keys.len();
        lo = @min(lo, p.lo);
        hi = @max(hi, p.hi);
        null_key = null_key or p.null_key;
    }
    const j = try a.create(Join);
    j.* = .{ .kind = spec.kind, .build_name = spec.build_name, .key_name = spec.key_name, .n = n, .keys = undefined, .payloads = &.{}, .bytes = 0, .domain = domain, .null_key = null_key, .cols = key_cols };
    // Bytes per structure, from the survivors' own domain: a bit per possible key plus a rank per 64, against two
    // slots per key of an 8-byte key and a 4-byte slot.
    const span: u128 = if (n == 0) 0 else @as(u128, @intCast(@as(i128, hi) - lo)) + 1;
    const dense_bytes: u128 = (span / 64 + 1) * 12;
    const hash_cap: usize = std.math.ceilPowerOfTwoAssert(usize, @max(16, n * 2));
    const hash_bytes: u128 = @as(u128, hash_cap) * 12;
    const payload_bytes: usize = n * 4 * spec.payload.len;
    const key_bytes: usize = @intCast(@min(dense_bytes, hash_bytes));
    j.bytes = key_bytes + payload_bytes;
    if (spec.budget) |b| if (j.bytes > b) return error.JoinBuildExceedsBudget;
    try parallel(parts.len * late.items.len, spec.threads, LateLocal{ .c = &ctx }, LateLocal.run);
    if (trace) t = lap("payloads' local dictionaries", t);
    if (dense_bytes <= hash_bytes) {
        const words: usize = @intCast(span / 64 + 1);
        const d: Dense = .{ .lo = if (n == 0) 0 else lo, .bits = try a.alloc(u64, words), .rank = try a.alloc(u32, words) };
        @memset(d.bits, 0);
        var dup = std.atomic.Value(bool).init(false);
        const disjoint = try SetBits.disjointParts(gpa, parts);
        try parallel(parts.len, spec.threads, SetBits{ .parts = parts, .d = &d, .dup = &dup, .disjoint = disjoint }, SetBits.run);
        // A SEMI join keeps a probe row with any match: a key twice is still one key.
        if (dup.load(.monotonic) and spec.kind.unique()) return error.DuplicateBuildKey;
        var r: u32 = 0;
        for (d.bits, d.rank) |w, *rk| {
            rk.* = r;
            r += @popCount(w);
        }
        j.keys = .{ .dense = d };
        if (trace) t = lap("dense map", t);
    } else {
        const h: Hash = .{ .keys = try a.alloc(i64, hash_cap), .slots = try a.alloc(u32, hash_cap) };
        @memset(h.slots, EMPTY);
        const mask = hash_cap - 1;
        var next: u32 = 0;
        for (parts) |p| switch (p.keys) {
            inline else => |keys| for (keys) |raw| {
                const k = Keys.of(raw, p.lo);
                var i = h.home(k);
                const dup = while (h.slots[i] != EMPTY) : (i = (i + 1) & mask) {
                    if (h.keys[i] == k) break true;
                } else false;
                if (dup) {
                    if (spec.kind.unique()) return error.DuplicateBuildKey;
                    continue;
                }
                h.keys[i] = k;
                h.slots[i] = next;
                next += 1;
            },
        };
        j.keys = .{ .hash = h };
    }
    const has_deferred = for (spec.deferred) |d| {
        if (d) break true;
    } else false;
    const locators = try a.alloc(Payload.Locator, if (has_deferred) n else 0);
    const payloads = try a.alloc(Payload, spec.payload.len);
    const codes = try a.alloc(Codes, spec.payload.len);
    // Each payload's dictionaries unify on a task of its own (`Unify`); only the copy into `a`, which serves one thread,
    // is allocated here between the two parallel passes.
    const unified = try gpa.alloc(Unified, spec.payload.len);
    defer gpa.free(unified);
    @memset(unified, .{});
    defer for (unified) |u| u.deinit(gpa);
    const un: Unify = .{ .c = &ctx, .out = unified };
    try parallel(spec.payload.len, spec.threads, un, Unify.run);
    const fill_plain = try gpa.alloc(FillCodes.Plain, spec.payload.len);
    defer gpa.free(fill_plain);
    @memset(fill_plain, .{});
    defer for (fill_plain) |fp| gpa.free(fp.starts);
    for (spec.payload, payloads, codes, unified, plain, fill_plain, 0..) |leaf, *out, *cs, *u, is_plain, *fp, pi| {
        const element = @import("../parquet/metadata.zig").leafSchemaElement(meta, leaf).?;
        // A DECIMAL's integers: in its own lane when stored as one, else 128 bits (`decimal.decodeColumnExact`).
        const lane: schema.Type = if (decimal.kindFromSchema(&element) == null) element.type.? else switch (element.type.?) {
            .INT32, .INT64 => |physical| physical,
            else => .FIXED_LEN_BYTE_ARRAY,
        };
        if (spec.defers(pi)) {
            cs.* = .{ .identity = n };
            out.* = .{ .name = element.name, .element = element, .codes = cs.*, .dict = .{ .string = &.{} }, .deferred = .{ .scratch = gpa, .bytes = bytes, .meta = meta, .leaf = leaf, .rows = locators } };
            continue;
        }
        if (is_plain) {
            // A value per slot, the parts' strings in one blob, each part's from where the ones before it end.
            const starts = try gpa.alloc(usize, parts.len);
            var total: usize = 0;
            for (parts, starts) |part, *st| {
                st.* = total;
                total += part.local[pi].bytes;
            }
            const values: Dict = switch (lane) {
                .INT32 => .{ .i32 = try a.alloc(i32, n) },
                .INT64 => .{ .i64 = try a.alloc(i64, n) },
                .FIXED_LEN_BYTE_ARRAY => .{ .i128 = try a.alloc(i128, n) },
                .DOUBLE => .{ .f64 = try a.alloc(f64, n) },
                else => .{ .string = try a.alloc([]const u8, n) },
            };
            fp.* = .{ .on = true, .values = values, .blob = try a.alloc(u8, total), .starts = starts };
            cs.* = .{ .identity = n };
            out.* = .{ .name = element.name, .element = element, .codes = cs.*, .dict = values, .plain = true };
            continue;
        }
        // With no row group the dictionary's type comes from the column's: a LEFT miss still reads it, as NULL.
        if (parts.len == 0) u.values = switch (lane) {
            .INT32 => .{ .i32 = &.{} },
            .FIXED_LEN_BYTE_ARRAY => .{ .i128 = &.{} },
            .DOUBLE => .{ .f64 = &.{} },
            .BYTE_ARRAY => .{ .string = &.{} },
            else => .{ .i64 = &.{} },
        };
        u.dict = switch (u.values) {
            .string => |v| .{ .string = try a.alloc([]const u8, v.len) },
            inline else => |v, tag| @unionInit(Dict, @tagName(tag), try a.alloc(@TypeOf(v[0]), v.len)),
        };
        if (u.values == .string) u.blob = try a.alloc(u8, u.bytes);
        cs.* = try .init(a, n, u.values.len());
        out.* = .{ .name = element.name, .element = element, .codes = cs.*, .dict = u.dict };
    }
    try parallel(spec.payload.len, spec.threads, un, Unify.copy);
    for (payloads, 0..) |*out, pi| for (parts) |part| if (part.local[pi].nulls.len != 0) {
        out.nulls = try a.alloc(u64, n / 64 + 1);
        @memset(out.nulls.?, 0);
        break;
    };
    j.payloads = payloads;
    if (trace) t = lap("unify dictionaries", t);
    try parallel(parts.len, spec.threads, FillCodes{ .j = j, .gpa = gpa, .parts = parts, .codes = codes, .plain = fill_plain, .deferred = spec.deferred, .locators = locators }, FillCodes.run);
    if (trace) _ = lap("codes by slot", t);
    // An estimate, now that each payload's code width is known; the caller measures what `a` holds.
    j.bytes = key_bytes;
    for (payloads) |p| j.bytes += p.codes.bytes() + p.dict.len() * 16;
    return j;
}

/// Publish `inner`'s payloads through `j` (a snowflake): each slot of `j` reads the `inner` slot its payloads `fks`,
/// the FK into `inner`, name, and the codes keep `inner`'s dictionaries. Every slot has one: `inner`'s lookup in `j`'s
/// build filter kept only the rows it matches. The payloads are `j`'s after its own, `inner`'s own publishing through
/// it already, so a chain of any length publishes at its top.
pub fn chain(a: std.mem.Allocator, j: *Join, fks: []const usize, inner: *const Join) !void {
    const fk = fks[0];
    const f = j.payloads[fk];
    // Each slot's place in `inner`: for a key of one column, each FK value's, once per dictionary entry.
    const by_code = try a.alloc(u32, if (fks.len == 1) f.dict.len() else j.n);
    defer a.free(by_code);
    if (fks.len == 1) switch (f.dict) {
        inline .i32, .i64, .string => |d| for (d, by_code) |k, *s| {
            s.* = inner.slotOf(k) orelse EMPTY;
        },
        else => return error.JoinKeyType,
    } else {
        // A composite FK: its columns as the scan would publish them for every slot, looked up as a probe's are.
        var scratch: std.heap.ArenaAllocator = .init(a);
        defer scratch.deinit();
        const sa = scratch.allocator();
        const all = try selection.SelectionVector.init(sa, j.n);
        const slots = try sa.alloc(u32, j.n);
        for (slots, 0..) |*s, i| s.* = @intCast(i);
        const cols = try sa.alloc(Column, fks.len);
        for (fks, cols) |p, *col| col.* = try column(j, p, slots, &all, sa, true);
        try slotsOfColumn(inner, cols[0], cols[1..], &all, by_code, sa);
    }
    const out = try a.alloc(Payload, j.payloads.len + inner.payloads.len);
    @memcpy(out[0..j.payloads.len], j.payloads);
    for (inner.payloads, out[j.payloads.len..], 0..) |ip, *op, ip_index| {
        const codes: Codes = try .init(a, j.n, if (ip.deferred) |d| d.rows.len else ip.dict.len());
        var nulls: []u64 = &.{};
        for (0..j.n) |s| {
            const at = by_code[if (fks.len == 1) f.codes.get(@intCast(s)) else s];
            if (at == EMPTY) return error.LookupWithoutMatch;
            codes.set(@intCast(s), ip.codes.get(at));
            if (ip.isNull(at)) try markBit(a, &nulls, j.n, s);
        }
        op.* = .{ .name = ip.name, .element = ip.element, .codes = codes, .dict = ip.dict, .plain = ip.plain, .deferred = ip.deferred, .via = .{ .fk = fk, .inner = inner, .p = ip_index }, .nulls = if (nulls.len != 0) nulls else null };
    }
    j.payloads = out;
}

/// Payload `p` at the build row whose key is `key` (`expr_ast.Fetch.value`, `ctx` the join); null for no such row.
pub fn fetch(ctx: *const anyopaque, p: usize, key: i64, a: std.mem.Allocator) anyerror!?@import("../expr/ast.zig").FetchValue {
    const j: *const Join = @ptrCast(@alignCast(ctx));
    const s = j.slot(key) orelse return null;
    const payload = j.payloads[p];
    if (payload.isNull(s)) return null;
    const code = payload.codes.get(s);
    if (payload.deferred) |d| {
        var scratch: std.heap.ArenaAllocator = .init(d.scratch);
        defer scratch.deinit();
        const sa = scratch.allocator();
        const loc = d.rows[code];
        const rg = &d.meta.row_groups.items[loc.rg];
        var gate = try selection.SelectionVector.init(sa, @intCast(rg.num_rows));
        gate.clearAll();
        gate.set(loc.row, true);
        const cm = rg.columns.items[d.leaf].meta_data orelse return error.ColumnMetaMissing;
        const levels = d.meta.getColumnLevels(cm.path_in_schema.items);
        const options = column_mod.chunkOptions(.{ .fast_levels = true, .row_gate = &gate }, d.meta, cm);
        const col = try @import("../consumer/column.zig").decodeColumnTWithOptions([]const u8, sa, try chunkOf(d.bytes, cm), cm.codec, levels, gate.len, options);
        return .{ .bytes = try a.dupe(u8, col.values[loc.row]) };
    }
    return switch (payload.dict) {
        .i32 => |d| .{ .int = d[code] },
        .i64 => |d| .{ .int = d[code] },
        .i128 => unreachable, // never fetched (`engine/join.zig` `withFetches`)
        .f64 => |d| .{ .float = d[code] },
        .string => |d| .{ .bytes = d[code] },
    };
}

// ---------------------------------------------------------------- group-join

const expr_ast = @import("../expr/ast.zig");
const expr_eval = @import("../expr/eval.zig");
const output_values = @import("../scan/output_values.zig");
const AggValue = output_values.AggValue;
const ResultProgram = @import("../result_program.zig").ResultProgram;

/// GROUP BY keys that the build key determines (the FK, build columns): every group is one build slot, so the
/// aggregates fold into per-slot accumulators, with no group table. Row groups fold in their order whatever the
/// worker that read them (a reorder buffer), so float sums are the same at every `-j`. A NULL argument is skipped, as
/// SQL skips it: a slot whose rows held only NULLs answers NULL for that call.
pub const GroupJoin = struct {
    pub const Func = enum { sum, count, min, max, avg };
    pub const Lane = enum { f64, i64 };
    pub const Call = struct { func: Func, arg: ?expr_ast.Expr, lane: Lane };
    pub const Output = union(enum) { key: usize, agg: usize };

    /// Probe leaves the keys read: the FK or a virtual build column.
    keys: []const usize,
    calls: []const Call,
    outputs: []const Output,
    order: expr_ast.ResultOrder,
    /// Per slot: whether any row reached it, and how many did when a `count(*)` is asked.
    seen: []u64,
    count: []u32,
    /// Per call and slot: its accumulator in its lane (the sum, for avg); whether a non-NULL argument reached it
    /// (sum, min, max: a bit per slot); and how many did (count of a column, avg).
    f: [][]f64,
    i: [][]i64,
    has: [][]u64,
    n: [][]u32,
    /// One entry per probe row group: its partial, until every earlier row group has folded.
    pending: []std.atomic.Value(?*Partial),
    next: std.atomic.Value(usize) = .init(0),
    draining: std.atomic.Value(bool) = .init(false),
    gpa: std.mem.Allocator,
    n_slots: usize,
    overflow: std.atomic.Value(bool) = .init(false),

    fn valued(c: Call) bool {
        return c.arg != null and c.func != .count;
    }

    fn counted(c: Call) bool {
        return c.arg != null and (c.func == .count or c.func == .avg);
    }

    /// One row group's contributions, as runs of rows with one slot, in row order. `nn[call]`: the run's rows whose
    /// argument was not NULL, when any was (else every row's was not: `counts`).
    const Partial = struct {
        slots: []u32,
        counts: []u32,
        f: [][]f64,
        i: [][]i64,
        nn: [][]u32,
        fn free(p: *Partial, gpa: std.mem.Allocator) void {
            for (p.f) |x| gpa.free(x);
            for (p.i) |x| gpa.free(x);
            for (p.nn) |x| gpa.free(x);
            gpa.free(p.f);
            gpa.free(p.i);
            gpa.free(p.nn);
            gpa.free(p.slots);
            gpa.free(p.counts);
            gpa.destroy(p);
        }
    };

    /// Takes `keys`, `calls` and `outputs` (`gpa`'s) once it succeeds; a refusal (the ledger's, under a budget) frees
    /// everything it allocated and leaves them the caller's.
    pub fn init(gpa: std.mem.Allocator, j: *const Join, keys: []const usize, calls: []const Call, outputs: []const Output, row_groups: usize) !*GroupJoin {
        const n_calls = calls.len;
        const g = try gpa.create(GroupJoin);
        errdefer gpa.destroy(g);
        const seen = try gpa.alloc(u64, j.n / 64 + 1);
        errdefer gpa.free(seen);
        const count: []u32 = for (calls) |c| {
            if (c.func == .count and c.arg == null) break try gpa.alloc(u32, j.n);
        } else &.{};
        errdefer gpa.free(count);
        const f = try gpa.alloc([]f64, n_calls);
        errdefer gpa.free(f);
        @memset(f, &.{});
        errdefer for (f) |x| gpa.free(x);
        const i = try gpa.alloc([]i64, n_calls);
        errdefer gpa.free(i);
        @memset(i, &.{});
        errdefer for (i) |x| gpa.free(x);
        const has = try gpa.alloc([]u64, n_calls);
        errdefer gpa.free(has);
        @memset(has, &.{});
        errdefer for (has) |x| gpa.free(x);
        const n = try gpa.alloc([]u32, n_calls);
        errdefer gpa.free(n);
        @memset(n, &.{});
        errdefer for (n) |x| gpa.free(x);
        const pending = try gpa.alloc(std.atomic.Value(?*Partial), row_groups);
        errdefer gpa.free(pending);
        for (calls, f, i, has, n) |c, *fl, *il, *h, *nl| {
            if (valued(c) and c.lane == .f64) fl.* = try gpa.alloc(f64, j.n);
            if (valued(c) and c.lane == .i64) il.* = try gpa.alloc(i64, j.n);
            if (valued(c) and c.func != .avg) h.* = try gpa.alloc(u64, j.n / 64 + 1);
            if (counted(c)) nl.* = try gpa.alloc(u32, j.n);
        }
        for (pending) |*p| p.* = .init(null);
        g.* = .{
            .keys = keys,
            .calls = calls,
            .outputs = outputs,
            .order = .{},
            .seen = seen,
            .count = count,
            .f = f,
            .i = i,
            .has = has,
            .n = n,
            .pending = pending,
            .gpa = gpa,
            .n_slots = j.n,
        };
        g.zero();
        return g;
    }

    fn zero(g: *GroupJoin) void {
        @memset(g.seen, 0);
        @memset(g.count, 0);
        for (g.has) |h| @memset(h, 0);
        for (g.n) |n| @memset(n, 0);
    }

    /// Forget every fold: a refused scan replays from its first row group (`scan.runMultiAggregate`).
    pub fn reset(g: *GroupJoin) void {
        g.discardPending();
        g.next.store(0, .monotonic);
        g.overflow.store(false, .monotonic);
        g.zero();
    }

    fn reached(g: *const GroupJoin, s: u32) bool {
        return bitAt(g.seen, s);
    }

    /// Free every partial still waiting for an earlier row group, unapplied: what a replay or an aborted scan left.
    fn discardPending(g: *GroupJoin) void {
        for (g.pending) |*p| if (p.swap(null, .acquire)) |partial| partial.free(g.gpa);
    }

    /// After the workers have joined. A scan that failed before its rows were taken leaves later row groups' partials
    /// waiting on the one that failed; they are discarded with the rest.
    pub fn deinit(g: *GroupJoin) void {
        g.discardPending();
        const gpa = g.gpa;
        for (g.f) |x| gpa.free(x);
        for (g.i) |x| gpa.free(x);
        for (g.has) |x| gpa.free(x);
        for (g.n) |x| gpa.free(x);
        gpa.free(g.f);
        gpa.free(g.i);
        gpa.free(g.has);
        gpa.free(g.n);
        gpa.free(g.seen);
        gpa.free(g.count);
        gpa.free(g.pending);
        gpa.free(g.keys);
        gpa.free(g.calls);
        gpa.free(g.outputs);
        gpa.destroy(g);
    }

    /// Bytes per slot this group-join holds: what the build is charged on top of its keys and payloads.
    pub fn bytesFor(slots: usize, calls: []const Call) usize {
        var per: usize = 0;
        var bits: usize = 1;
        for (calls) |c| {
            if (c.func == .count and c.arg == null) per += 4;
            if (valued(c)) per += 8;
            if (valued(c) and c.func != .avg) bits += 1;
            if (counted(c)) per += 4;
        }
        // One `count(*)` array serves every count(*).
        var star = false;
        for (calls) |c| if (c.func == .count and c.arg == null) {
            if (star) per -= 4;
            star = true;
        };
        return slots * per + slots / 8 * bits;
    }

    /// One argument's values in its lane, with its NULLs.
    const Arg = struct {
        f: []const f64 = &.{},
        i: []const i64 = &.{},
        def: ?[]const u32 = null,
        max_def: u32 = 0,

        fn isNull(a: Arg, r: usize) bool {
            const d = a.def orelse return false;
            return d[r] < a.max_def;
        }
    };

    /// `c`'s argument over `batch`; for a count, its NULLs alone.
    fn argColumn(a: std.mem.Allocator, batch: *const filter_eval.Batch, lookup: []const ?usize, call: Call) !Arg {
        const lane = call.lane;
        var col = try expr_eval.evalExpr(a, batch, lookup, call.arg.?);
        if (call.func == .count) {
            const info = col.nullInfo();
            const nulls = switch (col) {
                inline else => |x| x.has_nulls,
            };
            return .{ .def = if (nulls) info.def_levels else null, .max_def = info.max_def };
        }
        // A column decoded as codes alone reads as values here.
        if (col.codes() != null) col = try @import("../expr/agg_coded.zig").materialize(a, col);
        switch (col) {
            inline .f64, .i64, .i32 => |c, tag| {
                // A coded column's values are materialized for value consumers (a build column always is).
                if (c.values.len < batch.num_rows) return error.GroupJoinCodedArgument;
                const def = if (c.has_nulls) c.def_levels else null;
                if (lane == .f64) {
                    if (tag != .f64) return error.GroupJoinArgumentType;
                    return .{ .f = c.values, .def = def, .max_def = c.max_def };
                }
                if (tag == .f64) return error.GroupJoinArgumentType;
                if (tag == .i64) return .{ .i = c.values, .def = def, .max_def = c.max_def };
                const wide = try a.alloc(i64, c.values.len);
                for (c.values, wide) |v, *w| w.* = v;
                return .{ .i = wide, .def = def, .max_def = c.max_def };
            },
            else => return error.GroupJoinArgumentType,
        }
    }

    /// Fold one probe row group's selected rows (every one matched: the lookup ran in the filter).
    /// `slots`: the rows' slots when the lookup captured them, else empty and they come from the FK.
    pub fn foldRowGroup(g: *GroupJoin, j: *const Join, a: std.mem.Allocator, rg_index: usize, batch: *const filter_eval.Batch, lookup: []const ?usize, sel: *const selection.SelectionVector, slots: []const u32) !void {
        var fold: Fold = .{ .g = g };
        errdefer fold.deinit();
        const at = if (slots.len != 0) slots else blk: {
            const fk_pos = lookup[j.fk_leaf] orelse return error.BadColumn;
            break :blk try slotsFor(j, batch.cols[fk_pos], &.{}, sel, a);
        };
        try fold.add(a, at, batch, lookup, sel);
        try fold.finish(rg_index);
    }

    /// One row group's partial, a batch of its rows at a time in row order: a run of one slot continues across
    /// batches, so a sliced row group folds to the same bits as a whole one.
    pub const Fold = struct {
        g: *GroupJoin,
        slots: std.ArrayList(u32) = .empty,
        counts: std.ArrayList(u32) = .empty,
        /// Per call: its run accumulators, in the call's lane; and its non-NULL rows per run, once a NULL showed.
        f: []std.ArrayList(f64) = &.{},
        i: []std.ArrayList(i64) = &.{},
        nn: []std.ArrayList(u32) = &.{},

        /// Rows `sel` keeps of `batch`, with their slots in `at` (indexed as `sel`).
        pub fn add(fold: *Fold, a: std.mem.Allocator, at: []const u32, batch: *const filter_eval.Batch, lookup: []const ?usize, sel: *const selection.SelectionVector) !void {
            const g = fold.g;
            const gpa = g.gpa;
            try fold.lanes();
            const args = try a.alloc(Arg, g.calls.len);
            for (g.calls, args) |c, *arg| {
                arg.* = .{};
                if (c.arg == null) continue;
                arg.* = try argColumn(a, batch, lookup, c);
            }
            // Runs of one slot, in row order: on a key-sorted probe a slot's rows are one run. Counted first, so
            // each list grows once a batch.
            var runs: usize = 0;
            var prev: u32 = if (fold.slots.items.len == 0) EMPTY else fold.slots.items[fold.slots.items.len - 1];
            var it = sel.iterator();
            while (it.next()) |r| {
                if (at[r] != prev) runs += 1;
                prev = at[r];
            }
            try fold.slots.ensureUnusedCapacity(gpa, runs);
            try fold.counts.ensureUnusedCapacity(gpa, runs);
            for (g.calls, fold.f, fold.i, args, fold.nn) |c, *f, *i, arg, *nn| {
                if (valued(c)) switch (c.lane) {
                    .f64 => try f.ensureUnusedCapacity(gpa, runs),
                    .i64 => try i.ensureUnusedCapacity(gpa, runs),
                };
                // The first NULL of this call in the row group: every run before it had no NULL.
                if (arg.def != null and nn.items.len == 0 and fold.counts.items.len != 0) try nn.appendSlice(gpa, fold.counts.items);
                if (arg.def != null or nn.items.len != 0) try nn.ensureUnusedCapacity(gpa, runs + 1);
            }
            it = sel.iterator();
            while (it.next()) |r| {
                const s = at[r];
                if (s == EMPTY) return error.LookupWithoutMatch;
                const fresh = fold.slots.items.len == 0 or fold.slots.items[fold.slots.items.len - 1] != s;
                if (fresh) {
                    fold.slots.appendAssumeCapacity(s);
                    fold.counts.appendAssumeCapacity(0);
                }
                const run = fold.counts.items.len - 1;
                // The rows of this run before this one.
                const before_row = fold.counts.items[run];
                fold.counts.items[run] += 1;
                for (g.calls, fold.f, fold.i, fold.nn, args) |c, *f, *i, *nn, arg| {
                    if (c.arg == null) continue;
                    const null_row = arg.isNull(r);
                    if (null_row and nn.items.len == 0) {
                        // Every row so far had a value; this run has `before_row` of them.
                        try nn.appendSlice(gpa, fold.counts.items);
                        nn.items[run] = before_row;
                    } else if (nn.items.len != 0 and nn.items.len < fold.counts.items.len) nn.appendAssumeCapacity(0);
                    // Whether this run already holds a value of this call.
                    const started = if (nn.items.len != 0) nn.items[run] != 0 else before_row != 0;
                    if (fresh and valued(c)) switch (c.lane) {
                        .f64 => f.appendAssumeCapacity(0),
                        .i64 => i.appendAssumeCapacity(0),
                    };
                    if (null_row) continue;
                    if (nn.items.len != 0) nn.items[run] += 1;
                    if (!valued(c)) continue;
                    switch (c.lane) {
                        .f64 => {
                            const v = arg.f[r];
                            const dst = &f.items[run];
                            dst.* = if (!started) v else switch (c.func) {
                                .sum, .avg => dst.* + v,
                                .min => @min(dst.*, v),
                                .max => @max(dst.*, v),
                                .count => unreachable,
                            };
                        },
                        .i64 => {
                            const v = arg.i[r];
                            const dst = &i.items[run];
                            dst.* = if (!started) v else switch (c.func) {
                                .sum, .avg => std.math.add(i64, dst.*, v) catch return error.GroupJoinOverflow,
                                .min => @min(dst.*, v),
                                .max => @max(dst.*, v),
                                .count => unreachable,
                            };
                        },
                    }
                }
            }
        }

        fn lanes(fold: *Fold) !void {
            const n = fold.g.calls.len;
            if (fold.f.len == n) return;
            const gpa = fold.g.gpa;
            fold.f = try gpa.alloc(std.ArrayList(f64), n);
            @memset(fold.f, .empty);
            fold.i = try gpa.alloc(std.ArrayList(i64), n);
            @memset(fold.i, .empty);
            fold.nn = try gpa.alloc(std.ArrayList(u32), n);
            @memset(fold.nn, .empty);
        }

        /// Hand the row group's partial to the ordered merge.
        pub fn finish(fold: *Fold, rg_index: usize) !void {
            const g = fold.g;
            const gpa = g.gpa;
            try fold.lanes();
            // Empty first: on a refusal the partial frees what has moved into it, and the caller's `deinit` the rest.
            const p = try gpa.create(Partial);
            p.* = .{ .slots = &.{}, .counts = &.{}, .f = &.{}, .i = &.{}, .nn = &.{} };
            errdefer p.free(gpa);
            const n = g.calls.len;
            p.f = try gpa.alloc([]f64, n);
            @memset(p.f, &.{});
            p.i = try gpa.alloc([]i64, n);
            @memset(p.i, &.{});
            p.nn = try gpa.alloc([]u32, n);
            @memset(p.nn, &.{});
            for (fold.f, fold.i, fold.nn, p.f, p.i, p.nn) |*lf, *li, *ln, *df, *di, *dn| {
                df.* = try lf.toOwnedSlice(gpa);
                di.* = try li.toOwnedSlice(gpa);
                dn.* = try ln.toOwnedSlice(gpa);
            }
            p.slots = try fold.slots.toOwnedSlice(gpa);
            p.counts = try fold.counts.toOwnedSlice(gpa);
            fold.deinit();
            g.pending[rg_index].store(p, .release);
            g.drain(false);
        }

        pub fn deinit(fold: *Fold) void {
            const gpa = fold.g.gpa;
            for (fold.f) |*x| x.deinit(gpa);
            for (fold.i) |*x| x.deinit(gpa);
            for (fold.nn) |*x| x.deinit(gpa);
            gpa.free(fold.f);
            gpa.free(fold.i);
            gpa.free(fold.nn);
            fold.f = &.{};
            fold.i = &.{};
            fold.nn = &.{};
            fold.slots.deinit(gpa);
            fold.counts.deinit(gpa);
        }
    };

    fn apply(g: *GroupJoin, p: *const Partial) void {
        for (p.slots, p.counts, 0..) |s, run_rows, at| {
            const word = s >> 6;
            const bit = @as(u64, 1) << @intCast(s & 63);
            g.seen[word] |= bit;
            if (g.count.len != 0) g.count[s] += run_rows;
            for (g.calls, 0..) |c, ci| {
                if (c.arg == null) continue;
                const values = if (p.nn[ci].len != 0) p.nn[ci][at] else run_rows;
                if (values == 0) continue;
                // Whether the slot already holds a value of this call.
                const first = if (g.has[ci].len != 0) g.has[ci][word] & bit == 0 else g.n[ci][s] == 0;
                if (g.has[ci].len != 0) g.has[ci][word] |= bit;
                if (g.n[ci].len != 0) g.n[ci][s] += values;
                if (!valued(c)) continue;
                switch (c.lane) {
                    .f64 => {
                        const dst = &g.f[ci][s];
                        const v = p.f[ci][at];
                        dst.* = if (first) v else switch (c.func) {
                            .sum, .avg => dst.* + v,
                            .min => @min(dst.*, v),
                            .max => @max(dst.*, v),
                            .count => unreachable,
                        };
                    },
                    .i64 => {
                        const dst = &g.i[ci][s];
                        const v = p.i[ci][at];
                        dst.* = if (first) v else switch (c.func) {
                            .sum, .avg => std.math.add(i64, dst.*, v) catch blk: {
                                g.overflow.store(true, .monotonic);
                                break :blk dst.*;
                            },
                            .min => @min(dst.*, v),
                            .max => @max(dst.*, v),
                            .count => unreachable,
                        };
                    },
                }
            }
        }
    }

    /// Fold every partial whose earlier row groups have all folded. Whoever holds `draining` folds; the others leave
    /// their partial for it, so no worker waits. `all` (after the scan) also passes row groups the scan never read.
    pub fn drain(g: *GroupJoin, all: bool) void {
        while (true) {
            if (g.draining.cmpxchgStrong(false, true, .acquire, .monotonic) != null) return;
            var n = g.next.load(.monotonic);
            while (n < g.pending.len) {
                const p = g.pending[n].load(.acquire) orelse {
                    if (!all) break;
                    n += 1;
                    continue;
                };
                g.apply(p);
                p.free(g.gpa);
                g.pending[n].store(null, .monotonic);
                n += 1;
            }
            g.next.store(n, .monotonic);
            g.draining.store(false, .release);
            // A partial published while this drain held the flag, at the row group it stopped at, is folded here.
            if (n >= g.pending.len or g.pending[n].load(.acquire) == null) return;
        }
    }

    fn keyOfSlot(j: *const Join, s: u32) i64 {
        switch (j.keys) {
            .dense => |d| {
                // The word whose rank range holds `s`, then the bit of that rank within it.
                var lo: usize = 0;
                var hi: usize = d.rank.len;
                while (hi - lo > 1) {
                    const mid = (lo + hi) / 2;
                    if (d.rank[mid] <= s) lo = mid else hi = mid;
                }
                var w = d.bits[lo];
                var left = s - d.rank[lo];
                while (left > 0) : (left -= 1) w &= w - 1;
                return d.lo + @as(i64, @intCast(lo * 64 + @ctz(w)));
            },
            .hash => |h| {
                for (h.slots, h.keys) |sl, k| if (sl == s) return k;
                unreachable;
            },
        }
    }

    /// Output `out` of slot `s`; a string is copied onto `gpa`, or borrowed from the build's dictionary without one.
    fn value(g: *const GroupJoin, j: *const Join, out: Output, s: u32, gpa: ?std.mem.Allocator) !AggValue {
        switch (out) {
            .key => |k| {
                const leaf = g.keys[k];
                const p = j.virtualPayload(leaf) orelse return .{ .i = keyOfSlot(j, s) };
                if (j.payloads[p].isNull(s)) return .null_val;
                const code = j.payloads[p].codes.get(s);
                if (decimal.kindFromSchema(&j.payloads[p].element)) |dk| return .{ .dec = .{
                    .v = switch (j.payloads[p].dict) {
                        inline .i32, .i64, .i128 => |d| d[code],
                        else => unreachable,
                    },
                    .spec = .{ .precision = @intCast(dk.precision), .scale = @intCast(dk.scale) },
                } };
                // An unsigned column's lane holds its bits: read as the scan reads it, zero-extended.
                const unsigned = if (KeyDomain.of(j.payloads[p].element)) |dom| dom == .unsigned else false;
                return switch (j.payloads[p].dict) {
                    .i32 => |d| .{ .i = if (unsigned) @as(u32, @bitCast(d[code])) else d[code] },
                    .i64 => |d| .{ .i = if (unsigned) @as(u64, @bitCast(d[code])) else d[code] },
                    .i128 => unreachable,
                    .f64 => |d| .{ .f = d[code] },
                    .string => |d| .{ .s = if (gpa) |a| try a.dupe(u8, d[code]) else d[code] },
                };
            },
            .agg => |ci| {
                const c = g.calls[ci];
                switch (c.func) {
                    .count => return .{ .i = if (c.arg == null) g.count[s] else g.n[ci][s] },
                    .avg => {
                        const n = g.n[ci][s];
                        if (n == 0) return .null_val;
                        const sum: f64 = switch (c.lane) {
                            .f64 => g.f[ci][s],
                            .i64 => @floatFromInt(g.i[ci][s]),
                        };
                        return .{ .f = sum / @as(f64, @floatFromInt(n)) };
                    },
                    .sum, .min, .max => {
                        if (!bitAt(g.has[ci], s)) return .null_val;
                        return switch (c.lane) {
                            .f64 => .{ .f = g.f[ci][s] },
                            .i64 => .{ .i = g.i[ci][s] },
                        };
                    },
                }
            },
        }
    }

    fn before(g: *const GroupJoin, j: *const Join, x: u32, y: u32) bool {
        for (g.order.keys) |key| {
            const a = g.value(j, g.outputs[key.column], x, null) catch return x < y;
            const b = g.value(j, g.outputs[key.column], y, null) catch return x < y;
            switch (output_values.orderByKey(a, b, key)) {
                .lt => return true,
                .gt => return false,
                .eq => {},
            }
        }
        return x < y;
    }

    /// Whether slot `s` is a row of the answer: a row reached it, and HAVING (which the result program applies again)
    /// makes it TRUE.
    fn kept(g: *const GroupJoin, j: *const Join, s: u32) bool {
        if (!g.reached(s)) return false;
        const h = g.order.having orelse return true;
        const Slot = struct {
            g: *const GroupJoin,
            j: *const Join,
            s: u32,
            pub fn at(self: @This(), c: usize) AggValue {
                // Borrowed: nothing here allocates.
                return self.g.value(self.j, self.g.outputs[c], self.s, null) catch unreachable;
            }
        };
        return ResultProgram.truth(h.*, Slot{ .g = g, .j = j, .s = s }) == true;
    }

    /// The answer's rows, one per slot `kept` keeps: in slot order, or, under ORDER BY ... LIMIT, the first
    /// `offset + limit` by the order (ties by slot), which the result program then cuts as it would the full answer.
    pub fn rows(g: *GroupJoin, j: *const Join, gpa: std.mem.Allocator) ![]const []const AggValue {
        g.drain(true);
        if (g.overflow.load(.monotonic)) return error.GroupJoinOverflow;
        var chosen: std.ArrayList(u32) = .empty;
        defer chosen.deinit(gpa);
        const keep: ?usize = if (g.order.limit) |l| if (g.order.keys.len > 0) @intCast(g.order.offset +| l) else null else null;
        if (keep) |k| {
            // A sorted buffer of the best `k`: on this shape `k` is small, and each insert shifts at most `k`.
            for (0..g.n_slots) |si| {
                const s: u32 = @intCast(si);
                if (!g.kept(j, s)) continue;
                if (chosen.items.len == k and !g.before(j, s, chosen.items[k - 1])) continue;
                var pos = chosen.items.len;
                while (pos > 0 and g.before(j, s, chosen.items[pos - 1])) pos -= 1;
                if (chosen.items.len == k) _ = chosen.pop();
                try chosen.insert(gpa, pos, s);
            }
        } else for (0..g.n_slots) |si| if (g.kept(j, @intCast(si))) try chosen.append(gpa, @intCast(si));
        const out = try gpa.alloc([]const AggValue, chosen.items.len);
        for (chosen.items, out) |s, *row| {
            const vals = try gpa.alloc(AggValue, g.outputs.len);
            for (g.outputs, vals) |o, *v| v.* = try g.value(j, o, s, gpa);
            row.* = vals;
        }
        return out;
    }
};

test "plain dense join narrows and captures NULLs, misses, prior selections and a short last word" {
    const a = std.testing.allocator;
    var bits = [_]u64{0b100101};
    var rank = [_]u32{0};
    inline for (.{ i32, i64 }) |T| {
        var vals: [73]T = undefined;
        var defs: [73]u32 = undefined;
        for (&vals, &defs, 0..) |*v, *d, i| {
            v.* = @as(T, @intCast(i % 9)) - 1;
            d.* = @intFromBool(i % 5 != 0);
        }
        const tag = if (T == i32) "i32" else "i64";
        const fk: filter_eval.Batch.Column = @unionInit(filter_eval.Batch.Column, tag, .{
            .values = &vals,
            .def_levels = &defs,
            .max_def = 1,
        });
        for ([_]Kind{ .inner, .semi }) |kind| for ([_]bool{ false, true }) |capturing| {
            const j: Join = .{
                .kind = kind,
                .build_name = "dim",
                .key_name = "pk",
                .n = 3,
                .keys = .{ .dense = .{ .lo = 0, .bits = &bits, .rank = &rank } },
                .payloads = &.{},
                .bytes = 0,
            };
            var sel = try selection.SelectionVector.init(a, vals.len);
            defer sel.deinit();
            for (0..vals.len) |i| if (i % 7 == 0) sel.set(i, false);
            var at: [73]u32 = @splat(12345);
            try narrow(&j, fk, &.{}, &sel, a, if (capturing) &at else null);
            for (vals, defs, 0..) |v, d, i| {
                const slot: u32 = if (v == 0) 0 else if (v == 2) 1 else if (v == 5) 2 else EMPTY;
                const wanted = if (d == 0) EMPTY else slot;
                try std.testing.expectEqual(i % 7 != 0 and wanted != EMPTY, sel.isActive(i));
                if (capturing) try std.testing.expectEqual(if (i % 7 == 0) @as(u32, 12345) else wanted, at[i]);
            }
        };
    }
}

test "a composite key packs its columns' places; a NULL, a value out of range or an unknown string matches nothing" {
    const a = std.testing.allocator;
    // The build's keys (10, "a"), (12, "b") and (14, "c"): an integer in [10, 15) weighing 1, a string weighing 5.
    const values = [_][]const u8{ "a", "b", "c" };
    var table: [16]u32 = @splat(EMPTY);
    const strings: Strings = .{ .values = &values, .table = &table };
    for (0..values.len) |n| strings.put(@intCast(n));
    const cols = [_]KeyColumn{
        .{ .domain = .signed, .lo = 10, .span = 5 },
        .{ .domain = .string, .strings = strings, .span = 3, .weight = 5 },
    };
    var bits = [_]u64{(1 << 0) | (1 << 7) | (1 << 14)};
    var rank = [_]u32{0};
    const ints = [_]i64{ 10, 12, 14, 12, 9, 15, 10, 12, 10 };
    const defs = [_]u32{ 1, 1, 1, 1, 1, 1, 0, 1, 1 };
    const dict = [_][]const u8{ "b", "a", "zz", "c" };
    const codes = [_]u32{ 1, 0, 3, 3, 1, 1, 1, 0, 2 };
    const texts = [_][]const u8{ "a", "b", "c", "c", "a", "a", "a", "b", "zz" };
    const fk: Column = .{ .i64 = .{ .values = &ints, .def_levels = &defs, .max_def = 1 } };
    // Rows 3 (12, "c": no such pair), 4 and 5 (out of range), 6 (NULL) and 8 (no such string) have no slot.
    const wanted = [_]u32{ 0, 1, 2, EMPTY, EMPTY, EMPTY, EMPTY, 1, EMPTY };
    const coded: Column = .{ .string = .{ .values = &.{}, .codes = &codes, .dict = &dict } };
    for ([_]Column{ coded, .{ .string = .{ .values = &texts } } }) |more| {
        for ([_]Kind{ .inner, .left }) |kind| {
            const j: Join = .{
                .kind = kind,
                .build_name = "dim",
                .key_name = "pk",
                .n = 3,
                .keys = .{ .dense = .{ .lo = 0, .bits = &bits, .rank = &rank } },
                .cols = &cols,
                .payloads = &.{},
                .bytes = 0,
            };
            var sel = try selection.SelectionVector.init(a, ints.len);
            defer sel.deinit();
            var at: [9]u32 = @splat(12345);
            try narrow(&j, fk, &.{more}, &sel, a, &at);
            try std.testing.expectEqualSlices(u32, &wanted, &at);
            for (wanted, 0..) |slot, r| try std.testing.expectEqual(kind == .left or slot != EMPTY, sel.isActive(r));
        }
    }
}

test "an aborted scan frees the later row groups' partials waiting on it (review R10)" {
    const a = std.testing.allocator;
    const j: Join = .{ .kind = .inner, .build_name = "dimension", .key_name = "pk", .n = 1, .keys = undefined, .payloads = &.{}, .bytes = 0 };
    const keys = try a.alloc(usize, 1);
    keys[0] = 0;
    const calls = try a.alloc(GroupJoin.Call, 1);
    calls[0] = .{ .func = .count, .arg = null, .lane = .i64 };
    const outputs = try a.alloc(GroupJoin.Output, 2);
    outputs[0] = .{ .key = 0 };
    outputs[1] = .{ .agg = 0 };
    const g = try GroupJoin.init(a, &j, keys, calls, outputs, 2);
    // Row group 1 folds and waits on row group 0, which fails: the query releases the group-join without a drain.
    var fold: GroupJoin.Fold = .{ .g = g };
    errdefer fold.deinit();
    try fold.slots.append(a, 0);
    try fold.counts.append(a, 1);
    try fold.finish(1);
    try std.testing.expect(g.pending[1].load(.acquire) != null);
    g.deinit();
}

test "the group-join's cut keeps a NULL group first under NULLS FIRST (review R12)" {
    const a = std.testing.allocator;
    var bits = [_]u64{7};
    var rank = [_]u32{0};
    const j: Join = .{
        .kind = .inner,
        .build_name = "dim",
        .key_name = "pk",
        .n = 3,
        .keys = .{ .dense = .{ .lo = 0, .bits = &bits, .rank = &rank } },
        .payloads = &.{},
        .bytes = 0,
    };
    const keys = try a.alloc(usize, 1);
    keys[0] = 0;
    const calls = try a.alloc(GroupJoin.Call, 1);
    calls[0] = .{ .func = .sum, .arg = .{ .literal = .{ .f64 = 1.0 } }, .lane = .f64 };
    const outputs = try a.alloc(GroupJoin.Output, 2);
    outputs[0] = .{ .key = 0 };
    outputs[1] = .{ .agg = 0 };
    const g = try GroupJoin.init(a, &j, keys, calls, outputs, 0);
    defer g.deinit();
    // Slot 0's rows held only NULLs; slots 1 and 2 sum to 1 and 2.
    g.seen[0] = 7;
    g.has[0][0] = 6;
    g.f[0][1] = 1;
    g.f[0][2] = 2;
    g.order = .{ .keys = &.{.{ .column = 1, .descending = true, .nulls_first = true }}, .limit = 1 };
    const rows = try g.rows(&j, a);
    defer a.free(rows);
    defer for (rows) |row| a.free(row);
    try std.testing.expectEqual(@as(usize, 1), rows.len);
    try std.testing.expectEqual(@as(i128, 0), rows[0][0].i);
}

test "dense bits: disjoint parts set their own words plainly and still see a duplicate key" {
    const a = std.testing.allocator;
    var k0: [200]i64 = undefined;
    for (&k0, 0..) |*k, i| k.* = @intCast(i);
    var k1: [201]i64 = undefined;
    for (k1[0..200], 0..) |*k, i| k.* = @intCast(200 + i);
    k1[200] = 300;
    const parts = [_]Part{ .{ .keys = .{ .wide = &k1 }, .lo = 200, .hi = 399 }, .{ .keys = .{ .wide = &k0 }, .lo = 0, .hi = 199 } };
    try std.testing.expect(try SetBits.disjointParts(a, &parts));
    var bits: [7]u64 = @splat(0);
    var rank: [7]u32 = @splat(0);
    const d: Dense = .{ .lo = 0, .bits = &bits, .rank = &rank };
    var dup = std.atomic.Value(bool).init(false);
    const c: SetBits = .{ .parts = &parts, .d = &d, .dup = &dup, .disjoint = true };
    try c.run(0, 0);
    try std.testing.expect(dup.load(.monotonic));
    dup.store(false, .monotonic);
    try c.run(0, 1);
    try std.testing.expect(!dup.load(.monotonic));
    var set: usize = 0;
    for (bits) |w| set += @popCount(w);
    try std.testing.expectEqual(@as(usize, 400), set);
    const overlapping = [_]Part{ .{ .keys = .{ .wide = &k0 }, .lo = 0, .hi = 199 }, .{ .keys = .{ .wide = &k1 }, .lo = 199, .hi = 399 } };
    try std.testing.expect(!try SetBits.disjointParts(a, &overlapping));
}

/// Fetch one output column together so each selected build page is decoded once per cut, even when
/// several surviving groups name rows on it. Scratch values are copied into the answer by the caller.
pub fn fetchBatch(ctx: *const anyopaque, p: usize, keys: []const ?i64, a: std.mem.Allocator) anyerror![]?expr_ast.FetchValue {
    const j: *const Join = @ptrCast(@alignCast(ctx));
    const out = try a.alloc(?expr_ast.FetchValue, keys.len);
    @memset(out, null);
    if (j.payloads[p].deferred == null) {
        for (keys, out) |key, *v| if (key) |k| {
            v.* = try fetch(ctx, p, k, a);
        };
        return out;
    }
    const slots = try a.alloc(u32, keys.len);
    var sel = try selection.SelectionVector.init(a, keys.len);
    for (keys, slots, 0..) |key, *slot, i| {
        slot.* = if (key) |k| j.slot(k) orelse EMPTY else EMPTY;
        if (slot.* == EMPTY or j.payloads[p].isNull(slot.*)) sel.set(i, false);
    }
    const col = try column(j, p, slots, &sel, a, true);
    var it = sel.iterator();
    while (it.next()) |i| out[i] = .{ .bytes = col.string.values[i] };
    return out;
}
