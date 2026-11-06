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
//! downstream depends on `-j`.
const std = @import("std");
const schema = @import("../schema.zig");
const filter_ast = @import("../filter/ast.zig");
const filter_eval = @import("../filter/eval.zig");
const selection = @import("../filter/selection.zig");
const column_mod = @import("../parquet/column.zig");
const metadata = @import("../parquet/metadata.zig");

pub const Kind = enum { inner, semi };

/// A build column's distinct values, in the order its codes number them (first appearance in (row group, row)).
pub const Dict = union(enum) {
    i32: []const i32,
    i64: []const i64,
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
};

/// Per-slot codes, two bytes each when the dictionary has fewer than 2^16 values.
pub const Codes = union(enum) {
    u16: []u16,
    u32: []u32,

    pub fn get(c: Codes, s: u32) u32 {
        return switch (c) {
            inline else => |v| v[s],
        };
    }

    fn set(c: Codes, s: u32, code: u32) void {
        switch (c) {
            .u16 => |v| v[s] = @intCast(code),
            .u32 => |v| v[s] = code,
        }
    }

    fn bytes(c: Codes) usize {
        return switch (c) {
            inline else => |v| v.len * @sizeOf(@TypeOf(v[0])),
        };
    }
};

const EMPTY: u32 = std.math.maxInt(u32);

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

/// The SQL domain a key's stored integers compare in. Two keys join only within one: the same integers mean the same
/// value there, where a timestamp in another unit, an unsigned value over a signed one or a DECIMAL of another scale
/// would match integers that are not equal values (or miss ones that are). A DECIMAL key is not in this release: the
/// build reads a DECIMAL as its f64 view.
pub const KeyDomain = union(enum) {
    /// Plain or signed integers, of any width (they widen to i64 exactly).
    signed,
    /// Unsigned integers, stored in this physical type: an INT32 and an INT64 differ above 2^31.
    unsigned: schema.Type,
    date,
    timestamp: struct { unit: std.meta.Tag(schema.TimeUnit), utc: bool },
    time: struct { unit: std.meta.Tag(schema.TimeUnit), utc: bool },

    /// Null for a key no lookup takes (not stored as INT32 or INT64, or a logical type with no integer equality).
    pub fn of(e: schema.SchemaElement) ?KeyDomain {
        const t = e.type orelse return null;
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
    payloads: []const Payload,
    /// Bytes the build holds for the query (charged against `--max-memory`).
    bytes: usize,
    /// The probe's real leaf count: virtual leaf `p` is `virtual_base + p` (`extend`).
    virtual_base: usize = 0,
    /// The probe's own join, whose build columns the scan publishes and whose group-join it folds; false for a SEMI
    /// lookup beside it or inside a build.
    main: bool = false,
    /// The probe's foreign key leaf, set when the probe query binds.
    fk_leaf: usize = 0,
    /// Set when the query's GROUP BY is one the build key determines (`GroupJoin`).
    group: ?*GroupJoin = null,

    pub fn slot(j: *const Join, k: i64) ?u32 {
        return switch (j.keys) {
            inline else => |*s| s.slot(k),
        };
    }

    /// The payload a probe leaf index stands for, or null for a real leaf.
    pub fn virtualPayload(j: *const Join, ci: usize) ?usize {
        if (ci < j.virtual_base or ci >= j.virtual_base + j.payloads.len) return null;
        return ci - j.virtual_base;
    }
};

/// Extend a probe footer with one top-level REQUIRED leaf per build column, after its own leaves: what the query
/// binds the build columns to. Row groups keep their own chunks only, so every decode path sees a virtual leaf as a
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
        e.repetition_type = .REQUIRED;
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
        for (c.dict, by_code) |k, *s| s.* = j.slot(@intCast(k)) orelse EMPTY;
        var it = sel.iterator();
        while (it.next()) |r| {
            const is_null = if (c.def_levels) |dl| dl[r] < c.max_def else false;
            out[r] = if (is_null) EMPTY else by_code[codes[r]];
        }
        return;
    }
    var it = sel.iterator();
    while (it.next()) |r| out[r] = if (keyAt(T, c, r)) |k| j.slot(k) orelse EMPTY else EMPTY;
}

fn slotsOfColumn(j: *const Join, fk: filter_eval.Batch.Column, sel: *const selection.SelectionVector, out: []u32, a: std.mem.Allocator) filter_eval.Error!void {
    return switch (fk) {
        .i32 => |c| slotsOf(j, i32, c, sel, out, a),
        .i64 => |c| slotsOf(j, i64, c, sel, out, a),
        else => error.BadColumn,
    };
}

/// The slots of `fk`'s selected rows, on `a`.
pub fn slotsFor(j: *const Join, fk: filter_eval.Batch.Column, sel: *const selection.SelectionVector, a: std.mem.Allocator) filter_eval.Error![]u32 {
    const out = try a.alloc(u32, sel.len);
    try slotsOfColumn(j, fk, sel, out, a);
    return out;
}

/// Keep the selected rows whose foreign key has a build match (INNER and SEMI alike: the build is N:1). With
/// `capture` (one entry per row of `sel`), the selected rows' slots stay there for the columns published later.
pub fn narrow(j: *const Join, fk: filter_eval.Batch.Column, sel: *selection.SelectionVector, a: std.mem.Allocator, capture: ?[]u32) filter_eval.Error!void {
    const at = capture orelse try a.alloc(u32, sel.len);
    defer if (capture == null) a.free(at);
    try slotsOfColumn(j, fk, sel, at, a);
    for (sel.mask, 0..) |*w, wi| {
        var bits = w.*;
        while (bits != 0) : (bits &= bits - 1) {
            const r = wi * 64 + @ctz(bits);
            if (at[r] == EMPTY) w.* &= ~(@as(u64, 1) << @intCast(r & 63));
        }
    }
}

/// Payload `p` as a coded column over the rows `sel` keeps, from their slots (every one has a match: the lookup
/// conjunct ran first). Rows outside `sel` take code 0; no consumer reads them. With `values`, values are
/// materialized beside the codes for the consumers that read values.
pub fn column(j: *const Join, p: usize, at: []const u32, sel: *const selection.SelectionVector, a: std.mem.Allocator, values: bool) !filter_eval.Batch.Column {
    const payload = j.payloads[p];
    const codes = try a.alloc(u32, sel.len);
    @memset(codes, 0);
    var it = sel.iterator();
    while (it.next()) |r| {
        if (at[r] == EMPTY) return error.LookupWithoutMatch;
        codes[r] = payload.codes.get(at[r]);
    }
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
            break :blk @unionInit(filter_eval.Batch.Column, @tagName(tag), .{ .values = vals, .codes = codes, .dict = dict });
        },
    };
}

// ---------------------------------------------------------------- build

pub const Spec = struct {
    kind: Kind,
    build_name: []const u8,
    key_name: []const u8,
    /// Build leaf indices.
    key: usize,
    payload: []const usize,
    filter: ?filter_ast.Filter,
    threads: usize,
    /// Bytes the build may hold; null when the query has no `--max-memory`.
    budget: ?usize,
};

/// One build row group's survivors: their keys, and per payload a local dictionary and each survivor's local code.
const Part = struct {
    keys: []i64 = &.{},
    local: []Local = &.{},
    lo: i64 = std.math.maxInt(i64),
    hi: i64 = std.math.minInt(i64),
};

const Local = struct {
    codes: []u32,
    dict: Dict,
    /// Local code -> global code, filled when the dictionaries unify.
    global: []u32 = &.{},
};

pub const BuildError = error{ DuplicateBuildKey, JoinKeyType, JoinPayloadType, JoinBuildExceedsBudget, OutOfMemory };

const Ctx = struct {
    /// One per build row group: its survivors, until the build is laid out.
    arenas: []std.heap.ArenaAllocator,
    /// What each row group's decode scratch draws from.
    scratch: std.mem.Allocator,
    bytes: []const u8,
    meta: *const schema.FileMetaData,
    spec: Spec,
    parts: []Part,
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
    for (rows, codes) |r, *code| {
        if (c.def_levels) |dl| if (dl[r] < c.max_def) return error.JoinPayloadType; // NULL payloads: not in this prototype
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
        f64 => .{ .f64 = dict.items },
        []const u8 => .{ .string = dict.items },
        else => unreachable,
    };
    return .{ .codes = codes, .dict = d };
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
    if (coded.has_nulls) return null;
    const codes = try a.alloc(u32, keep.len);
    for (keep, codes) |row, *code| code.* = coded.codes[row];
    const d: Dict = switch (T) {
        i32 => .{ .i32 = coded.dict },
        i64 => .{ .i64 = coded.dict },
        f64 => .{ .f64 = coded.dict },
        []const u8 => .{ .string = coded.dict },
        else => unreachable,
    };
    return .{ .codes = codes, .dict = d };
}

fn buildPart(c: *Ctx, rg_index: usize) !void {
    const a = c.arenas[rg_index].allocator();
    var scratch = std.heap.ArenaAllocator.init(c.scratch);
    defer scratch.deinit();
    const sa = scratch.allocator();
    const rg = &c.meta.row_groups.items[rg_index];
    const rows: usize = @intCast(rg.num_rows);
    const n_leaves = rg.columns.items.len;
    var cols: std.ArrayList(filter_eval.Batch.Column) = .empty;
    const lookup = try sa.alloc(?usize, c.meta.schema.items.len);
    @memset(lookup, null);
    var need = try sa.alloc(bool, n_leaves);
    @memset(need, false);
    need[c.spec.key] = true;
    for (c.spec.payload) |p| need[p] = true;
    if (c.spec.filter) |f| {
        var fc: std.ArrayList(usize) = .empty;
        try f.collectColumns(&fc, sa);
        for (fc.items) |ci| need[ci] = true;
    }
    // Payload columns no filter reads decode after it, and as codes when their chunk allows.
    const filter_cols = try sa.alloc(bool, n_leaves);
    @memset(filter_cols, false);
    filter_cols[c.spec.key] = true;
    if (c.spec.filter) |f| {
        var fc: std.ArrayList(usize) = .empty;
        try f.collectColumns(&fc, sa);
        for (fc.items) |ci| filter_cols[ci] = true;
    }
    for (need, 0..) |n, ci| if (n and filter_cols[ci]) {
        lookup[ci] = cols.items.len;
        try cols.append(sa, try decodeValues(sa, c.bytes, c.meta, rg, ci));
    };
    var sel = try selection.SelectionVector.init(sa, rows);
    if (c.spec.filter) |f| {
        const batch: filter_eval.Batch = .{ .cols = cols.items, .num_rows = rows };
        try filter_eval.evaluate(f, &batch, &sel, lookup, sa);
    }
    const key_col = cols.items[lookup[c.spec.key].?];
    var keep: std.ArrayList(u32) = .empty;
    var keys: std.ArrayList(i64) = .empty;
    var part: Part = .{};
    var it = sel.iterator();
    while (it.next()) |r| {
        const k: ?i64 = switch (key_col) {
            .i32 => |kc| keyAt(i32, kc, r),
            .i64 => |kc| keyAt(i64, kc, r),
            else => return error.JoinKeyType,
        };
        // A NULL build key matches no probe row, NULL ones included.
        const key = k orelse continue;
        try keep.append(sa, @intCast(r));
        try keys.append(sa, key);
        part.lo = @min(part.lo, key);
        part.hi = @max(part.hi, key);
    }
    // Survivors' keys and local dictionaries outlive the scratch.
    const locals = try sa.alloc(Local, c.spec.payload.len);
    for (c.spec.payload, locals) |p, *l| {
        const cm = rg.columns.items[p].meta_data orelse return error.ColumnMetaMissing;
        if (!filter_cols[p]) if (switch (cm.type) {
            .INT32 => try codedLocal(i32, sa, c.bytes, c.meta, rg, p, keep.items),
            .INT64 => try codedLocal(i64, sa, c.bytes, c.meta, rg, p, keep.items),
            .DOUBLE => try codedLocal(f64, sa, c.bytes, c.meta, rg, p, keep.items),
            .BYTE_ARRAY => try codedLocal([]const u8, sa, c.bytes, c.meta, rg, p, keep.items),
            else => null,
        }) |coded| {
            l.* = coded;
            continue;
        };
        if (lookup[p] == null) {
            lookup[p] = cols.items.len;
            try cols.append(sa, try decodeValues(sa, c.bytes, c.meta, rg, p));
        }
        l.* = switch (cols.items[lookup[p].?]) {
            .i32 => |pc| try localDict(i32, sa, pc, keep.items),
            .i64 => |pc| try localDict(i64, sa, pc, keep.items),
            .f64 => |pc| try localDict(f64, sa, pc, keep.items),
            .string => |pc| try localDict([]const u8, sa, pc, keep.items),
            else => return error.JoinPayloadType,
        };
    }
    part.keys = try a.dupe(i64, keys.items);
    for (locals) |*l| {
        l.codes = try a.dupe(u32, l.codes);
        l.dict = switch (l.dict) {
            .string => |d| blk: {
                const out = try a.alloc([]const u8, d.len);
                for (d, out) |s, *o| o.* = try a.dupe(u8, s);
                break :blk .{ .string = out };
            },
            inline else => |d, tag| @unionInit(Dict, @tagName(tag), try a.dupe(@TypeOf(d[0]), d)),
        };
    }
    part.local = try a.dupe(Local, locals);
    c.parts[rg_index] = part;
}

fn parallel(n: usize, threads: usize, ctx: anytype, comptime f: fn (@TypeOf(ctx), usize) anyerror!void) !void {
    const C = @TypeOf(ctx);
    const W = struct {
        fn run(c: C, cursor: *std.atomic.Value(usize), total: usize, errs: []?anyerror) void {
            while (true) {
                const i = cursor.fetchAdd(1, .monotonic);
                if (i >= total) return;
                f(c, i) catch |e| {
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
    var pool: [64]std.Thread = undefined;
    const t = @max(1, @min(threads, pool.len, n));
    var started: usize = 0;
    while (started < t) : (started += 1) {
        pool[started] = std.Thread.spawn(.{}, W.run, .{ ctx, &cursor, n, errs }) catch |e| {
            cursor.store(n, .monotonic);
            for (pool[0..started]) |th| th.join();
            return e;
        };
    }
    for (pool[0..started]) |th| th.join();
    // The lowest failing row group's error, whatever the scheduling.
    for (errs) |e| if (e) |err| return err;
}

/// Unify payload `p`'s local dictionaries in row-group order: global codes number values by first appearance.
fn unify(comptime T: type, a: std.mem.Allocator, parts: []Part, p: usize) !Dict {
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
    for (parts) |*part| {
        const l = &part.local[p];
        const local = @field(l.dict, switch (T) {
            i32 => "i32",
            i64 => "i64",
            f64 => "f64",
            else => "string",
        });
        l.global = try a.alloc(u32, local.len);
        for (local, l.global) |v, *g| {
            const gop = try map.getOrPut(a, v);
            if (!gop.found_existing) {
                const owned = if (T == []const u8) try a.dupe(u8, v) else v;
                gop.key_ptr.* = owned;
                gop.value_ptr.* = @intCast(dict.items.len);
                try dict.append(a, owned);
            }
            g.* = gop.value_ptr.*;
        }
    }
    return switch (T) {
        i32 => .{ .i32 = dict.items },
        i64 => .{ .i64 = dict.items },
        f64 => .{ .f64 = dict.items },
        else => .{ .string = dict.items },
    };
}

const SetBits = struct {
    parts: []const Part,
    d: *const Dense,
    dup: *std.atomic.Value(bool),
    fn run(c: @This(), i: usize) !void {
        for (c.parts[i].keys) |k| {
            const at: u64 = @bitCast(k -% c.d.lo);
            const bit = @as(u64, 1) << @intCast(at & 63);
            const old = @atomicRmw(u64, &c.d.bits[@intCast(at >> 6)], .Or, bit, .monotonic);
            if (old & bit != 0) c.dup.store(true, .monotonic);
        }
    }
};

const FillCodes = struct {
    j: *const Join,
    parts: []const Part,
    codes: []const Codes,
    fn run(c: @This(), i: usize) !void {
        const part = c.parts[i];
        for (part.keys, 0..) |k, row| {
            const s = c.j.slot(k).?;
            for (c.codes, part.local) |dst, l| dst.set(s, l.global[l.codes[row]]);
        }
    }
};

/// Read the build side once: decode its key, filter and payload columns per row group in parallel, keep its
/// survivors, pick the key structure from their verified domain, and lay every payload out by slot.
fn lap(what: []const u8, since: i64) i64 {
    const now = @import("../../clock.zig").monoNs();
    std.debug.print("join build: {s} {d:.1} ms\n", .{ what, @as(f64, @floatFromInt(now - since)) / 1e6 });
    return now;
}

/// The build lives in `a` (used from this thread only) as long as the query; `gpa` (thread-safe) backs each row
/// group's survivors and decode scratch until the build is laid out. Both draw from the query's ledger as they
/// allocate (`engine/join.zig`), so a build that does not fit refuses while it builds.
pub fn build(a: std.mem.Allocator, gpa: std.mem.Allocator, bytes: []const u8, meta: *const schema.FileMetaData, spec: Spec) !*Join {
    const domain = KeyDomain.of(metadata.leafSchemaElement(meta, spec.key) orelse return error.JoinKeyType) orelse
        return error.JoinKeyType;
    const parts = try a.alloc(Part, meta.row_groups.items.len);
    @memset(parts, .{});
    const arenas = try a.alloc(std.heap.ArenaAllocator, parts.len);
    for (arenas) |*ar| ar.* = .init(gpa);
    defer for (arenas) |*ar| ar.deinit();
    var ctx: Ctx = .{ .arenas = arenas, .scratch = gpa, .bytes = bytes, .meta = meta, .spec = spec, .parts = parts };
    const trace = std.c.getenv("ZPQ_JOIN_TRACE") != null;
    var t = @import("../../clock.zig").monoNs();
    try parallel(parts.len, spec.threads, &ctx, struct {
        fn f(c: *Ctx, i: usize) anyerror!void {
            return buildPart(c, i);
        }
    }.f);
    if (trace) t = lap("decode+local dictionaries", t);
    var n: usize = 0;
    var lo: i64 = std.math.maxInt(i64);
    var hi: i64 = std.math.minInt(i64);
    for (parts) |p| {
        n += p.keys.len;
        lo = @min(lo, p.lo);
        hi = @max(hi, p.hi);
    }
    const j = try a.create(Join);
    j.* = .{ .kind = spec.kind, .build_name = spec.build_name, .key_name = spec.key_name, .n = n, .keys = undefined, .payloads = &.{}, .bytes = 0, .domain = domain };
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
    if (dense_bytes <= hash_bytes) {
        const words: usize = @intCast(span / 64 + 1);
        const d: Dense = .{ .lo = if (n == 0) 0 else lo, .bits = try a.alloc(u64, words), .rank = try a.alloc(u32, words) };
        @memset(d.bits, 0);
        var dup = std.atomic.Value(bool).init(false);
        try parallel(parts.len, spec.threads, SetBits{ .parts = parts, .d = &d, .dup = &dup }, SetBits.run);
        // A SEMI join keeps a probe row with any match: a key twice is still one key.
        if (dup.load(.monotonic) and spec.kind == .inner) return error.DuplicateBuildKey;
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
        for (parts) |p| for (p.keys) |k| {
            var i = h.home(k);
            const dup = while (h.slots[i] != EMPTY) : (i = (i + 1) & mask) {
                if (h.keys[i] == k) break true;
            } else false;
            if (dup) {
                if (spec.kind == .inner) return error.DuplicateBuildKey;
                continue;
            }
            h.keys[i] = k;
            h.slots[i] = next;
            next += 1;
        };
        j.keys = .{ .hash = h };
    }
    const payloads = try a.alloc(Payload, spec.payload.len);
    const codes = try a.alloc(Codes, spec.payload.len);
    for (spec.payload, payloads, codes, 0..) |leaf, *out, *cs, p| {
        const element = @import("../parquet/metadata.zig").leafSchemaElement(meta, leaf).?;
        const dict: Dict = if (parts.len == 0) .{ .i64 = &.{} } else switch (parts[0].local[p].dict) {
            .i32 => try unify(i32, a, parts, p),
            .i64 => try unify(i64, a, parts, p),
            .f64 => try unify(f64, a, parts, p),
            .string => try unify([]const u8, a, parts, p),
        };
        cs.* = if (dict.len() <= std.math.maxInt(u16)) .{ .u16 = try a.alloc(u16, n) } else .{ .u32 = try a.alloc(u32, n) };
        out.* = .{ .name = element.name, .element = element, .codes = cs.*, .dict = dict };
    }
    j.payloads = payloads;
    if (trace) t = lap("unify dictionaries", t);
    try parallel(parts.len, spec.threads, FillCodes{ .j = j, .parts = parts, .codes = codes }, FillCodes.run);
    if (trace) _ = lap("codes by slot", t);
    // An estimate, now that each payload's code width is known; the caller measures what `a` holds.
    j.bytes = key_bytes;
    for (payloads) |p| j.bytes += p.codes.bytes() + p.dict.len() * 16;
    return j;
}

// ---------------------------------------------------------------- group-join

const expr_ast = @import("../expr/ast.zig");
const expr_eval = @import("../expr/eval.zig");
const output_values = @import("../scan/output_values.zig");
const AggValue = output_values.AggValue;

/// GROUP BY keys that the build key determines (the FK, build columns): every group is one build slot, so the
/// aggregates fold into per-slot accumulators, with no group table. Row groups fold in their order whatever the
/// worker that read them (a reorder buffer), so float sums are the same at every `-j`.
pub const GroupJoin = struct {
    pub const Func = enum { sum, count, min, max };
    pub const Lane = enum { f64, i64 };
    pub const Call = struct { func: Func, arg: ?expr_ast.Expr, lane: Lane };
    pub const Output = union(enum) { key: usize, agg: usize };

    /// Probe leaves the keys read: the FK or a virtual build column.
    keys: []const usize,
    calls: []const Call,
    outputs: []const Output,
    order: expr_ast.ResultOrder,
    /// Per slot: whether any row reached it; rows folded, when a `count` is asked; each call's accumulator (sum, min
    /// or max) in its lane.
    seen: []u64,
    count: []u32,
    f: [][]f64,
    i: [][]i64,
    /// One entry per probe row group: its partial, until every earlier row group has folded.
    pending: []std.atomic.Value(?*Partial),
    next: std.atomic.Value(usize) = .init(0),
    draining: std.atomic.Value(bool) = .init(false),
    gpa: std.mem.Allocator,
    n_slots: usize,
    overflow: std.atomic.Value(bool) = .init(false),

    /// One row group's contributions, as runs of rows with one slot, in row order.
    const Partial = struct {
        slots: []u32,
        counts: []u32,
        f: [][]f64,
        i: [][]i64,
        fn free(p: *Partial, gpa: std.mem.Allocator) void {
            for (p.f) |x| gpa.free(x);
            for (p.i) |x| gpa.free(x);
            gpa.free(p.f);
            gpa.free(p.i);
            gpa.free(p.slots);
            gpa.free(p.counts);
            gpa.destroy(p);
        }
    };

    /// Takes `keys`, `calls` and `outputs` (`gpa`'s) once it succeeds; a refusal (the ledger's, under a budget) frees
    /// everything it allocated and leaves them the caller's.
    pub fn init(gpa: std.mem.Allocator, j: *const Join, keys: []const usize, calls: []const Call, outputs: []const Output, row_groups: usize) !*GroupJoin {
        const g = try gpa.create(GroupJoin);
        errdefer gpa.destroy(g);
        const seen = try gpa.alloc(u64, j.n / 64 + 1);
        errdefer gpa.free(seen);
        const count: []u32 = for (calls) |c| {
            if (c.func == .count) break try gpa.alloc(u32, j.n);
        } else &.{};
        errdefer gpa.free(count);
        const f = try gpa.alloc([]f64, calls.len);
        errdefer gpa.free(f);
        @memset(f, &.{});
        errdefer for (f) |x| gpa.free(x);
        const i = try gpa.alloc([]i64, calls.len);
        errdefer gpa.free(i);
        @memset(i, &.{});
        errdefer for (i) |x| gpa.free(x);
        const pending = try gpa.alloc(std.atomic.Value(?*Partial), row_groups);
        errdefer gpa.free(pending);
        for (calls, f, i) |c, *fl, *il| {
            if (c.lane == .f64 and c.func != .count) fl.* = try gpa.alloc(f64, j.n);
            if (c.lane == .i64 and c.func != .count) il.* = try gpa.alloc(i64, j.n);
        }
        @memset(seen, 0);
        @memset(count, 0);
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
            .pending = pending,
            .gpa = gpa,
            .n_slots = j.n,
        };
        return g;
    }

    /// Forget every fold: a refused scan replays from its first row group (`scan.runMultiAggregate`).
    pub fn reset(g: *GroupJoin) void {
        for (g.pending) |*p| if (p.swap(null, .acquire)) |partial| partial.free(g.gpa);
        g.next.store(0, .monotonic);
        g.overflow.store(false, .monotonic);
        @memset(g.seen, 0);
        @memset(g.count, 0);
    }

    fn reached(g: *const GroupJoin, s: u32) bool {
        return g.seen[s >> 6] & (@as(u64, 1) << @intCast(s & 63)) != 0;
    }

    pub fn deinit(g: *GroupJoin) void {
        const gpa = g.gpa;
        for (g.f) |x| gpa.free(x);
        for (g.i) |x| gpa.free(x);
        gpa.free(g.f);
        gpa.free(g.i);
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
        var counted = false;
        for (calls) |c| {
            if (c.func == .count) counted = true else per += 8;
        }
        return slots * (per + @as(usize, if (counted) 4 else 0)) + slots / 8;
    }

    fn argColumn(a: std.mem.Allocator, batch: *const filter_eval.Batch, lookup: []const ?usize, e: expr_ast.Expr, lane: Lane) !union(enum) { f: []const f64, i: []const i64 } {
        var col = try expr_eval.evalExpr(a, batch, lookup, e);
        // A column decoded as codes alone reads as values here.
        if (col.codes() != null) col = try @import("../expr/agg_coded.zig").materialize(a, col);
        switch (col) {
            inline .f64, .i64, .i32 => |c, tag| {
                if (c.has_nulls) return error.GroupJoinNullArgument;
                // A coded column's values are materialized for value consumers (a build column always is).
                if (c.values.len < batch.num_rows) return error.GroupJoinCodedArgument;
                if (lane == .f64) {
                    if (tag != .f64) return error.GroupJoinArgumentType;
                    return .{ .f = c.values };
                }
                if (tag == .f64) return error.GroupJoinArgumentType;
                if (tag == .i64) return .{ .i = c.values };
                const wide = try a.alloc(i64, c.values.len);
                for (c.values, wide) |v, *w| w.* = v;
                return .{ .i = wide };
            },
            else => return error.GroupJoinArgumentType,
        }
    }

    /// Fold one probe row group's selected rows (every one matched: the lookup ran in the filter).
    pub fn foldRowGroup(g: *GroupJoin, j: *const Join, a: std.mem.Allocator, rg_index: usize, batch: *const filter_eval.Batch, lookup: []const ?usize, sel: *const selection.SelectionVector) !void {
        const fk_pos = lookup[j.fk_leaf] orelse return error.BadColumn;
        var fold: Fold = .{ .g = g };
        errdefer fold.deinit();
        try fold.add(a, try slotsFor(j, batch.cols[fk_pos], sel, a), batch, lookup, sel);
        try fold.finish(rg_index);
    }

    /// One row group's partial, a batch of its rows at a time in row order: a run of one slot continues across
    /// batches, so a sliced row group folds to the same bits as a whole one.
    pub const Fold = struct {
        g: *GroupJoin,
        slots: std.ArrayList(u32) = .empty,
        counts: std.ArrayList(u32) = .empty,
        /// Per call, its run accumulators, in the call's lane.
        f: []std.ArrayList(f64) = &.{},
        i: []std.ArrayList(i64) = &.{},

        /// Rows `sel` keeps of `batch`, with their slots in `at` (indexed as `sel`).
        pub fn add(fold: *Fold, a: std.mem.Allocator, at: []const u32, batch: *const filter_eval.Batch, lookup: []const ?usize, sel: *const selection.SelectionVector) !void {
            const g = fold.g;
            const gpa = g.gpa;
            try fold.lanes();
            const args_f = try a.alloc([]const f64, g.calls.len);
            const args_i = try a.alloc([]const i64, g.calls.len);
            for (g.calls, args_f, args_i) |c, *af, *ai| {
                af.* = &.{};
                ai.* = &.{};
                const e = c.arg orelse continue;
                switch (try argColumn(a, batch, lookup, e, c.lane)) {
                    .f => |v| af.* = v,
                    .i => |v| ai.* = v,
                }
            }
            // Runs of one slot, in row order: on a key-sorted probe a slot's rows are one run.
            var it = sel.iterator();
            while (it.next()) |r| {
                const s = at[r];
                if (s == EMPTY) return error.LookupWithoutMatch;
                const fresh = fold.slots.items.len == 0 or fold.slots.items[fold.slots.items.len - 1] != s;
                if (fresh) {
                    try fold.slots.append(gpa, s);
                    try fold.counts.append(gpa, 0);
                }
                fold.counts.items[fold.counts.items.len - 1] += 1;
                for (g.calls, 0..) |c, ci| switch (c.lane) {
                    .f64 => if (c.func != .count) {
                        const v = args_f[ci][r];
                        if (fresh) try fold.f[ci].append(gpa, v) else {
                            const dst = &fold.f[ci].items[fold.f[ci].items.len - 1];
                            dst.* = switch (c.func) {
                                .sum => dst.* + v,
                                .min => @min(dst.*, v),
                                .max => @max(dst.*, v),
                                .count => unreachable,
                            };
                        }
                    },
                    .i64 => if (c.func != .count) {
                        const v = if (c.arg != null) args_i[ci][r] else 1;
                        if (fresh) try fold.i[ci].append(gpa, v) else {
                            const dst = &fold.i[ci].items[fold.i[ci].items.len - 1];
                            dst.* = switch (c.func) {
                                .sum => std.math.add(i64, dst.*, v) catch return error.GroupJoinOverflow,
                                .min => @min(dst.*, v),
                                .max => @max(dst.*, v),
                                .count => unreachable,
                            };
                        }
                    },
                };
            }
        }

        fn lanes(fold: *Fold) !void {
            const n = fold.g.calls.len;
            if (fold.f.len == n) return;
            fold.f = try fold.g.gpa.alloc(std.ArrayList(f64), n);
            @memset(fold.f, .empty);
            fold.i = try fold.g.gpa.alloc(std.ArrayList(i64), n);
            @memset(fold.i, .empty);
        }

        /// Hand the row group's partial to the ordered merge.
        pub fn finish(fold: *Fold, rg_index: usize) !void {
            const g = fold.g;
            const gpa = g.gpa;
            try fold.lanes();
            // Empty first: on a refusal the partial frees what has moved into it, and the caller's `deinit` the rest.
            const p = try gpa.create(Partial);
            p.* = .{ .slots = &.{}, .counts = &.{}, .f = &.{}, .i = &.{} };
            errdefer p.free(gpa);
            p.f = try gpa.alloc([]f64, g.calls.len);
            @memset(p.f, &.{});
            p.i = try gpa.alloc([]i64, g.calls.len);
            @memset(p.i, &.{});
            for (fold.f, fold.i, p.f, p.i) |*lf, *li, *df, *di| {
                df.* = try lf.toOwnedSlice(gpa);
                di.* = try li.toOwnedSlice(gpa);
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
            gpa.free(fold.f);
            gpa.free(fold.i);
            fold.f = &.{};
            fold.i = &.{};
            fold.slots.deinit(gpa);
            fold.counts.deinit(gpa);
        }
    };

    fn apply(g: *GroupJoin, p: *const Partial) void {
        for (p.slots, p.counts, 0..) |s, n, at| {
            const bit = @as(u64, 1) << @intCast(s & 63);
            const first = g.seen[s >> 6] & bit == 0;
            g.seen[s >> 6] |= bit;
            if (g.count.len != 0) g.count[s] += n;
            for (g.calls, 0..) |c, ci| switch (c.lane) {
                .f64 => if (c.func != .count) {
                    const dst = &g.f[ci][s];
                    const v = p.f[ci][at];
                    dst.* = if (first) v else switch (c.func) {
                        .sum => dst.* + v,
                        .min => @min(dst.*, v),
                        .max => @max(dst.*, v),
                        .count => unreachable,
                    };
                },
                .i64 => if (c.func != .count) {
                    const dst = &g.i[ci][s];
                    const v = p.i[ci][at];
                    dst.* = if (first) v else switch (c.func) {
                        .sum => std.math.add(i64, dst.*, v) catch blk: {
                            g.overflow.store(true, .monotonic);
                            break :blk dst.*;
                        },
                        .min => @min(dst.*, v),
                        .max => @max(dst.*, v),
                        .count => unreachable,
                    };
                },
            };
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

    fn value(g: *const GroupJoin, j: *const Join, out: Output, s: u32, gpa: std.mem.Allocator) !AggValue {
        switch (out) {
            .key => |k| {
                const leaf = g.keys[k];
                const p = j.virtualPayload(leaf) orelse return .{ .i = keyOfSlot(j, s) };
                const code = j.payloads[p].codes.get(s);
                return switch (j.payloads[p].dict) {
                    .i32 => |d| .{ .i = d[code] },
                    .i64 => |d| .{ .i = d[code] },
                    .f64 => |d| .{ .f = d[code] },
                    .string => |d| .{ .s = try gpa.dupe(u8, d[code]) },
                };
            },
            .agg => |ci| {
                const c = g.calls[ci];
                if (c.func == .count) return .{ .i = g.count[s] };
                return switch (c.lane) {
                    .f64 => .{ .f = g.f[ci][s] },
                    .i64 => .{ .i = g.i[ci][s] },
                };
            },
        }
    }

    fn before(g: *const GroupJoin, j: *const Join, x: u32, y: u32) bool {
        for (g.order.keys) |key| {
            const a = g.value(j, g.outputs[key.column], x, std.heap.page_allocator) catch return x < y;
            const b = g.value(j, g.outputs[key.column], y, std.heap.page_allocator) catch return x < y;
            defer if (a == .s) std.heap.page_allocator.free(a.s);
            defer if (b == .s) std.heap.page_allocator.free(b.s);
            switch (output_values.orderValues(a, b, key.descending)) {
                .lt => return true,
                .gt => return false,
                .eq => {},
            }
        }
        return x < y;
    }

    /// The answer's rows, one per slot any row reached: in slot order, or, under ORDER BY ... LIMIT, the first
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
                if (!g.reached(s)) continue;
                if (chosen.items.len == k and !g.before(j, s, chosen.items[k - 1])) continue;
                var pos = chosen.items.len;
                while (pos > 0 and g.before(j, s, chosen.items[pos - 1])) pos -= 1;
                if (chosen.items.len == k) _ = chosen.pop();
                try chosen.insert(gpa, pos, s);
            }
        } else for (0..g.n_slots) |si| if (g.reached(@intCast(si))) try chosen.append(gpa, @intCast(si));
        const out = try gpa.alloc([]const AggValue, chosen.items.len);
        for (chosen.items, out) |s, *row| {
            const vals = try gpa.alloc(AggValue, g.outputs.len);
            for (g.outputs, vals) |o, *v| v.* = try g.value(j, o, s, gpa);
            row.* = vals;
        }
        return out;
    }
};
