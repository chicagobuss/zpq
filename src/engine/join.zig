//! A query's join, as the engine runs it (0.6.0): the frontend resolved it by name (`syntax.Join`); here each build
//! side is read into a lookup (`core/join/lookup.zig`), charged to the query's ledger, the probe's footers gain the
//! build columns as virtual leaves (`extend`), and the probe query binds with the lookups as AND conjuncts of its
//! filter. Everything after that is the single-table scan.
const std = @import("std");
const schema = @import("../core/schema.zig");
const metadata = @import("../core/parquet/metadata.zig");
const decimal = @import("../core/parquet/decimal.zig");
const syntax = @import("../core/expr/syntax.zig");
const expr_ast = @import("../core/expr/ast.zig");
const filter_ast = @import("../core/filter/ast.zig");
const filters = @import("../core/filter/parser.zig");
const lookup = @import("../core/join/lookup.zig");
const core_memory = @import("../core/memory.zig");
const local_fs = @import("../local_fs.zig");
const Diag = @import("../core/diag.zig").Diag;
const plans = @import("bound_plan.zig");
const types = @import("types.zig");
const s3 = @import("../io/s3.zig");
const input = @import("input.zig");
const fanout_plan = @import("fanout_plan.zig");

/// Footers of the local files a join names, for the SQL frontend's binder (`sql_parser.parseSqlJoinPlan`). Each is
/// read once; the build reads its file again here (`prepare`). The plan the binder returns borrows nothing of them, so
/// the caller unmaps them (`deinit`) once it has the plan.
pub const LocalSchemas = struct {
    arena: std.mem.Allocator,
    seen: std.StringHashMapUnmanaged(*schema.FileMetaData) = .empty,
    maps: std.ArrayList(local_fs.Mapped) = .empty,

    pub fn footer(raw: *const anyopaque, path: []const u8) anyerror!*const schema.FileMetaData {
        const self: *LocalSchemas = @ptrCast(@alignCast(@constCast(raw)));
        if (self.seen.get(path)) |m| return m;
        try self.maps.ensureUnusedCapacity(self.arena, 1);
        const opened = try open(self.arena, path);
        self.maps.appendAssumeCapacity(opened.map.?);
        const m = try self.arena.create(schema.FileMetaData);
        m.* = opened.meta;
        try self.seen.put(self.arena, path, m);
        return m;
    }

    pub fn deinit(self: *LocalSchemas) void {
        for (self.maps.items) |m| m.unmap();
        self.maps = .empty;
        self.seen = .empty;
    }
};

const Opened = struct { map: ?local_fs.Mapped, bytes: []const u8, meta: schema.FileMetaData };

/// Builds read local files in this release: a build on S3 refuses, naming it. The caller owns the mapping.
fn open(arena: std.mem.Allocator, path: []const u8) !Opened {
    if (std.mem.startsWith(u8, path, "s3://")) return error.JoinNotLocal;
    const m = try local_fs.mapFile(path);
    errdefer m.unmap();
    return .{ .map = m, .bytes = m.bytes, .meta = try metadata.open(arena, m.bytes) };
}

/// What `--explain` says of a join: copied out of the builds, which the query's end releases.
pub const Explained = struct {
    kind: lookup.Kind,
    main: Lookup,
    semis: []const Lookup,
    /// A star's other dimensions, each publishing its own columns.
    dims: []const Lookup,
    /// The group-join's slots and their bytes, when one was admitted.
    group: ?struct { slots: usize, bytes: usize },
    charged: usize,

    pub const Lookup = struct {
        /// Written beside a SEMI join's or a dimension's lookup; the main one's is `Explained.kind`.
        kind: lookup.Kind,
        fk: []const u8,
        input: []const u8,
        key: []const u8,
        structure: []const u8,
        keys: usize,
        bytes: usize,
        columns: []const []const u8,
        semis: []const Lookup,
        /// A snowflake's tables under this build, whose columns it publishes too.
        dims: []const Lookup,

        fn of(a: std.mem.Allocator, l: Leaf) !Lookup {
            const j = l.join;
            const columns = try a.alloc([]const u8, j.payloads.len);
            for (j.payloads, columns) |p, *c| c.* = try a.dupe(u8, p.name);
            const semis = try a.alloc(Lookup, l.semis.len);
            for (l.semis, semis) |s, *out| out.* = try of(a, s);
            const dims = try a.alloc(Lookup, l.dims.len);
            for (l.dims, dims) |d, *out| out.* = try of(a, d);
            return .{
                .kind = j.kind,
                .fk = try a.dupe(u8, l.fk),
                .input = try a.dupe(u8, j.build_name),
                .key = try a.dupe(u8, j.key_name),
                .structure = @tagName(j.keys),
                .keys = j.n,
                .bytes = j.bytes,
                .columns = columns,
                .semis = semis,
                .dims = dims,
            };
        }
    };
};

/// One lookup conjunct of a filter: the FK it reads, by name, and the build it probes; with that build's own SEMI
/// joins, the inner tables of a chain (`--explain` shows them).
pub const Leaf = struct { fk: []const u8, join: *lookup.Join, semis: []const Leaf = &.{}, dims: []const Leaf = &.{} };

/// The builds' memory: an arena for their many small pieces, and an exact allocation of its own for each large array (a
/// key map, a payload's codes). In an arena alone each new block is half again the previous one plus the request, so a
/// few large arrays snowball: SF30 orders' 80 MB of map and codes held 152 MB. Used from one thread; all of it goes at
/// `deinit`.
const Store = struct {
    arena: std.heap.ArenaAllocator,
    child: std.mem.Allocator,
    large: std.ArrayList(Large) = .empty,

    const Large = struct { ptr: [*]u8, len: usize, alignment: std.mem.Alignment };
    /// A request of at least this many bytes takes an allocation of its own.
    const large_bytes = 1 << 20;

    fn init(child: std.mem.Allocator) Store {
        return .{ .arena = .init(child), .child = child };
    }

    fn deinit(s: *Store) void {
        for (s.large.items) |l| s.child.rawFree(l.ptr[0..l.len], l.alignment, @returnAddress());
        s.large.deinit(s.child);
        s.arena.deinit();
    }

    /// What the store holds: the arena's blocks and the large arrays.
    fn capacity(s: *const Store) usize {
        var n = s.arena.queryCapacity();
        for (s.large.items) |l| n += l.len;
        return n;
    }

    fn allocator(s: *Store) std.mem.Allocator {
        return .{ .ptr = s, .vtable = &.{ .alloc = alloc, .resize = resize, .remap = remap, .free = free } };
    }

    fn find(s: *const Store, ptr: [*]u8) ?usize {
        for (s.large.items, 0..) |l, i| if (l.ptr == ptr) return i;
        return null;
    }

    fn alloc(ctx: *anyopaque, len: usize, alignment: std.mem.Alignment, ret: usize) ?[*]u8 {
        const s: *Store = @ptrCast(@alignCast(ctx));
        if (len < large_bytes) return s.arena.allocator().rawAlloc(len, alignment, ret);
        s.large.ensureUnusedCapacity(s.child, 1) catch return null;
        const ptr = s.child.rawAlloc(len, alignment, ret) orelse return null;
        s.large.appendAssumeCapacity(.{ .ptr = ptr, .len = len, .alignment = alignment });
        return ptr;
    }

    // A piece stays where it was allocated, whatever it grows to.
    fn resize(ctx: *anyopaque, memory: []u8, alignment: std.mem.Alignment, new_len: usize, ret: usize) bool {
        const s: *Store = @ptrCast(@alignCast(ctx));
        const i = s.find(memory.ptr) orelse return s.arena.allocator().rawResize(memory, alignment, new_len, ret);
        if (!s.child.rawResize(memory, alignment, new_len, ret)) return false;
        s.large.items[i].len = new_len;
        return true;
    }

    fn remap(ctx: *anyopaque, memory: []u8, alignment: std.mem.Alignment, new_len: usize, ret: usize) ?[*]u8 {
        const s: *Store = @ptrCast(@alignCast(ctx));
        const i = s.find(memory.ptr) orelse return s.arena.allocator().rawRemap(memory, alignment, new_len, ret);
        const ptr = s.child.rawRemap(memory, alignment, new_len, ret) orelse return null;
        s.large.items[i].ptr = ptr;
        s.large.items[i].len = new_len;
        return ptr;
    }

    fn free(ctx: *anyopaque, memory: []u8, alignment: std.mem.Alignment, ret: usize) void {
        const s: *Store = @ptrCast(@alignCast(ctx));
        const i = s.find(memory.ptr) orelse return s.arena.allocator().rawFree(memory, alignment, ret);
        s.child.rawFree(memory, alignment, ret);
        _ = s.large.swapRemove(i);
    }
};

pub const Prepared = struct {
    /// The probe's own join: its build columns bind as virtual leaves, its group-join folds.
    main: Leaf,
    /// SEMI joins of the probe beside it.
    semis: []const Leaf,
    /// A star's other dimensions: INNER lookups whose build columns bind as virtual leaves too.
    dims: []const Leaf = &.{},
    /// What the builds hold (their arenas' capacity) and a group-join's slots once one is admitted, for `--explain`;
    /// and the group-join's part of it.
    charged: usize = 0,
    group_bytes: usize = 0,
    ledger: *core_memory.SharedBudget,
    gpa: std.mem.Allocator,
    /// Every allocation a build or the group-join makes draws from the query's ledger as it is made (`charge`): the
    /// builds' structures and dictionaries in `store`, their decode scratch beside it, the group-join's slots and
    /// partials. A build that does not fit refuses while it builds, not after.
    charge: core_memory.Charged = undefined,
    replica_limit: ?usize = null,
    replica_budget: core_memory.SharedBudget = undefined,
    replica_charge: core_memory.Charged = undefined,
    /// The top-level builds, each with what it holds (`Task`): released together.
    tasks: []Task = &.{},
    order: expr_ast.ResultOrder,
    inner: plans.Source = undefined,

    /// The probe query's own binding, with the lookups ANDed into its filter. It binds against footers `extend`
    /// gave the build columns.
    pub fn source(self: *Prepared, inner: plans.Source) plans.Source {
        self.inner = inner;
        return .{ .context = self, .kind = inner.kind, .bind_fn = bind, .join = inner.join };
    }

    /// Each publishing build's columns as virtual leaves, the main build's first, in the same order in every footer.
    pub fn extend(self: *Prepared, arena: std.mem.Allocator, meta: *schema.FileMetaData) !void {
        try lookup.extend(self.main.join, arena, meta);
        for (self.dims) |d| try lookup.extend(d.join, arena, meta);
    }

    fn bind(raw: *const anyopaque, arena: std.mem.Allocator, meta: *const schema.FileMetaData, diag: ?*Diag, options: plans.Options) anyerror!plans.Query {
        const self: *Prepared = @ptrCast(@alignCast(@constCast(raw)));
        var q = try self.inner.bind_fn(self.inner.context, arena, meta, diag, options);
        const a = switch (q) {
            .aggregate => |*a| a,
            // Row output of a join is not in this release.
            .projection => return error.JoinRowsNotSupported,
        };
        // Conditions on build columns come out of the probe's filter: they narrow after the lookups (`Residual`).
        const residual = try self.splitResidual(arena, &a.filter);
        for (self.semis) |s| a.filter = try withLookup(arena, a.filter, try leafOf(meta, s, diag));
        for (self.dims) |d| a.filter = try withLookup(arena, a.filter, try leafOf(meta, d, diag));
        var main_leaf = try leafOf(meta, self.main, diag);
        if (residual.items.len != 0) main_leaf.lookup.residual = try self.residualOf(arena, residual.items);
        a.filter = try withLookup(arena, a.filter, main_leaf);
        const j = self.main.join;
        if (j.group == null and j.kind == .inner) {
            const admitted = groupJoin(self.charge.allocator(), j, a.*, meta) catch |err| {
                Diag.setInput(diag, j.build_name, null);
                Diag.setColumn(diag, j.key_name);
                return if (self.charge.refusal(err) == error.ExceededMemoryBudget) error.JoinBuildExceedsBudget else err;
            };
            if (admitted) |g| {
                self.group_bytes = lookup.GroupJoin.bytesFor(j.n, g.calls);
                self.charged += self.group_bytes;
                j.group = g;
            }
        }
        // Under `post`, ORDER BY and HAVING index the result program's wider row, not the slots' outputs: the
        // group-join then keeps every slot, and the result program ranks and cuts them.
        if (j.group) |g| g.order = if (self.order.post) .{} else self.order;
        // Without a group-join, a GROUP BY key that is a build column another key determines groups by that key alone,
        // and reads once per group (`expr_ast.Fetch`).
        if (j.group == null) if (a.keys) |keys| {
            a.keys = try self.withFetches(arena, keys);
        };
        return q;
    }

    /// `keys` with each build-column key that another bare key determines marked with how to read it from that key's
    /// value: the join's probe FK determines its build's columns, and a snowflake's FK published through the build
    /// determines that table's columns (`lookup.Payload.via`).
    fn withFetches(self: *const Prepared, arena: std.mem.Allocator, keys: []const expr_ast.Expr) ![]const expr_ast.Expr {
        const all = try self.publishing(arena);
        var out: ?[]expr_ast.Expr = null;
        for (keys, 0..) |k, i| {
            if (k != .col_ref or k.col_ref.fetch != null) continue;
            const ci = k.col_ref.col_idx;
            const j = for (all) |x| {
                if (x.virtualPayload(ci) != null) break x;
            } else continue;
            const q = j.virtualPayload(ci).?;
            // Only a value read back exactly as the scan reads the column: a signed one, from a dictionary of its own
            // lane (a DECIMAL stored as bytes, or an unsigned INT32 the scan zero-extends, stays a key).
            if (k.col_ref.unsigned_32 or k.col_ref.unsigned_64) continue;
            const lane_ok = switch (j.payloads[q].dict) {
                .string => k.col_ref.expr_type == .str,
                .f64 => k.col_ref.expr_type == .f64,
                .i32, .i64 => k.col_ref.expr_type == .i64 or k.col_ref.expr_type == .dec,
            };
            if (!lane_ok) continue;
            const via = j.payloads[q].via;
            const f: expr_ast.Fetch = if (via != null and isKey(keys, j.virtual_base + via.?.fk))
                fetchOf(j.virtual_base + via.?.fk, via.?.inner, via.?.p)
            else if (j.domain == .signed and isKey(keys, j.fk_leaf))
                fetchOf(j.fk_leaf, j, q)
            else
                continue;
            if (out == null) out = try arena.dupe(expr_ast.Expr, keys);
            const fp = try arena.create(expr_ast.Fetch);
            fp.* = f;
            out.?[i].col_ref.fetch = fp;
        }
        return out orelse keys;
    }

    fn isKey(keys: []const expr_ast.Expr, leaf: usize) bool {
        for (keys) |k| if (k == .col_ref and k.col_ref.col_idx == leaf) return true;
        return false;
    }

    /// The publishing joins: the main one and a star's other dimensions.
    fn publishing(self: *const Prepared, a: std.mem.Allocator) ![]*const lookup.Join {
        const out = try a.alloc(*const lookup.Join, 1 + self.dims.len);
        out[0] = self.main.join;
        for (self.dims, out[1..]) |d, *o| o.* = d.join;
        return out;
    }

    fn isVirtual(self: *const Prepared, ci: usize) bool {
        if (self.main.join.virtualPayload(ci) != null) return true;
        for (self.dims) |d| if (d.join.virtualPayload(ci) != null) return true;
        return false;
    }

    /// The conjuncts of `filter` that read a build column, taken out of it.
    fn splitResidual(self: *const Prepared, a: std.mem.Allocator, filter: *?filter_ast.Filter) !std.ArrayList(filter_ast.Filter) {
        var out: std.ArrayList(filter_ast.Filter) = .empty;
        const f = filter.* orelse return out;
        const kids: []const filter_ast.Filter = if (f == .and_filter and f.and_filter.in_set == null and !f.and_filter.null_member)
            f.and_filter.children
        else
            (&f)[0..1];
        var kept: std.ArrayList(filter_ast.Filter) = .empty;
        var cols: std.ArrayList(usize) = .empty;
        for (kids) |k| {
            cols.clearRetainingCapacity();
            try k.collectColumns(&cols, a);
            const reads_build = for (cols.items) |ci| {
                if (self.isVirtual(ci)) break true;
            } else false;
            try (if (reads_build) &out else &kept).append(a, k);
        }
        if (out.items.len == 0) return out;
        filter.* = if (kept.items.len == 0) null else try filter_ast.join(a, true, kept.items);
        return out;
    }

    /// The residual of `conjuncts`: what it reads of the probe (its own columns and each join's FK) and of the builds.
    fn residualOf(self: *const Prepared, a: std.mem.Allocator, conjuncts: []const filter_ast.Filter) !*const filter_ast.Residual {
        const all = try self.publishing(a);
        var reads: std.ArrayList(usize) = .empty;
        var virtual: std.ArrayList(usize) = .empty;
        var joins: std.ArrayList(*const lookup.Join) = .empty;
        var cols: std.ArrayList(usize) = .empty;
        for (conjuncts) |k| try k.collectColumns(&cols, a);
        for (cols.items) |ci| {
            if (!self.isVirtual(ci)) {
                try reads.append(a, ci);
                continue;
            }
            try virtual.append(a, ci);
            for (all) |j| if (j.virtualPayload(ci) != null) {
                if (std.mem.indexOfScalar(*const lookup.Join, joins.items, j) == null) {
                    try joins.append(a, j);
                    try reads.append(a, j.fk_leaf);
                }
            };
        }
        const r = try a.create(filter_ast.Residual);
        r.* = .{ .filter = try filter_ast.join(a, true, conjuncts), .reads = reads.items, .virtual = virtual.items, .joins = joins.items };
        return r;
    }

    /// The group-join's answer in place of the empty one the group tables gave, then its slots go back.
    pub fn takeRows(self: *Prepared, gpa: std.mem.Allocator) !?[]const []const @import("../core/scan.zig").AggValue {
        const j = self.main.join;
        const g = j.group orelse return null;
        const rows = try g.rows(j, gpa);
        j.group = null;
        g.deinit();
        return rows;
    }

    /// The join as `--explain` reports it, on `a` (which outlives the builds).
    pub fn explained(self: *const Prepared, a: std.mem.Allocator) !Explained {
        const semis = try a.alloc(Explained.Lookup, self.semis.len);
        for (self.semis, semis) |s, *out| out.* = try Explained.Lookup.of(a, s);
        const dims = try a.alloc(Explained.Lookup, self.dims.len);
        for (self.dims, dims) |d, *out| out.* = try Explained.Lookup.of(a, d);
        return .{
            .kind = self.main.join.kind,
            .main = try Explained.Lookup.of(a, self.main),
            .semis = semis,
            .dims = dims,
            .group = if (self.group_bytes != 0) .{ .slots = self.main.join.n, .bytes = self.group_bytes } else null,
            .charged = self.charged,
        };
    }

    /// A refusal of the join's own allocations, named as the ledger's (`error.ExceededMemoryBudget`).
    pub fn refusal(self: *const Prepared, err: anytype) @TypeOf(self.charge.refusal(err)) {
        return self.charge.refusal(err);
    }

    /// Deferred build strings are read at the result cut, after `build` checked its file: one changed since refuses.
    pub fn intact(self: *const Prepared) error{InputChanged}!void {
        for (self.tasks) |t| for (t.maps.items) |h| try h.map.intact(h.path);
    }

    /// Everything the join holds goes back: a group-join whose rows were not taken (an explained or failed query),
    /// the builds, their mappings.
    pub fn release(self: *Prepared) void {
        if (self.main.join.group) |g| {
            self.main.join.group = null;
            g.deinit();
        }
        self.releaseBuilds();
    }

    fn releaseBuilds(self: *Prepared) void {
        const trace = std.c.getenv("ZPQ_JOIN_TRACE") != null;
        const start = if (trace) @import("../clock.zig").monoNs() else 0;
        for (self.tasks) |*t| t.release();
        self.tasks = &.{};
        if (trace) std.debug.print("join tail: build release {d:.2} ms\n", .{
            @as(f64, @floatFromInt(@import("../clock.zig").monoNs() - start)) / 1e6,
        });
    }
};

fn leafOf(meta: *const schema.FileMetaData, l: Leaf, diag: ?*Diag) !filter_ast.Filter {
    const fk = metadata.resolveColumn(meta, l.fk) catch |err| {
        Diag.setColumn(diag, l.fk);
        return err;
    };
    // The FK compares with the build key only within one SQL domain (`lookup.KeyDomain`).
    const same = if (metadata.leafSchemaElement(meta, fk)) |e| if (lookup.KeyDomain.of(e)) |d| d.eql(l.join.domain) else false else false;
    if (!same) {
        Diag.setColumn(diag, l.fk);
        return error.JoinKeyTypeMismatch;
    }
    if (l.join.publishes) l.join.fk_leaf = fk;
    return .{ .lookup = .{ .col_idx = fk, .join = l.join } };
}

/// The filter's top-level AND with the lookup last: the cheaper conjuncts narrow first.
fn withLookup(arena: std.mem.Allocator, f: ?filter_ast.Filter, leaf: filter_ast.Filter) !filter_ast.Filter {
    const base = f orelse return leaf;
    if (base == .and_filter and base.and_filter.in_set == null and !base.and_filter.null_member) {
        const kids = try arena.alloc(filter_ast.Filter, base.and_filter.children.len + 1);
        @memcpy(kids[0 .. kids.len - 1], base.and_filter.children);
        kids[kids.len - 1] = leaf;
        return .{ .and_filter = .{ .children = kids } };
    }
    const kids = try arena.alloc(filter_ast.Filter, 2);
    kids[0] = base;
    kids[1] = leaf;
    return .{ .and_filter = .{ .children = kids } };
}

/// A group-join when the FK is a GROUP BY key and every other key is a build column (each group is one build slot)
/// and every aggregate is one the slots fold; else null, and the query groups as any other.
fn groupJoin(gpa: std.mem.Allocator, j: *lookup.Join, q: plans.Aggregate, meta: *const schema.FileMetaData) !?*lookup.GroupJoin {
    // An ANTI join's rows have no slot, and a LEFT join's misses none: their groups are the probe's own.
    if (j.kind == .anti or j.kind == .left) return null;
    for (j.payloads) |p| if (p.deferred != null) return null;
    const keys = q.keys orelse return null;
    const outputs = q.outputs orelse return null;
    // Build columns alone (GROUP BY a dimension attribute) gather many slots into one group.
    var by_fk = false;
    for (keys) |k| {
        if (k != .col_ref) return null;
        const ci = k.col_ref.col_idx;
        if (ci != j.fk_leaf and j.virtualPayload(ci) == null) return null;
        by_fk = by_fk or ci == j.fk_leaf;
    }
    if (!by_fk) return null;
    for (q.calls) |c| {
        if (c.where != null or c.state or c.offset != 0 or c.as_date) return null;
        switch (c.func) {
            .sum, .count, .min, .max => if (c.result != .f64 and c.result != .i64) return null,
            .avg => if (c.result != .avg_f64) return null,
            else => return null,
        }
    }
    for (outputs) |o| if (o.derived != null) return null;
    const G = lookup.GroupJoin;
    // Each array is the group-join's once `init` succeeds, and freed here when anything refuses before.
    const leaves = try gpa.alloc(usize, keys.len);
    errdefer gpa.free(leaves);
    for (keys, leaves) |k, *leaf| leaf.* = k.col_ref.col_idx;
    const calls = try gpa.alloc(G.Call, q.calls.len);
    errdefer gpa.free(calls);
    for (q.calls, calls) |c, *out| out.* = .{
        .func = switch (c.func) {
            .sum => .sum,
            .count => .count,
            .min => .min,
            .max => .max,
            .avg => .avg,
            else => unreachable,
        },
        .arg = c.arg,
        // An avg sums in its argument's lane.
        .lane = if (c.result == .f64 or (c.func == .avg and c.arg.?.typeOf() == .f64)) .f64 else .i64,
    };
    const outs = try gpa.alloc(G.Output, outputs.len);
    errdefer gpa.free(outs);
    for (outputs, outs) |o, *out| out.* = switch (o.source) {
        .key => |k| .{ .key = k },
        .agg => |a| .{ .agg = a },
    };
    return try G.init(gpa, j, leaves, calls, outs, meta.row_groups.items.len);
}

/// A reduced INNER edge keeps the INNER join's unique keys; a SEMI or ANTI join the query wrote takes duplicates.
fn semiKind(s: syntax.Semi) lookup.Kind {
    return if (s.anti) .anti else if (s.unique) .inner else .semi;
}

/// One top-level build (the main join, a SEMI join of the probe, a star's dimension) and everything it holds: its
/// arena, its store, the build files' mappings. The builds are independent, so each runs on a thread of its own
/// beside the others (a chain's inner builds run inside their outer build's task, before it). The mappings are held
/// for the query: virtual leaves bind by the payloads' names, which the build footers own.
const Task = struct {
    p: *Prepared,
    ctx: types.Context,
    snapshot: std.crypto.hash.sha2.Sha256 = .init(.{}),
    threads: usize,
    fk: []const u8,
    build: syntax.Build,
    query: ?*const syntax.Query = null,
    kind: lookup.Kind,
    arena: std.heap.ArenaAllocator,
    store: Store,
    aligned_store: Store,
    aligned: ?types.AlignedJoin = null,
    opened: std.ArrayList(input.OpenedInputs) = .empty,
    aligned_source_bytes: usize = 0,
    maps: std.ArrayList(struct { map: local_fs.Mapped, path: []const u8 }) = .empty,
    charged: usize = 0,
    diag: Diag = .{},
    leaf: Leaf = undefined,
    err: ?anyerror = null,

    fn run(t: *Task) void {
        t.leaf = build(t, t.fk, t.build, t.kind, t.aligned) catch |err| {
            t.err = err;
            return;
        };
    }

    fn release(t: *Task) void {
        for (t.opened.items) |*opened| opened.deinit();
        t.aligned_store.deinit();
        t.store.deinit();
        for (t.maps.items) |h| h.map.unmap();
        t.arena.deinit();
    }
};

/// Read one build side (its own SEMI joins first) into the lookup conjunct on `fk`, charged to the ledger as it is
/// read.
fn build(t: *Task, fk: []const u8, b: syntax.Build, kind: lookup.Kind, aligned: ?types.AlignedJoin) !Leaf {
    const arena = t.arena.allocator();
    const p = t.p;
    const diag = &t.diag;
    try t.maps.ensureUnusedCapacity(arena, 1);
    const file = (if (p.replica_limit != null) openRemote(t, b, aligned) else open(arena, b.path)) catch |err| {
        Diag.setInput(diag, b.path, err);
        return err;
    };
    if (file.map) |m| t.maps.appendAssumeCapacity(.{ .map = m, .path = b.path });
    const meta = try arena.create(schema.FileMetaData);
    meta.* = file.meta;
    const filter_syntax = if (aligned) |selection| try fanout_plan.intervalFilter(arena, b.filter, b.key, selection.low, selection.high) else b.filter;
    var filter: ?filter_ast.Filter = if (filter_syntax) |f| try filters.bindSyntax(arena, f, meta, diag) else null;
    const semis = try arena.alloc(Leaf, b.semis.len);
    for (b.semis, semis) |s, *inner| {
        inner.* = try build(t, s.fk, s.build, semiKind(s), null);
        filter = try withLookup(arena, filter, try leafOf(meta, inner.*, diag));
    }
    // A snowflake's tables under this build: INNER lookups of its filter, whose columns it publishes (`lookup.chain`).
    const dims = try arena.alloc(Leaf, b.dims.len);
    for (b.dims, dims) |d, *inner| {
        inner.* = try build(t, d.fk, d.build, .inner, null);
        filter = try withLookup(arena, filter, try leafOf(meta, inner.*, diag));
    }
    const payload = try arena.alloc(usize, b.columns.len);
    for (b.columns, payload) |name, *leaf| leaf.* = metadata.resolveColumn(meta, name) catch |err| {
        Diag.setColumn(diag, name);
        return err;
    };
    const key = metadata.resolveColumn(meta, b.key) catch |err| {
        Diag.setColumn(diag, b.key);
        return err;
    };
    // A DECIMAL build column would reach the probe as its unscaled integers, where its expressions read exact
    // decimals: refused by name until a join carries a decimal's scale.
    var reads: std.ArrayList(usize) = .empty;
    if (filter) |f| try f.collectColumns(&reads, arena);
    try reads.appendSlice(arena, payload);
    for (reads.items) |leaf| if (metadata.leafSchemaElement(meta, leaf)) |e| if (decimal.kindFromSchema(&e) != null) {
        Diag.setInput(diag, b.path, null);
        Diag.setColumn(diag, e.name);
        return error.JoinDecimalColumn;
    };
    const store = if (aligned != null) &t.aligned_store else &t.store;
    const before = store.capacity();
    const j = lookup.build(store.allocator(), p.charge.allocator(), file.bytes, meta, .{
        .kind = kind,
        .build_name = b.path,
        .key_name = b.key,
        .key = key,
        .payload = payload,
        .deferred = try deferredPayloads(arena, t.query, b, fk, meta, payload),
        .filter = filter,
        .threads = t.threads,
        .budget = p.ledger.available(),
    }) catch |err| {
        Diag.setInput(diag, b.path, null);
        Diag.setColumn(diag, b.key);
        return if (p.charge.refusal(err) == error.ExceededMemoryBudget) error.JoinBuildExceedsBudget else err;
    };
    // The binder publishes each FK a snowflake's table reads.
    for (b.dims, dims) |d, inner| {
        const at = for (b.columns, 0..) |name, i| {
            if (std.mem.eql(u8, name, d.fk)) break i;
        } else unreachable;
        lookup.chain(store.allocator(), j, at, inner.join) catch |err| {
            Diag.setInput(diag, b.path, null);
            Diag.setColumn(diag, d.fk);
            return if (p.charge.refusal(err) == error.ExceededMemoryBudget) error.JoinBuildExceedsBudget else err;
        };
    }
    // The build read its file whole: one that changed meanwhile refuses, as a scanned input does.
    if (file.map) |m| m.intact(b.path) catch |err| {
        Diag.setInput(diag, b.path, err);
        return err;
    };
    j.bytes = store.capacity() - before;
    t.charged += j.bytes;
    return .{ .fk = fk, .join = j, .semis = semis, .dims = dims };
}

/// Read every build of `args`' join; null for a query without one. The probe's inputs must be local files too.
pub fn prepare(ctx: types.Context, arena: std.mem.Allocator, args: types.QueryArgs, ledger: *core_memory.SharedBudget) !?*Prepared {
    const gpa = ctx.gpa;
    const source = args.plan_source orelse return null;
    const sj = source.join orelse return null;
    for (args.inputs) |path| if (args.replicated_join_limit == null and std.mem.startsWith(u8, path, "s3://")) {
        Diag.setInput(args.diag, path, null);
        return error.JoinNotLocal;
    };
    const threads = if (args.parallelism == 0) std.Thread.getCpuCount() catch 1 else args.parallelism;
    const p = try arena.create(Prepared);
    p.* = .{ .main = undefined, .semis = &.{}, .ledger = ledger, .gpa = gpa, .order = args.result_order };
    p.charge = .{ .child = gpa, .budget = ledger };
    p.replica_limit = args.replicated_join_limit;
    p.replica_budget = .{ .limit = args.replicated_join_limit orelse ledger.limit, .parent = ledger };
    p.replica_charge = .{ .child = gpa, .budget = &p.replica_budget };
    const retained = if (p.replica_limit != null) p.replica_charge.allocator() else p.charge.allocator();
    // No group-join exists before the probe binds.
    errdefer p.releaseBuilds();
    // The main build, the probe's SEMI joins, then a star's dimensions: one task each.
    const tasks = try arena.alloc(Task, 1 + sj.semis.len + sj.dims.len);
    const task: Task = .{
        .p = p,
        .ctx = ctx,
        .threads = threads,
        .fk = undefined,
        .build = undefined,
        .query = if (!(args.typed_answer and args.output != null) and args.result_order.limit != null and
            !@import("env.zig").envDisables(ctx.env, "ZPQ_GROUP_KEY_FD") and
            !@import("env.zig").envDisables(ctx.env, "ZPQ_LATE_FETCH") and
            !@import("env.zig").envDisables(ctx.env, "ZPQ_JOIN_LATE_PAYLOAD")) deferredSyntax(source) else null,
        .kind = undefined,
        .arena = .init(if (args.aligned_join != null) p.charge.allocator() else if (p.replica_limit != null) retained else gpa),
        .store = .init(retained),
        .aligned_store = .init(p.charge.allocator()),
    };
    // A copy of an arena or a store that has allocated nothing is a fresh one.
    for (tasks) |*t| t.* = task;
    tasks[0].fk = sj.fk;
    tasks[0].build = sj.build;
    tasks[0].aligned = args.aligned_join;
    tasks[0].kind = switch (sj.kind) {
        inline else => |k| @field(lookup.Kind, @tagName(k)),
    };
    for (sj.semis, tasks[1..][0..sj.semis.len]) |s, *t| {
        t.fk = s.fk;
        t.build = s.build;
        t.kind = semiKind(s);
    }
    for (sj.dims, tasks[1 + sj.semis.len ..]) |d, *t| {
        t.fk = d.fk;
        t.build = d.build;
        t.kind = if (d.left) .left else .inner;
    }
    p.tasks = tasks;
    runTasks(tasks);
    // The first failing build's error, in the order the query names them, whatever the scheduling.
    for (tasks) |*t| if (t.err) |err| {
        if (args.diag) |d| {
            d.column = t.diag.column;
            d.input = t.diag.input;
            d.cause = t.diag.cause;
            d.detail = t.diag.detail;
        }
        // S3 wraps allocator failures (including signing scratch) as transport errors.
        if (p.charge.refused.load(.monotonic) or (p.replica_limit != null and p.replica_charge.refused.load(.monotonic))) {
            if (args.diag) |d| d.cause = error.JoinBuildExceedsBudget;
            return error.JoinBuildExceedsBudget;
        }
        return err;
    };
    for (tasks) |t| p.charged += t.charged;
    if (p.replica_limit != null) p.charged = p.replica_budget.used.load(.monotonic);
    for (tasks) |t| {
        p.charged += t.aligned_store.capacity() + t.aligned_source_bytes;
    }
    p.main = tasks[0].leaf;
    p.main.join.main = true;
    p.main.join.publishes = true;
    const semis = try arena.alloc(Leaf, sj.semis.len);
    for (semis, tasks[1..][0..sj.semis.len]) |*out, t| out.* = t.leaf;
    // A star's dimensions publish after the main build, each with its own slots.
    const dims = try arena.alloc(Leaf, sj.dims.len);
    for (dims, tasks[1 + sj.semis.len ..], 1..) |*out, t, slot| {
        out.* = t.leaf;
        out.join.publishes = true;
        out.join.slot_index = slot;
    }
    p.semis = semis;
    p.dims = dims;
    return p;
}

/// Every task at once, the first on this thread; a thread that cannot start runs its task here instead. Each build
/// parallelizes over its row groups as well, so a large build and a small one share the cores rather than queue.
fn runTasks(tasks: []Task) void {
    var threads: [64]?std.Thread = @splat(null);
    for (tasks[1..], 0..) |*t, i| {
        if (i < threads.len) threads[i] = std.Thread.spawn(.{}, Task.run, .{t}) catch null;
        if (i >= threads.len or threads[i] == null) t.run();
    }
    tasks[0].run();
    for (threads) |th| if (th) |x| x.join();
}

test "a build store gives each large array its own allocation and frees it all" {
    var s: Store = .init(std.testing.allocator);
    defer s.deinit();
    const a = s.allocator();
    const small = try a.alloc(u8, 100);
    _ = small;
    const big = try a.alloc(u64, Store.large_bytes / 8);
    try std.testing.expectEqual(@as(usize, 1), s.large.items.len);
    const bigger = try a.alloc(u32, Store.large_bytes);
    // Exact: the arena's block plus the two arrays, not a block half again their size.
    try std.testing.expectEqual(s.arena.queryCapacity() + Store.large_bytes + 4 * Store.large_bytes, s.capacity());
    a.free(big);
    try std.testing.expectEqual(@as(usize, 1), s.large.items.len);
    try std.testing.expectEqual(@as(*anyopaque, @ptrCast(bigger.ptr)), @as(*anyopaque, @ptrCast(s.large.items[0].ptr)));
    // A list that outgrows the threshold keeps working through resize, remap and free.
    var list: std.ArrayList(u8) = .empty;
    defer list.deinit(a);
    for (0..3 * Store.large_bytes) |i| try list.append(a, @truncate(i));
    try std.testing.expectEqual(@as(u8, @truncate(3 * Store.large_bytes - 1)), list.items[list.items.len - 1]);
}

/// The lookup consumes a compact file too: existing S3 fetch/rebase selects only its columns, without pruning rows
/// the lookup itself will filter. Nested dimensions retain their own replicated allocator and complete input.
fn openRemote(t: *Task, b: syntax.Build, aligned: ?types.AlignedJoin) !Opened {
    if (!std.mem.startsWith(u8, b.path, "s3://")) return error.ReplicatedJoinNeedsS3;
    if (aligned) |selection| if (selection.build.of != null) return error.AlignedJoinBadRange;
    var ctx = t.ctx;
    ctx.pool_gpa = ctx.pool_gpa orelse ctx.gpa;
    ctx.gpa = if (aligned != null) t.p.charge.allocator() else t.p.replica_charge.allocator();
    var cache: plans.Cache = .{};
    const source: plans.Source = .{ .context = &b, .kind = .rows, .bind_fn = buildColumns };
    const args: types.QueryArgs = .{
        .inputs = &.{b.path},
        .plan_source = source,
        .plan_cache = &cache,
        .row_groups = if (aligned) |selection| &.{selection.build} else null,
        .input_snapshots = if (aligned) |selection| &.{selection.build_snapshot} else null,
        .pin_input_generation = true,
        .max_memory = if (aligned != null) t.p.ledger.available() else t.p.replica_budget.available(),
        .parallelism = t.threads,
        .diag = &t.diag,
    };
    var opened = try input.openInputs(ctx, t.arena.allocator(), args, .{ .limit = std.math.maxInt(u64) });
    errdefer opened.deinit();
    const metas = try input.materializeMetas(t.arena.allocator(), opened, &t.diag, null);
    const snapshot = opened.snapshots[0] orelse return error.JoinBuildNoSnapshot;
    if (snapshot.etag.len == 0 or std.mem.startsWith(u8, snapshot.etag, "W/")) return error.JoinBuildNoSnapshot;
    const creds = try s3.Credentials.fromEnv(t.ctx.env);
    snapshotObject(&t.snapshot, creds.endpoint orelse creds.region, b.path, snapshot.etag, snapshot.size);
    try t.opened.append(t.arena.allocator(), opened);
    if (aligned != null) for (opened.s3_buffers) |bytes| {
        t.aligned_source_bytes += bytes.len;
    };
    return .{ .map = null, .bytes = opened.inputs[0].bytes, .meta = metas[0] };
}

fn buildColumns(raw: *const anyopaque, a: std.mem.Allocator, meta: *const schema.FileMetaData, diag: ?*Diag, _: plans.Options) anyerror!plans.Query {
    const b: *const syntax.Build = @ptrCast(@alignCast(raw));
    const mask = try a.alloc(bool, metadata.leafCount(meta));
    @memset(mask, false);
    mask[try metadata.resolveColumn(meta, b.key)] = true;
    for (b.columns) |name| mask[try metadata.resolveColumn(meta, name)] = true;
    for (b.semis) |s| mask[try metadata.resolveColumn(meta, s.fk)] = true;
    for (b.dims) |d| mask[try metadata.resolveColumn(meta, d.fk)] = true;
    if (b.filter) |node| {
        const filter = try filters.bindSyntax(a, node, meta, diag);
        var cols: std.ArrayList(usize) = .empty;
        try filter.collectColumns(&cols, a);
        for (cols.items) |ci| mask[ci] = true;
    }
    return .{ .projection = .{ .kept_set = mask } };
}

fn snapshotObject(hash: *std.crypto.hash.sha2.Sha256, origin: []const u8, path: []const u8, etag: []const u8, size: u64) void {
    snapshotWord(hash, origin.len);
    hash.update(origin);
    snapshotWord(hash, path.len);
    hash.update(path);
    // Whole-object and footer ETags have the same canonical representation in agreement.
    const canonical = std.mem.trim(u8, etag, "\"");
    snapshotWord(hash, canonical.len);
    hash.update(canonical);
    snapshotWord(hash, size);
}
fn snapshotWord(hash: *std.crypto.hash.sha2.Sha256, n: u64) void {
    var bytes: [8]u8 = undefined;
    std.mem.writeInt(u64, &bytes, n, .little);
    hash.update(&bytes);
}

/// A replicated dimension generation is part of partial agreement, alongside the probe's value schema.
pub fn replicaFingerprint(p: *const Prepared, schema_fingerprint: [16]u8) [16]u8 {
    if (p.replica_limit == null) return schema_fingerprint;
    var hash = std.crypto.hash.sha2.Sha256.init(.{});
    hash.update(&schema_fingerprint);
    for (p.tasks) |task| {
        var snapshot = task.snapshot;
        hash.update(&snapshot.finalResult());
    }
    return hash.finalResult()[0..16].*;
}

// Only bare, functionally dependent group keys can bypass the scan. Expressions and predicates keep
// their eager payloads. Deferred optional columns still validate their definition levels during the build.
fn deferredPayloads(a: std.mem.Allocator, query: ?*const syntax.Query, b: syntax.Build, fk: []const u8, meta: *const schema.FileMetaData, payload: []const usize) ![]const bool {
    const q = query orelse return &.{};
    const keys = q.keys orelse return &.{};
    const key_leaf = try metadata.resolveColumn(meta, b.key);
    const key_element = metadata.leafSchemaElement(meta, key_leaf) orelse return &.{};
    if ((lookup.KeyDomain.of(key_element) orelse return &.{}) != .signed) return &.{};
    const determines = for (keys) |k| {
        if (k.expr == .column and std.ascii.eqlIgnoreCase(k.expr.column, fk)) break true;
    } else false;
    if (!determines) return &.{};
    const out = try a.alloc(bool, payload.len);
    @memset(out, false);
    for (b.columns, payload, out) |name, leaf, *deferred| {
        const e = metadata.leafSchemaElement(meta, leaf) orelse continue;
        if (e.type != .BYTE_ARRAY or e.repetition_type == .REPEATED) continue;
        if ((e.effectiveLogicalType() orelse continue) != .STRING) continue;
        const bare = for (keys) |k| {
            if (k.expr == .column and std.ascii.eqlIgnoreCase(k.expr.column, name)) break true;
        } else false;
        if (!bare) continue;
        if (q.filter) |f| if (predicateReads(f, name)) continue;
        if (b.filter) |f| if (predicateReads(f, name)) continue;
        const read = for (q.calls) |c| {
            if (c.arg) |e2| if (exprReads(e2, name)) break true;
            if (c.arg2) |e2| if (exprReads(e2, name)) break true;
            if (c.where) |f| if (predicateReads(f, name)) break true;
        } else false;
        if (read) continue;
        const computed = for (keys) |k| {
            if (k.expr != .column and exprReads(k.expr, name)) break true;
        } else false;
        if (!computed) deferred.* = true;
    }
    return out;
}

fn exprReads(e: syntax.Expr, name: []const u8) bool {
    return switch (e) {
        .column => |n| std.ascii.eqlIgnoreCase(n, name),
        .number, .string => false,
        .negate, .cast_text => |x| exprReads(x.*, name),
        .binary => |x| exprReads(x.left.*, name) or exprReads(x.right.*, name),
        .call => |c| for (c.args) |arg| {
            if (exprReads(arg.*, name)) break true;
        } else false,
        .case_ => |c| blk: {
            for (c.whens) |w| {
                if (predicateReads(w.cond, name)) break :blk true;
                if (w.then) |x| if (exprReads(x.*, name)) break :blk true;
            }
            break :blk if (c.else_) |x| exprReads(x.*, name) else false;
        },
    };
}

fn predicateReads(f: syntax.Predicate, name: []const u8) bool {
    return switch (f) {
        .and_, .or_ => |children| for (children) |c| {
            if (predicateReads(c, name)) break true;
        } else false,
        .not_ => |x| predicateReads(x.*, name),
        inline .comparison, .in_list, .between => |x| std.ascii.eqlIgnoreCase(x.operand.column, name),
        inline .null_check, .like => |x| std.ascii.eqlIgnoreCase(x.column, name),
        .boolean => |n| std.ascii.eqlIgnoreCase(n, name),
        .expr_compare => |x| exprReads(x.left.*, name) or exprReads(x.right.*, name),
    };
}

fn fetchOf(base: usize, j: *const lookup.Join, p: usize) expr_ast.Fetch {
    const deferred = j.payloads[p].deferred;
    return .{ .base = base, .ctx = j, .payload = p, .value = lookup.fetch, .batch = if (deferred != null) lookup.fetchBatch else null, .scratch = if (deferred) |d| d.scratch else null };
}

fn deferredSyntax(source: plans.Source) ?*const syntax.Query {
    const q = @import("syntax_plan.zig").syntaxOf(source) orelse return null;
    const keys = q.keys orelse return null;
    const j = source.join orelse return null;
    // Keep the compact group-join when the main FK is a key. Replacing its slot arrays with
    // general grouping tables can turn an admitted bounded-memory query into a refusal.
    for (keys) |k| if (k.expr == .column and std.ascii.eqlIgnoreCase(k.expr.column, j.fk)) return null;
    return q;
}

test "a build file rewritten after its build refuses at the result cut" {
    if (comptime @import("../test_skip.zig").lacks(.posix)) return @import("../test_skip.zig").skipNeeding(.posix, "Windows disallows truncating a mapped file");
    var tmp = @import("../test_scratch.zig").tmpDir(.{});
    defer tmp.cleanup();
    var path_buf: [std.Io.Dir.max_path_bytes]u8 = undefined;
    const path = try std.fmt.bufPrint(&path_buf, "{s}/build.parquet", .{tmp.path});
    var fd = try local_fs.createFile(path);
    try local_fs.writeAll(fd, "PAR1 the build as it was read PAR1");
    local_fs.close(fd);
    var task: Task = undefined;
    task.maps = .empty;
    defer task.maps.deinit(std.testing.allocator);
    try task.maps.append(std.testing.allocator, .{ .map = try local_fs.mapFile(path), .path = path });
    defer task.maps.items[0].map.unmap();
    var p: Prepared = undefined;
    p.tasks = (&task)[0..1];
    try p.intact();
    fd = try local_fs.createFile(path);
    try local_fs.writeAll(fd, "PAR1 rewritten PAR1");
    local_fs.close(fd);
    try std.testing.expectError(error.InputChanged, p.intact());
}
