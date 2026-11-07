//! A query's join, as the engine runs it (0.6.0): the frontend resolved it by name (`syntax.Join`); here each build
//! side is read into a lookup (`core/join/lookup.zig`), charged to the query's ledger, the probe's footers gain the
//! build columns as virtual leaves (`extend`), and the probe query binds with the lookups as AND conjuncts of its
//! filter. Everything after that is the single-table scan.
const std = @import("std");
const schema = @import("../core/schema.zig");
const metadata = @import("../core/parquet/metadata.zig");
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
        self.maps.appendAssumeCapacity(opened.map);
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

const Opened = struct { map: local_fs.Mapped, meta: schema.FileMetaData };

/// Builds read local files in this release: a build on S3 refuses, naming it. The caller owns the mapping.
fn open(arena: std.mem.Allocator, path: []const u8) !Opened {
    if (std.mem.startsWith(u8, path, "s3://")) return error.JoinNotLocal;
    const m = try local_fs.mapFile(path);
    errdefer m.unmap();
    return .{ .map = m, .meta = try metadata.open(arena, m.bytes) };
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

        fn of(a: std.mem.Allocator, l: Leaf) !Lookup {
            const j = l.join;
            const columns = try a.alloc([]const u8, j.payloads.len);
            for (j.payloads, columns) |p, *c| c.* = try a.dupe(u8, p.name);
            const semis = try a.alloc(Lookup, l.semis.len);
            for (l.semis, semis) |s, *out| out.* = try of(a, s);
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
            };
        }
    };
};

/// One lookup conjunct of a filter: the FK it reads, by name, and the build it probes; with that build's own SEMI
/// joins, the inner tables of a chain (`--explain` shows them).
pub const Leaf = struct { fk: []const u8, join: *lookup.Join, semis: []const Leaf = &.{} };

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
    store: Store = undefined,
    /// The build files' mappings, held for the query: virtual leaves bind by the payloads' names, which the build
    /// footers own.
    maps: std.ArrayList(local_fs.Mapped) = .empty,
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
        for (self.semis) |s| a.filter = try withLookup(arena, a.filter, try leafOf(meta, s, diag));
        for (self.dims) |d| a.filter = try withLookup(arena, a.filter, try leafOf(meta, d, diag));
        a.filter = try withLookup(arena, a.filter, try leafOf(meta, self.main, diag));
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
        if (j.group) |g| g.order = self.order;
        return q;
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
        self.store.deinit();
        self.store = .init(self.charge.allocator());
        for (self.maps.items) |m| m.unmap();
        self.maps = .empty;
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

/// Read one build side (its own SEMI joins first) into the lookup conjunct on `fk`, charged to the ledger as it is
/// built.
fn build(p: *Prepared, arena: std.mem.Allocator, fk: []const u8, b: syntax.Build, kind: lookup.Kind, threads: usize, diag: ?*Diag) !Leaf {
    try p.maps.ensureUnusedCapacity(arena, 1);
    const file = open(arena, b.path) catch |err| {
        Diag.setInput(diag, b.path, err);
        return err;
    };
    p.maps.appendAssumeCapacity(file.map);
    const meta = try arena.create(schema.FileMetaData);
    meta.* = file.meta;
    var filter: ?filter_ast.Filter = if (b.filter) |f| try filters.bindSyntax(arena, f, meta, diag) else null;
    const semis = try arena.alloc(Leaf, b.semis.len);
    for (b.semis, semis) |s, *inner| {
        inner.* = try build(p, arena, s.fk, s.build, semiKind(s), threads, diag);
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
    const before = p.store.capacity();
    const j = lookup.build(p.store.allocator(), p.charge.allocator(), file.map.bytes, meta, .{
        .kind = kind,
        .build_name = b.path,
        .key_name = b.key,
        .key = key,
        .payload = payload,
        .filter = filter,
        .threads = threads,
        .budget = p.ledger.available(),
    }) catch |err| {
        Diag.setInput(diag, b.path, null);
        Diag.setColumn(diag, b.key);
        return if (p.charge.refusal(err) == error.ExceededMemoryBudget) error.JoinBuildExceedsBudget else err;
    };
    // The build read its file whole: one that changed meanwhile refuses, as a scanned input does.
    file.map.intact(b.path) catch |err| {
        Diag.setInput(diag, b.path, err);
        return err;
    };
    j.bytes = p.store.capacity() - before;
    p.charged += j.bytes;
    return .{ .fk = fk, .join = j, .semis = semis };
}

/// Read every build of `args`' join; null for a query without one. The probe's inputs must be local files too.
pub fn prepare(gpa: std.mem.Allocator, arena: std.mem.Allocator, args: types.QueryArgs, ledger: *core_memory.SharedBudget) !?*Prepared {
    const source = args.plan_source orelse return null;
    const sj = source.join orelse return null;
    for (args.inputs) |path| if (std.mem.startsWith(u8, path, "s3://")) {
        Diag.setInput(args.diag, path, null);
        return error.JoinNotLocal;
    };
    const threads = if (args.parallelism == 0) std.Thread.getCpuCount() catch 1 else args.parallelism;
    const p = try arena.create(Prepared);
    p.* = .{ .main = undefined, .semis = &.{}, .ledger = ledger, .gpa = gpa, .order = args.result_order };
    p.charge = .{ .child = gpa, .budget = ledger };
    p.store = .init(p.charge.allocator());
    // No group-join exists before the probe binds.
    errdefer p.releaseBuilds();
    const kind: lookup.Kind = switch (sj.kind) {
        inline else => |k| @field(lookup.Kind, @tagName(k)),
    };
    const main = try build(p, arena, sj.fk, sj.build, kind, threads, args.diag);
    main.join.main = true;
    main.join.publishes = true;
    const semis = try arena.alloc(Leaf, sj.semis.len);
    for (sj.semis, semis) |s, *out| out.* = try build(p, arena, s.fk, s.build, semiKind(s), threads, args.diag);
    // A star's dimensions publish after the main build, each with its own slots.
    const dims = try arena.alloc(Leaf, sj.dims.len);
    for (sj.dims, dims, 1..) |d, *out, slot| {
        out.* = try build(p, arena, d.fk, d.build, if (d.left) .left else .inner, threads, args.diag);
        out.join.publishes = true;
        out.join.slot_index = slot;
    }
    p.main = main;
    p.semis = semis;
    p.dims = dims;
    return p;
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
