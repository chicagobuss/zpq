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
/// read once; the build reads its file again here (`prepare`).
pub const LocalSchemas = struct {
    arena: std.mem.Allocator,
    seen: std.StringHashMapUnmanaged(*schema.FileMetaData) = .empty,

    pub fn footer(raw: *const anyopaque, path: []const u8) anyerror!*const schema.FileMetaData {
        const self: *LocalSchemas = @ptrCast(@alignCast(@constCast(raw)));
        if (self.seen.get(path)) |m| return m;
        const opened = try open(self.arena, path);
        const m = try self.arena.create(schema.FileMetaData);
        m.* = opened.meta;
        try self.seen.put(self.arena, path, m);
        return m;
    }
};

const Opened = struct { bytes: []const u8, meta: schema.FileMetaData };

/// Builds read local files in this release: a build on S3 refuses, naming it.
fn open(arena: std.mem.Allocator, path: []const u8) !Opened {
    if (std.mem.startsWith(u8, path, "s3://")) return error.JoinNotLocal;
    const m = try local_fs.mapFile(path);
    // Held for the query: the build decodes from this mapping.
    return .{ .bytes = m.bytes, .meta = try metadata.open(arena, m.bytes) };
}

/// One lookup conjunct of a filter: the FK it reads, by name, and the build it probes.
const Leaf = struct { fk: []const u8, join: *lookup.Join };

pub const Prepared = struct {
    /// The probe's own join: its build columns bind as virtual leaves, its group-join folds.
    main: Leaf,
    /// SEMI joins of the probe beside it.
    semis: []const Leaf,
    /// Every build's bytes, charged to the query's ledger; and a group-join's slots once one is admitted.
    charged: usize = 0,
    ledger: *core_memory.SharedBudget,
    gpa: std.mem.Allocator,
    order: expr_ast.ResultOrder,
    inner: plans.Source = undefined,

    /// The probe query's own binding, with the lookups ANDed into its filter. It binds against footers `extend`
    /// gave the build columns.
    pub fn source(self: *Prepared, inner: plans.Source) plans.Source {
        self.inner = inner;
        return .{ .context = self, .kind = inner.kind, .bind_fn = bind, .join = inner.join };
    }

    pub fn extend(self: *Prepared, arena: std.mem.Allocator, meta: *schema.FileMetaData) !void {
        try lookup.extend(self.main.join, arena, meta);
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
        a.filter = try withLookup(arena, a.filter, try leafOf(meta, self.main, diag));
        const j = self.main.join;
        if (j.group == null and j.kind == .inner and self.order.having == null and self.order.hidden.len == 0) {
            if (try groupJoin(self.gpa, j, a.*, meta)) |g| {
                const need = lookup.GroupJoin.bytesFor(j.n, g.calls);
                _ = self.ledger.drawExact(need) orelse {
                    g.deinit();
                    Diag.setInput(diag, j.build_name, null);
                    Diag.setColumn(diag, j.key_name);
                    return error.JoinBuildExceedsBudget;
                };
                self.charged += need;
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

    pub fn release(self: *Prepared) void {
        self.ledger.release(self.charged);
        self.charged = 0;
    }
};

fn leafOf(meta: *const schema.FileMetaData, l: Leaf, diag: ?*Diag) !filter_ast.Filter {
    const fk = metadata.resolveColumn(meta, l.fk) catch |err| {
        Diag.setColumn(diag, l.fk);
        return err;
    };
    if (l.join.main) l.join.fk_leaf = fk;
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
            .sum, .count, .min, .max => {},
            else => return null,
        }
        if (c.result != .f64 and c.result != .i64) return null;
    }
    for (outputs) |o| if (o.derived != null) return null;
    const G = lookup.GroupJoin;
    const leaves = try gpa.alloc(usize, keys.len);
    for (keys, leaves) |k, *leaf| leaf.* = k.col_ref.col_idx;
    const calls = try gpa.alloc(G.Call, q.calls.len);
    for (q.calls, calls) |c, *out| out.* = .{
        .func = switch (c.func) {
            .sum => .sum,
            .count => .count,
            .min => .min,
            .max => .max,
            else => unreachable,
        },
        .arg = c.arg,
        .lane = if (c.result == .f64) .f64 else .i64,
    };
    const outs = try gpa.alloc(G.Output, outputs.len);
    for (outputs, outs) |o, *out| out.* = switch (o.source) {
        .key => |k| .{ .key = k },
        .agg => |a| .{ .agg = a },
    };
    return try G.init(gpa, j, leaves, calls, outs, meta.row_groups.items.len);
}

/// Read one build side (its own SEMI joins first) into a lookup, charged to `ledger`.
fn build(p: *Prepared, arena: std.mem.Allocator, b: syntax.Build, kind: lookup.Kind, threads: usize, diag: ?*Diag) !*lookup.Join {
    const file = open(arena, b.path) catch |err| {
        Diag.setInput(diag, b.path, err);
        return err;
    };
    const meta = try arena.create(schema.FileMetaData);
    meta.* = file.meta;
    var filter: ?filter_ast.Filter = if (b.filter) |f| try filters.bindSyntax(arena, f, meta, diag) else null;
    for (b.semis) |s| {
        const inner = try build(p, arena, s.build, .semi, threads, diag);
        filter = try withLookup(arena, filter, try leafOf(meta, .{ .fk = s.fk, .join = inner }, diag));
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
    const j = lookup.build(arena, p.gpa, file.bytes, meta, .{
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
        return err;
    };
    _ = p.ledger.drawExact(j.bytes) orelse {
        Diag.setInput(diag, b.path, null);
        Diag.setColumn(diag, b.key);
        return error.JoinBuildExceedsBudget;
    };
    p.charged += j.bytes;
    return j;
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
    errdefer p.release();
    const main = try build(p, arena, sj.build, if (sj.kind == .semi) .semi else .inner, threads, args.diag);
    main.main = true;
    const semis = try arena.alloc(Leaf, sj.semis.len);
    for (sj.semis, semis) |s, *out| out.* = .{ .fk = s.fk, .join = try build(p, arena, s.build, .semi, threads, args.diag) };
    p.main = .{ .fk = sj.fk, .join = main };
    p.semis = semis;
    return p;
}
