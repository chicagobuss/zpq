const std = @import("std");
const schema = @import("../core/schema.zig");
const metadata = @import("../core/parquet/metadata.zig");
const consumer = @import("../core/consumer.zig");
const scan = @import("../core/scan.zig");
const expr_ast = @import("../core/expr/ast.zig");
const expr_parser = @import("../core/expr/parser.zig");
const filter_ast = @import("../core/filter/ast.zig");
const filter_parser = @import("../core/filter/parser.zig");
const Diag = @import("../core/diag.zig").Diag;
const QueryArgs = @import("types.zig").QueryArgs;
const RowSortKey = @import("types.zig").RowSortKey;

/// Set `set[i]` for each leaf a `--columns` name selects. A name that selects nothing is `UnknownColumn`, named
/// through `diag`, as in --select and --filter: dropping it wrote a file short of that column, or of every column.
/// A list naming no column at all (`--columns ','`) is `EmptyColumnList`, not a file with no columns.
/// The leaves of a SQL SELECT list that is bare columns alone, at least one of them a STRUCT, LIST or MAP, in the
/// file's order: the projection `--columns` would make of it, which keeps groups whole. Null for any other list
/// (an alias, an expression, a leaf out of order, a list of leaves alone), which the expression path takes.
fn nestedProjection(
    arena: std.mem.Allocator,
    meta: *const schema.FileMetaData,
    select_cols: ?[]const expr_ast.SelectColumn,
    num_leaves: usize,
) !?[]bool {
    const cols = select_cols orelse return null;
    if (cols.len == 0) return null;
    const set = try arena.alloc(bool, num_leaves);
    @memset(set, false);
    var any_group = false;
    var next: usize = 0;
    for (cols) |col| {
        if (col.alias != null) return null;
        const leaves = metadata.resolveProjection(arena, meta, col.expr) catch return null;
        if (leaves.len == 0) return null;
        _ = metadata.resolveColumn(meta, metadata.unquoteIdent(col.expr)) catch |err| switch (err) {
            error.UnknownColumn => any_group = true,
            else => return null,
        };
        for (leaves) |leaf| {
            if (leaf < next or leaf >= num_leaves) return null;
            set[leaf] = true;
            next = leaf + 1;
        }
    }
    return if (any_group) set else null;
}

fn markColumns(
    arena: std.mem.Allocator,
    meta: *const schema.FileMetaData,
    names: []const []const u8,
    set: []bool,
    diag: ?*Diag,
) !void {
    if (names.len == 0) return error.EmptyColumnList;
    for (names) |name| {
        const indices = metadata.resolveProjection(arena, meta, name) catch |err| {
            if (err == error.AmbiguousColumn) Diag.setColumn(diag, name);
            return err;
        };
        if (indices.len == 0) {
            Diag.setColumn(diag, name);
            return error.UnknownColumn;
        }
        for (indices) |idx| if (idx < set.len) {
            set[idx] = true;
        };
    }
}

/// Output names of a file's leaves (`metadata.leafLabels`), computed once per query on first use: labelling a
/// leaf at a time walks the schema per leaf.
const Labels = struct {
    arena: std.mem.Allocator,
    meta: *const schema.FileMetaData,
    diag: ?*Diag,
    all: ?metadata.LeafLabels = null,

    fn get(self: *Labels) !metadata.LeafLabels {
        if (self.all == null) self.all = try metadata.leafLabels(self.arena, self.meta);
        return self.all.?;
    }

    /// The label of leaf `leaf_idx`, reporting a leaf that has none: one a top-level column takes even the quoted
    /// path of.
    fn of(self: *Labels, leaf_idx: usize) ![]const u8 {
        const all = try self.get();
        if (leaf_idx >= all.names.len) return error.SchemaMismatch;
        return all.names[leaf_idx] orelse {
            const segments = (try metadata.leafPathSegments(self.arena, self.meta, leaf_idx)).?;
            Diag.setColumn(self.diag, try metadata.quotePath(self.arena, segments));
            return error.UnlabelledColumn;
        };
    }

    /// The label of a nested leaf, or null for a top-level column — whose label is its own name, dots included.
    fn ofNested(self: *Labels, leaf_idx: usize) !?[]const u8 {
        const all = try self.get();
        if (leaf_idx >= all.nested.len or !all.nested[leaf_idx]) return null;
        return try self.of(leaf_idx);
    }
};

/// The name each output column of a flat row result is printed under: an alias, or `metadata.leafLabel` of a
/// passthrough leaf, which no other leaf prints as.
fn outputColumnNames(
    arena: std.mem.Allocator,
    labels: *Labels,
    specs: []const consumer.OutputCol,
) ![]const []const u8 {
    const names = try arena.alloc([]const u8, specs.len);
    for (specs, names) |spec, *name| name.* = switch (spec) {
        .passthrough => |pt| pt.name orelse try labels.of(pt.ci),
        .computed => |c| c.alias,
    };
    return names;
}

fn sameName(name: []const u8) []const u8 {
    return name;
}

/// What a write or print outputs and reads, parsed from --columns / --select / --filter against the first file's
/// footer. Output-format restrictions, nested printing, and sink and codec choice stay with each consumer.
pub const ProjectionPlan = struct {
    /// Null without --select.
    select_items: ?[]expr_ast.SelectItem,
    filter: ?filter_ast.Filter,
    /// The --columns selection; null when no --columns narrows the leaves.
    kept_set: ?[]const bool,
    /// The leaves kept: `kept_set`, or every leaf.
    kept: []const bool,
    output_specs: []const consumer.OutputCol,
    /// Some output column is an expression or a renamed leaf rather than a leaf copied under its own name.
    any_computed: bool,
    /// Leaves to decode: those the output references plus the filter's and the row sort keys'.
    fetch: []const bool,
    labels: Labels,
    /// SQL ORDER BY over plain rows (`ResultOrder.rows`), bound to input leaves.
    row_keys: []const RowSortKey = &.{},

    /// The name each output column is printed under.
    pub fn names(self: *ProjectionPlan, arena: std.mem.Allocator) ![]const []const u8 {
        return outputColumnNames(arena, &self.labels, self.output_specs);
    }
};

pub fn planProjection(arena: std.mem.Allocator, args: QueryArgs, meta0: *const schema.FileMetaData) !ProjectionPlan {
    const num_leaves = metadata.leafCount(meta0);
    const bound = if (try args.bound(arena, meta0)) |query| switch (query.*) {
        .projection => |*p| p,
        .aggregate => return error.InvalidBoundPlan,
    } else null;

    // --columns resolves to leaf indices; --select is mutually exclusive with it (checked by the callers).
    var kept_set: ?[]const bool = null;
    var select = args.select;
    if (bound) |p| {
        kept_set = p.kept_set;
    } else if (args.columns) |cols_list| {
        const set = try arena.alloc(bool, num_leaves);
        @memset(set, false);
        try markColumns(arena, meta0, cols_list, set, args.diag);
        kept_set = set;
    } else if (args.output != null) if (try nestedProjection(arena, meta0, args.select_cols, num_leaves)) |set| {
        // A written SELECT of bare columns naming a STRUCT, LIST or MAP is the projection that keeps it whole.
        kept_set = set;
        select = null;
    };
    const select_items: ?[]expr_ast.SelectItem = if (bound) |p| p.select_items else if (select) |s|
        try expr_parser.parseSelect(arena, s, meta0, args.diag)
    else
        null;
    var filter: ?filter_ast.Filter = if (bound) |p| p.filter else null;
    if (bound == null) if (args.filter) |fs| if (fs.len > 0) {
        filter = try bindFilter(arena, fs, meta0, args);
    };

    const kept = try arena.alloc(bool, num_leaves);
    if (kept_set) |s| @memcpy(kept, s) else @memset(kept, true);
    const fetch = try arena.alloc(bool, num_leaves);
    @memset(fetch, false);

    var labels: Labels = .{ .arena = arena, .meta = meta0, .diag = args.diag };
    var output_specs: std.ArrayList(consumer.OutputCol) = .empty;
    var any_computed = false;
    if (select_items) |items| {
        for (items) |item| {
            switch (item.expr) {
                .col_ref => |c| {
                    // The select output is flat, so a nested leaf is written under its label (its dotted path)
                    // rather than its leaf name, which a top-level column may share.
                    const nested_name: ?[]const u8 =
                        if (item.alias == null) try labels.ofNested(c.col_idx) else null;
                    if (item.alias orelse nested_name) |alias| {
                        const source = metadata.leafSchemaElement(meta0, c.col_idx) orelse return error.SchemaLookupFailed;
                        if (source.logical_type_raw.len > 0 and source.logical_type != null and
                            source.logical_type.? == .UNKNOWN and (source.type == .INT32 or source.type == .FLOAT))
                            return error.UnsupportedLogicalTypeReencode;
                        // A rename: the same leaf, flat under its new name (`consumer.OutputCol.Passthrough`).
                        try output_specs.append(arena, .{ .passthrough = .{ .ci = c.col_idx, .name = alias } });
                    } else {
                        try output_specs.append(arena, .{ .passthrough = .{ .ci = c.col_idx } });
                    }
                    if (c.col_idx < num_leaves) fetch[c.col_idx] = true;
                },
                else => {
                    const alias = item.alias orelse return error.MissingOutputOrAggregate; // alias required
                    try output_specs.append(arena, .{ .computed = .{ .expr = item.expr, .alias = alias } });
                    any_computed = true;
                    item.expr.collectColumns(fetch);
                },
            }
        }
    } else {
        for (kept, 0..) |b, i| if (b) {
            try output_specs.append(arena, .{ .passthrough = .{ .ci = i } });
            fetch[i] = true;
        };
    }
    if (filter) |f| {
        var filter_cols: std.ArrayList(usize) = .empty;
        try f.collectColumns(&filter_cols, arena);
        for (filter_cols.items) |ci| {
            if (ci < num_leaves) fetch[ci] = true;
        }
    }
    const row_keys: []const RowSortKey = if (bound) |p| p.row_keys else blk: {
        const keys = try arena.alloc(RowSortKey, args.result_order.rows.len);
        for (args.result_order.rows, keys, 0..) |rk, *key, k| {
            const e = try expr_parser.parseExprOnly(arena, rk.expr, meta0, args.diag);
            key.* = try rowSortKey(arena, meta0, e, rk.descending, k, rk.expr, args.diag);
        }
        break :blk keys;
    };
    for (row_keys) |key| switch (key.spec) {
        .passthrough => |p| if (p.ci < fetch.len) {
            fetch[p.ci] = true;
        },
        .computed => |c| c.expr.collectColumns(fetch),
    };
    // A --select names its outputs, so two may clash, as aggregate and GROUP BY outputs may; a plain copy keeps the
    // source's own names and is not checked.
    if (select_items != null) {
        const names = try outputColumnNames(arena, &labels, output_specs.items);
        try scan.checkDistinctOutputNames([]const u8, names, sameName, args.diag);
    }

    return .{
        .select_items = select_items,
        .filter = filter,
        .kept_set = kept_set,
        .kept = kept,
        .output_specs = output_specs.items,
        .any_computed = any_computed,
        .fetch = fetch,
        .labels = labels,
        .row_keys = row_keys,
    };
}

/// The row-order consumer accepts the same bound expression from SQL or the fragment boundary.
pub fn rowSortKey(
    arena: std.mem.Allocator,
    meta: *const schema.FileMetaData,
    e: expr_ast.Expr,
    descending: bool,
    ordinal: usize,
    display: []const u8,
    diag: ?*Diag,
) !RowSortKey {
    if (e == .col_ref) {
        const ci = e.col_ref.col_idx;
        const path = (try metadata.leafPathSegments(arena, meta, ci)) orelse return error.UnknownColumn;
        if (meta.getColumnLevels(path).max_rep > 0) {
            Diag.setColumn(diag, display);
            return error.NestedRowOutputNotSupported;
        }
        const elem = metadata.leafSchemaElement(meta, ci) orelse return error.UnknownColumn;
        return .{ .spec = .{ .passthrough = .{ .ci = ci } }, .descending = descending, .unsigned = schema.isUnsignedInt(elem) };
    }
    const alias = try std.fmt.allocPrint(arena, "__order_{d}", .{ordinal});
    return .{ .spec = .{ .computed = .{ .expr = e, .alias = alias } }, .descending = descending, .unsigned = false };
}

/// The query's filter bound against `meta`. Under `--trust-stats` its leaves trust plausible statistics, and a
/// FLOAT/DOUBLE chunk without a NaN count prunes as if it had none (`ast.trustStats`); every row-group and page
/// decision reads that from the leaves.
pub fn bindFilter(arena: std.mem.Allocator, text: []const u8, meta: *const schema.FileMetaData, args: QueryArgs) !filter_ast.Filter {
    const f = try filter_parser.parse(arena, text, meta, args.diag);
    return if (args.trust_stats) filter_ast.trustStats(arena, f) else f;
}
