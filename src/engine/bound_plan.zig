//! Query-local handoff from a frontend binder to I/O planning and execution.
const std = @import("std");
const schema = @import("../core/schema.zig");
const metadata = @import("../core/parquet/metadata.zig");
const expr_ast = @import("../core/expr/ast.zig");
const filter_ast = @import("../core/filter/ast.zig");
const consumer = @import("../core/consumer.zig");
const scan_types = @import("../core/scan/types.zig");
const Diag = @import("../core/diag.zig").Diag;

pub const Kind = enum { rows, scalar, grouped };
pub const Aggregate = scan_types.BoundAggregate;

pub const RowSortKey = struct {
    spec: consumer.OutputCol,
    descending: bool,
    unsigned: bool,
};

pub const Projection = struct {
    select_items: ?[]expr_ast.SelectItem = null,
    filter: ?filter_ast.Filter = null,
    row_keys: []const RowSortKey = &.{},
    /// Whole nested fields keep the original lossless column-copy path.
    kept_set: ?[]const bool = null,
};

pub const Query = union(enum) {
    aggregate: scan_types.BoundAggregate,
    projection: Projection,

    pub fn filter(self: *const Query) ?filter_ast.Filter {
        return switch (self.*) {
            .aggregate => |a| a.filter,
            .projection => |p| p.filter,
        };
    }
};

pub const Options = struct {
    trust_stats: bool,
    nested_output: bool,
};

/// Immutable syntax may be reused; the cache belongs to one invocation, never to this source.
pub const Source = struct {
    context: *const anyopaque,
    kind: Kind,
    // Frontend extensions have their own binding errors; this stored callback preserves them unchanged.
    bind_fn: *const fn (*const anyopaque, std.mem.Allocator, *const schema.FileMetaData, ?*Diag, Options) anyerror!Query,
};

pub const Cache = struct {
    query: ?Query = null,
    schema_items: []const schema.SchemaElement = &.{},
    extra: ?*Entry = null,

    const Entry = struct { query: Query, schema_items: []const schema.SchemaElement, next: ?*Entry };

    fn sameBinding(a: []const schema.SchemaElement, b: []const schema.SchemaElement) bool {
        if (a.ptr == b.ptr and a.len == b.len) return true;
        if (metadata.schemaMismatch(a, b, .exact) != null) return false;
        // Output labels retain the first input's spelling even when schemas compare case-insensitively.
        for (a[@min(a.len, 1)..], b[@min(b.len, 1)..]) |x, y| if (!std.mem.eql(u8, x.name, y.name)) return false;
        return true;
    }

    fn bind(source: Source, arena: std.mem.Allocator, meta: *const schema.FileMetaData, diag: ?*Diag, options: Options) !Query {
        var query = try source.bind_fn(source.context, arena, meta, diag, options);
        switch (query) {
            .projection => if (source.kind != .rows) return error.InvalidBoundPlan,
            .aggregate => |a| {
                if (source.kind == .rows or (a.keys != null) != (source.kind == .grouped))
                    return error.InvalidBoundPlan;
                if ((a.keys == null) != (a.outputs == null)) return error.InvalidBoundPlan;
            },
        }
        if (options.trust_stats) switch (query) {
            .aggregate => |*a| if (a.filter) |f| {
                a.filter = try filter_ast.trustStats(arena, f);
            },
            .projection => |*p| if (p.filter) |f| {
                p.filter = try filter_ast.trustStats(arena, f);
            },
        };
        return query;
    }

    pub fn get(
        self: *Cache,
        source: Source,
        arena: std.mem.Allocator,
        meta: *const schema.FileMetaData,
        diag: ?*Diag,
        options: Options,
    ) !*const Query {
        if (self.query == null) {
            self.query = try bind(source, arena, meta, diag, options);
            self.schema_items = meta.schema.items;
        }
        if (sameBinding(self.schema_items, meta.schema.items)) return &self.query.?;
        const mode: metadata.SchemaMatch = if (source.kind == .rows) .exact else .values;
        if (metadata.schemaMismatch(self.schema_items, meta.schema.items, mode) != null)
            return error.SchemaMismatch;
        var entry = self.extra;
        while (entry) |e| : (entry = e.next) {
            if (sameBinding(e.schema_items, meta.schema.items)) return &e.query;
        }
        // Mixed local/S3 inputs may have compatible values but different physical types or label spelling.
        // Bind the immutable syntax once for that schema; never reuse an INT32 proof for an INT64 column.
        const query = try bind(source, arena, meta, diag, options);
        const e = try arena.create(Entry);
        e.* = .{ .query = query, .schema_items = meta.schema.items, .next = self.extra };
        self.extra = e;
        return &e.query;
    }
};
