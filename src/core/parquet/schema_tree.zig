//! Tree representation of a Parquet schema for nested-type support.
//!
//! The on-wire thrift schema is a flat DFS-ordered list of
//! SchemaElements. For flat schemas, every non-root element is a leaf
//! and the index into the list (minus 1) is also the column-chunk
//! index — so direct list access works. For nested schemas (struct,
//! list, map), GROUP nodes are interleaved into the list and the
//! direct-index assumption silently produces wrong column lookups.
//!
//! This module builds a tree once at `metadata.open` time and
//! provides the navigation and projection primitives that nested-
//! aware code paths need. The flat thrift list (in `core/schema.zig`)
//! stays as the on-wire serialisation; we round-trip back to it via
//! `writeFlatThrift` at footer-write time.
//!
//! Comparable implementations:
//!   - hardwood's `dev.hardwood.schema.SchemaNode` /
//!     `FileSchema.columnPathToIndex` (sealed-interface tree + map)
//!   - polars-parquet's `SchemaDescriptor` (`fields: Vec<ParquetType>`
//!     for the tree + `leaves: Vec<ColumnDescriptor>` for chunk-index
//!     access)
//!
//! Both build a tree at parse time and treat the flat thrift as a
//! serialisation format only. We do the same.

const std = @import("std");
const schema = @import("../schema.zig");

pub const Error = error{
    EmptySchema,
    MalformedListAnnotation,
    MalformedMapAnnotation,
    DuplicatePath,
    TruncatedFlat,
    BadLeafIndex,
} || std.mem.Allocator.Error;

pub const GroupKind = enum {
    /// Plain struct: zero or more children, no LIST/MAP annotation.
    struct_,
    /// LIST: child is `[REPEATED group { element_or_item }]`.
    list,
    /// Modern MAP: child is `[REPEATED group { key, value }]`. We
    /// fold the legacy `MAP_KEY_VALUE` converted-type variant into
    /// this same kind — the wire shape is identical.
    map,
    /// Future-proofing for parquet 2.13 Variant logical type. Not
    /// yet emitted by the builder; defined here so consumers can
    /// switch on the full enum without it changing under them.
    variant,
};

pub const PrimitiveNode = struct {
    /// Leaf field name. Borrowed from the source thrift bytes (which
    /// outlive the tree under our arena lifecycle).
    name: []const u8,
    /// Full path from root, in DFS order. Each element borrowed from
    /// the source thrift. `path[path.len - 1] == name`.
    path: []const []const u8,
    type: schema.Type,
    repetition: schema.FieldRepetitionType,
    logical_type: ?schema.LogicalType = null,
    converted_type: ?schema.ConvertedType = null,
    type_length: ?i32 = null,
    scale: ?i32 = null,
    precision: ?i32 = null,
    field_id: ?i32 = null,
    /// Index into a row group's `columns` list. Same number that
    /// existing fastpath / metadata code uses for "column index."
    column_index: u32,
    /// Pre-computed level bounds. `max_def` accounts for every
    /// non-REQUIRED ancestor + self; `max_rep` counts REPEATED.
    max_def: u8,
    max_rep: u8,
};

pub const GroupNode = struct {
    name: []const u8,
    repetition: schema.FieldRepetitionType,
    logical_type: ?schema.LogicalType = null,
    converted_type: ?schema.ConvertedType = null,
    children: []const Node,
    kind: GroupKind,
};

pub const Node = union(enum) {
    primitive: PrimitiveNode,
    group: GroupNode,

    pub fn name(self: Node) []const u8 {
        return switch (self) {
            .primitive => |p| p.name,
            .group => |g| g.name,
        };
    }

    pub fn repetition(self: Node) schema.FieldRepetitionType {
        return switch (self) {
            .primitive => |p| p.repetition,
            .group => |g| g.repetition,
        };
    }
};

/// Full tree built from a parsed thrift schema list.
pub const SchemaTree = struct {
    /// Root group. `root.name` is whatever the thrift recorded as the
    /// schema's "message" name (typically "schema" or
    /// "spark_schema"). `root.children` are the top-level fields.
    root: GroupNode,
    /// All leaves in column-chunk order. `leaves[i]` is the
    /// PrimitiveNode for column chunk `i` in any row group.
    leaves: []const PrimitiveNode,
    /// Captures whether the source thrift had `repetition_type = null`
    /// or some explicit value on the root element. Round-trip lossless.
    root_source_repetition: ?schema.FieldRepetitionType = null,

    /// Build the tree from a parsed flat thrift schema list.
    /// `flat[0]` must be the root GROUP. All allocations land on
    /// `arena`; the tree's lifetime equals the arena's.
    pub fn build(
        arena: std.mem.Allocator,
        flat: []const schema.SchemaElement,
    ) Error!SchemaTree {
        if (flat.len == 0) return error.EmptySchema;

        var b: Builder = .{
            .arena = arena,
            .flat = flat,
            .pos = 0,
        };

        // Root: pop directly so the path stack starts empty.
        const root_elem = flat[b.pos];
        b.pos += 1;
        const root_children_count: usize = if (root_elem.num_children) |nc|
            @intCast(nc)
        else
            0;
        const root_children = try b.buildChildren(root_children_count, 0, 0);

        const tree = SchemaTree{
            // Root preserves whatever the source had (some writers
            // record `repetition_type = null` here, others write
            // REQUIRED — we round-trip whichever).
            .root_source_repetition = root_elem.repetition_type,
            .root = .{
                .name = root_elem.name,
                .repetition = root_elem.repetition_type orelse .REQUIRED,
                .logical_type = root_elem.logical_type,
                .converted_type = root_elem.converted_type,
                .children = root_children,
                .kind = .struct_,
            },
            .leaves = try b.leaves.toOwnedSlice(arena),
        };

        if (b.pos != flat.len) return error.TruncatedFlat;
        return tree;
    }

    /// Build a new SchemaTree containing only the kept leaves and
    /// every node on a path from root to those leaves. Output
    /// preserves `repetition_type`, `logical_type`, `converted_type`
    /// at every level — list/map annotations survive.
    pub fn projectSubset(
        self: *const SchemaTree,
        arena: std.mem.Allocator,
        kept_leaf_indices: []const u32,
    ) Error!SchemaTree {
        // Validate input.
        for (kept_leaf_indices) |idx| {
            if (idx >= self.leaves.len) return error.BadLeafIndex;
        }

        // Mark which leaves to keep, and collect "needed" ancestor
        // paths (every prefix of every kept leaf's path).
        var keep_mask = try arena.alloc(bool, self.leaves.len);
        defer arena.free(keep_mask);
        @memset(keep_mask, false);
        for (kept_leaf_indices) |idx| keep_mask[idx] = true;

        // Walk the tree, emitting only nodes that have at least one
        // kept descendant (or are themselves kept primitives).
        var pp: Projector = .{
            .arena = arena,
            .keep_mask = keep_mask,
            .next_column_index = 0,
            .leaves_out = .empty,
            .path_stack = .empty,
        };
        defer pp.path_stack.deinit(arena);

        const new_children = try pp.projectChildren(self.root.children);
        return SchemaTree{
            .root_source_repetition = self.root_source_repetition,
            .root = .{
                .name = self.root.name,
                .repetition = self.root.repetition,
                .logical_type = self.root.logical_type,
                .converted_type = self.root.converted_type,
                .children = new_children,
                .kind = .struct_,
            },
            .leaves = try pp.leaves_out.toOwnedSlice(arena),
        };
    }

    /// Serialise the tree back to a flat thrift schema list, suitable
    /// for `FileMetaData.schema`. DFS order matches what the parquet
    /// thrift expects.
    pub fn writeFlatThrift(
        self: *const SchemaTree,
        arena: std.mem.Allocator,
    ) Error!std.ArrayListUnmanaged(schema.SchemaElement) {
        var out: std.ArrayListUnmanaged(schema.SchemaElement) = .empty;
        const root_count: i32 = @intCast(self.root.children.len);
        try out.append(arena, .{
            .type = null,
            .type_length = null,
            // Preserve whatever the source had (writer-specific).
            .repetition_type = self.root_source_repetition,
            .name = self.root.name,
            .num_children = root_count,
            .converted_type = self.root.converted_type,
            .logical_type = self.root.logical_type,
            .scale = null,
            .precision = null,
            .field_id = null,
        });
        for (self.root.children) |child| try writeNodeFlat(arena, child, &out);
        return out;
    }

    /// `parquet-tools schema`-style debug formatter. Compatible with
    /// hardwood's `schema` subcommand output.
    pub fn format(self: SchemaTree, writer: *std.Io.Writer) std.Io.Writer.Error!void {
        try writer.print("message {s} {{\n", .{self.root.name});
        for (self.root.children) |child| try formatNode(child, writer, 1);
        try writer.writeAll("}\n");
    }
};

// ============================================================
// Builder — single DFS pass over the flat thrift list
// ============================================================

const Builder = struct {
    arena: std.mem.Allocator,
    flat: []const schema.SchemaElement,
    pos: usize = 0,
    leaves: std.ArrayList(PrimitiveNode) = .empty,
    /// `.`-joined path of every leaf so far, to find a duplicate without comparing every pair.
    joined_paths: std.StringHashMapUnmanaged(void) = .empty,
    /// Active path during DFS; `path_stack.items` is a snapshot of
    /// the path from root down to the current cursor.
    path_stack: std.ArrayList([]const u8) = .empty,

    fn buildChildren(
        self: *Builder,
        n: usize,
        parent_def: u8,
        parent_rep: u8,
    ) Error![]const Node {
        // A child count past the end of the flat list is truncated, whatever it claims; don't allocate for it.
        if (n > self.flat.len - self.pos) return error.TruncatedFlat;
        const out = try self.arena.alloc(Node, n);
        var i: usize = 0;
        while (i < n) : (i += 1) {
            out[i] = try self.buildOne(parent_def, parent_rep);
        }
        return out;
    }

    fn buildOne(self: *Builder, parent_def: u8, parent_rep: u8) Error!Node {
        if (self.pos >= self.flat.len) return error.TruncatedFlat;
        const elem = self.flat[self.pos];
        self.pos += 1;

        // Accumulate def/rep from this element's own repetition type.
        // Per parquet: max_def += 1 unless REQUIRED; max_rep += 1 if REPEATED.
        const rt: schema.FieldRepetitionType = elem.repetition_type orelse .REQUIRED;
        // Footer open bounds the nesting depth (metadata.max_schema_depth); checked anyway, since a tree can be
        // built from a schema list that never went through it.
        const here_def: u8 = std.math.add(u8, parent_def, if (rt == .REQUIRED) 0 else 1) catch return error.TruncatedFlat;
        const here_rep: u8 = std.math.add(u8, parent_rep, if (rt == .REPEATED) 1 else 0) catch return error.TruncatedFlat;

        try self.path_stack.append(self.arena, elem.name);
        defer _ = self.path_stack.pop();

        const has_children = if (elem.num_children) |nc| nc > 0 else false;

        if (!has_children) {
            // Primitive leaf.
            const path_copy = try self.arena.dupe([]const u8, self.path_stack.items);
            const column_index: u32 = @intCast(self.leaves.items.len);

            const phys_type = elem.type orelse return error.TruncatedFlat;
            const leaf: PrimitiveNode = .{
                .name = elem.name,
                .path = path_copy,
                .type = phys_type,
                .repetition = rt,
                .logical_type = elem.logical_type,
                .converted_type = elem.converted_type,
                .type_length = elem.type_length,
                .scale = elem.scale,
                .precision = elem.precision,
                .field_id = elem.field_id,
                .column_index = column_index,
                .max_def = here_def,
                .max_rep = here_rep,
            };
            try self.leaves.append(self.arena, leaf);
            try checkLeafPath(self.arena, &self.joined_paths, self.leaves.items, column_index);

            return .{ .primitive = leaf };
        }

        // Group.
        const child_count: usize = @intCast(elem.num_children.?);
        const children = try self.buildChildren(child_count, here_def, here_rep);
        const kind = classifyGroupKind(elem, children);
        return .{ .group = .{
            .name = elem.name,
            .repetition = rt,
            .logical_type = elem.logical_type,
            .converted_type = elem.converted_type,
            .children = children,
            .kind = kind,
        } };
    }
};

fn classifyGroupKind(
    elem: schema.SchemaElement,
    _: []const Node,
) GroupKind {
    if (elem.converted_type) |ct| switch (ct) {
        .LIST => return .list,
        .MAP, .MAP_KEY_VALUE => return .map,
        else => {},
    };
    if (elem.logical_type) |lt| switch (lt) {
        .LIST => return .list,
        .MAP => return .map,
        else => {},
    };
    return .struct_;
}

/// Reject `leaves[idx]` if an earlier leaf has its path, segment for segment: a duplicate field. `seen` holds the
/// `.`-joined paths so far. Distinct paths can join alike (a top-level column `a.b` and the field `b` of a group `a`),
/// so only a joined collision, which is rare, costs a rescan.
fn checkLeafPath(
    arena: std.mem.Allocator,
    seen: *std.StringHashMapUnmanaged(void),
    leaves: []const PrimitiveNode,
    idx: u32,
) Error!void {
    const path = leaves[idx].path;
    const gop = try seen.getOrPut(arena, try joinPath(arena, path));
    if (!gop.found_existing) return;
    for (leaves[0..idx]) |l| if (pathsEql(l.path, path)) return error.DuplicatePath;
}

fn pathsEql(a: []const []const u8, b: []const []const u8) bool {
    if (a.len != b.len) return false;
    for (a, b) |x, y| if (!std.mem.eql(u8, x, y)) return false;
    return true;
}

fn joinPath(arena: std.mem.Allocator, parts: []const []const u8) Error![]const u8 {
    if (parts.len == 0) return "";
    var total: usize = 0;
    for (parts) |p| total += p.len;
    total += parts.len - 1; // separators
    const out = try arena.alloc(u8, total);
    var pos: usize = 0;
    for (parts, 0..) |p, i| {
        if (i > 0) {
            out[pos] = '.';
            pos += 1;
        }
        @memcpy(out[pos..][0..p.len], p);
        pos += p.len;
    }
    return out;
}

// ============================================================
// Projector — builds a new tree from a kept-leaves mask
// ============================================================

const Projector = struct {
    arena: std.mem.Allocator,
    keep_mask: []const bool,
    next_column_index: u32,
    leaves_out: std.ArrayList(PrimitiveNode),
    path_stack: std.ArrayList([]const u8),

    fn projectChildren(self: *Projector, children: []const Node) Error![]const Node {
        var out: std.ArrayList(Node) = .empty;
        for (children) |child| {
            if (try self.projectNode(child)) |projected| {
                try out.append(self.arena, projected);
            }
        }
        return out.toOwnedSlice(self.arena);
    }

    fn projectNode(self: *Projector, node: Node) Error!?Node {
        try self.path_stack.append(self.arena, node.name());
        defer _ = self.path_stack.pop();

        switch (node) {
            .primitive => |p| {
                if (!self.keep_mask[p.column_index]) return null;
                const path_copy = try self.arena.dupe([]const u8, self.path_stack.items);
                const new_idx: u32 = self.next_column_index;
                self.next_column_index += 1;

                var new_leaf = p;
                new_leaf.path = path_copy;
                new_leaf.column_index = new_idx;
                // A subset of a built tree's leaves, so no path repeats.
                try self.leaves_out.append(self.arena, new_leaf);

                return .{ .primitive = new_leaf };
            },
            .group => |g| {
                const new_children = try self.projectChildren(g.children);
                if (new_children.len == 0) return null; // no surviving descendants
                return .{ .group = .{
                    .name = g.name,
                    .repetition = g.repetition,
                    .logical_type = g.logical_type,
                    .converted_type = g.converted_type,
                    .children = new_children,
                    .kind = g.kind,
                } };
            },
        }
    }
};

// ============================================================
// Flat thrift writer (DFS)
// ============================================================

fn writeNodeFlat(
    arena: std.mem.Allocator,
    node: Node,
    out: *std.ArrayListUnmanaged(schema.SchemaElement),
) Error!void {
    switch (node) {
        .primitive => |p| try out.append(arena, .{
            .type = p.type,
            .type_length = p.type_length,
            .repetition_type = p.repetition,
            .name = p.name,
            .num_children = null,
            .converted_type = p.converted_type,
            .logical_type = p.logical_type,
            .scale = p.scale,
            .precision = p.precision,
            .field_id = p.field_id,
        }),
        .group => |g| {
            const child_count: i32 = @intCast(g.children.len);
            try out.append(arena, .{
                .type = null,
                .type_length = null,
                .repetition_type = g.repetition,
                .name = g.name,
                .num_children = child_count,
                .converted_type = g.converted_type,
                .logical_type = g.logical_type,
                .scale = null,
                .precision = null,
                .field_id = null,
            });
            for (g.children) |c| try writeNodeFlat(arena, c, out);
        },
    }
}

// ============================================================
// Pretty printer
// ============================================================

fn formatNode(node: Node, writer: *std.Io.Writer, depth: usize) std.Io.Writer.Error!void {
    var i: usize = 0;
    while (i < depth) : (i += 1) try writer.writeAll("  ");

    const rep_str: []const u8 = switch (node.repetition()) {
        .REQUIRED => "required",
        .OPTIONAL => "optional",
        .REPEATED => "repeated",
    };
    try writer.print("{s} ", .{rep_str});

    switch (node) {
        .primitive => |p| {
            try writer.print("{s} {s}", .{ @tagName(p.type), p.name });
            if (p.logical_type) |lt| try writer.print(" ({s})", .{@tagName(lt)});
            try writer.writeAll(";\n");
        },
        .group => |g| {
            try writer.print("group {s}", .{g.name});
            if (g.kind != .struct_) try writer.print(" ({s})", .{@tagName(g.kind)});
            try writer.writeAll(" {\n");
            for (g.children) |c| try formatNode(c, writer, depth + 1);
            var j: usize = 0;
            while (j < depth) : (j += 1) try writer.writeAll("  ");
            try writer.writeAll("}\n");
        },
    }
}

// ============================================================
// Tests
// ============================================================

const testing = std.testing;
const metadata = @import("metadata.zig");

fn readFile(arena: std.mem.Allocator, path: []const u8) ![]u8 {
    return metadata.readFileSlice(path, arena);
}

test "build tree from flat fixture (benchmark_100mb.parquet)" {
    var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_state.deinit();
    const arena = arena_state.allocator();
    const file_bytes = readFile(arena, "data/benchmark_100mb.parquet") catch |err| {
        if (err == error.FileNotFound) return error.SkipZigTest;
        return err;
    };
    const meta = try metadata.open(arena, file_bytes);
    const tree = try SchemaTree.build(arena, meta.schema.items);

    // Flat fixture: 27 leaves, all top-level, max_def == 1, max_rep == 0.
    try testing.expectEqual(@as(usize, 27), tree.leaves.len);
    try testing.expectEqual(@as(usize, 27), tree.root.children.len);
    for (tree.leaves) |leaf| {
        try testing.expectEqual(@as(u8, 1), leaf.max_def); // Polars writes everything OPTIONAL
        try testing.expectEqual(@as(u8, 0), leaf.max_rep);
        try testing.expectEqual(@as(usize, 1), leaf.path.len);
    }
}

test "build tree from nested fixture (nested_edges.parquet)" {
    var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_state.deinit();
    const arena = arena_state.allocator();
    const file_bytes = readFile(arena, "data/nested_edges.parquet") catch |err| {
        if (err == error.FileNotFound) {
            std.debug.print("skipping: data/nested_edges.parquet not present\n", .{});
            return error.SkipZigTest;
        }
        return err;
    };
    const meta = try metadata.open(arena, file_bytes);
    const tree = try SchemaTree.build(arena, meta.schema.items);

    // 9 column chunks total: id, score, tags.list.element,
    // meta.k, meta.v, events.list.element.ts, events.list.element.code,
    // counts.key_value.key, counts.key_value.value.
    try testing.expectEqual(@as(usize, 9), tree.leaves.len);
    try testing.expectEqual(@as(usize, 6), tree.root.children.len);

    // Verify a flat leaf: id (max_def=1, max_rep=0).
    const id = tree.leaves[0];
    try testing.expectEqualStrings("id", id.name);
    try testing.expectEqual(@as(u8, 1), id.max_def);
    try testing.expectEqual(@as(u8, 0), id.max_rep);
    try testing.expectEqual(@as(usize, 1), id.path.len);

    // Verify tags.list.element (LIST<string>, max_def=3, max_rep=1).
    const tags_leaf = tree.leaves[2];
    try testing.expectEqual(@as(u8, 3), tags_leaf.max_def);
    try testing.expectEqual(@as(u8, 1), tags_leaf.max_rep);
    try testing.expectEqual(@as(usize, 3), tags_leaf.path.len);
    try testing.expectEqualStrings("tags", tags_leaf.path[0]);

    // Verify events.list.element.ts (LIST<struct{ts,code}>, max_def=4, max_rep=1).
    const ev_ts = tree.leaves[5];
    try testing.expectEqual(@as(u8, 4), ev_ts.max_def);
    try testing.expectEqual(@as(u8, 1), ev_ts.max_rep);
    try testing.expectEqual(@as(usize, 4), ev_ts.path.len);
    try testing.expectEqualStrings("events", ev_ts.path[0]);
    try testing.expectEqualStrings("ts", ev_ts.path[3]);

    // Verify counts.key_value.key (MAP, key REQUIRED, max_def=2, max_rep=1).
    const counts_key = tree.leaves[7];
    try testing.expectEqual(@as(u8, 2), counts_key.max_def);
    try testing.expectEqual(@as(u8, 1), counts_key.max_rep);
}

test "resolveProjection returns set of leaves under top-level group" {
    var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_state.deinit();
    const arena = arena_state.allocator();
    const file_bytes = readFile(arena, "data/nested_edges.parquet") catch return;
    const meta = try metadata.open(arena, file_bytes);

    // Top-level "events" should yield two leaves (ts, code).
    const events_leaves = try metadata.resolveProjection(arena, &meta, "events");
    try testing.expectEqualSlices(u32, &.{ 5, 6 }, events_leaves);

    // Top-level "id" is a single primitive leaf.
    const id_leaves = try metadata.resolveProjection(arena, &meta, "id");
    try testing.expectEqualSlices(u32, &.{0}, id_leaves);

    // Top-level "counts" (MAP) yields key + value.
    const counts_leaves = try metadata.resolveProjection(arena, &meta, "counts");
    try testing.expectEqualSlices(u32, &.{ 7, 8 }, counts_leaves);

    // Dotted full path resolves to a single leaf.
    const ts_leaves = try metadata.resolveProjection(arena, &meta, "events.list.element.ts");
    try testing.expectEqualSlices(u32, &.{5}, ts_leaves);

    // Unknown path → empty.
    const empty = try metadata.resolveProjection(arena, &meta, "nonexistent");
    try testing.expectEqual(@as(usize, 0), empty.len);
}

test "leaf paths that join to the same dotted name" {
    var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_state.deinit();
    const arena = arena_state.allocator();
    const E = struct {
        fn root(n: i32) schema.SchemaElement {
            return .{ .type = null, .type_length = null, .repetition_type = null, .name = "schema", .num_children = n, .scale = null, .precision = null, .field_id = null };
        }
        fn group(name: []const u8, n: i32) schema.SchemaElement {
            return .{ .type = null, .type_length = null, .repetition_type = .OPTIONAL, .name = name, .num_children = n, .scale = null, .precision = null, .field_id = null };
        }
        fn leaf(name: []const u8) schema.SchemaElement {
            return .{ .type = .INT64, .type_length = null, .repetition_type = .OPTIONAL, .name = name, .num_children = null, .scale = null, .precision = null, .field_id = null };
        }
    };

    // A top-level `a.b` beside a group `a` with a field `b`: both are columns, in either schema order.
    for ([_][]const schema.SchemaElement{
        &.{ E.root(2), E.leaf("a.b"), E.group("a", 1), E.leaf("b") },
        &.{ E.root(2), E.group("a", 1), E.leaf("b"), E.leaf("a.b") },
    }) |flat| {
        const tree = try SchemaTree.build(arena, flat);
        try testing.expectEqual(@as(usize, 2), tree.leaves.len);
        const kept = try tree.projectSubset(arena, &.{ 0, 1 });
        try testing.expectEqual(@as(usize, 2), kept.leaves.len);
    }

    // So are two nested leaves that join alike.
    const nested = try SchemaTree.build(arena, &.{ E.root(2), E.group("x", 1), E.leaf("y.z"), E.group("x.y", 1), E.leaf("z") });
    try testing.expectEqual(@as(usize, 2), nested.leaves.len);

    // The same path twice is still a duplicate field.
    try testing.expectError(error.DuplicatePath, SchemaTree.build(arena, &.{ E.root(2), E.leaf("k"), E.leaf("k") }));
    try testing.expectError(error.DuplicatePath, SchemaTree.build(arena, &.{ E.root(3), E.leaf("a.b"), E.group("a", 1), E.leaf("b"), E.leaf("a.b") }));
}

test "projectSubset preserves group ancestors and reindexes leaves" {
    var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_state.deinit();
    const arena = arena_state.allocator();
    const file_bytes = readFile(arena, "data/nested_edges.parquet") catch return;
    const meta = try metadata.open(arena, file_bytes);
    const tree = try SchemaTree.build(arena, meta.schema.items);

    // Project [id, both events leaves] → expect output with [id, events]
    // top-level children. events keeps its LIST/REPEATED ancestor structure.
    const projected = try tree.projectSubset(arena, &.{ 0, 5, 6 });
    try testing.expectEqual(@as(usize, 3), projected.leaves.len);
    try testing.expectEqual(@as(usize, 2), projected.root.children.len);
    try testing.expectEqualStrings("id", projected.root.children[0].name());
    try testing.expectEqualStrings("events", projected.root.children[1].name());

    // events should still be a LIST.
    try testing.expectEqual(GroupKind.list, projected.root.children[1].group.kind);

    // Reindexed leaves: id=0, events.list.element.ts=1, events...code=2.
    try testing.expectEqual(@as(u32, 0), projected.leaves[0].column_index);
    try testing.expectEqualStrings("id", projected.leaves[0].name);
    try testing.expectEqual(@as(u32, 1), projected.leaves[1].column_index);
    try testing.expectEqualStrings("ts", projected.leaves[1].name);
    try testing.expectEqual(@as(u32, 2), projected.leaves[2].column_index);
    try testing.expectEqualStrings("code", projected.leaves[2].name);
}

test "writeFlatThrift round-trips through metadata.open" {
    var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_state.deinit();
    const arena = arena_state.allocator();
    const file_bytes = readFile(arena, "data/nested_edges.parquet") catch return;
    const meta = try metadata.open(arena, file_bytes);
    const tree = try SchemaTree.build(arena, meta.schema.items);

    const re_flat = try tree.writeFlatThrift(arena);

    // Same number of elements, same leaf paths.
    try testing.expectEqual(meta.schema.items.len, re_flat.items.len);
    for (meta.schema.items, re_flat.items) |orig, dup| {
        try testing.expectEqualStrings(orig.name, dup.name);
        try testing.expectEqual(orig.repetition_type, dup.repetition_type);
        try testing.expectEqual(orig.num_children, dup.num_children);
    }

    // Build a tree from the round-tripped list and check leaf parity.
    const tree2 = try SchemaTree.build(arena, re_flat.items);
    try testing.expectEqual(tree.leaves.len, tree2.leaves.len);
    for (tree.leaves, tree2.leaves) |a, b| {
        try testing.expectEqualStrings(a.name, b.name);
        try testing.expectEqual(a.max_def, b.max_def);
        try testing.expectEqual(a.max_rep, b.max_rep);
        try testing.expectEqual(a.column_index, b.column_index);
    }
}
