//! Parquet file metadata: open the footer and bind column names to leaves.
//!
//! Schema parsing already lives in `src/core/schema.zig` (the Thrift-
//! encoded structs). This file is the thin layer that:
//!   1. Validates the leading + trailing PAR1 magic.
//!   2. Reads the 4-byte little-endian footer length.
//!   3. Slices the footer bytes and hands them to schema.FileMetaData.read.
//!   4. Resolves user-supplied column names to leaves, and labels leaves for output.
//!
//! Reading the statistics the footer carries is `statistics.zig`'s job.
//!
//! Input is a `[]const u8` covering the whole file. The caller decides
//! how those bytes got there (mmap, S3 GET, in-memory). For the Lambda
//! S3 path we'll later add a "footer-only fetch" helper that does a HEAD
//! + ranged GET, but the metadata layer itself stays I/O-agnostic.

const std = @import("std");
const nowNs = @import("../../clock.zig").monoNs;
const schema = @import("../schema.zig");
const thrift = @import("../thrift.zig");

const MAGIC: [4]u8 = .{ 'P', 'A', 'R', '1' };
const MAGIC_LEN: usize = MAGIC.len;
const FOOTER_LEN_LEN: usize = 4;

pub const Error = error{
    TooSmall,
    BadMagic,
    FooterTooLarge,
    BadMetadata,
    /// A column chunk's `path_in_schema` names a different column than the schema leaf at its position.
    ColumnChunkPathMismatch,
    /// A row group lists a different number of column chunks than the schema has leaves.
    ColumnChunkCountMismatch,
    /// The file or a row group claims a negative row count.
    NegativeRowCount,
    /// A schema element claims a negative child count or type length.
    BadSchemaElement,
    /// A column chunk's value count, offsets or sizes are negative, or its byte range overflows.
    BadColumnChunkRange,
    /// The schema nests groups deeper than `max_schema_depth`.
    SchemaTooDeep,
    /// A non-repeated column's chunk holds a different number of values than its row group has rows.
    ColumnChunkRowCountMismatch,
    /// A column chunk's physical type differs from its schema leaf's.
    ColumnChunkTypeMismatch,
};

/// Deepest group nesting a footer may have: `schema.max_schema_depth`.
pub const max_schema_depth = schema.max_schema_depth;

/// Parse the file footer and return a fully-realized FileMetaData.
/// Caller owns the result and must call `deinit`. The metadata holds
/// slices that point *into* `file_bytes`, so `file_bytes` must outlive
/// the FileMetaData.
pub fn open(
    allocator: std.mem.Allocator,
    file_bytes: []const u8,
) !schema.FileMetaData {
    if (file_bytes.len < MAGIC_LEN * 2 + FOOTER_LEN_LEN) return error.TooSmall;

    if (!std.mem.eql(u8, file_bytes[0..MAGIC_LEN], &MAGIC)) return error.BadMagic;
    const trailing_start = file_bytes.len - MAGIC_LEN;
    if (!std.mem.eql(u8, file_bytes[trailing_start..], &MAGIC)) return error.BadMagic;

    const footer_len_start = trailing_start - FOOTER_LEN_LEN;
    const footer_len = std.mem.readInt(
        u32,
        file_bytes[footer_len_start..][0..4],
        .little,
    );

    if (footer_len > file_bytes.len - MAGIC_LEN * 2 - FOOTER_LEN_LEN) return error.FooterTooLarge;

    const footer_start = footer_len_start - footer_len;
    const footer_bytes = file_bytes[footer_start..footer_len_start];

    return openFooter(allocator, footer_bytes);
}

/// Parse a parquet footer thrift payload that was fetched separately from
/// the full file body. S3 range reads use this to avoid materializing a
/// virtual slice whose length is the whole object.
pub fn openFooter(
    allocator: std.mem.Allocator,
    footer_bytes: []const u8,
) !schema.FileMetaData {
    var reader = thrift.Reader.init(footer_bytes);
    var meta = schema.FileMetaData.read(allocator, &reader) catch return error.BadMetadata;
    errdefer meta.deinit(allocator);
    try checkFooterFields(&meta);
    try reconcileChunkPaths(allocator, &meta);
    return meta;
}

/// Reject footer counts, offsets and sizes no valid file can have, where they enter. Every reader downstream casts
/// these i32 / i64 fields to usize or adds them together, which on a hostile value is a panic in safe builds and
/// undefined behaviour in ReleaseFast. Bounds against the actual file length stay with the code that slices, since
/// a footer fetched on its own (S3) doesn't know the file's length.
pub fn checkFooterFields(meta: *schema.FileMetaData) Error!void {
    if (meta.num_rows < 0) return error.NegativeRowCount;
    // Every schema has a root element; code throughout walks `schema.items[1..]`.
    if (meta.schema.items.len == 0) return error.BadSchemaElement;
    for (meta.schema.items, 0..) |elem, i| {
        if (elem.num_children) |n| if (n < 0) return error.BadSchemaElement;
        if (elem.type_length) |n| if (n < 0) return error.BadSchemaElement;
        // A primitive can't have children. Some walkers find leaves by type, others by child count; this keeps
        // them agreeing on which element is leaf N. Not the root: every walker starts below it, and some writers
        // (segmentio/parquet-go) give the root a type anyway.
        if (i > 0 and elem.type != null and (elem.num_children orelse 0) > 0) return error.BadSchemaElement;
    }
    // ...and below the root an element with neither a type nor children is the other way they'd disagree: a leaf
    // with no physical type to one, an empty group with no chunk to the other.
    if (meta.schema.items.len > 1) for (meta.schema.items[1..]) |elem| {
        if (elem.type == null and (elem.num_children orelse 0) == 0) return error.BadSchemaElement;
    };
    // The tree shape, walked as `schema.LeafIterator` walks it: descending into any element with children, no deeper
    // than it goes, and finding exactly the elements the root's and each group's child counts promise. Otherwise
    // the walk would end early and leave elements out.
    var remaining: [max_schema_depth + 1]usize = undefined;
    remaining[0] = @intCast(meta.schema.items[0].num_children orelse 0);
    var depth: usize = 0; // open groups below the root
    for (meta.schema.items[1..]) |elem| {
        while (remaining[depth] == 0) {
            if (depth == 0) return error.BadSchemaElement; // past the root's last child
            depth -= 1;
        }
        remaining[depth] -= 1;
        const children: usize = @intCast(elem.num_children orelse 0);
        if (children == 0) continue;
        if (depth == max_schema_depth) return error.SchemaTooDeep;
        depth += 1;
        remaining[depth] = children;
    }
    for (remaining[0 .. depth + 1]) |left| if (left > 0) return error.BadSchemaElement; // children past the end
    for (meta.row_groups.items) |*rg| {
        if (rg.num_rows < 0) return error.NegativeRowCount;
        for (rg.columns.items) |*cc| {
            // The page index is optional, and an unusable one is ignored wherever it's read; a negative location
            // drops the chunk's page index (both halves, which pruning uses together) rather than the file.
            const bad_index = for ([_]?i64{ cc.offset_index_offset, cc.column_index_offset }, [_]?i32{ cc.offset_index_length, cc.column_index_length }) |off, len| {
                if ((off orelse 0) < 0 or (len orelse 0) < 0) break true;
            } else false;
            if (bad_index) {
                cc.offset_index_offset = null;
                cc.offset_index_length = null;
                cc.column_index_offset = null;
                cc.column_index_length = null;
            }
            const cm = cc.meta_data orelse continue;
            if (cm.num_values < 0 or cm.total_compressed_size < 0 or cm.total_uncompressed_size < 0)
                return error.BadColumnChunkRange;
            if (cm.data_page_offset < 0) return error.BadColumnChunkRange;
            if (cm.dictionary_page_offset) |d| if (d < 0) return error.BadColumnChunkRange;
            const start = cm.dictionary_page_offset orelse cm.data_page_offset;
            _ = std.math.add(i64, start, cm.total_compressed_size) catch return error.BadColumnChunkRange;
        }
    }
}

/// Check each column chunk against the schema leaf at its position, before anything reads it.
///
/// zpq picks a column's chunk by leaf ordinal but resolves that chunk's schema element and levels through the chunk's
/// own `path_in_schema`. The two are independent statements of the same alignment, so when they disagree the file is
/// internally inconsistent and decoding would silently pair one column's bytes with another column's name and type.
/// Rejected, as Hardwood does:
///   - a path that names a different leaf (swapped or misordered chunks),
///   - a path that names no leaf at all,
///   - a row group whose chunk count differs from the leaf count.
/// Tolerated, rewritten to the schema leaf's path so later path lookups resolve:
///   - an omitted (empty) path, which some writers leave out despite the format requiring it,
///   - a path that differs from its own leaf only in ASCII case and names no other leaf.
pub fn reconcileChunkPaths(allocator: std.mem.Allocator, meta: *schema.FileMetaData) (Error || std.mem.Allocator.Error)!void {
    if (meta.row_groups.items.len == 0 or meta.schema.items.len == 0) return;

    // Every leaf's full path (root excluded), flattened: leaf i is `names[ends[i-1]..ends[i]]`. `repeated[i]`: the
    // leaf or one of its ancestors is REPEATED, so its value count may exceed the row count.
    var names: std.ArrayList([]const u8) = .empty;
    defer names.deinit(allocator);
    var ends: std.ArrayList(usize) = .empty;
    defer ends.deinit(allocator);
    var repeated: std.ArrayList(bool) = .empty;
    defer repeated.deinit(allocator);
    var types: std.ArrayList(schema.Type) = .empty;
    defer types.deinit(allocator);
    var it: schema.LeafIterator = .init(meta.schema.items);
    while (it.next()) |l| {
        try names.appendSlice(allocator, l.path);
        try ends.append(allocator, names.items.len);
        try repeated.append(allocator, l.max_rep > 0);
        // `checkFooterFields` gives every leaf below the root a type.
        try types.append(allocator, l.element.type orelse return error.BadSchemaElement);
    }
    const leaf = struct {
        fn path(n: []const []const u8, e: []const usize, i: usize) []const []const u8 {
            return n[if (i == 0) 0 else e[i - 1]..e[i]];
        }
        fn eqlIgnoreCase(a: []const []const u8, b: []const []const u8) bool {
            if (a.len != b.len) return false;
            for (a, b) |x, y| if (!std.ascii.eqlIgnoreCase(x, y)) return false;
            return true;
        }
    };

    for (meta.row_groups.items) |*rg| {
        if (rg.columns.items.len != ends.items.len) return error.ColumnChunkCountMismatch;
        for (rg.columns.items, 0..) |*cc, i| {
            const cm = if (cc.meta_data) |*m| m else continue;
            // A flat column holds one value (or null) per row. Readers size its values from num_values and its row
            // selection from num_rows, then walk both together.
            if (!repeated.items[i] and cm.num_values != rg.num_rows) return error.ColumnChunkRowCountMismatch;
            // Readers pick the decoder from the chunk's type and the logical type from the schema leaf.
            if (cm.type != types.items[i]) return error.ColumnChunkTypeMismatch;
            const want = leaf.path(names.items, ends.items, i);
            const got = cm.path_in_schema.items;
            if (schema.pathEql(got, want)) continue;
            if (got.len > 0) {
                // Only reached for paths that are not already exact, so well-formed files never pay for this scan.
                for (0..ends.items.len) |j| {
                    if (schema.pathEql(got, leaf.path(names.items, ends.items, j))) return error.ColumnChunkPathMismatch;
                }
                if (!leaf.eqlIgnoreCase(got, want)) return error.ColumnChunkPathMismatch;
            }
            cm.path_in_schema.clearRetainingCapacity();
            try cm.path_in_schema.appendSlice(allocator, want);
        }
    }
}

pub const ColumnLookupError = error{ UnknownColumn, AmbiguousColumn };

/// Bind a user-supplied column name to its LEAF index (the index into
/// `row_group.columns[]`, which counts only primitive leaves; group nodes
/// such as struct/list/map wrappers have no chunk of their own).
///
///   1. A top-level column whose name is exactly `column_name` (dots
///      included) binds first: `key` is the top-level `key` even beside a
///      nested `r.key`, and a top-level column literally named `r.key`
///      wins over the field `key` of a group `r`.
///   2. Otherwise a nested leaf whose dot-joined path is `column_name`.
///   3. Otherwise a bare name (no `.`) binds to the one nested leaf whose
///      own name it is.
///
/// A name in quoted-path form (`"a"."b"`, see `isQuotedPath`) that is not a
/// top-level name binds by segments instead: to the leaf whose path is exactly
/// those segments. It names a leaf that `a.b` cannot reach, such as the field
/// `b` of a group `a` beside a top-level column named `a.b`.
///
/// Two candidates at the step that decides are `AmbiguousColumn`, never a
/// guess; `ambiguityHint` says what would tell them apart.
pub fn resolveColumn(
    file: *const schema.FileMetaData,
    column_name: []const u8,
) ColumnLookupError!usize {
    const hits: ColumnHits = .find(file, column_name, false);
    if (try hits.top.only()) |leaf| return leaf;
    if (isQuotedPath(column_name)) {
        const quoted: ColumnHits = .find(file, column_name, true);
        return (try quoted.path.only()) orelse error.UnknownColumn;
    }
    if (try hits.path.only()) |leaf| return leaf;
    if (std.mem.indexOfScalar(u8, column_name, '.') != null) return error.UnknownColumn;
    return (try hits.leaf.only()) orelse error.UnknownColumn;
}

/// The leaves a --columns name selects, in schema order. A top-level column or group whose name is exactly `name`
/// (quotes in it included) comes first, as in `resolveColumn`; then one quoted identifier names the top-level column
/// or group it quotes, as the filter and expression lexers unquote it; anything else is the one leaf
/// `resolveColumn` binds: a quoted or dotted path, or the bare name of the one nested field carrying it. An empty
/// slice when nothing matches.
pub fn resolveProjection(
    arena: std.mem.Allocator,
    file: *const schema.FileMetaData,
    name: []const u8,
) (ColumnLookupError || std.mem.Allocator.Error)![]const u32 {
    if (try topLevelLeaves(arena, file.schema.items, name)) |leaves| return leaves;
    const unquoted = unquoteIdent(name);
    if (unquoted.len != name.len) {
        if (try topLevelLeaves(arena, file.schema.items, unquoted)) |leaves| return leaves;
    }
    const leaf = resolveColumn(file, unquoted) catch |err| switch (err) {
        error.UnknownColumn => return &.{},
        else => return err,
    };
    const out = try arena.alloc(u32, 1);
    out[0] = @intCast(leaf);
    return out;
}

/// Leaves of the top-level column or group named exactly `name`, or null when there is none.
fn topLevelLeaves(arena: std.mem.Allocator, items: []const schema.SchemaElement, name: []const u8) !?[]const u32 {
    // A field's leaves are contiguous, so one top-level field is one range of leaf ordinals.
    var hit_top: ?usize = null;
    var range: [2]u32 = undefined;
    var it: schema.LeafIterator = .init(items);
    while (it.next()) |leaf| {
        if (!std.mem.eql(u8, leaf.path[0], name)) continue;
        if (hit_top == leaf.top) {
            range[1] += 1;
            continue;
        }
        if (hit_top != null) return error.AmbiguousColumn;
        hit_top = leaf.top;
        range = .{ @intCast(leaf.index), @intCast(leaf.index + 1) };
    }
    if (hit_top == null) return null;
    const out = try arena.alloc(u32, range[1] - range[0]);
    for (out, range[0]..) |*o, l| o.* = @intCast(l);
    return out;
}

/// For a name `resolveColumn` found ambiguous, what the candidates are and
/// how to name one of them.
pub fn ambiguityHint(file: *const schema.FileMetaData, column_name: []const u8) []const u8 {
    const hits: ColumnHits = .find(file, column_name, false);
    if (hits.top.n > 1) return "several top-level columns have this name";
    if (hits.path.n > 1) return "several nested fields have this path; quote each segment (\"a\".\"b\") to pick one";
    return "several nested fields have this name; use the dotted path of the one you mean";
}

/// `resolveColumn` for callers that only distinguish found / not found.
/// An ambiguous name is not found.
pub fn findColumnIndex(
    file: *const schema.FileMetaData,
    column_name: []const u8,
) ?usize {
    return resolveColumn(file, column_name) catch null;
}

/// The schema element of the `leaf_idx`-th primitive leaf, as
/// `resolveColumn` counts them. Null when the schema has fewer leaves.
pub fn leafSchemaElement(file: *const schema.FileMetaData, leaf_idx: usize) ?schema.SchemaElement {
    var it: schema.LeafIterator = .init(file.schema.items);
    while (it.next()) |leaf| {
        if (leaf.index == leaf_idx) return leaf.element.*;
    }
    return null;
}

/// The dot-joined path of the `leaf_idx`-th primitive leaf (`key` for a
/// top-level column, `r.key` for a nested one): the name that binds back
/// to it through `resolveColumn`. Null when the schema has fewer leaves.
pub fn leafPath(arena: std.mem.Allocator, file: *const schema.FileMetaData, leaf_idx: usize) !?[]const u8 {
    const segments = (try leafPathSegments(arena, file, leaf_idx)) orelse return null;
    if (segments.len == 1) return segments[0];
    return try std.mem.join(arena, ".", segments);
}

/// The output name of the `leaf_idx`-th primitive leaf, and the name that binds back to it through `resolveColumn`:
/// a top-level column's own name; a nested leaf's dot-joined path (`r.key`); or, when that path is taken (a top-level
/// column literally named `a.b` beside the field `b` of a group `a`, or two nested paths that join alike), its
/// quoted-path form (`"a"."b"`). Flat output (row formats, GROUP BY keys, a flat --select) must not print two columns
/// under one name. `DuplicateOutputColumn` when even the quoted form binds elsewhere, which only a top-level column
/// literally named `"a"."b"`, quotes included, can cause. Null when the schema has fewer leaves.
///
/// Walks the schema up to three times per call; `leafLabels` labels every leaf in one walk.
pub fn leafLabel(arena: std.mem.Allocator, file: *const schema.FileMetaData, leaf_idx: usize) !?[]const u8 {
    const segments = (try leafPathSegments(arena, file, leaf_idx)) orelse return null;
    if (segments.len == 1) return segments[0];
    const dotted = try std.mem.join(arena, ".", segments);
    if ((resolveColumn(file, dotted) catch null) == leaf_idx) return dotted;
    const quoted = try quotePath(arena, segments);
    if ((resolveColumn(file, quoted) catch null) == leaf_idx) return quoted;
    return error.DuplicateOutputColumn;
}

pub const LeafLabels = struct {
    names: []const ?[]const u8,
    /// Whether each leaf sits in a group, so is not a top-level column.
    nested: []const bool,
};

/// `leafLabel` of every leaf, null for a leaf that has none (`DuplicateOutputColumn` from `leafLabel`). One schema
/// walk and a few hash lookups per leaf: time linear in the total length of the leaves' paths, however many of them
/// collide.
///
/// `leafLabel` asks `resolveColumn` which leaf each candidate name binds. The walk counts the spellings `resolveColumn`
/// matches instead, and answers from the counts. A top-level leaf is labelled by its name. A nested leaf's dotted
/// path binds back to it when no other leaf spells that string (a top-level leaf by name, a nested one by dotted
/// path), or, when the string reads as a quoted path, when no top-level leaf is named it and it quotes this leaf's
/// path and no other leaf's. Failing that, its quoted path binds back when no top-level leaf is named that and no
/// other leaf has this path.
pub fn leafLabels(arena: std.mem.Allocator, file: *const schema.FileMetaData) !LeafLabels {
    const PathUse = struct { n: u32, leaf: usize };
    // Leaves per spelling: top-level leaves by name; every leaf by dotted path (a top-level leaf's is its name); and
    // every leaf by its path, segment by segment, which is what a quoted path binds.
    var tops: std.StringHashMapUnmanaged(u32) = .empty;
    var spelled: std.StringHashMapUnmanaged(u32) = .empty;
    const PathUses = std.HashMapUnmanaged([]const []const u8, PathUse, PathContext, std.hash_map.default_max_load_percentage);
    var paths: PathUses = .empty;
    var segments: std.ArrayList([]const []const u8) = .empty;
    var dotted: std.ArrayList([]const u8) = .empty;
    var it: schema.LeafIterator = .init(file.schema.items);
    while (it.next()) |leaf| {
        const path = try arena.dupe([]const u8, leaf.path);
        const joined = if (path.len == 1) path[0] else try std.mem.join(arena, ".", path);
        try segments.append(arena, path);
        try dotted.append(arena, joined);
        if (path.len == 1) (try tops.getOrPutValue(arena, joined, 0)).value_ptr.* += 1;
        (try spelled.getOrPutValue(arena, joined, 0)).value_ptr.* += 1;
        const use = try paths.getOrPutValue(arena, path, .{ .n = 0, .leaf = leaf.index });
        use.value_ptr.* = .{ .n = use.value_ptr.n + 1, .leaf = leaf.index };
    }

    const labels = try arena.alloc(?[]const u8, segments.items.len);
    const nested = try arena.alloc(bool, segments.items.len);
    for (labels, nested, segments.items, dotted.items, 0..) |*label, *in_group, path, joined, i| {
        in_group.* = path.len > 1;
        label.* = label: {
            if (path.len == 1) break :label joined;
            if (!isQuotedPath(joined)) {
                if (spelled.get(joined).? == 1) break :label joined;
            } else if (!tops.contains(joined)) {
                if (paths.get(try quotedPathSegments(arena, joined))) |use| {
                    if (use.n == 1 and use.leaf == i) break :label joined;
                }
            }
            const quoted = try quotePath(arena, path);
            if (!tops.contains(quoted) and paths.get(path).?.n == 1) break :label quoted;
            break :label null;
        };
    }
    return .{ .names = labels, .nested = nested };
}

/// Hashes a schema path segment by segment, so paths that join alike stay distinct.
const PathContext = struct {
    pub fn hash(_: PathContext, path: []const []const u8) u64 {
        var h: std.hash.Wyhash = .init(0);
        for (path) |seg| {
            h.update(std.mem.asBytes(&seg.len));
            h.update(seg);
        }
        return h.final();
    }

    pub fn eql(_: PathContext, a: []const []const u8, b: []const []const u8) bool {
        return schema.pathEql(a, b);
    }
};

/// `segments` as a quoted path: each segment in SQL double quotes, embedded quotes doubled, joined by `.`.
pub fn quotePath(arena: std.mem.Allocator, segments: []const []const u8) ![]const u8 {
    var out: std.ArrayList(u8) = .empty;
    for (segments, 0..) |seg, i| {
        if (i > 0) try out.append(arena, '.');
        try out.append(arena, '"');
        for (seg) |c| {
            if (c == '"') try out.append(arena, '"');
            try out.append(arena, c);
        }
        try out.append(arena, '"');
    }
    return out.toOwnedSlice(arena);
}

/// Bytes of `rest` that spell `segment` as one double-quoted segment (embedded quotes doubled), or null.
fn matchQuotedSegment(rest: []const u8, segment: []const u8) ?usize {
    if (rest.len == 0 or rest[0] != '"') return null;
    var pos: usize = 1;
    for (segment) |c| {
        if (pos >= rest.len or rest[pos] != c) return null;
        pos += 1;
        if (c == '"') {
            if (pos >= rest.len or rest[pos] != '"') return null;
            pos += 1;
        }
    }
    if (pos >= rest.len or rest[pos] != '"') return null;
    return pos + 1;
}

/// Length of the double-quoted segment `s` starts with (embedded quotes doubled), or null when it does not.
fn quotedSegmentLen(s: []const u8) ?usize {
    if (s.len == 0 or s[0] != '"') return null;
    var pos: usize = 1;
    while (pos < s.len) : (pos += 1) {
        if (s[pos] != '"') continue;
        if (pos + 1 < s.len and s[pos + 1] == '"') {
            pos += 1;
            continue;
        }
        return pos + 1;
    }
    return null;
}

/// Whether `name` is a quoted path: double-quoted segments joined by `.` (`"a"."b"`), embedded quotes doubled.
/// The form `leafLabel` renders, so its labels bind back through --columns, --filter and expressions.
pub fn isQuotedPath(name: []const u8) bool {
    var rest = name;
    while (true) {
        const n = quotedSegmentLen(rest) orelse return false;
        rest = rest[n..];
        if (rest.len == 0) return true;
        if (rest[0] != '.') return false;
        rest = rest[1..];
    }
}

/// Strip SQL double-quote identifier quoting from one identifier: `"col"` → `col`. A quoted path of several
/// segments, or one with a doubled quote, stays whole for `resolveColumn` to bind by segments; anything else passes
/// through trimmed.
pub fn unquoteIdent(name: []const u8) []const u8 {
    const t = std.mem.trim(u8, name, " \t");
    if (t.len < 2 or t[0] != '"' or t[t.len - 1] != '"') return t;
    const one_plain_segment = quotedSegmentLen(t) == t.len and std.mem.indexOf(u8, t[1 .. t.len - 1], "\"\"") == null;
    if (isQuotedPath(t) and !one_plain_segment) return t;
    return t[1 .. t.len - 1];
}

/// The segments the quoted path `name` (`isQuotedPath`) spells, embedded quotes undoubled.
fn quotedPathSegments(arena: std.mem.Allocator, name: []const u8) ![]const []const u8 {
    var out: std.ArrayList([]const u8) = .empty;
    var rest = name;
    while (true) {
        const n = quotedSegmentLen(rest).?;
        try out.append(arena, try std.mem.replaceOwned(u8, arena, rest[1 .. n - 1], "\"\"", "\""));
        rest = rest[n..];
        if (rest.len == 0) return out.toOwnedSlice(arena);
        rest = rest[1..];
    }
}

/// Whether the quoted path `name` spells exactly `segments`.
pub fn quotedPathEql(name: []const u8, segments: []const []const u8) bool {
    var rest = name;
    for (segments, 0..) |seg, i| {
        if (i > 0) {
            if (rest.len == 0 or rest[0] != '.') return false;
            rest = rest[1..];
        }
        rest = rest[matchQuotedSegment(rest, seg) orelse return false ..];
    }
    return rest.len == 0;
}

/// Number of leaf columns in the schema: the column-chunk count of every row group (enforced at footer open), and
/// still defined for a file with no row groups at all, which is a valid empty table.
pub fn leafCount(file: *const schema.FileMetaData) usize {
    var it: schema.LeafIterator = .init(file.schema.items);
    while (it.next()) |_| {}
    return it.index;
}

/// Full schema path (root excluded) of leaf `leaf_idx` as its segments, or null past the last leaf. Equal to that
/// leaf's chunk `path_in_schema` (enforced at footer open) but needs no row group. Strings borrow the schema's names.
/// Compare paths by these segments, never by `leafPath`'s dot-joined form: a top-level column named `a.b` and the
/// field `b` of a group `a` join to the same string.
pub fn leafPathSegments(arena: std.mem.Allocator, file: *const schema.FileMetaData, leaf_idx: usize) !?[]const []const u8 {
    var it: schema.LeafIterator = .init(file.schema.items);
    while (it.next()) |leaf| {
        if (leaf.index == leaf_idx) return try arena.dupe([]const u8, leaf.path);
    }
    return null;
}

/// The leaves a name matches at each step of `resolveColumn`: top-level leaves named exactly `name`; nested leaves
/// whose dot-joined path is `name`, or with `quoted`, leaves at any depth whose path the quoted path `name` spells;
/// and nested leaves whose own name is `name`.
const ColumnHits = struct {
    top: Hits = .{},
    path: Hits = .{},
    leaf: Hits = .{},

    const Hits = struct {
        n: usize = 0,
        last: usize = 0,

        fn add(self: *Hits, leaf_idx: usize) void {
            self.n += 1;
            self.last = leaf_idx;
        }

        /// The one leaf hit, null for none, `AmbiguousColumn` for several.
        fn only(self: Hits) ColumnLookupError!?usize {
            if (self.n > 1) return error.AmbiguousColumn;
            return if (self.n == 1) self.last else null;
        }
    };

    fn find(file: *const schema.FileMetaData, name: []const u8, quoted: bool) ColumnHits {
        var hits: ColumnHits = .{};
        var it: schema.LeafIterator = .init(file.schema.items);
        while (it.next()) |leaf| {
            if (quoted) {
                if (quotedPathEql(name, leaf.path)) hits.path.add(leaf.index);
            } else if (leaf.path.len == 1) {
                if (std.mem.eql(u8, leaf.path[0], name)) hits.top.add(leaf.index);
            } else if (dottedPathEql(name, leaf.path)) {
                hits.path.add(leaf.index);
            } else if (std.mem.eql(u8, leaf.path[leaf.path.len - 1], name)) {
                hits.leaf.add(leaf.index);
            }
        }
        return hits;
    }
};

/// Whether `segments` joined by `.` spell exactly `name`, without joining them.
fn dottedPathEql(name: []const u8, segments: []const []const u8) bool {
    var rest = name;
    for (segments, 0..) |seg, i| {
        if (i > 0) {
            if (rest.len == 0 or rest[0] != '.') return false;
            rest = rest[1..];
        }
        if (!std.mem.startsWith(u8, rest, seg)) return false;
        rest = rest[seg.len..];
    }
    return rest.len == 0;
}

// ============================================================
// Tests
// ============================================================

const testing = std.testing;
const test_fixtures = @import("test_fixtures.zig");

test "open rejects too-small input" {
    try testing.expectError(error.TooSmall, open(testing.allocator, ""));
    try testing.expectError(error.TooSmall, open(testing.allocator, "PAR1"));
}

test "open rejects bad magic" {
    var bytes: [16]u8 = undefined;
    @memcpy(bytes[0..4], "NOPE");
    @memset(bytes[4..12], 0);
    @memcpy(bytes[12..], "PAR1");
    try testing.expectError(error.BadMagic, open(testing.allocator, &bytes));
}

test "footer round-trip: read → write → read produces identical metadata" {
    const fixture_path = "data/benchmark_100mb.parquet";
    const file_bytes = readFileSlice(fixture_path, testing.allocator) catch |err| {
        if (err == error.FileNotFound) {
            std.debug.print("skipping: {s} not present\n", .{fixture_path});
            return error.SkipZigTest;
        }
        return err;
    };
    defer testing.allocator.free(file_bytes);

    var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_state.deinit();
    const arena = arena_state.allocator();

    const meta = try open(arena, file_bytes);

    // Write the parsed meta into a fresh thrift buffer.
    var w: thrift.Writer = .init(arena);
    defer w.deinit();
    try meta.write(&w);
    const footer_bytes = w.bytes();

    // Build a synthetic parquet "skeleton" with this footer so we can
    // re-open it via the same code path (round-trip).
    var synth: std.ArrayList(u8) = .empty;
    defer synth.deinit(arena);
    try synth.appendSlice(arena, &MAGIC);
    try synth.appendSlice(arena, footer_bytes);
    var len_bytes: [4]u8 = undefined;
    std.mem.writeInt(u32, &len_bytes, @intCast(footer_bytes.len), .little);
    try synth.appendSlice(arena, &len_bytes);
    try synth.appendSlice(arena, &MAGIC);

    // Open again — this will fail if our writer produces invalid thrift.
    const meta2 = try open(arena, synth.items);
    try testing.expectEqual(meta.num_rows, meta2.num_rows);
    try testing.expectEqual(meta.row_groups.items.len, meta2.row_groups.items.len);
    try testing.expectEqual(meta.schema.items.len, meta2.schema.items.len);
    for (meta.schema.items, meta2.schema.items) |a, b| {
        try testing.expectEqualStrings(a.name, b.name);
        try testing.expectEqual(a.repetition_type, b.repetition_type);
    }
}

test "open the bench fixture" {
    const fixture_path = "data/benchmark_100mb.parquet";
    const file_bytes = readFileSlice(fixture_path, testing.allocator) catch |err| {
        if (err == error.FileNotFound) {
            std.debug.print("skipping: {s} not present\n", .{fixture_path});
            return error.SkipZigTest;
        }
        return err;
    };
    defer testing.allocator.free(file_bytes);

    const t0 = nowNs();
    var meta = try open(testing.allocator, file_bytes);
    defer meta.deinit(testing.allocator);
    const elapsed_us = @divTrunc(nowNs() - t0, std.time.ns_per_us);

    try testing.expect(meta.num_rows > 0);
    try testing.expect(meta.row_groups.items.len > 0);
    try testing.expect(meta.schema.items.len > 1); // root + at least one column

    // Diagnostic: show enough shape to debug new metadata cases.
    std.debug.print(
        "\n[metadata] {s}: {d} rows, {d} row groups, {d} schema elems, parse {d} us\n",
        .{ fixture_path, meta.num_rows, meta.row_groups.items.len, meta.schema.items.len, elapsed_us },
    );
    if (meta.row_groups.items.len > 0) {
        const rg0 = &meta.row_groups.items[0];
        std.debug.print("[metadata] row group 0: {d} rows, {d} columns\n", .{ rg0.num_rows, rg0.columns.items.len });
        for (rg0.columns.items, 0..) |col, i| {
            const m = col.meta_data orelse continue;
            const path = if (m.path_in_schema.items.len > 0) m.path_in_schema.items[0] else "?";
            std.debug.print(
                "  col[{d}] {s} type={s} codec={s} encodings={d} num_values={d} compressed={d} uncompressed={d}\n",
                .{ i, path, @tagName(m.type), @tagName(m.codec), m.encodings.items.len, m.num_values, m.total_compressed_size, m.total_uncompressed_size },
            );
        }
    }
}

/// Serialized footer for a schema of `elems` (root prepended) and one row group whose chunks carry `chunk_paths`.
fn chunkPathFooterForTest(
    arena: std.mem.Allocator,
    elems: []const schema.SchemaElement,
    chunk_paths: []const []const []const u8,
) ![]const u8 {
    var top: i32 = 0;
    var depth_left: i32 = 0;
    for (elems) |e| {
        if (depth_left == 0) top += 1 else depth_left -= 1;
        depth_left += e.num_children orelse 0;
    }
    var meta: schema.FileMetaData = .{ .version = 1, .schema = .empty, .num_rows = 0, .created_by = null, .row_groups = .empty };
    try meta.schema.append(arena, .{ .type = null, .type_length = null, .repetition_type = null, .name = "root", .num_children = top, .scale = null, .precision = null, .field_id = null });
    try meta.schema.appendSlice(arena, elems);
    var rg: schema.RowGroup = .{ .columns = .empty, .total_byte_size = 0, .num_rows = 0 };
    for (chunk_paths) |p| {
        var path: schema.StringList = .empty;
        try path.appendSlice(arena, p);
        try rg.columns.append(arena, .{ .file_path = null, .file_offset = 4, .meta_data = .{
            .type = .INT64,
            .encodings = .empty,
            .path_in_schema = path,
            .codec = .UNCOMPRESSED,
            .num_values = 0,
            .total_uncompressed_size = 0,
            .total_compressed_size = 0,
            .data_page_offset = 4,
            .index_page_offset = null,
            .dictionary_page_offset = null,
            .statistics = null,
        } });
    }
    try meta.row_groups.append(arena, rg);
    var w: thrift.Writer = .init(arena);
    try meta.write(&w);
    return w.bytes();
}

fn leafForTest(name: []const u8) schema.SchemaElement {
    return .{ .type = .INT64, .type_length = null, .repetition_type = .REQUIRED, .name = name, .num_children = 0, .scale = null, .precision = null, .field_id = null };
}

fn groupForTest(name: []const u8, children: i32) schema.SchemaElement {
    return .{ .type = null, .type_length = null, .repetition_type = .REQUIRED, .name = name, .num_children = children, .scale = null, .precision = null, .field_id = null };
}

test "footer open rejects column chunks whose path_in_schema disagrees with the schema leaf" {
    // Chunks are picked by leaf ordinal but resolved through their own path, so a footer whose chunk paths are
    // swapped relative to the schema used to decode each column under the other's name. Omitted paths and
    // case-only differences are tolerated and rewritten to the schema's spelling.
    var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_state.deinit();
    const arena = arena_state.allocator();

    const flat = [_]schema.SchemaElement{ leafForTest("a"), leafForTest("b") };
    const nested = [_]schema.SchemaElement{ groupForTest("s", 2), leafForTest("x"), leafForTest("y"), leafForTest("z") };
    const twins = [_]schema.SchemaElement{ leafForTest("id"), leafForTest("ID") };

    const Case = struct {
        elems: []const schema.SchemaElement,
        chunks: []const []const []const u8,
        /// Paths every chunk must carry after open; null when open must fail with `err`.
        resolved: ?[]const []const []const u8 = null,
        err: ?anyerror = null,
    };
    const cases = [_]Case{
        .{ .elems = &flat, .chunks = &.{ &.{"a"}, &.{"b"} }, .resolved = &.{ &.{"a"}, &.{"b"} } },
        .{ .elems = &flat, .chunks = &.{ &.{"b"}, &.{"a"} }, .err = error.ColumnChunkPathMismatch },
        .{ .elems = &flat, .chunks = &.{ &.{"a"}, &.{"c"} }, .err = error.ColumnChunkPathMismatch },
        .{ .elems = &flat, .chunks = &.{ &.{}, &.{} }, .resolved = &.{ &.{"a"}, &.{"b"} } },
        .{ .elems = &flat, .chunks = &.{ &.{"A"}, &.{"B"} }, .resolved = &.{ &.{"a"}, &.{"b"} } },
        .{ .elems = &flat, .chunks = &.{&.{"a"}}, .err = error.ColumnChunkCountMismatch },
        .{ .elems = &twins, .chunks = &.{ &.{"ID"}, &.{"id"} }, .err = error.ColumnChunkPathMismatch },
        .{ .elems = &nested, .chunks = &.{ &.{ "s", "x" }, &.{ "s", "y" }, &.{"z"} }, .resolved = &.{ &.{ "s", "x" }, &.{ "s", "y" }, &.{"z"} } },
        .{ .elems = &nested, .chunks = &.{ &.{ "s", "y" }, &.{ "s", "x" }, &.{"z"} }, .err = error.ColumnChunkPathMismatch },
        .{ .elems = &nested, .chunks = &.{ &.{"x"}, &.{"y"}, &.{"z"} }, .err = error.ColumnChunkPathMismatch },
    };

    for (cases) |case| {
        const footer = try chunkPathFooterForTest(arena, case.elems, case.chunks);
        if (case.err) |want| {
            try testing.expectError(want, openFooter(testing.allocator, footer));
            continue;
        }
        var meta = try openFooter(testing.allocator, footer);
        defer meta.deinit(testing.allocator);
        const resolved = case.resolved.?;
        const cols = meta.row_groups.items[0].columns.items;
        try testing.expectEqual(resolved.len, cols.len);
        for (resolved, cols) |want, cc| {
            const got = cc.meta_data.?.path_in_schema.items;
            try testing.expectEqual(want.len, got.len);
            for (want, got) |w, g| try testing.expectEqualStrings(w, g);
        }
    }
}

test "chunk paths compare by segment: a top-level column named a.b is not the field b of a group a" {
    // Both leaves dot-join to `a.b`; path_in_schema validation must tell them apart by segments, and the resolver
    // binds `a.b` to the top-level column and the bare `b` to the nested field.
    var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_state.deinit();
    const arena = arena_state.allocator();
    const elems = [_]schema.SchemaElement{ leafForTest("a.b"), groupForTest("a", 1), leafForTest("b") };

    var meta = try openFooter(arena, try chunkPathFooterForTest(arena, &elems, &.{ &.{"a.b"}, &.{ "a", "b" } }));
    const cols = meta.row_groups.items[0].columns.items;
    try testing.expectEqual(@as(usize, 1), cols[0].meta_data.?.path_in_schema.items.len);
    try testing.expectEqual(@as(usize, 2), cols[1].meta_data.?.path_in_schema.items.len);
    try testing.expectEqual(@as(usize, 2), leafCount(&meta));
    try testing.expectEqualStrings((try leafPath(arena, &meta, 0)).?, (try leafPath(arena, &meta, 1)).?);
    try testing.expectEqual(@as(usize, 1), (try leafPathSegments(arena, &meta, 0)).?.len);
    try testing.expectEqual(@as(usize, 2), (try leafPathSegments(arena, &meta, 1)).?.len);
    try testing.expectEqual(@as(usize, 0), try resolveColumn(&meta, "a.b"));
    try testing.expectEqual(@as(usize, 1), try resolveColumn(&meta, "b"));
    try testing.expectEqualStrings("b", meta.getColumnSchema((try leafPathSegments(arena, &meta, 1)).?).?.name);

    // Swapped, or both chunks claiming one of the two: each path names the other leaf.
    const bad = [_][]const []const []const u8{
        &.{ &.{ "a", "b" }, &.{"a.b"} },
        &.{ &.{"a.b"}, &.{"a.b"} },
        &.{ &.{ "a", "b" }, &.{ "a", "b" } },
    };
    for (bad) |paths| {
        try testing.expectError(error.ColumnChunkPathMismatch, openFooter(arena, try chunkPathFooterForTest(arena, &elems, paths)));
    }
}

test "a chunk's path finds its leaf, not a group that shares the path" {
    // Two top-level fields named `g`, an OPTIONAL group and a REQUIRED leaf. Looking the leaf's levels up by path
    // used to stop at the group and hand the leaf the group's definition level.
    var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_state.deinit();
    const arena = arena_state.allocator();
    var group = groupForTest("g", 1);
    group.repetition_type = .OPTIONAL;
    const elems = [_]schema.SchemaElement{ group, leafForTest("x"), leafForTest("g") };
    const meta = try openFooter(arena, try chunkPathFooterForTest(arena, &elems, &.{ &.{ "g", "x" }, &.{"g"} }));
    try testing.expectEqual(schema.Levels{ .max_def = 0, .max_rep = 0 }, meta.getColumnLevels(&.{"g"}));
    try testing.expectEqual(@as(?schema.Type, .INT64), meta.getColumnSchema(&.{"g"}).?.type);
    try testing.expectEqual(schema.Levels{ .max_def = 1, .max_rep = 0 }, meta.getColumnLevels(&.{ "g", "x" }));
    try testing.expect(meta.getColumnSchema(&.{ "g", "x", "y" }) == null);
}

test "footer open rejects counts, offsets and sizes no valid file can have" {
    // Hardwood's fixture: a negative data_page_offset, which every chunk reader cast straight to usize.
    const bytes = try readFileSlice("ci/fixtures/parquet/negative_data_page_offset.parquet", testing.allocator);
    defer testing.allocator.free(bytes);
    try testing.expectError(error.BadColumnChunkRange, open(testing.allocator, bytes));

    var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_state.deinit();
    const arena = arena_state.allocator();
    const good = try chunkPathFooterForTest(arena, &.{ leafForTest("a"), leafForTest("b") }, &.{ &.{"a"}, &.{"b"} });
    var base = try openFooter(arena, good);

    const Mutation = struct { err: anyerror, apply: *const fn (*schema.FileMetaData) void };
    const mutations = [_]Mutation{
        .{ .err = error.NegativeRowCount, .apply = struct {
            fn f(m: *schema.FileMetaData) void {
                m.num_rows = -1;
            }
        }.f },
        .{ .err = error.NegativeRowCount, .apply = struct {
            fn f(m: *schema.FileMetaData) void {
                m.row_groups.items[0].num_rows = -5;
            }
        }.f },
        .{ .err = error.BadColumnChunkRange, .apply = struct {
            fn f(m: *schema.FileMetaData) void {
                m.row_groups.items[0].columns.items[1].meta_data.?.total_compressed_size = -1;
            }
        }.f },
        .{ .err = error.BadColumnChunkRange, .apply = struct {
            fn f(m: *schema.FileMetaData) void {
                m.row_groups.items[0].columns.items[0].meta_data.?.data_page_offset = std.math.maxInt(i64);
                m.row_groups.items[0].columns.items[0].meta_data.?.total_compressed_size = 1;
            }
        }.f },
        .{ .err = error.BadSchemaElement, .apply = struct {
            fn f(m: *schema.FileMetaData) void {
                m.schema.items[1].type_length = -16;
            }
        }.f },
        .{ .err = error.BadSchemaElement, .apply = struct {
            fn f(m: *schema.FileMetaData) void {
                m.schema.items[2].num_children = 3; // a primitive with children
            }
        }.f },
        .{ .err = error.BadSchemaElement, .apply = struct {
            fn f(m: *schema.FileMetaData) void {
                m.schema.items[1].type = null; // neither a type nor children
            }
        }.f },
        // `b` lies past the root's last child.
        .{ .err = error.BadSchemaElement, .apply = struct {
            fn f(m: *schema.FileMetaData) void {
                m.schema.items[0].num_children = 1;
            }
        }.f },
        // The root promises a child the list never reaches.
        .{ .err = error.BadSchemaElement, .apply = struct {
            fn f(m: *schema.FileMetaData) void {
                m.schema.items[0].num_children = 3;
            }
        }.f },
        .{ .err = error.ColumnChunkRowCountMismatch, .apply = struct {
            fn f(m: *schema.FileMetaData) void {
                m.row_groups.items[0].columns.items[0].meta_data.?.num_values = 7;
            }
        }.f },
        .{ .err = error.ColumnChunkTypeMismatch, .apply = struct {
            fn f(m: *schema.FileMetaData) void {
                m.row_groups.items[0].columns.items[1].meta_data.?.type = .DOUBLE;
            }
        }.f },
    };
    for (mutations) |mut| {
        var meta = base;
        meta.schema = try base.schema.clone(arena);
        meta.row_groups = .empty;
        var rg = base.row_groups.items[0];
        rg.columns = try rg.columns.clone(arena);
        try meta.row_groups.append(arena, rg);
        mut.apply(&meta);
        var w: thrift.Writer = .init(arena);
        try meta.write(&w);
        try testing.expectError(mut.err, openFooter(arena, w.bytes()));
    }

    // A typed root with children is tolerated: segmentio/parquet-go writes one (tools/gen_typed_root_fixture.py).
    {
        const root_bytes = try readFileSlice("ci/fixtures/parquet/typed_root.parquet", testing.allocator);
        defer testing.allocator.free(root_bytes);
        const meta = try open(arena, root_bytes);
        try testing.expect(meta.schema.items[0].type != null);
        try testing.expect((meta.schema.items[0].num_children orelse 0) > 0);
        try testing.expect(findColumnIndex(&meta, "mint") != null);
    }

    // A negative page-index location drops that chunk's page index, not the file.
    {
        var meta = base;
        meta.row_groups = .empty;
        var rg = base.row_groups.items[0];
        rg.columns = try rg.columns.clone(arena);
        rg.columns.items[0].column_index_offset = 100;
        rg.columns.items[0].column_index_length = 10;
        rg.columns.items[0].offset_index_offset = 110;
        rg.columns.items[0].offset_index_length = -1;
        rg.columns.items[1].offset_index_offset = 120;
        rg.columns.items[1].offset_index_length = 8;
        try meta.row_groups.append(arena, rg);
        var w: thrift.Writer = .init(arena);
        try meta.write(&w);
        const opened = try openFooter(arena, w.bytes());
        const cols = opened.row_groups.items[0].columns.items;
        try testing.expect(cols[0].offset_index_offset == null and cols[0].column_index_offset == null);
        try testing.expect(cols[0].offset_index_length == null and cols[0].column_index_length == null);
        try testing.expectEqual(@as(?i32, 8), cols[1].offset_index_length);
    }

    // Deeper nesting than max_schema_depth.
    var deep: std.ArrayList(schema.SchemaElement) = .empty;
    for (0..max_schema_depth + 1) |_| try deep.append(arena, groupForTest("g", 1));
    try deep.append(arena, leafForTest("x"));
    var path: std.ArrayList([]const u8) = .empty;
    for (0..max_schema_depth + 1) |_| try path.append(arena, "g");
    try path.append(arena, "x");
    const deep_footer = try chunkPathFooterForTest(arena, deep.items, &.{path.items});
    try testing.expectError(error.SchemaTooDeep, openFooter(arena, deep_footer));

    // A file with no row groups is a valid empty table, and its leaves are still known.
    var empty = base;
    empty.row_groups = .empty;
    var w: thrift.Writer = .init(arena);
    try empty.write(&w);
    const opened = try openFooter(arena, w.bytes());
    try testing.expectEqual(@as(usize, 2), leafCount(&opened));
    try testing.expectEqualStrings("b", (try leafPathSegments(arena, &opened, 1)).?[0]);
    try testing.expect((try leafPathSegments(arena, &opened, 2)) == null);
}

/// Read a whole file for tests: `pub` so other modules' tests can load parquet-testing fixtures. A missing file is
/// `error.FileNotFound`, which those tests turn into a skip.
pub fn readFileSlice(path: []const u8, allocator: std.mem.Allocator) ![]u8 {
    return @import("../../local_fs.zig").readFile(allocator, path);
}

test "resolveColumn: bare names bind top-level, dotted paths bind nested, shared names are ambiguous" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const meta = try test_fixtures.sharedLeafNameMeta(a);

    // The top-level column wins over the earlier nested leaf of the same name.
    try testing.expectEqual(@as(?usize, 2), findColumnIndex(&meta, "key"));
    try testing.expectEqual(@as(usize, 2), try resolveColumn(&meta, "key"));
    try testing.expectEqual(@as(usize, 3), try resolveColumn(&meta, "amount"));
    try testing.expectEqual(@as(usize, 0), try resolveColumn(&meta, "r.key"));
    try testing.expectEqual(@as(usize, 1), try resolveColumn(&meta, "r.name"));
    try testing.expectEqual(@as(usize, 4), try resolveColumn(&meta, "a.x"));
    try testing.expectEqual(@as(usize, 5), try resolveColumn(&meta, "b.x"));
    // A bare name that only one nested leaf carries still binds to it.
    try testing.expectEqual(@as(usize, 1), try resolveColumn(&meta, "name"));
    // Two nested leaves and no top-level column: refuse to pick one.
    try testing.expectError(error.AmbiguousColumn, resolveColumn(&meta, "x"));
    try testing.expectEqual(@as(?usize, null), findColumnIndex(&meta, "x"));
    // Groups have no column chunk; partial and over-long paths match nothing.
    try testing.expectError(error.UnknownColumn, resolveColumn(&meta, "r"));
    try testing.expectError(error.UnknownColumn, resolveColumn(&meta, "r.ke"));
    try testing.expectError(error.UnknownColumn, resolveColumn(&meta, "r.key.z"));
    try testing.expectError(error.UnknownColumn, resolveColumn(&meta, "a.key"));
    try testing.expectError(error.UnknownColumn, resolveColumn(&meta, "rkey"));

    try testing.expectEqualStrings("key", leafSchemaElement(&meta, 2).?.name);
    try testing.expectEqual(@as(?schema.Type, .BYTE_ARRAY), leafSchemaElement(&meta, 1).?.type);
    try testing.expectEqual(@as(?schema.SchemaElement, null), leafSchemaElement(&meta, 6));
    try testing.expectEqualStrings("r.key", (try leafPath(a, &meta, 0)).?);
    try testing.expectEqualStrings("key", (try leafPath(a, &meta, 2)).?);
    try testing.expectEqualStrings("b.x", (try leafPath(a, &meta, 5)).?);
    try testing.expectEqual(@as(?[]const u8, null), try leafPath(a, &meta, 6));
}

test "resolveColumn: an exact top-level name wins over a nested path it spells" {
    // The fixture with its top-level `key` renamed `r.key`: a column literally named like the path of the group
    // `r`'s field `key`. Leaves: r.key (nested), r.name, `r.key` (top-level), amount, a.x, b.x.
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var meta = try test_fixtures.sharedLeafNameMeta(a);
    meta.schema.items[4].name = "r.key"; // the top-level `key` becomes `r.key`

    try testing.expectEqual(@as(usize, 2), try resolveColumn(&meta, "r.key"));
    try testing.expectEqual(@as(usize, 0), try resolveColumn(&meta, "key"));
    try testing.expectEqualStrings("r.key", (try leafPath(a, &meta, 2)).?);

    // Ambiguity hints name what would tell the candidates apart.
    try testing.expectError(error.AmbiguousColumn, resolveColumn(&meta, "x"));
    try testing.expect(std.mem.indexOf(u8, ambiguityHint(&meta, "x"), "dotted path") != null);
    meta.schema.items[5].name = "r.key"; // `amount` too: two top-level columns named `r.key`
    try testing.expectError(error.AmbiguousColumn, resolveColumn(&meta, "r.key"));
    try testing.expect(std.mem.indexOf(u8, ambiguityHint(&meta, "r.key"), "top-level") != null);
}

fn emptyMetaForTest() schema.FileMetaData {
    return .{ .version = 1, .schema = .empty, .num_rows = 0, .created_by = null, .row_groups = .empty };
}

test "leafLabel: a nested leaf whose dotted path is taken prints as a quoted path that binds back to it" {
    var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_state.deinit();
    const arena = arena_state.allocator();
    const Fixture = struct { elems: []const schema.SchemaElement, labels: []const []const u8 };
    const fixtures = [_]Fixture{
        // A top-level column literally named `a.b` beside the field `b` of a group `a`.
        .{
            .elems = &.{ leafForTest("a.b"), groupForTest("a", 1), leafForTest("b") },
            .labels = &.{ "a.b", "\"a\".\"b\"" },
        },
        // Same, nested side first.
        .{
            .elems = &.{ groupForTest("a", 1), leafForTest("b"), leafForTest("a.b") },
            .labels = &.{ "\"a\".\"b\"", "a.b" },
        },
        // Two nested paths that join alike and no top-level column: neither takes `a.b.c`.
        .{
            .elems = &.{ groupForTest("a", 1), leafForTest("b.c"), groupForTest("a.b", 1), leafForTest("c") },
            .labels = &.{ "\"a\".\"b.c\"", "\"a.b\".\"c\"" },
        },
        // An embedded quote is doubled.
        .{
            .elems = &.{ leafForTest("q\"x.y"), groupForTest("q\"x", 1), leafForTest("y") },
            .labels = &.{ "q\"x.y", "\"q\"\"x\".\"y\"" },
        },
        // A dotted path that reads as a quoted path names other segments, so the leaf is quoted.
        .{
            .elems = &.{ groupForTest("\"x\"", 1), leafForTest("\"y\"") },
            .labels = &.{"\"\"\"x\"\"\".\"\"\"y\"\"\""},
        },
        // No clash: dotted paths, as before.
        .{
            .elems = &.{
                groupForTest("r", 2), leafForTest("key"), leafForTest("v"), leafForTest("key"), leafForTest("a.b"),
            },
            .labels = &.{ "r.key", "r.v", "key", "a.b" },
        },
    };
    for (fixtures) |f| {
        var meta = emptyMetaForTest();
        var top: i32 = 0;
        var left: i32 = 0;
        for (f.elems) |e| {
            if (left == 0) top += 1 else left -= 1;
            left += e.num_children orelse 0;
        }
        try meta.schema.append(arena, groupForTest("root", top));
        try meta.schema.appendSlice(arena, f.elems);
        const all = (try leafLabels(arena, &meta)).names;
        for (f.labels, all, 0..) |want, from_all, i| {
            const label = (try leafLabel(arena, &meta, i)).?;
            try testing.expectEqualStrings(want, label);
            try testing.expectEqualStrings(want, from_all.?);
            try testing.expectEqual(i, try resolveColumn(&meta, label));
        }
    }

    // Only a top-level column literally named like the quoted path leaves the nested leaf no name of its own.
    var meta = emptyMetaForTest();
    try meta.schema.appendSlice(arena, &.{
        groupForTest("root", 3), leafForTest("a.b"), leafForTest("\"a\".\"b\""), groupForTest("a", 1), leafForTest("b"),
    });
    try testing.expectError(error.DuplicateOutputColumn, leafLabel(arena, &meta, 2));
    try testing.expectEqualStrings("\"a\".\"b\"", (try leafLabel(arena, &meta, 1)).?);
    const all = (try leafLabels(arena, &meta)).names;
    try testing.expectEqualStrings("a.b", all[0].?);
    try testing.expectEqual(@as(?[]const u8, null), all[2]);
}

test "quoted paths: parse, unquote, and bind by segments" {
    try testing.expect(isQuotedPath("\"a\""));
    try testing.expect(isQuotedPath("\"a\".\"b\""));
    try testing.expect(isQuotedPath("\"a.b\".\"c\"\"d\""));
    try testing.expect(!isQuotedPath("a.b"));
    try testing.expect(!isQuotedPath("\"a\".b"));
    try testing.expect(!isQuotedPath("\"a\"."));
    try testing.expect(!isQuotedPath("\"a"));
    try testing.expect(!isQuotedPath("\"a\"\"b"));

    // One quoted identifier unquotes, as it always has; a quoted path stays whole.
    try testing.expectEqualStrings("a.b", unquoteIdent(" \"a.b\" "));
    try testing.expectEqualStrings("my col", unquoteIdent("\"my col\""));
    try testing.expectEqualStrings("x", unquoteIdent("x"));
    try testing.expectEqualStrings("\"a\".\"b\"", unquoteIdent("\"a\".\"b\""));
    try testing.expectEqualStrings("\"q\"\"x\"", unquoteIdent("\"q\"\"x\""));

    try testing.expect(quotedPathEql("\"a\".\"b\"", &.{ "a", "b" }));
    try testing.expect(quotedPathEql("\"q\"\"x\".\"y\"", &.{ "q\"x", "y" }));
    try testing.expect(!quotedPathEql("\"a\".\"b\"", &.{"a.b"}));
    try testing.expect(!quotedPathEql("\"a\".\"b\"", &.{ "a", "b", "c" }));
    try testing.expect(!quotedPathEql("\"a\"", &.{ "a", "b" }));

    var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_state.deinit();
    const arena = arena_state.allocator();
    const meta = try test_fixtures.sharedLeafNameMeta(arena);
    try testing.expectEqual(@as(usize, 0), try resolveColumn(&meta, "\"r\".\"key\""));
    try testing.expectEqual(@as(usize, 2), try resolveColumn(&meta, "\"key\""));
    try testing.expectEqual(@as(usize, 5), try resolveColumn(&meta, "\"b\".\"x\""));
    // Exact segments only: no bare-name fallback, no partial or over-long path, no group.
    try testing.expectError(error.UnknownColumn, resolveColumn(&meta, "\"x\""));
    try testing.expectError(error.UnknownColumn, resolveColumn(&meta, "\"r\""));
    try testing.expectError(error.UnknownColumn, resolveColumn(&meta, "\"r.key\""));
    try testing.expectError(error.UnknownColumn, resolveColumn(&meta, "\"r\".\"key\".\"z\""));
}

test "resolveProjection: an exact top-level name first, then the names resolveColumn binds" {
    var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_state.deinit();
    const arena = arena_state.allocator();
    var meta = emptyMetaForTest();
    // Leaves: `"x"` 0, `"a"."b"` 1, x 2, a.b (in group a) 3, r.v 4, r.w 5, `a.b` 6.
    try meta.schema.appendSlice(arena, &.{
        groupForTest("root", 6), leafForTest("\"x\""), leafForTest("\"a\".\"b\""), leafForTest("x"),
        groupForTest("a", 1),    leafForTest("b"),     groupForTest("r", 2),       leafForTest("v"),
        leafForTest("w"),        leafForTest("a.b"),
    });
    const Case = struct { name: []const u8, want: []const u32 };
    const cases = [_]Case{
        // Quotes in a top-level name match it exactly before they are read as quoting.
        .{ .name = "\"x\"", .want = &.{0} },
        .{ .name = "\"a\".\"b\"", .want = &.{1} },
        .{ .name = "x", .want = &.{2} },
        // A top-level group, by name or quoted name, is all its leaves.
        .{ .name = "r", .want = &.{ 4, 5 } },
        .{ .name = "\"r\"", .want = &.{ 4, 5 } },
        // A top-level column named `a.b` wins over the path; the path's leaf by quoted path.
        .{ .name = "a.b", .want = &.{6} },
        .{ .name = "\"a.b\"", .want = &.{6} },
        .{ .name = "\"r\".\"v\"", .want = &.{4} },
        // A dotted path, and the bare name of the one nested field carrying it, as --filter binds them.
        .{ .name = "r.w", .want = &.{5} },
        .{ .name = "v", .want = &.{4} },
        .{ .name = "nope", .want = &.{} },
        .{ .name = "r.nope", .want = &.{} },
    };
    for (cases) |c| {
        const got = try resolveProjection(arena, &meta, c.name);
        testing.expectEqualSlices(u32, c.want, got) catch |err| {
            std.debug.print("resolveProjection({s})\n", .{c.name});
            return err;
        };
    }
    // The bare `b` binds the one field carrying it; two top-level columns of one name bind neither.
    try testing.expectEqualSlices(u32, &.{3}, try resolveProjection(arena, &meta, "b"));
    meta.schema.items[3].name = "\"x\"";
    try testing.expectError(error.AmbiguousColumn, resolveProjection(arena, &meta, "\"x\""));
}

/// Shapes of `wideSchemaForTest`: the labelling cases that cost differently.
pub const WideShape = enum {
    /// Structs `s<i>{x, y}`: every leaf labelled by its own dotted path.
    ordinary,
    /// Structs `s<i>{x}`, each beside a top-level column named `s<i>.x`: every nested label is a quoted path.
    colliding,
    /// Structs `"s<i>"{"x"}`: every dotted path reads as a quoted path naming other segments.
    quoted,
};

/// A schema of `structs` top-level structs in the given shape, for tests and the label benchmark.
pub fn wideSchemaForTest(arena: std.mem.Allocator, structs: usize, shape: WideShape) !schema.FileMetaData {
    var meta = emptyMetaForTest();
    const top: i32 = @intCast(if (shape == .colliding) 2 * structs else structs);
    try meta.schema.append(arena, groupForTest("root", top));
    for (0..structs) |i| switch (shape) {
        .ordinary => try meta.schema.appendSlice(arena, &.{
            groupForTest(try std.fmt.allocPrint(arena, "s{d}", .{i}), 2), leafForTest("x"), leafForTest("y"),
        }),
        .colliding => try meta.schema.appendSlice(arena, &.{
            leafForTest(try std.fmt.allocPrint(arena, "s{d}.x", .{i})),
            groupForTest(try std.fmt.allocPrint(arena, "s{d}", .{i}), 1),
            leafForTest("x"),
        }),
        .quoted => try meta.schema.appendSlice(arena, &.{
            groupForTest(try std.fmt.allocPrint(arena, "\"s{d}\"", .{i}), 1), leafForTest("\"x\""),
        }),
    };
    return meta;
}

test "leafLabels on wide schemas, with and without colliding names" {
    // Timing lives in `zig build bench-labels`; this checks the labels and that a sample binds back.
    var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_state.deinit();
    const arena = arena_state.allocator();
    const structs = 2_000;
    const Case = struct { shape: WideShape, leaf: usize, label: []const u8 };
    const cases = [_]Case{
        .{ .shape = .ordinary, .leaf = 2 * structs - 1, .label = "s1999.y" },
        .{ .shape = .colliding, .leaf = 2 * structs - 2, .label = "s1999.x" },
        .{ .shape = .colliding, .leaf = 2 * structs - 1, .label = "\"s1999\".\"x\"" },
        .{ .shape = .quoted, .leaf = structs - 1, .label = "\"\"\"s1999\"\"\".\"\"\"x\"\"\"" },
    };
    for (cases) |c| {
        const meta = try wideSchemaForTest(arena, structs, c.shape);
        const labels = (try leafLabels(arena, &meta)).names;
        try testing.expectEqualStrings(c.label, labels[c.leaf].?);
        var i: usize = c.leaf % 97;
        while (i < labels.len) : (i += 97) try testing.expectEqual(i, try resolveColumn(&meta, labels[i].?));
    }
}

test "leafLabels agrees with leafLabel on random schemas of colliding names" {
    // `leafLabels` answers from counts what `leafLabel` asks `resolveColumn`; names drawn from a pool of dotted,
    // quoted and repeated spellings make the two cover every rule.
    const pool = [_][]const u8{
        "a", "b", "a.b", "b.c", "a.b.c", "\"a\"", "\"a\".\"b\"", "a\"b", "\"\"\"a\"\"\"", "\"a.b\"",
    };
    var prng = std.Random.DefaultPrng.init(0x1abe1);
    const rand = prng.random();
    for (0..300) |_| {
        var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
        defer arena_state.deinit();
        const arena = arena_state.allocator();
        var meta = emptyMetaForTest();
        const top = 1 + rand.uintLessThan(u8, 4);
        try meta.schema.append(arena, groupForTest("root", top));
        // Depth-first, with the children each open group still expects.
        var pending: std.ArrayList(usize) = .empty;
        try pending.append(arena, top);
        while (pending.items.len > 0) {
            const last = &pending.items[pending.items.len - 1];
            if (last.* == 0) {
                _ = pending.pop();
                continue;
            }
            last.* -= 1;
            const name = pool[rand.uintLessThan(usize, pool.len)];
            if (pending.items.len < 4 and rand.boolean()) {
                const n = 1 + rand.uintLessThan(u8, 3);
                try meta.schema.append(arena, groupForTest(name, n));
                try pending.append(arena, n);
            } else try meta.schema.append(arena, leafForTest(name));
        }
        const all = (try leafLabels(arena, &meta)).names;
        try testing.expectEqual(leafCount(&meta), all.len);
        for (all, 0..) |got, i| {
            const want = leafLabel(arena, &meta, i) catch |err| switch (err) {
                error.DuplicateOutputColumn => null,
                else => return err,
            };
            if (want) |w| try testing.expectEqualStrings(w, got.?) else try testing.expect(got == null);
        }
    }
}
