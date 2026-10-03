//! Parquet file metadata: open the footer, expose row groups, prune.
//!
//! Schema parsing already lives in `src/core/schema.zig` (the Thrift-
//! encoded structs). This file is the thin layer that:
//!   1. Validates the leading + trailing PAR1 magic.
//!   2. Reads the 4-byte little-endian footer length.
//!   3. Slices the footer bytes and hands them to schema.FileMetaData.read.
//!   4. Exposes a stats-based row-group pruner for predicate pushdown.
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

/// Deepest group nesting accepted. The schema walkers recurse once per level and count definition / repetition
/// levels in a u8, so an unbounded depth is a stack overflow or an integer overflow. Real schemas stay far below.
pub const max_schema_depth = 200;

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
    // Nesting depth, counted the way the recursive walkers descend: into any element with children.
    var remaining: [max_schema_depth]usize = undefined;
    var depth: usize = 0;
    if (meta.schema.items.len > 1) for (meta.schema.items[1..]) |elem| {
        while (depth > 0 and remaining[depth - 1] == 0) depth -= 1;
        if (depth > 0) remaining[depth - 1] -= 1;
        const children: usize = @intCast(elem.num_children orelse 0);
        if (children == 0) continue;
        if (depth == max_schema_depth) return error.SchemaTooDeep;
        remaining[depth] = children;
        depth += 1;
    };
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
    const Group = struct { name: []const u8, remaining: usize, repeated: bool };
    var groups: std.ArrayList(Group) = .empty;
    defer groups.deinit(allocator);
    for (meta.schema.items[1..]) |elem| {
        while (groups.items.len > 0 and groups.items[groups.items.len - 1].remaining == 0) _ = groups.pop();
        if (groups.items.len > 0) groups.items[groups.items.len - 1].remaining -= 1;
        const parent_repeated = groups.items.len > 0 and groups.items[groups.items.len - 1].repeated;
        const here_repeated = parent_repeated or elem.repetition_type == .REPEATED;
        if (elem.type == null) { // group node; `checkFooterFields` makes this agree with `resolveColumn`'s child count
            try groups.append(allocator, .{
                .name = elem.name,
                .remaining = @intCast(@max(elem.num_children orelse 0, 0)),
                .repeated = here_repeated,
            });
            continue;
        }
        for (groups.items) |g| try names.append(allocator, g.name);
        try names.append(allocator, elem.name);
        try ends.append(allocator, names.items.len);
        try repeated.append(allocator, here_repeated);
        try types.append(allocator, elem.type.?); // non-null: groups were handled above
    }
    const leaf = struct {
        fn path(n: []const []const u8, e: []const usize, i: usize) []const []const u8 {
            return n[if (i == 0) 0 else e[i - 1]..e[i]];
        }
        fn eql(a: []const []const u8, b: []const []const u8, comptime ignore_case: bool) bool {
            if (a.len != b.len) return false;
            for (a, b) |x, y| {
                const same = if (ignore_case) std.ascii.eqlIgnoreCase(x, y) else std.mem.eql(u8, x, y);
                if (!same) return false;
            }
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
            if (leaf.eql(got, want, false)) continue;
            if (got.len > 0) {
                // Only reached for paths that are not already exact, so well-formed files never pay for this scan.
                for (0..ends.items.len) |j| {
                    if (leaf.eql(got, leaf.path(names.items, ends.items, j), false)) return error.ColumnChunkPathMismatch;
                }
                if (!leaf.eql(got, want, true)) return error.ColumnChunkPathMismatch;
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
/// Two candidates at the step that decides are `AmbiguousColumn`, never a
/// guess; `ambiguityHint` says what would tell them apart.
pub fn resolveColumn(
    file: *const schema.FileMetaData,
    column_name: []const u8,
) ColumnLookupError!usize {
    const walk = walkColumns(file, column_name);
    if (walk.top_hits == 1) return walk.top_hit.?;
    if (walk.top_hits > 1) return error.AmbiguousColumn;
    if (walk.path_hits == 1) return walk.path_hit.?;
    if (walk.path_hits > 1) return error.AmbiguousColumn;
    if (std.mem.indexOfScalar(u8, column_name, '.') != null) return error.UnknownColumn;
    if (walk.leaf_hits == 1) return walk.leaf_hit.?;
    if (walk.leaf_hits > 1) return error.AmbiguousColumn;
    return error.UnknownColumn;
}

/// For a name `resolveColumn` found ambiguous, what the candidates are and
/// how to name one of them.
pub fn ambiguityHint(file: *const schema.FileMetaData, column_name: []const u8) []const u8 {
    const walk = walkColumns(file, column_name);
    if (walk.top_hits > 1) return "several top-level columns have this name";
    if (walk.path_hits > 1) return "several nested fields have this path";
    return "several nested fields have this name; use the dotted path of the one you mean";
}

fn walkColumns(file: *const schema.FileMetaData, column_name: []const u8) ColumnWalk {
    var walk: ColumnWalk = .{ .items = file.schema.items, .name = column_name };
    if (file.schema.items.len == 0) return walk;
    const root_children: usize = @intCast(@max(file.schema.items[0].num_children orelse 0, 0));
    walk.pos = 1;
    var i: usize = 0;
    while (i < root_children and walk.pos < walk.items.len) : (i += 1) walk.visit(0);
    return walk;
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
    if (file.schema.items.len == 0) return null;
    var seen: usize = 0;
    for (file.schema.items[1..]) |elem| {
        if ((elem.num_children orelse 0) > 0) continue;
        if (seen == leaf_idx) return elem;
        seen += 1;
    }
    return null;
}

/// The dot-joined path of the `leaf_idx`-th primitive leaf (`key` for a
/// top-level column, `r.key` for a nested one): the name that binds back
/// to it through `resolveColumn`. Null when the schema has fewer leaves.
pub fn leafPath(arena: std.mem.Allocator, file: *const schema.FileMetaData, leaf_idx: usize) !?[]const u8 {
    if (file.schema.items.len == 0) return null;
    // Open groups on the way down, each with the children it has yet to visit.
    const Open = struct { name: []const u8, left: usize };
    var stack: std.ArrayList(Open) = .empty;
    defer stack.deinit(arena);
    var seen: usize = 0;
    for (file.schema.items[1..]) |elem| {
        while (stack.items.len > 0 and stack.items[stack.items.len - 1].left == 0) _ = stack.pop();
        if (stack.items.len > 0) stack.items[stack.items.len - 1].left -= 1;
        const n_children: usize = @intCast(@max(elem.num_children orelse 0, 0));
        if (n_children > 0) {
            try stack.append(arena, .{ .name = elem.name, .left = n_children });
            continue;
        }
        if (seen == leaf_idx) {
            if (stack.items.len == 0) return elem.name;
            var out: std.ArrayList(u8) = .empty;
            for (stack.items) |g| {
                try out.appendSlice(arena, g.name);
                try out.append(arena, '.');
            }
            try out.appendSlice(arena, elem.name);
            return try out.toOwnedSlice(arena);
        }
        seen += 1;
    }
    return null;
}

/// Number of leaf columns in the schema: the column-chunk count of every row group (enforced at footer open), and
/// still defined for a file with no row groups at all, which is a valid empty table.
pub fn leafCount(file: *const schema.FileMetaData) usize {
    if (file.schema.items.len == 0) return 0;
    var n: usize = 0;
    for (file.schema.items[1..]) |elem| {
        if ((elem.num_children orelse 0) == 0) n += 1;
    }
    return n;
}

/// Full schema path (root excluded) of leaf `leaf_idx` as its segments, or null past the last leaf. Equal to that
/// leaf's chunk `path_in_schema` (enforced at footer open) but needs no row group. Strings borrow the schema's names.
/// Compare paths by these segments, never by `leafPath`'s dot-joined form: a top-level column named `a.b` and the
/// field `b` of a group `a` join to the same string.
pub fn leafPathSegments(arena: std.mem.Allocator, file: *const schema.FileMetaData, leaf_idx: usize) !?[]const []const u8 {
    if (file.schema.items.len == 0) return null;
    const Group = struct { name: []const u8, remaining: usize };
    var groups: std.ArrayList(Group) = .empty;
    defer groups.deinit(arena);
    var leaf: usize = 0;
    for (file.schema.items[1..]) |elem| {
        while (groups.items.len > 0 and groups.items[groups.items.len - 1].remaining == 0) _ = groups.pop();
        if (groups.items.len > 0) groups.items[groups.items.len - 1].remaining -= 1;
        const n_children: usize = @intCast(@max(elem.num_children orelse 0, 0));
        if (n_children > 0) {
            try groups.append(arena, .{ .name = elem.name, .remaining = n_children });
            continue;
        }
        if (leaf == leaf_idx) {
            const path = try arena.alloc([]const u8, groups.items.len + 1);
            for (groups.items, 0..) |g, i| path[i] = g.name;
            path[groups.items.len] = elem.name;
            return path;
        }
        leaf += 1;
    }
    return null;
}

/// DFS over the flat schema that counts leaves and matches `name` against
/// top-level leaf names, nested leaves' dot-joined paths, and nested leaves'
/// own names.
const ColumnWalk = struct {
    items: []const schema.SchemaElement,
    name: []const u8,
    pos: usize = 0,
    leaf_idx: usize = 0,
    top_hit: ?usize = null,
    top_hits: usize = 0,
    path_hit: ?usize = null,
    path_hits: usize = 0,
    leaf_hit: ?usize = null,
    leaf_hits: usize = 0,

    /// `matched` is how many bytes of `name` the ancestors' path consumed
    /// (including the trailing `.`), or null once they diverged.
    fn visitAt(self: *ColumnWalk, depth: usize, matched: ?usize) void {
        const elem = self.items[self.pos];
        self.pos += 1;

        var here: ?usize = null;
        if (matched) |m| {
            const rest = self.name[m..];
            if (std.mem.startsWith(u8, rest, elem.name)) here = m + elem.name.len;
        }

        const n_children: usize = @intCast(@max(elem.num_children orelse 0, 0));
        if (n_children == 0) {
            if (depth == 0 and std.mem.eql(u8, elem.name, self.name)) {
                self.top_hit = self.leaf_idx;
                self.top_hits += 1;
            } else if (depth > 0 and here != null and here.? == self.name.len) {
                self.path_hit = self.leaf_idx;
                self.path_hits += 1;
            } else if (depth > 0 and std.mem.eql(u8, elem.name, self.name)) {
                self.leaf_hit = self.leaf_idx;
                self.leaf_hits += 1;
            }
            self.leaf_idx += 1;
            return;
        }

        const child_matched: ?usize = if (here) |h|
            (if (h < self.name.len and self.name[h] == '.') h + 1 else null)
        else
            null;
        var i: usize = 0;
        while (i < n_children and self.pos < self.items.len) : (i += 1) self.visitAt(depth + 1, child_matched);
    }

    fn visit(self: *ColumnWalk, depth: usize) void {
        self.visitAt(depth, 0);
    }
};

/// Decision returned by the row-group pruner. `keep` if the row group
/// might contain matching rows; `skip` if its stats prove it can't.
/// `unknown` if the stats are missing/insufficient to decide.
pub const Decision = enum { keep, skip, unknown };

/// Equality pruner: returns `skip` iff the column's `[min_value, max_value]`
/// range doesn't include `needle`. Comparison is unsigned bytewise, the
/// type-defined order of a byte-array column. The deprecated `min`/`max`
/// are never read: they were written with a signed comparison, which
/// misorders any byte at or above 0x80.
pub fn pruneEqual(
    rg: *const schema.RowGroup,
    column_index: usize,
    needle: []const u8,
) Decision {
    if (column_index >= rg.columns.items.len) return .unknown;
    const meta = rg.columns.items[column_index].meta_data orelse return .unknown;
    const stats = meta.statistics orelse return .unknown;

    const min = stats.min_value orelse return .unknown;
    const max = stats.max_value orelse return .unknown;

    if (std.mem.lessThan(u8, needle, min)) return .skip;
    if (std.mem.lessThan(u8, max, needle)) return .skip;
    return .keep;
}

/// Range pruner: returns `skip` iff the column's `[min_value, max_value]`
/// doesn't overlap the half-open interval `[lo, hi)`. Same bytewise order,
/// and the same refusal of the deprecated pair, as `pruneEqual`.
pub fn pruneRange(
    rg: *const schema.RowGroup,
    column_index: usize,
    lo: []const u8,
    hi: []const u8,
) Decision {
    if (column_index >= rg.columns.items.len) return .unknown;
    const meta = rg.columns.items[column_index].meta_data orelse return .unknown;
    const stats = meta.statistics orelse return .unknown;

    const min = stats.min_value orelse return .unknown;
    const max = stats.max_value orelse return .unknown;

    // Disjoint if max < lo OR min >= hi.
    if (std.mem.lessThan(u8, max, lo)) return .skip;
    if (!std.mem.lessThan(u8, min, hi)) return .skip;
    return .keep;
}

// ============================================================
// Tests
// ============================================================

const testing = std.testing;

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

test "pruneEqual skips when stats range excludes the value" {
    // Build a synthetic RowGroup with one column whose stats say min='A', max='C'.
    var rg: schema.RowGroup = .{
        .columns = .empty,
        .total_byte_size = 0,
        .num_rows = 0,
    };
    defer rg.columns.deinit(testing.allocator);

    const meta: schema.ColumnMetaData = .{
        .type = .BYTE_ARRAY,
        .encodings = .empty,
        .path_in_schema = .empty,
        .codec = .UNCOMPRESSED,
        .num_values = 100,
        .total_uncompressed_size = 1024,
        .total_compressed_size = 1024,
        .data_page_offset = 0,
        .index_page_offset = null,
        .dictionary_page_offset = null,
        .statistics = .{
            .min_value = "A",
            .max_value = "C",
        },
    };
    try rg.columns.append(testing.allocator, .{
        .file_path = null,
        .file_offset = 0,
        .meta_data = meta,
    });

    try testing.expectEqual(Decision.keep, pruneEqual(&rg, 0, "B"));
    try testing.expectEqual(Decision.skip, pruneEqual(&rg, 0, "Z"));
    try testing.expectEqual(Decision.skip, pruneEqual(&rg, 0, "0"));
    try testing.expectEqual(Decision.unknown, pruneEqual(&rg, 99, "X")); // bad index

    // The deprecated pair alone is signed-ordered: for {"a", "b", "é"} it reads min "é" (0xC3 is negative), max "b".
    rg.columns.items[0].meta_data.?.statistics = .{ .min = "\xc3\xa9", .max = "b" };
    try testing.expectEqual(Decision.unknown, pruneEqual(&rg, 0, "a"));
    try testing.expectEqual(Decision.unknown, pruneRange(&rg, 0, "a", "aa"));
}

// Read a file fully into memory using raw syscalls. Std.Io.Dir would
// require an Io vtable; the test suite doesn't have one set up.
/// `pub` so other modules' tests can load parquet-testing fixtures without repeating the syscall dance.
pub fn readFileSlice(path: []const u8, allocator: std.mem.Allocator) ![]u8 {
    const linux = std.os.linux;
    var path_z: [256]u8 = undefined;
    if (path.len + 1 > path_z.len) return error.PathTooLong;
    @memcpy(path_z[0..path.len], path);
    path_z[path.len] = 0;

    const r_open = linux.openat(linux.AT.FDCWD, @ptrCast(&path_z[0]), .{ .ACCMODE = .RDONLY, .CLOEXEC = true }, 0);
    const fd: linux.fd_t = signedOrError(r_open) catch return error.FileNotFound;
    defer _ = linux.close(fd);

    // Get size via lseek(SEEK_END), then rewind. 0.16 stdlib dropped
    // fstat in favor of statx; lseek is the simplest path.
    const SEEK_END: usize = 2;
    const SEEK_SET: usize = 0;
    const end_pos = linux.lseek(fd, 0, SEEK_END);
    if (errIs(end_pos)) return error.SeekFailed;
    _ = linux.lseek(fd, 0, SEEK_SET);
    const size: usize = @intCast(end_pos);

    const buf = try allocator.alloc(u8, size);
    errdefer allocator.free(buf);

    var off: usize = 0;
    while (off < size) {
        const n = linux.read(fd, buf[off..].ptr, size - off);
        if (errIs(n)) return error.ReadFailed;
        const bytes: usize = @intCast(n);
        if (bytes == 0) break;
        off += bytes;
    }
    return buf;
}

fn errIs(r: usize) bool {
    const signed: isize = @bitCast(r);
    return signed >= -4095 and signed < 0;
}

fn signedOrError(r: usize) error{SyscallFailed}!std.os.linux.fd_t {
    if (errIs(r)) return error.SyscallFailed;
    return @intCast(@as(isize, @bitCast(r)));
}

/// Test fixture: `root{ r{key: INT64, name: BYTE_ARRAY}, key: INT64, amount: INT64, a{x: INT32}, b{x: INT32} }`.
/// Leaves in order: r.key, r.name, key, amount, a.x, b.x. The top-level `key` shares its leaf name with `r.key`
/// and sits after it; `x` names a leaf in two groups and none at the top level.
pub fn sharedLeafNameMetaForTest(arena: std.mem.Allocator) !schema.FileMetaData {
    const E = struct {
        fn group(name: []const u8, n: i32) schema.SchemaElement {
            return .{ .type = null, .type_length = null, .repetition_type = .REQUIRED, .name = name, .num_children = n, .scale = null, .precision = null, .field_id = null };
        }
        fn leaf(name: []const u8, t: schema.Type) schema.SchemaElement {
            return .{ .type = t, .type_length = null, .repetition_type = .REQUIRED, .name = name, .num_children = null, .scale = null, .precision = null, .field_id = null };
        }
    };
    var meta: schema.FileMetaData = .{ .version = 1, .schema = .empty, .num_rows = 0, .created_by = null, .row_groups = .empty };
    try meta.schema.appendSlice(arena, &.{
        E.group("schema", 5),
        E.group("r", 2),
        E.leaf("key", .INT64),
        E.leaf("name", .BYTE_ARRAY),
        E.leaf("key", .INT64),
        E.leaf("amount", .INT64),
        E.group("a", 1),
        E.leaf("x", .INT32),
        E.group("b", 1),
        E.leaf("x", .INT32),
    });
    return meta;
}

test "resolveColumn: bare names bind top-level, dotted paths bind nested, shared names are ambiguous" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const meta = try sharedLeafNameMetaForTest(a);

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
    var meta = try sharedLeafNameMetaForTest(a);
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
