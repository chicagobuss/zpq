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
};

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

    var reader = thrift.Reader.init(footer_bytes);
    return schema.FileMetaData.read(allocator, &reader) catch return error.BadMetadata;
}

/// Parse a parquet footer thrift payload that was fetched separately from
/// the full file body. S3 range reads use this to avoid materializing a
/// virtual slice whose length is the whole object.
pub fn openFooter(
    allocator: std.mem.Allocator,
    footer_bytes: []const u8,
) !schema.FileMetaData {
    var reader = thrift.Reader.init(footer_bytes);
    return schema.FileMetaData.read(allocator, &reader) catch return error.BadMetadata;
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
