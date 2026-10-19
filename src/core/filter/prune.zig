//! Row-group pruning by walking a filter AST + the row-group's
//! per-column statistics.
//!
//! Three-valued logic:
//!   keep    — stats overlap predicate, can't prune
//!   skip    — stats prove no row in this group can match
//!   unknown — stats absent or insufficient; conservatively keep
//!
//! AND/OR composites combine child decisions per the truth table.

const std = @import("std");
const ast = @import("ast.zig");
const encoded = @import("encoded.zig");
const schema = @import("../schema.zig");
const decimal_mod = @import("../parquet/decimal.zig");
const statistics = @import("../parquet/statistics.zig");
const membership = @import("membership.zig");

pub const Decision = enum {
    keep,
    skip,
    unknown,
    always_match,

    fn andCombine(a: Decision, b: Decision) Decision {
        // AND: if either says skip, the conjunction can't match.
        if (a == .skip or b == .skip) return .skip;
        // If both are always_match, the conjunction always matches.
        if (a == .always_match and b == .always_match) return .always_match;
        // Otherwise, if either is keep or always_match, we must keep/evaluate.
        if (a == .keep or b == .keep or a == .always_match or b == .always_match) return .keep;
        return .unknown;
    }

    fn orCombine(a: Decision, b: Decision) Decision {
        // OR: if either is always_match, the disjunction always matches.
        if (a == .always_match or b == .always_match) return .always_match;
        // OR: only skip if BOTH can't match.
        if (a == .skip and b == .skip) return .skip;
        // If either is keep, we must keep/evaluate.
        if (a == .keep or b == .keep) return .keep;
        return .unknown;
    }
};

/// Walk the filter AST against a row group's column statistics.
/// Returns whether this row group can be skipped entirely.
///
/// `file_meta` is used to recognise DECIMAL columns whose stat bytes
/// need byte-to-i128-to-f64 decoding instead of raw encoded-bytes
/// comparison. Pass null to disable that path (callers that don't
/// have file metadata handy will safely degrade to .unknown for any
/// DECIMAL column).
pub fn pruneRowGroup(
    rg: *const schema.RowGroup,
    filter: ast.Filter,
    file_meta: ?*const schema.FileMetaData,
) Decision {
    return switch (filter) {
        .int32, .int64, .uint64, .float, .double, .decimal, .string => withMembership(
            rg,
            filter,
            valueStats(rg, filter, file_meta),
        ),
        .boolean => |leaf| pruneBoolean(rg, leaf.col_idx, leaf.op, leaf.value, file_meta),
        .null_check => |nc| pruneNullCheck(rg, nc.col_idx, nc.is_not, file_meta),
        // LIKE: prefix patterns are stat-prunable via min/max (a future
        // win); conservatively keep for now. Correctness is in eval.
        .like => .unknown,
        .and_filter, .or_filter => |c| blk: {
            if (c.neverTrue(filter == .and_filter)) break :blk .skip;
            var d = pruneRowGroup(rg, c.children[0], file_meta);
            for (c.children[1..]) |child| {
                const e = pruneRowGroup(rg, child, file_meta);
                d = if (filter == .and_filter) Decision.andCombine(d, e) else Decision.orCombine(d, e);
            }
            break :blk d;
        },
    };
}

/// A comparison leaf's decision from statistics alone.
fn valueStats(rg: *const schema.RowGroup, leaf: ast.Filter, file_meta: ?*const schema.FileMetaData) Decision {
    return switch (leaf) {
        .int32 => |l| pruneNumeric(i32, rg, l.col_idx, l.op, l.value, .INT32, file_meta),
        .int64 => |l| pruneNumeric(i64, rg, l.col_idx, l.op, l.value, .INT64, file_meta),
        .uint64 => |l| pruneUnsigned(rg, l.col_idx, l.op, l.value, file_meta),
        .float => |l| pruneNumeric(f32, rg, l.col_idx, l.op, l.value, .FLOAT, file_meta),
        .double => |l| pruneNumeric(f64, rg, l.col_idx, l.op, l.value, .DOUBLE, file_meta),
        .decimal => |l| pruneDecimal(rg, l.col_idx, l.op, l.value, file_meta),
        .string => |l| pruneBytes(rg, l.col_idx, l.op, l.value, .BYTE_ARRAY, file_meta),
        else => unreachable,
    };
}

/// A leaf's statistics decision, sharpened by membership: a Bloom filter or dictionary can only rule a literal out, so
/// it turns an open leaf into a skip and never touches one statistics skip or prove fully matching.
fn withMembership(rg: *const schema.RowGroup, leaf: ast.Filter, stats: Decision) Decision {
    if (stats != .keep and stats != .unknown) return stats;
    return if (membership.provenAbsent(rg, leaf)) .skip else stats;
}

// ------------------------------------------------------------
// The full-match proof.
//
// `.always_match` is far more dangerous than `.skip`: callers act on it by never evaluating the filter (aggregates,
// which also stop fetching filter-only columns) or by byte-copying the row group (write path). A wrong answer is
// silently wrong output, so every `.always_match` must pass these gates, and anything zpq cannot read with certainty
// is rejected rather than guessed at:
//   - deprecated `min`/`max` (signed-ordered for every type) never count; only `min_value`/`max_value` do,
//   - a declared column order other than TYPE_DEFINED_ORDER,
//   - repeated columns, where null_count and num_values count leaf values rather than rows,
//   - unsigned integers (skipped in unsigned order, but no proof reads them yet), floats (NaN), DECIMAL, FLBA, INT96,
//     and any annotation not on the allow-lists below,
//   - a missing null_count when the predicate needs nulls absent.
// Missing file metadata disables the proof: none of the schema checks can run.
// ------------------------------------------------------------

/// The column's leaf schema element, when it is flat and its chunk's value count is the row count.
fn flatLeaf(rg: *const schema.RowGroup, col_idx: usize, file_meta: ?*const schema.FileMetaData) ?schema.SchemaElement {
    const fm = file_meta orelse return null;
    if (col_idx >= rg.columns.items.len) return null;
    const cm = rg.columns.items[col_idx].meta_data orelse return null;
    // getColumnLevels reports {0, 0} for a path it cannot find; resolve the element first so that can't pass as flat.
    const elem = fm.getColumnSchema(cm.path_in_schema.items) orelse return null;
    if (elem.type == null or elem.type.? != cm.type) return null;
    if (fm.getColumnLevels(cm.path_in_schema.items).max_rep != 0) return null;
    if (cm.num_values != rg.num_rows) return null;
    return elem;
}

const ComparisonProof = struct {
    elem: schema.SchemaElement,
    min: []const u8,
    max: []const u8,
    max_exact: bool,
};

/// Bounds a comparison's full-match proof may use: type-defined `min_value`/`max_value` on a flat column with
/// `null_count` present and zero (a null row fails every comparison).
fn comparisonProof(
    rg: *const schema.RowGroup,
    col_idx: usize,
    file_meta: ?*const schema.FileMetaData,
) ?ComparisonProof {
    const elem = flatLeaf(rg, col_idx, file_meta) orelse return null;
    if (!statistics.boundsReadable(rg, col_idx, file_meta)) return null;
    const stats = rg.columns.items[col_idx].meta_data.?.statistics orelse return null;
    if ((stats.null_count orelse return null) != 0) return null;
    return .{
        .elem = elem,
        .min = stats.min_value orelse return null,
        .max = stats.max_value orelse return null,
        .max_exact = stats.is_max_value_exact orelse false,
    };
}

/// Prune on `IS NULL` / `IS NOT NULL` using the `null_count` stat:
///   IS NULL     → skip when null_count == 0; always_match when every row is null
///   IS NOT NULL → skip when every row is null; always_match when null_count == 0
/// Missing null_count → .unknown (eval handles correctness either way).
fn pruneNullCheck(
    rg: *const schema.RowGroup,
    col_idx: usize,
    is_not: bool,
    file_meta: ?*const schema.FileMetaData,
) Decision {
    if (col_idx >= rg.columns.items.len) return .unknown;
    const meta = rg.columns.items[col_idx].meta_data orelse return .unknown;
    const stats = meta.statistics orelse return .unknown;
    const null_count = stats.null_count orelse return .unknown;
    const all_null = null_count >= rg.num_rows;
    if (is_not) {
        if (all_null) return .skip;
        if (null_count == 0 and flatLeaf(rg, col_idx, file_meta) != null) return .always_match;
        return .keep;
    }
    if (null_count == 0) return .skip;
    if (null_count == rg.num_rows and flatLeaf(rg, col_idx, file_meta) != null) return .always_match;
    return .keep;
}

fn pruneNumeric(
    comptime T: type,
    rg: *const schema.RowGroup,
    col_idx: usize,
    op: ast.Operator,
    value: T,
    parquet_type: schema.Type,
    file_meta: ?*const schema.FileMetaData,
) Decision {
    if (col_idx >= rg.columns.items.len) return .unknown;
    const meta = rg.columns.items[col_idx].meta_data orelse return .unknown;

    // Type-mismatch guard: stats bytes are in the column's physical
    // wire format. If the filter leaf was built with a different
    // parquet type, the encoded comparison would compare incompatible
    // byte formats and silently produce wrong pruning decisions.
    if (meta.type != parquet_type) return .unknown;

    const b = statistics.chunkBounds(rg, col_idx, file_meta) orelse return .unknown;

    if (!encoded.rangeIntersects(T, op, b.min, b.max, value, nanFree(meta))) return .skip;
    // The positive assertion reads only the bounds `comparisonProof` vouches for, never the deprecated pair.
    if (comparisonProof(rg, col_idx, file_meta)) |p| {
        if (statistics.signedIntOrder(p.elem) and encoded.rangeAlwaysMatches(T, op, p.min, p.max, value))
            return .always_match;
    }
    return .keep;
}

/// Whether a chunk is known to hold no NaN: a nan_count of zero. Float bounds leave NaN out, so without one a NaN row
/// may pass a comparison no bound does (see `encoded.rangeIntersects`).
fn nanFree(meta: schema.ColumnMetaData) bool {
    const stats = meta.statistics orelse return false;
    return stats.nan_count == 0;
}

/// Unsigned columns compare bounds as unsigned. `chunkBounds` never hands back the signed-ordered deprecated pair,
/// whose bounds are swapped for a chunk spanning 2^31 (or 2^63).
fn pruneUnsigned(
    rg: *const schema.RowGroup,
    col_idx: usize,
    op: ast.Operator,
    value: u64,
    file_meta: ?*const schema.FileMetaData,
) Decision {
    const b = statistics.chunkBounds(rg, col_idx, file_meta) orelse return .unknown;
    return unsignedRangeDecision(rg.columns.items[col_idx].meta_data.?.type, b.min, b.max, op, value);
}

fn unsignedRangeDecision(
    physical: schema.Type,
    min: []const u8,
    max: []const u8,
    op: ast.Operator,
    value: u64,
) Decision {
    const lo = readUnsignedBound(physical, min) orelse return .unknown;
    const hi = readUnsignedBound(physical, max) orelse return .unknown;
    if (lo > hi) return .unknown; // corrupt, or written in signed order by a non-conforming writer
    return if (encoded.rangeOverlapsValue(u64, op, lo, hi, value)) .keep else .skip;
}

fn readUnsignedBound(physical: schema.Type, bytes: []const u8) ?u64 {
    return switch (physical) {
        .INT32 => if (bytes.len == 4) std.mem.readInt(u32, bytes[0..4], .little) else null,
        .INT64 => if (bytes.len == 8) std.mem.readInt(u64, bytes[0..8], .little) else null,
        else => null,
    };
}

/// The DECIMAL kind of `col_idx`'s leaf, from the schema: the stat bytes are in its physical wire format (INT32/INT64
/// little-endian, FLBA and BYTE_ARRAY big-endian two's complement).
fn decimalKind(rg: *const schema.RowGroup, col_idx: usize, file_meta: ?*const schema.FileMetaData) ?decimal_mod.Kind {
    const fm = file_meta orelse return null;
    if (col_idx >= rg.columns.items.len) return null;
    const meta = rg.columns.items[col_idx].meta_data orelse return null;
    const elem = fm.getColumnSchema(meta.path_in_schema.items) orelse return null;
    return decimal_mod.kindFromSchema(&elem);
}

/// Pruning for DECIMAL columns, on the exact unscaled bounds the leaf's literal is compared with.
fn pruneDecimal(
    rg: *const schema.RowGroup,
    col_idx: usize,
    op: ast.Operator,
    value: i128,
    file_meta: ?*const schema.FileMetaData,
) Decision {
    const kind = decimalKind(rg, col_idx, file_meta) orelse return .unknown;
    const b = statistics.chunkBounds(rg, col_idx, file_meta) orelse return .unknown;
    const lo = decimal_mod.statToI128(b.min, kind) orelse return .unknown;
    const hi = decimal_mod.statToI128(b.max, kind) orelse return .unknown;
    if (lo > hi) return .unknown; // corrupt stats — be safe
    return if (encoded.rangeOverlapsValue(i128, op, lo, hi, value)) .keep else .skip;
}

fn pruneBytes(
    rg: *const schema.RowGroup,
    col_idx: usize,
    op: ast.Operator,
    value: []const u8,
    parquet_type: schema.Type,
    file_meta: ?*const schema.FileMetaData,
) Decision {
    _ = parquet_type;
    const b = statistics.chunkBounds(rg, col_idx, file_meta) orelse return .unknown;

    if (!encoded.rangeIntersectsBytes(op, b.min, b.max, value)) return .skip;
    // The evaluator compares strings bytewise, so only bounds in that order prove every row passes.
    if (comparisonProof(rg, col_idx, file_meta)) |p| {
        if (statistics.bytewiseOrder(p.elem) and encoded.rangeAlwaysMatchesBytes(op, p.min, p.max, p.max_exact, value))
            return .always_match;
    }
    return .keep;
}

fn pruneBoolean(
    rg: *const schema.RowGroup,
    col_idx: usize,
    op: ast.Operator,
    value: bool,
    file_meta: ?*const schema.FileMetaData,
) Decision {
    const b = statistics.chunkBounds(rg, col_idx, file_meta) orelse return .unknown;
    if (b.min.len != 1 or b.max.len != 1) return .unknown;

    const min_b = b.min[0] != 0;
    const max_b = b.max[0] != 0;
    const v = value;

    return switch (op) {
        .Eq => if (min_b == max_b and min_b == v)
            .keep
        else if (min_b == max_b and min_b != v)
            .skip
        else
            .keep, // mixed range definitely contains the value
        .NotEq => if (min_b == max_b and min_b == v) .skip else .keep,
        else => .unknown, // range ops on bool aren't meaningful
    };
}

// ============================================================
// Tests
// ============================================================

const testing = std.testing;

fn synthRowGroupWithStats(
    arena: std.mem.Allocator,
    parquet_type: schema.Type,
    min_bytes: []const u8,
    max_bytes: []const u8,
) !schema.RowGroup {
    var rg: schema.RowGroup = .{
        .columns = .empty,
        .total_byte_size = 0,
        .num_rows = 0,
    };
    const meta: schema.ColumnMetaData = .{
        .type = parquet_type,
        .encodings = .empty,
        .path_in_schema = .empty,
        .codec = .UNCOMPRESSED,
        .num_values = 0,
        .total_uncompressed_size = 0,
        .total_compressed_size = 0,
        .data_page_offset = 0,
        .index_page_offset = null,
        .dictionary_page_offset = null,
        .statistics = .{ .min_value = min_bytes, .max_value = max_bytes },
    };
    try rg.columns.append(arena, .{ .file_path = null, .file_offset = 0, .meta_data = meta });
    return rg;
}

/// An estimate, never a proof, of the share of row group `rg`'s rows `f` passes: each range comparison on an integer
/// or float column takes its place between the chunk's bounds, as if values spread evenly; AND multiplies, OR adds
/// (at most 1). A leaf it cannot place (strings, LIKE, nulls, no bounds) passes everything, so a filter it knows
/// nothing about looks unselective and changes no plan. Only cost rules read it (`scan.applyCostRules`).
pub fn passFraction(f: ast.Filter, rg: *const schema.RowGroup, file_meta: ?*const schema.FileMetaData) f64 {
    return switch (f) {
        .int32 => |l| leafFraction(i32, rg, l, .INT32, file_meta),
        .int64 => |l| leafFraction(i64, rg, l, .INT64, file_meta),
        .float => |l| leafFraction(f32, rg, l, .FLOAT, file_meta),
        .double => |l| leafFraction(f64, rg, l, .DOUBLE, file_meta),
        .and_filter => |c| blk: {
            var p: f64 = 1;
            for (c.children) |child| p *= passFraction(child, rg, file_meta);
            break :blk p;
        },
        .or_filter => |c| blk: {
            var p: f64 = 0;
            for (c.children) |child| p += passFraction(child, rg, file_meta);
            break :blk @min(1, p);
        },
        else => 1,
    };
}

fn leafFraction(
    comptime T: type,
    rg: *const schema.RowGroup,
    leaf: ast.Leaf(T),
    parquet_type: schema.Type,
    file_meta: ?*const schema.FileMetaData,
) f64 {
    if (leaf.col_idx >= rg.columns.items.len) return 1;
    const meta = rg.columns.items[leaf.col_idx].meta_data orelse return 1;
    if (meta.type != parquet_type) return 1;
    const b = statistics.chunkBounds(rg, leaf.col_idx, file_meta) orelse return 1;
    const lo_v = encoded.readFixedLE(T, b.min) orelse return 1;
    const hi_v = encoded.readFixedLE(T, b.max) orelse return 1;
    const lo: f64 = if (comptime @typeInfo(T) == .float) lo_v else @floatFromInt(lo_v);
    const hi: f64 = if (comptime @typeInfo(T) == .float) hi_v else @floatFromInt(hi_v);
    const v: f64 = if (comptime @typeInfo(T) == .float) leaf.value else @floatFromInt(leaf.value);
    if (!(lo <= hi) or std.math.isNan(v)) return 1;
    // Integers hold hi - lo + 1 values; a float range is continuous.
    const width: f64 = if (comptime @typeInfo(T) == .float) hi - lo else hi - lo + 1;
    if (width <= 0) return 1;
    const below = std.math.clamp((v - lo) / width, 0, 1); // share strictly below v, roughly
    const one: f64 = if (comptime @typeInfo(T) == .float) 0 else 1 / width;
    return switch (leaf.op) {
        .Lt => below,
        .LtEq => @min(1, below + one),
        .Gt => @max(0, 1 - below - one),
        .GtEq => 1 - below,
        // One value of an integer range; a float equality is anyone's guess.
        .Eq => if (v < lo or v > hi) 0 else if (comptime @typeInfo(T) == .float) 1 else one,
        .NotEq => 1,
    };
}

test "pruneRowGroup keeps when value is in range" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    var min_buf: [4]u8 = undefined;
    var max_buf: [4]u8 = undefined;
    std.mem.writeInt(i32, &min_buf, 0, .little);
    std.mem.writeInt(i32, &max_buf, 100, .little);
    var rg = try synthRowGroupWithStats(a, .INT32, &min_buf, &max_buf);
    defer rg.columns.deinit(a);

    const filter: ast.Filter = .{ .int32 = .{ .col_idx = 0, .op = .Eq, .value = 50 } };
    try testing.expectEqual(Decision.keep, pruneRowGroup(&rg, filter, null));
}

test "pruneRowGroup skips when value is below range" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    var min_buf: [4]u8 = undefined;
    var max_buf: [4]u8 = undefined;
    std.mem.writeInt(i32, &min_buf, 0, .little);
    std.mem.writeInt(i32, &max_buf, 100, .little);
    var rg = try synthRowGroupWithStats(a, .INT32, &min_buf, &max_buf);
    defer rg.columns.deinit(a);

    const filter: ast.Filter = .{ .int32 = .{ .col_idx = 0, .op = .Eq, .value = -1 } };
    try testing.expectEqual(Decision.skip, pruneRowGroup(&rg, filter, null));
}

test "pruneRowGroup AND skips when either child skips" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    var min_buf: [4]u8 = undefined;
    var max_buf: [4]u8 = undefined;
    std.mem.writeInt(i32, &min_buf, 0, .little);
    std.mem.writeInt(i32, &max_buf, 100, .little);
    var rg = try synthRowGroupWithStats(a, .INT32, &min_buf, &max_buf);
    defer rg.columns.deinit(a);

    const composite = try ast.join(a, true, &.{
        .{ .int32 = .{ .col_idx = 0, .op = .Eq, .value = 50 } }, // keep
        .{ .int32 = .{ .col_idx = 0, .op = .Eq, .value = 999 } }, // skip
    });

    try testing.expectEqual(Decision.skip, pruneRowGroup(&rg, composite, null));
}

test "pruneRowGroup OR skips only when both children skip" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    var min_buf: [4]u8 = undefined;
    var max_buf: [4]u8 = undefined;
    std.mem.writeInt(i32, &min_buf, 0, .little);
    std.mem.writeInt(i32, &max_buf, 100, .little);
    var rg = try synthRowGroupWithStats(a, .INT32, &min_buf, &max_buf);
    defer rg.columns.deinit(a);

    const composite = try ast.join(a, false, &.{
        .{ .int32 = .{ .col_idx = 0, .op = .Eq, .value = -1 } }, // skip
        .{ .int32 = .{ .col_idx = 0, .op = .Eq, .value = 50 } }, // keep
    });

    try testing.expectEqual(Decision.keep, pruneRowGroup(&rg, composite, null));
}

test "pruneRowGroup uses DECIMAL stat decoding when file_meta provided" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    // Synthesise: INT64-backed DECIMAL(18,2), stats min=100 (=1.00),
    // max=2400 (=24.00) — like the int64_decimal fixture's RG.
    var min_buf = try a.alloc(u8, 8);
    var max_buf = try a.alloc(u8, 8);
    std.mem.writeInt(i64, min_buf[0..8], 100, .little);
    std.mem.writeInt(i64, max_buf[0..8], 2400, .little);

    var path: schema.StringList = .empty;
    try path.append(a, "value");

    var rg: schema.RowGroup = .{
        .columns = .empty,
        .total_byte_size = 0,
        .num_rows = 24,
    };
    try rg.columns.append(a, .{
        .file_path = null,
        .file_offset = 0,
        .meta_data = .{
            .type = .INT64,
            .encodings = .empty,
            .path_in_schema = path,
            .codec = .UNCOMPRESSED,
            .num_values = 24,
            .total_uncompressed_size = 0,
            .total_compressed_size = 0,
            .data_page_offset = 0,
            .index_page_offset = null,
            .dictionary_page_offset = null,
            .statistics = .{ .min_value = min_buf, .max_value = max_buf },
        },
    });

    // File metadata with a schema declaring DECIMAL(18, 2) on "value".
    var meta_schema: std.ArrayListUnmanaged(schema.SchemaElement) = .empty;
    try meta_schema.append(a, .{
        .type = null,
        .type_length = null,
        .repetition_type = null,
        .name = "schema",
        .num_children = 1,
        .converted_type = null,
        .logical_type = null,
        .scale = null,
        .precision = null,
        .field_id = null,
    });
    try meta_schema.append(a, .{
        .type = .INT64,
        .type_length = null,
        .repetition_type = .OPTIONAL,
        .name = "value",
        .num_children = 0,
        .converted_type = null,
        .logical_type = .{ .DECIMAL = .{ .scale = 2, .precision = 18 } },
        .scale = null,
        .precision = null,
        .field_id = null,
    });
    const file_meta: schema.FileMetaData = .{
        .version = 1,
        .schema = meta_schema,
        .num_rows = 24,
        .created_by = null,
        .row_groups = .empty,
    };

    // value > 10.0 → max=24.0 > 10 → keep.
    const f_keep: ast.Filter = .{ .decimal = .{ .col_idx = 0, .op = .Gt, .value = 1000 } };
    try testing.expectEqual(Decision.keep, pruneRowGroup(&rg, f_keep, &file_meta));

    // value > 100.0 → max=24.0 < 100 → skip.
    const f_skip: ast.Filter = .{ .decimal = .{ .col_idx = 0, .op = .Gt, .value = 10000 } };
    try testing.expectEqual(Decision.skip, pruneRowGroup(&rg, f_skip, &file_meta));

    // value < 0.5 → min=1.0 >= 0.5 → skip.
    const f_skip_lt: ast.Filter = .{ .decimal = .{ .col_idx = 0, .op = .Lt, .value = 50 } };
    try testing.expectEqual(Decision.skip, pruneRowGroup(&rg, f_skip_lt, &file_meta));

    // Without file_meta, DECIMAL recognition is impossible → degrade
    // to .unknown (the type-mismatch guard kicks in).
    try testing.expectEqual(Decision.unknown, pruneRowGroup(&rg, f_keep, null));
}

test "pruneRowGroup unknown when filter type mismatches column physical type" {
    // A .double filter against an INT64-backed DECIMAL column: the
    // column's physical type is INT64 but the user's filter literal
    // is a double. The stats min/max bytes are in INT64 wire format,
    // not DOUBLE, so we cannot safely compare and must bail out.
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    var min_buf: [8]u8 = undefined;
    var max_buf: [8]u8 = undefined;
    std.mem.writeInt(i64, &min_buf, 100, .little); // 1.00 at scale=2
    std.mem.writeInt(i64, &max_buf, 2400, .little); // 24.00 at scale=2
    var rg = try synthRowGroupWithStats(a, .INT64, &min_buf, &max_buf);
    defer rg.columns.deinit(a);

    const filter: ast.Filter = .{ .double = .{ .col_idx = 0, .op = .Gt, .value = 5.0 } };
    try testing.expectEqual(Decision.unknown, pruneRowGroup(&rg, filter, null));
}

test "pruneRowGroup unknown when stats absent" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    var rg: schema.RowGroup = .{
        .columns = .empty,
        .total_byte_size = 0,
        .num_rows = 0,
    };
    defer rg.columns.deinit(a);
    const meta: schema.ColumnMetaData = .{
        .type = .INT32,
        .encodings = .empty,
        .path_in_schema = .empty,
        .codec = .UNCOMPRESSED,
        .num_values = 0,
        .total_uncompressed_size = 0,
        .total_compressed_size = 0,
        .data_page_offset = 0,
        .index_page_offset = null,
        .dictionary_page_offset = null,
        .statistics = null,
    };
    try rg.columns.append(a, .{ .file_path = null, .file_offset = 0, .meta_data = meta });

    const filter: ast.Filter = .{ .int32 = .{ .col_idx = 0, .op = .Eq, .value = 42 } };
    try testing.expectEqual(Decision.unknown, pruneRowGroup(&rg, filter, null));
}

test "regression: a float row group whose NaN rows its bounds leave out is kept for `>`, `>=` and their NOT forms" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const lo = std.mem.toBytes(@as(f64, 0.0));
    const hi = std.mem.toBytes(@as(f64, 1.0));
    const unknown_nan = try ProofFixture.init(a, leafElem(.DOUBLE), .{ .min_value = &lo, .max_value = &hi, .null_count = 0 });
    const no_nan = try ProofFixture.init(a, leafElem(.DOUBLE), .{ .min_value = &lo, .max_value = &hi, .null_count = 0, .nan_count = 0 });
    const some_nan = try ProofFixture.init(a, leafElem(.DOUBLE), .{ .min_value = &lo, .max_value = &hi, .null_count = 0, .nan_count = 3 });
    const leaf = struct {
        fn of(op: ast.Operator, v: f64) ast.Filter {
            return .{ .double = .{ .col_idx = 0, .op = op, .value = v } };
        }
    }.of;
    // `NOT (x < 5)` binds as `x >= 5`: NaN passes it, so only a chunk known to hold none skips.
    for ([_]ast.Filter{ leaf(.Gt, 5), leaf(.GtEq, 5) }) |f| {
        try testing.expect(unknown_nan.decide(f) != .skip);
        try testing.expect(some_nan.decide(f) != .skip);
        try testing.expectEqual(Decision.skip, no_nan.decide(f));
    }
    // NaN fails `<`, `<=` and `=` a number: the bounds alone still skip.
    for ([_]ast.Filter{ leaf(.Lt, -1), leaf(.LtEq, -1), leaf(.Eq, 5) }) |f| {
        try testing.expectEqual(Decision.skip, unknown_nan.decide(f));
    }
    // A NaN literal: only NaN rows pass `= NaN`; nothing passes `> NaN`.
    const nan = std.math.nan(f64);
    try testing.expect(unknown_nan.decide(leaf(.Eq, nan)) != .skip);
    try testing.expectEqual(Decision.skip, no_nan.decide(leaf(.Eq, nan)));
    try testing.expectEqual(Decision.skip, unknown_nan.decide(leaf(.Gt, nan)));
    try testing.expect(no_nan.decide(leaf(.Lt, nan)) != .skip);
}

test "float pruning reads bounds under IEEE_754_TOTAL_ORDER, and a page index's own NaN counts" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const lo = std.mem.toBytes(@as(f64, 0.0));
    const hi = std.mem.toBytes(@as(f64, 1.0));
    const gt5: ast.Filter = .{ .double = .{ .col_idx = 0, .op = .Gt, .value = 5 } };
    const lt_neg: ast.Filter = .{ .double = .{ .col_idx = 0, .op = .Lt, .value = -1 } };

    var fx = try ProofFixture.init(a, leafElem(.DOUBLE), .{ .min_value = &lo, .max_value = &hi, .null_count = 0, .nan_count = 0 });
    var orders: std.ArrayListUnmanaged(i16) = .empty;
    try orders.append(a, schema.COLUMN_ORDER_IEEE_754_TOTAL);
    fx.fm.column_orders = orders;
    try testing.expectEqual(Decision.skip, fx.decide(gt5));
    try testing.expectEqual(Decision.skip, fx.decide(lt_neg));
    // An all-NaN chunk under that order has NaN bounds: nothing is pruned on them.
    const nan = std.mem.toBytes(std.math.nan(f64));
    var all_nan = try ProofFixture.init(a, leafElem(.DOUBLE), .{ .min_value = &nan, .max_value = &nan, .null_count = 0, .nan_count = 9 });
    all_nan.fm.column_orders = orders;
    try testing.expect(all_nan.decide(lt_neg) != .skip);
    try testing.expect(all_nan.decide(gt5) != .skip);
    // The order is for floats alone; on a string column it is no order zpq reads.
    var str = try ProofFixture.init(a, leafElem(.BYTE_ARRAY), .{ .min_value = "a", .max_value = "c", .null_count = 0 });
    str.fm.column_orders = orders;
    try testing.expectEqual(Decision.unknown, str.decide(.{ .string = .{ .col_idx = 0, .op = .Eq, .value = "z" } }));

    // Pages: the chunk's nan_count unknown, the index's count for the page decides.
    const unknown = try ProofFixture.init(a, leafElem(.DOUBLE), .{ .min_value = &lo, .max_value = &hi, .null_count = 0 });
    for ([_]?i64{ null, 3, 0 }) |page_nans| {
        var ci: schema.ColumnIndex = .{};
        try ci.null_pages.append(a, false);
        try ci.min_values.append(a, &lo);
        try ci.max_values.append(a, &hi);
        if (page_nans) |n| {
            ci.nan_counts = .empty;
            try ci.nan_counts.?.append(a, n);
        }
        const cis = [_]?schema.ColumnIndex{ci};
        const d = prunePage(&unknown.rg, 0, gt5, &cis, &unknown.fm, false);
        if (page_nans == 0) try testing.expectEqual(Decision.skip, d) else try testing.expect(d != .skip);
        try testing.expectEqual(Decision.skip, prunePage(&unknown.rg, 0, lt_neg, &cis, &unknown.fm, false));
    }
}

/// `test_fixtures.StatsColumn`, deciding filters against itself.
const ProofFixture = struct {
    rg: schema.RowGroup,
    fm: schema.FileMetaData,

    fn init(a: std.mem.Allocator, elem: schema.SchemaElement, stats: schema.Statistics) !ProofFixture {
        const c = try test_fixtures.StatsColumn.init(a, elem, stats);
        return .{ .rg = c.rg, .fm = c.fm };
    }

    fn decide(self: *const ProofFixture, f: ast.Filter) Decision {
        return pruneRowGroup(&self.rg, f, &self.fm);
    }
};

const test_fixtures = @import("../parquet/test_fixtures.zig");
const leafElem = test_fixtures.statsLeaf;

fn le64(a: std.mem.Allocator, v: i64) ![]const u8 {
    const b = try a.alloc(u8, 8);
    std.mem.writeInt(i64, b[0..8], v, .little);
    return b;
}

test "full-match proof: INT64 needs trusted min_value/max_value and zero nulls" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const ge10: ast.Filter = .{ .int64 = .{ .col_idx = 0, .op = .GtEq, .value = 10 } };
    const lo = try le64(a, 20);
    const hi = try le64(a, 30);

    var ok = try ProofFixture.init(a, leafElem(.INT64), .{ .min_value = lo, .max_value = hi, .null_count = 0 });
    try testing.expectEqual(Decision.always_match, ok.decide(ge10));
    // Without file metadata no schema gate can run.
    try testing.expectEqual(Decision.keep, pruneRowGroup(&ok.rg, ge10, null));

    // A declared order other than TYPE_DEFINED_ORDER; an absent list is read as type-defined, like parquet-mr does.
    // Bounds in an order zpq cannot read neither prove nor rule out anything.
    var orders: std.ArrayListUnmanaged(i16) = .empty;
    try orders.append(a, 0);
    ok.fm.column_orders = orders;
    try testing.expectEqual(Decision.unknown, ok.decide(ge10));

    // Deprecated min/max are signed-ordered for every type: they may skip, never prove.
    const legacy = try ProofFixture.init(a, leafElem(.INT64), .{ .min = lo, .max = hi, .null_count = 0 });
    try testing.expectEqual(Decision.keep, legacy.decide(ge10));
    const lt10: ast.Filter = .{ .int64 = .{ .col_idx = 0, .op = .Lt, .value = 10 } };
    try testing.expectEqual(Decision.skip, legacy.decide(lt10));

    const unknown_nulls = try ProofFixture.init(a, leafElem(.INT64), .{ .min_value = lo, .max_value = hi });
    try testing.expectEqual(Decision.keep, unknown_nulls.decide(ge10));
    const some_nulls = try ProofFixture.init(a, leafElem(.INT64), .{
        .min_value = lo,
        .max_value = hi,
        .null_count = 1,
    });
    try testing.expectEqual(Decision.keep, some_nulls.decide(ge10));

    var u64_elem = leafElem(.INT64);
    u64_elem.logical_type = .{ .INTEGER = .{ .bitWidth = 64, .isSigned = false } };
    const unsigned = try ProofFixture.init(a, u64_elem, .{ .min_value = lo, .max_value = hi, .null_count = 0 });
    try testing.expectEqual(Decision.keep, unsigned.decide(ge10));
    var u64_conv = leafElem(.INT64);
    u64_conv.converted_type = .UINT_64;
    const unsigned_conv = try ProofFixture.init(a, u64_conv, .{ .min_value = lo, .max_value = hi, .null_count = 0 });
    try testing.expectEqual(Decision.keep, unsigned_conv.decide(ge10));

    var ts_elem = leafElem(.INT64);
    ts_elem.logical_type = .{ .TIMESTAMP = .{ .isAdjustedToUTC = true, .unit = .{ .MICROS = .{} } } };
    const ts = try ProofFixture.init(a, ts_elem, .{ .min_value = lo, .max_value = hi, .null_count = 0 });
    try testing.expectEqual(Decision.always_match, ts.decide(ge10));
}

test "full-match proof: strings rely on max only when the writer marks it exact" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var str = leafElem(.BYTE_ARRAY);
    str.logical_type = .{ .STRING = .{} };
    const ge: ast.Filter = .{ .string = .{ .col_idx = 0, .op = .GtEq, .value = "b" } };
    const lt: ast.Filter = .{ .string = .{ .col_idx = 0, .op = .Lt, .value = "z" } };
    const eq: ast.Filter = .{ .string = .{ .col_idx = 0, .op = .Eq, .value = "m" } };

    // A truncated max ("m" for "mzzz...") that was not rounded up is not an upper bound; min stays a lower bound.
    const inexact = try ProofFixture.init(a, str, .{ .min_value = "c", .max_value = "m", .null_count = 0 });
    try testing.expectEqual(Decision.always_match, inexact.decide(ge));
    try testing.expectEqual(Decision.keep, inexact.decide(lt));

    const exact = try ProofFixture.init(a, str, .{
        .min_value = "c",
        .max_value = "m",
        .null_count = 0,
        .is_max_value_exact = true,
    });
    try testing.expectEqual(Decision.always_match, exact.decide(lt));
    try testing.expectEqual(Decision.keep, exact.decide(eq));
    const constant = try ProofFixture.init(a, str, .{
        .min_value = "m",
        .max_value = "m",
        .null_count = 0,
        .is_max_value_exact = true,
    });
    try testing.expectEqual(Decision.always_match, constant.decide(eq));

    // Only bytewise-ordered annotations: a BYTE_ARRAY DECIMAL is signed, and FLBA (UUID, INTERVAL) is excluded.
    var dec = leafElem(.BYTE_ARRAY);
    dec.logical_type = .{ .DECIMAL = .{ .scale = 0, .precision = 9 } };
    const decimal = try ProofFixture.init(a, dec, .{ .min_value = "c", .max_value = "m", .null_count = 0 });
    try testing.expectEqual(Decision.keep, decimal.decide(ge));
    const flba = try ProofFixture.init(a, leafElem(.FIXED_LEN_BYTE_ARRAY), .{
        .min_value = "c",
        .max_value = "m",
        .null_count = 0,
    });
    try testing.expectEqual(Decision.keep, flba.decide(ge));
}

test "full-match proof: IS [NOT] NULL from null_count" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const not_null: ast.Filter = .{ .null_check = .{ .col_idx = 0, .is_not = true } };
    const is_null: ast.Filter = .{ .null_check = .{ .col_idx = 0, .is_not = false } };

    const none = try ProofFixture.init(a, leafElem(.DOUBLE), .{ .null_count = 0 });
    try testing.expectEqual(Decision.always_match, none.decide(not_null));
    try testing.expectEqual(Decision.skip, none.decide(is_null));
    const all = try ProofFixture.init(a, leafElem(.DOUBLE), .{ .null_count = 100 });
    try testing.expectEqual(Decision.skip, all.decide(not_null));
    try testing.expectEqual(Decision.always_match, all.decide(is_null));
    const unknown = try ProofFixture.init(a, leafElem(.DOUBLE), .{});
    try testing.expectEqual(Decision.unknown, unknown.decide(not_null));

    // A repeated leaf counts values, not rows.
    var repeated = try ProofFixture.init(a, leafElem(.DOUBLE), .{ .null_count = 0 });
    repeated.rg.columns.items[0].meta_data.?.num_values = 250;
    try testing.expectEqual(Decision.keep, repeated.decide(not_null));
}

test "bounds are unreadable under an undeclared column order or an unordered annotation, for chunks and pages" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var str = leafElem(.BYTE_ARRAY);
    str.logical_type = .{ .STRING = .{} };
    var fx = try ProofFixture.init(a, str, .{ .min_value = "a", .max_value = "c", .null_count = 1 });
    var ci: schema.ColumnIndex = .{};
    try ci.null_pages.append(a, false);
    try ci.min_values.append(a, "a");
    try ci.max_values.append(a, "c");
    const cis = [_]?schema.ColumnIndex{ci};
    const eq_b: ast.Filter = .{ .string = .{ .col_idx = 0, .op = .Eq, .value = "B" } };

    try testing.expectEqual(Decision.skip, fx.decide(eq_b));
    try testing.expectEqual(Decision.skip, prunePage(&fx.rg, 0, eq_b, &cis, &fx.fm, false));

    var orders: std.ArrayListUnmanaged(i16) = .empty;
    try orders.append(a, 3); // an order this reader does not implement
    fx.fm.column_orders = orders;
    try testing.expectEqual(Decision.unknown, fx.decide(eq_b));
    try testing.expectEqual(Decision.unknown, prunePage(&fx.rg, 0, eq_b, &cis, &fx.fm, false));
    fx.fm.column_orders.?.items[0] = schema.COLUMN_ORDER_TYPE_DEFINED;
    try testing.expectEqual(Decision.skip, fx.decide(eq_b));

    // INTERVAL, and annotations zpq reads as UNKNOWN, define no order even under TYPE_DEFINED_ORDER.
    fx.fm.schema.items[1].converted_type = .INTERVAL;
    fx.fm.schema.items[1].logical_type = null;
    try testing.expectEqual(Decision.unknown, fx.decide(eq_b));
    try testing.expectEqual(Decision.unknown, prunePage(&fx.rg, 0, eq_b, &cis, &fx.fm, false));
    fx.fm.schema.items[1].converted_type = null;
    fx.fm.schema.items[1].logical_type = .{ .UNKNOWN = .{} };
    try testing.expectEqual(Decision.unknown, fx.decide(eq_b));

    // IS NULL reads null counts, which carry no order.
    const is_null: ast.Filter = .{ .null_check = .{ .col_idx = 0, .is_not = false } };
    fx.fm.column_orders.?.items[0] = 3;
    try testing.expectEqual(Decision.keep, fx.decide(is_null));
}

/// Walk the filter AST against a specific page's ColumnIndex metadata.
pub fn prunePage(
    rg: *const schema.RowGroup,
    page_idx: usize,
    filter: ast.Filter,
    col_indexes: []const ?schema.ColumnIndex,
    file_meta: ?*const schema.FileMetaData,
    trust_stats: bool,
) Decision {
    switch (filter) {
        .null_check, .like, .and_filter, .or_filter => {},
        inline else => |leaf| if (!statistics.boundsReadable(rg, leaf.col_idx, file_meta)) return .unknown,
    }
    return switch (filter) {
        .int32 => |leaf| prunePageNumeric(i32, rg, page_idx, leaf.col_idx, leaf.op, leaf.value, .INT32, col_indexes, trust_stats),
        .int64 => |leaf| prunePageNumeric(i64, rg, page_idx, leaf.col_idx, leaf.op, leaf.value, .INT64, col_indexes, trust_stats),
        .uint64 => |leaf| prunePageUnsigned(rg, page_idx, leaf.col_idx, leaf.op, leaf.value, col_indexes),
        .float => |leaf| prunePageNumeric(f32, rg, page_idx, leaf.col_idx, leaf.op, leaf.value, .FLOAT, col_indexes, trust_stats),
        .double => |leaf| prunePageNumeric(f64, rg, page_idx, leaf.col_idx, leaf.op, leaf.value, .DOUBLE, col_indexes, trust_stats),
        .decimal => |leaf| prunePageDecimal(rg, page_idx, leaf.col_idx, leaf.op, leaf.value, col_indexes, file_meta, trust_stats),
        .string => |leaf| prunePageBytes(page_idx, leaf.col_idx, leaf.op, leaf.value, .BYTE_ARRAY, col_indexes),
        .boolean => |leaf| prunePageBoolean(page_idx, leaf.col_idx, leaf.op, leaf.value, col_indexes),
        .null_check => |nc| prunePageNullCheck(page_idx, nc.col_idx, nc.is_not, col_indexes),
        .like => .unknown,
        .and_filter, .or_filter => |c| blk: {
            if (c.neverTrue(filter == .and_filter)) break :blk .skip;
            var d = prunePage(rg, page_idx, c.children[0], col_indexes, file_meta, trust_stats);
            for (c.children[1..]) |child| {
                const e = prunePage(rg, page_idx, child, col_indexes, file_meta, trust_stats);
                d = if (filter == .and_filter) Decision.andCombine(d, e) else Decision.orCombine(d, e);
            }
            break :blk d;
        },
    };
}

fn prunePageNullCheck(page_idx: usize, col_idx: usize, is_not: bool, col_indexes: []const ?schema.ColumnIndex) Decision {
    if (col_idx >= col_indexes.len) return .unknown;
    const ci = col_indexes[col_idx] orelse return .unknown;
    if (page_idx >= ci.null_pages.items.len) return .unknown;
    const is_null_page = ci.null_pages.items[page_idx];
    if (is_null_page) {
        return if (is_not) .skip else .always_match;
    }
    if (ci.null_counts) |nc| {
        if (page_idx < nc.items.len) {
            const null_count = nc.items[page_idx];
            if (is_not) {
                if (null_count == 0) return .always_match;
            } else {
                if (null_count == 0) return .skip;
            }
        }
    }
    return .keep;
}

fn prunePageNumeric(
    comptime T: type,
    rg: *const schema.RowGroup,
    page_idx: usize,
    col_idx: usize,
    op: ast.Operator,
    value: T,
    parquet_type: schema.Type,
    col_indexes: []const ?schema.ColumnIndex,
    trust_stats: bool,
) Decision {
    if (col_idx >= col_indexes.len or col_idx >= rg.columns.items.len) return .unknown;
    const ci = col_indexes[col_idx] orelse return .unknown;
    if (page_idx >= ci.null_pages.items.len) return .unknown;
    const meta = rg.columns.items[col_idx].meta_data orelse return .unknown;

    if (meta.type != parquet_type) return .unknown;

    // Check if null page
    if (ci.null_pages.items[page_idx]) return .skip;

    if (page_idx >= ci.min_values.items.len or page_idx >= ci.max_values.items.len) return .unknown;
    const min = ci.min_values.items[page_idx];
    const max = ci.max_values.items[page_idx];

    // The chunk's nan_count covers each of its pages; the index may count each page's own.
    const page_nan_free = if (ci.nan_counts) |nc| page_idx < nc.items.len and nc.items[page_idx] == 0 else false;
    if (!encoded.rangeIntersects(T, op, min, max, value, nanFree(meta) or page_nan_free)) return .skip;

    if (trust_stats) {
        const has_nulls = blk: {
            if (ci.null_counts) |nc| {
                if (page_idx < nc.items.len) break :blk nc.items[page_idx] > 0;
            }
            break :blk true;
        };
        if (!has_nulls) {
            if (encoded.rangeAlwaysMatches(T, op, min, max, value)) return .always_match;
        }
    }

    return .keep;
}

fn prunePageUnsigned(
    rg: *const schema.RowGroup,
    page_idx: usize,
    col_idx: usize,
    op: ast.Operator,
    value: u64,
    col_indexes: []const ?schema.ColumnIndex,
) Decision {
    if (col_idx >= col_indexes.len or col_idx >= rg.columns.items.len) return .unknown;
    const ci = col_indexes[col_idx] orelse return .unknown;
    const meta = rg.columns.items[col_idx].meta_data orelse return .unknown;
    if (page_idx >= ci.null_pages.items.len) return .unknown;
    if (ci.null_pages.items[page_idx]) return .skip;
    if (page_idx >= ci.min_values.items.len or page_idx >= ci.max_values.items.len) return .unknown;
    return unsignedRangeDecision(meta.type, ci.min_values.items[page_idx], ci.max_values.items[page_idx], op, value);
}

fn prunePageDecimal(
    rg: *const schema.RowGroup,
    page_idx: usize,
    col_idx: usize,
    op: ast.Operator,
    value: i128,
    col_indexes: []const ?schema.ColumnIndex,
    file_meta: ?*const schema.FileMetaData,
    trust_stats: bool,
) Decision {
    if (col_idx >= col_indexes.len) return .unknown;
    const ci = col_indexes[col_idx] orelse return .unknown;
    const kind = decimalKind(rg, col_idx, file_meta) orelse return .unknown;
    if (page_idx >= ci.null_pages.items.len) return .unknown;
    if (ci.null_pages.items[page_idx]) return .skip;

    if (page_idx >= ci.min_values.items.len or page_idx >= ci.max_values.items.len) return .unknown;
    const lo = decimal_mod.statToI128(ci.min_values.items[page_idx], kind) orelse return .unknown;
    const hi = decimal_mod.statToI128(ci.max_values.items[page_idx], kind) orelse return .unknown;
    if (lo > hi) return .unknown; // corrupt stats

    if (!encoded.rangeOverlapsValue(i128, op, lo, hi, value)) return .skip;

    if (trust_stats) {
        const has_nulls = blk: {
            if (ci.null_counts) |nc| {
                if (page_idx < nc.items.len) break :blk nc.items[page_idx] > 0;
            }
            break :blk true;
        };
        if (!has_nulls) {
            const always = switch (op) {
                .Eq => lo == hi and lo == value,
                .NotEq => value < lo or value > hi,
                .Gt => lo > value,
                .GtEq => lo >= value,
                .Lt => hi < value,
                .LtEq => hi <= value,
            };
            if (always) return .always_match;
        }
    }

    return .keep;
}

fn prunePageBytes(
    page_idx: usize,
    col_idx: usize,
    op: ast.Operator,
    value: []const u8,
    parquet_type: schema.Type,
    col_indexes: []const ?schema.ColumnIndex,
) Decision {
    _ = parquet_type;
    if (col_idx >= col_indexes.len) return .unknown;
    const ci = col_indexes[col_idx] orelse return .unknown;
    if (page_idx >= ci.null_pages.items.len) return .unknown;
    if (ci.null_pages.items[page_idx]) return .skip;

    if (page_idx >= ci.min_values.items.len or page_idx >= ci.max_values.items.len) return .unknown;
    const min = ci.min_values.items[page_idx];
    const max = ci.max_values.items[page_idx];

    const intersects = encoded.rangeIntersectsBytes(op, min, max, value);
    return if (intersects) .keep else .skip;
}

fn prunePageBoolean(
    page_idx: usize,
    col_idx: usize,
    op: ast.Operator,
    value: bool,
    col_indexes: []const ?schema.ColumnIndex,
) Decision {
    if (col_idx >= col_indexes.len) return .unknown;
    const ci = col_indexes[col_idx] orelse return .unknown;
    if (page_idx >= ci.null_pages.items.len) return .unknown;
    if (ci.null_pages.items[page_idx]) return .skip;

    if (page_idx >= ci.min_values.items.len or page_idx >= ci.max_values.items.len) return .unknown;
    const min = ci.min_values.items[page_idx];
    const max = ci.max_values.items[page_idx];

    const intersects = encoded.rangeIntersectsBytes(if (op == .Eq) .Eq else .NotEq, min, max, &[_]u8{if (value) 1 else 0});
    return if (intersects) .keep else .skip;
}

test "passFraction places range comparisons between a row group's bounds and knows nothing of other leaves" {
    const metadata = @import("../parquet/metadata.zig");
    const a = std.testing.allocator;
    const bytes = try metadata.readFileSlice("ci/fixtures/parquet/topk_skew.parquet", a);
    defer a.free(bytes);
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const meta = try metadata.open(arena.allocator(), bytes);
    const rg = &meta.row_groups.items[0];
    const b = statistics.chunkBounds(rg, 1, &meta) orelse return error.SkipZigTest;
    const lo = encoded.readFixedLE(i64, b.min).?;
    const hi = encoded.readFixedLE(i64, b.max).?;
    const leaf = struct {
        fn f(op: ast.Operator, v: i64) ast.Filter {
            return .{ .int64 = .{ .col_idx = 1, .op = op, .value = v } };
        }
    }.f;
    const half = lo + @divTrunc(hi - lo, 2);
    try std.testing.expectEqual(@as(f64, 0), passFraction(leaf(.Lt, lo), rg, &meta));
    try std.testing.expectEqual(@as(f64, 1), passFraction(leaf(.LtEq, hi), rg, &meta));
    try std.testing.expectEqual(@as(f64, 1), passFraction(leaf(.GtEq, lo), rg, &meta));
    const mid = passFraction(leaf(.Lt, half), rg, &meta);
    try std.testing.expect(mid > 0.3 and mid < 0.7);
    try std.testing.expectEqual(@as(f64, 0), passFraction(leaf(.Eq, hi + 1), rg, &meta));
    // A conjunction multiplies; a disjunction adds, at most 1; a leaf it cannot place passes everything.
    const both: ast.Filter = .{ .and_filter = .{ .children = &.{ leaf(.Lt, half), leaf(.Lt, half) } } };
    try std.testing.expectApproxEqAbs(mid * mid, passFraction(both, rg, &meta), 1e-12);
    const either: ast.Filter = .{ .or_filter = .{ .children = &.{ leaf(.GtEq, lo), leaf(.GtEq, lo) } } };
    try std.testing.expectEqual(@as(f64, 1), passFraction(either, rg, &meta));
    const str: ast.Filter = .{ .string = .{ .col_idx = 0, .op = .Eq, .value = "x" } };
    try std.testing.expectEqual(@as(f64, 1), passFraction(str, rg, &meta));
}
