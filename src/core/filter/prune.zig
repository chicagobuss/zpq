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
    arena: std.mem.Allocator,
    file_meta: ?*const schema.FileMetaData,
) !Decision {
    return switch (filter) {
        .int32 => |leaf| pruneNumeric(i32, rg, leaf.col_idx, leaf.op, leaf.value, .INT32, arena, file_meta),
        .int64 => |leaf| pruneNumeric(i64, rg, leaf.col_idx, leaf.op, leaf.value, .INT64, arena, file_meta),
        .float => |leaf| pruneNumeric(f32, rg, leaf.col_idx, leaf.op, leaf.value, .FLOAT, arena, file_meta),
        .double => |leaf| pruneNumeric(f64, rg, leaf.col_idx, leaf.op, leaf.value, .DOUBLE, arena, file_meta),
        .string => |leaf| pruneBytes(rg, leaf.col_idx, leaf.op, leaf.value, .BYTE_ARRAY, file_meta),
        .boolean => |leaf| pruneBoolean(rg, leaf.col_idx, leaf.op, leaf.value),
        .null_check => |nc| pruneNullCheck(rg, nc.col_idx, nc.is_not, file_meta),
        // LIKE: prefix patterns are stat-prunable via min/max (a future
        // win); conservatively keep for now. Correctness is in eval.
        .like => .unknown,
        .and_filter => |c| Decision.andCombine(
            try pruneRowGroup(rg, c.left.*, arena, file_meta),
            try pruneRowGroup(rg, c.right.*, arena, file_meta),
        ),
        .or_filter => |c| Decision.orCombine(
            try pruneRowGroup(rg, c.left.*, arena, file_meta),
            try pruneRowGroup(rg, c.right.*, arena, file_meta),
        ),
    };
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
//   - unsigned integers (zpq compares their stats signed), floats (NaN), DECIMAL, FLBA, INT96, and any annotation
//     not on the allow-lists below,
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
    if (file_meta.?.column_orders) |co| {
        if (col_idx >= co.items.len or co.items[col_idx] != schema.COLUMN_ORDER_TYPE_DEFINED) return null;
    }
    const stats = rg.columns.items[col_idx].meta_data.?.statistics orelse return null;
    if ((stats.null_count orelse return null) != 0) return null;
    return .{
        .elem = elem,
        .min = stats.min_value orelse return null,
        .max = stats.max_value orelse return null,
        .max_exact = stats.is_max_value_exact orelse false,
    };
}

/// INT32/INT64 whose type-defined order is signed: plain, signed INTEGER, DATE, TIME, TIMESTAMP.
fn signedIntOrder(elem: schema.SchemaElement) bool {
    const t = elem.type orelse return false;
    if (t != .INT32 and t != .INT64) return false;
    if (schema.isUnsignedInt(elem)) return false;
    if (elem.logical_type) |lt| switch (lt) {
        .INTEGER => |it| if (!it.isSigned) return false,
        .DATE, .TIME, .TIMESTAMP => {},
        else => return false,
    };
    if (elem.converted_type) |ct| switch (ct) {
        .INT_8, .INT_16, .INT_32, .INT_64 => {},
        .DATE, .TIME_MILLIS, .TIME_MICROS, .TIMESTAMP_MILLIS, .TIMESTAMP_MICROS => {},
        else => return false,
    };
    return true;
}

/// BYTE_ARRAY whose type-defined order is unsigned bytewise, the order `applyOpStr` evaluates in.
fn bytewiseOrder(elem: schema.SchemaElement) bool {
    const t = elem.type orelse return false;
    if (t != .BYTE_ARRAY) return false;
    if (elem.logical_type) |lt| switch (lt) {
        .STRING, .ENUM, .JSON => {},
        else => return false,
    };
    if (elem.converted_type) |ct| switch (ct) {
        .UTF8, .ENUM, .JSON => {},
        else => return false,
    };
    return true;
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
    arena: std.mem.Allocator,
    file_meta: ?*const schema.FileMetaData,
) !Decision {
    if (col_idx >= rg.columns.items.len) return .unknown;
    const meta = rg.columns.items[col_idx].meta_data orelse return .unknown;

    // DECIMAL path: if the filter is .double against a column whose
    // physical type doesn't match DOUBLE, we may be looking at a
    // DECIMAL column (decoded to f64 by decimal_mod). Look up the
    // schema and decode stat bytes through the same byte → i128 →
    // f64-with-scale pipeline to get a comparable f64 min/max.
    if (parquet_type == .DOUBLE and meta.type != .DOUBLE and T == f64) {
        if (file_meta) |fm| {
            if (fm.getColumnSchema(meta.path_in_schema.items)) |elem| {
                if (decimal_mod.kindFromSchema(&elem)) |kind| {
                    return pruneDecimal(rg, col_idx, op, value, kind);
                }
            }
        }
    }

    // Type-mismatch guard: stats bytes are in the column's physical
    // wire format. If the filter leaf was built with a different
    // parquet type and we didn't take the DECIMAL branch above, the
    // encoded comparison would compare incompatible byte formats and
    // silently produce wrong pruning decisions. Bail to .unknown.
    if (meta.type != parquet_type) return .unknown;

    const stats = meta.statistics orelse return .unknown;
    const min = stats.min_value orelse stats.min orelse return .unknown;
    const max = stats.max_value orelse stats.max orelse return .unknown;

    // Encode `value` once, then compare against min/max bytes.
    const encoded_val = encoded.encode(arena, valueToString(T, value, arena) catch return .unknown, parquet_type) catch return .unknown;
    if (!encoded_val.rangeIntersects(op, min, max)) return .skip;
    // The positive assertion reads only the bounds `comparisonProof` vouches for, not the `min`/`max` fallback above.
    if (comparisonProof(rg, col_idx, file_meta)) |p| {
        if (signedIntOrder(p.elem) and encoded_val.rangeAlwaysMatches(op, p.min, p.max)) return .always_match;
    }
    return .keep;
}

/// Pruning for DECIMAL columns: stat min/max bytes are in the
/// column's physical wire format (INT32/INT64 little-endian, FLBA
/// big-endian two's-complement), and the filter compares against an
/// f64 value. Decode the bytes to f64 through decimal_mod and then
/// do a plain f64 range check.
fn pruneDecimal(
    rg: *const schema.RowGroup,
    col_idx: usize,
    op: ast.Operator,
    value: f64,
    kind: decimal_mod.Kind,
) Decision {
    const meta = rg.columns.items[col_idx].meta_data orelse return .unknown;
    const stats = meta.statistics orelse return .unknown;
    const min_bytes = stats.min_value orelse stats.min orelse return .unknown;
    const max_bytes = stats.max_value orelse stats.max orelse return .unknown;

    const min_f = decimalStatBytesToF64(min_bytes, kind) orelse return .unknown;
    const max_f = decimalStatBytesToF64(max_bytes, kind) orelse return .unknown;
    if (min_f > max_f) return .unknown; // corrupt stats — be safe

    return switch (op) {
        .Eq => if (value >= min_f and value <= max_f) .keep else .skip,
        .NotEq => if (min_f == max_f and min_f == value) .skip else .keep,
        .Gt => if (max_f > value) .keep else .skip,
        .GtEq => if (max_f >= value) .keep else .skip,
        .Lt => if (min_f < value) .keep else .skip,
        .LtEq => if (min_f <= value) .keep else .skip,
    };
}

fn decimalStatBytesToF64(bytes: []const u8, kind: decimal_mod.Kind) ?f64 {
    return switch (kind.physical) {
        .INT32 => blk: {
            if (bytes.len < 4) break :blk null;
            const i = std.mem.readInt(i32, bytes[0..4], .little);
            break :blk decimal_mod.applyScaleInt(i32, i, kind.scale);
        },
        .INT64 => blk: {
            if (bytes.len < 8) break :blk null;
            const i = std.mem.readInt(i64, bytes[0..8], .little);
            break :blk decimal_mod.applyScaleInt(i64, i, kind.scale);
        },
        .FIXED_LEN_BYTE_ARRAY => blk: {
            if (kind.byte_width == 0 or kind.byte_width > decimal_mod.MAX_FLBA_BYTE_WIDTH) break :blk null;
            if (bytes.len < kind.byte_width) break :blk null;
            break :blk decimal_mod.applyScaleI128(
                decimal_mod.flbaToI128(bytes[0..kind.byte_width]),
                kind.scale,
            );
        },
        // BYTE_ARRAY: stat min/max are raw variable-width big-endian
        // two's-complement bytes (no length prefix in the stats slot,
        // unlike the on-wire data-page format).
        .BYTE_ARRAY => blk: {
            if (bytes.len == 0 or bytes.len > decimal_mod.MAX_FLBA_BYTE_WIDTH) break :blk null;
            break :blk decimal_mod.applyScaleI128(
                decimal_mod.flbaToI128(bytes),
                kind.scale,
            );
        },
        else => null,
    };
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
    const stats = getStats(rg, col_idx) orelse return .unknown;
    const min = stats.min_value orelse stats.min orelse return .unknown;
    const max = stats.max_value orelse stats.max orelse return .unknown;

    // BYTE_ARRAY uses lexicographic byte comparison directly.
    const ev: encoded.EncodedValue = .{ .bytes = value, .parquet_type = .BYTE_ARRAY };
    if (!ev.rangeIntersects(op, min, max)) return .skip;
    if (comparisonProof(rg, col_idx, file_meta)) |p| {
        if (bytewiseOrder(p.elem) and encoded.rangeAlwaysMatchesBytes(op, p.min, p.max, p.max_exact, value))
            return .always_match;
    }
    return .keep;
}

fn pruneBoolean(
    rg: *const schema.RowGroup,
    col_idx: usize,
    op: ast.Operator,
    value: bool,
) Decision {
    const stats = getStats(rg, col_idx) orelse return .unknown;
    const min = stats.min_value orelse stats.min orelse return .unknown;
    const max = stats.max_value orelse stats.max orelse return .unknown;
    if (min.len != 1 or max.len != 1) return .unknown;

    const min_b = min[0] != 0;
    const max_b = max[0] != 0;
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

fn getStats(rg: *const schema.RowGroup, col_idx: usize) ?schema.Statistics {
    if (col_idx >= rg.columns.items.len) return null;
    const meta = rg.columns.items[col_idx].meta_data orelse return null;
    return meta.statistics;
}

/// Helper: turn a typed value back into its decimal string so we can
/// route it through the existing `encoded.encode`. A bit roundabout
/// but keeps the encoded.zig surface simpler. ~free in practice
/// because pruneRowGroup runs once per row group.
fn valueToString(comptime T: type, value: T, arena: std.mem.Allocator) ![]const u8 {
    return switch (T) {
        i32, i64 => std.fmt.allocPrint(arena, "{d}", .{value}),
        f32, f64 => std.fmt.allocPrint(arena, "{d}", .{value}),
        else => @compileError("valueToString: unsupported"),
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
    try testing.expectEqual(Decision.keep, try pruneRowGroup(&rg, filter, a, null));
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
    try testing.expectEqual(Decision.skip, try pruneRowGroup(&rg, filter, a, null));
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

    const left = try a.create(ast.Filter);
    left.* = .{ .int32 = .{ .col_idx = 0, .op = .Eq, .value = 50 } }; // keep
    const right = try a.create(ast.Filter);
    right.* = .{ .int32 = .{ .col_idx = 0, .op = .Eq, .value = 999 } }; // skip
    const composite: ast.Filter = .{ .and_filter = .{ .left = left, .right = right } };

    try testing.expectEqual(Decision.skip, try pruneRowGroup(&rg, composite, a, null));
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

    const left = try a.create(ast.Filter);
    left.* = .{ .int32 = .{ .col_idx = 0, .op = .Eq, .value = -1 } }; // skip
    const right = try a.create(ast.Filter);
    right.* = .{ .int32 = .{ .col_idx = 0, .op = .Eq, .value = 50 } }; // keep
    const composite: ast.Filter = .{ .or_filter = .{ .left = left, .right = right } };

    try testing.expectEqual(Decision.keep, try pruneRowGroup(&rg, composite, a, null));
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
    const f_keep: ast.Filter = .{ .double = .{ .col_idx = 0, .op = .Gt, .value = 10.0 } };
    try testing.expectEqual(Decision.keep, try pruneRowGroup(&rg, f_keep, a, &file_meta));

    // value > 100.0 → max=24.0 < 100 → skip.
    const f_skip: ast.Filter = .{ .double = .{ .col_idx = 0, .op = .Gt, .value = 100.0 } };
    try testing.expectEqual(Decision.skip, try pruneRowGroup(&rg, f_skip, a, &file_meta));

    // value < 0.5 → min=1.0 >= 0.5 → skip.
    const f_skip_lt: ast.Filter = .{ .double = .{ .col_idx = 0, .op = .Lt, .value = 0.5 } };
    try testing.expectEqual(Decision.skip, try pruneRowGroup(&rg, f_skip_lt, a, &file_meta));

    // Without file_meta, DECIMAL recognition is impossible → degrade
    // to .unknown (the type-mismatch guard kicks in).
    try testing.expectEqual(Decision.unknown, try pruneRowGroup(&rg, f_keep, a, null));
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
    try testing.expectEqual(Decision.unknown, try pruneRowGroup(&rg, filter, a, null));
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
    try testing.expectEqual(Decision.unknown, try pruneRowGroup(&rg, filter, a, null));
}

/// One flat column `c` with the given stats, 100 rows, in a file whose schema declares `elem`.
const ProofFixture = struct {
    rg: schema.RowGroup,
    fm: schema.FileMetaData,

    fn init(a: std.mem.Allocator, elem: schema.SchemaElement, stats: schema.Statistics) !ProofFixture {
        var path: schema.StringList = .empty;
        try path.append(a, "c");
        var rg: schema.RowGroup = .{ .columns = .empty, .total_byte_size = 0, .num_rows = 100 };
        try rg.columns.append(a, .{ .file_path = null, .file_offset = 0, .meta_data = .{
            .type = elem.type.?,
            .encodings = .empty,
            .path_in_schema = path,
            .codec = .UNCOMPRESSED,
            .num_values = 100,
            .total_uncompressed_size = 0,
            .total_compressed_size = 0,
            .data_page_offset = 0,
            .index_page_offset = null,
            .dictionary_page_offset = null,
            .statistics = stats,
        } });
        var items: std.ArrayListUnmanaged(schema.SchemaElement) = .empty;
        try items.append(a, .{
            .type = null,
            .type_length = null,
            .repetition_type = null,
            .name = "schema",
            .num_children = 1,
            .scale = null,
            .precision = null,
            .field_id = null,
        });
        var leaf = elem;
        leaf.name = "c";
        leaf.num_children = 0;
        try items.append(a, leaf);
        return .{
            .rg = rg,
            .fm = .{ .version = 1, .schema = items, .num_rows = 100, .created_by = null, .row_groups = .empty },
        };
    }

    fn decide(self: *const ProofFixture, a: std.mem.Allocator, f: ast.Filter) !Decision {
        return pruneRowGroup(&self.rg, f, a, &self.fm);
    }
};

fn leafElem(t: schema.Type) schema.SchemaElement {
    return .{
        .type = t,
        .type_length = null,
        .repetition_type = .OPTIONAL,
        .name = "c",
        .num_children = 0,
        .scale = null,
        .precision = null,
        .field_id = null,
    };
}

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
    try testing.expectEqual(Decision.always_match, try ok.decide(a, ge10));
    // Without file metadata no schema gate can run.
    try testing.expectEqual(Decision.keep, try pruneRowGroup(&ok.rg, ge10, a, null));

    // A declared order other than TYPE_DEFINED_ORDER; an absent list is read as type-defined, like parquet-mr does.
    var orders: std.ArrayListUnmanaged(i16) = .empty;
    try orders.append(a, 0);
    ok.fm.column_orders = orders;
    try testing.expectEqual(Decision.keep, try ok.decide(a, ge10));

    // Deprecated min/max are signed-ordered for every type: they may skip, never prove.
    const legacy = try ProofFixture.init(a, leafElem(.INT64), .{ .min = lo, .max = hi, .null_count = 0 });
    try testing.expectEqual(Decision.keep, try legacy.decide(a, ge10));
    const lt10: ast.Filter = .{ .int64 = .{ .col_idx = 0, .op = .Lt, .value = 10 } };
    try testing.expectEqual(Decision.skip, try legacy.decide(a, lt10));

    const unknown_nulls = try ProofFixture.init(a, leafElem(.INT64), .{ .min_value = lo, .max_value = hi });
    try testing.expectEqual(Decision.keep, try unknown_nulls.decide(a, ge10));
    const some_nulls = try ProofFixture.init(a, leafElem(.INT64), .{
        .min_value = lo,
        .max_value = hi,
        .null_count = 1,
    });
    try testing.expectEqual(Decision.keep, try some_nulls.decide(a, ge10));

    var u64_elem = leafElem(.INT64);
    u64_elem.logical_type = .{ .INTEGER = .{ .bitWidth = 64, .isSigned = false } };
    const unsigned = try ProofFixture.init(a, u64_elem, .{ .min_value = lo, .max_value = hi, .null_count = 0 });
    try testing.expectEqual(Decision.keep, try unsigned.decide(a, ge10));
    var u64_conv = leafElem(.INT64);
    u64_conv.converted_type = .UINT_64;
    const unsigned_conv = try ProofFixture.init(a, u64_conv, .{ .min_value = lo, .max_value = hi, .null_count = 0 });
    try testing.expectEqual(Decision.keep, try unsigned_conv.decide(a, ge10));

    var ts_elem = leafElem(.INT64);
    ts_elem.logical_type = .{ .TIMESTAMP = .{ .isAdjustedToUTC = true, .unit = .{ .MICROS = .{} } } };
    const ts = try ProofFixture.init(a, ts_elem, .{ .min_value = lo, .max_value = hi, .null_count = 0 });
    try testing.expectEqual(Decision.always_match, try ts.decide(a, ge10));
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
    try testing.expectEqual(Decision.always_match, try inexact.decide(a, ge));
    try testing.expectEqual(Decision.keep, try inexact.decide(a, lt));

    const exact = try ProofFixture.init(a, str, .{
        .min_value = "c",
        .max_value = "m",
        .null_count = 0,
        .is_max_value_exact = true,
    });
    try testing.expectEqual(Decision.always_match, try exact.decide(a, lt));
    try testing.expectEqual(Decision.keep, try exact.decide(a, eq));
    const constant = try ProofFixture.init(a, str, .{
        .min_value = "m",
        .max_value = "m",
        .null_count = 0,
        .is_max_value_exact = true,
    });
    try testing.expectEqual(Decision.always_match, try constant.decide(a, eq));

    // Only bytewise-ordered annotations: a BYTE_ARRAY DECIMAL is signed, and FLBA (UUID, INTERVAL) is excluded.
    var dec = leafElem(.BYTE_ARRAY);
    dec.logical_type = .{ .DECIMAL = .{ .scale = 0, .precision = 9 } };
    const decimal = try ProofFixture.init(a, dec, .{ .min_value = "c", .max_value = "m", .null_count = 0 });
    try testing.expectEqual(Decision.keep, try decimal.decide(a, ge));
    const flba = try ProofFixture.init(a, leafElem(.FIXED_LEN_BYTE_ARRAY), .{
        .min_value = "c",
        .max_value = "m",
        .null_count = 0,
    });
    try testing.expectEqual(Decision.keep, try flba.decide(a, ge));
}

test "full-match proof: IS [NOT] NULL from null_count" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const not_null: ast.Filter = .{ .null_check = .{ .col_idx = 0, .is_not = true } };
    const is_null: ast.Filter = .{ .null_check = .{ .col_idx = 0, .is_not = false } };

    const none = try ProofFixture.init(a, leafElem(.DOUBLE), .{ .null_count = 0 });
    try testing.expectEqual(Decision.always_match, try none.decide(a, not_null));
    try testing.expectEqual(Decision.skip, try none.decide(a, is_null));
    const all = try ProofFixture.init(a, leafElem(.DOUBLE), .{ .null_count = 100 });
    try testing.expectEqual(Decision.skip, try all.decide(a, not_null));
    try testing.expectEqual(Decision.always_match, try all.decide(a, is_null));
    const unknown = try ProofFixture.init(a, leafElem(.DOUBLE), .{});
    try testing.expectEqual(Decision.unknown, try unknown.decide(a, not_null));

    // A repeated leaf counts values, not rows.
    var repeated = try ProofFixture.init(a, leafElem(.DOUBLE), .{ .null_count = 0 });
    repeated.rg.columns.items[0].meta_data.?.num_values = 250;
    try testing.expectEqual(Decision.keep, try repeated.decide(a, not_null));
}

/// Walk the filter AST against a specific page's ColumnIndex metadata.
pub fn prunePage(
    rg: *const schema.RowGroup,
    page_idx: usize,
    filter: ast.Filter,
    col_indexes: []const ?schema.ColumnIndex,
    arena: std.mem.Allocator,
    file_meta: ?*const schema.FileMetaData,
    trust_stats: bool,
) !Decision {
    return switch (filter) {
        .int32 => |leaf| prunePageNumeric(i32, rg, page_idx, leaf.col_idx, leaf.op, leaf.value, .INT32, col_indexes, arena, file_meta, trust_stats),
        .int64 => |leaf| prunePageNumeric(i64, rg, page_idx, leaf.col_idx, leaf.op, leaf.value, .INT64, col_indexes, arena, file_meta, trust_stats),
        .float => |leaf| prunePageNumeric(f32, rg, page_idx, leaf.col_idx, leaf.op, leaf.value, .FLOAT, col_indexes, arena, file_meta, trust_stats),
        .double => |leaf| prunePageNumeric(f64, rg, page_idx, leaf.col_idx, leaf.op, leaf.value, .DOUBLE, col_indexes, arena, file_meta, trust_stats),
        .string => |leaf| prunePageBytes(page_idx, leaf.col_idx, leaf.op, leaf.value, .BYTE_ARRAY, col_indexes),
        .boolean => |leaf| prunePageBoolean(page_idx, leaf.col_idx, leaf.op, leaf.value, col_indexes),
        .null_check => |nc| prunePageNullCheck(page_idx, nc.col_idx, nc.is_not, col_indexes),
        .like => .unknown,
        .and_filter => |c| Decision.andCombine(
            try prunePage(rg, page_idx, c.left.*, col_indexes, arena, file_meta, trust_stats),
            try prunePage(rg, page_idx, c.right.*, col_indexes, arena, file_meta, trust_stats),
        ),
        .or_filter => |c| Decision.orCombine(
            try prunePage(rg, page_idx, c.left.*, col_indexes, arena, file_meta, trust_stats),
            try prunePage(rg, page_idx, c.right.*, col_indexes, arena, file_meta, trust_stats),
        ),
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
    arena: std.mem.Allocator,
    file_meta: ?*const schema.FileMetaData,
    trust_stats: bool,
) !Decision {
    if (col_idx >= col_indexes.len or col_idx >= rg.columns.items.len) return .unknown;
    const ci = col_indexes[col_idx] orelse return .unknown;
    if (page_idx >= ci.null_pages.items.len) return .unknown;
    const meta = rg.columns.items[col_idx].meta_data orelse return .unknown;

    // DECIMAL path
    if (parquet_type == .DOUBLE and meta.type != .DOUBLE and T == f64) {
        if (file_meta) |fm| {
            if (fm.getColumnSchema(meta.path_in_schema.items)) |elem| {
                if (decimal_mod.kindFromSchema(&elem)) |kind| {
                    return prunePageDecimal(page_idx, ci, op, value, kind, trust_stats);
                }
            }
        }
    }

    if (meta.type != parquet_type) return .unknown;

    // Check if null page
    if (ci.null_pages.items[page_idx]) return .skip;

    if (page_idx >= ci.min_values.items.len or page_idx >= ci.max_values.items.len) return .unknown;
    const min = ci.min_values.items[page_idx];
    const max = ci.max_values.items[page_idx];

    const encoded_val = encoded.encode(arena, valueToString(T, value, arena) catch return .unknown, parquet_type) catch return .unknown;
    if (!encoded_val.rangeIntersects(op, min, max)) return .skip;

    if (trust_stats) {
        const has_nulls = blk: {
            if (ci.null_counts) |nc| {
                if (page_idx < nc.items.len) break :blk nc.items[page_idx] > 0;
            }
            break :blk true;
        };
        if (!has_nulls) {
            if (encoded_val.rangeAlwaysMatches(op, min, max)) return .always_match;
        }
    }

    return .keep;
}

fn prunePageDecimal(
    page_idx: usize,
    ci: schema.ColumnIndex,
    op: ast.Operator,
    value: f64,
    kind: decimal_mod.Kind,
    trust_stats: bool,
) Decision {
    if (page_idx >= ci.null_pages.items.len) return .unknown;
    if (ci.null_pages.items[page_idx]) return .skip;

    if (page_idx >= ci.min_values.items.len or page_idx >= ci.max_values.items.len) return .unknown;
    const min_bytes = ci.min_values.items[page_idx];
    const max_bytes = ci.max_values.items[page_idx];

    const min_f = decimalStatBytesToF64(min_bytes, kind) orelse return .unknown;
    const max_f = decimalStatBytesToF64(max_bytes, kind) orelse return .unknown;
    if (min_f > max_f) return .unknown; // corrupt stats

    const intersects = switch (op) {
        .Eq => value >= min_f and value <= max_f,
        .NotEq => !(min_f == max_f and min_f == value),
        .Gt => max_f > value,
        .GtEq => max_f >= value,
        .Lt => min_f < value,
        .LtEq => min_f <= value,
    };
    if (!intersects) return .skip;

    if (trust_stats) {
        const has_nulls = blk: {
            if (ci.null_counts) |nc| {
                if (page_idx < nc.items.len) break :blk nc.items[page_idx] > 0;
            }
            break :blk true;
        };
        if (!has_nulls) {
            const always = switch (op) {
                .Eq => min_f == max_f and min_f == value,
                .NotEq => value < min_f or value > max_f,
                .Gt => min_f > value,
                .GtEq => min_f >= value,
                .Lt => max_f < value,
                .LtEq => max_f <= value,
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
