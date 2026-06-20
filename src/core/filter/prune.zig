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

    fn andCombine(a: Decision, b: Decision) Decision {
        // AND: if either says skip, the conjunction can't match.
        if (a == .skip or b == .skip) return .skip;
        // Otherwise "keep" wins over "unknown".
        if (a == .keep or b == .keep) return .keep;
        return .unknown;
    }

    fn orCombine(a: Decision, b: Decision) Decision {
        // OR: only skip if BOTH can't match.
        if (a == .skip and b == .skip) return .skip;
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
        .string => |leaf| pruneBytes(rg, leaf.col_idx, leaf.op, leaf.value, .BYTE_ARRAY),
        .boolean => |leaf| pruneBoolean(rg, leaf.col_idx, leaf.op, leaf.value),
        .null_check => |nc| pruneNullCheck(rg, nc.col_idx, nc.is_not),
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

/// Prune on `IS NULL` / `IS NOT NULL` using the `null_count` stat:
///   IS NULL     → skip when null_count == 0 (no null rows here)
///   IS NOT NULL → skip when null_count == num_rows (every row is null)
/// Missing null_count → .unknown (eval handles correctness either way).
fn pruneNullCheck(rg: *const schema.RowGroup, col_idx: usize, is_not: bool) Decision {
    if (col_idx >= rg.columns.items.len) return .unknown;
    const meta = rg.columns.items[col_idx].meta_data orelse return .unknown;
    const stats = meta.statistics orelse return .unknown;
    const null_count = stats.null_count orelse return .unknown;
    if (is_not) {
        return if (null_count >= rg.num_rows) .skip else .keep;
    }
    return if (null_count == 0) .skip else .keep;
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
    return if (encoded_val.rangeIntersects(op, min, max)) .keep else .skip;
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
) Decision {
    _ = parquet_type;
    const stats = getStats(rg, col_idx) orelse return .unknown;
    const min = stats.min_value orelse stats.min orelse return .unknown;
    const max = stats.max_value orelse stats.max orelse return .unknown;

    // BYTE_ARRAY uses lexicographic byte comparison directly.
    const ev: encoded.EncodedValue = .{ .bytes = value, .parquet_type = .BYTE_ARRAY };
    return if (ev.rangeIntersects(op, min, max)) .keep else .skip;
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
