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
pub fn pruneRowGroup(
    rg: *const schema.RowGroup,
    filter: ast.Filter,
    arena: std.mem.Allocator,
) !Decision {
    return switch (filter) {
        .int32 => |leaf| pruneNumeric(i32, rg, leaf.col_idx, leaf.op, leaf.value, .INT32, arena),
        .int64 => |leaf| pruneNumeric(i64, rg, leaf.col_idx, leaf.op, leaf.value, .INT64, arena),
        .float => |leaf| pruneNumeric(f32, rg, leaf.col_idx, leaf.op, leaf.value, .FLOAT, arena),
        .double => |leaf| pruneNumeric(f64, rg, leaf.col_idx, leaf.op, leaf.value, .DOUBLE, arena),
        .string => |leaf| pruneBytes(rg, leaf.col_idx, leaf.op, leaf.value, .BYTE_ARRAY),
        .boolean => |leaf| pruneBoolean(rg, leaf.col_idx, leaf.op, leaf.value),
        .and_filter => |c| Decision.andCombine(
            try pruneRowGroup(rg, c.left.*, arena),
            try pruneRowGroup(rg, c.right.*, arena),
        ),
        .or_filter => |c| Decision.orCombine(
            try pruneRowGroup(rg, c.left.*, arena),
            try pruneRowGroup(rg, c.right.*, arena),
        ),
    };
}

fn pruneNumeric(
    comptime T: type,
    rg: *const schema.RowGroup,
    col_idx: usize,
    op: ast.Operator,
    value: T,
    parquet_type: schema.Type,
    arena: std.mem.Allocator,
) !Decision {
    const stats = getStats(rg, col_idx) orelse return .unknown;
    const min = stats.min_value orelse stats.min orelse return .unknown;
    const max = stats.max_value orelse stats.max orelse return .unknown;

    // Encode `value` once, then compare against min/max bytes.
    const encoded_val = encoded.encode(arena, valueToString(T, value, arena) catch return .unknown, parquet_type) catch return .unknown;
    return if (encoded_val.rangeIntersects(op, min, max)) .keep else .skip;
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
    try testing.expectEqual(Decision.keep, try pruneRowGroup(&rg, filter, a));
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
    try testing.expectEqual(Decision.skip, try pruneRowGroup(&rg, filter, a));
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

    try testing.expectEqual(Decision.skip, try pruneRowGroup(&rg, composite, a));
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

    try testing.expectEqual(Decision.keep, try pruneRowGroup(&rg, composite, a));
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
    try testing.expectEqual(Decision.unknown, try pruneRowGroup(&rg, filter, a));
}
