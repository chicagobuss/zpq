const std = @import("std");
const testing = std.testing;
const schema = @import("../schema.zig");
const expr_agg = @import("../expr/agg.zig");
const expr_ast = @import("../expr/ast.zig");
const agg_dense = @import("../expr/agg_dense.zig");
const key_pack = @import("../expr/key_pack.zig");
const BoundOutputColumn = @import("types.zig").BoundOutputColumn;
const leafSchemaElem = @import("binding.zig").leafSchemaElem;

pub const IntegerRangeHint = struct {
    known: bool = true,
    lo: i64 = std.math.maxInt(i64),
    hi: i64 = std.math.minInt(i64),

    pub fn include(self: *@This(), rg: *const schema.RowGroup, meta: *const schema.FileMetaData, ci: usize) void {
        if (rg.num_rows <= 0) return;
        const bounds = @import("../parquet/statistics.zig").chunkStats(rg, ci, meta, .corroborated).bounds;
        if (bounds) |b| {
            if (b.min.len == 8 and b.max.len == 8) {
                self.lo = @min(self.lo, std.mem.readInt(i64, b.min[0..8], .little));
                self.hi = @max(self.hi, std.mem.readInt(i64, b.max[0..8], .little));
                return;
            }
        }
        self.known = false;
    }

    pub fn small(self: @This(), rows: usize) bool {
        if (!self.known or self.lo > self.hi) return false;
        const span: u128 = @intCast(@as(i128, self.hi) - self.lo + 1);
        return span < rows / 4;
    }
};

pub fn pairRawIdentity(gpa: std.mem.Allocator, arena: std.mem.Allocator, metas: []const schema.FileMetaData, axes: []const expr_ast.Expr, scan_all: bool) !?key_pack.Plan {
    const fields = try arena.alloc(key_pack.RawField, axes.len);
    for (axes, fields) |axis, *field| {
        if (axis != .col_ref) return null;
        const key = axis.col_ref;
        if (key.physical_type != .INT32 and key.physical_type != .INT64) return null;
        field.* = .{ .bits = if (key.physical_type == .INT32) 32 else 64, .signed = !key.unsigned_32 and !key.unsigned_64 };
        if (scan_all) continue;
        var lo: i128 = std.math.maxInt(i128);
        var hi: i128 = std.math.minInt(i128);
        const complete = complete: for (metas) |*meta| {
            for (meta.row_groups.items) |*rg| {
                if (rg.num_rows == 0) continue;
                const bounds = @import("../parquet/statistics.zig").chunkStats(rg, key.col_idx, meta, .corroborated).bounds orelse break :complete false;
                const width: usize = if (key.physical_type == .INT32) 4 else 8;
                if (bounds.min.len != width or bounds.max.len != width) break :complete false;
                const low: i128 = if (width == 4)
                    if (key.unsigned_32) std.mem.readInt(u32, bounds.min[0..4], .little) else std.mem.readInt(i32, bounds.min[0..4], .little)
                else if (key.unsigned_64) std.mem.readInt(u64, bounds.min[0..8], .little) else std.mem.readInt(i64, bounds.min[0..8], .little);
                const high: i128 = if (width == 4)
                    if (key.unsigned_32) std.mem.readInt(u32, bounds.max[0..4], .little) else std.mem.readInt(i32, bounds.max[0..4], .little)
                else if (key.unsigned_64) std.mem.readInt(u64, bounds.max[0..8], .little) else std.mem.readInt(i64, bounds.max[0..8], .little);
                lo = @min(lo, low);
                hi = @max(hi, high);
            }
        } else true;
        if (!complete or lo > hi) continue;
        var bits: u7 = 1;
        while (bits < field.bits) : (bits += 1) {
            const candidate: key_pack.RawField = .{ .bits = bits, .signed = field.signed };
            _ = candidate.encode(lo) catch continue;
            _ = candidate.encode(hi) catch continue;
            field.bits = bits;
            break;
        }
    }
    return key_pack.Plan.initFields(gpa, axes, null, fields);
}

pub fn outputNamesBytes(outputs: []const BoundOutputColumn) usize {
    var bytes = outputs.len *| @sizeOf([]const u8);
    for (outputs) |out| bytes +|= out.name.len;
    return bytes;
}

/// Equal-width raw numeric identities avoid a dictionary-wide comparison on every sliced callback. Retain coded
/// identities where they make the physical record narrower, or where strings have no raw numeric image.
pub fn ownerIdentity(
    gpa: std.mem.Allocator,
    keys: []const expr_ast.Expr,
    domain: ?*const agg_dense.Domain,
) !?key_pack.Plan {
    var best = try key_pack.Plan.init(gpa, keys, null);
    errdefer if (best) |*p| p.deinit(gpa);
    if (domain) |d| if (best == null or !best.?.fitsBits(u64)) {
        var coded = (try key_pack.Plan.init(gpa, keys, d)) orelse return best;
        if (best == null or coded.fitsBits(u64)) {
            if (best) |*p| p.deinit(gpa);
            return coded;
        }
        coded.deinit(gpa);
    };
    return best;
}

/// min/max of a bare column returns one of that column's values, so its output can carry the column's type.
pub fn passThroughSource(call: expr_agg.AggCall, meta: *const schema.FileMetaData) ?schema.SchemaElement {
    if (call.func != .min and call.func != .max) return null;
    const arg = call.arg orelse return null;
    if (arg != .col_ref) return if (arg.isBoolean()) boolean_answer else null;
    if (call.as_date) return date_answer;
    return leafSchemaElem(meta, arg.col_ref.col_idx);
}

/// The type `min`/`max` of `make_date(col)` answers as (`AggCall.as_date`), whatever integer type `col` has.
pub const date_answer: schema.SchemaElement = .{
    .type = .INT32,
    .type_length = null,
    .repetition_type = .OPTIONAL,
    .name = "",
    .num_children = null,
    .scale = null,
    .precision = null,
    .field_id = null,
    .converted_type = .DATE,
    .logical_type = .{ .DATE = .{} },
};

/// The type a computed BOOLEAN answer column (a key, min or max of a BOOLEAN expression) passes through as, as a bare
/// BOOLEAN column's does: printed true/false and written BOOLEAN, though its lane holds 0/1.
pub const boolean_answer: schema.SchemaElement = .{
    .type = .BOOLEAN,
    .type_length = null,
    .repetition_type = .OPTIONAL,
    .name = "",
    .num_children = null,
    .scale = null,
    .precision = null,
    .field_id = null,
};

test "sparse owner identity prefers raw numeric words but retains narrower coded or string keys" {
    const Trial = struct {
        fn run(a: std.mem.Allocator) !void {
            var values = [_]u64{9};
            var trans = [_]u32{ 0, 1 };
            var translations = [_][]u32{&trans};
            var files = [_]usize{0};
            var axes = [_]agg_dense.Axis{.{
                .values = .{ .int32 = &values },
                .trans = &translations,
                .file_base = &files,
                .bytes = 0,
            }};
            const domain: agg_dense.Domain = .{ .axes = &axes, .bytes = 0 };
            const key: expr_ast.Expr = .{ .col_ref = .{
                .col_idx = 0,
                .physical_type = .INT32,
                .expr_type = .i64,
            } };
            var raw = (try ownerIdentity(a, &.{key}, &domain)).?;
            defer raw.deinit(a);
            try testing.expect(raw.encoding.source == .raw and raw.domain == null and raw.fitsBits(u64));
            const wide: expr_ast.Expr = .{ .col_ref = .{
                .col_idx = 0,
                .physical_type = .INT64,
                .expr_type = .i64,
                .unsigned_64 = true,
            } };
            axes[0].values = .{ .int64 = &values };
            var coded = (try ownerIdentity(a, &.{wide}, &domain)).?;
            defer coded.deinit(a);
            try testing.expect(coded.encoding.source == .dictionary and coded.fitsBits(u64));
            var chars = [_]u8{'x'};
            var strings = [_][]u8{&chars};
            axes[0].values = .{ .bytes = &strings };
            const string: expr_ast.Expr = .{ .col_ref = .{
                .col_idx = 0,
                .physical_type = .BYTE_ARRAY,
                .expr_type = .str,
            } };
            var text = (try ownerIdentity(a, &.{string}, &domain)).?;
            defer text.deinit(a);
            try testing.expect(text.encoding.source == .dictionary);
        }
    };
    try Trial.run(testing.allocator);
    try testing.checkAllAllocationFailures(testing.allocator, Trial.run, .{});
}
