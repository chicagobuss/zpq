//! Query-local exact identity certificates. The query retains the borrowed domain until every finish completes.

const std = @import("std");
const ast = @import("ast.zig");
const dense = @import("agg_dense.zig");
const arithmetic = @import("key_pack.zig");
const eval = @import("../filter/eval.zig");
const SelectedBatch = @import("../selected_batch.zig").SelectedBatch;

pub const KeyId = union(enum) {
    dense_u32: u32,
    packed_u64: u64,
    packed_u128: u128,
    decline: void,
};

pub const Plan = struct {
    keys: []ast.ColRef,
    radices: []u64,
    fields: []arithmetic.RawField,
    encoding: arithmetic.KeyPack,
    domain: ?*const dense.Domain,

    pub fn init(gpa: std.mem.Allocator, keys: []const ast.Expr, domain: ?*const dense.Domain) !?Plan {
        if (keys.len == 0) return null;
        var raw = true;
        var raw_bits: usize = 0;
        for (keys) |key| {
            if (key != .col_ref) return null;
            const ref = key.col_ref;
            if (ref.expr_type == .str and (ref.physical_type == .BYTE_ARRAY or
                ref.physical_type == .FIXED_LEN_BYTE_ARRAY))
            {
                raw = false;
            } else if (ref.expr_type != .i64 or (ref.physical_type != .INT32 and ref.physical_type != .INT64))
                return null;
            raw_bits +|= if (ref.physical_type == .INT32) @as(usize, 33) else 65;
        }
        if (!raw and domain == null) return null;
        if (domain) |d| if (d.axes.len != keys.len) return null;
        const dictionary_width = if (domain) |d| width: {
            var n: u128 = 1;
            for (d.axes) |*axis| {
                n = std.math.mul(u128, n, axis.ids()) catch break :width null;
                if (n > @as(u128, 1) << arithmetic.max_bits) break :width null;
            }
            break :width arithmetic.productWidth(n, dense.max_ids);
        } else null;
        if (dictionary_width == null and (!raw or raw_bits > arithmetic.max_bits)) return null;
        const refs = try gpa.alloc(ast.ColRef, keys.len);
        errdefer gpa.free(refs);
        for (keys, refs) |key, *ref| ref.* = key.col_ref;
        const fields = try gpa.alloc(arithmetic.RawField, if (raw) keys.len else 0);
        errdefer gpa.free(fields);
        for (fields, refs[0..fields.len]) |*field, ref| field.* = .{
            .bits = if (ref.physical_type == .INT32) 32 else 64,
            .signed = !ref.unsigned_32 and !ref.unsigned_64,
        };
        const radices = try gpa.alloc(u64, if (domain != null) keys.len else 0);
        errdefer gpa.free(radices);
        if (domain) |d| for (radices, d.axes) |*radix, *axis| {
            radix.* = axis.ids();
        };
        const encoding = arithmetic.KeyPack.plan(if (domain != null) radices else null, fields, dense.max_ids).?;
        return .{ .keys = refs, .radices = radices, .fields = fields, .encoding = encoding, .domain = domain };
    }

    pub fn deinit(p: *Plan, gpa: std.mem.Allocator) void {
        gpa.free(p.keys);
        gpa.free(p.radices);
        gpa.free(p.fields);
        p.* = undefined;
    }

    pub fn kind(p: *const Plan) std.meta.Tag(KeyId) {
        return switch (p.encoding.width) {
            .dense => .dense_u32,
            .word => .packed_u64,
            .wide => .packed_u128,
        };
    }

    pub fn retainedBytes(p: *const Plan) usize {
        return arithmetic.planBytes(p.keys.len, p.radices.len != 0, p.fields.len != 0);
    }

    pub fn bind(
        p: *const Plan,
        arena: std.mem.Allocator,
        source: SelectedBatch,
        lookup: []const ?usize,
        file: usize,
        rg: usize,
    ) !?Bound {
        try source.validate();
        const bindings = try arena.alloc(Binding, p.keys.len);
        for (p.keys, bindings, 0..) |key, *binding, axis| {
            if (key.col_idx >= lookup.len) return null;
            const position = lookup[key.col_idx] orelse return null;
            if (position >= source.batch.cols.len) return error.BadColumn;
            const col = source.batch.cols[position];
            if (col.nullInfo().max_rep != 0 or col.levelsOnly()) return null;
            if (col.nullInfo().def_levels) |dl| if (dl.len < source.batch.num_rows) return error.BadColumn;
            switch (col) {
                inline else => |c| if (c.rowCount() < source.batch.num_rows) return error.BadColumn,
            }
            binding.* = .{ .col = col, .translation = &.{} };
            if (p.encoding.source == .dictionary) {
                const d = p.domain.?;
                const trans = d.axes[axis].forRowGroup(file, rg) orelse return null;
                const codes = col.codes() orelse return null;
                if (codes.len < source.batch.num_rows) return error.BadColumn;
                const same = if (key.unsigned_32)
                    d.axes[axis].sameUnsigned32Dictionary(col, trans)
                else
                    d.axes[axis].sameDictionary(col, trans);
                if (!same) return null;
                binding.translation = trans;
            } else switch (col) {
                .i32 => if (key.physical_type != .INT32) return null,
                .i64 => if (key.physical_type != .INT64 and !key.unsigned_32) return null,
                else => return null,
            }
        }
        return .{ .plan = p, .source = source, .bindings = bindings };
    }

    fn identity(p: *const Plan, bits: u128) !KeyId {
        return switch (p.kind()) {
            .dense_u32 => .{ .dense_u32 = std.math.cast(u32, bits) orelse return error.BadKey },
            .packed_u64 => .{ .packed_u64 = std.math.cast(u64, bits) orelse return error.BadKey },
            .packed_u128 => .{ .packed_u128 = bits },
            .decline => unreachable,
        };
    }

    pub fn pack(p: *const Plan, batch: Bound, row: usize) !KeyId {
        if (batch.plan != p) return error.BadKey;
        return p.identity(try batch.packAs(u128, row));
    }

    pub fn unpack(p: *const Plan, id: KeyId, output_keys: []dense.Domain.KeyValue) !void {
        if (output_keys.len != p.keys.len) return error.BadKey;
        for (output_keys, 0..) |*out, axis| out.* = try p.keyValue(id, axis);
    }

    pub fn keyValue(p: *const Plan, id: KeyId, axis: usize) !dense.Domain.KeyValue {
        if (std.meta.activeTag(id) != p.kind() or axis >= p.keys.len) return error.BadKey;
        const bits: u128 = switch (id) {
            .dense_u32 => |v| v,
            .packed_u64 => |v| v,
            .packed_u128 => |v| v,
            .decline => return error.BadKey,
        };
        return switch (try p.encoding.unpack(bits, axis)) {
            .null_val => .null_val,
            .integer => |v| .{ .i = v },
            .dictionary_id => |v| p.domain.?.valueAt(axis, @intCast(v), p.keys[axis]),
        };
    }

    const Binding = struct { col: eval.Batch.Column, translation: []const u32 };

    pub const Bound = struct {
        plan: *const Plan,
        source: SelectedBatch,
        bindings: []const Binding,

        /// Owners specialize Bits once; their records never retain KeyId's union tag.
        pub fn packAs(b: Bound, comptime Bits: type, row: usize) !Bits {
            if (row >= b.source.batch.num_rows) return error.BadColumn;
            const bits = if (b.plan.encoding.source == .dictionary)
                try b.dictionaryBits(row)
            else
                try b.rawBits(row);
            return std.math.cast(Bits, bits) orelse error.BadKey;
        }

        pub fn dictionaryBits(b: Bound, row: usize) !u128 {
            if (row >= b.source.batch.num_rows or b.plan.encoding.source != .dictionary) return error.BadColumn;
            var bits: u128 = 0;
            var stride: u128 = 1;
            for (b.bindings, b.plan.radices) |binding, radix| {
                const ni = binding.col.nullInfo();
                const is_null = if (ni.def_levels) |dl| dl[row] < ni.max_def else false;
                const code = if (is_null) binding.translation.len - 1 else binding.col.codes().?[row];
                if (!is_null and code >= binding.translation.len - 1) return error.BadColumn;
                bits += binding.translation[code] * stride;
                stride *= radix;
            }
            return bits;
        }

        pub fn rawBits(b: Bound, row: usize) !u128 {
            if (row >= b.source.batch.num_rows or b.plan.encoding.source != .raw) return error.BadColumn;
            var bits: u128 = 0;
            var shift: u7 = 0;
            for (b.bindings, b.plan.keys, b.plan.fields) |binding, key, field| {
                const ni = binding.col.nullInfo();
                const is_null = if (ni.def_levels) |dl| dl[row] < ni.max_def else false;
                const value: ?i128 = if (is_null) null else switch (binding.col) {
                    inline .i32, .i64 => |c| blk: {
                        const at = if (c.codes) |codes| codes[row] else row;
                        const values = if (c.codes != null) c.dict else c.values;
                        if (at >= values.len) return error.BadColumn;
                        const v = values[at];
                        if (@TypeOf(v) == i64 and key.unsigned_32 and
                            (v < 0 or v > std.math.maxInt(u32))) return error.BadColumn;
                        break :blk if (key.unsigned_32)
                            @as(u32, @truncate(@as(u64, @bitCast(@as(i64, v)))))
                        else if (key.unsigned_64)
                            @as(u64, @bitCast(@as(i64, v)))
                        else
                            v;
                    },
                    else => unreachable,
                };
                bits |= try field.encode(value) << shift;
                shift += field.bits + 1;
            }
            return bits;
        }
    };
};

const testing = std.testing;

test "key plan raw pack and common unpack preserve UINT64 bits, component NULLs and original row ordinals" {
    const a = testing.allocator;
    const keys = [_]ast.Expr{.{ .col_ref = .{
        .col_idx = 0,
        .physical_type = .INT64,
        .expr_type = .i64,
        .unsigned_64 = true,
    } }};
    var plan = (try Plan.init(a, &keys, null)).?;
    defer plan.deinit(a);
    try testing.expectEqual(.packed_u128, plan.kind());
    const values = [_]i64{ -1, std.math.minInt(i64), 0 };
    const defs = [_]u32{ 1, 1, 0 };
    const cols = [_]eval.Batch.Column{.{ .i64 = .{ .values = &values, .def_levels = &defs, .max_def = 1 } }};
    const batch: eval.Batch = .{ .cols = &cols, .num_rows = 3 };
    var arena: std.heap.ArenaAllocator = .init(a);
    defer arena.deinit();
    const bound = (try plan.bind(arena.allocator(), .{
        .batch = &batch,
        .input_base = 400,
        .input_rows = &.{ 7, 50, 99 },
    }, &.{0}, 0, 0)).?;
    const expected = [_]?i128{ std.math.maxInt(u64), @as(i128, 1) << 63, null };
    var output: [1]dense.Domain.KeyValue = undefined;
    for (expected, 0..) |value, row| {
        const id = try plan.pack(bound, row);
        try testing.expectEqual(try bound.packAs(u128, row), id.packed_u128);
        try plan.unpack(id, &output);
        try testing.expectEqualDeep(if (value) |v| dense.Domain.KeyValue{ .i = v } else .null_val, output[0]);
    }
    try testing.expectEqual(@as(u64, 450), try bound.source.ordinal(1));
    try testing.expectError(error.BadKey, plan.unpack(.{ .packed_u64 = 0 }, &output));
    try testing.expectError(error.BadColumn, bound.packAs(u128, 3));
}

test "key plan dense dictionary ids compare values across permutations and decline missing translations" {
    const a = testing.allocator;
    var ints = [_]u64{ @bitCast(@as(i64, -7)), 42 };
    var x = [_]u8{'x'};
    var empty: [0]u8 = .{};
    var strings = [_][]u8{ &x, &empty };
    var t0 = [_]u32{ 1, 0, 2 };
    var t1 = [_]u32{ 0, 1, 2 };
    var ts0 = [_][]u32{&t0};
    var ts1 = [_][]u32{&t1};
    var files = [_]usize{0};
    var axes = [_]dense.Axis{
        .{ .values = .{ .int32 = &ints }, .trans = &ts0, .file_base = &files, .bytes = 0 },
        .{ .values = .{ .bytes = &strings }, .trans = &ts1, .file_base = &files, .bytes = 0 },
    };
    const domain: dense.Domain = .{ .axes = &axes, .bytes = 0 };
    const keys = [_]ast.Expr{
        .{ .col_ref = .{ .col_idx = 0, .physical_type = .INT32, .expr_type = .i64 } },
        .{ .col_ref = .{ .col_idx = 1, .physical_type = .BYTE_ARRAY, .expr_type = .str } },
    };
    var plan = (try Plan.init(a, &keys, &domain)).?;
    defer plan.deinit(a);
    try testing.expectEqual(.dense_u32, plan.kind());
    const c0 = [_]u32{ 0, 1, 999 };
    const c1 = [_]u32{ 1, 999, 1 };
    const def0 = [_]u32{ 1, 1, 0 };
    const def1 = [_]u32{ 1, 0, 1 };
    var cols = [_]eval.Batch.Column{
        .{ .i32 = .{ .values = &.{}, .dict = &.{ 42, -7 }, .codes = &c0, .def_levels = &def0, .max_def = 1 } },
        .{ .string = .{ .values = &.{}, .dict = &.{ "x", "" }, .codes = &c1, .def_levels = &def1, .max_def = 1 } },
    };
    const batch: eval.Batch = .{ .cols = &cols, .num_rows = 3 };
    var arena: std.heap.ArenaAllocator = .init(a);
    defer arena.deinit();
    const bound = (try plan.bind(arena.allocator(), .{ .batch = &batch }, &.{ 0, 1 }, 0, 0)).?;
    var output: [2]dense.Domain.KeyValue = undefined;
    try plan.unpack(try plan.pack(bound, 0), &output);
    try testing.expectEqualDeep(dense.Domain.KeyValue{ .i = 42 }, output[0]);
    try testing.expectEqualDeep(dense.Domain.KeyValue{ .s = "" }, output[1]);
    try plan.unpack(try plan.pack(bound, 1), &output);
    try testing.expectEqualDeep(dense.Domain.KeyValue{ .i = -7 }, output[0]);
    try testing.expectEqualDeep(dense.Domain.KeyValue.null_val, output[1]);
    try plan.unpack(try plan.pack(bound, 2), &output);
    try testing.expectEqualDeep(dense.Domain.KeyValue.null_val, output[0]);
    try testing.expectEqualDeep(dense.Domain.KeyValue{ .s = "" }, output[1]);
    try testing.expectEqual(null, try plan.bind(arena.allocator(), .{ .batch = &batch }, &.{ 0, 1 }, 0, 1));
    cols[0].i32.dict = &.{ 42, 99 };
    try testing.expectEqual(null, try plan.bind(arena.allocator(), .{ .batch = &batch }, &.{ 0, 1 }, 0, 0));
}

test "key plan raw widths decline oversized tuples and reject unproved UINT32 narrowing" {
    const a = testing.allocator;
    const key: ast.Expr = .{ .col_ref = .{
        .col_idx = 0,
        .physical_type = .INT32,
        .expr_type = .i64,
        .unsigned_32 = true,
    } };
    try testing.expectEqual(null, try Plan.init(a, &.{ key, key, key, key }, null));
    var plan = (try Plan.init(a, &.{key}, null)).?;
    defer plan.deinit(a);
    try testing.expectEqual(.packed_u64, plan.kind());
    const cols = [_]eval.Batch.Column{.{ .i64 = .{ .values = &.{ std.math.maxInt(u32), -1, 1 << 32 } } }};
    const batch: eval.Batch = .{ .cols = &cols, .num_rows = 3 };
    var arena: std.heap.ArenaAllocator = .init(a);
    defer arena.deinit();
    const bound = (try plan.bind(arena.allocator(), .{ .batch = &batch }, &.{0}, 0, 0)).?;
    try testing.expectEqual(@as(u64, std.math.maxInt(u32)), try bound.packAs(u64, 0));
    try testing.expectError(error.BadColumn, bound.packAs(u64, 1));
    try testing.expectError(error.BadColumn, bound.packAs(u64, 2));
}

test "key plan allocations release all owned certificates on failure and decline" {
    try testing.checkAllAllocationFailures(testing.allocator, struct {
        fn run(a: std.mem.Allocator) !void {
            const key: ast.Expr = .{ .col_ref = .{ .col_idx = 0, .physical_type = .INT32, .expr_type = .i64 } };
            var plan = (try Plan.init(a, &.{ key, key, key }, null)).?;
            defer plan.deinit(a);
            try testing.expectEqual(.packed_u128, plan.kind());
            try testing.expect(plan.retainedBytes() > 0);
            try testing.expectEqual(null, try Plan.init(a, &.{ key, key, key, key }, null));
        }
    }.run, .{});
}

test "key plan impossible raw widths and dictionary products decline without an allocation attempt" {
    const key: ast.Expr = .{ .col_ref = .{ .col_idx = 0, .physical_type = .INT32, .expr_type = .i64 } };
    var failing = testing.FailingAllocator.init(testing.allocator, .{ .fail_index = 0 });
    try testing.expectEqual(null, try Plan.init(failing.allocator(), &.{ key, key, key, key }, null));
    var value = [_]u8{'x'};
    var values = [_][]u8{&value};
    var axes: [127]dense.Axis = @splat(.{
        .values = .{ .bytes = &values },
        .trans = &.{},
        .file_base = &.{},
        .bytes = 0,
    });
    const domain: dense.Domain = .{ .axes = &axes, .bytes = 0 };
    const keys: [127]ast.Expr = @splat(.{ .col_ref = .{
        .col_idx = 0,
        .physical_type = .BYTE_ARRAY,
        .expr_type = .str,
    } });
    try testing.expectEqual(null, try Plan.init(failing.allocator(), &keys, &domain));
    try testing.expectEqual(@as(usize, 0), failing.alloc_index);
    try testing.expect(!failing.has_induced_failure);
    try testing.expectError(error.OutOfMemory, Plan.init(failing.allocator(), &.{key}, null));
    try testing.expect(failing.has_induced_failure);
}

test "key plan dictionary products wider than u64 bind and unpack without machine-word strides" {
    const a = testing.allocator;
    var v0 = [_]u8{'a'};
    var v1 = [_]u8{'b'};
    var v2 = [_]u8{'c'};
    var v3 = [_]u8{'d'};
    var values = [_][]u8{ &v0, &v1, &v2, &v3 };
    var translation = [_]u32{ 0, 1, 2, 3, 4 };
    var translations = [_][]u32{&translation};
    var files = [_]usize{0};
    var axes: [28]dense.Axis = @splat(.{
        .values = .{ .bytes = &values },
        .trans = &translations,
        .file_base = &files,
        .bytes = 0,
    });
    const domain: dense.Domain = .{ .axes = &axes, .bytes = 0 };
    var keys: [28]ast.Expr = undefined;
    var lookup: [28]?usize = undefined;
    var columns: [28]eval.Batch.Column = undefined;
    for (&keys, &lookup, &columns, 0..) |*key, *position, *column, axis| {
        key.* = .{ .col_ref = .{ .col_idx = axis, .physical_type = .BYTE_ARRAY, .expr_type = .str } };
        position.* = axis;
        column.* = .{ .string = .{
            .values = &.{},
            .dict = &.{ "a", "b", "c", "d" },
            .codes = &.{ 3, 999 },
            .def_levels = &.{ 1, 0 },
            .max_def = 1,
        } };
    }
    var plan = (try Plan.init(a, &keys, &domain)).?;
    defer plan.deinit(a);
    try testing.expectEqual(.packed_u128, plan.kind());
    try testing.expectEqual(arithmetic.planBytes(28, true, false), plan.retainedBytes());
    const batch: eval.Batch = .{ .cols = &columns, .num_rows = 2 };
    var arena: std.heap.ArenaAllocator = .init(a);
    defer arena.deinit();
    const bound = (try plan.bind(arena.allocator(), .{ .batch = &batch }, &lookup, 0, 0)).?;
    var output: [28]dense.Domain.KeyValue = undefined;
    try plan.unpack(try plan.pack(bound, 0), &output);
    for (output) |value| try testing.expectEqualDeep(dense.Domain.KeyValue{ .s = "d" }, value);
    const nulls = try plan.pack(bound, 1);
    try testing.expectEqual(@as(u128, 37252902984619140624), nulls.packed_u128);
    try plan.unpack(nulls, &output);
    for (output) |value| try testing.expectEqualDeep(dense.Domain.KeyValue.null_val, value);
}
