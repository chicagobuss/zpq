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

    pub fn fitsBits(p: *const Plan, comptime Bits: type) bool {
        comptime arithmetic.requireBits(Bits);
        return @bitSizeOf(Bits) >= switch (p.encoding.width) {
            .dense => @as(usize, 32),
            .word => 64,
            .wide => 128,
        };
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
        return p.bindInto(bindings, source, lookup, file, rg);
    }

    /// Prepay scratch once per producer; the returned view expires before scratch or decoded columns are reused.
    /// A declined dictionary never touches owner state.
    pub fn bindInto(
        p: *const Plan,
        bindings: []Binding,
        source: SelectedBatch,
        lookup: []const ?usize,
        file: usize,
        rg: usize,
    ) !?Bound {
        try source.validate();
        if (bindings.len != p.keys.len) return error.BadColumn;
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
        return p.keyValueAs(u128, bits, axis);
    }

    pub fn unpackAs(p: *const Plan, comptime Bits: type, id: Bits, output_keys: []dense.Domain.KeyValue) !void {
        if (output_keys.len != p.keys.len) return error.BadKey;
        for (output_keys, 0..) |*out, axis| out.* = try p.keyValueAs(Bits, id, axis);
    }

    /// The owner has already selected Bits once. Final keys use the same reconstruction as tagged KeyId callers.
    pub fn keyValueAs(p: *const Plan, comptime Bits: type, id: Bits, axis: usize) !dense.Domain.KeyValue {
        if (!p.fitsBits(Bits) or axis >= p.keys.len) return error.BadKey;
        return switch (try p.encoding.unpack(id, axis)) {
            .null_val => .null_val,
            .integer => |v| .{ .i = v },
            .dictionary_id => |v| p.domain.?.valueAt(axis, @intCast(v), p.keys[axis]),
        };
    }

    pub const Binding = struct { col: eval.Batch.Column, translation: []const u32 };

    pub const Bound = struct {
        plan: *const Plan,
        source: SelectedBatch,
        bindings: []const Binding,

        /// Owners specialize Bits once; their records never retain KeyId's union tag.
        pub fn packAs(b: Bound, comptime Bits: type, row: usize) !Bits {
            if (row >= b.source.batch.num_rows) return error.BadColumn;
            if (!b.plan.fitsBits(Bits)) return error.BadKey;
            return if (b.plan.encoding.source == .dictionary)
                b.dictionaryAs(Bits, row)
            else
                b.rawAs(Bits, row);
        }

        /// `consume(context,row,key,ordinal)` can read payload lanes from the borrowed source. No IDs are buffered.
        /// A later identity decline requires replay of the entire private owner attempt, never a mixed key scheme.
        pub fn forEachSelected(b: Bound, comptime Bits: type, context: anytype, comptime consume: anytype) !void {
            try b.source.validate();
            if (!b.plan.fitsBits(Bits)) return error.BadKey;
            if (b.plan.encoding.source == .dictionary)
                return b.visit(Bits, true, context, consume);
            return b.visit(Bits, false, context, consume);
        }

        fn visit(
            b: Bound,
            comptime Bits: type,
            comptime dictionary: bool,
            context: anytype,
            comptime consume: anytype,
        ) !void {
            if (b.source.selected) |selected| {
                var rows = selected.iterator();
                while (rows.next()) |row| {
                    const bits = if (dictionary) try b.dictionaryAs(Bits, row) else try b.rawAs(Bits, row);
                    try consume(context, row, bits, try b.source.ordinal(row));
                }
            } else {
                for (0..b.source.batch.num_rows) |row| {
                    const bits = if (dictionary) try b.dictionaryAs(Bits, row) else try b.rawAs(Bits, row);
                    try consume(context, row, bits, try b.source.ordinal(row));
                }
            }
        }

        pub fn dictionaryBits(b: Bound, row: usize) !u128 {
            return b.dictionaryAs(u128, row);
        }

        fn dictionaryAs(b: Bound, comptime Bits: type, row: usize) !Bits {
            if (row >= b.source.batch.num_rows or b.plan.encoding.source != .dictionary) return error.BadColumn;
            var bits: Bits = 0;
            var stride: Bits = 1;
            for (b.bindings, b.plan.radices, 0..) |binding, radix, axis| {
                const ni = binding.col.nullInfo();
                const is_null = if (ni.def_levels) |dl| dl[row] < ni.max_def else false;
                const code = if (is_null) binding.translation.len - 1 else binding.col.codes().?[row];
                if (!is_null and code >= binding.translation.len - 1) return error.BadColumn;
                bits += @as(Bits, @intCast(binding.translation[code])) * stride;
                // A full 2^64 domain has u64 keys, but its final (unused) stride does not fit u64.
                if (axis + 1 < b.bindings.len) stride *= @intCast(radix);
            }
            return bits;
        }

        pub fn rawBits(b: Bound, row: usize) !u128 {
            return b.rawAs(u128, row);
        }

        fn rawAs(b: Bound, comptime Bits: type, row: usize) !Bits {
            if (row >= b.source.batch.num_rows or b.plan.encoding.source != .raw) return error.BadColumn;
            var bits: Bits = 0;
            var shift: usize = 0;
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
                bits |= try field.encodeAs(Bits, value) << @as(std.math.Log2Int(Bits), @intCast(shift));
                shift += field.bits + 1;
            }
            return bits;
        }
    };
};

const testing = std.testing;

const TestRecords = struct {
    const Record = struct { row: usize, bits: u128, ordinal: u64 };
    records: [8]Record = undefined,
    len: usize = 0,
    refuse: bool = false,

    fn append(s: *TestRecords, row: usize, bits: anytype, ordinal: u64) !void {
        if (s.refuse) return error.ExceededMemoryBudget;
        if (s.len == s.records.len) return error.TestUnexpectedResult;
        s.records[s.len] = .{ .row = row, .bits = bits, .ordinal = ordinal };
        s.len += 1;
    }
};

test "key plan sparse dictionary producers reuse fixed scratch and feed integer owners with gathered ordinals" {
    const a = testing.allocator;
    var values: [1000]u64 = undefined;
    for (&values, 0..) |*v, id| v.* = id;
    var t0 = [_]u32{ 7, 999, 1000 };
    var t1 = [_]u32{ 999, 7, 1000 };
    var trans = [_][]u32{ &t0, &t1 };
    var files = [_]usize{0};
    var axes: [3]dense.Axis = @splat(.{
        .values = .{ .int32 = &values },
        .trans = &trans,
        .file_base = &files,
        .bytes = 0,
    });
    const domain: dense.Domain = .{ .axes = &axes, .bytes = 0 };
    var keys: [3]ast.Expr = undefined;
    var codes: [3][65]u32 = undefined;
    var defs: [3][65]u32 = @splat(@splat(1));
    var cols: [3]eval.Batch.Column = undefined;
    for (&keys, &codes, &defs, &cols, 0..) |*key, *column_codes, *dl, *col, axis| {
        key.* = .{ .col_ref = .{ .col_idx = axis, .physical_type = .INT32, .expr_type = .i64 } };
        @memset(column_codes, 9999);
        for ([_]usize{ 0, 2, 64 }) |r| column_codes[r] = @intCast(axis % 2);
        if (axis == 2) dl[64] = 0;
        col.* = .{ .i32 = .{
            .values = &.{},
            .dict = &.{ 7, 999 },
            .codes = column_codes,
            .def_levels = dl,
            .max_def = 1,
        } };
    }
    var plan = (try Plan.init(a, &keys, &domain)).?;
    defer plan.deinit(a);
    try testing.expectEqual(.packed_u64, plan.kind());
    try testing.expect(domain.ids() > dense.max_ids);
    try testing.expect(plan.fitsBits(u64) and plan.fitsBits(u128) and !plan.fitsBits(u32));
    var mask = [_]u64{ 0b101, std.math.maxInt(u64) };
    const selected: @import("../filter/selection.zig").SelectionVector = .{
        .allocator = a,
        .len = 65,
        .mask = &mask,
    };
    var rows: [65]u32 = @splat(1);
    rows[0] = 40;
    rows[2] = 3;
    rows[64] = 900;
    const batch: eval.Batch = .{ .cols = &cols, .num_rows = 65 };
    const source: SelectedBatch = .{ .batch = &batch, .selected = &selected, .input_base = 100, .input_rows = &rows };
    var tracked = testing.FailingAllocator.init(a, .{});
    const scratch = try tracked.allocator().alloc(Plan.Binding, keys.len);
    defer tracked.allocator().free(scratch);
    try testing.expectEqual(arithmetic.bindingBytes(keys.len), tracked.allocated_bytes);
    var got: TestRecords = .{};
    const bound = (try plan.bindInto(scratch, source, &.{ 0, 1, 2 }, 0, 0)).?;
    try bound.forEachSelected(u64, &got, TestRecords.append);
    try testing.expectEqual(@as(usize, 3), got.len);
    const ordinary: u64 = 7 + 999 * 1001 + 7 * 1001 * 1001;
    const with_null: u64 = 7 + 999 * 1001 + 1000 * 1001 * 1001;
    try testing.expectEqual(ordinary, got.records[0].bits);
    try testing.expectEqual(ordinary, got.records[1].bits);
    try testing.expectEqual(with_null, got.records[2].bits);
    try testing.expectEqual(@as(u64, 140), got.records[0].ordinal);
    try testing.expectEqual(@as(u64, 103), got.records[1].ordinal);
    try testing.expectEqual(@as(usize, 64), got.records[2].row);
    try testing.expectEqual(@as(u64, 1000), got.records[2].ordinal);
    const Owner = @import("../owner_groups.zig").IntCountTop;
    var ledger: @import("../memory.zig").SharedBudget = .{ .limit = 256 << 20 };
    const owner = (try Owner.State.init(a, &ledger, 2000, 1, 0, 0, .{ .k = 10, .descending = true }, 64, 0)).?;
    defer owner.deinit();
    try bound.forEachSelected(u64, &owner.locals[0], struct {
        fn append(local: *Owner.Local, row: usize, bits: u64, ordinal: u64) !void {
            _ = row;
            try local.append(@as(i64, @bitCast(bits)), {}, std.math.cast(u32, ordinal) orelse return error.BadColumn);
        }
    }.append);
    try owner.finishSelected(3);
    const groups = owner.top();
    try testing.expectEqual(@as(usize, 2), groups.len);
    try testing.expectEqual(@as(i64, 2), groups[0].countValue());
    try testing.expectEqual(@as(u32, 103), groups[0].ordinal());
    try testing.expectEqual(ordinary, groups[0].key);
    var output: [3]dense.Domain.KeyValue = undefined;
    try plan.unpackAs(u64, groups[1].key, &output);
    try testing.expectEqualDeep(dense.Domain.KeyValue.null_val, output[2]);
    for (&cols, &codes) |*col, *column_codes| {
        col.i32.dict = &.{ 999, 7 };
        for ([_]usize{ 0, 2, 64 }) |r| column_codes[r] ^= 1;
    }
    var again: TestRecords = .{};
    const permuted = (try plan.bindInto(scratch, source, &.{ 0, 1, 2 }, 0, 1)).?;
    try permuted.forEachSelected(u64, &again, TestRecords.append);
    try testing.expectEqualDeep(got.records[0..got.len], again.records[0..again.len]);
    cols[1].i32.dict = &.{ 999, 42 };
    try testing.expectEqual(null, try plan.bindInto(scratch, source, &.{ 0, 1, 2 }, 0, 1));
    cols[1].i32.dict = &.{ 999, 7 };
    cols[1].i32.codes = null;
    const plain: [65]i32 = @splat(0);
    cols[1].i32.values = &plain;
    try testing.expectEqual(null, try plan.bindInto(scratch, source, &.{ 0, 1, 2 }, 0, 1));
}

test "key plan word specialization admits the full 2^64 dictionary product" {
    var keys = [_]ast.ColRef{
        .{ .col_idx = 0, .physical_type = .INT32, .expr_type = .i64 },
        .{ .col_idx = 1, .physical_type = .INT32, .expr_type = .i64 },
    };
    var radices = [_]u64{ 1 << 32, 1 << 32 };
    const plan: Plan = .{
        .keys = &keys,
        .radices = &radices,
        .fields = &.{},
        .domain = null,
        .encoding = .{ .width = .word, .source = .{ .dictionary = &radices } },
    };
    // A synthetic certificate tests the endpoint without allocating two 2^32-entry dictionaries.
    const bindings: [2]Plan.Binding = @splat(.{
        .col = .{ .i32 = .{
            .values = &.{},
            .codes = &.{ 0, 999 },
            .dict = &.{0},
            .def_levels = &.{ 1, 0 },
            .max_def = 1,
        } },
        .translation = &.{ 0xffff_fffe, 0xffff_ffff },
    });
    const batch: eval.Batch = .{ .cols = &.{}, .num_rows = 2 };
    const bound: Plan.Bound = .{ .plan = &plan, .source = .{ .batch = &batch }, .bindings = &bindings };
    try testing.expectEqual(@as(u64, 0xffff_fffe_ffff_fffe), try bound.packAs(u64, 0));
    try testing.expectEqual(@as(u64, std.math.maxInt(u64)), try bound.packAs(u64, 1));
}

test "key plan raw integer pairs deliver wide exact identities and refuse a narrow sink before append" {
    const a = testing.allocator;
    const keys = [_]ast.Expr{
        .{ .col_ref = .{ .col_idx = 0, .physical_type = .INT64, .expr_type = .i64, .unsigned_64 = true } },
        .{ .col_ref = .{ .col_idx = 1, .physical_type = .INT32, .expr_type = .i64, .unsigned_32 = true } },
    };
    var plan = (try Plan.init(a, &keys, null)).?;
    defer plan.deinit(a);
    const batch: eval.Batch = .{ .num_rows = 3, .cols = &.{
        .{ .i64 = .{ .values = &.{ std.math.minInt(i64), 0, undefined }, .def_levels = &.{ 1, 1, 0 }, .max_def = 1 } },
        .{ .i32 = .{ .values = &.{ -1, 0, undefined }, .def_levels = &.{ 1, 1, 0 }, .max_def = 1 } },
    } };
    var scratch: [2]Plan.Binding = undefined;
    const bound = (try plan.bindInto(&scratch, .{ .batch = &batch, .input_base = 900 }, &.{ 0, 1 }, 0, 0)).?;
    var got: TestRecords = .{};
    try testing.expectEqual(.packed_u128, plan.kind());
    try testing.expectError(error.BadKey, bound.forEachSelected(u64, &got, TestRecords.append));
    try testing.expectEqual(@as(usize, 0), got.len);
    try bound.forEachSelected(u128, &got, TestRecords.append);
    const expected = (@as(u128, std.math.maxInt(u32)) << 65) | (@as(u128, 1) << 63);
    try testing.expectEqual(expected, got.records[0].bits);
    try testing.expectEqual(@as(u128, 0), got.records[1].bits);
    try testing.expectEqual((@as(u128, 1) << 97) | (@as(u128, 1) << 64), got.records[2].bits);
    var output: [2]dense.Domain.KeyValue = undefined;
    try plan.unpackAs(u128, got.records[0].bits, &output);
    try testing.expectEqualDeep(dense.Domain.KeyValue{ .i = @as(i128, 1) << 63 }, output[0]);
    try testing.expectEqualDeep(dense.Domain.KeyValue{ .i = std.math.maxInt(u32) }, output[1]);
    try testing.expectError(error.BadKey, plan.unpackAs(u64, 0, &output));
    got = .{ .refuse = true };
    try testing.expectError(error.ExceededMemoryBudget, bound.forEachSelected(u128, &got, TestRecords.append));
    try testing.expectEqual(@as(usize, 0), got.len);
}

test "key plan raw INT32 and UINT32 pair keeps both NULL bits and wide original ordinals" {
    const a = testing.allocator;
    const keys = [_]ast.Expr{
        .{ .col_ref = .{ .col_idx = 0, .physical_type = .INT32, .expr_type = .i64 } },
        .{ .col_ref = .{ .col_idx = 1, .physical_type = .INT32, .expr_type = .i64, .unsigned_32 = true } },
    };
    var plan = (try Plan.init(a, &keys, null)).?;
    defer plan.deinit(a);
    const batch: eval.Batch = .{ .num_rows = 3, .cols = &.{
        .{ .i32 = .{ .values = &.{ std.math.minInt(i32), 0, undefined }, .def_levels = &.{ 1, 1, 0 }, .max_def = 1 } },
        .{ .i32 = .{ .values = &.{ -1, 0, undefined }, .def_levels = &.{ 1, 1, 0 }, .max_def = 1 } },
    } };
    var scratch: [2]Plan.Binding = undefined;
    const base: u64 = @as(u64, 1) << 40;
    const bound = (try plan.bindInto(&scratch, .{
        .batch = &batch,
        .input_base = base,
        .input_rows = &.{ 99, 7, 3 },
    }, &.{ 0, 1 }, 0, 0)).?;
    var got: TestRecords = .{};
    try testing.expectEqual(.packed_u128, plan.kind());
    try bound.forEachSelected(u128, &got, TestRecords.append);
    try testing.expectEqual((@as(u128, std.math.maxInt(u32)) << 33) | (1 << 31), got.records[0].bits);
    try testing.expectEqual(@as(u128, 0), got.records[1].bits);
    try testing.expectEqual((@as(u128, 1) << 65) | (1 << 32), got.records[2].bits);
    try testing.expectEqual(base + 99, got.records[0].ordinal);
    try testing.expectEqual(base + 7, got.records[1].ordinal);
    try testing.expectEqual(base + 3, got.records[2].ordinal);
    var output: [2]dense.Domain.KeyValue = undefined;
    try plan.unpackAs(u128, got.records[0].bits, &output);
    try testing.expectEqualDeep(dense.Domain.KeyValue{ .i = std.math.minInt(i32) }, output[0]);
    try testing.expectEqualDeep(dense.Domain.KeyValue{ .i = std.math.maxInt(u32) }, output[1]);
    try plan.unpackAs(u128, got.records[2].bits, &output);
    for (output) |value| try testing.expectEqualDeep(dense.Domain.KeyValue.null_val, value);
}

test "key plan prepaid binding rejects malformed views and propagates exact ordinal errors" {
    const a = testing.allocator;
    const key: ast.Expr = .{ .col_ref = .{ .col_idx = 0, .physical_type = .INT32, .expr_type = .i64 } };
    var plan = (try Plan.init(a, &.{key}, null)).?;
    defer plan.deinit(a);
    const batch: eval.Batch = .{ .num_rows = 2, .cols = &.{.{ .i32 = .{ .values = &.{ 4, 5 } } }} };
    var scratch: [1]Plan.Binding = undefined;
    var source: SelectedBatch = .{ .batch = &batch, .input_rows = &.{0} };
    try testing.expectError(error.BadColumn, plan.bindInto(&scratch, source, &.{0}, 0, 0));
    source.input_rows = null;
    try testing.expectError(error.BadColumn, plan.bindInto(scratch[0..0], source, &.{0}, 0, 0));
    var selected: @import("../filter/selection.zig").SelectionVector = .{
        .allocator = a,
        .len = 2,
        .mask = &.{},
    };
    source.selected = &selected;
    try testing.expectError(error.BadColumn, plan.bindInto(&scratch, source, &.{0}, 0, 0));
    var mask = [_]u64{0b10};
    selected.mask = &mask;
    source.input_base = std.math.maxInt(u64);
    var got: TestRecords = .{};
    const bound = (try plan.bindInto(&scratch, source, &.{0}, 0, 0)).?;
    try testing.expectError(error.Overflow, bound.forEachSelected(u64, &got, TestRecords.append));
    try testing.expectEqual(@as(usize, 0), got.len);
}

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
