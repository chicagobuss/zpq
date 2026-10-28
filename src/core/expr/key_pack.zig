//! Allocation-free identity arithmetic. Callers supply certified global dictionary cardinalities or fixed widths.
//! Local dictionary codes and unverified footer ranges must never be passed as global identities or width proofs.

const std = @import("std");
pub const SelectedBatch = @import("../selected_batch.zig").SelectedBatch;
pub const Plan = @import("key_plan.zig").Plan;
pub const KeyId = @import("key_plan.zig").KeyId;
pub const Binding = Plan.Binding;

pub fn planBytes(keys: usize, dictionary: bool, raw: bool) usize {
    return keys *| (@sizeOf(@import("ast.zig").ColRef) +|
        (if (dictionary) @as(usize, @sizeOf(u64)) else 0) +|
        (if (raw) @as(usize, @sizeOf(RawField)) else 0));
}

/// Per active producer, independent of the Cartesian domain and number of consumed windows.
pub fn bindingBytes(keys: usize) usize {
    return keys *| @sizeOf(Binding);
}

pub const Error = error{ BadKey, KeyTooWide };
// Leave the two high bits available to the shared record format; this is a layout bound, not an NDV heuristic.
pub const max_bits = 126;
pub const Width = enum { dense, word, wide };

/// Query-local descriptor, borrowing certificates that must outlive its batches and answer reconstruction.
/// Width selects the representation; the shared admission policy still decides dense addressing versus owners.
pub const KeyPack = struct {
    width: Width,
    source: union(enum) { dictionary: []const u64, raw: []const RawField },

    pub fn plan(radices: ?[]const u64, fields: []const RawField, dense_limit: u32) ?KeyPack {
        var dictionary: ?KeyPack = null;
        if (radices) |rs| if (product(rs)) |n| {
            dictionary = .{ .width = productWidth(n, dense_limit).?, .source = .{ .dictionary = rs } };
            if (dictionary.?.width != .wide) return dictionary;
        };
        const bits = rawBits(fields) catch return dictionary;
        if (bits > 64 and dictionary != null) return dictionary;
        return .{ .width = if (bits <= 64) .word else .wide, .source = .{ .raw = fields } };
    }

    pub fn dictionaryId(p: KeyPack, ids: []const u64) Error!u128 {
        if (p.source != .dictionary) return error.BadKey;
        return packRadix(p.source.dictionary, ids);
    }

    pub fn rawId(p: KeyPack, values: []const ?i128) Error!u128 {
        if (p.source != .raw) return error.BadKey;
        return packRaw(p.source.raw, values);
    }

    pub const Component = union(enum) { null_val, dictionary_id: u64, integer: i128 };

    pub fn unpack(p: KeyPack, id: u128, axis: usize) Error!Component {
        return switch (p.source) {
            .dictionary => |radices| blk: {
                const digit = try unpackRadix(radices, id, axis);
                break :blk if (digit == radices[axis] - 1) .null_val else .{ .dictionary_id = digit };
            },
            .raw => |fields| if (try unpackRaw(fields, id, axis)) |value| .{ .integer = value } else .null_val,
        };
    }
};

pub fn productWidth(cardinality: u128, dense_limit: u32) ?Width {
    if (cardinality == 0) return null;
    if (cardinality <= dense_limit) return .dense;
    if (cardinality <= @as(u128, 1) << 64) return .word;
    if (cardinality <= @as(u128, 1) << max_bits) return .wide;
    return null;
}

/// Cardinalities include each axis's NULL slot. A full 2^64 product still has a u64 identity for every tuple.
pub fn product(radices: []const u64) ?u128 {
    if (radices.len == 0) return null;
    var n: u128 = 1;
    for (radices) |radix| {
        if (radix == 0) return null;
        n = std.math.mul(u128, n, radix) catch return null;
        if (n > @as(u128, 1) << max_bits) return null;
    }
    return n;
}

pub fn packRadix(radices: []const u64, ids: []const u64) Error!u128 {
    if (radices.len != ids.len) return error.BadKey;
    _ = product(radices) orelse return error.KeyTooWide;
    var stride: u128 = 1;
    var out: u128 = 0;
    for (radices, ids) |radix, id| {
        if (id >= radix) return error.BadKey;
        out += id * stride;
        stride *= radix;
    }
    return out;
}

pub fn unpackRadix(radices: []const u64, id: u128, axis: usize) Error!u64 {
    const n = product(radices) orelse return error.KeyTooWide;
    if (axis >= radices.len or id >= n) return error.BadKey;
    var rem = id;
    for (radices[0..axis]) |radix| rem /= radix;
    return @intCast(rem % radices[axis]);
}

pub const RawField = struct {
    bits: u7,
    signed: bool,

    fn mask(f: RawField) u128 {
        return (@as(u128, 1) << f.bits) - 1;
    }

    pub fn encode(f: RawField, value: ?i128) Error!u128 {
        return f.encodeAs(u128, value);
    }

    pub fn encodeAs(f: RawField, comptime Bits: type, value: ?i128) Error!Bits {
        comptime requireBits(Bits);
        if (f.bits == 0 or f.bits > 64) return error.BadKey;
        if (@as(usize, f.bits) + 1 > @bitSizeOf(Bits)) return error.KeyTooWide;
        const shift: std.math.Log2Int(Bits) = @intCast(f.bits);
        const v = value orelse return @as(Bits, 1) << shift;
        if (f.signed) {
            const bound = @as(i128, 1) << (f.bits - 1);
            if (v < -bound or v >= bound) return error.BadKey;
        } else if (v < 0 or v > f.mask()) return error.BadKey;
        return @as(Bits, @truncate(@as(u128, @bitCast(v)))) & ((@as(Bits, 1) << shift) - 1);
    }

    fn decode(f: RawField, word: u128) Error!?i128 {
        const payload = word & f.mask();
        if (word & (@as(u128, 1) << f.bits) != 0) {
            if (payload != 0) return error.BadKey;
            return null;
        }
        const value: i128 = @intCast(payload);
        if (f.signed and payload & (@as(u128, 1) << (f.bits - 1)) != 0)
            return value - (@as(i128, 1) << f.bits);
        return value;
    }
};

pub fn requireBits(comptime Bits: type) void {
    if (Bits != u32 and Bits != u64 and Bits != u128) @compileError("KeyPack requires u32, u64 or u128");
}

pub fn rawBits(fields: []const RawField) Error!u7 {
    if (fields.len == 0) return error.BadKey;
    var total: usize = 0;
    for (fields) |f| {
        if (f.bits == 0 or f.bits > 64) return error.BadKey;
        total += @as(usize, f.bits) + 1;
        if (total > max_bits) return error.KeyTooWide;
    }
    return @intCast(total);
}

/// Reserve a separate NULL bit even for a currently REQUIRED component, so validity never aliases a value's bits.
pub fn packRaw(fields: []const RawField, values: []const ?i128) Error!u128 {
    if (fields.len != values.len) return error.BadKey;
    _ = try rawBits(fields);
    var shift: u7 = 0;
    var out: u128 = 0;
    for (fields, values) |f, value| {
        out |= try f.encode(value) << shift;
        shift += f.bits + 1;
    }
    return out;
}

pub fn unpackRaw(fields: []const RawField, id: u128, axis: usize) Error!?i128 {
    const total = try rawBits(fields);
    if (axis >= fields.len or id >> total != 0) return error.BadKey;
    var shift: u7 = 0;
    for (fields[0..axis]) |f| shift += f.bits + 1;
    return fields[axis].decode(id >> shift);
}

const testing = std.testing;

test "key pack typed raw lanes preserve payload and NULL bits without narrowing" {
    const signed: RawField = .{ .bits = 32, .signed = true };
    try testing.expectEqual(@as(u64, 0x8000_0000), try signed.encodeAs(u64, std.math.minInt(i32)));
    try testing.expectEqual(@as(u64, 0xffff_ffff), try signed.encodeAs(u64, -1));
    try testing.expectEqual(@as(u64, 1) << 32, try signed.encodeAs(u64, null));
    try testing.expectError(error.KeyTooWide, signed.encodeAs(u32, 0));
    const wide: RawField = .{ .bits = 64, .signed = false };
    try testing.expectEqual(@as(u128, std.math.maxInt(u64)), try wide.encodeAs(u128, std.math.maxInt(u64)));
    try testing.expectEqual(@as(u128, 1) << 64, try wide.encodeAs(u128, null));
    try testing.expectError(error.KeyTooWide, wide.encodeAs(u64, 0));
    try testing.expectError(error.BadKey, (RawField{ .bits = 0, .signed = true }).encodeAs(u64, 0));
}

test "key pack plans dense, dictionary word/wide, raw word/wide or decline and unpacks each form" {
    const fields = [_]RawField{ .{ .bits = 16, .signed = true }, .{ .bits = 32, .signed = false } };
    const dense = KeyPack.plan(&.{ 4, 3 }, &fields, 1 << 20).?;
    try testing.expectEqual(Width.dense, dense.width);
    const id = try dense.dictionaryId(&.{ 3, 1 });
    try testing.expectEqualDeep(KeyPack.Component.null_val, try dense.unpack(id, 0));
    try testing.expectEqualDeep(KeyPack.Component{ .dictionary_id = 1 }, try dense.unpack(id, 1));
    try testing.expectEqual(Width.word, KeyPack.plan(&.{ 1001, 101, 1001 }, &fields, 1 << 20).?.width);
    try testing.expectEqual(Width.wide, KeyPack.plan(&.{ 1 << 40, 1 << 40 }, &.{}, 1 << 20).?.width);
    const raw_word = KeyPack.plan(&.{ 1 << 40, 1 << 40 }, &fields, 1 << 20).?;
    try testing.expectEqual(Width.word, raw_word.width);
    try testing.expect(raw_word.source == .raw);
    const raw = KeyPack.plan(null, &fields, 1 << 20).?;
    try testing.expectEqual(Width.word, raw.width);
    const raw_id = try raw.rawId(&.{ -32768, std.math.maxInt(u32) });
    try testing.expectEqualDeep(KeyPack.Component{ .integer = -32768 }, try raw.unpack(raw_id, 0));
    try testing.expectEqualDeep(KeyPack.Component{ .integer = std.math.maxInt(u32) }, try raw.unpack(raw_id, 1));
    const wide = KeyPack.plan(null, &.{ .{ .bits = 64, .signed = true }, fields[1] }, 1 << 20).?;
    try testing.expectEqual(Width.wide, wide.width);
    try testing.expectEqual(null, KeyPack.plan(null, &.{}, 1 << 20));
    try testing.expectEqual(null, KeyPack.plan(null, &.{ fields[0], .{ .bits = 0, .signed = true } }, 1 << 20));
    try testing.expectEqual(Width.word, KeyPack.plan(&.{ 0, 3 }, &fields, 1 << 20).?.width);
    try testing.expectError(error.BadKey, raw.dictionaryId(&.{ 0, 0 }));
    try testing.expectError(error.BadKey, dense.rawId(&.{ 0, 0 }));
}

test "key pack mixed radix is injective across every tuple, including independent NULL slots" {
    const radices = [_]u64{ 4, 3, 5 };
    var seen: [60]bool = @splat(false);
    for (0..4) |a| for (0..3) |b| for (0..5) |c| {
        const ids = [_]u64{ a, b, c };
        const id = try packRadix(&radices, &ids);
        try testing.expect(!seen[@intCast(id)]);
        seen[@intCast(id)] = true;
        for (ids, 0..) |value, axis| try testing.expectEqual(value, try unpackRadix(&radices, id, axis));
    };
    for (seen) |v| try testing.expect(v);
    try testing.expectError(error.BadKey, packRadix(&radices, &.{ 4, 0, 0 }));
    try testing.expectError(error.BadKey, unpackRadix(&radices, 60, 0));
}

test "key pack product boundaries admit the full u64 range and decline beyond 126 bits" {
    const word = product(&.{ @as(u64, 1) << 32, @as(u64, 1) << 32 }).?;
    try testing.expectEqual(@as(u128, 1) << 64, word);
    try testing.expectEqual(Width.word, productWidth(word, 1 << 20).?);
    try testing.expectEqual(
        @as(u128, std.math.maxInt(u64)),
        try packRadix(&.{ @as(u64, 1) << 32, @as(u64, 1) << 32 }, &.{ 0xffff_ffff, 0xffff_ffff }),
    );
    try testing.expectEqual(Width.dense, productWidth(1 << 20, 1 << 20).?);
    try testing.expectEqual(Width.word, productWidth((1 << 20) + 1, 1 << 20).?);
    try testing.expectEqual(Width.wide, productWidth(word + 1, 1 << 20).?);
    try testing.expectEqual(@as(?u128, @as(u128, 1) << 126), product(&.{ 1 << 63, 1 << 63 }));
    try testing.expectEqual(null, product(&.{ 1 << 63, 1 << 63, 2 }));
    try testing.expectEqual(null, product(&.{ std.math.maxInt(u64), std.math.maxInt(u64), 4 }));
    try testing.expectEqual(null, product(&.{ 1, 0 }));
    try testing.expectEqual(null, product(&.{}));
}

test "key pack raw tuples preserve signed bits, unsigned extremes, component NULLs and range guards" {
    const fields = [_]RawField{ .{ .bits = 64, .signed = true }, .{ .bits = 32, .signed = false } };
    try testing.expectEqual(@as(u7, 98), try rawBits(&fields));
    const xs = [_]?i128{ null, 0, -1, std.math.minInt(i64), std.math.maxInt(i64) };
    const ys = [_]?i128{ null, 0, std.math.maxInt(u32) };
    var seen: [xs.len * ys.len]u128 = undefined;
    var n: usize = 0;
    for (xs) |x| for (ys) |y| {
        const id = try packRaw(&fields, &.{ x, y });
        for (seen[0..n]) |prior| try testing.expect(prior != id);
        seen[n] = id;
        n += 1;
        try testing.expectEqual(x, try unpackRaw(&fields, id, 0));
        try testing.expectEqual(y, try unpackRaw(&fields, id, 1));
    };
    try testing.expectError(error.BadKey, packRaw(&fields, &.{ @as(i128, 1) << 63, 0 }));
    try testing.expectError(error.BadKey, packRaw(&fields, &.{ 0, -1 }));
    try testing.expectError(error.BadKey, unpackRaw(&fields, @as(u128, 1) << 100, 0));
    try testing.expectError(error.BadKey, unpackRaw(&fields, (@as(u128, 1) << 64) | 1, 0));
    const unsigned = [_]RawField{.{ .bits = 64, .signed = false }};
    const id = try packRaw(&unsigned, &.{std.math.maxInt(u64)});
    try testing.expectEqual(@as(?i128, std.math.maxInt(u64)), try unpackRaw(&unsigned, id, 0));
    try testing.expectError(error.KeyTooWide, rawBits(&.{ fields[0], fields[0] }));
    try testing.expectError(error.BadKey, rawBits(&.{.{ .bits = 0, .signed = true }}));
}

test "key pack raw logical widths reject unproved narrowing and round-trip all small signed values" {
    const fields = [_]RawField{ .{ .bits = 4, .signed = true }, .{ .bits = 3, .signed = false } };
    var seen: [512]bool = @splat(false);
    for (0..17) |a| for (0..9) |b| {
        const x: ?i128 = if (a == 16) null else @as(i128, @intCast(a)) - 8;
        const y: ?i128 = if (b == 8) null else b;
        const id: usize = @intCast(try packRaw(&fields, &.{ x, y }));
        try testing.expect(!seen[id]);
        seen[id] = true;
        try testing.expectEqual(x, try unpackRaw(&fields, id, 0));
        try testing.expectEqual(y, try unpackRaw(&fields, id, 1));
    };
    try testing.expectError(error.BadKey, packRaw(&fields, &.{ -9, 0 }));
    try testing.expectError(error.BadKey, packRaw(&fields, &.{ 8, 0 }));
    try testing.expectError(error.BadKey, packRaw(&fields, &.{ 0, 8 }));
}
