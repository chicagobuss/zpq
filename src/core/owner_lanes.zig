//! Owned numeric partials. Identity packing belongs to KeyPlan; ordinals and weights are checked by admission.
const std = @import("std");
const eval = @import("filter/eval.zig");

pub const Lane = enum { sum, min, max, present_count };

const Wide = extern struct { value: i128 align(8) = 0 };

/// The full key remains authoritative. This multiply-fold is shared by cache, scatter and owner probe.
pub inline fn hashBits(bits: anytype) u64 {
    const hi: u64 = if (@TypeOf(bits) == u128) @truncate(bits >> 64) else 0;
    const product = @as(u128, @as(u64, @truncate(bits)) ^ 0x9E3779B97F4A7C15) *
        (hi ^ 0xD6E8FEB86659FD93);
    return @as(u64, @truncate(product)) ^ @as(u64, @truncate(product >> 64));
}

/// Ordinal=u32 requires the caller's original-input-rows <2^31 proof. Full key images never borrow flag bits.
pub fn Layout(comptime Bits: type, comptime kinds: []const Lane, comptime Ordinal: type) type {
    comptime {
        std.debug.assert(Bits == u64 or Bits == u128);
        std.debug.assert(Ordinal == u32 or Ordinal == u64);
        std.debug.assert(kinds.len <= 4);
    }
    const n = kinds.len;
    const mask: u8 = (1 << n) - 1;
    const value_types = comptime blk: {
        var types: [n]type = undefined;
        for (kinds, 0..) |kind, i| types[i] = switch (kind) {
            .sum => Wide,
            .min, .max => i64,
            .present_count => Ordinal,
        };
        break :blk types;
    };
    const Values = @Tuple(&value_types);
    return struct {
        const Self = @This();
        pub const Record = if (n == 0) extern struct {
            key: Bits align(8),
            ordinal: Ordinal,
            weight: Ordinal,
        } else extern struct {
            key: Bits align(8),
            ordinal: Ordinal,
            weight: Ordinal,
            values: [n]i64 = @splat(0),
            argument_nulls: u8 = 0,
        };

        pub const Bound = struct {
            columns: [n]eval.Batch.Column,
            unsigned32: [n]bool,
            reuse: [n]u8 = @splat(255),
            rows: usize,

            pub inline fn record(b: *const Bound, key: Bits, row: usize, input_ordinal: u64) !Record {
                if (row >= b.rows) return error.BadColumn;
                if (Ordinal == u32 and input_ordinal >= @as(u64, 1) << 31) return error.BadColumn;
                const pos = std.math.cast(Ordinal, input_ordinal) orelse return error.BadColumn;
                var r: Record = .{ .key = key, .ordinal = pos, .weight = 1 };
                if (n != 0) {
                    inline for (kinds, 0..) |kind, i| {
                        const col = b.columns[i];
                        const ni = col.nullInfo();
                        if (ni.def_levels != null and ni.def_levels.?[row] < ni.max_def) {
                            r.argument_nulls |= bit(i);
                        } else r.values[i] = if (kind == .present_count) 1 else value: {
                            inline for (0..i) |j| if (b.reuse[i] == j) break :value r.values[j];
                            break :value try signedAt(col, b.unsigned32[i], row);
                        };
                    }
                }
                return r;
            }
        };

        /// Borrow payloads once per callback. COUNT(col) needs only definition levels, including an empty values
        /// view; value-lane bounds and dictionary poison are checked only for selected non-NULL arguments.
        pub fn bind(program: anytype, view: *const eval.Batch, lookup: []const ?usize) !Bound {
            if (!program.matches(kinds)) return error.BadColumn;
            var b: Bound = .{ .columns = undefined, .unsigned32 = undefined, .rows = view.num_rows };
            inline for (kinds, 0..) |kind, i| {
                const col_ref = program.args[i].col;
                if (col_ref.col_idx >= lookup.len) return error.BadColumn;
                const at = lookup[col_ref.col_idx] orelse return error.BadColumn;
                if (at >= view.cols.len) return error.BadColumn;
                const col = view.cols[at];
                const ni = col.nullInfo();
                if (ni.def_levels) |dl| if (dl.len != view.num_rows) return error.BadColumn;
                if (kind != .present_count) {
                    if (col != .i32 and col != .i64) return error.BadColumn;
                    if (col.codes()) |codes| if (codes.len != view.num_rows) return error.BadColumn;
                }
                b.columns[i] = col;
                b.unsigned32[i] = col_ref.unsigned_32;
                // SUM/MIN/MAX over the same source read one original row value. Coalescing happens afterwards.
                if (kind != .present_count) inline for (0..i) |j| {
                    if (kinds[j] != .present_count and program.args[j].col.col_idx == col_ref.col_idx and
                        program.args[j].col.unsigned_32 == col_ref.unsigned_32)
                    {
                        b.reuse[i] = @intCast(j);
                        break;
                    }
                };
            }
            return b;
        }

        fn signedAt(col: eval.Batch.Column, unsigned32: bool, row: usize) !i64 {
            return switch (col) {
                .i32 => |c| blk: {
                    const value = if (c.codes) |codes| v: {
                        if (codes[row] >= c.dict.len) return error.BadColumn;
                        break :v c.dict[codes[row]];
                    } else v: {
                        if (row >= c.values.len) return error.BadColumn;
                        break :v c.values[row];
                    };
                    break :blk if (unsigned32) @as(u32, @bitCast(value)) else value;
                },
                .i64 => |c| if (c.codes) |codes| blk: {
                    if (codes[row] >= c.dict.len) return error.BadColumn;
                    break :blk c.dict[codes[row]];
                } else blk: {
                    if (row >= c.values.len) return error.BadColumn;
                    break :blk c.values[row];
                },
                else => error.BadColumn,
            };
        }

        pub fn validate(r: Record) error{BadPartial}!void {
            if (r.weight == 0) return error.BadPartial;
            if (n != 0) {
                if (r.argument_nulls & ~mask != 0) return error.BadPartial;
                inline for (kinds, 0..) |kind, i| if (kind == .present_count and r.argument_nulls & bit(i) == 0) {
                    const count = r.values[i];
                    if (count < 0 or @as(u64, @intCast(count)) > r.weight) return error.BadPartial;
                };
            }
        }

        /// A refused merge leaves the cache entry unchanged so it can flush before retrying this contribution.
        pub fn coalesce(into: *Record, from: Record) error{BadPartial}!bool {
            try validate(from);
            if (into.weight == 0) {
                into.* = from;
                return true;
            }
            if (into.key != from.key) return false;
            var next = into.*;
            next.weight = std.math.add(Ordinal, into.weight, from.weight) catch return false;
            next.ordinal = @min(into.ordinal, from.ordinal);
            if (n != 0) inline for (kinds, 0..) |kind, i| {
                if (from.argument_nulls & bit(i) == 0) {
                    if (into.argument_nulls & bit(i) != 0) {
                        next.values[i] = from.values[i];
                        next.argument_nulls &= ~bit(i);
                    } else next.values[i] = switch (kind) {
                        .sum, .present_count => std.math.add(i64, into.values[i], from.values[i]) catch return false,
                        .min => @min(into.values[i], from.values[i]),
                        .max => @max(into.values[i], from.values[i]),
                    };
                }
            };
            into.* = next;
            return true;
        }

        pub const Group = struct {
            key: Bits align(8),
            first: Ordinal = std.math.maxInt(Ordinal),
            weight: Ordinal = 0,
            values: Values = std.mem.zeroes(Values),
            present: if (n == 0) void else u8 = if (n == 0) {} else 0,

            pub fn init(r: Record) error{BadPartial}!Group {
                var g: Group = .{ .key = r.key };
                try g.add(r);
                return g;
            }

            pub fn add(g: *Group, r: Record) error{BadPartial}!void {
                try validate(r);
                if (g.key != r.key) return error.BadPartial;
                const weight = std.math.add(Ordinal, g.weight, r.weight) catch return error.BadPartial;
                inline for (kinds, 0..) |kind, i| {
                    if (r.argument_nulls & bit(i) == 0) {
                        switch (kind) {
                            .sum => g.values[i].value += r.values[i],
                            .present_count => g.values[i] += @intCast(r.values[i]),
                            .min => g.values[i] = if (g.present & bit(i) == 0)
                                r.values[i]
                            else
                                @min(g.values[i], r.values[i]),
                            .max => g.values[i] = if (g.present & bit(i) == 0)
                                r.values[i]
                            else
                                @max(g.values[i], r.values[i]),
                        }
                        g.present |= bit(i);
                    }
                }
                g.weight = weight;
                g.first = @min(g.first, r.ordinal);
            }

            pub fn merge(g: *Group, other: Group) error{BadPartial}!void {
                if (g.key != other.key) return error.BadPartial;
                const weight = std.math.add(Ordinal, g.weight, other.weight) catch return error.BadPartial;
                inline for (kinds, 0..) |kind, i| {
                    if (other.present & bit(i) != 0) {
                        switch (kind) {
                            .sum => g.values[i].value += other.values[i].value,
                            .present_count => g.values[i] += other.values[i],
                            .min => g.values[i] = if (g.present & bit(i) == 0)
                                other.values[i]
                            else
                                @min(g.values[i], other.values[i]),
                            .max => g.values[i] = if (g.present & bit(i) == 0)
                                other.values[i]
                            else
                                @max(g.values[i], other.values[i]),
                        }
                        g.present |= bit(i);
                    }
                }
                g.weight = weight;
                g.first = @min(g.first, other.first);
            }

            pub fn integer(g: *const Group, comptime i: usize) ?i128 {
                return switch (kinds[i]) {
                    .sum => if (g.present & bit(i) != 0) g.values[i].value else null,
                    .min, .max => if (g.present & bit(i) != 0) g.values[i] else null,
                    .present_count => g.values[i],
                };
            }

            pub fn integerAt(g: *const Group, i: usize) error{BadPartial}!?i128 {
                inline for (kinds, 0..) |_, at| if (i == at) return g.integer(at);
                return error.BadPartial;
            }
        };

        pub fn hashKey(key: Bits) u64 {
            return hashBits(key);
        }
        pub fn sameKey(a: Bits, b: Bits) bool {
            return a == b;
        }
        pub fn hashRecord(r: Record) u64 {
            return hashKey(r.key);
        }
        pub fn hashGroup(g: Group) u64 {
            return hashKey(g.key);
        }
        pub fn same(r: Record, g: Group) bool {
            return sameKey(r.key, g.key);
        }
        pub fn ordinal(r: Record) Ordinal {
            return r.ordinal;
        }
        pub fn admitsRows(rows: usize) bool {
            return if (Ordinal == u32) rows < @as(usize, 1) << 31 else true;
        }

        pub const CachePolicy = struct {
            pub const Entry = Record;
            pub fn validate(r: Record) error{BadPartial}!void {
                return Self.validate(r);
            }
            pub fn active(r: Record) bool {
                return r.weight != 0;
            }
            pub fn same(a: Record, b: Record) bool {
                return a.key == b.key;
            }
            pub fn hash(r: Record) u64 {
                return hashBits(r.key);
            }
            pub fn clear(r: *Record) void {
                r.weight = 0;
            }
            pub fn coalesce(a: *Record, b: Record) error{BadPartial}!bool {
                return Self.coalesce(a, b);
            }
        };

        fn bit(comptime i: usize) u8 {
            return @as(u8, 1) << i;
        }
    };
}

const testing = std.testing;

test "owner lanes retain the compact COUNT record and omit unused group lanes" {
    const C = Layout(u64, &.{}, u32);
    try testing.expectEqual(@as(usize, 16), @sizeOf(C.Record));
    try testing.expectEqual(@as(usize, 16), @sizeOf(C.Group));
    try testing.expectEqual(@as(usize, 24), @sizeOf(Layout(u128, &.{}, u32).Record));
    try testing.expectEqual(@as(usize, 24), @sizeOf(Layout(u64, &.{}, u64).Record));
    var r: C.Record = .{ .key = std.math.maxInt(u64), .ordinal = 90, .weight = 3 };
    try testing.expect(try C.coalesce(&r, .{ .key = r.key, .ordinal = 2, .weight = 4 }));
    const g = try C.Group.init(r);
    try testing.expectEqual(@as(u32, 7), g.weight);
    try testing.expectEqual(@as(u32, 2), g.first);
}

test "owner lanes keep exact weighted sums, extrema, NULL presence and original first ordinals" {
    const P = Layout(u128, &.{ .sum, .min, .max, .present_count }, u32);
    var r: P.Record = .{
        .key = std.math.maxInt(u128),
        .ordinal = 80,
        .weight = 2,
        .values = .{ 0, 0, 0, 0 },
        .argument_nulls = 0b1111,
    };
    var g = try P.Group.init(r);
    try testing.expectEqual(@as(?i128, null), g.integer(0));
    try testing.expectEqual(@as(?i128, 0), g.integer(3));
    r = .{ .key = r.key, .ordinal = 30, .weight = 3, .values = .{ 12, -2, 9, 3 } };
    var cached = r;
    try testing.expect(try P.coalesce(&cached, .{
        .key = r.key,
        .ordinal = 4,
        .weight = 2,
        .values = .{ -12, -10, 2, 1 },
    }));
    try g.add(cached);
    try testing.expectEqual(@as(u32, 7), g.weight);
    try testing.expectEqual(@as(u32, 4), g.first);
    try testing.expectEqual(@as(?i128, 0), g.integer(0));
    try testing.expectEqual(@as(?i128, -10), g.integer(1));
    try testing.expectEqual(@as(?i128, 9), g.integer(2));
    try testing.expectEqual(@as(?i128, 4), g.integer(3));
    var separate = try P.Group.init(r);
    try separate.add(.{ .key = r.key, .ordinal = 4, .weight = 2, .values = .{ -12, -10, 2, 1 } });
    var merged = try P.Group.init(.{
        .key = r.key,
        .ordinal = 80,
        .weight = 2,
        .values = .{ 0, 0, 0, 0 },
        .argument_nulls = 0b1111,
    });
    try merged.merge(separate);
    try testing.expectEqualDeep(g, merged);
}

test "owner lanes refuse cache overflow atomically and retain the exact wide result after flush" {
    const P = Layout(u64, &.{ .sum, .min }, u32);
    const one: P.Record = .{ .key = 4, .ordinal = 90, .weight = 1, .values = .{ std.math.minInt(i64), 7 } };
    var cache = one;
    const next: P.Record = .{ .key = 4, .ordinal = 2, .weight = 1, .values = .{ std.math.minInt(i64), -7 } };
    try testing.expect(!try P.coalesce(&cache, next));
    try testing.expectEqualDeep(one, cache);
    var g = try P.Group.init(cache);
    try g.add(next);
    try testing.expectEqual(-(@as(i128, 1) << 64), g.integer(0).?);
    try testing.expectEqual(@as(?i128, -7), g.integer(1));
    try testing.expectEqual(@as(u32, 2), g.first);
}

test "owner lanes reject invalid presence counts and identity changes without mutating cached partials" {
    const P = Layout(u64, &.{.present_count}, u64);
    var r: P.Record = .{ .key = 1, .ordinal = 9, .weight = 2, .values = .{1} };
    const original = r;
    try testing.expectError(error.BadPartial, P.coalesce(&r, .{ .key = 1, .ordinal = 2, .weight = 1, .values = .{2} }));
    try testing.expectEqualDeep(original, r);
    try testing.expect(!try P.coalesce(&r, .{ .key = 2, .ordinal = 1, .weight = 1, .values = .{1} }));
    try testing.expectEqualDeep(original, r);
}

test "owner lanes COUNT binding reads levels-only columns without touching empty values" {
    const Program = @import("agg_plan.zig").OwnerProgram;
    const call: @import("expr/agg.zig").AggCall = .{
        .func = .count,
        .alias = "c",
        .result = .i64,
        .where = null,
        .arg = .{ .col_ref = .{ .col_idx = 0, .physical_type = .INT64, .expr_type = .i64 } },
    };
    const program = Program.init(&.{call}).?;
    const L = Layout(u64, &.{.present_count}, u32);
    const batch: eval.Batch = .{ .num_rows = 3, .cols = &.{
        .{ .i64 = .{ .values = &.{}, .def_levels = &.{ 1, 0, 1 }, .max_def = 1 } },
    } };
    const bound = try L.bind(program, &batch, &.{0});
    var g = try L.Group.init(try bound.record(0, 0, 2));
    try g.add(try bound.record(0, 1, 1));
    try g.add(try bound.record(0, 2, 0));
    try testing.expectEqual(@as(?i128, 2), g.integer(0));
    try testing.expectEqual(@as(u32, 3), g.weight);
    try testing.expectEqual(@as(u32, 0), g.first);
    try testing.expectError(error.BadColumn, bound.record(0, 3, 3));
    try testing.expectError(error.BadColumn, bound.record(0, 0, @as(u64, 1) << 40));
    try testing.expectError(error.BadColumn, bound.record(0, 0, @as(u64, 1) << 31));
}

test "owner lanes bind unsigned coded payloads with NULL poison and share numeric presence" {
    const Program = @import("agg_plan.zig").OwnerProgram;
    const AggCall = @import("expr/agg.zig").AggCall;
    const col: @import("expr/ast.zig").Expr = .{ .col_ref = .{
        .col_idx = 0,
        .physical_type = .INT32,
        .expr_type = .i64,
        .unsigned_32 = true,
    } };
    const calls = [_]AggCall{
        .{ .func = .sum, .alias = "s", .result = .i64, .where = null, .arg = col },
        .{ .func = .avg, .alias = "a", .result = .avg_f64, .where = null, .arg = col },
        .{ .func = .min, .alias = "lo", .result = .i64, .where = null, .arg = col },
        .{ .func = .max, .alias = "hi", .result = .i64, .where = null, .arg = col },
    };
    const program = Program.init(&calls).?;
    const L = Layout(u128, &.{ .sum, .min, .max, .present_count }, u64);
    const batch: eval.Batch = .{ .num_rows = 3, .cols = &.{
        .{ .i32 = .{
            .values = &.{},
            .dict = &.{ -1, 0 },
            .codes = &.{ 0, 9999, 1 },
            .def_levels = &.{ 1, 0, 1 },
            .max_def = 1,
        } },
    } };
    const bound = try L.bind(program, &batch, &.{0});
    var g = try L.Group.init(try bound.record(std.math.maxInt(u128), 0, 1 << 40));
    try g.add(try bound.record(g.key, 1, 1));
    try g.add(try bound.record(g.key, 2, 2));
    try testing.expectEqual(@as(?i128, std.math.maxInt(u32)), g.integer(0));
    try testing.expectEqual(@as(?i128, 0), g.integer(1));
    try testing.expectEqual(@as(?i128, std.math.maxInt(u32)), g.integer(2));
    try testing.expectEqual(@as(?i128, 2), g.integer(3));
    try testing.expectEqual(@as(u64, 1), g.first);
}
