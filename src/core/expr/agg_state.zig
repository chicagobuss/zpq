//! Integer/count group records. The scalar union remains the interchange format and owns all non-integer states.
const std = @import("std");
const agg = @import("agg.zig");
const ast = @import("ast.zig");
const filter = @import("../filter/ast.zig");

pub const max_calls = 16;
const Kind = enum { count, sum, avg, min, max };
const Field = struct {
    kind: Kind,
    off: u16,
    unsigned: bool = false,
    shared_sum: bool = false,
};

pub const State = struct {
    fields: [max_calls]Field = undefined,
    stride: usize = 0,
    words: std.ArrayList(u64) = .empty,

    /// No float representation or merge order changes. Unsupported tuples keep the established union path.
    pub fn configure(self: *State, calls: []const agg.AggCall) void {
        std.debug.assert(self.words.items.len == 0);
        self.stride = 0;
        if (calls.len == 0 or calls.len > max_calls) return;
        var nullable = false;
        for (calls, 0..) |c, i| {
            const kind: Kind = switch (c.func) {
                .count => .count,
                .sum => if (c.result == .i64) .sum else return,
                .avg => if (c.arg != null and c.arg.?.typeOf() == .i64) .avg else return,
                .min => if (c.result == .i64) .min else return,
                .max => if (c.result == .i64) .max else return,
                .count_distinct => return,
            };
            // COUNT may evaluate arbitrary expressions; its state is still only a counter.
            self.fields[i] = .{ .kind = kind, .off = 0, .unsigned = if (c.arg) |a|
                a == .col_ref and a.col_ref.unsigned_64
            else
                false };
            nullable = nullable or kind == .sum or kind == .min or kind == .max;
        }
        // Sharing must not delay the first argument evaluation past another call's possible error.
        // A preceding AVG, or the immediately following call, preserves that evaluation order.
        var off: u16 = if (nullable) 8 else 0;
        for (calls, 0..) |c, i| {
            const f = &self.fields[i];
            if (f.kind == .sum and c.offset == 0) for (calls, 0..) |other, j| {
                if (self.fields[j].kind == .avg and (j < i or j == i + 1) and sameInput(c, other)) {
                    f.shared_sum = true;
                    break;
                }
            };
            if (f.shared_sum) continue;
            f.off = off;
            off += switch (f.kind) {
                .count, .min, .max => 8,
                .sum => 16,
                .avg => 24,
            };
        }
        for (calls, 0..) |c, i| if (self.fields[i].shared_sum) {
            for (calls, 0..) |other, j| {
                if (self.fields[j].kind == .avg and (j < i or j == i + 1) and sameInput(c, other)) {
                    self.fields[i].off = self.fields[j].off;
                    break;
                }
            }
        };
        self.stride = off / 8;
    }

    fn sameInput(a: agg.AggCall, b: agg.AggCall) bool {
        if (a.arg == null or b.arg == null or !ast.Expr.eql(a.arg.?, b.arg.?)) return false;
        if (a.where == null or b.where == null) return a.where == null and b.where == null;
        return filter.eql(a.where.?, b.where.?);
    }

    pub inline fn held(self: *const State) usize {
        return self.words.capacity * 8;
    }
    pub fn projected(self: *const State, groups: usize) usize {
        const need = groups * self.stride;
        return 8 * if (need <= self.words.capacity) self.words.capacity else std.ArrayList(u64).growCapacity(need);
    }
    pub fn reserve(self: *State, gpa: std.mem.Allocator, groups: usize) !void {
        try self.words.ensureTotalCapacity(gpa, groups * self.stride);
    }
    pub fn append(self: *State) void {
        const at = self.words.items.len;
        self.words.items.len += self.stride;
        @memset(self.words.items[at..], 0);
    }
    fn bytes(self: *const State, g: usize) []u8 {
        return std.mem.sliceAsBytes(self.words.items[g * self.stride ..][0..self.stride]);
    }
    pub inline fn count(self: *const State, g: usize, i: usize) u64 {
        const b = self.bytes(g)[self.fields[i].off..];
        return std.mem.readInt(u64, b[0..8], .little);
    }

    pub inline fn addCount(self: *const State, g: usize, i: usize, n: u64) void {
        const b = self.bytes(g)[self.fields[i].off..];
        std.mem.writeInt(u64, b[0..8], self.count(g, i) + n, .little);
    }

    pub fn foldRows(
        self: *const State,
        i: usize,
        values: ?@import("../filter/eval.zig").Batch.Column,
        sel: *const @import("../filter/selection.zig").SelectionVector,
        ids: []const u32,
        rows: ?[]const u32,
        offset: i64,
    ) void {
        if (self.fields[i].shared_sum) return;
        switch (self.fields[i].kind) {
            inline else => |kind| {
                if (rows) |rs| {
                    for (rs) |r| if (sel.isActive(r)) self.foldOne(kind, ids[r], i, values, r, offset);
                } else {
                    for (0..sel.len) |r| if (sel.isActive(r)) self.foldOne(kind, ids[r], i, values, r, offset);
                }
            },
        }
    }

    inline fn foldOne(
        self: *const State, comptime kind: Kind, g: usize, i: usize,
        values: ?@import("../filter/eval.zig").Batch.Column, r: usize, offset: i64,
    ) void {
        const f = self.fields[i];
        const b = self.bytes(g)[f.off..];
        if (kind == .count) {
            std.mem.writeInt(u64, b[0..8], std.mem.readInt(u64, b[0..8], .little) + 1, .little);
            return;
        }
        const v: i128 = switch (values.?) {
            .i32 => |c| c.values[r],
            .i64 => |c| if (f.unsigned) @as(u64, @bitCast(c.values[r])) else c.values[r],
            else => unreachable,
        };
        if (kind == .avg) {
            std.mem.writeInt(i128, b[0..16], std.mem.readInt(i128, b[0..16], .little) + v, .little);
            std.mem.writeInt(u64, b[16..24], std.mem.readInt(u64, b[16..24], .little) + 1, .little);
        } else {
            const flags = &self.words.items[g * self.stride];
            const mask = @as(u64, 1) << @intCast(i);
            const present = flags.* & mask != 0;
            if (kind == .sum) {
                std.mem.writeInt(i128, b[0..16], std.mem.readInt(i128, b[0..16], .little) + v + offset, .little);
            } else {
                const old = extreme(b, f.unsigned);
                const x = if (!present) v else if (kind == .min) @min(old, v) else @max(old, v);
                std.mem.writeInt(u64, b[0..8], @truncate(@as(u128, @bitCast(x))), .little);
            }
            flags.* |= mask;
        }
    }

    pub inline fn read(self: *const State, g: usize, i: usize) agg.Accumulator {
        const f = self.fields[i];
        const rec = self.bytes(g);
        const b = rec[f.off..];
        const present = if (f.shared_sum)
            std.mem.readInt(u64, b[16..24], .little) != 0
        else
            self.words.items[g * self.stride] & (@as(u64, 1) << @intCast(i)) != 0;
        return switch (f.kind) {
            .count => .{ .count = std.mem.readInt(u64, b[0..8], .little) },
            .sum => .{ .sum_i = if (present) std.mem.readInt(i128, b[0..16], .little) else null },
            .avg => .{ .avg = .{
                .sum = 0,
                .isum = std.mem.readInt(i128, b[0..16], .little),
                .count = std.mem.readInt(u64, b[16..24], .little),
            } },
            .min => .{ .min_i = if (present) extreme(b, f.unsigned) else null },
            .max => .{ .max_i = if (present) extreme(b, f.unsigned) else null },
        };
    }
    fn extreme(b: []const u8, unsigned: bool) i128 {
        return if (unsigned) std.mem.readInt(u64, b[0..8], .little) else std.mem.readInt(i64, b[0..8], .little);
    }
    pub fn write(self: *const State, g: usize, i: usize, a: agg.Accumulator) void {
        const f = self.fields[i];
        const rec = self.bytes(g);
        const b = rec[f.off..];
        const value: ?i128 = switch (f.kind) {
            .sum => a.sum_i,
            .min => a.min_i,
            .max => a.max_i,
            else => null,
        };
        if (f.kind == .sum or f.kind == .min or f.kind == .max) {
            if (!f.shared_sum) {
                const bit = @as(u64, 1) << @intCast(i);
                const flags = &self.words.items[g * self.stride];
                flags.* = if (value != null) flags.* | bit else flags.* & ~bit;
            }
        }
        switch (f.kind) {
            .count => std.mem.writeInt(u64, b[0..8], a.count, .little),
            .sum => std.mem.writeInt(i128, b[0..16], value orelse 0, .little),
            .avg => {
                std.mem.writeInt(i128, b[0..16], a.avg.isum, .little);
                std.mem.writeInt(u64, b[16..24], a.avg.count, .little);
            },
            .min, .max => std.mem.writeInt(u64, b[0..8], @truncate(@as(u128, @bitCast(value orelse 0))), .little),
        }
    }
};
