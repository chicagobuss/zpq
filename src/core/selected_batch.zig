//! Borrowed callback views. The producer validates gathered indices against the original source window;
//! this adapter retains those original indices so filtering and gathering cannot change group order or ties.

const std = @import("std");
const eval = @import("filter/eval.zig");
const selection = @import("filter/selection.zig");

pub const SelectedBatch = struct {
    batch: *const eval.Batch,
    selected: ?*const selection.SelectionVector = null,
    input_base: u64 = 0,
    input_rows: ?[]const u32 = null,

    pub fn validate(s: SelectedBatch) error{BadColumn}!void {
        const n = s.batch.num_rows;
        if (s.input_rows) |rows| if (rows.len != n) return error.BadColumn;
        if (s.selected) |selected| {
            const words = n / 64 + @intFromBool(n % 64 != 0);
            if (selected.len != n or selected.mask.len != words) return error.BadColumn;
        }
    }

    pub fn ordinal(s: SelectedBatch, i: usize) error{ BadColumn, Overflow }!u64 {
        try s.validate();
        if (i >= s.batch.num_rows) return error.BadColumn;
        const row: u64 = if (s.input_rows) |rows| rows[i] else @intCast(i);
        return std.math.add(u64, s.input_base, row);
    }
};

const testing = std.testing;

test "selected batch ordinals retain original gathered positions, selection and slice offsets" {
    const batch: eval.Batch = .{ .cols = &.{}, .num_rows = 3 };
    var mask = [_]u64{0b101};
    const selected: selection.SelectionVector = .{ .allocator = testing.allocator, .len = 3, .mask = &mask };
    var s: SelectedBatch = .{ .batch = &batch, .selected = &selected, .input_base = 400 };
    try testing.expectEqual(@as(u64, 401), try s.ordinal(1));
    s.input_rows = &.{ 7, 11, std.math.maxInt(u32) };
    try testing.expectEqual(@as(u64, 407), try s.ordinal(0));
    try testing.expectEqual(@as(u64, 411), try s.ordinal(1));
    try testing.expectEqual(@as(u64, std.math.maxInt(u32)) + 400, try s.ordinal(2));
    try testing.expectError(error.BadColumn, s.ordinal(3));
}

test "selected batch rejects malformed lengths and ordinal overflow before indexing" {
    var batch: eval.Batch = .{ .cols = &.{}, .num_rows = 2 };
    var s: SelectedBatch = .{ .batch = &batch, .input_base = std.math.maxInt(u64) };
    try testing.expectEqual(std.math.maxInt(u64), try s.ordinal(0));
    try testing.expectError(error.Overflow, s.ordinal(1));
    s.input_rows = &.{1};
    try testing.expectError(error.BadColumn, s.ordinal(0));
    s.input_rows = &.{ 0, 1 };
    try testing.expectError(error.Overflow, s.ordinal(1));
    var mask = [_]u64{1};
    var selected: selection.SelectionVector = .{ .allocator = testing.allocator, .len = 1, .mask = &mask };
    s.selected = &selected;
    try testing.expectError(error.BadColumn, s.ordinal(0));
    selected.len = 2;
    selected.mask = &.{};
    try testing.expectError(error.BadColumn, s.ordinal(0));
    s.selected = null;
    s.input_rows = null;
    batch.num_rows = 0;
    try s.validate();
    try testing.expectError(error.BadColumn, s.ordinal(0));
}
