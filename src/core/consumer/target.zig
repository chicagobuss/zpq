const std = @import("std");
const owner_groups = @import("../owner_groups.zig");
const msd_groups = @import("../msd_groups.zig");
const retained_pages = @import("../retained_pages.zig");
const expr_ast = @import("../expr/ast.zig");
const expr_agg = @import("../expr/agg.zig");
const agg_dense = @import("../expr/agg_dense.zig");
const partagg = @import("../partagg.zig");
const filter_eval = @import("../filter/eval.zig");
const filter_selection = @import("../filter/selection.zig");
const SelectedBatch = @import("../selected_batch.zig").SelectedBatch;
const selectedWindow = @import("types.zig").selectedWindow;

test "a selected record arm keeps group-key codes and original gathered rows" {
    const keys = [_]expr_ast.Expr{
        .{ .col_ref = .{ .col_idx = 0, .physical_type = .INT32, .expr_type = .i64 } },
        .{ .col_ref = .{ .col_idx = 1, .physical_type = .BYTE_ARRAY, .expr_type = .str } },
    };
    const Capture = struct {
        n: usize = 0,
        ords: [2]u64 = undefined,
        fn start(_: *anyopaque, _: u64, _: usize) void {}
        fn batch(ctx: *anyopaque, view: SelectedBatch, _: []const ?usize) !void {
            const c: *@This() = @ptrCast(@alignCast(ctx));
            for (0..view.batch.num_rows) |i| {
                c.ords[c.n] = try view.ordinal(i);
                c.n += 1;
            }
        }
    };
    var cap: Capture = .{};
    var arm: RecordSink = .{ .selected = .{
        .ctx = @ptrCast(&cap),
        .startFn = Capture.start,
        .batchFn = Capture.batch,
    } };
    var target: AggTarget = .{ .grouped = .{
        .calls = &.{},
        .keys = &keys,
        .table = undefined,
        .pos_base = 0,
        .records = &arm,
    } };
    try std.testing.expect(target.codedKey(0));
    try std.testing.expect(target.codedKey(1));
    try std.testing.expect(!target.codedKey(2));

    const batch: filter_eval.Batch = .{ .cols = &.{}, .num_rows = 2 };
    var mask = [_]u64{0b11};
    const sel: filter_selection.SelectionVector = .{ .allocator = std.testing.allocator, .len = 2, .mask = &mask };
    const gathered = try selectedWindow(&batch, null, 100, &.{ 4, 9 }, 10);
    try arm.emit(gathered, &.{}, 0, &sel);
    try std.testing.expectEqual(@as(u64, 104), cap.ords[0]);
    try std.testing.expectEqual(@as(u64, 109), cap.ords[1]);

    cap.n = 0;
    const plain = try selectedWindow(&batch, null, 50, null, 2);
    try arm.emit(plain, &.{}, 7, &sel);
    try std.testing.expectEqual(@as(u64, 50), cap.ords[0]);
    try std.testing.expectEqual(@as(u64, 51), cap.ords[1]);

    const backwards = try selectedWindow(&batch, null, 100, &.{ 9, 4 }, 10);
    try std.testing.expectError(error.RetainedEncodingUnsupported, arm.emit(backwards, &.{}, 0, &sel));
    var dummy: owner_groups.IntFull.Local = undefined;
    const legacy: RecordSink = .{ .integer = &dummy };
    try std.testing.expectError(error.RetainedEncodingUnsupported, legacy.emit(gathered, &.{}, 0, &sel));
}

pub const PackedCount = struct {
    pub const Source = struct { file: usize, rg: usize };

    domain: *const agg_dense.Domain,
    keys: []const expr_ast.Expr,
    sources: []const Source,
    bindings: []agg_dense.CountAxis,
    refused: *std.atomic.Value(bool),
};

pub const IntegerCountSink = struct {
    local: *owner_groups.IntCountTop.Local,
    packing: ?*PackedCount = null,

    fn batch(
        s: IntegerCountSink,
        view: filter_eval.Batch,
        lookup: []const ?usize,
        offset: usize,
        selected: ?*const filter_selection.SelectionVector,
    ) !void {
        const p = s.packing orelse return s.local.batchSelected(view, lookup, offset, selected);
        if (p.refused.load(.monotonic)) return;
        const source = p.sources[s.local.run_item];
        if (!try p.domain.bind(p.keys, source.file, source.rg, &view, lookup, p.bindings)) {
            p.refused.store(true, .monotonic);
            return;
        }
        if (selected) |sel| {
            var it = sel.iterator();
            while (it.next()) |row| try s.packedRow(p, offset, row);
        } else for (0..view.num_rows) |r| try s.packedRow(p, offset, r);
    }

    inline fn packedRow(s: IntegerCountSink, p: *const PackedCount, offset: usize, r: usize) !void {
        const id = try agg_dense.productId(p.bindings, r, true);
        const ordinal = s.local.ordinal_base + @as(u32, @intCast(offset + r));
        try s.local.append(@bitCast(@as(u64, @intCast(id))), {}, ordinal);
    }
};

/// One callback for every key and lane layout. `startFn` receives the original global row of the item, pruned rows
/// included. `batchFn` borrows the view until it returns; `row` indexes that view, and order and ties use
/// `SelectedBatch.ordinal` only. A gathered `input_rows` is window-relative.
pub const SelectedRecords = struct {
    ctx: *anyopaque,
    startFn: *const fn (*anyopaque, u64, usize) void,
    batchFn: *const fn (*anyopaque, SelectedBatch, []const ?usize) anyerror!void,

    pub fn start(s: SelectedRecords, original_input_base: u64, item: usize) void {
        s.startFn(s.ctx, original_input_base, item);
    }

    pub fn batch(s: SelectedRecords, view: SelectedBatch, lookup: []const ?usize) !void {
        return s.batchFn(s.ctx, view, lookup);
    }
};

/// The owned-record layouts share worker startup and decoded-batch routing. `.selected` is the only arm that may
/// see a gathered view. The other tags keep packed `ordinal_base + offset + i` for an ungathered window.
pub const RecordSink = union(enum) {
    integer: *owner_groups.IntFull.Local,
    integer_top: *owner_groups.IntSumTop.Local,
    integer_count_top: IntegerCountSink,
    bytes: *owner_groups.Full.Local,
    byte_top: *owner_groups.SumTop.Local,
    byte_count_top: *owner_groups.CountTop.Local,
    msd: *msd_groups.Local,
    selected: SelectedRecords,

    pub fn start(s: RecordSink, ordinal_base: u32, item: usize) void {
        switch (s) {
            // Packed work-item base. The scan starts `.selected` through `SelectedRecords.start` instead.
            .selected => unreachable,
            .msd => |local| local.start(ordinal_base, item),
            .integer_count_top => |s0| s0.local.start(ordinal_base, item),
            inline else => |local| local.start(ordinal_base, item),
        }
    }

    pub fn retention(s: RecordSink, column: usize) ?*retained_pages.Chunk {
        return switch (s) {
            .msd => |local| if (column == local.state.key_col) local.owner else null,
            else => null,
        };
    }

    pub fn rewindRetained(s: RecordSink) void {
        switch (s) {
            .msd => |local| local.owner.?.used = 0,
            else => {},
        }
    }

    pub fn decodeReservation(s: RecordSink, ordinary: usize) usize {
        return switch (s) {
            // Retained payload capacity is already funded outside the decoder. Reserving it again would force
            // slices, changing which column's decode error wins. Leave the quote's margin for arena rounding.
            .msd => |local| local.state.decode_budget.limit / local.state.locals.len - (4 << 20),
            else => ordinary,
        };
    }

    fn batch(
        s: RecordSink,
        view: filter_eval.Batch,
        lookup: []const ?usize,
        offset: usize,
        sel: *const filter_selection.SelectionVector,
    ) !void {
        const count = sel.count();
        if (count == 0) return;
        const selected: ?*const filter_selection.SelectionVector = if (count == view.num_rows) null else sel;
        switch (s) {
            .selected => unreachable,
            .integer_count_top => |s0| try s0.batch(view, lookup, offset, selected),
            inline else => |local| try local.batchSelected(view, lookup, offset, selected),
        }
    }

    /// A gathered view goes only to `.selected`, and only when `input_rows` is strictly increasing. Any other tag
    /// declines, so those original indexes cannot be read as `offset + i`. An ungathered `.selected` view uses
    /// `input_base` (the slice offset already included) and ignores `legacy_offset`.
    pub fn emit(
        s: RecordSink,
        view: SelectedBatch,
        lookup: []const ?usize,
        legacy_offset: usize,
        legacy_sel: *const filter_selection.SelectionVector,
    ) !void {
        if (view.input_rows) |rows| {
            if (!strictlyAscending(rows)) return error.RetainedEncodingUnsupported;
            if (rows.len != 0) {
                _ = view.ordinal(0) catch |err| switch (err) {
                    error.Overflow => return error.RetainedEncodingUnsupported,
                    error.BadColumn => return error.BadColumn,
                };
                _ = view.ordinal(rows.len - 1) catch |err| switch (err) {
                    error.Overflow => return error.RetainedEncodingUnsupported,
                    error.BadColumn => return error.BadColumn,
                };
            }
            switch (s) {
                .selected => |arm| return arm.batch(view, lookup),
                else => return error.RetainedEncodingUnsupported,
            }
        }
        switch (s) {
            .selected => |arm| return arm.batch(view, lookup),
            else => return s.batch(view.batch.*, lookup, legacy_offset, legacy_sel),
        }
    }
};

fn strictlyAscending(rows: []const u32) bool {
    if (rows.len < 2) return true;
    var prev = rows[0];
    for (rows[1..]) |row| {
        if (row <= prev) return false;
        prev = row;
    }
    return true;
}


/// What `scanRGForAgg` folds a row group's surviving rows into. The caller owns the state, which persists across
/// row groups of one query.
pub const AggTarget = union(enum) {
    /// One accumulator per call, in call order.
    scalar: struct { calls: []const expr_agg.AggCall, accumulators: []expr_agg.Accumulator },
    /// Rows go to the group their key values select; the table holds `calls.len` accumulators per group.
    grouped: struct {
        calls: []const expr_agg.AggCall,
        keys: []const expr_ast.Expr,
        table: *expr_agg.GroupTable,
        /// `expr_agg.firstPosition` of the row group's row 0.
        pos_base: u64,
        /// Set together: fold through shared partitions instead of leaving every group in `table`.
        part: ?*partagg.PartAgg = null,
        local: ?*partagg.Local = null,
        /// A retained key domain's lanes: a row group whose dictionary the domain translated folds there, by global
        /// id, instead of into `table`.
        dense: ?*agg_dense.Lanes = null,
        records: ?*const RecordSink = null,
    },

    pub fn exactInputs(self: AggTarget) bool {
        if (self != .grouped or self.grouped.records != null) return false;
        // Decimal keys and mixed float consumers retain their existing scaled views. This request concerns only
        // exact arithmetic operands, not their logical types or the decoder used for unsupported expressions.
        for (self.grouped.keys) |key| if (key.typeOf() == .dec) return false;
        for (self.grouped.calls) |call| {
            if (call.where != null or call.func == .count_distinct) return false;
            if (call.arg) |arg| {
                if (arg.typeOf() != .dec or !exactTree(arg)) return false;
            } else if (call.func != .count) return false;
        }
        return true;
    }

    fn exactTree(e: expr_ast.Expr) bool {
        return switch (e) {
            .literal => |l| l == .dec or l == .i64,
            .col_ref => |c| c.expr_type == .dec or c.expr_type == .i64,
            .binop => |b| b.result_type == .dec and exactTree(b.left.*) and exactTree(b.right.*),
            else => false,
        };
    }

    pub fn codedKey(self: AggTarget, ci: usize) bool {
        if (self == .grouped) if (self.grouped.records) |r| {
            if (r.* == .integer_count_top and r.integer_count_top.packing != null)
                for (r.integer_count_top.packing.?.keys) |key| {
                    if (key.col_ref.col_idx == ci) return true;
                };
            // The generic arm keeps dictionary codes for every group key. Materializing them makes bind decline.
            if (r.* == .selected) {
                for (self.grouped.keys) |key| if (key == .col_ref and key.col_ref.col_idx == ci) return true;
                return false;
            }
        };
        // The ordinary multi-aggregate consumer can fold dictionary products too. Keeping the original codes
        // avoids expanding strings only to intern them again; the row fallback materializes only if it needs to.
        if (self == .grouped and (self.grouped.dense != null or self.grouped.records == null)) {
            // Float extrema keep their established representation: histogram order can choose another NaN payload.
            if (self.grouped.dense == null) for (self.grouped.calls) |call| {
                if (call.result == .f64 and (call.func == .min or call.func == .max)) return false;
            };
            for (self.grouped.keys) |key| if (key == .col_ref and key.col_ref.col_idx == ci) return true;
        }
        return false;
    }

    pub fn calls(self: AggTarget) []const expr_agg.AggCall {
        return switch (self) {
            inline else => |t| t.calls,
        };
    }

    pub fn planCalls(self: AggTarget) []const expr_agg.AggCall {
        return if (self == .grouped and self.grouped.table.countOnly()) &.{} else self.calls();
    }
};

