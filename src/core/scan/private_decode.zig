const std = @import("std");
const testing = std.testing;
const huge_pages = @import("../huge_pages.zig");
const core_memory = @import("../memory.zig");
const partagg = @import("../partagg.zig");
const expr_agg = @import("../expr/agg.zig");

/// Fund a decoder floor before workers start. Whole row groups may borrow headroom; slices retain protected room
/// when that admission refuses. Cancelling the private sample returns the floors beside still-charged live values.
pub const PrivateDecode = struct {
    held: usize = 0,
    workers: []core_memory.SharedBudget = &.{},
    samples: []core_memory.SharedBudget = &.{},

    pub fn init(
        a: std.mem.Allocator,
        parent: *core_memory.SharedBudget,
        n: usize,
        limit: ?usize,
        dictionary_peak: u64,
    ) !PrivateDecode {
        const share = @min(limit orelse parent.limit / 2, parent.limit / 4) / n;
        // A floor consumed by a dictionary strands the sample on tiny slices even when it later cancels.
        // Decline before reserving runs or reading rows; leave room for a slice beside the fixed dictionary.
        if (share < @max(huge_pages.huge_page_size, dictionary_peak +| parent.block)) return .{};
        const held = parent.drawExact(share * n) orelse return .{};
        errdefer parent.release(held);
        const workers = try a.alloc(core_memory.SharedBudget, n);
        for (workers) |*w| w.* = .{ .limit = parent.limit, .parent = parent, .funded = share };
        return .{ .held = held, .workers = workers };
    }

    pub fn fundSamples(self: *PrivateDecode, a: std.mem.Allocator, part: *partagg.PartAgg, width: u64) !bool {
        // Decode loans must not strand every worker before its first flush. Transfer part of the prepaid run
        // ceiling into bounded local-table floors; it is the same charged credit, not another reservation.
        const share = (width +| 64) *| part.cap *| 4 +| part.budget.block;
        const total = share *| self.workers.len;
        const credit = part.run_credit.load(.monotonic);
        if (total > credit) return false;
        const samples = try a.alloc(core_memory.SharedBudget, self.workers.len);
        part.run_credit.store(credit - @as(usize, @intCast(total)), .monotonic);
        for (samples) |*b| b.* = .{ .limit = part.budget.limit, .parent = part.budget, .funded = @intCast(share) };
        self.samples = samples;
        part.sample_floors = samples;
        return true;
    }

    pub fn release(self: *PrivateDecode) void {
        for (self.workers) |*w| {
            std.debug.assert(w.used.load(.monotonic) == 0);
            w.releaseFloor();
        }
        // A failed worker can still hold a local table; cancellation keeps its live bytes charged until deinit.
        for (self.samples) |*b| b.releaseFloor();
        self.held = 0;
        self.workers = &.{};
        self.samples = &.{};
    }
};

test "private decoder floors stay funded and cannot consume another worker's credit" {
    const a = testing.allocator;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    var parent: core_memory.SharedBudget = .{ .limit = 128 << 20 };
    defer std.debug.assert(parent.used.load(.monotonic) == 0);
    var decoders = try PrivateDecode.init(arena.allocator(), &parent, 4, parent.limit / 2, 0);
    defer decoders.release();
    const rest = parent.drawExact(parent.available()).?;
    defer parent.release(rest);
    const first = &decoders.workers[0];
    const full = first.drawExact(first.funded).?;
    defer first.release(full);
    try testing.expect(first.drawExact(1) == null);
    const second = &decoders.workers[1];
    const independent = second.drawExact(second.funded).?;
    defer second.release(independent);
    try testing.expectEqual(parent.limit, parent.used.load(.monotonic));
    try testing.expectEqual(@as(usize, parent.limit / 4), decoders.held);
}

fn privateDecodeAllocationFailure(a: std.mem.Allocator) !void {
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    var parent: core_memory.SharedBudget = .{ .limit = 128 << 20 };
    defer std.debug.assert(parent.used.load(.monotonic) == 0);
    var decoders = try PrivateDecode.init(arena.allocator(), &parent, 4, parent.limit / 2, 0);
    defer decoders.release();
    var charged: core_memory.Charged = .{ .child = a, .budget = &decoders.workers[0] };
    const memory = try charged.allocator().alloc(u8, 4096);
    charged.allocator().free(memory);
}

test "private decoder funding returns after allocation failures or declined admission" {
    try testing.checkAllAllocationFailures(testing.allocator, privateDecodeAllocationFailure, .{});
    var parent: core_memory.SharedBudget = .{ .limit = 128 << 20 };
    const held = parent.drawExact(parent.limit).?;
    defer parent.release(held);
    var refused = try PrivateDecode.init(testing.allocator, &parent, 4, null, 0);
    defer refused.release();
    try testing.expectEqual(@as(usize, 0), refused.held);
    try testing.expectEqual(parent.limit, parent.used.load(.monotonic));
}

fn privateSampleAllocationFailure(a: std.mem.Allocator) !void {
    var parent: core_memory.SharedBudget = .{ .limit = 64 << 20 };
    defer std.debug.assert(parent.used.load(.monotonic) == 0);
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const Merge = struct {
        fn count(dst: *expr_agg.Accumulator, src: *expr_agg.Accumulator, _: std.mem.Allocator) !void {
            dst.count += src.count;
        }
    };
    const calls = [_]expr_agg.AggCall{.{ .func = .count, .arg = null, .where = null, .alias = "c", .result = .i64 }};
    var part = try partagg.PartAgg.init(a, &parent, &calls, 0, Merge.count, 4, 4, 2, true);
    defer part.deinit();
    part.compact = true;
    for (part.parts) |*p| p.table.compact.configure(&calls);
    var funding = try PrivateDecode.init(arena.allocator(), &parent, 2, parent.limit / 2, 0);
    defer funding.release();
    try testing.expect(part.reservePrivate(65536, 128));
    const before = parent.used.load(.monotonic);
    const credit = part.run_credit.load(.monotonic);
    try testing.expect(try funding.fundSamples(arena.allocator(), &part, 128));
    part.decoder_floors = funding.workers;
    try testing.expectEqual(before, parent.used.load(.monotonic));
    try testing.expectEqual(credit, part.run_credit.load(.monotonic) +
        funding.samples[0].funded + funding.samples[1].funded);
    const rest = parent.drawExact(parent.available()).?;
    defer parent.release(rest);
    var table = expr_agg.GroupTable.init(a, 0);
    table.shared = &funding.samples[0];
    table.compact.configure(&calls);
    defer table.deinit();
    for ([_][]const u8{ "alpha", "bravo", "charlie", "delta" }, 0..) |key, i| {
        _ = try table.getOrInsert(key, &calls, i);
    }
    try testing.expect(funding.samples[0].used.load(.monotonic) > 0);
    part.cancelPrivate();
    for (funding.samples) |*b| try testing.expectEqual(@as(usize, 0), b.funded);
    try testing.expect(funding.samples[0].used.load(.monotonic) > 0);
}

test "private decoder loans cannot starve local samples: prepaid tables survive a full ledger and allocation failures" {
    try testing.checkAllAllocationFailures(testing.allocator, privateSampleAllocationFailure, .{});
}

test "private decoder floors decline a dictionary that leaves no sliced sample room before charging" {
    var parent: core_memory.SharedBudget = .{ .limit = 2 << 30 };
    const dictionary_peak = 2 * (80_000_000 + 900_000 * @sizeOf([]const u8));
    var small = try PrivateDecode.init(testing.allocator, &parent, 4, parent.limit / 2, dictionary_peak);
    defer small.release();
    try testing.expectEqual(@as(usize, 0), small.held);
    try testing.expectEqual(@as(usize, 0), parent.used.load(.monotonic));
    parent.limit = 4 << 30;
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    var ample = try PrivateDecode.init(arena.allocator(), &parent, 4, parent.limit / 2, dictionary_peak);
    defer ample.release();
    try testing.expectEqual(@as(usize, parent.limit / 4), ample.held);
}

