const builtin = @import("builtin");
const std = @import("std");
const core_memory = @import("../memory.zig");
const SliceScratch = @import("../parquet/page.zig").SliceScratch;

const AllocatorOwner = struct { ctx: usize, vt: usize };

/// Identity of `alloc`. `page_allocator`, `smp_allocator` and (where linked) `c_allocator` have no context, so the
/// vtable is the whole id. Every other allocator's context is its own pointer.
fn allocatorOwner(alloc: std.mem.Allocator) AllocatorOwner {
    const vt = @intFromPtr(alloc.vtable);
    if (alloc.vtable == std.heap.page_allocator.vtable) return .{ .ctx = 0, .vt = vt };
    if (alloc.vtable == std.heap.smp_allocator.vtable) return .{ .ctx = 0, .vt = vt };
    if (comptime builtin.link_libc) {
        if (alloc.vtable == std.heap.c_allocator.vtable) return .{ .ctx = 0, .vt = vt };
    }
    if (comptime builtin.target.cpu.arch.isWasm()) {
        if (alloc.vtable == std.heap.wasm_allocator.vtable) return .{ .ctx = 0, .vt = vt };
    }
    return .{ .ctx = @intFromPtr(alloc.ptr), .vt = vt };
}

/// The pool on `slot`, charged to `alloc`. A different owner drops the old pool first.
pub fn poolOn(slot: *?*SliceScratch, alloc: std.mem.Allocator) !*SliceScratch {
    const owner = allocatorOwner(alloc);
    if (slot.*) |pool| {
        if (pool.owner_ctx == owner.ctx and pool.owner_vt == owner.vt) return pool;
        releaseSlicePool(slot);
    }
    const pool = try alloc.create(SliceScratch);
    pool.* = .init(alloc);
    pool.owner_ctx = owner.ctx;
    pool.owner_vt = owner.vt;
    slot.* = pool;
    return pool;
}

test "a slice pool follows the allocator of the current job" {
    var slot: ?*SliceScratch = null;
    defer releaseSlicePool(&slot);
    // A real singleton: its context pointer is not an identity.
    const pages = std.heap.page_allocator;
    const page_pool = try poolOn(&slot, pages);
    try std.testing.expect(try poolOn(&slot, pages) == page_pool);

    var budget_a: core_memory.SharedBudget = .{ .limit = 1 << 20 };
    var budget_b: core_memory.SharedBudget = .{ .limit = 1 << 20 };
    var charged_a: core_memory.Charged = .{ .child = std.testing.allocator, .budget = &budget_a };
    var charged_b: core_memory.Charged = .{ .child = std.testing.allocator, .budget = &budget_b };
    const a_alloc = charged_a.allocator();
    const b_alloc = charged_b.allocator();
    const a = try poolOn(&slot, a_alloc);
    try std.testing.expect(a != page_pool);
    try a.reserve(1);
    try std.testing.expect(try poolOn(&slot, a_alloc) == a);
    const b = try poolOn(&slot, b_alloc);
    try std.testing.expect(b != a);
    try std.testing.expect(b.alloc.ptr == b_alloc.ptr);
    try std.testing.expect(b.alloc.vtable == b_alloc.vtable);
    try std.testing.expect(try poolOn(&slot, b_alloc) == b);
}

/// Give back a pool created for a write or a print, including its ledger charge.
pub fn releaseSlicePool(slot: *?*SliceScratch) void {
    const pool = slot.* orelse return;
    const alloc = pool.alloc;
    slot.* = null;
    pool.deinit();
    alloc.destroy(pool);
}

