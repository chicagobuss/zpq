//! A scan worker's per-row-group decode arena, reused across row groups instead of rebuilt.
//!
//! The only code here that depends on `std.heap.ArenaAllocator`'s private layout (Zig 0.17 `State`): the node lists
//! and each node's `end_index`. The two tests below pin the behaviour that layout dependency buys; a stdlib bump that
//! breaks either must be answered here, not by dropping the reuse.

const std = @import("std");
const huge_pages = @import("huge_pages.zig");

/// Worker-owned and not thread-safe. Everything allocated from `allocator()` lives until the next `nextRowGroup`.
pub const RowGroupArena = struct {
    arena: std.heap.ArenaAllocator,
    /// Set by `initOn`: a region of the arena's own that it allocates from directly, rewound at each boundary.
    region: ?*huge_pages.Region = null,

    pub fn init(child: std.mem.Allocator) RowGroupArena {
        return .{ .arena = .init(child) };
    }

    /// The arena as `region` itself, which nothing else may allocate from. A region charged to a ledger charges what
    /// its blocks span, and an `ArenaAllocator`'s nodes span more than is written: a new node is half again past the
    /// last, and growing one in place leaves the request that did not fit as a gap. On the region, the blocks are
    /// what was asked for, the newest grows in place, and the boundary is a rewind that keeps the pages.
    pub fn initOn(region: *huge_pages.Region) RowGroupArena {
        return .{ .arena = .init(std.heap.page_allocator), .region = region };
    }

    pub fn deinit(self: *RowGroupArena) void {
        if (self.region) |r| return r.rewind();
        self.arena.deinit();
    }

    pub fn allocator(self: *RowGroupArena) std.mem.Allocator {
        if (self.region) |r| return r.allocator();
        return self.arena.allocator();
    }

    /// Row-group boundary: make every byte reusable while keeping every node. `reset(.retain_capacity)` instead
    /// merges the nodes into one freshly allocated block, so the second row group a worker decodes lands on new pages
    /// (and the old ones are unmapped mid-scan) before reuse starts at the third. The used nodes go onto the arena's
    /// own free list, which `alloc` already searches before asking the child allocator for more.
    ///
    /// After the boundary, memory from the previous row group may still be freed or resized (a deferred cleanup that
    /// runs late is safe), but its contents are garbage from the first new allocation on.
    pub fn nextRowGroup(self: *RowGroupArena) void {
        // On a region a block from before is never its newest after the rewind: freeing it does nothing, resizing it
        // only shrinks (as below for the nodes).
        if (self.region) |r| return r.rewind();
        const state = &self.arena.state;
        // The used list is newest-first. Reversing it hands the nodes back oldest-first, so the next row group fills
        // them in the order this one did and touches the same pages, not a scatter across every node's tail.
        var reversed = state.free_list;
        var it = state.used_list;
        while (it) |node| {
            it = node.next;
            node.end_index = 0;
            node.next = reversed;
            reversed = node;
        }
        // Keep the oldest node as the arena's (empty) current node instead of leaving no current node. ArenaAllocator's
        // free and resize unwrap the current node unconditionally, so with none, freeing or resizing pre-boundary
        // memory before the next allocation unwraps null: a panic in safe builds, UB in ReleaseFast. Against an empty
        // current node they are the ordinary not-most-recent no-ops. Fill order is unchanged: this is the node the
        // free-list search would have handed the next allocation first.
        const first = reversed orelse {
            state.free_list = null;
            state.used_list = null;
            return;
        };
        state.free_list = first.next;
        first.end_index = 0;
        first.next = null;
        state.used_list = first;
    }
};

test "a row-group boundary reuses nodes: a repeat of the same allocation pattern never reaches the child allocator" {
    const Counting = struct {
        child: std.mem.Allocator,
        allocs: usize = 0,
        frees: usize = 0,

        fn allocator(self: *@This()) std.mem.Allocator {
            return .{ .ptr = self, .vtable = &.{
                .alloc = alloc,
                .resize = std.mem.Allocator.noResize,
                .remap = std.mem.Allocator.noRemap,
                .free = free,
            } };
        }
        fn alloc(ctx: *anyopaque, n: usize, a: std.mem.Alignment, ra: usize) ?[*]u8 {
            const self: *@This() = @ptrCast(@alignCast(ctx));
            self.allocs += 1;
            return self.child.rawAlloc(n, a, ra);
        }
        fn free(ctx: *anyopaque, mem: []u8, a: std.mem.Alignment, ra: usize) void {
            const self: *@This() = @ptrCast(@alignCast(ctx));
            self.frees += 1;
            self.child.rawFree(mem, a, ra);
        }
    };
    var counting: Counting = .{ .child = std.testing.allocator };
    var arena = RowGroupArena.init(counting.allocator());
    defer arena.deinit();

    // Mixed small and large requests force several nodes, like a row group's metadata plus column buffers.
    const sizes = [_]usize{ 64, 300_000, 17, 1_200_000, 4096, 800_000, 3 };
    for (0..3) |round| {
        arena.nextRowGroup();
        for (sizes) |n| {
            const buf = try arena.allocator().alloc(u8, n);
            @memset(buf, @intCast(round));
        }
        if (round == 0) {
            try std.testing.expect(counting.allocs > 1);
            counting.allocs = 0;
        }
    }
    try std.testing.expectEqual(@as(usize, 0), counting.allocs);
    try std.testing.expectEqual(@as(usize, 0), counting.frees);
}

test "after a row-group boundary, freeing and resizing memory from before it is tolerated" {
    // Code that releases a previous row group's buffers after the boundary, before allocating anything new, must not
    // reach a null current node inside ArenaAllocator.
    var arena = RowGroupArena.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    const small = try a.alloc(u8, 100);
    const large = try a.alloc(u8, 300_000); // past the first node: the boundary has more than one node to hand back
    arena.nextRowGroup();

    a.free(large);
    try std.testing.expect(!a.resize(small, 200));
    try std.testing.expect(a.resize(small, 50));
    a.free(small);

    // Still a working arena that reuses its nodes: both allocations land in memory it already owned.
    const before = arena.arena.queryCapacity();
    const again_small = try a.alloc(u8, 100);
    const again_large = try a.alloc(u8, 300_000);
    @memset(again_small, 1);
    @memset(again_large, 2);
    try std.testing.expectEqual(before, arena.arena.queryCapacity());
    try std.testing.expectEqual(@as(u8, 1), again_small[99]);

    // A boundary on an arena that never allocated leaves it empty and usable.
    var empty = RowGroupArena.init(std.testing.allocator);
    defer empty.deinit();
    empty.nextRowGroup();
    try std.testing.expect(empty.arena.state.used_list == null);
    _ = try empty.allocator().alloc(u8, 8);
}

test "on a region of its own, the arena is charged for what it hands out, not for nodes reserved past it" {
    const huge = huge_pages.huge_page_size;
    var budget: @import("memory.zig").SharedBudget = .{ .limit = 64 * huge };
    var region = huge_pages.Region.init(std.testing.allocator, 256 * huge);
    defer region.deinit();
    if (region.buf.len == 0) return error.SkipZigTest;
    region.budget = &budget;
    var rga = RowGroupArena.initOn(&region);
    defer rga.deinit();
    const a = rga.allocator();
    // An `ArenaAllocator` here reserved nodes half again past these and left gaps where it grew one in place.
    var asked: usize = 0;
    for (0..10) |i| {
        const n = (i + 1) * huge / 2;
        @memset(try a.alloc(u8, n), 1);
        asked += n;
    }
    try std.testing.expectEqual(std.mem.alignForward(usize, asked, huge), region.charged);
    // Refused past the budget, holding nothing more; what fits is still served.
    const past = a.alloc(u8, 48 * huge);
    if (past) |_| return error.TestUnexpectedResult else |e| try std.testing.expectEqual(error.OutOfMemory, e);
    try std.testing.expectEqual(std.mem.alignForward(usize, asked, huge), region.charged);
    region.refused = false;
    // The boundary reuses the pages: a repeat charges nothing more.
    const first = try a.alloc(u8, huge);
    rga.nextRowGroup();
    // A block from before the boundary can be freed or shrunk, not grown, before the next allocation.
    try std.testing.expect(!a.resize(first, 2 * huge));
    try std.testing.expect(a.resize(first, huge / 2));
    a.free(first);
    const charged = region.charged;
    for (0..10) |i| @memset(try a.alloc(u8, (i + 1) * huge / 2), 2);
    try std.testing.expectEqual(charged, region.charged);
}
