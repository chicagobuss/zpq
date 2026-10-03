//! A scan worker's per-row-group decode arena, reused across row groups instead of rebuilt.
//!
//! The only code here that depends on `std.heap.ArenaAllocator`'s private layout (Zig 0.17 `State`): the node lists
//! and each node's `end_index`. The two tests below pin the behaviour that layout dependency buys; a stdlib bump that
//! breaks either must be answered here, not by dropping the reuse.

const std = @import("std");

/// Worker-owned and not thread-safe. Everything allocated from `allocator()` lives until the next `nextRowGroup`.
pub const RowGroupArena = struct {
    arena: std.heap.ArenaAllocator,

    pub fn init(child: std.mem.Allocator) RowGroupArena {
        return .{ .arena = .init(child) };
    }

    pub fn deinit(self: *RowGroupArena) void {
        self.arena.deinit();
    }

    pub fn allocator(self: *RowGroupArena) std.mem.Allocator {
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
