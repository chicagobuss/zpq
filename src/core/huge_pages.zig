//! Transparent-huge-page advice for large, long-lived scratch blocks.

const std = @import("std");
const builtin = @import("builtin");

pub const huge_page_size: usize = 2 << 20;

/// Forwards to `child`, and asks the kernel to back the 2 MiB-aligned interior of every block of at least 2 MiB
/// with transparent huge pages (MADV_HUGEPAGE). Meant for a scan worker's decode arena and scratch: they are
/// filled sequentially and fully, so one fault per 2 MiB replaces 512, and the TLB covers them with a handful of
/// entries.
///
/// Advice only. A no-op off Linux or where THP is "never"; under "madvise" it is what opts the block in. Pages are
/// still faulted lazily, so resident memory follows what is touched, rounded up to 2 MiB inside advised ranges.
pub const HugePageAdvisor = struct {
    child: std.mem.Allocator,

    pub fn allocator(self: *HugePageAdvisor) std.mem.Allocator {
        return .{ .ptr = self, .vtable = &.{
            .alloc = alloc,
            .resize = resize,
            .remap = remap,
            .free = free,
        } };
    }

    fn alloc(ctx: *anyopaque, len: usize, alignment: std.mem.Alignment, ret_addr: usize) ?[*]u8 {
        const self: *HugePageAdvisor = @ptrCast(@alignCast(ctx));
        const p = self.child.rawAlloc(len, alignment, ret_addr) orelse return null;
        advise(p[0..len]);
        return p;
    }

    fn resize(ctx: *anyopaque, memory: []u8, alignment: std.mem.Alignment, new_len: usize, ret_addr: usize) bool {
        const self: *HugePageAdvisor = @ptrCast(@alignCast(ctx));
        if (!self.child.rawResize(memory, alignment, new_len, ret_addr)) return false;
        if (new_len > memory.len) advise(memory.ptr[0..new_len]);
        return true;
    }

    fn remap(ctx: *anyopaque, memory: []u8, alignment: std.mem.Alignment, new_len: usize, ret_addr: usize) ?[*]u8 {
        const self: *HugePageAdvisor = @ptrCast(@alignCast(ctx));
        const p = self.child.rawRemap(memory, alignment, new_len, ret_addr) orelse return null;
        if (new_len > memory.len) advise(p[0..new_len]);
        return p;
    }

    fn free(ctx: *anyopaque, memory: []u8, alignment: std.mem.Alignment, ret_addr: usize) void {
        const self: *HugePageAdvisor = @ptrCast(@alignCast(ctx));
        self.child.rawFree(memory, alignment, ret_addr);
    }
};

fn advise(block: []u8) void {
    if (builtin.os.tag != .linux) return;
    const lo = std.mem.alignForward(usize, @intFromPtr(block.ptr), huge_page_size);
    const hi = std.mem.alignBackward(usize, @intFromPtr(block.ptr) + block.len, huge_page_size);
    if (hi <= lo) return;
    // Failure (EINVAL without CONFIG_TRANSPARENT_HUGEPAGE, ENOMEM on odd ranges) just leaves 4 KiB pages.
    _ = std.os.linux.madvise(@ptrFromInt(lo), hi - lo, std.os.linux.MADV.HUGEPAGE);
}

test "HugePageAdvisor round-trips allocations of every size class through its child" {
    var advisor: HugePageAdvisor = .{ .child = std.testing.allocator };
    const a = advisor.allocator();
    for ([_]usize{ 1, 4096, huge_page_size - 1, huge_page_size, 3 * huge_page_size + 17 }) |n| {
        const buf = try a.alloc(u8, n);
        @memset(buf, 0x5a);
        try std.testing.expectEqual(@as(u8, 0x5a), buf[n - 1]);
        a.free(buf);
    }
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    _ = try arena.allocator().alloc(u64, huge_page_size);
}
