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

/// A worker's private bump region: one reserved, 2 MiB-aligned span of address space that blocks are carved from
/// front to back, advised for transparent huge pages, and released with a single `munmap`. Meant as the child of a
/// scan worker's decode arena and scratch, which allocate a row group's buffers and grow them in place:
///   - a fresh process faults a worker's few MiB in 2 MiB pages instead of 4 KiB ones (hundreds of faults become a
///     handful, and the TLB covers them with a few entries);
///   - the newest block grows and shrinks in place, so an arena node keeps growing instead of chaining new nodes;
///   - teardown is one `munmap` of a few huge-page mappings, not one per block over thousands of 4 KiB entries,
///     which with every scan thread live costs a TLB shootdown on each.
/// Address space is reserved, not committed (MAP_NORESERVE): only touched pages count. A request past the reserve,
/// or a reservation the system refuses, goes to `child`. Not thread-safe: one per worker.
pub const Region = struct {
    child: std.mem.Allocator,
    /// The whole mapping, for `munmap`; empty when the reservation failed.
    mapping: []align(std.heap.page_size_min) u8 = &.{},
    /// The usable, 2 MiB-aligned span inside `mapping`.
    buf: []u8 = &.{},
    end: usize = 0,
    /// Offset of the newest block, the only one that can grow, shrink or be given back.
    last: usize = 0,
    /// How far blocks have reached since the last `trim`: pages past `end` up to here may still be resident.
    touched: usize = 0,

    pub fn init(child: std.mem.Allocator, reserve: usize) Region {
        var r: Region = .{ .child = child };
        if (builtin.os.tag != .linux and !builtin.os.tag.isDarwin()) return r;
        const len = std.mem.alignForward(usize, reserve, huge_page_size) + huge_page_size;
        var flags: std.posix.MAP = .{ .TYPE = .PRIVATE, .ANONYMOUS = true };
        if (@hasField(std.posix.MAP, "NORESERVE")) flags.NORESERVE = true;
        const m = std.posix.mmap(null, len, .{ .READ = true, .WRITE = true }, flags, -1, 0) catch return r;
        const lo = std.mem.alignForward(usize, @intFromPtr(m.ptr), huge_page_size);
        r.mapping = m;
        r.buf = @as([*]u8, @ptrFromInt(lo))[0 .. len - huge_page_size];
        advise(r.buf);
        return r;
    }

    pub fn deinit(self: *Region) void {
        if (self.mapping.len > 0) std.posix.munmap(self.mapping);
        self.* = .{ .child = self.child };
    }

    /// Make the whole region free again for the next phase of work, keeping its pages. Everything carved from it
    /// must be dead (an arena over it deinitialised); blocks spilled to `child` are not affected.
    pub fn rewind(self: *Region) void {
        self.end = 0;
        self.last = 0;
    }

    /// Give back the pages past the newest block: what an earlier phase faulted in and no live block holds, for work
    /// that is about to allocate elsewhere rather than reuse them. They read back as zeros.
    pub fn trim(self: *Region) void {
        const page = std.heap.pageSize();
        const lo = std.mem.alignForward(usize, self.end, page);
        if (self.touched > lo) {
            const hi = @min(std.mem.alignForward(usize, self.touched, page), self.buf.len);
            release(@alignCast(self.buf[lo..hi]));
        }
        self.touched = self.end;
    }

    pub fn allocator(self: *Region) std.mem.Allocator {
        return .{ .ptr = self, .vtable = &.{
            .alloc = regionAlloc,
            .resize = regionResize,
            .remap = regionRemap,
            .free = regionFree,
        } };
    }

    fn owns(self: *const Region, memory: []u8) bool {
        const p = @intFromPtr(memory.ptr);
        const base = @intFromPtr(self.buf.ptr);
        return p >= base and p < base + self.buf.len;
    }

    fn isLast(self: *const Region, memory: []u8) bool {
        return @intFromPtr(memory.ptr) == @intFromPtr(self.buf.ptr) + self.last and
            self.last + memory.len == self.end;
    }

    fn regionAlloc(ctx: *anyopaque, len: usize, alignment: std.mem.Alignment, ret_addr: usize) ?[*]u8 {
        const self: *Region = @ptrCast(@alignCast(ctx));
        const base = @intFromPtr(self.buf.ptr);
        const start = alignment.forward(base + self.end) - base;
        if (self.buf.len > 0 and start <= self.buf.len and len <= self.buf.len - start) {
            self.last = start;
            self.end = start + len;
            self.touched = @max(self.touched, self.end);
            return self.buf.ptr + start;
        }
        return self.child.rawAlloc(len, alignment, ret_addr);
    }

    fn regionResize(ctx: *anyopaque, memory: []u8, alignment: std.mem.Alignment, new_len: usize, ret_addr: usize) bool {
        const self: *Region = @ptrCast(@alignCast(ctx));
        if (!self.owns(memory)) return self.child.rawResize(memory, alignment, new_len, ret_addr);
        if (self.isLast(memory)) {
            if (new_len > self.buf.len - self.last) return false;
            self.end = self.last + new_len;
            self.touched = @max(self.touched, self.end);
            return true;
        }
        return new_len <= memory.len;
    }

    fn regionRemap(ctx: *anyopaque, memory: []u8, alignment: std.mem.Alignment, new_len: usize, ret_addr: usize) ?[*]u8 {
        const self: *Region = @ptrCast(@alignCast(ctx));
        if (!self.owns(memory)) return self.child.rawRemap(memory, alignment, new_len, ret_addr);
        return if (regionResize(ctx, memory, alignment, new_len, ret_addr)) memory.ptr else null;
    }

    fn regionFree(ctx: *anyopaque, memory: []u8, alignment: std.mem.Alignment, ret_addr: usize) void {
        const self: *Region = @ptrCast(@alignCast(ctx));
        if (!self.owns(memory)) return self.child.rawFree(memory, alignment, ret_addr);
        if (self.isLast(memory)) self.end = self.last;
    }
};

/// Drop `span`'s pages now. Linux keeps the mapping and its huge-page advice; Darwin's MADV_DONTNEED may keep the
/// pages resident, so the span is mapped afresh in place (a failed remap leaves them until `deinit`).
fn release(span: []align(std.heap.page_size_min) u8) void {
    if (builtin.os.tag == .linux) {
        _ = std.os.linux.madvise(span.ptr, span.len, std.os.linux.MADV.DONTNEED);
    } else if (builtin.os.tag.isDarwin()) {
        var flags: std.posix.MAP = .{ .TYPE = .PRIVATE, .ANONYMOUS = true, .FIXED = true };
        if (@hasField(std.posix.MAP, "NORESERVE")) flags.NORESERVE = true;
        _ = std.posix.mmap(span.ptr, span.len, .{ .READ = true, .WRITE = true }, flags, -1, 0) catch {};
    }
}

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

test "Region carves blocks in order, grows the newest in place, and spills past its reserve to the child" {
    var region = Region.init(std.testing.allocator, 4 * huge_page_size);
    defer region.deinit();
    const a = region.allocator();
    if (region.buf.len == 0) return error.SkipZigTest;
    try std.testing.expect(std.mem.isAligned(@intFromPtr(region.buf.ptr), huge_page_size));

    const x = try a.alloc(u8, 100);
    const y = try a.alignedAlloc(u64, .@"64", 10);
    try std.testing.expect(std.mem.isAligned(@intFromPtr(y.ptr), 64));
    try std.testing.expect(@intFromPtr(y.ptr) > @intFromPtr(x.ptr));
    // Only the newest block grows in place; an older one can only shrink.
    try std.testing.expect(a.resize(y, 1000));
    try std.testing.expect(!a.resize(x, 200));
    try std.testing.expect(a.resize(x, 50));
    // Freeing the newest block hands its space back to the next allocation.
    const y_ptr = @intFromPtr(y.ptr);
    a.free(y.ptr[0..1000]);
    const z = try a.alignedAlloc(u64, .@"64", 4);
    try std.testing.expectEqual(y_ptr, @intFromPtr(z.ptr));

    // Past the reserve: the child (testing.allocator would report a leak if it were not freed through the region).
    const big = try a.alloc(u8, 5 * huge_page_size);
    try std.testing.expect(!region.owns(big));
    big[big.len - 1] = 1;
    a.free(big);

    // A rewind makes the region's pages reusable from the start.
    region.rewind();
    const again = try a.alloc(u8, 10);
    try std.testing.expectEqual(@intFromPtr(region.buf.ptr), @intFromPtr(again.ptr));

    // An arena over it grows one node in place rather than chaining new ones.
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    for (0..64) |_| _ = try arena.allocator().alloc(u8, 32 * 1024);
    try std.testing.expect(region.end > 64 * 32 * 1024);
}

test "Region.trim hands back the pages past the newest block that an earlier phase touched" {
    var region = Region.init(std.testing.allocator, 4 * huge_page_size);
    defer region.deinit();
    if (region.buf.len == 0) return error.SkipZigTest;
    const a = region.allocator();
    const big = try a.alloc(u8, 3 * huge_page_size);
    @memset(big, 0xa5);
    region.rewind();
    const small = try a.alloc(u8, 100);
    @memset(small, 1);
    try std.testing.expectEqual(3 * huge_page_size, region.touched);
    region.trim();
    try std.testing.expectEqual(region.end, region.touched);
    const page = std.heap.pageSize();
    // Nothing past the newest block is resident any more (Linux reports residency per page through mincore).
    if (builtin.os.tag == .linux) {
        const span = big[huge_page_size..][0..huge_page_size];
        var vec: [huge_page_size / 4096]u8 = undefined;
        const rc = std.os.linux.syscall3(.mincore, @intFromPtr(span.ptr), span.len, @intFromPtr(&vec));
        try std.testing.expectEqual(@as(usize, 0), rc);
        for (vec[0 .. span.len / page]) |v| try std.testing.expectEqual(@as(u8, 0), v & 1);
    }
    // Every byte there reads back as a fresh zero page; the live block keeps its bytes.
    const past = big[std.mem.alignForward(usize, small.len, page)..];
    for ([_]usize{ 0, 12345, huge_page_size, past.len - 1 }) |i| try std.testing.expectEqual(@as(u8, 0), past[i]);
    try std.testing.expectEqual(@as(u8, 1), small[99]);
    // The region still allocates over the trimmed span.
    region.rewind();
    const again = try a.alloc(u8, 2 * huge_page_size);
    @memset(again, 3);
    try std.testing.expectEqual(@as(u8, 3), again[again.len - 1]);
}
