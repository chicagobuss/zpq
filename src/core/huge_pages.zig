//! Transparent-huge-page advice for large, long-lived scratch blocks.

const std = @import("std");
const builtin = @import("builtin");
const core_memory = @import("memory.zig");

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
///
/// With a `budget`, what the region may hold resident is drawn from it before it is handed out: the span blocks have
/// reached, in whole huge pages (what THP faults in), and each block spilled to `child`. A draw the budget refuses
/// fails the allocation and sets `refused`. Pages stay charged until `trim` or `deinit` gives them back, as they stay
/// resident until then.
pub const Region = struct {
    child: std.mem.Allocator,
    /// The whole mapping, for `munmap`; empty when the reservation failed.
    mapping: []align(std.heap.page_size_min) u8 = &.{},
    /// The usable span inside `mapping`, 2 MiB-aligned but for `phase`.
    buf: []u8 = &.{},
    /// Where `buf` starts in its first huge page: zero but for the arena of `initPair`, which starts past the
    /// scratch in the huge page the two share. Charges count whole huge pages from that page's start.
    phase: usize = 0,
    end: usize = 0,
    /// Offset of the newest block, the only one that can grow, shrink or be given back.
    last: usize = 0,
    /// How far blocks have reached since the last `trim`: pages past `end` up to here may still be resident.
    touched: usize = 0,
    budget: ?*core_memory.SharedBudget = null,
    /// Drawn from `budget` for `buf`: whole huge pages from its start, never less than `touched` covers.
    charged: usize = 0,
    /// Drawn for blocks spilled to `child`, given back as each is freed.
    spilled: usize = 0,
    /// What the region holds before it draws anything: a worker's first huge page, which the ceiling counts in its
    /// per-worker slack (`memory.zig`) since no worker makes progress on less.
    allowance: usize = 0,
    refused: bool = false,

    pub fn init(child: std.mem.Allocator, reserve: usize) Region {
        const r = map(child, reserve);
        advise(r.buf);
        return r;
    }

    pub const Pair = struct { scratch: Region, arena: Region };

    /// The scratch's span in a `Pair`: a scan's page buffers are mostly a few hundred KiB.
    pub const pair_scratch_span: usize = 1 << 20;

    /// A scan worker's decode arena and its page scratch, which lives across the arena's rewinds, on one mapping
    /// that `arena` owns: the scratch takes the first `pair_scratch_span` of its first huge page and the arena the
    /// rest, so a small scan faults in and zeroes one huge page per worker, not one each. That first page is the
    /// worker's slack, the allowance of both, counted once: the scratch never draws for its span and the arena draws
    /// from the next page on. A scratch block past its span spills straight to the OS (`page_allocator`), charged by
    /// its length: it grows in place by remapping rather than copying beside itself, and is resident only until
    /// freed, which a general heap's per-thread caches do not promise. Deinit both together, the scratch first.
    pub fn initPair(child: std.mem.Allocator, reserve: usize) Pair {
        var arena = map(child, reserve + pair_scratch_span);
        var scratch: Region = .{ .child = std.heap.page_allocator, .allowance = huge_page_size };
        arena.allowance = huge_page_size;
        if (arena.buf.len == 0) return .{ .scratch = scratch, .arena = arena };
        advise(arena.buf);
        scratch.buf = arena.buf[0..pair_scratch_span];
        arena.buf = arena.buf[pair_scratch_span..];
        arena.phase = pair_scratch_span;
        return .{ .scratch = scratch, .arena = arena };
    }

    fn map(child: std.mem.Allocator, reserve: usize) Region {
        var r: Region = .{ .child = child };
        if (builtin.os.tag != .linux and !builtin.os.tag.isDarwin()) return r;
        const len = std.mem.alignForward(usize, reserve, huge_page_size) + huge_page_size;
        var flags: std.posix.MAP = .{ .TYPE = .PRIVATE, .ANONYMOUS = true };
        if (@hasField(std.posix.MAP, "NORESERVE")) flags.NORESERVE = true;
        const m = std.posix.mmap(null, len, .{ .READ = true, .WRITE = true }, flags, -1, 0) catch return r;
        const lo = std.mem.alignForward(usize, @intFromPtr(m.ptr), huge_page_size);
        r.mapping = m;
        r.buf = @as([*]u8, @ptrFromInt(lo))[0 .. len - huge_page_size];
        return r;
    }

    pub fn deinit(self: *Region) void {
        if (self.mapping.len > 0) std.posix.munmap(self.mapping);
        if (self.budget) |b| b.release(self.charged);
        self.* = .{ .child = self.child, .budget = self.budget, .spilled = self.spilled };
    }

    /// Make sure `bytes` of the region are charged, drawing the rest now: the reservation a row group's decode makes
    /// before it allocates. False, holding nothing more, when the budget refuses it. Pages an earlier row group left
    /// charged count, as its arena reuses them.
    pub fn admit(self: *Region, bytes: usize) bool {
        if (self.budget == null or self.buf.len == 0) return true;
        return self.chargeTo(bytes);
    }

    /// Whether a row group's whole decode, by an `estimate` kept on the high side, fits what the region holds and
    /// the budget has free now: checked, not drawn. Drawn, every worker held its overestimate at once and queries
    /// that fit refused. Its blocks draw as they are placed; one refused there slices the row group.
    pub fn fitsRowGroup(self: *const Region, estimate: usize) bool {
        const b = self.budget orelse return true;
        if (self.buf.len == 0) return true;
        const held = self.charged + self.allowance;
        const need = self.phase +| estimate;
        return need <= held or need - held <= b.available();
    }

    /// Give back what `admit` reserved past the pages blocks have reached: at a row-group boundary, where the next
    /// row group's reservation replaces the last one's.
    pub fn settle(self: *Region) void {
        const b = self.budget orelse return;
        const keep = self.hugePagesTo(self.touched) -| self.allowance;
        if (self.charged <= keep) return;
        b.release(self.charged - keep);
        self.charged = keep;
    }

    fn chargeTo(self: *Region, end: usize) bool {
        const b = self.budget orelse return true;
        const want = self.hugePagesTo(end) -| self.allowance;
        if (want <= self.charged) return true;
        if (b.drawExact(want - self.charged) == null) {
            self.refused = true;
            return false;
        }
        self.charged = want;
        return true;
    }

    /// The whole huge pages `buf[0..offset]` lies on (capped at `buf`), in bytes from the start of its first one.
    fn hugePagesTo(self: *const Region, offset: usize) usize {
        return std.mem.alignForward(usize, self.phase + @min(offset, self.buf.len), huge_page_size);
    }

    fn chargeSpill(self: *Region, n: usize) bool {
        const b = self.budget orelse return true;
        if (b.drawExact(n) == null) {
            self.refused = true;
            return false;
        }
        self.spilled += n;
        return true;
    }

    fn releaseSpill(self: *Region, n: usize) void {
        const b = self.budget orelse return;
        b.release(n);
        self.spilled -= n;
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
        if (self.budget) |b| {
            const keep = @min(self.charged, self.hugePagesTo(self.end) -| self.allowance);
            b.release(self.charged - keep);
            self.charged = keep;
        }
    }

    pub fn allocator(self: *Region) std.mem.Allocator {
        return .{ .ptr = self, .vtable = &.{
            .alloc = regionAlloc,
            .resize = regionResize,
            .remap = regionRemap,
            .free = regionFree,
        } };
    }

    /// Whether `len` bytes at `alignment` fit the reserved span past the newest block: placed there, not spilled.
    pub fn fitsSpan(self: *const Region, len: usize, alignment: std.mem.Alignment) bool {
        if (self.buf.len == 0) return false;
        const base = @intFromPtr(self.buf.ptr);
        const start = alignment.forward(base + self.end) - base;
        return start <= self.buf.len and len <= self.buf.len - start;
    }

    pub fn owns(self: *const Region, memory: []u8) bool {
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
            if (!self.chargeTo(start + len)) return null;
            self.last = start;
            self.end = start + len;
            self.touched = @max(self.touched, self.end);
            return self.buf.ptr + start;
        }
        if (!self.chargeSpill(len)) return null;
        return self.child.rawAlloc(len, alignment, ret_addr) orelse {
            self.releaseSpill(len);
            return null;
        };
    }

    fn regionResize(ctx: *anyopaque, memory: []u8, alignment: std.mem.Alignment, new_len: usize, ret_addr: usize) bool {
        const self: *Region = @ptrCast(@alignCast(ctx));
        if (!self.owns(memory)) {
            if (new_len > memory.len and !self.chargeSpill(new_len - memory.len)) return false;
            if (!self.child.rawResize(memory, alignment, new_len, ret_addr)) {
                if (new_len > memory.len) self.releaseSpill(new_len - memory.len);
                return false;
            }
            if (new_len < memory.len) self.releaseSpill(memory.len - new_len);
            return true;
        }
        if (self.isLast(memory)) {
            if (new_len > self.buf.len - self.last) return false;
            if (!self.chargeTo(self.last + new_len)) return false;
            self.end = self.last + new_len;
            self.touched = @max(self.touched, self.end);
            return true;
        }
        return new_len <= memory.len;
    }

    fn regionRemap(ctx: *anyopaque, memory: []u8, alignment: std.mem.Alignment, new_len: usize, ret_addr: usize) ?[*]u8 {
        const self: *Region = @ptrCast(@alignCast(ctx));
        if (!self.owns(memory)) {
            if (new_len > memory.len and !self.chargeSpill(new_len - memory.len)) return null;
            const p = self.child.rawRemap(memory, alignment, new_len, ret_addr) orelse {
                if (new_len > memory.len) self.releaseSpill(new_len - memory.len);
                return null;
            };
            if (new_len < memory.len) self.releaseSpill(memory.len - new_len);
            return p;
        }
        return if (regionResize(ctx, memory, alignment, new_len, ret_addr)) memory.ptr else null;
    }

    fn regionFree(ctx: *anyopaque, memory: []u8, alignment: std.mem.Alignment, ret_addr: usize) void {
        const self: *Region = @ptrCast(@alignCast(ctx));
        if (!self.owns(memory)) {
            self.child.rawFree(memory, alignment, ret_addr);
            self.releaseSpill(memory.len);
            return;
        }
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

test "Region with a budget charges whole huge pages as blocks reach them, refuses past it, and gives them back" {
    var budget: core_memory.SharedBudget = .{ .limit = 3 * huge_page_size };
    var region = Region.init(std.testing.allocator, 8 * huge_page_size);
    region.budget = &budget;
    if (region.buf.len == 0) {
        region.deinit();
        return error.SkipZigTest;
    }
    const a = region.allocator();
    const x = try a.alloc(u8, 100);
    try std.testing.expectEqual(huge_page_size, budget.used.load(.monotonic));
    // The newest block grows in place across a huge-page boundary only as far as the budget lets it.
    var y = try a.alloc(u8, huge_page_size);
    try std.testing.expectEqual(2 * huge_page_size, budget.used.load(.monotonic));
    try std.testing.expect(!a.resize(y, 4 * huge_page_size));
    try std.testing.expect(region.refused);
    try std.testing.expectEqual(2 * huge_page_size, budget.used.load(.monotonic));
    try std.testing.expect(a.resize(y, huge_page_size + 4096));
    y = y.ptr[0 .. huge_page_size + 4096];
    try std.testing.expectError(error.OutOfMemory, a.alloc(u8, 2 * huge_page_size));
    // `admit` reserves ahead, and pages already charged count toward it.
    region.refused = false;
    try std.testing.expect(region.admit(2 * huge_page_size));
    try std.testing.expect(region.admit(3 * huge_page_size));
    try std.testing.expectEqual(3 * huge_page_size, budget.used.load(.monotonic));
    try std.testing.expect(!region.admit(4 * huge_page_size));
    // `settle` gives back what the reservation held past the pages blocks reached.
    region.settle();
    try std.testing.expectEqual(2 * huge_page_size, budget.used.load(.monotonic));
    try std.testing.expect(region.admit(3 * huge_page_size));
    // A rewind keeps the pages and their charge; a trim gives back what lies past the newest block.
    a.free(y);
    _ = x;
    region.rewind();
    try std.testing.expectEqual(3 * huge_page_size, budget.used.load(.monotonic));
    region.trim();
    try std.testing.expectEqual(@as(usize, 0), budget.used.load(.monotonic));
    // A block past the reserve spills to the child, charged by its length.
    budget.limit = 16 * huge_page_size;
    const big = try a.alloc(u8, 9 * huge_page_size);
    try std.testing.expectEqual(9 * huge_page_size, budget.used.load(.monotonic));
    a.free(big);
    try std.testing.expectEqual(@as(usize, 0), budget.used.load(.monotonic));
    _ = try a.alloc(u8, 10);
    region.deinit();
    try std.testing.expectEqual(@as(usize, 0), budget.used.load(.monotonic));
}

test "two regions' row-group fit checks can both pass; their draws still hold the shared limit" {
    var budget: core_memory.SharedBudget = .{ .limit = 2 * huge_page_size };
    var a = Region.init(std.testing.allocator, 8 * huge_page_size);
    defer a.deinit();
    var b = Region.init(std.testing.allocator, 8 * huge_page_size);
    defer b.deinit();
    if (a.buf.len == 0 or b.buf.len == 0) return error.SkipZigTest;
    a.budget = &budget;
    b.budget = &budget;
    try std.testing.expect(a.fitsRowGroup(2 * huge_page_size));
    try std.testing.expect(b.fitsRowGroup(2 * huge_page_size));
    @memset(try a.allocator().alloc(u8, 2 * huge_page_size), 1);
    try std.testing.expectError(error.OutOfMemory, b.allocator().alloc(u8, huge_page_size));
    try std.testing.expect(b.refused);
    try std.testing.expectEqual(budget.limit, budget.used.load(.monotonic));
    try std.testing.expectEqual(@as(usize, 0), b.end);
}

/// The number on `key`'s line for the mapping holding `addr`, as /proc/self/smaps reports it.
fn smapsFieldForTest(addr: usize, key: []const u8) !usize {
    const linux = std.os.linux;
    const rc = linux.open("/proc/self/smaps", .{ .ACCMODE = .RDONLY, .CLOEXEC = true }, 0);
    if (linux.errno(rc) != .SUCCESS) return error.SkipZigTest;
    const fd: i32 = @intCast(rc);
    defer _ = linux.close(fd);
    const buf = try std.testing.allocator.alloc(u8, 8 << 20);
    defer std.testing.allocator.free(buf);
    var len: usize = 0;
    while (true) {
        const nr = linux.read(fd, buf[len..].ptr, buf.len - len);
        if (linux.errno(nr) != .SUCCESS or len + nr == buf.len) return error.ReadFailed;
        if (nr == 0) break;
        len += nr;
    }
    var inside = false;
    var lines = std.mem.splitScalar(u8, buf[0..len], '\n');
    while (lines.next()) |line| {
        const dash = std.mem.indexOfScalar(u8, line, '-');
        const space = std.mem.indexOfScalar(u8, line, ' ');
        if (dash != null and space != null and dash.? < space.?) header: {
            const lo = std.fmt.parseInt(usize, line[0..dash.?], 16) catch break :header;
            const hi = std.fmt.parseInt(usize, line[dash.? + 1 .. space.?], 16) catch break :header;
            inside = lo <= addr and addr < hi;
            continue;
        }
        if (inside and std.mem.startsWith(u8, line, key)) {
            var words = std.mem.tokenizeScalar(u8, line[key.len..], ' ');
            return std.fmt.parseInt(usize, words.next() orelse return error.ReadFailed, 10);
        }
    }
    return error.SkipZigTest;
}

/// The resident bytes of the mapping holding `addr`, as /proc/self/smaps reports them.
fn rssForTest(addr: usize) !usize {
    return (try smapsFieldForTest(addr, "Rss:")) * 1024;
}

test "a worker's pair of regions shares its first huge page, counted once, and holds no more than it draws past it" {
    if (builtin.os.tag != .linux) return error.SkipZigTest;
    const huge = huge_page_size;
    const span = Region.pair_scratch_span;
    var budget: core_memory.SharedBudget = .{ .limit = 32 * huge };
    var pair = Region.initPair(std.testing.allocator, 8 * huge);
    const scratch = &pair.scratch;
    const arena = &pair.arena;
    defer {
        scratch.deinit();
        arena.deinit();
    }
    if (arena.buf.len == 0) return error.SkipZigTest;
    scratch.budget = &budget;
    arena.budget = &budget;
    const base = @intFromPtr(scratch.buf.ptr);
    // The scratch's span, then the arena, in one mapping the arena owns, starting on a huge page.
    try std.testing.expect(std.mem.isAligned(base, huge));
    try std.testing.expectEqual(span, scratch.buf.len);
    try std.testing.expectEqual(base + span, @intFromPtr(arena.buf.ptr));
    try std.testing.expect(arena.buf.len >= 8 * huge);
    try std.testing.expectEqual(@as(usize, 0), scratch.mapping.len);
    // Both on the shared first huge page draw nothing: it is the worker's slack.
    const s = try scratch.allocator().alloc(u8, 300 << 10);
    @memset(s, 1);
    @memset(try arena.allocator().alloc(u8, 200 << 10), 2);
    try std.testing.expectEqual(@as(usize, 0), budget.used.load(.monotonic));
    // The arena draws from the next huge page on, before its block is handed out.
    const a = try arena.allocator().alloc(u8, huge);
    try std.testing.expectEqual(huge, budget.used.load(.monotonic));
    @memset(a, 3);
    try std.testing.expectEqual(@as(u8, 1), s[s.len - 1]);
    // Past its span the scratch spills, charged by length; freed, given back.
    const big = try scratch.allocator().alloc(u8, span);
    try std.testing.expect(!scratch.owns(big) and !arena.owns(big));
    try std.testing.expectEqual(huge + span, budget.used.load(.monotonic));
    scratch.allocator().free(big);
    // Resident: never more than drawn plus the one slack page (whole huge pages where THP backs them).
    try std.testing.expect(try rssForTest(base) <= arena.charged + scratch.charged + huge);
    // A budget that refuses the arena's next page refuses the block; what it holds stays.
    budget.limit = budget.used.load(.monotonic);
    try std.testing.expectError(error.OutOfMemory, arena.allocator().alloc(u8, 2 * huge));
    try std.testing.expect(arena.refused);
    // Rewound and trimmed, the arena gives back all it drew; the scratch keeps its bytes.
    arena.rewind();
    arena.trim();
    try std.testing.expectEqual(@as(usize, 0), budget.used.load(.monotonic));
    try std.testing.expectEqual(@as(u8, 1), s[0]);
    try std.testing.expect(arena.fitsRowGroup(huge - span));
    budget.limit = 0;
    try std.testing.expect(!arena.fitsRowGroup(huge));
}

test "a pair's scratch block that its child refuses past the span holds no charge, and the pair still serves" {
    var failing = std.testing.FailingAllocator.init(std.testing.allocator, .{ .fail_index = 0 });
    var budget: core_memory.SharedBudget = .{ .limit = 32 * huge_page_size };
    var pair = Region.initPair(std.testing.allocator, 4 * huge_page_size);
    pair.scratch.child = failing.allocator();
    defer {
        pair.scratch.deinit();
        pair.arena.deinit();
    }
    if (pair.arena.buf.len == 0) return error.SkipZigTest;
    pair.scratch.budget = &budget;
    pair.arena.budget = &budget;
    try std.testing.expectError(error.OutOfMemory, pair.scratch.allocator().alloc(u8, 2 * Region.pair_scratch_span));
    try std.testing.expectEqual(@as(usize, 0), budget.used.load(.monotonic));
    try std.testing.expectEqual(@as(usize, 0), pair.scratch.spilled);
    @memset(try pair.scratch.allocator().alloc(u8, 4096), 1);
    @memset(try pair.arena.allocator().alloc(u8, 4096), 2);
}
