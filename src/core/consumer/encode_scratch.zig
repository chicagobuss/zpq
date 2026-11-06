const std = @import("std");
const memory = @import("../memory.zig");

/// Page-backed encode arenas return their nodes here between jobs. Live nodes belong to the job's existing
/// reservation; idle nodes draw their actual mapped size before being retained. The cache never forces a draw.
/// Its limit is the write window's byte credit, not a second allowance outside the query's ledger.
pub const EncodeScratch = struct {
    io: std.Io,
    /// Allocation and release can overlap on different workers.
    child: std.mem.Allocator = std.heap.page_allocator,
    ledger: *memory.SharedBudget,
    limit: usize,
    mutex: std.Io.Mutex = .init,
    idle: ?*Node = null,
    retained: usize = 0, // Bytes on the idle list, for observability and bulk retirement.
    /// Idle mappings, retiring mappings, and excess capacity still charged while a job borrows a larger mapping.
    held: usize = 0,
    live: usize = 0,
    paused: bool = false,

    const Node = struct {
        next: ?*Node = null,
        bytes: []u8,
        alignment: std.mem.Alignment,
        data: [*]u8,
        extra: usize = 0,
    };

    pub fn allocator(self: *EncodeScratch) std.mem.Allocator {
        return .{ .ptr = self, .vtable = &.{ .alloc = alloc, .resize = resize, .remap = remap, .free = free } };
    }

    pub fn deinit(self: *EncodeScratch) void {
        std.debug.assert(self.live == 0);
        self.trimIdle();
        std.debug.assert(self.held == 0);
    }

    pub fn trimIdle(self: *EncodeScratch) void {
        _ = self.trimIdleBytes();
    }

    pub fn trimIdleBytes(self: *EncodeScratch) usize {
        self.mutex.lockUncancelable(self.io);
        const retired = self.detachIdle();
        self.mutex.unlock(self.io);
        self.discard(retired);
        return retired.bytes;
    }

    pub fn setPaused(self: *EncodeScratch, paused: bool) void {
        self.mutex.lockUncancelable(self.io);
        self.paused = paused;
        const retired: Retired = if (paused) self.detachIdle() else .{};
        self.mutex.unlock(self.io);
        self.discard(retired);
    }

    const Retired = struct { head: ?*Node = null, bytes: usize = 0 };

    /// Detaching prevents reuse; credit and window capacity stay held until the physical release completes.
    fn detachIdle(self: *EncodeScratch) Retired {
        const retired: Retired = .{ .head = self.idle, .bytes = self.retained };
        self.idle = null;
        self.retained = 0;
        return retired;
    }

    fn discard(self: *EncodeScratch, retired: Retired) void {
        if (retired.head == null) return;
        var cursor = retired.head;
        while (cursor) |node| {
            cursor = node.next;
            const bytes = node.bytes;
            const alignment = node.alignment;
            self.child.rawFree(bytes, alignment, @returnAddress());
        }
        self.ledger.release(retired.bytes);
        self.mutex.lockUncancelable(self.io);
        self.held -= retired.bytes;
        self.mutex.unlock(self.io);
    }

    fn offset(alignment: std.mem.Alignment) usize {
        return std.mem.alignForward(usize, @sizeOf(Node) + @sizeOf(*Node), alignment.toByteUnits());
    }

    fn allocationSize(len: usize, alignment: std.mem.Alignment) ?usize {
        const bytes = std.math.add(usize, offset(alignment), len) catch return null;
        return std.mem.alignBackward(usize, std.math.add(usize, bytes, std.heap.pageSize() - 1) catch return null, std.heap.pageSize());
    }

    fn owner(ptr: [*]u8) *Node {
        const header: **Node = @ptrCast(@alignCast(ptr - @sizeOf(*Node)));
        return header.*;
    }

    fn alloc(ctx: *anyopaque, len: usize, alignment: std.mem.Alignment, ret: usize) ?[*]u8 {
        const self: *EncodeScratch = @ptrCast(@alignCast(ctx));
        const size = allocationSize(len, alignment) orelse return null;
        self.mutex.lockUncancelable(self.io);
        var link = &self.idle;
        var best: ?*?*Node = null;
        while (link.*) |node| {
            if (node.bytes.len >= size and len <= node.bytes.len - (node.data - node.bytes.ptr) and
                std.mem.isAligned(@intFromPtr(node.data), alignment.toByteUnits()))
            {
                if (best == null or node.bytes.len < best.?.*.?.bytes.len) best = link;
                if (node.bytes.len == size) break;
            }
            link = &node.next;
        }
        if (best) |found| {
            const node = found.*.?;
            found.* = node.next;
            self.retained -= node.bytes.len;
            // The job's reservation covers the requested pages. Excess capacity stays charged even while live.
            self.ledger.release(size);
            self.held -= size;
            node.extra = node.bytes.len - size;
            self.live += 1;
            self.mutex.unlock(self.io);
            return node.data;
        }
        const retired: Retired = if (!self.paused and self.ledger.tight()) self.detachIdle() else .{};
        self.mutex.unlock(self.io);
        self.discard(retired);
        const actual_alignment = alignment.max(.of(Node));
        const bytes = (self.child.rawAlloc(size, actual_alignment, ret) orelse return null)[0..size];
        const node: *Node = @ptrCast(@alignCast(bytes.ptr));
        const data = bytes.ptr + offset(alignment);
        node.* = .{ .bytes = bytes, .alignment = actual_alignment, .data = data };
        const header: **Node = @ptrCast(@alignCast(data - @sizeOf(*Node)));
        header.* = node;
        self.mutex.lockUncancelable(self.io);
        self.live += 1;
        self.mutex.unlock(self.io);
        return data;
    }

    fn resize(_: *anyopaque, bytes: []u8, alignment: std.mem.Alignment, len: usize, _: usize) bool {
        // A physical-size change must allocate/free normally so retained bytes and ledger credit stay exact.
        const node = owner(bytes.ptr);
        return allocationSize(len, alignment) == node.bytes.len and
            len <= node.bytes.len - (bytes.ptr - node.bytes.ptr);
    }

    fn remap(ctx: *anyopaque, bytes: []u8, alignment: std.mem.Alignment, len: usize, ret: usize) ?[*]u8 {
        return if (resize(ctx, bytes, alignment, len, ret)) bytes.ptr else null;
    }

    fn free(ctx: *anyopaque, bytes: []u8, _: std.mem.Alignment, ret: usize) void {
        const self: *EncodeScratch = @ptrCast(@alignCast(ctx));
        const node = owner(bytes.ptr);
        self.mutex.lockUncancelable(self.io);
        self.live -= 1;
        const extra = node.extra;
        const added = node.bytes.len - extra;
        if (!self.paused and !self.ledger.tight() and node.bytes.len <= self.limit - (self.held - extra)) {
            if (self.ledger.drawExact(added) != null) {
                node.extra = 0;
                node.next = self.idle;
                self.idle = node;
                self.retained += node.bytes.len;
                self.held += added;
                self.mutex.unlock(self.io);
                return;
            }
        }
        const allocation = node.bytes;
        const alignment = node.alignment;
        self.mutex.unlock(self.io);
        self.child.rawFree(allocation, alignment, ret);
        self.ledger.release(extra);
        self.mutex.lockUncancelable(self.io);
        self.held -= extra;
        self.mutex.unlock(self.io);
    }
};

test "encode scratch releases mappings outside its mutex and keeps retiring capacity charged" {
    const t = std.testing;
    const Child = struct {
        cache: *EncodeScratch = undefined,
        credit: usize = 0,
        failed: bool = false,

        fn check(self: *@This()) void {
            if (!self.cache.mutex.tryLock()) {
                self.failed = true;
                return;
            }
            defer self.cache.mutex.unlock(self.cache.io);
            if (self.cache.ledger.used.load(.monotonic) < self.credit or self.cache.held < self.credit)
                self.failed = true;
        }

        fn alloc(ptr: *anyopaque, n: usize, alignment: std.mem.Alignment, ret: usize) ?[*]u8 {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.check();
            return t.allocator.rawAlloc(n, alignment, ret);
        }

        fn free(ptr: *anyopaque, bytes: []u8, alignment: std.mem.Alignment, ret: usize) void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.check();
            t.allocator.rawFree(bytes, alignment, ret);
        }
    };
    var ledger: memory.SharedBudget = .{ .limit = 1 << 20 };
    var child: Child = .{};
    var cache: EncodeScratch = .{
        .io = t.io,
        .child = .{ .ptr = &child, .vtable = &.{
            .alloc = Child.alloc,
            .free = Child.free,
            .resize = std.mem.Allocator.noResize,
            .remap = std.mem.Allocator.noRemap,
        } },
        .ledger = &ledger,
        .limit = ledger.limit,
    };
    child.cache = &cache;
    defer cache.deinit();
    const a = cache.allocator();
    const idle = try a.alloc(u8, 4 * std.heap.pageSize());
    a.free(idle);
    child.credit = cache.retained;
    try t.expectEqual(child.credit, cache.trimIdleBytes());
    try t.expectEqual(@as(usize, 0), cache.held);
    child.credit = 0;
    const large = try a.alloc(u8, 4 * std.heap.pageSize());
    a.free(large);
    const borrowed = try a.alloc(u8, std.heap.pageSize());
    child.credit = cache.held;
    try t.expect(child.credit > 0);
    cache.limit = 0;
    a.free(borrowed);
    try t.expect(!child.failed);
    try t.expectEqual(@as(usize, 0), cache.held);
    try t.expectEqual(@as(usize, 0), ledger.used.load(.monotonic));
}

test "encode scratch reuses page nodes and charges every retained byte" {
    const t = std.testing;
    var ledger: memory.SharedBudget = .{ .limit = 1 << 20 };
    var cache: EncodeScratch = .{ .io = t.io, .child = t.allocator, .ledger = &ledger, .limit = ledger.limit };
    defer cache.deinit();
    const a = cache.allocator();
    const first = try a.alignedAlloc(u8, .@"64", 8192);
    @memset(first, 17);
    const ptr = first.ptr;
    try t.expectEqual(@as(usize, 0), ledger.used.load(.monotonic));
    a.free(first);
    const charged = ledger.used.load(.monotonic);
    try t.expect(charged >= 8192);
    try t.expectEqual(cache.retained, charged);
    const second = try a.alignedAlloc(u8, .@"64", 8192);
    try t.expectEqual(ptr, second.ptr);
    try t.expectEqual(@as(usize, 0), ledger.used.load(.monotonic));
    @memset(second, 29);
    a.free(second);
    cache.deinit();
    try t.expectEqual(@as(usize, 0), ledger.used.load(.monotonic));
}

test "encode scratch declines retention outside its window share or available ledger" {
    const t = std.testing;
    var ledger: memory.SharedBudget = .{ .limit = 65536 };
    var cache: EncodeScratch = .{ .io = t.io, .child = t.allocator, .ledger = &ledger, .limit = 0 };
    defer cache.deinit();
    const a = cache.allocator();
    const p = try a.alloc(u8, 8192);
    a.free(p);
    try t.expectEqual(@as(usize, 0), cache.retained);
    cache.limit = ledger.limit;
    const q = try a.alloc(u8, 8192);
    ledger.force(ledger.limit);
    a.free(q);
    try t.expectEqual(@as(usize, 0), cache.retained);
    try t.expectEqual(ledger.limit, ledger.used.load(.monotonic));
    ledger.release(ledger.limit);
}

test "encode scratch keeps excess capacity charged while a smaller job borrows it" {
    const t = std.testing;
    var ledger: memory.SharedBudget = .{ .limit = 1 << 20 };
    var cache: EncodeScratch = .{ .io = t.io, .child = t.allocator, .ledger = &ledger, .limit = ledger.limit };
    defer cache.deinit();
    const a = cache.allocator();
    const large = try a.alloc(u8, 32768);
    const ptr = large.ptr;
    a.free(large);
    const backing = cache.retained;
    const small = try a.alloc(u8, 8192);
    try t.expectEqual(ptr, small.ptr);
    try t.expectEqual(backing - EncodeScratch.allocationSize(8192, .@"1").?, ledger.used.load(.monotonic));
    try t.expectEqual(@as(usize, 0), cache.retained);
    @memset(small, 71);
    a.free(small);
    try t.expectEqual(backing, cache.retained);
    try t.expectEqual(backing, ledger.used.load(.monotonic));
    cache.deinit();
    try t.expectEqual(@as(usize, 0), ledger.used.load(.monotonic));
}

test "encode scratch window cap includes excess capacity in live blocks" {
    const t = std.testing;
    const page = std.heap.pageSize();
    var ledger: memory.SharedBudget = .{ .limit = 1 << 20 };
    var cache: EncodeScratch = .{ .io = t.io, .child = t.allocator, .ledger = &ledger, .limit = 5 * page };
    defer cache.deinit();
    const a = cache.allocator();
    const large = try a.alloc(u8, 4 * page);
    a.free(large);
    const small = try a.alloc(u8, page);
    const other = try a.alloc(u8, 3 * page);
    a.free(other);
    try t.expectEqual(@as(usize, 0), cache.retained);
    try t.expectEqual(3 * page, cache.held);
    try t.expectEqual(cache.held, ledger.used.load(.monotonic));
    a.free(small);
    try t.expect(cache.held <= cache.limit);
    try t.expectEqual(cache.held, ledger.used.load(.monotonic));
    cache.deinit();
    try t.expectEqual(@as(usize, 0), ledger.used.load(.monotonic));
}

test "encode scratch pressure trim refunds idle credit without touching a live borrower" {
    const t = std.testing;
    const page = std.heap.pageSize();
    var ledger: memory.SharedBudget = .{ .limit = 16 * page };
    var cache: EncodeScratch = .{ .io = t.io, .child = t.allocator, .ledger = &ledger, .limit = 8 * page };
    defer cache.deinit();
    const a = cache.allocator();
    const large = try a.alloc(u8, 4 * page);
    a.free(large);
    const live = try a.alloc(u8, page);
    @memset(live, 71);
    const other = try a.alloc(u8, 3 * page);
    a.free(other);
    try t.expect(ledger.drawExact(10 * page) == null);
    cache.trimIdle();
    try t.expectEqual(@as(usize, 0), cache.retained);
    try t.expectEqual(3 * page, cache.held);
    try t.expect(ledger.drawExact(10 * page) != null);
    for (live) |byte| try t.expectEqual(@as(u8, 71), byte);
    ledger.release(10 * page);
    a.free(live);
    cache.trimIdle();
    try t.expectEqual(@as(usize, 0), ledger.used.load(.monotonic));
}

test "encode scratch cannot retain against the writer's temporary decoder allowance" {
    const t = std.testing;
    const page = std.heap.pageSize();
    var ledger: memory.SharedBudget = .{ .limit = 16 * page };
    var cache: EncodeScratch = .{
        .io = t.io,
        .child = t.allocator,
        .ledger = &ledger,
        .limit = 8 * page,
    };
    defer cache.deinit();
    const a = cache.allocator();
    const large = try a.alloc(u8, 4 * page);
    a.free(large);
    try t.expectEqual(5 * page, cache.retained);
    cache.setPaused(true);
    ledger.limit += 4 * page;
    const borrowed = try a.alloc(u8, 3 * page);
    try t.expectEqual(@as(usize, 0), cache.held);
    a.free(borrowed);
    try t.expectEqual(@as(usize, 0), ledger.used.load(.monotonic));
    ledger.limit -= 4 * page;
    cache.setPaused(false);
    const normal = try a.alloc(u8, 3 * page);
    a.free(normal);
    try t.expectEqual(4 * page, cache.retained);
    try t.expectEqual(cache.retained, ledger.used.load(.monotonic));
}

test "encode scratch arena growth and reset release all idle credit" {
    const t = std.testing;
    var ledger: memory.SharedBudget = .{ .limit = 1 << 20 };
    var cache: EncodeScratch = .{ .io = t.io, .child = t.allocator, .ledger = &ledger, .limit = ledger.limit };
    defer cache.deinit();
    var arena: std.heap.ArenaAllocator = .init(cache.allocator());
    for (0..4) |_| {
        const bytes = try arena.allocator().alignedAlloc(u8, .@"64", 12345);
        @memset(bytes, 29);
        _ = arena.reset(.retain_capacity);
    }
    arena.deinit();
    try t.expectEqual(@as(usize, 0), cache.live);
    try t.expectEqual(cache.retained, ledger.used.load(.monotonic));
    cache.deinit();
    try t.expectEqual(@as(usize, 0), ledger.used.load(.monotonic));
}

test "encode scratch keeps room for the original alignment's header on reuse" {
    const t = std.testing;
    var ledger: memory.SharedBudget = .{ .limit = 1 << 20 };
    var cache: EncodeScratch = .{ .io = t.io, .child = t.allocator, .ledger = &ledger, .limit = ledger.limit };
    defer cache.deinit();
    const a = cache.allocator();
    const aligned = try a.alignedAlloc(u8, .fromByteUnits(std.heap.page_size_min), 1);
    const ptr = aligned.ptr;
    a.free(aligned);
    const wider = try a.alloc(u8, 2 * std.heap.pageSize() - @sizeOf(EncodeScratch.Node) - @sizeOf(*EncodeScratch.Node));
    defer a.free(wider);
    try t.expect(wider.ptr != ptr);
    @memset(wider, 31);
}

test "encode scratch shares returned nodes without sharing live task storage" {
    const t = std.testing;
    var ledger: memory.SharedBudget = .{ .limit = 1 << 20 };
    var cache: EncodeScratch = .{ .io = t.io, .child = t.allocator, .ledger = &ledger, .limit = ledger.limit };
    defer cache.deinit();
    var failed: std.atomic.Value(bool) = .init(false);
    const Worker = struct {
        fn trim(c: *EncodeScratch) void {
            for (0..100) |_| {
                c.setPaused(true);
                c.setPaused(false);
            }
        }

        fn run(c: *EncodeScratch, bad: *std.atomic.Value(bool), value: u8) void {
            const a = c.allocator();
            for (0..100) |_| {
                const bytes = a.alloc(u8, 8192) catch {
                    bad.store(true, .monotonic);
                    return;
                };
                defer a.free(bytes);
                @memset(bytes, value);
                for (bytes) |b| if (b != value) {
                    bad.store(true, .monotonic);
                    return;
                };
            }
        }
    };
    var group: std.Io.Group = .init;
    defer group.cancel(t.io);
    for (0..4) |i| try group.concurrent(t.io, Worker.run, .{ &cache, &failed, @as(u8, @intCast(i)) });
    try group.concurrent(t.io, Worker.trim, .{&cache});
    try group.await(t.io);
    try t.expect(!failed.load(.monotonic));
    try t.expectEqual(@as(usize, 0), cache.live);
    try t.expectEqual(cache.retained, ledger.used.load(.monotonic));
    try t.expect(ledger.peak.load(.monotonic) <= ledger.limit);
}
