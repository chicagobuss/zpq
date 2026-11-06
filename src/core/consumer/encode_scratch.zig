const std = @import("std");
const memory = @import("../memory.zig");

/// Page-backed encode arenas return their nodes here between jobs. Live nodes belong to the job's existing
/// reservation; idle nodes draw their actual mapped size before being retained. The cache never forces a draw.
/// Its limit is the write window's byte credit, not a second allowance outside the query's ledger.
pub const EncodeScratch = struct {
    io: std.Io,
    child: std.mem.Allocator = std.heap.page_allocator,
    ledger: *memory.SharedBudget,
    limit: usize,
    mutex: std.Io.Mutex = .init,
    idle: ?*Node = null,
    retained: usize = 0,
    live: usize = 0,

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
        self.dropIdle();
    }

    fn dropIdle(self: *EncodeScratch) void {
        while (self.idle) |node| {
            self.idle = node.next;
            const bytes = node.bytes;
            const alignment = node.alignment;
            self.ledger.release(bytes.len);
            self.child.rawFree(bytes, alignment, @returnAddress());
        }
        self.retained = 0;
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
        defer self.mutex.unlock(self.io);
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
            node.extra = node.bytes.len - size;
            self.live += 1;
            return node.data;
        }
        if (self.ledger.tight()) self.dropIdle();
        const actual_alignment = alignment.max(.of(Node));
        const bytes = (self.child.rawAlloc(size, actual_alignment, ret) orelse return null)[0..size];
        const node: *Node = @ptrCast(@alignCast(bytes.ptr));
        const data = bytes.ptr + offset(alignment);
        node.* = .{ .bytes = bytes, .alignment = actual_alignment, .data = data };
        const header: **Node = @ptrCast(@alignCast(data - @sizeOf(*Node)));
        header.* = node;
        self.live += 1;
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
        defer self.mutex.unlock(self.io);
        self.live -= 1;
        self.ledger.release(node.extra);
        node.extra = 0;
        if (!self.ledger.tight() and node.bytes.len <= self.limit - self.retained) {
            if (self.ledger.drawExact(node.bytes.len) != null) {
                node.next = self.idle;
                self.idle = node;
                self.retained += node.bytes.len;
                return;
            }
        }
        const allocation = node.bytes;
        const alignment = node.alignment;
        self.child.rawFree(allocation, alignment, ret);
    }
};

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
    try group.await(t.io);
    try t.expect(!failed.load(.monotonic));
    try t.expectEqual(@as(usize, 0), cache.live);
    try t.expectEqual(cache.retained, ledger.used.load(.monotonic));
    try t.expect(ledger.peak.load(.monotonic) <= ledger.limit);
}
