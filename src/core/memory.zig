//! A query's memory ceiling (`--max-memory`) and the ways memory is drawn from it.
//!
//! One `SharedBudget` per query is the ledger: group tables, decode arenas, merges and the answer all draw from it
//! before they allocate, so what a query holds at once stays under the one limit, whatever `-j` is. Fixed costs the
//! ledger does not see are the slack the ceiling documents: the binary, thread stacks, the input's mapping, allocator
//! bookkeeping, each scan worker's first huge page of decode memory, the first `finalize_allowance` bytes a GROUP BY's
//! merge and answer hold beside its tables, and a ledger of `lean_decode` bytes of its own for one decode: a refusal
//! chain's last scan (`scan.narrowed`), or a write's row group sliced on its writer.

const std = @import("std");
const builtin = @import("builtin");

/// What a GROUP BY's merge and answer may hold past the ledger's limit once the scan is done: a small answer never
/// refuses a query whose tables fit.
pub const finalize_allowance: usize = 16 << 20;

/// The ledger a refusal chain's last scan decodes on, one worker's, beside the query's.
pub const lean_decode: usize = 32 << 20;

/// Hand the C heap's free memory back to the OS (glibc's `malloc_trim`; a no-op where there is none). Freed blocks
/// otherwise stay resident in its per-thread arenas: memory the ledger has back is not memory the process gave up.
pub fn trimHeap() void {
    if (comptime !builtin.link_libc or builtin.os.tag != .linux) return;
    const trim = @extern(?*const fn (usize) callconv(.c) c_int, .{ .name = "malloc_trim", .linkage = .weak }) orelse
        return;
    _ = trim(0);
}

/// One `max_memory` shared by every group table filling in parallel, so required headroom stops scaling with `-j`
/// (see `scan.zig`). Usage never exceeds `limit`, but admission near it is approximate — tables draw in blocks and can
/// sit on part of one — because exact per-entry accounting would put a contended atomic on every insertion. Stranded
/// residue is at most ~5% of the budget in aggregate regardless of worker count (see `blockFor`).
pub const SharedBudget = struct {
    // 16-aligned so an inline distinct acc can use bit 3 of this pointer as a flag (`DistinctAcc.table_flag`).
    used: std.atomic.Value(usize) align(16) = .init(0),
    limit: usize,
    block: usize = 64 * 1024,

    /// Blocks keep the shared counter off the per-group path: the counter is cheap, but many workers invalidating one
    /// cache line is not. The cap stops a barely-grouping worker from stranding quota an active table needs.
    const BlockMax: usize = 64 * 1024;

    /// Caps blocks so all tables together strand at most ~5% of the budget. A per-worker fraction would be far wider:
    /// early workers can reserve most of the pool between them.
    pub fn blockFor(limit: usize, n_tables: usize) usize {
        return @max(1, @min(BlockMax, limit / @max(1, n_tables) / 20));
    }

    fn blockSize(self: *const SharedBudget) usize {
        return self.block;
    }

    /// Near the ceiling, held slack can fail a table while total use is under the limit, so blocks are dropped once
    /// the pool is tight — exact admission where it is observable, atomic off the hot path elsewhere.
    pub fn tight(self: *const SharedBudget) bool {
        return self.used.load(.monotonic) >= self.limit - self.limit / 4;
    }

    /// What is not drawn now: a hint for sizing work to fit, not a promise (other holders draw concurrently).
    pub fn available(self: *const SharedBudget) usize {
        return self.limit -| self.used.load(.monotonic);
    }

    /// `draw` of exactly `need`, never a block: for holders too many to each strand a block's slack, such as the
    /// per-partition lists of every worker.
    pub fn drawExact(self: *SharedBudget, need: usize) ?usize {
        var cur = self.used.load(.monotonic);
        while (cur + need <= self.limit) {
            cur = self.used.cmpxchgWeak(cur, cur + need, .monotonic, .monotonic) orelse return need;
        }
        return null;
    }

    pub fn draw(self: *SharedBudget, need: usize) ?usize {
        const first: usize = if (self.tight()) need else @max(need, self.blockSize());
        for ([_]usize{ first, need }) |want| {
            var cur = self.used.load(.monotonic);
            while (cur + want <= self.limit) {
                if (self.used.cmpxchgWeak(cur, cur + want, .monotonic, .monotonic)) |actual| {
                    cur = actual;
                    continue;
                }
                return want;
            }
        }
        return null;
    }

    pub fn release(self: *SharedBudget, bytes: usize) void {
        if (bytes == 0) return;
        _ = self.used.fetchSub(bytes, .monotonic);
    }
};

/// An allocator that draws every byte it hands out from `budget` before asking `child`, and gives it back when the
/// memory is freed: for memory whose size is only known as it is allocated (a merge's maps and lists, a sliced row
/// group's pages). A refusal fails the allocation (`OutOfMemory`) and sets `refused`, which its owner turns into
/// `error.ExceededMemoryBudget`: the query's refusal, not the host's. Thread-safe when `child` is.
///
/// Every byte allocated through it must be freed through it, and nothing else freed through it: a free gives back
/// what an allocation drew.
pub const Charged = struct {
    child: std.mem.Allocator,
    budget: *SharedBudget,
    refused: std.atomic.Value(bool) = .init(false),
    /// Set by the first draw, and kept: whether this allocator has held any of the budget.
    drew: std.atomic.Value(bool) = .init(false),

    pub fn allocator(self: *Charged) std.mem.Allocator {
        return .{ .ptr = self, .vtable = &.{ .alloc = alloc, .resize = resize, .remap = remap, .free = free } };
    }

    /// `err`, or `error.ExceededMemoryBudget` when it is the out-of-memory this allocator's refusal raised.
    pub fn refusal(self: *const Charged, err: anyerror) anyerror {
        return if (err == error.OutOfMemory and self.refused.load(.monotonic)) error.ExceededMemoryBudget else err;
    }

    fn take(self: *Charged, n: usize) bool {
        if (self.budget.drawExact(n) != null) {
            if (!self.drew.load(.monotonic)) self.drew.store(true, .monotonic);
            return true;
        }
        self.refused.store(true, .monotonic);
        return false;
    }

    fn alloc(ctx: *anyopaque, len: usize, alignment: std.mem.Alignment, ret: usize) ?[*]u8 {
        const self: *Charged = @ptrCast(@alignCast(ctx));
        if (!self.take(len)) return null;
        return self.child.rawAlloc(len, alignment, ret) orelse {
            self.budget.release(len);
            return null;
        };
    }

    fn resize(ctx: *anyopaque, memory: []u8, alignment: std.mem.Alignment, new_len: usize, ret: usize) bool {
        const self: *Charged = @ptrCast(@alignCast(ctx));
        if (new_len > memory.len and !self.take(new_len - memory.len)) return false;
        if (!self.child.rawResize(memory, alignment, new_len, ret)) {
            if (new_len > memory.len) self.budget.release(new_len - memory.len);
            return false;
        }
        if (new_len < memory.len) self.budget.release(memory.len - new_len);
        return true;
    }

    fn remap(ctx: *anyopaque, memory: []u8, alignment: std.mem.Alignment, new_len: usize, ret: usize) ?[*]u8 {
        const self: *Charged = @ptrCast(@alignCast(ctx));
        if (new_len > memory.len and !self.take(new_len - memory.len)) return null;
        const p = self.child.rawRemap(memory, alignment, new_len, ret) orelse {
            if (new_len > memory.len) self.budget.release(new_len - memory.len);
            return null;
        };
        if (new_len < memory.len) self.budget.release(memory.len - new_len);
        return p;
    }

    fn free(ctx: *anyopaque, memory: []u8, alignment: std.mem.Alignment, ret: usize) void {
        const self: *Charged = @ptrCast(@alignCast(ctx));
        self.child.rawFree(memory, alignment, ret);
        self.budget.release(memory.len);
    }
};

test "Charged draws what it hands out, gives it back on free, and refuses past the budget" {
    var budget: SharedBudget = .{ .limit = 2000 };
    var c: Charged = .{ .child = std.testing.allocator, .budget = &budget };
    const a = c.allocator();
    const x = try a.alloc(u8, 600);
    try std.testing.expectEqual(@as(usize, 600), budget.used.load(.monotonic));
    try std.testing.expectError(error.OutOfMemory, a.alloc(u8, 1500));
    try std.testing.expect(c.refused.load(.monotonic));
    try std.testing.expectEqual(error.ExceededMemoryBudget, c.refusal(error.OutOfMemory));
    try std.testing.expectEqual(error.Overflow, c.refusal(error.Overflow));
    try std.testing.expectEqual(@as(usize, 600), budget.used.load(.monotonic));
    // A list grown and shrunk through it holds exactly its capacity.
    var list: std.ArrayList(u8) = .empty;
    try list.appendNTimes(a, 7, 300);
    try std.testing.expectEqual(600 + list.capacity, budget.used.load(.monotonic));
    list.shrinkAndFree(a, 10);
    try std.testing.expectEqual(600 + list.capacity, budget.used.load(.monotonic));
    list.deinit(a);
    a.free(x);
    try std.testing.expectEqual(@as(usize, 0), budget.used.load(.monotonic));
}

test "Charged gives back the draw when its child fails" {
    var budget: SharedBudget = .{ .limit = 1 << 20 };
    var failing = std.testing.FailingAllocator.init(std.testing.allocator, .{ .fail_index = 0 });
    var c: Charged = .{ .child = failing.allocator(), .budget = &budget };
    try std.testing.expectError(error.OutOfMemory, c.allocator().alloc(u8, 4096));
    try std.testing.expect(!c.refused.load(.monotonic));
    try std.testing.expectEqual(error.OutOfMemory, c.refusal(error.OutOfMemory));
    try std.testing.expectEqual(@as(usize, 0), budget.used.load(.monotonic));
}
