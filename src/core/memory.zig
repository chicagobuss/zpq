//! A query's memory ceiling (`--max-memory`) and the ways memory is drawn from it.
//!
//! One `SharedBudget` per query is the ledger: group tables, decode arenas, merges and the answer all draw from it
//! before they allocate, so what a query holds at once stays under the one limit, whatever `-j` is. Fixed costs the
//! ledger does not see are the slack the ceiling documents: the binary, thread stacks, the input's mapping, allocator
//! bookkeeping, each scan worker's first huge page of decode memory, the first `finalize_allowance` bytes a GROUP BY's
//! merge and answer hold beside its tables, and `lean_decode` bytes for one decode: a refusal chain's last scan
//! (`scan.narrowed`), or a write's row group sliced on its writer (past the write's limit while its producers are
//! idle, else on a ledger of its own).

const std = @import("std");
const builtin = @import("builtin");

/// What a GROUP BY's merge and answer may hold past the ledger's limit once the scan is done: a small answer never
/// refuses a query whose tables fit.
pub const finalize_allowance: usize = 16 << 20;

/// The ledger a refusal chain's last scan decodes on, one worker's, beside the query's.
pub const lean_decode: usize = 32 << 20;

/// Whether the C heap is glibc's, the only one with `malloc_trim` and `mallopt`: decided at compile time from the
/// target, so a build without them (musl, macOS, Windows, no libc) never references them. A weak extern's null check
/// decided it at run time before, and under static musl it did not hold: the call jumped to address 0, and every query
/// of the release CLI and the Lambda crashed on start.
const glibc_heap = hasGlibcHeap(builtin.os.tag, builtin.abi, builtin.link_libc);

fn hasGlibcHeap(os: std.Target.Os.Tag, abi: std.Target.Abi, link_libc: bool) bool {
    return link_libc and os == .linux and abi.isGnu();
}

extern "c" fn malloc_trim(pad: usize) c_int;
extern "c" fn mallopt(param: c_int, value: c_int) c_int;

/// Hand the C heap's free memory back to the OS (glibc's `malloc_trim`; a no-op where there is none). Freed blocks
/// otherwise stay resident in its per-thread arenas: memory the ledger has back is not memory the process gave up.
pub fn trimHeap() void {
    if (comptime !glibc_heap) return;
    _ = malloc_trim(0);
}

/// Keep the C heap from holding blocks of 1 MiB and more once they are freed: glibc raises its mmap threshold as large
/// blocks come and go (up to 32 MiB), after which a freed growth buffer stays resident in a thread's arena, held but
/// charged to nothing. Fixed, such blocks are their own mappings, given back when freed. For a process zpq owns (the
/// CLI, the Lambda); a library leaves its host's heap alone. A no-op without glibc.
pub fn fixHeapThreshold() void {
    if (comptime !glibc_heap) return;
    const M_MMAP_THRESHOLD: c_int = -3;
    _ = mallopt(M_MMAP_THRESHOLD, 1 << 20);
}

test "the C heap calls are glibc's alone, and run where the target has them" {
    // The release targets (static musl CLI and Lambda, macOS) never reference `mallopt`/`malloc_trim`.
    try std.testing.expect(!hasGlibcHeap(.linux, .musl, true));
    try std.testing.expect(!hasGlibcHeap(.linux, .musleabihf, true));
    try std.testing.expect(!hasGlibcHeap(.macos, .none, true));
    try std.testing.expect(!hasGlibcHeap(.linux, .gnu, false));
    try std.testing.expect(hasGlibcHeap(.linux, .gnu, true));
    try std.testing.expect(hasGlibcHeap(.linux, .gnueabihf, true));
    fixHeapThreshold();
    trimHeap();
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
    /// The most `used` has been: what the query held at once by the ledger's count (`--profile-summary`).
    peak: std.atomic.Value(usize) = .init(0),
    /// A private scan's funded floor. Its parent holds max(funded, used); room past the floor is borrowed exactly.
    /// The lock serializes cancellation against the owning worker's draws and frees. Ordinary ledgers use atomics.
    parent: ?*SharedBudget = null,
    funded: usize = 0,
    funding_lock: std.atomic.Mutex = .unlocked,

    fn lockFunding(self: *SharedBudget) void {
        while (!self.funding_lock.tryLock()) std.Thread.yield() catch {};
    }

    /// Stop protecting the floor, preserving the parent's charge for every live allocation. Future draws use
    /// whatever the parent has available, just as they did before the private plan was offered.
    pub fn releaseFloor(self: *SharedBudget) void {
        const parent = self.parent orelse return;
        self.lockFunding();
        defer self.funding_lock.unlock();
        parent.release(self.funded -| self.used.load(.monotonic));
        self.funded = 0;
    }

    /// Return an empty child's floor to prepaid credit on the same parent, without exposing it to borrowers.
    /// Publish credit before dropping the floor; the child's lock prevents a draw against both representations.
    pub fn transferIdleFloor(self: *SharedBudget, credit: *std.atomic.Value(usize)) void {
        if (self.parent == null) return;
        self.lockFunding();
        defer self.funding_lock.unlock();
        std.debug.assert(self.used.load(.monotonic) == 0);
        _ = credit.fetchAdd(self.funded, .monotonic);
        self.funded = 0;
    }

    /// Slices use protected room rather than competing for borrowed headroom needed by whole row groups.
    /// A cancelled private plan has no floor and falls through to the ordinary ledger.
    pub fn sliceAvailable(self: *const SharedBudget) usize {
        if (self.parent != null) {
            const mutable = @constCast(self);
            mutable.lockFunding();
            defer mutable.funding_lock.unlock();
            if (self.funded > 0) return self.funded -| self.used.load(.monotonic);
        }
        return self.available();
    }

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
        if (self.parent) |parent| return parent.tight();
        return self.used.load(.monotonic) >= self.limit - self.limit / 4;
    }

    /// What is not drawn now: a hint for sizing work to fit, not a promise (other holders draw concurrently).
    pub fn available(self: *const SharedBudget) usize {
        if (self.parent) |parent| {
            const mutable = @constCast(self);
            mutable.lockFunding();
            defer mutable.funding_lock.unlock();
            return (self.funded -| self.used.load(.monotonic)) + parent.available();
        }
        return self.limit -| self.used.load(.monotonic);
    }

    /// `draw` of exactly `need`, never a block: for holders too many to each strand a block's slack, such as the
    /// per-partition lists of every worker.
    pub fn drawExact(self: *SharedBudget, need: usize) ?usize {
        if (self.parent) |parent| {
            self.lockFunding();
            defer self.funding_lock.unlock();
            const cur = self.used.load(.monotonic);
            if (need > self.limit -| cur) return null;
            const extra = @max(cur + need, self.funded) - @max(cur, self.funded);
            if (extra > 0) _ = parent.drawExact(extra) orelse return null;
            self.used.store(cur + need, .monotonic);
            return need;
        }
        var cur = self.used.load(.monotonic);
        while (cur + need <= self.limit) {
            cur = self.used.cmpxchgWeak(cur, cur + need, .monotonic, .monotonic) orelse {
                self.notePeak(cur + need);
                return need;
            };
        }
        return null;
    }

    /// Hold `n` whatever the limit: for what is already resident, or must go ahead alone for the query to progress.
    pub fn force(self: *SharedBudget, n: usize) void {
        self.notePeak(self.used.fetchAdd(n, .monotonic) + n);
    }

    fn notePeak(self: *SharedBudget, now: usize) void {
        if (now > self.peak.load(.monotonic)) _ = self.peak.fetchMax(now, .monotonic);
    }

    pub fn draw(self: *SharedBudget, need: usize) ?usize {
        if (self.parent) |parent| {
            const first = if (parent.tight()) need else @max(need, parent.blockSize());
            return self.drawExact(first) orelse self.drawExact(need);
        }
        const first: usize = if (self.tight()) need else @max(need, self.blockSize());
        for ([_]usize{ first, need }) |want| {
            var cur = self.used.load(.monotonic);
            while (cur + want <= self.limit) {
                if (self.used.cmpxchgWeak(cur, cur + want, .monotonic, .monotonic)) |actual| {
                    cur = actual;
                    continue;
                }
                self.notePeak(cur + want);
                return want;
            }
        }
        return null;
    }

    pub fn release(self: *SharedBudget, bytes: usize) void {
        if (bytes == 0) return;
        if (self.parent) |parent| {
            self.lockFunding();
            defer self.funding_lock.unlock();
            const cur = self.used.load(.monotonic);
            std.debug.assert(cur >= bytes);
            parent.release(@max(cur, self.funded) - @max(cur - bytes, self.funded));
            self.used.store(cur - bytes, .monotonic);
            return;
        }
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

test "a decoder borrows above its floor and cancellation preserves every live charge" {
    var parent: SharedBudget = .{ .limit = 1000 };
    _ = parent.drawExact(200).?;
    var worker: SharedBudget = .{ .limit = parent.limit, .parent = &parent, .funded = 200 };
    _ = worker.drawExact(600).?;
    try std.testing.expectEqual(@as(usize, 600), parent.used.load(.monotonic));
    try std.testing.expect(worker.drawExact(401) == null);
    try std.testing.expectEqual(@as(usize, 600), worker.used.load(.monotonic));
    worker.release(550);
    try std.testing.expectEqual(@as(usize, 200), parent.used.load(.monotonic));
    try std.testing.expectEqual(@as(usize, 150), worker.sliceAvailable());
    worker.releaseFloor();
    try std.testing.expectEqual(@as(usize, 50), parent.used.load(.monotonic));
    try std.testing.expectEqual(@as(usize, 950), worker.sliceAvailable());
    _ = worker.draw(300).?;
    try std.testing.expectEqual(@as(usize, 350), parent.used.load(.monotonic));
    worker.release(350);
    worker.releaseFloor();
    try std.testing.expectEqual(@as(usize, 0), parent.used.load(.monotonic));
}

test "a borrowed decode refusal leaves the other worker's sliced room protected" {
    var parent: SharedBudget = .{ .limit = 1000 };
    _ = parent.drawExact(400).?;
    var workers = [_]SharedBudget{
        .{ .limit = parent.limit, .parent = &parent, .funded = 200 },
        .{ .limit = parent.limit, .parent = &parent, .funded = 200 },
    };
    _ = workers[0].drawExact(800).?;
    try std.testing.expect(workers[1].drawExact(201) == null);
    _ = workers[1].drawExact(workers[1].sliceAvailable()).?;
    try std.testing.expectEqual(parent.limit, parent.used.load(.monotonic));
    for (&workers) |*w| {
        w.release(w.used.load(.monotonic));
        w.releaseFloor();
    }
    try std.testing.expectEqual(@as(usize, 0), parent.used.load(.monotonic));
}

test "cancelling a decoder floor while its worker holds memory refunds only unused room" {
    const Context = struct {
        budget: *SharedBudget,
        live: std.atomic.Value(bool) = .init(false),
        cancelled: std.atomic.Value(bool) = .init(false),

        fn run(ctx: *@This()) void {
            _ = ctx.budget.drawExact(4096).?;
            ctx.live.store(true, .release);
            while (!ctx.cancelled.load(.acquire)) std.Thread.yield() catch {};
            ctx.budget.release(4096);
        }
    };
    var parent: SharedBudget = .{ .limit = 1 << 20 };
    defer std.debug.assert(parent.used.load(.monotonic) == 0);
    _ = parent.drawExact(1 << 18).?;
    var worker: SharedBudget = .{ .limit = parent.limit, .parent = &parent, .funded = 1 << 18 };
    defer worker.releaseFloor();
    var ctx: Context = .{ .budget = &worker };
    const thread = try std.Thread.spawn(.{}, Context.run, .{&ctx});
    defer thread.join();
    while (!ctx.live.load(.acquire)) std.Thread.yield() catch {};
    worker.releaseFloor();
    const charged = parent.used.load(.monotonic);
    ctx.cancelled.store(true, .release);
    try std.testing.expectEqual(@as(usize, 4096), charged);
}

test "a child drops table slack when its parent is tight" {
    var parent: SharedBudget = .{ .limit = 1000 };
    _ = parent.drawExact(200).?;
    var child: SharedBudget = .{ .limit = parent.limit, .parent = &parent, .funded = 200 };
    const rest = parent.drawExact(600).?;
    try std.testing.expect(child.tight());
    parent.release(rest);
    try std.testing.expect(!child.tight());
    child.releaseFloor();
    try std.testing.expectEqual(@as(usize, 0), parent.used.load(.monotonic));
}
