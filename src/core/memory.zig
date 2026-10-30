//! A query's memory ceiling (`--max-memory`) and the ways memory is drawn from it.
//!
//! One `SharedBudget` per query is the ledger: group tables, decode arenas, merges and the answer all draw from it
//! before they allocate, so what a query holds at once stays under the one limit, whatever `-j` is. Fixed costs the
//! ledger does not see are the slack the ceiling documents: the binary, thread stacks, the input's mapping, allocator
//! bookkeeping, each scan worker's first huge page of decode memory, the first `finalize_allowance` bytes a GROUP BY's
//! merge and answer hold beside its tables, and `lean_decode` bytes for one decode: a refusal chain's last scan
//! (`scan.narrowed`), or a write's row group sliced on its writer (past the write's limit while its producers are
//! idle, else on a ledger of its own).
//!
//! Sliced page and survivor scratch stays with the worker. At the end of each row group it is trimmed to at most
//! one eighth of the ledger that scratch is charged to (`retainedCarry` of that ledger's `limit`). The next group
//! can draw the other seven eighths. A group that still does not fit is refused. Nothing drops that scratch and
//! retries underneath a live reader or a survivor mask.

const std = @import("std");
const builtin = @import("builtin");
const blocking = @import("blocking.zig");

/// What a GROUP BY's merge and answer may hold past the ledger's limit once the scan is done: a small answer never
/// refuses a query whose tables fit.
pub const finalize_allowance: usize = 16 << 20;

/// The ledger a refusal chain's last scan decodes on, one worker's, beside the query's.
pub const lean_decode: usize = 32 << 20;

/// Bytes of sliced scratch a row group may leave behind: one eighth of `allowance`, the `limit` of the ledger that
/// scratch is charged to. The next group can draw the other seven eighths. One that still does not fit is refused.
pub fn retainedCarry(allowance: usize) usize {
    return allowance / 8;
}

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

/// Reclaim freed heap pages between retries before the ledger lends their credit to the next attempt.
pub fn trimHeap() void {
    if (comptime use_mimalloc) {
        mi_collect(true);
        mi_arenas_purge();
        return;
    }
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
    // Run for real only where it leaves nothing behind: `fixHeapThreshold` would pin glibc's mmap threshold for the
    // rest of this test process, and glibc offers no way to read the dynamic one back. release-smoke runs both in the
    // shipped binaries; `trimHeap` only returns free memory.
    trimHeap();
}

/// The general-purpose allocator of a process zpq owns (the CLI, the Lambda), given the one `std.process.Init` chose.
/// A release build over musl takes mimalloc (linked by build.zig's `usesMimalloc`) instead of musl's malloc: musl's
/// allocator and Zig's `smp_allocator` both map and unmap pages as allocation-heavy folds (COUNT DISTINCT) grow, and
/// each `munmap` stalls every other thread; mimalloc keeps freed pages for reuse and purges them after a delay.
/// `Charged` counts bytes whatever its child, and huge-page regions take `page_allocator` directly, so neither the
/// budget nor the advice changes. Debug and safe builds keep Init's leak-checking allocator; glibc's heap stays where
/// it is (it has `trimHeap`).
pub fn processAllocator(init_gpa: std.mem.Allocator) std.mem.Allocator {
    if (comptime !use_mimalloc) return init_gpa;
    // Upstream's defaults keep freed pages for a second and back its arenas with transparent huge pages: on sub-second
    // queries that holds their freed buffers and every partly used 2 MiB region resident, well above a glibc build's
    // peak. Purged at once and without THP, peaks match glibc's at little cost. `MIMALLOC_*` still overrides them.
    mi_option_set_default(mi_option_purge_delay, 0);
    mi_option_set_default(mi_option_allow_thp, 0);
    return split_process_heap.allocator();
}

/// build.zig links mimalloc (`usesMimalloc`) for exactly the builds `ownHeap` names, libzpq's aside.
const use_mimalloc = @import("build_options").use_mimalloc;
comptime {
    if (use_mimalloc) std.debug.assert(ownHeap(builtin.mode, builtin.abi, builtin.link_libc, builtin.single_threaded));
}

/// mimalloc's aligned API as a Zig allocator: thread-safe, and a block's in-place growth is mimalloc's usable size.
const mimalloc: std.mem.Allocator = .{ .ptr = undefined, .vtable = &.{
    .alloc = miAlloc,
    .resize = miResize,
    .remap = miRemap,
    .free = miFree,
} };

// Larger transient buffers use their own mappings so a freed growth buffer cannot stay in a partly occupied heap page.
const SplitHeap = struct {
    small: std.mem.Allocator,
    large: std.mem.Allocator = std.heap.page_allocator,
    threshold: usize,

    fn allocator(self: *SplitHeap) std.mem.Allocator {
        return .{ .ptr = self, .vtable = &.{ .alloc = alloc, .resize = resize, .remap = remap, .free = free } };
    }

    fn child(self: *SplitHeap, len: usize) std.mem.Allocator {
        return if (len >= self.threshold) self.large else self.small;
    }

    fn ownerIsLarge(self: *SplitHeap, memory: []u8) bool {
        // Blocks move between heaps of different thresholds: group tables and their DISTINCT accumulators, and an
        // unbudgeted query's buffers freed by the process heap after it. mimalloc's page map knows its own blocks;
        // a mapping is never smaller than the lowest threshold.
        if (comptime use_mimalloc) return memory.len >= tight_distinct_threshold and !mi_is_in_heap_region(memory.ptr);
        return memory.len >= self.threshold;
    }

    fn alloc(ctx: *anyopaque, len: usize, a: std.mem.Alignment, ret: usize) ?[*]u8 {
        const self: *SplitHeap = @ptrCast(@alignCast(ctx));
        return self.child(len).rawAlloc(len, a, ret);
    }

    fn resize(ctx: *anyopaque, memory: []u8, a: std.mem.Alignment, len: usize, ret: usize) bool {
        const self: *SplitHeap = @ptrCast(@alignCast(ctx));
        const large = self.ownerIsLarge(memory);
        if (large != (len >= self.threshold)) return false;
        const old = if (large) self.large else self.small;
        return old.rawResize(memory, a, len, ret);
    }

    fn remap(ctx: *anyopaque, memory: []u8, a: std.mem.Alignment, len: usize, ret: usize) ?[*]u8 {
        const self: *SplitHeap = @ptrCast(@alignCast(ctx));
        const large = self.ownerIsLarge(memory);
        if (large != (len >= self.threshold)) return null;
        const old = if (large) self.large else self.small;
        return old.rawRemap(memory, a, len, ret);
    }

    fn free(ctx: *anyopaque, memory: []u8, a: std.mem.Alignment, ret: usize) void {
        const self: *SplitHeap = @ptrCast(@alignCast(ctx));
        const old = if (self.ownerIsLarge(memory)) self.large else self.small;
        old.rawFree(memory, a, ret);
    }
};

const group_heap_threshold = 16 * 1024;
const process_heap_threshold = 32 * 1024;
const tight_distinct_threshold = 8 * 1024;
const relaxed_distinct_threshold = 1024 * 1024;
var split_process_heap: SplitHeap = .{ .small = mimalloc, .threshold = process_heap_threshold };
var split_group_heap: SplitHeap = .{ .small = mimalloc, .threshold = group_heap_threshold };
var split_tight_distinct_heap: SplitHeap = .{ .small = mimalloc, .threshold = tight_distinct_threshold };
var split_distinct_heap: SplitHeap = .{ .small = mimalloc, .threshold = relaxed_distinct_threshold };
var split_unbudgeted_heap: SplitHeap = .{ .small = mimalloc, .threshold = std.math.maxInt(usize) };
var split_c_group_heap: SplitHeap = .{
    .small = if (builtin.link_libc) std.heap.c_allocator else std.heap.page_allocator,
    .threshold = group_heap_threshold,
};
// Used only to identify SafeAllocator's vtable; each real allocator retains its own backing and lifetime.
var safe_allocator_probe: std.heap.SafeAllocator = .init(std.heap.page_allocator, .{});

const DistinctHeapRoute = enum { tight, process, relaxed };

fn distinctHeapRoute(limit: usize, tight: bool) DistinctHeapRoute {
    if (limit <= 512 * 1024 * 1024) return .tight;
    return if (tight) .process else .relaxed;
}

/// The heap a query's buffers come from, given whether the caller set `--max-memory`. Only a configured budget pays
/// for promptly released mappings (the process heap, and `groupAllocator` and `distinctMapAllocator` under it). An
/// unbudgeted query keeps every buffer on the general heap, and those two leave it there.
pub fn queryAllocator(parent: std.mem.Allocator, budgeted: bool) std.mem.Allocator {
    if (comptime !use_mimalloc) return parent;
    return queryHeap(&split_process_heap, &split_unbudgeted_heap, parent, budgeted);
}

fn queryHeap(process: *SplitHeap, unbudgeted: *SplitHeap, parent: std.mem.Allocator, budgeted: bool) std.mem.Allocator {
    const p = process.allocator();
    if (budgeted or parent.vtable != p.vtable or parent.ptr != p.ptr) return parent;
    return unbudgeted.allocator();
}

test "an unbudgeted query's aggregation buffers stay off the mapped path" {
    // Mappings fail here: a buffer that took them would be refused.
    var mapped = std.testing.FailingAllocator.init(std.testing.allocator, .{ .fail_index = 0 });
    var process: SplitHeap = .{
        .small = std.testing.allocator,
        .large = mapped.allocator(),
        .threshold = process_heap_threshold,
    };
    var unbudgeted: SplitHeap = .{
        .small = std.testing.allocator,
        .large = mapped.allocator(),
        .threshold = std.math.maxInt(usize),
    };
    const budgeted = queryHeap(&process, &unbudgeted, process.allocator(), true);
    try std.testing.expect(budgeted.ptr == @as(*anyopaque, &process));
    try std.testing.expectError(error.OutOfMemory, budgeted.alloc(u8, process_heap_threshold));
    // Another parent (a test's, a ledger's `Charged`) is the caller's choice either way.
    const other = queryHeap(&process, &unbudgeted, std.testing.allocator, false);
    try std.testing.expect(other.ptr == std.testing.allocator.ptr and other.vtable == std.testing.allocator.vtable);
    // A group table's slots and a DISTINCT map grow by doubling well past every mapping threshold.
    const a = queryHeap(&process, &unbudgeted, process.allocator(), false);
    var slots = try a.alloc(u64, 64);
    defer a.free(slots);
    @memset(slots, 7);
    while (slots.len * @sizeOf(u64) < 4 * relaxed_distinct_threshold) {
        slots = try a.realloc(slots, slots.len * 2);
        try std.testing.expectEqual(@as(u64, 7), slots[0]);
        @memset(slots[slots.len / 2 ..], 7);
    }
    try std.testing.expectEqual(@as(usize, 0), mapped.allocations);
}

/// Tables use a lower mapping threshold without changing the process allocator used by scalar DISTINCT.
pub fn groupAllocator(parent: std.mem.Allocator) std.mem.Allocator {
    if (comptime !use_mimalloc) return parent;
    const process = split_process_heap.allocator();
    return if (parent.vtable == process.vtable and parent.ptr == process.ptr) split_group_heap.allocator() else parent;
}

/// Tight native-libc GROUP BY tables need page-backed large buffers so growing them can avoid a second copy.
/// Leave the normal native path alone when the query budget has ample headroom.
pub fn groupAllocatorForBudget(parent: std.mem.Allocator, limit: usize) std.mem.Allocator {
    if (comptime !use_mimalloc and builtin.link_libc) {
        if (limit <= 512 * 1024 * 1024 and parent.vtable == std.heap.c_allocator.vtable)
            return split_c_group_heap.allocator();
    }
    return groupAllocator(parent);
}

/// A scalar DISTINCT map can reuse small heap pages while the query has ample budget. Near the limit it switches to
/// promptly released mappings. Cross-owner frees use mimalloc ownership, so a map may grow across the switch.
pub fn distinctMapAllocator(parent: std.mem.Allocator, budget: ?*SharedBudget) std.mem.Allocator {
    if (comptime !use_mimalloc) return parent;
    const b = budget orelse return parent;
    const process = split_process_heap.allocator();
    if (parent.vtable != process.vtable or parent.ptr != process.ptr) return parent;
    // A small budget can become tight before even one map grows again; start with page-backed maps so the no-copy
    // fallback can resize them at that point.
    return switch (distinctHeapRoute(b.limit, b.tight())) {
        .tight => split_tight_distinct_heap.allocator(),
        .process => parent,
        .relaxed => split_distinct_heap.allocator(),
    };
}

test "DISTINCT heap routing keeps small or tight budgets page-backed" {
    try std.testing.expectEqual(DistinctHeapRoute.tight, distinctHeapRoute(128 << 20, false));
    try std.testing.expectEqual(DistinctHeapRoute.tight, distinctHeapRoute(512 << 20, true));
    try std.testing.expectEqual(DistinctHeapRoute.process, distinctHeapRoute(2 << 30, true));
    try std.testing.expectEqual(DistinctHeapRoute.relaxed, distinctHeapRoute(2 << 30, false));
}

/// Resize without a simultaneous copy. A generic remap may allocate and copy internally, so only use moving
/// remap for allocators whose backing is known to be page mappings; resize itself cannot move the allocation.
pub fn remapWithoutCopy(a: std.mem.Allocator, old: anytype, new_len: usize) ?@TypeOf(old) {
    if (old.len == 0) return null;
    if (a.resize(old, new_len)) return old.ptr[0..new_len];
    const page = std.heap.page_allocator;
    if (a.vtable == page.vtable) return a.remap(old, new_len);
    // Zig's SMP allocator either retains a small block in its size class or remaps a large page allocation;
    // unlike a general-purpose allocator, its remap never performs an allocating copy.
    if (a.vtable == std.heap.smp_allocator.vtable) return a.remap(old, new_len);
    if (new_len >= old.len and a.vtable == safe_allocator_probe.allocator().vtable) {
        const safe: *std.heap.SafeAllocator = @ptrCast(@alignCast(a.ptr));
        // For growth SafeAllocator either grows a bucket in place or delegates to its backing allocator.
        if (safe.backing.vtable == page.vtable) return a.remap(old, new_len);
    }
    if (comptime !use_mimalloc and builtin.link_libc) {
        const native_group = split_c_group_heap.allocator();
        if (a.vtable == native_group.vtable and a.ptr == native_group.ptr and old.len * @sizeOf(@TypeOf(old[0])) >= group_heap_threshold)
            return a.remap(old, new_len);
    }
    if (comptime use_mimalloc) {
        const group = split_group_heap.allocator();
        const process = split_process_heap.allocator();
        const tight_distinct = split_tight_distinct_heap.allocator();
        if (a.vtable == group.vtable and
            (a.ptr == group.ptr or a.ptr == process.ptr or a.ptr == tight_distinct.ptr))
        {
            const heap: *SplitHeap = @ptrCast(@alignCast(a.ptr));
            const bytes = std.math.mul(usize, new_len, @sizeOf(@TypeOf(old[0]))) catch return null;
            if (heap.ownerIsLarge(std.mem.sliceAsBytes(old)) and bytes >= heap.threshold)
                return a.remap(old, new_len);
        }
    }
    if (comptime builtin.is_test) {
        // The Zig test runner initializes this allocator with page_allocator as its backing allocator.
        const testing = std.testing.allocator;
        if (a.ptr == testing.ptr and a.vtable == testing.vtable) return a.remap(old, new_len);
    }
    return null;
}

test "no-copy remap supports page-backed SMP allocations" {
    const a = std.heap.smp_allocator;
    const old = try a.alloc(u8, 96 << 10);
    @memset(old, 0xa5);
    const grown = remapWithoutCopy(a, old, 192 << 10) orelse {
        a.free(old);
        return error.OutOfMemory;
    };
    defer a.free(grown);
    try std.testing.expectEqual(@as(usize, 192 << 10), grown.len);
    for (grown[0 .. 96 << 10]) |b| try std.testing.expectEqual(@as(u8, 0xa5), b);
}

test "no-copy remap supports page-backed safe allocations" {
    var safe: std.heap.SafeAllocator = .init(std.heap.page_allocator, .{});
    defer std.testing.expect(safe.deinit() == 0) catch @panic("safe allocator leaked");
    const a = safe.allocator();
    const old = try a.alloc(u8, 96 << 10);
    @memset(old, 0xa5);
    const grown = remapWithoutCopy(a, old, 192 << 10) orelse {
        a.free(old);
        return error.OutOfMemory;
    };
    defer a.free(grown);
    for (grown[0 .. 96 << 10]) |b| try std.testing.expectEqual(@as(u8, 0xa5), b);
}

test "tight native group heap remaps large buffers without copying" {
    if (comptime use_mimalloc or !builtin.link_libc) return error.SkipZigTest;
    const c = std.heap.c_allocator;
    const tight = groupAllocatorForBudget(c, 3500 * 1024);
    const ample = groupAllocatorForBudget(c, 2 << 30);
    try std.testing.expect(ample.vtable == c.vtable);
    try std.testing.expect(tight.vtable != c.vtable);
    const old = try tight.alloc(u64, 96 << 10);
    @memset(old, 0x12345678);
    const grown = remapWithoutCopy(tight, old, 192 << 10) orelse {
        tight.free(old);
        return error.OutOfMemory;
    };
    defer tight.free(grown);
    for (grown[0 .. 96 << 10]) |v| try std.testing.expectEqual(@as(u64, 0x12345678), v);
}

test "split heap preserves data and ownership across its size threshold" {
    const threshold = group_heap_threshold;
    var split: SplitHeap = .{ .small = std.testing.allocator, .threshold = threshold };
    const a = split.allocator();
    var bytes = try a.alloc(u8, threshold - 1);
    @memset(bytes, 0xa5);
    bytes = try a.realloc(bytes, threshold + 1);
    try std.testing.expectEqual(@as(u8, 0xa5), bytes[0]);
    try std.testing.expectEqual(@as(u8, 0xa5), bytes[threshold - 2]);
    bytes = try a.realloc(bytes, 100);
    try std.testing.expectEqual(@as(u8, 0xa5), bytes[99]);
    a.free(bytes);
}

test "split heap retains one owner when replacement allocations fail" {
    var small = std.testing.FailingAllocator.init(std.testing.allocator, .{});
    var large = std.testing.FailingAllocator.init(std.testing.allocator, .{ .fail_index = 0 });
    var split: SplitHeap = .{
        .small = small.allocator(),
        .large = large.allocator(),
        .threshold = group_heap_threshold,
    };
    const a = split.allocator();
    const first = try a.alloc(u8, group_heap_threshold - 1);
    @memset(first, 0xa5);
    try std.testing.expectError(error.OutOfMemory, a.realloc(first, group_heap_threshold + 1));
    try std.testing.expectEqual(@as(u8, 0xa5), first[0]);
    large.fail_index = std.math.maxInt(usize);
    const second = try a.realloc(first, group_heap_threshold + 1);
    small.fail_index = small.alloc_index;
    try std.testing.expectError(error.OutOfMemory, a.realloc(second, 100));
    try std.testing.expectEqual(@as(u8, 0xa5), second[0]);
    a.free(second);
    try std.testing.expectEqual(small.allocated_bytes, small.freed_bytes);
    try std.testing.expectEqual(large.allocated_bytes, large.freed_bytes);
    try std.testing.expectEqual(small.allocations, small.deallocations);
    try std.testing.expectEqual(large.allocations, large.deallocations);
}

extern fn mi_malloc_aligned(size: usize, alignment: usize) ?*anyopaque;
extern fn mi_realloc_aligned(p: ?*anyopaque, size: usize, alignment: usize) ?*anyopaque;
extern fn mi_usable_size(p: ?*const anyopaque) usize;
extern fn mi_free(p: ?*anyopaque) void;
extern fn mi_is_in_heap_region(p: ?*const anyopaque) bool;
extern fn mi_collect(force: bool) void;
extern fn mi_arenas_purge() void;
extern fn mi_option_set_default(option: c_int, value: c_long) void;
// `mi_option_t` values in vendor/mimalloc/include/mimalloc.h (3.6.0).
const mi_option_purge_delay: c_int = 15;
const mi_option_allow_thp: c_int = 43;

fn miAlloc(_: *anyopaque, len: usize, alignment: std.mem.Alignment, _: usize) ?[*]u8 {
    return @ptrCast(mi_malloc_aligned(len, alignment.toByteUnits()));
}

fn miResize(_: *anyopaque, memory: []u8, _: std.mem.Alignment, new_len: usize, _: usize) bool {
    return new_len <= mi_usable_size(memory.ptr);
}

fn miRemap(_: *anyopaque, memory: []u8, alignment: std.mem.Alignment, new_len: usize, _: usize) ?[*]u8 {
    return @ptrCast(mi_realloc_aligned(memory.ptr, new_len, alignment.toByteUnits()));
}

fn miFree(_: *anyopaque, memory: []u8, _: std.mem.Alignment, _: usize) void {
    mi_free(memory.ptr);
}

fn ownHeap(mode: @TypeOf(builtin.mode), abi: std.Target.Abi, link_libc: bool, single_threaded: bool) bool {
    return (mode == .fast or mode == .small) and link_libc and abi.isMusl() and !single_threaded;
}

test "a release build over musl's malloc takes its own heap; leak checks and glibc's heap stay" {
    try std.testing.expect(ownHeap(.fast, .musl, true, false));
    try std.testing.expect(ownHeap(.small, .musl, true, false));
    try std.testing.expect(!ownHeap(.safe, .musl, true, false));
    try std.testing.expect(!ownHeap(.debug, .musl, true, false));
    try std.testing.expect(!ownHeap(.fast, .gnu, true, false));
    try std.testing.expect(!ownHeap(.fast, .none, true, false));
    try std.testing.expect(!ownHeap(.fast, .musl, true, true));
    try std.testing.expect(processAllocator(std.testing.allocator).vtable == std.testing.allocator.vtable);
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
    /// Blocking, not spun on: every draw and release of a child with a parent takes it.
    funding_lock: std.Io.Mutex = .init,

    fn lockFunding(self: *SharedBudget) void {
        self.funding_lock.lockUncancelable(blocking.io);
    }

    fn unlockFunding(self: *SharedBudget) void {
        self.funding_lock.unlock(blocking.io);
    }

    /// Stop protecting the floor, preserving the parent's charge for every live allocation. Future draws use
    /// whatever the parent has available, just as they did before the private plan was offered.
    pub fn releaseFloor(self: *SharedBudget) void {
        const parent = self.parent orelse return;
        self.lockFunding();
        defer self.unlockFunding();
        parent.release(self.funded -| self.used.load(.monotonic));
        self.funded = 0;
    }

    /// Return an empty child's floor to prepaid credit on the same parent, without exposing it to borrowers.
    /// Publish credit before dropping the floor; the child's lock prevents a draw against both representations.
    pub fn transferIdleFloor(self: *SharedBudget, credit: *std.atomic.Value(usize)) void {
        if (self.parent == null) return;
        self.lockFunding();
        defer self.unlockFunding();
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
            defer mutable.unlockFunding();
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
            defer mutable.unlockFunding();
            return (self.funded -| self.used.load(.monotonic)) + parent.available();
        }
        return self.limit -| self.used.load(.monotonic);
    }

    /// `draw` of exactly `need`, never a block: for holders too many to each strand a block's slack, such as the
    /// per-partition lists of every worker.
    pub fn drawExact(self: *SharedBudget, need: usize) ?usize {
        if (self.parent) |parent| {
            self.lockFunding();
            defer self.unlockFunding();
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
            defer self.unlockFunding();
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
    pub fn refusal(self: *const Charged, err: anytype) (@TypeOf(err) || error{ExceededMemoryBudget}) {
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
