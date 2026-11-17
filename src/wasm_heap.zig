//! Zig 0.17's WASM heap is single-threaded. Serialize its C ABI across instances;
//! the engine and its TLS stay multithreaded. The separate lock object uses WASM atomics.
const std = @import("std");
const heap = std.heap.brk_allocator;
extern fn zpq_heap_lock() void;
extern fn zpq_heap_unlock() void;
const Header = extern struct { bytes: usize, alignment: usize };
fn header(ptr: [*]u8) *Header {
    return @ptrCast(@alignCast(ptr - @sizeOf(Header)));
}
fn allocate(n: usize, requested: usize) ?[*]u8 {
    const alignment = @max(16, requested);
    if (!std.math.isPowerOfTwo(alignment)) return null;
    const bytes = std.math.add(usize, n, alignment) catch return null;
    const base = heap.rawAlloc(bytes, .fromByteUnits(alignment), 0) orelse return null;
    const ptr = base + alignment;
    header(ptr).* = .{ .bytes = bytes, .alignment = alignment };
    return ptr;
}
fn release(ptr: [*]u8) void {
    const h = header(ptr).*;
    heap.rawFree((ptr - h.alignment)[0..h.bytes], .fromByteUnits(h.alignment), 0);
}
export fn malloc(n: usize) ?[*]u8 {
    zpq_heap_lock();
    defer zpq_heap_unlock();
    return allocate(n, 16);
}
export fn free(ptr: ?[*]u8) void {
    zpq_heap_lock();
    defer zpq_heap_unlock();
    if (ptr) |p| release(p);
}
export fn calloc(count: usize, size: usize) ?[*]u8 {
    zpq_heap_lock();
    defer zpq_heap_unlock();
    const n = std.math.mul(usize, count, size) catch return null;
    const ptr = allocate(n, 16) orelse return null;
    @memset(ptr[0..n], 0);
    return ptr;
}
export fn realloc(old: ?[*]u8, n: usize) ?[*]u8 {
    zpq_heap_lock();
    defer zpq_heap_unlock();
    const ptr = old orelse return allocate(n, 16);
    if (n == 0) {
        release(ptr);
        return null;
    }
    const h = header(ptr).*;
    const result = allocate(n, 16) orelse return null;
    @memcpy(result[0..@min(n, h.bytes - h.alignment)], ptr[0..@min(n, h.bytes - h.alignment)]);
    release(ptr);
    return result;
}
export fn malloc_usable_size(ptr: ?[*]u8) usize {
    const h = header(ptr orelse return 0).*;
    return h.bytes - h.alignment;
}
export fn aligned_alloc(alignment: usize, n: usize) ?[*]u8 {
    zpq_heap_lock();
    defer zpq_heap_unlock();
    return allocate(n, alignment);
}
export fn posix_memalign(out: *?[*]u8, alignment: usize, n: usize) c_int {
    if (!std.math.isPowerOfTwo(alignment) or alignment < @sizeOf(usize)) return 28;
    out.* = aligned_alloc(alignment, n) orelse return 48;
    return 0;
}
