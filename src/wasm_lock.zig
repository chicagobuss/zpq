//! Compiled with threading enabled separately from Zig 0.17's serial WASM heap.
const std = @import("std");
pub const panic = std.debug.no_panic;
var mutex: std.atomic.Value(u32) = .init(0);
export fn zpq_heap_lock() void {
    while (mutex.cmpxchgWeak(0, 1, .acquire, .monotonic) != null) {
        asm volatile (
            \\ local.get %[ptr]
            \\ i32.const 1
            \\ i64.const -1
            \\ memory.atomic.wait32 0
            \\ drop
            :
            : [ptr] "r" (&mutex.raw),
            : .{ .memory = true });
    }
}
export fn zpq_heap_unlock() void {
    mutex.store(0, .release);
    asm volatile (
        \\ local.get %[ptr]
        \\ i32.const 1
        \\ memory.atomic.notify 0
        \\ drop
        :
        : [ptr] "r" (&mutex.raw),
        : .{ .memory = true });
}
