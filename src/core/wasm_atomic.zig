//! Zig 0.17 restricts WASM atomic builtins to pointer-sized values. Use a short
//! 32-bit lock for wide counters; native builds retain std's atomics.
const std = @import("std");
const builtin = @import("builtin");

pub fn Value(comptime T: type) type {
    if (builtin.os.tag != .wasi) return std.atomic.Value(T);
    if (!builtin.single_threaded and @bitSizeOf(T) <= 32) return std.atomic.Value(T);
    return struct {
        raw: T,
        mutex: std.atomic.Value(u32) = .init(0),
        fn lock(s: *const @This()) void {
            if (builtin.single_threaded) return;
            const m = &@constCast(s).mutex;
            while (m.cmpxchgWeak(0, 1, .acquire, .monotonic) != null) std.atomic.spinLoopHint();
        }
        fn unlock(s: *const @This()) void {
            if (!builtin.single_threaded) @constCast(s).mutex.store(0, .release);
        }
        const Self = @This();
        pub fn init(v: T) Self {
            return .{ .raw = v };
        }
        pub fn load(s: *const Self, comptime order: std.builtin.AtomicOrder) T {
            _ = order;
            s.lock();
            defer s.unlock();
            return s.raw;
        }
        pub fn store(s: *Self, v: T, comptime order: std.builtin.AtomicOrder) void {
            _ = order;
            s.lock();
            defer s.unlock();
            s.raw = v;
        }
        pub fn fetchAdd(s: *Self, v: T, comptime order: std.builtin.AtomicOrder) T {
            _ = order;
            s.lock();
            defer s.unlock();
            const old = s.raw;
            s.raw +%= v;
            return old;
        }
        pub fn cmpxchgWeak(s: *Self, expected: T, desired: T, comptime success: std.builtin.AtomicOrder, comptime failure: std.builtin.AtomicOrder) ?T {
            _ = success;
            _ = failure;
            s.lock();
            defer s.unlock();
            if (s.raw != expected) return s.raw;
            s.raw = desired;
            return null;
        }
        pub fn fetchMax(s: *Self, v: T, comptime order: std.builtin.AtomicOrder) T {
            _ = order;
            s.lock();
            defer s.unlock();
            const old = s.raw;
            s.raw = @max(old, v);
            return old;
        }
        pub fn fetchMin(s: *Self, v: T, comptime order: std.builtin.AtomicOrder) T {
            _ = order;
            s.lock();
            defer s.unlock();
            const old = s.raw;
            s.raw = @min(old, v);
            return old;
        }
    };
}
