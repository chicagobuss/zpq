//! How far a recursive walk over a query's own tree may go: until it has used a share of the stack the running thread
//! actually has, asked of the OS, never a depth chosen in advance. A query can nest without limit (`1 + 1 + ...` twenty
//! thousand times is one left-deep tree the parser builds without recursing), and the thread that parses it may be a
//! host's own (libzpq runs in the caller's), so the budget is read from the thread, not assumed.
const std = @import("std");
const builtin = @import("builtin");

extern "c" fn pthread_getattr_np(thread: std.c.pthread_t, attr: *std.c.pthread_attr_t) c_int;
extern "c" fn pthread_attr_getstack(attr: *const std.c.pthread_attr_t, addr: *?*anyopaque, size: *usize) c_int;
extern "c" fn pthread_get_stackaddr_np(thread: std.c.pthread_t) ?*anyopaque;
extern "c" fn pthread_get_stacksize_np(thread: std.c.pthread_t) usize;

/// A walk may use this share of the stack that is free where it starts; the rest is for what runs after it (the
/// refusal's own report, the unwinding, a caller that returns into more work).
const share_num = 3;
const share_den = 4;

/// With no way to ask the thread, and no limit to read, a walk gets this much: enough for a deep expression, far less
/// than any stack.
const last_resort_bytes: usize = 64 << 10;

pub const Guard = struct {
    /// The lowest stack address the walk may reach.
    floor: usize,

    /// A guard for a walk that starts in the caller's frame.
    pub fn start() Guard {
        const here = @frameAddress();
        const low = stackLow() orelse return .{ .floor = here -| fallbackBytes() };
        if (low >= here) return .{ .floor = here };
        // Stacks grow down: what is free is `here - low`, and the walk keeps the last quarter of it.
        return .{ .floor = low + (here - low) / share_den * (share_den - share_num) };
    }

    /// A guard that lets the walk use `bytes` of the stack from the caller's frame, however much the thread has: for
    /// tests, which cannot depend on how deep the C parser before the walk can go.
    pub fn within(bytes: usize) Guard {
        return .{ .floor = @frameAddress() -| bytes };
    }

    /// Whether the walk may go one frame deeper. Cheap enough for every call.
    pub inline fn ok(self: Guard) bool {
        return @frameAddress() > self.floor;
    }
};

/// The lowest address of the running thread's stack, from the OS; null where it cannot be asked.
fn stackLow() ?usize {
    if (!builtin.link_libc) return null;
    switch (builtin.os.tag) {
        .linux => {
            var attr: std.c.pthread_attr_t = undefined;
            if (pthread_getattr_np(std.c.pthread_self(), &attr) != 0) return null;
            defer _ = std.c.pthread_attr_destroy(&attr);
            var addr: ?*anyopaque = null;
            var size: usize = 0;
            if (pthread_attr_getstack(&attr, &addr, &size) != 0) return null;
            return @intFromPtr(addr orelse return null);
        },
        .macos, .ios, .tvos, .watchos, .visionos => {
            const thread = std.c.pthread_self();
            // macOS gives the stack's highest address and its size.
            const top = @intFromPtr(pthread_get_stackaddr_np(thread) orelse return null);
            const size = pthread_get_stacksize_np(thread);
            return top -| size;
        },
        else => return null,
    }
}

/// A walk's bytes where the thread cannot be asked: the process's stack limit, as if the walk starts near the top of a
/// stack of that size (true of a main thread), else the last resort.
fn fallbackBytes() usize {
    const limit = std.posix.getrlimit(.STACK) catch return last_resort_bytes;
    if (limit.cur == std.posix.RLIM.INFINITY or limit.cur == 0) return last_resort_bytes;
    return @intCast(@min(limit.cur / share_den * share_num, std.math.maxInt(usize)));
}

/// Dives one frame at a time until the guard stops it; the depth reached.
fn dive(guard: Guard, depth: usize) usize {
    if (!guard.ok()) return depth;
    var pad: [128]u8 = undefined;
    @memset(&pad, @truncate(depth));
    std.mem.doNotOptimizeAway(&pad);
    return dive(guard, depth + 1) +% (pad[0] & 0);
}

fn diveFrom(out: *usize) void {
    out.* = dive(.start(), 0);
}

test "a walk stops at the stack the thread has, never at a depth chosen in advance" {
    if (!builtin.link_libc or builtin.os.tag != .linux and !builtin.os.tag.isDarwin()) return error.SkipZigTest;
    // The same walk on a small and a large thread stack: the budget follows the stack, and both end by refusal.
    var small: usize = 0;
    var large: usize = 0;
    const t1 = try std.Thread.spawn(.{ .stack_size = 1 << 20 }, diveFrom, .{&small});
    t1.join();
    const t2 = try std.Thread.spawn(.{ .stack_size = 16 << 20 }, diveFrom, .{&large});
    t2.join();
    try std.testing.expect(small > 100);
    try std.testing.expect(large > small * 8);
    // And on this thread's own stack.
    const top = Guard.start();
    try std.testing.expect(top.ok());
    try std.testing.expect(dive(top, 0) > 100);
}
