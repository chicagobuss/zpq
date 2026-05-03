//! ZPQ Lambda binary entry point.
//!
//! Constraints established by docs/lambda_capabilities.md:
//!   - io_uring is unavailable (AWS seccomp returns ENOSYS).
//!   - Kernel is AL2 5.10, not AL2023 6.x — no epoll_pwait2, no clone3.
//!   - epoll/eventfd2/timerfd_create/signalfd4/mlock are allowed.
//!   - SO_ZEROCOPY and TCP_FASTOPEN setsockopt allowed.
//!
//! This binary intentionally excludes io_uring code at compile time via
//! `build_options.lambda`. The in-tree epoll backend is the only event
//! loop driver linked here.

const std = @import("std");
const zpq = @import("zpq");

const Loop = zpq.io.loop.Loop;
const Completion = zpq.io.loop.Completion;
const Result = zpq.io.loop.Result;

pub fn main(init: std.process.Init.Minimal) !void {
    _ = init;

    var gpa: std.heap.DebugAllocator(.{}) = .{};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    // Smoke-test the event loop: schedule a 1 ms timer, run until it
    // fires, verify it did. Confirms epoll is actually usable in Lambda.
    var loop = try Loop.init(allocator);
    defer loop.deinit();

    var fired: bool = false;
    var c: Completion = .{
        .op = .{ .timer = .{ .ns_from_now = std.time.ns_per_ms } },
        .userdata = &fired,
        .callback = onTimer,
    };
    loop.submit(&c);

    var iters: u32 = 100;
    while (loop.active() > 0 and iters > 0) : (iters -= 1) {
        try loop.run_for_ns(10 * std.time.ns_per_ms);
    }

    if (!fired) {
        std.debug.print("zpq lambda: timer did not fire (epoll smoke test FAILED)\n", .{});
        return error.SmokeTestFailed;
    }
    std.debug.print("zpq lambda: epoll smoke test passed; loop is alive.\n", .{});
}

fn onTimer(ud: ?*anyopaque, _: *Loop, _: *Completion, _: Result) void {
    const fired: *bool = @ptrCast(@alignCast(ud.?));
    fired.* = true;
}
