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

pub fn main(init: std.process.Init.Minimal) !void {
    _ = init;

    // Touch a core symbol so the import isn't pruned.
    _ = zpq.io.strategy.MemoryReader;

    std.debug.print("zpq lambda: epoll-only runtime ready.\n", .{});
}
