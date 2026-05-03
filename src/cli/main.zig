//! ZPQ CLI binary entry point.
//!
//! Targets: native Linux (io_uring via libxev), macOS (kqueue), Windows
//! (handled by libxev where applicable). Hot paths assume the io_uring
//! backend is available; the Lambda binary lives in src/lambda/main.zig
//! and excludes io_uring code at compile time.
//!
//! NOTE: libxev's io_uring backend is currently incompatible with
//! Zig 0.16.0 (uses removed `posix.clock_gettime`). Once we start
//! porting hot-path code into core/io, we'll either patch the
//! vendored fork or bump to a newer libxev commit. Until then the
//! CLI is a placeholder so the build stays green.

const std = @import("std");
const zpq = @import("zpq");

pub fn main(init: std.process.Init.Minimal) !void {
    _ = init;

    // Touch a core symbol so the import isn't pruned. Replaced as soon as
    // the v2 pipeline is wired into core.
    _ = zpq.io.strategy.MemoryReader;

    std.debug.print("zpq cli: placeholder (libxev wiring TBD).\n", .{});
}
