//! ZPQ CLI binary entry point.
//!
//! Targets: native Linux (io_uring) and macOS (kqueue). The event loop
//! is in-tree (`src/io/`), comptime-selected by target. The Lambda
//! binary (src/lambda/main.zig) excludes io_uring code via
//! `build_options.lambda`.
//!
//! No event loop is wired in yet — the CLI is a placeholder until the
//! first hot-path code lands.

const std = @import("std");
const zpq = @import("zpq");

pub fn main(init: std.process.Init.Minimal) !void {
    _ = init;

    // Touch a core symbol so the import isn't pruned. Replaced as soon as
    // the v2 pipeline is wired into core.
    _ = zpq.io.strategy.MemoryReader;

    std.debug.print("zpq cli: placeholder.\n", .{});
}
