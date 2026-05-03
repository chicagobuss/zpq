//! ZPQ CLI binary entry point.
//!
//! Targets: native Linux (io_uring), macOS (kqueue). Hot paths assume the
//! io_uring backend is available; the Lambda binary (src/lambda/main.zig)
//! excludes io_uring code at compile time via build_options.lambda.
//!
//! No event loop is wired in yet — the CLI is a placeholder until the
//! v2 pipeline lands. libxev will be added back as a build.zig.zon
//! dependency at the same time the code that uses it lands.

const std = @import("std");
const zpq = @import("zpq");

pub fn main(init: std.process.Init.Minimal) !void {
    _ = init;

    // Touch a core symbol so the import isn't pruned. Replaced as soon as
    // the v2 pipeline is wired into core.
    _ = zpq.io.strategy.MemoryReader;

    std.debug.print("zpq cli: placeholder.\n", .{});
}
