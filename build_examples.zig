const std = @import("std");

pub fn addExamples(
    b: *std.Build,
    zpq_mod: *std.Build.Module,
    target: std.Build.ResolvedTarget,
    optimize: std.builtin.OptimizeMode,
    libxev_mod: *std.Build.Module,
    boring_tls_mod: *std.Build.Module,
    install_examples: bool,
) void {
    _ = libxev_mod;
    _ = boring_tls_mod;
    _ = install_examples;

    // Always add the Arrow shared library (for Python integration)
    addArrowLib(b, zpq_mod, target, optimize);

    // Note: Lambda examples have been removed. The zpq binary itself
    // can be used directly as a Lambda bootstrap - just rename to 'bootstrap'
    // and zip. See examples/serverless/README.md for details.
}

/// Build libzpq_arrow shared library for Python/ctypes integration
fn addArrowLib(
    b: *std.Build,
    zpq_mod: *std.Build.Module,
    target: std.Build.ResolvedTarget,
    optimize: std.builtin.OptimizeMode,
) void {
    // Always build with ReleaseFast for performance
    _ = optimize;
    _ = zpq_mod;

    // Get dependencies for building zpq with ReleaseFast
    const libxev_dep = b.dependency("libxev", .{
        .target = target,
        .optimize = .ReleaseFast,
    });
    const boring_tls_dep = b.dependency("boring_tls", .{
        .target = target,
        .optimize = .ReleaseFast,
        .@"use-prebuilt" = true,
    });

    // Create ReleaseFast zpq module
    const zpq_fast = b.addModule("zpq_fast", .{
        .root_source_file = b.path("src/zpq.zig"),
        .target = target,
        .optimize = .ReleaseFast,
        .imports = &.{
            .{ .name = "xev", .module = libxev_dep.module("xev") },
            .{ .name = "boring_tls", .module = boring_tls_dep.module("boring_tls") },
        },
    });

    const arrow_mod = b.createModule(.{
        .root_source_file = b.path("examples/python_arrow/zpq_arrow.zig"),
        .target = target,
        .optimize = .ReleaseFast,
    });
    arrow_mod.addImport("zpq", zpq_fast);

    const lib = b.addLibrary(.{
        .linkage = .dynamic,
        .name = "zpq_arrow",
        .root_module = arrow_mod,
    });

    // Install to examples/python_arrow/ for easy Python access
    const install = b.addInstallArtifact(lib, .{
        .dest_dir = .{ .override = .{ .custom = "../examples/python_arrow" } },
    });

    const step = b.step("arrow-lib", "Build libzpq_arrow.dylib for Python integration");
    step.dependOn(&install.step);
}
