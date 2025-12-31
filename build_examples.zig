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
    // Always add the Arrow shared library (for Python integration)
    addArrowLib(b, zpq_mod, target, optimize);

    if (!install_examples) return;

    const examples = [_]struct {
        name: []const u8,
        path: []const u8,
        needs_zpq: bool,
    }{
        .{ .name = "lambda-01-minimal", .path = "examples/lambda/01-minimal/main.zig", .needs_zpq = false },
        .{ .name = "lambda-02-dns-warming", .path = "examples/lambda/02-dns-warming/main.zig", .needs_zpq = true },
        .{ .name = "lambda-03-warm-s3", .path = "examples/lambda/03-warm-s3/main.zig", .needs_zpq = true },
        .{ .name = "lambda-04-scan-benchmark", .path = "examples/lambda/04-scan-benchmark/main.zig", .needs_zpq = true },
        .{ .name = "lambda-05-filter-s3", .path = "examples/lambda/05-filter-s3/main.zig", .needs_zpq = true },
    };

    for (examples) |example| {
        const mod = b.createModule(.{
            .root_source_file = b.path(example.path),
            .target = target,
            .optimize = optimize,
        });

        if (example.needs_zpq) {
            mod.addImport("zpq", zpq_mod);
            mod.addImport("xev", libxev_mod);
            mod.addImport("boring_tls", boring_tls_mod);
        }

        const exe = b.addExecutable(.{
            .name = "bootstrap",
            .root_module = mod,
        });
        exe.root_module.linkSystemLibrary("c", .{});

        // Install to zig-out/lambda/{example_name}/bootstrap
        const install = b.addInstallArtifact(exe, .{
            .dest_dir = .{ .override = .{ .custom = b.fmt("lambda/{s}", .{example.name}) } },
        });

        const step_name = b.fmt("example-{s}", .{example.name});
        const step = b.step(step_name, b.fmt("Build {s} example", .{example.name}));
        step.dependOn(&install.step);
    }
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
