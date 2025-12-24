const std = @import("std");

pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});
    const use_prebuilt = b.option(bool, "use-prebuilt", "Use pre-built static libraries if available") orelse true;

    // Dependencies
    const libxev_dep = b.dependency("libxev", .{
        .target = target,
        .optimize = optimize,
    });
    const libxev_mod = libxev_dep.module("xev");

    const boring_tls_dep = b.dependency("boring_tls", .{
        .target = target,
        .optimize = optimize,
        .@"use-prebuilt" = use_prebuilt,
    });
    const boring_tls_mod = boring_tls_dep.module("boring_tls");

    // Create the 'zpq' module
    const zpq_mod = b.createModule(.{
        .root_source_file = b.path("src/zpq.zig"),
        .target = target,
        .optimize = optimize,
    });
    // Link new IO stack deps
    zpq_mod.addImport("xev", libxev_mod);
    zpq_mod.addImport("boring_tls", boring_tls_mod);

    // Minish Module
    const minish = b.addModule("minish", .{
        .root_source_file = b.path("vendor/minish/src/lib.zig"),
    });

    // Create the exe module
    const exe_mod = b.createModule(.{
        .root_source_file = b.path("src/main.zig"),
        .target = target,
        .optimize = optimize,
    });
    exe_mod.addImport("zpq", zpq_mod);
    exe_mod.linkSystemLibrary("c", .{});

    const install_all = b.option(bool, "all", "Build all auxiliary tests, probes, and benchmarks") orelse false;

    const exe = b.addExecutable(.{
        .name = "zpq",
        .root_module = exe_mod,
    });

    b.installArtifact(exe);

    const run_cmd = b.addRunArtifact(exe);
    run_cmd.step.dependOn(b.getInstallStep());

    if (b.args) |args| {
        run_cmd.addArgs(args);
    }

    const run_step = b.step("run", "Run the app");
    run_step.dependOn(&run_cmd.step);

    const verbose_tests = b.option(bool, "verbose-tests", "Use a detailed test runner") orelse false;

    // Unit Tests
    const lib_unit_tests = b.addTest(.{
        .root_module = zpq_mod,
        .test_runner = if (verbose_tests) .{ .path = b.path("tests/verbose_runner.zig"), .mode = .simple } else null,
    });

    const run_lib_unit_tests = b.addRunArtifact(lib_unit_tests);
    if (b.args) |args| {
        run_lib_unit_tests.addArgs(args);
    }

    const test_step = b.step("test", "Run unit tests");
    test_step.dependOn(&run_lib_unit_tests.step);

    const check_step = b.step("check", "Check compilation");
    check_step.dependOn(&exe.step);
    check_step.dependOn(&lib_unit_tests.step);

    // Auxiliary Tools (Probes, Benchmarks, Fuzzers)
    const build_tests = @import("build_tests.zig");
    build_tests.addAuxiliaryTools(
        b,
        zpq_mod,
        target,
        optimize,
        libxev_mod,
        boring_tls_mod,
        minish,
        install_all,
    );

    // Examples
    const install_examples = b.option(bool, "examples", "Build examples (including Lambda bootstrap)") orelse false;
    const build_examples = @import("build_examples.zig");
    build_examples.addExamples(
        b,
        zpq_mod,
        target,
        optimize,
        libxev_mod,
        install_examples,
    );
}
