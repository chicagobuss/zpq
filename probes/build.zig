const std = @import("std");

/// Standalone build system for probes.
///
/// This allows building and running individual probes without rebuilding
/// the entire zpq project. Uses the parent project's dependencies.
///
/// Usage:
///   cd probes
///   zig build                        # list available probes
///   zig build probe_conn_reuse       # build specific probe
///   zig build run-probe_conn_reuse   # build and run
///
pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});

    // Get dependencies from parent project
    const parent_dep = b.dependency("zpq_parent", .{
        .target = target,
        .optimize = optimize,
    });

    const zpq_mod = parent_dep.module("zpq");
    const xev_mod = parent_dep.module("xev");

    // Register each probe explicitly (comptime string concat)
    addProbe(b, target, optimize, zpq_mod, xev_mod, "probe_conn_reuse");
    addProbe(b, target, optimize, zpq_mod, xev_mod, "probe_conn_cost");
    addProbe(b, target, optimize, zpq_mod, xev_mod, "probe_pool_cleanup");
    addProbe(b, target, optimize, zpq_mod, xev_mod, "probe_multi_conn");
    addProbe(b, target, optimize, zpq_mod, xev_mod, "probe_tls_echo");
    addProbe(b, target, optimize, zpq_mod, xev_mod, "probe_s3_head_hang");
    addProbe(b, target, optimize, zpq_mod, xev_mod, "probe_xev_s3_head");
    addProbe(b, target, optimize, zpq_mod, xev_mod, "probe_fast_feedback");
    addProbe(b, target, optimize, zpq_mod, xev_mod, "shootout_tls_throughput");
    addProbe(b, target, optimize, zpq_mod, xev_mod, "probe_batch_reuse");
    addProbe(b, target, optimize, zpq_mod, xev_mod, "test_simd_null_bug");
    addProbe(b, target, optimize, zpq_mod, xev_mod, "test_stats");
    addProbe(b, target, optimize, zpq_mod, xev_mod, "test_lazy_materialization");
    addProbe(b, target, optimize, zpq_mod, xev_mod, "test_skip_performance");
    addProbe(b, target, optimize, zpq_mod, xev_mod, "bench_skip_breakdown");
}

fn addProbe(
    b: *std.Build,
    target: std.Build.ResolvedTarget,
    optimize: std.builtin.OptimizeMode,
    zpq_mod: *std.Build.Module,
    xev_mod: *std.Build.Module,
    comptime name: []const u8,
) void {
    const exe = b.addExecutable(.{
        .name = name,
        .root_module = b.createModule(.{
            .root_source_file = b.path(name ++ ".zig"),
            .target = target,
            .optimize = optimize,
        }),
    });

    exe.root_module.addImport("zpq", zpq_mod);
    exe.root_module.addImport("xev", xev_mod);

    // Install step
    const install = b.addInstallArtifact(exe, .{});
    const install_step = b.step(name, "Build " ++ name);
    install_step.dependOn(&install.step);

    // Run step
    const run = b.addRunArtifact(exe);
    run.step.dependOn(&install.step);
    if (b.args) |args| {
        run.addArgs(args);
    }
    const run_step = b.step("run-" ++ name, "Run " ++ name);
    run_step.dependOn(&run.step);
}
