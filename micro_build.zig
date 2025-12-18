const std = @import("std");

pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});

    // Dependencies
    const libxev_dep = b.dependency("libxev", .{ .target = target, .optimize = optimize });
    const boring_tls_dep = b.dependency("boring_tls", .{ .target = target, .optimize = optimize });

    const xev_mod = libxev_dep.module("xev");
    const boring_mod = boring_tls_dep.module("boring_tls");

    // --- Microtests Definition ---

    // 1. test_boring_connect
    {
        const mod = b.createModule(.{
            .root_source_file = b.path("tests/io/test_boring_connect.zig"),
            .target = target,
            .optimize = optimize,
        });
        mod.addImport("xev", xev_mod);
        mod.addImport("boring_tls", boring_mod);

        const exe = b.addExecutable(.{
            .name = "test_boring_connect",
            .root_module = mod,
        });

        const run = b.addRunArtifact(exe);
        const step = b.step("test-boring-connect", "Run BoringTLS connection test");
        step.dependOn(&run.step);
    }

    // 2. bench_ping_pongs
    {
        const mod = b.createModule(.{
            .root_source_file = b.path("tests/bench/ping_pongs.zig"),
            .target = target,
            .optimize = .ReleaseFast,
        });
        mod.addImport("xev", xev_mod);

        const exe = b.addExecutable(.{
            .name = "bench_ping_pongs",
            .root_module = mod,
        });

        const run = b.addRunArtifact(exe);
        const step = b.step("bench-ping-pongs", "Run libxev ping-pong benchmark");
        step.dependOn(&run.step);
    }

    // 3. test_s3_head
    {
        const mod = b.createModule(.{
            .root_source_file = b.path("tests/io/test_s3_head.zig"),
            .target = target,
            .optimize = optimize,
        });
        mod.addImport("xev", xev_mod);
        mod.addImport("boring_tls", boring_mod);

        const exe = b.addExecutable(.{
            .name = "test_s3_head",
            .root_module = mod,
        });

        const run = b.addRunArtifact(exe);
        const step = b.step("test-s3-head", "Run S3 HEAD request test");
        step.dependOn(&run.step);
    }
}
