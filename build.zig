const std = @import("std");

/// Get the git commit hash at build time
fn getGitHash(b: *std.Build) []const u8 {
    var code: u8 = 0;
    const result = b.runAllowFail(
        &.{ "git", "rev-parse", "--short=7", "HEAD" },
        &code,
        .Inherit,
    ) catch return "unknown";

    if (code != 0) return "unknown";
    const hash = std.mem.trim(u8, result, &std.ascii.whitespace);

    const dirty_result = b.runAllowFail(
        &.{ "git", "status", "--porcelain" },
        &code,
        .Inherit,
    ) catch return hash;

    if (code != 0) return hash;
    if (dirty_result.len > 0) {
        return b.fmt("{s}-dirty", .{hash});
    }
    return hash;
}

pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});
    const use_prebuilt = b.option(bool, "use-prebuilt", "Use pre-built static libraries if available") orelse true;
    const no_omit_frame_pointer = b.option(bool, "no-omit-frame-pointer", "Don't omit the frame pointer") orelse false;

    // Feature flags
    const enable_zstd_compression = b.option(bool, "zstd-compression", "Enable ZSTD compression (requires libzstd)") orelse true;

    const git_hash = getGitHash(b);
    const build_options = b.addOptions();
    build_options.addOption([]const u8, "git_hash", git_hash);

    const zpq_options = b.addOptions();
    zpq_options.addOption(bool, "enable_zstd_compression", enable_zstd_compression);

    // Dependencies
    const libxev_dep = b.dependency("libxev", .{
        .target = target,
        .optimize = optimize,
    });
    const libxev_mod = libxev_dep.module("xev");

    const boring_tls_dep = b.dependency("boring_tls", .{
        .target = target,
        .optimize = .ReleaseFast,
        .@"use-prebuilt" = use_prebuilt,
    });
    const boring_tls_mod = boring_tls_dep.module("boring_tls");

    // ZPQ Core Module
    const zpq_mod = b.addModule("zpq", .{
        .root_source_file = b.path("src/zpq.zig"),
        .target = target,
        .optimize = optimize,
        .imports = &.{
            .{ .name = "xev", .module = libxev_mod },
            .{ .name = "boring_tls", .module = boring_tls_mod },
            .{ .name = "zpq_options", .module = zpq_options.createModule() },
        },
    });

    if (enable_zstd_compression) {
        const zstd_dep = b.dependency("zstd", .{
            .target = target,
            .optimize = optimize,
        });
        zpq_mod.linkLibrary(zstd_dep.artifact("zstd"));
    }

    // Expose xev for direct use in probes/tests
    b.modules.put("xev", libxev_mod) catch @panic("OOM");

    // Main Executable
    const exe = b.addExecutable(.{
        .name = "zpq",
        .root_module = b.createModule(.{
            .root_source_file = b.path("src/main.zig"),
            .target = target,
            .optimize = optimize,
            .link_libc = true,
            .imports = &.{
                .{ .name = "zpq", .module = zpq_mod },
                .{ .name = "xev", .module = libxev_mod },
            },
        }),
    });
    exe.root_module.omit_frame_pointer = !no_omit_frame_pointer;
    b.installArtifact(exe);

    // Run Command
    const run_cmd = b.addRunArtifact(exe);
    run_cmd.step.dependOn(b.getInstallStep());
    if (b.args) |args| {
        run_cmd.addArgs(args);
    }
    const run_step = b.step("run", "Run the app");
    run_step.dependOn(&run_cmd.step);

    // Unit Tests
    const lib_unit_tests = b.addTest(.{
        .root_module = zpq_mod,
    });
    const run_lib_unit_tests = b.addRunArtifact(lib_unit_tests);
    const test_step = b.step("test", "Run unit tests");
    test_step.dependOn(&run_lib_unit_tests.step);

    // Probes
    const probe_clean_s3 = b.addExecutable(.{
        .name = "probe-clean-s3",
        .root_module = b.createModule(.{
            .root_source_file = b.path("probes/probe_clean_s3.zig"),
            .target = target,
            .optimize = optimize,
            .link_libc = true,
            .imports = &.{
                .{ .name = "zpq", .module = zpq_mod },
                .{ .name = "xev", .module = libxev_mod },
            },
        }),
    });
    probe_clean_s3.root_module.omit_frame_pointer = !no_omit_frame_pointer;
    b.installArtifact(probe_clean_s3);

    const run_probe_s3 = b.addRunArtifact(probe_clean_s3);
    const probe_s3_step = b.step("probe-s3", "Run the clean S3 integration probe");
    probe_s3_step.dependOn(&run_probe_s3.step);

    const check = b.step("check", "Check compilation");
    const check_exe = b.addExecutable(.{
        .name = "zpq",
        .root_module = exe.root_module,
    });
    check.dependOn(&check_exe.step);

    const check_probe_s3 = b.addExecutable(.{
        .name = "probe-clean-s3",
        .root_module = probe_clean_s3.root_module,
    });
    check.dependOn(&check_probe_s3.step);

    const probe_filter_scan = b.addExecutable(.{
        .name = "probe-filter-scan",
        .root_module = b.createModule(.{
            .root_source_file = b.path("probes/probe_filter_scan.zig"),
            .target = target,
            .optimize = optimize,
            .imports = &.{
                .{ .name = "zpq", .module = zpq_mod },
            },
        }),
    });
    check.dependOn(&probe_filter_scan.step);
}
