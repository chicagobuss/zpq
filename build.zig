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

    const probe_async_log = b.addExecutable(.{
        .name = "probe-async-log",
        .root_module = b.createModule(.{
            .root_source_file = b.path("probes/probe_async_log.zig"),
            .target = target,
            .optimize = optimize,
            .imports = &.{
                .{ .name = "zpq", .module = zpq_mod },
                .{ .name = "xev", .module = libxev_mod },
            },
        }),
    });
    b.installArtifact(probe_async_log);
    const run_probe_log = b.addRunArtifact(probe_async_log);
    const probe_log_step = b.step("probe-async-log", "Run the async logger probe");
    probe_log_step.dependOn(&run_probe_log.step);

    const check_probe_async_log = b.addExecutable(.{
        .name = "probe-async-log",
        .root_module = probe_async_log.root_module,
    });
    check.dependOn(&check_probe_async_log.step);

    // Probe: Xev Lifecycle
    const probe_lifecycle = b.addExecutable(.{
        .name = "probe-xev-lifecycle",
        .root_module = b.createModule(.{
            .root_source_file = b.path("probes/probe_xev_lifecycle.zig"),
            .target = target,
            .optimize = optimize,
            .imports = &.{
                .{ .name = "xev", .module = libxev_mod },
            },
        }),
    });
    probe_lifecycle.root_module.omit_frame_pointer = !no_omit_frame_pointer;
    b.installArtifact(probe_lifecycle);

    const run_cmd_lifecycle = b.addRunArtifact(probe_lifecycle);
    run_cmd_lifecycle.step.dependOn(b.getInstallStep());
    if (b.args) |args| {
        run_cmd_lifecycle.addArgs(args);
    }
    const run_step_lifecycle = b.step("probe-xev-lifecycle", "Run the xev lifecycle probe");
    run_step_lifecycle.dependOn(&run_cmd_lifecycle.step);

    // Probe: S3 Retry/Leak
    const probe_retry = b.addExecutable(.{
        .name = "probe-s3-retry-leak",
        .root_module = b.createModule(.{
            .root_source_file = b.path("probes/probe_s3_retry_leak.zig"),
            .target = target,
            .optimize = optimize,
            .imports = &.{
                .{ .name = "zpq", .module = zpq_mod },
                .{ .name = "xev", .module = libxev_mod },
            },
        }),
    });
    probe_retry.root_module.omit_frame_pointer = !no_omit_frame_pointer;
    b.installArtifact(probe_retry);

    const run_cmd_retry = b.addRunArtifact(probe_retry);
    run_cmd_retry.step.dependOn(b.getInstallStep());
    if (b.args) |args| {
        run_cmd_retry.addArgs(args);
    }
    const run_step_retry = b.step("probe-s3-retry-leak", "Run the S3 retry/leak probe");
    run_step_retry.dependOn(&run_cmd_retry.step);

    // Probe: S3 Prefetch (parallel connection pool)
    const probe_prefetch = b.addExecutable(.{
        .name = "probe-s3-prefetch",
        .root_module = b.createModule(.{
            .root_source_file = b.path("probes/probe_s3_prefetch.zig"),
            .target = target,
            .optimize = optimize,
            .imports = &.{
                .{ .name = "zpq", .module = zpq_mod },
                .{ .name = "xev", .module = libxev_mod },
            },
        }),
    });
    probe_prefetch.root_module.omit_frame_pointer = !no_omit_frame_pointer;
    b.installArtifact(probe_prefetch);

    const run_cmd_prefetch = b.addRunArtifact(probe_prefetch);
    run_cmd_prefetch.step.dependOn(b.getInstallStep());
    if (b.args) |args| {
        run_cmd_prefetch.addArgs(args);
    }
    const run_step_prefetch = b.step("probe-s3-prefetch", "Run the S3 prefetch probe");
    run_step_prefetch.dependOn(&run_cmd_prefetch.step);

    const probe_columnar = b.addExecutable(.{
        .name = "probe-columnar-verify",
        .root_module = b.createModule(.{
            .root_source_file = b.path("probes/probe_columnar_verify.zig"),
            .target = target,
            .optimize = optimize,
            .imports = &.{
                .{ .name = "zpq", .module = zpq_mod },
            },
        }),
    });
    b.installArtifact(probe_columnar);
    const run_probe_columnar = b.addRunArtifact(probe_columnar);
    const probe_columnar_step = b.step("probe-columnar-verify", "Run the columnar verification probe");
    probe_columnar_step.dependOn(&run_probe_columnar.step);

    const probe_bench = b.addExecutable(.{
        .name = "probe-benchmark-columnar",
        .root_module = b.createModule(.{
            .root_source_file = b.path("probes/probe_benchmark_columnar.zig"),
            .target = target,
            .optimize = optimize,
            .imports = &.{
                .{ .name = "zpq", .module = zpq_mod },
            },
        }),
    });
    b.installArtifact(probe_bench);
    const run_probe_bench = b.addRunArtifact(probe_bench);
    const probe_bench_step = b.step("probe-benchmark-columnar", "Run the benchmark columnar probe");
    probe_bench_step.dependOn(&run_probe_bench.step);

    const probe_integrated = b.addExecutable(.{
        .name = "probe-integrated-pipeline",
        .root_module = b.createModule(.{
            .root_source_file = b.path("probes/probe_integrated_pipeline.zig"),
            .target = target,
            .optimize = optimize,
            .imports = &.{
                .{ .name = "zpq", .module = zpq_mod },
            },
        }),
    });
    b.installArtifact(probe_integrated);
    const run_probe_integrated = b.addRunArtifact(probe_integrated);
    const probe_integrated_step = b.step("probe-integrated-pipeline", "Run the integrated pipeline probe");
    probe_integrated_step.dependOn(&run_probe_integrated.step);

    const probe_parallel = b.addExecutable(.{
        .name = "probe-parallel-pipeline",
        .root_module = b.createModule(.{
            .root_source_file = b.path("probes/probe_parallel_pipeline.zig"),
            .target = target,
            .optimize = optimize,
            .imports = &.{
                .{ .name = "zpq", .module = zpq_mod },
            },
        }),
    });
    b.installArtifact(probe_parallel);
    const run_probe_parallel = b.addRunArtifact(probe_parallel);
    const probe_parallel_step = b.step("probe-parallel-pipeline", "Run the parallel pipeline probe");
    probe_parallel_step.dependOn(&run_probe_parallel.step);

    const probe_filter = b.addExecutable(.{
        .name = "probe-filter",
        .root_module = b.createModule(.{
            .root_source_file = b.path("probes/probe_filter.zig"),
            .target = target,
            .optimize = optimize,
            .imports = &.{
                .{ .name = "zpq", .module = zpq_mod },
            },
        }),
    });
    b.installArtifact(probe_filter);
    const run_probe_filter = b.addRunArtifact(probe_filter);
    const probe_filter_step = b.step("probe-filter", "Run the filter benchmark probe");
    probe_filter_step.dependOn(&run_probe_filter.step);
}
