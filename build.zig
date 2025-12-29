const std = @import("std");

/// Get the git commit hash at build time
fn getGitHash(b: *std.Build) []const u8 {
    var code: u8 = 0;

    // Try to get git hash via build-time command
    const result = b.runAllowFail(
        &.{ "git", "rev-parse", "--short=7", "HEAD" },
        &code,
        .Inherit,
    ) catch return "unknown";

    if (code != 0) return "unknown";

    const hash = std.mem.trim(u8, result, &std.ascii.whitespace);

    // Check if working directory is dirty
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

    // Get git hash for build info
    const git_hash = getGitHash(b);

    // Create build_options module with git hash for benchmarks
    const build_options = b.addOptions();
    build_options.addOption([]const u8, "git_hash", git_hash);

    // Dependencies
    const libxev_dep = b.dependency("libxev", .{
        .target = target,
        .optimize = optimize,
    });
    const libxev_mod = libxev_dep.module("xev");

    // Always build boring_tls with ReleaseFast:
    // 1. Crypto libraries need optimization for performance
    // 2. Debug builds enable UBSAN which requires runtime support not available in CI
    const boring_tls_dep = b.dependency("boring_tls", .{
        .target = target,
        .optimize = .ReleaseFast,
        .@"use-prebuilt" = use_prebuilt,
    });
    const boring_tls_mod = boring_tls_dep.module("boring_tls");

    // Create the 'zpq' module (exported for probes and other dependents)
    const zpq_mod = b.addModule("zpq", .{
        .root_source_file = b.path("src/zpq.zig"),
        .target = target,
        .optimize = optimize,
        .imports = &.{
            .{ .name = "xev", .module = libxev_mod },
            .{ .name = "boring_tls", .module = boring_tls_mod },
        },
    });

    // Also expose xev for probes that need direct access
    // Re-export the dependency's module rather than creating a new one
    b.modules.put("xev", libxev_mod) catch @panic("OOM");

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
    exe_mod.addImport("xev", libxev_mod);
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

    // Experimental I/O Stack Tests (libxev + boring_tls)
    const enable_experimental = b.option(bool, "experimental", "Enable experimental tests/benchmarks") orelse false;

    if (enable_experimental) {
        // MinIO Fixtures
        const minio_fixtures = b.addModule("minio_fixtures", .{
            .root_source_file = b.path("ci/fixtures/minio/fixtures.zig"),
        });

        // 2. test-http-client
        const test_http_client = b.addTest(.{
            .root_module = b.createModule(.{
                .root_source_file = b.path("tests/io/test_http_client.zig"),
                .target = target,
                .optimize = optimize,
            }),
        });
        test_http_client.root_module.addImport("xev", libxev_mod);
        test_http_client.root_module.addImport("zpq", zpq_mod);

        const run_test_http_client = b.addRunArtifact(test_http_client);
        test_step.dependOn(&run_test_http_client.step);

        // 3. test-minio-https
        const test_minio_https = b.addTest(.{
            .root_module = b.createModule(.{
                .root_source_file = b.path("tests/io/test_minio_https.zig"),
                .target = target,
                .optimize = optimize,
            }),
        });
        test_minio_https.root_module.addImport("xev", libxev_mod);
        test_minio_https.root_module.addImport("zpq", zpq_mod);
        const run_test_minio_https = b.addRunArtifact(test_minio_https);
        test_step.dependOn(&run_test_minio_https.step);

        // 4. test-minio-range-get
        const test_minio_range_get = b.addExecutable(.{
            .name = "test-minio-range-get",
            .root_module = b.createModule(.{
                .root_source_file = b.path("tests/integration/test_minio_range_get.zig"),
                .target = target,
                .optimize = optimize,
            }),
        });
        test_minio_range_get.root_module.addImport("xev", libxev_mod);
        test_minio_range_get.root_module.addImport("zpq", zpq_mod);
        test_minio_range_get.root_module.addImport("minio_fixtures", minio_fixtures);

        const run_test_minio_range_get = b.addRunArtifact(test_minio_range_get);
        test_step.dependOn(&run_test_minio_range_get.step);

        // 5. test-xev-s3-source
        const test_xev_s3_source = b.addExecutable(.{
            .name = "test-xev-s3-source",
            .root_module = b.createModule(.{
                .root_source_file = b.path("tests/integration/test_xev_s3_source.zig"),
                .target = target,
                .optimize = optimize,
            }),
        });
        test_xev_s3_source.root_module.addImport("xev", libxev_mod);
        test_xev_s3_source.root_module.addImport("zpq", zpq_mod);
        test_xev_s3_source.root_module.addImport("minio_fixtures", minio_fixtures);
        test_xev_s3_source.root_module.addImport("boring_tls", boring_tls_mod);

        const run_test_xev_s3_source = b.addRunArtifact(test_xev_s3_source);
        test_step.dependOn(&run_test_xev_s3_source.step);
        b.installArtifact(test_xev_s3_source);

        // Create a dedicated step for this test
        const step_xev_s3_source = b.step("test-xev-s3-source", "Run XevS3Source integration test");
        step_xev_s3_source.dependOn(&run_test_xev_s3_source.step);

        // 6. test-parquet-s3
        const test_parquet_s3 = b.addExecutable(.{
            .name = "test-parquet-s3",
            .root_module = b.createModule(.{
                .root_source_file = b.path("tests/integration/test_parquet_s3.zig"),
                .target = target,
                .optimize = optimize,
            }),
        });
        test_parquet_s3.root_module.addImport("xev", libxev_mod);
        test_parquet_s3.root_module.addImport("zpq", zpq_mod);
        test_parquet_s3.root_module.addImport("boring_tls", boring_tls_mod);

        const run_test_parquet_s3 = b.addRunArtifact(test_parquet_s3);
        test_step.dependOn(&run_test_parquet_s3.step);
        b.installArtifact(test_parquet_s3);

        const step_parquet_s3 = b.step("test-parquet-s3", "Run Parquet S3 integration test");
        step_parquet_s3.dependOn(&run_test_parquet_s3.step);

        // 7. bench-s3-cold
        const bench_s3_cold = b.addExecutable(.{
            .name = "bench-s3-cold",
            .root_module = b.createModule(.{
                .root_source_file = b.path("benchmarks/s3_cold_start.zig"),
                .target = target,
                .optimize = optimize,
            }),
        });
        bench_s3_cold.root_module.addImport("zpq", zpq_mod);
        b.installArtifact(bench_s3_cold);

        const run_bench_s3_cold = b.addRunArtifact(bench_s3_cold);
        const step_bench_s3_cold = b.step("bench-s3-cold", "Run S3 cold start baseline benchmark");
        step_bench_s3_cold.dependOn(&run_bench_s3_cold.step);

        // 8. probe-tcp-exit
        const probe_tcp_exit = b.addExecutable(.{
            .name = "probe-tcp-exit",
            .root_module = b.createModule(.{
                .root_source_file = b.path("probes/probe_tcp_close_exit.zig"),
                .target = target,
                .optimize = optimize,
            }),
        });
        probe_tcp_exit.root_module.addImport("xev", libxev_mod);
        b.installArtifact(probe_tcp_exit);

        const run_probe_tcp_exit = b.addRunArtifact(probe_tcp_exit);
        const step_probe_tcp_exit = b.step("probe-tcp-exit", "Run TCP close exit probe");
        step_probe_tcp_exit.dependOn(&run_probe_tcp_exit.step);

        // 9. probe-async-leak
        const probe_async_leak = b.addExecutable(.{
            .name = "probe-async-leak",
            .root_module = b.createModule(.{
                .root_source_file = b.path("probes/probe_async_leak.zig"),
                .target = target,
                .optimize = optimize,
            }),
        });
        probe_async_leak.root_module.addImport("xev", libxev_mod);
        b.installArtifact(probe_async_leak);

        const run_probe_async_leak = b.addRunArtifact(probe_async_leak);
        const step_probe_async_leak = b.step("probe-async-leak", "Run Async leak probe");
        step_probe_async_leak.dependOn(&run_probe_async_leak.step);

        // 10. probe-loop-active
        const probe_loop_active = b.addExecutable(.{
            .name = "probe-loop-active",
            .root_module = b.createModule(.{
                .root_source_file = b.path("probes/probe_loop_active.zig"),
                .target = target,
                .optimize = optimize,
            }),
        });
        probe_loop_active.root_module.addImport("xev", libxev_mod);
        probe_loop_active.root_module.addImport("zpq", zpq_mod);
        b.installArtifact(probe_loop_active);

        const run_probe_loop_active = b.addRunArtifact(probe_loop_active);
        const step_probe_loop_active = b.step("probe-loop-active", "Run Loop active count probe");
        step_probe_loop_active.dependOn(&run_probe_loop_active.step);

        // 11. probe-multi-conn
        const probe_multi_conn = b.addExecutable(.{
            .name = "probe-multi-conn",
            .root_module = b.createModule(.{
                .root_source_file = b.path("probes/probe_multi_conn.zig"),
                .target = target,
                .optimize = optimize,
            }),
        });
        probe_multi_conn.root_module.addImport("xev", libxev_mod);
        probe_multi_conn.root_module.addImport("zpq", zpq_mod);
        b.installArtifact(probe_multi_conn);

        const run_probe_multi_conn = b.addRunArtifact(probe_multi_conn);
        const step_probe_multi_conn = b.step("probe-multi-conn", "Run multi-connection probe");
        step_probe_multi_conn.dependOn(&run_probe_multi_conn.step);

        // Probe for DNS
        const probe_dns_xev = b.addExecutable(.{
            .name = "probe-dns-xev",
            .root_module = b.createModule(.{
                .root_source_file = b.path("probes/probe_dns_xev.zig"),
                .target = target,
                .optimize = optimize,
            }),
        });
        probe_dns_xev.root_module.addImport("xev", libxev_mod);
        probe_dns_xev.root_module.addImport("zpq", zpq_mod);
        b.installArtifact(probe_dns_xev);
    }

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
        build_options,
        check_step,
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
        boring_tls_mod,
        install_examples,
    );
}
