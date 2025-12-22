const std = @import("std");

pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});

    // Dependencies
    const libxev_dep = b.dependency("libxev", .{
        .target = target,
        .optimize = optimize,
    });
    const libxev_mod = libxev_dep.module("xev");

    const boring_tls_dep = b.dependency("boring_tls", .{
        .target = target,
        .optimize = optimize,
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

    const exe = b.addExecutable(.{
        .name = "zigaws",
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

    // Unit Tests
    const lib_unit_tests = b.addTest(.{
        .root_module = zpq_mod,
    });

    const run_lib_unit_tests = b.addRunArtifact(lib_unit_tests);

    const test_step = b.step("test", "Run unit tests");
    test_step.dependOn(&run_lib_unit_tests.step);

    // I/O Integration Tests
    const test_io_step = b.step("test-io", "Run I/O integration tests");

    // Fast Feedback Probe
    {
        const mod = b.createModule(.{
            .root_source_file = b.path("probes/probe_fast_feedback.zig"),
            .target = target,
            .optimize = optimize,
        });
        mod.addImport("zpq", zpq_mod);

        const pff_exe = b.addExecutable(.{
            .name = "probe-fast-feedback",
            .root_module = mod,
        });
        pff_exe.linkLibC();

        const run = b.addRunArtifact(pff_exe);
        const step = b.step("probe-fast-feedback", "Run fast feedback probe");
        step.dependOn(&run.step);
    }

    // test_async_request
    const test_async_request_mod = b.createModule(.{
        .root_source_file = b.path("tests/io/test_async_request.zig"),
        .target = target,
        .optimize = optimize,
    });
    test_async_request_mod.addImport("zpq", zpq_mod);
    test_async_request_mod.addImport("xev", libxev_mod);

    const test_async_request_exe = b.addExecutable(.{
        .name = "test_async_request",
        .root_module = test_async_request_mod,
    });
    const run_test_async_request = b.addRunArtifact(test_async_request_exe);
    test_io_step.dependOn(&run_test_async_request.step);

    // test_event_loop
    const test_event_loop_mod = b.createModule(.{
        .root_source_file = b.path("tests/io/test_event_loop.zig"),
        .target = target,
        .optimize = optimize,
    });
    test_event_loop_mod.addImport("zpq", zpq_mod);

    const test_event_loop_exe = b.addExecutable(.{
        .name = "test_event_loop",
        .root_module = test_event_loop_mod,
    });
    const run_test_event_loop = b.addRunArtifact(test_event_loop_exe);
    test_io_step.dependOn(&run_test_event_loop.step);

    const test_event_loop_step = b.step("test-event-loop", "Run EventLoop + AsyncRequest integration test");
    test_event_loop_step.dependOn(&run_test_event_loop.step);

    // test_async_source
    const test_async_source_mod = b.createModule(.{
        .root_source_file = b.path("tests/io/test_async_source.zig"),
        .target = target,
        .optimize = optimize,
    });
    test_async_source_mod.addImport("zpq", zpq_mod);

    const test_async_source_exe = b.addExecutable(.{
        .name = "test_async_source",
        .root_module = test_async_source_mod,
    });
    const run_test_async_source = b.addRunArtifact(test_async_source_exe);
    test_io_step.dependOn(&run_test_async_source.step);

    const test_async_source_step = b.step("test-async-source", "Run AsyncS3Source integration test");
    test_async_source_step.dependOn(&run_test_async_source.step);

    // raw_s3_source (restored)
    const raw_s3_source_mod = b.createModule(.{
        .root_source_file = b.path("src/zpq/io/s3/raw_s3_source.zig"),
        .target = target,
        .optimize = optimize,
    });

    // test_raw_s3
    const test_raw_s3_mod = b.createModule(.{
        .root_source_file = b.path("tests/io/test_raw_s3.zig"),
        .target = target,
        .optimize = optimize,
    });
    test_raw_s3_mod.addImport("raw_s3_source", raw_s3_source_mod);

    // test_tls
    const test_tls_mod = b.createModule(.{
        .root_source_file = b.path("tests/io/test_tls.zig"),
        .target = target,
        .optimize = optimize,
    });
    test_tls_mod.addImport("zpq", zpq_mod);

    const test_tls_exe = b.addExecutable(.{
        .name = "test_tls",
        .root_module = test_tls_mod,
    });
    const run_test_tls = b.addRunArtifact(test_tls_exe);
    test_io_step.dependOn(&run_test_tls.step);

    // test_xev_tcp
    const test_xev_tcp_mod = b.createModule(.{
        .root_source_file = b.path("tests/io/test_xev_tcp.zig"),
        .target = target,
        .optimize = optimize,
    });
    test_xev_tcp_mod.addImport("xev", libxev_mod);

    const test_xev_tcp_exe = b.addExecutable(.{
        .name = "test_xev_tcp",
        .root_module = test_xev_tcp_mod,
    });
    const run_test_xev_tcp = b.addRunArtifact(test_xev_tcp_exe);

    const step_test_xev_tcp = b.step("test-xev-tcp", "Run basic xev TCP test");
    step_test_xev_tcp.dependOn(&run_test_xev_tcp.step);

    const check_step = b.step("check", "Check compilation");
    check_step.dependOn(&exe.step);
    check_step.dependOn(&lib_unit_tests.step);
    check_step.dependOn(&test_async_request_exe.step);
    check_step.dependOn(&test_event_loop_exe.step);
    check_step.dependOn(&test_async_source_exe.step);
    check_step.dependOn(&test_tls_exe.step);
    check_step.dependOn(&test_xev_tcp_exe.step);

    // AWS Lambda Bootstrap
    const bootstrap_mod = b.createModule(.{
        .root_source_file = b.path("src/lambda_bootstrap.zig"),
        .target = b.resolveTargetQuery(.{ .cpu_arch = .aarch64, .os_tag = .linux }),
        .optimize = .ReleaseFast,
    });
    bootstrap_mod.addImport("zpq", zpq_mod);
    bootstrap_mod.addImport("xev", libxev_mod);

    const bootstrap_exe = b.addExecutable(.{
        .name = "bootstrap",
        .root_module = bootstrap_mod,
    });
    bootstrap_exe.linkLibC();

    // Install to zig-out/lambda/bootstrap instead of bin/
    const install_bootstrap = b.addInstallArtifact(bootstrap_exe, .{
        .dest_dir = .{ .override = .{ .custom = "lambda" } },
    });

    const build_lambda_step = b.step("lambda", "Build AWS Lambda bootstrap");
    build_lambda_step.dependOn(&install_bootstrap.step);

    // bench-e2e
    const bench_e2e_mod = b.createModule(.{
        .root_source_file = b.path("tools/bench_e2e/main.zig"),
        .target = target,
        .optimize = optimize,
    });
    bench_e2e_mod.addImport("zpq", zpq_mod);
    bench_e2e_mod.addImport("xev", libxev_mod);
    bench_e2e_mod.addImport("boring_tls", boring_tls_mod);

    const bench_e2e_exe = b.addExecutable(.{
        .name = "bench-e2e",
        .root_module = bench_e2e_mod,
    });
    bench_e2e_exe.linkLibC();
    b.installArtifact(bench_e2e_exe);

    const run_bench_e2e = b.addRunArtifact(bench_e2e_exe);
    const bench_e2e_step = b.step("bench-e2e", "Run E2E benchmark");
    bench_e2e_step.dependOn(&run_bench_e2e.step);

    // Hardening Proofs / Integration tests
    {
        // 12. test_gap_skipping
        {
            const mod = b.createModule(.{
                .root_source_file = b.path("tests/io/test_gap_skipping.zig"),
                .target = target,
                .optimize = optimize,
            });
            mod.addImport("xev", libxev_mod);
            mod.addImport("zpq", zpq_mod);

            const gs_exe = b.addExecutable(.{
                .name = "test_gap_skipping",
                .root_module = mod,
            });
            gs_exe.linkLibC();

            const run = b.addRunArtifact(gs_exe);
            const step = b.step("test-gap-skipping", "Run zero-allocation gap skipping integration test");
            step.dependOn(&run.step);
        }

        // 13. test_fuzz_demo
        {
            const test_exe = b.addTest(.{
                .root_module = b.createModule(.{
                    .root_source_file = b.path("tests/fuzz/demos/demo_minish_basics.zig"),
                    .target = target,
                    .optimize = optimize,
                }),
            });
            test_exe.root_module.addImport("minish", minish);

            const run = b.addRunArtifact(test_exe);
            const step = b.step("test-fuzz-demo", "Run Minish basics demo");
            step.dependOn(&run.step);
        }

        // 14. test_thrift_fuzz
        {
            const test_exe = b.addTest(.{
                .root_module = b.createModule(.{
                    .root_source_file = b.path("tests/fuzz/test_thrift_fuzz.zig"),
                    .target = target,
                    .optimize = optimize,
                }),
            });
            test_exe.root_module.addImport("minish", minish);
            test_exe.root_module.addImport("zpq", zpq_mod);

            const run = b.addRunArtifact(test_exe);
            const step = b.step("test-thrift-fuzz", "Run Thrift metadata fuzzer");
            step.dependOn(&run.step);
        }

        // 15. test_http_fuzz
        {
            const test_exe = b.addTest(.{
                .root_module = b.createModule(.{
                    .root_source_file = b.path("tests/fuzz/test_http_fuzz.zig"),
                    .target = target,
                    .optimize = optimize,
                }),
            });
            test_exe.root_module.addImport("minish", minish);
            test_exe.root_module.addImport("zpq", zpq_mod);

            const run = b.addRunArtifact(test_exe);
            const step = b.step("test-http-fuzz", "Run HTTP response parser fuzzer");
            step.dependOn(&run.step);
        }
        // 16. bench_dns
        {
            const mod = b.createModule(.{
                .root_source_file = b.path("tests/io/bench_dns.zig"),
                .target = target,
                .optimize = if (optimize == .Debug) .ReleaseFast else optimize,
            });
            mod.addImport("xev", libxev_mod);
            mod.addImport("zpq", zpq_mod);

            const bench_exe = b.addExecutable(.{
                .name = "bench_dns",
                .root_module = mod,
            });
            bench_exe.linkLibC();

            const run = b.addRunArtifact(bench_exe);
            const step = b.step("bench-dns", "Run DNS benchmark");
            step.dependOn(&run.step);
        }
        // 17. probe_sf_crash
        {
            const mod = b.createModule(.{
                .root_source_file = b.path("probes/probe_sf_crash.zig"),
                .target = target,
                .optimize = optimize,
            });
            mod.addImport("xev", libxev_mod);
            mod.addImport("zpq", zpq_mod);

            const probe_exe = b.addExecutable(.{
                .name = "probe_sf_crash",
                .root_module = mod,
            });
            probe_exe.linkLibC();

            const run = b.addRunArtifact(probe_exe);
            const step = b.step("probe-sf-crash", "Run SingleFlight crash probe");
            step.dependOn(&run.step);
        }
        {
            const mod = b.createModule(.{
                .root_source_file = b.path("probes/probe_tls_echo.zig"),
                .target = target,
                .optimize = optimize,
            });
            mod.addImport("zpq", zpq_mod);

            const probe_exe = b.addExecutable(.{
                .name = "probe-tls-echo",
                .root_module = mod,
            });
            probe_exe.linkLibC();

            const run = b.addRunArtifact(probe_exe);
            const step = b.step("probe-tls-echo", "Run TLS echo micro-test");
            step.dependOn(&run.step);
        }
    }
}
