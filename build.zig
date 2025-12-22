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
        .target = target,
        .optimize = .ReleaseFast,
    });
    bootstrap_mod.addImport("zpq", zpq_mod);
    bootstrap_mod.addImport("xev", libxev_mod);

    const bootstrap_exe = b.addExecutable(.{
        .name = "bootstrap",
        .root_module = bootstrap_mod,
    });

    // Install to zig-out/lambda/bootstrap instead of bin/
    const install_bootstrap = b.addInstallArtifact(bootstrap_exe, .{
        .dest_dir = .{ .override = .{ .custom = "lambda" } },
    });

    const build_lambda_step = b.step("build-lambda", "Build AWS Lambda bootstrap (aarch64-linux)");
    build_lambda_step.dependOn(&install_bootstrap.step);

    // Benchmark Bootstrap
    const bench_mod = b.createModule(.{
        .root_source_file = b.path("src/lambda_bench.zig"),
        .target = b.resolveTargetQuery(.{ .cpu_arch = .aarch64, .os_tag = .linux }),
        .optimize = .ReleaseFast,
    });
    bench_mod.addImport("zpq", zpq_mod);
    bench_mod.addImport("xev", libxev_mod);

    const exe_bench = b.addExecutable(.{
        .name = "bootstrap-bench",
        .root_module = bench_mod,
    });

    const install_bench = b.addInstallArtifact(exe_bench, .{
        .dest_dir = .{ .override = .{ .custom = "lambda-bench" } },
    });

    const build_lambda_bench_step = b.step("build-lambda-bench", "Build lambda benchmark bootstrap");
    build_lambda_bench_step.dependOn(&install_bench.step);

    // --- Experimental / Micro-tests ---
    const enable_experimental = b.option(bool, "experimental", "Enable experimental tests/benchmarks") orelse false;

    if (enable_experimental) {
        // 0. probe_async_dns
        {
            const mod = b.createModule(.{
                .root_source_file = b.path("probes/probe_async_dns.zig"),
                .target = target,
                .optimize = optimize,
            });
            mod.addImport("xev", libxev_mod);

            const exe_probe = b.addExecutable(.{
                .name = "probe_async_dns",
                .root_module = mod,
            });
            exe_probe.linkLibC();

            const run = b.addRunArtifact(exe_probe);
            const step = b.step("probe-async-dns", "Run Async DNS probe");
            step.dependOn(&run.step);
        }

        // 1. test_boring_connect
        {
            const mod = b.createModule(.{
                .root_source_file = b.path("tests/io/test_boring_connect.zig"),
                .target = target,
                .optimize = optimize,
            });
            mod.addImport("xev", libxev_mod);
            mod.addImport("boring_tls", boring_tls_mod);

            const exe_boring = b.addExecutable(.{
                .name = "test_boring_connect",
                .root_module = mod,
            });

            const run = b.addRunArtifact(exe_boring);
            const step = b.step("test-boring-connect", "Run BoringTLS connection test");
            step.dependOn(&run.step);
        }

        // 2. bench_ping_pongs
        {
            const mod = b.createModule(.{
                .root_source_file = b.path("tests/bench/ping_pongs.zig"),
                .target = target,
                // Benchmark should default to ReleaseFast if not specified, but respect user option
                .optimize = if (optimize == .Debug) .ReleaseFast else optimize,
            });
            mod.addImport("xev", libxev_mod);

            const exe_ping = b.addExecutable(.{
                .name = "bench_ping_pongs",
                .root_module = mod,
            });

            const run = b.addRunArtifact(exe_ping);
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
            mod.addImport("xev", libxev_mod);
            mod.addImport("boring_tls", boring_tls_mod);

            const exe_s3 = b.addExecutable(.{
                .name = "test_s3_head",
                .root_module = mod,
            });

            const run = b.addRunArtifact(exe_s3);
            const step = b.step("test-s3-head", "Run S3 HEAD request test");
            step.dependOn(&run.step);
        }

        // 4. test_http_client
        {
            const mod = b.createModule(.{
                .root_source_file = b.path("tests/io/test_http_client.zig"),
                .target = target,
                .optimize = optimize,
            });
            mod.addImport("xev", libxev_mod);
            mod.addImport("zpq", zpq_mod);

            const exe_http = b.addExecutable(.{
                .name = "test_http_client",
                .root_module = mod,
            });

            const run = b.addRunArtifact(exe_http);
            const step = b.step("test-http-client", "Run HTTP Client Integration Test");
            step.dependOn(&run.step);
        }

        // 5. test_minio_https
        {
            const mod = b.createModule(.{
                .root_source_file = b.path("tests/io/test_minio_https.zig"),
                .target = target,
                .optimize = optimize,
            });
            mod.addImport("xev", libxev_mod);
            mod.addImport("zpq", zpq_mod);

            const exe_minio = b.addExecutable(.{
                .name = "test_minio_https",
                .root_module = mod,
            });

            const run = b.addRunArtifact(exe_minio);
            const step = b.step("test-minio-https", "Run HTTP Client against local MinIO");
            step.dependOn(&run.step);
        }

        // 6. test_minio_range_get
        {
            const mod = b.createModule(.{
                .root_source_file = b.path("tests/io/test_minio_range_get.zig"),
                .target = target,
                .optimize = optimize,
            });
            mod.addImport("xev", libxev_mod);
            mod.addImport("zpq", zpq_mod);
            mod.addImport("response_parser", b.createModule(.{
                .root_source_file = b.path("src/zpq/io/http/response_parser.zig"),
                .target = target,
                .optimize = optimize,
            }));
            mod.addImport("minio_fixtures", b.createModule(.{
                .root_source_file = b.path("ci/fixtures/minio/fixtures.zig"),
                .target = target,
                .optimize = optimize,
            }));

            const exe_minio_range = b.addExecutable(.{
                .name = "test_minio_range_get",
                .root_module = mod,
            });

            const run = b.addRunArtifact(exe_minio_range);
            const step = b.step("test-minio-range-get", "Run MinIO TLS Range GET integration test");
            step.dependOn(&run.step);
        }
        // 7. test_dns
        {
            const mod = b.createModule(.{
                .root_source_file = b.path("tests/io/test_dns.zig"),
                .target = target,
                .optimize = optimize,
            });
            mod.addImport("xev", libxev_mod);
            mod.addImport("zpq", zpq_mod);

            const exe_dns = b.addExecutable(.{
                .name = "test_dns",
                .root_module = mod,
            });
            exe_dns.linkLibC();

            const run = b.addRunArtifact(exe_dns);
            const step = b.step("test-dns", "Run Async DNS integration test");
            step.dependOn(&run.step);
        }

        // 8. probe_xev_tcp_lifecycle
        {
            const mod = b.createModule(.{
                .root_source_file = b.path("probes/probe_xev_tcp_lifecycle.zig"),
                .target = target,
                .optimize = optimize,
            });
            mod.addImport("xev", libxev_mod);

            const probe_exe = b.addExecutable(.{
                .name = "probe_xev_tcp_lifecycle",
                .root_module = mod,
            });
            probe_exe.linkLibC();

            const run = b.addRunArtifact(probe_exe);
            const step = b.step("probe-xev-tcp", "Run libxev TCP lifecycle probe");
            step.dependOn(&run.step);
        }

        // 9. probe_tls_pump
        {
            const mod = b.createModule(.{
                .root_source_file = b.path("probes/probe_tls_pump.zig"),
                .target = target,
                .optimize = optimize,
            });
            mod.addImport("xev", libxev_mod);
            mod.addImport("boring_tls", boring_tls_mod);

            const probe_exe = b.addExecutable(.{
                .name = "probe_tls_pump",
                .root_module = mod,
            });
            probe_exe.linkLibC();

            const run = b.addRunArtifact(probe_exe);
            const step = b.step("probe-tls-pump", "Run libxev + boring_tls pump probe");
            step.dependOn(&run.step);
        }

        // 10. test_connection
        {
            const mod = b.createModule(.{
                .root_source_file = b.path("tests/io/test_connection.zig"),
                .target = target,
                .optimize = optimize,
            });
            mod.addImport("xev", libxev_mod);
            mod.addImport("boring_tls", boring_tls_mod);
            mod.addImport("zpq", zpq_mod);

            const conn_exe = b.addExecutable(.{
                .name = "test_connection",
                .root_module = mod,
            });
            conn_exe.linkLibC();

            const run = b.addRunArtifact(conn_exe);
            const step = b.step("test-connection", "Run S3 connection transport test");
            step.dependOn(&run.step);
        }

        // 11. test_backpressure
        {
            const mod = b.createModule(.{
                .root_source_file = b.path("tests/io/test_backpressure.zig"),
                .target = target,
                .optimize = optimize,
            });
            mod.addImport("xev", libxev_mod);
            mod.addImport("zpq", zpq_mod);

            const bp_exe = b.addExecutable(.{
                .name = "test_backpressure",
                .root_module = mod,
            });
            bp_exe.linkLibC();

            const run = b.addRunArtifact(bp_exe);
            const step = b.step("test-backpressure", "Run backpressure integration test");
            step.dependOn(&run.step);
        }

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
    }
}
