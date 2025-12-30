const std = @import("std");

pub fn addAuxiliaryTools(
    b: *std.Build,
    zpq_mod: *std.Build.Module,
    target: std.Build.ResolvedTarget,
    optimize: std.builtin.OptimizeMode,
    libxev_mod: *std.Build.Module,
    boring_tls_mod: *std.Build.Module,
    minish_mod: *std.Build.Module,
    install_all: bool,
    build_options: *std.Build.Step.Options,
    check_step: ?*std.Build.Step,
) void {
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
        pff_exe.root_module.linkSystemLibrary("c", .{});

        if (install_all) b.installArtifact(pff_exe);
        if (check_step) |s| s.dependOn(&pff_exe.step);

        const run = b.addRunArtifact(pff_exe);
        const step = b.step("probe-fast-feedback", "Run fast feedback probe");
        step.dependOn(&run.step);
    }

    // test_xev_tcp
    {
        const mod = b.createModule(.{
            .root_source_file = b.path("tests/integration/test_xev_tcp.zig"),
            .target = target,
            .optimize = optimize,
        });
        mod.addImport("xev", libxev_mod);

        const exe = b.addExecutable(.{
            .name = "test_xev_tcp",
            .root_module = mod,
        });
        if (install_all) b.installArtifact(exe);
        if (check_step) |s| s.dependOn(&exe.step);
        const run = b.addRunArtifact(exe);

        const step = b.step("test-xev-tcp", "Run basic xev TCP test");
        step.dependOn(&run.step);
    }

    // test_s3_range_get
    {
        const mod = b.createModule(.{
            .root_source_file = b.path("tests/integration/test_s3_range_get.zig"),
            .target = target,
            .optimize = optimize,
        });
        mod.addImport("zpq", zpq_mod);
        mod.addImport("xev", libxev_mod);

        const exe = b.addExecutable(.{
            .name = "test_s3_range_get",
            .root_module = mod,
        });
        exe.root_module.linkSystemLibrary("c", .{});
        if (install_all) b.installArtifact(exe);
        if (check_step) |s| s.dependOn(&exe.step);
        const run = b.addRunArtifact(exe);

        const step = b.step("test-s3-range-get", "Run S3 HTTPS range GET test (presigned URL; opt-in via env)");
        step.dependOn(&run.step);
    }

    // AWS Lambda Bootstrap -> Moved to examples/lambda (build_examples.zig)
    // Access via `zig build -Dexamples` or `zig build example-lambda`

    // bench-e2e
    {
        const mod = b.createModule(.{
            .root_source_file = b.path("benchmarks/e2e.zig"),
            .target = target,
            .optimize = optimize,
        });
        mod.addImport("zpq", zpq_mod);
        mod.addImport("xev", libxev_mod);
        mod.addImport("boring_tls", boring_tls_mod);
        mod.addOptions("build_options", build_options);

        const exe = b.addExecutable(.{
            .name = "bench-e2e",
            .root_module = mod,
        });
        exe.root_module.linkSystemLibrary("c", .{});

        if (install_all) b.installArtifact(exe);
        if (check_step) |s| s.dependOn(&exe.step);

        // Build-only step - just builds and installs the binary
        const build_step = b.step("bench-e2e", "Build end-to-end S3 benchmark");
        const install = b.addInstallArtifact(exe, .{});
        build_step.dependOn(&install.step);

        // Separate run step
        const run = b.addRunArtifact(exe);
        const run_step = b.step("run-bench-e2e", "Run end-to-end S3 benchmark");
        run_step.dependOn(&run.step);
    }

    // bench-projection (column projection benchmark - local files)
    {
        const mod = b.createModule(.{
            .root_source_file = b.path("benchmarks/projection.zig"),
            .target = target,
            .optimize = optimize,
        });
        mod.addImport("zpq", zpq_mod);
        mod.addOptions("build_options", build_options);

        const exe = b.addExecutable(.{
            .name = "bench-projection",
            .root_module = mod,
        });
        exe.root_module.linkSystemLibrary("c", .{});

        if (install_all) b.installArtifact(exe);
        if (check_step) |s| s.dependOn(&exe.step);

        // Build-only step - just builds and installs the binary
        const build_step = b.step("bench-projection", "Build local file projection benchmark");
        const install = b.addInstallArtifact(exe, .{});
        build_step.dependOn(&install.step);

        // Separate run step
        const run = b.addRunArtifact(exe);
        const run_step = b.step("run-bench-projection", "Run local file projection benchmark");
        run_step.dependOn(&run.step);
    }

    // bench-decode-full (apples-to-apples benchmark with full value decoding)
    {
        const mod = b.createModule(.{
            .root_source_file = b.path("benchmarks/decode_full.zig"),
            .target = target,
            .optimize = optimize,
        });
        mod.addImport("zpq", zpq_mod);
        mod.addOptions("build_options", build_options);

        const exe = b.addExecutable(.{
            .name = "bench-decode-full",
            .root_module = mod,
        });
        exe.root_module.linkSystemLibrary("c", .{});

        if (install_all) b.installArtifact(exe);
        if (check_step) |s| s.dependOn(&exe.step);

        // Build-only step - just builds and installs the binary
        const build_step = b.step("bench-decode", "Build full decode benchmark (apples-to-apples)");
        const install = b.addInstallArtifact(exe, .{});
        build_step.dependOn(&install.step);

        // Separate run step
        const run = b.addRunArtifact(exe);
        if (b.args) |args| {
            run.addArgs(args);
        }
        const run_step = b.step("run-bench-decode", "Run full decode benchmark (apples-to-apples)");
        run_step.dependOn(&run.step);
    }

    // bench-simd (SIMD bit-unpacking and RLE benchmark)
    {
        const mod = b.createModule(.{
            .root_source_file = b.path("benchmarks/simd_bitunpack.zig"),
            .target = target,
            .optimize = optimize,
        });
        mod.addImport("zpq", zpq_mod);

        const exe = b.addExecutable(.{
            .name = "bench-simd",
            .root_module = mod,
        });
        exe.root_module.linkSystemLibrary("c", .{});

        if (install_all) b.installArtifact(exe);
        if (check_step) |s| s.dependOn(&exe.step);

        const build_step = b.step("bench-simd", "Build SIMD bit-unpacking benchmark");
        const install = b.addInstallArtifact(exe, .{});
        build_step.dependOn(&install.step);

        const run = b.addRunArtifact(exe);
        const run_step = b.step("run-bench-simd", "Run SIMD bit-unpacking benchmark");
        run_step.dependOn(&run.step);
    }

    // test-batch-reader
    {
        const mod = b.createModule(.{
            .root_source_file = b.path("probes/test_batch_reader.zig"),
            .target = target,
            .optimize = optimize,
        });
        mod.addImport("zpq", zpq_mod);

        const exe = b.addExecutable(.{
            .name = "test-batch-reader",
            .root_module = mod,
        });
        exe.root_module.linkSystemLibrary("c", .{});

        const run = b.addRunArtifact(exe);
        if (b.args) |args| {
            run.addArgs(args);
        }
        const step = b.step("test-batch-reader", "Run BatchReader test probe");
        step.dependOn(&run.step);
    }

    // test-deranged-bloat
    {
        const mod = b.createModule(.{
            .root_source_file = b.path("probes/test_deranged_bloat.zig"),
            .target = target,
            .optimize = optimize,
        });
        mod.addImport("zpq", zpq_mod);

        const exe = b.addExecutable(.{
            .name = "test-deranged-bloat",
            .root_module = mod,
        });
        exe.root_module.linkSystemLibrary("c", .{});

        const run = b.addRunArtifact(exe);
        const step = b.step("test-deranged-bloat", "Run deranged bloat test probe");
        step.dependOn(&run.step);
    }

    // Hardening Proofs / Integration tests
    {
        // 13. test_fuzz_demo
        {
            const test_exe = b.addTest(.{
                .root_module = b.createModule(.{
                    .root_source_file = b.path("tests/fuzz/demos/demo_minish_basics.zig"),
                    .target = target,
                    .optimize = optimize,
                }),
            });
            test_exe.root_module.addImport("minish", minish_mod);

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
            test_exe.root_module.addImport("minish", minish_mod);
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
            test_exe.root_module.addImport("minish", minish_mod);
            test_exe.root_module.addImport("zpq", zpq_mod);

            const run = b.addRunArtifact(test_exe);
            const step = b.step("test-http-fuzz", "Run HTTP response parser fuzzer");
            step.dependOn(&run.step);
        }

        // bench-dns
        {
            const mod = b.createModule(.{
                .root_source_file = b.path("benchmarks/dns.zig"),
                .target = target,
                .optimize = if (optimize == .Debug) .ReleaseFast else optimize,
            });
            mod.addImport("xev", libxev_mod);
            mod.addImport("zpq", zpq_mod);
            mod.addOptions("build_options", build_options);

            const exe = b.addExecutable(.{
                .name = "bench-dns",
                .root_module = mod,
            });
            exe.root_module.linkSystemLibrary("c", .{});

            if (install_all) b.installArtifact(exe);
            if (check_step) |s| s.dependOn(&exe.step);

            // Build-only step - just builds and installs the binary
            const build_step = b.step("bench-dns", "Build DNS benchmark");
            const install = b.addInstallArtifact(exe, .{});
            build_step.dependOn(&install.step);

            // Separate run step
            const run = b.addRunArtifact(exe);
            const run_step = b.step("run-bench-dns", "Run DNS benchmark");
            run_step.dependOn(&run.step);
        }

        // ping-pongs
        {
            const mod = b.createModule(.{
                .root_source_file = b.path("benchmarks/ping_pongs.zig"),
                .target = target,
                .optimize = if (optimize == .Debug) .ReleaseFast else optimize,
            });
            mod.addImport("xev", libxev_mod);
            mod.addOptions("build_options", build_options);

            const exe = b.addExecutable(.{
                .name = "ping-pongs",
                .root_module = mod,
            });

            if (install_all) b.installArtifact(exe);
            if (check_step) |s| s.dependOn(&exe.step);

            // Build-only step - just builds and installs the binary
            const build_step = b.step("ping-pongs", "Build TCP ping-pong benchmark");
            const install = b.addInstallArtifact(exe, .{});
            build_step.dependOn(&install.step);

            // Separate run step
            const run = b.addRunArtifact(exe);
            const run_step = b.step("run-ping-pongs", "Run TCP ping-pong benchmark");
            run_step.dependOn(&run.step);
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

            const exe = b.addExecutable(.{
                .name = "probe_sf_crash",
                .root_module = mod,
            });
            exe.root_module.linkSystemLibrary("c", .{});
            if (install_all) b.installArtifact(exe);
            if (check_step) |s| s.dependOn(&exe.step);

            const run = b.addRunArtifact(exe);
            const step = b.step("probe-sf-crash", "Run SingleFlight crash probe");
            step.dependOn(&run.step);
        }

        // probe_xev_s3_head - Minimal XevS3Source HEAD probe
        {
            const mod = b.createModule(.{
                .root_source_file = b.path("probes/probe_xev_s3_head.zig"),
                .target = target,
                .optimize = optimize,
            });
            mod.addImport("zpq", zpq_mod);
            mod.addImport("xev", libxev_mod);

            const exe = b.addExecutable(.{
                .name = "probe-xev-s3-head",
                .root_module = mod,
            });
            exe.root_module.linkSystemLibrary("c", .{});
            if (install_all) b.installArtifact(exe);
            if (check_step) |s| s.dependOn(&exe.step);

            const run = b.addRunArtifact(exe);
            const step = b.step("probe-xev-s3-head", "Run XevS3Source HEAD probe");
            step.dependOn(&run.step);
        }

        // probe-pool-cleanup - Test connection pool cleanup
        {
            const mod = b.createModule(.{
                .root_source_file = b.path("probes/probe_pool_cleanup.zig"),
                .target = target,
                .optimize = optimize,
            });
            mod.addImport("zpq", zpq_mod);

            const exe = b.addExecutable(.{
                .name = "probe-pool-cleanup",
                .root_module = mod,
            });
            exe.root_module.linkSystemLibrary("c", .{});
            if (install_all) b.installArtifact(exe);
            if (check_step) |s| s.dependOn(&exe.step);

            const run = b.addRunArtifact(exe);
            const step = b.step("probe-pool-cleanup", "Test connection pool cleanup");
            step.dependOn(&run.step);
        }

        // shootout-tls - Micro-shootout for TLS throughput strategies
        {
            const mod = b.createModule(.{
                .root_source_file = b.path("probes/shootout_tls_throughput.zig"),
                .target = target,
                .optimize = optimize,
            });
            mod.addImport("zpq", zpq_mod);
            mod.addImport("xev", libxev_mod);
            mod.addOptions("build_options", build_options);

            const exe = b.addExecutable(.{
                .name = "shootout-tls",
                .root_module = mod,
            });
            exe.root_module.linkSystemLibrary("c", .{});

            if (install_all) b.installArtifact(exe);
            if (check_step) |s| s.dependOn(&exe.step);

            // Build-only step (no run) - depends on install so binary goes to zig-out
            const build_step = b.step("shootout-tls", "Build TLS throughput micro-shootout");
            const install = b.addInstallArtifact(exe, .{});
            build_step.dependOn(&install.step);

            // Separate run step
            const run = b.addRunArtifact(exe);
            const run_step = b.step("run-shootout-tls", "Run TLS throughput micro-shootout");
            run_step.dependOn(&run.step);
        }

        // test-dict-decode - Micro-test for dictionary decoding
        {
            const mod = b.createModule(.{
                .root_source_file = b.path("tests/integration/test_dict_decode.zig"),
                .target = target,
                .optimize = optimize,
            });
            mod.addImport("zpq", zpq_mod);

            const exe = b.addExecutable(.{
                .name = "test-dict-decode",
                .root_module = mod,
            });
            exe.root_module.linkSystemLibrary("c", .{});

            if (install_all) b.installArtifact(exe);
            if (check_step) |s| s.dependOn(&exe.step);

            const build_step = b.step("test-dict-decode", "Build dict decode micro-test");
            const install = b.addInstallArtifact(exe, .{});
            build_step.dependOn(&install.step);

            const run = b.addRunArtifact(exe);
            const run_step = b.step("run-test-dict-decode", "Run dict decode micro-test");
            run_step.dependOn(&run.step);
        }

        // test-passthrough - Integration test for passthrough vs decode/reencode
        {
            const mod = b.createModule(.{
                .root_source_file = b.path("tests/integration/test_passthrough.zig"),
                .target = target,
                .optimize = optimize,
            });
            mod.addImport("zpq", zpq_mod);

            const exe = b.addExecutable(.{
                .name = "test-passthrough",
                .root_module = mod,
            });
            exe.root_module.linkSystemLibrary("c", .{});

            if (install_all) b.installArtifact(exe);
            if (check_step) |s| s.dependOn(&exe.step);

            // Build-only step
            const build_step = b.step("test-passthrough", "Build passthrough integration test");
            const install = b.addInstallArtifact(exe, .{});
            build_step.dependOn(&install.step);

            // Separate run step
            const run = b.addRunArtifact(exe);
            const run_step = b.step("run-test-passthrough", "Run passthrough integration test");
            run_step.dependOn(&run.step);
        }

        // test-partitioned - Partitioned data passthrough demo
        {
            const mod = b.createModule(.{
                .root_source_file = b.path("tests/integration/test_partitioned_passthrough.zig"),
                .target = target,
                .optimize = optimize,
            });
            mod.addImport("zpq", zpq_mod);

            const exe = b.addExecutable(.{
                .name = "test-partitioned",
                .root_module = mod,
            });
            exe.root_module.linkSystemLibrary("c", .{});

            if (install_all) b.installArtifact(exe);
            if (check_step) |s| s.dependOn(&exe.step);

            const build_step = b.step("test-partitioned", "Build partitioned passthrough demo");
            const install = b.addInstallArtifact(exe, .{});
            build_step.dependOn(&install.step);

            const run = b.addRunArtifact(exe);
            const run_step = b.step("run-test-partitioned", "Run partitioned passthrough demo");
            run_step.dependOn(&run.step);
        }

        // test-roundtrip - Write parquet files and read them back
        {
            const mod = b.createModule(.{
                .root_source_file = b.path("tests/integration/test_roundtrip.zig"),
                .target = target,
                .optimize = optimize,
            });
            mod.addImport("zpq", zpq_mod);

            const exe = b.addExecutable(.{
                .name = "test-roundtrip",
                .root_module = mod,
            });
            exe.root_module.linkSystemLibrary("c", .{});

            if (install_all) b.installArtifact(exe);
            if (check_step) |s| s.dependOn(&exe.step);

            const build_step = b.step("test-roundtrip", "Build roundtrip integration test");
            const install = b.addInstallArtifact(exe, .{});
            build_step.dependOn(&install.step);

            const run = b.addRunArtifact(exe);
            const run_step = b.step("run-test-roundtrip", "Run roundtrip integration test");
            run_step.dependOn(&run.step);
        }
    }
}
