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
) void {
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
        pff_exe.root_module.linkSystemLibrary("c", .{});

        if (install_all) b.installArtifact(pff_exe);

        const run = b.addRunArtifact(pff_exe);
        const step = b.step("probe-fast-feedback", "Run fast feedback probe");
        step.dependOn(&run.step);
    }

    // test_async_request
    {
        const mod = b.createModule(.{
            .root_source_file = b.path("tests/integration/test_async_request.zig"),
            .target = target,
            .optimize = optimize,
        });
        mod.addImport("zpq", zpq_mod);
        mod.addImport("xev", libxev_mod);

        const exe = b.addExecutable(.{
            .name = "test_async_request",
            .root_module = mod,
        });
        if (install_all) b.installArtifact(exe);
        const run = b.addRunArtifact(exe);
        test_io_step.dependOn(&run.step);
    }

    // test_event_loop
    {
        const mod = b.createModule(.{
            .root_source_file = b.path("tests/integration/test_event_loop.zig"),
            .target = target,
            .optimize = optimize,
        });
        mod.addImport("zpq", zpq_mod);

        const exe = b.addExecutable(.{
            .name = "test_event_loop",
            .root_module = mod,
        });
        if (install_all) b.installArtifact(exe);
        const run = b.addRunArtifact(exe);
        test_io_step.dependOn(&run.step);

        const step = b.step("test-event-loop", "Run EventLoop + AsyncRequest integration test");
        step.dependOn(&run.step);
    }

    // test_async_source
    {
        const mod = b.createModule(.{
            .root_source_file = b.path("tests/integration/test_async_source.zig"),
            .target = target,
            .optimize = optimize,
        });
        mod.addImport("zpq", zpq_mod);

        const exe = b.addExecutable(.{
            .name = "test_async_source",
            .root_module = mod,
        });
        if (install_all) b.installArtifact(exe);
        const run = b.addRunArtifact(exe);
        test_io_step.dependOn(&run.step);

        const step = b.step("test-async-source", "Run AsyncS3Source integration test");
        step.dependOn(&run.step);
    }

    // test_tls
    {
        const mod = b.createModule(.{
            .root_source_file = b.path("tests/integration/test_tls.zig"),
            .target = target,
            .optimize = optimize,
        });
        mod.addImport("zpq", zpq_mod);

        const exe = b.addExecutable(.{
            .name = "test_tls",
            .root_module = mod,
        });
        if (install_all) b.installArtifact(exe);
        const run = b.addRunArtifact(exe);
        test_io_step.dependOn(&run.step);
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

        const exe = b.addExecutable(.{
            .name = "bench-e2e",
            .root_module = mod,
        });
        exe.root_module.linkSystemLibrary("c", .{});

        // Always install benchmark binaries
        b.installArtifact(exe);
    }

    // bench-projection (column projection benchmark)
    {
        const mod = b.createModule(.{
            .root_source_file = b.path("benchmarks/projection.zig"),
            .target = target,
            .optimize = optimize,
        });
        mod.addImport("zpq", zpq_mod);

        const exe = b.addExecutable(.{
            .name = "bench-projection",
            .root_module = mod,
        });
        exe.root_module.linkSystemLibrary("c", .{});

        b.installArtifact(exe);
    }

    // bench-decode-full (apples-to-apples benchmark with full value decoding)
    {
        const mod = b.createModule(.{
            .root_source_file = b.path("benchmarks/decode_full.zig"),
            .target = target,
            .optimize = optimize,
        });
        mod.addImport("zpq", zpq_mod);

        const exe = b.addExecutable(.{
            .name = "bench-decode-full",
            .root_module = mod,
        });
        exe.root_module.linkSystemLibrary("c", .{});

        b.installArtifact(exe);
    }

    // test-sf (SingleFlightResolver isolation test)
    {
        const mod = b.createModule(.{
            .root_source_file = b.path("tools/test_sf.zig"),
            .target = target,
            .optimize = optimize,
        });
        mod.addImport("zpq", zpq_mod);

        const exe = b.addExecutable(.{
            .name = "test-sf",
            .root_module = mod,
        });
        exe.root_module.linkSystemLibrary("c", .{});
        if (install_all) b.installArtifact(exe);

        const run = b.addRunArtifact(exe);
        const step = b.step("test-sf", "Test SingleFlightResolver in isolation");
        step.dependOn(&run.step);
    }

    // Hardening Proofs / Integration tests
    {
        // 12. test_gap_skipping
        {
            const mod = b.createModule(.{
                .root_source_file = b.path("tests/integration/test_gap_skipping.zig"),
                .target = target,
                .optimize = optimize,
            });
            mod.addImport("xev", libxev_mod);
            mod.addImport("zpq", zpq_mod);

            const exe = b.addExecutable(.{
                .name = "test_gap_skipping",
                .root_module = mod,
            });
            exe.root_module.linkSystemLibrary("c", .{});
            if (install_all) b.installArtifact(exe);

            const run = b.addRunArtifact(exe);
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

            const exe = b.addExecutable(.{
                .name = "bench-dns",
                .root_module = mod,
            });
            exe.root_module.linkSystemLibrary("c", .{});

            // Always install benchmark binaries
            b.installArtifact(exe);

            const run = b.addRunArtifact(exe);
            const step = b.step("run-bench-dns", "Run DNS benchmark");
            step.dependOn(&run.step);
        }

        // ping-pongs
        {
            const mod = b.createModule(.{
                .root_source_file = b.path("benchmarks/ping_pongs.zig"),
                .target = target,
                .optimize = if (optimize == .Debug) .ReleaseFast else optimize,
            });
            mod.addImport("xev", libxev_mod);

            const exe = b.addExecutable(.{
                .name = "ping-pongs",
                .root_module = mod,
            });

            // Always install benchmark binaries
            b.installArtifact(exe);

            const run = b.addRunArtifact(exe);
            const step = b.step("run-ping-pongs", "Run TCP ping-pong benchmark");
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

            const exe = b.addExecutable(.{
                .name = "probe_sf_crash",
                .root_module = mod,
            });
            exe.root_module.linkSystemLibrary("c", .{});
            if (install_all) b.installArtifact(exe);

            const run = b.addRunArtifact(exe);
            const step = b.step("probe-sf-crash", "Run SingleFlight crash probe");
            step.dependOn(&run.step);
        }

        // 18. probe_tls_echo
        {
            const mod = b.createModule(.{
                .root_source_file = b.path("probes/probe_tls_echo.zig"),
                .target = target,
                .optimize = optimize,
            });
            mod.addImport("zpq", zpq_mod);

            const exe = b.addExecutable(.{
                .name = "probe-tls-echo",
                .root_module = mod,
            });
            exe.root_module.linkSystemLibrary("c", .{});
            if (install_all) b.installArtifact(exe);

            const run = b.addRunArtifact(exe);
            const step = b.step("probe-tls-echo", "Run TLS echo micro-test");
            step.dependOn(&run.step);
        }

        // 19. probe_s3_head_hang - diagnose async S3 HEAD request hang
        {
            const mod = b.createModule(.{
                .root_source_file = b.path("probes/probe_s3_head_hang.zig"),
                .target = target,
                .optimize = optimize,
            });
            mod.addImport("zpq", zpq_mod);
            mod.addImport("xev", libxev_mod);

            const exe = b.addExecutable(.{
                .name = "probe-s3-head-hang",
                .root_module = mod,
            });
            exe.root_module.linkSystemLibrary("c", .{});
            if (install_all) b.installArtifact(exe);

            const run = b.addRunArtifact(exe);
        const step = b.step("probe-s3-head-hang", "Run S3 HEAD request hang probe");
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

        const exe = b.addExecutable(.{
            .name = "shootout-tls",
            .root_module = mod,
        });
        exe.root_module.linkSystemLibrary("c", .{});
        const install = b.addInstallArtifact(exe, .{});

        // Build-only step (no run) - depends on install so binary goes to zig-out
        const build_step = b.step("shootout-tls", "Build TLS throughput micro-shootout");
        build_step.dependOn(&install.step);

        // Separate run step
        const run = b.addRunArtifact(exe);
        const run_step = b.step("run-shootout-tls", "Run TLS throughput micro-shootout");
        run_step.dependOn(&run.step);
    }
}

}
