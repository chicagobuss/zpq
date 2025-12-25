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
