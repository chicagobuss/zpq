const std = @import("std");

pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});

    // Create the 'zpq' module
    const zpq_mod = b.createModule(.{
        .root_source_file = b.path("src/zpq.zig"),
        .target = target,
        .optimize = optimize,
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

    // raw_s3_source (restored)
    const raw_s3_source_mod = b.createModule(.{
        .root_source_file = b.path("src/zpq/s3/raw_s3_source.zig"),
        .target = target,
        .optimize = optimize,
    });
    // Raw S3 Source uses relative import for io.zig?
    // Let's check raw_s3_source.zig. 
    // It imports zpq_io in my previous thought? No, I added it.
    // I should probably fix raw_s3_source.zig to use relative too if it's used in library.
    // If raw_s3_source.zig is in src/zpq/s3/, it can use ../../io.zig
    
    // raw_s3_source_mod.addImport("zpq_io", io_mod);

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

    // Check step (Compile only, no run)
    const check_step = b.step("check", "Check compilation");
    check_step.dependOn(&exe.step);
    check_step.dependOn(&lib_unit_tests.step);
    check_step.dependOn(&test_async_request_exe.step);
    check_step.dependOn(&test_event_loop_exe.step);
    check_step.dependOn(&test_async_source_exe.step);
    check_step.dependOn(&test_tls_exe.step);
}
