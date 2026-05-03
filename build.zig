const std = @import("std");

/// ZPQ build system.
///
/// Produces two binaries from one source tree:
///   - `zpq`        : CLI binary. Uses libxev with the native backend
///                    (io_uring on Linux, kqueue on macOS).
///   - `zpq-lambda` : Lambda bootstrap binary. Excludes io_uring code at
///                    comptime. Linked against libxev's Epoll backend only.
///
/// The split is enforced by the `build_options.lambda` flag visible to
/// every translation unit in each binary.
pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});

    // Build options visible to the binaries via @import("build_options").
    const cli_opts = b.addOptions();
    cli_opts.addOption(bool, "lambda", false);

    const lambda_opts = b.addOptions();
    lambda_opts.addOption(bool, "lambda", true);

    // The pure ZPQ module — sans-IO logic. Both binaries import it.
    const zpq_mod = b.addModule("zpq", .{
        .root_source_file = b.path("src/zpq.zig"),
        .target = target,
        .optimize = optimize,
    });

    // libxev is currently incompatible with Zig 0.16.0 (uses removed
    // posix.clock_gettime, etc). The dep stays declared in build.zig.zon
    // but no binary imports it yet — re-add the import once libxev catches
    // up or once we patch the io_uring backend in vendor/libxev.

    // ----- CLI binary -----
    const cli = b.addExecutable(.{
        .name = "zpq",
        .root_module = b.createModule(.{
            .root_source_file = b.path("src/cli/main.zig"),
            .target = target,
            .optimize = optimize,
            .imports = &.{
                .{ .name = "zpq", .module = zpq_mod },
                .{ .name = "build_options", .module = cli_opts.createModule() },
            },
        }),
    });
    b.installArtifact(cli);

    const cli_step = b.step("cli", "Build the CLI binary (zpq)");
    cli_step.dependOn(&b.addInstallArtifact(cli, .{}).step);

    // ----- Lambda binary -----
    const lambda = b.addExecutable(.{
        .name = "zpq-lambda",
        .root_module = b.createModule(.{
            .root_source_file = b.path("src/lambda/main.zig"),
            .target = target,
            .optimize = optimize,
            .imports = &.{
                .{ .name = "zpq", .module = zpq_mod },
                .{ .name = "build_options", .module = lambda_opts.createModule() },
            },
        }),
    });
    b.installArtifact(lambda);

    const lambda_step = b.step("lambda", "Build the Lambda binary (zpq-lambda)");
    lambda_step.dependOn(&b.addInstallArtifact(lambda, .{}).step);

    // ----- Tests -----
    const lib_tests = b.addTest(.{
        .root_module = b.createModule(.{
            .root_source_file = b.path("src/zpq.zig"),
            .target = target,
            .optimize = optimize,
        }),
    });
    const run_lib_tests = b.addRunArtifact(lib_tests);
    const test_step = b.step("test", "Run unit tests");
    test_step.dependOn(&run_lib_tests.step);
}
