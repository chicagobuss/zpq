const std = @import("std");

/// ZPQ build system.
///
/// Produces two binaries from one source tree:
///   - `zpq`        : CLI binary. Native event-loop backend
///                    (io_uring on Linux, kqueue on macOS).
///   - `zpq-lambda` : Lambda bootstrap binary. Excludes io_uring code at
///                    comptime. Uses the in-tree epoll backend only.
///
/// The split is enforced by the `build_options.lambda` flag visible to
/// every translation unit in each binary. We don't depend on libxev or
/// any other event-loop library — see .agent/rules/tier2_knowledge.md.
pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});

    // ----- CLI binary -----
    const cli_zpq = makeZpqModule(b, target, optimize, false);
    const cli = b.addExecutable(.{
        .name = "zpq",
        .root_module = cli_zpq.root,
    });
    cli.root_module.addImport("zpq", cli_zpq.zpq);
    b.installArtifact(cli);

    const cli_step = b.step("cli", "Build the CLI binary (zpq)");
    cli_step.dependOn(&b.addInstallArtifact(cli, .{}).step);

    // ----- Lambda binary -----
    const lambda_zpq = makeZpqModule(b, target, optimize, true);
    const lambda = b.addExecutable(.{
        .name = "zpq-lambda",
        .root_module = lambda_zpq.root,
    });
    lambda.root_module.addImport("zpq", lambda_zpq.zpq);
    b.installArtifact(lambda);

    const lambda_step = b.step("lambda", "Build the Lambda binary (zpq-lambda)");
    lambda_step.dependOn(&b.addInstallArtifact(lambda, .{}).step);

    // ----- Tests -----
    // Tests pin lambda=true since epoll is the only backend implemented;
    // other backends @compileError until they exist.
    const test_opts = b.addOptions();
    test_opts.addOption(bool, "lambda", true);
    const test_opts_mod = test_opts.createModule();

    const test_boring_dep = b.dependency("boring_tls", .{
        .target = target,
        .optimize = optimize,
    });
    const test_boring_mod = test_boring_dep.module("boring_tls");

    const test_zpq_mod = b.createModule(.{
        .root_source_file = b.path("src/zpq.zig"),
        .target = target,
        .optimize = optimize,
        .imports = &.{
            .{ .name = "build_options", .module = test_opts_mod },
            .{ .name = "boring_tls", .module = test_boring_mod },
        },
    });

    // Sans-IO + io tests (live in src/zpq.zig and what it imports).
    const lib_tests = b.addTest(.{
        .root_module = test_zpq_mod,
    });
    const run_lib_tests = b.addRunArtifact(lib_tests);

    // Lambda binary tests (runtime.zig and anything else lambda-only).
    // Reuses the same zpq module so import paths match the production build.
    const lambda_tests = b.addTest(.{
        .root_module = b.createModule(.{
            .root_source_file = b.path("src/lambda/main.zig"),
            .target = target,
            .optimize = optimize,
            .imports = &.{
                .{ .name = "zpq", .module = test_zpq_mod },
                .{ .name = "build_options", .module = test_opts_mod },
            },
        }),
    });
    const run_lambda_tests = b.addRunArtifact(lambda_tests);

    const test_step = b.step("test", "Run unit tests");
    test_step.dependOn(&run_lib_tests.step);
    test_step.dependOn(&run_lambda_tests.step);

    // ----- Integration tests (Lambda fake runtime) -----
    // Spawns the built zpq-lambda binary against an in-process fake
    // runtime API. Doesn't run as part of `zig build test` because it
    // requires the binary to be installed first; run with
    // `zig build test-integration`.
    const integration_opts = b.addOptions();
    const lambda_install = b.addInstallArtifact(lambda, .{});
    integration_opts.addOption(
        []const u8,
        "lambda_bin",
        b.getInstallPath(.bin, "zpq-lambda"),
    );

    const integration_tests = b.addTest(.{
        .root_module = b.createModule(.{
            .root_source_file = b.path("tests/lambda_integration.zig"),
            .target = target,
            .optimize = optimize,
            .imports = &.{
                .{ .name = "integration_opts", .module = integration_opts.createModule() },
            },
        }),
    });
    integration_tests.step.dependOn(&lambda_install.step);
    const run_integration_tests = b.addRunArtifact(integration_tests);

    const integration_step = b.step("test-integration", "Run Lambda integration tests");
    integration_step.dependOn(&run_integration_tests.step);

    // ----- Bakeoff probes -----
    // Disposable. Probe std.Io.Threaded vs an epoll-driven variant for
    // multipart S3 PUTs. See docs/writer_design.md.
    const bakeoff_threaded = b.addExecutable(.{
        .name = "bakeoff_threaded",
        .root_module = b.createModule(.{
            .root_source_file = b.path("probes/bakeoff_threaded/main.zig"),
            .target = target,
            .optimize = optimize,
            .link_libc = true,
            .imports = &.{
                .{ .name = "zpq", .module = cli_zpq.zpq },
            },
        }),
    });
    b.installArtifact(bakeoff_threaded);
    const bakeoff_threaded_step = b.step("bakeoff-threaded", "Build the Io.Threaded multipart-PUT bakeoff probe");
    bakeoff_threaded_step.dependOn(&b.addInstallArtifact(bakeoff_threaded, .{}).step);
}

/// Bundle the per-binary modules. We give each binary its own zpq module
/// instance so the comptime `build_options.lambda` flag propagates
/// correctly into src/io/loop.zig (which lives inside the zpq namespace).
const Bundle = struct {
    /// The binary's root_source_file module (cli/main.zig or lambda/main.zig).
    root: *std.Build.Module,
    /// The zpq module (src/zpq.zig) wired with this binary's build_options.
    zpq: *std.Build.Module,
};

fn makeZpqModule(
    b: *std.Build,
    target: std.Build.ResolvedTarget,
    optimize: std.builtin.OptimizeMode,
    is_lambda: bool,
) Bundle {
    const opts = b.addOptions();
    opts.addOption(bool, "lambda", is_lambda);
    const opts_mod = opts.createModule();

    // BoringSSL bindings — required for HTTPS to S3 (and any other TLS).
    const boring_tls_dep = b.dependency("boring_tls", .{
        .target = target,
        .optimize = optimize,
    });
    const boring_tls_mod = boring_tls_dep.module("boring_tls");

    const zpq_mod = b.createModule(.{
        .root_source_file = b.path("src/zpq.zig"),
        .target = target,
        .optimize = optimize,
        .imports = &.{
            .{ .name = "build_options", .module = opts_mod },
            .{ .name = "boring_tls", .module = boring_tls_mod },
        },
    });

    const root_path = if (is_lambda) "src/lambda/main.zig" else "src/cli/main.zig";
    const root_mod = b.createModule(.{
        .root_source_file = b.path(root_path),
        .target = target,
        .optimize = optimize,
        // libc gives us getaddrinfo; under -O ReleaseSmall + musl static
        // it adds ~150 KB which is dwarfed by BoringSSL anyway. Until
        // we ship our own DNS resolver, this is the right tradeoff.
        .link_libc = true,
        .imports = &.{
            .{ .name = "build_options", .module = opts_mod },
            .{ .name = "boring_tls", .module = boring_tls_mod },
        },
    });

    return .{ .root = root_mod, .zpq = zpq_mod };
}
