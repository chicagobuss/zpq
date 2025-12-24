const std = @import("std");

pub fn addExamples(
    b: *std.Build,
    zpq_mod: *std.Build.Module,
    target: std.Build.ResolvedTarget,
    optimize: std.builtin.OptimizeMode,
    libxev_mod: *std.Build.Module,
    install_examples: bool,
) void {
    _ = target;
    _ = optimize;
    if (!install_examples) return;

    // Lambda Bootstrap Example
    {
        // Always build for Linux/ARM64 as that's the target runtime
        const lambda_target = b.resolveTargetQuery(.{ .cpu_arch = .aarch64, .os_tag = .linux });
        
        const mod = b.createModule(.{
            .root_source_file = b.path("examples/lambda/main.zig"),
            .target = lambda_target,
            .optimize = .ReleaseFast, // Always release for lambda to be realistic
        });
        mod.addImport("zpq", zpq_mod);
        mod.addImport("xev", libxev_mod);

        const exe = b.addExecutable(.{
            .name = "bootstrap",
            .root_module = mod,
        });
        exe.root_module.linkSystemLibrary("c", .{});

        // Install to zig-out/lambda/bootstrap
        const install = b.addInstallArtifact(exe, .{
            .dest_dir = .{ .override = .{ .custom = "lambda" } },
        });

        const step = b.step("example-lambda", "Build AWS Lambda bootstrap example");
        step.dependOn(&install.step);
    }
}

