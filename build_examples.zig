const std = @import("std");

pub fn addExamples(
    b: *std.Build,
    zpq_mod: *std.Build.Module,
    target: std.Build.ResolvedTarget,
    optimize: std.builtin.OptimizeMode,
    libxev_mod: *std.Build.Module,
    boring_tls_mod: *std.Build.Module,
    install_examples: bool,
) void {
    if (!install_examples) return;

    const examples = [_]struct {
        name: []const u8,
        path: []const u8,
        needs_zpq: bool,
    }{
        .{ .name = "lambda-01-minimal", .path = "examples/lambda/01-minimal/main.zig", .needs_zpq = false },
        .{ .name = "lambda-02-scan-benchmark", .path = "examples/lambda/02-scan-benchmark/main.zig", .needs_zpq = true },
    };

    for (examples) |example| {
        const mod = b.createModule(.{
            .root_source_file = b.path(example.path),
            .target = target,
            .optimize = optimize,
        });

        if (example.needs_zpq) {
            mod.addImport("zpq", zpq_mod);
            mod.addImport("xev", libxev_mod);
            mod.addImport("boring_tls", boring_tls_mod);
        }

        const exe = b.addExecutable(.{
            .name = "bootstrap",
            .root_module = mod,
        });
        exe.root_module.linkSystemLibrary("c", .{});

        // Install to zig-out/lambda/{example_name}/bootstrap
        const install = b.addInstallArtifact(exe, .{
            .dest_dir = .{ .override = .{ .custom = b.fmt("lambda/{s}", .{example.name}) } },
        });

        const step_name = b.fmt("example-{s}", .{example.name});
        const step = b.step(step_name, b.fmt("Build {s} example", .{example.name}));
        step.dependOn(&install.step);
    }
}
