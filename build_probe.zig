const std = @import("std");

pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});
    const no_omit_frame_pointer = b.option(bool, "no-omit-frame-pointer", "Don't omit frame pointer") orelse false;

    // Dependencies
    const libxev_dep = b.dependency("libxev", .{
        .target = target,
        .optimize = optimize,
    });
    const libxev_mod = libxev_dep.module("xev");

    const boring_tls_dep = b.dependency("boring_tls", .{
        .target = target,
        .optimize = .ReleaseFast, // BoringTLS usually release fast
    });
    const boring_tls_mod = boring_tls_dep.module("boring_tls");

    // ZPQ options (minimal for probe)
    const zpq_options = b.addOptions();
    zpq_options.addOption(bool, "enable_zstd_compression", false);

    const zpq_mod = b.addModule("zpq", .{
        .root_source_file = b.path("src/zpq.zig"),
        .target = target,
        .optimize = optimize,
        .imports = &.{
            .{ .name = "xev", .module = libxev_mod },
            .{ .name = "boring_tls", .module = boring_tls_mod },
            .{ .name = "zpq_options", .module = zpq_options.createModule() },
        },
    });

    // Probe: Local Parquet
    const probe_local = b.addExecutable(.{
        .name = "probe_local_parquet",
        .root_module = b.createModule(.{
            .root_source_file = b.path("probes/probe_local_parquet.zig"),
            .target = target,
            .optimize = optimize,
            .imports = &.{
                .{ .name = "zpq", .module = zpq_mod },
            },
        }),
    });
    probe_local.root_module.omit_frame_pointer = !no_omit_frame_pointer;

    const run_cmd_local = b.addRunArtifact(probe_local);
    run_cmd_local.step.dependOn(b.getInstallStep());
    if (b.args) |args| {
        run_cmd_local.addArgs(args);
    }
    const run_step_local = b.step("probe-local-parquet", "Run the local parquet probe");
    run_step_local.dependOn(&run_cmd_local.step);

    // Probe: S3 Parquet
    const probe_s3 = b.addExecutable(.{
        .name = "probe_s3_parquet",
        .root_module = b.createModule(.{
            .root_source_file = b.path("probes/probe_s3_parquet.zig"),
            .target = target,
            .optimize = optimize,
            .imports = &.{
                .{ .name = "zpq", .module = zpq_mod },
                .{ .name = "xev", .module = libxev_mod },
            },
        }),
    });

    const run_cmd_s3 = b.addRunArtifact(probe_s3);
    run_cmd_s3.step.dependOn(b.getInstallStep());
    if (b.args) |args| {
        run_cmd_s3.addArgs(args);
    }
    const run_step_s3 = b.step("probe-s3-parquet", "Run the S3 parquet probe");
    run_step_s3.dependOn(&run_cmd_s3.step);

    // Probe: Filter Scan
    const probe_filter = b.addExecutable(.{
        .name = "probe_filter_scan",
        .root_module = b.createModule(.{
            .root_source_file = b.path("probes/probe_filter_scan.zig"),
            .target = target,
            .optimize = optimize,
            .imports = &.{
                .{ .name = "zpq", .module = zpq_mod },
            },
        }),
    });
    probe_local.root_module.omit_frame_pointer = !no_omit_frame_pointer;
    probe_s3.root_module.omit_frame_pointer = !no_omit_frame_pointer;
    probe_filter.root_module.omit_frame_pointer = !no_omit_frame_pointer;

    const run_cmd_filter = b.addRunArtifact(probe_filter);
    run_cmd_filter.step.dependOn(b.getInstallStep());
    if (b.args) |args| {
        run_cmd_filter.addArgs(args);
    }
    const run_step_filter = b.step("probe-filter-scan", "Run the filter scan probe");
    run_step_filter.dependOn(&run_cmd_filter.step);

    // Probe: Schema
    const probe_schema = b.addExecutable(.{
        .name = "probe_schema",
        .root_module = b.createModule(.{
            .root_source_file = b.path("probes/probe_schema.zig"),
            .target = target,
            .optimize = optimize,
            .imports = &.{
                .{ .name = "zpq", .module = zpq_mod },
            },
        }),
    });
    probe_schema.root_module.omit_frame_pointer = !no_omit_frame_pointer;

    const run_cmd_schema = b.addRunArtifact(probe_schema);
    run_cmd_schema.step.dependOn(b.getInstallStep());
    if (b.args) |args| {
        run_cmd_schema.addArgs(args);
    }
    const run_step_schema = b.step("probe-schema", "Run the schema probe");
    run_step_schema.dependOn(&run_cmd_schema.step);

    // Probe: Sink
    const probe_sink = b.addExecutable(.{
        .name = "probe_sink",
        .root_module = b.createModule(.{
            .root_source_file = b.path("probes/probe_sink.zig"),
            .target = target,
            .optimize = optimize,
            .imports = &.{
                .{ .name = "zpq", .module = zpq_mod },
                .{ .name = "xev", .module = libxev_mod },
            },
        }),
    });
    probe_sink.root_module.omit_frame_pointer = !no_omit_frame_pointer;

    const run_cmd_sink = b.addRunArtifact(probe_sink);
    run_cmd_sink.step.dependOn(b.getInstallStep());
    if (b.args) |args| {
        run_cmd_sink.addArgs(args);
    }
    const run_step_sink = b.step("probe-sink", "Run the sink probe");
    run_step_sink.dependOn(&run_cmd_sink.step);

    // Probe: Writer
    const probe_writer = b.addExecutable(.{
        .name = "probe_writer",
        .root_module = b.createModule(.{
            .root_source_file = b.path("probes/probe_writer.zig"),
            .target = target,
            .optimize = optimize,
            .imports = &.{
                .{ .name = "zpq", .module = zpq_mod },
            },
        }),
    });
    probe_writer.root_module.omit_frame_pointer = !no_omit_frame_pointer;

    const run_cmd_writer = b.addRunArtifact(probe_writer);
    run_cmd_writer.step.dependOn(b.getInstallStep());
    if (b.args) |args| {
        run_cmd_writer.addArgs(args);
    }
    const run_step_writer = b.step("probe-writer", "Run the writer probe");
    run_step_writer.dependOn(&run_cmd_writer.step);
}
