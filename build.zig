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
/// any other event-loop library; the in-tree loop is part of the product.
pub fn build(b: *std.Build) void {
    const builtin = @import("builtin");
    if (builtin.zig_version.major != 0 or builtin.zig_version.minor != 16 or builtin.zig_version.patch != 0 or builtin.zig_version.pre != null) {
        @compileError(std.fmt.comptimePrint("Unsupported Zig version: {}. ZPQ requires exactly the 0.16.0 release version to prevent standard library drift.", .{builtin.zig_version}));
    }

    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});

    // "Build the binary you want." The SQL frontend (liteparser, ~950 KB C)
    // is opt-in: on by default for the CLI, off for a minimal lean-core build
    // (`-Dsql=false`). The Lambda binary never includes it regardless — it's
    // JSON-event-driven and cold-start scales with size.
    const sql = b.option(bool, "sql", "Compile the SQL frontend into the CLI (default true; Lambda never includes it)") orelse true;

    const liteparser_mod = b.createModule(.{
        .target = target,
        .optimize = optimize,
        .link_libc = true,
    });
    liteparser_mod.addCSourceFiles(.{
        .root = b.path("vendor/liteparser"),
        .files = &.{
            "arena.c",
            "liteparser.c",
            "lp_tokenize.c",
            "lp_unparse.c",
            "parse.c",
        },
        .flags = &.{
            "-Wall",
            "-Wextra",
            "-Wno-unused-parameter",
            "-Wno-sign-compare",
            "-Wno-unused-variable",
            "-DNDEBUG",
        },
    });
    liteparser_mod.addIncludePath(b.path("vendor/liteparser"));

    const liteparser_lib = b.addLibrary(.{
        .name = "liteparser",
        .root_module = liteparser_mod,
    });

    // ----- CLI binary -----
    const cli_zpq = makeZpqModule(b, target, optimize, false, liteparser_lib, sql);
    const cli = b.addExecutable(.{
        .name = "zpq",
        .root_module = cli_zpq.root,
    });
    cli.root_module.addImport("zpq", cli_zpq.zpq);
    b.installArtifact(cli);

    const cli_step = b.step("cli", "Build the CLI binary (zpq)");
    cli_step.dependOn(&b.addInstallArtifact(cli, .{}).step);

    // ----- Lambda binary -----
    const lambda_zpq = makeZpqModule(b, target, optimize, true, liteparser_lib, sql);
    const lambda = b.addExecutable(.{
        .name = "zpq-lambda",
        .root_module = lambda_zpq.root,
    });
    lambda.root_module.addImport("zpq", lambda_zpq.zpq);
    b.installArtifact(lambda);

    const lambda_step = b.step("lambda", "Build the Lambda binary (zpq-lambda)");
    lambda_step.dependOn(&b.addInstallArtifact(lambda, .{}).step);

    // ----- Public library module -----
    // Lets downstream projects consume the engine as a dependency:
    //
    //   .zpq = .{ .url = "...", .hash = "..." }        (build.zig.zon)
    //   const zpq = b.dependency("zpq", .{ .target = t, .optimize = o })
    //       .module("zpq");
    //
    // Configuration is the workstation one (lambda=false, epoll backend).
    // The SQL frontend stays out: it's a CLI concern, and excluding it means
    // consumers never link the liteparser C sources. Everything else (codecs,
    // TLS, S3) comes along — the module is the same surface the binaries use.
    const pub_opts = b.addOptions();
    pub_opts.addOption(bool, "lambda", false);
    pub_opts.addOption(bool, "enable_sql", false);
    const pub_zpq = b.addModule("zpq", .{
        .root_source_file = b.path("src/zpq.zig"),
        .target = target,
        .optimize = optimize,
        .omit_frame_pointer = false,
        .imports = &.{
            .{ .name = "build_options", .module = pub_opts.createModule() },
            .{
                .name = "boring_tls",
                .module = b.dependency("boring_tls", .{
                    .target = target,
                    .optimize = optimize,
                }).module("boring_tls"),
            },
            .{
                .name = "snappy",
                .module = b.dependency("snappy", .{
                    .target = target,
                    .optimize = optimize,
                }).module("snappy"),
            },
        },
    });
    pub_zpq.linkLibrary(b.dependency("zstd", .{
        .target = target,
        .optimize = optimize,
        .dictbuilder = false,
    }).artifact("zstd"));

    // ----- Tests -----
    // Tests pin lambda=true since epoll is the only backend implemented;
    // other backends @compileError until they exist.
    const test_opts = b.addOptions();
    test_opts.addOption(bool, "lambda", true);
    test_opts.addOption(bool, "enable_sql", true); // tests exercise the SQL parser
    const test_opts_mod = test_opts.createModule();

    const test_boring_dep = b.dependency("boring_tls", .{
        .target = target,
        .optimize = optimize,
    });
    const test_boring_mod = test_boring_dep.module("boring_tls");

    const test_snappy_dep = b.dependency("snappy", .{
        .target = target,
        .optimize = optimize,
    });
    const test_snappy_mod = test_snappy_dep.module("snappy");

    const test_zstd_dep = b.dependency("zstd", .{
        .target = target,
        .optimize = optimize,
        .dictbuilder = false,
    });
    const test_zstd_lib = test_zstd_dep.artifact("zstd");

    const test_zpq_mod = b.createModule(.{
        .root_source_file = b.path("src/zpq.zig"),
        .target = target,
        .optimize = optimize,
        .imports = &.{
            .{ .name = "build_options", .module = test_opts_mod },
            .{ .name = "boring_tls", .module = test_boring_mod },
            .{ .name = "snappy", .module = test_snappy_mod },
        },
    });
    test_zpq_mod.linkLibrary(test_zstd_lib);
    test_zpq_mod.linkLibrary(liteparser_lib);
    test_zpq_mod.addIncludePath(b.path("vendor/liteparser"));

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
    // Disposable probe for std.Io.Threaded multipart S3 PUTs.
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
    // Probes are opt-in via their explicit step (`zig build bakeoff-threaded`)
    // — they each pull a full BoringSSL + zstd link, blowing past CI's
    // 10 min budget when included in the default install.
    const bakeoff_threaded_step = b.step("bakeoff-threaded", "Build the Io.Threaded multipart-PUT bakeoff probe");
    bakeoff_threaded_step.dependOn(&b.addInstallArtifact(bakeoff_threaded, .{}).step);

    // probe_r2_latency: measure DNS / TLS / Range GET cost from a
    // workstation against R2 (or any S3-compatible endpoint). Drives
    // the architectural decision on whether to plumb S3 through the
    // CLI directly or fan out per-file Lambdas. Output is JSON; see
    // probes/probe_r2_latency/main.zig.
    const probe_r2_latency = b.addExecutable(.{
        .name = "probe_r2_latency",
        .root_module = b.createModule(.{
            .root_source_file = b.path("probes/probe_r2_latency/main.zig"),
            .target = target,
            .optimize = optimize,
            .link_libc = true,
            .imports = &.{
                .{ .name = "zpq", .module = cli_zpq.zpq },
            },
        }),
    });
    const probe_r2_latency_step = b.step("probe-r2-latency", "Build the R2/S3 latency probe");
    probe_r2_latency_step.dependOn(&b.addInstallArtifact(probe_r2_latency, .{}).step);

    // probe_r2_list: confirm ListObjectsV2 against R2/S3, time it,
    // exercise pagination. Inputs glob expansion design for `s3://...`.
    const probe_r2_list = b.addExecutable(.{
        .name = "probe_r2_list",
        .root_module = b.createModule(.{
            .root_source_file = b.path("probes/probe_r2_list/main.zig"),
            .target = target,
            .optimize = optimize,
            .link_libc = true,
            .imports = &.{
                .{ .name = "zpq", .module = cli_zpq.zpq },
            },
        }),
    });
    const probe_r2_list_step = b.step("probe-r2-list", "Build the ListObjectsV2 probe");
    probe_r2_list_step.dependOn(&b.addInstallArtifact(probe_r2_list, .{}).step);

    // probe_simd_decoders: compare zigzag, bit-unpacking, and gather performance
    const probe_simd_decoders = b.addExecutable(.{
        .name = "probe_simd_decoders",
        .root_module = b.createModule(.{
            .root_source_file = b.path("probes/probe_simd_decoders/main.zig"),
            .target = target,
            .optimize = optimize,
            .link_libc = true,
        }),
    });
    const probe_simd_decoders_step = b.step("probe-simd-decoders", "Build the SIMD decoders microbenchmark probe");
    probe_simd_decoders_step.dependOn(&b.addInstallArtifact(probe_simd_decoders, .{}).step);

    // microbench: time decode of one column-chunk in isolation.
    // Used to map ns/value across (encoding × type × bit_width × null
    // rate) — without the glob/mmap/aggregate noise of the full
    // pipeline. See benchmarks/microbench/.
    const microbench = b.addExecutable(.{
        .name = "microbench",
        .root_module = b.createModule(.{
            .root_source_file = b.path("benchmarks/microbench/main.zig"),
            .target = target,
            .optimize = optimize,
            .link_libc = true,
            .imports = &.{
                .{ .name = "zpq", .module = cli_zpq.zpq },
            },
        }),
    });
    const microbench_step = b.step("microbench", "Build the decode-path microbenchmark");
    microbench_step.dependOn(&b.addInstallArtifact(microbench, .{}).step);
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
    liteparser_lib: *std.Build.Step.Compile,
    sql: bool,
) Bundle {
    const opts = b.addOptions();
    opts.addOption(bool, "lambda", is_lambda);
    // SQL frontend (liteparser) is opt-in and CLI-only: Lambda is
    // JSON-event-driven and cold-start cost scales with binary size, so the
    // ~950 KB parser + its C dep are never in the Lambda build; the CLI gets
    // it unless `-Dsql=false` asks for a minimal binary. Gated via
    // build_options so the sql_parser module simply isn't compiled when off.
    const enable_sql = !is_lambda and sql;
    opts.addOption(bool, "enable_sql", enable_sql);
    const opts_mod = opts.createModule();

    // BoringSSL bindings — required for HTTPS to S3 (and any other TLS).
    const boring_tls_dep = b.dependency("boring_tls", .{
        .target = target,
        .optimize = optimize,
    });
    const boring_tls_mod = boring_tls_dep.module("boring_tls");

    // google/snappy 1.2.1 — vendor source-built, generic implementation.
    // Replaces the hand-rolled zig snappy compressor (still used for
    // decode-side fallback; the C version is 3-5× faster on compress).
    const snappy_dep = b.dependency("snappy", .{
        .target = target,
        .optimize = optimize,
    });
    const snappy_mod = snappy_dep.module("snappy");

    // facebook/zstd 1.5.7 — vendor via allyourcodebase/zstd. Used for
    // both encode (E2b) and decode. Zig stdlib's pure-Zig zstd
    // decoder works correctly but is ~10× slower than libzstd on
    // dict-encoded numeric column-chunks (perf profile 2026-05-06:
    // 92% of CLI aggregate CPU time was in `std.compress.zstd`).
    // Same library, different entry point — minor binary-size cost
    // for a large perf win.
    const zstd_dep = b.dependency("zstd", .{
        .target = target,
        .optimize = optimize,
        .dictbuilder = false,
    });
    const zstd_lib = zstd_dep.artifact("zstd");

    // Frame pointers on for both modules: makes `perf record --call-graph=fp`
    // produce clean stacks instead of DWARF unwinding gaps. Cost is 0-2% on
    // typical workloads (validated 2026-05-07 against the regression suite —
    // medians within noise). Worth it for always-available flamegraphs.
    // Flip to `true` (or remove) if a hot path ever shows real register-
    // pressure regression.
    const zpq_mod = b.createModule(.{
        .root_source_file = b.path("src/zpq.zig"),
        .target = target,
        .optimize = optimize,
        .omit_frame_pointer = false,
        .imports = &.{
            .{ .name = "build_options", .module = opts_mod },
            .{ .name = "boring_tls", .module = boring_tls_mod },
            .{ .name = "snappy", .module = snappy_mod },
        },
    });
    zpq_mod.linkLibrary(zstd_lib);
    if (enable_sql) {
        zpq_mod.linkLibrary(liteparser_lib);
        zpq_mod.addIncludePath(b.path("vendor/liteparser"));
    }

    const root_path = if (is_lambda) "src/lambda/main.zig" else "src/cli/main.zig";
    const root_mod = b.createModule(.{
        .root_source_file = b.path(root_path),
        .target = target,
        .optimize = optimize,
        .omit_frame_pointer = false,
        // libc gives us getaddrinfo; under -O ReleaseSmall + musl static
        // it adds ~150 KB which is dwarfed by BoringSSL anyway. Until
        // we ship our own DNS resolver, this is the right tradeoff.
        .link_libc = true,
        .imports = &.{
            .{ .name = "build_options", .module = opts_mod },
            .{ .name = "boring_tls", .module = boring_tls_mod },
            .{ .name = "snappy", .module = snappy_mod },
        },
    });
    if (enable_sql) {
        root_mod.linkLibrary(liteparser_lib);
        root_mod.addIncludePath(b.path("vendor/liteparser"));
    }

    return .{ .root = root_mod, .zpq = zpq_mod };
}
