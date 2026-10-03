const std = @import("std");
const zon = @import("build.zig.zon");

/// ZPQ build system.
///
/// Produces two binaries from one source tree:
///   - `zpq`        : CLI binary. Includes the SQL frontend unless
///                    `-Dsql=false`.
///   - `zpq-lambda` : Lambda bootstrap binary. JSON-event-driven, so it
///                    never includes the SQL frontend.
///
/// Both run the same engine (src/zpq.zig): blocking sockets over BoringSSL
/// for S3 and `std.Io` for concurrency, with no event-loop library. Each
/// binary compiles its own instance of that module so its `build_options`
/// (`enable_sql`, `version`) resolve at comptime.
pub fn build(b: *std.Build) void {
    const builtin = @import("builtin");
    if (builtin.zig_version.major != 0 or builtin.zig_version.minor != 17 or builtin.zig_version.patch != 0 or builtin.zig_version.pre != null) {
        @compileError(std.fmt.comptimePrint("Unsupported Zig version: {}. ZPQ requires exactly the 0.17.0 release version to prevent standard library drift.", .{builtin.zig_version}));
    }

    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});

    // "Build the binary you want." The SQL frontend (liteparser, ~950 KB C)
    // is opt-in: on by default for the CLI, off for a minimal lean-core build
    // (`-Dsql=false`). The Lambda binary never includes it regardless — it's
    // JSON-event-driven and cold-start scales with size.
    const sql = b.option(bool, "sql", "Compile the SQL frontend into the CLI (default true; Lambda never includes it)") orelse true;

    // BoringSSL comes prebuilt: from vendor/boring_tls/prebuilt/<arch>-<os>/
    // when present, else downloaded and sha256-checked by a build step.
    // `-Dfetch-prebuilt=false` turns a missing local copy into an error.
    const fetch_prebuilt = b.option(bool, "fetch-prebuilt", "Download pinned BoringSSL prebuilts that vendor/boring_tls/prebuilt lacks (default true)") orelse true;

    const deps = CoreDeps.init(b, target, optimize, fetch_prebuilt);

    // ----- CLI binary -----
    const cli_zpq = addBinaryModules(b, deps, target, optimize, .cli, sql);
    const cli = b.addExecutable(.{
        .name = "zpq",
        .root_module = cli_zpq.root,
    });
    b.installArtifact(cli);

    const cli_step = b.step("cli", "Build the CLI binary (zpq)");
    cli_step.dependOn(&b.addInstallArtifact(cli, .{}).step);

    // ----- Lambda binary -----
    const lambda_zpq = addBinaryModules(b, deps, target, optimize, .lambda, sql);
    const lambda = b.addExecutable(.{
        .name = "zpq-lambda",
        .root_module = lambda_zpq.root,
    });
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
    // The SQL frontend stays out: it's a CLI concern, and excluding it means
    // consumers never link the liteparser C sources. Everything else (codecs,
    // TLS, S3) comes along — the module is the same surface the binaries use.
    _ = addCoreModule(b, deps, target, optimize, .{
        .sql = false,
        .export_as = "zpq",
    });

    // ----- Tests -----
    // Tests compile the SQL frontend in, as the CLI does, and also root the
    // Lambda's own sources (src/lambda/main.zig), so one test build covers both.
    const test_core = addCoreModule(b, deps, target, optimize, .{
        .sql = true, // tests exercise the SQL parser
    });
    const test_zpq_mod = test_core.zpq;
    const test_opts_mod = test_core.build_options;

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

    // Build the unit-test executables without running them, so a cross-compiled pair can be copied to and run on
    // the target (tools/macos_bundle.sh). Run them from the repo root (or a bundle mirroring it): fixtures are
    // cwd-relative.
    const test_bin_step = b.step("test-bin", "Install the unit-test executables (zpq-test, zpq-lambda-test) without running them");
    test_bin_step.dependOn(&b.addInstallArtifact(lib_tests, .{ .dest_sub_path = "zpq-test" }).step);
    test_bin_step.dependOn(&b.addInstallArtifact(lambda_tests, .{ .dest_sub_path = "zpq-lambda-test" }).step);

    // Opt-in soak tests for edge-case regressions. Kept out of the default
    // unit-test step so Tier 1/Tier 2 stay fast; `just gauntlet` runs these.
    const soak_tests = b.addTest(.{
        .root_module = b.createModule(.{
            .root_source_file = b.path("tests/soak.zig"),
            .target = target,
            .optimize = optimize,
            .imports = &.{
                .{ .name = "zpq", .module = test_zpq_mod },
            },
        }),
    });
    const run_soak_tests = b.addRunArtifact(soak_tests);

    const soak_step = b.step("test-soak", "Run opt-in soak/regression tests");
    soak_step.dependOn(&run_soak_tests.step);

    // ----- Integration tests (Lambda fake runtime) -----
    // Spawns the built zpq-lambda binary against an in-process fake
    // runtime API. Doesn't run as part of `zig build test` because it
    // requires the binary to be installed first; run with
    // `zig build test-integration`.
    const integration_opts = b.addOptions();
    const lambda_install = b.addInstallArtifact(lambda, .{});
    integration_opts.addOptionPath("lambda_bin", lambda.getEmittedBin());

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

    // bench-labels: time output labelling on wide schemas, with and without colliding names. Opt-in, so wall-clock
    // limits stay out of the unit tests. See benchmarks/labels/.
    const bench_labels = b.addExecutable(.{
        .name = "bench_labels",
        .root_module = b.createModule(.{
            .root_source_file = b.path("benchmarks/labels/main.zig"),
            .target = target,
            .optimize = optimize,
            .link_libc = true,
            .imports = &.{
                .{ .name = "zpq", .module = cli_zpq.zpq },
            },
        }),
    });
    const bench_labels_step = b.step("bench-labels", "Time output labelling on wide and colliding schemas");
    bench_labels_step.dependOn(&b.addRunArtifact(bench_labels).step);
}

/// Third-party modules and libraries every zpq module instance links. Built
/// once per (target, optimize) and shared, so dependency wiring lives in one
/// place however many option sets the build compiles.
const CoreDeps = struct {
    /// BoringSSL bindings — required for HTTPS to S3 (and any other TLS).
    boring_tls: *std.Build.Module,
    /// google/snappy 1.2.1 — vendor source-built, generic implementation.
    /// Replaces the hand-rolled zig snappy compressor (still used for
    /// decode-side fallback; the C version is 3-5× faster on compress).
    snappy: *std.Build.Module,
    /// facebook/zstd 1.5.7 — vendor via allyourcodebase/zstd. Used for
    /// both encode (E2b) and decode. Zig stdlib's pure-Zig zstd
    /// decoder works correctly but is ~10× slower than libzstd on
    /// dict-encoded numeric column-chunks (perf profile 2026-05-06:
    /// 92% of CLI aggregate CPU time was in `std.compress.zstd`).
    /// Same library, different entry point — minor binary-size cost
    /// for a large perf win.
    zstd: *std.Build.Step.Compile,
    zstd_c: *std.Build.Module,
    /// The SQL frontend's C parser. Only linked into modules built with
    /// `sql = true`; an unreferenced library step is never compiled.
    liteparser: *std.Build.Step.Compile,
    liteparser_c: *std.Build.Module,

    fn init(
        b: *std.Build,
        target: std.Build.ResolvedTarget,
        optimize: std.builtin.Optimize,
        fetch_prebuilt: bool,
    ) CoreDeps {
        const zstd = b.dependency("zstd", .{
            .target = target,
            .optimize = optimize,
            .dictbuilder = false,
        }).artifact("zstd");

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

        return .{
            .boring_tls = b.dependency("boring_tls", .{
                .target = target,
                .optimize = optimize,
                .@"fetch-prebuilt" = fetch_prebuilt,
            }).module("boring_tls"),
            .snappy = b.dependency("snappy", .{
                .target = target,
                .optimize = optimize,
            }).module("snappy"),
            .zstd = zstd,
            .zstd_c = zstdCModule(b, target, optimize, zstd),
            .liteparser = b.addLibrary(.{
                .name = "liteparser",
                .root_module = liteparser_mod,
            }),
            .liteparser_c = liteparserCModule(b, target, optimize),
        };
    }
};

const CoreOptions = struct {
    /// Compile the SQL frontend (`build_options.enable_sql`) and link
    /// liteparser.
    sql: bool,
    /// Register the module under this name for dependents
    /// (`b.dependency("zpq", ...).module(name)`).
    export_as: ?[]const u8 = null,
};

/// A zpq core module (src/zpq.zig) and the build_options module it was
/// compiled with; roots that branch on the same options import both.
const Core = struct {
    zpq: *std.Build.Module,
    build_options: *std.Build.Module,
};

/// The one place a zpq core module is assembled. Each distinct option set
/// needs its own module instance, because `build_options` is resolved at
/// comptime inside the zpq namespace.
fn addCoreModule(
    b: *std.Build,
    deps: CoreDeps,
    target: std.Build.ResolvedTarget,
    optimize: std.builtin.Optimize,
    options: CoreOptions,
) Core {
    const opts = b.addOptions();
    opts.addOption(bool, "enable_sql", options.sql);
    opts.addOption([]const u8, "version", zon.version);
    const opts_mod = opts.createModule();

    // Frame pointers on: makes `perf record --call-graph=fp` produce clean
    // stacks instead of DWARF unwinding gaps. Cost is 0-2% on typical
    // workloads (validated 2026-05-07 against the regression suite —
    // medians within noise). Worth it for always-available flamegraphs.
    // Flip to `true` (or remove) if a hot path ever shows real register-
    // pressure regression.
    const module_options: std.Build.Module.CreateOptions = .{
        .root_source_file = b.path("src/zpq.zig"),
        .target = target,
        .optimize = optimize,
        .omit_frame_pointer = false,
        .imports = &.{
            .{ .name = "build_options", .module = opts_mod },
            .{ .name = "boring_tls", .module = deps.boring_tls },
            .{ .name = "snappy", .module = deps.snappy },
            .{ .name = "zstd_c", .module = deps.zstd_c },
        },
    };
    const zpq_mod = if (options.export_as) |name|
        b.addModule(name, module_options)
    else
        b.createModule(module_options);
    zpq_mod.linkLibrary(deps.zstd);
    if (options.sql) {
        zpq_mod.linkLibrary(deps.liteparser);
        zpq_mod.addIncludePath(b.path("vendor/liteparser"));
        zpq_mod.addImport("liteparser_c", deps.liteparser_c);
    }
    return .{ .zpq = zpq_mod, .build_options = opts_mod };
}

const Binary = enum { cli, lambda };

/// A binary's root module plus the zpq core module it imports.
const BinaryModules = struct {
    /// The binary's root_source_file module (cli/main.zig or lambda/main.zig).
    root: *std.Build.Module,
    /// The zpq module (src/zpq.zig) wired with this binary's build_options.
    zpq: *std.Build.Module,
};

fn addBinaryModules(
    b: *std.Build,
    deps: CoreDeps,
    target: std.Build.ResolvedTarget,
    optimize: std.builtin.Optimize,
    binary: Binary,
    sql: bool,
) BinaryModules {
    const is_lambda = binary == .lambda;
    // SQL frontend (liteparser) is opt-in and CLI-only: Lambda is
    // JSON-event-driven and cold-start cost scales with binary size, so the
    // ~950 KB parser + its C dep are never in the Lambda build; the CLI gets
    // it unless `-Dsql=false` asks for a minimal binary. Gated via
    // build_options so the sql_parser module simply isn't compiled when off.
    const enable_sql = !is_lambda and sql;
    const core = addCoreModule(b, deps, target, optimize, .{ .sql = enable_sql });

    const root_mod = b.createModule(.{
        .root_source_file = b.path(if (is_lambda) "src/lambda/main.zig" else "src/cli/main.zig"),
        .target = target,
        .optimize = optimize,
        .omit_frame_pointer = false,
        // libc gives us getaddrinfo; under -O ReleaseSmall + musl static
        // it adds ~150 KB which is dwarfed by BoringSSL anyway. Until
        // we ship our own DNS resolver, this is the right tradeoff.
        .link_libc = true,
        .imports = &.{
            .{ .name = "zpq", .module = core.zpq },
            .{ .name = "build_options", .module = core.build_options },
            .{ .name = "boring_tls", .module = deps.boring_tls },
            .{ .name = "snappy", .module = deps.snappy },
        },
    });
    if (enable_sql) {
        root_mod.linkLibrary(deps.liteparser);
        root_mod.addIncludePath(b.path("vendor/liteparser"));
    }

    return .{ .root = root_mod, .zpq = core.zpq };
}

/// Translates a one-off C header into a module. Replaces `@cImport`, which
/// Zig 0.17 removed.
fn cHeaderModule(
    b: *std.Build,
    target: std.Build.ResolvedTarget,
    optimize: std.builtin.Optimize,
    name: []const u8,
    contents: []const u8,
) *std.Build.Step.TranslateC {
    const header = b.addWriteFiles().add(name, contents);
    return b.addTranslateC(.{
        .root_source_file = header,
        .target = target,
        .optimize = optimize,
    });
}

fn zstdCModule(
    b: *std.Build,
    target: std.Build.ResolvedTarget,
    optimize: std.builtin.Optimize,
    zstd_lib: *std.Build.Step.Compile,
) *std.Build.Module {
    const tc = cHeaderModule(b, target, optimize, "zstd_c.h", "#include <zstd.h>\n");
    // The header tree is a WriteFile step the library step does not depend on; addIncludePath orders
    // translate-c after it. An `.other_step` include dir skipped that edge and raced the maker into a panic.
    tc.addIncludePath(zstd_lib.getEmittedIncludeTree());
    return tc.createModule();
}

fn liteparserCModule(
    b: *std.Build,
    target: std.Build.ResolvedTarget,
    optimize: std.builtin.Optimize,
) *std.Build.Module {
    const tc = cHeaderModule(b, target, optimize, "liteparser_c.h",
        \\#include "liteparser.h"
        \\#include "arena.h"
        \\
    );
    tc.addIncludePath(b.path("vendor/liteparser"));
    return tc.createModule();
}
