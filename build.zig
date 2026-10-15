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

    // Profiling (`zpq query --profile`, the Lambda's `"profile"`) costs one branch per span site when not asked for;
    // `-Dprofile=false` compiles the span sites out entirely.
    const profile = b.option(bool, "profile", "Compile in query profiling (--profile; default true)") orelse true;

    var deps = CoreDeps.init(b, target, optimize, fetch_prebuilt, false);
    deps.profile = profile;

    // ----- CLI binary -----
    const cli_zpq = addBinaryModules(b, deps, target, optimize, .cli, sql);
    const cli_version = b.addOptions();
    var revision_status: u8 = 0;
    const revision_args: []const []const u8 = &.{ "git", "describe", "--always", "--dirty", "--exclude=*", "--abbrev=12" };
    const revision = b.runAllowFail(revision_args, &revision_status, .ignore) catch "unknown";
    cli_version.addOption([]const u8, "revision", std.mem.trim(u8, revision, "\r\n"));
    cli_zpq.root.addImport("cli_version", cli_version.createModule());
    const cli = b.addExecutable(.{
        .name = "zpq",
        .root_module = cli_zpq.root,
    });
    b.installArtifact(cli);

    const cli_step = b.step("cli", "Build the CLI binary (zpq)");
    cli_step.dependOn(&b.addInstallArtifact(cli, .{}).step);

    const cli_tests = b.addTest(.{ .root_module = cli_zpq.root });
    const test_cli_step = b.step("test-cli", "Run CLI inspection tests");
    test_cli_step.dependOn(&b.addRunArtifact(cli_tests).step);

    // ----- Lambda binary -----
    const lambda_zpq = addBinaryModules(b, deps, target, optimize, .lambda, sql);
    const lambda = b.addExecutable(.{
        .name = "zpq-lambda",
        .root_module = lambda_zpq.root,
    });
    b.installArtifact(lambda);

    const lambda_step = b.step("lambda", "Build the Lambda binary (zpq-lambda)");
    lambda_step.dependOn(&b.addInstallArtifact(lambda, .{}).step);

    // ----- libzpq: the JSON handler behind a C ABI (include/zpq.h) -----
    // A shared library carrying everything it links, and a static archive of zpq's own objects (a static link adds
    // the codec and BoringSSL archives; see docs/libzpq.md). Opt-in: `zig build libzpq`.
    var libzpq_deps = CoreDeps.init(b, target, optimize, fetch_prebuilt, true);
    libzpq_deps.profile = profile;
    const libzpq_core = addCoreModule(b, libzpq_deps, target, optimize, .{ .sql = false });
    const libzpq_mod = b.createModule(.{
        .root_source_file = b.path("src/libzpq/libzpq.zig"),
        .target = target,
        .optimize = optimize,
        .omit_frame_pointer = false,
        .link_libc = true,
        .pic = true,
        .imports = &.{
            .{ .name = "zpq", .module = libzpq_core.zpq },
            .{ .name = "service", .module = serviceModule(b, target, optimize, libzpq_core.zpq) },
        },
    });
    const libzpq_step = b.step("libzpq", "Build libzpq (shared and static) and install include/zpq.h");
    for ([_]std.builtin.LinkMode{ .dynamic, .static }) |linkage| {
        const lib = b.addLibrary(.{ .name = "zpq", .linkage = linkage, .root_module = libzpq_mod });
        if (linkage == .dynamic and target.result.ofmt == .elf) {
            lib.setVersionScript(b.path("src/libzpq/libzpq.map"));
            // Zig's own ELF linker ignores version scripts; LLD needs the LLVM backend, which Debug otherwise skips.
            lib.use_llvm = true;
            lib.use_lld = true;
        }
        libzpq_step.dependOn(&b.addInstallArtifact(lib, .{}).step);
    }
    libzpq_step.dependOn(&b.addInstallFile(b.path("include/zpq.h"), "include/zpq.h").step);

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

    // `-Dtest-filter=TEXT` (repeatable) builds only the tests whose names contain TEXT, to loop a few of them.
    const test_filters = b.option(
        []const []const u8,
        "test-filter",
        "Build only the unit tests whose names contain this text",
    ) orelse &.{};

    // Sans-IO + io tests (live in src/zpq.zig and what it imports).
    const lib_tests = b.addTest(.{
        .root_module = test_zpq_mod,
        .filters = test_filters,
    });
    const run_lib_tests = b.addRunArtifact(lib_tests);

    // Lambda binary tests (runtime.zig and anything else lambda-only).
    // Reuses the same zpq module so import paths match the production build.
    const test_service_mod = serviceModule(b, target, optimize, test_zpq_mod);
    const lambda_tests = b.addTest(.{
        .root_module = b.createModule(.{
            .root_source_file = b.path("src/lambda/main.zig"),
            .target = target,
            .optimize = optimize,
            .link_libc = true,
            .imports = &.{
                .{ .name = "zpq", .module = test_zpq_mod },
                .{ .name = "build_options", .module = test_opts_mod },
                .{ .name = "service", .module = test_service_mod },
            },
        }),
        .filters = test_filters,
    });
    const run_lambda_tests = b.addRunArtifact(lambda_tests);
    // The shared JSON handler's own tests (src/service).
    const service_tests = b.addTest(.{ .root_module = test_service_mod, .filters = test_filters });
    const run_service_tests = b.addRunArtifact(service_tests);

    // The fuzz targets (`test "fuzz: ..."`) live in the core unit tests alone; `zig build fuzz --fuzz[=N]` searches
    // them without the other test binaries, which have none and so no coverage to report. LLVM-built: the coverage
    // instrumentation the fuzzer reads comes from that backend, not Debug's default one.
    const fuzz_tests = b.addTest(.{ .root_module = test_zpq_mod, .filters = test_filters, .use_llvm = true });
    const fuzz_step = b.step("fuzz", "Run the core unit tests built for `--fuzz` (with -Dtest-filter=\"fuzz:\")");
    fuzz_step.dependOn(&b.addRunArtifact(fuzz_tests).step);

    // The core unit tests alone, one binary: the loop to keep under `--watch -fincremental` while editing. Linked
    // against BoringSSL built from source: the pinned prebuilt archives repeat members, which lld tolerates and the
    // self-hosted linker that incremental builds use does not.
    const dev_core = addCoreModule(b, libzpq_deps, target, optimize, .{ .sql = true });
    const dev_tests = b.addTest(.{ .root_module = dev_core.zpq, .filters = test_filters });
    const test_core_step = b.step("test-core", "Run the core unit tests only (src/zpq.zig and what it imports)");
    test_core_step.dependOn(&b.addRunArtifact(dev_tests).step);

    const test_step = b.step("test", "Run unit tests");
    test_step.dependOn(&run_lib_tests.step);
    test_step.dependOn(&run_lambda_tests.step);
    test_step.dependOn(&run_service_tests.step);
    const libzpq_tests = b.addTest(.{ .root_module = b.createModule(.{
        .root_source_file = b.path("src/libzpq/libzpq.zig"),
        .target = target,
        .optimize = optimize,
        .link_libc = true,
        .imports = &.{
            .{ .name = "zpq", .module = test_zpq_mod },
            .{ .name = "service", .module = test_service_mod },
        },
    }), .filters = test_filters });
    test_step.dependOn(&b.addRunArtifact(libzpq_tests).step);

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
    // The CLI too, for the S3 paths a Lambda child can't show: in Debug its allocator reports leaks as it exits.
    integration_opts.addOptionPath("cli_bin", cli.getEmittedBin());

    const integration_tests = b.addTest(.{
        .root_module = b.createModule(.{
            .root_source_file = b.path("tests/lambda_integration.zig"),
            .target = target,
            .optimize = optimize,
            .imports = &.{
                .{ .name = "integration_opts", .module = integration_opts.createModule() },
            },
        }),
        .filters = test_filters,
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
    // in-bench: IN-list evaluation over one in-memory batch, with user-space instruction and cycle counters.
    const in_bench = b.addExecutable(.{
        .name = "in_bench",
        .root_module = b.createModule(.{
            .root_source_file = b.path("benchmarks/in_bench/main.zig"),
            .target = target,
            .optimize = optimize,
            .link_libc = true,
            .imports = &.{
                .{ .name = "zpq", .module = cli_zpq.zpq },
            },
        }),
    });
    b.step("in-bench", "Build the IN-list evaluation microbenchmark").dependOn(&b.addInstallArtifact(in_bench, .{}).step);

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
    /// facebook/zstd 1.5.7 — vendored (`vendor/zstd`, see vendor/README.md). Used for
    /// both encode (E2b) and decode. Zig stdlib's pure-Zig zstd
    /// decoder works correctly but is ~10× slower than libzstd on
    /// dict-encoded numeric column-chunks (perf profile 2026-05-06:
    /// 92% of CLI aggregate CPU time was in `std.compress.zstd`).
    /// Same library, different entry point — minor binary-size cost
    /// for a large perf win.
    zstd: *std.Build.Step.Compile,
    zstd_c: *std.Build.Module,
    /// ebiggers/libdeflate 1.26 — vendored (`vendor/libdeflate`, see vendor/README.md), decompression only: GZIP
    /// pages, about three times as fast as `std.compress.flate`.
    libdeflate: *std.Build.Step.Compile,
    libdeflate_c: *std.Build.Module,
    /// lz4/lz4 1.10.0 — vendored (`vendor/lz4`, see vendor/README.md), the block compressor only: LZ4_RAW pages are
    /// written with it and read with zpq's own decoder (src/core/parquet/lz4.zig).
    lz4: *std.Build.Step.Compile,
    lz4_c: *std.Build.Module,
    /// The SQL frontend's C parser. Only linked into modules built with
    /// `sql = true`; an unreferenced library step is never compiled.
    liteparser: *std.Build.Step.Compile,
    liteparser_c: *std.Build.Module,
    /// `build_options.enable_profile` for every module instance.
    profile: bool = true,

    fn init(
        b: *std.Build,
        target: std.Build.ResolvedTarget,
        optimize: std.builtin.Optimize,
        fetch_prebuilt: bool,
        /// BoringSSL compiled from source, position-independent, for a shared library; else the pinned prebuilts.
        boringssl_from_source: bool,
    ) CoreDeps {
        // BoringSSL from source is the shared library's (libzpq) build: see `archivePic`.
        const shared = boringssl_from_source;
        const zstd = zstdLibrary(b, target, optimize, shared);
        const libdeflate = libdeflateLibrary(b, target, optimize, shared);
        const lz4 = lz4Library(b, target, optimize, shared);

        const liteparser_mod = b.createModule(.{
            .target = target,
            .optimize = optimize,
            .link_libc = true,
            .pic = archivePic(shared),
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
            .flags = std.mem.concat(b.allocator, []const u8, &.{ &.{
                "-Wall",
                "-Wextra",
                "-Wno-unused-parameter",
                "-Wno-sign-compare",
                "-Wno-unused-variable",
                "-DNDEBUG",
            }, archiveFlags(shared) }) catch @panic("OOM"),
        });
        liteparser_mod.addIncludePath(b.path("vendor/liteparser"));

        return .{
            .boring_tls = b.dependency("boring_tls", .{
                .target = target,
                .optimize = optimize,
                .@"fetch-prebuilt" = fetch_prebuilt,
                .@"source-build" = boringssl_from_source,
            }).module("boring_tls"),
            .snappy = b.dependency("snappy", .{
                .target = target,
                .optimize = optimize,
                .pic = archivePic(shared),
                .@"hidden-symbols" = shared,
            }).module("snappy"),
            .zstd = zstd,
            .zstd_c = zstdCModule(b, target, optimize, zstd),
            .libdeflate = libdeflate,
            .libdeflate_c = libdeflateCModule(b, target, optimize, libdeflate),
            .lz4 = lz4,
            .lz4_c = lz4CModule(b, target, optimize, lz4),
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
    opts.addOption(bool, "enable_profile", deps.profile);
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
            .{ .name = "libdeflate_c", .module = deps.libdeflate_c },
            .{ .name = "lz4_c", .module = deps.lz4_c },
        },
    };
    const zpq_mod = if (options.export_as) |name|
        b.addModule(name, module_options)
    else
        b.createModule(module_options);
    zpq_mod.linkLibrary(deps.zstd);
    zpq_mod.linkLibrary(deps.libdeflate);
    zpq_mod.linkLibrary(deps.lz4);
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
    if (is_lambda) root_mod.addImport("service", serviceModule(b, target, optimize, core.zpq));

    return .{ .root = root_mod, .zpq = core.zpq };
}

/// The JSON request handler the Lambda and libzpq share (src/service/handler.zig).
fn serviceModule(
    b: *std.Build,
    target: std.Build.ResolvedTarget,
    optimize: std.builtin.Optimize,
    zpq_mod: *std.Build.Module,
) *std.Build.Module {
    return b.createModule(.{
        .root_source_file = b.path("src/service/handler.zig"),
        .target = target,
        .optimize = optimize,
        .omit_frame_pointer = false,
        .link_libc = true,
        .imports = &.{.{ .name = "zpq", .module = zpq_mod }},
    });
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

/// An archive a shared library (libzpq) links is position-independent, which musl targets otherwise are not, and
/// hides its symbols (`archiveFlags`), so the library exports its own API alone on every target, not only where its
/// version script applies (ELF). The CLI's and the Lambda's archives keep the target's defaults.
fn archivePic(shared: bool) ?bool {
    return if (shared) true else null;
}

fn archiveFlags(shared: bool) []const []const u8 {
    return if (shared) &.{"-fvisibility=hidden"} else &.{};
}

/// libzstd from `vendor/zstd/lib`: the sources, macros and headers the allyourcodebase/zstd package built before it
/// was vendored (compression and decompression, legacy frames from v0.5, no dictionary builder, the x86-64 Huffman
/// decoder in assembly).
fn zstdLibrary(
    b: *std.Build,
    target: std.Build.ResolvedTarget,
    optimize: std.builtin.Optimize,
    shared: bool,
) *std.Build.Step.Compile {
    const lib = b.addLibrary(.{
        .linkage = .static,
        .name = "zstd",
        .root_module = b.createModule(.{
            .target = target,
            .optimize = optimize,
            .link_libc = true,
            .pic = archivePic(shared),
        }),
    });
    const root = b.path("vendor/zstd/lib");
    lib.root_module.addCSourceFiles(.{ .root = root, .files = &.{
        "common/zstd_common.c",
        "common/threading.c",
        "common/entropy_common.c",
        "common/fse_decompress.c",
        "common/xxhash.c",
        "common/error_private.c",
        "common/pool.c",
        "compress/fse_compress.c",
        "compress/huf_compress.c",
        "compress/zstd_double_fast.c",
        "compress/zstd_compress_literals.c",
        "compress/zstdmt_compress.c",
        "compress/zstd_compress_superblock.c",
        "compress/zstd_opt.c",
        "compress/zstd_compress.c",
        "compress/zstd_compress_sequences.c",
        "compress/hist.c",
        "compress/zstd_ldm.c",
        "compress/zstd_lazy.c",
        "compress/zstd_preSplit.c",
        "compress/zstd_fast.c",
        "decompress/zstd_decompress.c",
        "decompress/huf_decompress.c",
        "decompress/zstd_ddict.c",
        "decompress/zstd_decompress_block.c",
        "legacy/zstd_v05.c",
        "legacy/zstd_v06.c",
        "legacy/zstd_v07.c",
    }, .flags = archiveFlags(shared) });
    // zpq's AVX-512 Huffman encoder, wired in by patches/0001 (vendor/README.md). Every target builds it; off x86-64
    // it is a stub and zstd's own encoder runs. huf4x_check.c is for the unit tests: nothing else references it, so
    // nothing else links it.
    lib.root_module.addCSourceFiles(.{ .root = b.path("vendor/zstd/zpq"), .files = &.{
        "huf4x_avx512.c",
        "huf4x_check.c",
    }, .flags = archiveFlags(shared) });
    lib.root_module.addCMacro("ZPQ_HUF4X_AVX512", "1");
    if (target.result.cpu.arch == .x86_64) {
        lib.root_module.addAssemblyFile(b.path("vendor/zstd/lib/decompress/huf_decompress_amd64.S"));
    } else {
        lib.root_module.addCMacro("ZSTD_DISABLE_ASM", "");
    }
    // zstd marks its API default-visibility itself; empty, these leave it to -fvisibility.
    if (shared) for ([_][]const u8{ "ZSTDLIB_VISIBLE", "ZSTDERRORLIB_VISIBLE", "ZDICTLIB_VISIBLE" }) |m| {
        lib.root_module.addCMacro(m, "");
    };
    lib.root_module.addCMacro("ZSTD_LEGACY_SUPPORT", "5");
    if (target.result.os.tag != .wasi) lib.root_module.addCMacro("ZSTD_MULTITHREAD", "1");
    lib.installHeader(b.path("vendor/zstd/lib/zstd.h"), "zstd.h");
    lib.installHeader(b.path("vendor/zstd/lib/zdict.h"), "zdict.h");
    lib.installHeader(b.path("vendor/zstd/lib/zstd_errors.h"), "zstd_errors.h");
    return lib;
}

fn zstdCModule(
    b: *std.Build,
    target: std.Build.ResolvedTarget,
    optimize: std.builtin.Optimize,
    zstd_lib: *std.Build.Step.Compile,
) *std.Build.Module {
    // The static-linking API (ZSTD_compressSequences) is safe: libzstd is vendored and linked statically.
    const tc = cHeaderModule(b, target, optimize, "zstd_c.h", "#define ZSTD_STATIC_LINKING_ONLY\n#include <zstd.h>\n");
    // The header tree is a WriteFile step the library step does not depend on; addIncludePath orders
    // translate-c after it. An `.other_step` include dir skipped that edge and raced the maker into a panic.
    tc.addIncludePath(zstd_lib.getEmittedIncludeTree());
    return tc.createModule();
}

/// libdeflate from `vendor/libdeflate`: the decompression sources only (DEFLATE and its gzip framing, CRC-32, the
/// allocator), with the x86 and arm CPU-feature probes, each of which compiles to nothing on other architectures.
/// libdeflate chooses its CPU-specific paths at run time and keeps a portable one for everything else.
fn libdeflateLibrary(
    b: *std.Build,
    target: std.Build.ResolvedTarget,
    optimize: std.builtin.Optimize,
    shared: bool,
) *std.Build.Step.Compile {
    const lib = b.addLibrary(.{
        .linkage = .static,
        .name = "deflate",
        .root_module = b.createModule(.{
            .target = target,
            .optimize = optimize,
            .link_libc = true,
            .pic = archivePic(shared),
        }),
    });
    lib.root_module.addCSourceFiles(.{ .root = b.path("vendor/libdeflate/lib"), .files = &.{
        "deflate_decompress.c",
        "gzip_decompress.c",
        "crc32.c",
        "utils.c",
        "x86/cpu_features.c",
        "arm/cpu_features.c",
    }, .flags = archiveFlags(shared) });
    // As for zstd, with patches/0001 (vendor/README.md) making the macro overridable.
    if (shared) lib.root_module.addCMacro("LIBDEFLATE_EXPORT_SYM", "");
    lib.root_module.addIncludePath(b.path("vendor/libdeflate"));
    lib.installHeader(b.path("vendor/libdeflate/libdeflate.h"), "libdeflate.h");
    return lib;
}

fn libdeflateCModule(
    b: *std.Build,
    target: std.Build.ResolvedTarget,
    optimize: std.builtin.Optimize,
    lib: *std.Build.Step.Compile,
) *std.Build.Module {
    const tc = cHeaderModule(b, target, optimize, "libdeflate_c.h", "#include <libdeflate.h>\n");
    tc.addIncludePath(lib.getEmittedIncludeTree());
    return tc.createModule();
}

/// liblz4 from `vendor/lz4`: `lz4.c`, the block format (no frames, no HC), as upstream builds it.
fn lz4Library(
    b: *std.Build,
    target: std.Build.ResolvedTarget,
    optimize: std.builtin.Optimize,
    shared: bool,
) *std.Build.Step.Compile {
    const lib = b.addLibrary(.{
        .linkage = .static,
        .name = "lz4",
        .root_module = b.createModule(.{
            .target = target,
            .optimize = optimize,
            .link_libc = true,
            .pic = archivePic(shared),
        }),
    });
    lib.root_module.addCSourceFiles(.{
        .root = b.path("vendor/lz4"),
        .files = &.{"lz4.c"},
        .flags = archiveFlags(shared),
    });
    if (shared) lib.root_module.addCMacro("LZ4LIB_VISIBILITY", ""); // as for zstd
    lib.installHeader(b.path("vendor/lz4/lz4.h"), "lz4.h");
    return lib;
}

fn lz4CModule(
    b: *std.Build,
    target: std.Build.ResolvedTarget,
    optimize: std.builtin.Optimize,
    lib: *std.Build.Step.Compile,
) *std.Build.Module {
    const tc = cHeaderModule(b, target, optimize, "lz4_c.h", "#include <lz4.h>\n");
    tc.addIncludePath(lib.getEmittedIncludeTree());
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
