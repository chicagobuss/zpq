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

    // Release artifacts ship stripped (tools/package_release.py, the release smoke and serverless/aws.sh
    // pass `-Dstrip=true`): debug info carries the builder's absolute paths into public binaries and makes their bytes
    // depend on where they were built. Off by default so local builds keep the symbols profilers and stack traces use.
    const strip = b.option(bool, "strip", "Strip debug info and symbols from zpq and zpq-lambda (default false)") orelse false;

    // A release candidate carries its tag in every version string (`--version`, `created_by`, profiles) so its output
    // is never mistaken for the final release's; tools/package_release.py derives it from the release tag.
    const prerelease = b.option([]const u8, "prerelease", "Prerelease identifier appended to the version (e.g. rc2)");
    const version = if (prerelease) |pre| blk: {
        if (!validPrerelease(pre)) {
            std.log.err("-Dprerelease={s}: want dot-separated [0-9A-Za-z-] identifiers (SemVer), e.g. rc2", .{pre});
            std.process.exit(1);
        }
        break :blk b.fmt("{s}-{s}", .{ zon.version, pre });
    } else zon.version;

    const test_filters = b.option(
        []const []const u8,
        "test-filter",
        "Build only the unit tests whose names contain this text",
    ) orelse &.{};
    const cfg: Config = .{
        .target = target,
        .fetch_prebuilt = fetch_prebuilt,
        .profile = profile,
        .version = version,
        .sql = sql,
        .strip = strip,
        .revision = b.option([]const u8, "revision", "Revision recorded in the artifact manifest") orelse
            @import("tools/build_revision.zig").current(b),
        .test_filters = test_filters,
    };
    generateTestRoots(b);
    const a = artifacts(b, cfg, optimize);

    b.installArtifact(a.cli);
    b.step("cli", "Build the CLI binary (zpq)").dependOn(&b.addInstallArtifact(a.cli, .{}).step);
    b.installArtifact(a.lambda);
    b.step("lambda", "Build the Lambda binary (zpq-lambda)").dependOn(&b.addInstallArtifact(a.lambda, .{}).step);

    // A shared library carrying everything it links, and a static archive of zpq's own objects (a static link adds
    // the codec and BoringSSL archives; see docs/libzpq.md). Opt-in: `zig build libzpq`.
    const libzpq_step = b.step("libzpq", "Build libzpq (shared and static) and install include/zpq.h");
    libzpq_step.dependOn(&b.addInstallArtifact(a.libzpq_shared, .{}).step);
    libzpq_step.dependOn(&b.addInstallArtifact(a.libzpq_static, .{}).step);
    libzpq_step.dependOn(&b.addInstallFile(b.path("include/zpq.h"), "include/zpq.h").step);

    // The correctness runner alone; `zig build check -Dtier=...` builds and runs a whole tier (addTiers).
    b.step("check-runner", "Build the correctness runner (zpq-check)").dependOn(&b.addInstallArtifact(a.check, .{}).step);
    b.step("check-test", "Run the correctness runner's own tests").dependOn(&b.addRunArtifact(a.check_tests).step);

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
    _ = addCoreModule(b, a.deps, target, optimize, .{
        .sql = false,
        .export_as = "zpq",
    });

    // ----- Tests -----
    b.step("test-cli", "Run CLI inspection tests").dependOn(&b.addRunArtifact(a.cli_tests).step);
    // The fuzz targets (`test "fuzz: ..."`) live in the core unit tests alone; `zig build fuzz --fuzz[=N]` searches
    // them without the other test binaries, which have none and so no coverage to report.
    b.step("fuzz", "Run the core unit tests built for `--fuzz` (with -Dtest-filter=\"fuzz:\")")
        .dependOn(&b.addRunArtifact(a.fuzz_tests).step);
    // The core unit tests alone, one binary: the loop to keep under `--watch -fincremental` while editing.
    b.step("test-core", "Run all core unit tests (-Doptimize selects mode; default Debug)")
        .dependOn(&b.addRunArtifact(a.core_tests).step);
    const test_step = b.step("test", "Run unit tests");
    for (a.unit_tests) |t| test_step.dependOn(&b.addRunArtifact(t.exe).step);
    // The unit-test executables without running them, to copy to and run on another host. Run them from the repo
    // root (or a bundle mirroring it): fixtures are cwd-relative.
    const test_bin_step = b.step("test-bin", "Install all unit-test executables without running them");
    for (a.unit_tests) |t| test_bin_step.dependOn(&b.addInstallArtifact(t.exe, .{ .dest_sub_path = t.bin }).step);
    test_bin_step.dependOn(&b.addInstallArtifact(a.core_tests, .{ .dest_sub_path = "zpq-core-test" }).step);
    // Spawns the built zpq-lambda against an in-process fake runtime API.
    b.step("test-integration", "Run Lambda integration tests").dependOn(&b.addRunArtifact(a.integration_tests).step);
    // The same harness built to drive binaries already shipped (the release smoke): `-Dintegration-runtime=DIR` names
    // the directory holding that release's zpq-lambda and zpq.
    if (b.option([]const u8, "integration-runtime", "Directory of shipped zpq-lambda and zpq for integration-bin")) |dir| {
        const opts = b.addOptions();
        opts.addOption([]const u8, "lambda_bin", b.fmt("{s}/zpq-lambda", .{dir}));
        opts.addOption([]const u8, "cli_bin", b.fmt("{s}/zpq", .{dir}));
        const harness = b.addTest(.{ .root_module = b.createModule(.{
            .root_source_file = b.path("tests/lambda_integration.zig"),
            .target = target,
            .optimize = optimize,
            .strip = strip,
            .imports = &.{.{ .name = "integration_opts", .module = opts.createModule() }},
        }) });
        b.step("integration-bin", "Install the Lambda integration harness for shipped binaries (zpq-integration)")
            .dependOn(&b.addInstallArtifact(harness, .{ .dest_sub_path = "zpq-integration" }).step);
    }

    addTiers(b, cfg);
}

/// Build settings every artifact graph shares; only the optimize mode differs between graphs.
const Config = struct {
    target: std.Build.ResolvedTarget,
    fetch_prebuilt: bool,
    profile: bool,
    version: []const u8,
    sql: bool,
    strip: bool,
    revision: []const u8,
    test_filters: []const []const u8,
};

const UnitTest = struct { exe: *std.Build.Step.Compile, bin: []const u8, stage: []const u8 };

/// Everything one optimize mode builds. Steps compile only what they reach, so a tier asking for ReleaseSafe tests
/// and a ReleaseFast CLI builds just those graphs.
const Artifacts = struct {
    deps: CoreDeps,
    cli: *std.Build.Step.Compile,
    lambda: *std.Build.Step.Compile,
    libzpq_shared: *std.Build.Step.Compile,
    libzpq_static: *std.Build.Step.Compile,
    check: *std.Build.Step.Compile,
    check_tests: *std.Build.Step.Compile,
    cli_tests: *std.Build.Step.Compile,
    core_tests: *std.Build.Step.Compile,
    fuzz_tests: *std.Build.Step.Compile,
    unit_tests: []const UnitTest,
    integration_tests: *std.Build.Step.Compile,
};

fn artifacts(b: *std.Build, cfg: Config, optimize: std.builtin.Optimize) Artifacts {
    const target = cfg.target;
    var deps = CoreDeps.init(b, target, optimize, cfg.fetch_prebuilt, false);
    deps.profile = cfg.profile;
    deps.version = cfg.version;

    const cli_zpq = addBinaryModules(b, deps, target, optimize, .cli, cfg.sql, cfg.strip);
    const cli_version = b.addOptions();
    cli_version.addOption([]const u8, "revision", cfg.revision);
    cli_zpq.root.addImport("cli_version", cli_version.createModule());
    const cli = b.addExecutable(.{ .name = "zpq", .root_module = cli_zpq.root });

    // The correctness runner: JSON cases, in-process zpq, the DuckDB CLI as the oracle.
    const check_module = b.createModule(.{
        .root_source_file = b.path("tools/check/main.zig"),
        .target = target,
        .optimize = optimize,
        .link_libc = true,
        .imports = &.{.{ .name = "zpq", .module = cli_zpq.zpq }},
    });
    const check = b.addExecutable(.{ .name = "zpq-check", .root_module = check_module });

    const lambda_zpq = addBinaryModules(b, deps, target, optimize, .lambda, cfg.sql, cfg.strip);
    const lambda = b.addExecutable(.{ .name = "zpq-lambda", .root_module = lambda_zpq.root });

    // libzpq: the JSON handler behind a C ABI (include/zpq.h).
    var libzpq_deps = CoreDeps.init(b, target, optimize, cfg.fetch_prebuilt, true);
    libzpq_deps.profile = cfg.profile;
    libzpq_deps.version = cfg.version;
    const libzpq_core = addCoreModule(b, libzpq_deps, target, optimize, .{ .sql = true });
    const libzpq_mod = b.createModule(.{
        .root_source_file = srcFile(b, "libzpq/libzpq.zig"),
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
    var libs: [2]*std.Build.Step.Compile = undefined;
    for ([_]std.builtin.LinkMode{ .dynamic, .static }, 0..) |linkage, i| {
        const lib = b.addLibrary(.{ .name = "zpq", .linkage = linkage, .root_module = libzpq_mod });
        if (linkage == .dynamic and target.result.ofmt == .elf) {
            lib.setVersionScript(b.path("src/libzpq/libzpq.map"));
            // Zig's own ELF linker ignores version scripts; LLD needs the LLVM backend, which Debug otherwise skips.
            lib.use_llvm = true;
            lib.use_lld = true;
        }
        libs[i] = lib;
    }

    // Tests compile the SQL frontend in, as the CLI does, and also root the Lambda's own sources.
    const filters = cfg.test_filters;
    const test_core = addCoreModule(b, deps, target, optimize, .{ .sql = true });
    const test_service = serviceModule(b, target, optimize, test_core.zpq);
    const lambda_tests = b.addTest(.{
        .root_module = b.createModule(.{
            .root_source_file = srcFile(b, "lambda/main.zig"),
            .target = target,
            .optimize = optimize,
            .link_libc = true,
            .imports = &.{
                .{ .name = "zpq", .module = test_core.zpq },
                .{ .name = "build_options", .module = test_core.build_options },
                .{ .name = "service", .module = test_service },
            },
        }),
        .filters = filters,
    });
    const libzpq_tests = b.addTest(.{ .root_module = b.createModule(.{
        .root_source_file = srcFile(b, "libzpq/libzpq.zig"),
        .target = target,
        .optimize = optimize,
        .link_libc = true,
        .imports = &.{
            .{ .name = "zpq", .module = test_core.zpq },
            .{ .name = "service", .module = test_service },
        },
    }), .filters = filters });
    const cli_tests = b.addTest(.{ .root_module = cli_zpq.root });
    const unit_tests = b.allocator.dupe(UnitTest, &.{
        .{ .exe = b.addTest(.{ .root_module = test_core.zpq, .filters = filters }), .bin = "zpq-test", .stage = "units-core" },
        .{ .exe = lambda_tests, .bin = "zpq-lambda-test", .stage = "units-lambda" },
        .{ .exe = cli_tests, .bin = "zpq-cli-test", .stage = "units-cli" },
        .{ .exe = b.addTest(.{ .root_module = test_service, .filters = filters }), .bin = "zpq-service-test", .stage = "units-service" },
        .{ .exe = libzpq_tests, .bin = "zpq-libzpq-test", .stage = "units-libzpq" },
    }) catch @panic("OOM");

    // The core unit tests linked against BoringSSL built from source: the pinned prebuilt archives repeat members,
    // which lld tolerates and the self-hosted linker that incremental builds use does not.
    const dev_core = addCoreModule(b, libzpq_deps, target, optimize, .{ .sql = true });

    // Lambda integration: the built zpq-lambda against an in-process fake runtime API, and the CLI for the S3 paths
    // a Lambda child can't show (in Debug its allocator reports leaks as it exits).
    const integration_opts = b.addOptions();
    integration_opts.addOptionPath("lambda_bin", lambda.getEmittedBin());
    integration_opts.addOptionPath("cli_bin", cli.getEmittedBin());
    const integration_tests = b.addTest(.{
        .root_module = b.createModule(.{
            .root_source_file = b.path("tests/lambda_integration.zig"),
            .target = target,
            .optimize = optimize,
            .imports = &.{.{ .name = "integration_opts", .module = integration_opts.createModule() }},
        }),
        .filters = filters,
    });

    return .{
        .deps = deps,
        .cli = cli,
        .lambda = lambda,
        .libzpq_shared = libs[0],
        .libzpq_static = libs[1],
        .check = check,
        .check_tests = b.addTest(.{ .root_module = check_module }),
        .cli_tests = cli_tests,
        .core_tests = b.addTest(.{ .root_module = dev_core.zpq, .filters = filters }),
        // LLVM-built: the coverage instrumentation the fuzzer reads comes from that backend, not Debug's default.
        .fuzz_tests = b.addTest(.{ .root_module = test_core.zpq, .filters = filters, .use_llvm = true }),
        .unit_tests = unit_tests,
        .integration_tests = integration_tests,
    };
}

/// `zig build check -Dtier=edit|commit|full [-Dshard=NAME]`: the gates. Each stage runs through `zpq-check stage`,
/// one after another, under its timeout, leaving a log and a receipt in zig-out/receipts; the last step's verdict
/// passes only on a clean receipt for this SHA from every stage the tier names. The tiers build their own artifacts
/// whatever -Doptimize says: a ReleaseSafe runner and core tests, a ReleaseFast CLI, and Debug units in full.
fn addTiers(b: *std.Build, cfg: Config) void {
    const Tier = enum { edit, commit, full };
    const tier = b.option(Tier, "tier", "Gate to run with `check`: edit, commit (default) or full") orelse .commit;
    const shard = b.option([]const u8, "shard", "Run one full-tier shard of tools/check/shards.json");
    const home = b.graph.environ_map.get("HOME") orelse "";
    const duckdb = b.option([]const u8, "duckdb", "DuckDB 2.0 CLI (the oracle)") orelse
        b.fmt("{s}/.cache/zpq-duckdb2/bin-2.0.0/duckdb", .{home});
    const python = b.option([]const u8, "python", "Python with PyArrow (readback, contracts)") orelse
        ".venv/bin/python";
    const clickbench = b.option([]const u8, "clickbench", "ClickBench partitions (sub1/, sub16/)") orelse
        b.fmt("{s}/bench/zpq-fair/data/clickbench", .{home});

    const safe = artifacts(b, cfg, .ReleaseSafe);
    const fast = artifacts(b, cfg, .ReleaseFast);
    const runner = safe.check;
    const dir = "zig-out/receipts";
    const tier_name = @tagName(tier);

    const Stage = struct { name: []const u8, run: *std.Build.Step.Run };
    var stages: std.ArrayList(Stage) = .empty;
    const wanted: ?[]const []const u8 = if (shard) |name| shardStages(b, name) else null;
    const S = struct {
        fn add(bb: *std.Build, list: *std.ArrayList(Stage), want: ?[]const []const u8, exe: *std.Build.Step.Compile, d: []const u8, t: []const u8, name: []const u8, kind: []const u8, timeout: u32) ?*std.Build.Step.Run {
            if (want) |names| {
                for (names) |n| {
                    if (std.mem.eql(u8, n, name)) break;
                } else return null;
            }
            const r = bb.addRunArtifact(exe);
            r.addArgs(&.{ "stage", "--name", name, "--dir", d, "--kind", kind, "--tier", t, "--timeout", bb.fmt("{d}", .{timeout}) });
            r.has_side_effects = true;
            if (list.items.len != 0) r.step.dependOn(&list.items[list.items.len - 1].run.step);
            list.append(bb.allocator, .{ .name = name, .run = r }) catch @panic("OOM");
            return r;
        }
    };
    const add = S.add;

    // The oracle's inputs and the generated fixtures come first: a missing fixture is a skipped test, never allowed.
    if (add(b, &stages, wanted, runner, dir, tier_name, "fixtures", "plain", 1800)) |r| {
        r.addArgs(&.{ "--", "tools/check/fixtures.sh" });
        r.setEnvironmentVariable("PYTHON", python);
    }
    const commit_cases = caseFiles(b, false);
    if (tier != .full) if (add(b, &stages, wanted, runner, dir, tier_name, "test-core", "units", 2400)) |r| {
        r.addArg("--");
        r.addArtifactArg(safe.core_tests);
    };
    if (tier == .full) {
        const debug = artifacts(b, cfg, .Debug);
        for (debug.unit_tests) |t| if (add(b, &stages, wanted, runner, dir, tier_name, t.stage, "units", 3600)) |r| {
            r.addArg("--");
            r.addArtifactArg(t.exe);
        };
        if (add(b, &stages, wanted, runner, dir, tier_name, "integration", "units", 1800)) |r| {
            r.addArg("--");
            r.addArtifactArg(debug.integration_tests);
        }
        // The fan-out coordinator against the same zpq-lambda behind a fake runtime, checked against DuckDB.
        if (add(b, &stages, wanted, runner, dir, tier_name, "fanout-self", "plain", 300)) |r| {
            r.addArgs(&.{ "--", python, "tools/serverless/test_fanout.py", "--duckdb", duckdb, "--lambda" });
            r.addArtifactArg(debug.lambda);
        }
    }
    if (tier != .edit) if (add(b, &stages, wanted, runner, dir, tier_name, "check-self", "plain", 900)) |r| {
        r.addArg("--");
        r.addArtifactArg(runner);
        r.addArgs(&.{ "--self-test", "--no-cache", "--duckdb", duckdb, "--python", python, "--work", "zig-out/check-self", "--zpq" });
        r.addArtifactArg(fast.cli);
    };
    // The scorecard's rules decide what a timing counts for (stalls, quarantine, the denominator): a gate too.
    if (tier != .edit) if (add(b, &stages, wanted, runner, dir, tier_name, "bench-self", "plain", 300)) |r| {
        r.addArgs(&.{ "--", python, "-m", "unittest", "discover", "-s", "bench/tests" });
    };
    const Cases = struct { name: []const u8, files: []const []const u8, fresh: bool, timeout: u32, data: ?[]const u8 = null };
    const case_stages: []const Cases = switch (tier) {
        .edit => &.{},
        .commit => &.{.{ .name = "cases", .files = commit_cases, .fresh = false, .timeout = 3600 }},
        .full => &.{
            .{ .name = "cases-fresh", .files = commit_cases, .fresh = true, .timeout = 3600 },
            .{ .name = "corpus", .files = &.{"tools/check/cases/corpus.json"}, .fresh = true, .timeout = 1800 },
            .{ .name = "canonical", .files = &.{ "tools/check/cases/decode_errors.json", "tools/check/cases/hardware.json" }, .fresh = true, .timeout = 1800 },
            .{ .name = "clickbench", .files = &.{"tools/check/cases/clickbench.json"}, .fresh = true, .timeout = 7200, .data = clickbench },
        },
    };
    for (case_stages) |c| if (add(b, &stages, wanted, runner, dir, tier_name, c.name, "cases", c.timeout)) |r| {
        r.addArg("--");
        r.addArtifactArg(runner);
        for (c.files) |f| r.addArgs(&.{ "--cases", f });
        r.addArgs(&.{ "--duckdb", duckdb, "--python", python, "--work", b.fmt("zig-out/check-work-{s}", .{c.name}) });
        if (c.fresh) r.addArg("--no-cache");
        if (c.data) |d| r.addArgs(&.{ "--data", d });
        r.addArg("--zpq");
        r.addArtifactArg(fast.cli);
    };
    if (tier == .full) {
        if (add(b, &stages, wanted, runner, dir, tier_name, "path-parity", "plain", 1800)) |r| {
            r.addArgs(&.{ "--", python, "tools/check/path_parity.py", "--duckdb", duckdb, "--out", "zig-out/path-parity", "--check" });
            r.addArtifactArg(runner);
        }
        // Hardware choices must preserve output bytes: the same cases, answers only, through native-CPU and
        // baseline-CPU builds, must hash alike case for case.
        if (add(b, &stages, wanted, runner, dir, tier_name, "cpu-parity", "plain", 1800)) |r| {
            var base_cfg = cfg;
            base_cfg.target = b.resolveTargetQuery(.{ .cpu_model = .baseline });
            const base_check = artifacts(b, base_cfg, .ReleaseSafe).check;
            const base_cli = artifacts(b, base_cfg, .ReleaseFast).cli;
            var script: std.ArrayList(u8) = .empty;
            script.appendSlice(b.allocator, "set -e; c=\"") catch @panic("OOM");
            for (commit_cases) |f| script.print(b.allocator, " --cases {s}", .{f}) catch @panic("OOM");
            script.appendSlice(b.allocator, "\"; w=zig-out/check-work-cpu; " ++
                "\"$1\" $c --zpq \"$2\" --work $w --python \"$5\" --answers-only --manifest $w-native.txt >/dev/null; " ++
                "\"$3\" $c --zpq \"$4\" --work $w --answers-only --reuse-fixtures --manifest $w-baseline.txt " ++
                ">/dev/null; \"$1\" manifest-diff $w-native.txt $w-baseline.txt") catch @panic("OOM");
            r.addArgs(&.{ "--", "sh", "-c", script.items, "cpu-parity" });
            r.addArtifactArg(runner);
            r.addArtifactArg(fast.cli);
            r.addArtifactArg(base_check);
            r.addArtifactArg(base_cli);
            r.addArg(python);
        }
        if (add(b, &stages, wanted, runner, dir, tier_name, "contracts", "contracts", 1200)) |r| {
            r.addArgs(&.{ "--", python, "tools/check/contracts.py" });
            r.addArtifactArg(fast.cli);
            r.addArg("--lib");
            r.addArtifactArg(fast.libzpq_shared);
        }
        // The C ABI plain and under ASan, the Python binding and the CLI/libzpq/Lambda byte parity; it builds its own.
        if (add(b, &stages, wanted, runner, dir, tier_name, "libzpq", "plain", 3600)) |r| {
            r.addArgs(&.{ "--", "bash", "tests/libzpq/run.sh" });
            r.setEnvironmentVariable("ZIG", b.graph.zig_exe);
            r.setEnvironmentVariable("OPTIMIZE", "ReleaseFast");
            r.setEnvironmentVariable("PYTHON", python);
        }
    }

    const verdict = b.addRunArtifact(runner);
    verdict.has_side_effects = true;
    var names: std.ArrayList(u8) = .empty;
    for (stages.items, 0..) |st, i| {
        if (i != 0) names.append(b.allocator, ',') catch @panic("OOM");
        names.appendSlice(b.allocator, st.name) catch @panic("OOM");
    }
    verdict.addArgs(&.{ "verdict", "--dir", dir, "--expect", names.items });
    if (stages.items.len != 0) verdict.step.dependOn(&stages.items[stages.items.len - 1].run.step);
    b.step("check", "Run a gate: -Dtier=edit|commit|full [-Dshard=NAME]; receipts in zig-out/receipts").dependOn(&verdict.step);
}

/// The case files a tier runs: every file under tools/check/cases, less the full tier's own sweeps.
fn caseFiles(b: *std.Build, full_only: bool) []const []const u8 {
    const full = [_][]const u8{ "corpus.json", "decode_errors.json", "hardware.json", "clickbench.json" };
    var dir = b.root.openDir(b.graph.io, "tools/check/cases", .{ .iterate = true }) catch @panic("tools/check/cases");
    defer dir.close(b.graph.io);
    var out: std.ArrayList([]const u8) = .empty;
    var it = dir.iterate();
    while (it.next(b.graph.io) catch @panic("tools/check/cases")) |e| {
        if (e.kind != .file or !std.mem.endsWith(u8, e.name, ".json")) continue;
        const is_full = for (full) |f| {
            if (std.mem.eql(u8, f, e.name)) break true;
        } else false;
        if (is_full == full_only) out.append(b.allocator, b.fmt("tools/check/cases/{s}", .{e.name})) catch @panic("OOM");
    }
    std.mem.sort([]const u8, out.items, {}, struct {
        fn lt(_: void, x: []const u8, y: []const u8) bool {
            return std.mem.order(u8, x, y) == .lt;
        }
    }.lt);
    return out.items;
}

/// The stages a full-tier shard runs, from tools/check/shards.json ({"shards": {NAME: {"stages": [...], ...}}}).
fn shardStages(b: *std.Build, name: []const u8) []const []const u8 {
    const text = b.root.root_dir.handle.readFileAlloc(b.graph.io, "tools/check/shards.json", b.allocator, .limited(1 << 20)) catch
        @panic("tools/check/shards.json");
    const Shard = struct { stages: []const []const u8 };
    const parsed = std.json.parseFromSliceLeaky(struct { shards: std.json.ArrayHashMap(Shard) }, b.allocator, text, .{ .ignore_unknown_fields = true }) catch
        @panic("tools/check/shards.json");
    const sh = parsed.shards.map.get(name) orelse std.debug.panic("-Dshard={s}: not in tools/check/shards.json", .{name});
    // Every shard makes its fixtures first.
    return std.mem.concat(b.allocator, []const u8, &.{ &.{"fixtures"}, sh.stages }) catch @panic("OOM");
}

/// Where every module's source lives: src/, or its copy in the build cache when src/ is read-only.
var staged_src: ?std.Build.LazyPath = null;

fn srcFile(b: *std.Build, rel: []const u8) std.Build.LazyPath {
    return if (staged_src) |dir| dir.path(b, rel) else b.path(b.fmt("src/{s}", .{rel}));
}

/// Each test root imports the `tests.gen.zig` beside it, written here from every file under its directory that
/// declares a test (less the directories other roots own). Zig finds a file's tests only through an import, so a
/// hand-kept list let new test files go unrun; this list cannot fall behind. Zig loads every imported file, test
/// blocks or not, so any build needs the lists. A writable tree gets them beside the roots (diagnostics keep pointing
/// into src/); a read-only one (a release built from a packaged tree) gets src/ staged in the build cache with the
/// lists added, and every module roots there.
fn generateTestRoots(b: *std.Build) void {
    b.graph.poisonCache();
    const io = b.graph.io;
    const Root = struct { dir: []const u8, file: []const u8 };
    const roots = [_]Root{
        .{ .dir = "", .file = "zpq.zig" },
        .{ .dir = "cli/", .file = "main.zig" },
        .{ .dir = "lambda/", .file = "main.zig" },
        .{ .dir = "service/", .file = "handler.zig" },
        .{ .dir = "libzpq/", .file = "libzpq.zig" },
    };
    var src = b.root.openDir(io, "src", .{ .iterate = true }) catch @panic("src");
    defer src.close(io);
    var lists: [roots.len]std.ArrayList([]const u8) = @splat(.empty);
    var walker = src.walk(b.allocator) catch @panic("OOM");
    defer walker.deinit();
    while (walker.next(io) catch @panic("src")) |e| {
        if (e.kind != .file or !std.mem.endsWith(u8, e.path, ".zig") or std.mem.endsWith(u8, e.path, "tests.gen.zig")) continue;
        const text = src.readFileAlloc(io, e.path, b.allocator, .limited(64 << 20)) catch @panic("src");
        if (!declaresTest(text)) continue;
        var owner: usize = 0;
        for (roots[1..], 1..) |r, i| if (std.mem.startsWith(u8, e.path, r.dir)) {
            owner = i;
        };
        const rel = e.path[roots[owner].dir.len..];
        if (std.mem.eql(u8, rel, roots[owner].file)) continue;
        lists[owner].append(b.allocator, b.dupe(rel)) catch @panic("OOM");
    }
    var texts: [roots.len][]const u8 = undefined;
    for (&lists, 0..) |*list, i| {
        std.mem.sort([]const u8, list.items, {}, struct {
            fn lt(_: void, x: []const u8, y: []const u8) bool {
                return std.mem.order(u8, x, y) == .lt;
            }
        }.lt);
        var text: std.ArrayList(u8) = .empty;
        text.appendSlice(b.allocator, "//! Generated by build.zig (generateTestRoots): the files under this root that declare tests.\n\ntest {\n") catch @panic("OOM");
        for (list.items) |f| text.print(b.allocator, "    _ = @import(\"{s}\");\n", .{f}) catch @panic("OOM");
        text.appendSlice(b.allocator, "}\n") catch @panic("OOM");
        texts[i] = text.items;
    }
    for (roots, texts) |r, text| {
        const path = b.fmt("{s}tests.gen.zig", .{r.dir});
        const old = src.readFileAlloc(io, path, b.allocator, .limited(1 << 20)) catch "";
        if (std.mem.eql(u8, old, text)) continue;
        src.writeFile(io, .{ .sub_path = path, .data = text }) catch break;
    } else return;
    const stage = b.addWriteFiles();
    const dir = stage.addCopyDirectory(b.path("src"), "src", .{ .exclude_extensions = &.{"tests.gen.zig"} });
    for (roots, texts) |r, text| _ = stage.add(b.fmt("src/{s}tests.gen.zig", .{r.dir}), text);
    staged_src = dir;
}

fn declaresTest(text: []const u8) bool {
    var lines = std.mem.splitScalar(u8, text, '\n');
    while (lines.next()) |line| {
        if (std.mem.startsWith(u8, line, "test ") or std.mem.startsWith(u8, line, "test{")) return true;
    }
    return false;
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
    /// mimalloc, the process allocator of a musl release build (`mimallocLibrary`); null elsewhere.
    mimalloc: ?*std.Build.Step.Compile,
    /// `build_options.enable_profile` for every module instance.
    profile: bool = true,
    /// `build_options.version`: build.zig.zon's, plus `-Dprerelease`'s suffix.
    version: []const u8 = zon.version,

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
            .mimalloc = if (!shared and usesMimalloc(target, optimize)) mimallocLibrary(b, target, optimize) else null,
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
    opts.addOption([]const u8, "version", deps.version);
    opts.addOption(bool, "use_mimalloc", deps.mimalloc != null);
    const opts_mod = opts.createModule();

    // Frame pointers on: makes `perf record --call-graph=fp` produce clean
    // stacks instead of DWARF unwinding gaps. Cost is 0-2% on typical
    // workloads (validated 2026-05-07 against the regression suite —
    // medians within noise). Worth it for always-available flamegraphs.
    // Flip to `true` (or remove) if a hot path ever shows real register-
    // pressure regression.
    const module_options: std.Build.Module.CreateOptions = .{
        .root_source_file = srcFile(b, "zpq.zig"),
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
    if (deps.mimalloc) |lib| zpq_mod.linkLibrary(lib);
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
    strip: bool,
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
        .root_source_file = srcFile(b, if (is_lambda) "lambda/main.zig" else "cli/main.zig"),
        .target = target,
        .optimize = optimize,
        .strip = strip,
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
        .root_source_file = srcFile(b, "service/handler.zig"),
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
/// A release build over musl takes mimalloc as its process allocator (`core.memory.processAllocator`): musl's malloc
/// and Zig's `smp_allocator` both map and unmap pages as allocation-heavy folds grow, and every `munmap` stalls the
/// other threads. glibc, macOS, Debug/ReleaseSafe and libzpq keep their heaps; the C codecs keep musl's malloc, since
/// mimalloc is built without `MI_MALLOC_OVERRIDE`.
fn usesMimalloc(target: std.Build.ResolvedTarget, optimize: std.builtin.Optimize) bool {
    return target.result.os.tag == .linux and target.result.abi.isMusl() and
        (optimize == .fast or optimize == .small);
}

/// mimalloc (vendor/mimalloc, microsoft/mimalloc, vendor/README.md) as upstream builds it: one translation unit.
fn mimallocLibrary(
    b: *std.Build,
    target: std.Build.ResolvedTarget,
    optimize: std.builtin.Optimize,
) *std.Build.Step.Compile {
    const lib = b.addLibrary(.{
        .linkage = .static,
        .name = "mimalloc",
        .root_module = b.createModule(.{ .target = target, .optimize = optimize, .link_libc = true }),
    });
    lib.root_module.addCSourceFiles(.{ .root = b.path("vendor/mimalloc/src"), .files = &.{"static.c"}, .flags = &.{
        "-DNDEBUG",
        "-DMI_STATIC_LIB",
        "-fno-builtin-malloc",
        "-ftls-model=initial-exec",
    } });
    lib.root_module.addIncludePath(b.path("vendor/mimalloc/include"));
    return lib;
}

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

fn validPrerelease(pre: []const u8) bool {
    var ids = std.mem.splitScalar(u8, pre, '.');
    while (ids.next()) |id| {
        if (id.len == 0) return false;
        for (id) |c| if (!std.ascii.isAlphanumeric(c) and c != '-') return false;
    }
    return true;
}
