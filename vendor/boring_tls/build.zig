const std = @import("std");

/// boring_tls build — prebuilt-only.
///
/// We never source-build BoringSSL from this build.zig; it links prebuilt
/// `libcrypto.a` + `libssl.a`, pinned by URL and sha256 in
/// `prebuilt.sha256`. Each archive comes from one of two places:
///
///   - `prebuilt/<arch>-<os>/`, if both files are there: a local override
///     for offline builds or a locally built BoringSSL, used as is.
///     `tools/r2-fetch-artifacts.sh` fills it with verified downloads.
///   - Otherwise a build step downloads the pinned URL, checks the digest,
///     and keeps the result in the Zig cache, so it runs once per cache.
///
/// The previous incarnation also had source-build paths (~500 lines).
/// They were ripped out when migrating to Zig 0.16.0 — the project has
/// always shipped prebuilt artifacts in practice, and the dead path was
/// dragging multiple 0.16.0 incompatibilities.
const manifest = @embedFile("prebuilt.sha256");

pub fn build(b: *std.Build) !void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});

    const fetch_prebuilt = b.option(
        bool,
        "fetch-prebuilt",
        "Fetch the pinned prebuilt libraries when prebuilt/<arch>-<os>/ lacks them",
    ) orelse true;

    // Compile BoringSSL from the source dep instead, position-independent, for a shared library (libzpq): the
    // prebuilt archives are not, and cannot go into one. Slower to build than fetching; nothing else needs it.
    const source_build = b.option(
        bool,
        "source-build",
        "Compile BoringSSL from source, position-independent (for shared libraries), instead of linking prebuilts",
    ) orelse false;

    // BoringSSL source dep — the public C headers, and the sources when `source_build`.
    const boringssl_dep = b.dependency("boringssl", .{});

    const target_info = target.result;
    const triple = try std.fmt.allocPrint(
        b.allocator,
        "{s}-{s}",
        .{ @tagName(target_info.cpu.arch), @tagName(target_info.os.tag) },
    );
    const libs = [_][]const u8{ "libcrypto.a", "libssl.a" };

    const have_locally = !source_build and probeLocalPrebuilts(b, triple, &libs);
    var lib_paths: [libs.len]std.Build.LazyPath = undefined;
    if (source_build) {} else if (have_locally) {
        for (libs, &lib_paths) |lib, *path| path.* = b.path(b.fmt("prebuilt/{s}/{s}", .{ triple, lib }));
    } else {
        if (!fetch_prebuilt) {
            std.log.err(
                "boring_tls: prebuilt/{s}/ lacks libcrypto.a and libssl.a, and -Dfetch-prebuilt=false; " ++
                    "run tools/r2-fetch-artifacts.sh {s} to fill it",
                .{ triple, triple },
            );
            return error.PrebuiltNotFound;
        }
        const fetcher = b.addExecutable(.{
            .name = "fetch_prebuilt",
            .root_module = b.createModule(.{
                .root_source_file = b.path("fetch_prebuilt.zig"),
                .target = b.graph.host,
                // Fixed, so the fetch step's cache key doesn't follow
                // -Doptimize. Debug compiles in seconds and still hashes
                // the archives faster than the network delivers them.
                .optimize = .Debug,
            }),
        });
        for (libs, &lib_paths) |lib, *path| {
            const pin = pinned(b.fmt("{s}/{s}", .{ triple, lib })) orelse {
                std.log.err(
                    "boring_tls: no prebuilt {s} is pinned for {s}; supported targets are listed in vendor/boring_tls/prebuilt.sha256",
                    .{ lib, triple },
                );
                return error.PrebuiltNotFound;
            };
            const fetch = b.addRunArtifact(fetcher);
            fetch.setName(b.fmt("fetch {s}/{s}", .{ triple, lib }));
            fetch.addArgs(&.{ pin.url, pin.sha256 });
            path.* = fetch.addOutputFileArg(lib);
        }
    }

    const boring_tls_mod = b.addModule("boring_tls", .{
        .root_source_file = b.path("src/root.zig"),
        .target = target,
        .optimize = optimize,
    });

    boring_tls_mod.addIncludePath(boringssl_dep.path("include"));
    boring_tls_mod.addCMacro("OPENSSL_64_BIT", "1");

    if (target.result.cpu.arch == .x86_64) {
        boring_tls_mod.addCMacro("__x86_64", "1");
    } else if (target.result.cpu.arch == .aarch64) {
        boring_tls_mod.addCMacro("__AARCH64EL__", "1");
    }

    const openssl_h = b.addWriteFiles().add("openssl_c.h",
        \\#include <openssl/ssl.h>
        \\#include <openssl/err.h>
        \\#include <openssl/bio.h>
        \\#include <openssl/x509v3.h>
        \\
    );
    const openssl_c = b.addTranslateC(.{
        .root_source_file = openssl_h,
        .target = target,
        .optimize = optimize,
    });
    openssl_c.defineCMacro("_FORTIFY_SOURCE", "0");
    openssl_c.defineCMacro("OPENSSL_64_BIT", "1");
    if (target.result.cpu.arch == .x86_64) {
        openssl_c.defineCMacro("__x86_64", "1");
    } else if (target.result.cpu.arch == .aarch64) {
        openssl_c.defineCMacro("__AARCH64EL__", "1");
    }
    openssl_c.addIncludePath(boringssl_dep.path("include"));
    boring_tls_mod.addImport("openssl_c", openssl_c.createModule());

    if (source_build) {
        for (try sourceLibraries(b, boringssl_dep, target, optimize)) |lib| boring_tls_mod.linkLibrary(lib);
    } else {
        for (lib_paths) |path| boring_tls_mod.addObjectFile(path);
    }
    boring_tls_mod.linkSystemLibrary("c++", .{});
}

/// libcrypto and libssl compiled from `dep`'s sources as BoringSSL's own CMake build lists them (gen/sources.json),
/// position-independent. Every generated assembly file is compiled: each guards itself by architecture and object
/// format, so those for other targets assemble to nothing.
fn sourceLibraries(
    b: *std.Build,
    dep: *std.Build.Dependency,
    target: std.Build.ResolvedTarget,
    optimize: std.builtin.OptimizeMode,
) ![2]*std.Build.Step.Compile {
    const json_path = try dep.builder.root.joinString(b.allocator, "gen/sources.json");
    const json_text = try std.Io.Dir.cwd().readFileAlloc(b.graph.io, json_path, b.allocator, .limited(4 << 20));
    const parsed = try std.json.parseFromSlice(std.json.Value, b.allocator, json_text, .{});
    const root = parsed.value.object;
    const lists = struct {
        fn of(gpa: std.mem.Allocator, v: std.json.Value, key: []const u8) ![]const []const u8 {
            const arr = (v.object.get(key) orelse return &.{}).array.items;
            const out = try gpa.alloc([]const u8, arr.len);
            for (arr, out) |e, *o| o.* = e.string;
            return out;
        }
    };
    // _XOPEN_SOURCE alone hides the BSD types (u_int) Darwin's own headers use; _DARWIN_C_SOURCE brings them back.
    const os_flags: []const []const u8 = if (target.result.os.tag.isDarwin()) &.{"-D_DARWIN_C_SOURCE"} else &.{};
    // The source build is for a shared library, which should export its own API alone: OPENSSL_EXPORT is empty
    // without BORINGSSL_SHARED_LIBRARY, so this hides all of BoringSSL's.
    const flags_c = try std.mem.concat(b.allocator, []const u8, &.{
        &.{ "-std=c11", "-DBORINGSSL_IMPLEMENTATION", "-D_XOPEN_SOURCE=700", "-fvisibility=hidden" },
        os_flags,
    });
    const flags_cc = try std.mem.concat(b.allocator, []const u8, &.{
        &.{ "-std=c++17", "-fno-exceptions", "-fno-rtti", "-DBORINGSSL_IMPLEMENTATION", "-D_XOPEN_SOURCE=700" },
        &.{"-fvisibility=hidden"},
        os_flags,
    });
    var out: [2]*std.Build.Step.Compile = undefined;
    for ([_][]const u8{ "crypto", "ssl" }, &out) |name, *lib| {
        const mod = b.createModule(.{
            .target = target,
            .optimize = optimize,
            .pic = true,
            .link_libc = true,
            .link_libcpp = true,
        });
        mod.addIncludePath(dep.path("include"));
        mod.addIncludePath(dep.path(""));
        const parts: []const []const u8 = if (std.mem.eql(u8, name, "crypto")) &.{ "bcm", "crypto" } else &.{"ssl"};
        for (parts) |part| {
            const v = root.get(part) orelse return error.BadSourcesJson;
            const srcs = try lists.of(b.allocator, v, "srcs");
            const asms = try lists.of(b.allocator, v, "asm");
            mod.addCSourceFiles(.{ .root = dep.path(""), .files = srcs, .flags = flags_cc });
            mod.addCSourceFiles(.{ .root = dep.path(""), .files = asms, .flags = flags_c });
        }
        lib.* = b.addLibrary(.{ .name = name, .linkage = .static, .root_module = mod });
    }
    out[1].root_module.linkLibrary(out[0]);
    return out;
}

/// Whether `prebuilt/<triple>/` holds every lib. The probe runs at configure
/// time, so it is declared to the configure cache. Zig cannot track a path
/// that doesn't exist, so this tracks the entry list of the deepest directory
/// on the way that does: whatever appears next changes that list.
fn probeLocalPrebuilts(b: *std.Build, triple: []const u8, libs: []const []const u8) bool {
    const dir = b.fmt("prebuilt/{s}", .{triple});
    for ([_][]const u8{ dir, "prebuilt", "." }) |rel| {
        if (b.root.access(b.graph.io, rel, .{})) |_| {
            b.dependOnDirectoryContents(b.path(rel));
            break;
        } else |_| {}
    }
    for (libs) |lib| {
        b.root.access(b.graph.io, b.fmt("{s}/{s}", .{ dir, lib }), .{}) catch return false;
    }
    return true;
}

const Pin = struct { url: []const u8, sha256: []const u8 };

/// Looks `<target>/<file>` up in prebuilt.sha256 (`<target>/<file> <sha256> <url>`
/// per line, `#` comments).
fn pinned(key: []const u8) ?Pin {
    var lines = std.mem.tokenizeScalar(u8, manifest, '\n');
    while (lines.next()) |line| {
        if (line[0] == '#') continue;
        var fields = std.mem.tokenizeAny(u8, line, " \t\r");
        if (!std.mem.eql(u8, fields.next() orelse continue, key)) continue;
        const sha256 = fields.next() orelse return null;
        const url = fields.next() orelse return null;
        return .{ .url = url, .sha256 = sha256 };
    }
    return null;
}
