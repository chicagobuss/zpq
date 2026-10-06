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

    // BoringSSL source dep — we only need the public C headers.
    const boringssl_dep = b.dependency("boringssl", .{});

    const target_info = target.result;
    const triple = try std.fmt.allocPrint(
        b.allocator,
        "{s}-{s}",
        .{ @tagName(target_info.cpu.arch), @tagName(target_info.os.tag) },
    );
    const libs = [_][]const u8{ "libcrypto.a", "libssl.a" };

    const have_locally = probeLocalPrebuilts(b, triple, &libs);
    var lib_paths: [libs.len]std.Build.LazyPath = undefined;
    if (have_locally) {
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

    for (lib_paths) |path| boring_tls_mod.addObjectFile(path);
    boring_tls_mod.linkSystemLibrary("c++", .{});
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
