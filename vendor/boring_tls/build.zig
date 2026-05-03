const std = @import("std");

/// boring_tls build — prebuilt-only.
///
/// We never source-build BoringSSL from this build.zig. The toolchain
/// requires prebuilt `libcrypto.a` + `libssl.a` to be present under
/// `prebuilt/<triple>/`, which `tools/r2-fetch-artifacts.sh` populates
/// from a public R2 bucket. If they're missing locally we attempt the
/// same fetch via curl and fail fast otherwise.
///
/// The previous incarnation also had source-build paths (~500 lines).
/// They were ripped out when migrating to Zig 0.16.0 — the project has
/// always shipped prebuilt artifacts in practice, and the dead path was
/// dragging multiple 0.16.0 incompatibilities.
const R2_PUBLIC_URL = "https://pub-4d2e7e2925bb43dc9d3c0323d6d61a84.r2.dev";

pub fn build(b: *std.Build) !void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});

    const fetch_prebuilt = b.option(
        bool,
        "fetch-prebuilt",
        "Fetch prebuilt libraries from R2 if not found locally",
    ) orelse true;

    // BoringSSL source dep — we only need the public C headers.
    const boringssl_dep = b.dependency("boringssl", .{});

    const target_info = target.result;
    const triple = try std.fmt.allocPrint(
        b.allocator,
        "{s}-{s}",
        .{ @tagName(target_info.cpu.arch), @tagName(target_info.os.tag) },
    );
    const prebuilt_path = b.path(b.fmt("prebuilt/{s}", .{triple}));

    const crypto_abs = b.path(b.fmt("prebuilt/{s}/libcrypto.a", .{triple})).getPath(b);
    const ssl_abs = b.path(b.fmt("prebuilt/{s}/libssl.a", .{triple})).getPath(b);

    const have_locally = (std.Io.Dir.accessAbsolute(b.graph.io, crypto_abs, .{}) catch null) != null and
        (std.Io.Dir.accessAbsolute(b.graph.io, ssl_abs, .{}) catch null) != null;

    if (!have_locally) {
        if (!fetch_prebuilt) {
            std.log.err(
                "boring_tls: prebuilt artifacts missing for {s} and -Dfetch-prebuilt=false",
                .{triple},
            );
            return error.PrebuiltNotFound;
        }
        std.log.info("boring_tls: fetching prebuilt artifacts for {s}...", .{triple});
        try fetchFromR2(b, triple);
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

    boring_tls_mod.addObjectFile(prebuilt_path.path(b, "libcrypto.a"));
    boring_tls_mod.addObjectFile(prebuilt_path.path(b, "libssl.a"));
    boring_tls_mod.linkSystemLibrary("c++", .{});
}

fn fetchFromR2(b: *std.Build, triple: []const u8) !void {
    const prebuilt_abs = b.path(b.fmt("prebuilt/{s}", .{triple})).getPath(b);

    std.Io.Dir.createDirAbsolute(b.graph.io, prebuilt_abs, @enumFromInt(0o755)) catch |err| {
        if (err != error.PathAlreadyExists) {
            std.log.err("Failed to create {s}: {}", .{ prebuilt_abs, err });
            return err;
        }
    };

    const files = [_][]const u8{ "libcrypto.a", "libssl.a" };
    for (files) |filename| {
        const url = b.fmt("{s}/boring_tls/{s}/{s}", .{ R2_PUBLIC_URL, triple, filename });
        const full_dest = b.fmt("{s}/{s}", .{ prebuilt_abs, filename });

        const result = std.process.run(b.allocator, b.graph.io, .{
            .argv = &[_][]const u8{ "curl", "-fSL", "--create-dirs", "-o", full_dest, url },
        }) catch |err| {
            std.log.err("curl spawn failed: {}", .{err});
            return err;
        };
        b.allocator.free(result.stdout);
        b.allocator.free(result.stderr);

        switch (result.term) {
            .exited => |code| if (code != 0) {
                std.log.err("curl exited with code {} fetching {s}", .{ code, filename });
                return error.FetchFailed;
            },
            else => {
                std.log.err("curl terminated abnormally: {any}", .{result.term});
                return error.FetchFailed;
            },
        }
    }
}
