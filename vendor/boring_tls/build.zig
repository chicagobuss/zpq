const std = @import("std");

/// Public R2 URL for pre-built BoringSSL artifacts
const R2_PUBLIC_URL = "https://pub-4d2e7e2925bb43dc9d3c0323d6d61a84.r2.dev";

pub fn build(b: *std.Build) !void {
    const target = b.standardTargetOptions(.{});

    const optimize = b.standardOptimizeOption(.{});

    const boringssl_dep = b.dependency("boringssl", .{});

    const use_prebuilt = b.option(bool, "use-prebuilt", "Use pre-built static libraries if available") orelse true;
    const fetch_prebuilt = b.option(bool, "fetch-prebuilt", "Fetch pre-built libraries from R2 if not found locally") orelse true;
    const target_info = target.result;
    const triple = try std.fmt.allocPrint(b.allocator, "{s}-{s}", .{ @tagName(target_info.cpu.arch), @tagName(target_info.os.tag) });
    const prebuilt_path = b.path(b.fmt("prebuilt/{s}", .{triple}));

    var crypto: *std.Build.Step.Compile = undefined;
    var ssl: *std.Build.Step.Compile = undefined;

    var found_prebuilt = false;
    if (use_prebuilt) {
        const crypto_path = b.fmt("prebuilt/{s}/libcrypto.a", .{triple});
        const ssl_path = b.fmt("prebuilt/{s}/libssl.a", .{triple});

        // Check if files exist via std.fs (relative to build.zig)
        const build_root = b.build_root.handle;
        if (build_root.access(crypto_path, .{}) catch null != null and
            build_root.access(ssl_path, .{}) catch null != null)
        {
            found_prebuilt = true;
        } else if (fetch_prebuilt) {
            // Try to fetch from R2
            std.log.info("Pre-built artifacts not found locally, fetching from R2 for {s}...", .{triple});
            if (fetchFromR2(b, triple)) {
                found_prebuilt = true;
                std.log.info("Successfully fetched pre-built artifacts from R2", .{});
            } else |err| {
                std.log.warn("Failed to fetch from R2: {}, will build from source", .{err});
            }
        }
    }

    if (!found_prebuilt) {
        // We only walk the source tree and gather files if we actually need to build BoringSSL.
        // This saves significant time on every 'zig build' invocation if pre-built libs are found.
        var arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
        defer arena.deinit();
        var crypto_sources = std.ArrayListUnmanaged([]const u8){};
        crypto_sources.ensureTotalCapacity(arena.allocator(), 200) catch @panic("OOM");

        const full_path = boringssl_dep.path("crypto/aes").getPath(b);
        try glob_sources(arena.allocator(), full_path, ".cc", &crypto_sources);

        crypto = try buildBoringCrypto(
            b,
            target,
            optimize,
            boringssl_dep,
        );
        ssl = buildBoringSSLSSL(
            b,
            target,
            optimize,
            boringssl_dep,
            crypto,
        );
    }

    const boring_tls_mod = b.addModule("boring_tls", .{
        .root_source_file = b.path("src/root.zig"),
        .target = target,
        .optimize = optimize,
    });

    boring_tls_mod.addIncludePath(boringssl_dep.path("include"));
    boring_tls_mod.addCMacro("OPENSSL_64_BIT", "1");
    // NOTE: OPENSSL_NO_ASM was removed to enable hardware-accelerated crypto (AES-NI, ARM NEON)
    // This provides ~100x speedup for TLS operations

    if (target.result.cpu.arch == .x86_64) {
        // BoringSSL's target.h checks for __x86_64 (no trailing underscore) to define OPENSSL_X86_64
        boring_tls_mod.addCMacro("__x86_64", "1");
    } else if (target.result.cpu.arch == .aarch64) {
        // BoringSSL expects __AARCH64EL__ for little-endian ARM64 detection
        boring_tls_mod.addCMacro("__AARCH64EL__", "1");
    }

    if (found_prebuilt) {
        // Link directly against the prebuilt .a files
        // We use addObjectFile which handles .a files correctly without nesting them
        boring_tls_mod.addObjectFile(prebuilt_path.path(b, "libcrypto.a"));
        boring_tls_mod.addObjectFile(prebuilt_path.path(b, "libssl.a"));
        boring_tls_mod.linkSystemLibrary("c++", .{});
    } else {
        boring_tls_mod.linkLibrary(crypto);
        boring_tls_mod.linkLibrary(ssl);
    }
}

fn buildBoringCrypto(
    b: *std.Build,
    target: std.Build.ResolvedTarget,
    optimize: std.builtin.OptimizeMode,
    boringssl_dep: *std.Build.Dependency,
) !*std.Build.Step.Compile {
    const crypto_mod = b.createModule(.{
        .target = target,
        .optimize = optimize,
    });
    const crypto = b.addLibrary(.{
        .linkage = .static,
        .name = "crypto",
        .root_module = crypto_mod,
    });

    crypto.root_module.linkSystemLibrary("c++", .{});
    crypto.root_module.addIncludePath(boringssl_dep.path("include"));
    crypto.root_module.addIncludePath(boringssl_dep.path("src/include"));

    var arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
    defer arena.deinit();
    var crypto_sources = std.ArrayListUnmanaged([]const u8){};
    crypto_sources.ensureTotalCapacity(arena.allocator(), 10) catch @panic("OOM");

    const crypto_dirs = [_][]const u8{
        "crypto",
        "crypto/asn1",
        "crypto/base64",
        "crypto/bio",
        "crypto/blake2",
        "crypto/bn_extra",
        "crypto/buf",
        "crypto/bytestring",
        "crypto/chacha",
        "crypto/cipher_extra",
        "crypto/conf",
        "crypto/curve25519",
        "crypto/des",
        "crypto/dh_extra",
        "crypto/digest_extra",
        "crypto/dilithium",
        "crypto/dsa",
        "crypto/ec_extra",
        "crypto/ecdh_extra",
        "crypto/ecdsa_extra",
        "crypto/engine",
        "crypto/err",
        "crypto/evp",
        "crypto/fipsmodule",
        "crypto/hmac_extra",
        "crypto/hpke",
        "crypto/hrss",
        "crypto/keccak",
        "crypto/kyber",
        "crypto/lhash",
        "crypto/md4",
        "crypto/md5",
        "crypto/mldsa",
        "crypto/mlkem",
        "crypto/obj",
        "crypto/pem",
        "crypto/pkcs7",
        "crypto/pkcs8",
        "crypto/poly1305",
        "crypto/pool",
        "crypto/rand_extra",
        "crypto/rc4",
        "crypto/rsa_extra",
        "crypto/sha",
        "crypto/siphash",
        "crypto/slhdsa",
        "crypto/spx",
        "crypto/stack",
        "crypto/trust_token",
        "crypto/x509",
        "gen/crypto",
    };

    const boringssl_root = boringssl_dep.path(".").getPath(b);

    for (crypto_dirs) |dir| {
        const full_dir_path = boringssl_dep.path(dir).getPath(b);
        glob_sources_relative(arena.allocator(), full_dir_path, boringssl_root, ".cc", &crypto_sources) catch continue;
        glob_sources_relative(arena.allocator(), full_dir_path, boringssl_root, ".c", &crypto_sources) catch continue;
    }

    // Build flags for crypto - target-specific arch defines for hardware acceleration
    const base_crypto_flags = [_][]const u8{
        "-Wall",
        "-Wformat=2",
        "-Wsign-compare",
        "-Wmissing-field-initializers",
        "-Wwrite-strings",
        "-DBORINGSSL_IMPLEMENTATION",
    };

    // Add arch-specific defines for hardware crypto
    // BoringSSL's target.h checks for __x86_64 (no trailing underscore) to define OPENSSL_X86_64
    const crypto_flags_x86_64 = base_crypto_flags ++ [_][]const u8{"-D__x86_64"};
    const crypto_flags_aarch64 = base_crypto_flags ++ [_][]const u8{"-D__AARCH64EL__"};

    const crypto_flags: []const []const u8 = if (target.result.cpu.arch == .x86_64)
        &crypto_flags_x86_64
    else if (target.result.cpu.arch == .aarch64)
        &crypto_flags_aarch64
    else
        &base_crypto_flags;

    crypto.root_module.addCSourceFiles(.{
        .root = boringssl_dep.path("."),
        .files = crypto_sources.items,
        .flags = crypto_flags,
    });

    // Add platform-specific assembly files for hardware acceleration
    if (target.result.cpu.arch == .aarch64 and target.result.os.tag == .linux) {
        // BCM (FIPS module) assembly - AES, SHA, GCM, etc.
        crypto.root_module.addCSourceFiles(.{
            .root = boringssl_dep.path("gen/bcm"),
            .files = &[_][]const u8{
                "aesv8-armv8-linux.S",
                "aesv8-gcm-armv8-linux.S",
                "armv8-mont-linux.S",
                "bn-armv8-linux.S",
                "ghash-neon-armv8-linux.S",
                "ghashv8-armv8-linux.S",
                "p256-armv8-asm-linux.S",
                "p256_beeu-armv8-asm-linux.S",
                "sha1-armv8-linux.S",
                "sha256-armv8-linux.S",
                "sha512-armv8-linux.S",
                "vpaes-armv8-linux.S",
            },
            .flags = &[_][]const u8{},
        });
        // Crypto assembly - ChaCha20
        crypto.root_module.addCSourceFiles(.{
            .root = boringssl_dep.path("gen/crypto"),
            .files = &[_][]const u8{
                "chacha-armv8-linux.S",
                "chacha20_poly1305_armv8-linux.S",
            },
            .flags = &[_][]const u8{},
        });
    } else if (target.result.cpu.arch == .x86_64 and target.result.os.tag == .linux) {
        // BCM (FIPS module) assembly - AES-NI, SHA, GCM, AVX, Montgomery, RSA, P-256, etc.
        crypto.root_module.addCSourceFiles(.{
            .root = boringssl_dep.path("gen/bcm"),
            .files = &[_][]const u8{
                "aes-gcm-avx2-x86_64-linux.S",
                "aes-gcm-avx512-x86_64-linux.S",
                "aesni-gcm-x86_64-linux.S",
                "aesni-x86_64-linux.S",
                "ghash-ssse3-x86_64-linux.S",
                "ghash-x86_64-linux.S",
                "p256-x86_64-asm-linux.S",
                "p256_beeu-x86_64-asm-linux.S",
                "rdrand-x86_64-linux.S",
                "rsaz-avx2-linux.S",
                "sha1-x86_64-linux.S",
                "sha256-x86_64-linux.S",
                "sha512-x86_64-linux.S",
                "vpaes-x86_64-linux.S",
                "x86_64-mont-linux.S",
                "x86_64-mont5-linux.S",
            },
            .flags = &[_][]const u8{},
        });
        // Crypto assembly - ChaCha20, MD5
        crypto.root_module.addCSourceFiles(.{
            .root = boringssl_dep.path("gen/crypto"),
            .files = &[_][]const u8{
                "chacha-x86_64-linux.S",
                "chacha20_poly1305_x86_64-linux.S",
                "md5-x86_64-linux.S",
                "aes128gcmsiv-x86_64-linux.S",
            },
            .flags = &[_][]const u8{},
        });
        // Fiat-crypto assembly - P-256 and Curve25519 ADX optimizations
        crypto.root_module.addCSourceFiles(.{
            .root = boringssl_dep.path("third_party/fiat/asm"),
            .files = &[_][]const u8{
                "fiat_curve25519_adx_mul.S",
                "fiat_curve25519_adx_square.S",
                "fiat_p256_adx_mul.S",
                "fiat_p256_adx_sqr.S",
            },
            .flags = &[_][]const u8{},
        });
    } else if (target.result.cpu.arch == .aarch64 and target.result.os.tag == .macos) {
        // BCM (FIPS module) assembly for macOS ARM64 - AES, SHA, GCM, etc.
        crypto.root_module.addCSourceFiles(.{
            .root = boringssl_dep.path("gen/bcm"),
            .files = &[_][]const u8{
                "aesv8-armv8-apple.S",
                "aesv8-gcm-armv8-apple.S",
                "armv8-mont-apple.S",
                "bn-armv8-apple.S",
                "ghash-neon-armv8-apple.S",
                "ghashv8-armv8-apple.S",
                "p256-armv8-asm-apple.S",
                "p256_beeu-armv8-asm-apple.S",
                "sha1-armv8-apple.S",
                "sha256-armv8-apple.S",
                "sha512-armv8-apple.S",
                "vpaes-armv8-apple.S",
            },
            .flags = &[_][]const u8{},
        });
        // Crypto assembly - ChaCha20
        crypto.root_module.addCSourceFiles(.{
            .root = boringssl_dep.path("gen/crypto"),
            .files = &[_][]const u8{
                "chacha-armv8-apple.S",
                "chacha20_poly1305_armv8-apple.S",
            },
            .flags = &[_][]const u8{},
        });
    } else if (target.result.cpu.arch == .x86_64 and target.result.os.tag == .macos) {
        // BCM (FIPS module) assembly for macOS x86_64 - AES-NI, SHA, GCM, AVX, etc.
        crypto.root_module.addCSourceFiles(.{
            .root = boringssl_dep.path("gen/bcm"),
            .files = &[_][]const u8{
                "aes-gcm-avx2-x86_64-apple.S",
                "aes-gcm-avx512-x86_64-apple.S",
                "aesni-gcm-x86_64-apple.S",
                "aesni-x86_64-apple.S",
                "ghash-ssse3-x86_64-apple.S",
                "ghash-x86_64-apple.S",
                "p256-x86_64-asm-apple.S",
                "p256_beeu-x86_64-asm-apple.S",
                "rdrand-x86_64-apple.S",
                "rsaz-avx2-apple.S",
                "sha1-x86_64-apple.S",
                "sha256-x86_64-apple.S",
                "sha512-x86_64-apple.S",
                "vpaes-x86_64-apple.S",
                "x86_64-mont-apple.S",
                "x86_64-mont5-apple.S",
            },
            .flags = &[_][]const u8{},
        });
        // Crypto assembly - ChaCha20, MD5
        crypto.root_module.addCSourceFiles(.{
            .root = boringssl_dep.path("gen/crypto"),
            .files = &[_][]const u8{
                "chacha-x86_64-apple.S",
                "chacha20_poly1305_x86_64-apple.S",
                "md5-x86_64-apple.S",
                "aes128gcmsiv-x86_64-apple.S",
            },
            .flags = &[_][]const u8{},
        });
        // Fiat-crypto assembly - P-256 and Curve25519 ADX optimizations
        crypto.root_module.addCSourceFiles(.{
            .root = boringssl_dep.path("third_party/fiat/asm"),
            .files = &[_][]const u8{
                "fiat_curve25519_adx_mul.S",
                "fiat_curve25519_adx_square.S",
                "fiat_p256_adx_mul.S",
                "fiat_p256_adx_sqr.S",
            },
            .flags = &[_][]const u8{},
        });
    }

    b.installArtifact(crypto);
    return crypto;
}

fn buildBoringSSLSSL(
    b: *std.Build,
    target: std.Build.ResolvedTarget,
    optimize: std.builtin.OptimizeMode,
    boringssl_dep: *std.Build.Dependency,
    crypto: *std.Build.Step.Compile,
) *std.Build.Step.Compile {
    const ssl_mod = b.createModule(.{
        .target = target,
        .optimize = optimize,
    });
    const ssl = b.addLibrary(.{
        .linkage = .static,
        .name = "ssl",
        .root_module = ssl_mod,
    });

    ssl.root_module.linkSystemLibrary("c++", .{});
    ssl.root_module.linkLibrary(crypto);
    ssl.root_module.addIncludePath(boringssl_dep.path("include"));
    ssl.root_module.addIncludePath(boringssl_dep.path("src/include"));

    const ssl_files = [_][]const u8{
        "bio_ssl.cc",
        "d1_both.cc",
        "d1_lib.cc",
        "d1_pkt.cc",
        "d1_srtp.cc",
        "dtls_method.cc",
        "dtls_record.cc",
        "encrypted_client_hello.cc",
        "extensions.cc",
        "handoff.cc",
        "handshake_client.cc",
        "handshake_server.cc",
        "handshake.cc",
        "s3_both.cc",
        "s3_lib.cc",
        "s3_pkt.cc",
        "ssl_aead_ctx.cc",
        "ssl_asn1.cc",
        "ssl_buffer.cc",
        "ssl_cert.cc",
        "ssl_cipher.cc",
        "ssl_credential.cc",
        "ssl_file.cc",
        "ssl_key_share.cc",
        "ssl_lib.cc",
        "ssl_privkey.cc",
        "ssl_session.cc",
        "ssl_stat.cc",
        "ssl_transcript.cc",
        "ssl_versions.cc",
        "ssl_x509.cc",
        "t1_enc.cc",
        "tls_method.cc",
        "tls_record.cc",
        "tls13_both.cc",
        "tls13_client.cc",
        "tls13_enc.cc",
        "tls13_server.cc",
    };

    // Build flags for ssl - target-specific arch defines
    const base_ssl_flags = [_][]const u8{"-Wall"};
    const ssl_flags_x86_64 = base_ssl_flags ++ [_][]const u8{"-D__x86_64"};
    const ssl_flags_aarch64 = base_ssl_flags ++ [_][]const u8{"-D__AARCH64EL__"};

    const ssl_flags: []const []const u8 = if (target.result.cpu.arch == .x86_64)
        &ssl_flags_x86_64
    else if (target.result.cpu.arch == .aarch64)
        &ssl_flags_aarch64
    else
        &base_ssl_flags;

    ssl.root_module.addCSourceFiles(.{
        .root = boringssl_dep.path("ssl"),
        .files = &ssl_files,
        .flags = ssl_flags,
    });

    b.installArtifact(ssl);
    return ssl;
}

pub fn glob_sources(
    allocator: std.mem.Allocator,
    base: []const u8,
    ext: []const u8,
    paths: *std.ArrayListUnmanaged([]const u8),
) !void {
    var dir = try std.fs.cwd().openDir(base, .{ .iterate = true });
    defer dir.close();

    var walker = try dir.walk(allocator);
    defer walker.deinit();

    while (try walker.next()) |entry| {
        const path_ext = std.fs.path.extension(entry.path);
        if (std.mem.eql(u8, path_ext, ext)) {
            const path = try std.fs.path.join(allocator, &.{ base, entry.path });
            try paths.append(allocator, path);
        }
    }
}

pub fn glob_sources_relative(
    allocator: std.mem.Allocator,
    search_dir: []const u8,
    root_dir: []const u8,
    ext: []const u8,
    paths: *std.ArrayListUnmanaged([]const u8),
) !void {
    var dir = try std.fs.cwd().openDir(search_dir, .{ .iterate = true });
    defer dir.close();

    var walker = try dir.walk(allocator);
    defer walker.deinit();

    while (try walker.next()) |entry| {
        const path_ext = std.fs.path.extension(entry.path);
        if (std.mem.eql(u8, path_ext, ext)) {
            if (shouldSkipFile(entry.path)) {
                continue;
            }

            const absolute_path = try std.fs.path.join(allocator, &.{ search_dir, entry.path });
            const relative_path = try std.fs.path.relative(allocator, root_dir, absolute_path);
            try paths.append(allocator, relative_path);
        }
    }
}

fn shouldSkipFile(file_path: []const u8) bool {
    const filename = std.fs.path.basename(file_path);

    if (std.mem.indexOf(u8, file_path, "test") != null) return true;

    const skip_files = [_][]const u8{
        "gtest_main.cc",
        "file_test_gtest.cc",
        "file_util.cc",
        "file_util.c",
    };

    for (skip_files) |skip_file| {
        if (std.mem.eql(u8, filename, skip_file)) return true;
    }

    return false;
}

/// Fetch pre-built BoringSSL artifacts from Cloudflare R2
fn fetchFromR2(b: *std.Build, triple: []const u8) !void {
    const prebuilt_dir = b.fmt("prebuilt/{s}", .{triple});

    // Create the prebuilt directory
    const build_root = b.build_root.handle;
    build_root.makePath(prebuilt_dir) catch |err| {
        std.log.err("Failed to create prebuilt directory: {}", .{err});
        return err;
    };

    // Get absolute path to build root for curl
    const abs_root = build_root.realpathAlloc(b.allocator, ".") catch {
        std.log.err("Failed to get absolute path", .{});
        return error.PathError;
    };

    // Files to fetch
    const files = [_][]const u8{ "libcrypto.a", "libssl.a" };

    for (files) |filename| {
        const url = b.fmt("{s}/boring_tls/{s}/{s}", .{ R2_PUBLIC_URL, triple, filename });
        const full_dest = b.fmt("{s}/{s}/{s}", .{ abs_root, prebuilt_dir, filename });

        std.log.info("Fetching {s}...", .{filename});

        // Use curl to download (available on all platforms)
        var child = std.process.Child.init(
            &[_][]const u8{ "curl", "-fSL", "--create-dirs", "-o", full_dest, url },
            b.allocator,
        );

        const term = child.spawnAndWait() catch |err| {
            std.log.err("Failed to spawn curl: {}", .{err});
            return err;
        };

        if (term.Exited != 0) {
            std.log.err("curl failed with exit code {}", .{term.Exited});
            return error.FetchFailed;
        }
    }
}
