const std = @import("std");

pub fn build(b: *std.Build) !void {
    const target = b.standardTargetOptions(.{});

    const optimize = b.standardOptimizeOption(.{});

    const boringssl_dep = b.dependency("boringssl", .{});

    const use_prebuilt = b.option(bool, "use-prebuilt", "Use pre-built static libraries if available") orelse true;
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
        boring_tls_mod.addCMacro("__x86_64__", "1");
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
    const crypto_flags_x86_64 = base_crypto_flags ++ [_][]const u8{"-D__x86_64__"};
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
    const ssl_flags_x86_64 = base_ssl_flags ++ [_][]const u8{"-D__x86_64__"};
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
