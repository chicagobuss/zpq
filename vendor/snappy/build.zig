//! vendor/snappy build — google/snappy 1.2.1, source-built.
//!
//! Produces a static `libsnappy.a` plus a Zig module exposing the
//! header. The implementation is C++; we link `libc++` statically
//! (the Zig-bundled libc++) to keep the lambda binary self-contained.
//!
//! Generic build only — no architecture-specific intrinsics. The
//! generic implementation hits ~250 MB/s on x86_64, plenty for our
//! workloads. SSSE3 / BMI2 / NEON-CRC32 paths can be re-enabled
//! later via per-target Zig flags if a workload demands them.

const std = @import("std");

pub fn build(b: *std.Build) !void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});

    const lib_mod = b.createModule(.{
        .target = target,
        .optimize = optimize,
        .link_libc = true,
        .link_libcpp = true,
    });
    lib_mod.addCSourceFiles(.{
        .root = b.path("."),
        .files = &.{
            "snappy.cc",
            "snappy-c.cc",
            "snappy-sinksource.cc",
            "snappy-stubs-internal.cc",
        },
        .flags = &.{
            "-std=c++17",
            "-DHAVE_CONFIG_H",
            "-fno-exceptions",
            "-fno-rtti",
            // Snappy's source uses inline AVX2 intrinsics under
            // `defined(__x86_64__) && defined(__AVX__)`, but only
            // includes <immintrin.h> when SNAPPY_HAVE_BMI2 /
            // SNAPPY_HAVE_X86_CRC32 are set. We target generic x86_64
            // without those flags, so we explicitly drop AVX from the
            // compiler's predefined macros to keep the code path
            // portable. -1% on AVX-capable hardware; portable to
            // older Lambda execution environments.
            "-mno-avx",
        },
    });
    lib_mod.addIncludePath(b.path("."));

    const lib = b.addLibrary(.{
        .name = "snappy",
        .root_module = lib_mod,
    });
    lib.installHeader(b.path("snappy-c.h"), "snappy-c.h");

    b.installArtifact(lib);

    // Expose the include directory and library to dependents.
    const mod = b.addModule("snappy", .{
        .root_source_file = b.path("root.zig"),
        .target = target,
        .optimize = optimize,
    });
    mod.addIncludePath(b.path("."));
    mod.linkLibrary(lib);
}
