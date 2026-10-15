// Generated for ZPQ vendor/snappy from cmake/config.h.in. Tuned for
// modern Linux toolchains (clang/gcc 8+). Architecture-specific
// intrinsics follow the target's CPU features (see below); NEON CRC32
// stays off.

#ifndef THIRD_PARTY_SNAPPY_OPENSOURCE_CMAKE_CONFIG_H_
#define THIRD_PARTY_SNAPPY_OPENSOURCE_CMAKE_CONFIG_H_

#define HAVE_ATTRIBUTE_ALWAYS_INLINE 1
#define HAVE_BUILTIN_CTZ 1
#define HAVE_BUILTIN_EXPECT 1
#define HAVE_BUILTIN_PREFETCH 1
#define HAVE_FUNC_MMAP 1
#define HAVE_FUNC_SYSCONF 1
#define HAVE_LIBLZO2 0
#define HAVE_LIBZ 0
#define HAVE_LIBLZ4 0
#define HAVE_SYS_MMAN_H 1
#define HAVE_SYS_RESOURCE_H 1
#define HAVE_SYS_TIME_H 1
#define HAVE_SYS_UIO_H 1
#define HAVE_UNISTD_H 1
#define HAVE_WINDOWS_H 0

// Intrinsic-specific defines. Conditionally enabled based on target CPU features.
#ifdef __SSSE3__
#define SNAPPY_HAVE_SSSE3 1
#else
#define SNAPPY_HAVE_SSSE3 0
#endif

#ifdef __SSE4_2__
#define SNAPPY_HAVE_X86_CRC32 1
#else
#define SNAPPY_HAVE_X86_CRC32 0
#endif

#ifdef __BMI2__
#define SNAPPY_HAVE_BMI2 1
#else
#define SNAPPY_HAVE_BMI2 0
#endif

// NEON is mandatory on aarch64, so its byte-shuffle copy paths (decompression only) need no runtime check.
#if defined(__ARM_NEON)
#define SNAPPY_HAVE_NEON 1
#else
#define SNAPPY_HAVE_NEON 0
#endif
// Off: its hash would change the compressed bytes arm64 builds write.
#define SNAPPY_HAVE_NEON_CRC32 0

#define SNAPPY_IS_BIG_ENDIAN 0

#endif  // THIRD_PARTY_SNAPPY_OPENSOURCE_CMAKE_CONFIG_H_
