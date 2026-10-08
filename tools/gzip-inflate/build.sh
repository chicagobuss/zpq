#!/bin/sh
# Builds `compare` against zpq's vendored libdeflate and the system zlib: ./build.sh [OUT] (default ./compare).
# FALLBACK=1 also compiles libdeflate's test hook, so LIBDEFLATE_DISABLE_CPU_FEATURES=sse2,pclmulqdq,avx,... runs its
# portable paths (zpq's own build never enables it).
set -eu
here=$(cd "$(dirname "$0")" && pwd)
v=$here/../../vendor/libdeflate
out=${1:-$here/compare}
extra=""
[ "${FALLBACK:-0}" = 1 ] && extra="-DTEST_SUPPORT__DO_NOT_USE"
zig cc -O2 $extra -I"$v" "$here/compare.c" "$v/lib/deflate_decompress.c" "$v/lib/gzip_decompress.c" "$v/lib/crc32.c" \
    "$v/lib/utils.c" "$v/lib/x86/cpu_features.c" "$v/lib/arm/cpu_features.c" -lz -o "$out"
echo "$out"
