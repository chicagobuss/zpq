#!/bin/sh
# Builds `compare` against the vendored, patched zstd: ./build.sh [OUT] (default ./compare). Needs zig (as `zig cc`).
set -eu
here=$(cd "$(dirname "$0")" && pwd)
z=$here/../../vendor/zstd
out=${1:-$here/compare}
zig cc -O3 -march=native -DZPQ_HUF4X_AVX512=1 -DZSTD_DISABLE_ASM \
    -I"$z/lib/common" -I"$z/lib/compress" -I"$z/zpq" \
    "$here/compare.c" "$z/zpq/huf4x_avx512.c" "$z/zpq/huf4x_check.c" \
    "$z"/lib/common/*.c "$z"/lib/compress/*.c \
    -o "$out" -lpthread
echo "$out"
