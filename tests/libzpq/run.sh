#!/usr/bin/env bash
# Build libzpq and run its C ABI test, plain and under AddressSanitizer, then the Python binding and service tests.
# Run from the repo root. LOOP sets the RSS check's call count (default 10000; the ASan run uses a tenth); CYCLES the
# open/run/close cycles both runs make (default 1000), which LeakSanitizer checks.
set -euo pipefail
LOOP=${LOOP:-10000}
CYCLES=${CYCLES:-1000}
PY=${PYTHON:-python3}
ZIG=${ZIG:-zig} # may be a command with a prefix, e.g. "zpq-heavy zig"
OPT=${OPTIMIZE:-ReleaseSafe}
$ZIG build libzpq -Doptimize="$OPT"
lib=$PWD/zig-out/lib
out=$(mktemp -d)
trap 'rm -rf "$out"' EXIT
$ZIG cc -O1 -g -o "$out/test_abi" tests/libzpq/test_abi.c -Izig-out/include -L"$lib" -lzpq -lpthread -Wl,-rpath,"$lib"
"$out/test_abi" "$LOOP" "$CYCLES"
# zig cc carries no sanitizer runtime: the system compiler's. use_sigaltstack=0: every thread zpq starts installs
# Zig's own signal stack, which ASan would otherwise try to unmap as its own when the thread exits.
${CC:-gcc} -O1 -g -fsanitize=address -fno-omit-frame-pointer -o "$out/test_abi_asan" tests/libzpq/test_abi.c \
  -Izig-out/include -L"$lib" -lzpq -lpthread -Wl,-rpath,"$lib"
ASAN_OPTIONS=detect_leaks=1:abort_on_error=1:use_sigaltstack=0 "$out/test_abi_asan" $((LOOP / 10)) "$CYCLES"
ZPQ_LIB="$lib/libzpq.so" "$PY" tests/libzpq/test_python.py
