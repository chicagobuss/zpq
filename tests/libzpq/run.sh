#!/usr/bin/env bash
# Build libzpq and run its C ABI test, plain and under AddressSanitizer, then the Python binding and service tests, and
# the differential corpus through libzpq, the CLI and the Lambda (byte-identical answers and files).
# Run from the repo root. LOOP sets the RSS check's call count (default 10000; the ASan run uses a tenth); CYCLES the
# open/run/close cycles both runs make (default 1000), which LeakSanitizer checks.
set -euo pipefail
LOOP=${LOOP:-10000}
CYCLES=${CYCLES:-1000}
PY=${PYTHON:-python3}
ZIG=${ZIG:-zig} # may be a command with a prefix, e.g. "zpq-heavy zig"
OPT=${OPTIMIZE:-ReleaseSafe}
$ZIG build libzpq -Doptimize="$OPT" -j8
$ZIG build -Doptimize="$OPT" -j8 # the CLI and the Lambda the differential compares with
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
ZPQ_LIB="$lib/libzpq.so" "$PY" tests/libzpq/write_publication.py
"$PY" tools/bloom/defaults.py zig-out/bin/zpq "$lib/libzpq.so"
ZPQ_LIB="$lib/libzpq.so" "$PY" tests/libzpq/finalize_failure.py
if [[ $(uname -s) == Linux ]] && command -v strace >/dev/null; then
  ZPQ_LIB="$lib/libzpq.so" "$PY" tools/write_sync_check.py zig-out/bin/zpq
fi
# The differential corpus needs DuckDB and pyarrow (tools/differential.py), as the repository's own gate does.
ZPQ_LIB="$lib/libzpq.so" "$PY" tests/libzpq/differential.py

ZPQ_LIB="$lib/libzpq.so" "$PY" tests/libzpq/sql_differential.py
ZPQ_LIB="$lib/libzpq.so" "$PY" tests/format_correctness.py zig-out/bin/zpq
