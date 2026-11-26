#!/usr/bin/env bash
# Build libzpq and run its C ABI test, plain and under AddressSanitizer, then the Python binding and service tests, and
# the differential corpus through libzpq, the CLI and the Lambda (byte-identical answers and files).
# Run from the repo root. LOOP sets the RSS check's call count (default 10000; the ASan run uses a tenth); CYCLES the
# open/run/close cycles both runs make (default 1000), which LeakSanitizer checks where it exists. JOBS is zig's -j.
set -euo pipefail
LOOP=${LOOP:-10000}
CYCLES=${CYCLES:-1000}
PY=${PYTHON:-python3}
ZIG=${ZIG:-zig} # may be a command with a prefix, e.g. "zpq-heavy zig"
OPT=${OPTIMIZE:-ReleaseSafe}
JOBS=${JOBS:-8}
$ZIG build libzpq -Doptimize="$OPT" -j"$JOBS"
$ZIG build -Doptimize="$OPT" -j"$JOBS" # the CLI and the Lambda the differential compares with
lib=$PWD/zig-out/lib
# LeakSanitizer is part of Linux's ASan only: macOS's refuses detect_leaks=1 and aborts. There the ASan run still
# checks every access; leaks are checked on Linux.
case $(uname -s) in
  Darwin) so=$lib/libzpq.dylib leaks=0 ;;
  *) so=$lib/libzpq.so leaks=1 ;;
esac
out=$(mktemp -d)
trap 'rm -rf "$out"' EXIT
$ZIG cc -O1 -g -o "$out/test_abi" tests/libzpq/test_abi.c -Izig-out/include -L"$lib" -lzpq -lpthread -Wl,-rpath,"$lib"
"$out/test_abi" "$LOOP" "$CYCLES"
# zig cc carries no sanitizer runtime: the system compiler's. use_sigaltstack=0: every thread zpq starts installs
# Zig's own signal stack, which ASan would otherwise try to unmap as its own when the thread exits.
${CC:-gcc} -O1 -g -fsanitize=address -fno-omit-frame-pointer -o "$out/test_abi_asan" tests/libzpq/test_abi.c \
  -Izig-out/include -L"$lib" -lzpq -lpthread -Wl,-rpath,"$lib"
ASAN_OPTIONS=detect_leaks=$leaks:abort_on_error=1:use_sigaltstack=0 "$out/test_abi_asan" $((LOOP / 10)) "$CYCLES"
ZPQ_LIB="$so" "$PY" tests/libzpq/test_python.py
ZPQ_LIB="$so" "$PY" tests/libzpq/write_publication.py
ZPQ_LIB="$so" "$PY" tests/libzpq/finalize_failure.py
# Opening and closing libzpq leaves a host's SIGPIPE disposition as it was, and a closed peer kills nothing.
"$PY" tests/libzpq/sigpipe.py "$so" x
# The differential corpus needs DuckDB and pyarrow (tests/libzpq/corpus.py), as the repository's own gate does.
ZPQ_LIB="$so" "$PY" tests/libzpq/differential.py

ZPQ_LIB="$so" "$PY" tests/libzpq/sql_differential.py
