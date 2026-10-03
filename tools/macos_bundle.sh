#!/usr/bin/env bash
# Build a self-contained macOS check bundle on Linux: the zpq CLI and the
# unit-test executables cross-compiled for aarch64-macos and x86_64-macos,
# the fixtures they read, golden CLI outputs recorded from a native Linux
# build of the same tree, and tools/macos_check.sh to run it all on a Mac.
#
#   tools/macos_bundle.sh [OUT_DIR]        (default: zig-out/macos-check)
#
# Then on the Mac:
#   tar -xzf zpq-macos-check-<rev>.tar.gz && zpq-macos-check-<rev>/macos_check.sh
#
# Needs: zig 0.17.0; vendor/boring_tls/prebuilt/{aarch64,x86_64}-macos (fetch
# them like .github/workflows/release.yml does); a python with pyarrow
# ($PYTHON, else .venv/bin/python) to generate the smoke fixtures.
set -euo pipefail

REPO="$(cd "$(dirname "$0")/.." && pwd)"
cd "$REPO"
OUT_DIR="$(mkdir -p "${1:-zig-out/macos-check}" && cd "${1:-zig-out/macos-check}" && pwd)"
TARGETS="aarch64-macos x86_64-macos"
PY="${PYTHON:-}"
if [ -z "$PY" ]; then if [ -x .venv/bin/python ]; then PY=.venv/bin/python; else PY=python3; fi; fi
"$PY" -c 'import pyarrow' 2> /dev/null || { echo "need a python with pyarrow (set PYTHON=...)" >&2; exit 2; }

for t in $TARGETS; do
  for lib in libssl.a libcrypto.a; do
    [ -s "vendor/boring_tls/prebuilt/$t/$lib" ] || {
      echo "missing vendor/boring_tls/prebuilt/$t/$lib — fetch it as .github/workflows/release.yml does" >&2; exit 2; }
  done
done

REV="$(git describe --always --dirty 2> /dev/null || echo unknown)"
NAME="zpq-macos-check-$REV"
TMP="$(mktemp -d "${TMPDIR:-/tmp}/zpq-macos-bundle.XXXXXX")"
trap 'rm -rf "$TMP"' EXIT
STAGE="$TMP/$NAME"
mkdir -p "$STAGE/bin" "$STAGE/smoke" "$STAGE/data" "$STAGE/ci"

# ---- binaries: ReleaseFast CLI (what release.yml ships) + Debug test executables (what `zig build test` runs)
for t in $TARGETS native; do
  tgt=(); [ "$t" = native ] || tgt=(-Dtarget="$t")
  echo "== building $t" >&2
  zig build "${tgt[@]}" -Doptimize=ReleaseFast cli --prefix "$TMP/build-$t"
  zig build "${tgt[@]}" test-bin --prefix "$TMP/build-$t"
done
for t in $TARGETS; do
  mkdir -p "$STAGE/bin/$t"
  cp "$TMP/build-$t/bin/zpq" "$TMP/build-$t/bin/zpq-test" "$TMP/build-$t/bin/zpq-lambda-test" "$STAGE/bin/$t/"
done

# A raw syscall instruction in a Mach-O binary is a Linux syscall that survived the port: macOS code goes through
# libSystem, never `svc`/`syscall` directly.
OBJDUMP="$(command -v llvm-objdump || true)"
if [ -n "$OBJDUMP" ]; then
  for t in $TARGETS; do
    for b in "$STAGE/bin/$t"/*; do
      n="$("$OBJDUMP" -d --no-show-raw-insn "$b" | grep -cE '[[:space:]](svc|syscall)([[:space:]]|$)' || true)"
      [ "$n" = 0 ] || { echo "$b: $n raw syscall instructions — Linux-only code reached the macOS build" >&2; exit 1; }
    done
  done
  echo "== no raw syscall instructions in the macOS binaries" >&2
else
  echo "== llvm-objdump not found; skipping the raw-syscall scan" >&2
fi

# ---- unit-test fixtures (cwd-relative paths the tests open)
cp -R ci/fixtures "$STAGE/ci/fixtures"
if [ -d data/parquet-testing/data ]; then
  mkdir -p "$STAGE/data/parquet-testing"
  cp -R data/parquet-testing/data "$STAGE/data/parquet-testing/data"
else
  echo "warning: data/parquet-testing missing; its tests will skip on both sides" >&2
fi
for f in benchmark_100mb.parquet nested_edges.parquet bench_types.parquet; do
  if [ -f "data/$f" ]; then cp "data/$f" "$STAGE/data/"; fi
done

# ---- CLI smoke fixtures: four multi-row-group files in four codecs, plus a brotli file zpq cannot recompress
"$PY" - "$STAGE/smoke" <<'PY'
import sys, datetime
import pyarrow as pa, pyarrow.parquet as pq
d = sys.argv[1]
n = 40_000
for p, codec in enumerate(["snappy", "zstd", "gzip", "none"]):
    ids = range(p * n, (p + 1) * n)
    t = pa.table({
        "id": pa.array(ids, type=pa.int64()),
        "qty": pa.array([(i * 37) % 100 for i in ids], type=pa.int32()),
        "price": pa.array([(i % 1000) * 0.25 for i in ids], type=pa.float64()),
        "name": pa.array([f"n{i % 500:04d}" for i in ids], type=pa.string()),
        "cat": pa.array([("red", "green", "blue", "cyan", "plum")[i % 5] for i in ids], type=pa.string()),
        "opt": pa.array([None if i % 7 == 0 else i % 13 for i in ids], type=pa.int32()),
        "day": pa.array([datetime.date(2026, 1, 1) + datetime.timedelta(days=i % 365) for i in ids], type=pa.date32()),
    })
    pq.write_table(t, f"{d}/part-{p}.parquet", compression=codec, row_group_size=10_000)
pq.write_table(pa.table({"x": pa.array([1, 2, 3], type=pa.int64())}), f"{d}/brotli.parquet", compression="brotli")
PY
cp data/parquet-testing/data/alltypes_plain.parquet "$STAGE/smoke/" 2> /dev/null || cp ci/fixtures/parquet/full_match.parquet "$STAGE/smoke/alltypes_plain.parquet"
cp tools/macos_check.sh "$STAGE/"

# ---- goldens from the native Linux build, and the Linux unit-test skip baseline macos_check.sh holds the Mac to
echo "== recording expected outputs with the Linux build" >&2
"$STAGE/macos_check.sh" --record --bin "$TMP/build-native/bin" > "$TMP/record.log" 2>&1 || {
  cat "$TMP/record.log" >&2; echo "recording goldens failed" >&2; exit 1; }
{
  echo "commit=$REV"
  echo "built=$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  echo "zig=$(zig version)"
  for t in zpq-test zpq-lambda-test; do
    log="$TMP/linux-$t.log"
    (cd "$STAGE" && "$TMP/build-native/bin/$t") > "$log" 2>&1 || { cat "$log" >&2; echo "Linux $t failed" >&2; exit 1; }
    echo "linux_skipped_$t=$(sed -n 's/.*passed; \([0-9]*\) skipped.*/\1/p' "$log" | tail -n 1 | grep . || echo 0)"
  done
  # Tests that skip off Linux by design (the epoll loop).
  echo "linux_only_tests_zpq-test=$(grep -c 'builtin.os.tag != .linux) return error.SkipZigTest' src/io/epoll.zig)"
  echo "linux_only_tests_zpq-lambda-test=0"
  if [ -n "$OBJDUMP" ]; then
    for t in $TARGETS; do
      echo "minos_$t=$("$OBJDUMP" --macho --private-headers "$STAGE/bin/$t/zpq" | sed -n 's/^ *minos //p' | head -n 1)"
    done
  fi
} > "$STAGE/MANIFEST"
rm -rf "$STAGE/.zig-cache"

tar -czf "$OUT_DIR/$NAME.tar.gz" -C "$TMP" "$NAME"
echo "$OUT_DIR/$NAME.tar.gz ($(du -h "$OUT_DIR/$NAME.tar.gz" | cut -f1))"
cat "$STAGE/MANIFEST"
