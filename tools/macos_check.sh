#!/usr/bin/env bash
# Run zpq's unit tests and a CLI smoke ON A MAC, from a bundle built on Linux
# by tools/macos_bundle.sh. Prints a PASS/FAIL summary; exits 1 on any FAIL.
#
#   ./macos_check.sh [--arch aarch64|x86_64] [--network] [--no-python] [--keep]
#
#   --arch      which bundled build to run (default: this Mac's, from uname -m;
#               --arch x86_64 on Apple silicon runs the Intel build under Rosetta)
#   --network   also check DNS + TCP + TLS + HTTP against real S3 with an
#               anonymous request (expects S3's refusal, not a connection failure).
#               With real AWS_*/S3_* credentials in the environment and
#               ZPQ_CHECK_S3_URL=s3://bucket/key.parquet, also reads that file.
#   --no-python skip the pyarrow cross-checks even if pyarrow is importable
#   --keep      keep the scratch directory with every output
#
# Bundle layout (relative to this script): bin/<arch>-macos/{zpq,zpq-test,
# zpq-lambda-test}, data/ and ci/ (unit-test fixtures), smoke/ (CLI fixtures),
# expected/ (golden outputs recorded from the Linux build of the same commit),
# skips/ (each test binary's skips on Linux, and its Linux-only tests), MANIFEST.
#
# CLI outputs are compared against expected/, so the check needs nothing but
# bash; with python3 + pyarrow it also validates every written parquet file
# independently. macos_bundle.sh runs this script on Linux with --record --bin
# to produce expected/.
set -u

# The tests a Zig test runner log reports skipped, one name per line. The runner prints `N/M name...` and then the
# outcome; a test that logs first puts `SKIP` on a line of its own instead of after the `...`.
skip_names() {
  awk '
    match($0, /^[0-9]+\/[0-9]+ /) { name = substr($0, RLENGTH + 1); sub(/\.\.\..*$/, "", name) }
    /(^|\.\.\.)SKIP$/ && name != "" { print name; name = "" }
  ' "$1"
}
if [ "${1:-}" = --skips ]; then skip_names "$2"; exit; fi   # for macos_bundle.sh

ROOT="$(cd "$(dirname "$0")" && pwd)"
ARCH="" BIN="" RECORD=0 NETWORK=0 USE_PY=1 KEEP=0
while [ $# -gt 0 ]; do
  case "$1" in
    --arch) ARCH="$2"; shift 2 ;;
    --bin) BIN="$2"; shift 2 ;;            # binary dir override (self-test on Linux)
    --record) RECORD=1; shift ;;            # write expected/ instead of comparing
    --network) NETWORK=1; shift ;;
    --no-python) USE_PY=0; shift ;;
    --keep) KEEP=1; shift ;;
    -h|--help) sed -n '2,24p' "$0"; exit 0 ;;
    *) echo "unknown arg: $1" >&2; exit 2 ;;
  esac
done

if [ -z "$BIN" ]; then
  if [ -z "$ARCH" ]; then
    case "$(uname -m)" in
      arm64|aarch64) ARCH=aarch64 ;;
      x86_64) ARCH=x86_64 ;;
      *) echo "unknown machine $(uname -m); pass --arch" >&2; exit 2 ;;
    esac
  fi
  BIN="$ROOT/bin/$ARCH-macos"
  [ "$(uname -s)" = Darwin ] || echo "warning: not macOS ($(uname -s)); the bundled binaries will not run here" >&2
fi
BIN="$(cd "$BIN" && pwd)" || exit 2
ZPQ="$BIN/zpq"
EXP="$ROOT/expected"
[ -x "$ZPQ" ] || { echo "no zpq binary at $ZPQ" >&2; exit 2; }

WORK="$(mktemp -d "${TMPDIR:-/tmp}/zpq-macos-check.XXXXXX")"
cleanup() { if [ "$KEEP" = 1 ]; then echo "kept: $WORK"; else rm -rf "$WORK"; fi; rm -rf "$ROOT/.zig-cache"; }
trap cleanup EXIT
cp -R "$ROOT/smoke" "$WORK/smoke"
mkdir -p "$WORK/out" "$WORK/log"
if [ "$RECORD" = 1 ]; then rm -rf "$EXP"; mkdir -p "$EXP"; fi

# Downloaded (not scp'd) bundles carry the quarantine flag; Gatekeeper would refuse the unsigned-by-Apple binaries.
if command -v xattr > /dev/null 2>&1; then xattr -dr com.apple.quarantine "$ROOT" 2> /dev/null || true; fi

N_PASS=0 N_FAIL=0 N_SKIP=0 FAILED=""
pass() { N_PASS=$((N_PASS + 1)); printf 'PASS  %s\n' "$1"; }
skip() { N_SKIP=$((N_SKIP + 1)); printf 'SKIP  %s (%s)\n' "$1" "$2"; }
fail() {
  N_FAIL=$((N_FAIL + 1)); FAILED="$FAILED $1"; printf 'FAIL  %s: %s\n' "$1" "$2"
  if [ -n "${3:-}" ] && [ -s "$3" ]; then sed -n '1,15p' "$3" | sed 's/^/      | /'; fi
}

# Timings and per-run numbers are the only nondeterministic part of an output.
normalize() { sed -e 's/,"total_ms":.*$//'; }

# compare <golden> <file> [case]: record <file> as expected/<golden>, or diff it against that.
compare() {
  local golden="$1" label="${3:-$1}" norm="$WORK/log/${3:-$1}.norm"
  normalize < "$2" > "$norm"
  if [ "$RECORD" = 1 ]; then
    # Cases sharing a golden must agree on Linux too.
    if [ -f "$EXP/$golden" ] && ! cmp -s "$EXP/$golden" "$norm"; then
      fail "$label" "differs from an earlier case recording expected/$golden"; return
    fi
    cp "$norm" "$EXP/$golden"; pass "$label (recorded)"; return
  fi
  if [ ! -f "$EXP/$golden" ]; then fail "$label" "no expected/$golden in bundle"; return; fi
  if cmp -s "$EXP/$golden" "$norm"; then pass "$label"; else
    diff "$EXP/$golden" "$norm" > "$WORK/log/$label.diff" 2>&1
    fail "$label" "output differs from the Linux build (expected/$golden)" "$WORK/log/$label.diff"
  fi
}

# zq <name> <args...>: run zpq in $WORK; stdout/stderr land in log/<name>.{out,err}.
zq() {
  local name="$1"; shift
  (cd "$WORK" && "$ZPQ" "$@") > "$WORK/log/$name.out" 2> "$WORK/log/$name.err"
}

# case_out <name> <args...>: zpq must succeed; its stdout is compared.
case_out() {
  local name="$1"
  if zq "$@"; then compare "$name" "$WORK/log/$name.out"; else fail "$name" "zpq exited $?" "$WORK/log/$name.err"; fi
}

# case_write <name> <golden> <codec or -> <args...>: zpq writes out/<name>.parquet; its content (dumped as CSV by
# zpq) is compared against <golden>, and the codec it reports against <codec>.
case_write() {
  local name="$1" golden="$2" codec="$3"; shift 3
  if ! zq "$name" query "$@" -o "out/$name.parquet"; then fail "$name" "write failed" "$WORK/log/$name.err"; return; fi
  if [ "$codec" != - ] && ! grep -q "\"codec\":\"$codec\"" "$WORK/log/$name.err"; then
    fail "$name" "reported codec is not $codec" "$WORK/log/$name.err"; return
  fi
  if zq "$name.dump" query "out/$name.parquet" --format csv; then
    compare "$golden" "$WORK/log/$name.dump.out" "$name"
  else
    fail "$name" "zpq cannot read back its own output" "$WORK/log/$name.dump.err"
  fi
}

# case_fail <name> <args...>: zpq must fail, and leave no out/<name>.parquet behind.
case_fail() {
  local name="$1"; shift
  if zq "$name" query "$@" -o "out/$name.parquet"; then fail "$name" "expected a failure"; return; fi
  if [ -e "$WORK/out/$name.parquet" ]; then fail "$name" "left a partial output file"; return; fi
  pass "$name"
}

echo "== zpq macOS check: $(sed -n 's/^commit=//p' "$ROOT/MANIFEST" 2> /dev/null)"
echo "   host: $(uname -sm)$(command -v sw_vers > /dev/null 2>&1 && printf ', macOS %s' "$(sw_vers -productVersion)")"
echo "   binary: $ZPQ$(command -v file > /dev/null 2>&1 && printf ' (%s)' "$(file -b "$ZPQ" | cut -d, -f1)")"

# ---------------------------------------------------------------- unit tests
if [ "$RECORD" = 0 ]; then
  for t in zpq-test zpq-lambda-test; do
    if [ ! -x "$BIN/$t" ]; then skip "unit:$t" "not in bundle"; continue; fi
    # Tests read fixtures cwd-relative (data/, ci/), so run from the bundle root.
    (cd "$ROOT" && "$BIN/$t") > "$WORK/log/unit-$t.log" 2>&1
    rc=$?
    summary="$(grep -E '^(All [0-9]+ tests passed|[0-9]+ passed; [0-9]+ skipped; [0-9]+ failed)' "$WORK/log/unit-$t.log" | tail -n 1)"
    if [ $rc -ne 0 ]; then
      grep -E 'FAIL|error|panic' "$WORK/log/unit-$t.log" > "$WORK/log/unit-$t.fail" 2>&1
      fail "unit:$t" "exit $rc${summary:+, $summary}" "$WORK/log/unit-$t.fail"
      continue
    fi
    # Every skip must be one the Linux run of this bundle had too, or a test that is Linux-only by design.
    skipped="$(printf '%s' "$summary" | sed -n 's/.*passed; \([0-9]*\) skipped.*/\1/p')"
    skip_names "$WORK/log/unit-$t.log" > "$WORK/log/unit-$t.skips"
    cat "$ROOT/skips/$t.linux" "$ROOT/skips/$t.linux-only" > "$WORK/log/unit-$t.allowed" 2> /dev/null
    grep -vxF -f "$WORK/log/unit-$t.allowed" "$WORK/log/unit-$t.skips" > "$WORK/log/unit-$t.extra"
    named="$(wc -l < "$WORK/log/unit-$t.skips" | tr -d ' ')"
    if [ ! -f "$ROOT/skips/$t.linux" ]; then
      fail "unit:$t" "no skips/$t.linux in bundle"
    elif [ "${skipped:-0}" != "$named" ]; then
      fail "unit:$t" "the summary counts ${skipped:-0} skipped, but $named skipped tests were found in the log" "$WORK/log/unit-$t.skips"
    elif [ -s "$WORK/log/unit-$t.extra" ]; then
      fail "unit:$t" "$(wc -l < "$WORK/log/unit-$t.extra" | tr -d ' ') skipped here, not on Linux, and not Linux-only" "$WORK/log/unit-$t.extra"
    else
      pass "unit:$t (${summary%.}; $(wc -l < "$ROOT/skips/$t.linux-only" | tr -d ' ') Linux-only)"
    fi
  done
fi

# ---------------------------------------------------------------- CLI smoke
AGG='count(*) AS n, sum(qty) AS s, min(price) AS lo, max(name) AS hi, avg(opt) AS a'
WCOLS='id,qty,price,name,opt,day'

case_out version --version
case_out schema schema smoke/part-0.parquet
case_out conform conform smoke/alltypes_plain.parquet

# Reads: one file, a filter, an explicit file list, a directory glob, a cwd glob.
case_out agg_single query smoke/part-0.parquet -a "$AGG"
case_out agg_filter query smoke/part-1.parquet -a "$AGG" --filter "qty > 50 AND cat = 'red'"
case_out agg_files query smoke/part-0.parquet smoke/part-1.parquet smoke/part-2.parquet smoke/part-3.parquet -a "$AGG"
case_out agg_glob query 'smoke/part-*.parquet' -a "$AGG"
if (cd "$WORK/smoke" && "$ZPQ" query 'part-*.parquet' -a "$AGG") > "$WORK/log/agg_glob_cwd.out" 2> "$WORK/log/agg_glob_cwd.err"; then
  compare agg_glob_cwd "$WORK/log/agg_glob_cwd.out"
else
  fail agg_glob_cwd "zpq exited $?" "$WORK/log/agg_glob_cwd.err"
fi
# Every -j answers the same; all compare against one golden.
for j in 1 4 16; do
  if zq "groupby_j$j" query 'smoke/part-*.parquet' --group-by cat -a "$AGG" -j "$j"; then
    compare groupby "$WORK/log/groupby_j$j.out" "groupby_j$j"
  else
    fail "groupby_j$j" "zpq exited $?" "$WORK/log/groupby_j$j.err"
  fi
done
case_out sql query --query "SELECT cat, count(*) AS n, sum(qty) AS s FROM 'smoke/part-*.parquet' WHERE qty > 50 GROUP BY cat"

# Row output.
case_out csv query 'smoke/part-*.parquet' --format csv --filter 'opt IS NULL' --columns id,qty,name,opt,day --limit 500 -j 4
case_out jsonl query 'smoke/part-*.parquet' --format jsonl --select "id, qty * 2 AS q2, name || '-' || cat AS nc" --filter 'qty < 3' --limit 300
if (cd "$WORK" && "$ZPQ" query 'smoke/part-*.parquet' --format csv 2> /dev/null | head -n 1) > "$WORK/log/csv_pipe.out"; then
  compare csv_pipe "$WORK/log/csv_pipe.out"
else
  fail csv_pipe "zpq into a closed pipe failed"
fi

# Writes: each codec, multi-file at several -j, a computed select, an aggregate.
for codec in snappy zstd gzip lz4 uncompressed; do
  upper="$(printf '%s' "$codec" | tr a-z A-Z)"; [ "$codec" = lz4 ] && upper=LZ4_RAW
  case_write "w_$codec" write_filter "$upper" smoke/part-0.parquet --filter 'qty >= 25' --columns "$WCOLS" --codec "$codec"
done
for j in 1 3 8; do
  case_write "w_multi_j$j" write_multi - 'smoke/part-*.parquet' --filter 'qty < 10' -j "$j"
done
case_write w_select write_select - smoke/part-2.parquet --select 'id, price * 2 AS p2, coalesce(opt, -1) AS o' --filter "cat = 'blue'"
case_write w_copy write_copy - smoke/part-3.parquet smoke/part-0.parquet
if zq w_agg query 'smoke/part-*.parquet' --group-by cat -a 'count(*) AS n, sum(qty) AS s' -o out/w_agg.parquet \
  && zq w_agg.dump query out/w_agg.parquet --format csv; then
  compare write_agg "$WORK/log/w_agg.dump.out" w_agg
else
  fail w_agg "aggregate write or read-back failed" "$WORK/log/w_agg.err"
fi

# Failures: a codec zpq cannot recompress mid-write, and a missing input.
case_fail fail_brotli smoke/brotli.parquet --codec zstd
case_fail fail_missing smoke/no-such-file.parquet

# ---------------------------------------------------------------- pyarrow cross-check
PY="${PYTHON:-python3}"
if [ "$RECORD" = 1 ]; then
  :
elif [ "$USE_PY" = 0 ]; then
  skip pyarrow "--no-python"
elif ! command -v "$PY" > /dev/null 2>&1 || ! "$PY" -c 'import pyarrow' > /dev/null 2>&1; then
  skip pyarrow "no python3 with pyarrow"
else
  if "$PY" - "$WORK" > "$WORK/log/pyarrow.out" 2>&1 <<'PY'
import sys, pyarrow as pa, pyarrow.compute as pc, pyarrow.parquet as pq
w = sys.argv[1]
parts = [pq.read_table(f"{w}/smoke/part-{i}.parquet") for i in range(4)]
cols = ["id", "qty", "price", "name", "opt", "day"]

def codecs(path):
    m = pq.ParquetFile(path).metadata
    return {m.row_group(r).column(c).compression for r in range(m.num_row_groups) for c in range(m.num_columns)}

def same(name, got, want):
    assert got.equals(want), f"{name}: values differ\n got {got.slice(0, 3).to_pylist()}\nwant {want.slice(0, 3).to_pylist()}"

src = parts[0]
want = src.filter(pc.greater_equal(src["qty"], 25)).select(cols)
# pyarrow names parquet's LZ4_RAW codec "LZ4".
for codec, name in [("snappy", "SNAPPY"), ("zstd", "ZSTD"), ("gzip", "GZIP"), ("lz4", "LZ4"), ("uncompressed", "UNCOMPRESSED")]:
    path = f"{w}/out/w_{codec}.parquet"
    assert codecs(path) == {name}, f"w_{codec}: chunks are {codecs(path)}"
    same(f"w_{codec}", pq.read_table(path), want)

allp = pa.concat_tables(parts)
for j in (1, 3, 8):
    same(f"w_multi_j{j}", pq.read_table(f"{w}/out/w_multi_j{j}.parquet"), allp.filter(pc.less(allp["qty"], 10)))

p2 = parts[2].filter(pc.equal(parts[2]["cat"], "blue"))
got = pq.read_table(f"{w}/out/w_select.parquet")
assert got.column_names == ["id", "p2", "o"], got.column_names
assert got["id"].to_pylist() == p2["id"].to_pylist()
assert got["p2"].to_pylist() == [v * 2 for v in p2["price"].to_pylist()]
assert got["o"].to_pylist() == [-1 if v is None else v for v in p2["opt"].to_pylist()]

same("w_copy", pq.read_table(f"{w}/out/w_copy.parquet"), pa.concat_tables([parts[3], parts[0]]))
print("pyarrow: every written file reads back with the expected codec and values")
PY
  then pass "pyarrow ($(tail -n 1 "$WORK/log/pyarrow.out"))"; else fail pyarrow "cross-check failed" "$WORK/log/pyarrow.out"; fi
fi

# ---------------------------------------------------------------- network (opt-in)
if [ "$RECORD" = 0 ] && [ "$NETWORK" = 1 ]; then
  # An anonymous request (S3_NO_SIGN_REQUEST: no keys, no Authorization header) for a bucket that does not exist:
  # S3 answers 403/404 (BadResponse), and getting that answer takes working DNS, TCP, TLS (certificate and hostname
  # checks) and HTTP. DnsFailed / ConnectFailed / HandshakeFailed mean the resolver, socket or TLS layer is broken
  # on this platform; NoCredentials means the binary never tried the network.
  (cd "$WORK" && env -u AWS_ACCESS_KEY_ID -u AWS_SECRET_ACCESS_KEY -u AWS_SESSION_TOKEN -u S3_ACCESS_KEY_ID \
    -u S3_SECRET_ACCESS_KEY -u S3_SESSION_TOKEN -u S3_REGION -u S3_ENDPOINT_URL S3_NO_SIGN_REQUEST=1 AWS_REGION=us-east-1 \
    "$ZPQ" schema s3://zpq-macos-check-no-such-bucket/x.parquet) > "$WORK/log/s3_reach.out" 2>&1
  if grep -q 'BadResponse' "$WORK/log/s3_reach.out"; then pass "s3_reach (DNS + TCP + TLS + HTTP)"; else
    fail s3_reach "expected S3 to refuse an anonymous request" "$WORK/log/s3_reach.out"
  fi
  if [ -n "${ZPQ_CHECK_S3_URL:-}" ]; then
    if (cd "$WORK" && "$ZPQ" schema "$ZPQ_CHECK_S3_URL" && "$ZPQ" query "$ZPQ_CHECK_S3_URL" -a 'count(*) AS n') \
      > "$WORK/log/s3_read.out" 2>&1; then pass "s3_read $ZPQ_CHECK_S3_URL"; else
      fail s3_read "$ZPQ_CHECK_S3_URL" "$WORK/log/s3_read.out"
    fi
  else
    skip s3_read "ZPQ_CHECK_S3_URL not set"
  fi
fi

echo
if [ "$N_FAIL" -eq 0 ]; then
  echo "PASS: $N_PASS passed, $N_SKIP skipped"
  exit 0
fi
echo "FAIL: $N_FAIL failed ($FAILED ), $N_PASS passed, $N_SKIP skipped"
exit 1
